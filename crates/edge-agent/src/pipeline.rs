use std::io;
use std::sync::Arc;
use std::time::Duration;

use j1939::DecodeError;
use thiserror::Error;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::aggregate::{Aggregator, Sample};
use crate::decode::{DecodeStats, Reading, decode_frame};
use crate::frame::Frame;
use crate::outbox::{Outbox, OutboxError, OutboxHandle};
use crate::source::FrameSource;
use crate::uplink::{Uplink, UplinkConfig, UplinkStats};

const CHANNEL_CAPACITY: usize = 1024;
/// Close the current aggregation bucket if no reading arrives for this long (bus went quiet).
const IDLE_FLUSH: Duration = Duration::from_secs(2);
/// Upper bound of samples written to SQLite in one transaction.
const WRITE_CHUNK: usize = 512;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("reading frames failed: {0}")]
    Source(#[from] io::Error),
    #[error(transparent)]
    Outbox(#[from] OutboxError),
    #[error("cannot create HTTP client: {0}")]
    Http(#[from] reqwest::Error),
}

#[derive(Debug)]
pub struct Summary {
    pub frames_read: u64,
    pub decode: DecodeStats,
    pub samples_written: u64,
    pub uplink: UplinkStats,
    pub outbox_depth: u64,
}

/// Runs reader -> decoder -> aggregator -> outbox writer, plus the uplink task.
///
/// Stops when the source is exhausted or `shutdown` is cancelled. Either way every stage
/// drains its input in order, so all readings end up in the outbox, and then the uplink
/// makes a final attempt to upload what is left.
pub async fn run<S>(
    source: S,
    outbox: Outbox,
    config: UplinkConfig,
    shutdown: CancellationToken,
) -> Result<Summary, PipelineError>
where
    S: FrameSource + Send + 'static,
{
    let outbox = OutboxHandle::new(outbox);
    let work_waiting = Arc::new(Notify::new());
    let batch_size = config.batch_size;
    let uplink = Uplink::new(config, outbox.clone(), Arc::clone(&work_waiting))?;
    let uplink_shutdown = CancellationToken::new();

    let (frame_tx, frame_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (reading_tx, reading_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (sample_tx, sample_rx) = mpsc::channel(CHANNEL_CAPACITY);

    let reader = tokio::spawn(read_frames(source, frame_tx, shutdown));
    let decoder = tokio::spawn(decode_frames(frame_rx, reading_tx));
    let aggregator = tokio::spawn(aggregate(reading_rx, sample_tx, IDLE_FLUSH));
    let writer = tokio::spawn(write_samples(
        sample_rx,
        outbox.clone(),
        work_waiting,
        batch_size,
    ));
    let uplink = tokio::spawn(uplink.run(uplink_shutdown.clone()));

    // A JoinError means a task panicked, which is a bug, so it is not turned into a PipelineError.
    let frames_read = reader.await.expect("reader task panicked");
    let decode = decoder.await.expect("decoder task panicked");
    aggregator.await.expect("aggregator task panicked");
    let samples_written = writer.await.expect("writer task panicked");
    // Everything that will ever reach the outbox is there now.
    uplink_shutdown.cancel();
    let uplink = uplink.await.expect("uplink task panicked");

    Ok(Summary {
        frames_read: frames_read?,
        decode,
        samples_written: samples_written?,
        uplink: uplink?,
        outbox_depth: outbox.depth().await,
    })
}

pub async fn read_frames<S: FrameSource>(
    mut source: S,
    tx: mpsc::Sender<Frame>,
    shutdown: CancellationToken,
) -> io::Result<u64> {
    let mut count = 0;
    loop {
        let frame = tokio::select! {
            _ = shutdown.cancelled() => break,
            frame = source.next_frame() => frame?,
        };
        let Some(frame) = frame else { break };
        if tx.send(frame).await.is_err() {
            break;
        }
        count += 1;
    }
    Ok(count)
}

pub async fn decode_frames(
    mut rx: mpsc::Receiver<Frame>,
    tx: mpsc::Sender<Reading>,
) -> DecodeStats {
    let mut stats = DecodeStats::default();
    while let Some(frame) = rx.recv().await {
        match decode_frame(&frame, &mut stats) {
            Ok(readings) => {
                for reading in readings {
                    if tx.send(reading).await.is_err() {
                        return stats;
                    }
                }
            }
            Err(e @ DecodeError::UnknownPgn(_)) => {
                debug!(id = format_args!("{:08X}", frame.id), error = %e, "skipping frame");
            }
            Err(e @ DecodeError::InvalidLength { .. }) => {
                warn!(id = format_args!("{:08X}", frame.id), error = %e, "skipping frame");
            }
        }
    }
    stats
}

pub async fn aggregate(
    mut rx: mpsc::Receiver<Reading>,
    tx: mpsc::Sender<Sample>,
    idle_flush: Duration,
) {
    let mut aggregator = Aggregator::default();
    loop {
        let samples = match tokio::time::timeout(idle_flush, rx.recv()).await {
            Ok(Some(reading)) => aggregator.push(&reading),
            Ok(None) => break,
            Err(_idle) => aggregator.flush(),
        };
        for sample in samples {
            debug!(signal = sample.signal, value = sample.value, "sample");
            if tx.send(sample).await.is_err() {
                return;
            }
        }
    }
    for sample in aggregator.flush() {
        if tx.send(sample).await.is_err() {
            return;
        }
    }
}

/// Writes samples to the outbox, grouping whatever is already queued into one transaction.
pub async fn write_samples(
    mut rx: mpsc::Receiver<Sample>,
    outbox: OutboxHandle,
    work_waiting: Arc<Notify>,
    batch_size: usize,
) -> Result<u64, OutboxError> {
    let mut written = 0;
    while let Some(first) = rx.recv().await {
        let mut samples = vec![first];
        while samples.len() < WRITE_CHUNK {
            match rx.try_recv() {
                Ok(sample) => samples.push(sample),
                Err(_) => break,
            }
        }
        written += samples.len() as u64;
        if outbox.append(samples).await? >= batch_size as u64 {
            work_waiting.notify_one();
        }
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use crate::decode::Signal;

    use super::*;

    fn reading(millis: u64, value: f64) -> Reading {
        Reading {
            timestamp: UNIX_EPOCH + Duration::from_millis(millis),
            source_address: 0,
            signal: Signal::CoolantTempC,
            value,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_input_closes_the_bucket() {
        let (reading_tx, reading_rx) = mpsc::channel(8);
        let (sample_tx, mut sample_rx) = mpsc::channel(8);
        tokio::spawn(aggregate(reading_rx, sample_tx, Duration::from_secs(2)));

        reading_tx.send(reading(1_000, 80.0)).await.unwrap();
        let sample = sample_rx.recv().await.unwrap();
        assert_eq!(sample.value, 80.0);
        assert_eq!(sample.ts, UNIX_EPOCH + Duration::from_secs(1));
    }

    #[tokio::test]
    async fn end_of_input_flushes_the_last_bucket() {
        let (reading_tx, reading_rx) = mpsc::channel(8);
        let (sample_tx, mut sample_rx) = mpsc::channel(8);
        let task = tokio::spawn(aggregate(reading_rx, sample_tx, Duration::from_secs(60)));

        reading_tx.send(reading(1_000, 80.0)).await.unwrap();
        reading_tx.send(reading(2_000, 81.0)).await.unwrap();
        drop(reading_tx);
        task.await.unwrap();

        let mut values = Vec::new();
        while let Some(sample) = sample_rx.recv().await {
            values.push(sample.value);
        }
        assert_eq!(values, [80.0, 81.0]);
    }
}
