use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use j1939::DecodeError;
use thiserror::Error;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::aggregate::{Aggregator, Sample};
use crate::decode::{Reading, decode_frame};
use crate::frame::Frame;
use crate::metrics::{self, Metrics, Snapshot};
use crate::outbox::{Outbox, OutboxError, OutboxHandle};
use crate::source::FrameSource;
use crate::uplink::{Uplink, UplinkConfig};

const CHANNEL_CAPACITY: usize = 1024;
/// Close the current aggregation bucket if no reading arrives for this long (bus went quiet).
const IDLE_FLUSH: Duration = Duration::from_secs(2);
/// Upper bound of samples written to SQLite in one transaction.
const WRITE_CHUNK: usize = 512;
/// A broken sender can produce bad frames at bus speed. Only the first ones are logged
/// one by one; after that the health line's `decode_errors` counter tells the story.
const LOGGED_DECODE_ERRORS: u64 = 20;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("reading frames failed: {0}")]
    Source(#[source] io::Error),
    #[error("writing to the outbox failed: {0}")]
    Writer(#[source] OutboxError),
    #[error("uploading from the outbox failed: {0}")]
    Uplink(#[source] OutboxError),
    #[error("cannot create the HTTP client: {0}")]
    HttpClient(#[source] reqwest::Error),
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub uplink: UplinkConfig,
    pub health_interval: Duration,
}

/// Counters at the end of the run, plus what is still waiting in the outbox.
#[derive(Debug, Clone, Copy)]
pub struct Summary {
    pub metrics: Snapshot,
    pub outbox_depth: u64,
}

/// Runs reader -> decoder -> aggregator -> outbox writer, plus the uplink and health tasks.
///
/// Stops when the source is exhausted, when `shutdown` is cancelled, or when a stage fails
/// (which cancels `shutdown`). Every stage then drains its input in order, so all readings
/// reach the outbox, and the uplink makes a final attempt to upload what is left.
///
/// # Errors
///
/// The first error of the source, the outbox writer or the uplink. The pipeline is
/// drained and stopped either way.
///
/// # Panics
///
/// Re-raises a panic from any of its tasks.
pub async fn run<S>(
    source: S,
    outbox: Outbox,
    config: PipelineConfig,
    shutdown: CancellationToken,
) -> Result<Summary, PipelineError>
where
    S: FrameSource + Send + 'static,
{
    let outbox = OutboxHandle::new(outbox);
    let metrics = Arc::new(Metrics::default());
    let work_waiting = Arc::new(Notify::new());
    let batch_size = config.uplink.batch_size;
    let uplink = Uplink::new(
        config.uplink,
        outbox.clone(),
        Arc::clone(&work_waiting),
        Arc::clone(&metrics),
    )
    .map_err(PipelineError::HttpClient)?;
    let uplink_shutdown = CancellationToken::new();
    let health_stop = CancellationToken::new();

    let (frame_tx, frame_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (reading_tx, reading_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (sample_tx, sample_rx) = mpsc::channel(CHANNEL_CAPACITY);

    let health = tokio::spawn(metrics::log_health(
        Arc::clone(&metrics),
        outbox.clone(),
        config.health_interval,
        health_stop.clone(),
    ));
    let reader = spawn_stage(
        &shutdown,
        read_frames(source, frame_tx, shutdown.clone(), Arc::clone(&metrics)),
    );
    let decoder = tokio::spawn(decode_frames(frame_rx, reading_tx, Arc::clone(&metrics)));
    let aggregator = tokio::spawn(aggregate(reading_rx, sample_tx, IDLE_FLUSH));
    let writer = spawn_stage(
        &shutdown,
        write_samples(
            sample_rx,
            outbox.clone(),
            work_waiting,
            batch_size,
            Arc::clone(&metrics),
        ),
    );
    let uplink = spawn_stage(&shutdown, uplink.run(uplink_shutdown.clone()));

    let read = join(reader).await;
    join(decoder).await;
    join(aggregator).await;
    let written = join(writer).await;
    // Everything that will ever reach the outbox is there now.
    uplink_shutdown.cancel();
    let uploaded = join(uplink).await;
    health_stop.cancel();
    join(health).await;

    read.map_err(PipelineError::Source)?;
    written.map_err(PipelineError::Writer)?;
    uploaded.map_err(PipelineError::Uplink)?;
    Ok(Summary {
        metrics: metrics.snapshot(),
        outbox_depth: outbox.depth(),
    })
}

/// Spawns a stage that can fail. A failure cancels `shutdown`, so the whole pipeline
/// stops instead of running on with a dead stage (e.g. reading but never uploading).
fn spawn_stage<T, E>(
    shutdown: &CancellationToken,
    stage: impl Future<Output = Result<T, E>> + Send + 'static,
) -> JoinHandle<Result<T, E>>
where
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    let shutdown = shutdown.clone();
    tokio::spawn(async move {
        let result = stage.await;
        if let Err(e) = &result {
            error!(error = %e, "pipeline stage failed, shutting down");
            shutdown.cancel();
        }
        result
    })
}

/// Waits for a task and re-raises its panic with the original payload.
async fn join<T>(task: JoinHandle<T>) -> T {
    match task.await {
        Ok(value) => value,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => unreachable!("pipeline tasks are never aborted: {e}"),
    }
}

pub(crate) async fn read_frames<S: FrameSource>(
    mut source: S,
    tx: mpsc::Sender<Frame>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
) -> io::Result<()> {
    loop {
        let frame = tokio::select! {
            () = shutdown.cancelled() => break,
            frame = source.next_frame() => frame?,
        };
        let Some(frame) = frame else { break };
        metrics.add_frame();
        if tx.send(frame).await.is_err() {
            break;
        }
    }
    Ok(())
}

pub(crate) async fn decode_frames(
    mut rx: mpsc::Receiver<Frame>,
    tx: mpsc::Sender<Reading>,
    metrics: Arc<Metrics>,
) {
    while let Some(frame) = rx.recv().await {
        match decode_frame(&frame) {
            Ok(readings) => {
                for reading in readings {
                    if tx.send(reading).await.is_err() {
                        return;
                    }
                }
            }
            Err(e @ DecodeError::UnknownPgn(_)) => {
                metrics.add_unknown_pgn();
                debug!(id = format_args!("{:08X}", frame.id), error = %e, "skipping frame");
            }
            Err(e @ DecodeError::InvalidLength { .. }) => {
                let count = metrics.add_decode_error();
                if count <= LOGGED_DECODE_ERRORS {
                    warn!(id = format_args!("{:08X}", frame.id), error = %e, "skipping frame");
                }
                if count == LOGGED_DECODE_ERRORS {
                    warn!("further decode errors are only counted in the health log");
                }
            }
        }
    }
}

pub(crate) async fn aggregate(
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
pub(crate) async fn write_samples(
    mut rx: mpsc::Receiver<Sample>,
    outbox: OutboxHandle,
    work_waiting: Arc<Notify>,
    batch_size: usize,
    metrics: Arc<Metrics>,
) -> Result<(), OutboxError> {
    while let Some(first) = rx.recv().await {
        let mut samples = vec![first];
        while samples.len() < WRITE_CHUNK {
            match rx.try_recv() {
                Ok(sample) => samples.push(sample),
                Err(_) => break,
            }
        }
        let count = samples.len() as u64;
        let depth = outbox.append(samples).await?;
        metrics.add_samples(count);
        if depth >= batch_size as u64 {
            work_waiting.notify_one();
        }
    }
    Ok(())
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

    #[tokio::test]
    async fn a_failing_stage_cancels_the_pipeline() {
        let shutdown = CancellationToken::new();
        let ok = spawn_stage(&shutdown, async { Ok::<_, String>(()) });
        assert_eq!(join(ok).await, Ok(()));
        assert!(!shutdown.is_cancelled());

        let failed = spawn_stage(&shutdown, async { Err::<(), _>("disk full".to_owned()) });
        assert!(join(failed).await.is_err());
        assert!(shutdown.is_cancelled());
    }

    #[tokio::test]
    #[should_panic(expected = "stage bug")]
    async fn join_re_raises_the_original_panic() {
        join(tokio::spawn(async { panic!("stage bug") })).await;
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
