use std::collections::BTreeMap;
use std::io;
use std::time::Duration;

use j1939::DecodeError;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::decode::{DecodeStats, Reading, Signal, decode_frame};
use crate::frame::Frame;
use crate::source::FrameSource;

const CHANNEL_CAPACITY: usize = 1024;
const LOG_EVERY: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct Summary {
    pub frames_read: u64,
    pub stats: DecodeStats,
    pub latest: BTreeMap<Signal, f64>,
}

/// Runs reader -> decoder -> logger as three tasks connected by bounded channels.
/// Returns when the source is exhausted and every stage has drained its input.
pub async fn run<S>(source: S) -> io::Result<Summary>
where
    S: FrameSource + Send + 'static,
{
    let (frame_tx, frame_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (reading_tx, reading_rx) = mpsc::channel(CHANNEL_CAPACITY);

    let reader = tokio::spawn(read_frames(source, frame_tx));
    let decoder = tokio::spawn(decode_frames(frame_rx, reading_tx));
    let logger = tokio::spawn(log_readings(reading_rx, LOG_EVERY));

    // A JoinError here means a task panicked, which is a bug, so it is not turned into an io::Error.
    let frames_read = reader.await.expect("reader task panicked")?;
    let stats = decoder.await.expect("decoder task panicked");
    let latest = logger.await.expect("logger task panicked");

    Ok(Summary {
        frames_read,
        stats,
        latest,
    })
}

pub async fn read_frames<S: FrameSource>(
    mut source: S,
    tx: mpsc::Sender<Frame>,
) -> io::Result<u64> {
    let mut count = 0;
    while let Some(frame) = source.next_frame().await? {
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

/// Logs every reading at debug level and the latest value of each signal once per `every`.
/// This is the end of the pipeline until the aggregator and outbox exist.
pub async fn log_readings(
    mut rx: mpsc::Receiver<Reading>,
    every: Duration,
) -> BTreeMap<Signal, f64> {
    let mut latest = BTreeMap::new();
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        tokio::select! {
            reading = rx.recv() => {
                let Some(reading) = reading else { break };
                debug!(
                    signal = %reading.signal,
                    value = reading.value,
                    source_address = reading.source_address,
                    "reading"
                );
                latest.insert(reading.signal, reading.value);
            }
            _ = ticker.tick() => {
                if !latest.is_empty() {
                    info!("{}", format_latest(&latest));
                }
            }
        }
    }
    latest
}

pub fn format_latest(latest: &BTreeMap<Signal, f64>) -> String {
    latest
        .iter()
        .map(|(signal, value)| match signal {
            Signal::LatitudeDeg | Signal::LongitudeDeg => format!("{signal}={value:.6}"),
            _ => format!("{signal}={value:.1}"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use crate::source::ReplaySource;

    use super::*;

    #[tokio::test]
    async fn runs_a_replay_to_the_end() {
        let log = "\
(1.000000) vcan0 0CF00400#FFFFFFE02EFFFFFF
(1.010000) vcan0 18FEEE00#7DFFFFFFFFFFFFFF
(1.020000) vcan0 18FF1200#0011223344556677
(1.030000) vcan0 0CF00400#FFFF
(1.040000) vcan0 0CF00400#FFFFFF0032FFFFFF
";
        let summary = run(ReplaySource::new(log.as_bytes(), false)).await.unwrap();

        assert_eq!(summary.frames_read, 5);
        assert_eq!(
            summary.stats,
            DecodeStats {
                frames: 5,
                decoded: 3,
                unknown_pgn: 1,
                invalid_length: 1,
            }
        );
        assert_eq!(
            summary.latest,
            BTreeMap::from([
                (Signal::EngineSpeedRpm, 1600.0),
                (Signal::CoolantTempC, 85.0)
            ])
        );
    }

    #[test]
    fn formats_latest_values() {
        let latest = BTreeMap::from([
            (Signal::EngineSpeedRpm, 1999.875),
            (Signal::LatitudeDeg, 49.47191234),
        ]);
        assert_eq!(
            format_latest(&latest),
            "engine_speed_rpm=1999.9 latitude_deg=49.471912"
        );
    }
}
