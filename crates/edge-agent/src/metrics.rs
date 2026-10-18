use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::outbox::OutboxHandle;

/// Live counters shared by all pipeline tasks, read by the health log.
///
/// Each counter is independent and only read for logging, so `Ordering::Relaxed` is enough:
/// there is no other memory these atomics have to synchronize with.
#[derive(Debug, Default)]
pub struct Metrics {
    frames_read: AtomicU64,
    unknown_pgns: AtomicU64,
    decode_errors: AtomicU64,
    samples_written: AtomicU64,
    upload_failures: AtomicU64,
    /// Unix time in milliseconds, 0 means no upload yet.
    last_upload_ms: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub frames_read: u64,
    pub unknown_pgns: u64,
    pub decode_errors: u64,
    pub samples_written: u64,
    pub upload_failures: u64,
    pub last_upload: Option<SystemTime>,
}

impl Metrics {
    pub fn add_frame(&self) {
        self.frames_read.fetch_add(1, Ordering::Relaxed);
    }

    /// Frames with a PGN we do not decode. Normal on a real bus, so not an error.
    pub fn add_unknown_pgn(&self) {
        self.unknown_pgns.fetch_add(1, Ordering::Relaxed);
    }

    /// Frames of a known PGN that could not be decoded, i.e. a broken sender.
    pub fn add_decode_error(&self) {
        self.decode_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_samples(&self, n: u64) {
        self.samples_written.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_upload_failure(&self) {
        self.upload_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_upload(&self, at: SystemTime) {
        let ms = at
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        self.last_upload_ms.store(ms, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Snapshot {
        let last_upload = match self.last_upload_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(UNIX_EPOCH + Duration::from_millis(ms)),
        };
        Snapshot {
            frames_read: self.frames_read.load(Ordering::Relaxed),
            unknown_pgns: self.unknown_pgns.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            samples_written: self.samples_written.load(Ordering::Relaxed),
            upload_failures: self.upload_failures.load(Ordering::Relaxed),
            last_upload,
        }
    }
}

/// Logs one status line every `every` until `stop` is cancelled.
pub async fn log_health(
    metrics: Arc<Metrics>,
    outbox: OutboxHandle,
    every: Duration,
    stop: CancellationToken,
) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    let mut last_frames = metrics.snapshot().frames_read;
    let mut last_tick = Instant::now();
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let now = metrics.snapshot();
        let frames_per_sec =
            (now.frames_read - last_frames) as f64 / last_tick.elapsed().as_secs_f64();
        (last_frames, last_tick) = (now.frames_read, Instant::now());

        let outbox_depth = outbox.depth().await;
        info!(
            frames_per_sec = format_args!("{frames_per_sec:.1}"),
            unknown_pgns = now.unknown_pgns,
            decode_errors = now.decode_errors,
            samples = now.samples_written,
            outbox_depth,
            upload_failures = now.upload_failures,
            last_upload = %describe_last_upload(now.last_upload, SystemTime::now()),
            "health"
        );
    }
}

fn describe_last_upload(last: Option<SystemTime>, now: SystemTime) -> String {
    match last {
        None => "never".to_owned(),
        Some(at) => {
            let ago = now.duration_since(at).unwrap_or_default().as_secs();
            format!("{ago}s ago")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_snapshots() {
        let metrics = Metrics::default();
        assert_eq!(metrics.snapshot().last_upload, None);

        metrics.add_frame();
        metrics.add_frame();
        metrics.add_unknown_pgn();
        metrics.add_decode_error();
        metrics.add_samples(8);
        metrics.add_upload_failure();
        let at = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        metrics.record_upload(at);

        assert_eq!(
            metrics.snapshot(),
            Snapshot {
                frames_read: 2,
                unknown_pgns: 1,
                decode_errors: 1,
                samples_written: 8,
                upload_failures: 1,
                last_upload: Some(at),
            }
        );
    }

    #[test]
    fn describes_last_upload() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        assert_eq!(describe_last_upload(None, now), "never");
        assert_eq!(
            describe_last_upload(Some(now - Duration::from_secs(42)), now),
            "42s ago"
        );
        assert_eq!(
            describe_last_upload(Some(now + Duration::from_secs(5)), now),
            "0s ago"
        );
    }
}
