use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::outbox::{Batch, OutboxError, OutboxHandle};

#[derive(Debug, Clone)]
pub struct UplinkConfig {
    /// Receiver base URL, batches go to `{url}/ingest`.
    pub url: String,
    pub token: String,
    pub device_id: String,
    pub batch_size: usize,
    /// Upload at least this often, or sooner when `batch_size` records are waiting.
    pub flush_interval: Duration,
    pub request_timeout: Duration,
    /// How long the last upload attempt on shutdown may take.
    pub final_flush_timeout: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UplinkStats {
    pub batches: u64,
    pub records: u64,
    pub failures: u64,
}

/// Exponential backoff with "equal jitter": half of the delay is fixed, half random.
/// Many devices that lost the network together do not retry in lockstep when it comes
/// back, and no retry ever happens immediately.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    pub fn next_delay(&mut self, rng: &mut impl Rng) -> Duration {
        let ceiling = self
            .base
            .saturating_mul(1 << self.attempt.min(20))
            .min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        let half = ceiling / 2;
        half + half.mul_f64(rng.random::<f64>())
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[derive(Debug, Error)]
enum SendError {
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("receiver answered {0}")]
    Status(StatusCode),
}

#[derive(Serialize)]
struct BatchBody<'a> {
    device_id: &'a str,
    batch_id: String,
    dropped_records: u64,
    records: Vec<RecordBody<'a>>,
}

#[derive(Serialize)]
struct RecordBody<'a> {
    seq: i64,
    ts: String,
    signal: &'a str,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct IngestResponse {
    inserted: u64,
    duplicates: u64,
}

pub struct Uplink {
    config: UplinkConfig,
    ingest_url: String,
    client: reqwest::Client,
    outbox: OutboxHandle,
    work_waiting: Arc<Notify>,
    backoff: Backoff,
    stats: UplinkStats,
}

impl Uplink {
    /// `work_waiting` is notified by the writer when a full batch is ready.
    pub fn new(
        config: UplinkConfig,
        outbox: OutboxHandle,
        work_waiting: Arc<Notify>,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()?;
        Ok(Self {
            ingest_url: format!("{}/ingest", config.url.trim_end_matches('/')),
            backoff: Backoff::new(config.backoff_base, config.backoff_max),
            config,
            client,
            outbox,
            work_waiting,
            stats: UplinkStats::default(),
        })
    }

    /// Uploads until `shutdown` is cancelled, then makes one last attempt to empty the
    /// outbox within `final_flush_timeout`. Only outbox (disk) errors end it early.
    pub async fn run(mut self, shutdown: CancellationToken) -> Result<UplinkStats, OutboxError> {
        while !shutdown.is_cancelled() {
            self.drain(&shutdown).await?;
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = tokio::time::sleep(self.config.flush_interval) => {}
                _ = self.work_waiting.notified() => {}
            }
        }

        let timeout = self.config.final_flush_timeout;
        let never = CancellationToken::new();
        if tokio::time::timeout(timeout, self.drain(&never))
            .await
            .is_err()
        {
            let remaining = self.outbox.depth().await;
            warn!(
                remaining,
                "final upload timed out, records stay in the outbox for the next start"
            );
        }
        Ok(self.stats)
    }

    /// Uploads batches until the outbox holds less than a full batch. Failed uploads are
    /// retried with backoff; records leave the outbox only after a 2xx.
    ///
    /// `stop` ends the loop between batches and cuts a backoff wait short, but never aborts
    /// a request in flight. Aborting one the receiver already stored would only mean
    /// sending the whole batch again.
    async fn drain(&mut self, stop: &CancellationToken) -> Result<(), OutboxError> {
        while !stop.is_cancelled() {
            let batch = self.outbox.next_batch(self.config.batch_size).await?;
            let Some(last) = batch.records.last() else {
                return Ok(());
            };
            let (first_seq, last_seq) = (batch.records[0].seq, last.seq);

            match self.send(&batch).await {
                Ok(response) => {
                    self.outbox.ack(last_seq, batch.dropped_records).await?;
                    self.backoff.reset();
                    self.stats.batches += 1;
                    self.stats.records += batch.records.len() as u64;
                    match response {
                        Some(r) => info!(
                            first_seq,
                            last_seq,
                            inserted = r.inserted,
                            duplicates = r.duplicates,
                            "uploaded batch"
                        ),
                        None => info!(first_seq, last_seq, "uploaded batch"),
                    }
                    if batch.records.len() < self.config.batch_size {
                        return Ok(());
                    }
                }
                Err(e) => {
                    self.stats.failures += 1;
                    let delay = self.backoff.next_delay(&mut rand::rng());
                    match &e {
                        SendError::Status(status) if status.is_client_error() => {
                            error!(error = %e, retry_in = ?delay, "upload rejected, check token and config")
                        }
                        _ => warn!(error = %e, retry_in = ?delay, "upload failed"),
                    }
                    tokio::select! {
                        _ = stop.cancelled() => {}
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// `Ok(None)` is a 2xx whose body we could not read. The data is stored, so it still counts.
    async fn send(&self, batch: &Batch) -> Result<Option<IngestResponse>, SendError> {
        let first = batch.records.first().map_or(0, |r| r.seq);
        let last = batch.records.last().map_or(0, |r| r.seq);
        let body = BatchBody {
            device_id: &self.config.device_id,
            // Same seq range means same batch, so a retry after a lost response keeps its id
            // and the receiver does not count `dropped_records` twice.
            batch_id: format!("{first}-{last}"),
            dropped_records: batch.dropped_records,
            records: batch
                .records
                .iter()
                .map(|r| RecordBody {
                    seq: r.seq,
                    ts: humantime::format_rfc3339_millis(r.ts).to_string(),
                    signal: &r.signal,
                    value: r.value,
                })
                .collect(),
        };

        let response = self
            .client
            .post(&self.ingest_url)
            .bearer_auth(&self.config.token)
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(SendError::Status(status));
        }
        let parsed = response.json().await;
        if let Err(e) = &parsed {
            debug!(error = %e, "could not read ingest response body");
        }
        Ok(parsed.ok())
    }
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    #[test]
    fn backoff_doubles_up_to_max_with_jitter() {
        let mut rng = StdRng::seed_from_u64(1);
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(1));
        let ceilings = [100, 200, 400, 800, 1000, 1000];
        for ceiling in ceilings {
            let delay = backoff.next_delay(&mut rng).as_millis() as u64;
            assert!(
                (ceiling / 2..=ceiling).contains(&delay),
                "{delay} ms not in [{}, {ceiling}]",
                ceiling / 2
            );
        }
    }

    #[test]
    fn backoff_reset_starts_over() {
        let mut rng = StdRng::seed_from_u64(1);
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(10));
        for _ in 0..5 {
            backoff.next_delay(&mut rng);
        }
        backoff.reset();
        assert!(backoff.next_delay(&mut rng) <= Duration::from_millis(100));
    }

    #[test]
    fn backoff_does_not_overflow() {
        let mut rng = StdRng::seed_from_u64(1);
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
        for _ in 0..1000 {
            assert!(backoff.next_delay(&mut rng) <= Duration::from_secs(60));
        }
    }

    #[test]
    fn jitter_actually_varies() {
        let mut rng = StdRng::seed_from_u64(1);
        let delays: Vec<Duration> = (0..20)
            .map(|_| {
                Backoff::new(Duration::from_secs(1), Duration::from_secs(1)).next_delay(&mut rng)
            })
            .collect();
        assert!(delays.iter().any(|d| *d != delays[0]));
    }
}
