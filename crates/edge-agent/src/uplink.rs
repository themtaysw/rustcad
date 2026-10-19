use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rand::Rng;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::metrics::Metrics;
use crate::outbox::{Batch, OutboxError, OutboxHandle};
use crate::secret::Secret;

#[derive(Debug, Clone)]
pub struct UplinkConfig {
    /// Receiver base URL, batches go to `{url}/ingest`.
    pub url: String,
    pub token: Secret,
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
    #[must_use]
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
    outbox_id: &'a str,
    batch_id: String,
    dropped_records: u64,
    records: Vec<RecordBody<'a>>,
}

#[derive(Serialize)]
struct RecordBody<'a> {
    seq: u64,
    ts: String,
    signal: &'a str,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct IngestResponse {
    inserted: u64,
    duplicates: u64,
}

#[derive(Debug)]
pub struct Uplink {
    config: UplinkConfig,
    ingest_url: String,
    client: reqwest::Client,
    outbox: OutboxHandle,
    work_waiting: Arc<Notify>,
    metrics: Arc<Metrics>,
    backoff: Backoff,
    /// A batch that was sent but not acknowledged. It is retried exactly as it was, so a
    /// resend after a lost response has the same `batch_id` and `dropped_records`.
    pending: Option<Batch>,
}

impl Uplink {
    /// `work_waiting` is notified by the writer when a full batch is ready.
    ///
    /// # Errors
    ///
    /// Fails if the HTTP client cannot be built (no TLS backend).
    pub fn new(
        config: UplinkConfig,
        outbox: OutboxHandle,
        work_waiting: Arc<Notify>,
        metrics: Arc<Metrics>,
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
            metrics,
            pending: None,
        })
    }

    /// Uploads until `shutdown` is cancelled, then makes one last attempt to empty the
    /// outbox within `final_flush_timeout`. Only outbox (disk) errors end it early.
    ///
    /// # Errors
    ///
    /// Returns the first outbox error; network errors are retried and never returned.
    pub async fn run(mut self, shutdown: CancellationToken) -> Result<(), OutboxError> {
        while !shutdown.is_cancelled() {
            self.drain(&shutdown).await?;
            tokio::select! {
                () = shutdown.cancelled() => {}
                () = tokio::time::sleep(self.config.flush_interval) => {}
                () = self.work_waiting.notified() => {}
            }
        }

        // The final attempt is bounded by a timeout, which may cut a request short. Whatever
        // is not acknowledged stays in the outbox and goes out after the next start.
        let never = CancellationToken::new();
        let timeout = self.config.final_flush_timeout;
        if tokio::time::timeout(timeout, self.drain(&never))
            .await
            .is_err()
        {
            warn!(
                remaining = self.outbox.depth(),
                "final upload timed out, records stay in the outbox for the next start"
            );
        }
        Ok(())
    }

    /// Uploads batches until the outbox holds less than a full batch. Failed uploads are
    /// retried with backoff; records leave the outbox only after a 2xx.
    ///
    /// `stop` ends the loop between batches and cuts a backoff wait short, but does not
    /// abort a request in flight: aborting one the receiver already stored would only mean
    /// sending the whole batch again.
    async fn drain(&mut self, stop: &CancellationToken) -> Result<(), OutboxError> {
        while !stop.is_cancelled() {
            let (batch, retry) = match self.pending.take() {
                Some(batch) => (batch, true),
                None => (self.outbox.next_batch(self.config.batch_size).await?, false),
            };
            let (Some(first), Some(last)) = (batch.records.first(), batch.records.last()) else {
                return Ok(());
            };
            let (first_seq, last_seq) = (first.seq, last.seq);

            match self.send(&batch).await {
                Ok(response) => {
                    self.outbox.ack(last_seq, batch.dropped_records).await?;
                    self.metrics
                        .add_upload(batch.records.len() as u64, SystemTime::now());
                    self.backoff.reset();
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
                    // A short batch fresh from the outbox means it is drained. A retried one
                    // is old, more records may have arrived since it was read.
                    if !retry && batch.records.len() < self.config.batch_size {
                        return Ok(());
                    }
                }
                Err(e) => {
                    self.pending = Some(batch);
                    self.metrics.add_upload_failure();
                    let delay = self.backoff.next_delay(&mut rand::rng());
                    match &e {
                        SendError::Status(status) if status.is_client_error() => {
                            error!(error = %e, retry_in = ?delay, "upload rejected, check token and config");
                        }
                        _ => warn!(error = %e, retry_in = ?delay, "upload failed"),
                    }
                    tokio::select! {
                        () = stop.cancelled() => {}
                        () = tokio::time::sleep(delay) => {}
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
            outbox_id: self.outbox.id(),
            // A failed batch is retried unchanged (see `pending`), so a resend after a lost
            // response keeps its id and the receiver does not count `dropped_records` twice.
            batch_id: format!("{first}-{last}"),
            dropped_records: batch.dropped_records,
            records: batch
                .records
                .iter()
                .map(|r| RecordBody {
                    seq: r.seq,
                    // Cannot fail: the outbox only stores timestamps up to year 9999.
                    ts: humantime::format_rfc3339_millis(r.ts).to_string(),
                    signal: &r.signal,
                    value: r.value,
                })
                .collect(),
        };

        let response = self
            .client
            .post(&self.ingest_url)
            .bearer_auth(self.config.token.expose())
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
        let ceilings: [u128; 6] = [100, 200, 400, 800, 1000, 1000];
        for ceiling in ceilings {
            let delay = backoff.next_delay(&mut rng).as_millis();
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
