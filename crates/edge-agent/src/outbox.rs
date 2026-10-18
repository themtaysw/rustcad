use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use thiserror::Error;
use tracing::warn;

use crate::aggregate::Sample;

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error("outbox database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

pub type Result<T, E = OutboxError> = std::result::Result<T, E>;

// AUTOINCREMENT makes SQLite remember the highest seq ever used (in `sqlite_sequence`).
// Without it, deleting the newest rows lets SQLite hand out the same seq again, and the
// receiver would silently discard the new record as a duplicate.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS records (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms  INTEGER NOT NULL,
    signal TEXT    NOT NULL,
    value  REAL    NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
INSERT OR IGNORE INTO meta (key, value) VALUES ('dropped_records', 0);
";

#[derive(Debug, Clone, PartialEq)]
pub struct StoredRecord {
    pub seq: i64,
    pub ts: SystemTime,
    pub signal: String,
    pub value: f64,
}

/// Records waiting for upload plus how many were dropped since the last successful upload.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Batch {
    pub records: Vec<StoredRecord>,
    pub dropped_records: u64,
}

/// Durable store-and-forward queue in SQLite.
///
/// Uploaded records are deleted, so every row in `records` is unsent.
pub struct Outbox {
    conn: Connection,
    cap: u64,
    depth: u64,
}

impl Outbox {
    /// Opens or creates the outbox. `cap` is the maximum number of unsent records kept.
    pub fn open(path: impl AsRef<Path>, cap: u64) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))?;
        // FULL fsyncs every commit: a tractor is switched off without warning, and a commit
        // that was acknowledged must survive that.
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(SCHEMA)?;
        let depth: i64 = conn.query_row("SELECT count(*) FROM records", [], |row| row.get(0))?;
        Ok(Self {
            conn,
            cap: cap.max(1),
            depth: depth as u64,
        })
    }

    /// Number of unsent records.
    pub fn depth(&self) -> u64 {
        self.depth
    }

    /// Stores samples in one transaction, then drops the oldest records if over the cap.
    /// Returns the depth afterwards.
    pub fn append(&mut self, samples: &[Sample]) -> Result<u64> {
        let tx = self.conn.transaction()?;
        {
            let mut insert = tx
                .prepare_cached("INSERT INTO records (ts_ms, signal, value) VALUES (?1, ?2, ?3)")?;
            for sample in samples {
                insert.execute(params![unix_millis(sample.ts), sample.signal, sample.value])?;
            }
        }

        let mut depth = self.depth + samples.len() as u64;
        let overflow = depth.saturating_sub(self.cap);
        if overflow > 0 {
            tx.execute(
                "DELETE FROM records WHERE seq IN (SELECT seq FROM records ORDER BY seq LIMIT ?1)",
                [overflow as i64],
            )?;
            tx.execute(
                "UPDATE meta SET value = value + ?1 WHERE key = 'dropped_records'",
                [overflow as i64],
            )?;
            depth = self.cap;
        }
        tx.commit()?;

        self.depth = depth;
        if overflow > 0 {
            warn!(
                dropped = overflow,
                cap = self.cap,
                "outbox full, dropped oldest records"
            );
        }
        Ok(depth)
    }

    /// The oldest unsent records, at most `limit`, and the current dropped counter.
    pub fn next_batch(&self, limit: usize) -> Result<Batch> {
        let mut select = self.conn.prepare_cached(
            "SELECT seq, ts_ms, signal, value FROM records ORDER BY seq LIMIT ?1",
        )?;
        let records = select
            .query_map([limit as i64], |row| {
                Ok(StoredRecord {
                    seq: row.get(0)?,
                    ts: from_unix_millis(row.get(1)?),
                    signal: row.get(2)?,
                    value: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Batch {
            records,
            dropped_records: self.dropped_records()?,
        })
    }

    pub fn dropped_records(&self) -> Result<u64> {
        let dropped: i64 = self.conn.query_row(
            "SELECT value FROM meta WHERE key = 'dropped_records'",
            [],
            |row| row.get(0),
        )?;
        Ok(dropped as u64)
    }

    /// Called after the receiver accepted a batch: deletes every record up to `through_seq`
    /// and subtracts the dropped count that batch reported. Drops that happened while the
    /// batch was in flight stay counted for the next one.
    pub fn ack(&mut self, through_seq: i64, dropped_reported: u64) -> Result<()> {
        let tx = self.conn.transaction()?;
        let deleted = tx.execute("DELETE FROM records WHERE seq <= ?1", [through_seq])?;
        tx.execute(
            "UPDATE meta SET value = max(value - ?1, 0) WHERE key = 'dropped_records'",
            [dropped_reported as i64],
        )?;
        tx.commit()?;
        self.depth = self.depth.saturating_sub(deleted as u64);
        Ok(())
    }
}

fn unix_millis(ts: SystemTime) -> i64 {
    ts.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn from_unix_millis(millis: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis.max(0) as u64)
}

/// Async handle shared by the writer and the uplink task.
///
/// SQLite calls block, so each one runs on tokio's blocking thread pool (`spawn_blocking`)
/// instead of stalling the async worker threads. `Arc` lets both tasks own the same outbox,
/// and the `Mutex` lets only one of them use the connection at a time, because
/// `rusqlite::Connection` can be moved between threads (`Send`) but not shared (`!Sync`).
#[derive(Clone)]
pub struct OutboxHandle {
    inner: Arc<Mutex<Outbox>>,
}

impl OutboxHandle {
    pub fn new(outbox: Outbox) -> Self {
        Self {
            inner: Arc::new(Mutex::new(outbox)),
        }
    }

    async fn with<T, F>(&self, f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(&mut Outbox) -> T + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || f(&mut inner.lock().expect("outbox mutex poisoned")))
            .await
            .expect("outbox task panicked")
    }

    pub async fn append(&self, samples: Vec<Sample>) -> Result<u64> {
        self.with(move |outbox| outbox.append(&samples)).await
    }

    pub async fn next_batch(&self, limit: usize) -> Result<Batch> {
        self.with(move |outbox| outbox.next_batch(limit)).await
    }

    pub async fn ack(&self, through_seq: i64, dropped_reported: u64) -> Result<()> {
        self.with(move |outbox| outbox.ack(through_seq, dropped_reported))
            .await
    }

    pub async fn depth(&self) -> u64 {
        self.with(|outbox| outbox.depth()).await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn samples(n: u64) -> Vec<Sample> {
        (0..n)
            .map(|i| Sample {
                ts: UNIX_EPOCH + Duration::from_secs(1_700_000_000 + i),
                signal: "coolant_temp_c",
                value: i as f64,
            })
            .collect()
    }

    fn seqs(batch: &Batch) -> Vec<i64> {
        batch.records.iter().map(|r| r.seq).collect()
    }

    #[test]
    fn stores_and_returns_records_in_order() {
        let dir = TempDir::new().unwrap();
        let mut outbox = Outbox::open(dir.path().join("o.db"), 100).unwrap();
        assert_eq!(outbox.append(&samples(5)).unwrap(), 5);

        let batch = outbox.next_batch(3).unwrap();
        assert_eq!(seqs(&batch), [1, 2, 3]);
        assert_eq!(
            batch.records[1],
            StoredRecord {
                seq: 2,
                ts: UNIX_EPOCH + Duration::from_secs(1_700_000_001),
                signal: "coolant_temp_c".into(),
                value: 1.0,
            }
        );
        assert_eq!(batch.dropped_records, 0);
    }

    #[test]
    fn ack_deletes_through_seq() {
        let dir = TempDir::new().unwrap();
        let mut outbox = Outbox::open(dir.path().join("o.db"), 100).unwrap();
        outbox.append(&samples(5)).unwrap();
        outbox.ack(3, 0).unwrap();
        assert_eq!(outbox.depth(), 2);
        assert_eq!(seqs(&outbox.next_batch(10).unwrap()), [4, 5]);
    }

    #[test]
    fn seq_is_never_reused_after_restart() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("o.db");

        let mut outbox = Outbox::open(&path, 100).unwrap();
        outbox.append(&samples(3)).unwrap();
        outbox.ack(3, 0).unwrap();
        assert_eq!(outbox.depth(), 0);
        drop(outbox);

        let mut outbox = Outbox::open(&path, 100).unwrap();
        outbox.append(&samples(2)).unwrap();
        assert_eq!(seqs(&outbox.next_batch(10).unwrap()), [4, 5]);
    }

    #[test]
    fn depth_survives_restart() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("o.db");
        Outbox::open(&path, 100)
            .unwrap()
            .append(&samples(7))
            .unwrap();
        assert_eq!(Outbox::open(&path, 100).unwrap().depth(), 7);
    }

    #[test]
    fn cap_drops_oldest_and_counts_them() {
        let dir = TempDir::new().unwrap();
        let mut outbox = Outbox::open(dir.path().join("o.db"), 10).unwrap();
        outbox.append(&samples(8)).unwrap();
        assert_eq!(outbox.append(&samples(8)).unwrap(), 10);

        let batch = outbox.next_batch(100).unwrap();
        assert_eq!(seqs(&batch), (7..=16).collect::<Vec<_>>());
        assert_eq!(batch.dropped_records, 6);
    }

    #[test]
    fn ack_keeps_drops_that_happened_in_flight() {
        let dir = TempDir::new().unwrap();
        let mut outbox = Outbox::open(dir.path().join("o.db"), 10).unwrap();
        outbox.append(&samples(12)).unwrap();
        let batch = outbox.next_batch(5).unwrap();
        assert_eq!(batch.dropped_records, 2);

        outbox.append(&samples(3)).unwrap();
        assert_eq!(outbox.dropped_records().unwrap(), 5);

        outbox
            .ack(batch.records.last().unwrap().seq, batch.dropped_records)
            .unwrap();
        assert_eq!(outbox.dropped_records().unwrap(), 3);
    }

    #[tokio::test]
    async fn handle_runs_calls_on_the_blocking_pool() {
        let dir = TempDir::new().unwrap();
        let handle = OutboxHandle::new(Outbox::open(dir.path().join("o.db"), 100).unwrap());
        let writer = handle.clone();
        assert_eq!(writer.append(samples(4)).await.unwrap(), 4);
        let batch = handle.next_batch(10).await.unwrap();
        handle.ack(batch.records[1].seq, 0).await.unwrap();
        assert_eq!(handle.depth().await, 2);
    }
}
