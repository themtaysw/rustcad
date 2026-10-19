use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::aggregate::Sample;
use crate::frame::timestamp_in_range;

/// Stored in SQLite's `user_version`. Bump it and add a migration when the schema changes.
const SCHEMA_VERSION: i64 = 1;

// AUTOINCREMENT makes SQLite remember the highest seq ever used (in `sqlite_sequence`).
// Without it, deleting the newest rows lets SQLite hand out the same seq again, and the
// receiver would silently discard the new record as a duplicate.
const SCHEMA: &str = "
CREATE TABLE records (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms  INTEGER NOT NULL,
    signal TEXT    NOT NULL,
    value  REAL    NOT NULL
);
CREATE TABLE state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    outbox_id       TEXT    NOT NULL,
    dropped_records INTEGER NOT NULL CHECK (dropped_records >= 0)
);
";

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error("outbox database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error(
        "outbox schema version {0} is not supported, this agent knows version {SCHEMA_VERSION}"
    )]
    UnsupportedVersion(i64),
    #[error("the file already contains tables but is not an outbox")]
    NotAnOutbox,
}

pub type Result<T, E = OutboxError> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq)]
pub struct StoredRecord {
    pub seq: u64,
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
#[derive(Debug)]
pub struct Outbox {
    conn: Connection,
    id: String,
    cap: u64,
    depth: u64,
}

impl Outbox {
    /// Opens or creates the outbox. `cap` is the maximum number of unsent records kept
    /// (at least 1).
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be opened, is not an outbox, or was written by a newer agent.
    pub fn open(path: impl AsRef<Path>, cap: u64) -> Result<Self> {
        let mut conn = Connection::open(path)?;
        let mode: String =
            conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            warn!(
                mode,
                "filesystem does not support WAL, outbox runs in another journal mode"
            );
        }
        // FULL fsyncs every commit: a tractor is switched off without warning, and a commit
        // that was acknowledged must survive that.
        conn.pragma_update(None, "synchronous", "FULL")?;
        init_schema(&mut conn)?;

        let id = conn.query_row("SELECT outbox_id FROM state", [], |row| row.get(0))?;
        let depth = conn.query_row("SELECT count(*) FROM records", [], |row| row.get(0))?;
        Ok(Self {
            conn,
            id,
            cap: cap.max(1),
            depth,
        })
    }

    /// Random id of this outbox file, created with it. `seq` starts at 1 in every new file,
    /// so the receiver dedupes on `(device_id, outbox_id, seq)`.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Number of unsent records.
    #[must_use]
    pub fn depth(&self) -> u64 {
        self.depth
    }

    /// Stores samples in one transaction, then drops the oldest records if over the cap.
    /// Returns the depth afterwards.
    ///
    /// A sample with a timestamp outside 1970..=9999 (a broken clock) cannot be sent, so it
    /// is not stored and counts as dropped instead of blocking the queue forever.
    ///
    /// # Errors
    ///
    /// Fails if SQLite cannot write; then nothing of this call is stored.
    pub fn append(&mut self, samples: &[Sample]) -> Result<u64> {
        let tx = self.conn.transaction()?;
        let mut stored = 0;
        {
            let mut insert = tx
                .prepare_cached("INSERT INTO records (ts_ms, signal, value) VALUES (?1, ?2, ?3)")?;
            for sample in samples.iter().filter(|s| timestamp_in_range(s.ts)) {
                insert.execute(params![unix_millis(sample.ts), sample.signal, sample.value])?;
                stored += 1;
            }
        }
        let invalid = samples.len() as u64 - stored;

        let mut depth = self.depth + stored;
        let overflow = depth.saturating_sub(self.cap);
        if overflow > 0 {
            tx.execute(
                "DELETE FROM records WHERE seq IN (SELECT seq FROM records ORDER BY seq LIMIT ?1)",
                [overflow],
            )?;
            depth = self.cap;
        }
        if overflow + invalid > 0 {
            tx.execute(
                "UPDATE state SET dropped_records = dropped_records + ?1",
                [overflow + invalid],
            )?;
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
        if invalid > 0 {
            warn!(
                dropped = invalid,
                "dropped samples with an impossible timestamp"
            );
        }
        Ok(depth)
    }

    /// The oldest unsent records, at most `limit`, and the current dropped counter.
    ///
    /// # Errors
    ///
    /// Fails if SQLite cannot read.
    pub fn next_batch(&self, limit: usize) -> Result<Batch> {
        let mut select = self.conn.prepare_cached(
            "SELECT seq, ts_ms, signal, value FROM records ORDER BY seq LIMIT ?1",
        )?;
        let records = select
            .query_map([limit], |row| {
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

    /// Records dropped because of the cap and not yet reported to the receiver.
    ///
    /// # Errors
    ///
    /// Fails if SQLite cannot read.
    pub fn dropped_records(&self) -> Result<u64> {
        let dropped = self
            .conn
            .query_row("SELECT dropped_records FROM state", [], |row| row.get(0))?;
        Ok(dropped)
    }

    /// Called after the receiver accepted a batch: deletes every record up to `through_seq`
    /// and subtracts the dropped count that batch reported. Drops that happened while the
    /// batch was in flight stay counted for the next one.
    ///
    /// # Errors
    ///
    /// Fails if SQLite cannot write; then the records stay and are sent again.
    pub fn ack(&mut self, through_seq: u64, dropped_reported: u64) -> Result<u64> {
        let tx = self.conn.transaction()?;
        let deleted = tx.execute("DELETE FROM records WHERE seq <= ?1", [through_seq])?;
        tx.execute(
            "UPDATE state SET dropped_records = max(dropped_records - ?1, 0)",
            [dropped_reported],
        )?;
        tx.commit()?;
        self.depth = self.depth.saturating_sub(deleted as u64);
        Ok(self.depth)
    }
}

/// Creates the schema in a new file and refuses files it does not understand.
fn init_schema(conn: &mut Connection) -> Result<()> {
    let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    match version {
        SCHEMA_VERSION => Ok(()),
        0 => {
            let tx = conn.transaction()?;
            let tables: u64 =
                tx.query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))?;
            if tables > 0 {
                return Err(OutboxError::NotAnOutbox);
            }
            tx.execute_batch(SCHEMA)?;
            tx.execute(
                "INSERT INTO state (id, outbox_id, dropped_records) VALUES (1, ?1, 0)",
                [Uuid::new_v4().to_string()],
            )?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            tx.commit()?;
            Ok(())
        }
        other => Err(OutboxError::UnsupportedVersion(other)),
    }
}

fn unix_millis(ts: SystemTime) -> i64 {
    ts.duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn from_unix_millis(millis: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(u64::try_from(millis).unwrap_or(0))
}

/// Async handle shared by the writer and the uplink task.
///
/// SQLite calls block, so each one runs on tokio's blocking thread pool (`spawn_blocking`)
/// instead of stalling the async worker threads. `Arc` lets both tasks own the same outbox,
/// and the `Mutex` lets only one of them use the connection at a time, because
/// `rusqlite::Connection` can be moved between threads (`Send`) but not shared (`!Sync`).
#[derive(Debug, Clone)]
pub struct OutboxHandle {
    inner: Arc<Mutex<Outbox>>,
    id: Arc<str>,
    /// Copy of the depth for readers that must not wait for the lock (the health log).
    depth: Arc<AtomicU64>,
}

impl OutboxHandle {
    #[must_use]
    pub fn new(outbox: Outbox) -> Self {
        Self {
            id: outbox.id().into(),
            depth: Arc::new(AtomicU64::new(outbox.depth())),
            inner: Arc::new(Mutex::new(outbox)),
        }
    }

    /// The id never changes, so it is copied out and needs no lock.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Number of unsent records as of the last completed call.
    #[must_use]
    pub fn depth(&self) -> u64 {
        self.depth.load(Ordering::Relaxed)
    }

    async fn with<T, F>(&self, f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(&mut Outbox) -> T + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        let depth = Arc::clone(&self.depth);
        tokio::task::spawn_blocking(move || {
            let mut outbox = inner.lock().expect("outbox mutex poisoned");
            let result = f(&mut outbox);
            depth.store(outbox.depth(), Ordering::Relaxed);
            result
        })
        .await
        .expect("outbox task panicked")
    }

    /// See [`Outbox::append`].
    ///
    /// # Errors
    ///
    /// See [`Outbox::append`].
    pub async fn append(&self, samples: Vec<Sample>) -> Result<u64> {
        self.with(move |outbox| outbox.append(&samples)).await
    }

    /// See [`Outbox::next_batch`].
    ///
    /// # Errors
    ///
    /// See [`Outbox::next_batch`].
    pub async fn next_batch(&self, limit: usize) -> Result<Batch> {
        self.with(move |outbox| outbox.next_batch(limit)).await
    }

    /// See [`Outbox::ack`].
    ///
    /// # Errors
    ///
    /// See [`Outbox::ack`].
    pub async fn ack(&self, through_seq: u64, dropped_reported: u64) -> Result<u64> {
        self.with(move |outbox| outbox.ack(through_seq, dropped_reported))
            .await
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
                value: f64::from(u32::try_from(i).unwrap()),
            })
            .collect()
    }

    fn seqs(batch: &Batch) -> Vec<u64> {
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
    fn id_survives_restart_and_differs_between_files() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("o.db");
        let id = Outbox::open(&path, 100).unwrap().id().to_owned();
        assert_eq!(Uuid::parse_str(&id).unwrap().get_version_num(), 4);
        assert_eq!(Outbox::open(&path, 100).unwrap().id(), id);

        let other = Outbox::open(dir.path().join("other.db"), 100).unwrap();
        assert_ne!(other.id(), id);
    }

    #[test]
    fn refuses_files_it_does_not_understand() {
        let dir = TempDir::new().unwrap();

        let foreign = dir.path().join("foreign.db");
        Connection::open(&foreign)
            .unwrap()
            .execute_batch("CREATE TABLE notes (text TEXT);")
            .unwrap();
        assert!(matches!(
            Outbox::open(&foreign, 100),
            Err(OutboxError::NotAnOutbox)
        ));

        let newer = dir.path().join("newer.db");
        Outbox::open(&newer, 100).unwrap();
        Connection::open(&newer)
            .unwrap()
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        assert!(matches!(
            Outbox::open(&newer, 100),
            Err(OutboxError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn impossible_timestamps_are_dropped_and_counted() {
        let dir = TempDir::new().unwrap();
        let mut outbox = Outbox::open(dir.path().join("o.db"), 100).unwrap();
        let mut batch = samples(2);
        batch[1].ts = UNIX_EPOCH + Duration::from_secs(300_000_000_000);
        assert_eq!(outbox.append(&batch).unwrap(), 1);
        assert_eq!(outbox.dropped_records().unwrap(), 1);
    }

    #[test]
    fn dropped_counter_cannot_go_negative() {
        let dir = TempDir::new().unwrap();
        let mut outbox = Outbox::open(dir.path().join("o.db"), 100).unwrap();
        outbox.append(&samples(1)).unwrap();
        outbox.ack(1, 5).unwrap();
        assert_eq!(outbox.dropped_records().unwrap(), 0);
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
        let outbox = Outbox::open(dir.path().join("o.db"), 100).unwrap();
        let id = outbox.id().to_owned();
        let handle = OutboxHandle::new(outbox);
        let writer = handle.clone();
        assert_eq!(writer.id(), id);
        assert_eq!(writer.append(samples(4)).await.unwrap(), 4);
        let batch = handle.next_batch(10).await.unwrap();
        handle.ack(batch.records[1].seq, 0).await.unwrap();
        assert_eq!(handle.depth(), 2);
    }
}
