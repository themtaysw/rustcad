//! End-to-end tests: replay a recorded CAN log through the whole agent into a fake receiver.
//!
//! The fixture was recorded with `candump -L vcan0` in the Lima VM while running
//! `tractor-sim --duration-secs 40 --garbage-rate 0.05 --seed 42`.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edge_agent::decode::DecodeStats;
use edge_agent::outbox::Outbox;
use edge_agent::pipeline::{self, PipelineConfig, Summary};
use edge_agent::source::ReplaySource;
use edge_agent::uplink::UplinkConfig;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const FIXTURE: &str = include_str!("fixtures/sim-40s.log");
const TOKEN: &str = "test-token";
const DEVICE: &str = "test-tractor";
const BATCH_SIZE: usize = 100;

#[derive(Debug, Clone, Copy)]
enum Behavior {
    Ok,
    /// 503 without storing anything.
    Unavailable,
    /// Stores the batch but answers 500, like a response lost on the way back.
    LostResponse,
}

type Plan = Box<dyn Fn(usize) -> Behavior + Send + Sync>;

/// Stands in for the real receiver. Answers call number `n` according to `plan(n)` and
/// remembers every batch it stored.
#[derive(Clone)]
struct FakeReceiver {
    calls: Arc<AtomicUsize>,
    plan: Arc<Plan>,
    stored: Arc<Mutex<Vec<Value>>>,
}

impl Respond for FakeReceiver {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let behavior = (self.plan)(call);
        if let Behavior::Unavailable = behavior {
            return ResponseTemplate::new(503);
        }

        let body: Value = serde_json::from_slice(&request.body).expect("body is JSON");
        let count = body["records"].as_array().map_or(0, Vec::len);
        self.stored.lock().unwrap().push(body);
        match behavior {
            Behavior::LostResponse => ResponseTemplate::new(500),
            _ => ResponseTemplate::new(200).set_body_json(json!({
                "accepted": count, "inserted": count, "duplicates": 0
            })),
        }
    }
}

impl FakeReceiver {
    async fn start(plan: impl Fn(usize) -> Behavior + Send + Sync + 'static) -> (MockServer, Self) {
        let receiver = Self {
            calls: Arc::new(AtomicUsize::new(0)),
            plan: Arc::new(Box::new(plan)),
            stored: Arc::new(Mutex::new(Vec::new())),
        };
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/ingest"))
            .and(header("authorization", format!("Bearer {TOKEN}")))
            .respond_with(receiver.clone())
            .mount(&server)
            .await;
        (server, receiver)
    }

    fn batches(&self) -> Vec<Value> {
        self.stored.lock().unwrap().clone()
    }

    /// Every stored seq in arrival order, duplicates included.
    fn seqs(&self) -> Vec<i64> {
        self.batches()
            .iter()
            .flat_map(|b| b["records"].as_array().unwrap().clone())
            .map(|r| r["seq"].as_i64().unwrap())
            .collect()
    }

    /// The receiver's dedupe key without device_id, which is the same in every test.
    fn unique_keys(&self) -> BTreeSet<(String, i64)> {
        self.batches()
            .iter()
            .flat_map(|b| {
                let outbox_id = b["outbox_id"].as_str().unwrap().to_owned();
                b["records"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(move |r| (outbox_id.clone(), r["seq"].as_i64().unwrap()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn unique_seqs(&self) -> Vec<i64> {
        self.seqs()
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

fn config(url: &str, final_flush: Duration) -> PipelineConfig {
    PipelineConfig {
        uplink: UplinkConfig {
            url: url.to_owned(),
            token: TOKEN.to_owned(),
            device_id: DEVICE.to_owned(),
            batch_size: BATCH_SIZE,
            flush_interval: Duration::from_millis(20),
            request_timeout: Duration::from_secs(2),
            final_flush_timeout: final_flush,
            backoff_base: Duration::from_millis(5),
            backoff_max: Duration::from_millis(40),
        },
        health_interval: Duration::from_secs(3600),
    }
}

async fn run_agent(
    log: &'static str,
    outbox_path: &Path,
    cap: u64,
    server: &MockServer,
    final_flush: Duration,
) -> Summary {
    let outbox = Outbox::open(outbox_path, cap).unwrap();
    let source = ReplaySource::new(log.as_bytes(), false);
    let run = pipeline::run(
        source,
        outbox,
        config(&server.uri(), final_flush),
        CancellationToken::new(),
    );
    tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("agent did not finish")
        .unwrap()
}

fn split_fixture(lines: usize) -> (&'static str, &'static str) {
    let at = FIXTURE.match_indices('\n').nth(lines - 1).unwrap().0 + 1;
    FIXTURE.split_at(at)
}

fn outbox_id(path: &Path) -> String {
    Outbox::open(path, 1).unwrap().id().to_owned()
}

fn range(first: i64, last: i64) -> Vec<i64> {
    (first..=last).collect()
}

#[tokio::test]
async fn uploads_every_sample_exactly_once() {
    let dir = TempDir::new().unwrap();
    let (server, receiver) = FakeReceiver::start(|_| Behavior::Ok).await;

    let outbox = dir.path().join("o.db");
    let summary = run_agent(FIXTURE, &outbox, 10_000, &server, Duration::from_secs(10)).await;

    assert_eq!(summary.frames_read, 1711);
    assert_eq!(
        summary.decode,
        DecodeStats {
            frames: 1711,
            decoded: 1694,
            unknown_pgn: 6,
            invalid_length: 11,
        }
    );
    let n = summary.samples_written as i64;
    assert!(n > 300, "only {n} samples from 40 s of data");
    assert_eq!(summary.uplink.records, n as u64);
    assert_eq!(summary.uplink.failures, 0);
    assert_eq!(summary.outbox_depth, 0);
    assert_eq!(receiver.seqs(), range(1, n));

    let batches = receiver.batches();
    for batch in &batches {
        let records = batch["records"].as_array().unwrap();
        assert!(!records.is_empty() && records.len() <= BATCH_SIZE);
        let (first, last) = (&records[0]["seq"], &records[records.len() - 1]["seq"]);
        assert_eq!(batch["batch_id"], format!("{first}-{last}"));
        assert_eq!(batch["device_id"], DEVICE);
        assert_eq!(batch["outbox_id"], outbox_id(&outbox));
        assert_eq!(batch["dropped_records"], 0);
    }

    let records: Vec<Value> = batches
        .iter()
        .flat_map(|b| b["records"].as_array().unwrap().clone())
        .collect();
    let per_second: BTreeSet<(String, String)> = records
        .iter()
        .map(|r| {
            (
                r["ts"].as_str().unwrap().to_owned(),
                r["signal"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        per_second.len(),
        records.len(),
        "more than one sample per signal and second"
    );
    assert!(
        records
            .iter()
            .all(|r| r["ts"].as_str().unwrap().ends_with(".000Z"))
    );
    let signals: BTreeSet<&str> = records
        .iter()
        .map(|r| r["signal"].as_str().unwrap())
        .collect();
    assert_eq!(
        signals,
        BTreeSet::from([
            "coolant_temp_c",
            "engine_speed_rpm",
            "engine_speed_rpm_max",
            "engine_speed_rpm_min",
            "fuel_rate_lph",
            "latitude_deg",
            "longitude_deg",
            "wheel_speed_kmh",
        ])
    );
}

#[tokio::test]
async fn survives_receiver_outage_without_losing_data() {
    let dir = TempDir::new().unwrap();
    let (server, receiver) = FakeReceiver::start(|call| match call {
        0..5 => Behavior::Unavailable,
        5..7 => Behavior::LostResponse,
        _ => Behavior::Ok,
    })
    .await;

    let summary = run_agent(
        FIXTURE,
        &dir.path().join("o.db"),
        10_000,
        &server,
        Duration::from_secs(10),
    )
    .await;

    let n = summary.samples_written as i64;
    assert_eq!(summary.uplink.failures, 7);
    assert_eq!(summary.outbox_depth, 0);
    // After dedupe on seq, every sample arrived. Lost responses caused resends.
    assert_eq!(receiver.unique_seqs(), range(1, n));
    assert!(receiver.seqs().len() > n as usize);
}

#[tokio::test]
async fn restart_continues_seq_without_gaps_or_reuse() {
    let dir = TempDir::new().unwrap();
    let outbox = dir.path().join("o.db");
    let (first_half, second_half) = split_fixture(850);

    // Run 1: network fine, everything is uploaded and deleted from the outbox.
    let (server, before) = FakeReceiver::start(|_| Behavior::Ok).await;
    let run1 = run_agent(
        first_half,
        &outbox,
        10_000,
        &server,
        Duration::from_secs(10),
    )
    .await;
    let n1 = run1.samples_written as i64;
    assert_eq!(run1.outbox_depth, 0);
    assert_eq!(before.seqs(), range(1, n1));

    // Run 2: network down, the agent stops with data still in the outbox.
    let (server, _down) = FakeReceiver::start(|_| Behavior::Unavailable).await;
    let run2 = run_agent(
        second_half,
        &outbox,
        10_000,
        &server,
        Duration::from_millis(200),
    )
    .await;
    let n2 = run2.samples_written as i64;
    assert_eq!(run2.uplink.records, 0);
    assert_eq!(run2.outbox_depth, n2 as u64);

    // Run 3: network back, no new input. The leftovers go out with the next seqs.
    let (server, after) = FakeReceiver::start(|_| Behavior::Ok).await;
    let run3 = run_agent("", &outbox, 10_000, &server, Duration::from_secs(10)).await;
    assert_eq!(run3.outbox_depth, 0);
    assert_eq!(after.seqs(), range(n1 + 1, n1 + n2));
}

#[tokio::test]
async fn fresh_outbox_after_deleted_file_is_not_deduped_away() {
    let dir = TempDir::new().unwrap();
    let outbox = dir.path().join("o.db");
    let (first_half, second_half) = split_fixture(850);
    let (server, receiver) = FakeReceiver::start(|_| Behavior::Ok).await;

    let run1 = run_agent(
        first_half,
        &outbox,
        10_000,
        &server,
        Duration::from_secs(10),
    )
    .await;
    let first_id = outbox_id(&outbox);
    for suffix in ["", "-wal", "-shm"] {
        let file = dir.path().join(format!("o.db{suffix}"));
        if file.exists() {
            std::fs::remove_file(file).unwrap();
        }
    }

    let run2 = run_agent(
        second_half,
        &outbox,
        10_000,
        &server,
        Duration::from_secs(10),
    )
    .await;
    let second_id = outbox_id(&outbox);
    assert_ne!(first_id, second_id);

    let (n1, n2) = (run1.samples_written as i64, run2.samples_written as i64);
    let seqs = receiver.seqs();
    assert_eq!(seqs[..n1 as usize], range(1, n1));
    assert_eq!(seqs[n1 as usize..], range(1, n2));
    // Deduped on seq alone, the second run would collide with the first one.
    assert!(receiver.unique_seqs().len() < (n1 + n2) as usize);
    assert_eq!(receiver.unique_keys().len(), (n1 + n2) as usize);
}

#[tokio::test]
async fn full_outbox_drops_oldest_and_reports_it() {
    let dir = TempDir::new().unwrap();
    let outbox = dir.path().join("o.db");
    let cap = 50;

    let (server, _down) = FakeReceiver::start(|_| Behavior::Unavailable).await;
    let offline = run_agent(FIXTURE, &outbox, cap, &server, Duration::from_millis(200)).await;
    let n = offline.samples_written as i64;
    assert_eq!(offline.outbox_depth, cap);

    let (server, receiver) = FakeReceiver::start(|_| Behavior::Ok).await;
    run_agent("", &outbox, cap, &server, Duration::from_secs(10)).await;

    assert_eq!(receiver.seqs(), range(n - cap as i64 + 1, n));
    let batches = receiver.batches();
    assert_eq!(batches[0]["dropped_records"], n - cap as i64);
    assert!(batches[1..].iter().all(|b| b["dropped_records"] == 0));

    let reopened = Outbox::open(&outbox, cap).unwrap();
    assert_eq!(reopened.depth(), 0);
    assert_eq!(reopened.dropped_records().unwrap(), 0);
}
