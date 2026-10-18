//! Replays a log recorded with `candump -L vcan0` while running
//! `tractor-sim --duration-secs 40 --garbage-rate 0.05 --seed 42` in the Lima VM.

use edge_agent::decode::{DecodeStats, Signal};
use edge_agent::pipeline;
use edge_agent::source::ReplaySource;
use tokio::fs::File;
use tokio::io::BufReader;

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/sim-40s.log");

#[tokio::test]
async fn recorded_simulator_log_decodes_to_realistic_values() {
    let file = File::open(FIXTURE).await.unwrap();
    let summary = pipeline::run(ReplaySource::new(BufReader::new(file), false))
        .await
        .unwrap();

    assert_eq!(summary.frames_read, 1711);
    assert_eq!(
        summary.stats,
        DecodeStats {
            frames: 1711,
            decoded: 1694,
            unknown_pgn: 6,
            invalid_length: 11,
        }
    );

    let latest = |signal| summary.latest[&signal];
    assert!((1800.0..2200.0).contains(&latest(Signal::EngineSpeedRpm)));
    assert!((8.0..11.0).contains(&latest(Signal::WheelSpeedKmh)));
    assert!((40.0..60.0).contains(&latest(Signal::CoolantTempC)));
    assert!((15.0..28.0).contains(&latest(Signal::FuelRateLph)));
    assert!((latest(Signal::LatitudeDeg) - 49.4719).abs() < 0.01);
    assert!((latest(Signal::LongitudeDeg) - 17.1118).abs() < 0.01);
}
