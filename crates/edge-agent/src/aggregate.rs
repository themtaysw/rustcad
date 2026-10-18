use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::decode::{Reading, Signal};

pub const ENGINE_SPEED_MIN: &str = "engine_speed_rpm_min";
pub const ENGINE_SPEED_MAX: &str = "engine_speed_rpm_max";

/// One value per signal per second. This is what gets stored and uploaded.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Start of the second the sample covers.
    pub ts: SystemTime,
    pub signal: &'static str,
    pub value: f64,
}

#[derive(Debug, Clone, Copy)]
struct Accumulator {
    last: f64,
    min: f64,
    max: f64,
}

/// Collapses readings into one sample per signal and second (by reading timestamp).
///
/// Every signal keeps its last value. Engine speed also gets min and max, because short
/// rpm spikes are what matters there and the last value alone would hide them.
#[derive(Debug, Default)]
pub struct Aggregator {
    second: Option<u64>,
    signals: BTreeMap<Signal, Accumulator>,
}

impl Aggregator {
    /// Adds a reading. When it belongs to a different second than the current bucket,
    /// the current bucket is closed and returned first.
    ///
    /// Any change of second closes the bucket, also backwards, so a clock jump never
    /// makes the agent hold on to or drop data.
    pub fn push(&mut self, reading: &Reading) -> Vec<Sample> {
        let second = unix_seconds(reading.timestamp);
        let closed = if self.second.is_some_and(|current| current != second) {
            self.flush()
        } else {
            Vec::new()
        };
        self.second = Some(second);

        self.signals
            .entry(reading.signal)
            .and_modify(|acc| {
                acc.last = reading.value;
                acc.min = acc.min.min(reading.value);
                acc.max = acc.max.max(reading.value);
            })
            .or_insert(Accumulator {
                last: reading.value,
                min: reading.value,
                max: reading.value,
            });
        closed
    }

    /// Closes the current bucket, e.g. when input ends or goes quiet.
    pub fn flush(&mut self) -> Vec<Sample> {
        let Some(second) = self.second.take() else {
            return Vec::new();
        };
        let ts = UNIX_EPOCH + Duration::from_secs(second);

        let mut samples = Vec::new();
        for (signal, acc) in std::mem::take(&mut self.signals) {
            samples.push(Sample {
                ts,
                signal: signal.name(),
                value: acc.last,
            });
            if signal == Signal::EngineSpeedRpm {
                samples.push(Sample {
                    ts,
                    signal: ENGINE_SPEED_MIN,
                    value: acc.min,
                });
                samples.push(Sample {
                    ts,
                    signal: ENGINE_SPEED_MAX,
                    value: acc.max,
                });
            }
        }
        samples
    }
}

fn unix_seconds(ts: SystemTime) -> u64 {
    // Timestamps before 1970 only come from a broken clock; they all land in second 0.
    ts.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(millis: u64, signal: Signal, value: f64) -> Reading {
        Reading {
            timestamp: UNIX_EPOCH + Duration::from_millis(millis),
            source_address: 0,
            signal,
            value,
        }
    }

    fn sample(secs: u64, signal: &'static str, value: f64) -> Sample {
        Sample {
            ts: UNIX_EPOCH + Duration::from_secs(secs),
            signal,
            value,
        }
    }

    #[test]
    fn keeps_last_value_and_rpm_min_max_per_second() {
        let mut agg = Aggregator::default();
        assert!(
            agg.push(&reading(10_000, Signal::EngineSpeedRpm, 1500.0))
                .is_empty()
        );
        assert!(
            agg.push(&reading(10_100, Signal::CoolantTempC, 80.0))
                .is_empty()
        );
        assert!(
            agg.push(&reading(10_200, Signal::EngineSpeedRpm, 1800.0))
                .is_empty()
        );
        assert!(
            agg.push(&reading(10_300, Signal::EngineSpeedRpm, 1200.0))
                .is_empty()
        );
        assert!(
            agg.push(&reading(10_999, Signal::CoolantTempC, 81.0))
                .is_empty()
        );

        let closed = agg.push(&reading(11_000, Signal::EngineSpeedRpm, 1300.0));
        assert_eq!(
            closed,
            vec![
                sample(10, "engine_speed_rpm", 1200.0),
                sample(10, ENGINE_SPEED_MIN, 1200.0),
                sample(10, ENGINE_SPEED_MAX, 1800.0),
                sample(10, "coolant_temp_c", 81.0),
            ]
        );

        assert_eq!(
            agg.flush(),
            vec![
                sample(11, "engine_speed_rpm", 1300.0),
                sample(11, ENGINE_SPEED_MIN, 1300.0),
                sample(11, ENGINE_SPEED_MAX, 1300.0),
            ]
        );
        assert!(agg.flush().is_empty());
    }

    #[test]
    fn signals_without_readings_in_a_second_are_not_repeated() {
        let mut agg = Aggregator::default();
        agg.push(&reading(1_000, Signal::LatitudeDeg, 49.0));
        let closed = agg.push(&reading(2_000, Signal::WheelSpeedKmh, 9.0));
        assert_eq!(closed, vec![sample(1, "latitude_deg", 49.0)]);
        assert_eq!(agg.flush(), vec![sample(2, "wheel_speed_kmh", 9.0)]);
    }

    #[test]
    fn clock_jumping_backwards_closes_the_bucket() {
        let mut agg = Aggregator::default();
        agg.push(&reading(100_000, Signal::CoolantTempC, 80.0));
        let closed = agg.push(&reading(50_000, Signal::CoolantTempC, 81.0));
        assert_eq!(closed, vec![sample(100, "coolant_temp_c", 80.0)]);
        assert_eq!(agg.flush(), vec![sample(50, "coolant_temp_c", 81.0)]);
    }

    #[test]
    fn empty_flush_is_empty() {
        assert!(Aggregator::default().flush().is_empty());
    }
}
