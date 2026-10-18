use std::fmt;
use std::time::SystemTime;

use j1939::{CanId, DecodeError, Message};

use crate::frame::Frame;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Signal {
    EngineSpeedRpm,
    CoolantTempC,
    FuelRateLph,
    WheelSpeedKmh,
    LatitudeDeg,
    LongitudeDeg,
}

impl Signal {
    pub fn name(self) -> &'static str {
        match self {
            Signal::EngineSpeedRpm => "engine_speed_rpm",
            Signal::CoolantTempC => "coolant_temp_c",
            Signal::FuelRateLph => "fuel_rate_lph",
            Signal::WheelSpeedKmh => "wheel_speed_kmh",
            Signal::LatitudeDeg => "latitude_deg",
            Signal::LongitudeDeg => "longitude_deg",
        }
    }
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One signal value from one frame. "Not available" values never become a `Reading`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reading {
    pub timestamp: SystemTime,
    pub source_address: u8,
    pub signal: Signal,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodeStats {
    pub frames: u64,
    pub decoded: u64,
    pub unknown_pgn: u64,
    pub invalid_length: u64,
}

impl DecodeStats {
    pub fn errors(&self) -> u64 {
        self.unknown_pgn + self.invalid_length
    }
}

/// Decodes a frame into zero or more readings and counts the outcome in `stats`.
pub fn decode_frame(frame: &Frame, stats: &mut DecodeStats) -> Result<Vec<Reading>, DecodeError> {
    stats.frames += 1;
    let id = CanId::from_raw(frame.id);

    let message = j1939::decode(id.pgn, &frame.data).inspect_err(|e| match e {
        DecodeError::UnknownPgn(_) => stats.unknown_pgn += 1,
        DecodeError::InvalidLength { .. } => stats.invalid_length += 1,
    })?;
    stats.decoded += 1;

    let reading = |signal, value: Option<f64>| {
        value.map(|value| Reading {
            timestamp: frame.timestamp,
            source_address: id.source_address,
            signal,
            value,
        })
    };
    let readings = match message {
        Message::Eec1(m) => vec![reading(Signal::EngineSpeedRpm, m.engine_speed_rpm)],
        Message::Et1(m) => vec![reading(Signal::CoolantTempC, m.coolant_temp_c)],
        Message::Lfe(m) => vec![reading(Signal::FuelRateLph, m.fuel_rate_lph)],
        Message::Ccvs(m) => vec![reading(Signal::WheelSpeedKmh, m.wheel_speed_kmh)],
        Message::VehiclePosition(m) => vec![
            reading(Signal::LatitudeDeg, m.latitude_deg),
            reading(Signal::LongitudeDeg, m.longitude_deg),
        ],
    };
    Ok(readings.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    fn frame(id: u32, data: &[u8]) -> Frame {
        Frame {
            timestamp: UNIX_EPOCH + Duration::from_secs(1),
            id,
            data: data.to_vec(),
        }
    }

    #[test]
    fn decodes_engine_speed() {
        let mut stats = DecodeStats::default();
        let readings = decode_frame(
            &frame(
                0x0CF0_0417,
                &[0xFF, 0xFF, 0xFF, 0xE0, 0x2E, 0xFF, 0xFF, 0xFF],
            ),
            &mut stats,
        )
        .unwrap();
        assert_eq!(
            readings,
            vec![Reading {
                timestamp: UNIX_EPOCH + Duration::from_secs(1),
                source_address: 0x17,
                signal: Signal::EngineSpeedRpm,
                value: 1500.0,
            }]
        );
        assert_eq!(
            stats,
            DecodeStats {
                frames: 1,
                decoded: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn position_gives_two_readings() {
        let mut stats = DecodeStats::default();
        let readings = decode_frame(
            &frame(
                0x18FE_F300,
                &[0x18, 0x45, 0xA8, 0x9A, 0xB0, 0x82, 0x5E, 0x87],
            ),
            &mut stats,
        )
        .unwrap();
        let signals: Vec<Signal> = readings.iter().map(|r| r.signal).collect();
        assert_eq!(signals, [Signal::LatitudeDeg, Signal::LongitudeDeg]);
    }

    #[test]
    fn not_available_is_decoded_but_gives_no_readings() {
        let mut stats = DecodeStats::default();
        let readings = decode_frame(&frame(0x18FE_F300, &[0xFF; 8]), &mut stats).unwrap();
        assert!(readings.is_empty());
        assert_eq!(stats.decoded, 1);
        assert_eq!(stats.errors(), 0);
    }

    #[test]
    fn counts_errors() {
        let mut stats = DecodeStats::default();
        assert!(decode_frame(&frame(0x18FF_1200, &[0; 8]), &mut stats).is_err());
        assert!(decode_frame(&frame(0x0CF0_0400, &[0xFF; 3]), &mut stats).is_err());
        assert_eq!(
            stats,
            DecodeStats {
                frames: 2,
                decoded: 0,
                unknown_pgn: 1,
                invalid_length: 1,
            }
        );
    }
}
