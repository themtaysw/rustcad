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
    #[must_use]
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

/// Decodes a frame into zero or more readings.
///
/// # Errors
///
/// Passes on [`DecodeError`] for unknown PGNs and wrong payload lengths.
pub fn decode_frame(frame: &Frame) -> Result<Vec<Reading>, DecodeError> {
    let id = CanId::from_raw(frame.id);
    let message = j1939::decode(id.pgn, &frame.data)?;

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
        let readings = decode_frame(&frame(
            0x0CF0_0417,
            &[0xFF, 0xFF, 0xFF, 0xE0, 0x2E, 0xFF, 0xFF, 0xFF],
        ))
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
    }

    #[test]
    fn position_gives_two_readings() {
        let readings = decode_frame(&frame(
            0x18FE_F300,
            &[0x18, 0x45, 0xA8, 0x9A, 0xB0, 0x82, 0x5E, 0x87],
        ))
        .unwrap();
        let signals: Vec<Signal> = readings.iter().map(|r| r.signal).collect();
        assert_eq!(signals, [Signal::LatitudeDeg, Signal::LongitudeDeg]);
    }

    #[test]
    fn not_available_is_decoded_but_gives_no_readings() {
        assert_eq!(decode_frame(&frame(0x18FE_F300, &[0xFF; 8])), Ok(vec![]));
    }

    #[test]
    fn passes_on_decode_errors() {
        assert_eq!(
            decode_frame(&frame(0x18FF_1200, &[0; 8])),
            Err(DecodeError::UnknownPgn(0xFF12))
        );
        assert_eq!(
            decode_frame(&frame(0x0CF0_0400, &[0xFF; 3])),
            Err(DecodeError::InvalidLength {
                pgn: 0xF004,
                len: 3
            })
        );
    }
}
