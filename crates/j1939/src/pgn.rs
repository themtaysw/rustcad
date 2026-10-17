use crate::spn::{Spn, Width};

const NOT_AVAILABLE: [u8; 8] = [0xFF; 8];

/// Electronic Engine Controller 1.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Eec1 {
    pub engine_speed_rpm: Option<f64>,
}

impl Eec1 {
    pub const PGN: u32 = 61444;

    const ENGINE_SPEED: Spn = Spn {
        byte: 4,
        width: Width::Two,
        resolution: 0.125,
        offset: 0.0,
    };

    pub fn decode(data: &[u8; 8]) -> Self {
        Self {
            engine_speed_rpm: Self::ENGINE_SPEED.decode(data),
        }
    }

    pub fn encode(&self) -> [u8; 8] {
        let mut data = NOT_AVAILABLE;
        Self::ENGINE_SPEED.encode(self.engine_speed_rpm, &mut data);
        data
    }
}

/// Engine Temperature 1.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Et1 {
    pub coolant_temp_c: Option<f64>,
}

impl Et1 {
    pub const PGN: u32 = 65262;

    const COOLANT_TEMP: Spn = Spn {
        byte: 1,
        width: Width::One,
        resolution: 1.0,
        offset: -40.0,
    };

    pub fn decode(data: &[u8; 8]) -> Self {
        Self {
            coolant_temp_c: Self::COOLANT_TEMP.decode(data),
        }
    }

    pub fn encode(&self) -> [u8; 8] {
        let mut data = NOT_AVAILABLE;
        Self::COOLANT_TEMP.encode(self.coolant_temp_c, &mut data);
        data
    }
}

/// Fuel Economy (Liquid).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Lfe {
    pub fuel_rate_lph: Option<f64>,
}

impl Lfe {
    pub const PGN: u32 = 65266;

    const FUEL_RATE: Spn = Spn {
        byte: 1,
        width: Width::Two,
        resolution: 0.05,
        offset: 0.0,
    };

    pub fn decode(data: &[u8; 8]) -> Self {
        Self {
            fuel_rate_lph: Self::FUEL_RATE.decode(data),
        }
    }

    pub fn encode(&self) -> [u8; 8] {
        let mut data = NOT_AVAILABLE;
        Self::FUEL_RATE.encode(self.fuel_rate_lph, &mut data);
        data
    }
}

/// Cruise Control / Vehicle Speed.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Ccvs {
    pub wheel_speed_kmh: Option<f64>,
}

impl Ccvs {
    pub const PGN: u32 = 65265;

    const WHEEL_SPEED: Spn = Spn {
        byte: 2,
        width: Width::Two,
        resolution: 1.0 / 256.0,
        offset: 0.0,
    };

    pub fn decode(data: &[u8; 8]) -> Self {
        Self {
            wheel_speed_kmh: Self::WHEEL_SPEED.decode(data),
        }
    }

    pub fn encode(&self) -> [u8; 8] {
        let mut data = NOT_AVAILABLE;
        Self::WHEEL_SPEED.encode(self.wheel_speed_kmh, &mut data);
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(actual: Option<f64>, expected: f64, tolerance: f64) {
        let actual = actual.expect("expected Some, got None");
        assert!(
            (actual - expected).abs() <= tolerance,
            "{actual} is not within {tolerance} of {expected}"
        );
    }

    #[test]
    fn eec1_pgn_matches_spec_hex() {
        assert_eq!(Eec1::PGN, 0xF004);
    }

    #[test]
    fn eec1_engine_speed() {
        let data = [0xFF, 0xFF, 0xFF, 0xE0, 0x2E, 0xFF, 0xFF, 0xFF];
        let msg = Eec1::decode(&data);
        assert_eq!(msg.engine_speed_rpm, Some(1500.0));
        assert_eq!(msg.encode(), data);
    }

    #[test]
    fn eec1_max_valid_and_clamping() {
        let max = Eec1 {
            engine_speed_rpm: Some(8031.875),
        };
        assert_eq!(Eec1::decode(&max.encode()), max);

        let too_fast = Eec1 {
            engine_speed_rpm: Some(9000.0),
        };
        assert_eq!(Eec1::decode(&too_fast.encode()), max);
    }

    #[test]
    fn eec1_error_indicator_is_none() {
        let data = [0xFF, 0xFF, 0xFF, 0x00, 0xFE, 0xFF, 0xFF, 0xFF];
        assert_eq!(Eec1::decode(&data).engine_speed_rpm, None);
    }

    #[test]
    fn eec1_not_available() {
        assert_eq!(Eec1::decode(&NOT_AVAILABLE), Eec1::default());
        assert_eq!(Eec1::default().encode(), NOT_AVAILABLE);
    }

    #[test]
    fn eec1_round_trip_within_resolution() {
        for rpm in [0.0, 650.0, 1234.567, 2100.3, 8031.875] {
            let msg = Eec1 {
                engine_speed_rpm: Some(rpm),
            };
            assert_close(
                Eec1::decode(&msg.encode()).engine_speed_rpm,
                rpm,
                0.125 / 2.0,
            );
        }
    }

    #[test]
    fn et1_pgn_matches_spec_hex() {
        assert_eq!(Et1::PGN, 0xFEEE);
    }

    #[test]
    fn et1_coolant_temp() {
        let data = [0x7D, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let msg = Et1::decode(&data);
        assert_eq!(msg.coolant_temp_c, Some(85.0));
        assert_eq!(msg.encode(), data);
    }

    #[test]
    fn et1_offset_and_limits() {
        assert_eq!(Et1::decode(&[0x00; 8]).coolant_temp_c, Some(-40.0));
        assert_eq!(Et1::decode(&[0xFA; 8]).coolant_temp_c, Some(210.0));
        assert_eq!(Et1::decode(&[0xFE; 8]).coolant_temp_c, None);
    }

    #[test]
    fn et1_not_available() {
        assert_eq!(Et1::decode(&NOT_AVAILABLE), Et1::default());
        assert_eq!(Et1::default().encode(), NOT_AVAILABLE);
    }

    #[test]
    fn et1_round_trip_within_resolution() {
        for temp in [-40.0, -5.4, 20.0, 89.6, 210.0] {
            let msg = Et1 {
                coolant_temp_c: Some(temp),
            };
            assert_close(Et1::decode(&msg.encode()).coolant_temp_c, temp, 0.5);
        }
    }

    #[test]
    fn lfe_pgn_matches_spec_hex() {
        assert_eq!(Lfe::PGN, 0xFEF2);
    }

    #[test]
    fn lfe_fuel_rate() {
        let data = [0xF7, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let msg = Lfe::decode(&data);
        assert_close(msg.fuel_rate_lph, 12.35, 1e-9);
        assert_eq!(msg.encode(), data);
    }

    #[test]
    fn lfe_not_available() {
        assert_eq!(Lfe::decode(&NOT_AVAILABLE), Lfe::default());
        assert_eq!(Lfe::default().encode(), NOT_AVAILABLE);
    }

    #[test]
    fn lfe_round_trip_within_resolution() {
        for rate in [0.0, 3.21, 12.35, 47.99, 3212.75] {
            let msg = Lfe {
                fuel_rate_lph: Some(rate),
            };
            assert_close(Lfe::decode(&msg.encode()).fuel_rate_lph, rate, 0.05 / 2.0);
        }
    }

    #[test]
    fn ccvs_pgn_matches_spec_hex() {
        assert_eq!(Ccvs::PGN, 0xFEF1);
    }

    #[test]
    fn ccvs_wheel_speed() {
        let data = [0xFF, 0x80, 0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let msg = Ccvs::decode(&data);
        assert_eq!(msg.wheel_speed_kmh, Some(8.5));
        assert_eq!(msg.encode(), data);
    }

    #[test]
    fn ccvs_not_available() {
        assert_eq!(Ccvs::decode(&NOT_AVAILABLE), Ccvs::default());
        assert_eq!(Ccvs::default().encode(), NOT_AVAILABLE);
    }

    #[test]
    fn ccvs_round_trip_within_resolution() {
        for speed in [0.0, 0.3, 7.77, 12.0, 250.996] {
            let msg = Ccvs {
                wheel_speed_kmh: Some(speed),
            };
            assert_close(
                Ccvs::decode(&msg.encode()).wheel_speed_kmh,
                speed,
                1.0 / 512.0,
            );
        }
    }
}
