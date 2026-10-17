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
}
