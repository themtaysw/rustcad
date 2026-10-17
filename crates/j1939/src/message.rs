use thiserror::Error;

use crate::pgn::{Ccvs, Eec1, Et1, Lfe, VehiclePosition};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("unknown PGN {0} ({0:#06X})")]
    UnknownPgn(u32),
    #[error("PGN {pgn}: expected 8 data bytes, got {len}")]
    InvalidLength { pgn: u32, len: usize },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Message {
    Eec1(Eec1),
    Et1(Et1),
    Lfe(Lfe),
    Ccvs(Ccvs),
    VehiclePosition(VehiclePosition),
}

impl Message {
    pub fn pgn(&self) -> u32 {
        match self {
            Message::Eec1(_) => Eec1::PGN,
            Message::Et1(_) => Et1::PGN,
            Message::Lfe(_) => Lfe::PGN,
            Message::Ccvs(_) => Ccvs::PGN,
            Message::VehiclePosition(_) => VehiclePosition::PGN,
        }
    }

    pub fn encode(&self) -> [u8; 8] {
        match self {
            Message::Eec1(m) => m.encode(),
            Message::Et1(m) => m.encode(),
            Message::Lfe(m) => m.encode(),
            Message::Ccvs(m) => m.encode(),
            Message::VehiclePosition(m) => m.encode(),
        }
    }
}

pub fn decode(pgn: u32, data: &[u8]) -> Result<Message, DecodeError> {
    let message = match pgn {
        Eec1::PGN => Message::Eec1(Eec1::decode(payload(pgn, data)?)),
        Et1::PGN => Message::Et1(Et1::decode(payload(pgn, data)?)),
        Lfe::PGN => Message::Lfe(Lfe::decode(payload(pgn, data)?)),
        Ccvs::PGN => Message::Ccvs(Ccvs::decode(payload(pgn, data)?)),
        VehiclePosition::PGN => {
            Message::VehiclePosition(VehiclePosition::decode(payload(pgn, data)?))
        }
        _ => return Err(DecodeError::UnknownPgn(pgn)),
    };
    Ok(message)
}

fn payload(pgn: u32, data: &[u8]) -> Result<&[u8; 8], DecodeError> {
    data.try_into().map_err(|_| DecodeError::InvalidLength {
        pgn,
        len: data.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatches_by_pgn() {
        let messages = [
            Message::Eec1(Eec1 {
                engine_speed_rpm: Some(1500.0),
            }),
            Message::Et1(Et1 {
                coolant_temp_c: Some(85.0),
            }),
            Message::Lfe(Lfe {
                fuel_rate_lph: None,
            }),
            Message::Ccvs(Ccvs {
                wheel_speed_kmh: Some(8.5),
            }),
            Message::VehiclePosition(VehiclePosition {
                latitude_deg: None,
                longitude_deg: None,
            }),
        ];
        for message in messages {
            assert_eq!(decode(message.pgn(), &message.encode()), Ok(message));
        }
    }

    #[test]
    fn unknown_pgn_is_an_error_not_a_panic() {
        assert_eq!(
            decode(0xFECA, &[0; 8]),
            Err(DecodeError::UnknownPgn(0xFECA))
        );
        assert_eq!(decode(0xFECA, &[]), Err(DecodeError::UnknownPgn(0xFECA)));
    }

    #[test]
    fn wrong_length_is_an_error() {
        assert_eq!(
            decode(Eec1::PGN, &[0xFF; 5]),
            Err(DecodeError::InvalidLength {
                pgn: Eec1::PGN,
                len: 5
            })
        );
        assert_eq!(
            decode(Eec1::PGN, &[0xFF; 9]),
            Err(DecodeError::InvalidLength {
                pgn: Eec1::PGN,
                len: 9
            })
        );
    }

    #[test]
    fn error_messages() {
        assert_eq!(
            DecodeError::UnknownPgn(0xFECA).to_string(),
            "unknown PGN 65226 (0xFECA)"
        );
    }
}
