//! SAE J1939 helpers: 29-bit CAN ID parsing and decoding/encoding of a few PGNs.
//!
//! Pure library, no IO.

mod id;
mod message;
mod pgn;
mod spn;

pub use id::CanId;
pub use message::{DecodeError, Message, decode};
pub use pgn::{Ccvs, Eec1, Et1, Lfe, VehiclePosition};
