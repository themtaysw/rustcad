//! SAE J1939 helpers: 29-bit CAN ID parsing and decoding/encoding of a few PGNs.
//!
//! Pure library, no IO.

mod id;

pub use id::CanId;
