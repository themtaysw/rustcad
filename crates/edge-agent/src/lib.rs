//! Edge agent internals: frame sources, J1939 decoding, aggregation, the SQLite outbox,
//! the HTTP uplink and the async pipeline that connects them.

pub mod aggregate;
pub mod config;
pub mod decode;
pub mod frame;
pub mod metrics;
pub mod outbox;
pub mod pipeline;
pub mod secret;
pub mod source;
pub mod uplink;
