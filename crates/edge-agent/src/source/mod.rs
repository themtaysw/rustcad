use std::future::Future;
use std::io;

use crate::frame::Frame;

mod replay;
#[cfg(target_os = "linux")]
mod socketcan;

pub use replay::{ParseError, ReplaySource, parse_line};
#[cfg(target_os = "linux")]
pub use socketcan::SocketCanSource;

/// Something that produces CAN frames: a live bus or a recorded log.
///
/// Written as `-> impl Future + Send` instead of `async fn` so the reader task can be
/// spawned on the multi-threaded runtime, which requires `Send` futures.
pub trait FrameSource {
    /// `Ok(None)` means the source is exhausted (end of a replay file).
    fn next_frame(&mut self) -> impl Future<Output = io::Result<Option<Frame>>> + Send;
}
