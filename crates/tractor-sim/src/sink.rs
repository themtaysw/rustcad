use std::fmt::Write as _;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::sim::Frame;

pub trait FrameSink {
    fn send(&mut self, frame: &Frame) -> Result<()>;
}

/// Writes frames in `candump -l` log format, so the output can be replayed later.
pub struct LogSink<W> {
    out: W,
    iface: String,
}

impl<W: Write> LogSink<W> {
    pub fn new(out: W, iface: &str) -> Self {
        Self {
            out,
            iface: iface.to_owned(),
        }
    }
}

impl<W: Write> FrameSink for LogSink<W> {
    fn send(&mut self, frame: &Frame) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        writeln!(
            self.out,
            "({}.{:06}) {}",
            now.as_secs(),
            now.subsec_micros(),
            format_frame(&self.iface, frame)
        )?;
        Ok(())
    }
}

fn format_frame(iface: &str, frame: &Frame) -> String {
    let mut line = format!("{iface} {:08X}#", frame.id);
    for byte in &frame.data {
        // Writing to a String cannot fail.
        let _ = write!(line, "{byte:02X}");
    }
    line
}

#[cfg(target_os = "linux")]
pub use socket::SocketCanSink;

#[cfg(target_os = "linux")]
mod socket {
    use anyhow::{Context, Result};
    use socketcan::{CanFrame, CanSocket, EmbeddedFrame, ExtendedId, Socket};

    use super::FrameSink;
    use crate::sim::Frame;

    pub struct SocketCanSink {
        socket: CanSocket,
    }

    impl SocketCanSink {
        pub fn open(iface: &str) -> Result<Self> {
            let socket = CanSocket::open(iface)
                .with_context(|| format!("cannot open CAN interface {iface}"))?;
            Ok(Self { socket })
        }
    }

    impl FrameSink for SocketCanSink {
        fn send(&mut self, frame: &Frame) -> Result<()> {
            let id = ExtendedId::new(frame.id).context("CAN ID does not fit in 29 bits")?;
            let can_frame = CanFrame::new(id, &frame.data).context("frame data too long")?;
            self.socket.write_frame(&can_frame)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_like_candump() {
        let frame = Frame {
            id: 0x0CF0_0400,
            data: vec![0xFF, 0xFF, 0xFF, 0xE0, 0x2E, 0xFF, 0xFF, 0xFF],
        };
        assert_eq!(
            format_frame("vcan0", &frame),
            "vcan0 0CF00400#FFFFFFE02EFFFFFF"
        );
    }

    #[test]
    fn empty_payload() {
        let frame = Frame {
            id: 0x0CF0_0400,
            data: vec![],
        };
        assert_eq!(format_frame("vcan0", &frame), "vcan0 0CF00400#");
    }

    #[test]
    fn log_line_has_timestamp_prefix() {
        let mut out = Vec::new();
        let frame = Frame {
            id: 0x18FE_EE00,
            data: vec![0x7D],
        };
        LogSink::new(&mut out, "vcan0").send(&frame).unwrap();

        let line = String::from_utf8(out).unwrap();
        let (timestamp, rest) = line.split_once(' ').unwrap();
        assert!(timestamp.starts_with('(') && timestamp.ends_with(')'));
        assert_eq!(timestamp.len(), "(1700000000.000000)".len());
        assert_eq!(rest, "vcan0 18FEEE00#7D\n");
    }
}
