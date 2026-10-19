use std::io;
use std::time::SystemTime;

use socketcan::tokio::CanSocket;
use socketcan::{CanFrame, EmbeddedFrame, Id};
use tracing::debug;

use super::FrameSource;
use crate::frame::Frame;

/// Live frames from a `SocketCAN` interface such as `can0` or `vcan0`.
pub struct SocketCanSource {
    socket: CanSocket,
}

impl SocketCanSource {
    pub fn open(iface: &str) -> io::Result<Self> {
        Ok(Self {
            socket: CanSocket::open(iface)?,
        })
    }
}

impl FrameSource for SocketCanSource {
    async fn next_frame(&mut self) -> io::Result<Option<Frame>> {
        loop {
            let frame = self.socket.read_frame().await?;
            let timestamp = SystemTime::now();
            match frame {
                CanFrame::Data(data_frame) => match data_frame.id() {
                    Id::Extended(id) => {
                        return Ok(Some(Frame {
                            timestamp,
                            id: id.as_raw(),
                            data: data_frame.data().to_vec(),
                        }));
                    }
                    Id::Standard(id) => debug!(id = id.as_raw(), "skipping 11-bit frame"),
                },
                CanFrame::Remote(_) | CanFrame::Error(_) => debug!("skipping remote/error frame"),
            }
        }
    }
}
