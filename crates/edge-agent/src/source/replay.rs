use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use tracing::warn;

use super::FrameSource;
use crate::frame::Frame;

const MAX_EXTENDED_ID: u32 = 0x1FFF_FFFF;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseError {
    #[error("expected `(timestamp) interface ID#DATA`")]
    Format,
    #[error("invalid timestamp")]
    Timestamp,
    #[error("invalid CAN ID, expected 8 hex digits up to 1FFFFFFF")]
    Id,
    #[error("invalid data, expected up to 8 bytes as hex pairs")]
    Data,
    #[error("CAN FD and remote frames are not supported")]
    Unsupported,
}

/// Parses one line of `candump -l` / `candump -L` output, e.g.
/// `(1700000000.123456) vcan0 0CF00400#FFFFFFE02EFFFFFF`.
pub fn parse_line(line: &str) -> Result<Frame, ParseError> {
    let mut parts = line.split_whitespace();
    let (Some(timestamp), Some(_iface), Some(frame), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(ParseError::Format);
    };

    let timestamp = parse_timestamp(timestamp)?;
    let (id, data) = frame.split_once('#').ok_or(ParseError::Format)?;
    if data.starts_with('#') || data.starts_with('R') {
        return Err(ParseError::Unsupported);
    }

    Ok(Frame {
        timestamp,
        id: parse_id(id)?,
        data: parse_data(data)?,
    })
}

fn parse_timestamp(s: &str) -> Result<SystemTime, ParseError> {
    let inner = s
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or(ParseError::Timestamp)?;
    let (secs, frac) = inner.split_once('.').ok_or(ParseError::Timestamp)?;
    if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ParseError::Timestamp);
    }
    let secs: u64 = secs.parse().map_err(|_| ParseError::Timestamp)?;
    let nanos: u32 = format!("{frac:0<9}")
        .parse()
        .map_err(|_| ParseError::Timestamp)?;
    Ok(UNIX_EPOCH + Duration::new(secs, nanos))
}

fn parse_id(s: &str) -> Result<u32, ParseError> {
    if s.len() != 8 {
        return Err(ParseError::Id);
    }
    let id = u32::from_str_radix(s, 16).map_err(|_| ParseError::Id)?;
    if id > MAX_EXTENDED_ID {
        return Err(ParseError::Id);
    }
    Ok(id)
}

fn parse_data(s: &str) -> Result<Vec<u8>, ParseError> {
    if !s.len().is_multiple_of(2) || s.len() > 16 || !s.is_ascii() {
        return Err(ParseError::Data);
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ParseError::Data))
        .collect()
}

/// Reads frames from a candump log. Broken lines are logged and skipped.
pub struct ReplaySource<R> {
    reader: R,
    line: String,
    line_number: u64,
    realtime: bool,
    previous: Option<SystemTime>,
    pub skipped_lines: u64,
}

impl<R> ReplaySource<R> {
    /// With `realtime`, frames are delayed by the gaps between their log timestamps.
    /// Otherwise the log is read as fast as the pipeline accepts it.
    pub fn new(reader: R, realtime: bool) -> Self {
        Self {
            reader,
            line: String::new(),
            line_number: 0,
            realtime,
            previous: None,
            skipped_lines: 0,
        }
    }
}

impl<R: AsyncBufRead + Unpin + Send> FrameSource for ReplaySource<R> {
    async fn next_frame(&mut self) -> io::Result<Option<Frame>> {
        loop {
            self.line.clear();
            if self.reader.read_line(&mut self.line).await? == 0 {
                return Ok(None);
            }
            self.line_number += 1;
            if self.line.trim().is_empty() {
                continue;
            }

            match parse_line(&self.line) {
                Ok(frame) => {
                    if self.realtime {
                        self.wait_for(frame.timestamp).await;
                    }
                    return Ok(Some(frame));
                }
                Err(e) => {
                    self.skipped_lines += 1;
                    warn!(line = self.line_number, error = %e, "skipping replay line");
                }
            }
        }
    }
}

impl<R> ReplaySource<R> {
    async fn wait_for(&mut self, timestamp: SystemTime) {
        if let Some(gap) = self
            .previous
            .and_then(|previous| timestamp.duration_since(previous).ok())
        {
            tokio::time::sleep(gap).await;
        }
        self.previous = Some(timestamp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: u64, micros: u32) -> SystemTime {
        UNIX_EPOCH + Duration::new(secs, micros * 1000)
    }

    #[test]
    fn parses_candump_line() {
        let frame = parse_line("(1700000000.123456) vcan0 0CF00400#FFFFFFE02EFFFFFF").unwrap();
        assert_eq!(
            frame,
            Frame {
                timestamp: at(1_700_000_000, 123_456),
                id: 0x0CF0_0400,
                data: vec![0xFF, 0xFF, 0xFF, 0xE0, 0x2E, 0xFF, 0xFF, 0xFF],
            }
        );
    }

    #[test]
    fn accepts_short_frames_and_trailing_newline() {
        let frame = parse_line("(1700000000.000001) can0 18FEEE00#7d\n").unwrap();
        assert_eq!(frame.data, vec![0x7D]);
        assert!(parse_line("(1.5) can0 18FEEE00#").unwrap().data.is_empty());
        assert_eq!(
            parse_line("(1.5) can0 18FEEE00#").unwrap().timestamp,
            at(1, 500_000)
        );
    }

    #[test]
    fn rejects_broken_lines() {
        let cases = [
            ("", ParseError::Format),
            ("(1.0) vcan0", ParseError::Format),
            ("(1.0) vcan0 0CF00400#FF extra", ParseError::Format),
            ("(1.0) vcan0 0CF00400", ParseError::Format),
            ("1.0 vcan0 0CF00400#FF", ParseError::Timestamp),
            ("(1) vcan0 0CF00400#FF", ParseError::Timestamp),
            ("(1.x) vcan0 0CF00400#FF", ParseError::Timestamp),
            ("(1.0) vcan0 123#FF", ParseError::Id),
            ("(1.0) vcan0 2CF00400#FF", ParseError::Id),
            ("(1.0) vcan0 0CF0040G#FF", ParseError::Id),
            ("(1.0) vcan0 0CF00400#FFF", ParseError::Data),
            ("(1.0) vcan0 0CF00400#ZZ", ParseError::Data),
            ("(1.0) vcan0 0CF00400#FFFFFFFFFFFFFFFFFF", ParseError::Data),
            ("(1.0) vcan0 0CF00400##0FF", ParseError::Unsupported),
            ("(1.0) vcan0 0CF00400#R", ParseError::Unsupported),
        ];
        for (line, expected) in cases {
            assert_eq!(parse_line(line), Err(expected), "{line:?}");
        }
    }

    #[tokio::test]
    async fn skips_bad_lines_and_stops_at_eof() {
        let log = "\
(1.000000) vcan0 18FEEE00#7DFFFFFFFFFFFFFF
garbage

(2.000000) vcan0 0CF00400#FFFFFFE02EFFFFFF
";
        let mut source = ReplaySource::new(log.as_bytes(), false);
        assert_eq!(source.next_frame().await.unwrap().unwrap().id, 0x18FE_EE00);
        assert_eq!(source.next_frame().await.unwrap().unwrap().id, 0x0CF0_0400);
        assert_eq!(source.next_frame().await.unwrap(), None);
        assert_eq!(source.skipped_lines, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn realtime_replay_keeps_the_gaps() {
        let log = "\
(10.000000) vcan0 18FEEE00#7D
(10.500000) vcan0 18FEEE00#7D
(12.500000) vcan0 18FEEE00#7D
";
        let mut source = ReplaySource::new(log.as_bytes(), true);
        let start = tokio::time::Instant::now();
        while source.next_frame().await.unwrap().is_some() {}
        assert_eq!(start.elapsed(), Duration::from_millis(2500));
    }
}
