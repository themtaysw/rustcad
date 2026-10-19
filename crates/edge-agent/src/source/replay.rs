use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};
use tracing::warn;

use super::FrameSource;
use crate::frame::{Frame, timestamp_in_range};

const MAX_EXTENDED_ID: u32 = 0x1FFF_FFFF;
/// A candump line is about 60 bytes. Anything much longer is not one and is skipped
/// without buffering all of it.
const MAX_LINE_BYTES: u64 = 256;

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
    #[error("line is not UTF-8")]
    Encoding,
    #[error("line is longer than {MAX_LINE_BYTES} bytes")]
    TooLong,
}

/// Parses one line of `candump -l` / `candump -L` output, e.g.
/// `(1700000000.123456) vcan0 0CF00400#FFFFFFE02EFFFFFF`.
///
/// # Errors
///
/// Returns which part of the line is malformed.
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

fn is_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

fn is_hex(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn parse_timestamp(s: &str) -> Result<SystemTime, ParseError> {
    let inner = s
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or(ParseError::Timestamp)?;
    let (secs, frac) = inner.split_once('.').ok_or(ParseError::Timestamp)?;
    if !is_digits(secs) || !is_digits(frac) || frac.len() > 9 {
        return Err(ParseError::Timestamp);
    }
    let secs: u64 = secs.parse().map_err(|_| ParseError::Timestamp)?;
    let nanos: u32 = format!("{frac:0<9}")
        .parse()
        .map_err(|_| ParseError::Timestamp)?;
    UNIX_EPOCH
        .checked_add(Duration::new(secs, nanos))
        .filter(|&ts| timestamp_in_range(ts))
        .ok_or(ParseError::Timestamp)
}

fn parse_id(s: &str) -> Result<u32, ParseError> {
    if s.len() != 8 || !is_hex(s) {
        return Err(ParseError::Id);
    }
    let id = u32::from_str_radix(s, 16).map_err(|_| ParseError::Id)?;
    if id > MAX_EXTENDED_ID {
        return Err(ParseError::Id);
    }
    Ok(id)
}

fn parse_data(s: &str) -> Result<Vec<u8>, ParseError> {
    if !s.len().is_multiple_of(2) || s.len() > 16 || !is_hex(s) {
        return Err(ParseError::Data);
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ParseError::Data))
        .collect()
}

/// Reads frames from a candump log. Broken lines are logged and skipped.
#[derive(Debug)]
pub struct ReplaySource<R> {
    reader: R,
    line: Vec<u8>,
    line_number: u64,
    realtime: bool,
    previous: Option<SystemTime>,
    skipped_lines: u64,
}

impl<R> ReplaySource<R> {
    /// With `realtime`, frames are delayed by the gaps between their log timestamps.
    /// Otherwise the log is read as fast as the pipeline accepts it.
    pub fn new(reader: R, realtime: bool) -> Self {
        Self {
            reader,
            line: Vec::new(),
            line_number: 0,
            realtime,
            previous: None,
            skipped_lines: 0,
        }
    }

    /// Lines that were not valid candump frames.
    #[must_use]
    pub fn skipped_lines(&self) -> u64 {
        self.skipped_lines
    }
}

impl<R: AsyncBufRead + Unpin + Send> ReplaySource<R> {
    /// Reads the next line into `self.line`, at most [`MAX_LINE_BYTES`] of it.
    /// `Ok(None)` at end of input.
    async fn read_line(&mut self) -> io::Result<Option<Result<(), ParseError>>> {
        self.line.clear();
        let n = (&mut self.reader)
            .take(MAX_LINE_BYTES + 1)
            .read_until(b'\n', &mut self.line)
            .await?;
        if n == 0 {
            return Ok(None);
        }
        let complete = self.line.last() == Some(&b'\n');
        if complete || self.line.len() as u64 <= MAX_LINE_BYTES {
            return Ok(Some(Ok(())));
        }

        // Too long: throw away the rest of the line in bounded chunks.
        loop {
            self.line.clear();
            let n = (&mut self.reader)
                .take(MAX_LINE_BYTES)
                .read_until(b'\n', &mut self.line)
                .await?;
            if n == 0 || self.line.last() == Some(&b'\n') {
                return Ok(Some(Err(ParseError::TooLong)));
            }
        }
    }

    fn skip(&mut self, error: &ParseError) {
        self.skipped_lines += 1;
        warn!(line = self.line_number, %error, "skipping replay line");
    }
}

impl<R: AsyncBufRead + Unpin + Send> FrameSource for ReplaySource<R> {
    async fn next_frame(&mut self) -> io::Result<Option<Frame>> {
        loop {
            let Some(read) = self.read_line().await? else {
                if self.skipped_lines > 0 {
                    warn!(
                        skipped = self.skipped_lines,
                        "replay finished with skipped lines"
                    );
                }
                return Ok(None);
            };
            self.line_number += 1;

            let parsed = read.and_then(|()| {
                let line = std::str::from_utf8(&self.line).map_err(|_| ParseError::Encoding)?;
                if line.trim().is_empty() {
                    Ok(None)
                } else {
                    parse_line(line).map(Some)
                }
            });
            match parsed {
                Ok(None) => {}
                Ok(Some(frame)) => {
                    if self.realtime {
                        self.wait_for(frame.timestamp).await;
                    }
                    return Ok(Some(frame));
                }
                Err(error) => self.skip(&error),
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
            ("(+1.0) vcan0 0CF00400#FF", ParseError::Timestamp),
            ("(1.+5) vcan0 0CF00400#FF", ParseError::Timestamp),
            (
                "(18446744073709551615.0) vcan0 0CF00400#FF",
                ParseError::Timestamp,
            ),
            ("(253402300800.0) vcan0 0CF00400#FF", ParseError::Timestamp),
            ("(1.0) vcan0 +CF00400#FF", ParseError::Id),
            ("(1.0) vcan0 123#FF", ParseError::Id),
            ("(1.0) vcan0 2CF00400#FF", ParseError::Id),
            ("(1.0) vcan0 0CF0040G#FF", ParseError::Id),
            ("(1.0) vcan0 0CF00400#FFF", ParseError::Data),
            ("(1.0) vcan0 0CF00400#ZZ", ParseError::Data),
            ("(1.0) vcan0 0CF00400#+F", ParseError::Data),
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
        assert_eq!(source.skipped_lines(), 1);
    }

    #[tokio::test]
    async fn skips_lines_that_are_not_utf8_or_too_long() {
        let mut log = b"(1.000000) vcan0 18FEEE00#7D\n".to_vec();
        log.extend_from_slice(b"\xFF\xFE broken\n");
        log.extend_from_slice(&[b'A'; 10_000]);
        log.extend_from_slice(b"\n(2.000000) vcan0 18FEEE00#7E\n");
        log.extend_from_slice(b"(3.000000) vcan0 18FEEE00#7F");

        let mut source = ReplaySource::new(log.as_slice(), false);
        let mut data = Vec::new();
        while let Some(frame) = source.next_frame().await.unwrap() {
            data.push(frame.data[0]);
        }
        assert_eq!(data, [0x7D, 0x7E, 0x7F]);
        assert_eq!(source.skipped_lines(), 2);
    }

    #[test]
    fn accepts_the_last_representable_second() {
        let frame = parse_line("(253402300799.999999) vcan0 18FEEE00#7D").unwrap();
        assert!(timestamp_in_range(frame.timestamp));
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
