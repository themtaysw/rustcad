mod field;
mod model;
mod sim;
mod sink;

use std::io;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use rand::{SeedableRng, rngs::StdRng};

use crate::sim::{Simulator, ticks};
use crate::sink::{FrameSink, LogSink};

/// Simulates a tractor working a field and writes its J1939 frames to a CAN interface.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// CAN interface to write to.
    #[arg(long, default_value = "vcan0")]
    iface: String,

    /// Simulation ticks per second. EEC1 is sent on every tick, slower PGNs less often.
    #[arg(long, default_value_t = 20.0, value_parser = parse_rate)]
    rate_hz: f64,

    /// J1939 source address of the simulated engine, decimal or 0x hex.
    #[arg(long, default_value = "0x00", value_parser = parse_u8)]
    source_address: u8,

    /// Probability (0.0-1.0) per tick of an extra bad frame: unknown PGN, wrong length
    /// or "not available" values.
    #[arg(long, default_value_t = 0.0, value_parser = parse_probability)]
    garbage_rate: f64,

    /// Stop after this many seconds. Runs forever if not set.
    #[arg(long, value_parser = parse_duration)]
    duration_secs: Option<f64>,

    /// Seed for the random noise, for reproducible runs.
    #[arg(long)]
    seed: Option<u64>,

    /// Print frames to stdout in candump log format instead of sending them.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let rng = match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_os_rng(),
    };
    let mut sim = Simulator::new(args.rate_hz, args.source_address, args.garbage_rate, rng);
    let mut sink = open_sink(&args)?;

    let tick = Duration::from_secs_f64(1.0 / args.rate_hz);
    let status_every = ticks(5.0, args.rate_hz);
    let total_ticks = args.duration_secs.map(|secs| ticks(secs, args.rate_hz));
    let start = Instant::now();

    eprintln!("tractor-sim: {args:?}");

    let mut n: u64 = 0;
    while total_ticks.is_none_or(|total| n < total) {
        for frame in sim.tick() {
            if let Err(e) = sink.send(&frame) {
                // `--dry-run | head` closes the pipe early; that is a normal way to stop.
                if is_broken_pipe(&e) {
                    return Ok(());
                }
                return Err(e);
            }
        }
        if n.is_multiple_of(status_every) {
            eprintln!("{}", sim.tractor().status());
        }

        n += 1;
        // Sleeping until an absolute deadline keeps the rate exact even when a tick runs long.
        let deadline = start + tick.mul_f64(n as f64);
        if let Some(wait) = deadline.checked_duration_since(Instant::now()) {
            thread::sleep(wait);
        }
    }
    Ok(())
}

fn open_sink(args: &Args) -> Result<Box<dyn FrameSink>> {
    if args.dry_run {
        return Ok(Box::new(LogSink::new(io::stdout().lock(), &args.iface)));
    }
    open_can(&args.iface)
}

#[cfg(target_os = "linux")]
fn open_can(iface: &str) -> Result<Box<dyn FrameSink>> {
    Ok(Box::new(sink::SocketCanSink::open(iface)?))
}

#[cfg(not(target_os = "linux"))]
fn open_can(_iface: &str) -> Result<Box<dyn FrameSink>> {
    anyhow::bail!("SocketCAN is only available on Linux, use --dry-run to print frames instead")
}

fn is_broken_pipe(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<io::Error>()
        .is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe)
}

fn parse_u8(s: &str) -> Result<u8, String> {
    let invalid = || format!("{s:?} is not a byte (0-255 or 0x00-0xFF)");
    // `from_str_radix` and `parse` accept a leading `+`, so check the digits first.
    match s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        Some(hex) if !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit()) => {
            u8::from_str_radix(hex, 16).map_err(|_| invalid())
        }
        None if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => {
            s.parse().map_err(|_| invalid())
        }
        _ => Err(invalid()),
    }
}

fn parse_rate(s: &str) -> Result<f64, String> {
    let rate: f64 = s.parse().map_err(|e| format!("{e}"))?;
    if !(rate > 0.0 && rate <= 1000.0) {
        return Err(format!("{rate} is not in (0, 1000]"));
    }
    Ok(rate)
}

fn parse_duration(s: &str) -> Result<f64, String> {
    let secs: f64 = s.parse().map_err(|e| format!("{e}"))?;
    // About a year; also rejects NaN, which fails every comparison.
    if !(secs > 0.0 && secs <= 31_536_000.0) {
        return Err(format!("{secs} is not in (0, 31536000]"));
    }
    Ok(secs)
}

fn parse_probability(s: &str) -> Result<f64, String> {
    let p: f64 = s.parse().map_err(|e| format!("{e}"))?;
    if !(0.0..=1.0).contains(&p) {
        return Err(format!("{p} is not in [0.0, 1.0]"));
    }
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_source_address_in_hex_and_decimal() {
        assert_eq!(parse_u8("0x00"), Ok(0));
        assert_eq!(parse_u8("0xF9"), Ok(0xF9));
        assert_eq!(parse_u8("0XAB"), Ok(0xAB));
        assert_eq!(parse_u8("42"), Ok(42));
        assert!(parse_u8("0x100").is_err());
        assert!(parse_u8("256").is_err());
        assert!(parse_u8("abc").is_err());
        assert!(parse_u8("+1").is_err());
        assert!(parse_u8("0x+F").is_err());
        assert!(parse_u8("0x").is_err());
        assert!(parse_u8("").is_err());
    }

    #[test]
    fn rejects_bad_rates_and_probabilities() {
        assert_eq!(parse_rate("10"), Ok(10.0));
        assert!(parse_rate("0").is_err());
        assert!(parse_rate("-5").is_err());
        assert!(parse_rate("NaN").is_err());
        assert_eq!(parse_probability("0.05"), Ok(0.05));
        assert!(parse_probability("1.5").is_err());
        assert!(parse_probability("NaN").is_err());
        assert_eq!(parse_duration("40"), Ok(40.0));
        assert!(parse_duration("0").is_err());
        assert!(parse_duration("-1").is_err());
        assert!(parse_duration("NaN").is_err());
        assert!(parse_duration("inf").is_err());
    }

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        Args::command().debug_assert();
    }
}
