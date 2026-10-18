use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use edge_agent::outbox::Outbox;
use edge_agent::pipeline::{self, Summary};
use edge_agent::source::ReplaySource;
use edge_agent::uplink::UplinkConfig;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Reads J1939 frames, aggregates them, stores them in a SQLite outbox and uploads them.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[command(flatten)]
    source: SourceArgs,

    /// With --replay: keep the original timing between frames instead of reading at full speed.
    #[arg(long, conflicts_with = "iface")]
    realtime: bool,

    #[arg(long, env = "EDGE_DEVICE_ID", default_value = "tractor-1")]
    device_id: String,

    /// Receiver base URL.
    #[arg(
        long,
        env = "EDGE_RECEIVER_URL",
        default_value = "http://127.0.0.1:18000"
    )]
    receiver_url: String,

    /// Bearer token for the receiver.
    #[arg(long, env = "EDGE_RECEIVER_TOKEN", hide_env_values = true)]
    receiver_token: String,

    #[arg(long, env = "EDGE_OUTBOX_PATH", default_value = "edge-outbox.db")]
    outbox_path: PathBuf,

    /// Maximum number of unsent records kept. The oldest are dropped beyond that.
    #[arg(long, env = "EDGE_OUTBOX_CAP", default_value_t = 500_000)]
    outbox_cap: u64,

    #[arg(long, env = "EDGE_BATCH_SIZE", default_value_t = 500, value_parser = clap::value_parser!(u32).range(1..=10_000))]
    batch_size: u32,

    #[arg(long, env = "EDGE_FLUSH_INTERVAL_SECS", default_value_t = 5.0)]
    flush_interval_secs: f64,

    /// How long the last upload attempt on shutdown may take.
    #[arg(long, env = "EDGE_FINAL_FLUSH_SECS", default_value_t = 10.0)]
    final_flush_secs: f64,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
struct SourceArgs {
    /// Read live frames from this SocketCAN interface (Linux only).
    #[arg(long)]
    iface: Option<String>,

    /// Replay frames from a candump log file (`candump -l` / `candump -L` format).
    #[arg(long)]
    replay: Option<PathBuf>,
}

impl Args {
    fn uplink_config(&self) -> UplinkConfig {
        UplinkConfig {
            url: self.receiver_url.clone(),
            token: self.receiver_token.clone(),
            device_id: self.device_id.clone(),
            batch_size: self.batch_size as usize,
            flush_interval: Duration::from_secs_f64(self.flush_interval_secs),
            request_timeout: Duration::from_secs(10),
            final_flush_timeout: Duration::from_secs_f64(self.final_flush_secs),
            backoff_base: Duration::from_millis(500),
            backoff_max: Duration::from_secs(60),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    info!(
        device_id = args.device_id,
        receiver = args.receiver_url,
        outbox = %args.outbox_path.display(),
        "starting"
    );

    let shutdown = CancellationToken::new();
    tokio::spawn(stop_on_ctrl_c(shutdown.clone()));

    let summary = run(&args, shutdown).await?;
    info!(
        frames = summary.frames_read,
        decoded = summary.decode.decoded,
        unknown_pgn = summary.decode.unknown_pgn,
        invalid_length = summary.decode.invalid_length,
        samples = summary.samples_written,
        uploaded_records = summary.uplink.records,
        upload_failures = summary.uplink.failures,
        outbox_depth = summary.outbox_depth,
        "stopped"
    );
    Ok(())
}

/// First Ctrl-C stops reading and lets the pipeline drain and upload. A second one exits at once.
async fn stop_on_ctrl_c(shutdown: CancellationToken) {
    if tokio::signal::ctrl_c().await.is_err() {
        return;
    }
    info!("shutting down, press Ctrl-C again to exit immediately");
    shutdown.cancel();
    if tokio::signal::ctrl_c().await.is_ok() {
        warn!("exiting without final upload");
        std::process::exit(130);
    }
}

async fn run(args: &Args, shutdown: CancellationToken) -> Result<Summary> {
    let outbox = Outbox::open(&args.outbox_path, args.outbox_cap)
        .with_context(|| format!("cannot open outbox {}", args.outbox_path.display()))?;
    let config = args.uplink_config();

    if let Some(path) = &args.source.replay {
        let file = File::open(path)
            .await
            .with_context(|| format!("cannot open replay file {}", path.display()))?;
        let source = ReplaySource::new(BufReader::new(file), args.realtime);
        return Ok(pipeline::run(source, outbox, config, shutdown).await?);
    }

    let iface = args
        .source
        .iface
        .as_deref()
        .context("either --iface or --replay is required")?;
    run_live(iface, outbox, config, shutdown).await
}

#[cfg(target_os = "linux")]
async fn run_live(
    iface: &str,
    outbox: Outbox,
    config: UplinkConfig,
    shutdown: CancellationToken,
) -> Result<Summary> {
    let source = edge_agent::source::SocketCanSource::open(iface)
        .with_context(|| format!("cannot open CAN interface {iface}"))?;
    Ok(pipeline::run(source, outbox, config, shutdown).await?)
}

#[cfg(not(target_os = "linux"))]
async fn run_live(
    _iface: &str,
    _outbox: Outbox,
    _config: UplinkConfig,
    _shutdown: CancellationToken,
) -> Result<Summary> {
    anyhow::bail!("SocketCAN is only available on Linux, use --replay instead")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, clap::Error> {
        let mut full = vec!["edge-agent", "--receiver-token", "t"];
        full.extend_from_slice(args);
        Args::try_parse_from(full)
    }

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        Args::command().debug_assert();
    }

    #[test]
    fn exactly_one_source_is_required() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--iface", "vcan0", "--replay", "x.log"]).is_err());
        assert!(parse(&["--iface", "vcan0", "--realtime"]).is_err());
        assert!(parse(&["--replay", "x.log", "--realtime"]).is_ok());
    }

    #[test]
    fn batch_size_must_be_positive() {
        assert!(parse(&["--iface", "vcan0", "--batch-size", "0"]).is_err());
        assert!(parse(&["--iface", "vcan0", "--batch-size", "100"]).is_ok());
    }
}
