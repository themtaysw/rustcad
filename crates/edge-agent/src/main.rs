use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use edge_agent::config::Config;
use edge_agent::outbox::Outbox;
use edge_agent::pipeline::{self, PipelineConfig, Summary};
use edge_agent::source::ReplaySource;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Reads J1939 frames, aggregates them, stores them in a SQLite outbox and uploads them.
///
/// Settings come from the config file and `EDGE_*` environment variables, which win.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// TOML config file. Without it, built-in defaults plus `EDGE_*` variables are used.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Replay a candump log instead of reading the CAN interface from the config.
    #[arg(long)]
    replay: Option<PathBuf>,

    /// With --replay: keep the original timing between frames instead of reading at full speed.
    #[arg(long, requires = "replay")]
    realtime: bool,
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
    let config = Config::load(args.config.as_deref(), |name| std::env::var(name).ok())?;
    info!(?config, "starting");

    let shutdown = CancellationToken::new();
    tokio::spawn(stop_on_signal(shutdown.clone()));

    let summary = run(&args, &config, shutdown).await?;
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

/// The first SIGTERM or SIGINT stops reading and lets the pipeline drain and upload.
/// A second one exits at once.
async fn stop_on_signal(shutdown: CancellationToken) {
    let signal = wait_for_signal().await;
    info!(
        signal,
        "shutting down, send the signal again to exit immediately"
    );
    shutdown.cancel();

    let signal = wait_for_signal().await;
    warn!(signal, "exiting without final upload");
    std::process::exit(130);
}

#[cfg(unix)]
async fn wait_for_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("cannot listen for SIGTERM");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => "SIGINT",
        _ = terminate.recv() => "SIGTERM",
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "Ctrl-C"
}

async fn run(args: &Args, config: &Config, shutdown: CancellationToken) -> Result<Summary> {
    let outbox = Outbox::open(&config.outbox_path, config.outbox_cap)
        .with_context(|| format!("cannot open outbox {}", config.outbox_path.display()))?;
    let pipeline_config = PipelineConfig {
        uplink: config.uplink(),
        health_interval: config.health_interval(),
    };

    if let Some(path) = &args.replay {
        let file = File::open(path)
            .await
            .with_context(|| format!("cannot open replay file {}", path.display()))?;
        let source = ReplaySource::new(BufReader::new(file), args.realtime);
        return Ok(pipeline::run(source, outbox, pipeline_config, shutdown).await?);
    }
    run_live(&config.iface, outbox, pipeline_config, shutdown).await
}

#[cfg(target_os = "linux")]
async fn run_live(
    iface: &str,
    outbox: Outbox,
    config: PipelineConfig,
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
    _config: PipelineConfig,
    _shutdown: CancellationToken,
) -> Result<Summary> {
    anyhow::bail!("SocketCAN is only available on Linux, use --replay instead")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        Args::command().debug_assert();
    }

    #[test]
    fn realtime_needs_replay() {
        assert!(Args::try_parse_from(["edge-agent"]).is_ok());
        assert!(Args::try_parse_from(["edge-agent", "--realtime"]).is_err());
        assert!(Args::try_parse_from(["edge-agent", "--replay", "x.log", "--realtime"]).is_ok());
        assert!(
            Args::try_parse_from(["edge-agent", "--config", "/etc/edge-agent/config.toml"]).is_ok()
        );
    }
}
