use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use edge_agent::pipeline::{self, Summary};
use edge_agent::source::ReplaySource;
use tokio::fs::File;
use tokio::io::BufReader;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Reads J1939 frames from a CAN interface or a candump log and decodes them.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    #[command(flatten)]
    source: SourceArgs,

    /// With --replay: keep the original timing between frames instead of reading at full speed.
    #[arg(long, conflicts_with = "iface")]
    realtime: bool,
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    info!(?args, "starting");

    let summary = tokio::select! {
        summary = run(&args) => summary?,
        _ = tokio::signal::ctrl_c() => {
            info!("interrupted");
            return Ok(());
        }
    };

    info!(
        frames = summary.frames_read,
        decoded = summary.stats.decoded,
        unknown_pgn = summary.stats.unknown_pgn,
        invalid_length = summary.stats.invalid_length,
        "source finished"
    );
    info!("last values: {}", pipeline::format_latest(&summary.latest));
    Ok(())
}

async fn run(args: &Args) -> Result<Summary> {
    if let Some(path) = &args.source.replay {
        let file = File::open(path)
            .await
            .with_context(|| format!("cannot open replay file {}", path.display()))?;
        let source = ReplaySource::new(BufReader::new(file), args.realtime);
        return Ok(pipeline::run(source).await?);
    }

    let iface = args
        .source
        .iface
        .as_deref()
        .context("either --iface or --replay is required")?;
    run_live(iface).await
}

#[cfg(target_os = "linux")]
async fn run_live(iface: &str) -> Result<Summary> {
    let source = edge_agent::source::SocketCanSource::open(iface)
        .with_context(|| format!("cannot open CAN interface {iface}"))?;
    Ok(pipeline::run(source).await?)
}

#[cfg(not(target_os = "linux"))]
async fn run_live(_iface: &str) -> Result<Summary> {
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
    fn exactly_one_source_is_required() {
        assert!(Args::try_parse_from(["edge-agent"]).is_err());
        assert!(
            Args::try_parse_from(["edge-agent", "--iface", "vcan0", "--replay", "x.log"]).is_err()
        );
        assert!(Args::try_parse_from(["edge-agent", "--iface", "vcan0", "--realtime"]).is_err());
        assert!(Args::try_parse_from(["edge-agent", "--replay", "x.log", "--realtime"]).is_ok());
    }
}
