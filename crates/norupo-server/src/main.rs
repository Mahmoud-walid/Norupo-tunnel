//! `norupo-server` entry point.

use clap::Parser;
use norupo_server::{start, ServerConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ServerConfig::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log).unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(true)
        .init();

    let server = start(config).await?;

    // Wait for Ctrl-C (works on Windows, macOS and Linux alike), then drain.
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received; draining");
    server.shutdown().await;

    Ok(())
}
