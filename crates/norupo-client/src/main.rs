//! `norupo` — the tunnel agent CLI.

use clap::Parser;
use norupo_client::agent::{self, AgentEvent, AgentOptions};
use norupo_client::config::{Cli, Command};
use norupo_proto::tunnel_control_client::TunnelControlClient;
use norupo_proto::HealthRequest;
use tokio::sync::{mpsc, watch};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&cli.log).unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    match cli.command {
        Command::Doctor => doctor(&cli.server).await,
        Command::Http(args) => {
            let options = AgentOptions {
                server: cli.server.clone(),
                token: cli.token.clone(),
                forward_addr: args.forward_addr()?,
                subdomain: args.subdomain.clone(),
                domain: args.domain.clone(),
                rewrite_host: args.rewrite_host,
                max_retries: args.max_retries,
            };
            serve(options).await
        }
    }
}

/// Reports whether the configured edge is reachable and compatible.
async fn doctor(server: &str) -> anyhow::Result<()> {
    println!("checking {server} ...");
    let mut client = TunnelControlClient::connect(server.to_string()).await?;
    let health = client.health(HealthRequest {}).await?.into_inner();

    println!("  reachable      yes");
    println!("  node           {}", health.node_id);
    println!("  server version {}", health.server_version);
    println!("  sessions       {}", health.active_sessions);
    println!("  tunnels        {}", health.active_tunnels);
    println!("  agent protocol v{}", norupo_proto::PROTOCOL_VERSION);
    Ok(())
}

/// Runs the agent until Ctrl-C.
async fn serve(options: AgentOptions) -> anyhow::Result<()> {
    let forward = options.forward_addr.clone();
    let (events_tx, mut events_rx) = mpsc::channel(64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Render events to stdout. Users watch this pane, so it is deliberately
    // plainer and louder than the structured log output.
    tokio::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            match event {
                AgentEvent::Connected {
                    session_id,
                    node_id,
                } => {
                    println!("connected  session={session_id} node={node_id}");
                }
                AgentEvent::TunnelReady(status) => {
                    println!();
                    println!("  {}  ->  {forward}", status.public_url);
                    println!();
                    println!("  press Ctrl-C to stop");
                }
                AgentEvent::TunnelRejected(reason) => {
                    eprintln!("tunnel rejected: {reason}");
                }
                AgentEvent::Disconnected(reason) => {
                    eprintln!("disconnected: {reason} (reconnecting)");
                }
            }
        }
    });

    let agent = tokio::spawn(agent::run(options, events_tx, shutdown_rx));

    tokio::select! {
        result = agent => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            println!("\nshutting down");
            let _ = shutdown_tx.send(true);
            Ok(())
        }
    }
}
