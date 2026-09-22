//! The Norupo edge server.
//!
//! A single process runs three listeners:
//!
//! | Listener | Audience | Purpose |
//! |----------|----------|---------|
//! | control  | agents   | gRPC `TunnelControl.Session` |
//! | http     | internet | public traffic, routed by `Host` |
//! | peer     | siblings | cross-node hand-off, `/__norupo/*` |
//!
//! [`start`] wires them together and hands back a [`RunningServer`] that knows
//! the addresses it actually bound — which is what makes integration tests able
//! to use port 0 and avoid flaky fixed-port collisions.

pub mod config;
pub mod control;
pub mod http;
pub mod ingress;
pub mod peer;
pub mod session;
pub mod state;
pub mod tokens;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use norupo_core::auth::{AuthProvider, StaticTokenAuth};
use norupo_core::ids;
use norupo_core::registry::{MemoryRegistry, SharedRegistry};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::info;

pub use config::ServerConfig;
pub use state::AppState;

/// A started edge server.
pub struct RunningServer {
    /// Address the public HTTP listener actually bound.
    pub http_addr: SocketAddr,
    /// Address the gRPC control plane actually bound.
    pub control_addr: SocketAddr,
    /// Address the internal peer listener actually bound.
    pub peer_addr: SocketAddr,
    /// Shared state, exposed so tests and admin surfaces can inspect it.
    pub state: state::Shared,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl RunningServer {
    /// Signals every listener to stop and waits for them.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        for task in self.tasks {
            // A listener that already exited is not an error.
            let _ = task.await;
        }
    }
}

/// Builds the routing table backend named by the config.
async fn build_registry(config: &ServerConfig) -> anyhow::Result<SharedRegistry> {
    match config.redis_url.as_deref() {
        #[cfg(feature = "redis-registry")]
        Some(url) => {
            let registry =
                norupo_core::registry::RedisRegistry::connect(url, config.redis_prefix.clone())
                    .await
                    .with_context(|| format!("connecting to redis at {url}"))?;
            info!("routing table: redis ({url})");
            Ok(Arc::new(registry))
        }
        #[cfg(not(feature = "redis-registry"))]
        Some(_) => anyhow::bail!(
            "this build has the `redis-registry` feature disabled; rebuild with it to use --redis-url"
        ),
        None => {
            info!("routing table: in-memory (single node only)");
            Ok(Arc::new(MemoryRegistry::new()))
        }
    }
}

/// Builds the auth provider named by the config.
fn build_auth(config: &ServerConfig) -> anyhow::Result<Arc<dyn AuthProvider>> {
    if let Some(path) = &config.tokens_file {
        let principals = tokens::load(path)
            .with_context(|| format!("loading tokens from {}", path.display()))?;
        info!(tokens = principals.len(), "auth: static token file");
        return Ok(Arc::new(StaticTokenAuth::new(principals)));
    }
    if config.allow_anonymous {
        tracing::warn!(
            "auth: ANONYMOUS — every connection is accepted. Never do this on a public edge."
        );
        return Ok(Arc::new(StaticTokenAuth::allow_anonymous()));
    }
    anyhow::bail!("no authentication configured: pass --tokens-file, or --allow-anonymous for local development")
}

/// Starts every listener. Returns once all three are bound.
///
/// # Errors
/// Fails if a listener cannot bind, Redis is unreachable, or auth is
/// misconfigured.
pub async fn start(config: ServerConfig) -> anyhow::Result<RunningServer> {
    let registry = build_registry(&config).await?;
    start_with_registry(config, registry).await
}

/// Starts the server against a routing table the caller already built.
///
/// This is how you run several edge nodes in one process — which is exactly
/// what the cross-node tests do, and what an embedded deployment would do.
///
/// # Errors
/// Fails if a listener cannot bind or auth is misconfigured.
pub async fn start_with_registry(
    config: ServerConfig,
    registry: SharedRegistry,
) -> anyhow::Result<RunningServer> {
    let node_id = config.node_id.clone().unwrap_or_else(ids::node_id);
    let auth = build_auth(&config)?;

    // Bind everything before spawning anything: a half-started server that
    // serves control but not HTTP is worse than a clean startup failure.
    let http_listener = TcpListener::bind(config.http_addr)
        .await
        .with_context(|| format!("binding public HTTP listener on {}", config.http_addr))?;
    let peer_listener = TcpListener::bind(config.peer_addr)
        .await
        .with_context(|| format!("binding peer listener on {}", config.peer_addr))?;
    let control_listener = TcpListener::bind(config.control_addr)
        .await
        .with_context(|| format!("binding gRPC control listener on {}", config.control_addr))?;

    let http_addr = http_listener.local_addr()?;
    let peer_addr = peer_listener.local_addr()?;
    let control_addr = control_listener.local_addr()?;

    // When the operator did not pin an advertise address, derive it from the
    // port we actually bound, so port 0 still yields something dialable.
    let node_addr = config
        .advertise_addr
        .clone()
        .unwrap_or_else(|| peer_addr.to_string());

    let state: state::Shared = Arc::new(AppState {
        config: config.clone(),
        node_id: node_id.clone(),
        node_addr,
        registry,
        auth,
        sessions: Arc::new(session::SessionManager::new()),
    });

    let peers = Arc::new(peer::PeerClient::new(node_id.clone()));
    let (shutdown, shutdown_rx) = watch::channel(false);
    let mut tasks = Vec::with_capacity(3);

    // Public ingress.
    tasks.push(tokio::spawn({
        let state = Arc::clone(&state);
        let peers = Arc::clone(&peers);
        let rx = shutdown_rx.clone();
        async move {
            if let Err(e) =
                ingress::serve(state, http_listener, ingress::Role::Public, peers, rx).await
            {
                tracing::error!(error = %e, "public ingress stopped");
            }
        }
    }));

    // Internal peer ingress.
    tasks.push(tokio::spawn({
        let state = Arc::clone(&state);
        let peers = Arc::clone(&peers);
        let rx = shutdown_rx.clone();
        async move {
            if let Err(e) =
                ingress::serve(state, peer_listener, ingress::Role::Peer, peers, rx).await
            {
                tracing::error!(error = %e, "peer ingress stopped");
            }
        }
    }));

    // gRPC control plane.
    tasks.push(tokio::spawn({
        let state = Arc::clone(&state);
        let mut rx = shutdown_rx.clone();
        async move {
            let service = control::ControlService::new(state).into_server();
            let result = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(control_listener),
                    async move {
                        // Resolve when the shutdown flag flips to true.
                        while rx.changed().await.is_ok() {
                            if *rx.borrow() {
                                return;
                            }
                        }
                    },
                )
                .await;
            if let Err(e) = result {
                tracing::error!(error = %e, "control plane stopped");
            }
        }
    }));

    info!(
        %node_id,
        %http_addr,
        %control_addr,
        %peer_addr,
        base_domain = %config.base_domain,
        "norupo edge ready"
    );

    Ok(RunningServer {
        http_addr,
        control_addr,
        peer_addr,
        state,
        shutdown,
        tasks,
    })
}
