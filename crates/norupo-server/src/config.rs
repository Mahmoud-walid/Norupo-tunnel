//! Edge server configuration.
//!
//! Everything is settable by CLI flag and by environment variable, because the
//! two deployment shapes we care about are "a binary on a VPS" and "a container
//! in an orchestrator", and those disagree about which one is ergonomic.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;

/// Runtime configuration for `norupo-server`.
#[derive(Debug, Clone, Parser)]
#[command(name = "norupo-server", version, about = "Norupo tunnel edge server")]
pub struct ServerConfig {
    /// Address for the gRPC control plane that agents connect to.
    #[arg(long, env = "NORUPO_CONTROL_ADDR", default_value = "0.0.0.0:7000")]
    pub control_addr: SocketAddr,

    /// Address serving public HTTP traffic.
    #[arg(long, env = "NORUPO_HTTP_ADDR", default_value = "0.0.0.0:8080")]
    pub http_addr: SocketAddr,

    /// Internal address siblings use to hand off requests for tunnels this
    /// node owns. Must be reachable from other edge nodes but NOT from the
    /// public internet.
    #[arg(long, env = "NORUPO_PEER_ADDR", default_value = "0.0.0.0:7100")]
    pub peer_addr: SocketAddr,

    /// Address other nodes should dial to reach this node's peer listener,
    /// e.g. `10.0.3.14:7100`. Defaults to `peer_addr`, which is only correct
    /// when that is already a routable address.
    #[arg(long, env = "NORUPO_ADVERTISE_ADDR")]
    pub advertise_addr: Option<String>,

    /// Base domain under which tunnels are published, e.g. `tunnel.com`.
    #[arg(long, env = "NORUPO_BASE_DOMAIN", default_value = "localhost")]
    pub base_domain: String,

    /// Scheme used when building public URLs shown to users. Set to `http`
    /// for local development without TLS termination.
    #[arg(long, env = "NORUPO_PUBLIC_SCHEME", default_value = "https")]
    pub public_scheme: String,

    /// Redis URL backing the shared routing table. Omit to run single-node
    /// with a process-local table.
    #[arg(long, env = "NORUPO_REDIS_URL")]
    pub redis_url: Option<String>,

    /// Key prefix for routing table entries, so several environments can share
    /// one Redis.
    #[arg(long, env = "NORUPO_REDIS_PREFIX", default_value = "norupo:")]
    pub redis_prefix: String,

    /// Stable identity for this node. Set it to the pod or instance name.
    #[arg(long, env = "NORUPO_NODE_ID")]
    pub node_id: Option<String>,

    /// Accept any agent token. Development only — this lets anyone publish
    /// tunnels through your server.
    #[arg(long, env = "NORUPO_ALLOW_ANONYMOUS")]
    pub allow_anonymous: bool,

    /// Path to a JSON file of `{ "token": { ...principal... } }`.
    #[arg(long, env = "NORUPO_TOKENS_FILE")]
    pub tokens_file: Option<std::path::PathBuf>,

    /// Heartbeat interval in milliseconds. Routing-table TTLs are derived from
    /// this, so lowering it speeds up failover at the cost of Redis traffic.
    #[arg(long, env = "NORUPO_HEARTBEAT_MS", default_value_t = 15_000)]
    pub heartbeat_ms: u64,

    /// How long the edge waits for an agent's response head before giving up
    /// and returning 504.
    #[arg(long, env = "NORUPO_RESPONSE_TIMEOUT_MS", default_value_t = 30_000)]
    pub response_timeout_ms: u64,

    /// Per-stream flow-control window in bytes.
    #[arg(long, env = "NORUPO_WINDOW_BYTES", default_value_t = norupo_proto::DEFAULT_WINDOW)]
    pub window_bytes: u32,

    /// Log filter, e.g. `info` or `norupo_server=debug,tower=warn`.
    #[arg(long, env = "NORUPO_LOG", default_value = "info")]
    pub log: String,
}

impl ServerConfig {
    #[must_use]
    pub fn heartbeat(&self) -> Duration {
        Duration::from_millis(self.heartbeat_ms)
    }

    /// TTL written into the routing table.
    ///
    /// Three missed heartbeats before a route is considered dead: tight enough
    /// that failover is measured in seconds, loose enough that one slow Redis
    /// round-trip does not evict a perfectly healthy tunnel.
    #[must_use]
    pub fn route_ttl(&self) -> Duration {
        self.heartbeat() * 3
    }

    #[must_use]
    pub fn response_timeout(&self) -> Duration {
        Duration::from_millis(self.response_timeout_ms)
    }

    /// Address this node advertises to its siblings.
    #[must_use]
    pub fn advertised_peer_addr(&self) -> String {
        self.advertise_addr
            .clone()
            .unwrap_or_else(|| self.peer_addr.to_string())
    }

    /// Builds the URL shown to the user for a tunnel host.
    #[must_use]
    pub fn public_url(&self, host: &str) -> String {
        format!("{}://{}", self.public_scheme, host)
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        // `clap` derives defaults from the attributes above; parsing an empty
        // argv is the single source of truth for them.
        Self::parse_from(["norupo-server"])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_is_three_heartbeats() {
        let cfg = ServerConfig {
            heartbeat_ms: 1000,
            ..Default::default()
        };
        assert_eq!(cfg.route_ttl(), Duration::from_millis(3000));
    }

    #[test]
    fn advertise_addr_falls_back_to_the_peer_listener() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.advertised_peer_addr(), cfg.peer_addr.to_string());

        let cfg = ServerConfig {
            advertise_addr: Some("10.0.0.7:7100".into()),
            ..Default::default()
        };
        assert_eq!(cfg.advertised_peer_addr(), "10.0.0.7:7100");
    }

    #[test]
    fn public_urls_respect_the_configured_scheme() {
        let cfg = ServerConfig {
            public_scheme: "http".into(),
            ..Default::default()
        };
        assert_eq!(cfg.public_url("a.localhost"), "http://a.localhost");
    }

    #[test]
    fn cli_parsing_accepts_the_documented_flags() {
        let cfg = ServerConfig::parse_from([
            "norupo-server",
            "--http-addr",
            "127.0.0.1:9000",
            "--base-domain",
            "tunnel.com",
            "--redis-url",
            "redis://127.0.0.1:6379",
        ]);
        assert_eq!(cfg.http_addr.port(), 9000);
        assert_eq!(cfg.base_domain, "tunnel.com");
        assert_eq!(cfg.redis_url.as_deref(), Some("redis://127.0.0.1:6379"));
    }
}
