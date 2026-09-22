//! CLI surface for the `norupo` agent.

use std::time::Duration;

use clap::{Parser, Subcommand};

/// Expose a local port through a Norupo edge.
#[derive(Debug, Parser)]
#[command(name = "norupo", version, about, long_about = None)]
pub struct Cli {
    /// gRPC endpoint of the edge, e.g. `http://edge.tunnel.com:7000`.
    #[arg(
        long,
        short = 's',
        env = "NORUPO_SERVER",
        default_value = "http://127.0.0.1:7000",
        global = true
    )]
    pub server: String,

    /// Agent token issued by the edge operator.
    #[arg(
        long,
        short = 't',
        env = "NORUPO_TOKEN",
        default_value = "",
        global = true
    )]
    pub token: String,

    /// Log filter, e.g. `info` or `norupo_client=debug`.
    #[arg(long, env = "NORUPO_LOG", default_value = "info", global = true)]
    pub log: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Forward public HTTP traffic to a local port or address.
    Http(HttpArgs),
    /// Check that the configured edge is reachable and speaks our protocol.
    Doctor,
}

#[derive(Debug, Clone, Parser)]
pub struct HttpArgs {
    /// Local port (`3000`) or address (`127.0.0.1:3000`) to forward to.
    #[arg(value_name = "PORT_OR_ADDR")]
    pub target: String,

    /// Request a specific subdomain instead of a randomly assigned one.
    #[arg(long)]
    pub subdomain: Option<String>,

    /// Serve a verified custom domain instead of a subdomain.
    #[arg(long, conflicts_with = "subdomain")]
    pub domain: Option<String>,

    /// Rewrite the `Host` header to the forward target, for local servers that
    /// virtual-host on it.
    #[arg(long)]
    pub rewrite_host: bool,

    /// Give up after this many reconnect attempts. 0 means never give up.
    #[arg(long, default_value_t = 0)]
    pub max_retries: u32,
}

impl HttpArgs {
    /// Resolves the forward target into a dialable `host:port`.
    ///
    /// Accepts a bare port (`3000`), a host:port (`127.0.0.1:3000`), or a URL
    /// (`http://localhost:3000`), because users type all three.
    ///
    /// # Errors
    /// Returns an error when the target is not one of those forms.
    pub fn forward_addr(&self) -> anyhow::Result<String> {
        parse_forward_target(&self.target)
    }
}

/// See [`HttpArgs::forward_addr`].
///
/// # Errors
/// Returns an error for unparseable targets.
pub fn parse_forward_target(raw: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("forward target must not be empty");
    }

    // A bare port is the most common form: `norupo http 3000`.
    if let Ok(port) = trimmed.parse::<u16>() {
        if port == 0 {
            anyhow::bail!("port 0 is not a valid forward target");
        }
        return Ok(format!("127.0.0.1:{port}"));
    }

    // Strip a scheme if the user pasted a URL.
    let without_scheme = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .unwrap_or(trimmed);
    // Drop any path component: we only need the authority.
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);

    let Some((host, port)) = authority.rsplit_once(':') else {
        anyhow::bail!(
            "could not parse `{raw}` as a forward target; use a port (3000) or host:port (127.0.0.1:3000)"
        );
    };
    if host.is_empty() {
        anyhow::bail!("forward target `{raw}` is missing a host");
    }
    port.parse::<u16>()
        .map_err(|_| anyhow::anyhow!("`{port}` in `{raw}` is not a valid port"))?;

    Ok(authority.to_string())
}

/// Backoff schedule for reconnects.
///
/// Exponential with a ceiling: an edge that is briefly restarting should be
/// picked back up in under a second, but a fleet-wide outage must not have
/// every agent hammering it.
#[must_use]
pub fn reconnect_delay(attempt: u32) -> Duration {
    const BASE_MS: u64 = 250;
    const MAX_MS: u64 = 30_000;
    let millis = BASE_MS.saturating_mul(1u64 << attempt.min(8));
    Duration::from_millis(millis.min(MAX_MS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_ports_become_loopback_addresses() {
        assert_eq!(parse_forward_target("3000").unwrap(), "127.0.0.1:3000");
        assert_eq!(parse_forward_target(" 8080 ").unwrap(), "127.0.0.1:8080");
    }

    #[test]
    fn host_port_and_urls_are_both_accepted() {
        assert_eq!(
            parse_forward_target("127.0.0.1:3000").unwrap(),
            "127.0.0.1:3000"
        );
        assert_eq!(
            parse_forward_target("localhost:3000").unwrap(),
            "localhost:3000"
        );
        assert_eq!(
            parse_forward_target("http://localhost:3000").unwrap(),
            "localhost:3000"
        );
        assert_eq!(
            parse_forward_target("http://localhost:3000/ignored").unwrap(),
            "localhost:3000"
        );
    }

    #[test]
    fn nonsense_targets_are_rejected_with_a_useful_message() {
        for bad in [
            "",
            "   ",
            "0",
            "localhost",
            "localhost:notaport",
            ":3000",
            "99999999",
        ] {
            assert!(
                parse_forward_target(bad).is_err(),
                "expected rejection: {bad:?}"
            );
        }
    }

    #[test]
    fn backoff_grows_then_plateaus() {
        assert_eq!(reconnect_delay(0), Duration::from_millis(250));
        assert_eq!(reconnect_delay(1), Duration::from_millis(500));
        assert_eq!(reconnect_delay(2), Duration::from_millis(1000));
        // Capped, and never overflows however long the outage lasts.
        assert_eq!(reconnect_delay(100), Duration::from_millis(30_000));
    }

    #[test]
    fn cli_parses_the_documented_invocation() {
        let cli = Cli::parse_from(["norupo", "http", "3000", "--subdomain", "my-app"]);
        match cli.command {
            Command::Http(args) => {
                assert_eq!(args.forward_addr().unwrap(), "127.0.0.1:3000");
                assert_eq!(args.subdomain.as_deref(), Some("my-app"));
            }
            Command::Doctor => panic!("expected the http subcommand"),
        }
    }

    #[test]
    fn subdomain_and_domain_are_mutually_exclusive() {
        let result = Cli::try_parse_from([
            "norupo",
            "http",
            "3000",
            "--subdomain",
            "a",
            "--domain",
            "b.example.com",
        ]);
        assert!(result.is_err(), "conflicting flags must be rejected");
    }
}
