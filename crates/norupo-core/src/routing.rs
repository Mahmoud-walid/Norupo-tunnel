//! Routing keys: the canonical identity a public request is matched against.
//!
//! Every tunnel is reachable by exactly one *routing key*. Getting the
//! canonicalisation right is load-bearing: the key produced when a tunnel is
//! registered must be byte-identical to the key derived from an inbound
//! request's `Host` header, or the lookup misses and the user sees a 404 for a
//! tunnel they can see in their terminal.

use std::fmt;

use crate::{Error, Result};

/// Maximum length of a single DNS label (RFC 1035).
const MAX_LABEL_LEN: usize = 63;

/// Subdomains we never hand out, because they either collide with
/// infrastructure or let a user impersonate us to their own visitors.
const RESERVED_SUBDOMAINS: &[&str] = &[
    "www",
    "api",
    "admin",
    "dashboard",
    "app",
    "status",
    "docs",
    "mail",
    "smtp",
    "ns",
    "ns1",
    "ns2",
    "mx",
    "cdn",
    "static",
    "assets",
    "edge",
    "control",
    "grpc",
    "internal",
    "metrics",
    "health",
    "localhost",
    "test",
    "support",
    "billing",
    "account",
    "login",
    "auth",
    "security",
    "norupo",
];

/// Characters used for generated subdomains.
///
/// Deliberately excludes `0/o/1/l` — these get read aloud and typed by hand.
const SUBDOMAIN_ALPHABET: &[u8] = b"abcdefghijkmnpqrstuvwxyz23456789";

/// Length of a generated subdomain. 12 chars of a 32-symbol alphabet is 60
/// bits, so guessing a live tunnel is not a practical attack.
const GENERATED_SUBDOMAIN_LEN: usize = 12;

/// The canonical identity of a tunnel within the routing table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RoutingKey {
    /// An HTTP(S) tunnel addressed by hostname.
    Host(String),
    /// A raw TCP tunnel addressed by the edge's public port.
    TcpPort(u16),
}

impl RoutingKey {
    /// Builds a host key, canonicalising the input.
    ///
    /// # Errors
    /// Returns [`Error::InvalidRoute`] if the host is not a usable DNS name.
    pub fn host(raw: &str) -> Result<Self> {
        Ok(Self::Host(normalize_host(raw)?))
    }

    /// The string form stored in Redis and compared against inbound requests.
    #[must_use]
    pub fn as_storage_key(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for RoutingKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoutingKey::Host(host) => f.write_str(host),
            RoutingKey::TcpPort(port) => write!(f, "tcp:{port}"),
        }
    }
}

/// Canonicalises a `Host` header (or a configured domain) into a lookup key.
///
/// Handles the four ways the same host arrives in practice: with a port
/// (`api.tunnel.com:8080`), with mixed case (`API.Tunnel.Com`), fully
/// qualified with a trailing dot (`api.tunnel.com.`), and IPv6 literal form
/// (`[::1]:8080`).
///
/// # Errors
/// Returns [`Error::InvalidRoute`] when the result is not a valid hostname.
pub fn normalize_host(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidRoute("empty host".into()));
    }

    // Strip the port, being careful not to mangle IPv6 literals where colons
    // are part of the address itself.
    let without_port = if let Some(rest) = trimmed.strip_prefix('[') {
        // `[::1]:8080` -> `[::1]`
        match rest.find(']') {
            Some(end) => &trimmed[..=end + 1],
            None => {
                return Err(Error::InvalidRoute(format!(
                    "unterminated IPv6 host: {raw}"
                )))
            }
        }
    } else {
        match trimmed.split_once(':') {
            Some((host, port)) => {
                if !port.is_empty() && !port.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(Error::InvalidRoute(format!("invalid port in host: {raw}")));
                }
                host
            }
            None => trimmed,
        }
    };

    // A fully-qualified name may carry a trailing root dot; it addresses the
    // same host, so it must produce the same key.
    let without_root = without_port.trim_end_matches('.');
    if without_root.is_empty() {
        return Err(Error::InvalidRoute("empty host".into()));
    }

    let lowered = without_root.to_ascii_lowercase();

    // We route on ASCII only. Unicode domains must arrive already punycoded,
    // which is what every HTTP client does anyway.
    if !lowered.is_ascii() {
        return Err(Error::InvalidRoute(format!(
            "non-ASCII host (punycode it first): {raw}"
        )));
    }
    if lowered.len() > 253 {
        return Err(Error::InvalidRoute("host exceeds 253 characters".into()));
    }

    Ok(lowered)
}

/// Whether a label is on the reserved list.
///
/// Reserved labels are not claimable by default, but an account that has been
/// explicitly granted one (see [`crate::auth::Principal::reserved_subdomains`])
/// may still take it — which is why this is exposed separately from
/// [`validate_subdomain`].
#[must_use]
pub fn is_reserved_subdomain(label: &str) -> bool {
    RESERVED_SUBDOMAINS
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(label))
}

/// Validates a user-requested subdomain label, including the reserved list.
///
/// # Errors
/// Returns [`Error::InvalidRoute`] for anything that is not a safe, reserved-free
/// DNS label.
pub fn validate_subdomain(label: &str) -> Result<String> {
    let lowered = validate_subdomain_syntax(label)?;
    if is_reserved_subdomain(&lowered) {
        return Err(Error::InvalidRoute(format!(
            "subdomain `{lowered}` is reserved"
        )));
    }
    Ok(lowered)
}

/// Validates the DNS-label *syntax* of a subdomain, ignoring the reserved list.
///
/// # Errors
/// Returns [`Error::InvalidRoute`] when the label is not a valid DNS label.
pub fn validate_subdomain_syntax(label: &str) -> Result<String> {
    let lowered = label.trim().to_ascii_lowercase();

    if lowered.is_empty() {
        return Err(Error::InvalidRoute("subdomain must not be empty".into()));
    }
    if lowered.len() > MAX_LABEL_LEN {
        return Err(Error::InvalidRoute(format!(
            "subdomain must be at most {MAX_LABEL_LEN} characters"
        )));
    }
    if !lowered
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(Error::InvalidRoute(
            "subdomain may only contain a-z, 0-9 and hyphen".into(),
        ));
    }
    if lowered.starts_with('-') || lowered.ends_with('-') {
        return Err(Error::InvalidRoute(
            "subdomain must not start or end with a hyphen".into(),
        ));
    }
    // `xn--` is the punycode marker; letting users claim it would let them
    // register a label that renders as someone else's brand.
    if lowered.starts_with("xn--") {
        return Err(Error::InvalidRoute(
            "subdomain must not use the punycode prefix".into(),
        ));
    }

    Ok(lowered)
}

/// Generates a random, pronounceable-ish subdomain that is guaranteed to pass
/// [`validate_subdomain`].
#[must_use]
pub fn random_subdomain() -> String {
    use rand::RngExt;
    let mut rng = rand::rng();
    let label: String = (0..GENERATED_SUBDOMAIN_LEN)
        .map(|_| SUBDOMAIN_ALPHABET[rng.random_range(0..SUBDOMAIN_ALPHABET.len())] as char)
        .collect();

    // The alphabet has no digits-only failure mode and no hyphens, so the only
    // way this can be rejected is a collision with a reserved word, which is
    // impossible at 12 characters. Assert it in debug builds anyway.
    debug_assert!(validate_subdomain(&label).is_ok());
    label
}

/// Joins a subdomain and the edge's base domain into a public hostname.
///
/// Only the label *syntax* is checked here. Whether the caller is allowed to
/// claim a reserved label is an authorization question, answered by the
/// control plane against the caller's [`crate::auth::Principal`].
///
/// # Errors
/// Propagates validation failures from [`validate_subdomain_syntax`] and
/// [`normalize_host`].
pub fn public_host(subdomain: &str, base_domain: &str) -> Result<String> {
    let label = validate_subdomain_syntax(subdomain)?;
    let base = normalize_host(base_domain)?;
    let host = format!("{label}.{base}");
    if host.len() > 253 {
        return Err(Error::InvalidRoute("resulting host is too long".into()));
    }
    Ok(host)
}

/// Extracts the subdomain label from a host, given the edge's base domain.
///
/// Returns `None` when `host` is not a direct child of `base_domain` — used to
/// tell "this is one of our tunnels" apart from "this is a custom domain".
#[must_use]
pub fn subdomain_of(host: &str, base_domain: &str) -> Option<String> {
    let host = normalize_host(host).ok()?;
    let base = normalize_host(base_domain).ok()?;
    let label = host.strip_suffix(&base)?.strip_suffix('.')?;
    // Only direct children: `a.b.tunnel.com` is not the subdomain `a.b`.
    if label.is_empty() || label.contains('.') {
        return None;
    }
    Some(label.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_normalization_collapses_equivalent_forms() {
        // All five of these must produce the same routing key, or a request
        // silently fails to find a tunnel that is demonstrably connected.
        for raw in [
            "api.tunnel.com",
            "API.Tunnel.Com",
            "api.tunnel.com:8080",
            "api.tunnel.com.",
            "  api.tunnel.com  ",
        ] {
            assert_eq!(normalize_host(raw).unwrap(), "api.tunnel.com", "raw={raw}");
        }
    }

    #[test]
    fn ipv6_literals_keep_their_colons() {
        assert_eq!(normalize_host("[::1]:8080").unwrap(), "[::1]");
        assert_eq!(normalize_host("[::1]").unwrap(), "[::1]");
    }

    #[test]
    fn host_normalization_rejects_junk() {
        for raw in ["", "   ", "api.tunnel.com:http", "[::1", "tünnel.com"] {
            assert!(normalize_host(raw).is_err(), "expected rejection: {raw:?}");
        }
        assert!(normalize_host(&format!("{}.com", "a".repeat(260))).is_err());
    }

    #[test]
    fn subdomain_validation_enforces_dns_label_rules() {
        assert_eq!(validate_subdomain("My-App").unwrap(), "my-app");
        assert_eq!(validate_subdomain("a").unwrap(), "a");

        for bad in [
            "",
            "-lead",
            "trail-",
            "has_underscore",
            "has.dot",
            "xn--fancy",
            "api",   // reserved
            "ADMIN", // reserved, case-insensitively
            &"a".repeat(64),
        ] {
            assert!(
                validate_subdomain(bad).is_err(),
                "expected rejection: {bad:?}"
            );
        }
    }

    #[test]
    fn generated_subdomains_are_valid_and_not_repeated() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..512 {
            let label = random_subdomain();
            assert!(
                validate_subdomain(&label).is_ok(),
                "generated invalid: {label}"
            );
            assert!(
                seen.insert(label),
                "generator produced a collision in 512 draws"
            );
        }
    }

    #[test]
    fn public_host_and_subdomain_of_are_inverses() {
        let host = public_host("my-app", "tunnel.com").unwrap();
        assert_eq!(host, "my-app.tunnel.com");
        assert_eq!(subdomain_of(&host, "tunnel.com").as_deref(), Some("my-app"));

        // Syntax is still enforced...
        assert!(public_host("-bad", "tunnel.com").is_err());
        // ...but a reserved label is the control plane's call, not ours.
        assert_eq!(public_host("api", "tunnel.com").unwrap(), "api.tunnel.com");
    }

    #[test]
    fn subdomain_of_rejects_non_children() {
        // A nested host is not a subdomain of the base, and an unrelated host
        // must not be mistaken for one. The `eviltunnel.com` case matters:
        // a naive `ends_with("tunnel.com")` would match it.
        assert_eq!(subdomain_of("a.b.tunnel.com", "tunnel.com"), None);
        assert_eq!(subdomain_of("tunnel.com", "tunnel.com"), None);
        assert_eq!(subdomain_of("eviltunnel.com", "tunnel.com"), None);
    }

    #[test]
    fn reserved_labels_pass_syntax_but_fail_full_validation() {
        // An account that owns `api` must still be able to register it, so the
        // syntax check and the reserved check have to stay separable.
        assert!(validate_subdomain_syntax("api").is_ok());
        assert!(validate_subdomain("api").is_err());
        assert!(is_reserved_subdomain("API"));
        assert!(!is_reserved_subdomain("my-app"));

        // Syntax failures must fail both ways.
        assert!(validate_subdomain_syntax("-nope").is_err());
    }

    #[test]
    fn routing_key_display_is_stable() {
        assert_eq!(
            RoutingKey::host("API.tunnel.com:443").unwrap().to_string(),
            "api.tunnel.com"
        );
        assert_eq!(RoutingKey::TcpPort(41235).to_string(), "tcp:41235");
    }
}
