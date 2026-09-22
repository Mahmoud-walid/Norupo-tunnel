//! Error type shared across Norupo crates.

use std::fmt;

/// Convenience alias used throughout the workspace.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every failure mode the core domain can produce.
///
/// Kept deliberately small: transport-specific failures are mapped into these
/// at the edge of each crate so that callers never have to match on, say, a
/// `redis::RedisError` leaking out of the routing table.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The requested hostname/port is already claimed by another session.
    #[error("routing key `{0}` is already claimed")]
    RouteTaken(String),

    /// The requested subdomain or domain is syntactically invalid.
    #[error("invalid routing target: {0}")]
    InvalidRoute(String),

    /// Authentication failed or the credential was missing.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// The account exceeded a configured limit.
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    /// The routing table backend misbehaved (network, serialization, ...).
    #[error("routing table unavailable: {0}")]
    RegistryUnavailable(String),

    /// Anything genuinely unexpected.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl Error {
    /// Whether the caller can reasonably retry the operation as-is.
    ///
    /// Used by the agent's reconnect loop to distinguish "the edge is having a
    /// bad minute" (retry with backoff) from "your token is wrong" (give up and
    /// tell the human).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::RegistryUnavailable(_) | Error::Internal(_))
    }

    /// Stable machine-readable code, surfaced to clients and metrics labels.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Error::RouteTaken(_) => "route_taken",
            Error::InvalidRoute(_) => "invalid_route",
            Error::Unauthorized(_) => "unauthorized",
            Error::LimitExceeded(_) => "limit_exceeded",
            Error::RegistryUnavailable(_) => "registry_unavailable",
            Error::Internal(_) => "internal",
        }
    }
}

/// Redacts a secret for logging: keeps a short prefix so operators can still
/// correlate, drops everything that would let a log reader authenticate.
#[must_use]
pub fn redact(secret: &str) -> impl fmt::Display + '_ {
    struct Redacted<'a>(&'a str);
    impl fmt::Display for Redacted<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let visible: String = self.0.chars().take(4).collect();
            write!(f, "{visible}***({} chars)", self.0.len())
        }
    }
    Redacted(secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification_matches_reconnect_policy() {
        assert!(Error::RegistryUnavailable("redis down".into()).is_retryable());
        assert!(!Error::Unauthorized("bad token".into()).is_retryable());
        assert!(!Error::RouteTaken("api.tunnel.com".into()).is_retryable());
    }

    #[test]
    fn redact_never_prints_the_whole_secret() {
        let out = redact("supersecrettoken").to_string();
        assert!(!out.contains("supersecrettoken"));
        assert!(out.starts_with("supe"));
    }
}
