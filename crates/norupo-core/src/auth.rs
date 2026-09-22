//! Agent authentication.
//!
//! The edge only ever sees a bearer token. Where that token comes from (a
//! static file for self-hosters, a control-plane database for a SaaS
//! deployment) is behind [`AuthProvider`], so swapping one for the other is a
//! config change rather than a fork.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::{Error, Result};

/// Who a validated token belongs to, plus what they are allowed to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// Stable account identifier, used as the metrics/quota dimension.
    pub account_id: String,
    /// Human-facing label for logs and the dashboard.
    pub display_name: String,
    /// Subdomains this principal may claim beyond randomly assigned ones.
    /// Empty means "random subdomains only".
    pub reserved_subdomains: Vec<String>,
    /// Custom domains whose ownership has been verified for this account.
    pub custom_domains: Vec<String>,
    /// Quotas applied to this principal.
    pub limits: Limits,
}

/// Per-account ceilings enforced at the edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_tunnels: u32,
    pub max_concurrent_streams: u32,
    pub max_bytes_per_second: u64,
}

impl Default for Limits {
    fn default() -> Self {
        // Generous defaults suited to a self-hosted deployment, where the
        // operator and the users are the same people.
        Self {
            max_tunnels: 8,
            max_concurrent_streams: 512,
            max_bytes_per_second: 0, // 0 == unmetered
        }
    }
}

impl Principal {
    /// Whether this principal may claim `subdomain`.
    #[must_use]
    pub fn may_claim_subdomain(&self, subdomain: &str) -> bool {
        self.reserved_subdomains
            .iter()
            .any(|s| s.eq_ignore_ascii_case(subdomain))
    }

    /// Whether this principal may serve `domain` as a custom domain.
    ///
    /// Matches exactly, or as a wildcard when the entry starts with `*.`.
    #[must_use]
    pub fn may_use_custom_domain(&self, domain: &str) -> bool {
        self.custom_domains.iter().any(|allowed| {
            if let Some(suffix) = allowed.strip_prefix("*.") {
                // `*.example.com` covers `a.example.com` but not
                // `example.com` itself, and not `evilexample.com`.
                domain
                    .strip_suffix(suffix)
                    .is_some_and(|head| head.ends_with('.') && head.len() > 1)
            } else {
                allowed.eq_ignore_ascii_case(domain)
            }
        })
    }
}

/// Validates agent credentials.
#[async_trait]
pub trait AuthProvider: Send + Sync + 'static {
    /// Resolves a bearer token to a [`Principal`].
    ///
    /// # Errors
    /// Returns [`Error::Unauthorized`] when the token is unknown, revoked or
    /// malformed. Implementations MUST NOT distinguish these cases to the
    /// caller — doing so turns the endpoint into a token oracle.
    async fn authenticate(&self, token: &str) -> Result<Principal>;
}

/// An in-memory token table, loaded from config.
///
/// This is the default for self-hosted single-operator deployments.
#[derive(Debug, Default)]
pub struct StaticTokenAuth {
    tokens: HashMap<String, Principal>,
    /// When true, any token (including an empty one) is accepted as an
    /// anonymous principal. Intended for local development only.
    allow_anonymous: bool,
}

impl StaticTokenAuth {
    #[must_use]
    pub fn new(tokens: HashMap<String, Principal>) -> Self {
        Self {
            tokens,
            allow_anonymous: false,
        }
    }

    /// Accepts every connection as an anonymous principal.
    ///
    /// Never enable this on a publicly reachable edge: it lets anyone on the
    /// internet publish tunnels through your server.
    #[must_use]
    pub fn allow_anonymous() -> Self {
        Self {
            tokens: HashMap::new(),
            allow_anonymous: true,
        }
    }
}

#[async_trait]
impl AuthProvider for StaticTokenAuth {
    async fn authenticate(&self, token: &str) -> Result<Principal> {
        if let Some(principal) = self.tokens.get(token) {
            return Ok(principal.clone());
        }
        if self.allow_anonymous {
            return Ok(Principal {
                account_id: "anonymous".into(),
                display_name: "anonymous".into(),
                reserved_subdomains: Vec::new(),
                custom_domains: Vec::new(),
                limits: Limits::default(),
            });
        }
        Err(Error::Unauthorized("unknown or revoked token".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal() -> Principal {
        Principal {
            account_id: "acct_1".into(),
            display_name: "Ada".into(),
            reserved_subdomains: vec!["ada".into()],
            custom_domains: vec!["hooks.example.com".into(), "*.dev.example.com".into()],
            limits: Limits::default(),
        }
    }

    #[tokio::test]
    async fn static_auth_accepts_known_tokens_and_rejects_others() {
        let auth = StaticTokenAuth::new(HashMap::from([("tok_good".to_string(), principal())]));
        assert_eq!(
            auth.authenticate("tok_good").await.unwrap().account_id,
            "acct_1"
        );

        let err = auth.authenticate("tok_bad").await.unwrap_err();
        assert_eq!(err.code(), "unauthorized");
        assert!(
            !err.is_retryable(),
            "a bad token must not trigger reconnect loops"
        );
    }

    #[tokio::test]
    async fn anonymous_mode_accepts_anything() {
        let auth = StaticTokenAuth::allow_anonymous();
        assert_eq!(auth.authenticate("").await.unwrap().account_id, "anonymous");
    }

    #[test]
    fn reserved_subdomain_matching_is_case_insensitive() {
        assert!(principal().may_claim_subdomain("ADA"));
        assert!(!principal().may_claim_subdomain("grace"));
    }

    #[test]
    fn wildcard_domains_do_not_leak_to_siblings() {
        let p = principal();
        assert!(p.may_use_custom_domain("hooks.example.com"));
        assert!(p.may_use_custom_domain("a.dev.example.com"));
        // The bare wildcard parent is not covered...
        assert!(!p.may_use_custom_domain("dev.example.com"));
        // ...and neither is a host that merely ends with the same text.
        assert!(!p.may_use_custom_domain("evildev.example.com"));
        assert!(!p.may_use_custom_domain("other.example.com"));
    }
}
