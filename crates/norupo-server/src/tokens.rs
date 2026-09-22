//! Loading static agent credentials from a JSON file.
//!
//! Format:
//!
//! ```json
//! {
//!   "tok_live_abc123": {
//!     "account_id": "acct_ada",
//!     "display_name": "Ada",
//!     "reserved_subdomains": ["ada", "api"],
//!     "custom_domains": ["*.dev.example.com"],
//!     "limits": { "max_tunnels": 4, "max_concurrent_streams": 256, "max_bytes_per_second": 0 }
//!   }
//! }
//! ```
//!
//! Everything but `account_id` is optional.

use std::collections::HashMap;
use std::path::Path;

use norupo_core::auth::{Limits, Principal};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct LimitsFile {
    #[serde(default)]
    max_tunnels: Option<u32>,
    #[serde(default)]
    max_concurrent_streams: Option<u32>,
    #[serde(default)]
    max_bytes_per_second: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PrincipalFile {
    account_id: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    reserved_subdomains: Vec<String>,
    #[serde(default)]
    custom_domains: Vec<String>,
    #[serde(default)]
    limits: Option<LimitsFile>,
}

impl From<PrincipalFile> for Principal {
    fn from(file: PrincipalFile) -> Self {
        let defaults = Limits::default();
        let limits = file.limits.map_or(defaults, |l| Limits {
            max_tunnels: l.max_tunnels.unwrap_or(defaults.max_tunnels),
            max_concurrent_streams: l
                .max_concurrent_streams
                .unwrap_or(defaults.max_concurrent_streams),
            max_bytes_per_second: l
                .max_bytes_per_second
                .unwrap_or(defaults.max_bytes_per_second),
        });
        Principal {
            display_name: file.display_name.unwrap_or_else(|| file.account_id.clone()),
            account_id: file.account_id,
            reserved_subdomains: file.reserved_subdomains,
            custom_domains: file.custom_domains,
            limits,
        }
    }
}

/// Reads and parses a token file.
///
/// # Errors
/// Fails if the file is unreadable or not valid JSON in the documented shape.
pub fn load(path: &Path) -> anyhow::Result<HashMap<String, Principal>> {
    let raw = std::fs::read_to_string(path)?;
    parse(&raw)
}

/// Parses the token file contents. Split out so it is testable without I/O.
///
/// # Errors
/// Fails on malformed JSON.
pub fn parse(raw: &str) -> anyhow::Result<HashMap<String, Principal>> {
    let parsed: HashMap<String, PrincipalFile> = serde_json::from_str(raw)?;
    Ok(parsed
        .into_iter()
        .map(|(token, p)| (token, p.into()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_entries_get_sensible_defaults() {
        let parsed = parse(r#"{"tok_a": {"account_id": "acct_a"}}"#).unwrap();
        let principal = &parsed["tok_a"];

        assert_eq!(principal.account_id, "acct_a");
        assert_eq!(principal.display_name, "acct_a");
        assert_eq!(principal.limits, Limits::default());
        assert!(principal.reserved_subdomains.is_empty());
    }

    #[test]
    fn full_entries_are_parsed_and_partial_limits_fall_back() {
        let parsed = parse(
            r#"{
              "tok_b": {
                "account_id": "acct_b",
                "display_name": "Ada",
                "reserved_subdomains": ["ada"],
                "custom_domains": ["*.dev.example.com"],
                "limits": { "max_tunnels": 64 }
              }
            }"#,
        )
        .unwrap();
        let principal = &parsed["tok_b"];

        assert_eq!(principal.display_name, "Ada");
        assert_eq!(principal.reserved_subdomains, vec!["ada".to_string()]);
        assert_eq!(principal.limits.max_tunnels, 64);
        // Unspecified limits keep their defaults rather than becoming zero.
        assert_eq!(
            principal.limits.max_concurrent_streams,
            Limits::default().max_concurrent_streams
        );
    }

    #[test]
    fn malformed_files_are_rejected_loudly() {
        assert!(parse("not json").is_err());
        // Missing the one required field.
        assert!(parse(r#"{"tok_c": {"display_name": "nope"}}"#).is_err());
    }
}
