//! Opaque identifiers.
//!
//! Norupo uses prefixed, URL-safe random ids rather than UUIDs so that an id
//! pasted into a bug report immediately says what it is.

use rand::RngExt;

const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const RANDOM_LEN: usize = 16;

fn random_suffix() -> String {
    let mut rng = rand::rng();
    (0..RANDOM_LEN)
        .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
        .collect()
}

/// `sess_<16>` — one per agent connection.
#[must_use]
pub fn session_id() -> String {
    format!("sess_{}", random_suffix())
}

/// `tun_<16>` — one per registered tunnel, stable for the tunnel's lifetime.
#[must_use]
pub fn tunnel_id() -> String {
    format!("tun_{}", random_suffix())
}

/// `node_<16>` — identity of an edge server process.
///
/// In production you override this with the pod/instance name so that the
/// routing table's `node_id` is greppable against your orchestrator.
#[must_use]
pub fn node_id() -> String {
    format!("node_{}", random_suffix())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_prefixed_and_unique() {
        assert!(session_id().starts_with("sess_"));
        assert!(tunnel_id().starts_with("tun_"));
        assert!(node_id().starts_with("node_"));

        let a: std::collections::HashSet<_> = (0..1000).map(|_| session_id()).collect();
        assert_eq!(a.len(), 1000, "session ids collided");
    }

    #[test]
    fn ids_are_url_safe() {
        let id = tunnel_id();
        assert!(id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
    }
}
