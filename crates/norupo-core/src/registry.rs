//! The routing table: which edge node currently owns a given routing key.
//!
//! # Why this is a trait
//!
//! A single-node deployment wants a `DashMap` and zero operational surface. A
//! thousand-node deployment needs a shared, TTL'd, atomically-claimed table.
//! Both satisfy [`Registry`], so the ingress path is written exactly once.
//!
//! # The three properties that matter
//!
//! 1. **Claims are atomic.** Two agents racing for `api.tunnel.com` against two
//!    different edge nodes must produce exactly one winner. Redis `SET NX`
//!    gives us that; a read-then-write would not.
//! 2. **Releases are owner-checked.** A node that was network-partitioned,
//!    then came back, must not delete a claim that a *different* session has
//!    since taken over. Every mutation is a compare-and-swap on `session_id`.
//! 3. **Entries expire.** If a node is SIGKILLed it cannot run cleanup, so
//!    every record carries a TTL that the owning node renews on a heartbeat.
//!    A dead node's routes free themselves within one TTL.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::{Error, Result};

/// One row of the routing table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRecord {
    /// Canonical key, from [`crate::routing::RoutingKey`].
    pub routing_key: String,
    /// Edge node that currently terminates the agent session.
    pub node_id: String,
    /// Address a sibling edge node dials to hand a request over, e.g.
    /// `10.0.3.14:7100`. This is an internal address; it is never exposed.
    pub node_addr: String,
    /// Agent session that owns the claim. This is the CAS token.
    pub session_id: String,
    /// Tunnel within that session.
    pub tunnel_id: String,
    /// Owning account, carried so that ingress can attribute traffic without a
    /// second lookup.
    pub account_id: String,
}

impl RouteRecord {
    /// Serialises to the compact form stored in Redis.
    ///
    /// A tab-separated record rather than JSON: it is a hot path, the schema is
    /// fixed, and none of these fields may contain a tab (ids are alphanumeric,
    /// hosts are DNS names, addresses are host:port).
    #[must_use]
    pub fn encode(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            self.routing_key,
            self.node_id,
            self.node_addr,
            self.session_id,
            self.tunnel_id,
            self.account_id
        )
    }

    /// Parses the form written by [`RouteRecord::encode`].
    ///
    /// # Errors
    /// Returns [`Error::RegistryUnavailable`] on a malformed record, which is
    /// treated as a transient backend fault rather than a routing decision.
    pub fn decode(raw: &str) -> Result<Self> {
        let mut parts = raw.split('\t');
        let mut next = |field: &str| -> Result<String> {
            parts.next().map(str::to_string).ok_or_else(|| {
                Error::RegistryUnavailable(format!("record missing field `{field}`"))
            })
        };
        Ok(Self {
            routing_key: next("routing_key")?,
            node_id: next("node_id")?,
            node_addr: next("node_addr")?,
            session_id: next("session_id")?,
            tunnel_id: next("tunnel_id")?,
            account_id: next("account_id")?,
        })
    }
}

/// Outcome of a claim attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// The caller now owns the routing key.
    Acquired,
    /// Someone else owns it; the current owner is returned so the edge can
    /// produce a useful error (or, for reconnects, recognise itself).
    Taken(Box<RouteRecord>),
}

/// The shared routing table.
#[async_trait]
pub trait Registry: Send + Sync + 'static {
    /// Atomically claims `record.routing_key` for `record.session_id`.
    ///
    /// Re-claiming a key you already own succeeds and refreshes the TTL; this
    /// is what makes agent reconnects idempotent.
    ///
    /// # Errors
    /// Returns [`Error::RegistryUnavailable`] if the backend is unreachable.
    async fn claim(&self, record: &RouteRecord, ttl: Duration) -> Result<Claim>;

    /// Resolves a routing key to its current owner.
    ///
    /// # Errors
    /// Returns [`Error::RegistryUnavailable`] if the backend is unreachable.
    async fn lookup(&self, routing_key: &str) -> Result<Option<RouteRecord>>;

    /// Extends the TTL, but only if `session_id` still owns the key.
    ///
    /// Returns `false` when ownership was lost, which the caller must treat as
    /// "my tunnel is gone, tear the session down" rather than retrying.
    ///
    /// # Errors
    /// Returns [`Error::RegistryUnavailable`] if the backend is unreachable.
    async fn renew(&self, routing_key: &str, session_id: &str, ttl: Duration) -> Result<bool>;

    /// Releases a key, but only if `session_id` still owns it.
    ///
    /// # Errors
    /// Returns [`Error::RegistryUnavailable`] if the backend is unreachable.
    async fn release(&self, routing_key: &str, session_id: &str) -> Result<()>;
}

/// Convenience alias for the shared handle passed around the server.
pub type SharedRegistry = Arc<dyn Registry>;

// ---------------------------------------------------------------------------
// In-memory implementation
// ---------------------------------------------------------------------------

/// Process-local routing table.
///
/// Correct and fast for a single-node deployment; useless the moment you run
/// two replicas, since each has its own view. Also used as the test double for
/// everything above the [`Registry`] boundary.
#[derive(Debug, Default)]
pub struct MemoryRegistry {
    inner: std::sync::Mutex<std::collections::HashMap<String, (RouteRecord, std::time::Instant)>>,
}

impl MemoryRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Drops expired entries. Called on every access, which is cheap enough at
    /// the scale a single node serves.
    fn evict_expired(
        map: &mut std::collections::HashMap<String, (RouteRecord, std::time::Instant)>,
    ) {
        let now = std::time::Instant::now();
        map.retain(|_, (_, expires_at)| *expires_at > now);
    }

    fn lock(
        &self,
    ) -> std::sync::MutexGuard<
        '_,
        std::collections::HashMap<String, (RouteRecord, std::time::Instant)>,
    > {
        // A poisoned lock means another thread panicked mid-update. The map is
        // a cache of claims with TTLs, so recovering the guard is strictly
        // better than propagating a panic into every future request.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[async_trait]
impl Registry for MemoryRegistry {
    async fn claim(&self, record: &RouteRecord, ttl: Duration) -> Result<Claim> {
        let mut map = self.lock();
        Self::evict_expired(&mut map);

        match map.get(&record.routing_key) {
            // Someone else holds it.
            Some((existing, _)) if existing.session_id != record.session_id => {
                Ok(Claim::Taken(Box::new(existing.clone())))
            }
            // Either free, or already ours: (re)claim and refresh the TTL.
            _ => {
                map.insert(
                    record.routing_key.clone(),
                    (record.clone(), std::time::Instant::now() + ttl),
                );
                Ok(Claim::Acquired)
            }
        }
    }

    async fn lookup(&self, routing_key: &str) -> Result<Option<RouteRecord>> {
        let mut map = self.lock();
        Self::evict_expired(&mut map);
        Ok(map.get(routing_key).map(|(record, _)| record.clone()))
    }

    async fn renew(&self, routing_key: &str, session_id: &str, ttl: Duration) -> Result<bool> {
        let mut map = self.lock();
        Self::evict_expired(&mut map);
        match map.get_mut(routing_key) {
            Some((record, expires_at)) if record.session_id == session_id => {
                *expires_at = std::time::Instant::now() + ttl;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn release(&self, routing_key: &str, session_id: &str) -> Result<()> {
        let mut map = self.lock();
        if map
            .get(routing_key)
            .is_some_and(|(record, _)| record.session_id == session_id)
        {
            map.remove(routing_key);
        }
        Ok(())
    }
}

#[cfg(feature = "redis-registry")]
pub use self::redis_registry::RedisRegistry;

#[cfg(feature = "redis-registry")]
mod redis_registry {
    use super::{Claim, Registry, Result, RouteRecord};
    use crate::Error;
    use async_trait::async_trait;
    use redis::aio::ConnectionManager;
    use std::time::Duration;

    /// Claim script: `SET key value NX PX ttl`, but also succeeding when the
    /// existing value already belongs to us.
    ///
    /// Doing this in Lua rather than as `SET NX` + fallback round-trip keeps
    /// the whole decision atomic, so a reconnecting agent can never lose its
    /// own key to itself.
    ///
    /// KEYS[1] = routing key, ARGV[1] = encoded record, ARGV[2] = ttl millis,
    /// ARGV[3] = session id. Returns the empty string on success, or the
    /// current owner's encoded record on failure.
    const CLAIM_SCRIPT: &str = r"
        local current = redis.call('GET', KEYS[1])
        if current then
          local owner = string.match(current, '^[^\t]*\t[^\t]*\t[^\t]*\t([^\t]*)')
          if owner ~= ARGV[3] then
            return current
          end
        end
        redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]))
        return ''
    ";

    /// Renew script: extend the TTL iff we are still the owner.
    ///
    /// KEYS[1] = routing key, ARGV[1] = ttl millis, ARGV[2] = session id.
    /// Returns 1 when renewed, 0 when ownership was lost.
    const RENEW_SCRIPT: &str = r"
        local current = redis.call('GET', KEYS[1])
        if not current then return 0 end
        local owner = string.match(current, '^[^\t]*\t[^\t]*\t[^\t]*\t([^\t]*)')
        if owner ~= ARGV[2] then return 0 end
        redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[1]))
        return 1
    ";

    /// Release script: delete iff we are still the owner.
    ///
    /// Without the ownership check, a node recovering from a partition would
    /// happily delete the claim that a *newer* session has taken over,
    /// blackholing a live tunnel.
    const RELEASE_SCRIPT: &str = r"
        local current = redis.call('GET', KEYS[1])
        if not current then return 0 end
        local owner = string.match(current, '^[^\t]*\t[^\t]*\t[^\t]*\t([^\t]*)')
        if owner ~= ARGV[1] then return 0 end
        redis.call('DEL', KEYS[1])
        return 1
    ";

    /// Redis-backed routing table, suitable for horizontal scale-out.
    ///
    /// Every edge node points at the same Redis (or Redis Cluster — all
    /// operations are single-key, so they shard cleanly).
    #[derive(Clone)]
    pub struct RedisRegistry {
        conn: ConnectionManager,
        prefix: String,
    }

    impl std::fmt::Debug for RedisRegistry {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RedisRegistry")
                .field("prefix", &self.prefix)
                .finish_non_exhaustive()
        }
    }

    impl RedisRegistry {
        /// Connects to `url` (e.g. `redis://127.0.0.1:6379`).
        ///
        /// Uses a `ConnectionManager`, which reconnects transparently — a Redis
        /// failover must not require restarting every edge node.
        ///
        /// # Errors
        /// Returns [`Error::RegistryUnavailable`] if the initial connection
        /// cannot be established.
        pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self> {
            let client = redis::Client::open(url)
                .map_err(|e| Error::RegistryUnavailable(format!("bad redis url: {e}")))?;
            let conn = ConnectionManager::new(client)
                .await
                .map_err(|e| Error::RegistryUnavailable(format!("redis connect failed: {e}")))?;
            Ok(Self {
                conn,
                prefix: prefix.into(),
            })
        }

        fn key(&self, routing_key: &str) -> String {
            format!("{}route:{}", self.prefix, routing_key)
        }
    }

    fn unavailable(op: &str, e: redis::RedisError) -> Error {
        Error::RegistryUnavailable(format!("redis {op} failed: {e}"))
    }

    #[async_trait]
    impl Registry for RedisRegistry {
        async fn claim(&self, record: &RouteRecord, ttl: Duration) -> Result<Claim> {
            let mut conn = self.conn.clone();
            let current: String = redis::Script::new(CLAIM_SCRIPT)
                .key(self.key(&record.routing_key))
                .arg(record.encode())
                .arg(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
                .arg(&record.session_id)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| unavailable("claim", e))?;

            if current.is_empty() {
                Ok(Claim::Acquired)
            } else {
                Ok(Claim::Taken(Box::new(RouteRecord::decode(&current)?)))
            }
        }

        async fn lookup(&self, routing_key: &str) -> Result<Option<RouteRecord>> {
            let mut conn = self.conn.clone();
            let raw: Option<String> = redis::cmd("GET")
                .arg(self.key(routing_key))
                .query_async(&mut conn)
                .await
                .map_err(|e| unavailable("lookup", e))?;

            raw.as_deref().map(RouteRecord::decode).transpose()
        }

        async fn renew(&self, routing_key: &str, session_id: &str, ttl: Duration) -> Result<bool> {
            let mut conn = self.conn.clone();
            let renewed: i64 = redis::Script::new(RENEW_SCRIPT)
                .key(self.key(routing_key))
                .arg(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
                .arg(session_id)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| unavailable("renew", e))?;
            Ok(renewed == 1)
        }

        async fn release(&self, routing_key: &str, session_id: &str) -> Result<()> {
            let mut conn = self.conn.clone();
            let _: i64 = redis::Script::new(RELEASE_SCRIPT)
                .key(self.key(routing_key))
                .arg(session_id)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| unavailable("release", e))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &str, session: &str) -> RouteRecord {
        RouteRecord {
            routing_key: key.into(),
            node_id: "node_a".into(),
            node_addr: "10.0.0.1:7100".into(),
            session_id: session.into(),
            tunnel_id: "tun_1".into(),
            account_id: "acct_1".into(),
        }
    }

    #[test]
    fn records_round_trip() {
        let original = record("api.tunnel.com", "sess_1");
        assert_eq!(RouteRecord::decode(&original.encode()).unwrap(), original);
    }

    #[test]
    fn session_id_sits_where_the_lua_scripts_expect_it() {
        // The Redis scripts extract the owner as the 4th tab-separated field
        // without decoding the whole record. If `encode`'s field order ever
        // changes, every compare-and-swap silently starts comparing the wrong
        // column — so pin the layout here.
        let encoded = record("api.tunnel.com", "sess_owner").encode();
        assert_eq!(encoded.split('\t').nth(3), Some("sess_owner"));
    }

    #[tokio::test]
    async fn first_claimer_wins_and_the_loser_learns_the_owner() {
        let reg = MemoryRegistry::new();
        let ttl = Duration::from_secs(30);

        assert_eq!(
            reg.claim(&record("api.tunnel.com", "sess_1"), ttl)
                .await
                .unwrap(),
            Claim::Acquired
        );

        let outcome = reg
            .claim(&record("api.tunnel.com", "sess_2"), ttl)
            .await
            .unwrap();
        match outcome {
            Claim::Taken(owner) => assert_eq!(owner.session_id, "sess_1"),
            Claim::Acquired => panic!("two sessions both won the same routing key"),
        }
    }

    #[tokio::test]
    async fn reclaiming_your_own_key_is_idempotent() {
        // This is the agent-reconnect path: same session id, same key.
        let reg = MemoryRegistry::new();
        let ttl = Duration::from_secs(30);
        let rec = record("api.tunnel.com", "sess_1");

        assert_eq!(reg.claim(&rec, ttl).await.unwrap(), Claim::Acquired);
        assert_eq!(reg.claim(&rec, ttl).await.unwrap(), Claim::Acquired);
    }

    #[tokio::test]
    async fn expired_claims_free_themselves() {
        let reg = MemoryRegistry::new();
        reg.claim(
            &record("api.tunnel.com", "sess_1"),
            Duration::from_millis(30),
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(60)).await;

        assert_eq!(reg.lookup("api.tunnel.com").await.unwrap(), None);
        // ...and a different session can now take over.
        assert_eq!(
            reg.claim(&record("api.tunnel.com", "sess_2"), Duration::from_secs(30))
                .await
                .unwrap(),
            Claim::Acquired
        );
    }

    #[tokio::test]
    async fn renew_requires_ownership() {
        let reg = MemoryRegistry::new();
        let ttl = Duration::from_secs(30);
        reg.claim(&record("api.tunnel.com", "sess_1"), ttl)
            .await
            .unwrap();

        assert!(reg.renew("api.tunnel.com", "sess_1", ttl).await.unwrap());
        assert!(!reg.renew("api.tunnel.com", "sess_2", ttl).await.unwrap());
        assert!(!reg
            .renew("nonexistent.tunnel.com", "sess_1", ttl)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn a_stale_node_cannot_release_someone_elses_claim() {
        // The partition-recovery scenario: sess_1 lost the key, sess_2 took
        // over, then sess_1 finally runs its cleanup. It must be a no-op.
        let reg = MemoryRegistry::new();
        let ttl = Duration::from_secs(30);

        reg.claim(&record("api.tunnel.com", "sess_2"), ttl)
            .await
            .unwrap();
        reg.release("api.tunnel.com", "sess_1").await.unwrap();

        let still_there = reg
            .lookup("api.tunnel.com")
            .await
            .unwrap()
            .expect("claim was deleted");
        assert_eq!(still_there.session_id, "sess_2");
    }

    #[tokio::test]
    async fn release_by_the_owner_frees_the_key() {
        let reg = MemoryRegistry::new();
        reg.claim(&record("api.tunnel.com", "sess_1"), Duration::from_secs(30))
            .await
            .unwrap();
        reg.release("api.tunnel.com", "sess_1").await.unwrap();
        assert_eq!(reg.lookup("api.tunnel.com").await.unwrap(), None);
    }
}
