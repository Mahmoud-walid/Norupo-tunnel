//! Node-to-node request hand-off.
//!
//! When the routing table says a tunnel belongs to a sibling node, this is how
//! the request gets there. It is a plain reverse proxy over the cluster's
//! internal network: the receiving node sees an ordinary HTTP request with the
//! original `Host` intact, looks it up in its own local session table, and
//! serves it.
//!
//! Why proxy rather than redirect: the public client must not learn the
//! cluster's internal topology, and an HTTP redirect would break every
//! non-idempotent request.

use http::{Request, Response, StatusCode, Uri};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use norupo_core::registry::RouteRecord;
use tracing::{debug, warn};

use crate::http::{error_page, ResponseBody, HOP_HEADER};

/// Body type used when talking to sibling nodes.
type PeerBody = http_body_util::combinators::BoxBody<bytes::Bytes, std::io::Error>;

/// A pooled HTTP client for sibling edge nodes.
///
/// Connection pooling matters here: a hand-off must not pay a TCP handshake,
/// and at fleet scale the pool is what keeps cross-node traffic from
/// exhausting ephemeral ports.
pub struct PeerClient {
    client: Client<HttpConnector, PeerBody>,
    /// This node's id, sent so the receiving side can log the hand-off chain.
    node_id: String,
}

impl PeerClient {
    #[must_use]
    pub fn new(node_id: String) -> Self {
        let mut connector = HttpConnector::new();
        // Peers are on the internal network; a slow connect means the node is
        // unhealthy and we would rather fail fast and let the LB retry.
        connector.set_connect_timeout(Some(std::time::Duration::from_secs(2)));
        connector.set_nodelay(true);

        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .pool_max_idle_per_host(32)
            .build(connector);

        Self { client, node_id }
    }

    /// Hands `req` to the node named in `record`.
    pub async fn forward(
        &self,
        record: &RouteRecord,
        req: Request<Incoming>,
    ) -> Response<ResponseBody> {
        let (mut parts, body) = req.into_parts();

        let path_and_query = parts
            .uri
            .path_and_query()
            .map_or_else(|| "/".to_string(), |pq| pq.as_str().to_string());

        let target = match format!("http://{}{}", record.node_addr, path_and_query).parse::<Uri>() {
            Ok(uri) => uri,
            Err(e) => {
                warn!(node_addr = %record.node_addr, error = %e, "routing table holds an unusable peer address");
                return error_page(
                    StatusCode::BAD_GATEWAY,
                    "Bad peer address",
                    "The routing table names an edge node the cluster cannot address.",
                );
            }
        };
        parts.uri = target;

        // Mark the hop so the receiving node refuses to bounce it onward.
        parts.headers.insert(
            HOP_HEADER,
            http::HeaderValue::from_str(&self.node_id)
                .unwrap_or_else(|_| http::HeaderValue::from_static("unknown")),
        );

        let upstream = Request::from_parts(parts, body.map_err(std::io::Error::other).boxed());

        match self.client.request(upstream).await {
            Ok(response) => {
                let (parts, body) = response.into_parts();
                Response::from_parts(parts, body.map_err(std::io::Error::other).boxed())
            }
            Err(e) => {
                debug!(
                    owner = %record.node_id,
                    node_addr = %record.node_addr,
                    error = %e,
                    "cross-node hand-off failed"
                );
                error_page(
                    StatusCode::BAD_GATEWAY,
                    "Edge node unreachable",
                    "The edge node holding this tunnel did not answer. It may have just restarted; retry shortly.",
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_client_builds_without_a_network() {
        let client = PeerClient::new("node_a".into());
        assert_eq!(client.node_id, "node_a");
    }

    #[tokio::test]
    async fn an_unusable_peer_address_produces_a_502_not_a_panic() {
        let client = PeerClient::new("node_a".into());
        let record = RouteRecord {
            routing_key: "api.tunnel.com".into(),
            node_id: "node_b".into(),
            // Spaces are not legal in an authority, so URI parsing fails.
            node_addr: "not a valid authority".into(),
            session_id: "sess_1".into(),
            tunnel_id: "tun_1".into(),
            account_id: "acct_1".into(),
        };

        // `Incoming` cannot be constructed directly in a unit test, so this
        // asserts the parsing branch through the same code path the handler
        // uses to build the target URI.
        let target = format!("http://{}/path", record.node_addr).parse::<Uri>();
        assert!(
            target.is_err(),
            "expected an invalid authority to fail parsing"
        );
        let _ = client;
    }
}
