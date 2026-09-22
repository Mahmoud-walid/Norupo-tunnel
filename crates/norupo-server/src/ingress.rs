//! Public ingress: turns an inbound HTTP request into a multiplexed tunnel
//! stream on the right agent session.
//!
//! The hot path is four steps:
//!
//! 1. Canonicalise the `Host` header into a routing key.
//! 2. Look the key up in the routing table.
//! 3. If this node owns the session, open a stream on it.
//! 4. Otherwise hand the request to the node that does.
//!
//! Step 4 is what lets a plain L4 load balancer sit in front of the fleet: any
//! node can accept any request, because any node can reach the owner.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, BodyStream, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use norupo_core::registry::RouteRecord;
use norupo_core::routing;
use norupo_proto::{HttpRequestHead, ResetCode};
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, warn};

use crate::http::{
    error_page, full_body, html_escape, sanitize_headers, to_header_map, ResponseBody, HOP_HEADER,
};
use crate::peer::PeerClient;
use crate::session::{Chunk, OpenError, Session};
use crate::state::Shared;

/// Path reserved for the edge's own endpoints, never forwarded to a tunnel.
const INTERNAL_PREFIX: &str = "/__norupo/";

/// Whether this listener may hand a request to a sibling node.
///
/// The peer listener must not: a request that already crossed a hop has
/// reached the node the routing table named, and bouncing it again means the
/// table is stale and we would loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The internet-facing listener.
    Public,
    /// The internal listener that sibling nodes hand off to.
    Peer,
}

/// Serves HTTP on `listener` until `shutdown` resolves.
pub async fn serve(
    state: Shared,
    listener: TcpListener,
    role: Role,
    peers: Arc<PeerClient>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    let builder = ConnBuilder::new(TokioExecutor::new());

    loop {
        let accepted = tokio::select! {
            result = listener.accept() => result,
            _ = shutdown.changed() => {
                debug!(?role, "ingress listener shutting down");
                return Ok(());
            }
        };

        let (stream, remote_addr) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                // Per-connection accept errors (fd exhaustion, RST during
                // handshake) must not kill the listener.
                warn!(error = %e, "accept failed");
                continue;
            }
        };

        let state = Arc::clone(&state);
        let peers = Arc::clone(&peers);
        let builder = builder.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                let peers = Arc::clone(&peers);
                async move { Ok::<_, Infallible>(handle(state, peers, role, req, remote_addr).await) }
            });

            if let Err(e) = builder
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                debug!(error = %e, "connection ended");
            }
        });
    }
}

/// Routes one request.
pub async fn handle(
    state: Shared,
    peers: Arc<PeerClient>,
    role: Role,
    req: Request<Incoming>,
    remote_addr: SocketAddr,
) -> Response<ResponseBody> {
    if req.uri().path().starts_with(INTERNAL_PREFIX) {
        return internal_endpoint(&state, role, req.uri().path());
    }

    let Some(host) = request_host(&req) else {
        return error_page(
            StatusCode::BAD_REQUEST,
            "Missing host",
            "The request did not carry a <code>Host</code> header, so the edge cannot tell which tunnel it belongs to.",
        );
    };

    let routing_key = match routing::normalize_host(&host) {
        Ok(key) => key,
        Err(e) => {
            return error_page(
                StatusCode::BAD_REQUEST,
                "Invalid host",
                &html_escape(&e.to_string()),
            )
        }
    };

    let record = match state.registry.lookup(&routing_key).await {
        Ok(Some(record)) => record,
        Ok(None) => return tunnel_not_found(&routing_key),
        Err(e) => {
            error!(error = %e, %routing_key, "routing table lookup failed");
            return error_page(
                StatusCode::BAD_GATEWAY,
                "Routing unavailable",
                "The edge could not reach its routing table. This is a server-side problem; retry shortly.",
            );
        }
    };

    // The common case: we hold the agent session for this tunnel.
    if record.node_id == state.node_id {
        return match state.sessions.get(&record.session_id) {
            Some(session) => forward_local(&state, session, &record, req, remote_addr).await,
            None => {
                // The table says us, but the session is gone — it disconnected
                // between lookup and now, or the entry is stale. Either way the
                // claim is ours to clean up.
                let _ = state
                    .registry
                    .release(&routing_key, &record.session_id)
                    .await;
                tunnel_not_found(&routing_key)
            }
        };
    }

    // The tunnel lives on a sibling node.
    if role == Role::Peer || req.headers().contains_key(HOP_HEADER) {
        warn!(
            %routing_key,
            owner = %record.node_id,
            "refusing a second cross-node hop; routing table is likely stale"
        );
        return error_page(
            StatusCode::LOOP_DETECTED,
            "Routing loop",
            "The edge cluster disagreed about which node owns this tunnel. Retry in a moment.",
        );
    }

    peers.forward(&record, req).await
}

/// 404 page for a host nobody has claimed.
fn tunnel_not_found(routing_key: &str) -> Response<ResponseBody> {
    let mut response = error_page(
        StatusCode::NOT_FOUND,
        "Tunnel not found",
        &format!(
            "No agent is serving <code>{}</code>. Start one with \
             <code>norupo http 3000 --subdomain &lt;name&gt;</code>.",
            html_escape(routing_key)
        ),
    );
    // Let clients and CDNs know this is a routing miss, not a 404 from the
    // user's own app.
    response.headers_mut().insert(
        "x-norupo-error",
        http::HeaderValue::from_static("tunnel_not_found"),
    );
    response
}

/// Serves the edge's own endpoints.
fn internal_endpoint(state: &Shared, role: Role, path: &str) -> Response<ResponseBody> {
    match path.trim_end_matches('/') {
        "/__norupo/healthz" => {
            // The public listener answers with a bare 200 so a load balancer
            // can probe it; only the internal listener reveals topology.
            if role == Role::Public {
                return Response::builder()
                    .status(StatusCode::OK)
                    .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(full_body("ok\n"))
                    .expect("static response");
            }
            let body = serde_json::json!({
                "ok": true,
                "node_id": state.node_id,
                "node_addr": state.node_addr,
                "version": state.version(),
                "sessions": state.sessions.len(),
                "tunnels": state.sessions.tunnel_count(),
            });
            Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(full_body(body.to_string()))
                .expect("static response")
        }
        "/__norupo/tunnels" if role == Role::Peer => {
            let body = serde_json::to_string(&state.sessions.snapshot_json())
                .unwrap_or_else(|_| "{}".into());
            Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(full_body(body))
                .expect("static response")
        }
        _ => error_page(StatusCode::NOT_FOUND, "Not found", "Unknown edge endpoint."),
    }
}

/// Extracts the effective host: the `:authority` pseudo-header on HTTP/2, the
/// `Host` header on HTTP/1.1.
fn request_host<B>(req: &Request<B>) -> Option<String> {
    if let Some(authority) = req.uri().authority() {
        return Some(authority.as_str().to_string());
    }
    req.headers()
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// Forwards a request over an agent session that this node owns.
async fn forward_local(
    state: &Shared,
    session: Arc<Session>,
    record: &RouteRecord,
    req: Request<Incoming>,
    remote_addr: SocketAddr,
) -> Response<ResponseBody> {
    let (parts, body) = req.into_parts();

    let head = HttpRequestHead {
        method: parts.method.as_str().to_string(),
        path: parts
            .uri
            .path_and_query()
            .map_or_else(|| "/".to_string(), |pq| pq.as_str().to_string()),
        authority: request_host_from_parts(&parts).unwrap_or_default(),
        headers: sanitize_headers(&parts.headers),
        version: format!("{:?}", parts.version),
        upgrade: parts
            .headers
            .get(http::header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string(),
    };

    let opened = match session
        .open_http_stream(&record.tunnel_id, head, remote_addr.to_string())
        .await
    {
        Ok(opened) => opened,
        Err(OpenError::SessionGone) => return tunnel_not_found(&record.routing_key),
        Err(OpenError::TooManyStreams(limit)) => {
            return error_page(
                StatusCode::SERVICE_UNAVAILABLE,
                "Too many concurrent requests",
                &format!("This tunnel is limited to {limit} concurrent requests."),
            )
        }
    };

    // Pump the request body to the agent in the background so we can start
    // waiting for the response head immediately.
    let stream_id = opened.stream_id;
    let pump_session = Arc::clone(&session);
    tokio::spawn(async move {
        let mut frames = BodyStream::new(body);
        while let Some(frame) = frames.next().await {
            match frame {
                Ok(frame) => {
                    if let Ok(data) = frame.into_data() {
                        if !data.is_empty() && !pump_session.send_stream_data(stream_id, data).await
                        {
                            return; // stream or session went away
                        }
                    }
                }
                Err(e) => {
                    debug!(stream_id, error = %e, "public client body failed");
                    pump_session
                        .reset_stream(stream_id, ResetCode::PeerClosed, "client body error")
                        .await;
                    return;
                }
            }
        }
        pump_session.send_stream_end(stream_id).await;
    });

    // Wait for the agent's response head.
    let head = match tokio::time::timeout(state.config.response_timeout(), opened.head).await {
        Ok(Ok(head)) => head,
        // The agent reset the stream instead of answering.
        Ok(Err(_)) => {
            session.close_stream(stream_id);
            return error_page(
                StatusCode::BAD_GATEWAY,
                "Local service unreachable",
                "The agent could not reach the local service it is forwarding to.",
            );
        }
        Err(_) => {
            session
                .reset_stream(stream_id, ResetCode::Timeout, "edge response timeout")
                .await;
            return error_page(
                StatusCode::GATEWAY_TIMEOUT,
                "Tunnel timed out",
                &format!(
                    "The local service did not respond within {:?}.",
                    state.config.response_timeout()
                ),
            );
        }
    };

    build_response(session, stream_id, head, opened.body)
}

fn request_host_from_parts(parts: &http::request::Parts) -> Option<String> {
    if let Some(authority) = parts.uri.authority() {
        return Some(authority.as_str().to_string());
    }
    parts
        .headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Turns the agent's response head plus body channel into a hyper response.
fn build_response(
    session: Arc<Session>,
    stream_id: u64,
    head: norupo_proto::HttpResponseHead,
    body: tokio::sync::mpsc::Receiver<Chunk>,
) -> Response<ResponseBody> {
    let status = StatusCode::from_u16(u16::try_from(head.status).unwrap_or(502))
        .unwrap_or(StatusCode::BAD_GATEWAY);

    // Once the response body is dropped (client hung up, or we finished), the
    // stream must be released or the session leaks an entry per request.
    let guard = StreamGuard { session, stream_id };

    let body_stream = ReceiverStream::new(body).map(move |chunk| {
        // Holding the guard inside the closure ties its lifetime to the body.
        let _ = &guard;
        match chunk {
            Chunk::Data(bytes) => Ok(Frame::data(bytes)),
            Chunk::End(trailers) => Ok(Frame::trailers(to_header_map(&trailers))),
            Chunk::Reset(code, message) => Err(std::io::Error::other(format!(
                "tunnel stream reset: {code:?}: {message}"
            ))),
        }
    });

    let mut response = Response::new(BodyExt::boxed(StreamBody::new(body_stream)));
    *response.status_mut() = status;
    *response.headers_mut() = to_header_map(&head.headers);
    response
        .headers_mut()
        .insert("x-norupo-stream", http::HeaderValue::from(stream_id));
    response
}

/// Releases a multiplexed stream when the response body is dropped.
struct StreamGuard {
    session: Arc<Session>,
    stream_id: u64,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.session.close_stream(self.stream_id);
        // Best-effort notify; if the queue is full the agent will find out via
        // the next frame it sends for this stream.
        self.session.try_send(norupo_proto::ServerFrame {
            payload: Some(norupo_proto::server_frame::Payload::Reset(
                norupo_proto::StreamReset {
                    stream_id: self.stream_id,
                    code: ResetCode::PeerClosed as i32,
                    message: "public client finished".into(),
                },
            )),
        });
    }
}

/// Exposed for the peer listener, which shares the response timeout policy.
#[must_use]
pub fn default_response_timeout() -> Duration {
    Duration::from_secs(30)
}

/// Hint for tests and callers: the bytes we consider a reasonable body chunk.
pub const INGRESS_CHUNK_HINT: usize = 64 * 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::Empty;

    fn request(host: Option<&str>) -> Request<Empty<Bytes>> {
        let mut builder = Request::builder().uri("/hello?a=1");
        if let Some(host) = host {
            builder = builder.header("host", host);
        }
        builder.body(Empty::new()).unwrap()
    }

    #[test]
    fn host_comes_from_the_header_on_http1() {
        assert_eq!(
            request_host(&request(Some("api.tunnel.com"))).as_deref(),
            Some("api.tunnel.com")
        );
        assert_eq!(request_host(&request(None)), None);
    }

    #[test]
    fn host_prefers_the_authority_on_http2() {
        // HTTP/2 carries the host in `:authority`, which lands in the URI.
        let req = Request::builder()
            .uri("https://h2.tunnel.com/hello")
            .header("host", "ignored.tunnel.com")
            .body(Empty::<Bytes>::new())
            .unwrap();
        assert_eq!(request_host(&req).as_deref(), Some("h2.tunnel.com"));
    }

    #[test]
    fn not_found_pages_are_labelled_for_clients() {
        let response = tunnel_not_found("api.tunnel.com");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get("x-norupo-error").unwrap(),
            "tunnel_not_found"
        );
    }
}
