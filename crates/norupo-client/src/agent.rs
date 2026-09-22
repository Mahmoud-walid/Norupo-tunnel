//! The agent: one long-lived gRPC session, N multiplexed local requests.
//!
//! ```text
//!   edge ──ServerFrame::Open──▶ agent ──HTTP──▶ localhost:3000
//!   edge ◀──ResponseHead/Data── agent ◀─HTTP──  localhost:3000
//! ```
//!
//! Everything below is driven by a single read loop. Per-request work happens
//! on spawned tasks so that one slow local handler cannot stall the session's
//! control traffic — which is the whole reason the protocol multiplexes in the
//! first place.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use norupo_proto::tunnel_control_client::TunnelControlClient;
use norupo_proto::{
    client_frame, open_tunnel, server_frame, ClientFrame, Header, Hello, HttpResponseHead,
    HttpTunnelSpec, OpenTunnel, Pong, ResetCode, StreamData, StreamEnd, StreamOpen, StreamReset,
    TunnelStatus, WindowUpdate, PROTOCOL_VERSION,
};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use crate::config::reconnect_delay;

/// Largest payload the agent puts in one `StreamData` frame.
const MAX_CHUNK: usize = 64 * 1024;

/// Depth of the frame queue heading back to the edge.
const OUTBOUND_QUEUE: usize = 1024;

/// Per-stream request-body buffer, in frames.
const BODY_QUEUE: usize = 16;

/// What the agent is asked to publish.
#[derive(Debug, Clone)]
pub struct AgentOptions {
    /// gRPC endpoint of the edge.
    pub server: String,
    /// Bearer token.
    pub token: String,
    /// `host:port` the agent forwards to.
    pub forward_addr: String,
    /// Requested subdomain, if any.
    pub subdomain: Option<String>,
    /// Requested custom domain, if any.
    pub domain: Option<String>,
    /// Rewrite `Host` to the forward target.
    pub rewrite_host: bool,
    /// Reconnect attempts before giving up. 0 == forever.
    pub max_retries: u32,
}

/// Lifecycle events, so callers (the CLI, and tests) can react without parsing
/// log output.
#[derive(Debug, Clone)]
pub enum AgentEvent {
    /// The handshake completed.
    Connected { session_id: String, node_id: String },
    /// A tunnel is live and serving.
    TunnelReady(TunnelStatus),
    /// The edge refused a tunnel. Carries the reason.
    TunnelRejected(String),
    /// The session ended; the agent will reconnect unless retries ran out.
    Disconnected(String),
}

/// Body type for requests the agent makes to the local service: a channel the
/// session read loop pushes `StreamData` payloads into.
type LocalRequestBody = StreamBody<ReceiverStream<Result<Frame<Bytes>, std::io::Error>>>;

/// HTTP client used to reach the local service.
type LocalClient = Client<HttpConnector, LocalRequestBody>;

/// Per-stream state held by the agent.
struct StreamState {
    /// Request body bytes flowing edge -> local service.
    body_tx: mpsc::Sender<Result<Frame<Bytes>, std::io::Error>>,
    /// Credit for bytes flowing agent -> edge.
    send_credit: Arc<Semaphore>,
}

/// Shared handle to the outbound frame queue plus the live stream table.
struct SessionCtx {
    outbound: mpsc::Sender<ClientFrame>,
    streams: Mutex<HashMap<u64, StreamState>>,
    http: LocalClient,
    options: AgentOptions,
}

impl SessionCtx {
    async fn send(&self, payload: client_frame::Payload) -> bool {
        self.outbound
            .send(ClientFrame {
                payload: Some(payload),
            })
            .await
            .is_ok()
    }

    /// Removes a stream, waking anything parked on its send window.
    async fn drop_stream(&self, stream_id: u64) {
        if let Some(state) = self.streams.lock().await.remove(&stream_id) {
            state.send_credit.close();
        }
    }
}

/// Runs the agent until it is told to stop or runs out of retries.
///
/// # Errors
/// Returns an error only when the agent gives up: an unrecoverable handshake
/// failure (bad token, protocol mismatch) or exhausted retries.
pub async fn run(
    options: AgentOptions,
    events: mpsc::Sender<AgentEvent>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut attempt: u32 = 0;

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        match run_session(&options, &events, shutdown.clone()).await {
            Ok(()) => {
                // Clean disconnect: reset backoff, the edge was healthy.
                attempt = 0;
            }
            Err(SessionError::Fatal(e)) => return Err(e),
            Err(SessionError::Transient(e)) => {
                let _ = events.send(AgentEvent::Disconnected(e.to_string())).await;
                warn!(error = %e, "session ended");
            }
        }

        if *shutdown.borrow() {
            return Ok(());
        }
        if options.max_retries > 0 && attempt >= options.max_retries {
            anyhow::bail!("giving up after {} reconnect attempts", options.max_retries);
        }

        let delay = reconnect_delay(attempt);
        attempt = attempt.saturating_add(1);
        info!(?delay, attempt, "reconnecting");

        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => return Ok(()),
        }
    }
}

/// Distinguishes "retry" from "stop and tell the human".
enum SessionError {
    /// Reconnecting might help.
    Transient(anyhow::Error),
    /// Reconnecting will not help (bad credentials, protocol mismatch).
    Fatal(anyhow::Error),
}

async fn run_session(
    options: &AgentOptions,
    events: &mpsc::Sender<AgentEvent>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), SessionError> {
    let mut client = TunnelControlClient::connect(options.server.clone())
        .await
        .map_err(|e| {
            SessionError::Transient(anyhow::anyhow!("connect to {}: {e}", options.server))
        })?;

    let (outbound_tx, outbound_rx) = mpsc::channel::<ClientFrame>(OUTBOUND_QUEUE);

    // Hello must be queued before the RPC starts, since the server reads it as
    // the first frame.
    let hello = ClientFrame {
        payload: Some(client_frame::Payload::Hello(Hello {
            token: options.token.clone(),
            agent_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: PROTOCOL_VERSION,
            metadata: agent_metadata(),
        })),
    };
    outbound_tx
        .send(hello)
        .await
        .map_err(|_| SessionError::Transient(anyhow::anyhow!("outbound queue closed")))?;

    let response = client
        .session(ReceiverStream::new(outbound_rx))
        .await
        .map_err(|status| match status.code() {
            // These mean the agent itself is wrong; retrying just spams logs.
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => SessionError::Fatal(
                anyhow::anyhow!("authentication failed: {}", status.message()),
            ),
            tonic::Code::FailedPrecondition => SessionError::Fatal(anyhow::anyhow!(
                "edge refused the session: {}",
                status.message()
            )),
            _ => SessionError::Transient(anyhow::anyhow!("session RPC failed: {status}")),
        })?;

    let mut inbound = response.into_inner();

    let ctx = Arc::new(SessionCtx {
        outbound: outbound_tx,
        streams: Mutex::new(HashMap::new()),
        http: Client::builder(TokioExecutor::new()).build(HttpConnector::new()),
        options: options.clone(),
    });

    loop {
        let frame = tokio::select! {
            frame = inbound.next() => frame,
            _ = shutdown.changed() => {
                debug!("shutdown requested; closing session");
                return Ok(());
            }
        };

        match frame {
            Some(Ok(frame)) => {
                if let Some(payload) = frame.payload {
                    handle_server_frame(&ctx, events, payload).await;
                }
            }
            Some(Err(status)) => {
                return Err(SessionError::Transient(anyhow::anyhow!(
                    "stream error: {status}"
                )))
            }
            None => return Ok(()),
        }
    }
}

async fn handle_server_frame(
    ctx: &Arc<SessionCtx>,
    events: &mpsc::Sender<AgentEvent>,
    payload: server_frame::Payload,
) {
    match payload {
        server_frame::Payload::Welcome(welcome) => {
            info!(
                session_id = %welcome.session_id,
                node = %welcome.node_id,
                server = %welcome.server_version,
                "connected to edge"
            );
            let _ = events
                .send(AgentEvent::Connected {
                    session_id: welcome.session_id,
                    node_id: welcome.node_id,
                })
                .await;

            // The edge is ready; ask for our tunnel.
            ctx.send(client_frame::Payload::OpenTunnel(build_open_tunnel(
                &ctx.options,
            )))
            .await;
        }
        server_frame::Payload::TunnelStatus(status) => {
            if status.accepted {
                info!(url = %status.public_url, "tunnel online");
                let _ = events.send(AgentEvent::TunnelReady(status)).await;
            } else {
                error!(reason = %status.error, "tunnel rejected");
                let _ = events.send(AgentEvent::TunnelRejected(status.error)).await;
            }
        }
        server_frame::Payload::Open(open) => {
            // Register the stream HERE, on the read loop, before spawning.
            //
            // The edge starts pumping the request body the instant it has sent
            // `Open`, so `Data` and `End` for this stream can be the very next
            // frames we read. If registration happened inside the spawned task,
            // those frames would arrive before the entry existed and be
            // dropped — the request body would then never terminate and the
            // local service would hang until the edge timed the request out.
            let (body_tx, body_rx) =
                mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(BODY_QUEUE);
            let send_credit = Arc::new(Semaphore::new(open.initial_window as usize));
            ctx.streams.lock().await.insert(
                open.stream_id,
                StreamState {
                    body_tx,
                    send_credit: Arc::clone(&send_credit),
                },
            );

            // One task per public request: a slow local handler must not block
            // the session read loop.
            let ctx = Arc::clone(ctx);
            tokio::spawn(async move { serve_stream(ctx, open, body_rx, send_credit).await });
        }
        server_frame::Payload::Data(data) => {
            let stream_id = data.stream_id;
            let len = data.payload.len();
            let sender = ctx
                .streams
                .lock()
                .await
                .get(&stream_id)
                .map(|s| s.body_tx.clone());
            if let Some(tx) = sender {
                if tx.send(Ok(Frame::data(data.payload))).await.is_ok() {
                    // Return credit only for bytes we actually buffered.
                    ctx.send(client_frame::Payload::WindowUpdate(WindowUpdate {
                        stream_id,
                        increment: u32::try_from(len).unwrap_or(u32::MAX),
                    }))
                    .await;
                } else {
                    ctx.drop_stream(stream_id).await;
                }
            }
        }
        server_frame::Payload::End(end) => close_request_body(ctx, end.stream_id).await,
        server_frame::Payload::Reset(reset) => {
            debug!(stream_id = reset.stream_id, reason = %reset.message, "edge reset stream");
            ctx.drop_stream(reset.stream_id).await;
        }
        server_frame::Payload::WindowUpdate(update) => {
            if let Some(state) = ctx.streams.lock().await.get(&update.stream_id) {
                state.send_credit.add_permits(update.increment as usize);
            }
        }
        server_frame::Payload::Ping(ping) => {
            ctx.send(client_frame::Payload::Pong(Pong {
                nonce: ping.nonce,
                client_time_ms: now_ms(),
            }))
            .await;
        }
        server_frame::Payload::Shutdown(shutdown) => {
            warn!(reason = %shutdown.reason, "edge is draining this session");
        }
        server_frame::Payload::ResponseHead(_) => { /* server never sends this */ }
    }
}

/// Marks the request body complete for a stream.
///
/// The stream entry itself has to stay: the response is very likely still in
/// flight. Swapping in a sender whose receiver is already gone drops the last
/// live handle to the body channel, which is what tells hyper the request body
/// has ended.
async fn close_request_body(ctx: &Arc<SessionCtx>, stream_id: u64) {
    let mut streams = ctx.streams.lock().await;
    if let Some(state) = streams.get_mut(&stream_id) {
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        state.body_tx = closed_tx;
    }
}

/// Handles one public request end to end.
///
/// The stream is already registered in `ctx.streams` by the time this runs;
/// see the `Open` arm of [`handle_server_frame`] for why.
async fn serve_stream(
    ctx: Arc<SessionCtx>,
    open: StreamOpen,
    body_rx: mpsc::Receiver<Result<Frame<Bytes>, std::io::Error>>,
    send_credit: Arc<Semaphore>,
) {
    let stream_id = open.stream_id;
    let Some(head) = open.http else {
        reset(
            &ctx,
            stream_id,
            ResetCode::ProtocolError,
            "Open frame without an HTTP head",
        )
        .await;
        ctx.drop_stream(stream_id).await;
        return;
    };

    let uri = format!("http://{}{}", ctx.options.forward_addr, head.path);
    let mut builder = hyper::Request::builder()
        .method(head.method.as_str())
        .uri(&uri);

    for header in &head.headers {
        // The local service is ours, but the public client's headers are not:
        // drop anything hyper would reject rather than failing the request.
        let lower = header.name.to_ascii_lowercase();
        if lower == "host" && ctx.options.rewrite_host {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            http::HeaderName::try_from(header.name.as_str()),
            http::HeaderValue::from_bytes(&header.value),
        ) {
            builder = builder.header(name, value);
        }
    }
    if ctx.options.rewrite_host {
        builder = builder.header(http::header::HOST, ctx.options.forward_addr.as_str());
    }
    // Tell the local service who actually made the request.
    if let Some((ip, _)) = open.remote_addr.rsplit_once(':') {
        builder = builder.header("x-forwarded-for", ip.trim_matches(['[', ']']));
    }
    builder = builder.header("x-forwarded-host", head.authority.as_str());
    builder = builder.header("x-forwarded-proto", "https");

    let request = match builder.body(StreamBody::new(ReceiverStream::new(body_rx))) {
        Ok(request) => request,
        Err(e) => {
            reset(
                &ctx,
                stream_id,
                ResetCode::ProtocolError,
                &format!("bad request: {e}"),
            )
            .await;
            ctx.drop_stream(stream_id).await;
            return;
        }
    };

    let response = match ctx.http.request(request).await {
        Ok(response) => response,
        Err(e) => {
            // This is the error users hit most: nothing listening on the port.
            warn!(target = %ctx.options.forward_addr, error = %e, "local service unreachable");
            reset(
                &ctx,
                stream_id,
                ResetCode::LocalUnreachable,
                &format!("cannot reach {}: {e}", ctx.options.forward_addr),
            )
            .await;
            ctx.drop_stream(stream_id).await;
            return;
        }
    };

    let (parts, mut body) = response.into_parts();
    let sent = ctx
        .send(client_frame::Payload::ResponseHead(HttpResponseHead {
            stream_id,
            status: u32::from(parts.status.as_u16()),
            headers: parts
                .headers
                .iter()
                .map(|(name, value)| Header {
                    name: name.as_str().to_string(),
                    value: Bytes::copy_from_slice(value.as_bytes()),
                })
                .collect(),
        }))
        .await;
    if !sent {
        ctx.drop_stream(stream_id).await;
        return;
    }

    let mut trailers = Vec::new();
    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if let Some(data) = frame.data_ref() {
                    if !send_payload(&ctx, stream_id, &send_credit, data.clone()).await {
                        ctx.drop_stream(stream_id).await;
                        return;
                    }
                } else if let Some(map) = frame.trailers_ref() {
                    trailers = map
                        .iter()
                        .map(|(name, value)| Header {
                            name: name.as_str().to_string(),
                            value: Bytes::copy_from_slice(value.as_bytes()),
                        })
                        .collect();
                }
            }
            Err(e) => {
                reset(
                    &ctx,
                    stream_id,
                    ResetCode::Internal,
                    &format!("local body error: {e}"),
                )
                .await;
                ctx.drop_stream(stream_id).await;
                return;
            }
        }
    }

    ctx.send(client_frame::Payload::End(StreamEnd {
        stream_id,
        trailers,
    }))
    .await;
    ctx.drop_stream(stream_id).await;
}

/// Sends response bytes upstream, honouring the edge's flow-control window.
async fn send_payload(
    ctx: &Arc<SessionCtx>,
    stream_id: u64,
    credit: &Arc<Semaphore>,
    mut payload: Bytes,
) -> bool {
    while !payload.is_empty() {
        let take = payload.len().min(MAX_CHUNK);
        let chunk = payload.split_to(take);

        let permits = u32::try_from(chunk.len()).unwrap_or(u32::MAX);
        let Ok(permit) = credit.acquire_many(permits).await else {
            return false; // stream torn down while we waited for credit
        };
        permit.forget();

        if !ctx
            .send(client_frame::Payload::Data(StreamData {
                stream_id,
                payload: chunk,
            }))
            .await
        {
            return false;
        }
    }
    true
}

async fn reset(ctx: &Arc<SessionCtx>, stream_id: u64, code: ResetCode, message: &str) {
    ctx.send(client_frame::Payload::Reset(StreamReset {
        stream_id,
        code: code as i32,
        message: message.to_string(),
    }))
    .await;
}

/// Builds the registration request from CLI options.
pub fn build_open_tunnel(options: &AgentOptions) -> OpenTunnel {
    OpenTunnel {
        request_id: format!("req_{}", now_ms()),
        spec: Some(open_tunnel::Spec::Http(HttpTunnelSpec {
            subdomain: options.subdomain.clone().unwrap_or_default(),
            custom_domain: options.domain.clone().unwrap_or_default(),
            forward_to: options.forward_addr.clone(),
            rewrite_host_header: options.rewrite_host,
        })),
    }
}

/// Labels surfaced in the edge's dashboards and logs.
fn agent_metadata() -> HashMap<String, String> {
    HashMap::from([
        ("os".to_string(), std::env::consts::OS.to_string()),
        ("arch".to_string(), std::env::consts::ARCH.to_string()),
        (
            "hostname".to_string(),
            std::env::var("HOSTNAME")
                .or_else(|_| std::env::var("COMPUTERNAME")) // Windows
                .unwrap_or_else(|_| "unknown".into()),
        ),
    ])
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> AgentOptions {
        AgentOptions {
            server: "http://127.0.0.1:7000".into(),
            token: "tok".into(),
            forward_addr: "127.0.0.1:3000".into(),
            subdomain: Some("my-app".into()),
            domain: None,
            rewrite_host: false,
            max_retries: 0,
        }
    }

    #[test]
    fn open_tunnel_carries_the_requested_subdomain() {
        let open = build_open_tunnel(&options());
        match open.spec {
            Some(open_tunnel::Spec::Http(spec)) => {
                assert_eq!(spec.subdomain, "my-app");
                assert_eq!(spec.custom_domain, "");
                assert_eq!(spec.forward_to, "127.0.0.1:3000");
            }
            other => panic!("expected an HTTP spec, got {other:?}"),
        }
    }

    #[test]
    fn open_tunnel_with_neither_flag_asks_for_a_random_subdomain() {
        // Empty subdomain is the protocol's "you pick" signal.
        let opts = AgentOptions {
            subdomain: None,
            ..options()
        };
        match build_open_tunnel(&opts).spec {
            Some(open_tunnel::Spec::Http(spec)) => assert_eq!(spec.subdomain, ""),
            other => panic!("expected an HTTP spec, got {other:?}"),
        }
    }

    #[test]
    fn custom_domains_take_the_domain_field() {
        let opts = AgentOptions {
            subdomain: None,
            domain: Some("hooks.example.com".into()),
            ..options()
        };
        match build_open_tunnel(&opts).spec {
            Some(open_tunnel::Spec::Http(spec)) => {
                assert_eq!(spec.custom_domain, "hooks.example.com");
                assert_eq!(spec.subdomain, "");
            }
            other => panic!("expected an HTTP spec, got {other:?}"),
        }
    }

    #[test]
    fn metadata_always_reports_os_and_arch() {
        let meta = agent_metadata();
        assert_eq!(
            meta.get("os").map(String::as_str),
            Some(std::env::consts::OS)
        );
        assert!(meta.contains_key("arch"));
        assert!(meta.contains_key("hostname"));
    }

    #[tokio::test]
    async fn a_connection_refusal_is_transient_not_fatal() {
        // Port 1 on loopback has nothing listening; the agent must classify
        // this as retryable rather than exiting.
        let opts = AgentOptions {
            server: "http://127.0.0.1:1".into(),
            ..options()
        };
        let (events, _rx) = mpsc::channel(8);
        let (_tx, shutdown) = tokio::sync::watch::channel(false);

        match run_session(&opts, &events, shutdown).await {
            Err(SessionError::Transient(_)) => {}
            Err(SessionError::Fatal(e)) => panic!("connect refusal must not be fatal: {e}"),
            Ok(()) => panic!("expected the session to fail"),
        }
    }

    #[tokio::test]
    async fn run_gives_up_after_max_retries() {
        let opts = AgentOptions {
            server: "http://127.0.0.1:1".into(),
            max_retries: 1,
            ..options()
        };
        let (events, _rx) = mpsc::channel(64);
        let (_tx, shutdown) = tokio::sync::watch::channel(false);

        let err = run(opts, events, shutdown)
            .await
            .expect_err("should give up");
        assert!(
            err.to_string().contains("giving up"),
            "unexpected error: {err}"
        );
    }
}
