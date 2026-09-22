//! Agent sessions and the streams multiplexed over them.
//!
//! One [`Session`] == one connected agent == one long-lived gRPC bidi stream.
//! Inside it, every public request becomes a logical stream identified by a
//! `stream_id`. This module owns the bookkeeping for both.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use norupo_core::auth::Principal;
use norupo_proto::{
    server_frame, Header, HttpRequestHead, HttpResponseHead, ResetCode, ServerFrame, StreamData,
    StreamEnd, StreamKind, StreamOpen, StreamReset, WindowUpdate,
};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tonic::Status;

/// Largest payload we put in a single `StreamData` frame.
///
/// 64 KiB keeps individual frames comfortably under gRPC's default message
/// limit while amortising per-frame overhead over a useful amount of data.
pub const MAX_CHUNK: usize = 64 * 1024;

/// How much buffered body we allow per stream before applying backpressure.
const STREAM_BUFFER_FRAMES: usize = 16;

/// Depth of a session's outbound frame queue.
///
/// This is the shared egress path for every stream on the session, so it needs
/// slack; if it fills, the session is genuinely slower than its offered load
/// and blocking is the correct response.
const OUTBOUND_QUEUE: usize = 1024;

/// A chunk travelling from the agent back towards the public client.
#[derive(Debug)]
pub enum Chunk {
    /// Payload bytes.
    Data(Bytes),
    /// Clean end of body, with optional trailers.
    End(Vec<Header>),
    /// Abortive termination.
    Reset(ResetCode, String),
}

/// Server-side state for one multiplexed stream.
struct Stream {
    /// Fires once, when the agent sends its response head.
    head_tx: std::sync::Mutex<Option<oneshot::Sender<HttpResponseHead>>>,
    /// Body chunks flowing agent -> public client.
    body_tx: mpsc::Sender<Chunk>,
    /// Credit for bytes flowing server -> agent, replenished by the agent's
    /// `WindowUpdate` frames.
    send_credit: Arc<Semaphore>,
    /// Set once the stream has been torn down, so late frames are dropped
    /// rather than resurrecting it.
    closed: AtomicBool,
}

/// Handle returned when the ingress opens a stream: the response head future
/// and the body receiver.
#[derive(Debug)]
pub struct StreamResponse {
    pub stream_id: u64,
    pub head: oneshot::Receiver<HttpResponseHead>,
    pub body: mpsc::Receiver<Chunk>,
}

/// A tunnel currently registered by a session.
#[derive(Debug, Clone)]
pub struct Tunnel {
    pub tunnel_id: String,
    pub routing_key: String,
    pub public_url: String,
}

/// One connected agent.
pub struct Session {
    pub id: String,
    pub node_id: String,
    pub principal: Principal,
    /// Frames queued for the agent. `Err` terminates the RPC.
    outbound: mpsc::Sender<Result<ServerFrame, Status>>,
    streams: DashMap<u64, Arc<Stream>>,
    /// Tunnels this session owns, keyed by `tunnel_id`.
    tunnels: DashMap<String, Tunnel>,
    next_stream_id: AtomicU64,
    /// Credit the agent granted us per new stream (our send window).
    initial_window: u32,
    open_streams: AtomicU64,
}

/// Why opening a stream failed.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The agent disconnected between route lookup and stream open.
    #[error("agent session is gone")]
    SessionGone,
    /// The session is already at its concurrent-stream ceiling.
    #[error("session exceeded its concurrent stream limit ({0})")]
    TooManyStreams(u32),
}

impl Session {
    /// Creates a session and the receiver that feeds the gRPC response stream.
    pub fn new(
        id: String,
        node_id: String,
        principal: Principal,
        initial_window: u32,
    ) -> (Arc<Self>, mpsc::Receiver<Result<ServerFrame, Status>>) {
        let (outbound, rx) = mpsc::channel(OUTBOUND_QUEUE);
        let session = Arc::new(Self {
            id,
            node_id,
            principal,
            outbound,
            streams: DashMap::new(),
            tunnels: DashMap::new(),
            // Stream ids start at 1 so that 0 stays available as "unset".
            next_stream_id: AtomicU64::new(1),
            initial_window,
            open_streams: AtomicU64::new(0),
        });
        (session, rx)
    }

    /// Queues a frame for the agent.
    ///
    /// Returns `false` when the session's egress is closed, which means the
    /// agent is gone and the caller should abandon whatever it was doing.
    pub async fn send(&self, frame: ServerFrame) -> bool {
        self.outbound.send(Ok(frame)).await.is_ok()
    }

    /// Non-blocking send, for paths that must not await (e.g. `Drop`).
    pub fn try_send(&self, frame: ServerFrame) -> bool {
        self.outbound.try_send(Ok(frame)).is_ok()
    }

    pub fn tunnels(&self) -> Vec<Tunnel> {
        self.tunnels.iter().map(|e| e.value().clone()).collect()
    }

    pub fn tunnel_count(&self) -> usize {
        self.tunnels.len()
    }

    pub fn insert_tunnel(&self, tunnel: Tunnel) {
        self.tunnels.insert(tunnel.tunnel_id.clone(), tunnel);
    }

    pub fn remove_tunnel(&self, tunnel_id: &str) -> Option<Tunnel> {
        self.tunnels.remove(tunnel_id).map(|(_, t)| t)
    }

    pub fn open_stream_count(&self) -> u64 {
        self.open_streams.load(Ordering::Relaxed)
    }

    /// Opens a new multiplexed stream and tells the agent about it.
    ///
    /// # Errors
    /// See [`OpenError`].
    pub async fn open_http_stream(
        &self,
        tunnel_id: &str,
        head: HttpRequestHead,
        remote_addr: String,
    ) -> Result<StreamResponse, OpenError> {
        let limit = self.principal.limits.max_concurrent_streams;
        if limit > 0 && self.open_streams.load(Ordering::Relaxed) >= u64::from(limit) {
            return Err(OpenError::TooManyStreams(limit));
        }

        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let (head_tx, head_rx) = oneshot::channel();
        let (body_tx, body_rx) = mpsc::channel(STREAM_BUFFER_FRAMES);

        let stream = Arc::new(Stream {
            head_tx: std::sync::Mutex::new(Some(head_tx)),
            body_tx,
            send_credit: Arc::new(Semaphore::new(self.initial_window as usize)),
            closed: AtomicBool::new(false),
        });
        self.streams.insert(stream_id, stream);
        self.open_streams.fetch_add(1, Ordering::Relaxed);

        let opened = self
            .send(ServerFrame {
                payload: Some(server_frame::Payload::Open(StreamOpen {
                    stream_id,
                    tunnel_id: tunnel_id.to_string(),
                    kind: StreamKind::Http as i32,
                    http: Some(head),
                    remote_addr,
                    initial_window: self.initial_window,
                })),
            })
            .await;

        if !opened {
            self.close_stream(stream_id);
            return Err(OpenError::SessionGone);
        }

        Ok(StreamResponse {
            stream_id,
            head: head_rx,
            body: body_rx,
        })
    }

    /// Sends body bytes to the agent, respecting the per-stream send window.
    ///
    /// Returns `false` if the stream or session went away mid-transfer.
    pub async fn send_stream_data(&self, stream_id: u64, payload: Bytes) -> bool {
        let credit = match self.streams.get(&stream_id) {
            Some(stream) => Arc::clone(&stream.send_credit),
            None => return false,
        };

        for chunk in split_chunks(payload) {
            // Block until the agent has advertised room for these bytes. This
            // is the whole point of flow control: a slow local service must
            // slow the public upload down, not consume edge memory unboundedly.
            let permits = u32::try_from(chunk.len()).unwrap_or(u32::MAX);
            let Ok(permit) = credit.acquire_many(permits).await else {
                return false; // semaphore closed == stream torn down
            };
            permit.forget();

            let sent = self
                .send(ServerFrame {
                    payload: Some(server_frame::Payload::Data(StreamData {
                        stream_id,
                        payload: chunk,
                    })),
                })
                .await;
            if !sent {
                return false;
            }
        }
        true
    }

    /// Signals end-of-body for the server -> agent direction.
    pub async fn send_stream_end(&self, stream_id: u64) -> bool {
        self.send(ServerFrame {
            payload: Some(server_frame::Payload::End(StreamEnd {
                stream_id,
                trailers: Vec::new(),
            })),
        })
        .await
    }

    /// Grants the agent more send credit after we forwarded bytes downstream.
    pub async fn grant_window(&self, stream_id: u64, increment: u32) -> bool {
        if increment == 0 {
            return true;
        }
        self.send(ServerFrame {
            payload: Some(server_frame::Payload::WindowUpdate(WindowUpdate {
                stream_id,
                increment,
            })),
        })
        .await
    }

    /// Aborts a stream and tells the agent.
    pub async fn reset_stream(&self, stream_id: u64, code: ResetCode, message: &str) {
        self.close_stream(stream_id);
        self.send(ServerFrame {
            payload: Some(server_frame::Payload::Reset(StreamReset {
                stream_id,
                code: code as i32,
                message: message.to_string(),
            })),
        })
        .await;
    }

    // -- inbound frame handling (called by the control-plane read loop) -----

    /// Delivers the agent's response head to the waiting ingress task.
    pub fn on_response_head(&self, head: HttpResponseHead) {
        let Some(stream) = self.streams.get(&head.stream_id) else {
            return; // stream already torn down; nothing to do
        };
        let tx = stream
            .head_tx
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(tx) = tx {
            // The receiver is gone if the public client hung up first. That is
            // normal, not an error.
            let _ = tx.send(head);
        }
    }

    /// Delivers body bytes. Returns the number of bytes accepted, which the
    /// caller uses to decide how much window to grant back.
    pub async fn on_stream_data(&self, stream_id: u64, payload: Bytes) -> usize {
        let Some(tx) = self.stream_body_sender(stream_id) else {
            return 0;
        };
        let len = payload.len();
        if tx.send(Chunk::Data(payload)).await.is_err() {
            self.close_stream(stream_id);
            return 0;
        }
        len
    }

    /// Delivers a clean end-of-body.
    pub async fn on_stream_end(&self, stream_id: u64, trailers: Vec<Header>) {
        if let Some(tx) = self.stream_body_sender(stream_id) {
            let _ = tx.send(Chunk::End(trailers)).await;
        }
        self.close_stream(stream_id);
    }

    /// Delivers an abortive reset from the agent.
    pub async fn on_stream_reset(&self, stream_id: u64, code: ResetCode, message: String) {
        if let Some(tx) = self.stream_body_sender(stream_id) {
            let _ = tx.send(Chunk::Reset(code, message)).await;
        }
        self.close_stream(stream_id);
    }

    /// Applies credit granted by the agent for the server -> agent direction.
    pub fn on_window_update(&self, stream_id: u64, increment: u32) {
        if let Some(stream) = self.streams.get(&stream_id) {
            stream.send_credit.add_permits(increment as usize);
        }
    }

    fn stream_body_sender(&self, stream_id: u64) -> Option<mpsc::Sender<Chunk>> {
        self.streams
            .get(&stream_id)
            .filter(|s| !s.closed.load(Ordering::Relaxed))
            .map(|s| s.body_tx.clone())
    }

    /// Removes a stream and wakes anything blocked on its send window.
    pub fn close_stream(&self, stream_id: u64) {
        if let Some((_, stream)) = self.streams.remove(&stream_id) {
            stream.closed.store(true, Ordering::Relaxed);
            // Closing the semaphore unblocks `send_stream_data`, which would
            // otherwise wait forever for credit that will never arrive.
            stream.send_credit.close();
            self.open_streams.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Tears every stream down; called when the agent disconnects.
    pub fn close_all_streams(&self) {
        let ids: Vec<u64> = self.streams.iter().map(|e| *e.key()).collect();
        for id in ids {
            self.close_stream(id);
        }
    }
}

/// Splits a payload into frame-sized chunks without copying.
fn split_chunks(mut payload: Bytes) -> Vec<Bytes> {
    if payload.len() <= MAX_CHUNK {
        return vec![payload];
    }
    let mut chunks = Vec::with_capacity(payload.len().div_ceil(MAX_CHUNK));
    while payload.len() > MAX_CHUNK {
        chunks.push(payload.split_to(MAX_CHUNK));
    }
    chunks.push(payload);
    chunks
}

/// All sessions terminated by this node, keyed by session id.
#[derive(Default)]
pub struct SessionManager {
    sessions: DashMap<String, Arc<Session>>,
}

impl SessionManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, session: Arc<Session>) {
        self.sessions.insert(session.id.clone(), session);
    }

    #[must_use]
    pub fn get(&self, session_id: &str) -> Option<Arc<Session>> {
        self.sessions.get(session_id).map(|s| Arc::clone(s.value()))
    }

    pub fn remove(&self, session_id: &str) -> Option<Arc<Session>> {
        self.sessions.remove(session_id).map(|(_, s)| s)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Total tunnels across every local session, for `/healthz`.
    #[must_use]
    pub fn tunnel_count(&self) -> usize {
        self.sessions.iter().map(|s| s.tunnel_count()).sum()
    }

    /// Snapshot of every local session, for the admin endpoint.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<String, Vec<Tunnel>> {
        self.sessions
            .iter()
            .map(|s| (s.key().clone(), s.value().tunnels()))
            .collect()
    }

    /// The same snapshot, shaped for the internal JSON endpoint.
    #[must_use]
    pub fn snapshot_json(&self) -> serde_json::Value {
        let sessions: Vec<serde_json::Value> = self
            .sessions
            .iter()
            .map(|entry| {
                let session = entry.value();
                serde_json::json!({
                    "session_id": session.id,
                    "account_id": session.principal.account_id,
                    "open_streams": session.open_stream_count(),
                    "tunnels": session.tunnels().into_iter().map(|t| serde_json::json!({
                        "tunnel_id": t.tunnel_id,
                        "routing_key": t.routing_key,
                        "public_url": t.public_url,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        serde_json::json!({ "sessions": sessions })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use norupo_core::auth::Limits;

    fn principal(max_streams: u32) -> Principal {
        Principal {
            account_id: "acct_1".into(),
            display_name: "test".into(),
            reserved_subdomains: vec![],
            custom_domains: vec![],
            limits: Limits {
                max_concurrent_streams: max_streams,
                ..Limits::default()
            },
        }
    }

    fn head() -> HttpRequestHead {
        HttpRequestHead {
            method: "GET".into(),
            path: "/".into(),
            authority: "api.tunnel.com".into(),
            headers: vec![],
            version: "HTTP/1.1".into(),
            upgrade: String::new(),
        }
    }

    #[test]
    fn chunking_splits_on_the_frame_boundary_without_losing_bytes() {
        let payload = Bytes::from(vec![7u8; MAX_CHUNK * 2 + 13]);
        let chunks = split_chunks(payload.clone());

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), MAX_CHUNK);
        assert_eq!(chunks[1].len(), MAX_CHUNK);
        assert_eq!(chunks[2].len(), 13);
        assert_eq!(chunks.concat(), payload);
    }

    #[test]
    fn small_payloads_are_not_split() {
        assert_eq!(split_chunks(Bytes::from_static(b"hi")).len(), 1);
        // Exactly at the boundary must stay a single frame.
        assert_eq!(split_chunks(Bytes::from(vec![0u8; MAX_CHUNK])).len(), 1);
    }

    #[tokio::test]
    async fn opening_a_stream_emits_an_open_frame_to_the_agent() {
        let (session, mut rx) = Session::new("sess_1".into(), "node_1".into(), principal(0), 65536);

        let opened = session
            .open_http_stream("tun_1", head(), "1.2.3.4:5678".into())
            .await
            .expect("open");

        let frame = rx.recv().await.expect("frame").expect("ok");
        match frame.payload {
            Some(server_frame::Payload::Open(open)) => {
                assert_eq!(open.stream_id, opened.stream_id);
                assert_eq!(open.tunnel_id, "tun_1");
                assert_eq!(open.kind, StreamKind::Http as i32);
                assert_eq!(open.remote_addr, "1.2.3.4:5678");
            }
            other => panic!("expected Open, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn concurrent_stream_limits_are_enforced() {
        let (session, _rx) = Session::new("sess_1".into(), "node_1".into(), principal(1), 65536);

        session
            .open_http_stream("tun_1", head(), "1.2.3.4:1".into())
            .await
            .expect("first");
        let err = session
            .open_http_stream("tun_1", head(), "1.2.3.4:2".into())
            .await
            .expect_err("second must be refused");

        assert!(matches!(err, OpenError::TooManyStreams(1)));
    }

    #[tokio::test]
    async fn response_head_and_body_reach_the_waiting_ingress_task() {
        let (session, _rx) = Session::new("sess_1".into(), "node_1".into(), principal(0), 65536);
        let opened = session
            .open_http_stream("tun_1", head(), "1.2.3.4:1".into())
            .await
            .expect("open");
        let mut opened = opened;

        session.on_response_head(HttpResponseHead {
            stream_id: opened.stream_id,
            status: 204,
            headers: vec![],
        });
        assert_eq!(opened.head.await.expect("head").status, 204);

        let accepted = session
            .on_stream_data(opened.stream_id, Bytes::from_static(b"hello"))
            .await;
        assert_eq!(accepted, 5);
        session.on_stream_end(opened.stream_id, vec![]).await;

        match opened.body.recv().await {
            Some(Chunk::Data(b)) => assert_eq!(&b[..], b"hello"),
            other => panic!("expected data, got {other:?}"),
        }
        assert!(matches!(opened.body.recv().await, Some(Chunk::End(_))));
    }

    #[tokio::test]
    async fn flow_control_blocks_once_credit_is_exhausted_and_resumes_on_update() {
        // Window of 4 bytes: the first 4 go through, the next send must park
        // until the agent grants more credit.
        let (session, mut rx) = Session::new("sess_1".into(), "node_1".into(), principal(0), 4);
        let opened = session
            .open_http_stream("tun_1", head(), "1.2.3.4:1".into())
            .await
            .unwrap();
        let _open_frame = rx.recv().await.unwrap();

        let id = opened.stream_id;
        let s = Arc::clone(&session);
        let writer = tokio::spawn(async move {
            s.send_stream_data(id, Bytes::from_static(b"abcd")).await;
            s.send_stream_data(id, Bytes::from_static(b"efgh")).await;
        });

        // First 4 bytes are within the window.
        let first = rx.recv().await.unwrap().unwrap();
        assert!(
            matches!(first.payload, Some(server_frame::Payload::Data(d)) if &d.payload[..] == b"abcd")
        );

        // The second send is parked: nothing more should arrive.
        let parked = tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;
        assert!(parked.is_err(), "sender ignored the flow-control window");

        session.on_window_update(id, 4);
        let second = rx.recv().await.unwrap().unwrap();
        assert!(
            matches!(second.payload, Some(server_frame::Payload::Data(d)) if &d.payload[..] == b"efgh")
        );

        writer.await.unwrap();
    }

    #[tokio::test]
    async fn closing_a_stream_unparks_a_blocked_writer() {
        // Without closing the semaphore, a disconnected agent would leak a
        // task blocked forever on credit.
        let (session, mut rx) = Session::new("sess_1".into(), "node_1".into(), principal(0), 2);
        let opened = session
            .open_http_stream("tun_1", head(), "1.2.3.4:1".into())
            .await
            .unwrap();
        let _ = rx.recv().await;

        let id = opened.stream_id;
        let s = Arc::clone(&session);
        let writer =
            tokio::spawn(async move { s.send_stream_data(id, Bytes::from(vec![0u8; 64])).await });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        session.close_stream(id);

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), writer).await;
        let sent = result
            .expect("writer never unparked")
            .expect("writer task panicked");
        assert!(!sent, "a send on a closed stream must report failure");
    }

    #[tokio::test]
    async fn session_manager_tracks_sessions_and_tunnels() {
        let manager = SessionManager::new();
        let (session, _rx) = Session::new("sess_1".into(), "node_1".into(), principal(0), 65536);
        session.insert_tunnel(Tunnel {
            tunnel_id: "tun_1".into(),
            routing_key: "api.tunnel.com".into(),
            public_url: "https://api.tunnel.com".into(),
        });
        manager.insert(Arc::clone(&session));

        assert_eq!(manager.len(), 1);
        assert_eq!(manager.tunnel_count(), 1);
        assert!(manager.get("sess_1").is_some());

        manager.remove("sess_1");
        assert!(manager.is_empty());
        assert!(manager.get("sess_1").is_none());
    }
}
