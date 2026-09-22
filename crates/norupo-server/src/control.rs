//! The gRPC control plane: `TunnelControl.Session`.
//!
//! Each accepted RPC owns one agent session for its whole lifetime. The shape
//! is:
//!
//! ```text
//!   Hello ──▶ authenticate ──▶ Welcome
//!                                │
//!            ┌───────────────────┼───────────────────┐
//!            ▼                   ▼                   ▼
//!      read loop            heartbeat            (ingress opens
//!   (agent -> edge)      (Ping + renew TTL)       streams here)
//!            │
//!            ▼
//!     disconnect ──▶ release routing-table claims, drop streams
//! ```

use std::sync::Arc;
use std::time::Duration;

use norupo_core::registry::{Claim, RouteRecord};
use norupo_core::routing::{self, RoutingKey};
use norupo_core::{ids, Error};
use norupo_proto::tunnel_control_server::{TunnelControl, TunnelControlServer};
use norupo_proto::{
    client_frame, open_tunnel, server_frame, ClientFrame, HealthRequest, HealthResponse,
    HttpTunnelSpec, Limits, OpenTunnel, Ping, ResetCode, ServerFrame, TcpTunnelSpec, TunnelStatus,
    Welcome, PROTOCOL_VERSION,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::session::{Session, Tunnel};
use crate::state::Shared;

/// gRPC service implementation.
pub struct ControlService {
    state: Shared,
}

impl ControlService {
    #[must_use]
    pub fn new(state: Shared) -> Self {
        Self { state }
    }

    /// Wraps this service in its tonic server, ready to add to a router.
    #[must_use]
    pub fn into_server(self) -> TunnelControlServer<Self> {
        TunnelControlServer::new(self)
    }
}

#[tonic::async_trait]
impl TunnelControl for ControlService {
    type SessionStream = ReceiverStream<Result<ServerFrame, Status>>;

    async fn session(
        &self,
        request: Request<Streaming<ClientFrame>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let peer = request
            .remote_addr()
            .map_or_else(|| "unknown".to_string(), |a| a.to_string());
        let mut inbound = request.into_inner();

        // The first frame must be `Hello`. Authenticating before allocating
        // any per-session state keeps an unauthenticated peer from costing us
        // more than one channel read.
        let hello = match inbound.next().await {
            Some(Ok(ClientFrame {
                payload: Some(client_frame::Payload::Hello(hello)),
            })) => hello,
            Some(Ok(_)) => {
                return Err(Status::failed_precondition("first frame must be Hello"));
            }
            Some(Err(status)) => return Err(status),
            None => return Err(Status::failed_precondition("stream closed before Hello")),
        };

        if hello.protocol_version != PROTOCOL_VERSION {
            return Err(Status::failed_precondition(format!(
                "unsupported protocol version {} (server speaks {PROTOCOL_VERSION}); upgrade the norupo CLI",
                hello.protocol_version
            )));
        }

        let principal = self
            .state
            .auth
            .authenticate(&hello.token)
            .await
            .map_err(|e| Status::unauthenticated(e.to_string()))?;

        let session_id = ids::session_id();
        let (session, rx) = Session::new(
            session_id.clone(),
            self.state.node_id.clone(),
            principal,
            self.state.config.window_bytes,
        );

        let heartbeat_ms = u32::try_from(self.state.config.heartbeat_ms).unwrap_or(u32::MAX);
        let limits = session.principal.limits;
        let welcome = ServerFrame {
            payload: Some(server_frame::Payload::Welcome(Welcome {
                session_id: session_id.clone(),
                server_version: self.state.version().to_string(),
                node_id: self.state.node_id.clone(),
                heartbeat_interval_ms: heartbeat_ms,
                initial_window: self.state.config.window_bytes,
                limits: Some(Limits {
                    max_tunnels: limits.max_tunnels,
                    max_concurrent_streams: limits.max_concurrent_streams,
                    max_bytes_per_second: limits.max_bytes_per_second,
                }),
            })),
        };
        if !session.send(welcome).await {
            return Err(Status::unavailable("agent hung up during handshake"));
        }

        self.state.sessions.insert(Arc::clone(&session));
        info!(
            session_id = %session_id,
            account = %session.principal.account_id,
            agent_version = %hello.agent_version,
            %peer,
            "agent session established"
        );

        // Drive the session on its own task so the RPC can return its response
        // stream immediately.
        let state = Arc::clone(&self.state);
        let driver = Arc::clone(&session);
        tokio::spawn(async move {
            run_session(state, driver, inbound).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ok: true,
            node_id: self.state.node_id.clone(),
            server_version: self.state.version().to_string(),
            active_sessions: self.state.sessions.len() as u64,
            active_tunnels: self.state.sessions.tunnel_count() as u64,
        }))
    }
}

/// Owns a session for its lifetime: reads frames, heartbeats, cleans up.
async fn run_session(state: Shared, session: Arc<Session>, mut inbound: Streaming<ClientFrame>) {
    let heartbeat = spawn_heartbeat(Arc::clone(&state), Arc::clone(&session));

    loop {
        match inbound.next().await {
            Some(Ok(frame)) => handle_client_frame(&state, &session, frame).await,
            Some(Err(status)) => {
                debug!(session_id = %session.id, %status, "agent stream failed");
                break;
            }
            // Clean EOF: the agent shut down or the network dropped.
            None => break,
        }
    }

    heartbeat.abort();
    teardown(&state, &session).await;
}

/// Pings the agent and renews every routing-table claim it owns.
///
/// Renewal is what makes the routing table self-healing: if this node dies, no
/// renewals happen and the TTL reaps the routes within `route_ttl`.
fn spawn_heartbeat(state: Shared, session: Arc<Session>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = state.config.heartbeat();
        let ttl = state.config.route_ttl();
        let mut ticker = tokio::time::interval(interval);
        // We only care that beats happen, not that missed ones are made up.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // the first tick completes immediately

        loop {
            ticker.tick().await;

            let nonce = rand_nonce();
            let ping = ServerFrame {
                payload: Some(server_frame::Payload::Ping(Ping {
                    nonce,
                    server_time_ms: now_ms(),
                })),
            };
            if !session.send(ping).await {
                return; // agent is gone; the read loop is already unwinding
            }

            for tunnel in session.tunnels() {
                match state
                    .registry
                    .renew(&tunnel.routing_key, &session.id, ttl)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        // Someone else owns our key now. Continuing would mean
                        // silently serving nothing, so surface it instead.
                        warn!(
                            session_id = %session.id,
                            routing_key = %tunnel.routing_key,
                            "lost routing-table claim; dropping tunnel"
                        );
                        session.remove_tunnel(&tunnel.tunnel_id);
                        notify_tunnel_lost(&session, &tunnel).await;
                    }
                    Err(e) => {
                        // A transient Redis blip must not tear down live
                        // tunnels: the TTL gives us `route_ttl` of slack.
                        warn!(session_id = %session.id, error = %e, "routing-table renew failed");
                    }
                }
            }
        }
    })
}

async fn notify_tunnel_lost(session: &Session, tunnel: &Tunnel) {
    session
        .send(ServerFrame {
            payload: Some(server_frame::Payload::TunnelStatus(TunnelStatus {
                request_id: String::new(),
                tunnel_id: tunnel.tunnel_id.clone(),
                accepted: false,
                error: "routing claim lost; reconnect to re-register".into(),
                public_url: tunnel.public_url.clone(),
                routing_key: tunnel.routing_key.clone(),
            })),
        })
        .await;
}

/// Releases claims and drops streams once a session ends.
async fn teardown(state: &Shared, session: &Arc<Session>) {
    for tunnel in session.tunnels() {
        // Owner-checked release: if a newer session already took the key over,
        // this is a no-op rather than a blackhole.
        if let Err(e) = state
            .registry
            .release(&tunnel.routing_key, &session.id)
            .await
        {
            warn!(routing_key = %tunnel.routing_key, error = %e, "failed to release routing claim");
        }
    }
    session.close_all_streams();
    state.sessions.remove(&session.id);
    info!(session_id = %session.id, "agent session closed");
}

async fn handle_client_frame(state: &Shared, session: &Arc<Session>, frame: ClientFrame) {
    let Some(payload) = frame.payload else { return };

    match payload {
        client_frame::Payload::Hello(_) => {
            // A second Hello is a protocol violation, but not worth killing a
            // live session over; log and ignore.
            warn!(session_id = %session.id, "ignoring duplicate Hello");
        }
        client_frame::Payload::OpenTunnel(open) => {
            let status = register_tunnel(state, session, open).await;
            session
                .send(ServerFrame {
                    payload: Some(server_frame::Payload::TunnelStatus(status)),
                })
                .await;
        }
        client_frame::Payload::CloseTunnel(close) => {
            if let Some(tunnel) = session.remove_tunnel(&close.tunnel_id) {
                let _ = state
                    .registry
                    .release(&tunnel.routing_key, &session.id)
                    .await;
                info!(session_id = %session.id, routing_key = %tunnel.routing_key, "tunnel closed");
            }
        }
        client_frame::Payload::ResponseHead(head) => session.on_response_head(head),
        client_frame::Payload::Data(data) => {
            let stream_id = data.stream_id;
            let accepted = session.on_stream_data(stream_id, data.payload).await;
            // Return credit only for bytes we actually handed downstream.
            if accepted > 0 {
                session
                    .grant_window(stream_id, u32::try_from(accepted).unwrap_or(u32::MAX))
                    .await;
            }
        }
        client_frame::Payload::End(end) => session.on_stream_end(end.stream_id, end.trailers).await,
        client_frame::Payload::Reset(reset) => {
            let code = ResetCode::try_from(reset.code).unwrap_or(ResetCode::Unspecified);
            session
                .on_stream_reset(reset.stream_id, code, reset.message)
                .await;
        }
        client_frame::Payload::WindowUpdate(update) => {
            session.on_window_update(update.stream_id, update.increment);
        }
        client_frame::Payload::Pong(_) => { /* liveness only; timing is logged by the agent */ }
    }
}

/// Validates and claims a tunnel registration.
async fn register_tunnel(state: &Shared, session: &Arc<Session>, open: OpenTunnel) -> TunnelStatus {
    let request_id = open.request_id.clone();
    let reject = |error: String| TunnelStatus {
        request_id: request_id.clone(),
        tunnel_id: String::new(),
        accepted: false,
        error,
        public_url: String::new(),
        routing_key: String::new(),
    };

    let max_tunnels = session.principal.limits.max_tunnels;
    if max_tunnels > 0 && session.tunnel_count() >= max_tunnels as usize {
        return reject(format!(
            "account is limited to {max_tunnels} concurrent tunnels"
        ));
    }

    let (routing_key, public_url) = match open.spec {
        Some(open_tunnel::Spec::Http(spec)) => match resolve_http_target(state, session, &spec) {
            Ok(pair) => pair,
            Err(e) => return reject(e.to_string()),
        },
        Some(open_tunnel::Spec::Tcp(spec)) => match resolve_tcp_target(state, &spec) {
            Ok(pair) => pair,
            Err(e) => return reject(e.to_string()),
        },
        None => return reject("OpenTunnel is missing a spec".into()),
    };

    let tunnel_id = ids::tunnel_id();
    let record = RouteRecord {
        routing_key: routing_key.as_storage_key(),
        node_id: state.node_id.clone(),
        node_addr: state.node_addr.clone(),
        session_id: session.id.clone(),
        tunnel_id: tunnel_id.clone(),
        account_id: session.principal.account_id.clone(),
    };

    match state
        .registry
        .claim(&record, state.config.route_ttl())
        .await
    {
        Ok(Claim::Acquired) => {
            let tunnel = Tunnel {
                tunnel_id: tunnel_id.clone(),
                routing_key: record.routing_key.clone(),
                public_url: public_url.clone(),
            };
            session.insert_tunnel(tunnel);
            info!(
                session_id = %session.id,
                routing_key = %record.routing_key,
                %public_url,
                "tunnel registered"
            );
            TunnelStatus {
                request_id,
                tunnel_id,
                accepted: true,
                error: String::new(),
                public_url,
                routing_key: record.routing_key,
            }
        }
        Ok(Claim::Taken(owner)) => reject(
            Error::RouteTaken(format!(
                "{} (held by session {} on node {})",
                record.routing_key, owner.session_id, owner.node_id
            ))
            .to_string(),
        ),
        Err(e) => reject(e.to_string()),
    }
}

/// Turns an HTTP tunnel request into a routing key plus the URL we show the user.
fn resolve_http_target(
    state: &Shared,
    session: &Arc<Session>,
    spec: &HttpTunnelSpec,
) -> Result<(RoutingKey, String), Error> {
    let host = if !spec.custom_domain.is_empty() {
        let domain = routing::normalize_host(&spec.custom_domain)?;
        if !session.principal.may_use_custom_domain(&domain) {
            return Err(Error::Unauthorized(format!(
                "custom domain `{domain}` is not verified for this account"
            )));
        }
        domain
    } else if spec.subdomain.is_empty() {
        // No preference: hand out a random one.
        routing::public_host(&routing::random_subdomain(), &state.config.base_domain)?
    } else {
        let label = routing::validate_subdomain_syntax(&spec.subdomain)?;
        // Reserved labels are claimable only by accounts explicitly granted
        // them; everything else is first-come-first-served, arbitrated by the
        // routing table's atomic claim.
        if routing::is_reserved_subdomain(&label) && !session.principal.may_claim_subdomain(&label)
        {
            return Err(Error::Unauthorized(format!(
                "subdomain `{label}` is reserved"
            )));
        }
        routing::public_host(&label, &state.config.base_domain)?
    };

    let url = state.config.public_url(&host);
    Ok((RoutingKey::Host(host), url))
}

fn resolve_tcp_target(state: &Shared, spec: &TcpTunnelSpec) -> Result<(RoutingKey, String), Error> {
    let port = u16::try_from(spec.remote_port)
        .map_err(|_| Error::InvalidRoute(format!("port {} is out of range", spec.remote_port)))?;
    if port == 0 {
        return Err(Error::InvalidRoute(
            "automatic TCP port assignment is not implemented yet; pass --remote-port".into(),
        ));
    }
    let url = format!("tcp://{}:{port}", state.config.base_domain);
    Ok((RoutingKey::TcpPort(port), url))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn rand_nonce() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    // A nonce only has to be unpredictable enough to match a Pong to its Ping;
    // the default hasher's random seed is plenty and costs no dependency.
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

/// Exposed for tests and for the ingress, which shares the same TTL policy.
#[must_use]
pub fn default_route_ttl(heartbeat: Duration) -> Duration {
    heartbeat * 3
}
