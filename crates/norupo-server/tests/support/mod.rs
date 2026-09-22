//! Shared fixtures for the end-to-end tests.
//!
//! Every helper binds on port 0 and reports the address it got, so the suite
//! can run in parallel (and in CI, alongside anything else) without fixed-port
//! collisions.

#![allow(dead_code)]

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use norupo_client::{AgentEvent, AgentOptions};
use norupo_server::{start, RunningServer, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

/// How long a test waits for a tunnel to come online before failing.
pub const READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// A local "user application" the agent forwards to.
pub struct LocalApp {
    pub addr: SocketAddr,
}

/// Starts a local HTTP app that echoes what it was asked.
///
/// Routes:
/// * `GET  /`        -> `200 hello from local`
/// * `POST /echo`    -> `200` with the request body echoed back
/// * `GET  /big/:n`  -> `200` with `n` bytes of payload
/// * `GET  /headers` -> `200` with two `Set-Cookie` headers
/// * `GET  /teapot`  -> `418`
pub async fn start_local_app() -> LocalApp {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local app");
    let addr = listener.local_addr().expect("local app addr");

    tokio::spawn(async move {
        let builder = ConnBuilder::new(TokioExecutor::new());
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let builder = builder.clone();
            tokio::spawn(async move {
                let _ = builder
                    .serve_connection(TokioIo::new(stream), service_fn(local_app_handler))
                    .await;
            });
        }
    });

    LocalApp { addr }
}

async fn local_app_handler(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let forwarded_host = req
        .headers()
        .get("x-forwarded-host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let response = match (method.as_str(), path.as_str()) {
        ("GET", "/") => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain")
            .header("x-app", "local")
            .body(Full::new(Bytes::from_static(b"hello from local")))
            .unwrap(),

        ("POST", "/echo") => {
            let body = req
                .into_body()
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/octet-stream")
                .body(Full::new(body))
                .unwrap()
        }

        ("GET", "/headers") => Response::builder()
            .status(StatusCode::OK)
            .header("set-cookie", "a=1")
            .header("set-cookie", "b=2")
            .header("x-forwarded-seen", forwarded_host)
            .body(Full::new(Bytes::from_static(b"headers")))
            .unwrap(),

        ("GET", "/teapot") => Response::builder()
            .status(StatusCode::IM_A_TEAPOT)
            .body(Full::new(Bytes::from_static(b"short and stout")))
            .unwrap(),

        ("GET", p) if p.starts_with("/big/") => {
            let n: usize = p.trim_start_matches("/big/").parse().unwrap_or(0);
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from(vec![b'x'; n])))
                .unwrap()
        }

        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"local: not found")))
            .unwrap(),
    };

    Ok(response)
}

/// Starts an edge server bound to ephemeral ports, with anonymous auth.
pub async fn start_edge() -> RunningServer {
    let config = ServerConfig {
        control_addr: "127.0.0.1:0".parse().unwrap(),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        peer_addr: "127.0.0.1:0".parse().unwrap(),
        base_domain: "localhost".into(),
        public_scheme: "http".into(),
        allow_anonymous: true,
        heartbeat_ms: 1_000,
        response_timeout_ms: 5_000,
        ..ServerConfig::default()
    };
    start(config).await.expect("edge server should start")
}

/// A running agent plus the public URL it was granted.
pub struct RunningAgent {
    pub public_url: String,
    pub host: String,
    shutdown: watch::Sender<bool>,
}

impl RunningAgent {
    pub fn stop(&self) {
        let _ = self.shutdown.send(true);
    }
}

/// Starts an agent against `edge`, forwarding to `target`, and waits until its
/// tunnel is online.
///
/// Returns `Err` with the edge's rejection reason when registration fails, so
/// tests can assert on it.
pub async fn start_agent(
    edge: &RunningServer,
    target: SocketAddr,
    subdomain: Option<&str>,
) -> Result<RunningAgent, String> {
    let options = AgentOptions {
        server: format!("http://{}", edge.control_addr),
        token: "test-token".into(),
        forward_addr: target.to_string(),
        subdomain: subdomain.map(str::to_string),
        domain: None,
        rewrite_host: false,
        max_retries: 3,
    };

    let (events_tx, mut events_rx) = mpsc::channel(64);
    let (shutdown, shutdown_rx) = watch::channel(false);
    tokio::spawn(norupo_client::run(options, events_tx, shutdown_rx));

    let wait = async {
        while let Some(event) = events_rx.recv().await {
            match event {
                AgentEvent::TunnelReady(status) => {
                    let host = status
                        .public_url
                        .split("://")
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();
                    return Ok(RunningAgent {
                        public_url: status.public_url,
                        host,
                        shutdown: shutdown.clone(),
                    });
                }
                AgentEvent::TunnelRejected(reason) => return Err(reason),
                _ => {}
            }
        }
        Err("agent stopped before its tunnel came online".to_string())
    };

    tokio::time::timeout(READY_TIMEOUT, wait)
        .await
        .unwrap_or_else(|_| Err("timed out waiting for the tunnel to come online".into()))
}

/// A response collected from the public edge.
pub struct PublicResponse {
    pub status: StatusCode,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

/// Sends a request to the edge's public listener with an explicit `Host`.
///
/// This is exactly what a browser does once DNS has resolved the tunnel
/// hostname to the edge's address.
pub async fn public_request(
    edge: &RunningServer,
    host: &str,
    method: &str,
    path: &str,
    body: Bytes,
) -> PublicResponse {
    let client: hyper_util::client::legacy::Client<_, Full<Bytes>> =
        hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .build(hyper_util::client::legacy::connect::HttpConnector::new());

    let request = Request::builder()
        .method(method)
        .uri(format!("http://{}{}", edge.http_addr, path))
        // The edge routes on this header, not on the address we dialled.
        .header("host", host)
        .body(Full::new(body))
        .expect("valid request");

    let response = client.request(request).await.expect("edge should answer");
    let (parts, body) = response.into_parts();
    let body = body
        .collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default();

    PublicResponse {
        status: parts.status,
        headers: parts.headers,
        body,
    }
}
