//! End-to-end tests: a real edge, a real agent, a real local app, real HTTP.
//!
//! These are the tests that would have caught every routing bug worth catching.
//! They exercise the full path — `Host` header -> routing table -> gRPC session
//! -> multiplexed stream -> local service -> back again — with no mocks in the
//! middle.

mod support;

use bytes::Bytes;
use http::StatusCode;
use support::{public_request, start_agent, start_edge, start_local_app};

#[tokio::test]
async fn a_public_get_reaches_the_local_service() {
    let app = start_local_app().await;
    let edge = start_edge().await;
    let agent = start_agent(&edge, app.addr, Some("my-app"))
        .await
        .expect("tunnel");

    assert_eq!(agent.public_url, "http://my-app.localhost");

    let response = public_request(&edge, &agent.host, "GET", "/", Bytes::new()).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, Bytes::from_static(b"hello from local"));
    // The local service's own headers must survive the round trip.
    assert_eq!(response.headers.get("x-app").unwrap(), "local");

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn request_and_response_bodies_survive_the_round_trip() {
    let app = start_local_app().await;
    let edge = start_edge().await;
    let agent = start_agent(&edge, app.addr, Some("echo-app"))
        .await
        .expect("tunnel");

    let payload = Bytes::from_static(b"the quick brown fox jumps over the lazy dog");
    let response = public_request(&edge, &agent.host, "POST", "/echo", payload.clone()).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, payload);

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn payloads_larger_than_the_flow_control_window_transfer_intact() {
    // 1 MiB each way, against a 256 KiB window: this only passes if window
    // updates are actually flowing in both directions.
    const SIZE: usize = 1024 * 1024;

    let app = start_local_app().await;
    let edge = start_edge().await;
    let agent = start_agent(&edge, app.addr, Some("big-app"))
        .await
        .expect("tunnel");

    // Response direction (agent -> edge -> public client).
    let download = public_request(
        &edge,
        &agent.host,
        "GET",
        &format!("/big/{SIZE}"),
        Bytes::new(),
    )
    .await;
    assert_eq!(download.status, StatusCode::OK);
    assert_eq!(download.body.len(), SIZE, "downloaded body was truncated");
    assert!(
        download.body.iter().all(|&b| b == b'x'),
        "downloaded body was corrupted"
    );

    // Request direction (public client -> edge -> agent).
    let payload = Bytes::from(vec![7u8; SIZE]);
    let upload = public_request(&edge, &agent.host, "POST", "/echo", payload.clone()).await;
    assert_eq!(upload.status, StatusCode::OK);
    assert_eq!(upload.body.len(), SIZE, "uploaded body was truncated");
    assert_eq!(upload.body, payload, "uploaded body was corrupted");

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn status_codes_and_repeated_headers_are_preserved() {
    let app = start_local_app().await;
    let edge = start_edge().await;
    let agent = start_agent(&edge, app.addr, Some("hdr-app"))
        .await
        .expect("tunnel");

    let teapot = public_request(&edge, &agent.host, "GET", "/teapot", Bytes::new()).await;
    assert_eq!(teapot.status, StatusCode::IM_A_TEAPOT);

    let headers = public_request(&edge, &agent.host, "GET", "/headers", Bytes::new()).await;
    assert_eq!(headers.status, StatusCode::OK);
    // Two distinct Set-Cookie headers must not be collapsed into one.
    let cookies: Vec<&str> = headers
        .headers
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap())
        .collect();
    assert_eq!(
        cookies.len(),
        2,
        "Set-Cookie headers were collapsed: {cookies:?}"
    );
    assert!(cookies.contains(&"a=1") && cookies.contains(&"b=2"));

    // The agent tells the local service the original public hostname.
    assert_eq!(
        headers.headers.get("x-forwarded-seen").unwrap(),
        agent.host.as_str()
    );

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn query_strings_and_paths_arrive_unmangled() {
    let app = start_local_app().await;
    let edge = start_edge().await;
    let agent = start_agent(&edge, app.addr, Some("path-app"))
        .await
        .expect("tunnel");

    // The local app 404s anything it does not know, which proves the full path
    // (including the query) reached it rather than being rewritten to `/`.
    let response = public_request(&edge, &agent.host, "GET", "/nope?a=1&b=2", Bytes::new()).await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
    assert_eq!(response.body, Bytes::from_static(b"local: not found"));

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn an_unclaimed_host_gets_the_edges_own_404() {
    let edge = start_edge().await;

    let response =
        public_request(&edge, "nobody-is-here.localhost", "GET", "/", Bytes::new()).await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
    // Distinguishable from the user app's own 404s.
    assert_eq!(
        response.headers.get("x-norupo-error").unwrap(),
        "tunnel_not_found"
    );

    edge.shutdown().await;
}

#[tokio::test]
async fn host_matching_is_case_and_port_insensitive() {
    let app = start_local_app().await;
    let edge = start_edge().await;
    let agent = start_agent(&edge, app.addr, Some("case-app"))
        .await
        .expect("tunnel");

    for host in [
        "CASE-APP.localhost",
        "case-app.LOCALHOST",
        "case-app.localhost:8080",
    ] {
        let response = public_request(&edge, host, "GET", "/", Bytes::new()).await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "host {host} failed to route"
        );
    }

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn a_dead_local_service_produces_a_502_not_a_hang() {
    // Bind a port, learn its address, then drop the listener: now nothing is
    // listening there, which is the most common real-world failure.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = listener.local_addr().unwrap();
    drop(listener);

    let edge = start_edge().await;
    let agent = start_agent(&edge, dead_addr, Some("dead-app"))
        .await
        .expect("tunnel");

    let response = public_request(&edge, &agent.host, "GET", "/", Bytes::new()).await;

    assert_eq!(response.status, StatusCode::BAD_GATEWAY);
    assert!(
        String::from_utf8_lossy(&response.body).contains("Local service unreachable"),
        "unexpected error page: {}",
        String::from_utf8_lossy(&response.body)
    );

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn two_agents_cannot_claim_the_same_subdomain() {
    let app = start_local_app().await;
    let edge = start_edge().await;

    let first = start_agent(&edge, app.addr, Some("contested"))
        .await
        .expect("first agent");
    let second = start_agent(&edge, app.addr, Some("contested")).await;

    match second {
        Err(reason) => assert!(
            reason.contains("already claimed"),
            "expected a route-taken rejection, got: {reason}"
        ),
        Ok(agent) => panic!("two agents both claimed {}", agent.host),
    }

    // The winner keeps serving.
    let response = public_request(&edge, &first.host, "GET", "/", Bytes::new()).await;
    assert_eq!(response.status, StatusCode::OK);

    first.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn a_randomly_assigned_subdomain_works_without_being_requested() {
    let app = start_local_app().await;
    let edge = start_edge().await;
    let agent = start_agent(&edge, app.addr, None).await.expect("tunnel");

    assert!(
        agent.host.ends_with(".localhost"),
        "unexpected host: {}",
        agent.host
    );
    let response = public_request(&edge, &agent.host, "GET", "/", Bytes::new()).await;
    assert_eq!(response.status, StatusCode::OK);

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn reserved_subdomains_are_refused() {
    let app = start_local_app().await;
    let edge = start_edge().await;

    // `admin` is on the reserved list and this account was granted nothing.
    let result = start_agent(&edge, app.addr, Some("admin")).await;

    match result {
        Err(reason) => assert!(reason.contains("reserved"), "unexpected reason: {reason}"),
        Ok(agent) => panic!("agent was allowed to claim {}", agent.host),
    }

    edge.shutdown().await;
}

#[tokio::test]
async fn concurrent_requests_are_multiplexed_over_one_session() {
    let app = start_local_app().await;
    let edge = start_edge().await;
    let agent = start_agent(&edge, app.addr, Some("mux-app"))
        .await
        .expect("tunnel");

    // 50 requests in flight over a single gRPC stream. If stream ids or the
    // body channels were mixed up, bodies would come back cross-wired.
    let mut tasks = Vec::new();
    for i in 0..50u32 {
        let host = agent.host.clone();
        let http_addr = edge.http_addr;
        tasks.push(tokio::spawn(async move {
            let client: hyper_util::client::legacy::Client<_, http_body_util::Full<Bytes>> =
                hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                    .build(hyper_util::client::legacy::connect::HttpConnector::new());
            let payload = Bytes::from(format!("payload-{i}"));
            let request = http::Request::builder()
                .method("POST")
                .uri(format!("http://{http_addr}/echo"))
                .header("host", host)
                .body(http_body_util::Full::new(payload.clone()))
                .unwrap();
            let response = client.request(request).await.expect("response");
            let body = http_body_util::BodyExt::collect(response.into_body())
                .await
                .map(|c| c.to_bytes())
                .unwrap();
            (payload, body)
        }));
    }

    for task in tasks {
        let (sent, received) = task.await.expect("task");
        assert_eq!(
            sent, received,
            "a multiplexed response came back cross-wired"
        );
    }

    agent.stop();
    edge.shutdown().await;
}

#[tokio::test]
async fn the_public_health_endpoint_answers_without_a_tunnel() {
    let edge = start_edge().await;

    let response = public_request(
        &edge,
        "anything.localhost",
        "GET",
        "/__norupo/healthz",
        Bytes::new(),
    )
    .await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, Bytes::from_static(b"ok\n"));

    edge.shutdown().await;
}
