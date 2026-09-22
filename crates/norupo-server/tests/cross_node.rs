//! Horizontal-scaling tests: two edge nodes, one shared routing table.
//!
//! This is the scenario a load balancer creates every day in production — the
//! public request lands on a node that does *not* hold the agent's session.
//! Both nodes run in this process and share one [`MemoryRegistry`], which
//! stands in for the Redis every node would point at in a real cluster.

mod support;

use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use norupo_core::registry::{MemoryRegistry, SharedRegistry};
use norupo_server::{start_with_registry, RunningServer, ServerConfig};
use support::{public_request, start_agent, start_local_app};

/// Starts an edge node on the shared routing table.
async fn start_node(registry: SharedRegistry, node_id: &str) -> RunningServer {
    let config = ServerConfig {
        control_addr: "127.0.0.1:0".parse().unwrap(),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        peer_addr: "127.0.0.1:0".parse().unwrap(),
        base_domain: "localhost".into(),
        public_scheme: "http".into(),
        allow_anonymous: true,
        node_id: Some(node_id.to_string()),
        heartbeat_ms: 1_000,
        response_timeout_ms: 5_000,
        ..ServerConfig::default()
    };
    start_with_registry(config, registry)
        .await
        .expect("node should start")
}

#[tokio::test]
async fn a_request_landing_on_the_wrong_node_is_handed_to_the_right_one() {
    let registry: SharedRegistry = Arc::new(MemoryRegistry::new());
    let app = start_local_app().await;

    let node_a = start_node(Arc::clone(&registry), "node_a").await;
    let node_b = start_node(Arc::clone(&registry), "node_b").await;

    // The agent connects to node A, so node A holds its session.
    let agent = start_agent(&node_a, app.addr, Some("spread"))
        .await
        .expect("tunnel");

    // ...but the load balancer sends the public request to node B.
    let response = public_request(&node_b, &agent.host, "GET", "/", Bytes::new()).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, Bytes::from_static(b"hello from local"));

    agent.stop();
    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test]
async fn bodies_survive_a_cross_node_hand_off() {
    let registry: SharedRegistry = Arc::new(MemoryRegistry::new());
    let app = start_local_app().await;

    let node_a = start_node(Arc::clone(&registry), "node_a").await;
    let node_b = start_node(Arc::clone(&registry), "node_b").await;
    let agent = start_agent(&node_a, app.addr, Some("spread-body"))
        .await
        .expect("tunnel");

    // Large enough to cross the flow-control window on the far side of the hop.
    let payload = Bytes::from(vec![3u8; 512 * 1024]);
    let response = public_request(&node_b, &agent.host, "POST", "/echo", payload.clone()).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, payload, "body was corrupted crossing nodes");

    agent.stop();
    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test]
async fn either_node_can_serve_the_same_tunnel() {
    let registry: SharedRegistry = Arc::new(MemoryRegistry::new());
    let app = start_local_app().await;

    let node_a = start_node(Arc::clone(&registry), "node_a").await;
    let node_b = start_node(Arc::clone(&registry), "node_b").await;
    let agent = start_agent(&node_a, app.addr, Some("anycast"))
        .await
        .expect("tunnel");

    // A real LB has no affinity, so both paths must work interchangeably.
    for node in [&node_a, &node_b] {
        let response = public_request(node, &agent.host, "GET", "/", Bytes::new()).await;
        assert_eq!(response.status, StatusCode::OK);
    }

    agent.stop();
    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test]
async fn a_subdomain_claimed_on_one_node_is_refused_on_every_other() {
    // This is the property that makes the routing table worth having: the
    // claim is cluster-wide, not per-node.
    let registry: SharedRegistry = Arc::new(MemoryRegistry::new());
    let app = start_local_app().await;

    let node_a = start_node(Arc::clone(&registry), "node_a").await;
    let node_b = start_node(Arc::clone(&registry), "node_b").await;

    let first = start_agent(&node_a, app.addr, Some("global-claim"))
        .await
        .expect("first");
    let second = start_agent(&node_b, app.addr, Some("global-claim")).await;

    match second {
        Err(reason) => assert!(
            reason.contains("already claimed"),
            "expected a cluster-wide rejection, got: {reason}"
        ),
        Ok(agent) => panic!("node_b handed out {} which node_a already owns", agent.host),
    }

    first.stop();
    node_a.shutdown().await;
    node_b.shutdown().await;
}

#[tokio::test]
async fn releasing_a_tunnel_frees_the_name_for_another_node() {
    let registry: SharedRegistry = Arc::new(MemoryRegistry::new());
    let app = start_local_app().await;

    let node_a = start_node(Arc::clone(&registry), "node_a").await;
    let node_b = start_node(Arc::clone(&registry), "node_b").await;

    let first = start_agent(&node_a, app.addr, Some("handover"))
        .await
        .expect("first");
    first.stop();

    // The agent's session teardown releases the claim; give it a moment to
    // unwind, then a different node must be able to hand the name out.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let second = loop {
        match start_agent(&node_b, app.addr, Some("handover")).await {
            Ok(agent) => break agent,
            Err(reason) if std::time::Instant::now() < deadline => {
                assert!(
                    reason.contains("already claimed"),
                    "unexpected reason: {reason}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(reason) => panic!("name was never released: {reason}"),
        }
    };

    let response = public_request(&node_b, &second.host, "GET", "/", Bytes::new()).await;
    assert_eq!(response.status, StatusCode::OK);

    second.stop();
    node_a.shutdown().await;
    node_b.shutdown().await;
}
