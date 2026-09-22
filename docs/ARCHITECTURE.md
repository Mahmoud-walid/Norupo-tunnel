# Architecture

## The problem

A tunnel service has to accept a connection from the public internet and get it
to a process that is *not addressable* — behind NAT, behind a corporate proxy,
on a laptop that changes networks. The only thing that works is for the
unaddressable side to dial out and hold the connection open.

Everything below follows from that one constraint.

## The shape

```
                     ┌──────────────────────────────────────────┐
  public internet    │              edge node                   │
       │             │                                          │
       │  :80/:443   │  ┌────────────┐                          │
       ├────────────▶│  │  ingress   │──┐                       │
       │             │  └────────────┘  │  lookup(Host)         │
       │             │                  ▼                       │
       │             │           ┌─────────────┐                │
       │             │           │  routing    │◀──── Redis ────┼──▶ other nodes
       │             │           │   table     │                │
       │             │           └─────────────┘                │
       │             │                  │  session_id           │
       │             │                  ▼                       │
       │             │  ┌────────────────────────────────────┐  │
       │             │  │  session table (this node only)    │  │
       │             │  └────────────────────────────────────┘  │
       │             │                  │                       │
       │             │           ┌──────▼──────┐  :7000         │
       │             │           │   control   │◀───────────────┼── agent (gRPC)
       │             │           │    plane    │                │
       │             │           └─────────────┘                │
       └─────────────┴──────────────────────────────────────────┘
```

Three listeners per process:

| Listener | Port (default) | Audience | Carries |
|----------|---------------|----------|---------|
| `control` | 7000 | agents | gRPC `TunnelControl.Session` |
| `http` | 8080 | the internet | public traffic, routed by `Host` |
| `peer` | 7100 | sibling nodes | cross-node hand-off, `/__norupo/*` |

The peer listener must never be publicly reachable. It is the one surface that
trusts its caller.

## Why one gRPC stream per agent

The obvious design is one gRPC stream per public request. It is also wrong at
scale:

* HTTP/2 caps concurrent streams per connection (`SETTINGS_MAX_CONCURRENT_STREAMS`),
  so a busy agent would need several TCP connections and the server would need
  to track which connection a given tunnel lives on.
* Every new stream pays a round trip before the first byte of the request can
  move.
* An edge node's stream table would grow with *traffic* rather than with
  *agents*, which is the number you can actually capacity-plan against.

So Norupo opens exactly one bidirectional stream per agent and multiplexes on
top of it, with a `stream_id` on every frame. Frame ordering per `stream_id` is
free — HTTP/2 already guarantees in-order delivery within a stream.

The cost of this choice is that we have to implement flow control ourselves,
because HTTP/2's per-stream windows now apply to the aggregate rather than to
each logical request. That is `WindowUpdate` in
[`tunnel.proto`](../proto/norupo/v1/tunnel.proto).

## Flow control

Each logical stream has a send window in each direction, in bytes:

* The edge grants the agent credit in `StreamOpen.initial_window`, and tops it
  up with `WindowUpdate` as it hands response bytes to the public client.
* The agent grants the edge credit with `WindowUpdate` as it hands request
  bytes to the local service.

A sender that runs out of credit parks until more arrives. Concretely, in
`Session::send_stream_data` the window is a `tokio::sync::Semaphore` whose
permits *are* the bytes.

Without this, a public client uploading a 2 GB file to a local service that
reads at 1 MB/s would buffer the difference in edge memory — multiplied by
every concurrent upload on the node. With it, the TCP window on the public side
closes and the uploader slows down, which is exactly what should happen.

Tearing a stream down closes its semaphore, which unparks any blocked sender.
That detail is what stops a disconnected agent from leaking a task blocked
forever on credit that will never arrive.

## The request path

1. **`Host` → routing key.** `normalize_host` strips the port, lowercases,
   drops a trailing root dot and handles IPv6 literals. The canonical form
   produced here must match byte-for-byte the one produced at registration
   time, or a live tunnel 404s. This is tested from both directions.
2. **Routing key → `RouteRecord`.** One lookup against the routing table.
3. **`RouteRecord` → session.** If `record.node_id` is us, the session is in
   this process's table. Otherwise the request is handed to the node that owns
   it (see [SCALING.md](SCALING.md)).
4. **Session → stream.** Allocate a `stream_id`, send `StreamOpen`, pump the
   request body in a background task, and await the response head with a
   timeout.
5. **Stream → response.** The response body is a `Stream` fed by the session's
   inbound frames. A guard tied to the body's lifetime releases the stream when
   the public client disconnects, so a client hanging up mid-download does not
   leak an entry.

Hop-by-hop headers (`Connection`, `Transfer-Encoding`, `Upgrade`, and anything
nominated by `Connection`) are stripped in both directions. Header values are
carried as `bytes`, not `string`, because a latin-1 cookie must not be able to
crash the edge, and header values from the agent are re-validated before they
go anywhere near a response.

## Failure handling

| Failure | What the user sees | Why |
|---------|-------------------|-----|
| No tunnel for the host | `404` with `x-norupo-error: tunnel_not_found` | Distinguishable from the user app's own 404s. |
| Agent's local service is down | `502 Local service unreachable` | The agent resets the stream with `LOCAL_UNREACHABLE`. |
| Local service too slow | `504` after `--response-timeout-ms` | The edge resets the stream rather than holding it. |
| Routing table unreachable | `502 Routing unavailable` | Fail closed; routing decisions are not guessable. |
| Owning node unreachable | `502 Edge node unreachable` | The hand-off failed; the LB can retry another node. |
| Two nodes disagree on ownership | `508 Routing loop` | One hop only, ever. A second hop means a stale table. |

Session teardown releases every routing claim the session held — but only if
that session still owns it. A node coming back from a network partition must
not delete a claim that a newer session has since taken over.

## Security posture

* The agent authenticates with a bearer token; `AuthProvider` is a trait, so a
  static file (self-hosting) and a control-plane database (SaaS) are a config
  change apart.
* A token is validated *before* any per-session state is allocated.
* Reserved subdomains (`api`, `admin`, `www`, ...) are refused unless the
  account was explicitly granted them, so a user cannot publish something that
  looks like edge infrastructure to their own visitors.
* Custom domains must be on the principal's verified list; wildcard entries
  cover children only, never the bare parent or a same-suffix impostor.
* The release profile does **not** set `panic = "abort"`: a panic in one
  request handler must kill that task, not the process holding every other
  user's tunnel.

## Testing

`crates/norupo-server/tests/` runs the real thing: a real edge, a real agent, a
real local HTTP service, over loopback on ephemeral ports.

* `end_to_end.rs` — GET/POST round trips, bodies larger than the flow-control
  window in both directions, repeated `Set-Cookie` headers, path and query
  fidelity, case/port-insensitive host matching, a dead local service, reserved
  and contested subdomains, and 50 concurrent requests multiplexed over one
  session.
* `cross_node.rs` — two edge nodes on one shared routing table: hand-off,
  bodies across the hop, either-node service, cluster-wide claim exclusivity,
  and name release.

These are the tests that matter. The registration race that made request bodies
hang — the agent registering a stream inside a spawned task, so `Data` frames
arriving first were dropped — was caught by `request_and_response_bodies_survive_the_round_trip`
and by nothing else.
