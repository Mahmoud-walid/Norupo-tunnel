# Scaling Norupo horizontally

The short version: **edge nodes hold sessions, a shared routing table holds
ownership, and any node can serve any request by handing it to the node that
owns the session.** That combination is what lets you put a dumb L4 load
balancer in front and add nodes without coordination.

## Why a shared routing table is unavoidable

An agent's session lives on exactly one node — it is a TCP connection, and TCP
connections do not move. So when a public request arrives, the node that
accepted it has to answer one question: *who holds the session for this
hostname?*

Three answers, in increasing order of how well they work:

1. **Sticky routing at the load balancer.** Route `my-app.tunnel.com` to the
   node that owns it. This needs an L7 load balancer that knows your tunnel
   topology, updated on every agent connect and disconnect. You have moved the
   problem into a component that is much harder to change.
2. **Broadcast.** Every node asks every other node. Works at five nodes;
   at five hundred it is O(n) network calls on the request path.
3. **A shared table.** One lookup, O(1), and the load balancer stays dumb.
   This is what Norupo does.

## The table

One key per tunnel, one `SET` per registration, one `GET` per request:

```
norupo:route:my-app.tunnel.com -> "my-app.tunnel.com\tnode_7\t10.0.3.14:7100\tsess_x\ttun_y\tacct_z"
                                   routing_key       node_id node_addr      session tunnel account
```

Every entry carries a TTL, renewed by the owning node on each heartbeat.

Three properties do all the work, and each of them is a correctness
requirement rather than an optimisation:

**1. Claims are atomic.** Two agents racing for `api.tunnel.com` against two
different nodes must produce exactly one winner. A read-then-write would let
both win. Norupo claims with a Lua script that is `SET NX`-equivalent but also
succeeds when the existing value already belongs to the same session — which is
what makes an agent reconnect idempotent.

**2. Mutations are owner-checked.** Renew and release both compare-and-swap on
`session_id`. Without this, a node recovering from a network partition would
run its cleanup and cheerfully delete a claim that a *newer* session had taken
over, blackholing a live tunnel. This is the single most likely bug in a system
like this, and it is tested explicitly
(`a_stale_node_cannot_release_someone_elses_claim`).

**3. Entries expire.** A `SIGKILL`ed node cannot run cleanup. Its routes free
themselves within `route_ttl`, which is three heartbeats — tight enough that
failover is seconds, loose enough that one slow Redis round trip does not evict
a healthy tunnel.

All operations are single-key, so the table shards cleanly across Redis Cluster
with no cross-slot transactions.

## Cross-node hand-off

When the routing table names a different node, the receiving node reverse
proxies the request to that node's internal peer listener:

```
  client ──▶ LB ──▶ node B ──(x-norupo-hop: node_b)──▶ node A ──▶ agent ──▶ localhost:3000
```

* The public client never learns the internal topology, and a redirect would
  break every non-idempotent request — so it is a proxy, not a 3xx.
* The `x-norupo-hop` header caps this at exactly one hop. A request arriving at
  the peer listener that would need *another* hop means the table is stale, and
  the node returns `508` instead of ping-ponging.
* The peer client pools connections. At fleet scale, a fresh TCP handshake per
  hand-off is what exhausts ephemeral ports.

The cost of a hand-off is one extra intra-datacenter round trip. If you want to
avoid paying it on most requests, see "Reducing hand-offs" below.

## Deployment recipe

```sh
norupo-server \
  --redis-url redis://routing.internal:6379 \
  --node-id "$POD_NAME" \
  --advertise-addr "$POD_IP:7100" \
  --base-domain tunnel.example.com \
  --tokens-file /etc/norupo/tokens.json
```

* `--node-id` should be the pod or instance name, so a `node_id` in the routing
  table is greppable against your orchestrator.
* `--advertise-addr` is what siblings dial. It must be the *routable* address,
  not `0.0.0.0`.
* The peer listener needs a network policy or security group that admits only
  the other edge nodes.

Load balancer: plain L4 (TCP) to `:80`/`:443`, no affinity, health check
`GET /__norupo/healthz` on the public listener. The control plane (`:7000`)
needs an L4 or HTTP/2-aware balancer; do not terminate HTTP/2 and re-establish
it unless you are sure it preserves long-lived bidirectional streams.

## Capacity

The numbers that actually bind, roughly in the order you hit them:

| Resource | Scales with | Notes |
|----------|-------------|-------|
| File descriptors | agents + concurrent public connections | The default `LimitNOFILE=1024` is the first wall. The shipped systemd unit sets 262144. |
| Memory | concurrent streams × flow-control window | `--window-bytes` (256 KiB default) is the direct knob. |
| Redis ops/sec | public requests + (agents × tunnels ÷ heartbeat) | One `GET` per request dominates. |
| Ephemeral ports | cross-node hand-offs | Mitigated by connection pooling; eliminated by reducing hand-offs. |

The lookup per request is the thing to watch. Two mitigations, in order of how
much they buy you:

**A short-TTL local cache.** A tunnel's owner changes only when an agent
reconnects. A 1–2 second in-process cache of `routing_key -> RouteRecord`
removes nearly every `GET` at the cost of up to 2 seconds of stale routing
after a reconnect — and a stale route fails loudly (`404`, or a hand-off to a
node that no longer owns it) rather than silently. This is the highest-leverage
change once you are past a few thousand requests per second per node.

**Redis read replicas.** Lookups can read from a replica; claims, renewals and
releases must hit the primary, because they are the compare-and-swap operations
that correctness depends on.

## Reducing hand-offs

Hand-offs are correct but not free. Two ways to make most requests land on the
owning node:

1. **Consistent hashing at the load balancer.** Hash the SNI or `Host` to a
   node, and have agents register preferentially on the node their hostname
   hashes to. Hand-offs then only happen during rebalancing.
2. **Reconnect steering.** `Shutdown.reconnect_to` in the protocol exists for
   this: an edge draining for deploy can tell an agent exactly where to
   reconnect, so a rolling restart does not scatter sessions randomly.

Neither is implemented yet. Both are protocol-compatible additions rather than
rewrites, which is why the fields are already in the contract.

## Going further

Things that become worth doing well past a single-region fleet, and roughly
when:

* **Anycast + regional clusters.** One routing table per region, with the
  hostname's home region in DNS. Cross-region hand-off is a wide-area round
  trip; you want to avoid it rather than optimise it.
* **Splitting the control plane from the data plane.** Today one process does
  both. Separating them lets you scale ingress (CPU-bound on TLS) independently
  of session termination (memory- and fd-bound).
* **A durable control plane.** `AuthProvider` is already a trait; swapping the
  static token file for a database is where accounts, quotas and billing live.
* **Per-account rate limiting.** `Limits.max_bytes_per_second` is in the
  protocol and surfaced to the agent, but not yet enforced at the edge.

## What is deliberately not here

* **Session migration.** You cannot move a live TCP connection between nodes.
  A node going away means its agents reconnect, which they already do with
  exponential backoff. Making that fast (drain, `reconnect_to`, short TTLs) is
  the achievable goal; making it invisible is not.
* **Strong consistency across regions.** A hostname is owned in one place at a
  time. A global consensus system on the request path would cost more than the
  failure mode it prevents.
