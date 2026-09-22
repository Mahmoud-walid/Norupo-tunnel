# Norupo

A self-hosted, open-source local tunneling service written in Rust. Point a
public hostname at a port on your laptop, the way ngrok or `cloudflared` do —
except you run the edge.

```
  https://my-app.tunnel.example.com
            │
            ▼
   ┌──────────────────┐   gRPC bidi stream    ┌─────────────────┐
   │  norupo-server   │◀─────────────────────▶│     norupo      │
   │   (edge node)    │   (one per agent,     │   (your laptop) │
   └──────────────────┘    multiplexed)       └─────────────────┘
                                                      │
                                                      ▼
                                              http://localhost:3000
```

* **One HTTP/2 stream per agent, not per request.** Every public request is a
  logical stream multiplexed onto a single long-lived gRPC connection, so an
  edge node's stream table scales with the number of *agents*, not with traffic.
* **The agent dials out.** Nothing has to reach your machine, so it works from
  behind NAT, corporate proxies and hotel wifi.
* **Horizontally scalable.** Edge nodes are stateless apart from a shared
  routing table; any node can serve any request. See [docs/SCALING.md](docs/SCALING.md).
* **Per-stream flow control.** A slow local service applies backpressure to the
  public upload instead of consuming edge memory.
* **Runs everywhere.** Linux (any distro, via static musl builds), macOS
  (Intel and Apple Silicon), Windows.

> **Status:** early. The HTTP tunnel path is complete and covered by
> end-to-end tests; raw TCP tunnels are defined in the protocol but not yet
> implemented end to end.

## Install

**Arch Linux, CachyOS, EndeavourOS, Manjaro** — via `paru` or `yay`:

```sh
paru -S norupo-bin     # prebuilt static binaries
paru -S norupo         # build from source
```

or with `pacman`, from the package attached to each release:

```sh
sudo pacman -U norupo-*.pkg.tar.zst
```

**Any Linux distribution, or macOS:**

```sh
curl -fsSL https://raw.githubusercontent.com/Mahmoud-walid/Norupo-tunnel/main/scripts/install.sh | sh
```

**Windows** (PowerShell):

```powershell
irm https://raw.githubusercontent.com/Mahmoud-walid/Norupo-tunnel/main/scripts/install.ps1 | iex
```

**From source** — needs a Rust toolchain, nothing else. `protoc` is vendored by
the build, so there is no system protobuf dependency on any platform:

```sh
cargo install --git https://github.com/Mahmoud-walid/Norupo-tunnel norupo-client norupo-server
```

## Quick start

Run an edge and an agent on one machine to see the whole path work:

```sh
# 1. Something to tunnel to.
python3 -m http.server 3000

# 2. An edge server. `--allow-anonymous` is development only.
norupo-server \
  --base-domain localhost \
  --public-scheme http \
  --http-addr 127.0.0.1:8080 \
  --control-addr 127.0.0.1:7000 \
  --allow-anonymous

# 3. An agent.
norupo http 3000 --server http://127.0.0.1:7000 --subdomain my-app
#   http://my-app.localhost  ->  127.0.0.1:3000

# 4. A request. (`--resolve` stands in for the wildcard DNS record you would
#    have in a real deployment.)
curl -H 'Host: my-app.localhost' http://127.0.0.1:8080/
```

`norupo doctor` checks that a configured edge is reachable and speaks a
compatible protocol version.

## Running a real edge

1. **DNS.** Point a wildcard record at the edge: `*.tunnel.example.com`.
2. **TLS.** Terminate in front of the edge (a load balancer, or Caddy/nginx),
   and set `--public-scheme https`.
3. **Credentials.** Write `/etc/norupo/tokens.json`:

   ```json
   {
     "tok_live_replace_me": {
       "account_id": "acct_ada",
       "display_name": "Ada",
       "reserved_subdomains": ["ada"],
       "custom_domains": ["*.dev.example.com"],
       "limits": { "max_tunnels": 8, "max_concurrent_streams": 512 }
     }
   }
   ```

4. **Start it.** The AUR packages ship a hardened systemd unit:

   ```sh
   sudo systemctl enable --now norupo-server
   ```

Every flag has a matching environment variable — see `norupo-server --help`,
or `packaging/systemd/server.env`.

## Scaling out

Run several edge nodes behind a plain L4 load balancer and give them a shared
routing table:

```sh
norupo-server --redis-url redis://routing.internal:6379 \
              --advertise-addr "$POD_IP:7100" \
              --node-id "$POD_NAME"
```

Claims are atomic and TTL'd, so two agents can never win the same hostname, and
a node that dies frees its routes automatically. A request that lands on the
"wrong" node is handed to the right one over the internal peer listener, which
is why no load-balancer affinity is required.
[docs/SCALING.md](docs/SCALING.md) covers the reasoning, the failure modes and
the path from one node to a very large fleet.

## Repository layout

| Crate | What it is |
|-------|------------|
| [`proto/norupo/v1/tunnel.proto`](proto/norupo/v1/tunnel.proto) | The wire contract. Single source of truth. |
| [`crates/norupo-proto`](crates/norupo-proto) | Generated tonic/prost bindings. |
| [`crates/norupo-core`](crates/norupo-core) | Routing keys, the routing table, auth. Transport-agnostic. |
| [`crates/norupo-server`](crates/norupo-server) | The edge: gRPC control plane, public ingress, peer hand-off. |
| [`crates/norupo-client`](crates/norupo-client) | The `norupo` agent CLI (also usable as a library). |

## Development

```sh
cargo test --workspace                      # unit + end-to-end + cross-node
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

The integration suites in `crates/norupo-server/tests/` run a real edge, a real
agent and a real local service over loopback, on ephemeral ports — including a
two-node cluster that exercises cross-node hand-off. CI runs everything on
Linux, macOS and Windows.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how the pieces fit
together, and [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR.

## License

Apache-2.0. See [LICENSE](LICENSE).
