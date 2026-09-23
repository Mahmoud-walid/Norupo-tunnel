# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While Norupo is pre-1.0, a breaking change may ship in a minor release.

## [Unreleased]

## [0.1.1] - 2026-09-23

No user-facing behaviour changes: the protocol, the edge and the agent do
exactly what 0.1.0 did. This release is packaging, supply chain and repository
health.

### Added

- A security policy with a private reporting route, and an explicit
  out-of-scope list so that documented behaviour - `--allow-anonymous`, the
  cluster-internal peer listener - does not generate reports.
- Automated dependency updates for both `cargo` and `github-actions`.
  `cargo audit` already reported advisories; nothing opened the pull request
  that fixes them.
- Issue and pull request templates, and this changelog.
- The hardened systemd unit and `server.env` now ship inside the Linux release
  archives, so `norupo-bin` installs the same server-side files as `norupo`.

### Changed

- `rand` upgraded to 0.10, which moved `random_range` to a new `RngExt` trait.
  No logic change.

### Removed

- Eight declared-but-unused dependencies: `toml`, `prost`, `futures`,
  `tokio-util` and `async-trait` from the server; `futures`, `serde` and
  `serde_json` from the client. Each was verified by removing it and building.

### Fixed

- Dependabot was rewriting `dtolnay/rust-toolchain@1.88` - which is a Rust
  toolchain selector, not a version tag - and would have silently turned the
  MSRV job into a second copy of the stable test.
- `norupo-bin` declared `backup=('etc/norupo/server.env')` for a file it never
  installed.

## [0.1.0] - 2026-09-22

First release. The HTTP tunnel path is complete and covered end to end.

### Added

- **Wire protocol** (`norupo.v1`) — a single long-lived bidirectional gRPC
  stream per agent carrying control traffic and multiplexed request data, with
  per-stream flow control. One HTTP/2 stream per *agent* rather than per
  request, so an edge node's stream table scales with agents, not traffic.
- **Edge server** (`norupo-server`) — gRPC control plane, public HTTP ingress
  routed by `Host`, and an internal peer listener for cross-node hand-off.
- **Agent CLI** (`norupo`) — `norupo http <port>` with `--subdomain`,
  `--domain` and `--rewrite-host`, plus `norupo doctor` for checking an edge is
  reachable and protocol-compatible.
- **Distributed routing table** — atomic, owner-checked, TTL'd claims over
  Redis, so a fleet behind a plain L4 load balancer can serve any request from
  any node. An in-memory backend covers single-node deployments.
- **Flow control** — a per-stream byte window in both directions, so a slow
  local service applies backpressure to the public upload instead of consuming
  edge memory.
- **Cross-platform binaries** — static musl builds for Linux (x86_64,
  aarch64), glibc for x86_64, macOS (Apple Silicon and Intel), Windows (x86_64,
  aarch64), each with a published SHA-256.
- **Arch packaging** — `norupo` and `norupo-bin` for the AUR, a
  `.pkg.tar.zst` attached to each release for `pacman -U`, and a hardened
  systemd unit.
- **Installers** — `scripts/install.sh` (POSIX sh, runs under dash and ash) and
  `scripts/install.ps1`, both verifying the published checksum before
  installing.
- **Test gates** — 81 tests including end-to-end suites that run a real edge, a
  real agent and a real local service over loopback, and a two-node suite
  covering cross-node hand-off and cluster-wide claim exclusivity. CI runs on
  Linux, macOS and Windows.
- **Documentation** — [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and
  [`docs/SCALING.md`](docs/SCALING.md), which record *why* each design decision
  was made, including what is deliberately not attempted.

### Known limitations

- Raw TCP tunnels are defined in the protocol but not implemented end to end.
- `Limits.max_bytes_per_second` is carried in the protocol and surfaced to the
  agent, but not yet enforced at the edge.
- Automatic TCP port assignment (`remote_port = 0`) is not implemented.
- Reconnect steering (`Shutdown.reconnect_to`) is in the contract but unused.

[Unreleased]: https://github.com/Mahmoud-walid/Norupo-tunnel/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/Mahmoud-walid/Norupo-tunnel/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/Mahmoud-walid/Norupo-tunnel/releases/tag/v0.1.0
