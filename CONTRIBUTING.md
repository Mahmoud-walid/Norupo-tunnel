# Contributing

## Before you open a pull request

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

CI runs all three on Linux, macOS and Windows, plus a build with
`--no-default-features` (no Redis) and `cargo doc` with warnings denied. A PR
is mergeable when the **CI passed** check is green.

## Where things live

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) first. Briefly:

* `proto/norupo/v1/tunnel.proto` is the single source of truth for the wire
  contract. Nothing in `crates/norupo-proto` is hand-written.
* `crates/norupo-core` is transport-agnostic. If it imports `tonic` or `hyper`,
  it is in the wrong crate.
* `crates/norupo-server` and `crates/norupo-client` are the two sides of the
  protocol. Anything they both need belongs in `norupo-core`.

## Changing the protocol

* Field numbers are permanent. Never renumber or reuse one; mark removed fields
  `reserved`.
* Adding a `oneof` variant, a message or an optional field is backward
  compatible. Anything else is not.
* Breaking changes bump `norupo_proto::PROTOCOL_VERSION`. The server rejects a
  `Hello` carrying a version it cannot speak, with an error telling the user to
  upgrade — keep that message accurate.

## Tests

New behaviour needs a test that would fail without it. In particular:

* Anything touching host canonicalisation, claim/release semantics or flow
  control needs a unit test — these are the areas where a bug is silent.
* Anything touching the request path needs a case in
  `crates/norupo-server/tests/end_to_end.rs`.
* Anything touching routing or ownership needs a case in
  `crates/norupo-server/tests/cross_node.rs`.

Tests bind ephemeral ports (`127.0.0.1:0`) and must stay parallel-safe. No
fixed ports, no `sleep`-based synchronisation where an event or a poll loop
will do.

## Commits and PRs

* One logical change per PR, with a title that says what changed.
* Reference the issue it closes (`Closes #12`).
* Explain *why* in the body; the diff already says what.

## Releasing

Tag `vX.Y.Z` on `main`. The release workflow cross-compiles every supported
target, publishes a GitHub Release with checksums, builds an Arch package, and
pushes the updated `norupo` and `norupo-bin` PKGBUILDs to the AUR.

Publishing to the AUR needs one repository secret, `AUR_SSH_PRIVATE_KEY`, whose
public half is registered on the AUR account that maintains the packages.
