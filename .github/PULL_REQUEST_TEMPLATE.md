## What this changes

<!-- What changed, and why. The diff already says what; explain the reasoning. -->

Closes #

## How it was verified

<!--
Not "tests pass" — say what you actually ran and what it showed. If a change
cannot be covered by a test (a release workflow, a PKGBUILD), say how you
checked it instead, and say plainly if you could not.
-->

```
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
```

## Checklist

- [ ] New behaviour has a test that would fail without it
- [ ] Host canonicalisation, claim/release semantics or flow control touched → unit test added
- [ ] Request path touched → case added to `tests/end_to_end.rs`
- [ ] Routing or ownership touched → case added to `tests/cross_node.rs`
- [ ] `proto/norupo/v1/tunnel.proto` touched → field numbers unchanged, and `PROTOCOL_VERSION` bumped if the change breaks deployed agents
