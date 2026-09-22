# Security Policy

Norupo takes a port on a machine that is normally unreachable and publishes it
to the open internet. That is the whole point of the tool, and it is also why a
bug here can matter more than the same bug elsewhere.

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Report it privately through GitHub:

1. Go to the [Security tab](https://github.com/Mahmoud-walid/Norupo-tunnel/security)
2. Click **Report a vulnerability**

That opens a private thread visible only to the maintainers. If private
reporting is unavailable to you, open a public issue that says only *"I would
like to report a security issue privately"* — with no details — and you will be
contacted.

Please include, as far as you can:

- What an attacker can do, and what they need in order to do it
- Steps to reproduce, ideally a minimal case
- The version (`norupo --version`) and platform
- Whether the edge was running with `--allow-anonymous`

You should get a first response within **72 hours**, and an assessment within
**7 days**. This is a small project, so those are honest targets rather than a
contractual guarantee.

## Supported versions

Only the latest release receives security fixes.

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |
| < 0.1   | ❌        |

Norupo is pre-1.0. Until it reaches 1.0, a security fix may ship in a minor
release with a breaking change if that is what a correct fix requires.

## What counts as a vulnerability

In scope, roughly in order of how much we care:

- **Routing confusion** — anything that makes a request reach the wrong
  tunnel, or lets one account serve traffic for a hostname it does not own.
- **Claim hijacking** — taking over another session's routing-table entry, or
  making a live tunnel unreachable.
- **Authentication bypass** — registering a tunnel without a valid token, or
  claiming a reserved subdomain or an unverified custom domain.
- **Request smuggling or response splitting** through the edge, in either
  direction.
- **Peer listener exposure** — reaching the internal cross-node listener from
  outside the cluster and having it honour the request.
- **Resource exhaustion** that a single unauthenticated peer can trigger:
  memory, file descriptors, or unbounded buffering past the flow-control
  window.
- **Agent-side** issues where a malicious edge can make the agent do something
  beyond forwarding to its configured local address.

## What does not count

These are documented behaviours, not bugs:

- **`--allow-anonymous` accepting anyone.** It says so in `--help`, in the
  README, and it logs a warning on startup. It is a development flag.
- **The peer listener trusting its caller.** It is designed to be reachable
  only from sibling edge nodes; restricting it is a deployment responsibility,
  documented in [docs/SCALING.md](docs/SCALING.md).
- **A tunnel exposing whatever the local service exposes.** Norupo forwards
  requests; it is not a WAF and does not claim to be.
- **Traffic being plaintext when you run the edge without TLS in front.** The
  README tells you to terminate TLS at the edge.
- Findings from automated scanners with no demonstrated impact.

## Deployment hardening

If you run a public edge:

- Never enable `--allow-anonymous`.
- Terminate TLS in front of the public listener, and set `--public-scheme https`.
- Keep `--peer-addr` on a private network, behind a security group or network
  policy that admits only your other edge nodes.
- Use the systemd unit in [`packaging/systemd/`](packaging/systemd/) — it runs
  with `DynamicUser`, `ProtectSystem=strict`, a syscall filter, and only
  `CAP_NET_BIND_SERVICE`.
- Treat `tokens.json` as a secret. It is in `.gitignore` for a reason.
- Set per-account `limits` rather than leaving them unbounded.
