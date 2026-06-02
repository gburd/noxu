# Wave GA — mTLS Phase 3 (dispatcher end-to-end TLS + QUIC client-cert)

**Branch**: `fix/ga-mtls-phase3`. Targets the next release.

Phase 3 extends the Phase 2 peer-allowlist enforcement
(`docs/src/internal/wave-fb-mtls-phase2.md`) to the two replication paths that
were still unauthenticated: the service dispatcher (was plain TCP) and the
QUIC transport (was `with_no_client_auth`).

## Per-path enforcement status

| Path | Status after Phase 3 |
|------|----------------------|
| `TlsTcpChannelListener::bind_with_tls_and_allowlist` | Enforced (Phase 2) |
| **`TlsTcpServiceDispatcher`** (the replication service) | **Enforced** — binds via `bind_with_tls_and_allowlist`; only allowlisted peers reach service-name routing |
| **`ReplicatedEnvironment` with `transport_kind = Tls`** | **Enforced** — `build_dispatcher` constructs a `TlsTcpServiceDispatcher` |
| **QUIC (`QuicChannelListener::bind_with_tls_and_allowlist`)** | **Enforced** — reuses the same `PeerAllowlistVerifier` via `TlsConfig::to_quinn_server_config_with_allowlist` |
| `tls-native` backend | Still no client-cert API — mTLS intent returns `ConfigError` (unchanged) |

## Empty-allowlist policy (fail-closed, now consistent everywhere)

An empty `peer_allowlist` with TLS configured is a misconfiguration and is
rejected with `RepError::ConfigError` at construction on **all** enforced
paths:

- `TlsTcpChannelListener::bind_with_tls_and_allowlist` (Phase 2)
- `TlsTcpServiceDispatcher::new` (Phase 3)
- `QuicChannelListener::bind_with_tls_and_allowlist` (Phase 3)
- `ReplicatedEnvironment::build_dispatcher` (Phase 3) — a node that requests
  `transport_kind = Tls` **must** supply a non-empty allowlist; it no longer
  silently downgrades to plain TCP (that fail-*open* path in the Phase-3 WIP
  was removed before merge).

`TrustedCerts::SkipVerification` is rejected on every enforced path (no CA =
no chain validation possible).

## How QUIC enforcement works

`to_quinn_server_config_with_allowlist` builds the rustls `ServerConfig` via
the same `to_rustls_server_config_with_allowlist` used by the TCP-TLS path
(installing `PeerAllowlistVerifier` as the `ClientCertVerifier`), then wraps it
in a `quinn::crypto::rustls::QuicServerConfig`. The QUIC client presents its
certificate via `to_quinn_client_config`. So a QUIC peer is authenticated
(chain + CN/SAN allowlist) before any application stream data is exchanged —
identical policy to the TCP-TLS path. The legacy `default_server_config` /
`insecure_client_config` (skip-verify, dev only) are unchanged.

## Tests

`crates/noxu-rep/tests/peer_allowlist_tls_test.rs` (gated on `tls-rustls`):

- `tls_dispatcher_admits_allowlisted_peer_end_to_end` — an allowlisted peer
  completes the mTLS handshake, routes to a registered `echo` service, and
  round-trips an application payload.
- `tls_dispatcher_rejects_non_allowlisted_peer` — a peer with a CA-signed but
  non-allowlisted cert is rejected at the handshake (never reaches the
  handler).
- `tls_dispatcher_empty_allowlist_errors` — `TlsTcpServiceDispatcher::new`
  with an empty allowlist is a `ConfigError`.

The Phase-2 QUIC mTLS tests (`admitted_peer_connects_and_exchanges_data`,
`rejected_peer_fails_at_handshake`, `foreign_ca_peer_is_rejected_*`) continue
to cover the QUIC verifier.

## Still deferred (beyond Phase 3)

- **`tls-native` client-cert verification**: `native_tls` exposes no
  `ClientCertVerifier` equivalent; allowlist enforcement remains
  `tls-rustls`-only. Documented in known limitations.
- **NA-5 / NA-6 — per-message authentication**: mTLS authenticates the wire
  (channel-level); per-message MAC / channel binding for election messages is
  separate work.
- **`TlsConfig::insecure` deprecation alias removal**: deferred to avoid a
  breaking change inside this wave.

## Platforms

Builds and tests pass on x86_64 Linux, RISC-V 64, and Windows on ARM64 — TLS
uses `rustls`/`ring` (cross-platform); no platform-specific code was added.
