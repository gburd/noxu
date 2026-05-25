# Authentication via mTLS-by-default — Design

Status: **draft, in flight.** This document describes the
intended end-state for closing the auth-class blockers
NA-1 / NA-2 / NA-3 / NA-5 / NA-6 / TLS-1 from
`security-review-2026-05.md`. The final implementation is
expected to land across multiple commits on the
`chore/auth-mtls-by-default` branch.

## Goal

Make it impossible to deploy a Noxu DB replicated cluster
without authenticated peer-to-peer transport, while
preserving the existing `TlsConfig::insecure(name)` path as
an explicit opt-in for development and trusted-network
deployments.

## Approach

Three coordinated changes:

1. **mTLS as the dispatcher's default transport.** The
   `TcpServiceDispatcher` currently accepts plain TCP. We
   change the constructor to require a `TlsConfig`. The plain
   variant is renamed `TcpServiceDispatcher::with_plain_tcp`
   and is documented as for tests / explicit insecure
   deployments only.

2. **Peer-allowlist verifier.** A new
   `noxu-rep::auth::PeerAllowlistVerifier` implements both
   `rustls::server::danger::ClientCertVerifier` and
   `rustls::client::danger::ServerCertVerifier`. After
   normal cert-chain validation succeeds, the verifier
   extracts the leaf certificate's subject Common Name and
   confirms it is in the configured allowlist. If not, the
   handshake aborts.
   Allowlist entries are configured via a new
   `RepConfig::with_peer_allowlist(Vec<String>)` method.

3. **Renamed insecure constructors.** `TlsConfig::insecure`
   becomes `TlsConfig::insecure_for_dev_only` (with a
   deprecation alias for the old name). The new
   `TlsConfig::for_replication(identity, trusted_certs,
   server_name)` is the documented path; it accepts only
   `TlsIdentity::PemFiles` / `PemBytes` / `Pkcs12` (rejects
   `SelfSigned` for production use) and only
   `TrustedCerts::CaFiles` / `CaBytes` (rejects
   `SkipVerification`).

## Mapping to security-review findings

| Finding | Closed by |
|---|---|
| NA-1 (no auth) | mTLS — peer identity is the cert subject CN, validated by the rustls handshake |
| NA-2 (NetworkRestoreServer leaks env) | Allowlist verifier — only registered peers reach the service handler |
| NA-3 (PeerFeederService leaks WAL) | Same as NA-2 |
| NA-4 (TcpServiceDispatcher has no auth) | mTLS by default; plain-TCP path is explicit opt-in |
| NA-5 (election votes unsigned) | **Not closed by mTLS alone.** Election messages still need either a per-message MAC or transport-binding (channel-binding token) to prevent on-path replay. Tracked separately. |
| NA-6 (no replay protection on elections) | **Not closed by mTLS alone.** Needs persistent `promised_term` + nonce in the proposal. Tracked separately. |
| NA-7 (heartbeats unauthenticated) | mTLS prevents injection from off-path attackers; an on-path / credentialed peer attack still requires per-message auth (out of scope). |
| NA-8 (plain TCP) | Subsumed by mTLS-by-default. |
| TLS-1 (skip-verify is silent default) | New default constructors disallow `SkipVerification`; the explicitly-named `insecure_for_dev_only` is the only entry to the skip-verify path. |

## What this branch will NOT close

- **NA-5, NA-6, NA-7** — message-level auth on the elections
  RPC and heartbeats. mTLS authenticates the wire; it does
  not bind a particular message to the cert that signed the
  handshake. A separate signed-handshake or per-message-MAC
  scheme is needed. Tracked under a future
  `chore/auth-message-signing` branch.
- **The 7 high-severity claim-audit items** are unaffected
  by this branch.

## Implementation plan

### Phase 1 — Foundation (this commit)

- Add this design doc.
- Add `crates/noxu-rep/src/auth.rs` with
  `PeerAllowlistVerifier` and tests for the allowlist
  matching logic in isolation (no actual handshake yet).
- Add `RepConfig::with_peer_allowlist(Vec<String>)` and a
  test that the field round-trips through the builder.
- Add `TlsConfig::for_replication(...)` as the documented
  constructor; keep `insecure` available for now but
  document it as deprecated.

### Phase 2 — Dispatcher integration

- Change `TcpServiceDispatcher` to require a `TlsConfig`.
- Rename the plain-TCP constructor.
- Wire `PeerAllowlistVerifier` through to the rustls config.
- Update `ReplicatedEnvironment::new` and tests.
- Backward-compat note: v1.5.0 peers cannot talk to v1.4.x
  peers without coordination; document the migration path.

### Phase 3 — Server-side verification + cleanup

- Make `ClientCertVerifier` actually run (currently
  rustls server config is built with `with_no_client_auth`).
- Remove the deprecated `TlsConfig::insecure` alias.
- Update the README and `known-limitations.md` to reflect
  that replication is no longer "deploy only on a trusted
  network" — assuming Phase 1 + Phase 2 both land.

## API shape (target)

```rust
use noxu_rep::TlsConfig;

let tls = TlsConfig::for_replication(
    TlsIdentity::PemFiles {
        cert: "/etc/noxu/cert.pem".into(),
        key:  "/etc/noxu/key.pem".into(),
    },
    TrustedCerts::CaFiles(vec!["/etc/noxu/ca.pem".into()]),
    "node-1.cluster.example",
)?;

let rep_config = RepConfig::new("group", "node-1", addr)
    .with_peer_allowlist(vec![
        "node-1.cluster.example".to_string(),
        "node-2.cluster.example".to_string(),
        "node-3.cluster.example".to_string(),
    ])
    .with_tls_config(tls);

let env = ReplicatedEnvironment::new(env_config, rep_config)?;
```

A peer trying to join the cluster with a cert whose subject
CN is not in the allowlist will be rejected at the TLS
handshake layer, before any application-level data is
exchanged. A peer with an expired or wrong-CA cert is
rejected by the rustls chain validation that runs before
the allowlist check.

## Open design questions

1. **Allowlist vs membership service.** Static allowlist is
   simpler but requires a config push to add peers. A
   dynamic membership service (paxos-elected list) is more
   flexible but introduces a chicken-and-egg with elections.
   Phase 1 starts with static; Phase 4 may reconsider.

2. **Cert subject extraction.** We currently propose CN.
   Most modern certs use SAN (Subject Alternative Name) DNS
   entries instead. Phase 2 should accept both.

3. **Cert revocation.** OCSP / CRL is out of scope for
   Phase 1-3. A compromised cert remains valid until expiry
   or operator intervention (push a new allowlist that
   excludes the bad CN).

4. **Performance.** mTLS handshake is more expensive than
   plain TCP (additional public-key operation). A workload
   that opens many short-lived peer connections per second
   will see throughput regression. Phase 4 will measure and
   may add session-resumption.
