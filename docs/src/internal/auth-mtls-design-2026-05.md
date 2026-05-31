# Authentication via mTLS-by-default — Design

Status: **Phase 1 complete; Phase 2 landed in v3.1.0** on
branch `fix/fb-mtls-phase2`.

Phase 1 added the allowlist matching logic and the
`PeerAllowlist` data structure.  Phase 2 wired
`PeerAllowlistVerifier` into the rustls `ServerConfig` and
enabled client-cert presentation in the rustls `ClientConfig`.

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

### Phase 2 — Wire enforcement (v3.1.0) ✓ LANDED

- **`PeerAllowlistVerifier`** implemented in `auth.rs` as a
  `rustls::server::danger::ClientCertVerifier`.  Chain validation
  delegates to `WebPkiClientVerifier`; name check uses DER-parsed
  CN + DNS SANs.  Empty allowlist = `ConfigError` at construction
  (fail-closed).
- **`TlsConfig::to_rustls_server_config_with_allowlist`** builds a
  `ServerConfig` with client-cert verification enabled.
- **`TlsTcpChannelListener::bind_with_tls_and_allowlist`** is the
  enforcement entry-point for server listeners.
- **Client-cert presentation**: `to_rustls_client_config` now calls
  `with_client_auth_cert` for `PemFiles`/`PemBytes` identities.
- **`known-limitations.md`** updated; Phase-1 inert warn removed.
- **Tests**: 10 integration tests in
  `crates/noxu-rep/tests/peer_allowlist_tls_test.rs`.

### Phase 3 — Full dispatcher integration (planned)

- Wire `TlsTcpServiceDispatcher` (TLS-capable service dispatcher)
  so `ReplicatedEnvironment::new` enforces the allowlist
  automatically when `RepTransportKind::Tls` is configured.
- Remove the deprecated `TlsConfig::insecure` alias.
- Update the README to reflect deployment without trusted-network
  requirement.

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
