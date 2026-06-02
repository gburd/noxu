# Wave FB — mTLS Phase 2: peer_allowlist Enforcement

**Branch**: `fix/fb-mtls-phase2`
**Target release**: v3.1.0
**Status**: merged

## What this wave closes

The re-audit flagged that `RepConfig::peer_allowlist` was "inert":
the field was accepted and stored, but the server TLS config continued
to use `.with_no_client_auth()`, so any peer could connect regardless
of the allowlist.  This wave closes that trap.

## What enforcement now does

When `TlsTcpChannelListener::bind_with_tls_and_allowlist(addr, tls, allowlist)` is used:

1. The rustls `ServerConfig` is built with a
   `PeerAllowlistVerifier` as the `ClientCertVerifier`.
2. Every incoming TLS connection **must** present a client certificate.
3. The certificate chain is validated against the CA roots in `tls.trusted_certs`
   via rustls's built-in `WebPkiClientVerifier`.
4. The leaf certificate's Subject CN and all DNS SANs are extracted with a
   minimal DER parser (no new dependencies).
5. At least one name must match a `peer_allowlist` entry
   (case-insensitive, exact, no wildcards).
6. If the chain validation **or** the name check fails, rustls aborts the
   handshake and closes the TCP connection before any application data
   is exchanged.

On the client side, `TlsConfig::to_rustls_client_config` now calls
`with_client_auth_cert` for `PemFiles`/`PemBytes` identities, so the
client automatically presents its certificate during the TLS handshake.

## Empty-allowlist policy

An empty `PeerAllowlist` means **no peer is authorised**.
`PeerAllowlistVerifier::new(root_store, empty_allowlist)` returns
`Err(RepError::ConfigError)` at construction time.  This is intentional
fail-closed behaviour per the design doc:

> "An allowlist with zero entries means 'no peer is authorised',
> which is a valid (if useless) state — the caller should treat
> zero-entry allowlists as a configuration error before constructing
> the verifier."

Propagates through `bind_with_tls_and_allowlist` as a `ConfigError`.

## Client-auth wiring status

| Path | Status |
|------|--------|
| `TlsTcpChannelListener::bind_with_tls_and_allowlist` (server) | Enforced — client cert mandatory |
| `TlsConfig::to_rustls_client_config` with `PemFiles`/`PemBytes` | Presents client cert |
| `TlsConfig::to_rustls_client_config` with `SelfSigned` | No client cert (dev mode unchanged) |
| QUIC channels (`to_quinn_server_config`) | Phase 2: `with_no_client_auth`. **Phase 3: enforced** via `QuicChannelListener::bind_with_tls_and_allowlist` |
| `ReplicatedEnvironment::new` / `TcpServiceDispatcher` | Phase 2: plain TCP. **Phase 3: `TlsTcpServiceDispatcher` enforces mTLS end-to-end when `transport_kind=Tls`** |

## rustls-vs-native gap

`bind_with_tls_and_allowlist` is only available under `tls-rustls`.
The `tls-native` backend (`native_tls`) does not expose
`TlsAcceptorBuilder::client_cert_required()` or any client-cert
verification API.  Attempting to use mTLS intent (non-empty `TrustedCerts`)
with a `tls-native` acceptor already returns a `ConfigError` (TLS-4 finding,
closed in Phase 1).

## What is deferred to Phase 3

> **Update (Wave GA):** the `ReplicatedEnvironment`/dispatcher wiring and QUIC
> server-side enforcement listed below are now **done** — see
> `wave-ga-mtls-phase3.md`. The remaining deferrals are the `tls-native`
> client-cert gap and per-message auth (NA-5/NA-6).

- **`ReplicatedEnvironment` end-to-end wiring**: The production flow uses
  `TcpServiceDispatcher` (plain TCP).  Full end-to-end enforcement requires
  a `TlsTcpServiceDispatcher` that accepts TLS connections and dispatches
  services after the handshake.  This wiring is tracked as Phase 3.
- **QUIC server-side enforcement**: `to_quinn_server_config` still calls
  `to_rustls_server_config` (no client auth).  Phase 3 will add
  `to_quinn_server_config_with_allowlist`.
- **`TlsConfig::insecure` deprecation alias**: Still present; Phase 3 removes it.
- **NA-5, NA-6** (election message-level auth): mTLS authenticates the wire;
  per-message MAC / channel binding is separate work.

## Test coverage

File: `crates/noxu-rep/tests/peer_allowlist_tls_test.rs`
(gated: `#[cfg(feature = "tls-rustls")]`)

| Test | What it verifies |
|------|-----------------|
| `admitted_peer_connects_and_exchanges_data` | Allowlisted peer passes handshake and exchanges data |
| `rejected_peer_fails_at_handshake` | Non-allowlisted peer is rejected |
| `foreign_ca_peer_is_rejected_despite_allowlisted_name` | Chain validation runs before name check |
| `empty_allowlist_errors_at_construction` | Empty allowlist = ConfigError |
| `skip_verification_with_allowlist_errors` | SkipVerification + allowlist = ConfigError |
| `two_admitted_peers_connect_sequentially` | Multiple sequential admitted peers |
| `extract_cert_names_from_rcgen_cert` | DER parser extracts SAN from rcgen cert |
| `extract_cert_names_multiple_sans` | All SANs extracted |
| `extract_cert_names_are_lowercased` | Names normalised to lowercase |
| `extract_cert_names_garbage_input_is_empty` | Invalid DER → empty list (fail-closed) |

All 10 tests pass under `cargo test -p noxu-rep --features tls-rustls`.

## Files changed

| File | Change |
|------|--------|
| `crates/noxu-rep/src/auth.rs` | DER parser + `PeerAllowlistVerifier` |
| `crates/noxu-rep/src/tls.rs` | `to_rustls_server_config_with_allowlist`, updated client config |
| `crates/noxu-rep/src/net/channel.rs` | `bind_with_tls_and_allowlist` |
| `crates/noxu-rep/src/lib.rs` | Re-export `PeerAllowlist`, `TlsIdentity`, `TrustedCerts` |
| `crates/noxu-rep/src/rep_config.rs` | Updated `peer_allowlist` docs |
| `crates/noxu-rep/src/replicated_environment.rs` | Replaced inert warn with accurate message |
| `crates/noxu-rep/tests/peer_allowlist_tls_test.rs` | 10 new integration tests |
| `docs/src/internal/auth-mtls-design-2026-05.md` | Phase 2 marked landed |
| `docs/src/operations/known-limitations.md` | `peer_allowlist` entry updated |
| `docs/src/maintainer/design-decisions.md` | Decision 11 updated |
| `docs/src/replication/transport.md` | TLS/mTLS section added |
