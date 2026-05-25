# Security Review — May 2026

A focused security review of three areas: TLS path
configuration, network protocol authentication, and
malformed-log-entry defenses. The review was read-only — no
fixes are applied here, and the findings are deliberately
preserved verbatim so the security boundary of v1.3.0 is on
the record.

**Bottom line up front**: as shipped, **the replication wire
protocol has no authentication**. An attacker on the
replication network can:

- pull the entire on-disk environment from any node
- stream the WAL from the master without being a registered
  peer
- write attacker-controlled bytes to any filesystem path the
  noxu process can write (path traversal in the network-restore
  client)
- flip the cluster master through forged or replayed election
  messages
- trigger a 4 GiB allocation per inbound TCP frame

Replication should not be deployed across an untrusted network
boundary in v1.3.0. The README and `known-limitations.md` are
updated to call this out.

## Severity counts

- **Blockers (6 distinct, 8 referenced)**: TLS-1, NA-1, NA-2,
  NA-3, NA-5, NA-6, LOG-2, LOG-4
- **Important (10)**: TLS-2, TLS-3, TLS-4, NA-4, NA-7, NA-8,
  LOG-3, LOG-5, LOG-6, LOG-7
- **Minor (4)**: TLS-5, LOG-8, LOG-9, LOG-10
- **Observation (2)**: TLS-6, LOG-1

## 1. TLS Path

### TLS-1 — `SkipCertVerification` is the silent default for QUIC channels [blocker]

- `crates/noxu-rep/src/net/quic_channel.rs:148-160`
  (`insecure_client_config`) installs a no-op
  `ServerCertVerifier`.
- `crates/noxu-rep/src/net/quic_channel.rs:115-138`
  (`default_server_config`) generates an `rcgen` self-signed
  cert, no client auth.
- `QuicChannel::connect(addr, server_name)` at
  `quic_channel.rs:209-211` calls `connect_with_config(addr,
  server_name, insecure_client_config())` — i.e. the ergonomic
  constructor is the insecure path.
- `QuicChannelListener::bind(addr)` at
  `quic_channel.rs:445-461` uses `default_server_config()`.
- `QuicMultiplexedChannel::connect` at `net/quic_mux.rs:316-322`
  uses `mux_insecure_client_config()`;
  `QuicMultiplexedChannelListener::bind` uses
  `mux_server_config()` (`net/quic_mux.rs:89-100`).

The user has to explicitly opt OUT
(`connect_with_config` / `with_server_config`) to get
authenticated TLS. The doc comment at
`quic_channel.rs:14-25` admits this and points at
"authenticated at the Paxos layer" — see NA-5/NA-6 for why
that claim is also false.

### TLS-2 — No way to distinguish "no CA configured" from "skip-verification opt-in" [important]

- `crates/noxu-rep/src/tls.rs:226-273` (`rustls_root_store`):
  `TrustedCerts::SkipVerification` returns an empty store;
  `TrustedCerts::CaBytes(vec![])` and
  `TrustedCerts::CaFiles(vec![])` also return empty stores
  but without installing a custom verifier.
- `to_rustls_client_config()` at `tls.rs:213-225` builds
  `Ok(...)` regardless of whether the root store is empty.
- Tests `rustls_skip_verification_root_store_is_empty_but_ok`
  (`tls.rs:996-1015`) and
  `rustls_client_config_with_empty_ca_succeeds`
  (`tls.rs:778-790`) assert this behaviour.

### TLS-3 — Malformed CA PEM silently produces an empty trust store [important]

- `crates/noxu-rep/src/tls.rs:266-282`: `rustls_pemfile::certs(...)`
  silently skips non-cert PEM blocks. Garbage bytes for
  `CaBytes`/`CaFiles` produce an empty root store and
  `to_rustls_client_config` returns `Ok`.

### TLS-4 — `tls-native` server path silently ignores mTLS configuration [important]

- `crates/noxu-rep/src/tls.rs:402-423` (`to_native_acceptor`):
  when `TrustedCerts` carries non-empty CA roots (clear mTLS
  intent), the code only emits `log::warn!` and proceeds to
  build an acceptor with no client-cert verification, because
  `native_tls::TlsAcceptorBuilder` doesn't expose mTLS knobs.
  A warning is not a security boundary.

### TLS-5 — `tls-rustls` is silently preferred when both TLS features are enabled [minor]

- `crates/noxu-rep/src/net/channel.rs:609-617` (client side)
  and `:780-790` (listener side) prefer rustls unconditionally
  when both features are compiled in.

### TLS-6 — No explicit TLS version / cipher pin [observation]

- `crates/noxu-rep/src/tls.rs:189-194` and `:208-225` use
  `rustls::ServerConfig::builder()` /
  `ClientConfig::builder()` with rustls defaults (TLS 1.2 +
  1.3, AEAD ciphers). QUIC mandates TLS 1.3; the TCP TLS path
  will negotiate TLS 1.2 if requested. No explicit
  `with_protocol_versions(&[&TLS13])`.

### TLS-7 — Private keys are not echoed in error messages [no finding]

- `crates/noxu-rep/src/tls.rs:316-340` (`parse_pem_cert_and_key`):
  error messages from `rustls_pemfile` do not embed key
  material; PKCS12 password is held in a `String` field and
  not logged.

## 2. Network Protocol Authentication

### NA-1 — There is NO authentication anywhere in the replication wire protocol [blocker]

Direct answer: **no.**

- `crates/noxu-rep/src/protocol.rs:28-31` defines
  `ProtocolMessage::Handshake { node_name, group_name,
  node_type }` and `HandshakeResponse { accepted, reason }`,
  but a workspace grep shows these variants are only used in
  round-trip encoding tests. They are never sent or matched in
  `feeder.rs`, `peer_feeder.rs`, `replica_stream.rs`,
  `replicated_environment.rs`, `network_restore_server.rs`, or
  `service_dispatcher.rs`.
- `FeederRunner::run` (`crates/noxu-rep/src/stream/feeder.rs:299-348`)
  immediately starts streaming framed
  `[vlsn:8][type:1][payload_len:4][crc32:4][payload]` entries
  the moment a `Channel` is handed in. No identity, no
  `group_name` verification, no challenge-response.
- `ReplicaReceiver::run`
  (`crates/noxu-rep/src/stream/replica_stream.rs:178-244`)
  blindly applies anything that arrives with a valid CRC32.

### NA-2 — `NetworkRestoreServer` streams the entire on-disk environment to anyone who connects [blocker]

- `crates/noxu-rep/src/network_restore_server.rs:100-114`
  (`serve_raw`): reads 4-byte magic `NRST`, then
  `send_files_to(&mut stream)` enumerates every `.ndb` file in
  `env_home` (`:122-204`) and streams name+size+contents.
- `ServiceHandler::handle` at `network_restore_server.rs:259-345`
  does the same over a multiplexed channel.
- No identity, no group, no allowlist, no rate-limit. Service
  registered unconditionally at `replicated_environment.rs:204-228`.

### NA-3 — `PeerFeederService` streams WAL to anyone who can connect [blocker]

- `crates/noxu-rep/src/stream/peer_feeder.rs:329-379`
  (`PeerFeederService::handle`): reads 8-byte `start_vlsn`,
  calls `negotiate_syncup`, replies with one byte and either
  streams via `PeerFeederRunner::run` or returns
  `NEEDS_RESTORE`.
- No identity check; service registered at
  `replicated_environment.rs:268-275`.

### NA-4 — `TcpServiceDispatcher` itself has no auth [important]

- `crates/noxu-rep/src/net/service_dispatcher.rs:222-262`
  (`handle_incoming`): reads `[len: u32 LE][utf8 service_name]`
  and dispatches. The only registered services are RESTORE and
  PEER_FEEDER (both blockers above).
- `TlsTcpChannelListener` exists in `net/channel.rs` but is
  not wired into `replicated_environment.rs` — grep confirms it
  is only used in unit tests. Shipping config is plain TCP.

### NA-5 — Election votes / proposals are unsigned and unauthenticated [blocker]

- `crates/noxu-rep/src/elections/paxos.rs:130-143`: proposer
  broadcasts `ProtocolMessage::ElectionProposal { node_name,
  vlsn, priority, term }` — all fields self-claimed plaintext.
- `paxos.rs:225-243`: acceptor returns `ElectionVote { voter,
  granted, term }` — `voter` is again self-claimed; no binding
  to TCP source identity.
- No signature, MAC, or pre-shared secret. An on-path attacker
  can inject votes, propose with arbitrary
  `node_name`/`priority`, or forge `ElectionResult { master,
  term }` to flip the cluster master.

### NA-6 — Election RPC has no replay protection [blocker]

- `crates/noxu-rep/src/elections/master_tracker.rs:130-143`
  (`update_master`) accepts `term >= current_term` (note `>=`,
  not `>`) — a current-term `ElectionResult` overwrites
  silently.
- `crates/noxu-rep/src/elections/paxos.rs:222`: `let mut
  promised_term: Option<u64> = None;` is reset on every
  `run_acceptor` invocation. No persisted ballot state across
  connections, so a prior-term proposal replayed on a new
  connection is accepted again.
- Combined with NA-5: a fabricated `ElectionResult { master:
  "evil", term: current_term }` sent unsolicited is enough to
  set `MasterTracker::current_master` to the attacker's
  choice.

### NA-7 — Heartbeats and Phi-detector inputs are unauthenticated [important]

- `crates/noxu-rep/src/protocol.rs:41-44`: `Heartbeat {
  master_vlsn, timestamp_ms }` carries no auth tag.
- `crates/noxu-rep/src/elections/master_tracker.rs:97-103`
  (`record_heartbeat`) is called whenever a heartbeat arrives.
  An off-path attacker can inject heartbeats to suppress
  elections (keep `phi` low) or fake `master_vlsn` to skew
  `update_master_vlsn`
  (`replica_stream.rs:367-373`) and election decisions.

### NA-8 — `connect_to_service` uses plain TCP [important]

- `crates/noxu-rep/src/net/service_dispatcher.rs:267-285`:
  client opens raw `TcpStream`. NA-2/NA-3 are reachable over
  unencrypted, unauthenticated TCP in the shipping config.

## 3. Malformed Log-Entry Defenses

### LOG-1 — A forged entry with recomputed valid CRC32 is silently accepted [observation — by design but worth flagging]

- CRC32 is non-cryptographic.
  `crates/noxu-log/src/file_reader.rs:329-358` and
  `log_file_reader.rs:178-209` only verify CRC32. A
  disk-write attacker (or, via NA-1/NA-3, a network attacker)
  can craft entries whose CRC matches their forged content;
  recovery trusts them. No HMAC or signature.

### LOG-2 — `payload_len` on the replication wire is bounded only by `u32::MAX` (4 GiB allocation) [blocker]

All channel implementations allocate `vec![0u8; payload_len]`
directly from a `u32` wire field with no upper bound:

- `crates/noxu-rep/src/net/channel.rs:363-376` (TcpChannel)
- `crates/noxu-rep/src/net/channel.rs:725-732` (TlsTcpChannel)
- `crates/noxu-rep/src/net/quic_channel.rs:370-380` (QuicChannel)
- `crates/noxu-rep/src/net/quic_mux.rs:188-196` (QuicSubChannel)

A single attacker frame with `payload_len = 0xFFFFFFFF`
triggers a 4 GiB allocation. Combined with NA-1 this is an
unauthenticated remote OOM.

### LOG-3 — `item_size` cap of 100 MB is inconsistent across readers [important]

- `crates/noxu-log/src/entry_header.rs:131-137`: 100 MB cap,
  returns `InvalidEntrySize`.
- `crates/noxu-log/src/log_file_reader.rs:115-126` and
  `:226-232`: 100 MB cap, treats over-cap as end-of-log
  (silent — see LOG-5).
- `crates/noxu-log/src/file_reader.rs::LogEntryHeader::from_bytes`
  (~line 100-145): does NOT enforce the cap; checksum-validation
  reconstruct path at `file_reader.rs:336-344` allocates
  `Vec::with_capacity(total_size)` up to ~100 MB.
- `crates/noxu-rep/src/stream/feeder.rs:135-138`: 64 MB cap in
  `read_raw_entry` — different from the log-layer cap.

### LOG-4 — `NetworkRestore` client trusts server-supplied filenames — path traversal [blocker]

- `crates/noxu-rep/src/network_restore.rs:200-235`:
  - `let filename = String::from_utf8(name_buf)?;`
  - `let dest_path = log_dir.join(&filename);`
  - `let mut out = std::fs::File::create(&dest_path)?;`
- Filename is checked for UTF-8 only. No rejection of `..`,
  `/`, leading slashes, drive letters, or NUL.
  `Path::join("/etc/passwd")` yields `"/etc/passwd"`
  (absolute paths replace base). A malicious peer (reachable
  via NA-1) writes attacker-controlled bytes to any path the
  noxu process can write.

### LOG-5 — Unknown entry-type bytes silently truncate recovery [important]

- `crates/noxu-log/src/log_file_reader.rs:114-125`
  (`read_next`): when `LogEntryType::from_type_num(...)`
  returns `None`, the reader logs a warning and returns
  `None` — i.e. treats the unknown type as end-of-log. Same
  lenient behaviour for implausible `item_size` (`:127-133`)
  and truncated entries (`:153-160`).
- An attacker with disk-write access can place one
  CRC-correct entry with an unknown entry-type byte to
  silently truncate replay of every subsequent log entry: the
  database recovers "successfully" but is missing all later
  commits, with no operator-facing alarm.
- The strict variant `read_next_strict` (`:213-285`) returns
  `InvalidEntryType`, but it is only used in tests.

### LOG-6 — VLSN ordering is NOT verified during recovery [important]

- Workspace grep across `crates/noxu-recovery/src` for `vlsn`
  / `Vlsn` / `VLSN` returns zero matches. Recovery is purely
  LSN-driven.
- The replication-side index explicitly accepts out-of-order
  VLSN inserts:
  `crates/noxu-rep/src/vlsn/vlsn_index.rs:75-80`,
  `vlsn/vlsn_bucket.rs:310-330`. A forged or corrupted log
  file with VLSNs in wrong order passes recovery. Spec
  invariants like "monotone non-decreasing commit VLSN"
  (`vlsn/vlsn_range.rs:541`) are not checked at recovery time.

### LOG-7 — Replica blindly trusts master's VLSN ordering [important]

- `crates/noxu-rep/src/stream/replica_stream.rs:174-244`
  (`ReplicaReceiver::run`): no check that the incoming `vlsn`
  is greater than the previously received one or that frames
  are contiguous.
- `ReplicaStream::receive_entry`
  (`replica_stream.rs:341-348`) only updates `received_vlsn`
  if `vlsn > current` — out-of-order frames are still applied
  to the local WAL via `LogManager::log` and registered in
  the VLSN index.

### LOG-8 — Header `vlsn_present` flag flow trusts on-wire flag bits [minor]

- `crates/noxu-log/src/file_reader.rs:111-145`: parsed
  `vlsn_present` flag controls header_size and downstream
  reads. A forger flipping the flag bit misaligns subsequent
  entry reads. Combined with LOG-5 this is another
  silent-truncation oracle.

### LOG-9 — `EnvironmentLogScanner` accepts negative i64 VLSN as "no VLSN" silently [minor]

- `crates/noxu-rep/src/stream/feeder.rs:154-159`: `let raw =
  i64::from_le_bytes(...); if raw > 0 { Some(raw as u64) }
  else { None }`. Header with the VLSN-present flag set but
  negative i64 VLSN is silently demoted to "no VLSN" and
  skipped — a quiet "skip this entry on the master feeder"
  oracle for a disk-write attacker.

### LOG-10 — Replication frame entry_type byte not validated at the framing layer [minor]

- `crates/noxu-rep/src/stream/replica_stream.rs:202-211`:
  wire `entry_type` is forwarded to `LogWriter::write_entry`
  without check. `EnvironmentLogWriter` rejects unknown values
  with `ProtocolError` (`replica_stream.rs:103-110`), but
  `PeerLogScanner` accepts any byte and re-emits it. Garbage
  entry types can propagate through a peer-feeder ring until
  each terminal replica rejects them.

## Highest-impact composite chain

**NA-1 + NA-2 + LOG-4** — an unauthenticated TCP attacker can
either pull the entire WAL from the noxu node (NA-2), or, if
the local node is induced to call `NetworkRestore` against an
attacker-controlled "peer," write attacker-controlled bytes to
arbitrary filesystem paths (LOG-4). **TLS-1** does not mitigate
any of this in the default configuration. **NA-5/NA-6**
independently allow an off-path attacker on the unencrypted
wire to flip the cluster master via fabricated/replayed
election messages.

## Disposition

This review is preserved as evidence of the v1.3.0 security
posture. Each finding is now an open issue for a future
release. Until the **NA-1 / NA-2 / NA-3 / NA-5 / NA-6 / LOG-4 /
LOG-2** items are closed, replication should not be deployed
across an untrusted network boundary, and the README's
documentation has been updated to call this out explicitly.

### Update — v1.4.1

`v1.4.1` (commit `f2ba3d5`) closes the bug-class subset of
this review:

- **LOG-2 closed**: `MAX_FRAME_PAYLOAD = 64 MiB` enforced
  across every `Channel` impl. A single attacker frame can no
  longer trigger a 4 GiB allocation.
- **LOG-3, LOG-5, LOG-6, LOG-7 closed**: item-size cap
  centralised, unknown entry types logged at error,
  recovery + replica reject out-of-order VLSNs.
- **LOG-4 closed**: `validate_restore_filename` rejects
  empty / `.` / `..` / dotfile / path-separator / NUL.
- **TLS-2, TLS-3, TLS-4 closed**: silent empty trust-store,
  malformed PEM, and mTLS-on-tls-native are now `Err`s.
- **LOG-8, LOG-9, LOG-10 closed**: VLSN sentinel rejection,
  feeder warns on negative VLSN, replica skips unknown entry
  types.

Still open (the auth-class blockers — see
`auth-mtls-design-2026-05.md` for the in-flight plan):

- **NA-1, NA-2, NA-3, NA-5, NA-6**: replication wire still
  has no authentication. The `chore/auth-mtls-by-default`
  branch starts the foundation: `RepConfig::peer_allowlist`,
  `TlsConfig::for_replication` (a stricter constructor that
  rejects `SelfSigned` identity and `SkipVerification`
  trust), and the `noxu_rep::auth::PeerAllowlist` matching
  primitive. Phase 2 (dispatcher integration) is not landed.
- **NA-4, NA-7, NA-8**: subsumed by the same plan.
- **TLS-1**: addressed by Phase 3 of the same plan
  (deprecate the silent skip-verify default).

The deployment guidance in `known-limitations.md` is
unchanged — replication still must NOT be deployed across an
untrusted network boundary in v1.4.x.
