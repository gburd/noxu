# Design Decisions

This page documents the "why" behind non-obvious implementation choices in
Noxu DB. Future maintainers encountering these decisions should read this
before changing them.

## 1. Lock-Based Isolation (Not MVCC)

**Decision**: Noxu DB uses record-level locking. Writers hold locks until
commit or abort. Readers block on write-locked records.

**Why**: This is Noxu's isolation model. Noxu was designed for embedded use
where a single application controls both readers and writers. MVCC trades
storage and GC overhead for non-blocking reads — a different point in the
design space. Noxu DB requires the same isolation semantics.

**Consequence**: Under high write concurrency, readers can block. Use
`txn_timeout_ms` to bound wait times. Use `ReadUncommitted` isolation
(the only non-blocking isolation in Noxu) for analytics.

**Where**: `crates/noxu-txn/src/`, `crates/noxu-dbi/src/cursor_impl.rs`
**Session**: Corrected in Session 28 after a tentative write-buffering approach was tried and removed.

## 2. CRC32 Not CRC32C for Replication Feeder Protocol

**Decision**: The replication feeder frame header uses CRC32 (via `crc32fast`)
not CRC32C (via `crc32c`).

**Why**: On x86-64, `crc32fast` uses PCLMULQDQ and achieves ~18 GiB/s vs
~4 GiB/s for `crc32c`. At typical payload sizes (256B+), CRC32 is 3.8–4.4x
faster. `crc32fast` is already a workspace dependency for log entry checksums;
adding `crc32c` would increase build complexity for no benefit on x86-64.

**Trade-off**: CRC32C would be 15% faster at 64B payloads and would have
hardware acceleration on ARM (SSE4.2 crc32c instruction). If ARM becomes
a primary deployment target, reconsider.

**Evidence**: Benchmarks in `crates/noxu-util/benches/util_bench.rs`.
**Decision document**: `docs/src/internal/checksum-selection.md`.

## 3. Rust-Native Log Format

**Decision**: `.ndb` files use a Rust-native encoding, not Noxu's Java
serialization format.

**Why**: The alternative uses Java's object serialization and class-based dispatch for
log entries. Porting this faithfully would require reimplementing Java's
serialization protocol — complex, brittle, and not idiomatic Rust.
The log format is an internal implementation detail; applications use the
public API, not the log files.

**Consequence**: Noxu DB tools cannot read BDB-JE (`.jdb`) log files. Migration
from BDB-JE to Noxu DB requires an export/import step at the application layer.

## 4. TupleSerdeBinding Uses Serde Binary Encoding

**Decision**: `TupleSerdeBinding` uses serde's binary encoding, not
sort-preserving tuple encoding.

**Why**: Sort-preserving encoding is complex to implement correctly for all
Rust types (especially signed integers and floats). Per project decision,
this is an accepted deviation.

**Consequence**: `StoredMap<K, V>` with `TupleSerdeBinding` does **not**
maintain sort order by K's Rust `Ord` value. Use `TupleBinding<T>` with
explicit big-endian integer encoding for sorted keys.

## 5. TCP + QUIC Transports (Not Java NIO)

**Decision**: Replication uses `TcpChannel` (default) and
`QuicMultiplexedChannel` (optional `quic` feature), not Java's NIO or Netty.

**Why**: Java NIO has no Rust equivalent. QUIC (via `quinn`) provides the same
multiplexed stream model as Noxu's HA transport while being a first-class Rust
library. TCP is simpler and requires no TLS setup.

**QUIC PMTUD disabled**: `mtu_discovery_config(None)` on all QUIC configs
because PMTUD probes are corrupted by tc netem and trigger a quinn-proto
assertion at `mtud.rs:88`. On loopback (where tests run), MTU is 65535 and
PMTUD adds no value.

## 6. Per-BIN Interior Mutability

**Decision**: Each BIN is wrapped in `Arc<RwLock<Bin>>`.

**Why**: Allows concurrent readers to different BINs without contending on a
tree-level lock. Added in Session 26 as a performance optimization matching
Noxu's per-BIN latch model.

**Trade-off**: Each BIN requires an allocation for the `RwLock`. For
write-heavy workloads with many small BINs, the allocation overhead is
visible. Accepted: correct and performant for typical mixed workloads.

## 7. Blocking I/O in Core Engine (No async)

**Decision**: `noxu-db` through `noxu-recovery` use blocking I/O. `noxu-rep`
networking may use tokio but the core engine does not.

**Why**: Noxu uses blocking I/O with explicit daemon threads. Async would
require pervasive `await` throughout the codebase, complicating porting and
making the latch hierarchy harder to reason about. Background daemon threads
(evictor, cleaner, etc.) are straightforward to implement with blocking I/O.

**Exception**: `noxu-rep` uses tokio for the QUIC transport because `quinn`
requires an async runtime. The interface between `noxu-rep` and the core
engine is synchronous.

## 8. Limited unsafe in Library Code

**Decision**: Core data-path crates (`noxu-tree`, `noxu-txn`,
`noxu-evictor`, `noxu-cleaner`, `noxu-recovery`, `noxu-dbi`,
`noxu-engine`, `noxu-bind`, `noxu-collections`, `noxu-persist`,
`noxu-config`, `noxu-util`) target zero `unsafe`. New `unsafe` in those
crates needs review and a justification comment.

**Why**: Safety is a primary project goal. Confining `unsafe` to a small
set of well-understood subsystems makes correctness easier to audit.

**Where unsafe is allowed and why**:

| Location | Reason |
|---|---|
| `crates/noxu-sync/src/{raw_mutex,raw_rwlock,condvar,futex}.rs` | FFI to libc futex syscalls and `parking_lot` raw lock-API operations |
| `crates/noxu-log/src/file_manager.rs` | Memory-mapped I/O for log files |
| `crates/noxu-rep/src/**` | Network I/O glue (TLS handshake, raw socket options) |
| Single-line blocks in `noxu-latch`, `noxu-db`, `noxu-xa` | Each documented inline |

## 9. Single Umbrella Crate (`noxu = "3"`)

**Decision**: All component crates are accessible through a single `noxu`
umbrella crate. `noxu-persist-derive` emits `::noxu::persist::` paths in
generated code by default so derive-macro users can depend on `noxu` without
additional configuration. Components remain individually publishable for
engine-internal extension work.

**Why**: Reduces dependency graph complexity for users; a single version pin
captures the entire engine. Derive macro path coupling is a deliberate
trade-off: it guarantees generated code always resolves against the
user-visible umbrella namespace rather than an internal crate path.

**Consequence**: By default, users of `#[derive(Entity)]`,
`#[derive(PrimaryKey)]`, or `#[derive(SecondaryKey)]` must have
`noxu = "3"` in their `Cargo.toml` — unless they use the escape hatch
described below.

**v3.1.0 — Escape hatch implemented** (Wave FA): Users who depend on
`noxu-persist` directly (without the umbrella) can add
`#[entity(crate = "noxu_persist")]` to each annotated struct.  Generated
code then uses `::noxu_persist::…` paths instead of `::noxu::persist::…`,
following the `serde` / `#[serde(crate = "…")]` pattern.  The attribute is
recognised by all three derives; the default behaviour is unchanged.  See
`docs/src/collections/entity-persistence.md` § *Crate-path override* and
the 2026 review.

## 10. `cache_size` = Total Memory Budget (v3.0, X-12)

**Decision**: `EnvironmentConfig::with_cache_size(n)` sets the **total** memory
ceiling for the environment: the Arbiter budget for the B-tree node pool is
`n − log_buffer_total − off_heap_reserved` (floor: 1 MiB).

**Why**: Matches JE semantics for `setCacheSize`. Users migrating from JE
should not need to reason about sub-budget splits. Eliminates the previous
surprise where log buffers and off-heap storage expanded silently beyond the
configured cache ceiling.

**Consequence (v3.0 breaking change from v2.x)**: Users who set a small
`cache_size` with default log buffer settings may find the Arbiter
initialized at the 1 MiB floor. Increase `cache_size` or reduce
`log_num_buffers` × `log_buffer_size` to restore the previous balance.
See `docs/src/operations/configuration.md` and `docs/src/operations/sizing.md`.

## 11. mTLS Phase 2 Landed — peer_allowlist Enforced (v3.1.0)

**Decision**: mTLS peer enforcement landed in v3.1.0 (branch
`fix/fb-mtls-phase2`).  `PeerAllowlistVerifier` implements
`rustls::server::danger::ClientCertVerifier` and is wired into the
rustls `ServerConfig` via
`TlsConfig::to_rustls_server_config_with_allowlist` and
`TlsTcpChannelListener::bind_with_tls_and_allowlist`.

**Enforcement model**:

1. The server requests a client certificate (mandatory).
2. Chain validation via `WebPkiClientVerifier` against the configured CA.
3. Subject CN and DNS SANs extracted via a minimal DER parser (no new deps).
4. At least one name must match a `peer_allowlist` entry
   (case-insensitive, exact match, no wildcards).
5. Empty allowlist = `ConfigError` at construction (fail-closed per design
   doc).

**Client-side**: `to_rustls_client_config` calls `with_client_auth_cert`
for `PemFiles`/`PemBytes` identities so the server can verify the peer.

**Remaining gap (Phase 3)**: `ReplicatedEnvironment::new` uses a plain
`TcpServiceDispatcher`.  Full end-to-end enforcement requires callers to
use `TlsTcpChannelListener::bind_with_tls_and_allowlist` directly.
See `docs/src/internal/auth-mtls-design-2026-05.md`.

## Lock storage: a side table, not embedded in the BIN slot (TXN-11)

**Decision**: Locks are stored in a sharded `HashMap<LSN, Lock>` keyed by record LSN,
not embedded in the BIN slot the way JE embeds a `ThinLockImpl` in the IN.

**Why**: Noxu's BIN slots (`BinEntry`) do not carry lock state — the lock table is a
separate structure in `noxu-txn`, consistent with the lock-based-isolation-not-in-tree
architecture. This is a consequence of keeping the tree (`noxu-tree`) free of any
`noxu-txn` dependency.

**What is preserved**: the JE thin-lock *memory win* IS reproduced — `Lock = Thin(ThinLockImpl)
| Full(LockImpl)` starts thin (single owner, no waiters) and inflates to full only on the
second owner or first waiter, and an unlocked record costs **zero** lock-table memory (the
entry is removed on last release). So the cheap-uncontended / inflate-on-contention /
zero-standing-cost properties match JE; only the storage *locus* (side table vs in-slot) differs.

**Trade-off**: each *currently held* lock costs a hash-table entry rather than an in-slot
field, and the lock is not co-located with the BIN. For the common case (few locks held at
once relative to the resident node count) this is immaterial. This is an **authorized
deviation**, not a fidelity gap.

## Diverged-tail replica syncup rollback (REP-1)

**Status**: the *durable rollback machinery* (REP-1 STEPS 1-4) is implemented and
faithful; the *live replica-feeder matchpoint protocol* (STEP 5) is partially
implemented — the decision core is done and unit-tested, the networked driver is
the remaining piece.

**What this is**: when a replica reconnects to a (possibly new) master, the two
must reconcile by finding the latest common log entry (the *matchpoint*) and, if
the replica has applied entries past it (a *diverged tail*, e.g. after a failed
election), ROLLING THOSE BACK to the matchpoint before resuming the stream from
`matchpoint + 1`. Port of JE `ReplicaFeederSyncup` / `FeederReplicaSyncup`.

### Done (STEPS 1-4) — durable rollback machinery

These make recovery-time self-rollback faithful and are the substrate the live
syncup uses:

1. **RollbackStart/RollbackEnd entries** carry `matchpointVLSN` + `activeTxnIds`
   (RollbackStart) and `matchpointLSN` (RollbackEnd), matching JE
   `RollbackStart.java` / `RollbackEnd.java`. On-disk format change for the HA
   rollback entries (see CHANGELOG).
2. **`RollbackTracker.RollbackPeriod.containsLN`** gates rollback on
   `activeTxnIds`, so a committed/aborted txn's LNs in the window are excluded
   (JE `RollbackTracker.RollbackPeriod`).
3. **`TxnChain`** reverts each rolled-back LN to its *immediately previous
   version* (intra- or inter-txnal), split at the matchpoint, instead of
   skipping it (JE `txn/TxnChain.java`, `RecoveryManager.rollbackUndo`). The
   recovery undo path drives it.
4. **Invisible re-marking + fsync** during recovery for open-ended (crash-mid-
   rollback) periods, with the entry checksum cloaking the invisible bit (JE
   `RollbackTracker.singlePassSetInvisible` / `recoveryEndFsyncInvisible`,
   `LogEntryHeader.turnOffInvisible`).

### Done (STEP 5, decision core) — `stream/syncup.rs`

- **`find_matchpoint`** (JE `ReplicaFeederSyncup.findMatchpoint`): the matchpoint
  search — highest VLSN both nodes hold with a matching record at the same LSN,
  scanning the replica's sync points backward from `lastSync`. Pure function over
  a `SyncupView` (the replica's VLSN index + the feeder's `EntryRequest`
  responses).
- **`verify_rollback`** (JE `ReplicaFeederSyncup.verifyRollback` truth table):
  decides `RollbackToMatchpoint` / `HardRecovery` / `NetworkRestore` from the
  matchpoint, `lastTxnEnd`, `lastSync` and `numPassedCommits`.

### Remaining (STEP 5, live driver)

To turn the decision core into a live syncup, the replica stream driver
(`stream/replica_stream.rs` + `stream/peer_feeder.rs`) needs:

1. **Wire protocol** extension: `EntryRequest(vlsn)` / `EntryNotFound` /
   `AlternateMatchpoint(vlsn)` messages (JE `BaseProtocol`) so the replica can
   ask the feeder for the record at each candidate VLSN and the feeder can
   counter-offer the highest VLSN it holds.
2. **`ReplicaSyncupReader`**: a backward log reader on the replica that yields,
   for each sync-point VLSN, its LSN + record fingerprint + `numPassedCommits`
   (the `SyncupView::entry` substrate against real log records — the VLSN index
   alone does not store per-VLSN sync flags or checksums, matching JE which
   re-reads the log).
3. **Live `replay.rollback(matchpointVLSN, matchpointLSN)`**: execute the
   rollback against the *running* engine — truncate the in-memory B-tree and the
   log to the matchpoint, writing a `RollbackStart`/`RollbackEnd` pair (so a
   crash mid-rollback recovers via STEPS 1-4) and marking the divergent tail
   invisible. The recovery-time X-1 self-rollback (`recovery_rollback_matchpoint`
   → `vlsn_index.truncate_after`) is the equivalent at restart; the live path
   shares STEPS 1-4's durable entries and invisible-marking.
4. **`vlsnIndex.truncateFromTail(startVLSN, matchpointLSN)`** then resume
   streaming from `matchpoint + 1`.

`negotiate_syncup` (currently a range CanServe/NeedsRestore check) is extended by
the decision core; the live driver replaces the range check with
`find_matchpoint` + `verify_rollback` once the wire protocol and syncup reader
land.
