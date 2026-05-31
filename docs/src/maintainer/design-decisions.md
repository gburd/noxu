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
generated code so derive-macro users must depend on `noxu` directly (not
only on `noxu-persist`). Components remain individually publishable for
engine-internal extension work.

**Why**: Reduces dependency graph complexity for users; a single version pin
captures the entire engine. Derive macro path coupling is a deliberate
trade-off: it guarantees generated code always resolves against the
user-visible umbrella namespace rather than an internal crate path.

**Consequence**: Any user of `#[derive(Entity)]`, `#[derive(PrimaryKey)]`, or
`#[derive(SecondaryKey)]` must have `noxu = "3"` in their `Cargo.toml`.
A future escape hatch (`#[entity(crate = "...")]`, following the `serde`
pattern) can relax this for users who depend on `noxu-persist` directly.

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
