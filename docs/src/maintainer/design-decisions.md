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

**Consequence**: Noxu tools cannot read Noxu log files. Migration between Noxu
and Noxu requires an export/import step at the application level.

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

## 8. No unsafe in Library Code

**Decision**: Zero `unsafe` in library code, with exactly two exceptions:
`memmap2` usage for memory-mapped files and off-heap cache storage.

**Why**: Safety is a primary project goal. The exceptions are justified by
the documented safety invariants of the mmap syscall.

**Where unsafe appears**: `crates/noxu-log/src/file_manager.rs` (mmap),
`crates/noxu-evictor/src/off_heap.rs` (off-heap BIN storage).
