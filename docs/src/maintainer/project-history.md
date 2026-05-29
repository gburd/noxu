# Project History

Noxu DB is an embedded transactional key-value database engine written in Rust. This
page documents why the project exists and how it evolved session by session.

## Why Noxu DB Exists

Noxu DB is an embedded transactional key-value database engine written
in Rust, modeled on Berkeley DB Java Edition 7.5.11 plus 10
extended-fork enhancements.  The goal is to provide a dependency-light
embedded database with:

- The same API contract (familiar to embedded database users)
- The same algorithm fidelity (proven storage engineering)
- No JVM — eliminates GC pauses, JVM startup overhead, and deployment complexity
- Idiomatic Rust — `thiserror`, `parking_lot`, `Arc<RwLock<T>>`, RAII latches

All 10 extended-fork enhancements are included (Record Extinction, TTL,
Group Commit, ByteComparator, DataEraser, ExtinctionFilter, ScanFilter,
UncachedLN, BackupManager, AsyncAcks).

## Development Timeline

| Sessions | Major milestone |
|---|---|
| 1–22 | Foundation: B-tree, WAL, transaction manager, recovery, evictor, cleaner, checkpointer, public API |
| 23 | First full benchmark comparison — Noxu reads 25x faster than Noxu at 1K scale (no JVM warmup) |
| 24 | BIN-delta chaining, Sequence transactions, upper-IN cleaner, comment audit |
| 25 | 10 Noxu enhancements: ByteComparator, ScanFilter, ExtinctionFilter, GroupCommit, per-slot BIN times, VerifyCheckpointInterval, DataEraser, ExtinctionScanner, BackupManager |
| 26 | Lock/latch hierarchy: Locker trait, ThreadLocker sharing, HandleLocker buddy system, DummyLockManager wired, TxnChain for replication partial rollback |
| 27 | Non-standard write-buffering (superseded by Session 28) |
| 28 | **Critical fix**: Replaced tentative MVCC with Noxu's lock-based isolation. Writers block readers; no snapshot isolation. |
| 29 | Replication wiring: EnvironmentLogScanner, EnvironmentLogWriter, NetworkRestore TCP, GroupCommit wiring, TTL file selection, O(1) Database::count() |
| 31 | Adaptive cleaner throttling; replication server-side network restore provider. Canonical NVMe benchmarks. |
| 36 | EnvironmentConfig 100% parameter coverage (150+ params), typed EnvironmentFailureReason (19 variants), ExceptionListener trait, is_valid() |
| 37 | P0-P2 production hardening: LM concurrency (incremental waiter graph), cleaner back-pressure wiring, sorted-dup cursor routing, join cursor |
| 38 | QuorumPolicy (SimpleMajority/Flexible/Expression), PhiAccrualDetector, RepNode capacity/latency hints, quoracle integration, FPaxos split-phase, dynamic add_peer/remove_peer |
| 39 | QUIC multiplexed channels: 4 independent streams per connection, ReconnectToken, VLSN datagrams, chaos test fixes |
| 40 | Soak bugs fixed (TCP timeout, PMTUD assertion, SYN hang), adaptive phase timeout (phi-derived), update_peer_metadata(), dynamic membership chaos phases, docs/replication.md |

## Current Fidelity

As of Session 40:

- **Named-algorithm completeness: ~92% (all major algorithms implemented)
- **Operational completeness**: ~85% (API surface coverage)
- **Production hardening**: ~100% (EnvironmentConfig, ExceptionListener, is_valid())
- **Zero clippy errors** on `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- **623 noxu-rep tests passing**, 5,000+ tests across all crates

## Key Design Decisions

| Deviation | Reason |
|---|---|
| Rust-native log format | Rust serialization is simpler and safer than Java's |
| `TupleSerdeBinding` uses serde binary encoding, not sort-preserving tuple encoding | Per-project decision; Noxu's sort-preserving encoding is complex to port |
| No XA/two-phase commit implementation | Out of scope for initial port |
| QUIC transport | Rust has no equivalent to Java NIO; QUIC provides similar multiplexing |
