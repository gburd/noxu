# Crate Guide

All 22 crates in the Noxu DB workspace, with purpose, key files, critical
types, and crate purpose.

## Phase 0 — Foundation

### `noxu-util`

 the corresponding Noxu type

Core types used across all crates.

| Type | Description |
|---|---|
| `Lsn` | 64-bit `(file_number, offset)` pair; `NULL_LSN = 0` |
| `Vlsn` | 64-bit signed replication sequence number; `NULL_VLSN = i64::MIN` |
| `PackedInteger` | Variable-length integer encoding (Noxu's `PackedInteger`) |
| `StatGroup` | Hierarchical statistics registry |
| `DaemonThread` | Background thread lifecycle management |

Re-exports: `Lsn`, `Vlsn`, `NULL_LSN` at crate root.

### `noxu-latch`

 the corresponding Noxu type

Thin wrappers around `parking_lot`:

- `ExclusiveLatch<T>` — RAII exclusive latch (wraps `Mutex<T>`)
- `SharedLatch<T>` — RAII reader-writer latch (wraps `RwLock<T>`)

### `noxu-sync`

 the corresponding Noxu type

Internal sync primitives that sit below `noxu-latch` and replication
networking. Provides:

- `RawMutex` / `RawRwLock` — pluggable raw locking that can be swapped for
  parking_lot or libc futexes
- `Condvar` — condition variable that cooperates with the raw locks
- `Futex` — Linux futex wrappers (FFI to libc) used as a fallback path
- `Mutex<T>` / `RwLock<T>` — typed wrappers over the raw primitives

This crate hosts the bulk of the workspace's `unsafe` because it is the
syscall / raw-API boundary; everything above it consumes the safe
`Mutex` / `RwLock` types.

### `noxu-config`

 the corresponding Noxu type

400+ configuration parameters with validation. Key types:

- `EnvironmentConfig` / `EnvironmentConfigBuilder` — all 150+ env parameters
- `DatabaseConfig` — per-database options
- `TransactionConfig` — per-transaction options
- `DurabilityPolicy` / `SyncPolicy` / `ReplicaAckPolicy`
- `EnvironmentFailureReason` — 19 variants for invalidation

## Phase 1 — Storage

### `noxu-log`

 the corresponding Noxu type

The write-ahead log. All mutations go here first.

Key files:

- `src/file_manager.rs` — `FileManager`: file creation, rotation, handle LRU
- `src/log_manager.rs` — `LogManager`: write serialization, group commit, CRC32
- `src/buffer.rs` — `LogBuffer` / `LogBufferPool`: write buffering
- `src/readers/` — `FileReader`, `LastFileReader`, `CheckpointFileReader`, `CleanerFileReader`
- `src/entry_type.rs` — all log entry type codes

## Phase 2 — Data Structures

### `noxu-tree`

 the corresponding Noxu type

The B+tree. Key files:

- `src/tree.rs` — `Tree`: root management, `get/put/delete`, dirty node
  collection.  Also home to the runtime B-tree node types `BinStub` (BIN node:
  slots, key prefix, modification_times, delta tracking) and `InNodeStub` (IN
  upper node: child pointers).  These stubs are the implementation that runs;
  a property-based conformance test (`tests/bin_stub_conformance.rs`) pins
  `BinStub` to a JE-faithful oracle so the leaf-level semantics cannot drift
  (the former shelved faithful `bin::Bin` / `in_node::InNode` transliterations
  were removed under T-1).
- `src/ln.rs` — `Ln` (LN leaf node): key/value pair

Critical: `Tree::set_comparator()` / `take_comparator()` for `TwoPartKeyComparator`.

## Phase 3 — Transactions

### `noxu-txn`

 the corresponding Noxu type

Record-level locking and transaction lifecycle.

Key files:

- `src/lock_manager.rs` — `LockManager`: 64-shard lock table, waiter graph, deadlock detection
- `src/transaction.rs` — `Transaction`: locker hierarchy, undo records, commit/abort
- `src/locker.rs` — `Locker` trait, `BasicLocker`, `ThreadLocker`, `HandleLocker`
- `src/group_commit.rs` — `GroupCommit`: fsync batching for concurrent commits
- `src/txn_chain.rs` — `TxnChain` + `CompareSlot` + `RevertInfo` for partial rollback

## Phase 4 — Internals

### `noxu-dbi`

 the corresponding Noxu type

The bridge between the public API and internal subsystems.

Key files:

- `src/environment_impl.rs` — `EnvironmentImpl`: coordinates all subsystems, daemon lifecycle
- `src/database_impl.rs` — `DatabaseImpl`: tree ownership, recovered tree handling
- `src/cursor_impl.rs` — `CursorImpl`: all cursor operations, sorted-dup routing
- `src/memory_budget.rs` — `MemoryBudget`: explicit memory accounting

`EnvironmentImpl` fields: `checkpointer`, `primary_tree`, `cleaner`, `evictor`,
`evictor_handle`, `in_compressor_handle`, `data_eraser`, `extinction_scanner`,
`backup_manager`.

## Phase 5 — Background Services

### `noxu-evictor`

 the corresponding Noxu type

Dual-priority LRU cache eviction. Key type: `Evictor`.

### `noxu-cleaner`

 the corresponding Noxu type

Log GC pipeline. Key files:

- `src/cleaner.rs` — `Cleaner` daemon
- `src/utilization_profile.rs` — `UtilizationProfile` / `FileSummary`
- `src/file_selector.rs` — `FileSelector`
- `src/file_processor.rs` — `FileProcessor`
- `src/cleaner_throttle.rs` — `CleanerThrottle`
- `src/data_eraser.rs` — `DataEraser` (Noxu)
- `src/extinction_scanner.rs` — `ExtinctionScanner` (Noxu)

### `noxu-recovery`

 the corresponding Noxu type

Checkpoint and 3-phase crash recovery. Key file: `src/recovery_manager.rs`.

## Phase 6 — Orchestration

### `noxu-engine`

Daemon lifecycle and environment open/close coordination.

### `noxu-db`

 the corresponding Noxu type public API.

Public types: `Environment`, `Database`, `Cursor`, `Transaction`,
`DatabaseEntry`, `OperationStatus`, `SecondaryDatabase`, `JoinCursor`.

## Phase 7 — Higher-Level APIs

### `noxu-bind`

 the corresponding Noxu type

Serialization bindings:

- `TupleBinding<T>` — sort-preserving tuple encoding
- `EntryBinding<T>` — passthrough `&[u8]`
- `SerialBinding<T>` — serde-based binary serialization

### `noxu-collections`

 `noxu_collections`

`StoredMap<K,V>`, `StoredSet<K>`, `StoredList<V>`.

### `noxu-persist`

 `noxu_persist`

Trait-based entity persistence layer (Direct Persistence Layer). Users
implement `Entity` (declaring the primary-key type and entity name) and
an `EntitySerializer` (manual byte serialization) for their types.
`PrimaryIndex<K, E>` and `SecondaryIndex<K, E>` provide typed CRUD and
range scans on top of `Database`. Schema evolution mutations live in
`src/evolve/` (`Renamer`, `Deleter`, `Converter`). Derive macros are
provided by `noxu-persist-derive` (see below) and re-exported by the `noxu`
umbrella at `noxu::persist::*`.
Key type: `EntityStore`.

### `noxu-persist-derive`

Procedural macro crate that provides `#[derive(Entity)]`, `#[derive(PrimaryKey)]`,
and `#[derive(SecondaryKey)]`. These derive macros emit `::noxu::persist::` paths
in generated code, so users must depend on the `noxu` umbrella crate (not
`noxu-persist` alone). The umbrella re-exports the derives at `noxu::persist::*`.

## Phase 7b — Distributed Transactions

### `noxu-xa`

XA (X/Open) distributed transaction support.

Key files:

- `src/environment.rs` — `XaEnvironment`: wraps `Environment`, manages branch state machine
- `src/resource.rs` — `XaResource` trait: `xa_start`/`xa_end`/`xa_prepare`/`xa_commit`/`xa_rollback`/`xa_recover`/`xa_forget`
- `src/xid.rs` — `Xid`: format_id + global_transaction_id + branch_qualifier
- `src/flags.rs` — `XaFlags`: NOFLAGS, JOIN, RESUME, TMSUCCESS, TMFAIL, TMSUSPEND, ONEPHASE
- `src/error.rs` — `XaError`, `PrepareResult` (Ok | ReadOnly)
- `tests/xa_chaos_test.rs` — multi-cluster chaos, scale, and performance tests
- `tests/xa_protocol_test.rs` — deterministic protocol corner-case coverage (51 tests)

State machine per Xid:

```text
[none] → xa_start → Active → xa_end(SUCCESS) → Idle → xa_prepare → Prepared → xa_commit → [done]
                           → xa_end(SUSPEND) → Suspended → xa_start(RESUME) → Active
                           → xa_end(FAIL) → RollbackOnly → xa_rollback → [done]
                                             Idle → xa_rollback → [done]
                                             Idle → xa_commit(ONEPHASE) → [done]
                                             Prepared → xa_rollback → [done]
```

## Phase 8 — Replication

### `noxu-rep`

 the corresponding Noxu type

Master-replica HA. Key files:

- `src/replicated_environment.rs` — `ReplicatedEnvironment`
- `src/elections/paxos.rs` — FPaxos proposer/acceptor
- `src/elections/phi_detector.rs` — phi accrual failure detector
- `src/quorum_policy.rs` — `QuorumPolicy`, `QuorumSystem` via quoracle
- `src/rep_group.rs` — `RepGroup`, `RepNode`, `NodeInfo`
- `src/net/channel.rs` — `TcpChannel`, `TcpChannelListener`
- `src/net/quic_channel.rs` — `QuicChannel`
- `src/net/quic_mux.rs` — `QuicMultiplexedChannel`
- `src/stream/feeder.rs` — `FeederRunner`, `PeerLogScanner`
- `src/stream/peer_feeder.rs` — `PeerFeederService`, `MultiPeerCatchUp`
- `src/stream/replica_stream.rs` — `ReplicaStream`, frame parsing + CRC32 verification
- `tests/torture_test.rs` — chaos/soak test harness

## Cross-cutting

### `noxu-observe`

 the corresponding Noxu type

Optional observability glue. Re-exports a small set of helpers so other
crates can opt in to `tracing` spans, `metrics` counters/gauges, and
OpenTelemetry export without each crate growing its own observability
dependency tree. Off by default — only pulled in when the consuming
crate enables the `observability` (or `otel`) feature. No public API
beyond a few thin wrappers; see `crates/noxu-observe/src/lib.rs`.

### `noxu` (umbrella)

The single user-facing crate. Re-exports the entire public API of all
component crates under one name and version. Users add `noxu = "3"` to
their `Cargo.toml` and receive everything: core engine, collections,
persistence layer, XA, and optionally replication and observability via
feature flags.

The umbrella is also necessary for the `#[derive(Entity)]` / `#[derive(PrimaryKey)]`
/ `#[derive(SecondaryKey)]` macros, because the generated code references
`::noxu::persist::` paths.

Key file: `crates/noxu/src/lib.rs` — all re-exports.

### `noxu-spec`

Stateright executable specifications of the protocols the engine implements.
These are **abstract protocol models**: they model-check the protocol design's
safety/liveness properties, not a mechanical refinement of the Rust code. Model
↔ code correspondence is maintained by review convention (two specs —
`lock_manager_deadlock` and `xa_two_phase_commit` — additionally anchor to
production types at compile time). A passing spec proves the *protocol* is
safe; it does not by itself prove the implementation matches the model.
Each spec is a `cargo test` case; failures print a counterexample trace.
Run with `make spec`.

Covers: B+tree latching, Flexible Paxos elections, WAL group-commit,
crash recovery (analysis/redo/undo), lock manager deadlock detection,
VLSN streaming, master transfer, network restore, XA two-phase commit,
cleaner safety, cache\u2194cleaner ordering.

All specs carry a `VALIDATED-AS-OF` stamp (see spec headers) indicating
the last version at which the spec was confirmed to match the production
code. Re-run `make spec` when updating a modelled subsystem and update the
stamp accordingly.
