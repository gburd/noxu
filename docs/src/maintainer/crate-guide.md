# Crate Guide

All 16 crates in the Noxu DB workspace, with purpose, key files, critical
types, and JE correspondence.

## Phase 0 — Foundation

### `noxu-util`
**JE**: `com.sleepycat.je.utilint`

Core types used across all crates.

| Type | Description |
|---|---|
| `Lsn` | 64-bit `(file_number, offset)` pair; `NULL_LSN = 0` |
| `Vlsn` | 64-bit signed replication sequence number; `NULL_VLSN = i64::MIN` |
| `PackedInteger` | Variable-length integer encoding (JE's `PackedInteger`) |
| `StatGroup` | Hierarchical statistics registry |
| `DaemonThread` | Background thread lifecycle management |

Re-exports: `Lsn`, `Vlsn`, `NULL_LSN` at crate root.

### `noxu-latch`
**JE**: `com.sleepycat.je.latch`

Thin wrappers around `parking_lot`:
- `ExclusiveLatch<T>` — RAII exclusive latch (wraps `Mutex<T>`)
- `SharedLatch<T>` — RAII reader-writer latch (wraps `RwLock<T>`)

### `noxu-config`
**JE**: `com.sleepycat.je.config`

400+ configuration parameters with validation. Key types:
- `EnvironmentConfig` / `EnvironmentConfigBuilder` — all 150+ env parameters
- `DatabaseConfig` — per-database options
- `TransactionConfig` — per-transaction options
- `DurabilityPolicy` / `SyncPolicy` / `ReplicaAckPolicy`
- `EnvironmentFailureReason` — 19 variants for invalidation

## Phase 1 — Storage

### `noxu-log`
**JE**: `com.sleepycat.je.log`

The write-ahead log. All mutations go here first.

Key files:
- `src/file_manager.rs` — `FileManager`: file creation, rotation, handle LRU
- `src/log_manager.rs` — `LogManager`: write serialization, group commit, CRC32
- `src/buffer.rs` — `LogBuffer` / `LogBufferPool`: write buffering
- `src/readers/` — `FileReader`, `LastFileReader`, `CheckpointFileReader`, `CleanerFileReader`
- `src/entry_type.rs` — all log entry type codes

## Phase 2 — Data Structures

### `noxu-tree`
**JE**: `com.sleepycat.je.tree`

The B+tree. Key files:
- `src/tree.rs` — `Tree`: root management, `get/put/delete`, dirty node collection
- `src/bin.rs` — `Bin` (BIN node): slots, key prefix, modification_times, delta tracking
- `src/ln.rs` — `Ln` (LN leaf node): key/value pair
- `src/in_node.rs` — `InNode` (IN upper node): child pointers

Critical: `Tree::set_comparator()` / `take_comparator()` for `TwoPartKeyComparator`.

## Phase 3 — Transactions

### `noxu-txn`
**JE**: `com.sleepycat.je.txn`

Record-level locking and transaction lifecycle.

Key files:
- `src/lock_manager.rs` — `LockManager`: 64-shard lock table, waiter graph, deadlock detection
- `src/transaction.rs` — `Transaction`: locker hierarchy, undo records, commit/abort
- `src/locker.rs` — `Locker` trait, `BasicLocker`, `ThreadLocker`, `HandleLocker`
- `src/group_commit.rs` — `GroupCommit`: fsync batching for concurrent commits
- `src/txn_chain.rs` — `TxnChain` + `CompareSlot` + `RevertInfo` for partial rollback

## Phase 4 — Internals

### `noxu-dbi`
**JE**: `com.sleepycat.je.dbi`

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
**JE**: `com.sleepycat.je.evictor`

Dual-priority LRU cache eviction. Key type: `Evictor`.

### `noxu-cleaner`
**JE**: `com.sleepycat.je.cleaner`

Log GC pipeline. Key files:
- `src/cleaner.rs` — `Cleaner` daemon
- `src/utilization_profile.rs` — `UtilizationProfile` / `FileSummary`
- `src/file_selector.rs` — `FileSelector`
- `src/file_processor.rs` — `FileProcessor`
- `src/cleaner_throttle.rs` — `CleanerThrottle`
- `src/data_eraser.rs` — `DataEraser` (NoSQL)
- `src/extinction_scanner.rs` — `ExtinctionScanner` (NoSQL)

### `noxu-recovery`
**JE**: `com.sleepycat.je.recovery`

Checkpoint and 3-phase crash recovery. Key file: `src/recovery_manager.rs`.

## Phase 6 — Orchestration

### `noxu-engine`
Daemon lifecycle and environment open/close coordination.

### `noxu-db`
**JE**: `com.sleepycat.je` public API.

Public types: `Environment`, `Database`, `Cursor`, `Transaction`,
`DatabaseEntry`, `OperationStatus`, `SecondaryDatabase`, `JoinCursor`.

## Phase 7 — Higher-Level APIs

### `noxu-bind`
**JE**: `com.sleepycat.bind`

Serialization bindings:
- `TupleBinding<T>` — sort-preserving tuple encoding
- `EntryBinding<T>` — passthrough `&[u8]`
- `SerialBinding<T>` — serde-based binary serialization

### `noxu-collections`
**JE**: `com.sleepycat.collections`

`StoredMap<K,V>`, `StoredSet<K>`, `StoredList<V>`.

### `noxu-persist`
**JE**: `com.sleepycat.persist`

DPL derive macros: `#[derive(Entity)]`, `#[primary_key]`, `#[secondary_key]`.
Key type: `EntityStore`.

## Phase 8 — Replication

### `noxu-rep`
**JE**: `com.sleepycat.je.rep`

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
