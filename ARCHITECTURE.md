# Noxu DB Architecture

Noxu DB is an embedded transactional key-value database written in Rust. This document describes the system architecture, data flow, on-disk format, concurrency model, and recovery protocol.

## Data Flow

A write operation flows through the system as follows:

```
Application
    |
    v
noxu-db         Public API: Environment, Database, Cursor, Transaction
    |
    v
noxu-engine     Engine orchestration, daemon coordination
    |
    v
noxu-dbi        EnvironmentImpl, DatabaseImpl, CursorImpl
    |
    +----------+----------+
    |          |          |
    v          v          v
noxu-tree   noxu-txn   noxu-log
B+tree      Locking     Write-ahead log
    |          |          |
    +----------+----------+
               |
    +----------+----------+---------+
    |          |          |         |
    v          v          v         v
noxu-evictor  noxu-cleaner  noxu-recovery  noxu-rep
Cache mgmt    GC             Checkpoint     Replication
```

A read follows the same path down to `noxu-dbi`, which searches the B+tree (`noxu-tree`) and acquires read locks (`noxu-txn`). If the target node is not in cache, the log is read (`noxu-log`) and the node is loaded into the tree.

## Crate Dependency Graph

The 19 crates form a layered dependency structure:

```
Layer 0 (Foundation):
    noxu-util          Core types: LSN, VLSN, packed integers, stats
    noxu-sync          Internal sync primitives (raw locks, condvar, futex)
    noxu-latch         Latches wrapping parking_lot
    noxu-config        400+ configuration parameters

Layer 1 (Storage):
    noxu-log           Write-ahead log, file manager, buffers, readers
        depends on: noxu-util, noxu-latch, noxu-config

Layer 2 (Data Structures):
    noxu-tree          B+tree nodes (IN, BIN, LN), search, split
        depends on: noxu-util, noxu-latch, noxu-log

Layer 3 (Transactions):
    noxu-txn           Lock manager, deadlock detection, transaction lifecycle
        depends on: noxu-util, noxu-latch, noxu-config

Layer 4 (Internals):
    noxu-dbi           EnvironmentImpl, DatabaseImpl, CursorImpl, MemoryBudget
        depends on: noxu-util, noxu-latch, noxu-config, noxu-log, noxu-tree, noxu-txn

Layer 5 (Background Services):
    noxu-evictor       LRU/CLOCK/LIRS/ARC/CAR cache eviction, memory budget enforcement
    noxu-cleaner       Log garbage collection, utilization tracking
    noxu-recovery      Checkpointing, 3-phase crash recovery
        depend on: layers 0-4

Layer 6 (Orchestration):
    noxu-engine        Daemon lifecycle, environment open/close
    noxu-db            Public API
        depend on: layers 0-5

Layer 7 (Higher-Level APIs):
    noxu-bind          Serialization bindings (tuple, entry, serial)
    noxu-collections   Iterator-based collection views
    noxu-persist       Trait-based entity persistence (DPL)
        depend on: noxu-db

Layer 7b (Distributed Transactions):
    noxu-xa            X/Open XA two-phase commit
        depends on: noxu-db, noxu-engine

Layer 8 (Replication):
    noxu-rep           Master-replica HA, elections, VLSN index
        depends on: layers 0-6

Cross-cutting:
    noxu-observe       Optional `tracing`/`metrics`/OpenTelemetry glue
        depends on: nothing in noxu (pluggable)
```

## Key Subsystems

### Write-Ahead Log (noxu-log)

The log is the foundation of durability. All mutations are written to the log before being applied. The log is structured as a sequence of numbered files in a single directory.

**FileManager** presents the abstraction of one contiguous logical log built from physical files. It handles file creation, rotation at configurable size boundaries, file handle caching with LRU eviction, and LSN allocation. Files are named with 8-digit lowercase hex numbers and the `.ndb` extension (e.g., `00000000.ndb`, `0000002a.ndb`).

**LogManager** is the central coordinator for log writes and reads. It manages a pool of write buffers (`LogBufferPool`), serializes concurrent writers, computes CRC32 checksums, and coordinates fsync. Reads are served from the buffer pool (for recent writes) or from disk via the file manager.

**LogBuffer / LogBufferPool** provide write buffering. A pool of reusable buffers absorbs burst writes. When a buffer fills, it is flushed to disk and recycled.

**File readers** (`FileReader`, `LastFileReader`, `CheckpointFileReader`, `CleanerFileReader`, etc.) provide sequential scanning of the log for recovery, cleaning, and other purposes.

### Entry Header Format

Every log entry begins with a header:

```
Offset  Size  Field
------  ----  -----
0       4     Checksum (CRC32, little-endian)
4       1     Entry type
5       1     Flags
6       4     Previous entry offset (little-endian)
10      4     Item size (little-endian)
[14     8     VLSN (optional, present when VLSN_PRESENT flag is set)]
```

The base header is 14 bytes. When replication is active, entries carry an 8-byte VLSN (Virtual LSN) bringing the header to 22 bytes. Flag bits encode provisional status, replication status, invisibility, and VLSN presence.

An **LSN** (Log Sequence Number) uniquely identifies any log entry as a `(file_number: u32, offset: u32)` pair packed into a `u64`.

### B+tree (noxu-tree)

Data is stored in a B+tree with three node types:

- **IN (Internal Node)**: Upper-level tree nodes containing keys and child references. Each child reference points to another IN or a BIN.
- **BIN (Bottom Internal Node)**: Leaf-level internal nodes. Each slot in a BIN references an LN or contains an embedded LN for small records.
- **LN (Leaf Node)**: The actual data records (key-value pairs).

The tree supports:
- **Key prefix encoding**: Common prefixes among keys in a node are stored once, reducing memory and serialization cost.
- **BIN-deltas**: Instead of logging an entire BIN, only the changed slots are logged as a delta, reducing write amplification.
- **Embedded LNs**: Small data values are stored directly in the BIN slot, avoiding a separate LN allocation.
- **Latch-coupling traversal**: Tree descents acquire a latch on the child before releasing the parent latch, ensuring consistency without global locks.

Splits propagate upward when a node exceeds its maximum entry count. The root splits by creating a new root above it.

### Transaction Manager (noxu-txn)

Noxu DB provides serializable ACID transactions with record-level locking.

**Lock types** follow a compatibility matrix: READ, WRITE, and RANGE locks with well-defined conflict and upgrade rules. The lock manager maintains a lock table mapping `(database_id, key)` pairs to lock holders and waiters.

**Deadlock detection** identifies cycles in the waiter graph. When a deadlock is found, one transaction is selected as the victim and forced to abort.

**Locker hierarchy**: `BasicLocker` for non-transactional auto-commit operations, `ThreadLocker` for per-thread implicit locking, `HandleLocker` for cursor-lifetime locks, and full `Txn` for explicit transactions.

**Write lock info** tracks enough information for undo: the previous LSN and previous data of each modified record, enabling rollback on abort.

### Evictor (noxu-evictor)

The evictor keeps the in-memory cache within its memory budget. It uses a **dual-priority LRU** system:

- **Priority 1 (mixed)**: Contains both clean and dirty nodes under normal operation.
- **Priority 2 (dirty)**: Dirty nodes are moved here so they are evicted last, maximizing write absorption.

Eviction is triggered by:
- **Daemon threads**: Background evictor threads that run continuously.
- **Inline eviction**: Application threads that exceed the memory budget during operations.
- **Critical eviction**: Emergency eviction when memory usage approaches hard limits.

Applications can influence caching behavior per-operation through `CacheMode`: `Default`, `Unchanged`, `EvictLn`, `EvictBin`, `KeepHot`, `MakeEvictable`.

The `MemoryBudget` (in `noxu-dbi`) explicitly tracks memory consumption of every tree node, lock, and buffer. Noxu DB does not rely on the allocator for memory accounting.

### Cleaner (noxu-cleaner)

Over time, updates and deletions make log entries obsolete. The cleaner reclaims this space:

1. **Utilization tracking** (`UtilizationProfile`, `FileSummary`): Each log file has a summary of live vs. obsolete bytes. Summaries are maintained incrementally as operations occur.
2. **File selection** (`FileSelector`): Files below a utilization threshold are candidates for cleaning. The file with the lowest utilization is selected first.
3. **File processing** (`FileProcessor`): The cleaner reads a selected file, identifies live entries by checking the tree, and migrates live entries by logging them again (causing them to appear in a newer file).
4. **File deletion**: Once a file has been fully cleaned and a checkpoint has completed, the file can be safely deleted.

The cleaner runs as a background daemon and can also be invoked explicitly.

### Checkpointer (noxu-recovery)

The checkpointer bounds recovery time by periodically writing dirty tree nodes to the log and recording a checkpoint record. A checkpoint consists of:

1. **CheckpointStart**: Logged at the beginning, capturing the set of dirty nodes (the `DirtyINMap`).
2. **Dirty node flush**: All dirty INs and BINs are written to the log.
3. **CheckpointEnd**: Logged at the end, recording the root LSN, first active LSN, and the LSN of the CheckpointStart.

The interval between checkpoints is configurable. More frequent checkpoints mean faster recovery but higher I/O overhead.

### Recovery (noxu-recovery)

When an environment is opened, the `RecoveryManager` performs three-phase recovery:

**Phase 1 -- Find End of Log**: Scan backward from the end of the last log file to find the last valid entry. The `LastFileReader` validates checksums to determine the true end of the log, discarding any partially written entries.

**Phase 2 -- Build Tree**: Find the last `CheckpointEnd` and read its root LSN. Scan forward from the `CheckpointStart` LSN, reading IN and BIN log entries to reconstruct the in-memory B+tree. This re-populates the tree to the state it was in at checkpoint time.

**Phase 3 -- Replay and Undo LNs**: Scan forward from the first active LSN (recorded in the checkpoint). For committed transactions, redo their LN operations. For uncommitted transactions, undo their LN operations using the saved write lock info. This brings the database to a transaction-consistent state.

After recovery completes, the environment is ready for normal operation.

### Replication (noxu-rep)

Noxu DB supports master-replica high availability:

**Group topology**: A replication group consists of one master and zero or more replicas. The master accepts all writes. Replicas receive a stream of log entries from the master and apply them locally.

**VLSN (Virtual Log Sequence Number)**: Every replicated log entry is assigned a monotonically increasing VLSN. The `VlsnIndex` maps VLSNs to physical LSNs, enabling efficient positioning in the log stream. VLSNs are independent of physical log file layout, surviving log cleaning.

**Elections**: When the master is lost, replicas hold an election using majority voting. Each node proposes the candidate with the highest VLSN (most up-to-date). The winner becomes the new master. Elections use a proposer/acceptor protocol inspired by Paxos.

**Feeder/Replica streams**: The master runs a feeder thread per replica that reads log entries and sends them over the network. Each replica runs a replay thread that receives entries and applies them to its local log and tree.

**Consistency policies**: Applications can configure how up-to-date a replica must be before serving reads: `NoConsistencyRequiredPolicy`, `TimeConsistencyPolicy`, `CommitPointConsistencyPolicy`.

**Durability policies**: The master can wait for replica acknowledgments before considering a commit durable. Configurable via `ReplicaAckPolicy`: `NONE`, `SIMPLE_MAJORITY`, `ALL`.

**Master transfer**: Controlled, non-disruptive transfer of the master role to a designated replica.

**Network restore**: A replica that has fallen too far behind (its required log files have been cleaned) can perform a full restore from another node.

## Concurrency Model

Noxu DB uses a **latch-based** concurrency model, directly porting JE's approach:

- **ExclusiveLatch**: Wraps `parking_lot::Mutex`. Used for single-writer access to mutable structures. RAII-based -- the guard drops on scope exit.
- **SharedLatch**: Wraps `parking_lot::RwLock`. Used for reader-writer access to tree nodes and other shared structures.
- **Atomics**: `std::sync::atomic` types are used for volatile fields (counters, flags, sequence numbers) that JE marks as `volatile`.
- **No lock-free data structures**: Noxu DB uses traditional latch-based concurrency. The latching protocol is straightforward to reason about and extensively tested.

Tree traversal uses **latch coupling** (also called lock coupling or crabbing): acquire the child's latch, then release the parent's latch. This allows concurrent access to different parts of the tree without holding a global lock.

The `LogManager` serializes log writes through a write latch, but readers can access the log concurrently. Write buffering amortizes the cost of serialization.

## On-Disk Format

Noxu DB uses its own Rust-native format (`.ndb` files).

### Directory Layout

```
/path/to/environment/
    noxu.lck                  Environment lock file
    00000000.ndb              Log file 0
    00000001.ndb              Log file 1
    0000002a.ndb              Log file 42
    ...
```

### File Structure

Each `.ndb` file begins with a file header containing:
- Log version number
- File number (for consistency checking)

After the header, log entries are written sequentially. Each entry consists of a header (14 or 22 bytes) followed by the entry payload. Entries are not aligned to any boundary -- they are packed tightly.

### Entry Types

The log contains many entry types, organized into categories:
- **Tree nodes**: IN, BIN, LN, BIN-delta
- **Transaction records**: Commit, abort, prepare (for XA)
- **Administrative**: CheckpointStart, CheckpointEnd, FileHeader, trace
- **Database management**: Database name mapping (MapLN), database deletion
- **Replication**: VLSN-tagged entries, matchpoint

All multi-byte integers in the header are stored in **little-endian** byte order. Entry payloads may use either endianness depending on the entry type.

## Configuration

The `noxu-config` crate provides a typed configuration system with 400+ parameters. Parameters are organized by subsystem and validated at environment open time. Key parameters include:

| Parameter | Default | Description |
|-----------|---------|-------------|
| Cache size | 60% of max heap | Total in-memory cache |
| Lock timeout | 500ms | Per-lock acquisition timeout |
| Max log file size | 10 MB | Trigger file rotation |
| Checkpoint interval | 20s | Time between checkpoints |
| Cleaner min utilization | 50% | Below this, files are cleaned |
| Evictor thread count | 1 | Background evictor threads |

## External Dependencies

Noxu DB uses a small set of external crates. The core engine pulls in:

| Crate | Purpose |
|-------|---------|
| `parking_lot` | Fast mutex and rwlock implementations for latches |
| `thiserror` | Derive macro for error enums |
| `log` | Logging facade |
| `bytes` | Byte buffer utilities |
| `crc32fast` | CRC32 checksums for log entries |
| `byteorder` | Endian-aware integer I/O |
| `memmap2` | Memory-mapped file I/O |
| `fs2` | File locking |
| `serde` | Serialization framework (for bindings and persistence) |
| `hashbrown` | Hash maps and sets |
| `lock_api` | Trait-level lock primitives consumed by `noxu-sync` |
| `lru` | Bounded LRU cache used by `noxu-evictor` and `noxu-log` |
| `libc` | FFI for futex syscall in `noxu-sync` |

Replication (`noxu-rep`) and observability (`noxu-observe`) pull in
additional dependencies only when their cargo features are enabled:
`tokio`, `quinn`, `rustls` / `native-tls`, `tracing`, `tracing-opentelemetry`,
`metrics`, and `opentelemetry`.
