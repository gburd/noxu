# Architecture

Noxu DB is a Rust port of Noxu DB (Noxu DB), an embedded
transactional key-value database. This chapter documents the system
architecture, data flow, crate structure, and subsystem interactions.

The canonical prose version of this document is also maintained at
[`ARCHITECTURE.md`](https://github.com/gburd/lamdb/blob/main/ARCHITECTURE.md)
in the repository root.

## Heritage

Noxu DB is a mature, production-grade embedded database with a
well-tested architecture built around a write-ahead log, a B+tree, and
checkpoint-based recovery. Noxu DB preserves this architecture faithfully: the
same subsystem boundaries, the same algorithms, and the same naming conventions.
Noxu uses `parking_lot`
latches, and enums and traits for class hierarchies. The
invariants and control flow are the same.

## Data Flow

A write operation flows through the system as follows:

```text
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

A read follows the same path down to `noxu-dbi`, which searches the B+tree
(`noxu-tree`) and acquires read locks (`noxu-txn`). If the target node is not
in cache, the log is read (`noxu-log`) and the node is loaded into the tree.

## Crate Dependency Graph

The 19 crates form a layered dependency structure:

```text
Layer 0 (Foundation):
    noxu-util          Core types: LSN, VLSN, packed integers, stats
    noxu-sync          Internal sync primitives (raw locks, condvar, futex)
    noxu-latch         Latches wrapping parking_lot
    noxu-config        400+ configuration parameters

Layer 1 (Storage):
    noxu-log           Write-ahead log, file manager, buffers, readers

Layer 2 (Data Structures):
    noxu-tree          B+tree nodes (IN, BIN, LN), search, split

Layer 3 (Transactions):
    noxu-txn           Lock manager, deadlock detection, transaction lifecycle

Layer 4 (Internals):
    noxu-dbi           EnvironmentImpl, DatabaseImpl, CursorImpl, MemoryBudget

Layer 5 (Background Services):
    noxu-evictor       LRU/CLOCK/LIRS/ARC/CAR cache eviction, memory budget enforcement
    noxu-cleaner       Log garbage collection, utilization tracking
    noxu-recovery      Checkpointing, 3-phase crash recovery

Layer 6 (Orchestration):
    noxu-engine        Daemon lifecycle, environment open/close
    noxu-db            Public API

Layer 7 (Higher-Level APIs):
    noxu-bind          Serialization bindings (tuple, entry, serial)
    noxu-collections   Iterator-based collection views
    noxu-persist       Trait-based entity persistence (DPL)

Layer 7b (Distributed Transactions):
    noxu-xa            X/Open XA two-phase commit

Layer 8 (Replication):
    noxu-rep           Master-replica HA, elections, VLSN index

Cross-cutting:
    noxu-observe       Optional `tracing`/`metrics`/OpenTelemetry glue
```

## Subsystem Overview

| Subsystem | Crate | Purpose |
|---|---|---|
| Write-ahead log | `noxu-log` | Durability foundation; all mutations written here first |
| B+tree | `noxu-tree` | IN/BIN/LN nodes, key prefix, BIN-delta |
| Transaction manager | `noxu-txn` | Record-level locking, deadlock, locker hierarchy |
| Evictor | `noxu-evictor` | LRU cache management, memory budget |
| Cleaner | `noxu-cleaner` | Log GC, utilization tracking, file deletion |
| Checkpointer / Recovery | `noxu-recovery` | Checkpoint, 3-phase crash recovery |
| Replication | `noxu-rep` | FPaxos elections, feeder/replica streams, VLSN |

## External Dependencies

| Crate | Purpose |
|---|---|
| `parking_lot` | Fast mutex/rwlock for latches |
| `thiserror` | Derive macro for error enums |
| `log` | Logging facade |
| `bytes` | Byte buffer utilities |
| `crc32fast` | CRC32 checksums for log entries |
| `byteorder` | Endian-aware integer I/O |
| `memmap2` | Memory-mapped file I/O |
| `fs2` | File locking |
| `serde` | Serialization for bindings and persistence |
| `hashbrown` / `lock_api` / `lru` / `libc` | Core utility deps |
| `quinn` / `rustls` / `rcgen` | QUIC transport (optional, `quic` feature) |
| `native-tls` | OpenSSL/LibreSSL TLS (optional, `tls-native` feature) |
| `tracing` / `metrics` / `tracing-opentelemetry` / `opentelemetry` | Observability (optional, off by default) |
| `quoracle` | LP-optimal quorum systems (replication) |
