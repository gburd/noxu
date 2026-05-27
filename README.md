# Noxu DB

[![crates.io](https://img.shields.io/crates/v/noxu-db.svg)](https://crates.io/crates/noxu-db)
[![docs.rs](https://docs.rs/noxu-db/badge.svg)](https://docs.rs/noxu-db)
[![license](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](#license)

An embedded transactional key-value database engine, written in Rust. Noxu DB provides ACID transactions, a log-structured B+tree, checkpoint-based crash recovery, and optional master-replica replication — all in a single library with no external database process required.

## Quick Start

```rust
use noxu_db::{Environment, EnvironmentConfig, DatabaseConfig, DatabaseEntry, OperationStatus};
use std::path::PathBuf;

fn main() -> noxu_db::Result<()> {
    // Open an environment
    let env_config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
        .allow_create(true)
        .transactional(true);
    let env = Environment::open(env_config)?;

    // Open a database
    let db_config = DatabaseConfig::new().allow_create(true);
    let db = env.open_database(None, "mydb", &db_config)?;

    // Insert a record
    let key = DatabaseEntry::from_bytes(b"hello");
    let value = DatabaseEntry::from_bytes(b"world");
    db.put(None, &key, &value)?;

    // Read it back
    let mut result = DatabaseEntry::new();
    let status = db.get(None, &key, &mut result, None)?;
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(result.data(), b"world");

    // Use transactions for ACID guarantees
    let txn = env.begin_transaction(None)?;
    db.put(Some(&txn), &DatabaseEntry::from_bytes(b"key2"), &DatabaseEntry::from_bytes(b"val2"))?;
    txn.commit()?;

    // Iterate with a cursor
    let mut cursor = db.open_cursor(None, None)?;
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    while cursor.get_next(&mut k, &mut v, None)? == OperationStatus::Success {
        println!("{:?} => {:?}", k.data(), v.data());
    }
    cursor.close()?;

    // Clean up
    db.close()?;
    env.close()?;
    Ok(())
}
```

## Features

- **ACID Transactions** -- Serializable transactions with record-level locking and deadlock detection. Supports configurable durability policies.
- **B-tree Storage** -- Classic B+tree with Internal Nodes (IN), Bottom Internal Nodes (BIN), and Leaf Nodes (LN). Key prefix encoding and BIN-deltas reduce memory and I/O overhead.
- **Write-Ahead Log** -- Append-only log with CRC32 checksums, configurable file sizes, and memory-mapped I/O. Log files use `.ndb` extension with hex naming (`00000000.ndb`).
- **Crash Recovery** -- Three-phase checkpoint-based recovery: find end of log, rebuild the tree, replay/undo operations. Bounded recovery time through periodic checkpointing.
- **Cache Eviction** -- LRU-based evictor with dual-priority queues and per-operation cache mode control (Default, KeepHot, EvictLn, EvictBin, MakeEvictable). Explicit memory budget tracking.
- **Log Cleaning** -- Background garbage collection of obsolete log entries with per-file utilization tracking and configurable thresholds.
- **Replication & HA** -- Master-replica replication with automatic elections, VLSN-based log streaming, network restore, master transfer, and configurable consistency/durability policies. **Security note**: as of v1.3.0 the replication wire protocol has no authentication; deploy only across a trusted network boundary. See [`docs/src/operations/known-limitations.md`](docs/src/operations/known-limitations.md) for the full list of replication-security limitations and the May-2026 security review.
- **Serialization Bindings** -- Tuple and entry bindings for structured data, plus a trait-based entity persistence layer (Direct Persistence Layer). Users implement `Entity` and an `EntitySerializer` for their types; entries are stored in typed `PrimaryIndex` and `SecondaryIndex` collections.
- **Collection Views** -- Iterator-based collection abstractions over databases, with sorted map and sorted set semantics.
- **400+ Configuration Parameters** -- Fine-grained tuning of every subsystem through a validated, typed configuration framework.

## Workspace Structure

Noxu DB is organized as a Cargo workspace of 19 crates:

| Crate | Purpose |
|-------|---------|
| `noxu-util` | LSN, VLSN, packed integers, stats, daemon threads |
| `noxu-sync` | Internal sync primitives (raw mutex/rwlock, condvar, futex) |
| `noxu-latch` | Exclusive and shared/exclusive latches (`parking_lot`) |
| `noxu-config` | 400+ typed configuration parameters with validation |
| `noxu-log` | Write-ahead log: file manager, log manager, entry I/O |
| `noxu-tree` | B+tree: IN, BIN, LN, key prefixing, splits |
| `noxu-txn` | Transactions, record-level locking, deadlock detection |
| `noxu-evictor` | LRU/CLOCK/LIRS/ARC/CAR cache eviction with memory budget |
| `noxu-cleaner` | Log file garbage collection, utilization tracking |
| `noxu-recovery` | Checkpoint-based crash recovery |
| `noxu-dbi` | Internal implementations: EnvironmentImpl, DatabaseImpl, CursorImpl |
| `noxu-engine` | Engine orchestration, daemon lifecycle, environment open/close |
| `noxu-db` | Public API: Environment, Database, Cursor, Transaction |
| `noxu-bind` | Serialization bindings (tuple, entry, serial) |
| `noxu-collections` | Iterator-based collection views over databases |
| `noxu-persist` | Trait-based entity persistence (DPL) |
| `noxu-xa` | XA distributed transactions (X/Open XA two-phase commit) |
| `noxu-rep` | Master-replica HA, elections, VLSN tracking |
| `noxu-observe` | Optional `tracing`/`metrics` observability glue |

## Building

```bash
# First-time setup: initialize the quoracle submodule (used by noxu-rep).
git submodule update --init --recursive

cargo build          # Build all crates
cargo test           # Run all tests
cargo test -p noxu-db    # Test a single crate
cargo clippy         # Lint
cargo fmt            # Format
```

Requires Rust 1.85+ (2024 edition); the workspace pins a specific stable
toolchain in `rust-toolchain.toml`.

## Design Principles

- **Correctness first.** Algorithms and invariants are implemented to match their specifications. Divergence from intended behaviour is a bug.
- **Idiomatic Rust.** RAII latches, `Result<T, NoxuError>` error handling, enums for closed hierarchies, traits for open extension points.
- **Minimal core dependencies.** The core engine pulls in only `parking_lot`,
  `thiserror`, `log`, `bytes`, `crc32fast`, `byteorder`, `memmap2`, `fs2`,
  plus `hashbrown`, `lock_api`, `lru`, `libc`, and `serde`. Replication
  (`noxu-rep`) and observability (`noxu-observe`) pull in additional
  dependencies (`tokio`, `quinn`, `rustls`/`native-tls`, `tracing`,
  `metrics`, `opentelemetry`) only when their features are enabled.
- **Limited unsafe.** Core data-path crates (`noxu-tree`, `noxu-txn`, `noxu-evictor`, `noxu-cleaner`, `noxu-recovery`, `noxu-dbi`, `noxu-engine`, `noxu-bind`, `noxu-collections`, `noxu-persist`, `noxu-config`, `noxu-util`) target zero `unsafe`. The exceptions are `noxu-sync` (FFI to libc futex / `parking_lot` raw locking), `noxu-log` (memory-mapped I/O), `noxu-rep` (network I/O glue and `parking_lot` raw locking), and a single-line `unsafe` block each in `noxu-latch`, `noxu-db`, and `noxu-xa` documented inline. `noxu-evictor::off_heap` is implemented entirely through safe `memmap2` and `lru` wrappers.
- **No async.** Core engine uses blocking I/O with explicit threading. Only replication networking may use async.
- **Own log format.** Noxu DB uses a Rust-native on-disk format — `.ndb` files — not compatible with any other database.

## Acknowledgements

Noxu DB's architecture draws on research and engineering work that spans several decades of embedded database design. The B+tree with write-ahead logging and checkpoint recovery follows the structure established in the embedded database literature. The log-structured approach to record management, BIN-delta write optimisation, and the memory-budget accounting model are derived from published techniques for transactional embedded stores.

The replication subsystem implements Flexible Paxos for leader election (Howard, Malkhi, and Spiegelman, 2016), the Phi Accrual Failure Detector (Hayashibara et al., 2004), and VLSN-based log streaming. The adaptive replacement cache policy (Megiddo and Modha, 2003) and its CART variant (Bansal and Modha, 2004) are available as optional eviction strategies. The Clock with Adaptive Replacement policy references work by Jiang and Zhang (2005).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
