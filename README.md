# Noxu DB

[![crates.io](https://img.shields.io/crates/v/noxu-db.svg)](https://crates.io/crates/noxu-db)
[![docs.rs](https://docs.rs/noxu-db/badge.svg)](https://docs.rs/noxu-db)
[![license](https://img.shields.io/badge/license-Apache--2.0%2FMIT-blue.svg)](LICENSE)

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
    let txn = env.begin_transaction(None, None)?;
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
- **Replication & HA** -- Master-replica replication with automatic elections, VLSN-based log streaming, network restore, master transfer, and configurable consistency/durability policies.
- **Serialization Bindings** -- Tuple and entry bindings for structured data, including derive-macro entity persistence (Direct Persistence Layer).
- **Collection Views** -- Iterator-based collection abstractions over databases, with sorted map and sorted set semantics.
- **400+ Configuration Parameters** -- Fine-grained tuning of every subsystem through a validated, typed configuration framework.

## Workspace Structure

Noxu DB is organized as a Cargo workspace of 16 crates:

| Crate | Purpose |
|-------|---------|
| `noxu-util` | LSN, VLSN, packed integers, stats, daemon threads |
| `noxu-latch` | Exclusive and shared/exclusive latches (`parking_lot`) |
| `noxu-config` | 400+ typed configuration parameters with validation |
| `noxu-log` | Write-ahead log: file manager, log manager, entry I/O |
| `noxu-tree` | B+tree: IN, BIN, LN, key prefixing, splits |
| `noxu-txn` | Transactions, record-level locking, deadlock detection |
| `noxu-evictor` | LRU cache eviction with memory budget |
| `noxu-cleaner` | Log file garbage collection, utilization tracking |
| `noxu-recovery` | Checkpoint-based crash recovery |
| `noxu-dbi` | Internal implementations: EnvironmentImpl, DatabaseImpl, CursorImpl |
| `noxu-engine` | Engine orchestration, daemon lifecycle, environment open/close |
| `noxu-db` | Public API: Environment, Database, Cursor, Transaction |
| `noxu-bind` | Serialization bindings (tuple, entry, serial) |
| `noxu-collections` | Iterator-based collection views over databases |
| `noxu-persist` | Derive-macro entity persistence (DPL) |
| `noxu-rep` | Master-replica HA, elections, VLSN tracking |

## Building

```bash
cargo build          # Build all crates
cargo test           # Run all tests (2200+)
cargo test -p noxu-db    # Test a single crate
cargo clippy         # Lint
cargo fmt            # Format
```

Requires Rust 1.85+ (2024 edition).

## Design Principles

- **Correctness first.** Algorithms and invariants are implemented to match their specifications. Divergence from intended behaviour is a bug.
- **Idiomatic Rust.** RAII latches, `Result<T, NoxuError>` error handling, enums for closed hierarchies, traits for open extension points.
- **Minimal dependencies.** Core set: `parking_lot`, `thiserror`, `log`, `bytes`, `crc32fast`, `byteorder`, `memmap2`, `fs2`.
- **No unsafe.** Target zero `unsafe` in core crates. Exceptions only for memory-mapped I/O and off-heap cache.
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
