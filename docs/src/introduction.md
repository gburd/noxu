# Introduction

Noxu DB is an embedded, transactional key-value database written in Rust. It is
a faithful port of [Berkeley DB Java Edition (BDB JE) 7.5.11](https://docs.oracle.com/cd/E17277_02/html/index.html)
— preserving JE's algorithms, naming conventions, and documented behaviour while
being idiomatic Rust with zero unsafe code in library logic.

## Quick Start

Add Noxu DB to your project:

```toml
[dependencies]
noxu-db = "0.1"
```

Open an environment, write a record, and read it back:

```rust
use noxu_db::{Environment, EnvironmentConfig, DatabaseConfig, Transaction};
use std::path::Path;

fn main() -> noxu_db::Result<()> {
    // Open (or create) a database environment on disk.
    let env = Environment::open(
        Path::new("./mydb"),
        EnvironmentConfig::default(),
    )?;

    // Open a named database within the environment.
    let db = env.open_database(
        None,
        Some("my-store"),
        DatabaseConfig::default(),
    )?;

    // Write a record under a transaction.
    let txn = env.begin_transaction(None)?;
    db.put(&txn, b"hello", b"world")?;
    txn.commit()?;

    // Read it back with auto-commit (null transaction).
    let value = db.get(None, b"hello")?;
    assert_eq!(value.as_deref(), Some(b"world".as_ref()));

    drop(db);
    env.close()?;
    Ok(())
}
```

## What Noxu DB Provides

- **ACID transactions** with configurable isolation and durability
- **B-tree storage** with key prefix compression and BIN-delta incremental updates
- **Record-level locking** with deadlock detection (not MVCC)
- **Write-ahead logging** with group commit and fsync coalescing
- **Log cleaning** (garbage collection of obsolete log files)
- **Cache eviction** with LRU eviction and optional off-heap allocation
- **Crash recovery** via checkpoint-based 3-phase recovery
- **Replication / High Availability** via FPaxos leader election over TCP or QUIC
- **Collections API** (`StoredMap`, `StoredSet`, `StoredList`) and **DPL** entity persistence
- **NoSQL JE enhancements**: TTL, ByteComparator, ExtinctionFilter, GroupCommit, BackupManager, DataEraser, and more

## Heritage

Noxu DB derives from a 40-year lineage:

```
UC Berkeley INGRES (1970s)
  └─ Berkeley DB (Sleepycat Software, 1991)
       └─ Berkeley DB Java Edition (Sleepycat / Oracle, 2002)
            └─ Oracle NoSQL Database JE fork (Oracle, 2011)
                 └─ Noxu DB (Rust port, 2024–)
```

The reference Java source lives at `_/je/` (BDB JE 7.5.11) and
`_/nosql/kvmain/src/main/java/com/sleepycat/` (Oracle NoSQL fork) in the
development tree.

## Documentation Map

| If you want to… | Go to… |
|---|---|
| Write your first Noxu program | [Getting Started](getting-started/README.md) |
| Understand transactions and isolation | [Transaction Processing](transactions/README.md) |
| Set up multi-node replication | [High Availability](replication/README.md) |
| Use the collections or DPL API | [Collections and Persistence](collections/README.md) |
| Tune performance or operate in production | [Operations Guide](operations/README.md) |
| Understand the internals | [Programmer's Reference](reference/README.md) |
| Contribute or port new JE features | [Contributing](contributing/README.md) |
| Take over maintenance of the project | [Maintainer's Guide](maintainer/README.md) |
