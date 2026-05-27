# Introduction

Noxu DB is an embedded, transactional key-value database written in Rust. It is
being idiomatic Rust with zero unsafe code in library logic.

## v1.5 capability matrix

This matrix is the canonical statement of what v1.5 actually delivers.
It was reconciled against the May 2026 API audits
(`docs/src/internal/api-audit-2026-05-rep.md`,
`docs/src/internal/je-port-audit-2026-05-overview.md`) and the
Sprint 1–3 restriction notes
(`docs/src/internal/sprint-1-followup-f12.md`,
`docs/src/internal/sprint-3-xa-restriction.md`,
`docs/src/internal/sprint-3-dpl-restriction.md`,
`docs/src/internal/sprint-3-collections-restriction.md`,
`docs/src/internal/sprint-3-decisions-enforced.md`).

| Feature | v1.5 | v1.6 (planned) | v2.0 (planned) |
|---|---|---|---|
| Single-process transactional KV          | ✅ | ✅ | ✅ |
| Sorted-duplicate values (primary DB)     | ✅ | ✅ | ✅ |
| One-to-one secondary indexes (manual maintenance) | ✅ | ✅ (auto via `associate()`) | ✅ |
| `Cursor::get` with `Get::SearchGte` / range scans | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::Search` / `SearchBoth` (validated on non-dup) | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::SearchLte` / `FirstDup` / `LastDup` | ❌ (`NoxuError::Unsupported`) | ⚠️ planned | ✅ |
| `DiskOrderedCursor` (high-throughput unordered scan; multi-DB) | ❌ | ✅ (Wave 2C-3) | ✅ |
| `Cursor::get` with `Get::NextDup` / `PrevDup` on non-dup DB | ❌ (`NotFound`) | ✅ | ✅ |
| Read-uncommitted isolation (env + per-txn config) | ✅ (honoured) | ✅ | ✅ |
| Auto-commit + explicit-txn co-existence | ✅ | ✅ | ✅ |
| Auto-commit through cursor with proper lock manager | ✅ (Sprint 1C / F12 verified) | ✅ | ✅ |
| Cursor on `Database` honours `Some(&txn)`         | ✅ (Sprint 1C) | ✅ | ✅ |
| Cursor on `SecondaryDatabase` honours `Some(&txn)` | ✅ (Sprint 1C) | ✅ | ✅ |
| `Database::count()` correct on sorted-dup         | ✅ | ✅ | ✅ |
| `Database::delete(key)` removes all dups          | ✅ | ✅ | ✅ |
| `Environment::close()` after `txn.commit()`       | ✅ (Sprint 1) | ✅ | ✅ |
| `EnvironmentConfig::durability` honoured          | ✅ (Sprint 1) | ✅ | ✅ |
| `TransactionConfig::read_uncommitted` honoured    | ✅ (Sprint 1) | ✅ | ✅ |
| In-process XA (`xa_prepare` / `xa_commit` same process) | ⚠️ in-process only | ⚠️ in-process only | ✅ |
| Crash-durable XA (`TxnPrepare` WAL + recovery)    | ❌ (`XaError::CrashDurabilityNotSupported` after restart) | ❌ | ✅ |
| Sorted-dup secondary indexes / `JoinCursor` over true dups | ❌ (`NoxuError::Unsupported` on collision) | ✅ | ✅ |
| Foreign-key constraints (Abort / Cascade / Nullify) | ❌ (rejected at `SecondaryDatabase::open` with `NoxuError::Unsupported`) | ✅ | ✅ |
| `associate()`-style automatic secondary maintenance | ❌ (manual `secondary.update_secondary` only) | ✅ | ✅ |
| Atomic primary + secondary writes under one txn (manual-update pattern) | ✅ (Sprint 4½ — thread same `txn` through `Database::put` and `SecondaryDatabase::update_secondary`) | ✅ | ✅ |
| Nested / child transactions (`begin_transaction(Some(parent), …)`) | ❌ (`NoxuError::Unsupported`) | ❌ | ❌ (`parent` parameter scheduled for removal) |
| `Stored*` collections under explicit txn          | ✅ (Wave 2B — every Stored* method takes `Option<&Transaction>`) | ✅ | ✅ |
| Typed `StoredMap<K, V>` / `StoredSet<K>` / `StoredList<V>` API | ✅ (Wave 2B — typed views parameterised by `EntryBinding`) | ✅ | ✅ |
| `StoredList::next_index` persistent across reopen | ✅ (via `StoredList::open`; `StoredList::new` resets) | ✅ | ✅ |
| `StoredList::remove` compacts the freed slot      | ✅ (Wave 2B — shift-down compaction; atomic under user txn) | ✅ | ✅ |
| `TransactionRunner` drives Stored* methods (deadlock retry + jittered backoff) | ✅ (Wave 2B) | ✅ | ✅ |
| `SerdeBinding` version-checking (2-byte magic + version header) | ✅ (breaking on-disk vs pre-Sprint-3 builds) | ✅ | ✅ |
| Schema evolution for `SerdeBinding` (read older struct shapes) | ❌ (header catches inter-format drift only) | ✅ | ✅ |
| DPL primary-index reads/writes participate in user txn (`PrimaryIndex::{put,get,delete,…}(txn, …)`) | ✅ (Sprint 3B — BREAKING source-level signature change) | ✅ | ✅ |
| DPL `#[derive(Entity)]` / `#[derive(PrimaryKey)]` / `#[derive(SecondaryKey)]` proc-macros | ❌ (manual `impl` only) | ✅ (Wave 2C-1) | ✅ |
| DPL schema evolution (Mutations wired into open path; Renamer / Deleter / Converter; per-record class-version envelope) | ✅ (Wave 2C-2 — BREAKING on-disk shape vs. pre-v1.6) | ✅ | ✅ |
| DPL secondary indexes are durable (survive restart) | ❌ (in-memory `BTreeMap` only) | ✅ | ✅ |
| DPL secondary updates atomic with user txn        | ❌ (`PersistError::SecondariesNotTransactional` warning) | ✅ | ✅ |
| Replication — single-process election test, 2-node sync, FPaxos shape | preview | refined | GA |
| `ReplicaAckPolicy` honoured on commit             | ❌ (config not plumbed; commits return after local fsync) | ⚠️ planned | ✅ |
| Election driver wired into `ReplicatedEnvironment` | ❌ (constructor sits in `Detached` until `become_master`) | ⚠️ | ✅ |
| Network restore via dispatcher (`ReplicatedEnvironment` bootstrap) | ❌ (broken framing; standalone path works) | ⚠️ | ✅ |
| Acceptor promise persistent across restart        | ❌ (Stateright spec doesn’t match impl) | ⚠️ | ✅ |
| `transfer_master` / `shutdown_group` operator APIs | ❌ (silently no-op) | ⚠️ | ✅ |

Legend: ✅ supported, ❌ not supported in that release,
⚠️ partial / preview — see the cited audit or sprint note for
the exact scope.

The replication rows reflect the May 2026 noxu-rep audit's
[GA-blocker list (10 items)](internal/api-audit-2026-05-rep.md)
and are unchanged by Sprints 1–3, which did not touch noxu-rep.
Replication is **preview / proof-of-concept** in v1.5 and is not
recommended for production.

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
- **Extended capabilities**: TTL record expiry, ByteComparator, ExtinctionFilter, GroupCommit,
  BackupManager, DataEraser, and more

## Reference Archives

Reference source archives used during development are kept read-only in the
development tree:

```text
_/je/       embedded database reference — Java, read-only
_/nosql/    extended fork with 10 additional capabilities — Java, read-only
```

## Documentation Map

| If you want to… | Go to… |
|---|---|
| Write your first Noxu program | [Getting Started](getting-started/README.md) |
| Understand transactions and isolation | [Transaction Processing](transactions/README.md) |
| Set up multi-node replication | [High Availability](replication/README.md) |
| Use the collections or DPL API | [Collections and Persistence](collections/README.md) |
| Tune performance or operate in production | [Operations Guide](operations/README.md) |
| Understand the internals | [Programmer's Reference](reference/README.md) |
| Contribute or port new Noxu features | [Contributing](contributing/README.md) |
| Take over maintenance of the project | [Maintainer's Guide](maintainer/README.md) |
