# Introduction

Noxu DB is an embedded, transactional key-value database written in Rust.
The project's design goal is idiomatic Rust with zero `unsafe` in library
logic — only narrowly-scoped, documented `unsafe` for FFI to the OS, for
memory-mapped I/O, and for a handful of `parking_lot`/`Send` shims.

## Capability matrix (v1.5 → v2.2)

This matrix states what each released line delivers.  Columns are
git tags (`v1.5.0`, `v1.6.0`, `v2.0.0`, `v2.2.1`).

| Feature | v1.5 | v1.6 | v2.0 | v2.2 (current) |
|---|---|---|---|---|
| **Storage and transactions** | | | | |
| Single-process transactional KV | ✅ | ✅ | ✅ | ✅ |
| Sorted-duplicate values (primary DB) | ✅ | ✅ | ✅ | ✅ |
| Read-uncommitted / read-committed / repeatable-read / serializable isolation | ✅ | ✅ | ✅ | ✅ |
| `EnvironmentConfig::durability` honoured | ✅ | ✅ | ✅ | ✅ |
| `TransactionConfig::read_uncommitted` honoured | ✅ | ✅ | ✅ | ✅ |
| Auto-commit + explicit-txn co-existence | ✅ | ✅ | ✅ | ✅ |
| `Database::count()` correct on sorted-dup | ✅ | ✅ | ✅ | ✅ |
| `Database::delete(key)` removes all dups | ✅ | ✅ | ✅ | ✅ |
| `Environment::close()` after `txn.commit()` | ✅ | ✅ | ✅ | ✅ |
| Nested / child transactions | ❌ (`Unsupported`) | ❌ | ❌ (parent param removed — compile-time error) | ❌ |
| **Cursors** | | | | |
| `Cursor::get` with `Get::SearchGte` / range scans | ✅ | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::Search` / `SearchBoth` (validated on non-dup) | ✅ | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::NextDup` / `PrevDup` on dup DB | ✅ | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::NextDup` / `PrevDup` on non-dup DB returns `NotFound` | ✅ | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::SearchLte` / `FirstDup` / `LastDup` | ❌ (`Unsupported`) | ❌ | ❌ | ❌ (`Unsupported`) |
| `DiskOrderedCursor` (high-throughput unordered scan; multi-DB) | ❌ | ✅ (v1.6) | ✅ | ✅ |
| Auto-commit through cursor with proper lock manager | ✅ | ✅ | ✅ | ✅ |
| Cursor on `Database` honours `Some(&txn)` | ✅ | ✅ | ✅ | ✅ |
| Cursor on `SecondaryDatabase` honours `Some(&txn)` | ✅ | ✅ | ✅ | ✅ |
| **Secondary databases and foreign keys** | | | | |
| One-to-one secondary indexes (manual maintenance) | ✅ | ✅ | ✅ | ✅ |
| Sorted-dup secondary indexes / `JoinCursor` over true dups | ❌ (`Unsupported` on collision) | ✅ (v1.6) | ✅ | ✅ |
| `associate()`-style automatic secondary maintenance | ❌ (manual `secondary.update_secondary` only) | ✅ (v1.6) | ✅ | ✅ |
| Foreign-key constraints (`Abort` / `Cascade` / `Nullify`, single + multi-key) | ❌ (rejected at `SecondaryDatabase::open`) | ✅ (v1.6) | ✅ | ✅ |
| Atomic primary + secondary writes under one txn | ✅ (manual path) | ✅ (v1.6) | ✅ | ✅ |
| **Distributed transactions (XA)** | | | | |
| In-process XA (`xa_prepare` / `xa_commit` same process) | ⚠️ in-process only | ✅ | ✅ | ✅ |
| Crash-durable XA (`TxnPrepare` WAL + recovery) | ❌ (`XaError::CrashDurabilityNotSupported` after restart) | ✅ (v1.6) | ✅ | ✅ |
| **Collections (`StoredMap` / `StoredSet` / `StoredList`)** | | | | |
| `Stored*` collections under explicit txn (`Option<&Transaction>` on every method) | ✅ | ✅ | ✅ | ✅ |
| Typed `StoredMap<K, V>` / `StoredSet<K>` / `StoredList<V>` parameterised by `EntryBinding` | ✅ | ✅ | ✅ | ✅ |
| `StoredList::next_index` persistent across reopen (via `StoredList::open`) | ✅ | ✅ | ✅ | ✅ |
| `StoredList::remove` compacts the freed slot atomically | ✅ | ✅ | ✅ | ✅ |
| `TransactionRunner` deadlock retry + jittered backoff | ✅ | ✅ | ✅ | ✅ |
| **Serialization / DPL** | | | | |
| `SerdeBinding` 2-byte magic + version header | ✅ (BREAKING vs pre-v1.5 builds) | ✅ | ✅ | ✅ |
| Schema evolution for `SerdeBinding` (read older struct shapes) | ❌ (header catches inter-format drift only) | ✅ (v1.6) | ✅ | ✅ |
| DPL primary-index reads/writes participate in user txn | ✅ (BREAKING signature change) | ✅ | ✅ | ✅ |
| DPL `#[derive(Entity)]` / `#[derive(PrimaryKey)]` / `#[derive(SecondaryKey)]` proc-macros (`noxu-persist-derive`) | ❌ (manual `impl` only) | ✅ (v1.6) | ✅ | ✅ |
| DPL schema evolution (`Mutations` wired into open path; `Renamer` / `Deleter` / `Converter`; per-record class-version envelope) | ❌ | ✅ (v1.6 — BREAKING on-disk shape vs. pre-v1.6) | ✅ | ✅ |
| DPL secondary indexes durable (survive restart) | ❌ (in-memory `BTreeMap` only) | ✅ (v1.6) | ✅ | ✅ |
| DPL secondary updates atomic with user txn | ❌ (`PersistError::SecondariesNotTransactional` warning) | ✅ (v1.6) | ✅ | ✅ |
| Read-only reopen of an existing entity store (`allow_create=false`) | ❌ | ❌ | ❌ | ✅ |
| **Replication / HA** | | | | |
| Single-process election test, 2-node sync, FPaxos shape | preview | refined | GA | GA |
| `ReplicaAckPolicy` honoured on commit | ❌ (config not plumbed; commits return after local fsync) | ❌ | ✅ | ✅ |
| Election driver wired into `ReplicatedEnvironment` | ❌ (sat in `Detached` until `become_master`) | ❌ | ✅ | ✅ |
| Dispatcher service-name length bound (DoS hardening) | ❌ (4-byte unbounded length prefix) | ❌ | ✅ | ✅ |
| `apply_entry` peer-scanner bounded under sustained load | ❌ (unbounded growth) | ❌ | ✅ | ✅ |
| Arbiters cannot win Paxos elections | ❌ (could be elected master, wedging the cluster) | ❌ | ✅ | ✅ |
| Network restore via dispatcher (`ReplicatedEnvironment` bootstrap) | ❌ (broken framing) | ❌ | ✅ | ✅ |
| Acceptor promise persistent across restart | ❌ | ❌ | ✅ | ✅ |
| `transfer_master` / `shutdown_group` operator APIs | ❌ (silently no-op) | ❌ | ✅ | ✅ |
| Master spawns Feeder per known replica on `become_master` | ❌ (no feeders dispatched) | ❌ | ✅ | ✅ |
| VLSN index persistent across restart (no forced full restore) | ❌ (in-memory only) | ❌ | ✅ | ✅ |
| `become_master` rejects non-`Electable` node types | ❌ (silently transitioned `Secondary` → `Master`) | ❌ | ❌ | ✅ |
| Replica I/O thread auto-bootstraps on `NeedsRestore` | ❌ (manual `bootstrap_via_dispatcher` required) | ❌ | ❌ | ✅ |
| Stateright executable specs match implementation | n/a | n/a | ⚠️ deferred at v2.0 | ✅ (all 5 updated specs pass) |
| In-memory transport for production use (`InMemoryTransport`, `RepTransportKind::InMemory`) | ❌ | ❌ | ⚠️ cfg(test) / `test-harness` only | ⚠️ promoted to first-class in v2.4 |
| **Test coverage** | | | | |
| Workspace test gate (`cargo test --workspace`) | ~3,800 passed | 5,384 passed | 5,540 passed | 5,625 passed |
| JE TCK ported tests (`PORTED-EQUIVALENT`) | n/a | partial | 205 | 243 |
| JE TCK enumeration tracked in TSV under `internal/` | n/a | partial | ✅ | ✅ |

Legend: ✅ supported, ❌ not supported in that release, ⚠️ partial /
preview — see the release notes for the exact scope.

The replication subsystem reached GA in v2.0 with all ten pre-v2.0 blockers
closed.  Two regressions identified post-v2.0 were fixed in v2.2.  The
Stageright executable specifications were re-validated against the v2.0+
code as part of that work.  See
[`docs/src/internal/api-audit-2026-05-rep.md`](internal/api-audit-2026-05-rep.md)
for the per-finding notes.

## Quick Start

Add `noxu` to your `Cargo.toml`:

```toml
[dependencies]
noxu = "3"
```

Or depend on the git source directly:

```toml
[dependencies]
noxu = { git = "https://codeberg.org/gregburd/noxu.git", tag = "v3.0.2" }
```

Open an environment, write a record, and read it back:

```rust
use noxu::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use std::path::PathBuf;

fn main() -> noxu::Result<()> {
    // Open (or create) a transactional environment on disk.
    let env_config = EnvironmentConfig::new(PathBuf::from("./mydb"))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config)?;

    // Open a named database within the environment.
    let db_config = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true);
    let db = env.open_database(None, "my-store", &db_config)?;

    // Write a record under an explicit transaction.
    let txn = env.begin_transaction(None)?;
    db.put(
        Some(&txn),
        &DatabaseEntry::from_bytes(b"hello"),
        &DatabaseEntry::from_bytes(b"world"),
    )?;
    txn.commit()?;

    // Read it back with auto-commit.
    let mut value = DatabaseEntry::new();
    let status = db.get(
        None,
        &DatabaseEntry::from_bytes(b"hello"),
        &mut value,
        None,
    )?;
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(value.data(), b"world");

    db.close()?;
    env.close()?;
    Ok(())
}
```

For a complete worked example (vendors + items, secondary indexes, joins),
see [`examples/getting_started.rs`](https://codeberg.org/gregburd/noxu/src/branch/main/examples/getting_started.rs)
and the [Getting Started guide](getting-started/index.html).

## What Noxu DB Provides

- **ACID transactions** with configurable isolation (`Serializable`,
  `RepeatableRead`, `ReadCommitted`, `ReadUncommitted`) and durability
  (`SyncWriteNoSync` / `WriteNoSync` / `NoSync`).
- **B+tree storage** with key-prefix compression and BIN-delta incremental
  updates; sorted duplicates on primary databases.
- **Record-level locking** with deadlock detection (lock-based, not MVCC).
- **Write-ahead logging** in a Rust-native `.ndb` format with CRC32, group
  commit, and fsync coalescing.
- **Log cleaning** (background GC of obsolete log files).
- **Cache eviction** with LRU/CLOCK/LIRS/ARC/CAR strategies and optional
  off-heap allocation.
- **Crash recovery** via three-phase checkpoint-based recovery.
- **Replication / High Availability** via Flexible Paxos leader election
  over TCP or QUIC, with Phi Accrual Failure Detection and VLSN-based log
  streaming.
- **Collections API** (`StoredMap` / `StoredSet` / `StoredList`) and **DPL**
  entity persistence with `#[derive(Entity)]` and full schema evolution.
- **XA distributed transactions** (X/Open XA two-phase commit), crash-durable
  across restart.
- **Extended capabilities**: TTL record expiry, `ByteComparator`,
  `ExtinctionFilter`, group commit, `BackupManager`, `DataEraser`, and
  more.

## Reference Archives

Reference source archives used during development are kept read-only in the
development tree (gitignored — not part of the published repository):

```text
_/je/       embedded database reference — Java, read-only
_/nosql/    extended fork with 10 additional capabilities — Java, read-only
```

Contributors who do not have these archives can still build, test, and run
Noxu DB; references to them in `AGENTS.md` and
[Porting Guidelines](contributing/porting-guidelines.md) are guidance for
porting work, not a build prerequisite.

## Documentation Map

| If you want to… | Go to… |
|---|---|
| Write your first Noxu program | [Getting Started](getting-started/index.html) |
| Understand transactions and isolation | [Transaction Processing](transactions/index.html) |
| Set up multi-node replication | [High Availability](replication/index.html) |
| Use the collections or DPL API | [Collections and Persistence](collections/index.html) |
| Tune performance or operate in production | [Operations Guide](operations/index.html) |
| Understand the internals | [Programmer's Reference](reference/index.html) |
| Contribute or port new Noxu features | [Contributing](contributing/index.html) |
| Take over maintenance of the project | [Maintainer's Guide](maintainer/index.html) |
