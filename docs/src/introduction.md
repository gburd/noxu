# Introduction

Noxu DB is an embedded, transactional key-value database written in Rust.
The project's design goal is idiomatic Rust with zero `unsafe` in library
logic — only narrowly-scoped, documented `unsafe` for FFI to the OS, for
memory-mapped I/O, and for a handful of `parking_lot`/`Send` shims.

## Capability matrix (v1.5 → v2.2)

This matrix is the canonical statement of what each released line actually
delivers.  Columns are git tags (`v1.5.0`, `v1.6.0`, `v2.0.0`, `v2.2.1`),
not roadmap entries.

The matrix was reconciled against the released source plus the audit and
wave reports under [`internal/`](internal/) — in particular
[`api-audit-2026-05-rep.md`](internal/api-audit-2026-05-rep.md) (the 10
GA-blocker findings closed in Wave 4-A), the JE-port enumeration
([`je-port-audit-2026-05-overview.md`](internal/je-port-audit-2026-05-overview.md),
[`je-tck-port-2026-05-overview.md`](internal/je-tck-port-2026-05-overview.md)),
the Wave 2A–4-A series, the Wave 7–9 follow-ups
([`wave-9-a-rep-fixes.md`](internal/wave-9-a-rep-fixes.md),
[`wave-9-b-stateright-revalidation.md`](internal/wave-9-b-stateright-revalidation.md),
[`wave-9-c-je-tck-ports.md`](internal/wave-9-c-je-tck-ports.md)),
and the Sprint 1–3 restriction notes.

| Feature | v1.5 | v1.6 | v2.0 | v2.2 (current) |
|---|---|---|---|---|
| **Storage and transactions** | | | | |
| Single-process transactional KV | ✅ | ✅ | ✅ | ✅ |
| Sorted-duplicate values (primary DB) | ✅ | ✅ | ✅ | ✅ |
| Read-uncommitted / read-committed / repeatable-read / serializable isolation | ✅ | ✅ | ✅ | ✅ |
| `EnvironmentConfig::durability` honoured | ✅ (Sprint 1) | ✅ | ✅ | ✅ |
| `TransactionConfig::read_uncommitted` honoured | ✅ (Sprint 1) | ✅ | ✅ | ✅ |
| Auto-commit + explicit-txn co-existence | ✅ | ✅ | ✅ | ✅ |
| `Database::count()` correct on sorted-dup | ✅ | ✅ | ✅ | ✅ |
| `Database::delete(key)` removes all dups | ✅ | ✅ | ✅ | ✅ |
| `Environment::close()` after `txn.commit()` | ✅ (Sprint 1) | ✅ | ✅ | ✅ |
| Nested / child transactions | ❌ (`Unsupported`) | ❌ | ❌ (Wave 3-1: parent param removed — compile-time error) | ❌ |
| **Cursors** | | | | |
| `Cursor::get` with `Get::SearchGte` / range scans | ✅ | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::Search` / `SearchBoth` (validated on non-dup) | ✅ | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::NextDup` / `PrevDup` on dup DB | ✅ | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::NextDup` / `PrevDup` on non-dup DB returns `NotFound` (JE-equivalent) | ✅ (Sprint 1C / audit Finding 5) | ✅ | ✅ | ✅ |
| `Cursor::get` with `Get::SearchLte` / `FirstDup` / `LastDup` | ❌ (`Unsupported`) | ❌ | ❌ | ❌ (still `Unsupported` — tracked for a later sprint) |
| `DiskOrderedCursor` (high-throughput unordered scan; multi-DB) | ❌ | ✅ (Wave 2C-3) | ✅ | ✅ |
| Auto-commit through cursor with proper lock manager | ✅ (Sprint 1C / F12 verified) | ✅ | ✅ | ✅ |
| Cursor on `Database` honours `Some(&txn)` | ✅ (Sprint 1C) | ✅ | ✅ | ✅ |
| Cursor on `SecondaryDatabase` honours `Some(&txn)` | ✅ (Sprint 1C) | ✅ | ✅ | ✅ |
| **Secondary databases and foreign keys** | | | | |
| One-to-one secondary indexes (manual maintenance) | ✅ | ✅ | ✅ | ✅ |
| Sorted-dup secondary indexes / `JoinCursor` over true dups | ❌ (`Unsupported` on collision) | ✅ (Wave 2A: sorted-dup inner DB + `SecondaryCursor::get_next_dup_full`) | ✅ | ✅ |
| `associate()`-style automatic secondary maintenance | ❌ (manual `secondary.update_secondary` only) | ✅ (Wave 2A: `Database::put`/`delete` fans out under caller's txn) | ✅ | ✅ |
| Foreign-key constraints (`Abort` / `Cascade` / `Nullify`, single + multi-key) | ❌ (rejected at `SecondaryDatabase::open`) | ✅ (Wave 2A: end-to-end with cycle detection) | ✅ | ✅ |
| Atomic primary + secondary writes under one txn | ✅ (Sprint 4½ — manual path) | ✅ (Wave 2A: applies to auto-maintenance path too) | ✅ | ✅ |
| **Distributed transactions (XA)** | | | | |
| In-process XA (`xa_prepare` / `xa_commit` same process) | ⚠️ in-process only | ✅ | ✅ | ✅ |
| Crash-durable XA (`TxnPrepare` WAL + recovery) | ❌ (`XaError::CrashDurabilityNotSupported` after restart) | ✅ (Wave 3-2) | ✅ | ✅ |
| **Collections (`StoredMap` / `StoredSet` / `StoredList`)** | | | | |
| `Stored*` collections under explicit txn (`Option<&Transaction>` on every method) | ✅ (Wave 2B) | ✅ | ✅ | ✅ |
| Typed `StoredMap<K, V>` / `StoredSet<K>` / `StoredList<V>` parameterised by `EntryBinding` | ✅ (Wave 2B) | ✅ | ✅ | ✅ |
| `StoredList::next_index` persistent across reopen (via `StoredList::open`) | ✅ (Wave 2B) | ✅ | ✅ | ✅ |
| `StoredList::remove` compacts the freed slot atomically | ✅ (Wave 2B — shift-down compaction) | ✅ | ✅ | ✅ |
| `TransactionRunner` deadlock retry + jittered backoff | ✅ (Wave 2B) | ✅ | ✅ | ✅ |
| **Serialization / DPL** | | | | |
| `SerdeBinding` 2-byte magic + version header | ✅ (BREAKING vs pre-Sprint-3 builds) | ✅ | ✅ | ✅ |
| Schema evolution for `SerdeBinding` (read older struct shapes) | ❌ (header catches inter-format drift only) | ✅ (Wave 2C-2) | ✅ | ✅ |
| DPL primary-index reads/writes participate in user txn | ✅ (Sprint 3B — BREAKING signature change) | ✅ | ✅ | ✅ |
| DPL `#[derive(Entity)]` / `#[derive(PrimaryKey)]` / `#[derive(SecondaryKey)]` proc-macros (`noxu-persist-derive`) | ❌ (manual `impl` only) | ✅ (Wave 2C-1) | ✅ | ✅ |
| DPL schema evolution (`Mutations` wired into open path; `Renamer` / `Deleter` / `Converter`; per-record class-version envelope) | ❌ | ✅ (Wave 2C-2 — BREAKING on-disk shape vs. pre-v1.6) | ✅ | ✅ |
| DPL secondary indexes durable (survive restart) | ❌ (in-memory `BTreeMap` only) | ✅ (Wave 2C-2) | ✅ | ✅ |
| DPL secondary updates atomic with user txn | ❌ (`PersistError::SecondariesNotTransactional` warning) | ✅ (Wave 2C-2) | ✅ | ✅ |
| Read-only reopen of an existing entity store (`allow_create=false`) | ❌ | ❌ | ❌ | ✅ (Wave 7 polish, v2.0.1-equivalent) |
| **Replication / HA** | | | | |
| Single-process election test, 2-node sync, FPaxos shape | preview | refined | GA (Wave 3-3 / 4-A — all 10 audit blockers closed) | GA |
| `ReplicaAckPolicy` honoured on commit | ❌ (config not plumbed; commits return after local fsync) | ❌ | ✅ (Wave 3-3, F1) | ✅ |
| Election driver wired into `ReplicatedEnvironment` | ❌ (sat in `Detached` until `become_master`) | ❌ | ✅ (Wave 3-3, F6) | ✅ |
| Dispatcher service-name length bound (DoS hardening) | ❌ (4-byte unbounded length prefix) | ❌ | ✅ (Wave 3-3, F3) | ✅ |
| `apply_entry` peer-scanner bounded under sustained load | ❌ (unbounded growth) | ❌ | ✅ (Wave 3-3, F10) | ✅ |
| Arbiters cannot win Paxos elections | ❌ (could be elected master, wedging the cluster) | ❌ | ✅ (Wave 3-3, F22) | ✅ |
| Network restore via dispatcher (`ReplicatedEnvironment` bootstrap) | ❌ (broken framing) | ❌ | ✅ (Wave 4-A, F2/F4) | ✅ |
| Acceptor promise persistent across restart | ❌ | ❌ | ✅ (Wave 4-A, F5/F31) | ✅ |
| `transfer_master` / `shutdown_group` operator APIs | ❌ (silently no-op) | ❌ | ✅ (Wave 4-A, F7/F8) | ✅ |
| Master spawns Feeder per known replica on `become_master` | ❌ (no feeders dispatched) | ❌ | ✅ (Wave 4-A, F9) | ✅ |
| VLSN index persistent across restart (no forced full restore) | ❌ (in-memory only) | ❌ | ✅ (Wave 4-A, F11) | ✅ |
| `become_master` rejects non-`Electable` node types | ❌ (silently transitioned `Secondary` → `Master`) | ❌ | ❌ (regression surfaced by Wave 8 RepTestBase) | ✅ (Wave 9-A) |
| Replica I/O thread auto-bootstraps on `NeedsRestore` | ❌ (manual `bootstrap_via_dispatcher` required) | ❌ | ❌ | ✅ (Wave 9-A) |
| Stateright executable specs match implementation (persistent acceptor, persistent VLSN, F9 feeder spawn, F2/F4 dispatcher restore) | n/a | n/a | ⚠️ deferred at v2.0 | ✅ (Wave 9-B re-validation; all 5 updated specs pass) |
| **Test coverage** | | | | |
| Workspace test gate (`cargo test --workspace`) | ~3,800 passed | 5,384 passed | 5,540 passed | 5,625 passed |
| JE TCK ported tests (`PORTED-EQUIVALENT`) | n/a | partial | 205 | 243 (Wave 9-C) |
| JE TCK enumeration tracked in TSV under `internal/` | n/a | partial | ✅ | ✅ |

Legend: ✅ supported, ❌ not supported in that release, ⚠️ partial /
preview — see the cited audit, sprint, or wave note for the exact scope.

The replication rows reflect the May-2026 `noxu-rep` audit's
[GA-blocker list (10 items)](internal/api-audit-2026-05-rep.md).
Waves 3-3 and 4-A close all ten blockers; v2.0 is the first release
where the replication subsystem honours its documented contract
end-to-end.  Wave 9-A closes the two regressions surfaced after v2.0
(Wave 8 RepTestBase + Wave 4-A follow-up), and Wave 9-B re-validates
the Stateright executable specifications against the post-Wave-4-A
production code.  See
[Wave 4-A report](internal/wave-4-a-rep-ga-finish.md),
[Wave 9-A report](internal/wave-9-a-rep-fixes.md), and
[Wave 9-B report](internal/wave-9-b-stateright-revalidation.md) for
per-finding resolution notes.

## Quick Start

Until the crate is published to crates.io, depend on Noxu DB via the
Codeberg git URL:

```toml
[dependencies]
noxu-db = { git = "https://codeberg.org/gregburd/noxu.git", tag = "v2.2.1" }
```

Open an environment, write a record, and read it back:

```rust
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use std::path::PathBuf;

fn main() -> noxu_db::Result<()> {
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
and the [Getting Started guide](getting-started/README.md).

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
| Write your first Noxu program | [Getting Started](getting-started/README.md) |
| Understand transactions and isolation | [Transaction Processing](transactions/README.md) |
| Set up multi-node replication | [High Availability](replication/README.md) |
| Use the collections or DPL API | [Collections and Persistence](collections/README.md) |
| Tune performance or operate in production | [Operations Guide](operations/README.md) |
| Understand the internals | [Programmer's Reference](reference/README.md) |
| Contribute or port new Noxu features | [Contributing](contributing/README.md) |
| Take over maintenance of the project | [Maintainer's Guide](maintainer/README.md) |
