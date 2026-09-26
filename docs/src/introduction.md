# Introduction

Noxu DB is an embedded, transactional key-value database written in Rust.
The project's design goal is idiomatic Rust with zero `unsafe` in library
logic — only narrowly-scoped, documented `unsafe` for FFI to the OS (the
Linux futex, socket options), for memory-mapped I/O, and for a handful of
`Send`/lifetime shims.

## Capability matrix

> **Source of truth.** This summary reflects the current release (v7.10.1).
> The authoritative, continuously-maintained statement of what is
> implemented, partially implemented, or deliberately bounded lives in
> [Known Limitations](operations/known-limitations.md) — consult it for the
> exact status, workarounds, and residual risk of any feature below.
> (A per-version matrix was maintained through the pre-v3.0 remediation
> phase; it was retired in favour of this summary plus the tracked
> limitations list, which are kept current with the code.)

**Storage and transactions**

- Single-process transactional key-value storage with ACID commit.
- Sorted-duplicate values on primary databases.
- Four isolation levels: read-uncommitted, read-committed, repeatable-read
  (default), and serializable (next-key range locking for phantom
  prevention).
- Configurable durability (`SyncWriteNoSync` / `WriteNoSync` / `NoSync`),
  group commit, and fsync coalescing (fail-stop on WAL sync error).
- Auto-commit and explicit-transaction paths coexist; `Database::count()`
  and `delete(key)` are correct on sorted-dup databases.
- Per-record TTL / expiration (hour/day granularity), reclaimed by the
  cleaner and honoured across recovery.

**Cursors and secondary indexes**

- Range and duplicate navigation (`Get::SearchGte`, `NextDup`/`PrevDup`,
  etc.); `DiskOrderedCursor` for high-throughput unordered multi-database
  scans.
- `associate()`-style automatic secondary maintenance, sorted-duplicate
  secondaries, `JoinCursor`, and foreign-key constraints
  (`Abort` / `Cascade` / `Nullify`).

**Higher-level APIs**

- Collections: typed `StoredMap<K, V>`, `StoredSet<K>`, `StoredList<V>` with
  `TransactionRunner` deadlock retry.
- Direct Persistence Layer with `#[derive(Entity)]` / `#[derive(PrimaryKey)]`
  / `#[derive(SecondaryKey)]`, durable transactional secondary indexes, and
  schema evolution (`Renamer` / `Deleter` / `Converter`).
- Serialization bindings (tuple, entry, serde) with version-checking magic
  headers.

**Distribution and durability**

- XA distributed transactions (two-phase commit), crash-durable across
  restart via a `TxnPrepare` WAL record.
- Master-replica replication / HA: Flexible Paxos leader election, Phi
  Accrual failure detection, VLSN log streaming, network restore, master
  transfer, dynamic membership, and configurable `ReplicaAckPolicy` /
  consistency policies. Transport over TCP, TLS, or QUIC.
- Hot backup (`Environment::start_backup`) that pins the log-file set against
  the cleaner while the caller copies it.

**Known bounds** (see [Known Limitations](operations/known-limitations.md)
for the full, current list):

- Replication defaults to mutually-authenticated mTLS and **refuses to start
  on an unauthenticated transport** unless the operator opts out
  (`RepConfig::insecure_no_auth(true)`); per-message election authentication
  is not implemented, so a compromised allowlisted peer is trusted.
- Some replication client connectors (restore / feeder / admin) still use
  plain TCP even under a TLS deployment.
- Sustained `COMMIT_SYNC` throughput trails BDB-JE at low writer counts
  (Noxu trades peak throughput for flatter tail latency); it closes to within
  ~10% at relaxed durability or high writer concurrency.
- Nested / child transactions are not supported.

## Quick Start

Add `noxu` to your `Cargo.toml`:

```toml
[dependencies]
noxu = "7"
```

Or depend on the git source directly:

```toml
[dependencies]
noxu = { git = "https://codeberg.org/gregburd/noxu.git", tag = "v7.10.1" }
```

Open an environment, write a record, and read it back:

```rust
use noxu::{DatabaseConfig, Environment, EnvironmentConfig};
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

    // Write a record under an explicit transaction (`put_in` names the txn).
    let txn = env.begin_transaction(None)?;
    db.put_in(&txn, b"hello", b"world")?;
    txn.commit()?;

    // Read it back with auto-commit. Reads return `Result<Option<Bytes>>`.
    if let Some(value) = db.get(b"hello")? {
        assert_eq!(value.as_ref(), b"world");
    }

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
  `ExtinctionFilter`, group commit, hot backup (`Environment::start_backup`),
  `DataEraser`, and more.

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
