# Noxu DB

[![license](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](#license)
[![rust](https://img.shields.io/badge/rust-stable%201.95+-orange.svg)](rust-toolchain.toml)
[![docs](https://img.shields.io/badge/docs-codeberg.page-blue.svg)](https://codeberg.page/gregburd/noxu/)
[![crates.io](https://img.shields.io/crates/v/noxu.svg)](https://crates.io/crates/noxu)
[![docs.rs](https://docs.rs/noxu/badge.svg)](https://docs.rs/noxu)

Noxu DB is an embedded transactional key-value database engine, written in
Rust.  It provides ACID transactions, a log-structured B+tree, checkpoint-based
crash recovery, master-replica replication with automatic leader elections,
and an entity-persistence layer — all in a single library with no external
database process required.

**Current version**: 3.0.2.  See [CHANGELOG.md](CHANGELOG.md) for the full
release history.

## Quick Start

Add `noxu` to your `Cargo.toml`:

```toml
[dependencies]
noxu = "3"  # or pin to a specific version, e.g. "3.0.2"
```

Alternatively, depend on the git source directly (useful before a crates.io
release, or to track unreleased commits):

```toml
[dependencies]
noxu = { git = "https://codeberg.org/gregburd/noxu.git", tag = "v3.0.2" }
```

The engine is composed of `noxu-*` component crates published as internal
dependencies; applications should depend on `noxu`, not on a component crate
directly.

Open an environment, write a record, and read it back:

```rust
use noxu::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use std::path::PathBuf;

fn main() -> noxu::Result<()> {
    // Open (or create) a transactional environment on disk.
    let env_config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config)?;

    // Open a named database within the environment.
    let db_config = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true);
    let db = env.open_database(None, "mydb", &db_config)?;

    // Auto-commit put.
    let key = DatabaseEntry::from_bytes(b"hello");
    let value = DatabaseEntry::from_bytes(b"world");
    db.put(None, &key, &value)?;

    // Auto-commit get.
    let mut result = DatabaseEntry::new();
    let status = db.get(None, &key, &mut result)?;
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(result.data(), b"world");

    // Explicit transaction.
    let txn = env.begin_transaction(None)?;
    db.put(
        Some(&txn),
        &DatabaseEntry::from_bytes(b"key2"),
        &DatabaseEntry::from_bytes(b"val2"),
    )?;
    txn.commit()?;

    // Cursor scan.
    let mut cursor = db.open_cursor(None, None)?;
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    while cursor.get(&mut k, &mut v, Get::Next, None)? == OperationStatus::Success {
        println!("{:?} => {:?}", k.data(), v.data());
    }
    cursor.close()?;

    db.close()?;
    env.close()?;
    Ok(())
}
```

For a fuller worked example (vendors + items, secondary indexes, joins),
see [`examples/getting_started.rs`](examples/getting_started.rs) and the
[Getting Started guide](https://codeberg.page/gregburd/noxu/getting-started/).

## Features

### Storage and transactions

- **ACID transactions** with record-level locking, deadlock detection, and
  configurable durability (`SyncPolicy::SyncWriteNoSync` /
  `WriteNoSync` / `NoSync`) and isolation (`Serializable`,
  `RepeatableRead`, `ReadCommitted`, `ReadUncommitted`).
- **B+tree storage** with key prefix encoding and BIN-deltas for incremental
  updates.  Sorted-duplicate values supported on primary databases.
- **Write-ahead log** in a Rust-native `.ndb` format with CRC32 checksums
  (15+ GiB/s on CLMUL hardware), configurable file sizes, group commit, and
  fsync coalescing.
- **Crash recovery** via three-phase checkpoint-based recovery; bounded by
  the configured checkpoint interval.
- **Cache eviction** with LRU/CLOCK/LIRS/ARC/CAR strategies, dual-priority
  queues, per-operation cache modes, and optional off-heap allocation.
- **Log cleaning** — background garbage collection of obsolete log entries
  with per-file utilization tracking.

### Higher-level APIs

- **Cursors**, including `DiskOrderedCursor` for high-throughput unordered
  scans across one or more databases.
- **Secondary indexes** with `associate()`-style auto-maintenance, sorted
  duplicates, and foreign-key constraints (`Abort` / `Cascade` / `Nullify`).
- **Collections**: typed `StoredMap<K, V>`, `StoredSet<K>`, `StoredList<V>`
  views with `TransactionRunner`-driven deadlock retry.
- **Direct Persistence Layer (DPL)**: trait-based entity persistence with
  `#[derive(Entity)]`, `#[derive(PrimaryKey)]`, `#[derive(SecondaryKey)]`
  proc-macros, and full schema evolution (`Renamer`, `Deleter`,
  `Converter`, per-record class-version envelope).
- **Serialization bindings**: tuple, entry, and serde bindings with
  version-checking magic headers.
- **400+ configuration parameters** with typed validation.

### Distribution

- **XA distributed transactions** (X/Open XA two-phase commit), crash-durable
  across restart via a `TxnPrepare` WAL record.
- **Master-replica replication / HA**: Flexible Paxos leader election,
  Phi Accrual Failure Detection, VLSN-based log streaming, network restore,
  master transfer, dynamic membership, and configurable
  `ReplicaAckPolicy` / `ReplicaConsistencyPolicy`.  Transport over TCP or
  QUIC (`rustls`-based).  **Security note**: as of v3.0.2 the replication
  wire protocol has no authentication; deploy only across a trusted network
  boundary.  See
  [`docs/src/operations/known-limitations.md`](docs/src/operations/known-limitations.md)
  for the full list.

## Workspace Structure

Noxu DB is a Cargo workspace of **21 crates**:

| Layer | Crates |
|---|---|
| **Umbrella** | `noxu` (the crate users depend on) |
| Foundation | `noxu-util`, `noxu-sync`, `noxu-latch`, `noxu-config` |
| Storage / log / recovery | `noxu-log`, `noxu-tree`, `noxu-evictor`, `noxu-cleaner`, `noxu-recovery` |
| Transactions / engine | `noxu-txn`, `noxu-dbi`, `noxu-engine`, `noxu-db` |
| Higher-level APIs | `noxu-bind`, `noxu-collections`, `noxu-persist`, `noxu-persist-derive`, `noxu-xa` |
| Replication | `noxu-rep` |
| Cross-cutting | `noxu-observe` (optional `tracing` / `metrics` / OpenTelemetry), `noxu-spec` (Stateright executable specifications) |

See the [crate guide](https://codeberg.page/gregburd/noxu/maintainer/crate-guide.html)
for a per-crate purpose statement and the
[v3.0.2 capability matrix](https://codeberg.page/gregburd/noxu/introduction.html#capability-matrix).

## Building and Testing

```bash
# First-time setup — initialize the quoracle submodule used by noxu-rep.
git submodule update --init --recursive

cargo build                    # Build all crates
cargo nextest run --workspace  # Run all tests (preferred)
cargo test --workspace         # Run all tests (fallback)
cargo test -p noxu          # Test via the umbrella crate
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all
cargo doc --workspace --no-deps

make docs-check   # typos + markdownlint + mdbook build
make docs-serve   # live-reload docs at http://localhost:3000
```

The toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml)
(currently stable 1.95).

## Documentation

Full documentation is published as an mdBook:

- **Online**: <https://codeberg.page/gregburd/noxu/>
- **Source**: [`docs/src/`](docs/src/)
- **Local preview**: `make docs-serve`

Starting points:

- [Introduction & capability matrix](https://codeberg.page/gregburd/noxu/introduction.html)
- [Getting Started](https://codeberg.page/gregburd/noxu/getting-started/)
- [Transaction Processing](https://codeberg.page/gregburd/noxu/transactions/)
- [High Availability](https://codeberg.page/gregburd/noxu/replication/)
- [Programmer's Reference](https://codeberg.page/gregburd/noxu/reference/)
- [Operations Guide](https://codeberg.page/gregburd/noxu/operations/)

## Design Principles

- **Correctness first.**  Algorithms and invariants are implemented to match
  their specifications; divergence is treated as a bug.  Critical protocols
  (B+tree latching, Flexible Paxos, WAL group-commit, recovery, lock
  manager and deadlock detection, VLSN streaming, master transfer,
  network restore, XA 2PC, cleaner safety, cache↔cleaner ordering)
  are modelled in
  Stateright executable specifications under `crates/noxu-spec`.
- **Idiomatic Rust.**  RAII latches, `Result<T, NoxuError>` error handling,
  enums for closed hierarchies, traits for open extension points.
- **Minimal core dependencies.**  The core engine pulls in only
  `parking_lot`, `thiserror`, `log`, `bytes`, `crc32fast`, `byteorder`,
  `memmap2`, `fs2`, plus `hashbrown`, `lock_api`, `lru`, `libc`, and `serde`.
  Replication (`noxu-rep`) and observability (`noxu-observe`) pull in
  additional dependencies (`tokio`, `quinn`, `rustls` / `native-tls`,
  `tracing`, `metrics`, `opentelemetry`) only when their features are
  enabled.
- **Limited unsafe.**  Core data-path crates target zero `unsafe`.  The
  exceptions are `noxu-sync` (FFI to libc futex / `parking_lot` raw
  locking), `noxu-log` (memory-mapped I/O), `noxu-rep` (network I/O glue +
  `parking_lot` raw locking), and one `unsafe` block each in `noxu-latch`
  (RAII force-unlock), `noxu-db` (`unsafe impl Send for SecondaryConfig`),
  and `noxu-xa` (transaction-pointer dereference); each is documented inline.
- **No async in the core.**  Core engine uses blocking I/O with explicit
  threading.  Only `noxu-rep` networking uses tokio.
- **Own log format.**  `.ndb` files are Rust-native and not compatible with
  any other database.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow,
[AGENTS.md](AGENTS.md) for the agent / contributor guide, and
[`docs/src/contributing/`](docs/src/contributing/) for in-depth notes on
build, testing, PR process, and release.

Issues and patches are welcome at
<https://codeberg.org/gregburd/noxu>.

## Acknowledgements

Noxu DB's architecture draws on several decades of embedded database
design.  The B+tree with write-ahead logging and checkpoint recovery
follows the structure established in the embedded database literature.
The log-structured approach, BIN-delta write optimisation, and
memory-budget accounting model are derived from published techniques for
transactional embedded stores.

The replication subsystem implements Flexible Paxos for leader election
(Howard, Malkhi, and Spiegelman, 2016), the Phi Accrual Failure Detector
(Hayashibara et al., 2004), and VLSN-based log streaming.  The adaptive
replacement cache policy (Megiddo and Modha, 2003) and its CART variant
(Bansal and Modha, 2004) are available as optional eviction strategies.
The Clock with Adaptive Replacement policy references work by Jiang and
Zhang (2005).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.
