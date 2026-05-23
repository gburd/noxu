# Contributing to Noxu DB

## Prerequisites

- **Rust stable** (edition 2024)
- **cargo-deny** for dependency auditing: `cargo install cargo-deny`
- **cargo-nextest** for faster test runs: `cargo install cargo-nextest`

## Building

```bash
# First-time setup: initialize the quoracle submodule used by noxu-rep.
git submodule update --init --recursive

cargo build              # Build all crates
cargo build -p noxu-util # Build a single crate
```

## Testing

```bash
cargo test               # Run all tests
cargo test -p noxu-util  # Test a single crate
cargo nextest run        # Run tests with nextest (faster)
```

## Code Style

- Run `cargo fmt` before committing. All code must be formatted.
- Run `cargo clippy` and resolve all warnings. CI enforces zero warnings.
- Workspace-level clippy lints are configured in the root `Cargo.toml`.

## Development Guidelines

When modifying or extending Noxu DB subsystems:

- **Preserve naming**: Keep established type/method names, comments, and doc strings.
- **Preserve logic**: The existing logic reflects careful design. Divergence from the intended algorithm is likely a bug.
- **Use enums** for closed class hierarchies (node types, log entry types).
- **Use traits** for open extension points (comparators, key creators).
- **Update MemoryBudget** explicitly — do not rely on the allocator.
- **Limit unsafe**: core data-path crates target zero `unsafe`. New `unsafe`
  blocks need review and an inline comment explaining why they are sound.
- **No async** in the core engine. Only `noxu-rep` networking may use tokio.

If you have a local checkout of the upstream Java reference sources at
`_/je/` and `_/nosql/`, treat them as read-only. They are gitignored and
not required to build, test, or contribute.

## External Dependencies

The core engine pulls in only `parking_lot`, `thiserror`, `log`, `bytes`,
`crc32fast`, `byteorder`, `memmap2`, `fs2`, `serde`, `hashbrown`,
`lock_api`, `lru`, and `libc`. Replication (`noxu-rep`) and observability
(`noxu-observe`) pull in extra dependencies (`tokio`, `quinn`, `rustls` /
`native-tls`, `tracing`, `metrics`, `opentelemetry`) only when their
features are enabled. Adding new external crates requires discussion.

## Architecture

The workspace contains 19 crates under `crates/`:

- **Foundation**: noxu-util, noxu-sync, noxu-latch, noxu-config
- **Core Engine**: noxu-log, noxu-tree, noxu-txn, noxu-evictor, noxu-cleaner, noxu-recovery, noxu-dbi, noxu-engine, noxu-db
- **Higher-Level APIs**: noxu-bind, noxu-collections, noxu-persist
- **Distributed Transactions**: noxu-xa
- **Replication**: noxu-rep
- **Observability**: noxu-observe (optional)

## PR Process

1. Create a feature branch from `main`.
2. Make your changes, ensuring `cargo fmt`, `cargo clippy`, and `cargo test` all pass.
3. Write tests for new functionality.
4. Keep commits focused and well-described.
5. Open a pull request against `main`.
