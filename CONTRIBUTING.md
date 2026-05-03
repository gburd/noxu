# Contributing to Noxu DB

## Prerequisites

- **Rust stable** (edition 2024)
- **cargo-deny** for dependency auditing: `cargo install cargo-deny`
- **cargo-nextest** for faster test runs: `cargo install cargo-nextest`

## Building

```bash
cargo build              # Build all 16 crates
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

## Porting Guidelines

Noxu DB is a Rust port of Berkeley DB Java Edition (BDB JE). When porting Java code to Rust:

- **Preserve naming**: Keep JE's class/method names, comments, and doc strings as Rust doc comments.
- **Preserve logic**: JE's logic is there for a reason. When Rust code diverges from JE logic, it is likely a bug.
- **Use enums** for closed class hierarchies (node types, log entry types).
- **Use traits** for open extension points (comparators, key creators).
- **Port MemoryBudget tracking** explicitly -- do not rely on the allocator.
- **No unsafe** in core code. Exceptions only for memmap2 and off-heap cache.
- **No async** in the core engine. Only `noxu-rep` networking may use tokio.

Reference codebases live in `_/je/` (standalone JE 7.5.11) and `_/nosql/` (Oracle NoSQL with enhanced JE fork).

## External Dependencies

Keep the dependency set minimal. The approved core set is: parking_lot, thiserror, log, bytes, crc32fast, byteorder, memmap2, fs2, serde. Adding new external crates requires discussion.

## Architecture

The workspace contains 16 crates under `crates/`, each mapping to a JE package:

- **Foundation**: noxu-util, noxu-latch, noxu-config
- **Core Engine**: noxu-log, noxu-tree, noxu-txn, noxu-evictor, noxu-cleaner, noxu-recovery, noxu-dbi, noxu-engine, noxu-db
- **Higher-Level APIs**: noxu-bind, noxu-collections, noxu-persist
- **Replication**: noxu-rep

## PR Process

1. Create a feature branch from `main`.
2. Make your changes, ensuring `cargo fmt`, `cargo clippy`, and `cargo test` all pass.
3. Write tests for new functionality.
4. Keep commits focused and well-described.
5. Open a pull request against `main`.
