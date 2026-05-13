# Noxu DB Agent Guide

<!-- This file is the canonical agent instruction document for the lamdb repo.
     It re-exports the content of .agent/AGENT.md and adds the Documentation
     section below. Keep both files in sync when updating agent instructions. -->

## Project Overview

Noxu DB is an embedded transactional key-value database engine written in Rust.
It provides ACID transactions, a log-structured B+tree, checkpoint-based crash
recovery, and optional master-replica replication — all in a single library with
no external database process required.

The project lives at `/home/gburd/ws/lamdb/` and uses a Cargo workspace with 16
crates under `crates/`.

## Crate Map

The 16 crates are organized by implementation layer:

### Phase 0 — Foundation (complete)

| Crate | Purpose |
|---|---|
| `noxu-util` | LSN, VLSN, packed integers, stats, daemon threads |
| `noxu-latch` | Exclusive and shared/exclusive latches (parking_lot) |
| `noxu-config` | 400+ configuration parameters with validation |

### Phase 1–6 — Core Engine (complete)

| Crate | Purpose |
|---|---|
| `noxu-log` | Write-ahead log, FileManager, LogManager, checksums |
| `noxu-tree` | B-tree: IN, BIN, LN, key prefixing, BIN-deltas |
| `noxu-txn` | Transactions, record-level locking, deadlock detection |
| `noxu-dbi` | EnvironmentImpl, DatabaseImpl, CursorImpl |
| `noxu-evictor` | LRU cache eviction, off-heap support |
| `noxu-cleaner` | Log file garbage collection, utilization tracking |
| `noxu-recovery` | Checkpoint-based crash recovery |
| `noxu-engine` | Engine orchestration, daemon lifecycle |
| `noxu-db` | Public API: Environment, Database, Cursor, Transaction |

### Phase 7 — Higher-Level APIs (complete)

| Crate | Purpose |
|---|---|
| `noxu-bind` | Serialization bindings (tuple, entry, serial) |
| `noxu-collections` | Iterator-based collection views |
| `noxu-persist` | Derive-macro entity persistence (DPL) |

### Phase 8 — Replication (complete)

| Crate | Purpose |
|---|---|
| `noxu-rep` | Master-replica HA, elections, VLSN tracking |

## Build, Test, and Lint Commands

```bash
cargo build                    # Build all crates
cargo nextest run --workspace  # Run all tests (preferred)
cargo test                     # Run all tests (fallback)
cargo test -p noxu-util        # Test a single crate
cargo clippy --workspace --all-targets --all-features -- -D warnings  # Full CI lint
cargo fmt --all                # Format all crates
cargo fmt --all -- --check     # Check formatting without modifying
cargo doc --workspace --no-deps  # Build Rust API documentation

make docs-check   # Full docs quality gate: typos + markdownlint + mdbook build
make docs-serve   # Live-reload docs at http://localhost:3000
```

## Key Design Decisions

- **Log format**: Rust-native `.ndb` format. Not compatible with other database formats.
- **External crates**: Minimal — parking_lot, thiserror, log, bytes, crc32fast,
  byteorder, memmap2, fs2.
- **Concurrency**: `parking_lot::Mutex/RwLock` for latches, `std::sync::atomic`
  for volatile fields, `Arc<RwLock<IN>>` for tree nodes.
- **Isolation model**: Lock-based, NOT MVCC. Writers lock BIN slots; readers
  block on write-locked records.
- **Error handling**: `thiserror` enums, `Result<T, NoxuError>` everywhere.
  No `unwrap()` in library code.
- **No async**: Core engine uses blocking I/O with explicit threading. Only
  `noxu-rep` networking uses tokio.
- **No unsafe**: Target zero unsafe in core. Exceptions only for memmap2 and
  off-heap cache.
- **CRC32**: Uses `crc32fast` (CLMUL/PCLMULQDQ hardware acceleration, 15.8
  GiB/s at 1KiB). Not CRC32C — see `docs/src/internal/checksum-selection.md`.

## Reference Archives

When auditing or extending subsystems, compare against the archived reference
source (read-only — do not modify):

- **Core archive**: `_/je/src/`
- **Extended-fork archive**: `_/nosql/kvmain/src/main/java/`

## Development Guidelines

When implementing or modifying subsystem code:

1. Preserve method names, doc comment content, and algorithm structure.
2. The existing logic reflects careful design — when Rust code diverges from
   the intended algorithm, it is likely a bug.
3. Use enums for closed class hierarchies (node types, log entry types).
4. Use traits for open extension points (comparators, key creators).
5. Port explicit MemoryBudget tracking — do not rely on the allocator.
6. See `docs/src/contributing/porting-guidelines.md` for the naming
   conventions and adaptation patterns.

## Common Tasks

### Adding a new feature to a crate

1. Locate the corresponding reference in `_/je/` or `_/nosql/`.
2. Read the reference implementation and its tests.
3. Implement preserving names, comments, and algorithm structure.
4. Write unit tests (in-module `#[cfg(test)]`) and integration tests (in `tests/`).
5. Run `cargo test -p <crate>` and `cargo clippy -p <crate>`.

### Investigating a test failure

1. Run the failing test with `cargo test -p <crate> -- <test_name> --nocapture`.
2. Compare the Rust logic against the reference source for that component.
3. Check whether the divergence is intentional (Rust idiom) or an algorithm bug.

### Auditing implementation completeness

See `.agent/skills/design-audit.md` for the full process.

### Running the full CI suite locally

```bash
cargo fmt --all -- --check && \
cargo clippy --workspace --all-targets --all-features -- -D warnings && \
cargo nextest run --workspace && \
cargo doc --workspace --no-deps && \
make docs-check
```

## Environment Notes

- NixOS system, Rust 1.94 via nix-profile.
- `ThreadId::as_u64()` is unstable — use hash-based thread ID instead.
- noxu-util re-exports `Lsn`/`Vlsn`/`NULL_LSN` at crate root.
- `Lsn::new(file_number: u32, file_offset: u32)` or `Lsn::from_u64(val: u64)`.

## Skills

Agent skill files are in `.agent/skills/`:

- [hegel-pbt.md](/.agent/skills/hegel-pbt.md) — Property-based testing with Hegel
- [git-workflow.md](/.agent/skills/git-workflow.md) — Git workflow conventions
- [code-review.md](/.agent/skills/code-review.md) — Rust code review checklist
- [design-audit.md](/.agent/skills/design-audit.md) — Design audit process
- [testing.md](/.agent/skills/testing.md) — Testing guide for Noxu DB

---

## Documentation

**Tool**: mdBook 0.4.40. **Source**: `docs/src/`. **Output**: `docs/book/`.

Published at:
- GitHub Pages: `https://gburd.github.io/lamdb/`
- Codeberg Pages: `https://codeberg.page/gburd/lamdb`

### What belongs where

| Content | Location |
|---|---|
| User API guide, tutorials, how-to examples | `docs/src/getting-started/`, `transactions/`, `replication/`, `collections/` |
| Architecture internals (contributors) | `docs/src/reference/` |
| Production sizing / monitoring / ops | `docs/src/operations/` |
| Maintainer context: algorithms, design decisions, crate guide | `docs/src/maintainer/` |
| Internal analysis, audits, research | `docs/src/internal/` |
| Contributor process (build, test, PR, release) | `docs/src/contributing/` |
| Root-of-repo files (README, CONTRIBUTING, SAFETY, SECURITY) | **project root** — do NOT move |

### When to update docs

| Change made | What to update |
|---|---|
| New public API | `docs/src/getting-started/` or relevant chapter |
| Architecture or on-disk format change | `docs/src/reference/` AND `docs/src/maintainer/algorithms.md` |
| New design decision | `docs/src/maintainer/design-decisions.md` |
| Config parameter added/changed | `docs/src/reference/configuration.md` |
| Replication behaviour change | `docs/src/replication/` |
| Operational behaviour change | `docs/src/operations/` |
| New crate added | `docs/src/maintainer/crate-guide.md` |
| Audit / research notes | `docs/src/internal/` only |
| Any docs change | Run `make docs-check` before committing |

### Quality gates (must pass before merge)

1. `typos docs/src/` — zero spelling errors
2. `markdownlint-cli2 "docs/src/**/*.md"` — zero lint violations
3. `mdbook build docs/` — zero build errors, zero broken links

### Building locally

```bash
# Install tools (one-time)
cargo install mdbook --version 0.4.40 --locked
cargo install mdbook-mermaid --version 0.13.0 --locked
npm install -g markdownlint-cli2

# Daily use
make docs-serve    # live-reload at http://localhost:3000
make docs-check    # full gate run (spell + lint + build)
```

### docs/src/ structure

```
docs/src/
├── SUMMARY.md              ← mdBook table of contents (source of truth)
├── introduction.md         ← landing page
├── getting-started/        ← installation, environments, databases, cursors
├── transactions/           ← basics, concurrency, isolation, durability
├── replication/            ← concepts, setup, elections, consistency
├── collections/            ← StoredMap, StoredSet, StoredList, DPL
├── reference/              ← architecture, log format, B-tree, concurrency model
├── operations/             ← sizing, monitoring, tuning, backup, recovery
├── contributing/           ← build, testing, PR process, release
├── maintainer/             ← project history, algorithms, design decisions
└── internal/               ← design reviews, audit reports, research
```
