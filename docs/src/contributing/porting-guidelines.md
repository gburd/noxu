# Porting Guidelines

Noxu DB is an embedded transactional database modeled on Berkeley DB
Java Edition 7.5.11 plus all 10 of its extended-fork enhancements.
Fidelity to those algorithms and behaviours is the primary quality
criterion — not idiomatic Rust style.

## Guiding Principle

> When the Rust code diverges from intended logic, it is likely a bug, not an
> improvement.

The reference implementations in `_/je/` and `_/nosql/` carry decades
of production experience in the embedded database field.  Noxu DB
attempts to inherit that algorithmic maturity through faithful algorithm
porting and test-suite porting.  When the Rust code diverges from the
intended logic, it is a bug.

## Java → Rust Naming Rules

| Java pattern | Rust pattern | Example |
|---|---|---|
| `camelCase` method | `snake_case` method | `getNodeId()` → `node_id()` |
| `getX()` / `setX(v)` | `x()` / `set_x(v)` | `getName()` → `name()` |
| `isX()` | `is_x()` | `isDirty()` → `is_dirty()` |
| Abstract class | trait | `IN` (abstract) → `NodeInfo` trait |
| Final class | struct | `BIN` → `Bin` struct |
| `static final` constant | `const` | `MAX_ENTRIES` → `MAX_ENTRIES: u32` |
| Package name | crate name | `je.tree` → `noxu-tree` |
| the corresponding Noxu type | `noxu_X::Foo` | `DatabaseImpl` → `noxu_dbi::DatabaseImpl` |
| Nested class | separate module or inner struct | `IN.Entry` → `bin::BinEntry` |
| Exception hierarchy | `NoxuError` enum variants | `DatabaseException` → `NoxuError::Database` |

### Preserved Names

The following identifiers are kept **exactly** as Noxu uses them (even though they
are abbreviations or acronyms):

- `BIN` / `Bin` — Bottom Internal Node
- `LN` / `Ln` — Leaf Node
- `IN` — Internal Node
- `LSN` / `Lsn` — Log Sequence Number
- `VLSN` / `Vlsn` — Virtual LSN (replication)
- `CBVLSN` — Committed Barrier VLSN
- `DPL` — Direct Persistence Layer

## What to Preserve

**Always preserve:**

1. **Method names** — rename only to follow the Java→Rust table above.
2. **Doc comments** — translate Javadoc to Rust doc comments; keep all content.
3. **Algorithm structure** — latch order, traversal direction, retry loops must
   match Noxu's `synchronized` blocks / latch scopes.
4. **Error conditions** — if the algorithm specification requires a specific error in a branch, Noxu
   must return the corresponding `NoxuError` variant.
5. **Configuration parameter names** — `EnvironmentConfig`, `DatabaseConfig`,
   `CursorConfig` parameter names are public API; do not rename them.
6. **MemoryBudget tracking** — MemoryBudget explicitly tracks every allocation; Noxu must
   call the equivalent `memory_budget().update_*()` methods.

## What to Adapt

**Always adapt:**

1. **Null handling** — replace `null` checks with `Option<T>`.
2. **Checked exceptions** — map to `Result<T, NoxuError>`.
3. **Synchronized methods** — replace with `parking_lot::Mutex`/`RwLock`; see
   the latch hierarchy in `docs/src/reference/concurrency-model.md`.
4. **Static mutable state** — replace with `Arc<...>` fields.
5. **`instanceof`** — replace with `match` on enum variants or trait objects.
6. **Inheritance** — use trait objects or enum dispatch; never inheritance.
7. **Thread locals** — replace with function arguments or thread-local
   `std::cell::RefCell<>` where unavoidable.

## Finding the Noxu Source

Reference code lives in the repository:

```text
_/je/  ← reference archive (read-only)
_/nosql/  ← extended fork reference (read-only)
```

The crate→Noxu package mapping is documented in
`docs/src/maintainer/crate-guide.md`. The full naming guide is in
`docs/src/maintainer/je-source-guide.md`.

## Porting a New Feature

1. Find the Java source for the feature in `_/je/` or `_/nosql/`.
2. Read the full implementation and all its tests.
3. Identify the corresponding Rust crate (see crate guide).
4. Port the implementation, preserving names, doc comments, and algorithm order.
5. Port the Java tests to Rust `#[test]` functions.
6. Run `cargo clippy -p <crate>` and fix any warnings.
7. Run `cargo test -p <crate>` and verify all tests pass.

## Noxu Enhancements

The 10 Noxu enhancements not present in standalone Noxu are:

| Enhancement | Rust location |
|---|---|
| Record Extinction + ExtinctionFilter | `noxu-db/src/extinction_filter.rs` |
| Before-Image / DataEraser | `noxu-cleaner/src/data_eraser.rs` |
| Async Acks + GroupCommit | `noxu-txn/src/group_commit.rs` |
| ByteComparator | `noxu-db/src/byte_comparator.rs` |
| UncachedLN mode | `noxu-dbi/src/cursor_impl.rs` (CacheMode enum) |
| Auto-Backup (BackupManager) | `noxu-dbi/src/backup_manager.rs` |
| Enhanced Verify (VerifyCheckpointInterval) | `noxu-recovery/src/recovery_manager.rs` |
| ScanFilter + ScanResult | `noxu-db/src/scan_filter.rs` |
| Per-slot BIN timestamps (TTL) | `noxu-tree/src/bin.rs` (modification_times, creation_times) |
| ExtinctionScanner daemon | `noxu-cleaner/src/extinction_scanner.rs` |

These are sourced from `_/nosql/` rather than `_/je/`.
