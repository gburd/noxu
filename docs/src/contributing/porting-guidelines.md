# Porting Guidelines

Noxu DB is a faithful Rust port of Berkeley DB Java Edition 7.5.11 plus all 10
Oracle NoSQL JE enhancements. Fidelity to JE's algorithms and behaviour is the
primary quality criterion — not idiomatic Rust style.

## Guiding Principle

> When the Rust code diverges from JE logic, it is likely a bug, not an
> improvement.

JE's implementation has been battle-tested in production systems for over 20
years. Its algorithms — the B-tree latch-coupling traversal, the three-phase
recovery, the VLSN tracking, the phi accrual failure detector — are correct and
have known complexity bounds. Before "simplifying" any ported code, verify
against the JE source.

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
| `com.sleepycat.je.Foo` | `noxu_X::Foo` | `DatabaseImpl` → `noxu_dbi::DatabaseImpl` |
| Nested class | separate module or inner struct | `IN.Entry` → `bin::BinEntry` |
| Exception hierarchy | `NoxuError` enum variants | `DatabaseException` → `NoxuError::Database` |

### Preserved Names

The following identifiers are kept **exactly** as JE uses them (even though they
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
   match JE's `synchronized` blocks / latch scopes.
4. **Error conditions** — if JE throws a specific exception in a branch, Noxu
   must return the corresponding `NoxuError` variant.
5. **Configuration parameter names** — `EnvironmentConfig`, `DatabaseConfig`,
   `CursorConfig` parameter names are public API; keep them identical to JE.
6. **MemoryBudget tracking** — JE explicitly tracks every allocation; Noxu must
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

## Finding the JE Source

Reference code lives in the repository:

```
_/je/src/com/sleepycat/je/         ← standalone JE 7.5.11
_/nosql/kvmain/src/main/java/com/sleepycat/je/  ← NoSQL enhanced fork
```

The crate→JE package mapping is documented in
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

## NoSQL Enhancements

The 10 NoSQL enhancements not present in standalone JE are:

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
