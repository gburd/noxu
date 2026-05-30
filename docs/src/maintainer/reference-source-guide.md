# Codebase Navigation

This guide explains how Noxu DB's crates, modules, and types are organised and how to navigate them efficiently.

## Reference Archives

The `_/` directory is intended for read-only reference material used
during porting work. It is gitignored — these files are not committed and
are not required to build, test, or contribute. Place them at the
repository root if you have local copies. Do NOT modify them.

```text
<repo-root>/_/je/            embedded transactional database reference (Java, read-only)
<repo-root>/_/nosql/         extended variant with 10 additional features (Java, read-only)
```

## Crate Structure

| Crate | Module path | Key source files |
|---|---|---|
| `noxu-tree` | `noxu_tree::` | `in_node.rs`, `bin.rs`, `tree.rs`, `node.rs` |
| `noxu-log` | `noxu_log::` | `file_manager.rs`, `log_manager.rs`, `log_buffer.rs`, `fsync_manager.rs` |
| `noxu-txn` | `noxu_txn::` | `txn.rs`, `lock_manager.rs`, `locker.rs`, `group_commit.rs` |
| `noxu-dbi` | `noxu_dbi::` | `environment_impl.rs`, `database_impl.rs`, `cursor_impl.rs` |
| `noxu-evictor` | `noxu_evictor::` | `evictor.rs` |
| `noxu-cleaner` | `noxu_cleaner::` | `cleaner.rs`, `utilization_profile.rs`, `file_selector.rs` |
| `noxu-recovery` | `noxu_recovery::` | `recovery_manager.rs`, `checkpointer.rs` |
| `noxu-rep` | `noxu::replication::` | `replicated_environment.rs`, `stream/peer_feeder.rs`, `elections/paxos.rs` |
| `noxu-config` | `noxu_config::` | `params.rs` |

## Naming Conventions

Noxu uses idiomatic Rust naming throughout:

| Convention | Rule | Example |
|---|---|---|
| Types | `UpperCamelCase` | `RecoveryManager`, `UtilizationProfile` |
| Methods | `snake_case` | `get_last_known_master_address()` |
| Constants | `SCREAMING_SNAKE_CASE` | `NULL_TXN_ID` |
| Boolean accessors | `fn is_x() -> bool` | `is_master()` |
| Mutating setters | `fn set_x(&mut self, v)` | `set_master_address(a)` |
| Shared mutable state | `Arc<parking_lot::RwLock<T>>` | tree nodes |
| Volatile counters | `AtomicT` | `AtomicU64`, `AtomicBool` |

## Finding a Type

Look up the crate by subsystem concern:

- B+tree nodes, search, splits → `noxu-tree`
- Write-ahead log, file I/O, fsync → `noxu-log`
- Lock management, deadlock detection, transactions → `noxu-txn`
- Internal environment/database/cursor implementations → `noxu-dbi`
- Cache eviction, memory budget → `noxu-evictor`
- Log garbage collection → `noxu-cleaner`
- Crash recovery, checkpointing → `noxu-recovery`
- Replication, elections, VLSN → `noxu-rep`
- 400+ configuration parameters → `noxu-config`
- Public user-facing API → `noxu-db`

## Key Invariants to Preserve

When modifying subsystem code, preserve these invariants:

1. **Algorithm structure** — control flow, edge case handling, and data structure invariants are load-bearing
2. **Naming** — type names, method names, and constant names are part of the public API surface
3. **Assertions** — `debug_assert!` macros document and enforce internal invariants; do not remove them
4. **Error conditions** — each `NoxuError` variant has defined semantics; new failure modes need a new variant
5. **Memory budget** — every allocation that touches tree nodes, locks, or buffers must update `MemoryBudget`

## Design Patterns

| Concern | Rust implementation |
|---|---|
| Manual memory management | RAII + explicit `MemoryBudget` accounting |
| Closed type hierarchies | `enum` variants |
| Open extension points | `trait` objects |
| Error propagation | `Result<T, NoxuError>` |
| Mutual exclusion | `parking_lot::Mutex`/`RwLock` + RAII guards |
| Atomic state | `AtomicT` (`AtomicU64`, `AtomicBool`) |
| Generic collections | Rust generics with explicit `Send + Sync` bounds |
