# Noxu DB Safety Model

This document applies
[STPA](http://psas.scripts.mit.edu/home/get_file.php?name=STPA_handbook.pdf)-style
hazard analysis to the Noxu DB embedded transactional database engine for the
purpose of guiding design and testing efforts to prevent unacceptable losses.

## Losses

We wish to prevent the following undesirable situations:

- **Data loss**  -  committed data disappears or becomes unreadable
- **Data corruption**  -  B-tree or log enters inconsistent state
- **Inconsistent reads**  -  queries return stale or phantom results
- **Process crash**  -  panic or resource exhaustion kills the host process
- **Deadlock**  -  transactions permanently block, preventing forward progress

## System Boundary

**Inside the boundary** (things we control):

- Codebase: safe control actions that prevent losses
- Documentation: guidance on safe usage, recommended configurations
- Test suite: unit, integration, property-based, and fuzz tests

**Outside the boundary** (things we cannot control):

- Hardware failures (disk corruption, power loss mid-write)
- Kernel bugs (filesystem, scheduler)
- User code (application logic, configuration choices)

## Hazards

### H1: Data Loss

Data may be lost if:

- WAL flush ordering is violated (modification applied before log write)
- `fsync` fails silently or is not called at commit time
- Checkpoint references data that has been cleaned (log file deleted)
- Recovery fails to replay all committed operations after crash

**Mitigations:**
- Write-ahead logging: all modifications logged before B-tree update
- CRC32 checksums on every log entry, validated during recovery
- Cleaner coordinates with checkpoint to avoid deleting active data
- 3-phase recovery: find checkpoint -> redo -> undo uncommitted

### H2: Data Corruption

The B-tree may become inconsistent if:

- Latch coupling protocol is violated during tree traversal
- Split/merge operations leave orphaned or duplicate entries
- Key prefix compression produces incorrect key reconstruction
- BIN-delta application produces incorrect slot state

**Mitigations:**
- Strict top-down latch coupling (parent before child)
- `Verify` subsystem validates B-tree structural invariants
- Serialization round-trip tests for all node types
- Property-based tests for key ordering invariants

### H3: Deadlock

Transactions may deadlock if:

- Two transactions hold locks that the other needs
- Latch ordering is violated (child before parent)

**Mitigations:**
- `DeadlockDetector` runs DFS cycle detection on wait graph
- Victim transaction aborted with `LockConflict` error
- 16-shard lock table reduces contention
- Strict latch ordering protocol documented and enforced

### H4: Resource Exhaustion

The process may crash if:

- Cache grows beyond configured memory budget
- Log files accumulate without cleaning
- File handles are leaked

**Mitigations:**
- `MemoryBudget` explicitly tracks all allocations
- LRU evictor enforces cache size limits
- Cleaner daemon reclaims space from obsolete log entries
- RAII patterns for file handles and latch guards

## Memory Safety

Noxu DB targets **zero `unsafe` code** in core crates:

- All concurrency through `parking_lot::Mutex/RwLock`
- Tree nodes use `Arc<RwLock<IN>>` for shared ownership
- Atomic operations use `std::sync::atomic` with correct orderings
- Exceptions limited to: `memmap2` (memory-mapped files), potential off-heap cache

## Replication Safety

- VLSN (Version Log Sequence Number) provides total ordering across replicas
- Election protocol requires quorum (majority of electable nodes)
- Master transfer uses coordinated handoff with VLSN synchronization
- Network restore detects and repairs VLSN gaps

## WAL write-error handling (the "fsyncgate" stance)

A `write()` that reached the OS page cache is **not durable**; only a
successful `fdatasync` is. The hard question is what to do when that
`fdatasync` (or the preceding `pwrite`) **fails**. On Linux a failed
`fsync`/`fdatasync` may drop the dirty page and is **not reliably
retryable** — a second `fsync` can return success while the data is already
gone (the "fsyncgate" problem, PostgreSQL 2018).

**Noxu's stance:** *fail-stop on any WAL sync error, never retry, never
swallow.* Concretely:

- The single `fdatasync` runs inside `FsyncManager::flush_and_sync`
  (`crates/noxu-log/src/fsync_manager.rs`). Any `io::Error` from the leader's
  drain+`fdatasync` closure is propagated to the leader **and to every
  piggybacking waiter** (`wakeup_all_with_error`) — every committer in the
  failed group sees the error; none returns `Ok` on a failed sync.
- On that error, `LogManager::flush_sync` sets the permanent `io_invalid`
  flag (`crates/noxu-log/src/log_manager.rs`). Once set, **every subsequent
  `log()` call is refused** with `WriteFailed`, so the environment cannot
  accept writes it might silently lose. `is_io_invalid()` exposes the state.
- Noxu does **not** retry the failed `fdatasync`. Retrying is the unsafe
  behavior fsyncgate warns against, because the kernel may already have
  discarded the dirty page and a retry can report a false success.

**What Noxu does NOT do (documented limitation):** it does not attempt a
full fsyncgate mitigation (e.g. re-`open`-and-re-`fsync`, or panicking the
process the way PostgreSQL now does). It marks the environment invalid and
refuses further writes; the operator must close and re-open (which runs
recovery from the last durable checkpoint). See
`docs/src/operations/known-limitations.md` and
`docs/src/operations/power-loss.md` for the runbook and the residual risk.

Regression coverage: `fsync_manager.rs` unit tests
(`test_fsync_error_propagated_to_waiters`,
`test_leader_fsync_failure_fails_all_piggybacking_waiters`) prove
per-waiter error propagation; `log_manager.rs`'s `io_invalid` regression
test proves writes are refused after a sync error.

## ThreadSanitizer suppression justifications

`tsan_suppressions.txt` (repo root) suppresses a small set of TSAN reports.
**Every suppression is a claim that the reported race is a TSAN modeling
limitation, not a real race** — a database's sync layer cannot afford an
unjustified suppression, because it would hide a real data race behind a lid.
The justifications live in the suppressions file itself and are summarized
here for durability:

| Suppression | Why it is a TSAN false positive (not a bug) |
|---|---|
| `race:Arc*drop` | `Arc::drop` decrements the strong count `Release` and the last dropper issues a standalone `atomic::fence(Acquire)` before freeing. TSAN models happens-before through atomic ops on a *location*, not a decoupled `fence()`, so it flags the final teardown load as a race. The libstd Arc teardown is sound. |
| `race:std::thread::local` | `thread_local!` `LocalKey` accessors publish the initialized value through a libstd-internal acquire/release handshake TSAN instruments as opaque; the reader load looks unsynchronized. The init is fully synchronized by contract. |
| `race:std::sync::Once` | `Once` / `OnceLock` / `LazyLock` one-time init publishes through the same internal state machine; same modeling gap. Covers all one-time-init statics (cleaner, evictor, clock, replicated-environment). |
| `race:lazy_static` | **Vestigial / defense-in-depth only.** Noxu has NO `lazy_static` dependency (verified: not in `Cargo.lock`). Retained so a transitively-pulled dep using the crate does not reintroduce a spurious report. Suppresses nothing in Noxu's own code. |

**Audit invariant:** if a suppression is ever needed for a race in Noxu's
*own* code that is not one of the four modeling limitations above, that is a
critical finding — fix the race, do not add the suppression.

## Summary

| Hazard | Primary Mitigation | Secondary Mitigation |
|--------|-------------------|---------------------|
| Data loss | WAL + fsync | 3-phase recovery |
| WAL sync failure | Fail-stop (`io_invalid`) + per-waiter error | No silent retry (fsyncgate-safe) |
| Corruption | Latch coupling + CRC32 | B-tree verification |
| Deadlock | DFS cycle detection | Latch ordering protocol |
| Resource exhaustion | MemoryBudget + LRU | Cleaner daemon |
