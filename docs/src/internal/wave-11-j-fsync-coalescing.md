# Wave 11-J — fsync Coalescing Optimization

**Status.** In progress.
**Branch.** `fix/wave11-j-fsync-coalescing` off `711cb65` (post-Wave-11-I).
**Depends on.** Wave 11-I (cursor double-descent collapse, already merged).

## Summary

Wave 11-H identified two binding constraints on W10 (concurrent 4r/4w and
8r/8w):

1. `NoxuRawMutex::lock_slow` (7.90 % self-time) driven by contention on the
   `Mutex<FsyncState>` inside `FsyncManager` plus the LogManager pending-buffer
   mutex.
2. Linear BIN-scan memcmp (15.45 %) — already addressed by Wave 11-I.

On real NVMe storage the fsync serialization is the binding constraint because
each `fdatasync` blocks for ~50–200 µs, and the `Mutex<FsyncState>` leader
election forces serialization of those waits.  This wave replaces the
contended global state mutex with a per-waiter notification scheme and caps
the leader's group-commit window at a configurable value.

## Diagnosis

### Call path to the hot mutex

`txn.commit()` → `LogManager::flush_sync_if_needed()` →
`FsyncManager::fsync()` → acquires `state: Mutex<FsyncState>`.

Every concurrent committer serializes through this single `Mutex<FsyncState>`.
Under 8 writer threads, 8 threads compete to acquire the state mutex just to
decide whether to lead or wait — even before anyone touches the file system.

The specific frame `NoxuRawMutex::lock_slow` (7.90 % self-time) is the futex
slow-path for this mutex.  The condvar wait `leader_condvar.wait_timeout`
holds the mutex for the full `grpc_interval_ms` window, blocking all waiters.

See `crates/noxu-log/src/fsync_manager.rs` for the original implementation.

### What the old code does wrong

The old `FsyncManager` leader-election path:

1. Acquires `Mutex<FsyncState>` (contended under N writers).
2. If `work_in_progress`, joins `next_fsync_waiters` (still holding nothing,
   but the mutex was already released).  The waiter then calls
   `FSyncGroup::wait_for_event` which acquires the `FSyncGroup::inner` mutex.
3. The leader, on wakeup, must re-acquire `Mutex<FsyncState>` to swap in the
   fresh `FSyncGroup`.

Each `notify_one()` wakes a single waiter, which then acquires
`Mutex<FsyncState>` to become the new leader — causing a thundering-herd-lite
effect under bursts.

### Fix (file:line summary)

**`crates/noxu-log/src/fsync_manager.rs`** — complete rewrite of the
`FsyncManager` internals:

- Remove `Mutex<FsyncState>` with leader-condvar pair.
- Introduce a single `AtomicU64` "epoch" counter and a per-waiter
  `WaiterSlot` (a `parking_lot::Mutex<WaiterState>` + `parking_lot::Condvar`
  per calling thread, allocated on the stack).
- A `AtomicPtr` singly-linked list of `WaiterSlot`s forms the lock-free
  enqueue path: each thread CAS-appends its own slot before checking whether
  it is the leader.
- The leader drains the list atomically (single CAS swap), calls
  `fdatasync`, then iterates the captured list notifying each waiter.
- Group-commit window: the leader parks on a separate `Condvar` for up to
  `grpc_interval_us` microseconds (configurable, default 50 µs NVMe /
  5000 µs HDD); it is woken early if `grpc_threshold` waiters accumulate.
- Public API unchanged: `FsyncManager::new`, `FsyncManager::fsync`,
  all stat accessors.

**`crates/noxu-config/src/lib.rs`** (if needed) — add
`log_fsync_wait_us: u64` and `log_fsync_threshold: usize` parameters.

**New test** — `crates/noxu-log/src/fsync_manager.rs::tests::test_fsync_before_commit_invariant`
(property test: every committed LSN ≤ last fsynced LSN after `fsync()` returns).

## Before/After W10 Benchmarks

*Numbers will be filled in once both measurement runs complete.*

### Baseline (after Wave 11-I, before Wave 11-J)

| Scale | Threads | Storage | Noxu ops/s | JE ops/s | Ratio |
|------:|---------|---------|------------|----------|-------|
| 1K    | 4r/4w   | tmpfs   | TBD        | TBD      | TBD×  |
| 1K    | 8r/8w   | tmpfs   | TBD        | TBD      | TBD×  |
| 10K   | 4r/4w   | tmpfs   | TBD        | TBD      | TBD×  |
| 10K   | 8r/8w   | tmpfs   | TBD        | TBD      | TBD×  |
| 100K  | 4r/4w   | tmpfs   | TBD        | TBD      | TBD×  |
| 100K  | 8r/8w   | tmpfs   | TBD        | TBD      | TBD×  |
| 1K    | 4r/4w   | NVMe    | TBD        | TBD      | TBD×  |
| 1K    | 8r/8w   | NVMe    | TBD        | TBD      | TBD×  |
| 10K   | 4r/4w   | NVMe    | TBD        | TBD      | TBD×  |
| 10K   | 8r/8w   | NVMe    | TBD        | TBD      | TBD×  |
| 100K  | 4r/4w   | NVMe    | TBD        | TBD      | TBD×  |
| 100K  | 8r/8w   | NVMe    | TBD        | TBD      | TBD×  |

### After Wave 11-J

*(same table — to be filled in)*

## Crash-safety Verification

The fsync-before-commit invariant is tested by
`noxu_log::fsync_manager::tests::test_fsync_before_commit_invariant`.
That test spawns N concurrent committers, each recording a monotonically
increasing LSN, and asserts that when `FsyncManager::fsync()` returns `Ok(())`
the flushed-LSN counter (simulated by the test) is ≥ every committer's LSN.

The invariant is also covered by the existing stateright spec
`noxu_spec` (if applicable) and by the `noxu-log` integration tests.
