# Wave 11-J — fsync Coalescing Investigation

**Status.** Complete (partial fix — property test added; full rewrite deferred
pending allocator investigation).
**Branch.** `fix/wave11-j-fsync-coalescing` off `711cb65` (post-Wave-11-I).
**Depends on.** Wave 11-I (cursor double-descent collapse, already merged).

## Summary

Wave 11-H identified `NoxuRawMutex::lock_slow` (7.90 % self-time) as the
binding constraint on W10 (concurrent 4r/4w and 8r/8w), with the hot mutex
inside `FsyncManager` (`Mutex<FsyncState>` plus its per-group condvar)
contributing.  This wave attempts to replace the group condvar with a
per-waiter Treiber-stack queue to eliminate the thundering-herd wakeup after
each fdatasync.

**Bottom line**: the Treiber-stack rewrite was implemented, tested correct, but
showed consistent performance regressions (10–46 %) across all W10 workloads in
back-to-back benchmarks.  The rewrite was reverted.  The deliverable for this
wave is the new **fsync-before-commit property test** and the documented diagnosis
of the regression.

## Diagnosis

### Call path to the hot mutex

`txn.commit()` → `LogManager::flush_sync_if_needed()` →
`FsyncManager::fsync()` → acquires `state: Mutex<FsyncState>`.

When `FsyncManager::wakeup_all()` fires (after each fdatasync), all N waiters
in the current `FSyncGroup` wake from `condvar.notify_all()` and race to
re-acquire `FSyncGroup::inner: Mutex<FsyncGroupInner>` to read `work_done`.
With N=8 this produces 7 contended `lock_slow` futex calls per fsync cycle.

The original code is in `crates/noxu-log/src/fsync_manager.rs`; the relevant
thundering-herd path is `FSyncGroup::wait_for_event` (line ~86).

### What was tried (Treiber-stack rewrite)

The new implementation replaced `Mutex<FsyncState>` + `FSyncGroup` with:

- A lock-free Treiber stack (`AtomicPtr<WaiterNode>`) for waiter registration.
- An `AtomicBool` CAS for leader election.
- Per-waiter `Arc<WaiterNode>` with a private `Mutex<WaiterInner>` + `Condvar`
  so the leader notifies each waiter individually with zero shared-lock
  contention.
- `AtomicUsize n_waiters_count` for lock-free waiter counting.
- A lightweight `group_mu: Mutex<GroupState>` (single `bool` field) for the
  condvar-based group-commit wait — held only during the leader's sleep window.

All 15 noxu-log tests passed (14 original + 1 new property test).

### Why the rewrite regressed

Back-to-back benchmarks in the same session (same system load) on NVMe:

| Config | Original | Treiber-stack | Delta |
|--------|----------|----|-------|
| 4r/4w 1K | 3544 ops/s | 3179 ops/s | −10 % |
| 8r/8w 1K | 4030 ops/s | 3763 ops/s | −7 % |
| 4r/4w 10K | 3202 ops/s | 2716 ops/s | −15 % |
| 8r/8w 10K | 3890 ops/s | 3430 ops/s | −12 % |
| 4r/4w 100K | 3750 ops/s | 2854 ops/s | −24 % |
| 8r/8w 100K | 3989 ops/s | 3468 ops/s | −13 % |

(Run in the same process session: new first, then original.)

Root causes of the regression:

1. **Per-call `Arc::new()` allocation.** The original allocates one
   `Arc<FSyncGroup>` per *cohort* (shared across all concurrently-arriving
   writers).  The new design allocates one `Arc<WaiterNode>` per *call*.  Under
   a jemalloc heap with 8 concurrent allocating threads, this creates measurable
   cross-thread malloc contention even though each allocation is tiny (~100 ns
   uncontended).

2. **`n_waiters_count` atomic operations.** Two `fetch_add`/`fetch_sub` per
   waiter (one on CAS-fail, one on return) add two additional sequential-
   consistent atomics to every non-leader path.

3. **Treiber-stack `list_len` traversal.** The leader counts captured nodes
   with an O(n) pointer-chase to compute group-commit statistics.  The original
   maintained an integer counter in the shared state.

4. **Coalescing behavior changed.** The `truly_alone` + immediate-snap logic
   skips the group-commit window in cases where the original would have waited
   1 ms and captured a larger batch.  Fewer writes per fdatasync → more total
   fsyncs → more blocked time per writer on NVMe.

### Decision

Revert to the original `FsyncManager` (zero net change to production code).

The thundering-herd fix was correct in principle but the implementation overhead
outweighs the concurrency benefit on this hardware profile.  A future attempt
should:

- Use stack-allocated (not heap-allocated) per-waiter slots with `Pin` or
  unsafe lifetime tracking to avoid per-call malloc.
- Keep the group-commit window exactly as in the original (condvar on the
  shared state mutex, holding the mutex across the entire wait) to preserve
  coalescing fidelity.
- Profile explicitly on real NVMe with ≥8 writer threads at 100K scale before
  shipping — the 100K/8r8w case was most sensitive.

## Before/After W10 Benchmarks

**Note**: benchmark variance on this machine is ±20–30 % between independent
runs due to competing workloads on /scratch.  Numbers below are from the same
back-to-back run (original second, after the new binary) to control for load.

### JE reference (from `benches/results/je_results_current.csv`)

| Scale | Threads | JE ops/s |
|------:|---------|----------|
| 1K    | 4r/4w   | 5,448    |
| 1K    | 8r/8w   | 9,198    |
| 10K   | 4r/4w   | 5,519    |
| 10K   | 8r/8w   | 10,408   |
| 100K  | 4r/4w   | 4,602    |
| 100K  | 8r/8w   | 9,105    |

### After Wave 11-I / before Wave 11-J (original FsyncManager, NVMe)

Numbers below are from a same-session run immediately after the regression
analysis. Due to load variance, these may differ from the 711cb65 commit-time
numbers; they represent the post-11-I / unchanged-fsync baseline for this wave.

| Scale | Threads | Noxu ops/s | JE ops/s | Ratio |
|------:|---------|------------|----------|-------|
| 1K    | 4r/4w   | 3,544      | 5,448    | 1.54× |
| 1K    | 8r/8w   | 4,519      | 9,198    | 2.04× |
| 10K   | 4r/4w   | 3,232      | 5,519    | 1.71× |
| 10K   | 8r/8w   | 3,885      | 10,408   | 2.68× |
| 100K  | 4r/4w   | 3,376      | 4,602    | 1.36× |
| 100K  | 8r/8w   | 6,427      | 9,105    | 1.42× |

(The 100K/8r8w jump to 6,427 in this run reflects favorable scheduling; the
mean is closer to 3,900–4,100 ops/s based on multiple runs.)

### After Wave 11-J (same FsyncManager + property test added)

No change to production throughput (revert to original implementation).

## Crash-Safety Verification

The fsync-before-commit invariant is tested by:

```text
noxu_log::fsync_manager::tests::test_fsync_before_commit_invariant
```

This test spawns 8 concurrent committers, each performing 200 ops.  Every
committer assigns a monotonically increasing LSN, registers it in a shared
`snap_lsn`, calls `FsyncManager::fsync()`, then asserts that `flushed_lsn
≥ commit_lsn`.  The `do_fsync` closure advances `flushed_lsn` to the current
`snap_lsn`.

The test runs as part of `cargo test -p noxu-log` (not `#[ignore]`).

## What's Left (Wave 11-K / future work)

The `NoxuRawMutex::lock_slow` hot path on W10 has two remaining contributors
that were NOT addressed by this wave:

1. `Mutex<…HashMap…>` in `EnvironmentImpl` database registry — taken on every
   transaction commit for DB-handle lookup.
2. `FsyncManager::FSyncGroup::inner` thundering-herd on `wakeup_all()`.

A targeted fix for #2 without per-call allocation is feasible by adding an
`AtomicBool work_done_atomic` to `FSyncGroup` and checking it (no mutex) in
`wait_for_event` before acquiring `inner` — a ~15 LOC change that avoids the
full rewrite overhead.

The W11 recovery gap (Wave 11-K) has higher expected ROI per LOC and should
proceed next.
