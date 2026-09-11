# Design note: batched per-txn obsolete counting (feat/local-utilization-tracker)

Status: design locked, implementation starting. This file is the working
record so a resumed session (or a different agent) does not start from
scratch.

## What is actually happening today (the mechanism, traced from code)

Two independent things already take the *same* global
`Arc<Mutex<UtilizationTracker>>` per user operation, even with the gates OFF:

1. `LogManager::log*` calls `obs.count_new_entry(...)` for every log entry
   written, under the log-write latch (LWL) — this is unconditional, not
   gated, and is the baseline cost already paid at 267k-318k ops/s.
2. With `NOXU_COUNT_AUTOCOMMIT_OBSOLETE` / `NOXU_COUNT_TXN_OBSOLETE` on,
   `Txn::commit_append_phase` additionally calls
   `self.count_obsolete_abort_lsns()` — a SEPARATE call, NOT under the LWL,
   that builds a `Vec<(Lsn, Option<u32>, i32)>` from `self.write_locks` and
   passes it to `LogManager::count_obsolete_commit_lsns`, which loops and
   calls `UtilizationTrackerObserver::count_obsolete` ONCE PER LSN — and
   that method takes `self.tracker.lock()` **inside itself, per call**.

Arithmetically that is "one extra global-mutex acquisition per commit" for
the single-key auto-commit workload xbench's `ycsb_a` runs (one write lock
per put). One extra acquisition of an already-taken-267k-times/sec mutex
should cost roughly 2x, not the measured ~33x. The multiplier is explained by
*where* it happens, not *how many times*:

- `count_obsolete_abort_lsns()` runs **before** the Fix-3a write-lock release
  (the comment in `txn.rs` says so explicitly, and it is doing so on purpose
  — `self.write_locks` must still be intact to read the abort LSNs).
- So for the whole duration of "wait for + hold the GLOBAL tracker mutex",
  this thread is **also still holding the PER-RECORD write lock**.
- Under a Zipfian-skewed workload, many other threads are already queued on
  that same hot record's write lock. Each of them now waits not just for
  this thread's own work, but for this thread to also win a highly-contended
  *global* mutex before it releases the *local* one. That is a lock-ordering
  convoy: contention on one lock is being funneled through contention on a
  second, unrelated lock, while the first is still held. This is the kind of
  interaction that produces super-linear (not 2x) throughput cliffs under
  concurrency — consistent with the measured 33x.

JE does not hit this because JE's equivalent per-commit obsolete counting
(`Txn.getObsoleteLsnInfo` → `LogParams.obsoleteWriteLockInfo` →
`LogManager.updateObsolete`) runs **inside the same `synchronized
(logWriteMutex)` block that writes the commit log entry itself** — it is
piggybacked onto a lock acquisition the commit path needs anyway, not an
extra independent one, and JE's write-lock release (`releaseWriteLocks()`)
happens *after* that whole logging step, so there is no "local lock held
while independently waiting on global lock" ordering issue to begin with
(JE just has one lock-then-log-then-release sequence). Note: this means
JE's actual `LocalUtilizationTracker` class is used for a *different* set of
call sites in JE (Checkpointer, Evictor, INCompressor, DupConvert, DB
delete/truncate) — bulk background scans that touch many nodes outside the
LWL — not for the per-commit write-lock path. The per-commit path in JE
already goes straight to the global tracker, safely, because of the
lock-ordering point above.

## Design choice

**Per-txn accumulation, flushed once at commit, in TWO parts:**

1. **Batch the merge into one lock acquisition per commit.** Add a batched
   entry point (`LogWriteObserver::count_obsolete_batch`,
   `UtilizationTrackerObserver::count_obsolete_batch`,
   `UtilizationTracker::count_obsolete_batch`) that takes the tracker mutex
   ONCE and applies every `(Lsn, db_id, size)` tuple from a single
   `count_obsolete_commit_lsns` call inside that one critical section,
   instead of the current per-LSN `self.tracker.lock()`. This is JE's
   `BaseLocalUtilizationTracker.transferToUtilizationTracker` shape (many
   local updates, one lock, one merge) — except the "local" accumulation
   Noxu already has for free in `Txn::write_locks` /
   `WriteLockInfo::abort_lsn`, so no new per-thread or per-txn data
   structure is needed to hold it. `Txn::write_locks` IS the local tracker;
   it just needed a batched drain API on the merge side.
2. **Move the merge to AFTER the write-lock release, not before.** The only
   reason `count_obsolete_abort_lsns()` currently runs before Fix-3a's
   unlock is that it reads `self.write_locks` to build the LSN list. That
   list (`Vec<(Lsn, Option<u32>, i32)>`) is plain owned data once built —
   nothing after that point needs the locks held. So: build the list before
   the unlock (as today), but move the call that actually merges it into
   the global tracker (the one taking the contended global mutex) to run
   after `self.write_locks.clear()` / after the per-record locks are
   released. This directly removes the "hold hot per-record lock while
   waiting on unrelated global lock" convoy identified above.

Why not thread_local, why not sharded:

- **thread_local**: would require a periodic/explicit flush trigger
  (nothing currently calls back into a per-thread tracker on a schedule),
  adds a new global registry of live thread-locals to iterate at
  checkpoint/close time, and does not shrink the *per-commit* lock-hold
  window that the mechanism analysis above says is the actual problem —
  it only reduces the *frequency* of lock acquisitions across MULTIPLE
  commits by the same thread, which is a different (and, for this bug,
  probably smaller) win than fixing the ordering.
- **sharded global tracker**: real option for reducing steady-state
  contention on the mutex itself, but changes the on-disk / in-memory
  aggregation shape (utilization is now split across N shards that must be
  summed for file selection) and does not address the lock-ordering convoy
  either. Bigger, riskier change for a problem the ordering fix already
  targets.
- The task brief explicitly flags per-txn-flushed-at-commit as the natural
  fit given the counting sites are already per-txn, and asks it to be
  considered first. It fits, and the code already has 90% of the
  "local tracker" for free (`write_locks`), so this is also the smallest
  correct change — ladder rung reached, stop climbing.

## Merge protocol / crash-safety

- The batch is built and merged synchronously inside `commit_append_phase`,
  after the WAL commit entry is duraudibly appended (LSN assigned) but
  before `commit_with_durability` returns to the caller. If the process
  dies between "write locks released" and "batch merged", the batch is
  lost — the cleaner simply never learns those LSNs are obsolete
  (under-counting). That is the same safe direction the existing
  best-effort code already accepts (a `debug_assert`-guarded exact/dedup
  path elsewhere in this file explicitly documents "double-count only
  makes a file MORE cleanable, never deletes live data" as the safe
  bias) — under-counting is safe, over-counting is not, and this design
  never over-counts because the batch is exactly the same
  `WriteLockInfo`-derived list `count_obsolete_abort_lsns` already builds
  today; only *when* and *how many lock acquisitions* the merge costs
  changes, not *what* is counted or *whether* it can double-count.
- A txn that ABORTS never calls `count_obsolete_abort_lsns` at all (only
  the commit path does) — no batch is built, nothing to lose.
- No data can be double-merged: the batch is built once from
  `self.write_locks`, which is cleared by the unlock loop, and the merge
  call happens exactly once per successful `commit_append_phase` call.

## Mechanism-vs-measurement judgement (asked explicitly by the requester)

**Keep the gates until measured**, despite the mechanism argument above
being real. Reasoning:

- The batching half of the fix (N lock acquisitions -> 1) provably reduces
  lock acquisitions only for multi-key transactions. `xbench`'s `ycsb_a`
  auto-commit path writes ONE record per commit, so batching alone changes
  nothing for the exact workload that measured the 33x regression — it still
  takes exactly one extra global-lock acquisition per commit, same as today.
- The lock-ordering fix (moving the merge after write-lock release) is the
  one with real mechanistic bite for the measured scenario, and I am
  confident in the mechanism (holding a hot per-record lock while blocking
  on an unrelated contended global lock is a well-known convoy generator).
  But "removes a plausible convoy generator" is a qualitative argument, not
  a bound — I cannot derive from first principles whether the residual cost
  (still one extra global-lock acquisition per commit, now uncontended by
  the per-record lock) lands within "a few percent" or "20-40%" of baseline.
  Both are physically plausible outcomes of the same fix, and only a real
  concurrent run distinguishes them.
- The original 33x number was produced on a real box under real contention;
  a fix aimed at a contention pathology has to be validated under contention
  to know if it worked, by the same logic that made the pathology only
  visible under contention in the first place.

So: implement the fix, argue the mechanism (above) in the PR/doc, but do NOT
flip the defaults to "on, no gate" on mechanism alone. Land it gated the same
way the current code is (env vars, or better: a `NoxuConfig` flag defaulting
to on-but-overridable) with the measured numbers filled in once available on
non-shared hardware. If local (loaded-box) measurements land within a
plausible range of the target and don't regress further, that is supporting
evidence to lean on removing the gate, but a loaded shared box cannot by
itself supply the "few percent of default" confirmation the task's
Definition of Done requires (see EC2_BRIEF status: box no longer reachable in
this session; recorded as a limitation, not silently ignored).

## Implementation plan (commit each step separately)

1. `UtilizationTracker::count_obsolete_batch` (this file/note) — DONE (this commit).
2. `UtilizationTracker` batched merge method (single lock acquisition point
   is the CALLER's job — the tracker's own methods are already `&mut self`,
   no internal locking; the batching happens at the
   `UtilizationTrackerObserver` layer, which owns the `Arc<Mutex<...>>`).
3. `LogWriteObserver::count_obsolete_batch` trait method + default impl
   that falls back to N `count_obsolete` calls (so no other implementors
   break), plus the real batched impl in `UtilizationTrackerObserver`.
4. `LogManager::count_obsolete_commit_lsns` calls the batched observer
   method once instead of looping `count_obsolete`.
5. `Txn::count_obsolete_abort_lsns` / `commit_append_phase`: reorder so the
   list is built before unlock (unchanged) but the merge call happens after
   `self.write_locks.clear()`, for both the explicit-txn and auto-commit
   arms.
6. Remove `NOXU_COUNT_AUTOCOMMIT_OBSOLETE` / `NOXU_COUNT_TXN_OBSOLETE` gates:
   counting on unconditionally in both arms; delete the env-var checks.
7. Update `utilization_obsolete_counting_test.rs` (remove the env-var setup
   helper, tests exercise the always-on path).
8. Add a merge-protocol test: accumulate N obsolete LSNs across a multi-key
   txn, assert the batched merge produces identical tracker state to N
   sequential single-LSN merges (oracle-style, mirrors the existing
   prop_tests.rs pattern).
9. Local A/B measurement with `noxu-xbench` (loaded box, `uptime` reported,
   >=3 interleaved runs, range not point).
10. Space re-measurement across >1 workload shape (du -sb + log counters —
    trustworthy despite CPU load).
11. Docs: Phase 4 section in `space-amplification-2026-09.md`, CHANGELOG
    `## [Unreleased]`.

## Test-suite status after fixes (2026, this session)

Box load average ~18-28 throughout (peer agents running `rustc`, memory-pressure
suites, and synthetic `yes` load concurrently -- confirmed via `ps`/`uptime`).
Correctness (pass/fail, not timing) verified clean:

- `noxu-txn --lib`: 292/292 pass.
- `noxu-db --lib` (transaction module): 43/43 pass, including the two tests
  that failed deterministically before the `SUPPRESS_OWN_END_FRAME` fix
  (`durability_controls_whether_the_commit_fsyncs`,
  `read_only_transactions_do_not_yet_reject_writes`).
- Full 4-crate suite (`noxu-cleaner`, `noxu-txn`, `noxu-dbi`, `noxu-db`)
  excluding known load-sensitive suites: 2376/2378 pass; the 2 remaining
  (`shared_cache_test::shared_cache_balances_one_budget_across_envs`,
  `sustained_load_test::test_cleaner_reduces_log_files_under_load`) are
  wall-clock->60s tests that hit nextest's 120s per-test timeout under
  this box's contention but PASS when given room (154s / 204s via plain
  `cargo test`, no timeout).
- `eviction_pressure_test` / `evictor_reclaim_multitree_test`: excluded from
  every run in this session -- these are the exact suites the user's
  first message named as being run concurrently by peer agents, and
  independently they collide on the shared box regardless of any change
  here.
- `read_fault_rss_leak_test::read_only_workload_rss_stays_bounded`: FAILS
  on a `noxu-evictor/src/slab.rs:129` debug_assert
  (`!self.index.contains_key(&id)`) -- confirmed present on the
  PRE-TASK baseline commit (634cf6f7) too, via a disposable worktree.
  Pre-existing, unrelated to this branch, not investigated further (out
  of scope for this task; flagged here so it is not re-discovered from
  scratch).
- `xa_adversarial_test::test_rapid_fire_10k_with_prepared_log`: failed once
  under full-suite contention with an fsync `EnvironmentFailure`; the
  test's own doc comment identifies it as "the single most frequent
  source of red-but-not-broken CI runs" under load. Passes standalone
  (32s) on this box at the same load average.
- `je_recovery_test::recovery_duplicates_with_deletion_survives_recovery`:
  failed once in a single nextest run (assertion count mismatch), but
  10/10 standalone runs and 3 full `noxu-db` suite runs (2 clean, 1 with
  unrelated timeouts) never reproduced it again. Treated as a one-off
  scheduling/tempdir artifact of heavy parallel test execution on a loaded
  box, not a reproducible regression -- flagged here in case it recurs on
  clean hardware, which would change this conclusion.

Net: every test that fails deterministically and repeatably is now fixed
(the double-end-frame bug). Every remaining failure in this session is
either pre-existing (confirmed against baseline) or resolves with wall-clock
room, consistent with the box being shared and heavily loaded throughout.
