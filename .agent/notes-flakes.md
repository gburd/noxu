# CI flake investigation notes (in progress)

Working branch: `fix/ci-flakes`. EC2 box (i4i.16xlarge, 64 vCPU) used for
load-based reproduction, directory `/data/flake`.

## Flake 1: `noxu-db::dst_crash_sweep dst_same_seed_reproduces_exactly`

### Status: characterising — not yet reproduced under this session's load

Reported to fail ~2/10 runs on main. Reproduction attempts so far:

- 20x isolated `cargo nextest run ... dst_same_seed_reproduces_exactly`: 0/20 failures.
- On EC2 (64 vCPU), with `yes > /dev/null` x60 background load (load avg ~60):
  40 sequential invocations of the compiled test binary directly
  (`dst_same_seed_reproduces_exactly --nocapture`): 0/40 failures.
  32-way parallel x 30 iterations each (960 total) of the same binary
  invocation under the same load: 0/960 failures (see
  `/tmp/flake1_parallel1.log` on the controlling host, not committed).

So the flake has NOT reproduced yet purely by re-running the
`dst_same_seed_reproduces_exactly` test in isolation, even under heavy
external CPU load. This matches the task brief's observation that "it passes
in isolation more often than not" — the interesting case is likely
cross-test interference **within the same nextest test-binary process**,
because nextest/cargo-test run all tests in one binary as threads in one
process, and `noxu_log::faultdisk` is **process-global** state
(`static ACTIVE: AtomicBool`, `static CONTROLLER: Mutex<Option<FaultController>>`,
`static WRITE_COUNT: AtomicU64`, all in `crates/noxu-log/src/faultdisk.rs`).

### Leading hypothesis (not yet confirmed)

`dst_crash_sweep.rs` has 4 tests in one binary:
- `dst_crash_sweep_fast` (sweeps seeds 0..120, ~40s, NOT `#[ignore]`)
- `dst_same_seed_reproduces_exactly` (uses `find_torn_write_seed()`, currently
  resolves to seed 1 per the observed "torn write" log line)
- `oracle_catches_violations` (fast, no faultdisk)
- `long_sweep` (`#[ignore]`d, so excluded from normal CI)

All of these run **in the crash_worker subprocess**, not in the test
process itself — `faultdisk::install_seed` is called inside
`crash_worker.rs`'s `main()`, itself a fresh process per `run_one_seed`/
determinism-test invocation. So the process-global state in `faultdisk` is
NOT actually shared between the sweep test and the determinism test at the
Rust level — each `crash_worker` invocation is a brand new OS process with
its own fresh statics. This weakens (but doesn't eliminate — need to check
for host-level shared state, e.g. filesystem page cache reuse across
TempDirs, or PID-reuse-based nondeterminism) the "process-global fault
state leaks across tests" hypothesis. Re-examine before pursuing further.

### Other candidates identified in the engine but not yet confirmed as root cause

1. **Background daemons run inside `crash_worker`** during the workload:
   `noxu-evictor` (5ms poll), `noxu-cleaner` (do_clean immediately on
   thread start, no initial sleep — asymmetric vs. checkpointer/
   in-compressor which sleep first), `noxu-checkpointer`,
   `noxu-in-compressor`, `noxu-log-flusher`. Any of these that call
   `posio::write_all_at`/`sync_data` while `faultdisk` is active will
   consume slots from the shared `WRITE_COUNT` counter and can make the
   *n*th write (the seed's `target_write` index) correspond to a
   *different logical write* across two runs, if daemon-triggered writes
   race nondeterministically with the foreground workload's writes. This
   is the single most plausible root cause: `FaultController::target_write`
   is chosen by **write index**, not by any semantic marker, so if two runs
   of the same seed have their background daemons fire a different number
   of times before the workload finishes (a real scheduling race — 5ms
   evictor poll, cleaner asymmetric immediate-start, checkpointer time/byte
   trigger), the *same* `target_write` index hits a *different* write in
   the two runs, breaking byte-for-byte determinism. Needs confirmation:
   instrument `on_write` to log `(idx, kind_of_write, caller)` and diff two
   runs of seed 1 under load.
2. Thread-hash-based locker id (`noxu-txn/src/thread_locker.rs::get_thread_id`,
   `DefaultHasher` over `ThreadId`) is stable per-thread per-process but
   thread creation order / OS thread-id reuse could differ run-to-run;
   currently believed NOT to affect the recovered *key set* (only lock
   ownership bookkeeping), but not yet ruled out for recovery ordering.
3. HashMap/HashSet iteration order: `noxu-recovery`'s `AnalysisResult`,
   `RecoveryManager` structures use `hashbrown::HashMap`/`HashSet` (default
   `RandomState`? — needs check whether workspace pins a fixed hasher) for
   per-txn chains, dirty-IN maps, etc. If iteration order over these affects
   the ORDER of redo/undo application when two entries touch the same key,
   a torn write could recover different final bytes depending on hash seed
   (which IS randomized per-process by default in std/hashbrown unless
   explicitly seeded) even with the same seed driving the fault injection.
   **This is a strong independent candidate** and should be checked before
   the write-count one: grep confirms extensive `hashbrown::HashMap`/`HashSet`
   usage in `noxu-recovery/src/{recovery_manager,analysis_result,
   rollback_tracker,dirty_in_map}.rs` for exactly the structures recovery
   iterates over when reconstructing/redoing.

### Next steps if resumed
- Grep for `RandomState`/`with_hasher` to see if hashbrown maps in
  noxu-recovery use a fixed seed or the default (randomized) one — if
  randomized, that's the bug, and recovery iteration order must not be
  allowed to affect final state (it should already be LSN-ordered
  internally, so this may be a red herring if recovery always sorts by LSN
  before applying — verify with a targeted read of
  `recovery_manager.rs`'s redo/undo loops).
- Add temporary logging in `faultdisk::on_write`/`on_fsync` to print
  `(idx, target_write, kind)` to stderr in the crash_worker, run seed 1
  twice under EC2 load, diff.
- If confirmed as the write-index hypothesis: the fix is almost certainly
  to make `FaultController` target a *logical* write (e.g. only count
  writes from the foreground workload's log entries, or tag writes with a
  caller-provided "kind" and have the controller only count kind==Data
  writes) rather than raw global write ordinal, OR to disable background
  daemons in the crash_worker (`run_evictor`/`run_cleaner`/
  `run_checkpointer` = false) since the DST harness's fault model is about
  the workload's own writes, not incidental daemon I/O racing on top.

## Flake 2: `noxu-xa::xa_adversarial_test test_rapid_fire_10k_with_prepared_log`

### Status: not yet started (time budget spent on flake 1 characterisation)

Per the task brief: known to time out at nextest's 120s cap in debug,
passes in release (~212s for the whole suite). Not yet run in this session.
Plan when resumed: run under `timeout 900 cargo test -p noxu-xa --test
xa_adversarial_test test_rapid_fire_10k_with_prepared_log` in debug and
measure. If it completes in, say, 300-600s, that's evidence for "genuinely
slow in debug" (option a) and the fix is to cut the iteration count for the
non-release run or gate it `#[ignore]` with a documented `--release`
invocation. If it hangs past a generous multiple of the release time
(~5-10x, i.e. >20 min) with no progress, attach gdb per the brief's
instructions before concluding livelock.

## Time/tool-call accounting at this checkpoint

~120+ tool calls spent, mostly on: reading daemon_manager.rs,
environment_impl.rs, faultdisk.rs, cleaner.rs, checkpointer.rs to map every
background-thread write path that could touch the faultdisk write counter;
960 reproduction attempts of flake 1 in isolation (0 failures) plus 40
sequential (0 failures) — all under heavy synthetic EC2 load. No repro yet.
Flake 2 not started.


---

# RESOLUTION (maintainer, continuing from the stopped agent)

## Flake 2 (`xa_adversarial_test::test_rapid_fire_10k_with_prepared_log`) — FIXED

Measured rather than assumed: in isolation with a generous budget it passes in
47.8 s (so NOT deadlocked), but under suite parallelism it is flagged SLOW and
exceeds nextest's 120 s cap once the whole workspace competes for CPU. Genuine
slowness, not a hang.

Fixed by scaling the cycle count with the build profile (2,000 debug / 10,000
release). The property under test is "resolved prepared branches do not
accumulate", which holds at any sufficiently large count; 10,000 was arbitrary.
Debug 47.8 s -> 10.8 s; whole noxu-xa suite 68 s -> 32.5 s with no SLOW flag;
release still runs the full 10,000.

Deliberately NOT fixed by raising the global timeout (weakens the cap for every
other test) or by `#[ignore]` (stops exercising the path in normal runs).

## Flake 1 (`dst_crash_sweep::dst_same_seed_reproduces_exactly`) — NOT REPRODUCED

Total reproduction attempts across this investigation: 960 parallel + 40
sequential (agent, under synthetic load avg ~60 on 64 vCPU), plus 12 whole-binary
runs and 3 full-workspace runs (maintainer). **Zero failures.** It did not fail
once, including in the full-workspace configuration that is closest to CI.

It is left as-is, unfixed and un-annotated, because there is nothing to act on:
the process-global `faultdisk` interference hypothesis is plausible and
documented above, but unconfirmed, and adding a retry or an `#[ignore]` to a test
that never failed in ~1,000 attempts would be pure superstition.

## What DID reproduce: two other tests, both load-induced

The full-workspace runs failed a *different* test each time, which is the real
signature here — CPU starvation under 6,300-test parallelism, not a specific
broken test.

- `noxu-rep::tcp_integration::test_channel_drop_on_receiver_side_is_detected_by_sender`
  — **FIXED.** Measured 1/20 failures in isolation. Root cause was a hard-coded
  timing assumption: 10 sends x 10 ms (~100 ms) to observe a broken pipe. How
  fast the kernel surfaces the peer's RST is not ours to control, and under
  contention the scheduler did not run the loop often enough inside that window.
  Re-bounded by a 10 s DEADLINE instead of an iteration count. 0/25 after.

- `noxu-xa::xa_adversarial_test::test_concurrent_prepared_log_stress` — 0/15 in
  isolation, so it only fails under full-workspace load. Same starvation class,
  left open and recorded here rather than guessed at.

## The generalisable lesson

Three of the four flakes examined were tests asserting that something happens
within a hard-coded number of short sleeps. That pattern is a latent flake on any
loaded machine. Prefer a generous deadline over an iteration count whenever the
thing being waited for is scheduled by the kernel or another thread.
