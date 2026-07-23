# Write ceiling: the cleaner throttle (resolved 2026-07)

## Symptom

Noxu DB's write throughput was capped at ~7k ops/s on a 64-thread `tdb_write`
SYNC workload, invariant to the storage device and to fsync coalescing. An
earlier WriteQueue / fsync-coalescing effort and raising `max_leaders` above 1
both moved the number by nothing (~7k in every configuration).

## Diagnosis (gdb-proven)

Thread wait-state sampling during the 64-thread run: **58 of 69 threads were
parked in `hrtimer_nanosleep`**, only ~10 in `futex_wait`, and only ~2–3 on the
fsync path. The committers were not fsync-bound — they were *sleeping*. The
sleep was the cleaner write-path throttle in
`Transaction::commit_with_durability` (and the auto-commit path in
`Database::put`).

## Root cause

`CleanerThrottle::should_throttle_writer` gated on a fixed raw write **rate**:
it returned a 1–50 ms sleep whenever the EWMA log write rate exceeded
`HIGH_WRITE_THRESHOLD_BYTES_PER_SEC = 1,000,000` bytes/s (1 MB/s), scaled by the
overshoot factor. A 64-thread `tdb_write` runs at ~7 MB/s aggregate — 7× the
threshold — so every logged commit slept ~7 ms, pinning throughput at ~7k
ops/s on an NVMe capable of GB/s. 1 MB/s is absurd for such a device; the
throttle *was* the write ceiling.

The rate gate had no BDB-JE basis. The code comment claimed it implemented
`CleanerThrottle.getWriteDelay()`, but JE has no such method and no raw-rate
write throttle at all.

## JE-faithful mechanism

JE's write-path backpressure is `EnvironmentImpl.checkDiskLimitViolation()`
(`src/com/sleepycat/je/dbi/EnvironmentImpl.java:2616`), which returns/throws
based on `Cleaner.getDiskLimitViolation()`
(`src/com/sleepycat/je/cleaner/Cleaner.java:1162`) — a signal driven by the
cleaner's inability to reclaim obsolete log space (a genuine backlog), updated
by `Cleaner.manageDiskUsage()`. It is checked on the write path from
`FileProcessor.doClean` (`FileProcessor.java:359,786`),
`Checkpointer.checkpoint`, and `DirtyINMap.selectDirtyINsForCheckpoint`. When
the cleaner keeps up, JE does **not** throttle; when it genuinely cannot keep
up, JE *prohibits* writes (`DiskLimitException`). There is no bytes/sec rate
throttle anywhere in JE.

## Fix

`should_throttle_writer` now gates on the cleaner **backlog** — the count of
files queued for cleaning that the cleaner has not caught up on
(`FileSelector.to_be_cleaned`) — not on the raw write rate:

- The cleaner publishes its backlog into the throttle after each pass via
  `CleanerThrottle::set_backlog`, wired in `Cleaner::do_clean` and
  `Engine::clean_adaptive`.
- Below `BACKLOG_THROTTLE_THRESHOLD` (8 files) the write path is never
  throttled — a fresh insert workload with nothing to clean is not slowed.
- Above the threshold a graduated 1–50 ms sleep engages so writers slow to let
  the cleaner catch up and the log does not grow unboundedly.

This preserves the safety purpose of the backpressure (writers must not outrun
the cleaner) while removing the artificial slow-down that fired when the
cleaner was idle or keeping up. The raw-write-rate EWMA is retained but now
drives only the cleaner-*daemon* sleep interval and files-per-pass tuning, not
write-path backpressure.

## Tests

- `throttle::tests::test_no_backlog_no_throttle` — high write rate, zero
  backlog ⇒ no write-path throttle.
- `throttle::tests::test_write_rate_does_not_gate_write_throttle` — regression
  guard: EWMA rate above the old 1 MB/s gate with zero backlog ⇒ no sleep.
- `throttle::tests::test_backlog_over_threshold_throttles` /
  `test_backlog_delay_scales_and_clamps` — real backlog ⇒ backpressure engages
  and scales, clamped to `MAX_WRITE_DELAY_MS`.
- `cleaner_test::throttle_backlog_wiring_gates_write_path` — FileSelector →
  throttle → write-path signal end to end.

## Process note

This was the third reframing of the write ceiling by measurement:
(1) "LWL convoy" → (2) "missing WriteQueue / fsync coalescing" → (3) the actual
cause, a miscalibrated 1 MB/s cleaner throttle sleeping every committer. gdb
thread-state (58/69 in `hrtimer_nanosleep`) was the decisive signal. Lesson:
trace *where* threads actually block; do not assume the theorized bottleneck.
The WriteQueue re-check work remains correct (it removes redundant fsyncs when
committers genuinely overlap) but was never the ceiling lever; whether it
matters is to be re-measured at the new, higher post-fix write rate.
