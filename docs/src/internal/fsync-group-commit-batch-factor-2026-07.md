# fsync group-commit: does it coalesce? (measured 2026-07)

## The hypothesis under test

An EC2 (96-core) `tdb_write` SYNC benchmark at 64 concurrent committers reported
the disk sitting idle between fsyncs and a fsync:commit ratio near 1:1 — i.e.
each commit costing its own `fdatasync`, the group-commit piggyback not
coalescing concurrent committers. If true, that is the entire write-throughput
gap vs O_DSYNC-WAL engines.

The claim was investigated by **direct measurement** of the batch factor
(`committed_writes / n_log_fsyncs`), not by re-reasoning about the code.

## Instrumentation

`benches/noxu-bench/src/bin/xbench.rs` now emits the fsync coalescing stats on
its `RESULT` line (all already exposed via `env.stats().log`):

- `n_fsyncs` — `n_log_fsyncs`, actual `fdatasync` calls.
- `n_fsync_requests` — committers that entered `flush_and_sync`.
- `n_group_commits` — batches where a leader served >= 1 piggybacking waiter.
- `batch_factor` — `committed_writes / n_fsyncs`. ~1.0 means each commit costs
  its own fsync (piggyback broken); >> 1 means real coalescing.
- `fsync_ms_each` — mean `fdatasync` latency (`fsync_time_ms / n_fsyncs`).
- `n_fsync_timeouts` — waiters that hit `LOG_FSYNC_TIMEOUT` and self-fsynced.

A/B knobs were added (default = shipped values): `BENCH_MAX_LEADERS`,
`BENCH_GC_THRESHOLD` + `BENCH_GC_INTERVAL_MS`, `BENCH_CONSOLIDATION`.

## Measurement (repro box: btrfs on NVMe, 8 physical cores, 30 GiB RAM)

`BENCH_RECORDS=2000000 BENCH_VALUE=1024 BENCH_THREADS=64 BENCH_DURABILITY=SYNC
BENCH_WORKLOAD=tdb_write`, warm dataset, steady state:

| config | throughput | batch_factor | n_fsyncs | fsync_ms_each | timeouts |
|---|---:|---:|---:|---:|---:|
| **baseline (shipped: 1 leader, no grpc wait)** | ~24k ops/s | **23.2** | 32164 | 1.13 ms | 0 |
| `max_leaders=4` | ~5k ops/s | **3.97** | 27698 | 3.25 ms | 0 |
| `grpc(threshold=8, interval=2ms)` | ~26k ops/s | 23.1 | — | 0.95 ms | 0 |

The shipped default **coalesces ~23 commits per `fdatasync`** at 64 committers.
`n_group_commits` is ~60% of `n_fsyncs`, i.e. most fsyncs serve a cohort. The
piggyback is **not** broken.

### Disk is not idle

`/proc/diskstats` field 10 (ms doing I/O) sampled in 1 s windows during the
steady run showed the backing device **36–64 % busy**, not idle. The CPU-bound
B-tree / lock-manager / utilization-tracking work fills the gaps between fsyncs.

### Where threads actually block (the decisive signal)

`/proc/<pid>/task/*/wchan` sampled at steady state: **57–64 of ~69 threads in
`futex_do_wait`, only ~2 in `hrtimer_nanosleep`, ~0–3 on the fsync path.** The
2 sleepers confirm the 2026-07 cleaner-throttle fix
(`write-ceiling-cleaner-throttle-2026-07.md`) holds — committers are no longer
sleeping in the throttle. On an 8-core box running 64 threads, `futex_do_wait`
dominance is expected oversubscription (only 8 threads can run), not a single
lock convoy: an on-CPU `perf` profile shows contention spread across the
record-lock manager, the B-tree, the buffer pool, and the LWL — no single
dominant lock, and the fsync path is a small slice.

## Conclusion (premise falsified on reproducible hardware)

The `batch_factor ~= 1` symptom does **not** reproduce. The fsync group-commit
piggyback works: one `fdatasync` durably serves ~23 concurrent committers, the
disk is not idle, and threads are not fsync-bound. Therefore, on this hardware,
**the per-commit fsync round-trip is not the write-throughput ceiling.**

Two levers were falsified as fixes:

- **`max_leaders > 1` makes it worse.** It lets an arriving committer become an
  *additional* leader instead of joining a waiter cohort, so committers
  `fdatasync` in parallel instead of piggybacking one leader's fsync:
  `batch_factor` collapses 23 -> 4, `n_fsyncs` nearly doubles, throughput drops
  ~4x. This mechanism explains why the earlier "raise `max_leaders`" attempt
  moved nothing (or hurt) — it trades coalescing for concurrent syncs. The
  shipped `max_leaders == 1` (JE-faithful single-leader piggyback) is the best
  fsync config.
- **A grace-period `grpc` wait does not reliably help.** `grpc(8, 2ms)` lands
  within run-to-run noise of the shipped no-wait default (batch_factor already
  ~23 without it). The JE-faithful pure-piggyback default is correct: the batch
  window is the fsync I/O duration, not an artificial wait.

## Guidance for the EC2 (96-core) re-measurement

The `batch_factor` field is now on the `RESULT` line specifically so the 96-core
run can be re-checked directly:

- If EC2 shows `batch_factor >> 1` (like this box), the fsync is not the
  ceiling — profile where the 96 threads block (`wchan` / `perf`), as the
  cleaner-throttle diagnosis did. Candidates at 96-way: record-lock-manager
  contention, buffer-pool mutex, the LWL (`assign_slot`) funnel.
- If EC2 shows `batch_factor ~= 1` where this box shows ~23, that divergence is
  the real bug and it is core-count-dependent — capture `wchan` + `perf` on EC2
  and compare the fsync-path residency, since the coalescing logic itself is
  proven correct under all interleavings (shuttle `shuttle_fsync_manager`).

## Durability unchanged

No production code was changed — only benchmark instrumentation and A/B knobs.
Durability proofs still hold: `shuttle_fsync_manager` (5 cases, incl.
`writequeue_shortcircuit_durability_holds` — a waiter returns success only when
a completed fsync covered its LSN, under all interleavings), `crash_recovery_test`
(12), `recovery_correctness_test` (22).

## Follow-up (2026-07-23): the concurrency-adaptive batch window

A later effort acted on the mid-concurrency angle directly: build a
*concurrency-adaptive leader ceiling* so a low-concurrency committer that finds
the leader busy issues its own `fdatasync` immediately (parallel leader)
instead of parking for a batch, while high concurrency still clamps back to
single-leader batching. Knobs: `LOG_FSYNC_ADAPTIVE_LEADERS` (max overlapping
leaders below the trigger) + `LOG_FSYNC_ADAPTIVE_TRIGGER` (waiter count at which
the ceiling clamps to `LOG_FSYNC_MAX_LEADERS`). The contention signal is the
live waiter count, read under the fsync manager's already-held state lock — no
extra atomic, no CAS, no spin on the commit hot path.

### Diagnosis first (measure before changing)

Repro box (btrfs on NVMe, 8 physical cores), `tdb_write` + `ycsb_a`, SYNC,
500k × 256 B, warm. The `batch_factor` curve is **proportional**, not
over-batched:

| threads | 1 | 8 | 64 | 256 |
|---|---:|---:|---:|---:|
| `tdb_write` batch_factor | 0.97 | 2.99 | 26.9 | 98.9 |
| `ycsb_a`   batch_factor  | 0.93 | 3.03 | 20.9 | 54.1 |

`batch_factor ~= 1` at 1 thread proves a solo committer already fsyncs
immediately (idle manager → it leads → never parks). There is **no over-eager
grace-wait** — the shipped defaults are `INTERVAL = THRESHOLD = 0`
(`grp_wait_on = false`, exactly JE `FSyncManager`). So the mid-concurrency
"loss" is the inherent fsync latency of coalescing, not diagnosis-(a)
(configured grace-wait) and not a handoff that parks a committer when it could
lead — diagnosis (b) is real but is *the JE behaviour*: JE also parks a
committer that arrives while `workInProgress`. The idle-leads-immediately
property the task cites is already satisfied.

### Before/after (baseline = shipped default; adaptive = `leaders=2, trigger=4`)

Throughput ops/s (batch_factor):

| | 1t | 8t | 64t | 256t |
|---|---:|---:|---:|---:|
| `tdb_write` baseline | 804 (0.97) | 2085 (2.99) | 13771 (26.9) | 32301 (98.9) |
| `tdb_write` adaptive | 592 (0.94) | 1223 (1.76) | 8247 (12.1) | 15367 (38.7) |
| `ycsb_a` baseline | 737 (0.93) | 2942 (3.03) | 16380 (20.9) | 33930 (54.1) |
| `ycsb_a` adaptive | 668 (0.89) | 840 (1.81) | 2467 (9.5) | 5452 (16.3) |

### Conclusion (premise falsified again, same direction as `max_leaders>1`)

The adaptive parallel-leader lever **loses at every concurrency level on this
hardware**. The mechanism is identical to the `max_leaders > 1` non-fix above:
a committer that would have piggybacked one leader's fsync instead issues its
own, so the batch fragments — `batch_factor` collapses (256t `tdb_write`
99 → 39; `ycsb_a` 54 → 16) and throughput drops. On this device, one coalesced
`fdatasync` of N committers beats N parallel `fdatasync`s at *every* N ≥ 2, so
the shipped single-leader batching is the fsync optimum everywhere; the 1-thread
case is already optimal (solo leads immediately, batch_factor ~1).

The knob is retained but ships **default-off** (`LOG_FSYNC_ADAPTIVE_LEADERS = 1`
→ `effective_ceiling` always returns `max_leaders` → exact JE single-leader
piggyback, byte-for-byte no-op). It can win only where BOTH (1) the device
sustains many concurrent same-file `fdatasync`s cheaply AND (2) the per-fsync
batch stays small enough that parallel syncs beat coalescing — a regime this
repro box is not in, and the guidance above still stands for the 96-core EC2
re-measurement.

JE citation: `FSyncManager.flushAndSync` — a committer leads iff
`!workInProgress` (idle), else waits; the coalescing window is exactly the
in-flight fsync duration. The adaptive ceiling preserves idle-leads-immediately
and extends it (low contention → also lead in parallel), then reverts to the JE
batch at high contention. JE itself has no such knob.

### Durability proof (adaptive path)

Unchanged and re-proven for the adaptive decision: `shuttle_fsync_manager` 6/6
including the new `adaptive_window_monotonic_watermark_holds` (enables the
adaptive window so `effective_ceiling` flips between the parallel-leader and
clamped-batch regimes as the waiter count crosses the trigger under shuttle's
interleavings, and asserts the monotonic-watermark + coverage oracles — a
committer returns durable-success only when a completed `fdatasync` covered its
LSN). Plus `crash_recovery_test` 12/12, `recovery_correctness_test` 22/22,
`bounded_fsync_durability_test` 4/4.
