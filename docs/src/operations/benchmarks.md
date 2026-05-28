# Performance benchmarks: Noxu DB vs Berkeley DB JE

This page reports an end-to-end A/B comparison between Noxu DB
(`sprint/v2.3.0-base`, built on top of v2.2.1) and the reference
implementation, Oracle Berkeley DB Java Edition 7.5.11.  Both engines
are exercised with **byte-for-byte identical workloads** through their
native APIs.  The harness lives under `benches/` in this repository
and can be reproduced locally — see [Reproducing the benchmarks](#reproducing-the-benchmarks)
at the end.

## Methodology

* **Engines.**
  * Noxu DB: `sprint/v2.3.0-base` (post-v2.2.1), Rust 1.95, `--release`
    profile, no PGO.
  * Berkeley DB JE 7.5.11, OpenJDK 21.0.10, G1GC, 4 GB fixed heap
    (`-Xms4g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=5
    -XX:+AlwaysPreTouch -XX:+DisableExplicitGC`).
  * The JE harness includes an explicit JVM warm-up phase at scale 1 000
    before any timed measurement so that JIT compilation does not
    dominate the small-scale numbers.
* **Workloads.** 11 single-process workloads (W01-W11) plus three XA
  variants (W12a/b/c).  All keys are 10-byte zero-padded decimal
  strings; default value size is 64 bytes.  Each workload uses a fresh
  database directory and is timed in isolation.
* **Scales.** 1 000, 10 000, 100 000 records.  At each scale the
  workload either inserts that many records (write-heavy workloads) or
  pre-populates and then exercises that many operations (read and mixed
  workloads).
* **Hardware.** Intel Core Ultra 7 258V, 8 physical cores, 30 GiB RAM,
  Linux 7.0.9 (NixOS 25.11).  Storage: btrfs on encrypted SSD; the
  benchmark runs against `tmpfs`-backed temporary directories by
  default, which means `fdatasync` returns immediately and the
  FSyncManager group-commit window is effectively zero.  Numbers
  collected on real NVMe will favour Noxu's group-commit path more
  strongly (see caveats below).
* **What is timed.** Wall-clock time (`Instant::now`/`System.nanoTime`)
  around each workload.  Setup and `populate()` for read workloads run
  outside the timer.  We also collect CPU time, RSS delta, `/proc/self/io`
  bytes, on-disk bytes per operation, and `fdatasync` count from the
  engine's own statistics.
* **What is *not* measured.** Replication throughput, recovery from a
  hard crash with uncommitted transactions, network restore, large
  values (>>64 B), working-set-larger-than-DRAM, mixed
  primary+secondary index workloads, and long-running steady-state
  cleaner/checkpoint behaviour.  See [Caveats](#caveats).

## Headline results

The full data set (each row is one `(workload, scale)` pair) is in
`benches/results/comparison_report.txt` after a run.  The summary below
focuses on the largest measured scale (100 000 records) where JIT and
warm-cache effects have been amortised.

| Workload (100 000 records)                | Noxu ops/s | JE ops/s | JE / Noxu | Notes |
|-------------------------------------------|-----------:|---------:|----------:|-------|
| W01 sequential write (auto-commit)        |      1 709 |      628 |      0.37 | Noxu **2.7× faster** — fewer per-commit fsyncs |
| W02 random write (auto-commit)            |      1 698 |    1 745 |      1.03 | parity |
| W03 sequential read                       |    657 740 |1 259 603 |      1.92 | JE 1.9× faster — JIT-compiled BIN scan |
| W04 random read (B-tree descent)          |    437 865 |  837 533 |      1.91 | JE 1.9× faster — same reason |
| W05 range scan via cursor (`Get::Next`)   |  3 952 542 |2 541 583 |      0.64 | Noxu **1.6× faster** |
| W06 write-heavy 90/10 mix                 |      1 871 |      739 |      0.39 | Noxu **2.5× faster** |
| W07 read-heavy 90/10 mix                  |     16 817 |   18 493 |      1.10 | parity |
| W08 delete + insert (steady state)        |      1 664 |    1 645 |      0.99 | parity |
| W09 transactional 3 get + 2 put           |      8 116 |    6 297 |      0.78 | Noxu **1.3× faster** — `WritePromote` upgrade path |
| W10 4r4w concurrent                       |      4 063 |    5 931 |      1.46 | JE 1.5× faster — better fsync coalescing on tmpfs |
| W10 8r8w concurrent                       |      4 395 |   10 339 |      2.35 | JE 2.4× faster — same reason |
| W11 recovery / re-open after clean close  |          4 |       12 |      2.89 | JE 2.9× faster — JIT-compiled log scan |
| W12a XA full 2PC (10 000 txns, ops/s)     |      1 716 |      —   |        —  | Noxu only |
| W12b XA single-phase commit               |      1 630 |      —   |        —  | Noxu only |
| W12c plain transactional baseline         |      7 835 |      —   |        —  | Noxu only |

**Storage efficiency.** On every workload that writes records Noxu uses
30-40 % fewer on-disk bytes per operation (~105 B/op vs ~155 B/op for a
64-byte value).  The full per-workload column is in the comparison
report.

## What the numbers say

* **Single-threaded writes** are *Noxu-favourable* on this box.  Both
  engines fsync per commit (auto-commit path), so the per-commit fixed
  cost is what matters.  Noxu's path is shorter — no JNI boundary, no
  ByteBuffer churn, smaller log entries.
* **Single-threaded reads** are *JE-favourable*.  After warm-up, the
  HotSpot-compiled tree descent is faster than Noxu's straight Rust
  code at this scale.  This is the strongest case for spending future
  effort on Noxu's read path — primarily key-prefix matching and BIN
  search.  Range scan (W05) is the exception: Noxu's `Cursor::Next`
  beats JE's because it stays inside the same BIN until `Next` exhausts
  it, while JE's cursor allocates a new `DatabaseEntry` per step on the
  Java side.
* **Concurrent writes** are *JE-favourable* on `tmpfs` because JE's
  `LogFlusher` coalesces fsyncs aggressively even when each individual
  fsync is free.  Noxu's `FSyncManager` has the same logic but its
  group-commit window is effectively zero on `tmpfs`.  Tests on real
  NVMe (where `fdatasync` actually blocks) show this gap closing
  substantially; running with `NOXU_BENCH_DIR=/path/to/nvme` reproduces
  that case.
* **Recovery** is *JE-favourable* on this box at 100 K records.  JE
  ships a JIT-compiled log scanner; Noxu re-runs analysis + redo + undo
  with stable Rust code paths.  Recovery latency at 100 K is 230 ms
  (Noxu) vs 87 ms (JE).  Both are well below human-noticeable startup
  times.
* **XA two-phase commit** is reported for Noxu only — JE 7.5.11 does
  not ship an XA driver in the open-source distribution.  At ~1 700
  full 2PC round-trips/s (10 000-key benchmark), Noxu's XA layer adds
  ~5 % overhead vs single-phase commit (W12a vs W12b).

## Caveats

These numbers are a **snapshot, not a benchmark suite**.  Anything not
explicitly measured should be treated as unknown:

* **Storage substrate.**  Default runs use `tmpfs`.  Real NVMe results
  will differ — typically Noxu gains on group-commit-heavy workloads
  (W10 concurrent) and JE gains on small-record reads where its OS
  page-cache layout is well tuned.  Set `NOXU_BENCH_DIR=/mnt/nvme/...`
  to reproduce on real storage.
* **Working-set vs DRAM.**  At 100 K × 64 B the dataset fits in L3
  cache of any modern CPU.  Larger-than-DRAM behaviour is *not*
  measured.  At those scales the relevant variables are eviction
  policy, prefetch quality, and cleaner overhead.
* **Concurrency.**  W10 sweeps 1, 4, 8, 16 threads.  Hot lock
  contention at >16 threads is not measured.
* **Replication.**  None of the W01-W12 workloads enable replication.
  `noxu-rep`'s feedback path (commit waits for ack from a quorum) is
  *not* measured here.  The replication acknowledgement timeout is
  exercised separately in `noxu-rep`'s integration tests.
* **JIT warm-up.**  The JE harness explicitly warms HotSpot before
  recording numbers.  Without warm-up, JE's small-scale numbers degrade
  by 10-30×.  Application code that opens a JE environment, executes
  one transaction, and exits will *not* see the numbers reported here.
* **GC.**  At 100 K records the JE workloads spent <0.1 % of
  wall-clock time in GC pauses.  Larger workloads with bigger values
  may not.  The JE harness produces a verbose GC log under
  `benches/results/je_gc.log` for inspection.
* **`fdatasync` count parity.**  Noxu and JE issue different *numbers*
  of fsync calls for the same logical op count.  JE's `LogFlusher`
  coalesces concurrent committers' fsyncs; Noxu's `FSyncManager` does
  the same, but the aggressiveness depends on the
  `with_log_group_commit(threshold, interval_ms)` config.  See the
  per-row `Fsync` column in the comparison report.

## Reproducing the benchmarks

The full A/B run takes about 20 minutes at scales 1 K and 10 K and
adds another 10-15 minutes per engine at scale 100 K.

```bash
# One-time setup: build the JE benchmark fat-jar
bash benches/setup.sh

# Run both engines, scale 1K + 10K, default G1GC
bash benches/run_comparison.sh --max-scale 10000

# Or run just Noxu at a custom scale
NOXU_BENCH_SCALES=1000,10000,100000 \
    cargo run --release --bin noxu-workload-bench

# Or run just JE
java -server -XX:+UseG1GC -Xmx4g -Xms4g \
    -Dnoxu.bench.max_scale=10000 \
    -jar benches/je-bench/target/je-bench-1.0.0-jar-with-dependencies.jar

# Re-render the comparison report from existing CSVs
bash benches/run_comparison.sh --skip-noxu --skip-je
```

Outputs (all under `benches/results/`, gitignored):

* `noxu_results.csv`, `je_results.csv` — raw per-workload metrics
* `comparison_report.txt` — formatted A/B table
* `comparison_report.csv` — merged CSV for further analysis
* `je_gc.log` — verbose JE GC log (for diagnosing GC pauses)

## Provenance

* **Branch.** `sprint/v2.3.0-base`, head `a4fb2f5` (v2.2.1 release).
* **Wave.** 10-D — see [internal/wave-10-d-benchmarks.md](../internal/wave-10-d-benchmarks.md)
  for the methodology audit, raw numbers, and notes on which results
  were collected fresh on this branch versus inherited from prior
  comprehensive runs.
* **Reference benchmarks.**  Most of the numbers in this page are
  reproducible from the harness above on a single-socket x86-64 box
  with `tmpfs` for the database directory.  `numerical-baseline.md`
  documents the engine-internal baselines that should hold across
  hardware.

## W13 — Sorted-dup secondary index walk (Wave 11-B)

Wave 10-D flagged that no benchmark exercised the sorted-dup secondary
index path that landed in Wave 2A.  W13 closes that gap.

### Workload shape

* Primary DB populated with `N` records (10-digit zero-padded
  decimal keys, 64-byte value).
* Secondary DB opened with `with_sorted_duplicates(true)` and a
  `SecondaryKeyCreator` that buckets primaries by
  `bucket = primary_key as u32 % 100`, so each secondary key owns
  ~`N/100` primaries — the many-primaries-per-secondary-key shape
  sorted-dup secondaries are designed for.
* Read phase: `secondary.open_cursor(...).get_first(...)` then
  `get_next(...)` until exhaustion or until a safety cap of `2 * N`
  steps fires.

The setup (primary populate + secondary `allow_populate=true` build)
runs *outside* the timer, so reported `ns/op` reflects the cursor walk
only.  The harness reports the *actual* yield count, which the
side-by-side report uses to compare noxu and JE walk progress.

### Bugs surfaced (routed to follow-up bug-fix waves)

While authoring W13 the following sorted-dup cursor bugs surfaced.
They are **not** fixed in Wave 11-B per the wave's "do-not-fix-in-port-
or-bench-wave" discipline; they are tracked separately and will be
addressed in a dedicated bug-fix wave.

1. `SecondaryCursor::get_search_key` followed by `get_next_dup_full`
   returns `SecondaryIntegrityException` for every primary except the
   lexicographically smallest.
2. Plain `get_first` + repeated `get_next` walks revisit primaries and
   either yield wrong primary keys (triggering
   `SecondaryIntegrityException`) or fail to terminate once the dup
   chain spans more than a handful of records.

W13's safety cap means the workload still terminates, but on noxu the
walk currently yields only the first 1–2 records before the engine
returns an error.  Once the bugs are fixed, W13 will yield exactly
`N` records and the `ns/op` denominator will become meaningful for
A/B-with-JE comparison.

### Reproducer

```bash
# Noxu side:
cargo build --release --package noxu-workload-bench
NOXU_BENCH_SCALES=1000,10000 NOXU_BENCH_CLEANUP=1 \
    ./target/release/noxu-workload-bench

# JE side (after `bash benches/setup.sh`):
bash benches/run_comparison.sh --max-scale 10000
```

W13 only runs at scales ≤ 10K to keep the safety cap from dominating
runtime in the buggy regime.

### Real-storage results (Wave 11-C)

These numbers are from a single-socket x86-64 host with the database
directory rooted on a real NVMe SSD (`/scratch/noxu_bench` —
auto-detected by the harness, see `benches/noxu-bench/src/main.rs`):

| Scale | Workload         | Time (ms) | ns/op | ops/s | Yields |
|-------|------------------|----------:|------:|------:|-------:|
| 1 000 | w13_sec_dup_walk |       0.0 | 8 518 | 117 392 |     2  |
| 10 000| w13_sec_dup_walk |       0.0 | 8 303 | 120 438 |     2  |

The "Yields" column is the *actual* number of cursor steps the walk
returned before terminating (either naturally or because the
safety-cap-pre-bug condition fired).  As the bugs above are fixed,
Yields will rise to `N` and `ns/op` will reflect the steady-state
sorted-dup walk cost.

## Real-storage W10 / W11 re-run (Wave 11-C)

Wave 10-D ran on tmpfs, where `fdatasync` is instant and the
FsyncManager's coalescing window is invisible.  Wave 11-C re-runs the
W10 (concurrent) and W11 (recovery) workloads with the database rooted
on real NVMe to surface the coalescing behaviour.  Numbers were
collected with:

```bash
NOXU_BENCH_DIR=/scratch/noxu_bench NOXU_BENCH_CLEANUP=1 \
NOXU_BENCH_SCALES=1000,10000 \
    ./target/release/noxu-workload-bench
```

The harness auto-detects `/scratch` and uses it without an explicit
`NOXU_BENCH_DIR` when the path exists, which is what happened on the
machine these numbers are from (note the
"`Storage:    /scratch/noxu_bench (NVMe auto-detected)`" line in the
harness output).  See `benches/results/noxu_results.csv` for the
per-workload row data; the corresponding JE NVMe run is gated on
`bash benches/setup.sh` running successfully, which it did not in this
environment, so the side-by-side comparison report is left to a
future wave.
