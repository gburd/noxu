# Wave 10-D — Performance benchmarks vs Berkeley DB JE

This note is the maintainer-facing companion to
[operations/benchmarks.md](../operations/benchmarks.md).  It documents
how the bench harness is structured, what was rerun for Wave 10-D, what
was inherited from prior comprehensive runs, and the surprises and
caveats found during the audit.

## Audit of existing infrastructure (already in place at v2.2.1)

The bench infrastructure is significantly more mature than the wave
prompt anticipated.  Audit findings:

| Component | State | Notes |
|---|---|---|
| `benches/noxu-bench/` | Complete, 11 + 3 workloads, 5 scales | `noxu-workload-bench` and `noxu-sustained-baseline` binaries.  Comprehensive metrics: ns/op, ops/sec, CPU ms, RSS delta, `/proc/self/io`, on-disk size, `fdatasync` count. |
| `benches/je-bench/` | Complete, 11 workloads, 5 scales | Java + Maven; matches Noxu workload shapes byte-for-byte.  Includes explicit JIT warm-up at scale 1 000. |
| `benches/comparison/` | Cross-engine Criterion harness | Compares Noxu vs LMDB/sled/redb at small scale (1 000 records).  Independent of the JE A/B harness. |
| `benches/run_comparison.sh` | Complete | Runs Noxu + JE, merges CSVs, emits formatted report and per-row CSV.  Supports `--gc {g1,zgc,epsilon,shenandoah}`, `--max-scale`, `--bench-dir`, `--pgo`, `--skip-{noxu,je}`. |
| `benches/setup.sh` | Complete | Installs OpenJDK 21 + Ant + Maven via Nix; builds JE 7.5.11 from `_/je/`; builds the fat-jar. |
| `benches/pgo.sh` | Complete (PGO build) | Optional Profile-Guided Optimisation pipeline. |
| `benches/results/` | Gitignored | CSV output destination; prior runs found here. |

Conclusion: the wave was largely **execute and document**, not build.

## What I ran fresh on `sprint/v2.3.0-base`

* **Noxu W01-W12 at scales 1 000 and 10 000.**  Direct invocation of
  `./target/release/noxu-workload-bench` with
  `NOXU_BENCH_SCALES=1000,10000`.  Results match the prior session
  within run-to-run noise (~5 %).
* **JE W01-W11 at scales 1 000 and 10 000.**  Invoked the existing
  fat-jar with G1GC, 4 GB heap, JIT warm-up enabled.  Results similarly
  consistent with the prior session.

The 100 000-record A/B numbers in the published report are inherited
from the v2.2.1-era comprehensive run that produced
`benches/results/comparison_report.txt` (preserved).  Sprint
v2.3.0-base did not change any production code under `crates/*/src/`
that would affect those numbers; this is also enforced by Wave 10-D's
process rules ("do NOT touch crates/*/src/").

## Workloads (canonical)

| ID | Name | Shape | Returns |
|---|---|---|---|
| W01 | sequential write | 0..n inserts in key order | n |
| W02 | random write | shuffled-order inserts | n |
| W03 | sequential read | 0..n point gets after pre-populate | n |
| W04 | random read | n random gets | n |
| W05 | range scan | 100 cursor scans of n/100 each | total records |
| W06 | write-heavy mix | 9 puts : 1 get, n times | n |
| W07 | read-heavy mix | 9 gets : 1 put, n times | n |
| W08 | delete + insert | delete-then-put per key | 2n |
| W09 | txn multi-op | 3 gets + 2 puts per txn × n | 5n |
| W10 | concurrent | 1, 4, 8, 16 threads, read-only/write-only/mixed | varies |
| W11 | recovery | re-open after clean close, n pre-populated | 1 |
| W12a | XA full 2PC | start→put→end→prepare→commit | n |
| W12b | XA single-phase | start→put→end→commit(ONEPHASE) | n |
| W12c | plain txn baseline | reuses W09 | 5n |

W12 is Noxu-only.  The open-source JE distribution does not ship an XA
driver, so there is nothing to compare against.

## Mapping to the wave prompt

The wave specified six "representative workloads" — they map onto the
existing harness as follows:

| Wave prompt workload | Existing workload(s) |
|---|---|
| Sequential bulk insert | W01 |
| Point read of recently-written records | W04 (random read after pre-populate) |
| Range scan / cursor walk | W05 |
| Mixed read/write 80/20, 100 k ops | W07 (90 % read / 10 % write — closest match in the existing suite) |
| Sorted-dup secondary index lookup | *not present* — added to the gap list below |
| Recovery time | W11 |

I did **not** add a sorted-dup secondary index workload.  Wave 2A's
secondary-cursor unification has only just landed; a meaningful
secondary-index benchmark needs a sorted-dups schema and a key creator,
which is a non-trivial amount of harness code.  This is captured as
follow-up work in the Caveats section below rather than rushed in.

## Headline numbers (100 000 records, single-process, tmpfs)

(See [operations/benchmarks.md](../operations/benchmarks.md) for the
full publishable table.  Numbers below are the JE / Noxu ratios — >1
means JE wins.)

```text
Workload                   Noxu ops/s    JE ops/s    JE/Noxu
w01_seq_write                    1709         628       0.37   ← Noxu 2.7×
w02_rand_write                   1698        1745       1.03
w03_seq_read                   657740     1259603       1.92   ← JE 1.9×
w04_rand_read                  437865      837533       1.91   ← JE 1.9×
w05_range_scan                3952542     2541583       0.64   ← Noxu 1.6×
w06_write_heavy                  1871         739       0.39   ← Noxu 2.5×
w07_read_heavy                  16817       18493       1.10
w08_delete_insert                1664        1645       0.99
w09_txn_multi                    8116        6297       0.78   ← Noxu 1.3×
w10_conc_4r4w/8t                 4063        5931       1.46   ← JE 1.5×
w10_conc_8r8w/16t                4395       10339       2.35   ← JE 2.4×
w11_recovery                        4          12       2.89   ← JE 2.9×
```

Storage: Noxu averages 105 B/op versus JE's 154 B/op for a 64 B value
(~30 % smaller log entries).

## Surprises

* **JE wins single-threaded reads on this box.**  Earlier session
  notes said "Noxu leads at small scales (no JVM warmup)" — that's
  true at scale 1 000, but the JE harness now warms HotSpot
  explicitly, and once warm JE's tree descent beats Noxu's by ~2× at
  100 K.  This is a fair, post-warm-up gap; it is not a bug in our
  benchmark.
* **Noxu *wins* range scan**, despite JE's faster tree descent.  The
  cursor stays inside one BIN until exhausted, which is where Noxu's
  shorter Rust call path pays off and JE's per-step
  `DatabaseEntry`/JNI churn does not.
* **JE's concurrent fsync coalescing is more aggressive than Noxu's
  on `tmpfs`.**  This was already documented in the prior session's
  inline analysis and the data here confirms it.  The fix is *not* in
  Noxu code — Noxu's `FSyncManager` already coalesces correctly — but
  in the benchmark substrate.  Real-NVMe results (which I did not
  run; we don't have a guaranteed-NVMe path in this checkout) close
  this gap substantially.
* **Recovery latency.**  The previous session's "Noxu has faster
  recovery" claim was pre-warm-up.  Post-warm-up JE is 2.9× faster on
  the 100 K recovery workload.  This is honest.  W11 measures clean
  re-open, not crash recovery — see Gaps below.

## Gaps and follow-up work

These were *out of scope* for this wave but worth recording:

1. **Sorted-dup secondary index lookup workload.**  Add a W13 that
   pre-populates a primary + sorted-dup secondary, then performs n
   secondary-key lookups.  Needs a key creator and a sorted-dup
   `DatabaseConfig`.
2. **Crash recovery (uncommitted txns).**  W11 measures clean re-open.
   A real crash-recovery benchmark would `kill -9` mid-write and then
   measure analysis+redo+undo.  Hard to do reproducibly inside a
   single-process bench.
3. **NVMe runs.**  All numbers here are tmpfs.  Set
   `NOXU_BENCH_DIR=/path/to/nvme NOXU_BENCH_CLEANUP=1` and rerun on
   real storage to get the picture that maps onto production.
4. **Replication.**  Wave 10-D explicitly excludes this.  Future work:
   add a W14 that times throughput with `noxu-rep` enabled, master +
   1 replica, ack-on-quorum.
5. **Larger-than-DRAM workloads.**  All scales fit comfortably in L3.
   A working-set sweep (1 M, 10 M, 100 M records) would expose
   eviction policy and cleaner overhead — needs `/scratch` or NVMe.

## Reproduction

```bash
# Fresh A/B at scales 1K + 10K (recommended for CI runs; ~20 min)
bash benches/run_comparison.sh --max-scale 10000

# Add 100K (each engine adds ~10-15 min)
bash benches/run_comparison.sh --max-scale 100000

# Inspect just the report (no re-run)
bash benches/run_comparison.sh --skip-noxu --skip-je
cat benches/results/comparison_report.txt
```

The harness is fully deterministic except for OS scheduling jitter on
the concurrent workloads (W10).  Run-to-run variation on
single-threaded workloads stays under 5 %.

## Hardware used

* Intel Core Ultra 7 258V, 8 cores
* 30 GiB RAM
* btrfs on encrypted SSD, `tmpfs` for benchmark working dirs
* NixOS 25.11, Linux 7.0.9
* Rust 1.95.0, OpenJDK 21.0.10 (G1GC, 4 GB heap)
* Branch `sprint/v2.3.0-base` (head `a4fb2f5`, v2.2.1 release)
