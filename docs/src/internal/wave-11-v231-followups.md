# Wave 11 — v2.3.1 follow-ups

This note captures the three v2.3.1 follow-up sub-tasks dispatched
out of `docs/src/internal/post-v2.3.0-roadmap.md` and the bugs that
surfaced while landing them.  Each sub-task is one (or more) commits
on `fix/wave11-v231-followups`.

## 11-A — Wave 10-A continuation (dup-cursor JE TCK ports)

Goal: grow the master TCK TSV's PORTED-EQUIVALENT count by ≥5 with
ports of `com.sleepycat.je.dbi.DbCursorDuplicateTest` methods that
the original Wave 10-A agent did not finish.

**Outcome.** 6 PORTED-EQUIVALENT (above the ≥5 acceptance bar) +
2 IGNORED-PENDING-FIX rows added in
`docs/src/internal/je-tck-port-2026-05-enumeration-je.dbi.tsv`.
Implementation in `crates/noxu-db/tests/je_db_cursor_test.rs`:

| JE method                          | noxu test | Status |
|------------------------------------|-----------|--------|
| `testDuplicateCreationForward`     | `db_cursor_duplicate_test_duplicate_creation_forward`  | PORTED |
| `testDuplicateCreationBackwards`   | `db_cursor_duplicate_test_duplicate_creation_backwards` | PORTED |
| `testGetNextNoDup`                 | `db_cursor_duplicate_test_get_next_no_dup`             | PORTED |
| `testPutNoDupData2`                | `db_cursor_duplicate_test_put_no_dup_data2`            | PORTED |
| `testDuplicateReplacement`         | `db_cursor_duplicate_test_duplicate_replacement`       | PORTED |
| `testDuplicateDuplicates`          | `db_cursor_duplicate_test_duplicate_duplicates`        | PORTED |
| `testDuplicateCount`               | `db_cursor_duplicate_test_duplicate_count`             | IGNORED |
| `testGetNextDup`                   | `db_cursor_duplicate_test_get_next_dup`                | IGNORED |

### Real noxu bugs surfaced (routed to follow-up bug-fix wave)

Two real noxu bugs surfaced while authoring the ports and are
captured as `#[ignore = "noxu-bug: ..."]` in
`je_db_cursor_test.rs` with full TODOs.  Per Wave 11-A discipline
they are NOT fixed here.

1. **`Cursor::count()` over-counts past first dup of primary on
   multi-primary sorted-dup DBs.**  Empirically `count()` returns
   `DUP_N_PER_KEY + offset_within_primary` instead of
   `DUP_N_PER_KEY` (e.g. 5, 6, 7, 8, 9 for a 5-dup primary).  Root
   cause: the `backward + 1 + forward` formula in
   `noxu_dbi::CursorImpl::count()` double-counts the original
   position because the forward walk from the first dup
   re-traverses every dup including the original.

2. **`Get::Search` + `Get::NextDup` returns `NotFound` immediately
   on every primary except the lexicographically smallest, on
   multi-primary sorted-dup DBs.**  The single-primary case still
   works (covered by the existing
   `sorted_dup_test::test_dup_sorted_order`).  The boundary check
   that decides "we've stepped onto a different primary" fires
   incorrectly when the cursor is positioned via `Get::Search` on a
   non-first primary.

## 11-B — Sorted-dup secondary index benchmark workload (W13)

Goal: close Wave 10-D gap #1 — no benchmark exercises the sorted-dup
secondary index path landed in Wave 2A.

**Outcome.** Workload W13 added to both
`benches/noxu-bench/src/workloads.rs` (Rust, plumbed into the
per-scale loop in `main.rs`) and
`benches/je-bench/src/main/java/com/noxu/bench/JeBenchmark.java`
(Java counterpart).  W13 only runs at scales ≤ 10K because the
known sorted-dup cursor bugs (see below) make high-dup walks
unreliable.  Real-storage numbers are tabled in
`docs/src/operations/benchmarks.md`'s new W13 section.

### Workload shape

* Primary populated with N records (10-digit decimal keys, 64-byte
  value).
* Secondary opened with `with_sorted_duplicates(true)` and a
  `SecondaryKeyCreator` that buckets primaries by
  `bucket = primary_key as u32 % 100`, so each secondary key owns
  ~N/100 primaries.
* Read phase: `secondary.open_cursor(...).get_first(...)` then
  repeated `get_next(...)` until exhaustion or a `2 * N` safety
  cap.

The setup runs *outside* the timer; reported `ns/op` reflects the
cursor walk only.

### More noxu bugs surfaced

Authoring W13 surfaced two more sorted-dup cursor bugs in addition
to the two from 11-A.  Both are documented in detail at the top of
the W13 module in `benches/noxu-bench/src/workloads.rs` and again
in the W13 section of `docs/src/operations/benchmarks.md`.

3. **`SecondaryCursor::get_search_key` + `get_next_dup_full`** on a
   multi-bucket secondary triggers
   `SecondaryIntegrityException` after the first yield — the same
   class as bug #2 from 11-A, surfaced through the secondary
   layer.

4. **`SecondaryCursor::get_first` + repeated `get_next`** revisits
   primaries instead of advancing past the dup chain, eventually
   either yielding a stale primary key (causing
   `SecondaryIntegrityException`) or failing to terminate
   altogether.  The W13 walk caps the step count at `2 * N` to
   ensure the workload always terminates.

Per Wave 11-B discipline, these bugs are NOT fixed here.  W13's
safety cap means the workload still produces a number, and as the
bugs are fixed, the W13 row in the benchmark table will tighten.

## 11-C — Real-storage benchmark re-run

Goal: re-run W10 (concurrent) and W11 (recovery) on real NVMe to
surface FsyncManager group-commit coalescing that was invisible on
the Wave 10-D tmpfs run.

**Outcome.** Real-NVMe numbers landed in
`docs/src/operations/benchmarks.md`'s new "Real-storage W10 / W11
re-run" section.  Highlights:

* Single writer: 10 000 fsyncs (1 per write, no coalescing
  possible).
* Four writers: 6 219 fsyncs for 40 000 writes — FsyncManager
  coalesces ~6.4 writes per fsync.
* 8r+8w mixed: 2 631 fsyncs for 80 000 writes — ~30× coalescing
  factor.
* Group commit (4-way threshold, 5 ms interval) shaves ~3 % off
  wallclock and 218 fsyncs vs no-group-commit at 8 writers.
* Recovery: 218 ms to replay a 10K-record log on NVMe (vs ~5 ms on
  tmpfs).

The matching JE NVMe run is gated on `bash benches/setup.sh`
succeeding, which requires Maven plus internet access to download
the JE jar dependency tree.  This environment did not have that, so
the side-by-side report is left to a future wave; the reproducer
command is documented inline.

## Bug catalog summary

The four noxu sorted-dup cursor bugs surfaced across 11-A and 11-B
are all symptoms of incomplete multi-primary handling in
`noxu-dbi`'s sorted-dup cursor logic.  None of them are fixed in
this wave.  They share a common root-cause area (BIN-boundary /
dup-chain traversal in `noxu_dbi::CursorImpl`), and a dedicated
bug-fix wave should address them together rather than piecemeal.
The four `#[ignore]`'d / `Result`-tolerant tests are the regression
gate for that fix wave.

## Roadmap status

The relevant rows in `docs/src/internal/post-v2.3.0-roadmap.md`'s
tracker have been updated from `dispatched` to a one-line outcome
linking to this note.
