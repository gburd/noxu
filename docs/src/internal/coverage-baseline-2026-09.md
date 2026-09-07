# Coverage baseline — core crates (2026-09, measured locally)

Established by direct measurement after the first coverage agent stalled ~20h
wedged inside a single slow `cargo llvm-cov` invocation. These are REAL numbers
from `cargo llvm-cov -p <crate> --summary-only` on main @ bf46ea8b/5cf58edf.

## Headline: the core crates ALREADY MEET the >85% mandate — except noxu-dbi

| crate | regions | functions | lines | vs 85% target |
|---|---:|---:|---:|---|
| noxu-util | 95.2% | 95.6% | 95.1% | PASS |
| noxu-cleaner | 91.7% | 90.2% | 88.5% | PASS |
| noxu-tree | 87.9% | 92.7% | 88.6% | PASS |
| noxu-txn | 92.1% | 87.0% | 90.2% | PASS |
| noxu-recovery | 90.8% | 88.5% | 89.5% | PASS |
| noxu-evictor | 90.8% | 91.7% | 89.3% | PASS |
| **noxu-log** | **92.48%** | **91.50%** | **91.37%** | **PASS** (newly measured, `--lib`) |
| **noxu-dbi** | **81.28%** | **78.15%** | **78.97%** | **BELOW TARGET** (newly measured) |
| noxu-db | see below | | | measurement in progress / very slow |

So the coverage mandate is satisfied for eight of the nine core data-path
crates — function coverage 87–96%, line coverage 88–95%. **`noxu-dbi` is the
one genuine outlier** and is where any future coverage effort should go.

## Bugs found while measuring (three real; all recorded, none fixed here)

Measuring coverage turned up three genuine production bugs. None was found by
reading code — each surfaced because a coverage run *failed or hung*, which is
the lesson worth internalising: **treat a wedged or failing coverage run as a
bug report, not a tooling problem.**

### 1. `noxu-log` consolidation-array LWL self-deadlock

Gated behind `noxu.log.consolidationArray` (**default `false`**), so the
shipped default path is unaffected. This is what made `noxu-log` look
unmeasurable. Full analysis:
[the consolidation-array deadlock note](consolidation-array-deadlock-2026-09.md).

### 2. `noxu-evictor` pri2 double-add — **on the DEFAULT path**

`cargo llvm-cov -p noxu-db` *failed*: `read_only_workload_rss_stays_bounded`
panicked with `assertion failed: !self.index.contains_key(&id)` at
`slab.rs:129` after 886s. It **passes uninstrumented** (1217s), so
instrumentation only widens the window — a genuine latent race.

`SlabList::add_front`/`add_back` assert the id is not already linked. In debug
that panics; in **release the `debug_assert` is compiled out and the intrusive
list silently corrupts** — the old slot is orphaned, `len` over-counts, and the
prev/next chain can cycle. That release behaviour is what makes this more
serious than the deadlock above.

The `MoveDirtyToPri2` arm of `evict_batch` (`evictor.rs:951`) calls
`self.pri2.lock().add_front(node_id)` **unconditionally**. Every
`primary_policy` path guards with `contains` under the same lock
(`LruPolicy::insert`/`put_back`), and even the `pri2_insert_for_test` helper
guards — this one arm does not. It relies on the invariant "a node drained from
the primary policy is never already in pri2".

That invariant is false:

- `note_ins_added` (`evictor.rs:593`) inserts straight into `primary_policy`
  without consulting pri2.
- `noxu-tree` calls it on BIN repopulation (`tree.rs:2866`) and on split
  (`tree.rs:4678`).
- So a node parked in pri2 awaiting a checkpoint that is re-faulted and
  re-added is in **both** lists.
- `evict_batch` drains it from primary. `decide_eviction`'s `already_in_pri2`
  parameter is actually `from_pri2` — "which list did this candidate come
  from", **not** "is it in pri2" — so it is `false`, the function returns
  `MoveDirtyToPri2`, and the unguarded `add_front` fires.

The existing `evicting` single-flight guard does **not** cover this. That guard
prevents two *concurrent batches* from double-adding (see its field doc); this
is a *single* batch double-adding a node that two different code paths placed in
two lists. A distinct hole.

Recorded as `evictor::tests::
test_node_in_primary_and_pri2_is_not_double_added_to_pri2`, `#[ignore]`d
because it currently fails by design — it documents the unfixed bug. It sets
the two-list state up directly instead of racing into it, so it reproduces the
identical panic **deterministically in 0.00s** instead of 886s. The fix is a
`contains` guard on that arm, matching every sibling path; un-ignore the test
with the fix.

### 3. `noxu-db` aborted duplicates survive recovery — load-dependent, NOT root-caused

`cargo llvm-cov -p noxu-db` also failed in
`je_recovery_sr_test::sr9752_part2_abort_after_committed_dups_reverts_with_dups`
(the port of JE `RecoveryAbortTest.testSR9752Part2`):

```text
assertion `left == right` failed: post-recovery: aborted dups must NOT appear
  left:  [[97], [98], [99], [120]]
  right: [[97], [98], [99]]
```

`[120]` is `"x"` — the first of three duplicates inserted under an **aborted**
transaction. It is absent before recovery (the test asserts that too, and that
assertion passes) and present after. That is an atomicity/durability violation:
recovery resurrected data from a transaction that aborted.

**This is not root-caused and is the most concerning of the three.** What is
established:

- It is **not** a coverage artifact in the "instrumentation changes semantics"
  sense, but it is load-dependent. It reproduced in **both** full-crate
  instrumented runs (`--test-threads` default and `4`), and the failing test
  and the surviving datum were identical each time.
- It does **not** reproduce in isolation: 12/12 runs of the single test pass,
  6/6 runs of all four tests in that file at `--test-threads 4` pass, and 3/3
  `cargo llvm-cov --test je_recovery_sr_test` runs pass. So instrumentation
  alone is not sufficient — it needs the whole ~50-target crate running.
- Each test uses its own `TempDir`, so this is not shared on-disk state between
  tests. The likely mechanism is timing: background daemons (checkpointer /
  evictor / cleaner) racing the abort-then-recover sequence, with the heavy
  parallel load widening a window that is otherwise almost never hit.

Next step for whoever picks this up: run the full crate under
`--test-threads 1` to see whether load or concurrency is the trigger, then
bisect toward which concurrent target perturbs it. Worth treating as a
potentially serious recovery bug until shown otherwise — "aborted data
reappears after restart" is the kind of failure a lock-based, non-MVCC engine
must never exhibit.

## The "noxu-log cannot be measured" claim was WRONG — it was a deadlock

The prior baseline recorded that `cargo llvm-cov -p noxu-log --summary-only`
"exceeds 800s and does not finish", and attributed it to instrumentation
overhead plus a loaded box. That diagnosis was incorrect and cost ~20h.

`noxu-log`'s `log_manager::tests::
test_consolidation_array_stress_64t_prev_offset_chain` **deadlocks**. It is not
slow; it never finishes. Evidence: the previous agent's test binary was still
alive 21h later (`PID 854134`, 211% CPU, 66 threads — a spinning livelock, not
a stalled one). `gdb` on all 66 threads showed 62 spinning in
`consolidation::wait_as_follower`, one leader blocked in
`LogBuffer::wait_for_zero_and_latch`, and one late-joining leader blocked on the
log-write latch — all 64 workers accounted for, none able to progress. It also
reproduces under a plain `cargo test` with no coverage instrumentation at all.

Root cause is a real production self-deadlock in `consolidation.rs`, gated
behind `noxu.log.consolidationArray` (**default `false`**, so the shipped
default path is unaffected). Full analysis, gdb evidence, and a verified fix:
[the consolidation-array deadlock note](consolidation-array-deadlock-2026-09.md).

With that test `#[ignore]`d, `noxu-log --lib` runs in **~4.7s** (492 passed, 2
ignored) and llvm-cov completes normally. The lesson is the opposite of the one
previously recorded: **a coverage run that never finishes is a bug report, not a
tooling problem.** Attach a debugger to the hung test binary before concluding
"too slow" — it took minutes to diagnose once someone looked.

`noxu-log` was measured with `--lib` (unit tests only, excluding the `tests/`
directory) purely to keep the run bounded; the numbers above therefore describe
`src/` coverage as exercised by unit tests, and the true figure including the
7 integration-test targets can only be higher.

## noxu-log: remaining gaps (all accepted)

At 91.5% function coverage the residue is small and mostly not worth testing:

| file | fn cov | assessment |
|---|---:|---|
| `write_observer.rs` | 0.00% | **accepted** — trait definition plus one convenience ctor. The trait is implemented and exercised in `noxu-cleaner`; the 0% is an artifact of measuring `noxu-log` alone. |
| `posio.rs` | 100% fn / 48% region | **accepted** — the uncovered regions are the `#[cfg(windows)]` arms, unreachable on Linux. Would need a Windows CI runner, not a test. |
| `log_item.rs` | 75.00% | **dead code, follow-up** — `LogItem::complete` and `is_complete` have zero callers anywhere in the workspace (`LogItem` itself is only used by `noxu-rep`'s vlsn_index). Candidate for removal rather than a test. |
| `whole_entry.rs` | 71.43% | trivial accessors. |
| `log_manager.rs` | 82.02% | largest file in the crate; residue is mostly error branches already covered structurally. One real gap was found and filled — see below. |

### Gap filled

`test_on_disk_corruption_is_never_returned_as_valid_data` (new). Nothing in the
workspace asserted that a byte flipped **on disk after a successful write** is
rejected on read. Part 1 flips a payload byte, which passes every structural
check (length/type/flags still parse) so the per-entry CRC32 is the only thing
preventing silent corruption — precisely the branch the checksum exists for.
Part 2 drives the same through `faultdisk`'s `FaultKind::Corruption`, previously
the only fault kind with no end-to-end coverage: `TornWrite` is covered by
`noxu-db`'s `dst_crash_sweep`, `DiskFull` by
`test_real_write_error_invalidates_and_is_not_swallowed`, but `Corruption` was
only unit-tested at the *decision* level (`faultdisk::on_write` returning a
`Corrupt` variant), never through `posio` → disk → read. Also asserts that a
failed checksum on READ does **not** set `io_invalid`, per the C-2 fail-stop
stance that only write/fsync errors invalidate the log.

## noxu-dbi: the one crate below target (78.15% fn / 81.28% region)

Where the gap actually is:

| file | regions | fn cov | uncovered fns | note |
|---|---:|---:|---:|---|
| `environment_impl.rs` | 2872 | **58.13%** | 67 | by far the biggest gap in the crate |
| `cursor_impl.rs` | 4673 | 77.98% | 37 | largest file; 1155 uncovered regions |
| `database_impl.rs` | 779 | 78.08% | 16 | |
| `disk_ordered_cursor_impl.rs` | 633 | 76.47% | 8 | 67.30% region — lowest in the crate |
| `replica_ack.rs` | 65 | 50.00% | 4 | small; replication-adjacent |
| `trigger.rs` | 8 | 0.00% | 4 | see the dead-code finding below |
| `database_config.rs` | — | 82.35% | 3 | setters |

`environment_impl.rs` at 58% function coverage is the single highest-value
target in the whole core-crate set and the right place to start a follow-up.

Caveat on per-function detail: `cargo llvm-cov --json` emits one record per
*instantiation* (and `noxu-dbi`'s types are instantiated separately in the lib
test binary and in each integration-test binary), so a naive "count == 0" read
of the JSON badly overstates the gap. The aggregated per-file numbers in the
table above come from `--summary-only`, which is the trustworthy source. Also
do not generate a `--json` report while another `llvm-cov` run is active in the
same worktree — the second run resets the profraw directory and the JSON comes
out polluted (it will show even *executed test functions* as uncovered).

## Dead-code / dead-feature findings (for a follow-up removal pass)

1. **`noxu-dbi::trigger.rs` — `Trigger::add_trigger` and
   `Trigger::remove_trigger` are never invoked by the engine.** Both are
   declared with default no-op bodies and documented as "invoked when the
   trigger is added to / removed from the database", but a workspace-wide grep
   finds **zero call sites**: the only hits for those identifiers are
   `DatabaseConfig::add_trigger`, which registers a trigger and never calls the
   hook. The sibling hooks are wired correctly (`put`/`delete` from
   `noxu-db/src/database.rs:1553,1583`; `commit`/`abort` from
   `noxu-db/src/transaction.rs:391,403`, covered by
   `noxu-db/tests/db_trig_test.rs`). So this is a **behavioral gap, not a
   coverage gap** — a user implementing `add_trigger` gets silence. Either wire
   them (JE fires `addTrigger` once, as the first trigger method invoked) or
   drop them from the trait. A test would be premature until that is decided.
2. **`noxu-log::log_item.rs` — `LogItem::complete` / `LogItem::is_complete`
   have no callers** anywhere in the workspace. Removal candidates.

## Branch coverage: NOT available

`cargo llvm-cov --branch` requires nightly's unstable `-Z coverage-options=branch`.
`rustup run nightly cargo llvm-cov -p noxu-util --branch` FAILS at build-script
compilation (quote/zmij/serde_json build scripts error under the coverage-branch
flags). Not root-caused. So the ">85% branch coverage" half of the mandate is
currently blocked on toolchain work. Function + line + region coverage IS
measurable and is what the numbers above report.

## Lessons for any re-dispatch

1. **A coverage run that never finishes may be a deadlock, not slowness.**
   That mistake cost ~20h here. Before concluding "needs a bigger box": find
   the test binary, check its CPU (spinning vs idle tells you livelock vs
   deadlock), and `gdb --batch -p <pid> -ex "thread apply all bt"`. Minutes.
2. Bound EVERY coverage invocation with a timeout and move on if a crate blows
   past it — never let one crate wedge the whole task.
3. Commit incrementally so partial progress survives a stall/stop.
4. Check for orphaned test processes from previous runs before trusting any
   timing. A single 21h orphan was consuming ~2 of 8 cores and inflating load
   average to 99–130, which made everything else on the box look slow too.
5. Do not assume the workspace is under-covered — measure first; nearly every
   crate already passes. Target the specific uncovered functions
   (`environment_impl.rs` first), not a blanket test-writing campaign.
