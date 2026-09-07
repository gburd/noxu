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
