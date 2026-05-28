# Wave 11-F: Stateright spec coverage expansion

**Branch**: `fix/wave11-f-stateright-coverage`
**Base**: `sprint/v2.3.1-base` (built on v2.3.0).
**Status**: complete.

## Background

Wave 9-B (`docs/src/internal/wave-9-b-stateright-revalidation.md`)
re-validated five of the eleven Stateright models in `noxu-spec`
against the persistent-acceptor / persistent-VLSN-index / feeder-
spawning / dispatcher-mediated-restore work that landed in Wave
4-A.  The other six models were inspected and left unchanged with
a note in the Wave 9-B audit table.

Wave 11-F follows the
`docs/src/internal/post-v2.3.0-roadmap.md` Wave 11-F entry: every
protocol modelled in `noxu-spec` should have either an explicit
`VALIDATED-AS-OF` annotation in its module preamble, or an updated
model with passing tests.  Where the original model had a
documented limitation (a TODO, a coarse property, or a 1-state
proxy for what should be a 2-state invariant), this wave
strengthens the model rather than just stamping the date.

## Per-protocol report

| Module                  | Status                            | Action                                                                                                         |
|-------------------------|-----------------------------------|----------------------------------------------------------------------------------------------------------------|
| `btree_latching`        | validated unchanged               | Added `VALIDATED-AS-OF: v2.4.0` annotation. Production entry points (`Tree::insert`/`split_child`/etc.) match. |
| `wal_commit`            | strengthened                      | `FsyncedNeverDecreases` upgraded from 1-state termination check to true 2-state monotonicity invariant.        |
| `recovery_three_phase`  | strengthened                      | `IdempotentReplay` upgraded to a 2-state predicate using a `materialised_after_first_redo` snapshot.           |
| `lock_manager_deadlock` | validated unchanged               | Added `VALIDATED-AS-OF: v2.4.0` annotation. Compile-time `LockType` anchor remains the primary safety net.     |
| `cleaner_safety`        | strengthened                      | Added `LiveCheckHonoured` invariant catching live-check-bypass regressions.                                    |
| `cache_vs_cleaner`      | strengthened                      | Added `MigratedReflectsDisk` invariant catching migrate-without-snapshot regressions.                          |
| `xa_two_phase_commit`   | strengthened (closes preamble TODO) | Added `RecoveryConsistent` 2-state invariant.  Replaces the original "future work" TODO from the module preamble. |

The five protocols Wave 9-B already touched (`flexible_paxos`,
`vlsn_streaming`, `master_transfer`, `network_restore`,
`recovery_three_phase` — note Wave 9-B classified
`recovery_three_phase` as "validated unchanged" but Wave 11-F
re-opens it for the 2-state idempotency strengthening) are listed
in Wave 9-B's audit table and not duplicated here.

## Production-code bugs surfaced

**None.**  Every newly introduced or strengthened invariant is
satisfied on the post-v2.3.0 code under model.  The Wave 11-F
counterexample searches all terminate without discoveries.

## What each strengthening protects against

### `wal_commit::FsyncedNeverDecreases`

**Before**: `s.fsynced_lsn < s.next_lsn`.  This is a termination
check (the LSN allocator is monotonic, fsynced LSNs can never
exceed it).  A regression that *reduced* `fsynced_lsn` between
transitions — for example, a future change to group-commit that
reset the high-water mark on flush failure — would still satisfy
this property.

**After**: `s.fsynced_lsn >= s.previous_fsynced_lsn`, where
`previous_fsynced_lsn` is snapshotted at the head of every
`next_state`.  Now the property is a true 2-state monotonicity
invariant in Stateright's BFS world.

### `recovery_three_phase::IdempotentReplay`

**Before**: only checked that after a redo, every committed txn is
materialised.  A regression that, on a second redo, *un*-marked an
already-materialised slot for a non-committed txn would still
satisfy the property (the property only inspects committed slots).

**After**: snapshots the full `materialised` vector after the
first `Action::Redo` into `materialised_after_first_redo` and
asserts that after `Action::RedoAgain` the materialisation equals
the snapshot.  This is a true idempotency check across redo runs,
exercising the production scenario where recovery is interrupted
mid-way and re-runs from the head of the WAL.

### `cleaner_safety::LiveCheckHonoured`

**Before**: only `NoLiveDelete` (a deleted file has no live
readers).  A future model edit that bypassed the live-check
entirely — setting `file_deleted[f] = true` without first checking
`cleared_for_delete[f]` — would still satisfy `NoLiveDelete` in
states where no reader happened to acquire a reference.

**After**: explicit `LiveCheckHonoured` invariant: every deleted
file must have its `cleared_for_delete` bit cleared at the moment
of deletion.  This is a defensive invariant against future model
edits.

### `cache_vs_cleaner::MigratedReflectsDisk`

**Before**: `NoStaleMigration` only asserted
`migrated_version <= disk_version`.  A regression that committed a
migration referencing a stale snapshot (one taken before a
subsequent dirty/evict cycle) would not be caught if the resulting
version was numerically `<= disk_version`.

**After**: explicit `MigratedReflectsDisk`: when a migration is
committed, `migrated_version == cleaner_seen_version`.  Combined
with the existing `DirtyTheBin` action that nullifies stale
snapshots, this pins the snapshot → migrate handshake.

### `xa_two_phase_commit::RecoveryConsistent`

**Before**: the module preamble carried a TODO requesting a
2-state recovery-consistency predicate.  Without it, only per-
state safety invariants (`PreparedImpliesDecided`,
`NoMixedDecision`, `NoUnilateralCommit`) were checked.

**After**: snapshots the TM's pre-crash decision into
`tm_decision_before_crash` and asserts that the post-recovery
decision matches the pre-crash decision (when there was one), and
that recovery from a mid-Preparing crash never silently flips to
commit unless an RM had already committed (which under
`PreparedImpliesDecided` cannot happen before the TM decided).

## CI integration

Existing infrastructure suffices:

- `make spec` (in the root `Makefile`) runs
  `cargo test -p noxu-spec --release`.
- `.github/workflows/spec.yml` runs the same target on every push
  to `main` and every pull request.

Total release-mode runtime after the Wave 11-F additions is **~31
seconds** (up from ~28 s pre-Wave-11-F).  The increase is dominated
by the extra `previous_fsynced_lsn` field in `wal_commit::State`
(which doubles the LSN-tuple state space) and the
`materialised_after_first_redo` field in `recovery_three_phase`
(which adds one bool per transaction).  Well within the CI
workflow's default job timeout; no `make spec-quick` escape hatch
needed.

## Known divergences that remain

These are intentional and documented; not action items for this
wave:

1. **`master_transfer` does not model VLSN catch-up.**  Same as
   noted in Wave 9-B.
2. **`network_restore` does not model the `NeedsRestore` signal
   path.**  Same as noted in Wave 9-B.
3. **`flexible_paxos` runs at `MAX_TERM = 1`.**  Same as noted in
   Wave 9-B.
4. **`btree_latching` BIN capacity is 1.**  Bounding the split
   trigger at 1 gives full descent/split coverage in seconds; a
   higher capacity would re-run the same race shapes at much
   greater BFS cost.

## Commits

| Commit  | Subject                                                                         |
|---------|---------------------------------------------------------------------------------|
| e7bef33 | wave11-f(docs): placeholder for Stateright coverage expansion                   |
| 6e6cbae | feat(spec): annotate btree_latching as VALIDATED-AS-OF v2.4.0                   |
| b62fb75 | feat(spec): strengthen wal_commit FsyncedNeverDecreases to 2-state              |
| cf088d9 | feat(spec): strengthen recovery_three_phase IdempotentReplay to 2-state         |
| 08fdd3d | feat(spec): annotate lock_manager_deadlock as VALIDATED-AS-OF v2.4.0            |
| 6232a92 | feat(spec): add LiveCheckHonoured invariant to cleaner_safety                   |
| 81027b7 | feat(spec): add MigratedReflectsDisk invariant to cache_vs_cleaner              |
| ac17748 | feat(spec): add RecoveryConsistent 2-state invariant to xa_two_phase_commit     |

All commits are on branch `fix/wave11-f-stateright-coverage`,
based on `sprint/v2.3.1-base`.  None are breaking model API
changes (no `feat(spec)!`); the Wave 11-F additions only add
fields and properties.

## Final Stateright spec status (post-Wave-11-F)

| Module                  | Stamp                | Notes                                                            |
|-------------------------|----------------------|------------------------------------------------------------------|
| `btree_latching`        | VALIDATED-AS-OF v2.4.0 | Wave 9-B + Wave 11-F.  Variant pair (HandOverHand / DropParentEarly) holds. |
| `flexible_paxos`        | VALIDATED-AS-OF v2.0.0 (Wave 9-B) | Variant pair (PersistentAcceptor / EphemeralAcceptor).         |
| `wal_commit`            | VALIDATED-AS-OF v2.4.0 | Strengthened with 2-state monotonicity.                          |
| `recovery_three_phase`  | VALIDATED-AS-OF v2.4.0 | Strengthened with 2-state idempotency.                           |
| `lock_manager_deadlock` | VALIDATED-AS-OF v2.4.0 | Compile-time `LockType` anchor + production-driving tests.       |
| `vlsn_streaming`        | VALIDATED-AS-OF v2.0.0 (Wave 9-B) | Variant pair (PersistentVlsnIndex / EphemeralVlsnIndex).       |
| `master_transfer`       | VALIDATED-AS-OF v2.0.0 (Wave 9-B) | Adds `MasterHasFeeders`.                                       |
| `network_restore`       | VALIDATED-AS-OF v2.0.0 (Wave 9-B) | Adds post-restore `EnableStreamFeeder`.                        |
| `cleaner_safety`        | VALIDATED-AS-OF v2.4.0 | Adds `LiveCheckHonoured`.                                        |
| `cache_vs_cleaner`      | VALIDATED-AS-OF v2.4.0 | Adds `MigratedReflectsDisk`.                                     |
| `xa_two_phase_commit`   | VALIDATED-AS-OF v2.4.0 | Adds `RecoveryConsistent`; closes original module preamble TODO. |

Eleven of eleven specs covered by an explicit
`VALIDATED-AS-OF` stamp at either v2.0.0 (Wave 9-B) or v2.4.0
(Wave 11-F).
