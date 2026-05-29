# Wave 11-T — Cross-Feature Critical Correctness Fixes

**Branch**: `fix/wave11-t-crossfeature`
**Target**: v3.0.0
**Date**: 2026-05-29
**Source**: Second-pass cross-feature audit (`audit-2026-05-2ndpass-crossfeature.md`)

## Scope

This wave fixes the 3 critical items plus the tightly-coupled high-value highs from
the 14-finding second-pass cross-feature audit.  Items X-2, X-4, X-7, X-8,
X-10, X-11, X-12 are deferred — they are noted at the bottom of this document.

---

## Items Fixed

### X-13 (High) — DB/Cursor check\_open bypasses env validity check

**Status**: Fully fixed.
**Files changed**: `noxu-dbi/src/environment_impl.rs`, `noxu-dbi/src/cursor_impl.rs`,
`noxu-db/src/database.rs`, `noxu-db/src/cursor.rs`

After the C-2 fsync-gate fix, `Database::check_open()` and
`CursorImpl::check_state()` did not check the environment validity flags.
Reads and cursor operations silently succeeded on an invalidated environment.

**Fix**: Changed `EnvironmentImpl::is_invalid` to `Arc<AtomicBool>` and exposed
`is_invalid_flag()`.  Both `Database::check_open()` (checking `env_invalid` and
`log_manager.io_invalid`) and `CursorImpl::check_state()` (checking
`env_invalid` and `lm.io_invalid`) now return `EnvironmentFailure` on a bad
environment.  Added `map_cursor_err()` in `cursor.rs` so `DbiError::EnvironmentFailure`
propagates as `NoxuError::EnvironmentFailure` rather than `OperationNotAllowed`.

**Tests**: `test_x13_io_invalid_blocks_db_get`,
`test_x13_env_invalid_blocks_cursor_get`

---

### X-15 (Critical) — Open-ended rollback interval not detected

**Status**: Fully fixed (was "unverified" in audit; now confirmed and corrected).
**Files changed**: `noxu-recovery/src/rollback_tracker.rs`

`is_in_rollback_period()` only checked completed rollback periods.  A
`RollbackStart` without a matching `RollbackEnd` (crash mid-rollback) stayed
in `pending_rollback_starts` and was never consulted, so entries in the
incomplete window were re-applied during redo — corrupting the post-crash state.

**Fix**: `is_in_rollback_period()` now also checks `pending_rollback_starts`
for entries with a valid `rollback_start_lsn`, treating `(matchpoint_lsn,
rollback_start_lsn)` as a live exclusion window.  Added `safe_matchpoint_lsn()`
and `incomplete_period_count()` helpers.

**Tests**: `test_x15_open_ended_rollback_period_is_detected`,
`test_x15_open_ended_period_becomes_complete_on_end`,
`test_x15_multiple_open_ended_periods`

---

### X-5 (Critical) — Cleaner checkpoint barrier completely disconnected

**Status**: Fully fixed.
**Files changed**: `noxu-cleaner/src/file_selector.rs`, `noxu-cleaner/src/cleaner.rs`,
`noxu-recovery/src/checkpointer.rs`, `noxu-dbi/src/environment_impl.rs`

The three-state deletion barrier (`cleaned → checkpointed → safe_to_delete`)
was fully implemented in `FileSelector` but **never called from outside
the cleaner**.  Files were deleted in the same cleaning pass before any
checkpoint, allowing recovery undo to read `None` from a deleted file and
delete the slot instead of restoring the before-image — silent data corruption.

**Fix**:

- `FileSelector::process_checkpoint_end()` now implements the two-checkpoint
  JE barrier: already-checkpointed files advance to `safe_to_delete`; newly
  cleaned files (snapshotted at checkpoint start) advance to `checkpointed`.
- `Cleaner::do_clean()` no longer pushes to `pending_deletions` directly.
  New `after_checkpoint(state)` and `delete_safe_files()` methods replace it.
- `Checkpointer` now holds an optional `Arc<Cleaner>` (via `with_cleaner()`),
  snapshots the cleaner state at checkpoint start, and calls
  `cleaner.after_checkpoint(&state)` on successful completion.
- `EnvironmentImpl::new()` wires the cleaner into the checkpointer.

**Breaking**: `CleanResult::files_deleted` now reports the count of files
deleted via `delete_safe_files()` (post-checkpoint), not immediate deletions.

**Tests**: `x5_file_not_safe_to_delete_before_checkpoint`,
`x5_file_cleaned_after_checkpoint_start_waits` (in `cleaner_test.rs`);
all 313 existing cleaner tests updated and passing.

---

### X-6 (High) — Cleaner migration writes no WAL LN entry

**Status**: Fully fixed.
**Files changed**: `noxu-cleaner/src/file_processor.rs`, `noxu-cleaner/Cargo.toml`

`SharedTreeLookup::migrate_ln_slot` and `RealTreeLookup::migrate_ln_slot` used
`get_end_of_log()` as a fake LSN and wrote no WAL entry.  After a crash before
the next checkpoint, recovery could not find the migrated data at its new
position (the BIN slot pointed to the old, possibly-deleted log file).

**Fix**: Added `write_migration_ln()` helper that writes a non-transactional
`UpdateLN` WAL entry and returns the allocated LSN.  Both migration paths call
it when a `LogManager` is wired; tests fall back to the old `log_lsn`.
Added `bytes` to `noxu-cleaner`'s runtime dependencies.

**Tests**: `test_x6_migration_writes_real_wal_entry`

---

### X-3 (Critical) — Recovered XA commit written with NULL\_VLSN

**Status**: Fully fixed.
**Files changed**: `noxu-dbi/src/replica_ack.rs`, `noxu-db/src/environment.rs`,
`noxu-rep/src/replicated_environment.rs`

In a replicated environment, `write_txn_end_for_recovered` wrote `TxnCommit`
with `NULL_VLSN`, making the resolved XA commit invisible to the VLSN tracker.
Replicas never learned about the recovered commit; their VLSN high-watermark
stalled.

**Fix**:

- Added `alloc_vlsn_for_recovered_commit(lsn: Lsn) -> u64` default method to
  `ReplicaAckCoordinator` (default returns 0 for non-replicated envs).
- `write_txn_end_for_recovered` now returns `Result<Lsn>`.
- `write_txn_commit_for_recovered` calls `coordinator.alloc_vlsn_for_recovered_commit(commit_lsn)`
  after the WAL write.
- `ReplicatedEnvironment` implements the method: on a master node it allocates
  `vlsn_index.get_latest_vlsn() + 1` and registers the commit LSN.

**Tests**: `test_x3_recovered_commit_calls_alloc_vlsn`

---

### X-1 (High) — VLSN index not truncated after replica rollback recovery

**Status**: Fully fixed.
**Files changed**: `noxu-recovery/src/rollback_tracker.rs`,
`noxu-recovery/src/recovery_info.rs`, `noxu-recovery/src/recovery_manager.rs`,
`noxu-dbi/src/environment_impl.rs`, `noxu-rep/src/replicated_environment.rs`

After a replica crashed mid-rollback and recovered, the VLSN index retained
entries for VLSNs that were rolled back, causing feeders to stream from the
wrong position or trigger erroneous network restores.

**Fix**: Added `safe_matchpoint_lsn()` to `RollbackTracker`.  `recover_all()`
populates `RecoveryInfo::rollback_matchpoint_lsn` with the lowest matchpoint
across completed rollback periods.  `EnvironmentImpl` stores it as
`recovery_rollback_matchpoint`.  `ReplicatedEnvironment::with_environment()`
reads it and calls `vlsn_index.truncate_after(safe_vlsn)` where `safe_vlsn` is
the latest VLSN at or before the matchpoint.

**Tests**: `test_x1_rollback_matchpoint_lsn_set`

---

### X-14 (High) — VLSN not rebuilt after recovery

**Status**: Fully fixed (combined with X-1).
**Files changed**: Same as X-1 above.

After a crash, the loaded `vlsn.idx` could be stale (ahead or behind the
recovered B-tree).  The VLSN index was never rebuilt from the recovered log.

**Fix**: `RecoveryManager::run_redo_all()` now collects `(vlsn, lsn)` pairs
from every applied redo entry that carries a non-zero VLSN, stored in
`RecoveryInfo::recovered_vlsns`.  `ReplicatedEnvironment::with_environment()`
re-registers all collected pairs into the VLSN index before applying the X-1
truncation, ensuring the index is consistent with the recovered B-tree.

**Tests**: `test_x14_recovered_vlsns_populated`

---

## Deferred Items (follow-up wave)

The following items from the second-pass audit are **not** fixed in this wave.
They are tracked for a follow-up pass.

| Item | Severity | Summary |
|------|----------|---------|
| X-2  | Medium | `vlsn.idx` persistence not tied to checkpoint boundaries |
| X-4  | High | Recovered XA branch TOCTOU in `xa_commit` resolution |
| X-7  | Medium | Cleaner uses primary tree for secondary LN liveness check |
| X-8  | Medium | Redundant empty BINDelta when evictor races checkpointer |
| X-10 | High | Secondary index abort undo has cross-cursor torn-state window |
| X-11 | High | `log_flush_no_sync_interval_ms` silently ignored |
| X-12 | High | Memory budget fractured across 3 independent pools |

---

## Test Summary

| Item | New Tests | Location |
|------|-----------|----------|
| X-13 | `test_x13_io_invalid_blocks_db_get`, `test_x13_env_invalid_blocks_cursor_get` | `noxu-db/src/database.rs` |
| X-15 | `test_x15_open_ended_rollback_period_is_detected`, `test_x15_open_ended_period_becomes_complete_on_end`, `test_x15_multiple_open_ended_periods` | `noxu-recovery/src/rollback_tracker.rs` |
| X-5  | `x5_file_not_safe_to_delete_before_checkpoint`, `x5_file_cleaned_after_checkpoint_start_waits` | `noxu-cleaner/tests/cleaner_test.rs` |
| X-6  | `test_x6_migration_writes_real_wal_entry` | `noxu-cleaner/src/cleaner.rs` |
| X-3  | `test_x3_recovered_commit_calls_alloc_vlsn` | `noxu-db/src/environment.rs` |
| X-1  | `test_x1_rollback_matchpoint_lsn_set` | `noxu-recovery/src/recovery_manager.rs` |
| X-14 | `test_x14_recovered_vlsns_populated` | `noxu-recovery/src/recovery_manager.rs` |

Total new tests: **11** (baseline was 5796 on `main`; branch has 5807).

---

## Breaking Changes

- `CleanResult::files_deleted` now counts only files deleted via the
  two-checkpoint barrier (`delete_safe_files()`), not immediate same-pass
  deletions.  Test code that expected immediate deletion must be updated.
  See `noxu-cleaner/src/cleaner.rs` for updated test patterns.
- `write_txn_end_for_recovered` now returns `Result<Lsn>` instead of
  `Result<()>` (internal API, not public).
- `ReplicaAckCoordinator` has a new default method
  `alloc_vlsn_for_recovered_commit`; no action required for existing impls.

See `docs/src/getting-started/migrating.md` for the migration guide entry.
