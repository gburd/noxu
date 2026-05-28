# Wave 11 Bug-Fix Wave — v2.3.2

**Status**: merged.  **Release**: v2.3.2.

Six regression tests landed in v2.3.1 (Wave 11-E and Wave 11-G) with
`#[ignore]` because they surface real Noxu deviations from the JE invariant.
This wave fixes all six and promotes them to full `#[test]`.

---

## Bug 1 — `AnalysisResult::record_active_txn` precondition gap

**Wave**: 11-E
**Crate**: `noxu-recovery`
**Test**: `prop_active_txn_after_terminal_resurrects_phantom_active`
**Commit**: `86e3664`

### Root cause

`record_active_txn` did not check whether the txn had already been recorded
as committed or aborted.  Calling it after `record_commit` re-inserted the
txn id into `active_txn_ids`, so `has_active_txns()` reported a phantom
active txn.  The undo phase would then attempt to undo a committed
transaction.

In production the analysis pass sees log entries in chronological order,
so `record_commit` always precedes any later re-encounter of the same txn
id.  The gap was only reachable via an out-of-order caller.

### Fix

Added an early-return guard at the top of `record_active_txn`: if
`committed_txns.contains_key(&txn_id) || aborted_txns.contains(&txn_id)`,
return immediately without touching `active_txn_ids`.

---

## Bug 2 — Transactional cursor on non-transactional database permitted

**Wave**: 11-G
**Crate**: `noxu-db`
**Test**: `database_txn_cursor_on_non_txn_db_rejected`
**Commit**: `90918c5`

### Root cause

`Database::open_cursor(Some(&txn), None)` had no validation that the
database was transactional.  JE throws `IllegalArgumentException`; Noxu
silently accepted the combination and treated the cursor as auto-commit.

### Fix

Added an early `IllegalArgument` guard in `Database::open_cursor`:
if `txn.is_some() && !self.config.transactional`, return
`IllegalArgument("cannot open a transactional cursor on a non-transactional database")`.

Four pre-existing tests (`cursor_with_txn_*`) and the secondary cursor
test in `cursor_test.rs`, plus unit tests in `noxu-collections`, were
using a non-transactional database config with txn cursors.  Updated to
`with_transactional(true)`.

---

## Bug 3 — `put_no_overwrite` on dup-DB checks (key, data) instead of key only

**Wave**: 11-G
**Crate**: `noxu-dbi`
**Tests**: `database_put_no_overwrite_in_dup_db_txn`,
`database_put_no_overwrite_in_dup_db_no_txn`
**Commit**: `e21effb`

### Root cause

`CursorImpl::put_dup()` handled `PutMode::NoDupData | PutMode::NoOverwrite`
in a single arm that checked the exact two-part `(key, data)` composite
key.  JE's invariant distinguishes them:

- `NoDupData`: check the exact `(key, data)` pair.
- `NoOverwrite`: check the **key only** — once any dup exists for a key,
  any further `putNoOverwrite` with that key must return `KEYEXIST`,
  regardless of the data value.

### Fix

Split the `PutMode::NoDupData | PutMode::NoOverwrite` arm into two separate
arms.  For `NoDupData`, keep the existing `tree.search(&two_part_key)` check.
For `NoOverwrite`, use `tree.first_entry_at_or_after_with_index(&lower_bound(key))`
and check `matches_key(found_key, key)` to detect any existing dup.

Restructured `put_dup()` so that `Current` and `Overwrite` return early
from inside the match, and the common insert code follows the match.

---

## Bug 4 — Database name registry not preserved across clean close+reopen

**Wave**: 11-G (also wave 10-A)
**Crates**: `noxu-recovery`, `noxu-dbi`
**Tests**: `environment_read_only_rejects_db_name_ops`,
`recovery_edge_test_non_txnal_db`
**Commit**: `d9bc4c1`

### Root cause

`EnvironmentImpl::open_database()` stored the `name → DatabaseId` mapping
only in the in-memory `name_map` HashMap.  On the next `Environment::open()`
the `name_map` was always empty; `open_database()` with `allow_create=false`
returned `DatabaseNotFound`, and `get_database_names()` returned an empty
list.  This affected both read-only reopens and non-transactional databases.

### Fix

Multi-part fix:

1. **WAL persistence**: when `open_database()` creates a new database,
   write a `LogEntryType::NameLN` entry.  Format: `LnLogEntry` with
   `key = db_name bytes`, `data = 8-byte LE db_id`.

2. **Recovery parsing**: added `NameLnRecord` to `noxu_recovery::LogEntry`.
   `FileManagerLogScanner::parse_payload()` handles `NameLN | NameLNTxn`.

3. **Analysis pass**: `run_analysis()` handles `LogEntry::NameLn` entries
   to build `recovered_db_names`.  Propagated to `RecoveryInfo`.

4. **Env init**: pre-populate `name_map` from `recovered_db_names`.
   Read-only envs run a separate read-only recovery scan.

5. **open_database reopen path**: use the recovered `db_id` when a name
   is already in `name_map` from recovery.

---

## Bug 5 — Checkpoint loses committed data on next reopen

**Wave**: 11-G
**Crate**: `noxu-recovery`
**Test**: `environment_checkpoint_after_commit_loses_data`
**Commit**: `81c1f42`

### Root cause

Two compounding issues:

**Issue A**: `Checkpointer::do_checkpoint()` wrote `NULL_LSN` as
`first_active_lsn` in `CkptEnd`.  Recovery's scan-start selection:

```
if first_active_lsn != NULL_LSN  →  scan from first_active_lsn
else if checkpoint_start_lsn != NULL_LSN  →  scan from checkpoint_start_lsn
```

With `first_active_lsn = NULL_LSN`, recovery scanned from
`checkpoint_start_lsn` (AFTER all the committed LN entries), skipping them.

**Issue B**: `eligible_for_redo()` required `lsn >= ckpt_start` for
committed LNs.  Even if the scan started from the beginning, pre-checkpoint
committed LNs would be skipped.

The underlying design assumption — that the checkpoint BIN flush captures
all committed data — does not hold because the checkpointer is wired to an
in-memory `primary_tree` separate from the actual per-database trees.

### Fix

**Fix A**: Write `Lsn::new(0, 0)` (beginning of log) as `first_active_lsn`
in `CkptEnd` instead of `NULL_LSN`.

**Fix B**: In `eligible_for_redo()`, remove the `after_ckpt_start`
requirement for committed transactional LNs.  Always redo a committed LN.
`redo_ln()` is idempotent.

---

## Bug 6 — Truncate not durable across clean close+reopen

**Wave**: 11-G
**Crate**: `noxu-dbi`
**Test**: `truncate_survives_clean_close_reopen`
**Commit**: `b947b34`

### Root cause

`EnvironmentImpl::truncate_database()` replaced the in-memory tree with an
empty tree but wrote no WAL record.  On the next reopen, recovery replayed
all the original committed `InsertLN` entries and rebuilt the pre-truncation
tree.

### Fix

Before replacing the tree, iterate all BIN entries via
`tree.rebuild_in_list()` and write a non-transactional `DeleteLN` entry to
the WAL for each key.  On recovery these deletes are replayed after the
original inserts (in LSN order), leaving an empty tree.

Added `EnvironmentImpl::log_delete_ln()` as a private helper.

---

## Gate results

```
cargo fmt --all -- --check              ✓  zero violations
cargo clippy --workspace … -D warnings ✓  zero warnings
RUSTDOCFLAGS=-D warnings cargo doc …   ✓  zero warnings
cargo test --workspace --no-fail-fast   ✓  5757 passed, 0 failed, 58 ignored
make docs-check                         ✓  typos=0, markdownlint=0, mdbook OK
```

The `noxu-rep::phi_detector_test::test_master_tracker_phi_mode` test is a
pre-existing timing-sensitive test unrelated to this wave.
