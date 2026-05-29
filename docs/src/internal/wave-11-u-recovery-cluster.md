# Wave 11-U — Recovery / Checkpoint / Cleaner / VLSN Cross-Feature Fixes

**Branch**: `fix/wave11-u-recovery-cluster`  
**Target**: v3.0.0  
**Status**: Complete (X-8 ✓, X-2 ✓, X-7 ✓, C-6 partial ✓)

---

## Summary

Wave 11-U fixes the recovery/checkpoint/cleaner cluster identified in the
second-pass cross-feature audit
(`docs/src/internal/audit-2026-05-2ndpass-crossfeature.md`) plus completes
C-6 recovery two-pass parity scaffolded by Wave 11-R.

| Item | Severity | Status | Files |
|------|----------|--------|-------|
| X-8  | Medium   | ✓ Fixed | `noxu-recovery/src/checkpointer.rs` |
| X-2  | Medium   | ✓ Fixed | `noxu-rep/src/vlsn/persist.rs`, `replicated_environment.rs` |
| X-7  | Medium   | ✓ Fixed | `noxu-cleaner/src/file_processor.rs`, `cleaner.rs`, `noxu-dbi/src/database_impl.rs`, `environment_impl.rs` |
| C-6  | Medium   | ✓ Partial (see below) | `noxu-recovery/src/recovery_manager.rs`, `log_scanner.rs`, `analysis_result.rs`, `noxu-dbi/src/file_manager_scanner.rs` |

---

## X-8 — Evictor + Checkpointer Redundant Empty BINDelta

**Finding**: `flush_dirty_bins_internal` snapshotted dirty BINs under a tree
read lock.  Between snapshot and per-node write-lock acquisition, the evictor
could flush and clear a BIN (`dirty=false`, `dirty_count()=0`).  The old guard:

```rust
if total == 0 && !b.dirty { continue; }
```

skipped only empty-AND-clean nodes, not the evictor-raced case.  A BIN with
entries but zero dirty slots was re-logged as an empty BINDelta — wasting log
space and incorrectly advancing `last_delta_lsn`.

**Fix**: replaced the guard with the correct:

```rust
if !b.dirty && dirty == 0 { continue; }
```

This subsumes the old empty-node case.

**Test**: `checkpointer::tests::test_x8_no_redundant_bindelta_after_evictor_flush`
— simulates the race (calls `clear_dirty_after_full_log` on all BINs before
running `flush_dirty_bins_internal`) and asserts zero BINDelta and zero
full-BIN entries are written.

---

## X-2 — VLSN Index Persistence Not Tied to Checkpoint Boundaries

**Finding**: `vlsn.idx` was flushed periodically with no coordination with
the checkpointer.  After a crash the B-tree could recover to VLSN N while
`vlsn.idx` claimed VLSN M > N, causing a feedgap mismatch on the feeder.

**Fix**: added `flush_to_disk_capped(index, env_home, cap_lsn)` to
`crates/noxu-rep/src/vlsn/persist.rs`.  This function filters out any
VLSN-index entry whose WAL position (file_no, file_offset) > `cap_lsn`, then
writes only the covered portion.  `cap_lsn` is the last durable checkpoint
end LSN, obtained from the environment's checkpointer.

The periodic VLSN flush daemon in `start_vlsn_persistence_daemon` now calls
`flush_to_disk_capped` with the checkpointer's `get_last_checkpoint_end()`.
When `cap_lsn == NULL_LSN` (no checkpoint yet), the function is a no-op.

The final flush on shutdown also uses the capped variant.

**Tests**:

- `vlsn::persist::tests::test_x2_flush_capped_excludes_post_checkpoint_entries`
- `vlsn::persist::tests::test_x2_flush_capped_null_lsn_is_noop`

---

## X-7 — Cleaner Migration Does Not Distinguish Secondary Databases

**Finding**: `SharedTreeLookup` ignored `db_id` and always looked up keys in
the primary tree.  Secondary databases store `sec_key → pri_key`; a
`sec_key` does not exist in the primary tree, so every secondary LN was
classified as `MigrationOutcome::Obsolete` and silently dropped when the
cleaner processed a log file containing secondary entries.

**Fix**: multi-layer change:

1. **`noxu-dbi/src/database_impl.rs`**: changed `real_tree: Option<Tree>` to
   `real_tree: Option<Arc<RwLock<Tree>>>`.  Added `get_real_tree()` returning
   `Option<RwLockReadGuard<'_, Tree>>` (zero changes to cursor call sites via
   Deref coercion) and `get_real_tree_arc()` returning the Arc for sharing.

2. **`noxu-dbi/src/environment_impl.rs`**: added `db_trees_registry:
   Arc<Mutex<HashMap<i64, Arc<RwLock<Tree>>>>>`.  In `open_database_inner`,
   each new database's tree Arc is registered.  A clone of the registry is
   passed to the cleaner at construction via `with_tree_registry`.

3. **`noxu-cleaner/src/file_processor.rs`**: added `extra_trees:
   HashMap<i64, Arc<RwLock<Tree>>>` to `SharedTreeLookup`.
   `lookup_parent_bin` and `migrate_ln_slot` now call `resolve_tree(db_id)`
   to dispatch to the correct tree; unknown db_ids fall back to the primary.

4. **`noxu-cleaner/src/cleaner.rs`**: added `extra_trees:
   Arc<Mutex<HashMap<...>>>` (shared live registry), `with_tree_registry`
   builder, and `register_db_tree` method.  `process_single_file` snapshots
   the registry and passes it to `SharedTreeLookup::with_extra_trees`.

**Test**: `cleaner::tests::test_x7_secondary_ln_migrated_in_correct_tree`
— builds separate primary and secondary trees, wires them via `extra_trees`,
and asserts:

- Primary key found in primary tree.
- Secondary key found in secondary tree (not primary → not Obsolete).
- Without `extra_trees`, secondary key returns NotFound (pre-fix confirmation).

---

## C-6 — Recovery MapLN Two-Pass Needs txn_id in NameLN (Partial)

**Finding**: Wave 11-R added `run_mapping_tree_undo_pass()` but it was a
no-op because `NameLnRecord` lacked the `txn_id` field needed to identify
which NameLNs belong to aborted transactions.

**Implemented**:

1. **`noxu-recovery/src/log_scanner.rs`**: added `txn_id: Option<u64>` to
   `NameLnRecord`.  `None` = pre-C6 WAL or commit-time write → treated as
   committed (no undo).

2. **`noxu-dbi/src/file_manager_scanner.rs`**: populated `txn_id` from
   `LnLogEntry.txn_id.map(|id| id.unsigned_abs())` when parsing `NameLN` /
   `NameLNTxn` entries.

3. **`noxu-recovery/src/analysis_result.rs`**: added `recovered_db_txn_ids:
   HashMap<String, u64>` to accumulate txn_ids alongside the existing
   `recovered_db_names`.

4. **`noxu-recovery/src/recovery_manager.rs`**: updated analysis pass to
   populate `recovered_db_txn_ids`; updated `run_mapping_tree_undo_pass` to
   check `recovered_db_txn_ids` against `aborted_txns` — now functionally
   removes NameLN entries with aborted txn_ids.

**What remains (follow-up wave)**:

Noxu currently writes the NameLN WAL entry **at commit time** (not inside the
transaction).  So `recovered_db_txn_ids` is always empty for current WAL
files — there are no NameLN entries with txn_ids to undo.  The full
end-to-end fix requires:

1. Writing `NameLNTxn` inside the transaction (with the live txn_id) instead
   of deferring to commit.
2. Not writing a second NameLN at commit (since it was already written).
3. This interacts with the C-4 fix (committed-only visibility in
   `get_database_names()`): writing the WAL entry inside the txn is safe
   because `get_database_names()` reads from `name_map` (not the WAL).

**Tests**:

- `recovery_manager::tests::test_c6_mapping_tree_undo_removes_aborted_namelns`
  — unit test that populates `AnalysisResult` synthetically and verifies the
  undo predicate: aborted NameLN removed, committed and txn_id-less survive.
- `recovery_manager::tests::test_c6_aborted_db_creation_not_recovered`
  — `#[ignore]` end-to-end test that pins the intended post-fix behavior.
  Un-ignore when the write-path change is implemented.

---

## On-Disk Format Change

**X-7** changes `DatabaseImpl.real_tree` from `Option<Tree>` to
`Option<Arc<RwLock<Tree>>>`.  This is an in-memory structure change only —
no on-disk format is affected.

**C-6** adds `txn_id: Option<u64>` to `NameLnRecord` (in-memory recovery
struct only).  The on-disk `NameLN` / `NameLNTxn` WAL entry format is
**unchanged** — `txn_id` is read from the existing `LnLogEntry.txn_id` field
that was already present.  No log version bump is required.  Old logs without
a `txn_id` in the `LnLogEntry` parse correctly with `txn_id = None`.

---

## Gate Results

- `cargo fmt --all -- --check`: ✓
- `cargo clippy --workspace --all-targets -- -D warnings`: ✓
- `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps`: ✓
- `cargo test --workspace --no-fail-fast`: ✓ (all pass, 2 C-6 tests ignored)
- `make docs-check`: ✓
