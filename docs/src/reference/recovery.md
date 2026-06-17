# Recovery Protocol

When an environment is opened, `RecoveryManager` in `noxu-recovery` performs
crash recovery to bring the database to a transaction-consistent state.

Single-database environments use 3-phase recovery (analysis → redo → undo).
Multi-database environments (`recover_all`) use 4 logical phases, adding a
catalog-consistency pass (C-6 mapping-tree undo) between analysis and data-LN redo.

## Phase 1 — Find End of Log

`LastFileReader` scans backward from the end of the last log file, validating
CRC32 checksums. The first entry that fails its checksum marks the true end
of the log. Any partially written entries are discarded.

## Phase 2 — Build Tree from Checkpoint

1. Scan backward to find the last `CheckpointEnd` entry.
2. Read its `root_lsn` and `checkpoint_start_lsn`.
3. Scan forward from `checkpoint_start_lsn`, reading `IN` and `BIN` entries
   logged during the last checkpoint interval into the dirty-IN map.

At the end of Phase 2, the tree reflects all writes that had been checkpointed.

## Phase 2b — Mapping-Tree Undo Pass (multi-DB only, v3.0.0+)

*This phase runs only in `recover_all()` (multi-database environments).
Single-database `recover()` has no catalog entries to process.*

After analysis, before data-LN redo, `run_mapping_tree_undo_pass()` removes
aborted `NameLNTxn` entries from the recovered database name registry.

**Why this matters**: `open_database(Some(&txn), name, ...)` logs a `NameLNTxn`
entry with `Provisional::Yes` inside the creating transaction. If that
transaction is rolled back (explicit abort or crash-before-commit), the database
name must not survive recovery. The mapping-tree undo pass removes any
`NameLNTxn` whose creating transaction ID is absent from `committed_txns`.

**Invariant**: after this pass, the database name registry contains only
databases whose creation was committed. No data-LN redo occurs for a database
whose creation was rolled back.

This is Noxu's simplified equivalent of JE's `buildTree()` phases A–D, which
walk a separate on-disk `_jeNameTree` B-tree. Noxu uses a `HashMap` for the
catalog, so the structural MapLN undo pass is replaced by a targeted name-map
fixup.

## Phase 3 — IN-redo then LN-redo

### IN-redo (JE `RecoveryManager.buildINs`)

1. Collect all `IN`/`BIN`/`BINDelta` entries logged in
   `[checkpointStartLsn, EOF)` from the dirty-IN map, deduped to the
   latest logged version per node.
2. Sort by level **descending** (root INs first) — mirrors JE's
   `readRootINs` / `readNonRootINs` two-pass ordering so a post-checkpoint
   split's new root wins over any sub-tree references.
3. Filter provisional INs (entries logged with `Provisional::Yes` or
   `Provisional::BeforeCkptEnd` that are not covered by a `CkptEnd` — JE
   `INFileReader.isProvisional()`).
4. Deserialise each non-provisional IN/BIN and splice into the in-memory tree
   using the three-case LSN currency check (JE
   `RecoveryManager.recoverChildIN`, `RecoveryManager.java` ~line 1412):
   - slot LSN == log LSN → noop (physical match, case 2)
   - slot LSN < log LSN → replace (tree older, case 3)
   - slot LSN > log LSN → skip (tree already holds newer version)
5. For root INs (`recover_root_bin` / `recover_root_upper_in`): install
   logged IN as root when tree is empty, or when `log_lsn > root_log_lsn`.
6. For `BINDelta` entries: reconstitute the full BIN by reading the base
   full BIN at `prev_full_lsn` (JE `BINDelta.reconstituteBIN`), then
   merging the delta slots via `BinStub::apply_delta` and recomputing the
   key prefix.

### LN-redo

1. Start from `first_active_lsn` (recorded in `CheckpointEnd`).
2. Scan forward to the end of the log.
3. For committed transactions: **redo** their LN writes to the tree.
   `redo_ln` is idempotent: if the tree already holds an equal or newer
   LSN for the key, the write is skipped.
4. Note: the `afterCheckpointStart` gate (JE DRIFT-2) is intentionally
   **not** enabled. Pre-checkpoint committed LNs are always re-applied
   because the IN-redo pass only covers BINs logged in the current
   checkpoint interval, not baseline BINs from prior checkpoints. Until a
   "load tree from checkpoint snapshot" path is implemented, the full
   LN scan range is required for correctness.

## Phase 4 — Undo

1. For uncommitted transactions: collect and **undo** in reverse order.

After Phase 4, the database is transaction-consistent: committed writes are
visible, uncommitted writes are not.

## Checkpoint Protocol

1. **`CheckpointStart`** — written to the log; captures the `DirtyINMap`
2. **Dirty node flush** — all dirty INs and BINs are written via
   `flush_dirty_bins()` and `flush_upper_ins_internal()`
3. **`CheckpointEnd`** — written with `root_lsn`, `first_active_lsn`, and
   the LSN of the `CheckpointStart`

Checkpoint interval is controlled by `checkpoint_bytes` (default 20 MiB) or
`checkpoint_interval_ms` (default 20 000 ms), whichever triggers first.

## VerifyCheckpointInterval

A background thread monitors the time since the last checkpoint. If the
configured maximum interval is exceeded, it triggers an immediate checkpoint.

## Interaction with Log Cleaning

A log file can only be deleted after a checkpoint has completed since the
file was last written. This invariant ensures recovery never needs a deleted
file. If it does, `NoxuError::LogFileNotFound` is returned and a backup
restore is required.
