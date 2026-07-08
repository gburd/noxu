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
2. Read its `checkpoint_start_lsn` and, for each open user database, its
   **per-database tree root LSN** (`per_db_roots`, the v2 `CheckpointEnd`
   trailer — see [on-disk format](on-disk-format.md)).
3. **Seed** each reconstructed tree from its recorded root LSN
   (`Tree::set_root_lsn`) without materialising the root: the first access
   lazily fetches the root from the log, and each descent then fetches the
   referenced pre-checkpoint child BIN on demand (`child_at_or_fetch`, the
   reference `fetchTarget`-in-recovery path). A `BINDelta` slot is
   reconstituted by walking its full delta chain back to the base full BIN
   and merging every delta by key.
4. Scan forward from `checkpoint_start_lsn`, reading `IN` and `BIN` entries
   logged during the last checkpoint interval into the dirty-IN map.

A database whose `CheckpointEnd` carries **no** root LSN (an old-format v1
checkpoint, or a database created after the last checkpoint) is left
unseeded; recovery then reconstructs it entirely from LN redo (the safe
full-redo fallback).

At the end of Phase 2, the seeded tree reflects all writes that had been
checkpointed, materialised lazily as the tree is walked.

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
   merging the delta slots and recomputing the key prefix.  (When a BIN is
   re-fetched from a checkpoint-seeded parent slot, the **entire** delta
   chain since the base full is followed and merged by key — see Phase 2.)

### LN-redo (streaming, JE `RecoveryManager.redoLNs`)

The analysis pass (Phase 2's forward scan) records **only lightweight
metadata** for LNs — the committed / aborted / prepared transaction sets, the
active-transaction set, per-database LN counts (used to pre-size BINs), and
obsolete tracking. It does **not** collect LN payloads. Redo then re-reads
the log:

1. Start from `first_active_lsn` (recorded in `CheckpointEnd`).
2. **Stream a second forward scan** to the end of the log, applying each
   eligible LN directly into its tree as it is read, then dropping the
   record. This is the reference `RecoveryManager.redoLNs` methodology (a
   `while (reader.readNextEntry())` loop that checks `eligibleForRedo` per
   entry and applies each LN as it reads) — recovery never holds all LN
   records at once, so peak redo memory is one LN plus the lazily-fetched
   tree rather than O(all records). Recovery stays single-threaded.
3. For committed transactions: **redo** their LN writes to the tree.
   `redo_ln` is idempotent: if the tree already holds an equal or newer
   LSN for the key, the write is skipped.
4. **`AfterCheckpointStart` redo gate.** For a database that was **seeded**
   from a checkpointed root in Phase 2, an LN logged before
   `checkpoint_start_lsn` is **skipped**: its record is guaranteed present in
   a pre-checkpoint BIN reachable from the seeded root (a clean-close
   checkpoint flushes every dirty BIN, so the checkpointed tree is a complete
   snapshot as of checkpoint time), and the covering BIN is materialised by
   lazy fetch rather than by replaying the LN.  This turns redo-on-open from
   O(records) LN replays into O(nodes) lazy fetches.

   For a database that was **not** seeded (no checkpoint, a crash with no
   durable `CheckpointEnd`, an old-format checkpoint with no per-DB roots, or
   recovery without a log manager) the gate stays **inactive** and every
   committed / non-transactional LN is replayed from the full scan range —
   the safe fallback.  `Environment::recovery_redo_counts()` reports
   `(lns_redone, lns_gated)` so callers can confirm which path ran.

Recovery therefore reads the log forward **twice** — once in analysis (to
gather metadata) and once in redo (to apply LNs) — plus the backward undo
scan. The log is immutable during recovery, so the second forward scan sees
the identical entries in identical LSN order; streaming redo reconstructs the
exact same tree the earlier collect-then-redo design produced. In-doubt (XA
prepared) transaction LNs, the minority subset the redo/undo phases need
materialised, are gathered by a targeted scan that runs only when a
transaction is prepared.

## Phase 4 — Undo

1. For uncommitted transactions: collect and **undo** in reverse order.

After Phase 4, the database is transaction-consistent: committed writes are
visible, uncommitted writes are not.

## Checkpoint Protocol

1. **`CheckpointStart`** — written to the log; captures the `DirtyINMap`
2. **Dirty node flush** — all dirty INs and BINs are written via
   `flush_dirty_bins()` and `flush_upper_ins_internal()`. As each child is
   logged, its new on-disk LSN is stamped into its parent IN's slot
   (reference `IN.updateEntry`) and the parent is marked dirty, bottom-up, so
   the logged root's child pointers are followable by recovery lazy fetch.
3. **Per-database roots** — each open database's post-flush tree root LSN
   (`Tree::get_root_lsn`) is collected into `CheckpointEnd.per_db_roots`.
4. **`CheckpointEnd`** — written with `per_db_roots`, `first_active_lsn`, and
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
