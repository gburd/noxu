# Recovery Protocol

When an environment is opened, `RecoveryManager` in `noxu-recovery` performs
three-phase crash recovery to bring the database to a transaction-consistent
state.

## Phase 1 — Find End of Log

`LastFileReader` scans backward from the end of the last log file, validating
CRC32 checksums. The first entry that fails its checksum marks the true end
of the log. Any partially written entries are discarded.

## Phase 2 — Build Tree from Checkpoint

1. Scan backward to find the last `CheckpointEnd` entry.
2. Read its `root_lsn` and `checkpoint_start_lsn`.
3. Scan forward from `checkpoint_start_lsn`, reading `IN` and `BIN` entries
   to reconstruct the in-memory B+tree.

At the end of Phase 2, the tree reflects all writes that had been checkpointed.

## Phase 3 — Replay and Undo LNs

1. Start from `first_active_lsn` (recorded in `CheckpointEnd`).
2. Scan forward to the end of the log.
3. For committed transactions: **redo** their LN writes to the tree.
4. For uncommitted transactions: collect and **undo** in reverse order.

After Phase 3, the database is transaction-consistent: committed writes are
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
