# Recovery Procedure

## Automatic recovery (normal path)

WAL recovery runs automatically on `Environment::open()`.  No manual steps
are required after a clean or unclean shutdown.  Recovery time is proportional
to the amount of data written since the last checkpoint.

## BIN-delta deletion recovery

Physical record deletion now forces the affected BIN's next checkpoint image
to be full, rather than a partial BIN-delta. Sparse updates after deletion
previously could log a delta that omitted the removed keys, allowing recovery
to restore those keys from the older full image. Comparator-based deletion
and compressor slot removal use the same full-image requirement. After that
full image is logged, ordinary delta logging is eligible again.

The on-disk format is unchanged; no file conversion is required. This prevents
new incorrect deletion images, but does **not** repair existing logs or detect
keys already resurrected by an earlier reopen. Preserve a copy of a suspected
environment and reconcile its keys and values against an authoritative source
or known-good backup before resuming writes. Successful reopen alone is not
proof that the deleted-key set is correct.

This fix does not certify partial-page logging overall: split/merge base
invalidation, successive-delta history, and index-based IN-redo remain separate
review items.

## Manual recovery steps (corrupted environment)

1. **Identify corruption scope** — check logs for `NoxuError::EnvironmentFailure`
   with `EnvironmentFailureReason::LogChecksum` or `BtreeCorruption`.

2. **Stop all writers immediately** — do not attempt further writes once
   corruption is detected; the environment is invalidated and all operations
   return errors.

3. **Copy environment directory** — back up the entire `.ndb` directory before
   attempting any repair.

4. **Attempt normal reopen**:

   ```rust
   let env = Environment::open(
       EnvironmentConfig::new(path)
           .with_allow_create(false)
           .with_transactional(true),
   )?;
   ```

   If this succeeds, recovery is complete.

5. **If reopen fails — restore from replica** (replication environments only):
   Use the network restore protocol to sync from a healthy replica.
   The `env_home` field on `RepConfig` must be set on the source node.

6. **Last resort — restore from backup** using files copied via
   [`Environment::start_backup`](backup.md).
   Replace the corrupted environment directory with the backup and reopen.

## Cleaner entry-type data loss

Older cleaner code used entry-type numbers that did not match the Noxu log
format. Cleaning could delete files containing live records; a successful
reopen alone does not establish that all records survived.

Before upgrading an affected environment, stop cleaning and preserve a copy of
its directory. Validate expected keys **and values** against a trusted backup or
application source. Prior cleaner runs may already have deleted unrecoverable
records. The corrected cleaner prevents this entry-type misclassification; it
cannot reconstruct deleted files. Restore missing data from an independently
verified backup or replica. No log-format conversion is required.

The cleaner retains files containing unknown or unsupported entry types, or
incomplete or invalid entries. Known but unsupported types include current XA
`TxnPrepare` records, even when a commit follows: the cleaner has no lifetime
proof that permits discarding them. Such files are deferred until pass end and
requeued; independent eligible files can still be cleaned, including in
one-file daemon passes. Each unsupported-file error is logged, and the pass
returns its first unsupported-file error after publishing progress statistics
and performing normal checkpoint-gated deletion. An error therefore does not
mean that no other files were cleaned.

Unknown types, corruption, and I/O failures still stop the pass immediately;
they are not treated as unsupported semantics or obsolete bytes. Investigate
errors and do not manually delete retained files. Attempts remain bounded by
the initial summary/queue count. Unsupported files consume this attempt cap,
not the requested cleaning budget; migration output may be selected within
the cap. Subsequent passes can reclaim more space after checkpoint barriers
complete.

## Disk-full recovery

If a write returns `NoxuError::DiskLimitExceeded { used, limit }`, a disk-space
limit (`MAX_DISK` and/or `FREE_DISK`) is currently violated and new **user**
writes are refused so that recovery stays possible. Reads, transaction aborts,
and the cleaner/checkpointer's own (internal) writes continue to work — the
cleaner needs to write to free space.

Writes resume **automatically** once space is reclaimed: the cleaner deletes
obsolete log files on its next pass (and the checkpointer daemon refreshes the
limit on its interval), which clears the violation. To recover faster:

1. Reduce `MAX_DISK` pressure: delete obsolete records and call
   `Environment::clean_log()` to force a cleaner pass (it reclaims whole
   obsolete log files and refreshes the disk-limit state).
2. For a `FREE_DISK` violation, free filesystem space (remove files outside the
   environment directory, expand the volume).
3. Call `Environment::refresh_disk_limit()` to recompute the violation state
   immediately rather than waiting for the next daemon wakeup.

The environment does **not** need to be closed and reopened — the limit clears
in place. See [Sizing → Disk-space limits](sizing.md#disk-space-limits-max_disk--free_disk).

---
