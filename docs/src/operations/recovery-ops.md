# Recovery Procedure

## Automatic recovery (normal path)

WAL recovery runs automatically on `Environment::open()`.  No manual steps
are required after a clean or unclean shutdown.  Recovery time is proportional
to the amount of data written since the last checkpoint.

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

6. **Last resort — restore from backup** using `BackupManager`-copied files.
   Replace the corrupted environment directory with the backup and reopen.

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
