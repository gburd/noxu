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

If `NoxuError::EnvironmentFailure { reason: DiskLimitExceeded, .. }` is
returned:

1. Free disk space (remove old log files outside the environment directory,
   expand the volume, etc.).
2. Close and reopen the environment.  The cleaner will resume and reclaim
   additional space automatically.

---

