# Backup and Recovery

## Normal Recovery

Noxu DB organizes its data as a B-tree, and all write operations are logged to
`.ndb` log files on disk. When database records are created, modified, or deleted,
the modifications are represented in the B-tree's leaf nodes. On a transactional
commit, only the leaf nodes modified by the transaction are written to the log.

**Normal recovery** is the process of reconstructing the complete B-tree from the
leaf-node information in the log files. This is run automatically every time a Noxu
DB environment is opened; no application action is required. The checkpointer
background thread runs periodically to write a complete, consistent checkpoint to
disk, which reduces the amount of log that must be replayed on the next recovery
and thus shortens startup time.

If an `EnvironmentFailure` error is returned, call `env.is_valid()`:

- If it returns `true`, you can continue using the environment.
- If it returns `false`, close and reopen all `Environment` handles so that normal
  recovery runs.

## Performing Backups

The fundamental backup operation is to copy Noxu DB log files (`.ndb` files) to
safe storage. To restore, copy the files back to the environment directory and
reopen the environment; normal recovery reconstructs the B-tree automatically.

### Hot Backup (Online)

A hot backup is taken while write operations are in progress, using the
[`Environment::start_backup`](../operations/backup.md) API. It pins the current
log-file set so the log cleaner cannot delete or replace a file while you copy
it, then hands you the exact list of files to copy. This is the JE `DbBackup`
contract.

> **Do not** naively `cp`/`rsync` a live environment directory without
> `start_backup()`. The log cleaner may delete or replace files mid-copy,
> producing an inconsistent, unrecoverable set. Use the API below, or take an
> offline backup.

```rust,no_run
use noxu_db::{CheckpointConfig, Environment};
use std::path::Path;

/// Consistent hot backup: pin the file set, copy it, release.
fn hot_backup(env: &Environment, backup_dir: &Path) -> noxu_db::error::Result<()> {
    std::fs::create_dir_all(backup_dir)?;

    // Force a checkpoint first to reduce recovery time after a restore.
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))?;

    // Pin the log-file set against cleaner deletion.
    let backup = env.start_backup()?;

    for src in backup.log_files_in_backup_set() {
        let name = src.file_name().expect("log file name");
        std::fs::copy(&src, backup_dir.join(name))?;
    }

    // Re-enable cleaning of the copied files.
    backup.end_backup()?;
    Ok(())
}
```

### Offline Backup

An offline backup guarantees you capture the database including all in-memory cache
contents at the moment of the backup:

1. Stop all write operations on the database.
2. Ensure all in-memory changes are flushed to disk:
   - If using durable transactions (the default `SyncPolicy::Sync`), simply make
     sure all in-progress transactions are committed or aborted.
   - If using non-durable transactions, run a checkpoint, or close the environment
     (which runs a checkpoint automatically).
3. Optionally run a checkpoint to shorten future recovery time.
4. Copy all `.ndb` log files to the archival location.
5. Resume normal operations.

### Incremental Backups

An incremental backup copies only those log files created since the last backup.
Save `Backup::last_file_in_backup_set()` after each backup; on the next run,
copy only the files whose number is greater than that value (older files are
immutable and already archived).

### Restore

To restore from backup:

1. Copy the backed-up `.ndb` log files to the environment directory.
2. Open the environment normally. Normal recovery will reconstruct the B-tree.

For catastrophic recovery (e.g., after a disk failure), restore from the most
recent full backup and then apply any subsequent incremental backups in order
before opening the environment.

---
