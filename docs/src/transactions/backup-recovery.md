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

**Hot Backup (Online)**

A hot backup is taken while write operations are in progress. Copy all `.ndb` log
files from the environment directory to your archival location. Files must be
copied in alphabetical (numerical) order. You do not need to stop database
operations.

The complication with hot backups is that the log cleaner may delete or create
files while you are copying. A naive copy loop may miss newly created files. The
recommended solution is to do two passes:

1. Enumerate all log files and begin copying.
2. After finishing, check for any new files created during the copy and copy those
   as well.

Or use a systematic approach:

```rust
use std::fs;
use std::path::{Path, PathBuf};

/// Copy all .ndb log files from `env_dir` to `backup_dir` in order.
/// A simple hot-backup approach; for production use, implement two-pass
/// logic or freeze the log file set before copying.
fn hot_backup(env_dir: &Path, backup_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(backup_dir)?;

    let mut log_files: Vec<PathBuf> = fs::read_dir(env_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "ndb").unwrap_or(false))
        .collect();

    // Sort numerically (alphabetical order matches numerical for hex-named files).
    log_files.sort();

    for file in &log_files {
        let dest = backup_dir.join(file.file_name().unwrap());
        fs::copy(file, dest)?;
        println!("Backed up {:?}", file.file_name().unwrap());
    }

    Ok(())
}
```

**Offline Backup**

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

**Incremental Backups**

An incremental backup copies only those log files modified or created since the
last backup. Track the last log file number included in each backup and on the next
run copy only files with higher numbers. Most system backup tools support
incremental backup natively.

**Restore**

To restore from backup:
1. Copy the backed-up `.ndb` log files to the environment directory.
2. Open the environment normally. Normal recovery will reconstruct the B-tree.

For catastrophic recovery (e.g., after a disk failure), restore from the most
recent full backup and then apply any subsequent incremental backups in order
before opening the environment.

---

