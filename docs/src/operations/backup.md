# Backup

Noxu DB provides a hot-backup API modelled on BDB-JE's `DbBackup`
(`com.sleepycat.je.util.DbBackup`). Like JE, it is **not a copy engine**: it
pins the current log-file set so the log cleaner cannot delete it, and hands
you the exact list of `.ndb` files to copy. You copy those files with any tool
you like (`std::fs::copy`, `rsync`, an object-store client, …), then release the
backup. This lets you take a consistent snapshot **while the environment keeps
serving reads and writes**.

## Taking a backup

```rust,no_run
use noxu_db::{CheckpointConfig, Environment, EnvironmentConfig};
use std::path::Path;

fn back_up(env: &Environment, dest: &Path) -> noxu_db::error::Result<()> {
    // Force a checkpoint first to reduce recovery time after a restore.
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))?;

    // Pin the current log-file set against cleaner deletion.
    let backup = env.start_backup()?;

    // Copy each file in the backup set to the destination directory.
    for src in backup.log_files_in_backup_set() {
        let name = src.file_name().expect("log file name");
        std::fs::copy(&src, dest.join(name))?;
    }

    // Re-enable cleaner deletion of the copied files.
    backup.end_backup()?;
    Ok(())
}
```

`start_backup()` requires a read-write environment with the log cleaner running
(the cleaner owns the file protector that pins the set). It returns an error on
a read-only environment.

While a backup is open, the cleaner cannot delete any file in the set, so log
files accumulate on disk until you call `end_backup()`. Always call it (or drop
the [`Backup`] handle — dropping releases the pins too) so cleaning resumes.

## Incremental backups

`Backup::last_file_in_backup_set()` returns the highest file number in the
current backup. Persist it, and on the next backup copy only the files whose
number is greater than that value — the older files are unchanged and already
in your archive.

## No built-in copy engine

Noxu DB does not copy the files for you and has no scheduled/daemon backup.
This matches JE, where `DbBackup` also leaves the copy to the caller. If you
want periodic backups, run the workflow above from your own scheduler (cron, a
timer thread, an operator, …).

## Recovery from backup

Copy the backup directory to a new location and open it as a normal
environment. Noxu DB performs normal crash recovery on open, replaying from the
last checkpoint contained in the copied files.

```rust,no_run
use noxu_db::{Environment, EnvironmentConfig};
use std::path::PathBuf;

let restored = Environment::open(
    EnvironmentConfig::new(PathBuf::from("/restore/noxu")),
)?;
# Ok::<(), noxu_db::error::NoxuError>(())
```

## Cold backup (environment closed)

If you can afford downtime, the simplest safe procedure is to close the
environment and copy the directory:

1. `env.close()`.
2. Copy the entire environment directory (all `*.ndb` files) to the archive.
3. Restore by copying the directory back and opening it.

A `cp`/`rsync` of a **live** environment without `start_backup()` is **not**
safe: the cleaner may delete or replace files mid-copy, producing an
inconsistent set. Use the hot-backup API above, or close the environment first.

[`Backup`]: https://docs.rs/noxu-db/latest/noxu_db/struct.Backup.html
