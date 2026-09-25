//! Public hot-backup API — the JE `DbBackup` contract.
//!
//! See [`Environment::start_backup`](crate::Environment::start_backup) for the
//! entry point and a full worked example.  This is a thin, error-mapping
//! wrapper over [`noxu_dbi::DbBackup`], which drives the cleaner's
//! `FileProtector` to pin the backup file set (JE `util/DbBackup.java`).

use std::path::PathBuf;

use crate::error::{NoxuError, Result};

/// An open hot backup.
///
/// While this handle is alive, the log cleaner will not delete any file in the
/// backup set.  Enumerate the files with
/// [`log_files_in_backup_set`](Backup::log_files_in_backup_set), copy them, then
/// call [`end_backup`](Backup::end_backup) (or drop the handle) to re-enable
/// cleaning.
///
/// JE: `com.sleepycat.je.util.DbBackup`.
pub struct Backup {
    inner: noxu_dbi::DbBackup,
}

impl Backup {
    pub(crate) fn new(inner: noxu_dbi::DbBackup) -> Self {
        Backup { inner }
    }

    /// Returns the absolute paths of every log file to copy for this backup,
    /// sorted ascending by file number.
    ///
    /// JE: `DbBackup.getLogFilesInBackupSet` (`DbBackup.java:648`).
    pub fn log_files_in_backup_set(&self) -> Vec<PathBuf> {
        self.inner.log_files_in_backup_set()
    }

    /// Returns the file number of the last file in this backup set.  Persist
    /// it to drive an incremental backup next time (copy only files strictly
    /// greater than this number).
    ///
    /// JE: `DbBackup.getLastFileInBackupSet` (`DbBackup.java:632`).
    pub fn last_file_in_backup_set(&self) -> u32 {
        self.inner.last_file_in_backup_set()
    }

    /// Ends backup mode, re-enabling cleaner deletion of the pinned files.
    /// Dropping the handle does the same; call this to surface any error.
    ///
    /// JE: `DbBackup.endBackup` (`DbBackup.java:588`).
    pub fn end_backup(self) -> Result<()> {
        self.inner
            .end_backup()
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))
    }
}
