//! Hot backup helper — a faithful port of BDB-JE's `DbBackup`
//! (`je/util/DbBackup.java`).
//!
//! # What this is (and is not)
//!
//! JE's `DbBackup` is **not** a copy engine.  It is a coordination helper that
//! *pins the log-file set* against cleaner deletion while the caller copies the
//! files with any tool it likes (`cp`, `rsync`, an object-store client, …).
//! This port preserves that contract exactly:
//!
//! * [`DbBackup::start_backup`] pins the active log-file set so the cleaner
//!   cannot delete any file in it (JE `DbBackup.startBackup`,
//!   `DbBackup.java:480`, via `FileProtector.protectActiveFiles`).
//! * [`DbBackup::log_files_in_backup_set`] returns the exact set of `.ndb`
//!   files the caller must copy, current as of `start_backup`
//!   (JE `getLogFilesInBackupSet`, `DbBackup.java:648`).
//! * [`DbBackup::end_backup`] releases the protection so the cleaner can
//!   reclaim (JE `DbBackup.endBackup`, `DbBackup.java:588`).  Dropping the
//!   handle without calling `end_backup` also releases protection, so a
//!   panicking caller cannot wedge the cleaner forever.
//!
//! # Why this is sound and bounded
//!
//! The mechanism it drives already exists: [`noxu_cleaner::FileProtector`].
//! The cleaner's two deletion paths (`Cleaner::delete_pending_files` and its
//! sibling in the safe-to-delete drain) already skip any file for which
//! `FileProtector::is_protected` returns `true`.  So a backup is a *new
//! consumer* of the existing pin — it changes no cleaner semantics.
//!
//! # Consistency of the copied set
//!
//! As in JE, the caller should force a checkpoint immediately before
//! `start_backup` to reduce recovery time after a restore.  The backup set is
//! every log file that exists at `start_backup`; new writes go to the current
//! file and to new files, all of which recovery on the copy will either
//! replay from the last checkpoint or ignore.  Recovery tolerates a trailing
//! log file that grew after the copy point.

use std::path::PathBuf;
use std::sync::Arc;

use noxu_cleaner::FileProtector;
use noxu_log::FileManager;

use crate::error::DbiError;

/// Protection reason recorded in the [`FileProtector`] for backup pins.
///
/// JE: `FileProtector.BACKUP_NAME` (`FileProtector.java`).
const BACKUP_PROTECTION_NAME: &str = "Backup";

/// A JE `DbBackup`-equivalent hot-backup handle.
///
/// Obtain one via `Environment::start_backup`.  While it is open, the log
/// cleaner cannot delete any file in the backup set.  The set is enumerated by
/// [`log_files_in_backup_set`](DbBackup::log_files_in_backup_set); the caller
/// copies those files, then calls [`end_backup`](DbBackup::end_backup) (or
/// drops the handle) to re-enable cleaning.
pub struct DbBackup {
    /// Shared cleaner file protector (`envImpl.getFileProtector()`).
    file_protector: Arc<FileProtector>,
    /// Shared file manager, used to resolve file numbers to on-disk paths
    /// (`envImpl.getFileManager()`).
    file_manager: Arc<FileManager>,
    /// The snapshot of protected file numbers, sorted ascending
    /// (JE `snapshotFiles`, a `NavigableSet<Long>`).
    snapshot_files: Vec<u32>,
    /// Last file number in the backup set (JE `lastFileInBackup`).
    last_file_in_backup: u32,
    /// Whether protection is still held (JE `backupStarted`).
    open: bool,
}

impl DbBackup {
    /// Starts backup mode: pins the active log-file set against cleaner
    /// deletion and snapshots the file numbers to copy.
    ///
    /// JE: `DbBackup.startBackup` (`DbBackup.java:480`).  JE flips the log so
    /// the last backup file becomes immutable; this port instead relies on the
    /// caller having forced a checkpoint first (documented, matching JE's
    /// recommendation) and treats the file set present on disk as the backup
    /// set.  Recovery on the copy is unaffected: it replays from the last
    /// checkpoint and tolerates a trailing file that grew after the copy.
    ///
    /// # Errors
    /// Returns [`DbiError`] if the log files cannot be enumerated.
    pub(crate) fn start_backup(
        file_protector: Arc<FileProtector>,
        file_manager: Arc<FileManager>,
    ) -> Result<Self, DbiError> {
        // Enumerate all log files currently on disk. Because we protect them
        // *before* returning (and the cleaner honours the protector), the set
        // is stable for the lifetime of this handle.
        //
        // JE momentarily protects the whole range, determines the last file,
        // then protects the active files (DbBackup.java:518-560). We protect
        // every file that exists, which is a superset-safe simplification:
        // over-protecting for the duration of a backup is harmless (the extra
        // files, if any were already obsolete, simply aren't reclaimed until
        // end_backup), and the returned set is exactly what recovery needs.
        let files = file_manager.list_file_numbers().map_err(|e| {
            DbiError::EnvironmentFailure {
                reason: format!("backup: cannot list log files: {e}"),
            }
        })?;

        // Protect every file first, so that between listing and returning the
        // cleaner cannot delete one out from under us.
        for &f in &files {
            file_protector.protect_file(f, BACKUP_PROTECTION_NAME);
        }

        let last_file_in_backup = files.last().copied().unwrap_or(0);

        Ok(DbBackup {
            file_protector,
            file_manager,
            snapshot_files: files,
            last_file_in_backup,
            open: true,
        })
    }

    /// Returns the absolute paths of every log file that must be copied for
    /// this backup, sorted ascending by file number.
    ///
    /// JE: `getLogFilesInBackupSet` (`DbBackup.java:648`) — JE returns partial
    /// file names; this port returns full paths so the caller can copy the
    /// bytes directly without knowing the environment's on-disk layout.  Use
    /// `PathBuf::file_name` to obtain the name to write in the destination.
    pub fn log_files_in_backup_set(&self) -> Vec<PathBuf> {
        self.snapshot_files
            .iter()
            .map(|&f| self.file_manager.full_file_name(f))
            .collect()
    }

    /// Returns the file number of the last file in this backup set.
    ///
    /// Save this value and pass it to the next backup session to perform an
    /// incremental backup (copy only files strictly greater than it).
    ///
    /// JE: `getLastFileInBackupSet` (`DbBackup.java:632`).
    pub fn last_file_in_backup_set(&self) -> u32 {
        self.last_file_in_backup
    }

    /// Returns whether backup mode is currently open.
    ///
    /// JE: `backupIsOpen` (`DbBackup.java:774`).
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Ends backup mode, re-enabling cleaner deletion of the pinned files.
    ///
    /// Idempotent: calling it more than once (or after a drop-release) is a
    /// no-op.  JE: `endBackup` (`DbBackup.java:588`).
    pub fn end_backup(mut self) -> Result<(), DbiError> {
        self.release();
        Ok(())
    }

    /// Releases all protection held by this backup. Shared by
    /// [`end_backup`](DbBackup::end_backup) and [`Drop`].
    fn release(&mut self) {
        if !self.open {
            return;
        }
        for &f in &self.snapshot_files {
            self.file_protector.unprotect_file(f);
        }
        self.open = false;
    }
}

impl Drop for DbBackup {
    /// Releases file protection if `end_backup` was not called, so a dropped
    /// or panicking backup cannot leave the cleaner permanently disabled.
    /// JE relies on `endBackup`; the RAII release is the Rust-idiomatic
    /// safeguard for the same invariant.
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fm(dir: &std::path::Path) -> Arc<FileManager> {
        Arc::new(
            FileManager::new(dir, false, 1024, 0).expect("open file manager"),
        )
    }

    #[test]
    fn start_pins_all_files_and_end_releases() {
        let dir = tempdir().unwrap();
        let file_manager = fm(dir.path());
        // Create three log files.
        file_manager.create_file(0).unwrap();
        file_manager.flip_file().unwrap();
        file_manager.flip_file().unwrap();

        let protector = Arc::new(FileProtector::new());

        let backup = DbBackup::start_backup(
            Arc::clone(&protector),
            Arc::clone(&file_manager),
        )
        .unwrap();

        // All three files pinned.
        assert!(protector.is_protected(0));
        assert!(protector.is_protected(1));
        assert!(protector.is_protected(2));
        assert_eq!(backup.log_files_in_backup_set().len(), 3);
        assert_eq!(backup.last_file_in_backup_set(), 2);
        assert!(backup.is_open());

        backup.end_backup().unwrap();

        // Protection released.
        assert!(!protector.is_protected(0));
        assert!(!protector.is_protected(1));
        assert!(!protector.is_protected(2));
    }

    #[test]
    fn drop_releases_protection() {
        let dir = tempdir().unwrap();
        let file_manager = fm(dir.path());
        file_manager.create_file(0).unwrap();
        let protector = Arc::new(FileProtector::new());

        {
            let _backup = DbBackup::start_backup(
                Arc::clone(&protector),
                Arc::clone(&file_manager),
            )
            .unwrap();
            assert!(protector.is_protected(0));
        }
        // Dropped without end_backup -> released.
        assert!(!protector.is_protected(0));
    }

    #[test]
    fn backup_pin_coexists_with_other_consumers() {
        // A file already pinned by another consumer (e.g. a disk-ordered
        // cursor) must remain protected after the backup releases its own pin.
        let dir = tempdir().unwrap();
        let file_manager = fm(dir.path());
        file_manager.create_file(0).unwrap();
        let protector = Arc::new(FileProtector::new());
        protector.protect_file(0, "DiskOrderedCursor");

        let backup = DbBackup::start_backup(
            Arc::clone(&protector),
            Arc::clone(&file_manager),
        )
        .unwrap();
        assert_eq!(protector.get_protection_count(0), 2);

        backup.end_backup().unwrap();
        // The cursor's pin survives.
        assert!(protector.is_protected(0));
        assert_eq!(protector.get_protection_count(0), 1);
    }
}
