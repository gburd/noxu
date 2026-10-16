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
use noxu_log::{
    FileManager, LogEntryType, LogManager, Provisional,
    entry::ImmutableFileEntry,
};

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

/// Protection reason recorded in the [`FileProtector`] for a network-restore
/// backup pin.  JE: `FileProtector.NETWORK_RESTORE_NAME`
/// (`FileProtector.java`).  A network restore pins the same active set as a
/// plain backup; the name only distinguishes the consumer in diagnostics.
const NETWORK_RESTORE_PROTECTION_NAME: &str = "NetworkRestore";

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
        log_manager: Option<Arc<LogManager>>,
    ) -> Result<Self, DbiError> {
        Self::start_backup_inner(
            file_protector,
            file_manager,
            log_manager,
            false,
        )
    }

    /// Starts a *network-restore* backup: identical to [`start_backup`], but
    /// logs the immutable-file marker and flips the log so the last file in
    /// the backup set is sealed (immutable) before it is streamed to the
    /// restoring node.
    ///
    /// JE: `DbBackup.setNetworkRestore()` + `startBackup()`
    /// (`DbBackup.java:505`, `LogFileFeeder.sendFileList`,
    /// `LogFileFeeder.java:443`).  The restore SERVER holds this handle over
    /// the file set it is streaming so the cleaner cannot delete or truncate
    /// a file mid-send.
    ///
    /// [`start_backup`]: DbBackup::start_backup
    pub(crate) fn start_network_restore_backup(
        file_protector: Arc<FileProtector>,
        file_manager: Arc<FileManager>,
        log_manager: Option<Arc<LogManager>>,
    ) -> Result<Self, DbiError> {
        Self::start_backup_inner(
            file_protector,
            file_manager,
            log_manager,
            true,
        )
    }

    fn start_backup_inner(
        file_protector: Arc<FileProtector>,
        file_manager: Arc<FileManager>,
        log_manager: Option<Arc<LogManager>>,
        network_restore: bool,
    ) -> Result<Self, DbiError> {
        // FID-BACKUP-1 (JE DbBackup.startBackup, DbBackup.java:534-549): for a
        // read-write environment, write an "immutable file" marker into the
        // current log file and then flip the log, so every file in the backup
        // set is immutable (sealed) and cannot be mutated after it is copied
        // or restored. JE: "Flip the log so that all files in the backup list
        // are immutable. But first, write an "immutable file" marker in the
        // last file in the backup, so it cannot be modified after it is
        // restored. Recovery enforces this rule." A read-only environment's
        // files are already immutable, so no marker/flip is logged there
        // (JE's `envIsReadOnly` branch).
        //
        // `marker_file` carries the file number the marker landed in, which
        // becomes `lastFileInBackup` (JE: `DbLsn.getFileNumber(markerLsn)`).
        let marker_file: Option<u32> =
            match (&log_manager, file_manager.is_read_only()) {
                (Some(lm), false) => {
                    let marker = ImmutableFileEntry::new(0);
                    let mut buf = bytes::BytesMut::new();
                    marker.write_to_log(&mut buf);
                    // Log the marker into the current file, forcing it durable so
                    // the sealed file carries the marker on disk, then flip the
                    // log so the current file is sealed.
                    let marker_lsn = lm
                        .log(
                            LogEntryType::ImmutableFile,
                            &buf,
                            Provisional::No,
                            true, // flush_required
                            true, // fsync_required
                        )
                        .map_err(|e| DbiError::EnvironmentFailure {
                            reason: format!(
                                "backup: cannot log immutable-file marker: {e}"
                            ),
                        })?;
                    lm.file_manager().flip_file().map_err(|e| {
                        DbiError::EnvironmentFailure {
                            reason: format!(
                                "backup: cannot flip log after marker: {e}"
                            ),
                        }
                    })?;
                    Some(marker_lsn.file_number())
                }
                _ => None,
            };

        let name = if network_restore {
            NETWORK_RESTORE_PROTECTION_NAME
        } else {
            BACKUP_PROTECTION_NAME
        };
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
        let mut files = files;

        // A2 (v7.11.1): validate every file header before snapshotting it
        // into the backup set. A backup copy that includes a file whose
        // `log_version` the engine cannot read is a backup recovery cannot
        // mount; refuse the backup up front with the typed
        // `LogError::VersionMismatch` (via `get_file_handle`) rather than
        // silently copying an unreadable future-version file.
        for &f in &files {
            file_manager.get_file_handle(f).map_err(|e| {
                DbiError::EnvironmentFailure {
                    reason: format!("backup: refusing log file {f}: {e}"),
                }
            })?;
        }

        // Protect every file first, so that between listing and returning the
        // cleaner cannot delete one out from under us.
        for &f in &files {
            file_protector.protect_file(f, name);
        }

        // JE sets `lastFileInBackup = DbLsn.getFileNumber(markerLsn)` — the
        // file the immutable marker landed in. The flip created a new current
        // file strictly greater than that, which is NOT part of the backup
        // set, so trim it off the snapshot. When no marker was logged
        // (read-only env, or no log manager wired) fall back to the highest
        // file on disk, as before.
        let last_file_in_backup = match marker_file {
            Some(m) => m,
            None => files.last().copied().unwrap_or(0),
        };
        files.retain(|&f| f <= last_file_in_backup);

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
            None,
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
                None,
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
            None,
        )
        .unwrap();
        assert_eq!(protector.get_protection_count(0), 2);

        backup.end_backup().unwrap();
        // The cursor's pin survives.
        assert!(protector.is_protected(0));
        assert_eq!(protector.get_protection_count(0), 1);
    }

    /// A2 (v7.11.1 backport, entry point: BACKUP): if a future-version log
    /// file appears AFTER the FileManager was opened, `start_backup` must
    /// refuse it rather than silently snapshot it into the backup set. A
    /// backup copy that includes a header the engine cannot read is a backup
    /// recovery cannot mount.
    #[test]
    fn start_backup_rejects_future_version_file() {
        use noxu_log::file_header::LOG_VERSION;
        let dir = tempdir().unwrap();
        let file_manager = fm(dir.path());
        // One valid v3 file.
        file_manager.create_file(0).unwrap();

        // Plant a CRC-valid future-version file 1 that the open-time preflight
        // never saw.
        let mut bytes = vec![
            78u8, 79, 88, 85, 68, 66, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        bytes[8..12].copy_from_slice(&(LOG_VERSION + 1).to_be_bytes());
        bytes[24..28].copy_from_slice(&1u32.to_be_bytes());
        let crc =
            noxu_log::checksum::ChecksumValidator::compute_range(&bytes, 0, 32);
        bytes[32..36].copy_from_slice(&crc.to_be_bytes());
        std::fs::write(dir.path().join("00000001.ndb"), &bytes).unwrap();

        let protector = Arc::new(FileProtector::new());
        let res = DbBackup::start_backup(
            Arc::clone(&protector),
            Arc::clone(&file_manager),
            None,
        );
        assert!(
            res.is_err(),
            "start_backup must refuse a future-version file in the set",
        );
        // The future file must be untouched by the refusal.
        assert_eq!(
            std::fs::read(dir.path().join("00000001.ndb")).unwrap(),
            bytes
        );
    }

    /// FID-BACKUP-1 (JE `DbBackup.startBackup`, `DbBackup.java:534`): a
    /// read-write backup must log the immutable-file marker and flip the log,
    /// so the last file in the backup set is sealed. Any subsequent write
    /// lands in a NEW file strictly greater than `last_file_in_backup_set()`
    /// and is NOT part of the backup set — the copied last file can never be
    /// mutated after the snapshot.
    ///
    /// Witness: RED at 81b2f048 (no marker/flip: `start_backup` leaves the
    /// current file open, a later write grows the backup's last file in
    /// place, so `last_file_in_backup_set()` equals the file that keeps
    /// growing). GREEN after the fix (the flip seals it; later writes go to a
    /// strictly-higher file outside the set).
    #[test]
    fn start_backup_seals_last_file_via_marker_and_flip() {
        use noxu_log::LogManager;

        let dir = tempdir().unwrap();
        let file_manager = fm(dir.path());
        file_manager.create_file(0).unwrap();
        let log_manager =
            Arc::new(LogManager::new(Arc::clone(&file_manager), 4, 4096, 4096));
        let protector = Arc::new(FileProtector::new());

        let backup = DbBackup::start_backup(
            Arc::clone(&protector),
            Arc::clone(&file_manager),
            Some(Arc::clone(&log_manager)),
        )
        .unwrap();

        let sealed = backup.last_file_in_backup_set();
        // The backup set must not include any file past the sealed one.
        for p in backup.log_files_in_backup_set() {
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            let stem = name.strip_suffix(".ndb").expect("backup-set .ndb name");
            let num = u32::from_str_radix(stem, 16).expect("hex file number");
            assert!(
                num <= sealed,
                "backup set must not include a file past the sealed last file"
            );
        }

        // A write after start_backup must land in a file strictly greater
        // than the sealed last backup file: the backup's last file is
        // immutable and never grows.
        let mut buf = bytes::BytesMut::new();
        ImmutableFileEntry::new(1).write_to_log(&mut buf);
        let after = log_manager
            .log(LogEntryType::ImmutableFile, &buf, Provisional::No, true, true)
            .unwrap();
        assert!(
            after.file_number() > sealed,
            "a write after start_backup ({}) must go to a file strictly \
             greater than the sealed backup last file ({sealed})",
            after.file_number()
        );

        backup.end_backup().unwrap();
    }
}
