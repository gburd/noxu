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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use noxu_cleaner::FileProtector;
use noxu_log::{
    FileManager, LogEntryType, LogManager, Provisional,
    entry::ImmutableFileEntry,
};
use noxu_sync::Mutex;

use crate::error::DbiError;

/// Protection reason recorded in the [`FileProtector`] for backup pins.
///
/// JE: `FileProtector.BACKUP_NAME` (`FileProtector.java`).
const BACKUP_PROTECTION_NAME: &str = "Backup";

/// Per-backup invalidation state shared between a live [`DbBackup`] handle and
/// the environment's [`DbBackupRegistry`].
///
/// JE keeps `DbBackup.invalid` + `rollbackStartedFileNumber` as fields on each
/// `DbBackup`; `RepImpl.invalidateBackups(fileNumber)` iterates every open
/// backup and flips them (`RepImpl.java:1957`, called from
/// `Replay.rollback()`). This Arc is the shared cell the registry flips while
/// the backup is open.
#[derive(Debug)]
pub(crate) struct DbBackupState {
    /// `true` once a concurrent replication rollback has overwritten log files
    /// from `rollback_file` onward (JE `DbBackup.invalid`).
    invalid: AtomicBool,
    /// The file number at which the rollback started (JE
    /// `DbBackup.rollbackStartedFileNumber`).
    rollback_file: AtomicU32,
}

impl DbBackupState {
    fn new() -> Arc<Self> {
        Arc::new(DbBackupState {
            invalid: AtomicBool::new(false),
            rollback_file: AtomicU32::new(0),
        })
    }

    /// JE: `DbBackup.invalidate(fileNumber)` (`DbBackup.java:779`).
    fn invalidate(&self, file_number: u32) {
        self.rollback_file.store(file_number, Ordering::SeqCst);
        self.invalid.store(true, Ordering::SeqCst);
    }

    fn is_invalid(&self) -> bool {
        self.invalid.load(Ordering::SeqCst)
    }

    fn rollback_file(&self) -> u32 {
        self.rollback_file.load(Ordering::SeqCst)
    }
}

/// Environment-side registry of open backups, mirroring JE's
/// `RepImpl.backups` set + `backupProhibited` flag.
///
/// A standalone (non-replicated) environment never prohibits or invalidates
/// backups, matching JE's base `EnvironmentImpl.addDbBackup` which simply
/// returns `true`. A replicated environment's rollback path
/// (`execute_rollback`) calls [`set_backup_prohibited`] /
/// [`invalidate_backups`] exactly as JE's `Replay.rollback()` calls
/// `setBackupProhibited` / `invalidateBackups`.
#[derive(Debug, Default)]
pub struct DbBackupRegistry {
    inner: Mutex<DbBackupRegistryInner>,
}

#[derive(Debug, Default)]
struct DbBackupRegistryInner {
    /// Open backups (JE `RepImpl.backups`).
    backups: Vec<Arc<DbBackupState>>,
    /// Whether new backups are refused because a rollback is in progress
    /// (JE `RepImpl.backupProhibited`).
    prohibited: bool,
}

impl DbBackupRegistry {
    /// Register a new backup. Returns `false` if backups are currently
    /// prohibited (a replication rollback is in progress), in which case the
    /// caller must refuse to start the backup.
    ///
    /// JE: `RepImpl.addDbBackup(backup)` (`RepImpl.java:1935`).
    fn add(&self, state: &Arc<DbBackupState>) -> bool {
        let mut inner = self.inner.lock();
        if inner.prohibited {
            return false;
        }
        inner.backups.push(Arc::clone(state));
        true
    }

    /// Deregister a backup on `end_backup`/drop.
    ///
    /// JE: `RepImpl.removeDbBackup(backup)` (`RepImpl.java:1949`).
    fn remove(&self, state: &Arc<DbBackupState>) {
        let mut inner = self.inner.lock();
        inner.backups.retain(|b| !Arc::ptr_eq(b, state));
    }

    /// Invalidate every open backup because a replication rollback has
    /// overwritten log files from `file_number` onward.
    ///
    /// JE: `RepImpl.invalidateBackups(fileNumber)` (`RepImpl.java:1957`),
    /// called from `Replay.rollback()`.
    pub fn invalidate_backups(&self, file_number: u32) {
        let inner = self.inner.lock();
        for backup in &inner.backups {
            backup.invalidate(file_number);
        }
    }

    /// Set the backup-prohibited status so that new `start_backup` calls are
    /// refused for the duration of a replication rollback.
    ///
    /// JE: `RepImpl.setBackupProhibited(boolean)` (`RepImpl.java:1967`).
    pub fn set_backup_prohibited(&self, prohibited: bool) {
        self.inner.lock().prohibited = prohibited;
    }
}

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
    /// Files whose protection is still held (not yet released individually via
    /// [`release_file_protection`](DbBackup::release_file_protection) nor by a
    /// final [`release`]). Interior-mutable so a `&self` per-file release and
    /// the final `&mut self` release each unprotect every file exactly once
    /// (the `FileProtector` is ref-counted; a double unprotect would corrupt
    /// another consumer's count).
    ///
    /// [`release`]: DbBackup::release
    protected: Mutex<hashbrown::HashSet<u32>>,
    /// Last file number in the backup set (JE `lastFileInBackup`).
    last_file_in_backup: u32,
    /// Whether protection is still held (JE `backupStarted`).
    open: bool,
    /// Shared invalidation state (JE `DbBackup.invalid`), registered with the
    /// environment registry for the lifetime of the backup. `None` for a
    /// standalone backup with no registry (invalidation is a replication-only
    /// concept; JE's base `addDbBackup` always succeeds and never
    /// invalidates).
    state: Option<Arc<DbBackupState>>,
    /// The environment registry to deregister from on end/drop (JE
    /// `RepImpl.removeDbBackup`).
    registry: Option<Arc<DbBackupRegistry>>,
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
    /// Returns [`DbiError`] if the log files cannot be enumerated, or
    /// [`DbiError::EnvironmentFailure`] (JE `LogOverwriteException`) if a
    /// replication rollback currently prohibits backups.
    pub(crate) fn start_backup(
        file_protector: Arc<FileProtector>,
        file_manager: Arc<FileManager>,
        log_manager: Option<Arc<LogManager>>,
        registry: Option<Arc<DbBackupRegistry>>,
    ) -> Result<Self, DbiError> {
        Self::start_backup_inner(
            file_protector,
            file_manager,
            log_manager,
            registry,
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
        registry: Option<Arc<DbBackupRegistry>>,
    ) -> Result<Self, DbiError> {
        Self::start_backup_inner(
            file_protector,
            file_manager,
            log_manager,
            registry,
            true,
        )
    }

    fn start_backup_inner(
        file_protector: Arc<FileProtector>,
        file_manager: Arc<FileManager>,
        log_manager: Option<Arc<LogManager>>,
        registry: Option<Arc<DbBackupRegistry>>,
        network_restore: bool,
    ) -> Result<Self, DbiError> {
        // FID-BACKUP-2 (JE DbBackup.startBackup, DbBackup.java:488): register
        // this backup with the environment FIRST. If a replication rollback
        // currently prohibits backups, `add` returns false and JE throws
        // LogOverwriteException ("A replication operation is overwriting log
        // files. The backup can not proceed..."). We surface it as an
        // EnvironmentFailure, the existing variant used for backup refusals.
        let state = DbBackupState::new();
        if let Some(reg) = &registry
            && !reg.add(&state)
        {
            return Err(DbiError::EnvironmentFailure {
                reason: "a replication operation is overwriting log files; \
                         the backup can not proceed because files may be \
                         invalid. The backup may be attempted at a later time"
                    .to_string(),
            });
        }
        // From here on, if we fail before returning the handle we must
        // deregister so a later rollback does not try to flip a dead state.
        // The guard removes the state from the registry on drop unless the
        // handle is successfully built (then it is disarmed).
        let mut guard = DeregisterGuard {
            registry: registry.clone(),
            state: Arc::clone(&state),
            armed: true,
        };
        let backup = Self::build_backup_set(
            file_protector,
            file_manager,
            log_manager,
            network_restore,
            state,
            registry,
        )?;
        guard.armed = false;
        Ok(backup)
    }

    /// The fallible body of [`start_backup_inner`].
    fn build_backup_set(
        file_protector: Arc<FileProtector>,
        file_manager: Arc<FileManager>,
        log_manager: Option<Arc<LogManager>>,
        network_restore: bool,
        state: Arc<DbBackupState>,
        registry: Option<Arc<DbBackupRegistry>>,
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

        let protected = files.iter().copied().collect();
        Ok(DbBackup {
            file_protector,
            file_manager,
            snapshot_files: files,
            protected: Mutex::new(protected),
            last_file_in_backup,
            open: true,
            state: Some(state),
            registry,
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

    /// Returns the (full-path, file-number) pairs for every log file in this
    /// backup set, sorted ascending by file number.
    ///
    /// Used by the network-restore SERVER (FID-NR-8) to stream exactly the
    /// protected snapshot and release each file's protection as it finishes.
    pub fn backup_set_paths(&self) -> Vec<(PathBuf, u32)> {
        self.snapshot_files
            .iter()
            .map(|&f| (self.file_manager.full_file_name(f), f))
            .collect()
    }

    /// Releases the cleaner protection on a single file in the backup set
    /// after it has been fully streamed, allowing the cleaner to reclaim it
    /// while the rest of the restore continues.
    ///
    /// JE: `DbBackup.removeFileProtection(prevFileName)`
    /// (`LogFileFeeder.java:290`). Dropping or ending the backup releases any
    /// files not individually released here.
    pub fn release_file_protection(&self, file_number: u32) {
        if !self.open {
            return;
        }
        // Only unprotect if still protected; the ref-counted FileProtector
        // must not be unprotected twice for the same backup pin.
        if self.protected.lock().remove(&file_number) {
            self.file_protector.unprotect_file(file_number);
        }
    }

    /// Returns `true` if a concurrent replication rollback has invalidated
    /// this backup (JE `DbBackup.invalid`). The network-restore server checks
    /// this after streaming so it does not hand a replica a torn set.
    pub fn is_invalidated(&self) -> bool {
        self.state.as_ref().is_some_and(|s| s.is_invalid())
    }

    /// Ends backup mode, re-enabling cleaner deletion of the pinned files.
    ///
    /// Idempotent: calling it more than once (or after a drop-release) is a
    /// no-op.  JE: `endBackup` (`DbBackup.java:588`).
    ///
    /// # Errors
    /// FID-BACKUP-2 (JE `DbBackup.endBackup`, `DbBackup.java:606`): if a
    /// concurrent replication rollback invalidated this backup, returns a
    /// [`DbiError::EnvironmentFailure`] (JE `LogOverwriteException`): "A
    /// replication operation has overwritten log files ... Any copied files
    /// should be considered invalid and discarded."
    pub fn end_backup(mut self) -> Result<(), DbiError> {
        self.release();
        if let Some(state) = &self.state
            && state.is_invalid()
        {
            // Mirror JE: clear the flag so a second end_backup is a no-op.
            return Err(DbiError::EnvironmentFailure {
                reason: format!(
                    "a replication operation has overwritten log files from \
                     file {}. Any copied files should be considered invalid \
                     and discarded. The backup may be attempted at a later \
                     time",
                    state.rollback_file()
                ),
            });
        }
        Ok(())
    }

    /// Releases all protection held by this backup, and deregisters it from
    /// the environment registry. Shared by
    /// [`end_backup`](DbBackup::end_backup) and [`Drop`].
    fn release(&mut self) {
        if !self.open {
            return;
        }
        // Unprotect every file still protected (those not released
        // individually via release_file_protection), each exactly once.
        let remaining: Vec<u32> = self.protected.lock().drain().collect();
        for f in remaining {
            self.file_protector.unprotect_file(f);
        }
        // JE `RepImpl.removeDbBackup(this)` (`RepImpl.java:1949`).
        if let (Some(reg), Some(state)) = (&self.registry, &self.state) {
            reg.remove(state);
        }
        self.open = false;
    }
}

/// Removes a registered [`DbBackupState`] from the registry on drop unless
/// disarmed. Used by `start_backup_inner` so that a failure between
/// registration and a successfully returned handle does not leave a dangling
/// registration a later rollback would flip.
struct DeregisterGuard {
    registry: Option<Arc<DbBackupRegistry>>,
    state: Arc<DbBackupState>,
    armed: bool,
}

impl Drop for DeregisterGuard {
    fn drop(&mut self) {
        if self.armed
            && let Some(reg) = &self.registry
        {
            reg.remove(&self.state);
        }
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
            None,
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

    /// FID-BACKUP-2 (JE `DbBackup`, `DbBackup.java:488/606`,
    /// `RepImpl.invalidateBackups`): a concurrent replication rollback that
    /// overwrites log files in the backup set must invalidate the backup, so
    /// `end_backup` fails with a `LogOverwriteException` equivalent and the
    /// copied files are discarded.
    ///
    /// Witness: RED at 81b2f048 (no invalidation concept: `end_backup`
    /// returns `Ok` regardless of a concurrent rollback). GREEN after the fix
    /// (`invalidate_backups` flips the registered state, `end_backup`
    /// returns `Err`).
    #[test]
    fn concurrent_rollback_invalidates_backup() {
        let dir = tempdir().unwrap();
        let file_manager = fm(dir.path());
        file_manager.create_file(0).unwrap();
        let protector = Arc::new(FileProtector::new());
        let registry = Arc::new(DbBackupRegistry::default());

        let backup = DbBackup::start_backup(
            Arc::clone(&protector),
            Arc::clone(&file_manager),
            None,
            Some(Arc::clone(&registry)),
        )
        .unwrap();

        // A replication rollback overwrites log files from file 0 onward
        // (JE Replay.rollback -> RepImpl.invalidateBackups(fileNumber)).
        registry.invalidate_backups(0);

        let res = backup.end_backup();
        assert!(
            res.is_err(),
            "end_backup must fail after a concurrent rollback invalidated \
             the backup set (JE LogOverwriteException)"
        );
        assert!(
            res.unwrap_err().to_string().contains("overwritten log files"),
            "error must describe the overwrite"
        );
    }

    /// FID-BACKUP-2: while a rollback is in progress (`backup_prohibited`),
    /// starting a new backup must be refused (JE `RepImpl.addDbBackup`
    /// returns false -> `startBackup` throws `LogOverwriteException`).
    #[test]
    fn prohibited_backup_is_refused() {
        let dir = tempdir().unwrap();
        let file_manager = fm(dir.path());
        file_manager.create_file(0).unwrap();
        let protector = Arc::new(FileProtector::new());
        let registry = Arc::new(DbBackupRegistry::default());

        registry.set_backup_prohibited(true);
        let res = DbBackup::start_backup(
            Arc::clone(&protector),
            Arc::clone(&file_manager),
            None,
            Some(Arc::clone(&registry)),
        );
        assert!(
            res.is_err(),
            "start_backup must be refused while a rollback prohibits backups"
        );

        // Once the rollback finishes and un-prohibits, a backup succeeds.
        registry.set_backup_prohibited(false);
        let backup = DbBackup::start_backup(
            Arc::clone(&protector),
            Arc::clone(&file_manager),
            None,
            Some(Arc::clone(&registry)),
        )
        .unwrap();
        backup.end_backup().unwrap();
    }

    /// A clean backup (no rollback) with a registry must end successfully and
    /// deregister, so a later rollback does not flip a dead state.
    #[test]
    fn clean_backup_with_registry_ends_ok_and_deregisters() {
        let dir = tempdir().unwrap();
        let file_manager = fm(dir.path());
        file_manager.create_file(0).unwrap();
        let protector = Arc::new(FileProtector::new());
        let registry = Arc::new(DbBackupRegistry::default());

        let backup = DbBackup::start_backup(
            Arc::clone(&protector),
            Arc::clone(&file_manager),
            None,
            Some(Arc::clone(&registry)),
        )
        .unwrap();
        backup.end_backup().unwrap();

        // After end_backup, a rollback must not affect anything (deregistered).
        registry.invalidate_backups(0);
    }
}
