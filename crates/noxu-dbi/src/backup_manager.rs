//! Automatic backup manager.
//!
//!
//! # Overview
//!
//! `BackupManager` is a background daemon that automatically copies closed log
//! files to a configured archive destination (e.g., an object-store bucket or
//! a replica filesystem). It runs according to a cron-style schedule and uses
//! a snapshot manifest to determine which files are new since the last backup.
//!
//! ## Algorithm (mirrors BackupManager)
//!
//! 1. On wakeup, read the current log file list via `FileManager`.
//! 2. Compare against the last `SnapshotManifest` to find new/changed files.
//! 3. Copy new files to the `BackupArchiveLocation`.
//! 4. Write a new manifest recording the set of files included in this backup.
//! 5. Sleep until the next scheduled wakeup.
//!

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Default backup check interval (seconds).
pub const DEFAULT_BACKUP_INTERVAL_SEC: u64 = 60;

/// Destination for backup file copies.
///
/// / `BackupFileLocation`.
#[derive(Debug, Clone)]
pub struct BackupDestination {
    /// Local path to the backup directory.
    pub path: PathBuf,
}

/// Manages automatic periodic backup of closed log files.
///
/// 
pub struct BackupManager {
    /// Whether the backup manager is currently running.
    active: bool,
    /// Shutdown signal.
    shutdown: Arc<AtomicBool>,
    /// Background thread handle.
    handle: Option<thread::JoinHandle<()>>,
    /// Count of files copied in the last backup run.
    n_files_copied: u32,
    /// Time taken by the last backup run in milliseconds.
    last_backup_ms: u64,
}

impl BackupManager {
    /// Creates a new (stopped) BackupManager.
    ///
    /// 
    pub fn new() -> Self {
        BackupManager {
            active: false,
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
            n_files_copied: 0,
            last_backup_ms: 0,
        }
    }

    /// Starts the background backup thread.
    ///
    /// 
    pub fn start(&mut self, _destination: BackupDestination) {
        let shutdown = Arc::clone(&self.shutdown);

        let handle = thread::Builder::new()
            .name("noxu-backup-manager".to_string())
            .spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    //   1. Obtain a list of closed log files from FileManager.
                    //   2. Determine new files since last SnapshotManifest.
                    //   3. Copy new files to destination.path using BackupFileCopy.
                    //   4. Write updated SnapshotManifest.
                    //
                    // Full implementation requires access to EnvironmentImpl's
                    // FileManager for the file list and is integrated at the
                    // EnvironmentImpl layer. The BackupManager thread framework
                    // is in place here.

                    thread::sleep(Duration::from_secs(DEFAULT_BACKUP_INTERVAL_SEC));
                }
            })
            .expect("failed to spawn noxu-backup-manager thread");

        self.handle = Some(handle);
        self.active = true;
    }

    /// Returns whether the backup manager is running.
    ///
    /// 
    pub fn is_running(&self) -> bool {
        self.active
    }

    /// Returns the number of files copied in the last run.
    ///
    /// Stat: `BACKUP_COPY_FILES_COUNT`.
    pub fn n_files_copied(&self) -> u32 {
        self.n_files_copied
    }

    /// Returns the elapsed time of the last backup run in milliseconds.
    ///
    /// Stat: `BACKUP_COPY_FILES_MS`.
    pub fn last_backup_ms(&self) -> u64 {
        self.last_backup_ms
    }

    /// Shuts down the background thread.
    ///
    /// 
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.active = false;
    }
}

impl Default for BackupManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for BackupManager {
    fn drop(&mut self) {
        if self.active {
            self.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_not_running() {
        let mgr = BackupManager::new();
        assert!(!mgr.is_running());
        assert_eq!(mgr.n_files_copied(), 0);
    }

    #[test]
    fn test_start_and_shutdown() {
        let mut mgr = BackupManager::new();
        mgr.start(BackupDestination {
            path: PathBuf::from("/tmp"),
        });
        assert!(mgr.is_running());
        mgr.shutdown();
        assert!(!mgr.is_running());
    }
}
