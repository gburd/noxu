//! Network restore for copying database files from another node.
//!
//! Port of `com.sleepycat.je.rep.NetworkRestore`. JE's NetworkRestore
//! copies log files from a network peer to restore a node that has fallen
//! too far behind the replication stream. This is used when a replica
//! discovers an `InsufficientLogException`  -  its local log files are too
//! old for the feeder to supply a contiguous stream.

use std::time::Duration;

use parking_lot::Mutex;

use crate::error::{RepError, Result};

/// Configuration for a network restore operation.
///
/// Corresponds to JE's `NetworkRestoreConfig`. Specifies the source node
/// to copy from and whether existing log files should be retained.
#[derive(Debug, Clone)]
pub struct NetworkRestoreConfig {
    /// Name of the source node to restore from.
    pub source_node: String,
    /// Hostname of the source node.
    pub source_host: String,
    /// Port of the source node.
    pub source_port: u16,
    /// Whether to retain existing log files (rename rather than delete).
    pub retain_log_files: bool,
}

/// The current state of a network restore operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreState {
    /// The restore has not yet started.
    NotStarted,
    /// The restore is currently transferring files.
    InProgress,
    /// The restore completed successfully.
    Completed,
    /// The restore failed.
    Failed,
}

/// Progress information for a network restore operation.
#[derive(Debug, Clone)]
pub struct RestoreProgress {
    /// Current state of the restore.
    pub state: RestoreState,
    /// Total bytes transferred so far.
    pub bytes_transferred: u64,
    /// Total files transferred so far.
    pub files_transferred: u32,
    /// Time elapsed since the restore started.
    pub elapsed: Duration,
}

/// A network restore operation that copies database files from a peer node.
///
/// Port of JE's `NetworkRestore`. Manages the lifecycle of a restore:
/// starting the transfer, tracking progress, and completing or failing.
pub struct NetworkRestore {
    /// Configuration for this restore.
    config: NetworkRestoreConfig,
    /// Current restore state.
    state: Mutex<RestoreState>,
    /// Progress tracking.
    progress: Mutex<RestoreProgress>,
}

impl NetworkRestore {
    /// Create a new network restore with the given configuration.
    pub fn new(config: NetworkRestoreConfig) -> Self {
        Self {
            config,
            state: Mutex::new(RestoreState::NotStarted),
            progress: Mutex::new(RestoreProgress {
                state: RestoreState::NotStarted,
                bytes_transferred: 0,
                files_transferred: 0,
                elapsed: Duration::ZERO,
            }),
        }
    }

    /// Get the current restore state.
    pub fn get_state(&self) -> RestoreState {
        *self.state.lock()
    }

    /// Get a snapshot of the current progress.
    pub fn get_progress(&self) -> RestoreProgress {
        self.progress.lock().clone()
    }

    /// Get the restore configuration.
    pub fn get_config(&self) -> &NetworkRestoreConfig {
        &self.config
    }

    /// Start the network restore.
    ///
    /// Transitions from `NotStarted` to `InProgress`. In the full
    /// implementation, this would initiate the network connection and
    /// begin transferring files.
    pub fn start(&self) -> Result<()> {
        let mut state = self.state.lock();
        match *state {
            RestoreState::NotStarted => {
                *state = RestoreState::InProgress;
                let mut progress = self.progress.lock();
                progress.state = RestoreState::InProgress;
                Ok(())
            }
            RestoreState::Completed => Err(RepError::NetworkRestoreError(
                "restore already completed".into(),
            )),
            RestoreState::Failed => Err(RepError::NetworkRestoreError(
                "restore already failed; create a new instance".into(),
            )),
            RestoreState::InProgress => Err(RepError::NetworkRestoreError(
                "restore already in progress".into(),
            )),
        }
    }

    /// Update the progress of an in-progress restore.
    ///
    /// # Arguments
    /// * `bytes` - Total bytes transferred so far.
    /// * `files` - Total files transferred so far.
    pub fn update_progress(&self, bytes: u64, files: u32) {
        let mut progress = self.progress.lock();
        progress.bytes_transferred = bytes;
        progress.files_transferred = files;
    }

    /// Update the elapsed time for progress tracking.
    pub fn update_elapsed(&self, elapsed: Duration) {
        let mut progress = self.progress.lock();
        progress.elapsed = elapsed;
    }

    /// Mark the restore as completed successfully.
    pub fn complete(&self) -> Result<()> {
        let mut state = self.state.lock();
        match *state {
            RestoreState::InProgress => {
                *state = RestoreState::Completed;
                let mut progress = self.progress.lock();
                progress.state = RestoreState::Completed;
                Ok(())
            }
            other => Err(RepError::NetworkRestoreError(format!(
                "cannot complete from state {:?}",
                other
            ))),
        }
    }

    /// Mark the restore as failed.
    pub fn fail(&self) -> Result<()> {
        let mut state = self.state.lock();
        match *state {
            RestoreState::InProgress => {
                *state = RestoreState::Failed;
                let mut progress = self.progress.lock();
                progress.state = RestoreState::Failed;
                Ok(())
            }
            other => Err(RepError::NetworkRestoreError(format!(
                "cannot fail from state {:?}",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> NetworkRestoreConfig {
        NetworkRestoreConfig {
            source_node: "node1".into(),
            source_host: "192.168.1.10".into(),
            source_port: 5001,
            retain_log_files: false,
        }
    }

    #[test]
    fn test_initial_state() {
        let restore = NetworkRestore::new(test_config());
        assert_eq!(restore.get_state(), RestoreState::NotStarted);

        let progress = restore.get_progress();
        assert_eq!(progress.state, RestoreState::NotStarted);
        assert_eq!(progress.bytes_transferred, 0);
        assert_eq!(progress.files_transferred, 0);
        assert_eq!(progress.elapsed, Duration::ZERO);
    }

    #[test]
    fn test_start() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        assert_eq!(restore.get_state(), RestoreState::InProgress);
        assert_eq!(restore.get_progress().state, RestoreState::InProgress);
    }

    #[test]
    fn test_start_twice_fails() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        let result = restore.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_update_progress() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();

        restore.update_progress(1024 * 1024, 3);
        let progress = restore.get_progress();
        assert_eq!(progress.bytes_transferred, 1024 * 1024);
        assert_eq!(progress.files_transferred, 3);
    }

    #[test]
    fn test_update_elapsed() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();

        let elapsed = Duration::from_secs(42);
        restore.update_elapsed(elapsed);
        assert_eq!(restore.get_progress().elapsed, elapsed);
    }

    #[test]
    fn test_complete() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.complete().unwrap();
        assert_eq!(restore.get_state(), RestoreState::Completed);
        assert_eq!(restore.get_progress().state, RestoreState::Completed);
    }

    #[test]
    fn test_complete_from_not_started_fails() {
        let restore = NetworkRestore::new(test_config());
        let result = restore.complete();
        assert!(result.is_err());
    }

    #[test]
    fn test_fail() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.fail().unwrap();
        assert_eq!(restore.get_state(), RestoreState::Failed);
        assert_eq!(restore.get_progress().state, RestoreState::Failed);
    }

    #[test]
    fn test_fail_from_not_started_fails() {
        let restore = NetworkRestore::new(test_config());
        let result = restore.fail();
        assert!(result.is_err());
    }

    #[test]
    fn test_start_after_completed_fails() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.complete().unwrap();
        let result = restore.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_start_after_failed_fails() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.fail().unwrap();
        let result = restore.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_accessor() {
        let config = test_config();
        let restore = NetworkRestore::new(config);
        assert_eq!(restore.get_config().source_node, "node1");
        assert_eq!(restore.get_config().source_host, "192.168.1.10");
        assert_eq!(restore.get_config().source_port, 5001);
        assert!(!restore.get_config().retain_log_files);
    }

    #[test]
    fn test_retain_log_files_config() {
        let mut config = test_config();
        config.retain_log_files = true;
        let restore = NetworkRestore::new(config);
        assert!(restore.get_config().retain_log_files);
    }

    #[test]
    fn test_full_lifecycle() {
        let restore = NetworkRestore::new(test_config());

        assert_eq!(restore.get_state(), RestoreState::NotStarted);

        restore.start().unwrap();
        assert_eq!(restore.get_state(), RestoreState::InProgress);

        restore.update_progress(512, 1);
        restore.update_progress(2048, 2);
        restore.update_elapsed(Duration::from_secs(5));

        let progress = restore.get_progress();
        assert_eq!(progress.bytes_transferred, 2048);
        assert_eq!(progress.files_transferred, 2);
        assert_eq!(progress.elapsed, Duration::from_secs(5));

        restore.complete().unwrap();
        assert_eq!(restore.get_state(), RestoreState::Completed);
    }

    #[test]
    fn test_fail_lifecycle() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.update_progress(256, 1);
        restore.fail().unwrap();

        assert_eq!(restore.get_state(), RestoreState::Failed);
        // Progress data should still be accessible after failure.
        let progress = restore.get_progress();
        assert_eq!(progress.bytes_transferred, 256);
        assert_eq!(progress.files_transferred, 1);
    }
}
