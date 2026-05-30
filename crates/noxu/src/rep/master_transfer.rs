//! Master transfer support.
//!
//! allows transferring
//! master status from the current master to a designated target node.

use crate::sync::Mutex;
use std::time::{Duration, Instant};

use crate::rep::error::{RepError, Result};

/// Configuration for transferring master status to another node.
#[derive(Debug, Clone)]
pub struct MasterTransferConfig {
    /// Name of the target node to become master.
    pub target_node: String,
    /// Maximum time to wait for the transfer to complete.
    pub timeout: Duration,
    /// Whether to force the transfer even if the target is behind.
    pub force: bool,
}

impl MasterTransferConfig {
    /// Creates a new master transfer configuration.
    pub fn new(target_node: String, timeout: Duration) -> Self {
        Self { target_node, timeout, force: false }
    }

    /// Set whether to force the transfer.
    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }
}

/// State of an in-progress master transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferState {
    /// Transfer has not been started.
    NotStarted,
    /// Transfer is in progress.
    InProgress,
    /// Transfer completed successfully.
    Completed,
    /// Transfer failed.
    Failed,
    /// Transfer timed out.
    TimedOut,
}

/// Manages a master transfer operation.
pub struct MasterTransfer {
    config: MasterTransferConfig,
    state: Mutex<TransferState>,
    start_time: Mutex<Option<Instant>>,
}

impl MasterTransfer {
    /// Creates a new master transfer.
    pub fn new(config: MasterTransferConfig) -> Self {
        Self {
            config,
            state: Mutex::new(TransferState::NotStarted),
            start_time: Mutex::new(None),
        }
    }

    /// Get the current transfer state.
    pub fn get_state(&self) -> TransferState {
        *self.state.lock()
    }

    /// Get the target node name.
    pub fn get_target(&self) -> &str {
        &self.config.target_node
    }

    /// Get the transfer configuration.
    pub fn get_config(&self) -> &MasterTransferConfig {
        &self.config
    }

    /// Start the transfer.
    pub fn start(&self) -> Result<()> {
        let mut state = self.state.lock();
        if *state != TransferState::NotStarted {
            return Err(RepError::StateError(
                "Transfer already started".to_string(),
            ));
        }
        *state = TransferState::InProgress;
        *self.start_time.lock() = Some(Instant::now());
        Ok(())
    }

    /// Mark the transfer as completed.
    pub fn complete(&self) -> Result<()> {
        let mut state = self.state.lock();
        if *state != TransferState::InProgress {
            return Err(RepError::StateError(
                "Transfer not in progress".to_string(),
            ));
        }
        *state = TransferState::Completed;
        Ok(())
    }

    /// Mark the transfer as failed.
    pub fn fail(&self, _reason: &str) -> Result<()> {
        let mut state = self.state.lock();
        if *state != TransferState::InProgress {
            return Err(RepError::StateError(
                "Transfer not in progress".to_string(),
            ));
        }
        *state = TransferState::Failed;
        Ok(())
    }

    /// Check if the transfer has timed out.
    pub fn is_timed_out(&self) -> bool {
        if let Some(start) = *self.start_time.lock() {
            start.elapsed() > self.config.timeout
        } else {
            false
        }
    }

    /// Get the elapsed time since the transfer started.
    pub fn elapsed(&self) -> Option<Duration> {
        self.start_time.lock().map(|t| t.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MasterTransferConfig {
        MasterTransferConfig::new("node2".to_string(), Duration::from_secs(30))
    }

    #[test]
    fn test_config_builder() {
        let config = MasterTransferConfig::new(
            "node2".to_string(),
            Duration::from_secs(30),
        )
        .with_force(true);
        assert_eq!(config.target_node, "node2");
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert!(config.force);
    }

    #[test]
    fn test_initial_state() {
        let transfer = MasterTransfer::new(test_config());
        assert_eq!(transfer.get_state(), TransferState::NotStarted);
        assert_eq!(transfer.get_target(), "node2");
    }

    #[test]
    fn test_start() {
        let transfer = MasterTransfer::new(test_config());
        assert!(transfer.start().is_ok());
        assert_eq!(transfer.get_state(), TransferState::InProgress);
        assert!(transfer.elapsed().is_some());
    }

    #[test]
    fn test_start_twice_fails() {
        let transfer = MasterTransfer::new(test_config());
        transfer.start().unwrap();
        assert!(transfer.start().is_err());
    }

    #[test]
    fn test_complete() {
        let transfer = MasterTransfer::new(test_config());
        transfer.start().unwrap();
        assert!(transfer.complete().is_ok());
        assert_eq!(transfer.get_state(), TransferState::Completed);
    }

    #[test]
    fn test_complete_without_start_fails() {
        let transfer = MasterTransfer::new(test_config());
        assert!(transfer.complete().is_err());
    }

    #[test]
    fn test_fail() {
        let transfer = MasterTransfer::new(test_config());
        transfer.start().unwrap();
        assert!(transfer.fail("test reason").is_ok());
        assert_eq!(transfer.get_state(), TransferState::Failed);
    }

    #[test]
    fn test_timeout_not_started() {
        let transfer = MasterTransfer::new(test_config());
        assert!(!transfer.is_timed_out());
    }

    #[test]
    fn test_timeout_short() {
        let config = MasterTransferConfig::new(
            "node2".to_string(),
            Duration::from_millis(1),
        );
        let transfer = MasterTransfer::new(config);
        transfer.start().unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert!(transfer.is_timed_out());
    }

    #[test]
    fn test_timeout_long() {
        let config = MasterTransferConfig::new(
            "node2".to_string(),
            Duration::from_secs(60),
        );
        let transfer = MasterTransfer::new(config);
        transfer.start().unwrap();
        assert!(!transfer.is_timed_out());
    }
}
