//! Error types for noxu-recovery.
//!
//! Defines error conditions that can occur during recovery and checkpointing.

use thiserror::Error;

/// Errors that can occur during recovery and checkpointing operations.
///
#[derive(Debug, Error)]
pub enum RecoveryError {
    /// Recovery process failed.
    #[error("recovery failed: {0}")]
    RecoveryFailed(String),

    /// Checkpoint operation error.
    #[error("checkpoint error: {0}")]
    CheckpointError(String),

    /// Invalid checkpoint data encountered.
    #[error("invalid checkpoint: {0}")]
    InvalidCheckpoint(String),

    /// Required log file not found.
    #[error("log file not found: {file_number}")]
    LogFileNotFound { file_number: u32 },

    /// Rollback operation error.
    #[error("rollback error: {0}")]
    RollbackError(String),

    /// I/O error during recovery.
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Result type for noxu-recovery operations.
pub type Result<T> = std::result::Result<T, RecoveryError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recovery_failed_error() {
        let err = RecoveryError::RecoveryFailed("test failure".to_string());
        assert_eq!(err.to_string(), "recovery failed: test failure");
    }

    #[test]
    fn test_checkpoint_error() {
        let err =
            RecoveryError::CheckpointError("checkpoint failed".to_string());
        assert_eq!(err.to_string(), "checkpoint error: checkpoint failed");
    }

    #[test]
    fn test_invalid_checkpoint_error() {
        let err = RecoveryError::InvalidCheckpoint("bad data".to_string());
        assert_eq!(err.to_string(), "invalid checkpoint: bad data");
    }

    #[test]
    fn test_log_file_not_found_error() {
        let err = RecoveryError::LogFileNotFound { file_number: 42 };
        assert_eq!(err.to_string(), "log file not found: 42");
    }

    #[test]
    fn test_rollback_error() {
        let err = RecoveryError::RollbackError("rollback failed".to_string());
        assert_eq!(err.to_string(), "rollback error: rollback failed");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err =
            std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: RecoveryError = io_err.into();
        assert!(err.to_string().contains("io error"));
    }
}
