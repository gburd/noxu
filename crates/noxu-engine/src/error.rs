//! Error types for the Noxu DB engine.

use thiserror::Error;

/// Errors that can occur during engine operations.
#[derive(Debug, Error)]
pub enum EngineError {
    /// Environment is not open.
    #[error("environment not open")]
    EnvironmentNotOpen,

    /// Environment is already open.
    #[error("environment already open")]
    EnvironmentAlreadyOpen,

    /// Environment has been closed.
    #[error("environment closed")]
    EnvironmentClosed,

    /// Environment has failed and is invalid.
    #[error("environment failure: {0}")]
    EnvironmentFailure(String),

    /// Invalid configuration parameter.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Database operation error.
    #[error("database error: {0}")]
    DatabaseError(String),

    /// Lock conflict occurred.
    #[error("lock conflict: {0}")]
    LockConflict(String),

    /// Deadlock was detected.
    #[error("deadlock detected")]
    DeadlockDetected,

    /// Transaction error.
    #[error("transaction error: {0}")]
    TransactionError(String),

    /// I/O error occurred.
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error from the DBI layer.
    #[error("dbi error: {0}")]
    DbiError(#[from] noxu_dbi::DbiError),

    /// Error from the evictor.
    #[error("evictor error: {0}")]
    EvictorError(#[from] noxu_evictor::EvictorError),

    /// Error from the cleaner.
    #[error("cleaner error: {0}")]
    CleanerError(#[from] noxu_cleaner::CleanerError),

    /// Error from the recovery subsystem.
    #[error("recovery error: {0}")]
    RecoveryError(#[from] noxu_recovery::RecoveryError),
}

/// Result type for engine operations.
pub type Result<T> = std::result::Result<T, EngineError>;

impl From<String> for EngineError {
    fn from(msg: String) -> Self {
        EngineError::DatabaseError(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = EngineError::EnvironmentNotOpen;
        assert_eq!(err.to_string(), "environment not open");

        let err =
            EngineError::EnvironmentFailure("corruption detected".to_string());
        assert_eq!(err.to_string(), "environment failure: corruption detected");

        let err =
            EngineError::InvalidConfig("cache size too small".to_string());
        assert_eq!(
            err.to_string(),
            "invalid configuration: cache size too small"
        );
    }

    #[test]
    fn test_error_from_io() {
        let io_err =
            std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: EngineError = io_err.into();
        assert!(matches!(err, EngineError::IoError(_)));
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn test_lock_errors() {
        let err =
            EngineError::LockConflict("timeout waiting for lock".to_string());
        assert!(err.to_string().contains("lock conflict"));

        let err = EngineError::DeadlockDetected;
        assert_eq!(err.to_string(), "deadlock detected");
    }

    #[test]
    fn test_transaction_error() {
        let err =
            EngineError::TransactionError("txn already aborted".to_string());
        assert!(err.to_string().contains("transaction error"));
    }

    #[test]
    fn test_database_error() {
        let err = EngineError::DatabaseError("database not found".to_string());
        assert!(err.to_string().contains("database error"));
    }
}
