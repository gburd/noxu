//! Error types for the DBI layer.
//!
//! Port of error handling from `com.sleepycat.je.dbi`.

use thiserror::Error;

/// Errors that can occur in the DBI layer.
#[derive(Debug, Error)]
pub enum DbiError {
    /// Database not found.
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    /// Database already exists.
    #[error("database already exists: {0}")]
    DatabaseAlreadyExists(String),

    /// Database already exists (compatibility alias).
    #[error("database already exists: {0}")]
    DatabaseExists(String),

    /// Environment failure.
    #[error("environment failure: {reason}")]
    EnvironmentFailure { reason: String },

    /// Environment is not open.
    #[error("environment not open")]
    EnvironmentNotOpen,

    /// Environment is locked by another process.
    #[error("environment locked: {0}")]
    EnvironmentLocked(String),

    /// Cursor not initialized.
    #[error("cursor not initialized")]
    CursorNotInitialized,

    /// Cursor is closed.
    #[error("cursor closed")]
    CursorClosed,

    /// Operation failed.
    #[error("operation status: {0}")]
    OperationFailed(String),

    /// Lock conflict occurred.
    #[error("lock conflict: {0}")]
    LockConflict(String),

    /// Transaction error.
    #[error("transaction error: {0}")]
    TxnError(#[from] noxu_txn::TxnError),

    /// Tree error.
    #[error("tree error: {0}")]
    TreeError(#[from] noxu_tree::TreeError),

    /// I/O error.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Result type for DBI operations.
pub type Result<T> = std::result::Result<T, DbiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = DbiError::DatabaseNotFound("test_db".to_string());
        assert_eq!(err.to_string(), "database not found: test_db");

        let err = DbiError::EnvironmentNotOpen;
        assert_eq!(err.to_string(), "environment not open");
    }

    #[test]
    fn test_error_from_io() {
        let io_err =
            std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let dbi_err: DbiError = io_err.into();
        assert!(matches!(dbi_err, DbiError::IoError(_)));
    }
}
