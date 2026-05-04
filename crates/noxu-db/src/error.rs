//! Error types for Noxu DB.
//!
//! Port of exception types from `com.sleepycat.je` package.

use thiserror::Error;

/// Errors that can occur when using Noxu DB.
///
/// Port of Berkeley DB Java Edition exception hierarchy.
#[derive(Debug, Error)]
pub enum NoxuError {
    /// A fatal condition has occurred that will cause the environment to close.
    #[error("environment failure: {0}")]
    EnvironmentFailure(String),

    /// The requested database was not found in the environment.
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    /// An attempt was made to create a database that already exists.
    #[error("database already exists: {0}")]
    DatabaseAlreadyExists(String),

    /// A lock conflict occurred (e.g., deadlock or timeout).
    #[error("lock conflict: {0}")]
    LockConflict(String),

    /// A deadlock was detected between two or more transactions.
    #[error("deadlock detected")]
    DeadlockDetected,

    /// The transaction was aborted.
    #[error("transaction aborted: {0}")]
    TransactionAborted(String),

    /// An operation was attempted on a closed cursor.
    #[error("cursor closed")]
    CursorClosed,

    /// An illegal argument was provided to a method.
    #[error("illegal argument: {0}")]
    IllegalArgument(String),

    /// The operation is not allowed in the current state.
    #[error("operation not allowed: {0}")]
    OperationNotAllowed(String),

    /// An operation was attempted on a closed database.
    #[error("database closed")]
    DatabaseClosed,

    /// An operation was attempted on a closed environment.
    #[error("environment closed")]
    EnvironmentClosed,

    /// An I/O error occurred.
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    /// A key or data item was not found.
    #[error("not found")]
    NotFound,

    /// The key already exists (for noOverwrite operations).
    #[error("key already exists")]
    KeyExists,

    /// A secondary database integrity constraint was violated.
    #[error("secondary integrity constraint violated: {0}")]
    SecondaryIntegrityException(String),

    /// A version mismatch occurred.
    #[error("version mismatch: {0}")]
    VersionMismatch(String),

    /// The database is in read-only mode.
    #[error("read-only mode")]
    ReadOnly,

    /// The operation timed out.
    #[error("operation timed out")]
    Timeout,

    /// Invalid operation.
    #[error("invalid operation: {0}")]
    InvalidOperation(String),
}

impl NoxuError {
    /// Helper to create an EnvironmentFailure error.
    pub fn environment(msg: impl Into<String>) -> Self {
        NoxuError::EnvironmentFailure(msg.into())
    }

    /// Helper to create an OperationNotAllowed error (for database-level errors).
    pub fn database(msg: impl Into<String>) -> Self {
        NoxuError::OperationNotAllowed(msg.into())
    }

    /// Helper to create an IllegalArgument error.
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        NoxuError::IllegalArgument(msg.into())
    }
}

impl From<noxu_dbi::DbiError> for NoxuError {
    fn from(e: noxu_dbi::DbiError) -> Self {
        NoxuError::OperationNotAllowed(e.to_string())
    }
}

/// Result type for Noxu DB operations.
pub type Result<T> = std::result::Result<T, NoxuError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = NoxuError::DatabaseNotFound("test_db".to_string());
        assert_eq!(err.to_string(), "database not found: test_db");
    }

    #[test]
    fn test_environment_failure() {
        let err = NoxuError::EnvironmentFailure("disk full".to_string());
        assert!(err.to_string().contains("environment failure"));
    }

    #[test]
    fn test_deadlock_detected() {
        let err = NoxuError::DeadlockDetected;
        assert_eq!(err.to_string(), "deadlock detected");
    }

    #[test]
    fn test_cursor_closed() {
        let err = NoxuError::CursorClosed;
        assert_eq!(err.to_string(), "cursor closed");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err =
            std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: NoxuError = io_err.into();
        assert!(matches!(err, NoxuError::IoError(_)));
    }

    #[test]
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        assert_eq!(ok_result.unwrap(), 42);

        let err_result: Result<i32> = Err(NoxuError::NotFound);
        assert!(err_result.is_err());
    }

    #[test]
    fn test_not_found() {
        let err = NoxuError::NotFound;
        assert_eq!(err.to_string(), "not found");
    }

    #[test]
    fn test_key_exists() {
        let err = NoxuError::KeyExists;
        assert_eq!(err.to_string(), "key already exists");
    }

    #[test]
    fn test_timeout() {
        let err = NoxuError::Timeout;
        assert_eq!(err.to_string(), "operation timed out");
    }

    #[test]
    fn test_from_dbi_error() {
        let dbi_err = noxu_dbi::DbiError::CursorClosed;
        let err: NoxuError = NoxuError::from(dbi_err);
        assert!(matches!(err, NoxuError::OperationNotAllowed(_)));
        assert!(err.to_string().contains("cursor closed"));
    }

    #[test]
    fn test_database_already_exists() {
        let e = NoxuError::DatabaseAlreadyExists("mydb".into());
        assert!(e.to_string().contains("mydb"));
    }

    #[test]
    fn test_lock_conflict() {
        let e = NoxuError::LockConflict("timeout".into());
        assert!(e.to_string().contains("lock conflict"));
    }

    #[test]
    fn test_transaction_aborted() {
        let e = NoxuError::TransactionAborted("rolled back".into());
        assert!(e.to_string().contains("transaction aborted"));
    }

    #[test]
    fn test_operation_not_allowed() {
        let e = NoxuError::OperationNotAllowed("read only".into());
        assert!(e.to_string().contains("operation not allowed"));
    }

    #[test]
    fn test_database_closed() {
        let e = NoxuError::DatabaseClosed;
        assert_eq!(e.to_string(), "database closed");
    }

    #[test]
    fn test_environment_closed() {
        let e = NoxuError::EnvironmentClosed;
        assert_eq!(e.to_string(), "environment closed");
    }

    #[test]
    fn test_secondary_integrity_exception() {
        let e = NoxuError::SecondaryIntegrityException("stale key".into());
        assert!(e.to_string().contains("secondary integrity"));
    }

    #[test]
    fn test_version_mismatch() {
        let e = NoxuError::VersionMismatch("v1 vs v2".into());
        assert!(e.to_string().contains("version mismatch"));
    }

    #[test]
    fn test_read_only() {
        let e = NoxuError::ReadOnly;
        assert_eq!(e.to_string(), "read-only mode");
    }

    #[test]
    fn test_invalid_operation() {
        let e = NoxuError::InvalidOperation("bad state".into());
        assert!(e.to_string().contains("invalid operation"));
    }

    #[test]
    fn test_helpers() {
        assert!(matches!(NoxuError::environment("x"), NoxuError::EnvironmentFailure(_)));
        assert!(matches!(NoxuError::database("x"), NoxuError::OperationNotAllowed(_)));
        assert!(matches!(NoxuError::invalid_argument("x"), NoxuError::IllegalArgument(_)));
    }
}
