//! Error types for the DBI layer.
//!

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

    /// DBI-14: the comparator supplied on open does not match the comparator
    /// identity persisted in the database record, and the corresponding
    /// override flag was not set.
    ///
    /// Mirrors JE's comparator mismatch semantics: a database whose keys are
    /// ordered by a persisted comparator must be reopened with a matching
    /// comparator, or its sort order would be silently corrupted.
    /// (`DatabaseImpl.ComparatorReader` / `setOverrideBtreeComparator`).
    #[error(
        "comparator mismatch for database '{name}': persisted {kind} \
         comparator identity {persisted:?} but configured {configured:?} \
         (set the override flag to replace it)"
    )]
    ComparatorMismatch {
        /// Database name.
        name: String,
        /// "btree" or "duplicate".
        kind: &'static str,
        /// Identity persisted in the database record (None = byte order).
        persisted: Option<String>,
        /// Identity supplied in this open's config (None = byte order).
        configured: Option<String>,
    },

    /// Database cannot be deleted or renamed while handles are open.
    ///
    /// Thrown when
    /// `EnvironmentImpl.dbRemove()`/`dbRename()` detect open handles.
    #[error("database is in use (open handles exist): {0}")]
    DatabaseInUse(String),

    /// Environment failure.
    #[error("environment failure: {reason}")]
    EnvironmentFailure { reason: String },

    /// Recovery failed during environment open.
    ///
    /// Distinct from the more general `EnvironmentFailure` so callers
    /// can branch on "recovery couldn't replay the WAL" specifically.
    /// Wave 1C audit cleanup (transaction-env F22 typed recovery-
    /// failure variant): previously every recovery failure surfaced
    /// as `EnvironmentFailure { reason: "recovery failed: ..." }`,
    /// which forced callers to string-match the prefix.
    #[error("recovery failed: {reason}")]
    RecoveryFailure { reason: String },

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

    /// Log subsystem error.
    #[error("log error: {0}")]
    LogError(#[from] noxu_log::NoxuLogError),
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
