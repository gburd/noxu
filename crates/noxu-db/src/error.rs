//! Error types for Noxu DB.
//!

use thiserror::Error;

/// Errors that can occur when using Noxu DB.
///
/// Mirrors JE's exception hierarchy:
///
/// - [`NoxuError::EnvironmentFailure`] — fatal; environment must be closed.
/// - Operation-failure variants — environment is still valid; the operation
///   failed.  Retryable variants are marked by [`NoxuError::is_retryable()`].
/// - HA / replication variants ([`NoxuError::InsufficientReplicas`],
///   [`NoxuError::ReplicaWrite`], [`NoxuError::RollbackRequired`]).
#[derive(Debug, Error)]
pub enum NoxuError {
    // ── Fatal ─────────────────────────────────────────────────────────────

    /// A fatal condition has occurred that will cause the environment to close.
    ///
    /// Mirrors JE `EnvironmentFailureException`.  After this error the
    /// environment must be closed and re-opened.
    #[error("environment failure: {0}")]
    EnvironmentFailure(String),

    // ── Database / cursor lifecycle ────────────────────────────────────────

    /// The requested database was not found in the environment.
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    /// An attempt was made to create a database that already exists.
    #[error("database already exists: {0}")]
    DatabaseAlreadyExists(String),

    /// An operation was attempted on a closed database.
    #[error("database closed")]
    DatabaseClosed,

    /// An operation was attempted on a closed environment.
    #[error("environment closed")]
    EnvironmentClosed,

    /// An operation was attempted on a closed cursor.
    #[error("cursor closed")]
    CursorClosed,

    // ── Lock / transaction failures ────────────────────────────────────────

    /// A lock conflict occurred (locker blocked and could not acquire).
    ///
    /// Mirrors JE `LockConflictException`.  Retryable after abort.
    #[error("lock conflict: {0}")]
    LockConflict(String),

    /// A deadlock was detected between two or more transactions.
    ///
    /// Mirrors JE `DeadlockException`.  Retryable after abort.
    #[error("deadlock detected")]
    DeadlockDetected,

    /// A lock-wait timeout expired.
    ///
    /// Mirrors JE `LockTimeoutException`.  Retryable after abort.
    #[error("lock timeout after {timeout_ms}ms")]
    LockTimeout {
        /// How long the locker waited before giving up.
        timeout_ms: u64,
    },

    /// A transaction-level timeout expired.
    ///
    /// Mirrors JE `TransactionTimeoutException`.  Retryable after abort.
    #[error("transaction timeout after {timeout_ms}ms for txn {txn_id}")]
    TransactionTimeout {
        /// Transaction-level timeout in milliseconds.
        timeout_ms: u64,
        /// ID of the timed-out transaction.
        txn_id: i64,
    },

    /// A lock was preempted by a higher-priority locker (HA).
    ///
    /// Mirrors JE `LockPreemptedException`.  The holder must release all
    /// resources and re-read before retrying.  Retryable after abort.
    #[error("lock preempted by higher-priority locker")]
    LockPreempted,

    /// The transaction was aborted.
    #[error("transaction aborted: {0}")]
    TransactionAborted(String),

    // ── Constraint violations ──────────────────────────────────────────────

    /// The key already exists (for `put_no_overwrite` / cursor `put_no_dup_data`).
    #[error("key already exists")]
    KeyExists,

    /// A unique-index constraint was violated.
    ///
    /// Mirrors JE `UniqueConstraintException`.
    #[error("unique constraint violated: {0}")]
    UniqueConstraintViolation(String),

    /// A secondary database integrity constraint was violated.
    ///
    /// Mirrors JE `SecondaryIntegrityException`.
    #[error("secondary integrity constraint violated: {0}")]
    SecondaryIntegrityException(String),

    // ── Not-found / access control ─────────────────────────────────────────

    /// A key or data item was not found.
    #[error("not found")]
    NotFound,

    /// The database or environment is in read-only mode.
    #[error("read-only mode")]
    ReadOnly,

    // ── HA / replication ───────────────────────────────────────────────────

    /// A write was attempted on a replica node.
    ///
    /// Mirrors JE `ReplicaWriteException`.
    #[error("write not allowed on replica")]
    ReplicaWrite,

    /// Insufficient replicas acknowledged the commit.
    ///
    /// Mirrors JE `InsufficientReplicasException`.
    #[error("insufficient replicas: required {required}, available {available}")]
    InsufficientReplicas {
        /// Acknowledgement quorum required.
        required: u32,
        /// Number of replicas that responded.
        available: u32,
    },

    /// The transaction must be rolled back due to a replication state change.
    ///
    /// Mirrors JE `RollbackException`.
    #[error("rollback required: {0}")]
    RollbackRequired(String),

    // ── Log / I/O ──────────────────────────────────────────────────────────

    /// A log checksum mismatch was detected (potential corruption).
    ///
    /// This is fatal when found during normal operation; the environment will
    /// be invalidated.  During recovery it may be recoverable.
    #[error("log checksum mismatch: {0}")]
    LogChecksumMismatch(String),

    /// A log file was not found.
    ///
    /// Mirrors JE `LogFileNotFoundException`.
    #[error("log file not found: {0}")]
    LogFileNotFound(String),

    /// An I/O error occurred.
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    // ── General ────────────────────────────────────────────────────────────

    /// The operation is not allowed in the current state.
    ///
    /// Mirrors JE `OperationNotAllowedException`.
    #[error("operation not allowed: {0}")]
    OperationNotAllowed(String),

    /// An illegal argument was provided to a method.
    ///
    /// Mirrors JE `IllegalArgumentException` (DB flavour).
    #[error("illegal argument: {0}")]
    IllegalArgument(String),

    /// A version mismatch occurred (e.g. on-disk format vs. code version).
    #[error("version mismatch: {0}")]
    VersionMismatch(String),

    /// The operation timed out (non-lock, non-txn — e.g. network or sync).
    #[error("operation timed out")]
    Timeout,

    /// An invalid operation was requested.
    #[error("invalid operation: {0}")]
    InvalidOperation(String),
}

impl NoxuError {
    // ── Classification helpers ─────────────────────────────────────────────

    /// Returns `true` if the failed operation may be retried after aborting
    /// the current transaction.
    ///
    /// Mirrors JE `OperationFailureException.isRetryable()`.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            NoxuError::LockConflict(_)
                | NoxuError::DeadlockDetected
                | NoxuError::LockTimeout { .. }
                | NoxuError::TransactionTimeout { .. }
                | NoxuError::LockPreempted
        )
    }

    /// Returns `true` if this error is fatal to the environment.
    ///
    /// After a fatal error the environment must be closed and re-opened;
    /// further operations will fail with `EnvironmentClosed`.
    ///
    /// Mirrors JE `EnvironmentFailureException` detection.
    pub fn is_fatal_to_environment(&self) -> bool {
        matches!(
            self,
            NoxuError::EnvironmentFailure(_) | NoxuError::LogChecksumMismatch(_)
        )
    }

    // ── Constructor helpers ────────────────────────────────────────────────

    /// Creates an `EnvironmentFailure` error.
    pub fn environment(msg: impl Into<String>) -> Self {
        NoxuError::EnvironmentFailure(msg.into())
    }

    /// Creates an `OperationNotAllowed` error.
    pub fn database(msg: impl Into<String>) -> Self {
        NoxuError::OperationNotAllowed(msg.into())
    }

    /// Creates an `IllegalArgument` error.
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        NoxuError::IllegalArgument(msg.into())
    }
}

// ── Conversions from sub-crate errors ─────────────────────────────────────

impl From<noxu_dbi::DbiError> for NoxuError {
    fn from(e: noxu_dbi::DbiError) -> Self {
        use noxu_dbi::DbiError;
        match e {
            DbiError::DatabaseNotFound(s) => NoxuError::DatabaseNotFound(s),
            DbiError::DatabaseAlreadyExists(s) | DbiError::DatabaseExists(s) => {
                NoxuError::DatabaseAlreadyExists(s)
            }
            DbiError::EnvironmentFailure { reason } => NoxuError::EnvironmentFailure(reason),
            DbiError::EnvironmentNotOpen | DbiError::EnvironmentLocked(_) => {
                NoxuError::EnvironmentClosed
            }
            DbiError::CursorClosed | DbiError::CursorNotInitialized => NoxuError::CursorClosed,
            DbiError::LockConflict(s) => NoxuError::LockConflict(s),
            DbiError::IoError(io) => NoxuError::IoError(io),
            DbiError::TxnError(txn_err) => NoxuError::from(txn_err),
            DbiError::LogError(log_err) => NoxuError::OperationNotAllowed(log_err.to_string()),
            DbiError::TreeError(tree_err) => NoxuError::OperationNotAllowed(tree_err.to_string()),
            DbiError::DatabaseInUse(s) => NoxuError::OperationNotAllowed(s),
            DbiError::OperationFailed(s) => NoxuError::OperationNotAllowed(s),
        }
    }
}

impl From<noxu_txn::TxnError> for NoxuError {
    fn from(e: noxu_txn::TxnError) -> Self {
        use noxu_txn::TxnError;
        match e {
            TxnError::Deadlock(_) => NoxuError::DeadlockDetected,
            TxnError::LockConflict(s) => NoxuError::LockConflict(s),
            TxnError::LockTimeout { timeout_ms, .. } => NoxuError::LockTimeout { timeout_ms },
            TxnError::TransactionTimeout { timeout_ms, txn_id } => {
                NoxuError::TransactionTimeout { timeout_ms, txn_id }
            }
            TxnError::LockNotAvailable { .. } => NoxuError::LockConflict("lock not available".into()),
            TxnError::RangeRestart => NoxuError::LockConflict("range restart".into()),
            TxnError::InvalidTransaction { txn_id, state } => {
                NoxuError::TransactionAborted(format!("txn {txn_id}: {state}"))
            }
            TxnError::StateError(s) => NoxuError::TransactionAborted(s),
            TxnError::LogError(log_err) => NoxuError::OperationNotAllowed(log_err.to_string()),
        }
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
        assert!(ok_result.is_ok_and(|v| v == 42));

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
        // CursorClosed now maps to NoxuError::CursorClosed (not OperationNotAllowed)
        let dbi_err = noxu_dbi::DbiError::CursorClosed;
        let err: NoxuError = NoxuError::from(dbi_err);
        assert!(matches!(err, NoxuError::CursorClosed));
        assert!(err.to_string().contains("cursor closed"));

        // DatabaseNotFound maps correctly
        let e: NoxuError = noxu_dbi::DbiError::DatabaseNotFound("x".into()).into();
        assert!(matches!(e, NoxuError::DatabaseNotFound(_)));

        // EnvironmentFailure maps correctly
        let e: NoxuError =
            noxu_dbi::DbiError::EnvironmentFailure { reason: "disk".into() }.into();
        assert!(matches!(e, NoxuError::EnvironmentFailure(_)));
    }

    #[test]
    fn test_from_txn_error() {
        use noxu_txn::{LockType, TxnError};

        let e: NoxuError = TxnError::Deadlock("cycle".into()).into();
        assert!(matches!(e, NoxuError::DeadlockDetected));

        let e: NoxuError = TxnError::LockTimeout {
            timeout_ms: 500,
            lsn: 1,
            owner: "t1".into(),
            requested_type: LockType::Write,
            requester: "t2".into(),
        }
        .into();
        assert!(matches!(e, NoxuError::LockTimeout { timeout_ms: 500 }));

        let e: NoxuError = TxnError::TransactionTimeout { timeout_ms: 1000, txn_id: 42 }.into();
        assert!(matches!(e, NoxuError::TransactionTimeout { timeout_ms: 1000, txn_id: 42 }));
    }

    #[test]
    fn test_is_retryable() {
        assert!(NoxuError::DeadlockDetected.is_retryable());
        assert!(NoxuError::LockConflict("x".into()).is_retryable());
        assert!(NoxuError::LockTimeout { timeout_ms: 500 }.is_retryable());
        assert!(NoxuError::TransactionTimeout { timeout_ms: 1000, txn_id: 1 }.is_retryable());
        assert!(NoxuError::LockPreempted.is_retryable());

        assert!(!NoxuError::NotFound.is_retryable());
        assert!(!NoxuError::EnvironmentFailure("x".into()).is_retryable());
        assert!(!NoxuError::DatabaseClosed.is_retryable());
    }

    #[test]
    fn test_is_fatal_to_environment() {
        assert!(NoxuError::EnvironmentFailure("x".into()).is_fatal_to_environment());
        assert!(NoxuError::LogChecksumMismatch("bad".into()).is_fatal_to_environment());

        assert!(!NoxuError::DeadlockDetected.is_fatal_to_environment());
        assert!(!NoxuError::NotFound.is_fatal_to_environment());
        assert!(!NoxuError::LockConflict("x".into()).is_fatal_to_environment());
    }

    #[test]
    fn test_new_variants() {
        let e = NoxuError::LockTimeout { timeout_ms: 250 };
        assert!(e.to_string().contains("250ms"));

        let e = NoxuError::TransactionTimeout { timeout_ms: 1000, txn_id: 7 };
        assert!(e.to_string().contains("1000ms"));
        assert!(e.to_string().contains("7"));

        let e = NoxuError::LockPreempted;
        assert!(e.to_string().contains("preempted"));

        let e = NoxuError::UniqueConstraintViolation("idx_email".into());
        assert!(e.to_string().contains("unique constraint"));

        let e = NoxuError::ReplicaWrite;
        assert!(e.to_string().contains("replica"));

        let e = NoxuError::InsufficientReplicas { required: 3, available: 1 };
        assert!(e.to_string().contains("required 3"));
        assert!(e.to_string().contains("available 1"));

        let e = NoxuError::RollbackRequired("ha failover".into());
        assert!(e.to_string().contains("rollback required"));

        let e = NoxuError::LogChecksumMismatch("file 7".into());
        assert!(e.to_string().contains("log checksum mismatch"));

        let e = NoxuError::LogFileNotFound("00000007.ndb".into());
        assert!(e.to_string().contains("log file not found"));
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
