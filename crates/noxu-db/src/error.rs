//! Error types for Noxu DB.
//!
//! Implements exception hierarchy:
//!
//! ```text
//! DatabaseException (base)
//!   ├── EnvironmentFailureException      → NoxuError::EnvironmentFailure
//!   │     ├── LogWriteException          → NoxuError::LogWriteFailure
//!   │     ├── DiskLimitException         → NoxuError::DiskLimitExceeded
//!   │     ├── ThreadInterruptedException → NoxuError::ThreadInterrupted
//!   │     ├── EnvironmentWedgedException → NoxuError::EnvironmentWedged
//!   │     └── VersionMismatchException   → NoxuError::VersionMismatch
//!   └── OperationFailureException
//!         ├── LockConflictException      → NoxuError::LockConflict
//!         │     ├── DeadlockException    → NoxuError::DeadlockDetected
//!         │     ├── LockTimeoutException → NoxuError::LockTimeout
//!         │     └── LockNotAvailableEx   → NoxuError::LockNotAvailable
//!         ├── TransactionTimeoutException→ NoxuError::TransactionTimeout
//!         ├── LockPreemptedException     → NoxuError::LockPreempted
//!         ├── UniqueConstraintException  → NoxuError::UniqueConstraintViolation
//!         ├── DeleteConstraintException  → NoxuError::DeleteConstraintViolation
//!         ├── ForeignConstraintException → NoxuError::ForeignConstraintViolation
//!         ├── SecondaryIntegrityException→ NoxuError::SecondaryIntegrityException
//!         └── DuplicateDataException     → NoxuError::DuplicateDataException
//! ```

use thiserror::Error;

// ── EnvironmentFailureReason ───────────────────────────────────────────────

/// Distinguishes the root cause of an `EnvironmentFailure`.
///
/// Callers can
/// match on this to decide whether to attempt restart (`invalidates_environment
/// = false`) or give up (`invalidates_environment = true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvironmentFailureReason {
    // ── Log / checksum ─────────────────────────────────────────────────────
    /// A checksum mismatch was detected in the log (persistent corruption).
    /// `isCorrupted() == true`.  : `LOG_CHECKSUM`.
    LogChecksum,

    /// A log write I/O error occurred.  : `LOG_WRITE`.
    LogWrite,

    /// A log file was not found during read (truncation or deletion).
    /// : `LOG_FILE_NOT_FOUND`.
    LogFileNotFound,

    /// The log is incomplete or internally inconsistent.
    /// : `LOG_INTEGRITY`.
    LogIntegrity,

    // ── B-tree ──────────────────────────────────────────────────────────────
    /// A persistent B-tree structure inconsistency was detected.
    /// `isCorrupted() == true`.  : `BTREE_CORRUPTION`.
    BtreeCorruption,

    // ── Unexpected internal state ──────────────────────────────────────────
    /// An unexpected internal state was reached (non-fatal; env still valid).
    /// : `UNEXPECTED_STATE`.
    UnexpectedState,

    /// An unexpected internal state was reached (fatal; env is invalidated).
    /// : `UNEXPECTED_STATE_FATAL`.
    UnexpectedStateFatal,

    /// An unexpected exception was caught internally (non-fatal).
    /// : `UNEXPECTED_EXCEPTION`.
    UnexpectedException,

    /// An unexpected exception was caught internally (fatal; env invalidated).
    /// : `UNEXPECTED_EXCEPTION_FATAL`.
    UnexpectedExceptionFatal,

    // ── Resource limits ─────────────────────────────────────────────────────
    /// The disk limit (`MAX_DISK`) or free-disk threshold (`FREE_DISK`) was
    /// exceeded.  : `DISK_LIMIT`.
    DiskLimit,

    /// A latch acquisition timed out.  : `LATCH_TIMEOUT`.
    LatchTimeout,

    // ── Thread lifecycle ────────────────────────────────────────────────────
    /// The calling thread was interrupted while performing a
    /// : `THREAD_INTERRUPTED`.
    ThreadInterrupted,

    // ── Replication ─────────────────────────────────────────────────────────
    /// The master transitioned to a replica while a transaction was active.
    /// : `MASTER_TO_REPLICA_TRANSITION`.
    MasterToReplicaTransition,

    /// The replica was fenced by the master.
    /// : `REPLICA_FENCING`.
    ReplicaFencing,

    /// A replication handshake error occurred.
    /// : `HANDSHAKE_ERROR`.
    HandshakeError,

    /// Replication protocol version mismatch.
    /// : `PROTOCOL_VERSION_MISMATCH`.
    ProtocolVersionMismatch,

    /// An uncaught exception in a background replication thread.
    /// : `UNCAUGHT_EXCEPTION`.
    UncaughtException,

    /// Forced shutdown was requested.
    /// : `FORCED_SHUTDOWN`.
    ForcedShutdown,

    // ── Catch-all ──────────────────────────────────────────────────────────
    /// The specific reason is not mapped to a named variant.
    Other(String),
}

impl EnvironmentFailureReason {
    /// Returns `true` if this reason causes the environment to be invalidated.
    ///
    /// After an invalidating failure, all open `Environment` handles become
    /// unusable; they must be closed and re-opened to run recovery.
    ///
    /// Mirrors `EnvironmentFailureReason.invalidatesEnvironment()`.
    pub fn invalidates_environment(&self) -> bool {
        matches!(
            self,
            EnvironmentFailureReason::LogChecksum
                | EnvironmentFailureReason::LogWrite
                | EnvironmentFailureReason::LogIntegrity
                | EnvironmentFailureReason::BtreeCorruption
                | EnvironmentFailureReason::UnexpectedStateFatal
                | EnvironmentFailureReason::UnexpectedExceptionFatal
                | EnvironmentFailureReason::DiskLimit
                | EnvironmentFailureReason::LatchTimeout
                | EnvironmentFailureReason::ReplicaFencing
                | EnvironmentFailureReason::ForcedShutdown
        )
    }

    /// Returns `true` if the environment log is persistently corrupted,
    /// meaning a network restore or backup restore may be required.
    ///
    /// Mirrors `EnvironmentFailureException.isCorrupted()`.
    pub fn is_corrupted(&self) -> bool {
        matches!(
            self,
            EnvironmentFailureReason::LogChecksum
                | EnvironmentFailureReason::BtreeCorruption
        )
    }
}

impl std::fmt::Display for EnvironmentFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvironmentFailureReason::LogChecksum => write!(f, "LOG_CHECKSUM"),
            EnvironmentFailureReason::LogWrite => write!(f, "LOG_WRITE"),
            EnvironmentFailureReason::LogFileNotFound => {
                write!(f, "LOG_FILE_NOT_FOUND")
            }
            EnvironmentFailureReason::LogIntegrity => {
                write!(f, "LOG_INTEGRITY")
            }
            EnvironmentFailureReason::BtreeCorruption => {
                write!(f, "BTREE_CORRUPTION")
            }
            EnvironmentFailureReason::UnexpectedState => {
                write!(f, "UNEXPECTED_STATE")
            }
            EnvironmentFailureReason::UnexpectedStateFatal => {
                write!(f, "UNEXPECTED_STATE_FATAL")
            }
            EnvironmentFailureReason::UnexpectedException => {
                write!(f, "UNEXPECTED_EXCEPTION")
            }
            EnvironmentFailureReason::UnexpectedExceptionFatal => {
                write!(f, "UNEXPECTED_EXCEPTION_FATAL")
            }
            EnvironmentFailureReason::DiskLimit => write!(f, "DISK_LIMIT"),
            EnvironmentFailureReason::LatchTimeout => {
                write!(f, "LATCH_TIMEOUT")
            }
            EnvironmentFailureReason::ThreadInterrupted => {
                write!(f, "THREAD_INTERRUPTED")
            }
            EnvironmentFailureReason::MasterToReplicaTransition => {
                write!(f, "MASTER_TO_REPLICA_TRANSITION")
            }
            EnvironmentFailureReason::ReplicaFencing => {
                write!(f, "REPLICA_FENCING")
            }
            EnvironmentFailureReason::HandshakeError => {
                write!(f, "HANDSHAKE_ERROR")
            }
            EnvironmentFailureReason::ProtocolVersionMismatch => {
                write!(f, "PROTOCOL_VERSION_MISMATCH")
            }
            EnvironmentFailureReason::UncaughtException => {
                write!(f, "UNCAUGHT_EXCEPTION")
            }
            EnvironmentFailureReason::ForcedShutdown => {
                write!(f, "FORCED_SHUTDOWN")
            }
            EnvironmentFailureReason::Other(s) => write!(f, "{s}"),
        }
    }
}

// ── ExceptionListener ──────────────────────────────────────────────────────

/// The source subsystem that raised an exception event.
///
/// Mirrors the thread-name conventions used by reporting background
/// daemon exceptions via `ExceptionListener`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExceptionSource {
    Checkpointer,
    Cleaner,
    Evictor,
    INCompressor,
    Verifier,
    ReplicationThread,
    Unknown(String),
}

impl std::fmt::Display for ExceptionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExceptionSource::Checkpointer => write!(f, "Checkpointer"),
            ExceptionSource::Cleaner => write!(f, "Cleaner"),
            ExceptionSource::Evictor => write!(f, "Evictor"),
            ExceptionSource::INCompressor => write!(f, "INCompressor"),
            ExceptionSource::Verifier => write!(f, "Verifier"),
            ExceptionSource::ReplicationThread => {
                write!(f, "ReplicationThread")
            }
            ExceptionSource::Unknown(s) => write!(f, "{s}"),
        }
    }
}

/// An exception event delivered to an [`ExceptionListener`].
///

#[derive(Debug, Clone)]
pub struct ExceptionEvent {
    /// Human-readable error message.
    pub message: String,
    /// The background subsystem or thread that encountered the exception.
    pub source: ExceptionSource,
    /// Name of the OS thread (for logging / diagnostics).
    pub thread_name: String,
}

impl ExceptionEvent {
    /// Create a new `ExceptionEvent`.
    pub fn new(
        message: impl Into<String>,
        source: ExceptionSource,
        thread_name: impl Into<String>,
    ) -> Self {
        Self {
            message: message.into(),
            source,
            thread_name: thread_name.into(),
        }
    }
}

/// Callback interface for exceptions thrown in background daemon threads.
///
/// Register an implementation via
/// `EnvironmentConfig.set_exception_listener()`.  Background threads
/// (Checkpointer, Cleaner, Evictor, INCompressor, Verifier) call
/// [`ExceptionListener::exception_event`] when they encounter an unhandled
/// error.
pub trait ExceptionListener: Send + Sync {
    fn exception_event(&self, event: &ExceptionEvent);
}

// ── NoxuError ──────────────────────────────────────────────────────────────

/// Errors that can occur when using Noxu DB.
///
/// Implements exception hierarchy:
///
/// - [`NoxuError::EnvironmentFailure`] — potentially fatal; check
///   [`NoxuError::is_fatal_to_environment`].  Carries an
///   [`EnvironmentFailureReason`] for discriminating the cause.
/// - Operation-failure variants are retryable after abort
///   ([`NoxuError::is_retryable`]).
/// - HA / replication variants ([`NoxuError::InsufficientReplicas`],
///   [`NoxuError::ReplicaWrite`], [`NoxuError::RollbackRequired`]).
#[derive(Debug, Error)]
pub enum NoxuError {
    // ── Fatal / environment failure ────────────────────────────────────────
    /// A failure has occurred that may require the environment to be closed
    /// and re-opened.  Check [`NoxuError::is_fatal_to_environment`] /
    /// [`NoxuError::reason`] to determine whether restart is required.
    ///

    #[error("environment failure ({reason}): {msg}")]
    EnvironmentFailure {
        /// The root cause of the failure.
        reason: EnvironmentFailureReason,
        /// Human-readable detail message.
        msg: String,
    },

    /// The environment is permanently wedged and cannot recover even after
    /// close/re-open.  Operator intervention or backup restore is required.
    ///

    #[error("environment wedged (permanent failure): {0}")]
    EnvironmentWedged(String),

    /// The environment home directory was not found and `allow_create = false`.
    ///

    #[error("environment not found: {0}")]
    EnvironmentNotFound(String),

    /// The environment is already open by another process.
    ///

    #[error("environment locked by another process: {0}")]
    EnvironmentLocked(String),

    /// An I/O error occurred while writing to the log.  The disk may be full.
    ///

    #[error("log write failure: {0}")]
    LogWriteFailure(String),

    /// The disk limit (`MAX_DISK` / `FREE_DISK`) was exceeded.
    ///

    #[error("disk limit exceeded: used={used}, limit={limit}")]
    DiskLimitExceeded {
        /// Bytes currently used by the environment.
        used: u64,
        /// Configured limit in bytes.
        limit: u64,
    },

    /// The calling thread was interrupted while performing a
    ///

    #[error("thread interrupted during database operation")]
    ThreadInterrupted,

    // ── Database / cursor lifecycle ────────────────────────────────────────
    /// The requested database was not found in the environment.
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    /// An attempt was made to create a database that already exists.
    ///

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
    /// Retryable.
    #[error("lock conflict: {0}")]
    LockConflict(String),

    /// A deadlock was detected between two or more transactions.
    ///
    /// Retryable.
    #[error("deadlock detected")]
    DeadlockDetected,

    /// A lock-wait timeout expired.
    ///
    /// Retryable.
    #[error("lock timeout after {timeout_ms}ms")]
    LockTimeout {
        /// How long the locker waited before giving up.
        timeout_ms: u64,
    },

    /// A lock was requested with `no-wait` semantics and was not immediately
    /// available.
    ///
    /// Retryable.
    #[error("lock not available (no-wait)")]
    LockNotAvailable,

    /// A transaction-level timeout expired.
    ///
    /// Retryable.
    #[error("transaction timeout after {timeout_ms}ms for txn {txn_id}")]
    TransactionTimeout {
        /// Transaction-level timeout in milliseconds.
        timeout_ms: u64,
        /// ID of the timed-out transaction.
        txn_id: i64,
    },

    /// A lock was preempted by a higher-priority locker (HA).
    ///
    /// Retryable.
    #[error("lock preempted by higher-priority locker")]
    LockPreempted,

    /// The transaction was aborted.
    #[error("transaction aborted: {0}")]
    TransactionAborted(String),

    // ── Constraint violations ──────────────────────────────────────────────
    /// The key already exists (`put_no_overwrite` / cursor `put_no_dup_data`).
    #[error("key already exists")]
    KeyExists,

    /// A unique-index constraint was violated.
    ///

    #[error("unique constraint violated: {0}")]
    UniqueConstraintViolation(String),

    /// A delete was attempted on a primary record referenced by a secondary
    /// index.
    ///

    #[error("delete constraint violated: {0}")]
    DeleteConstraintViolation(String),

    /// A foreign-key constraint was violated.
    ///

    #[error("foreign constraint violated: {0}")]
    ForeignConstraintViolation(String),

    /// Duplicate data was supplied to a `putNoDupData` operation in a
    /// duplicate-sorted database.
    ///

    #[error("duplicate data not allowed in no-dup-data operation")]
    DuplicateDataException,

    /// A secondary database integrity constraint was violated.
    ///

    #[error("secondary integrity constraint violated: {0}")]
    SecondaryIntegrityException(String),

    // ── Sequence errors ────────────────────────────────────────────────────
    /// A sequence with the given name already exists.
    ///

    #[error("sequence already exists: {0}")]
    SequenceExists(String),

    /// A sequence with the given name was not found.
    ///

    #[error("sequence not found: {0}")]
    SequenceNotFound(String),

    /// A sequence has overflowed or underflowed its range.
    ///

    #[error("sequence overflow")]
    SequenceOverflow,

    /// A sequence integrity violation was detected.
    ///

    #[error("sequence integrity violation: {0}")]
    SequenceIntegrity(String),

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

    #[error("write not allowed on replica")]
    ReplicaWrite,

    /// Insufficient replicas acknowledged the commit.
    ///

    #[error(
        "insufficient replicas: required {required}, available {available}"
    )]
    InsufficientReplicas {
        /// Acknowledgement quorum required.
        required: u32,
        /// Number of replicas that responded.
        available: u32,
    },

    /// The transaction must be rolled back due to a replication state change.
    ///

    #[error("rollback required: {0}")]
    RollbackRequired(String),

    // ── Log / I/O ──────────────────────────────────────────────────────────
    /// A log checksum mismatch was detected (potential corruption).
    ///
    /// Fatal: the environment will be invalidated.
    #[error("log checksum mismatch: {0}")]
    LogChecksumMismatch(String),

    /// A log file was not found.
    ///

    #[error("log file not found: {0}")]
    LogFileNotFound(String),

    /// An I/O error occurred.
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    // ── Version ─────────────────────────────────────────────────────────────
    /// A version mismatch occurred (e.g. on-disk format vs. code version).
    ///

    #[error("version mismatch: {0}")]
    VersionMismatch(String),

    // ── General ────────────────────────────────────────────────────────────
    /// The operation is not allowed in the current state.
    ///

    #[error("operation not allowed: {0}")]
    OperationNotAllowed(String),

    /// An illegal argument was provided to a method.
    ///
    /// Mirrors `IllegalArgumentException` (DB flavour).
    #[error("illegal argument: {0}")]
    IllegalArgument(String),

    /// The operation timed out (non-lock, non-txn — e.g. network or sync).
    #[error("operation timed out")]
    Timeout,

    /// An invalid operation was requested.
    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    /// The requested operation is recognised by the API but not yet
    /// implemented.  The argument names the operation (for example
    /// `"Get::SearchLte"`).
    ///
    /// Returned by API arms that previously fell through to a silent
    /// `OperationStatus::NotFound`; users now see a loud, typed error
    /// instead of a misleading miss.  Tracked in
    /// `docs/src/internal/api-audit-2026-05-cursor.md` Finding 3.
    #[error("operation not yet supported: {0}")]
    Unsupported(String),
}

impl NoxuError {
    // ── Classification helpers ─────────────────────────────────────────────

    /// Returns `true` if the failed operation may be retried after aborting
    /// the current transaction.
    ///
    /// Mirrors `OperationFailureException.isRetryable()`.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            NoxuError::LockConflict(_)
                | NoxuError::DeadlockDetected
                | NoxuError::LockTimeout { .. }
                | NoxuError::LockNotAvailable
                | NoxuError::TransactionTimeout { .. }
                | NoxuError::LockPreempted
        )
    }

    /// Returns `true` if this error is fatal to the environment.
    ///
    /// After a fatal error the environment must be closed and re-opened.
    /// Subsequent operations on an invalidated environment will return
    /// `EnvironmentClosed`.
    ///
    /// Mirrors `EnvironmentFailureException` detection + `isValid()`.
    pub fn is_fatal_to_environment(&self) -> bool {
        match self {
            NoxuError::EnvironmentFailure { reason, .. } => {
                reason.invalidates_environment()
            }
            NoxuError::LogChecksumMismatch(_)
            | NoxuError::LogWriteFailure(_)
            | NoxuError::DiskLimitExceeded { .. }
            | NoxuError::EnvironmentWedged(_) => true,
            _ => false,
        }
    }

    /// Returns the `EnvironmentFailureReason` if this is an
    /// `EnvironmentFailure` variant, `None` otherwise.
    ///
    /// Mirrors `EnvironmentFailureException.getReason()`.
    pub fn reason(&self) -> Option<&EnvironmentFailureReason> {
        match self {
            NoxuError::EnvironmentFailure { reason, .. } => Some(reason),
            _ => None,
        }
    }

    /// Returns `true` if the environment log is persistently corrupted.
    ///
    /// Mirrors `EnvironmentFailureException.isCorrupted()`.
    pub fn is_corrupted(&self) -> bool {
        match self {
            NoxuError::EnvironmentFailure { reason, .. } => {
                reason.is_corrupted()
            }
            NoxuError::LogChecksumMismatch(_) => true,
            _ => false,
        }
    }

    /// Returns `true` if this is a lock-conflict error.
    pub fn is_lock_conflict(&self) -> bool {
        matches!(
            self,
            NoxuError::LockConflict(_)
                | NoxuError::DeadlockDetected
                | NoxuError::LockPreempted
                | NoxuError::LockNotAvailable
        )
    }

    /// Returns `true` if this is a lock or transaction timeout.
    pub fn is_lock_timeout(&self) -> bool {
        matches!(
            self,
            NoxuError::LockTimeout { .. }
                | NoxuError::TransactionTimeout { .. }
        )
    }

    /// Returns `true` if the named database was not found.
    pub fn is_database_not_found(&self) -> bool {
        matches!(self, NoxuError::DatabaseNotFound(_))
    }

    /// Returns `true` for any `OperationFailureException`-equivalent.
    pub fn is_operation_failure(&self) -> bool {
        self.is_retryable()
    }

    // ── Constructor helpers ────────────────────────────────────────────────

    /// Creates an `EnvironmentFailure` with `UnexpectedState` reason.
    /// Use when the specific reason is unknown.
    pub fn environment(msg: impl Into<String>) -> Self {
        NoxuError::EnvironmentFailure {
            reason: EnvironmentFailureReason::UnexpectedState,
            msg: msg.into(),
        }
    }

    /// Creates an `EnvironmentFailure` with an explicit reason.
    pub fn environment_with_reason(
        reason: EnvironmentFailureReason,
        msg: impl Into<String>,
    ) -> Self {
        NoxuError::EnvironmentFailure { reason, msg: msg.into() }
    }

    /// Creates an `OperationNotAllowed` error.
    pub fn database(msg: impl Into<String>) -> Self {
        NoxuError::OperationNotAllowed(msg.into())
    }

    /// Creates an `IllegalArgument` error.
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        NoxuError::IllegalArgument(msg.into())
    }

    /// Creates a `LockConflict` error.
    pub fn lock_conflict(msg: impl Into<String>) -> Self {
        NoxuError::LockConflict(msg.into())
    }

    /// Creates a `LockTimeout` error.
    pub fn lock_timeout(timeout_ms: u64) -> Self {
        NoxuError::LockTimeout { timeout_ms }
    }

    /// Creates a `DatabaseNotFound` error.
    pub fn database_not_found(name: impl Into<String>) -> Self {
        NoxuError::DatabaseNotFound(name.into())
    }

    /// Creates a `DiskLimitExceeded` error.
    pub fn disk_limit_exceeded(used: u64, limit: u64) -> Self {
        NoxuError::DiskLimitExceeded { used, limit }
    }
}

// ── Conversions from sub-crate errors ─────────────────────────────────────

impl From<noxu_dbi::DbiError> for NoxuError {
    fn from(e: noxu_dbi::DbiError) -> Self {
        use noxu_dbi::DbiError;
        match e {
            DbiError::DatabaseNotFound(s) => NoxuError::DatabaseNotFound(s),
            DbiError::DatabaseAlreadyExists(s)
            | DbiError::DatabaseExists(s) => {
                NoxuError::DatabaseAlreadyExists(s)
            }
            DbiError::EnvironmentFailure { reason } => {
                NoxuError::EnvironmentFailure {
                    reason: EnvironmentFailureReason::UnexpectedState,
                    msg: reason,
                }
            }
            DbiError::EnvironmentNotOpen | DbiError::EnvironmentLocked(_) => {
                NoxuError::EnvironmentClosed
            }
            DbiError::CursorClosed | DbiError::CursorNotInitialized => {
                NoxuError::CursorClosed
            }
            DbiError::LockConflict(s) => NoxuError::LockConflict(s),
            DbiError::IoError(io) => NoxuError::IoError(io),
            DbiError::TxnError(txn_err) => NoxuError::from(txn_err),
            DbiError::LogError(log_err) => {
                NoxuError::OperationNotAllowed(log_err.to_string())
            }
            DbiError::TreeError(tree_err) => {
                NoxuError::OperationNotAllowed(tree_err.to_string())
            }
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
            TxnError::LockTimeout { timeout_ms, .. } => {
                NoxuError::LockTimeout { timeout_ms }
            }
            TxnError::TransactionTimeout { timeout_ms, txn_id } => {
                NoxuError::TransactionTimeout { timeout_ms, txn_id }
            }
            TxnError::LockNotAvailable { .. } => NoxuError::LockNotAvailable,
            TxnError::RangeRestart => {
                NoxuError::LockConflict("range restart".into())
            }
            TxnError::InvalidTransaction { txn_id, state } => {
                NoxuError::TransactionAborted(format!("txn {txn_id}: {state}"))
            }
            TxnError::StateError(s) => NoxuError::TransactionAborted(s),
            TxnError::LogError(log_err) => {
                NoxuError::OperationNotAllowed(log_err.to_string())
            }
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
        let err = NoxuError::environment("disk full");
        assert!(err.to_string().contains("environment failure"));
        assert!(err.to_string().contains("disk full"));
    }

    #[test]
    fn test_environment_failure_with_reason() {
        let err = NoxuError::environment_with_reason(
            EnvironmentFailureReason::LogChecksum,
            "checksum mismatch in file 7",
        );
        assert!(err.to_string().contains("LOG_CHECKSUM"));
        assert!(err.is_corrupted());
        assert!(err.is_fatal_to_environment());
        assert_eq!(err.reason(), Some(&EnvironmentFailureReason::LogChecksum));
    }

    #[test]
    fn test_environment_failure_reason_invalidates() {
        assert!(
            EnvironmentFailureReason::LogChecksum.invalidates_environment()
        );
        assert!(
            EnvironmentFailureReason::BtreeCorruption.invalidates_environment()
        );
        assert!(EnvironmentFailureReason::DiskLimit.invalidates_environment());
        assert!(
            !EnvironmentFailureReason::UnexpectedState
                .invalidates_environment()
        );
        assert!(
            !EnvironmentFailureReason::UnexpectedException
                .invalidates_environment()
        );
    }

    #[test]
    fn test_environment_failure_reason_corrupted() {
        assert!(EnvironmentFailureReason::LogChecksum.is_corrupted());
        assert!(EnvironmentFailureReason::BtreeCorruption.is_corrupted());
        assert!(!EnvironmentFailureReason::DiskLimit.is_corrupted());
        assert!(!EnvironmentFailureReason::LogWrite.is_corrupted());
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
        let dbi_err = noxu_dbi::DbiError::CursorClosed;
        let err: NoxuError = NoxuError::from(dbi_err);
        assert!(matches!(err, NoxuError::CursorClosed));

        let e: NoxuError =
            noxu_dbi::DbiError::DatabaseNotFound("x".into()).into();
        assert!(matches!(e, NoxuError::DatabaseNotFound(_)));

        let e: NoxuError =
            noxu_dbi::DbiError::EnvironmentFailure { reason: "disk".into() }
                .into();
        assert!(matches!(e, NoxuError::EnvironmentFailure { .. }));
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

        let e: NoxuError =
            TxnError::TransactionTimeout { timeout_ms: 1000, txn_id: 42 }
                .into();
        assert!(matches!(
            e,
            NoxuError::TransactionTimeout { timeout_ms: 1000, txn_id: 42 }
        ));

        // LockNotAvailable maps to NoxuError::LockNotAvailable (not LockConflict)
        let e: NoxuError = TxnError::LockNotAvailable { lsn: 0 }.into();
        assert!(matches!(e, NoxuError::LockNotAvailable));
    }

    #[test]
    fn test_is_retryable() {
        assert!(NoxuError::DeadlockDetected.is_retryable());
        assert!(NoxuError::LockConflict("x".into()).is_retryable());
        assert!(NoxuError::LockTimeout { timeout_ms: 500 }.is_retryable());
        assert!(
            NoxuError::TransactionTimeout { timeout_ms: 1000, txn_id: 1 }
                .is_retryable()
        );
        assert!(NoxuError::LockPreempted.is_retryable());
        assert!(NoxuError::LockNotAvailable.is_retryable());

        assert!(!NoxuError::NotFound.is_retryable());
        assert!(!NoxuError::environment("x").is_retryable());
        assert!(!NoxuError::DatabaseClosed.is_retryable());
    }

    #[test]
    fn test_is_fatal_to_environment() {
        assert!(
            NoxuError::environment_with_reason(
                EnvironmentFailureReason::LogChecksum,
                "x"
            )
            .is_fatal_to_environment()
        );
        assert!(
            NoxuError::LogChecksumMismatch("bad".into())
                .is_fatal_to_environment()
        );
        assert!(
            NoxuError::LogWriteFailure("io".into()).is_fatal_to_environment()
        );
        assert!(
            NoxuError::DiskLimitExceeded { used: 100, limit: 50 }
                .is_fatal_to_environment()
        );
        assert!(
            NoxuError::EnvironmentWedged("x".into()).is_fatal_to_environment()
        );

        // Non-fatal EnvironmentFailure variants
        assert!(
            !NoxuError::environment_with_reason(
                EnvironmentFailureReason::UnexpectedState,
                "x"
            )
            .is_fatal_to_environment()
        );

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

        let e = NoxuError::LockNotAvailable;
        assert!(e.to_string().contains("no-wait"));

        let e = NoxuError::UniqueConstraintViolation("idx_email".into());
        assert!(e.to_string().contains("unique constraint"));

        let e = NoxuError::DeleteConstraintViolation("key=42".into());
        assert!(e.to_string().contains("delete constraint"));

        let e = NoxuError::ForeignConstraintViolation("fk_user".into());
        assert!(e.to_string().contains("foreign constraint"));

        let e = NoxuError::DuplicateDataException;
        assert!(e.to_string().contains("duplicate data"));

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

        let e = NoxuError::EnvironmentWedged("perm fail".into());
        assert!(e.to_string().contains("permanent failure"));

        let e = NoxuError::EnvironmentNotFound("/bad/path".into());
        assert!(e.to_string().contains("not found"));

        let e = NoxuError::EnvironmentLocked("/data".into());
        assert!(e.to_string().contains("locked"));

        let e = NoxuError::LogWriteFailure("ENOSPC".into());
        assert!(e.to_string().contains("log write failure"));

        let e = NoxuError::DiskLimitExceeded { used: 1000, limit: 500 };
        assert!(e.to_string().contains("used=1000"));
        assert!(e.to_string().contains("limit=500"));

        let e = NoxuError::ThreadInterrupted;
        assert!(e.to_string().contains("interrupted"));
    }

    #[test]
    fn test_sequence_variants() {
        let e = NoxuError::SequenceExists("counter".into());
        assert!(e.to_string().contains("counter"));

        let e = NoxuError::SequenceNotFound("counter".into());
        assert!(e.to_string().contains("sequence not found"));

        let e = NoxuError::SequenceOverflow;
        assert!(e.to_string().contains("overflow"));

        let e = NoxuError::SequenceIntegrity("bad state".into());
        assert!(e.to_string().contains("integrity"));
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
    fn test_helpers() {
        assert!(matches!(
            NoxuError::environment("x"),
            NoxuError::EnvironmentFailure { .. }
        ));
        assert!(matches!(
            NoxuError::database("x"),
            NoxuError::OperationNotAllowed(_)
        ));
        assert!(matches!(
            NoxuError::invalid_argument("x"),
            NoxuError::IllegalArgument(_)
        ));
        assert!(matches!(
            NoxuError::lock_conflict("x"),
            NoxuError::LockConflict(_)
        ));
        assert!(matches!(
            NoxuError::lock_timeout(500),
            NoxuError::LockTimeout { timeout_ms: 500 }
        ));
        assert!(matches!(
            NoxuError::database_not_found("db"),
            NoxuError::DatabaseNotFound(_)
        ));
        assert!(matches!(
            NoxuError::disk_limit_exceeded(100, 50),
            NoxuError::DiskLimitExceeded { used: 100, limit: 50 }
        ));
    }

    #[test]
    fn test_is_lock_conflict() {
        assert!(NoxuError::LockConflict("x".into()).is_lock_conflict());
        assert!(NoxuError::DeadlockDetected.is_lock_conflict());
        assert!(NoxuError::LockPreempted.is_lock_conflict());
        assert!(NoxuError::LockNotAvailable.is_lock_conflict());
        assert!(!NoxuError::LockTimeout { timeout_ms: 500 }.is_lock_conflict());
        assert!(!NoxuError::NotFound.is_lock_conflict());
    }

    #[test]
    fn test_is_lock_timeout() {
        assert!(NoxuError::LockTimeout { timeout_ms: 500 }.is_lock_timeout());
        assert!(
            NoxuError::TransactionTimeout { timeout_ms: 1000, txn_id: 1 }
                .is_lock_timeout()
        );
        assert!(!NoxuError::LockConflict("x".into()).is_lock_timeout());
        assert!(!NoxuError::NotFound.is_lock_timeout());
    }

    #[test]
    fn test_is_database_not_found() {
        assert!(
            NoxuError::DatabaseNotFound("mydb".into()).is_database_not_found()
        );
        assert!(!NoxuError::DatabaseClosed.is_database_not_found());
        assert!(!NoxuError::NotFound.is_database_not_found());
    }

    #[test]
    fn test_exception_event() {
        let evt = ExceptionEvent::new(
            "cleaner failed",
            ExceptionSource::Cleaner,
            "cleaner-1",
        );
        assert_eq!(evt.message, "cleaner failed");
        assert_eq!(evt.source, ExceptionSource::Cleaner);
        assert_eq!(evt.thread_name, "cleaner-1");
        assert!(evt.source.to_string().contains("Cleaner"));
    }

    #[test]
    fn test_exception_listener_trait() {
        struct NoopListener;
        impl ExceptionListener for NoopListener {
            fn exception_event(&self, _event: &ExceptionEvent) {}
        }
        let listener: Box<dyn ExceptionListener> = Box::new(NoopListener);
        let evt =
            ExceptionEvent::new("x", ExceptionSource::Checkpointer, "ckpt");
        listener.exception_event(&evt);
    }

    #[test]
    fn test_reason_display() {
        assert_eq!(
            EnvironmentFailureReason::LogChecksum.to_string(),
            "LOG_CHECKSUM"
        );
        assert_eq!(
            EnvironmentFailureReason::BtreeCorruption.to_string(),
            "BTREE_CORRUPTION"
        );
        assert_eq!(
            EnvironmentFailureReason::DiskLimit.to_string(),
            "DISK_LIMIT"
        );
        assert_eq!(
            EnvironmentFailureReason::Other("CUSTOM".into()).to_string(),
            "CUSTOM"
        );
    }
}
