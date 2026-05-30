//! Transaction and locking error types.
//!

use thiserror::Error;

use crate::txn::LockType;
use crate::log::NoxuLogError;

/// Transaction and locking errors.
#[derive(Debug, Error)]
pub enum TxnError {
    /// Lock conflict detected.
    #[error("lock conflict: {0}")]
    LockConflict(String),

    /// Lock timeout occurred while waiting for a lock.
    ///
    ///
    #[error(
        "lock timeout after {timeout_ms}ms on LSN {lsn}: held by {owner}, requested {requested_type:?} by locker {requester}"
    )]
    LockTimeout {
        /// Timeout duration in milliseconds.
        timeout_ms: u64,
        /// LSN of the locked record.
        lsn: u64,
        /// Description of the current lock owner.
        owner: String,
        /// Type of lock that was requested.
        requested_type: LockType,
        /// ID or description of the requester.
        requester: String,
    },

    /// Transaction timeout occurred.
    ///
    ///
    #[error("transaction timeout after {timeout_ms}ms for txn {txn_id}")]
    TransactionTimeout {
        /// Timeout duration in milliseconds.
        timeout_ms: u64,
        /// ID of the transaction that timed out.
        txn_id: i64,
    },

    /// Deadlock detected during lock acquisition.
    ///
    ///
    #[error("deadlock detected: {0}")]
    Deadlock(String),

    /// Transaction is in an invalid state.
    #[error("transaction {txn_id} is not valid (state: {state})")]
    InvalidTransaction {
        /// Transaction ID.
        txn_id: i64,
        /// Description of the invalid state.
        state: String,
    },

    /// Lock is not available.
    #[error("lock not available for LSN {lsn}")]
    LockNotAvailable {
        /// LSN of the record that could not be locked.
        lsn: u64,
    },

    /// Range restart required due to range lock conflict.
    ///
    ///
    #[error("range restart required")]
    RangeRestart,

    /// Transaction state error.
    #[error("transaction state error: {0}")]
    StateError(String),

    /// An error occurred while writing to the log.
    ///
    /// Wraps `NoxuLogError` so callers do not need to depend on `noxu-log`
    /// directly to handle commit/abort failures.
    #[error("log write failed: {0}")]
    LogError(#[from] NoxuLogError),
}

/// Type alias for transaction results.
pub type TxnResult<T> = Result<T, TxnError>;
