//! XA error types.

use crate::xid::XidError;

/// XA errors returned from XaResource operations.
#[derive(Debug, thiserror::Error)]
pub enum XaError {
    /// The XID is not valid or cannot be found.
    #[error("XAER_NOTA: unknown XID")]
    NotFound,

    /// Invalid arguments passed.
    #[error("XAER_INVAL: invalid arguments")]
    Invalid,

    /// Protocol error (operation called in wrong state).
    #[error("XAER_PROTO: protocol error — {0}")]
    Protocol(String),

    /// Resource manager failure.
    #[error("XAER_RMFAIL: resource manager failure — {0}")]
    RmFail(String),

    /// Duplicate XID.
    #[error("XAER_DUPID: duplicate XID")]
    DuplicateXid,

    /// Work performed outside global transaction.
    #[error("XAER_OUTSIDE: work outside global transaction")]
    Outside,

    /// Heuristic commit (TM should forget).
    #[error("XA_HEURCOM: heuristic commit")]
    HeuristicCommit,

    /// Heuristic rollback (TM should forget).
    #[error("XA_HEURRB: heuristic rollback")]
    HeuristicRollback,

    /// Xid construction error.
    #[error(transparent)]
    Xid(#[from] XidError),

    /// Underlying database error.
    #[error("database error: {0}")]
    Db(#[from] noxu_db::NoxuError),
}

/// XA result type.
pub type XaResult<T> = Result<T, XaError>;

/// Return value indicating the branch was read-only (no commit needed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareResult {
    /// Branch has modifications; commit or rollback required.
    Ok,
    /// Branch was read-only; no commit or rollback needed.
    ReadOnly,
}
