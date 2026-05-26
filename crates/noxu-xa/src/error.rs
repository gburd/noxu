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

    /// The operation requires crash-durable XA recovery, which is not
    /// implemented in v1.5.
    ///
    /// In v1.5, [`XaResource::xa_prepare`](crate::XaResource::xa_prepare)
    /// records prepared XIDs in a fsync'd `PreparedLog`, but the engine does
    /// not write a `TxnPrepare` WAL record and `noxu-recovery` does not
    /// reconstruct the prepared in-memory `Transaction` on a fresh process.
    /// As a result:
    ///
    /// * `xa_recover` *does* return such XIDs (so operators can see what is
    ///   in doubt), but
    /// * `xa_commit` and `xa_rollback` of those XIDs return this error
    ///   because the underlying transaction state — write locks, undo
    ///   chain, dirty in-memory tree pages — does not exist anymore.
    ///
    /// To clear an in-doubt entry from the persistent prepared log without
    /// resolving its data, use [`XaResource::xa_forget`].
    ///
    /// Crash-durable XA — adding a `TxnPrepare` log record and integrating
    /// it with `noxu-recovery` — is planned for v2.0. See
    /// `docs/src/internal/sprint-3-xa-restriction.md` for the rationale.
    #[error(
        "XA crash durability is not supported in v1.5: this XID exists only \
         in the persistent prepared log; the in-memory branch was lost on \
         process restart. Use xa_forget to discard the persistent record. \
         Crash-durable XA is planned for v2.0."
    )]
    CrashDurabilityNotSupported,

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
