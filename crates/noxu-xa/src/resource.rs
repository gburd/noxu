//! XA Resource Manager trait.

use crate::error::{PrepareResult, XaResult};
use crate::flags::XaFlags;
use crate::xid::Xid;

/// XA Resource Manager interface.
///
/// Implements the X/Open XA interface for distributed transaction coordination.
/// A Transaction Manager (TM) calls these methods to drive the two-phase commit
/// protocol across multiple resource managers (RMs).
///
/// State machine per Xid:
/// ```text
/// IDLE → (xa_start) → ACTIVE
/// ACTIVE → (xa_end TMSUCCESS) → IDLE
/// ACTIVE → (xa_end TMFAIL) → ROLLBACK_ONLY
/// ACTIVE → (xa_end TMSUSPEND) → SUSPENDED
/// SUSPENDED → (xa_start RESUME) → ACTIVE
/// IDLE → (xa_prepare) → PREPARED
/// IDLE → (xa_commit ONEPHASE) → [committed, removed]
/// PREPARED → (xa_commit) → [committed, removed]
/// PREPARED → (xa_rollback) → [rolled back, removed]
/// ROLLBACK_ONLY → (xa_rollback) → [rolled back, removed]
/// ```
pub trait XaResource: Send + Sync {
    /// Start work on behalf of a transaction branch.
    fn xa_start(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;

    /// End work on behalf of a transaction branch.
    fn xa_end(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;

    /// Prepare to commit the transaction branch (phase 1 of 2PC).
    ///
    /// Returns `PrepareResult::ReadOnly` if the branch performed no writes.
    fn xa_prepare(&self, xid: &Xid, flags: XaFlags) -> XaResult<PrepareResult>;

    /// Commit the transaction branch (phase 2 of 2PC).
    ///
    /// If `flags` contains `ONEPHASE`, this is a one-phase commit optimization.
    ///
    /// # v1.5 limitation
    ///
    /// If `xid` exists only in the persistent prepared log (i.e. it was
    /// prepared in a previous process and the in-memory branch was lost on
    /// restart), this returns [`XaError::CrashDurabilityNotSupported`].
    /// Crash-durable XA is planned for v2.0.
    fn xa_commit(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;

    /// Roll back the transaction branch.
    ///
    /// # v1.5 limitation
    ///
    /// As with `xa_commit`, calling `xa_rollback` on a XID that survives
    /// only in the persistent prepared log returns
    /// [`XaError::CrashDurabilityNotSupported`]. Use
    /// [`XaResource::xa_forget`] to discard the persistent record.
    fn xa_rollback(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;

    /// Recover prepared transaction branches after a crash.
    ///
    /// Returns a list of XIDs that are in PREPARED state and need resolution.
    ///
    /// # v1.5 honesty caveat
    ///
    /// In v1.5, XA is in-process only: the persistent prepared log is
    /// fsync'd by `xa_prepare`, but the engine does not write a
    /// `TxnPrepare` WAL record and `noxu-recovery` does not reconstruct the
    /// in-memory `Transaction` on restart. After a fresh process start the
    /// returned list may include XIDs that *cannot* be committed or rolled
    /// back — attempting to do so returns
    /// [`XaError::CrashDurabilityNotSupported`]. Use `xa_forget` to discard
    /// such entries from the persistent log. Crash-durable XA is planned
    /// for v2.0.
    fn xa_recover(&self, flags: XaFlags) -> XaResult<Vec<Xid>>;

    /// Forget a heuristically completed transaction branch.
    fn xa_forget(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;
}
