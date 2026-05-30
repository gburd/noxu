//! XA Resource Manager trait.

use crate::xa::error::{PrepareResult, XaResult};
use crate::xa::flags::XaFlags;
use crate::xa::xid::Xid;

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
    /// Crash-durable XA is supported as of v2.0: a `TxnPrepare`
    /// WAL frame is written during `xa_prepare`, and recovery surfaces
    /// in-doubt XIDs via `xa_recover()`.  The deprecated variant
    /// [`crate::xa::error::XaError::CrashDurabilityNotSupported`] is no longer returned.
    fn xa_commit(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;

    /// Roll back the transaction branch.
    ///
    /// Crash-durable XA is supported as of v2.0.
    /// The deprecated variant
    /// [`crate::xa::error::XaError::CrashDurabilityNotSupported`] is no
    /// longer returned; use [`XaResource::xa_forget`] to discard a heuristic
    /// record from the persistent log.
    fn xa_rollback(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;

    /// Recover prepared transaction branches after a crash.
    ///
    /// Returns a list of XIDs that are in PREPARED state and need resolution.
    /// As of v2.0 the engine writes a `TxnPrepare` WAL record
    /// on `xa_prepare` and `noxu-recovery` reconstructs the in-memory
    /// `Transaction` on restart.  The deprecated variant
    /// [`crate::xa::error::XaError::CrashDurabilityNotSupported`] is no longer returned.
    fn xa_recover(&self, flags: XaFlags) -> XaResult<Vec<Xid>>;

    /// Forget a heuristically completed transaction branch.
    fn xa_forget(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;
}
