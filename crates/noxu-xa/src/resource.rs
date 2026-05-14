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
    fn xa_commit(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;

    /// Roll back the transaction branch.
    fn xa_rollback(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;

    /// Recover prepared transaction branches after a crash.
    ///
    /// Returns a list of XIDs that are in PREPARED state and need resolution.
    fn xa_recover(&self, flags: XaFlags) -> XaResult<Vec<Xid>>;

    /// Forget a heuristically completed transaction branch.
    fn xa_forget(&self, xid: &Xid, flags: XaFlags) -> XaResult<()>;
}
