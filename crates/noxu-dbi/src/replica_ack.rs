//! Replica-acknowledgment coordination trait used by `Transaction::commit`
//! to honour `ReplicaAckPolicy` when an environment is replicated.
//!
//! This module exists in `noxu-dbi` (which both `noxu-db` and
//! `noxu-rep` depend on) so that `noxu-db::Transaction` can call into a
//! replication-aware ack coordinator without `noxu-db` taking a direct
//! dependency on `noxu-rep`.  `noxu-rep::ReplicatedEnvironment`
//! implements this trait; users wire an instance into a `noxu-db::Environment`
//! via `Environment::set_replica_coordinator()`.
//!
//! Closes finding F1 of `docs/src/internal/api-audit-2026-05-rep.md`.

use std::sync::Arc;
use std::time::Duration;

/// Replica acknowledgment policy as visible to the durability path.
///
/// Mirrors `noxu_db::durability::ReplicaAckPolicy` and
/// `noxu_rep::commit_durability::ReplicaAckPolicy` without taking
/// either as a dependency. The enum is enum-stable; adding a variant
/// is a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicaAckPolicyKind {
    /// All electable replicas must acknowledge before the commit
    /// returns.
    All,
    /// A simple majority of electable nodes (including the master)
    /// must acknowledge.
    SimpleMajority,
    /// No replica acknowledgment required; commit returns as soon as
    /// the master has fsynced locally.
    None,
}

impl ReplicaAckPolicyKind {
    /// Number of acks required from peer replicas for the given
    /// total electable count (including the master itself). The
    /// master's own write counts as one ack, so `All` requires
    /// `electable_count - 1` peer acks.
    pub fn required_acks(self, electable_count: u32) -> u32 {
        match self {
            ReplicaAckPolicyKind::All => {
                if electable_count == 0 {
                    0
                } else {
                    electable_count - 1
                }
            }
            ReplicaAckPolicyKind::SimpleMajority => {
                if electable_count <= 1 {
                    0
                } else {
                    let majority = electable_count / 2 + 1;
                    majority - 1
                }
            }
            ReplicaAckPolicyKind::None => 0,
        }
    }
}

/// Reason an ack-wait did not satisfy the durability contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckWaitErrorKind {
    /// `ack_timeout` elapsed before enough replicas acknowledged the
    /// commit. The commit is durably written locally but does not meet
    /// the configured replication policy.
    Timeout,
    /// Commit was attempted on a replica node, which is not permitted.
    NotMaster,
    /// The replicated environment is shutting down and cannot wait for
    /// acks.
    Shutdown,
}

/// Error returned by [`ReplicaAckCoordinator::await_replica_acks`] when
/// the configured number of replica acks could not be obtained within
/// the supplied timeout.
#[derive(Debug, Clone)]
pub struct AckWaitError {
    /// Kind of failure (timeout / not master / shutdown).
    pub kind: AckWaitErrorKind,
    /// Number of acks required by the policy.
    pub needed: u32,
    /// Number of acks actually received before the deadline.
    pub received: u32,
}

impl std::fmt::Display for AckWaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            AckWaitErrorKind::Timeout => write!(
                f,
                "replica ack timeout: needed {}, received {}",
                self.needed, self.received,
            ),
            AckWaitErrorKind::NotMaster => {
                write!(f, "commit attempted on non-master node")
            }
            AckWaitErrorKind::Shutdown => {
                write!(f, "replicated environment is shutting down")
            }
        }
    }
}

impl std::error::Error for AckWaitError {}

/// Coordinates with a replication subsystem to satisfy a replica-ack
/// policy on commit.
///
/// Implementations are typically `noxu_rep::ReplicatedEnvironment`. The
/// `noxu-db::Environment` holds an `Option<Arc<dyn ReplicaAckCoordinator>>`;
/// when present, `Transaction::commit_with_durability` calls
/// [`Self::await_replica_acks`] after the local WAL fsync and propagates
/// any error as `NoxuError::InsufficientReplicas`.
pub trait ReplicaAckCoordinator: Send + Sync {
    /// Block until at least `policy.required_acks(electable_count)`
    /// replicas have acknowledged the most-recent local commit, or
    /// until `timeout` elapses, whichever comes first.
    ///
    /// Returns `Ok(received_acks)` on success. Returns
    /// [`AckWaitError`] if the deadline expires before the policy is
    /// satisfied, or if this coordinator is not in a state where
    /// commits may be acknowledged (replica node / shutting down).
    ///
    /// Implementations are responsible for assigning the commit VLSN
    /// internally and for cleaning up internal tracking state on both
    /// success and failure paths.
    fn await_replica_acks(
        &self,
        policy: ReplicaAckPolicyKind,
        timeout: Duration,
    ) -> std::result::Result<u32, AckWaitError>;

    /// Allocate the next commit VLSN and register `lsn` in the VLSN index.
    ///
    /// Called by `Environment::write_txn_commit_for_recovered` after
    /// writing a `TxnCommit` WAL frame for a recovered prepared (XA)
    /// transaction.  In a replicated environment the commit must be visible
    /// to feeders and replicas, so it needs a real VLSN assigned and
    /// registered in the `VlsnIndex`.
    ///
    /// Returns the allocated VLSN (> 0) on success, or 0
    /// (`NULL_VLSN`) if this node is not in a replicated or master
    /// state where VLSN assignment makes sense.
    ///
    /// The default implementation returns 0 (non-replicated env).  X-3 fix.
    fn alloc_vlsn_for_recovered_commit(&self, _lsn: noxu_util::Lsn) -> u64 {
        0
    }

    /// Pre-allocate the next VLSN for a recovered XA commit *without*
    /// registering it in the VLSN index yet.
    ///
    /// R-3 fix: called BEFORE writing the `TxnCommit` WAL entry so the entry
    /// can carry the allocated VLSN.  The caller then writes the entry and
    /// calls `register_recovered_commit_vlsn` with the resulting commit LSN.
    ///
    /// Returns 0 (NULL_VLSN) for non-replicated environments.
    fn pre_alloc_vlsn_for_recovered_commit(&self) -> u64 {
        0
    }

    /// Register a previously pre-allocated VLSN in the VLSN index, mapping
    /// it to the actual WAL commit LSN.
    ///
    /// R-3 fix: called AFTER writing the `TxnCommit` WAL entry with the
    /// pre-allocated VLSN.  The `commit_lsn` is the LSN of the TxnCommit
    /// entry just written to the log.
    ///
    /// No-op for non-replicated environments (default).
    fn register_recovered_commit_vlsn(
        &self,
        _vlsn: u64,
        _commit_lsn: noxu_util::Lsn,
    ) {
    }
}

/// Type alias used in `noxu-db::Environment` to hold the optional
/// installed coordinator.
pub type SharedReplicaAckCoordinator = Arc<dyn ReplicaAckCoordinator>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_acks_all() {
        assert_eq!(ReplicaAckPolicyKind::All.required_acks(0), 0);
        assert_eq!(ReplicaAckPolicyKind::All.required_acks(1), 0);
        assert_eq!(ReplicaAckPolicyKind::All.required_acks(3), 2);
        assert_eq!(ReplicaAckPolicyKind::All.required_acks(5), 4);
    }

    #[test]
    fn required_acks_simple_majority() {
        assert_eq!(ReplicaAckPolicyKind::SimpleMajority.required_acks(0), 0);
        assert_eq!(ReplicaAckPolicyKind::SimpleMajority.required_acks(1), 0);
        assert_eq!(ReplicaAckPolicyKind::SimpleMajority.required_acks(3), 1);
        assert_eq!(ReplicaAckPolicyKind::SimpleMajority.required_acks(5), 2);
    }

    #[test]
    fn required_acks_none() {
        assert_eq!(ReplicaAckPolicyKind::None.required_acks(0), 0);
        assert_eq!(ReplicaAckPolicyKind::None.required_acks(100), 0);
    }
}
