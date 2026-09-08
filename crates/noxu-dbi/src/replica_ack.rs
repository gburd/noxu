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
//! Closes finding F1 of the 2026 review.

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

    /// A coordinator that implements ONLY the required method. The three VLSN
    /// methods default to "no VLSN" (0 = NULL_VLSN), which is the documented
    /// non-replicated behaviour -- `Environment::write_txn_commit_for_recovered`
    /// calls them unconditionally, so a default that panicked or returned a
    /// bogus non-zero VLSN would break XA recovery on every non-replicated
    /// environment.
    struct NonReplicated;

    impl ReplicaAckCoordinator for NonReplicated {
        fn await_replica_acks(
            &self,
            _policy: ReplicaAckPolicyKind,
            _timeout: Duration,
        ) -> std::result::Result<u32, AckWaitError> {
            Ok(0)
        }
    }

    #[test]
    fn vlsn_defaults_report_null_vlsn_for_a_non_replicated_coordinator() {
        let c = NonReplicated;
        assert_eq!(
            c.alloc_vlsn_for_recovered_commit(noxu_util::Lsn::new(1, 1)),
            0,
            "0 is NULL_VLSN: a non-replicated env allocates no VLSN"
        );
        assert_eq!(c.pre_alloc_vlsn_for_recovered_commit(), 0);
        // The register half must be a silent no-op, not a panic -- XA recovery
        // calls it after every recovered commit.
        c.register_recovered_commit_vlsn(0, noxu_util::Lsn::new(1, 1));
        c.register_recovered_commit_vlsn(42, noxu_util::Lsn::new(9, 9));
    }

    /// The same through a trait object, which is how `noxu-db::Environment`
    /// actually holds the coordinator (`Option<Arc<dyn ReplicaAckCoordinator>>`).
    #[test]
    fn vlsn_defaults_are_reachable_through_a_trait_object() {
        let c: SharedReplicaAckCoordinator = Arc::new(NonReplicated);
        assert_eq!(c.pre_alloc_vlsn_for_recovered_commit(), 0);
        assert_eq!(
            c.alloc_vlsn_for_recovered_commit(noxu_util::Lsn::new(2, 2)),
            0
        );
        c.register_recovered_commit_vlsn(0, noxu_util::Lsn::new(2, 2));
    }

    /// An overriding coordinator must get ITS versions, not the defaults --
    /// otherwise a replicated environment's VLSN assignment would be silently
    /// swallowed and its commits would never reach a feeder.
    #[test]
    fn an_overriding_coordinator_gets_its_own_vlsn_methods() {
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct Replicated {
            next: AtomicU64,
            registered: Mutex<Vec<(u64, u64)>>,
        }
        impl ReplicaAckCoordinator for Replicated {
            fn await_replica_acks(
                &self,
                _p: ReplicaAckPolicyKind,
                _t: Duration,
            ) -> std::result::Result<u32, AckWaitError> {
                Ok(1)
            }
            fn pre_alloc_vlsn_for_recovered_commit(&self) -> u64 {
                self.next.fetch_add(1, Ordering::SeqCst) + 1
            }
            fn register_recovered_commit_vlsn(
                &self,
                vlsn: u64,
                commit_lsn: noxu_util::Lsn,
            ) {
                self.registered
                    .lock()
                    .unwrap()
                    .push((vlsn, commit_lsn.as_u64()));
            }
        }

        let c = Replicated {
            next: AtomicU64::new(0),
            registered: Mutex::new(Vec::new()),
        };
        // R-3 ordering: pre-allocate, write the WAL entry, then register.
        let v1 = c.pre_alloc_vlsn_for_recovered_commit();
        let v2 = c.pre_alloc_vlsn_for_recovered_commit();
        assert_eq!((v1, v2), (1, 2), "VLSNs must be handed out in order");
        assert!(v1 > 0, "a replicated env must allocate a REAL vlsn, not 0");

        let lsn = noxu_util::Lsn::new(3, 7);
        c.register_recovered_commit_vlsn(v1, lsn);
        assert_eq!(
            *c.registered.lock().unwrap(),
            vec![(1, lsn.as_u64())],
            "the override must receive the pre-allocated vlsn and its LSN"
        );
    }

    /// `AckWaitError`'s Display is what surfaces in a
    /// `NoxuError::InsufficientReplicas`, so it must distinguish the three
    /// failure kinds -- an operator cannot tell a timeout from a
    /// wrong-node commit otherwise -- and the timeout case must carry the
    /// counts that explain it.
    #[test]
    fn ack_wait_error_display_distinguishes_all_three_kinds() {
        let timeout = AckWaitError {
            kind: AckWaitErrorKind::Timeout,
            needed: 2,
            received: 1,
        }
        .to_string();
        assert!(timeout.contains("timeout"), "got: {timeout}");
        assert!(
            timeout.contains('2') && timeout.contains('1'),
            "the timeout message must report needed and received; got: {timeout}"
        );

        let not_master = AckWaitError {
            kind: AckWaitErrorKind::NotMaster,
            needed: 2,
            received: 0,
        }
        .to_string();
        let shutdown = AckWaitError {
            kind: AckWaitErrorKind::Shutdown,
            needed: 2,
            received: 0,
        }
        .to_string();

        assert!(not_master.contains("non-master"), "got: {not_master}");
        assert!(shutdown.contains("shutting down"), "got: {shutdown}");
        assert_ne!(timeout, not_master);
        assert_ne!(not_master, shutdown);
        assert_ne!(timeout, shutdown);

        // It must also be a real std::error::Error, since callers wrap it.
        let e: &dyn std::error::Error = &AckWaitError {
            kind: AckWaitErrorKind::Timeout,
            needed: 1,
            received: 0,
        };
        assert!(!e.to_string().is_empty());
    }
}
