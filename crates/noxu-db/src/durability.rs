//! Durability and sync policies for transactions.
//!

use noxu_dbi::ReplicaAckPolicyKind;

/// Sync policy for local commit synchronization.
///
/// Determines how transaction commits are synchronized to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SyncPolicy {
    /// Write and fsync to disk on commit.
    ///
    /// Maximum durability, but slowest performance. Guarantees that committed
    /// data is written to stable storage.
    #[default]
    Sync,

    /// Write to OS buffers on commit (no fsync).
    ///
    /// Data is written to OS buffers but not necessarily to disk. Faster than
    /// Sync but less durable in case of OS crash.
    WriteNoSync,

    /// No fsync on commit; the commit record is written only to the log
    /// buffer in application memory (not to the OS).
    ///
    /// Maximum performance, minimum durability. The commit stays in the
    /// in-memory log buffer until a later write flushes it to the OS, so a
    /// process crash before that flush loses the commit. (Contrast
    /// [`SyncPolicy::WriteNoSync`], which does write to OS buffers and so
    /// survives a process crash, losing data only on OS/power failure.)
    NoSync,
}

/// Acknowledgment policy for replicated environments.
///
/// Determines how many replicas must acknowledge a transaction before
/// the commit returns to the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ReplicaAckPolicy {
    /// All replicas must acknowledge.
    All,

    /// No acknowledgment required.
    None,

    /// Simple majority must acknowledge.
    #[default]
    SimpleMajority,
}

/// Durability characteristics for a transaction.
///
/// Specifies the durability guarantees associated with a transaction when
/// it's committed. The durability policy consists of:
/// - Local sync policy: how the master node synchronizes
/// - Replica sync policy: how replica nodes synchronize
/// - Replica acknowledgment policy: how many replicas must acknowledge
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Durability {
    /// Sync policy for the local (master) node.
    pub local_sync: SyncPolicy,

    /// Sync policy for replica nodes.
    pub replica_sync: SyncPolicy,

    /// Acknowledgment policy for replicas.
    pub replica_ack: ReplicaAckPolicy,
}

impl ReplicaAckPolicy {
    /// Convert this policy to the dependency-free `ReplicaAckPolicyKind`
    /// used by the `ReplicaAckCoordinator` trait in `noxu-dbi`.
    pub fn as_kind(self) -> ReplicaAckPolicyKind {
        match self {
            ReplicaAckPolicy::All => ReplicaAckPolicyKind::All,
            ReplicaAckPolicy::SimpleMajority => {
                ReplicaAckPolicyKind::SimpleMajority
            }
            ReplicaAckPolicy::None => ReplicaAckPolicyKind::None,
        }
    }
}

impl Durability {
    /// Creates a new Durability with the specified policies.
    pub fn new(
        local_sync: SyncPolicy,
        replica_sync: SyncPolicy,
        replica_ack: ReplicaAckPolicy,
    ) -> Self {
        Self { local_sync, replica_sync, replica_ack }
    }

    /// Convenience constant matching JE `Durability.COMMIT_SYNC`: the master
    /// fsyncs on commit, replicas do not fsync, and a simple majority must
    /// acknowledge. (JE `Durability.java`: `SYNC` / `NO_SYNC` /
    /// `SIMPLE_MAJORITY`.)
    pub const COMMIT_SYNC: Self = Self {
        local_sync: SyncPolicy::Sync,
        replica_sync: SyncPolicy::NoSync,
        replica_ack: ReplicaAckPolicy::SimpleMajority,
    };

    /// Convenience constant matching JE `Durability.COMMIT_NO_SYNC`: neither
    /// the master nor replicas fsync on commit, but a simple majority must
    /// still acknowledge. (JE `Durability.java`: `NO_SYNC` / `NO_SYNC` /
    /// `SIMPLE_MAJORITY`.) The acknowledgment guarantee is preserved even
    /// though the sync policy is relaxed.
    pub const COMMIT_NO_SYNC: Self = Self {
        local_sync: SyncPolicy::NoSync,
        replica_sync: SyncPolicy::NoSync,
        replica_ack: ReplicaAckPolicy::SimpleMajority,
    };

    /// Convenience constant matching JE `Durability.COMMIT_WRITE_NO_SYNC`: the
    /// master writes to OS buffers without fsync, replicas do not fsync, and a
    /// simple majority must acknowledge. (JE `Durability.java`:
    /// `WRITE_NO_SYNC` / `NO_SYNC` / `SIMPLE_MAJORITY`.)
    pub const COMMIT_WRITE_NO_SYNC: Self = Self {
        local_sync: SyncPolicy::WriteNoSync,
        replica_sync: SyncPolicy::NoSync,
        replica_ack: ReplicaAckPolicy::SimpleMajority,
    };
}

impl Default for Durability {
    fn default() -> Self {
        Self::COMMIT_SYNC
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_policy_default() {
        assert_eq!(SyncPolicy::default(), SyncPolicy::Sync);
    }

    #[test]
    fn test_sync_policy_equality() {
        assert_eq!(SyncPolicy::Sync, SyncPolicy::Sync);
        assert_ne!(SyncPolicy::Sync, SyncPolicy::NoSync);
    }

    #[test]
    fn test_replica_ack_policy_default() {
        assert_eq!(
            ReplicaAckPolicy::default(),
            ReplicaAckPolicy::SimpleMajority
        );
    }

    #[test]
    fn test_replica_ack_policy_equality() {
        assert_eq!(ReplicaAckPolicy::All, ReplicaAckPolicy::All);
        assert_ne!(ReplicaAckPolicy::All, ReplicaAckPolicy::None);
    }

    #[test]
    fn test_durability_new() {
        let d = Durability::new(
            SyncPolicy::Sync,
            SyncPolicy::WriteNoSync,
            ReplicaAckPolicy::All,
        );
        assert_eq!(d.local_sync, SyncPolicy::Sync);
        assert_eq!(d.replica_sync, SyncPolicy::WriteNoSync);
        assert_eq!(d.replica_ack, ReplicaAckPolicy::All);
    }

    #[test]
    fn test_durability_commit_sync() {
        // JE Durability.COMMIT_SYNC = (SYNC, NO_SYNC, SIMPLE_MAJORITY).
        let d = Durability::COMMIT_SYNC;
        assert_eq!(d.local_sync, SyncPolicy::Sync);
        assert_eq!(d.replica_sync, SyncPolicy::NoSync);
        assert_eq!(d.replica_ack, ReplicaAckPolicy::SimpleMajority);
    }

    #[test]
    fn test_durability_commit_no_sync() {
        // JE Durability.COMMIT_NO_SYNC = (NO_SYNC, NO_SYNC, SIMPLE_MAJORITY):
        // the majority-ack guarantee is retained even with sync relaxed.
        let d = Durability::COMMIT_NO_SYNC;
        assert_eq!(d.local_sync, SyncPolicy::NoSync);
        assert_eq!(d.replica_sync, SyncPolicy::NoSync);
        assert_eq!(d.replica_ack, ReplicaAckPolicy::SimpleMajority);
    }

    #[test]
    fn test_durability_commit_write_no_sync() {
        // JE Durability.COMMIT_WRITE_NO_SYNC = (WRITE_NO_SYNC, NO_SYNC,
        // SIMPLE_MAJORITY).
        let d = Durability::COMMIT_WRITE_NO_SYNC;
        assert_eq!(d.local_sync, SyncPolicy::WriteNoSync);
        assert_eq!(d.replica_sync, SyncPolicy::NoSync);
        assert_eq!(d.replica_ack, ReplicaAckPolicy::SimpleMajority);
    }

    #[test]
    fn test_durability_default() {
        let d = Durability::default();
        assert_eq!(d, Durability::COMMIT_SYNC);
    }

    #[test]
    fn test_durability_equality() {
        let d1 = Durability::new(
            SyncPolicy::Sync,
            SyncPolicy::NoSync,
            ReplicaAckPolicy::SimpleMajority,
        );
        let d2 = Durability::COMMIT_SYNC;
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_durability_clone() {
        let d1 = Durability::COMMIT_SYNC;
        let d2 = d1;
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_sync_policy_copy() {
        let s1 = SyncPolicy::Sync;
        let s2 = s1;
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_replica_ack_policy_copy() {
        let r1 = ReplicaAckPolicy::All;
        let r2 = r1;
        assert_eq!(r1, r2);
    }
}
