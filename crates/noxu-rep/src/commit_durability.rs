//! Commit durability settings for replication.
//!
//! specifically `Durability.ReplicaAckPolicy`.

use std::time::Duration;

/// Policy for how many replicas must acknowledge a commit before it
/// is considered durable.
///
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ReplicaAckPolicy {
    /// All electable replicas must acknowledge the commit.
    All,

    /// A simple majority of electable nodes (including the master)
    /// must acknowledge the commit.
    #[default]
    SimpleMajority,

    /// No replica acknowledgment is required. The commit returns
    /// as soon as the master has written it locally.
    None,
}

impl ReplicaAckPolicy {
    /// Returns the number of acknowledgments required for the given
    /// number of electable nodes in the group.
    ///
    /// - `All`: requires `electable_count - 1` acks (all replicas).
    /// - `SimpleMajority`: requires `(electable_count / 2 + 1) - 1` acks
    ///   (majority minus the master itself).
    /// - `None`: requires 0 acks.
    pub fn required_acks(&self, electable_count: u32) -> u32 {
        match self {
            ReplicaAckPolicy::All => {
                if electable_count == 0 {
                    0
                } else {
                    electable_count - 1
                }
            }
            ReplicaAckPolicy::SimpleMajority => {
                if electable_count <= 1 {
                    0
                } else {
                    // Majority of all electable nodes, minus the master.
                    let majority = electable_count / 2 + 1;
                    majority - 1
                }
            }
            ReplicaAckPolicy::None => 0,
        }
    }
}

impl std::fmt::Display for ReplicaAckPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicaAckPolicy::All => write!(f, "ALL"),
            ReplicaAckPolicy::SimpleMajority => write!(f, "SIMPLE_MAJORITY"),
            ReplicaAckPolicy::None => write!(f, "NONE"),
        }
    }
}

/// Commit durability settings for replicated transactions.
///
/// Combines the acknowledgment policy with a timeout for waiting
/// for replica acks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitDurability {
    /// The replica acknowledgment policy.
    pub ack_policy: ReplicaAckPolicy,
    /// How long to wait for replica acknowledgments before giving up.
    pub ack_timeout: Duration,
}

impl CommitDurability {
    /// Creates a new `CommitDurability` with the given policy and timeout.
    pub fn new(ack_policy: ReplicaAckPolicy, ack_timeout: Duration) -> Self {
        Self { ack_policy, ack_timeout }
    }

    /// Returns the number of acknowledgments required for the given
    /// number of electable nodes.
    pub fn required_acks(&self, electable_count: u32) -> u32 {
        self.ack_policy.required_acks(electable_count)
    }
}

impl Default for CommitDurability {
    fn default() -> Self {
        Self {
            ack_policy: ReplicaAckPolicy::default(),
            ack_timeout: Duration::from_secs(5),
        }
    }
}

impl std::fmt::Display for CommitDurability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CommitDurability(ack_policy={}, ack_timeout={:?})",
            self.ack_policy, self.ack_timeout
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- ReplicaAckPolicy tests ---

    #[test]
    fn test_all_required_acks() {
        assert_eq!(ReplicaAckPolicy::All.required_acks(0), 0);
        assert_eq!(ReplicaAckPolicy::All.required_acks(1), 0);
        assert_eq!(ReplicaAckPolicy::All.required_acks(2), 1);
        assert_eq!(ReplicaAckPolicy::All.required_acks(3), 2);
        assert_eq!(ReplicaAckPolicy::All.required_acks(5), 4);
    }

    #[test]
    fn test_simple_majority_required_acks() {
        assert_eq!(ReplicaAckPolicy::SimpleMajority.required_acks(0), 0);
        assert_eq!(ReplicaAckPolicy::SimpleMajority.required_acks(1), 0);
        // 2 electable: majority=2, minus master=1
        assert_eq!(ReplicaAckPolicy::SimpleMajority.required_acks(2), 1);
        // 3 electable: majority=2, minus master=1
        assert_eq!(ReplicaAckPolicy::SimpleMajority.required_acks(3), 1);
        // 4 electable: majority=3, minus master=2
        assert_eq!(ReplicaAckPolicy::SimpleMajority.required_acks(4), 2);
        // 5 electable: majority=3, minus master=2
        assert_eq!(ReplicaAckPolicy::SimpleMajority.required_acks(5), 2);
    }

    #[test]
    fn test_none_required_acks() {
        assert_eq!(ReplicaAckPolicy::None.required_acks(0), 0);
        assert_eq!(ReplicaAckPolicy::None.required_acks(1), 0);
        assert_eq!(ReplicaAckPolicy::None.required_acks(5), 0);
        assert_eq!(ReplicaAckPolicy::None.required_acks(100), 0);
    }

    #[test]
    fn test_ack_policy_display() {
        assert_eq!(ReplicaAckPolicy::All.to_string(), "ALL");
        assert_eq!(
            ReplicaAckPolicy::SimpleMajority.to_string(),
            "SIMPLE_MAJORITY"
        );
        assert_eq!(ReplicaAckPolicy::None.to_string(), "NONE");
    }

    #[test]
    fn test_ack_policy_default() {
        assert_eq!(
            ReplicaAckPolicy::default(),
            ReplicaAckPolicy::SimpleMajority
        );
    }

    #[test]
    fn test_ack_policy_clone_copy_eq_hash() {
        use std::collections::HashSet;
        let p = ReplicaAckPolicy::All;
        let p2 = p;
        assert_eq!(p, p2);
        let mut set = HashSet::new();
        set.insert(ReplicaAckPolicy::All);
        set.insert(ReplicaAckPolicy::SimpleMajority);
        set.insert(ReplicaAckPolicy::None);
        assert_eq!(set.len(), 3);
    }

    // --- CommitDurability tests ---

    #[test]
    fn test_commit_durability_new() {
        let cd = CommitDurability::new(
            ReplicaAckPolicy::All,
            Duration::from_secs(10),
        );
        assert_eq!(cd.ack_policy, ReplicaAckPolicy::All);
        assert_eq!(cd.ack_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_commit_durability_required_acks() {
        let cd = CommitDurability::new(
            ReplicaAckPolicy::SimpleMajority,
            Duration::from_secs(5),
        );
        assert_eq!(cd.required_acks(3), 1);
        assert_eq!(cd.required_acks(5), 2);
    }

    #[test]
    fn test_commit_durability_default() {
        let cd = CommitDurability::default();
        assert_eq!(cd.ack_policy, ReplicaAckPolicy::SimpleMajority);
        assert_eq!(cd.ack_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_commit_durability_display() {
        let cd = CommitDurability::default();
        let s = cd.to_string();
        assert!(s.contains("SIMPLE_MAJORITY"));
        assert!(s.contains("5s"));
    }

    #[test]
    fn test_commit_durability_clone_eq() {
        let cd = CommitDurability::default();
        let cloned = cd;
        assert_eq!(cd, cloned);
    }
}
