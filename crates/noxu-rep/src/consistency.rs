//! Consistency policies for replica reads.
//!
//! Rep.TimeConsistencyPolicy`, and
//! Rep.CommitPointConsistencyPolicy`.

use std::time::Duration;

use crate::error::{RepError, Result};

/// A consistency policy that determines what state a replica must be in
/// before a read operation can proceed.
///
/// Consistency policy hierarchy for replication.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ConsistencyPolicy {
    /// No consistency requirement -- read from any state.
    ///
    /// 
    #[default]
    NoConsistency,

    /// Time-based consistency: the replica must be within `max_lag` of
    /// the master's commit point.
    ///
    /// 
    TimeConsistency {
        /// Maximum permissible lag behind the master.
        max_lag: Duration,
        /// How long to wait for the replica to catch up.
        timeout: Duration,
    },

    /// Commit-point consistency: the replica must have applied up to
    /// a specific VLSN before the read can proceed.
    ///
    /// 
    CommitPointConsistency {
        /// The VLSN sequence that must be applied on the replica.
        vlsn: i64,
        /// How long to wait for the replica to reach the VLSN.
        timeout: Duration,
    },
}

impl ConsistencyPolicy {
    /// Checks whether the given replica state satisfies this consistency
    /// policy.
    ///
    /// - `current_vlsn`: The replica's current VLSN sequence.
    /// - `master_vlsn`: The master's current VLSN sequence.
    ///
    /// Returns `Ok(true)` if the consistency requirement is met, or an
    /// error describing why it is not.
    pub fn check_consistency(
        &self,
        current_vlsn: i64,
        master_vlsn: i64,
    ) -> Result<bool> {
        match self {
            ConsistencyPolicy::NoConsistency => Ok(true),

            ConsistencyPolicy::TimeConsistency { max_lag, .. } => {
                // Approximate: each VLSN is roughly 1ms of lag.
                // In a real implementation this would use timestamps from
                // heartbeat messages. Here we use VLSN difference as a proxy.
                let lag_vlsns = master_vlsn.saturating_sub(current_vlsn);
                if lag_vlsns < 0 {
                    // Replica is ahead -- shouldn't happen, but treat as ok.
                    return Ok(true);
                }
                let lag_ms = lag_vlsns as u64;
                let limit_ms = max_lag.as_millis() as u64;
                if lag_ms <= limit_ms {
                    Ok(true)
                } else {
                    Err(RepError::ReplicaLagExceeded { lag_ms, limit_ms })
                }
            }

            ConsistencyPolicy::CommitPointConsistency { vlsn, .. } => {
                if current_vlsn >= *vlsn {
                    Ok(true)
                } else {
                    Err(RepError::ConsistencyTimeout(
                        // Report the timeout configured for this policy.
                        self.timeout().unwrap_or(Duration::ZERO),
                    ))
                }
            }
        }
    }

    /// Returns the timeout associated with this policy, if any.
    pub fn timeout(&self) -> Option<Duration> {
        match self {
            ConsistencyPolicy::NoConsistency => None,
            ConsistencyPolicy::TimeConsistency { timeout, .. } => {
                Some(*timeout)
            }
            ConsistencyPolicy::CommitPointConsistency { timeout, .. } => {
                Some(*timeout)
            }
        }
    }
}

impl std::fmt::Display for ConsistencyPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsistencyPolicy::NoConsistency => write!(f, "NoConsistency"),
            ConsistencyPolicy::TimeConsistency { max_lag, timeout } => {
                write!(
                    f,
                    "TimeConsistency(max_lag={:?}, timeout={:?})",
                    max_lag, timeout
                )
            }
            ConsistencyPolicy::CommitPointConsistency { vlsn, timeout } => {
                write!(
                    f,
                    "CommitPointConsistency(vlsn={}, timeout={:?})",
                    vlsn, timeout
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_consistency_always_passes() {
        let policy = ConsistencyPolicy::NoConsistency;
        assert!(policy.check_consistency(0, 1000).unwrap());
        assert!(policy.check_consistency(1000, 1000).unwrap());
        assert!(policy.check_consistency(1000, 0).unwrap());
    }

    #[test]
    fn test_no_consistency_timeout_is_none() {
        let policy = ConsistencyPolicy::NoConsistency;
        assert!(policy.timeout().is_none());
    }

    #[test]
    fn test_time_consistency_within_lag() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        // Replica is 50 VLSNs behind, limit is 100ms.
        assert!(policy.check_consistency(950, 1000).unwrap());
    }

    #[test]
    fn test_time_consistency_at_limit() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        // Exactly at limit.
        assert!(policy.check_consistency(900, 1000).unwrap());
    }

    #[test]
    fn test_time_consistency_exceeds_lag() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        let result = policy.check_consistency(800, 1000);
        assert!(result.is_err());
        match result.unwrap_err() {
            RepError::ReplicaLagExceeded { lag_ms, limit_ms } => {
                assert_eq!(lag_ms, 200);
                assert_eq!(limit_ms, 100);
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_time_consistency_replica_ahead() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        // Replica ahead of master -- should pass.
        assert!(policy.check_consistency(1000, 500).unwrap());
    }

    #[test]
    fn test_time_consistency_timeout() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        assert_eq!(policy.timeout(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_commit_point_satisfied() {
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 500,
            timeout: Duration::from_secs(10),
        };
        assert!(policy.check_consistency(500, 1000).unwrap());
        assert!(policy.check_consistency(600, 1000).unwrap());
    }

    #[test]
    fn test_commit_point_not_satisfied() {
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 500,
            timeout: Duration::from_secs(10),
        };
        let result = policy.check_consistency(400, 1000);
        assert!(result.is_err());
        match result.unwrap_err() {
            RepError::ConsistencyTimeout(d) => {
                assert_eq!(d, Duration::from_secs(10));
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_commit_point_timeout() {
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 100,
            timeout: Duration::from_secs(10),
        };
        assert_eq!(policy.timeout(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_default_is_no_consistency() {
        assert_eq!(
            ConsistencyPolicy::default(),
            ConsistencyPolicy::NoConsistency
        );
    }

    #[test]
    fn test_display_no_consistency() {
        assert_eq!(
            ConsistencyPolicy::NoConsistency.to_string(),
            "NoConsistency"
        );
    }

    #[test]
    fn test_display_time_consistency() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(500),
            timeout: Duration::from_secs(10),
        };
        let s = policy.to_string();
        assert!(s.contains("TimeConsistency"));
        assert!(s.contains("500ms"));
    }

    #[test]
    fn test_display_commit_point() {
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 42,
            timeout: Duration::from_secs(5),
        };
        let s = policy.to_string();
        assert!(s.contains("CommitPointConsistency"));
        assert!(s.contains("42"));
    }

    #[test]
    fn test_clone_and_eq() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        let cloned = policy.clone();
        assert_eq!(policy, cloned);
    }
}
