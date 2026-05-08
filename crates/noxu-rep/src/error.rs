//! Replication error types.
//!

use thiserror::Error;

/// Errors that can occur during replication operations.
#[derive(Debug, Error)]
pub enum RepError {
    /// The node is not the master. Thrown when a write operation is attempted
    /// on a replica node.
    /// 
    #[error("node is not master: current master is {master:?}")]
    NotMaster {
        /// The name of the current master, if known.
        master: Option<String>,
    },

    /// The node is not a replica. Thrown when a replica-only operation is
    /// attempted on the master.
    #[error("node is not replica")]
    NotReplica,

    /// The replication group does not exist.
    /// (group aspect).
    #[error("replication group does not exist: {0}")]
    GroupNotFound(String),

    /// A node was not found in the replication group.
    /// 
    #[error("node {0} not found in group")]
    NodeNotFound(String),

    /// An election failed to produce a master.
    /// 
    #[error("election failed: {0}")]
    ElectionFailed(String),

    /// Insufficient replica acknowledgments for a commit.
    /// 
    #[error("insufficient acks: needed {needed}, got {received}")]
    InsufficientAcks {
        /// Number of acks required by the durability policy.
        needed: u32,
        /// Number of acks actually received.
        received: u32,
    },

    /// A replica consistency policy timed out.
    /// 
    #[error("replica consistency timeout after {0:?}")]
    ConsistencyTimeout(std::time::Duration),

    /// The replica's replication lag exceeds the configured limit.
    #[error("replica lag too high: {lag_ms}ms exceeds limit {limit_ms}ms")]
    ReplicaLagExceeded {
        /// Current lag in milliseconds.
        lag_ms: u64,
        /// Configured limit in milliseconds.
        limit_ms: u64,
    },

    /// A hard rollback is required on the replica.
    /// 
    #[error("rollback required: from VLSN {from} to {to}")]
    RollbackRequired {
        /// The VLSN sequence to roll back from.
        from: i64,
        /// The VLSN sequence to roll back to.
        to: i64,
    },

    /// A network-level error occurred.
    #[error("network error: {0}")]
    NetworkError(String),

    /// A replication protocol error occurred (unexpected message, version
    /// mismatch, etc.).
    #[error("protocol error: {0}")]
    ProtocolError(String),

    /// The node is in an invalid state for the requested operation.
    #[error("node state error: {0}")]
    StateError(String),

    /// An underlying database error occurred.
    #[error("database error: {0}")]
    DatabaseError(String),

    /// A configuration error was detected.
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// The node is shutting down and cannot accept new operations.
    #[error("shutdown in progress")]
    ShutdownInProgress,

    /// Invalid state transition attempted.
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(String),

    /// A node with the same name already exists in the group.
    #[error("node already exists: {0}")]
    NodeAlreadyExists(String),

    /// A network channel has been closed.
    #[error("channel closed: {0}")]
    ChannelClosed(String),

    /// A requested service was not found.
    #[error("service not found: {0}")]
    ServiceNotFound(String),

    /// A subscription error occurred.
    #[error("subscription error: {0}")]
    SubscriptionError(String),

    /// A network restore error occurred.
    #[error("network restore error: {0}")]
    NetworkRestoreError(String),

    /// The environment has been closed.
    #[error("environment closed")]
    EnvironmentClosed,

    /// The node is in an invalid state.
    #[error("invalid state: {0}")]
    InvalidState(String),
}

/// Convenience type alias for replication results.
pub type Result<T> = std::result::Result<T, RepError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_not_master_with_master() {
        let err = RepError::NotMaster { master: Some("node1".to_string()) };
        assert_eq!(
            err.to_string(),
            "node is not master: current master is Some(\"node1\")"
        );
    }

    #[test]
    fn test_not_master_without_master() {
        let err = RepError::NotMaster { master: None };
        assert_eq!(
            err.to_string(),
            "node is not master: current master is None"
        );
    }

    #[test]
    fn test_not_replica() {
        let err = RepError::NotReplica;
        assert_eq!(err.to_string(), "node is not replica");
    }

    #[test]
    fn test_group_not_found() {
        let err = RepError::GroupNotFound("mygroup".to_string());
        assert_eq!(
            err.to_string(),
            "replication group does not exist: mygroup"
        );
    }

    #[test]
    fn test_node_not_found() {
        let err = RepError::NodeNotFound("node2".to_string());
        assert_eq!(err.to_string(), "node node2 not found in group");
    }

    #[test]
    fn test_election_failed() {
        let err = RepError::ElectionFailed("no quorum".to_string());
        assert_eq!(err.to_string(), "election failed: no quorum");
    }

    #[test]
    fn test_insufficient_acks() {
        let err = RepError::InsufficientAcks { needed: 3, received: 1 };
        assert_eq!(err.to_string(), "insufficient acks: needed 3, got 1");
    }

    #[test]
    fn test_consistency_timeout() {
        let err = RepError::ConsistencyTimeout(Duration::from_secs(5));
        assert_eq!(err.to_string(), "replica consistency timeout after 5s");
    }

    #[test]
    fn test_replica_lag_exceeded() {
        let err = RepError::ReplicaLagExceeded { lag_ms: 5000, limit_ms: 1000 };
        assert_eq!(
            err.to_string(),
            "replica lag too high: 5000ms exceeds limit 1000ms"
        );
    }

    #[test]
    fn test_rollback_required() {
        let err = RepError::RollbackRequired { from: 100, to: 50 };
        assert_eq!(err.to_string(), "rollback required: from VLSN 100 to 50");
    }

    #[test]
    fn test_network_error() {
        let err = RepError::NetworkError("connection refused".to_string());
        assert_eq!(err.to_string(), "network error: connection refused");
    }

    #[test]
    fn test_protocol_error() {
        let err = RepError::ProtocolError("version mismatch".to_string());
        assert_eq!(err.to_string(), "protocol error: version mismatch");
    }

    #[test]
    fn test_state_error() {
        let err = RepError::StateError("not initialized".to_string());
        assert_eq!(err.to_string(), "node state error: not initialized");
    }

    #[test]
    fn test_database_error() {
        let err = RepError::DatabaseError("corrupt log".to_string());
        assert_eq!(err.to_string(), "database error: corrupt log");
    }

    #[test]
    fn test_config_error() {
        let err = RepError::ConfigError("invalid port".to_string());
        assert_eq!(err.to_string(), "configuration error: invalid port");
    }

    #[test]
    fn test_shutdown_in_progress() {
        let err = RepError::ShutdownInProgress;
        assert_eq!(err.to_string(), "shutdown in progress");
    }

    #[test]
    fn test_result_type_alias() {
        let ok: Result<u32> = Ok(42);
        assert!(ok.is_ok_and(|v| v == 42));

        let err: Result<u32> = Err(RepError::NotReplica);
        assert!(err.is_err());
    }
}
