#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Replication and high availability for Noxu DB.
//!
//! Port of `com.sleepycat.je.rep` -- master-replica replication with
//! automatic elections, VLSN tracking, network restore, and subscription.
//!
//! # Architecture
//!
//! The replication layer consists of:
//!
//! - **ReplicatedEnvironment** -- Entry point that wraps a standard Environment
//!   and adds replication capabilities. Port of
//!   `com.sleepycat.je.rep.ReplicatedEnvironment`.
//! - **Elections** -- Automatic master election using majority voting. Port of
//!   `com.sleepycat.je.rep.elections`.
//! - **VLSN Index** -- Maps version sequence numbers to log file positions.
//!   Port of `com.sleepycat.je.rep.vlsn`.
//! - **Feeder/Replica Stream** -- Master-to-replica log entry streaming. Port
//!   of `com.sleepycat.je.rep.stream`.
//! - **Network Transport** -- Pluggable channel-based communication. Port of
//!   `com.sleepycat.je.rep.net`.
//! - **Group Service** -- Replication group membership management.
//! - **Consistency Policies** -- Configurable replica consistency guarantees.
//! - **Master Transfer** -- Controlled transfer of master role.
//! - **Network Restore** -- Full node restore from another replica.
//! - **Subscription** -- External subscription to the replication stream.
//!
//! # Node States
//!
//! A replication node transitions through the following states:
//!
//! - **Detached** -- Not associated with the group (handle closed).
//! - **Unknown** -- Not in contact with the master, actively trying to
//!   establish contact or decide upon a master.
//! - **Master** -- The unique master of the group; can read and write.
//! - **Replica** -- Being updated by the master; read-only.
//!
//! The state transitions visible to the application follow:
//! ```text
//! [ MASTER | REPLICA | UNKNOWN ]+ DETACHED
//! ```
//!
//! # Example
//!
//! ```ignore
//! use noxu_rep::{ReplicatedEnvironment, RepConfig, NodeType};
//!
//! let config = RepConfig::new(
//!     "my_group".to_string(),
//!     "node1".to_string(),
//!     "localhost".to_string(),
//!     5001,
//! );
//! let rep_env = ReplicatedEnvironment::new(config).unwrap();
//! ```

// Foundation modules
pub mod commit_durability;
pub mod consistency;
pub mod error;
pub mod node_type;
pub mod protocol;
pub mod rep_config;
pub mod rep_group;
pub mod rep_node;

// Election subsystem
pub mod elections;

// VLSN tracking subsystem
pub mod vlsn;

// Replication stream subsystem
pub mod stream;

// Node state management
pub mod node_state;

// Group membership
pub mod group_service;

// Acknowledgment tracking
pub mod ack_tracker;

// Statistics
pub mod rep_stats;

// Master transfer
pub mod master_transfer;

// Network transport
pub mod net;

// Subscription API
pub mod subscription;

// Network restore
pub mod network_restore;

// Main API
pub mod replicated_environment;
pub mod state_change_listener;

// Re-export primary types
pub use commit_durability::{CommitDurability, ReplicaAckPolicy};
pub use consistency::ConsistencyPolicy;
pub use error::{RepError, Result};
pub use master_transfer::{
    MasterTransfer, MasterTransferConfig, TransferState,
};
pub use network_restore::{NetworkRestore, NetworkRestoreConfig, RestoreState};
pub use node_state::{NodeState, NodeStateMachine};
pub use node_type::NodeType;
pub use rep_config::RepConfig;
pub use rep_group::RepGroup;
pub use rep_node::RepNode;
pub use rep_stats::RepStats;
pub use replicated_environment::ReplicatedEnvironment;
pub use state_change_listener::{StateChangeEvent, StateChangeListener};
pub use subscription::{
    Subscription, SubscriptionCallback, SubscriptionConfig, SubscriptionState,
};
