// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Replication and high availability for Noxu DB.
//!
//! master-replica replication with
//! automatic elections, VLSN tracking, network restore, and subscription.
//!
//! # Architecture
//!
//! The replication layer consists of:
//!
//! - **ReplicatedEnvironment** -- Entry point that wraps a standard Environment
//!   and adds replication capabilities.
//!   Rep.ReplicatedEnvironment`.
//! - **Elections** -- Automatic master election using majority voting.
//!   Rep.elections`.
//! - **VLSN Index** -- Maps version sequence numbers to log file positions.
//! - **Feeder/Replica Stream** -- Master-to-replica log entry streaming. Port
//! - **Network Transport** -- Pluggable channel-based communication.
//!   Rep.net`.
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
//! use crate::rep::{ReplicatedEnvironment, RepConfig, NodeType};
//!
//! let config = RepConfig::builder("my_group", "node1", "localhost")
//!     .node_port(14_001)
//!     .node_type(NodeType::Electable)
//!     .build();
//! let rep_env = ReplicatedEnvironment::new(config).unwrap();
//! ```

// Foundation modules
pub mod auth;
pub mod commit_durability;
pub mod consistency;
pub mod error;
pub mod node_type;
pub mod protocol;
pub mod quorum_policy;
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
pub mod group_admin;
pub mod master_transfer;

// Network transport
pub mod net;

// TLS configuration
pub mod tls;

// Subscription API
pub mod subscription;

// Network restore
pub mod network_restore;
pub mod network_restore_server;

// Main API
pub mod replicated_environment;
pub mod state_change_listener;

// In-memory `RepTestBase` / `RepEnvInfo` harness for porting JE rep
// tests and for production in-process clusters.  Available under
// `cfg(any(test, feature = "test-harness"))` and as a first-class
// production module via [`net::InMemoryTransport`].  The
// `test-harness` feature flag is retained as a no-op for backward
// compatibility with downstream Cargo.toml entries.
pub mod test_harness;

// Re-export primary types
pub use commit_durability::{CommitDurability, ReplicaAckPolicy};
pub use consistency::ConsistencyPolicy;
pub use elections::phi_detector::PhiAccrualDetector;
pub use error::{RepError, Result};
pub use master_transfer::{
    MasterTransfer, MasterTransferConfig, TransferState,
};
pub use net::{InMemoryEndpoint, InMemoryGroup, InMemoryTransport};
#[cfg(feature = "quic")]
pub use net::{
    QuicChannel, QuicChannelListener, default_server_config,
    insecure_client_config,
};
#[cfg(feature = "quic")]
pub use net::{
    QuicMultiplexedChannel, QuicMultiplexedChannelListener, ReconnectToken,
    ReplicationChannel, mux_insecure_client_config, mux_server_config,
};
pub use network_restore::{NetworkRestore, NetworkRestoreConfig, RestoreState};
pub use network_restore_server::{NetworkRestoreServer, RESTORE_SERVICE_NAME};
pub use node_state::{NodeState, NodeStateMachine};
pub use node_type::NodeType;
pub use quorum_policy::QuorumPolicy;
pub use rep_config::RepConfig;
pub use rep_config::RepTransportKind;
pub use rep_group::RepGroup;
pub use rep_node::RepNode;
pub use rep_stats::RepStats;
pub use replicated_environment::ReplicatedEnvironment;
pub use state_change_listener::{StateChangeEvent, StateChangeListener};
pub use stream::reconnect::{
    ReconnectConfig, ReconnectOutcome, catch_up_with_retry,
};
pub use subscription::{
    Subscription, SubscriptionCallback, SubscriptionConfig, SubscriptionState,
};
#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
pub use tls::TlsConfig;
