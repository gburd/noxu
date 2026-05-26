//! Replication configuration.
//!

use std::path::PathBuf;
use std::time::Duration;

use crate::commit_durability::CommitDurability;
use crate::consistency::ConsistencyPolicy;
use crate::node_type::NodeType;
use crate::quorum_policy::QuorumPolicy;
use crate::rep_node::RepNode;
use crate::stream::reconnect::ReconnectConfig;

/// Default election timeout.
const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_secs(10);
/// Default heartbeat interval.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
/// Default replication port.
///
/// Wave 1C audit cleanup (rep low "default port collision"):
/// changed from `5001` (which collides with PostgreSQL's REPMGR
/// default and various Cisco services) to `14_001`, an unprivileged
/// IANA-unassigned default.  Most production deployments override
/// this; the new default is just intended to fail closed during
/// development rather than silently bind on something else's port.
const DEFAULT_NODE_PORT: u16 = 14_001;
/// Default per-phase election message timeout.
const DEFAULT_ELECTION_PHASE_TIMEOUT: Duration = Duration::from_millis(500);
/// Default phi accrual sample window size.
const DEFAULT_PHI_WINDOW_SIZE: usize = 200;

/// Configuration for a replication node.
///
/// Use the builder
/// pattern to construct.
#[derive(Debug, Clone)]
pub struct RepConfig {
    /// Name of the replication group.
    pub group_name: String,
    /// Name of this node within the group (must be unique).
    pub node_name: String,
    /// Hostname or IP address for this node.
    pub node_host: String,
    /// Port for replication communication.
    pub node_port: u16,
    /// Type of this node.
    pub node_type: NodeType,
    /// Timeout for elections.
    pub election_timeout: Duration,
    /// Interval between heartbeat messages.
    pub heartbeat_interval: Duration,
    /// Default consistency policy for read operations.
    pub consistency_policy: ConsistencyPolicy,
    /// Default commit durability for replicated transactions.
    ///
    /// The `ack_timeout` field on `commit_durability` governs the
    /// commit-side wait for replica acks; there is no separate
    /// per-RepConfig replica-ack timeout.
    pub commit_durability: CommitDurability,
    /// Path to the local environment home directory (`.ndb` files).
    ///
    /// When set, `ReplicatedEnvironment` registers a `NetworkRestoreServer`
    /// on the service dispatcher so that other nodes can restore from this
    /// node via the `"RESTORE"` service.
    pub env_home: Option<PathBuf>,
    /// Quorum policy for elections. Default: `SimpleMajority`.
    pub quorum_policy: QuorumPolicy,
    /// Phi accrual suspicion threshold.
    ///
    /// `None` (default) uses a binary heartbeat timeout.
    /// `Some(8.0)` enables phi accrual detection with the paper's recommended
    /// threshold (mistake rate ≈ 10⁻⁸).
    pub phi_threshold: Option<f64>,
    /// Sliding-window size for phi accrual inter-arrival samples.
    ///
    /// Default `200` is adequate for LAN; use `1000` for WAN.
    pub phi_window_size: usize,
    /// Fully-described peers added to the replication group at startup.
    ///
    /// Useful for pre-populating quoracle capacity/latency metadata.
    pub initial_peers: Vec<RepNode>,
    /// Timeout per peer message exchange during Phase 1 and Phase 2 of an
    /// election.  Default: 500 ms.
    pub election_phase_timeout: Duration,
    /// Reconnection backoff configuration for replica partition recovery.
    pub reconnect_config: ReconnectConfig,
}

impl RepConfig {
    /// Creates a builder for `RepConfig`.
    pub fn builder(
        group_name: &str,
        node_name: &str,
        node_host: &str,
    ) -> RepConfigBuilder {
        RepConfigBuilder {
            group_name: group_name.to_string(),
            node_name: node_name.to_string(),
            node_host: node_host.to_string(),
            node_port: DEFAULT_NODE_PORT,
            node_type: NodeType::Electable,
            election_timeout: DEFAULT_ELECTION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            consistency_policy: ConsistencyPolicy::default(),
            commit_durability: CommitDurability::default(),
            env_home: None,
            quorum_policy: QuorumPolicy::SimpleMajority,
            phi_threshold: None,
            phi_window_size: DEFAULT_PHI_WINDOW_SIZE,
            initial_peers: Vec::new(),
            election_phase_timeout: DEFAULT_ELECTION_PHASE_TIMEOUT,
            reconnect_config: ReconnectConfig::default(),
        }
    }

    /// Convenience constructor matching the original v1.4 shape.
    ///
    /// Equivalent to `builder(group, node, host).node_port(port).build()`.
    /// Provided so doc snippets and short tests don't need to write the
    /// full builder chain.  Wave 1C audit cleanup (rep low
    /// "`RepConfig::new` example").
    pub fn new(
        group_name: impl Into<String>,
        node_name: impl Into<String>,
        node_host: impl Into<String>,
        node_port: u16,
    ) -> RepConfig {
        let g = group_name.into();
        let n = node_name.into();
        let h = node_host.into();
        RepConfig::builder(&g, &n, &h).node_port(node_port).build()
    }

    /// Returns the socket address string for this node.
    pub fn socket_address(&self) -> String {
        format!("{}:{}", self.node_host, self.node_port)
    }
}

/// Builder for [`RepConfig`].
#[derive(Debug, Clone)]
pub struct RepConfigBuilder {
    group_name: String,
    node_name: String,
    node_host: String,
    node_port: u16,
    node_type: NodeType,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    consistency_policy: ConsistencyPolicy,
    commit_durability: CommitDurability,
    env_home: Option<PathBuf>,
    quorum_policy: QuorumPolicy,
    phi_threshold: Option<f64>,
    phi_window_size: usize,
    initial_peers: Vec<RepNode>,
    election_phase_timeout: Duration,
    reconnect_config: ReconnectConfig,
}

impl RepConfigBuilder {
    /// Sets the replication port.
    pub fn node_port(mut self, port: u16) -> Self {
        self.node_port = port;
        self
    }

    /// Sets the node type.
    pub fn node_type(mut self, node_type: NodeType) -> Self {
        self.node_type = node_type;
        self
    }

    /// Sets the election timeout.
    pub fn election_timeout(mut self, timeout: Duration) -> Self {
        self.election_timeout = timeout;
        self
    }

    /// Sets the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Sets the consistency policy.
    pub fn consistency_policy(mut self, policy: ConsistencyPolicy) -> Self {
        self.consistency_policy = policy;
        self
    }

    /// Sets the commit durability.
    pub fn commit_durability(mut self, durability: CommitDurability) -> Self {
        self.commit_durability = durability;
        self
    }

    /// Sets the environment home directory (serves `.ndb` files for network restore).
    pub fn env_home(mut self, path: impl Into<PathBuf>) -> Self {
        self.env_home = Some(path.into());
        self
    }

    /// Sets the quorum policy for elections (default: `SimpleMajority`).
    pub fn quorum_policy(mut self, policy: QuorumPolicy) -> Self {
        self.quorum_policy = policy;
        self
    }

    /// Enable phi accrual failure detection with the given suspicion threshold.
    ///
    /// `8.0` is the paper's recommended production value (mistake rate ≈ 10⁻⁸).
    /// Call with `None` to revert to binary heartbeat timeout detection.
    pub fn phi_threshold(mut self, threshold: Option<f64>) -> Self {
        self.phi_threshold = threshold;
        self
    }

    /// Sets the phi accrual inter-arrival sample window size (default 200).
    pub fn phi_window_size(mut self, size: usize) -> Self {
        self.phi_window_size = size;
        self
    }

    /// Add a fully-described initial peer to the group at startup.
    pub fn add_initial_peer(mut self, node: RepNode) -> Self {
        self.initial_peers.push(node);
        self
    }

    /// Set the per-peer message timeout for Phase 1 and Phase 2 election
    /// exchanges (default: 500 ms).
    pub fn election_phase_timeout(mut self, timeout: Duration) -> Self {
        self.election_phase_timeout = timeout;
        self
    }

    /// Sets the reconnection backoff configuration for replica partition recovery.
    pub fn reconnect_config(mut self, config: ReconnectConfig) -> Self {
        self.reconnect_config = config;
        self
    }

    /// Builds the `RepConfig`.
    pub fn build(self) -> RepConfig {
        RepConfig {
            group_name: self.group_name,
            node_name: self.node_name,
            node_host: self.node_host,
            node_port: self.node_port,
            node_type: self.node_type,
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            consistency_policy: self.consistency_policy,
            commit_durability: self.commit_durability,
            env_home: self.env_home,
            quorum_policy: self.quorum_policy,
            phi_threshold: self.phi_threshold,
            phi_window_size: self.phi_window_size,
            initial_peers: self.initial_peers,
            election_phase_timeout: self.election_phase_timeout,
            reconnect_config: self.reconnect_config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_durability::ReplicaAckPolicy;

    #[test]
    fn test_builder_defaults() {
        let config = RepConfig::builder("group1", "node1", "localhost").build();
        assert_eq!(config.group_name, "group1");
        assert_eq!(config.node_name, "node1");
        assert_eq!(config.node_host, "localhost");
        assert_eq!(config.node_port, DEFAULT_NODE_PORT);
        assert_eq!(config.node_type, NodeType::Electable);
        assert_eq!(config.election_timeout, DEFAULT_ELECTION_TIMEOUT);
        assert_eq!(config.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL);
        assert_eq!(config.consistency_policy, ConsistencyPolicy::NoConsistency);
    }

    #[test]
    fn test_default_port_is_unprivileged() {
        // Wave 1C audit cleanup (rep low "default port collision"): the
        // default port must be in the IANA unassigned range and is not
        // shared with another well-known service we might collide with
        // (5001 was the v1.5.0 default; it overlaps with REPMGR among
        // others).
        let config = RepConfig::builder("g", "n", "h").build();
        assert_eq!(config.node_port, 14_001);
    }

    #[test]
    fn test_new_constructor_matches_builder() {
        let a = RepConfig::new("g", "n", "h", 6000);
        let b = RepConfig::builder("g", "n", "h").node_port(6000).build();
        // The two paths must produce the same on-the-wire identity.
        assert_eq!(a.group_name, b.group_name);
        assert_eq!(a.node_name, b.node_name);
        assert_eq!(a.node_host, b.node_host);
        assert_eq!(a.node_port, b.node_port);
        assert_eq!(a.node_type, b.node_type);
    }

    #[test]
    fn test_builder_custom_port() {
        let config = RepConfig::builder("g", "n", "h").node_port(6000).build();
        assert_eq!(config.node_port, 6000);
    }

    #[test]
    fn test_builder_node_type() {
        let config = RepConfig::builder("g", "n", "h")
            .node_type(NodeType::Secondary)
            .build();
        assert_eq!(config.node_type, NodeType::Secondary);
    }

    #[test]
    fn test_builder_timeouts() {
        let config = RepConfig::builder("g", "n", "h")
            .election_timeout(Duration::from_secs(20))
            .heartbeat_interval(Duration::from_millis(500))
            .build();
        assert_eq!(config.election_timeout, Duration::from_secs(20));
        assert_eq!(config.heartbeat_interval, Duration::from_millis(500));
    }

    #[test]
    fn test_builder_consistency_policy() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(500),
            timeout: Duration::from_secs(10),
        };
        let config = RepConfig::builder("g", "n", "h")
            .consistency_policy(policy.clone())
            .build();
        assert_eq!(config.consistency_policy, policy);
    }

    #[test]
    fn test_builder_commit_durability() {
        let durability = CommitDurability::new(
            ReplicaAckPolicy::All,
            Duration::from_secs(15),
        );
        let config = RepConfig::builder("g", "n", "h")
            .commit_durability(durability)
            .build();
        assert_eq!(config.commit_durability.ack_policy, ReplicaAckPolicy::All);
        assert_eq!(
            config.commit_durability.ack_timeout,
            Duration::from_secs(15)
        );
    }

    #[test]
    fn test_socket_address() {
        let config =
            RepConfig::builder("g", "n", "192.168.1.1").node_port(7000).build();
        assert_eq!(config.socket_address(), "192.168.1.1:7000");
    }

    #[test]
    fn test_builder_chaining() {
        let config = RepConfig::builder("mygroup", "node1", "10.0.0.1")
            .node_port(5555)
            .node_type(NodeType::Arbiter)
            .election_timeout(Duration::from_secs(30))
            .build();
        assert_eq!(config.group_name, "mygroup");
        assert_eq!(config.node_name, "node1");
        assert_eq!(config.node_host, "10.0.0.1");
        assert_eq!(config.node_port, 5555);
        assert_eq!(config.node_type, NodeType::Arbiter);
        assert_eq!(config.election_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_config_clone() {
        let config = RepConfig::builder("g", "n", "h").build();
        let cloned = config.clone();
        assert_eq!(config.group_name, cloned.group_name);
        assert_eq!(config.node_name, cloned.node_name);
    }

    #[test]
    fn test_config_debug() {
        let config = RepConfig::builder("g", "n", "h").build();
        let s = format!("{:?}", config);
        assert!(s.contains("RepConfig"));
    }
}
