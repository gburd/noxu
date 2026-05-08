//! Replication configuration.
//!

use std::path::PathBuf;
use std::time::Duration;

use crate::commit_durability::CommitDurability;
use crate::consistency::ConsistencyPolicy;
use crate::node_type::NodeType;

/// Default election timeout.
const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_secs(10);
/// Default heartbeat interval.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
/// Default replica ack timeout.
const DEFAULT_REPLICA_ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// Default feeder timeout (how long master waits for replica to respond).
const DEFAULT_FEEDER_TIMEOUT: Duration = Duration::from_secs(30);
/// Default replication port.
const DEFAULT_NODE_PORT: u16 = 5001;

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
    /// Helper hosts for joining the group ("host:port" strings).
    pub helper_hosts: Vec<String>,
    /// Timeout for elections.
    pub election_timeout: Duration,
    /// Interval between heartbeat messages.
    pub heartbeat_interval: Duration,
    /// How long to wait for replica acknowledgments.
    pub replica_ack_timeout: Duration,
    /// How long the master waits for a replica feeder response.
    pub feeder_timeout: Duration,
    /// Default consistency policy for read operations.
    pub consistency_policy: ConsistencyPolicy,
    /// Default commit durability for replicated transactions.
    pub commit_durability: CommitDurability,
    /// Path to the local environment home directory (`.ndb` files).
    ///
    /// When set, `ReplicatedEnvironment` registers a `NetworkRestoreServer`
    /// on the service dispatcher so that other nodes can restore from this
    /// node via the `"RESTORE"` service.
    pub env_home: Option<PathBuf>,
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
            helper_hosts: Vec::new(),
            election_timeout: DEFAULT_ELECTION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            replica_ack_timeout: DEFAULT_REPLICA_ACK_TIMEOUT,
            feeder_timeout: DEFAULT_FEEDER_TIMEOUT,
            consistency_policy: ConsistencyPolicy::default(),
            commit_durability: CommitDurability::default(),
            env_home: None,
        }
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
    helper_hosts: Vec<String>,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    replica_ack_timeout: Duration,
    feeder_timeout: Duration,
    consistency_policy: ConsistencyPolicy,
    commit_durability: CommitDurability,
    env_home: Option<PathBuf>,
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

    /// Sets the helper hosts for joining the group.
    pub fn helper_hosts(mut self, hosts: Vec<String>) -> Self {
        self.helper_hosts = hosts;
        self
    }

    /// Adds a single helper host.
    pub fn add_helper_host(mut self, host: String) -> Self {
        self.helper_hosts.push(host);
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

    /// Sets the replica ack timeout.
    pub fn replica_ack_timeout(mut self, timeout: Duration) -> Self {
        self.replica_ack_timeout = timeout;
        self
    }

    /// Sets the feeder timeout.
    pub fn feeder_timeout(mut self, timeout: Duration) -> Self {
        self.feeder_timeout = timeout;
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

    /// Builds the `RepConfig`.
    pub fn build(self) -> RepConfig {
        RepConfig {
            group_name: self.group_name,
            node_name: self.node_name,
            node_host: self.node_host,
            node_port: self.node_port,
            node_type: self.node_type,
            helper_hosts: self.helper_hosts,
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            replica_ack_timeout: self.replica_ack_timeout,
            feeder_timeout: self.feeder_timeout,
            consistency_policy: self.consistency_policy,
            commit_durability: self.commit_durability,
            env_home: self.env_home,
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
        assert!(config.helper_hosts.is_empty());
        assert_eq!(config.election_timeout, DEFAULT_ELECTION_TIMEOUT);
        assert_eq!(config.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL);
        assert_eq!(config.replica_ack_timeout, DEFAULT_REPLICA_ACK_TIMEOUT);
        assert_eq!(config.feeder_timeout, DEFAULT_FEEDER_TIMEOUT);
        assert_eq!(config.consistency_policy, ConsistencyPolicy::NoConsistency);
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
    fn test_builder_helper_hosts() {
        let config = RepConfig::builder("g", "n", "h")
            .helper_hosts(vec![
                "host1:5001".to_string(),
                "host2:5002".to_string(),
            ])
            .build();
        assert_eq!(config.helper_hosts.len(), 2);
    }

    #[test]
    fn test_builder_add_helper_host() {
        let config = RepConfig::builder("g", "n", "h")
            .add_helper_host("host1:5001".to_string())
            .add_helper_host("host2:5002".to_string())
            .build();
        assert_eq!(config.helper_hosts.len(), 2);
        assert_eq!(config.helper_hosts[0], "host1:5001");
    }

    #[test]
    fn test_builder_timeouts() {
        let config = RepConfig::builder("g", "n", "h")
            .election_timeout(Duration::from_secs(20))
            .heartbeat_interval(Duration::from_millis(500))
            .replica_ack_timeout(Duration::from_secs(10))
            .feeder_timeout(Duration::from_secs(60))
            .build();
        assert_eq!(config.election_timeout, Duration::from_secs(20));
        assert_eq!(config.heartbeat_interval, Duration::from_millis(500));
        assert_eq!(config.replica_ack_timeout, Duration::from_secs(10));
        assert_eq!(config.feeder_timeout, Duration::from_secs(60));
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
            .add_helper_host("10.0.0.2:5555".to_string())
            .election_timeout(Duration::from_secs(30))
            .build();
        assert_eq!(config.group_name, "mygroup");
        assert_eq!(config.node_name, "node1");
        assert_eq!(config.node_host, "10.0.0.1");
        assert_eq!(config.node_port, 5555);
        assert_eq!(config.node_type, NodeType::Arbiter);
        assert_eq!(config.helper_hosts.len(), 1);
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
