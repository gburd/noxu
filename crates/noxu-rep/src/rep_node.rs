//! Replication node information.
//!

use std::time::Duration;

use crate::node_type::NodeType;

/// Information about a node in a replication group.
///
/// Each node has a unique name within its group, a type that determines
/// its role, and a network address (host and port) for replication
/// communication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepNode {
    /// The unique name of this node within the replication group.
    pub name: String,
    /// The type of this node (electable, monitor, secondary, arbiter).
    pub node_type: NodeType,
    /// The hostname or IP address for replication communication.
    pub host: String,
    /// The port number for replication communication.
    pub port: u16,
    /// The unique numeric identifier assigned to this node by the group.
    pub node_id: u32,
    /// Relative read throughput capacity × 100 (default 100 = 1.0).
    ///
    /// Used by quoracle strategy when computing load-optimal quorums.
    pub read_capacity_pct: u32,
    /// Relative write throughput capacity × 100 (default 100 = 1.0).
    pub write_capacity_pct: u32,
    /// Expected one-way latency hint in milliseconds (default 1).
    pub latency_hint_ms: u64,
}

impl RepNode {
    /// Creates a new `RepNode` with the given parameters.
    ///
    /// Capacity fields default to 1.0 (100 pct) and latency to 1 ms.
    pub fn new(
        name: String,
        node_type: NodeType,
        host: String,
        port: u16,
        node_id: u32,
    ) -> Self {
        Self {
            name,
            node_type,
            host,
            port,
            node_id,
            read_capacity_pct: 100,
            write_capacity_pct: 100,
            latency_hint_ms: 1,
        }
    }

    /// Set relative read capacity (e.g. `0.5` for half-speed node).
    ///
    /// The value is stored as `(cap * 100) as u32`.
    pub fn with_read_capacity(mut self, cap: f64) -> Self {
        self.read_capacity_pct = (cap * 100.0).round() as u32;
        self
    }

    /// Set relative write capacity.
    pub fn with_write_capacity(mut self, cap: f64) -> Self {
        self.write_capacity_pct = (cap * 100.0).round() as u32;
        self
    }

    /// Set expected one-way latency hint.
    pub fn with_latency_hint(mut self, d: Duration) -> Self {
        self.latency_hint_ms = d.as_millis() as u64;
        self
    }

    /// Build a `quoracle::Node<String>` from this `RepNode`, embedding
    /// the capacity and latency hints so that quoracle's LP strategy
    /// can factor them into load-optimal quorum selection.
    pub fn to_quoracle_node(&self) -> quoracle::Node<String> {
        let read_cap = self.read_capacity_pct as f64 / 100.0;
        let write_cap = self.write_capacity_pct as f64 / 100.0;
        let latency = Duration::from_millis(self.latency_hint_ms);
        quoracle::Node::new(self.name.clone())
            .with_read_write_capacity(read_cap, write_cap)
            .with_latency(latency)
    }

    /// Returns the name of this node.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of this node.
    pub fn node_type(&self) -> NodeType {
        self.node_type
    }

    /// Returns the hostname of this node.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the port number of this node.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns the numeric node identifier.
    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    /// Returns the socket address string in "host:port" format.
    pub fn socket_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Returns `true` if this node can participate in elections.
    pub fn is_electable(&self) -> bool {
        self.node_type.is_electable()
    }

    /// Returns `true` if this node stores data.
    pub fn is_data_node(&self) -> bool {
        self.node_type.is_data_node()
    }

    /// Returns `true` if this node can become master.
    pub fn can_be_master(&self) -> bool {
        self.node_type.can_be_master()
    }
}

impl std::fmt::Display for RepNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RepNode(name={}, type={}, addr={}, id={})",
            self.name,
            self.node_type,
            self.socket_address(),
            self.node_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node() -> RepNode {
        RepNode::new(
            "node1".to_string(),
            NodeType::Electable,
            "localhost".to_string(),
            5001,
            1,
        )
    }

    #[test]
    fn test_new_and_getters() {
        let node = make_node();
        assert_eq!(node.name(), "node1");
        assert_eq!(node.node_type(), NodeType::Electable);
        assert_eq!(node.host(), "localhost");
        assert_eq!(node.port(), 5001);
        assert_eq!(node.node_id(), 1);
    }

    #[test]
    fn test_socket_address() {
        let node = make_node();
        assert_eq!(node.socket_address(), "localhost:5001");
    }

    #[test]
    fn test_socket_address_with_ip() {
        let node = RepNode::new(
            "node2".to_string(),
            NodeType::Secondary,
            "192.168.1.100".to_string(),
            6000,
            2,
        );
        assert_eq!(node.socket_address(), "192.168.1.100:6000");
    }

    #[test]
    fn test_delegation_methods() {
        let electable = make_node();
        assert!(electable.is_electable());
        assert!(electable.is_data_node());
        assert!(electable.can_be_master());

        let monitor = RepNode::new(
            "mon".to_string(),
            NodeType::Monitor,
            "localhost".to_string(),
            5002,
            2,
        );
        assert!(!monitor.is_electable());
        assert!(!monitor.is_data_node());
        assert!(!monitor.can_be_master());
    }

    #[test]
    fn test_display() {
        let node = make_node();
        let s = node.to_string();
        assert!(s.contains("node1"));
        assert!(s.contains("ELECTABLE"));
        assert!(s.contains("localhost:5001"));
        assert!(s.contains("id=1"));
    }

    #[test]
    fn test_clone_and_eq() {
        let node = make_node();
        let cloned = node.clone();
        assert_eq!(node, cloned);
    }

    #[test]
    fn test_not_equal() {
        let node1 = make_node();
        let node2 = RepNode::new(
            "node2".to_string(),
            NodeType::Electable,
            "localhost".to_string(),
            5001,
            2,
        );
        assert_ne!(node1, node2);
    }

    #[test]
    fn test_debug() {
        let node = make_node();
        let s = format!("{:?}", node);
        assert!(s.contains("RepNode"));
        assert!(s.contains("node1"));
    }
}
