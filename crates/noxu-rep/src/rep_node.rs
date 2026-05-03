//! Replication node information.
//!
//! Port of `com.sleepycat.je.rep.ReplicationNode`.

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
}

impl RepNode {
    /// Creates a new `RepNode` with the given parameters.
    pub fn new(
        name: String,
        node_type: NodeType,
        host: String,
        port: u16,
        node_id: u32,
    ) -> Self {
        Self { name, node_type, host, port, node_id }
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
