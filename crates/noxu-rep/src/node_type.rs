//! Replication node types.
//!
//! Port of `com.sleepycat.je.rep.NodeType`.

/// The type of a node within a replication group.
///
/// Each node type determines what role the node can play in the group,
/// whether it participates in elections, and whether it stores data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeType {
    /// An electable node that can serve as either master or replica.
    /// Electable nodes participate in elections and store a complete
    /// copy of the data.
    Electable,

    /// A monitor node that observes group state changes but does not
    /// participate in elections or store data. Monitors receive
    /// notifications about master changes and group membership.
    Monitor,

    /// A secondary node that replicates data from the master but
    /// cannot be elected master. Secondary nodes do not participate
    /// in elections or contribute to quorum calculations.
    Secondary,

    /// An arbiter node used for tie-breaking in elections. Arbiters
    /// participate in elections and acknowledge transactions but do
    /// not store a full copy of the data.
    Arbiter,
}

impl NodeType {
    /// Returns `true` if this node type can participate in elections.
    ///
    /// Electable nodes and arbiters participate in elections.
    pub fn is_electable(&self) -> bool {
        matches!(self, NodeType::Electable | NodeType::Arbiter)
    }

    /// Returns `true` if this node type stores a full copy of the data.
    ///
    /// Electable and secondary nodes are data nodes.
    pub fn is_data_node(&self) -> bool {
        matches!(self, NodeType::Electable | NodeType::Secondary)
    }

    /// Returns `true` if this node type can become master.
    ///
    /// Only electable nodes can become master.
    pub fn can_be_master(&self) -> bool {
        matches!(self, NodeType::Electable)
    }
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Electable => write!(f, "ELECTABLE"),
            NodeType::Monitor => write!(f, "MONITOR"),
            NodeType::Secondary => write!(f, "SECONDARY"),
            NodeType::Arbiter => write!(f, "ARBITER"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_electable() {
        assert!(NodeType::Electable.is_electable());
        assert!(!NodeType::Monitor.is_electable());
        assert!(!NodeType::Secondary.is_electable());
        assert!(NodeType::Arbiter.is_electable());
    }

    #[test]
    fn test_is_data_node() {
        assert!(NodeType::Electable.is_data_node());
        assert!(!NodeType::Monitor.is_data_node());
        assert!(NodeType::Secondary.is_data_node());
        assert!(!NodeType::Arbiter.is_data_node());
    }

    #[test]
    fn test_can_be_master() {
        assert!(NodeType::Electable.can_be_master());
        assert!(!NodeType::Monitor.can_be_master());
        assert!(!NodeType::Secondary.can_be_master());
        assert!(!NodeType::Arbiter.can_be_master());
    }

    #[test]
    fn test_display() {
        assert_eq!(NodeType::Electable.to_string(), "ELECTABLE");
        assert_eq!(NodeType::Monitor.to_string(), "MONITOR");
        assert_eq!(NodeType::Secondary.to_string(), "SECONDARY");
        assert_eq!(NodeType::Arbiter.to_string(), "ARBITER");
    }

    #[test]
    fn test_clone_and_copy() {
        let nt = NodeType::Electable;
        let cloned = nt;
        let copied = nt;
        assert_eq!(nt, cloned);
        assert_eq!(nt, copied);
    }

    #[test]
    fn test_eq_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(NodeType::Electable);
        set.insert(NodeType::Monitor);
        set.insert(NodeType::Secondary);
        set.insert(NodeType::Arbiter);
        assert_eq!(set.len(), 4);

        // Duplicate insert should not increase size.
        set.insert(NodeType::Electable);
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn test_debug() {
        let s = format!("{:?}", NodeType::Electable);
        assert_eq!(s, "Electable");
    }
}
