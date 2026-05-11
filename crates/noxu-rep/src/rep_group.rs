//! Replication group management.
//!

use std::collections::{HashMap, HashSet};

use crate::node_type::NodeType;
use crate::quorum_policy::QuorumPolicy;
use crate::rep_node::RepNode;

/// A replication group consisting of named nodes.
///
/// The group tracks its members and provides queries for electable nodes,
/// monitors, quorum size, etc. Each node in the group has a unique name.
#[derive(Debug, Clone)]
pub struct RepGroup {
    /// The name of this replication group.
    name: String,
    /// A unique identifier for this group instance.
    group_id: u64,
    /// Map from node name to node info.
    nodes: HashMap<String, RepNode>,
    /// Quorum policy controlling Phase 1 / Phase 2 election sizes.
    quorum_policy: QuorumPolicy,
}

impl RepGroup {
    /// Creates a new replication group with the given name and ID.
    ///
    /// Defaults to [`QuorumPolicy::SimpleMajority`].  Use
    /// [`RepGroup::with_policy`] to specify a Flexible or Expression policy.
    pub fn new(name: String, group_id: u64) -> Self {
        Self {
            name,
            group_id,
            nodes: HashMap::new(),
            quorum_policy: QuorumPolicy::SimpleMajority,
        }
    }

    /// Creates a new replication group with an explicit quorum policy.
    pub fn with_policy(name: String, group_id: u64, policy: QuorumPolicy) -> Self {
        Self { name, group_id, nodes: HashMap::new(), quorum_policy: policy }
    }

    /// Replace the quorum policy for this group.
    pub fn set_quorum_policy(&mut self, policy: QuorumPolicy) {
        self.quorum_policy = policy;
    }

    /// Returns a reference to the current quorum policy.
    pub fn quorum_policy(&self) -> &QuorumPolicy {
        &self.quorum_policy
    }

    /// Returns the group name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the group ID.
    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    /// Adds a node to the group. Returns the previous node with the same
    /// name, if any.
    pub fn add_node(&mut self, node: RepNode) -> Option<RepNode> {
        self.nodes.insert(node.name.clone(), node)
    }

    /// Removes a node from the group by name. Returns the removed node,
    /// if it existed.
    pub fn remove_node(&mut self, name: &str) -> Option<RepNode> {
        self.nodes.remove(name)
    }

    /// Returns a reference to the node with the given name, if present.
    pub fn get_node(&self, name: &str) -> Option<&RepNode> {
        self.nodes.get(name)
    }

    /// Returns all nodes in the group.
    pub fn get_nodes(&self) -> Vec<&RepNode> {
        self.nodes.values().collect()
    }

    /// Returns all electable nodes (those that participate in elections).
    pub fn get_electable_nodes(&self) -> Vec<&RepNode> {
        self.nodes.values().filter(|n| n.node_type().is_electable()).collect()
    }

    /// Returns all monitor nodes.
    pub fn get_monitors(&self) -> Vec<&RepNode> {
        self.nodes
            .values()
            .filter(|n| n.node_type() == NodeType::Monitor)
            .collect()
    }

    /// Returns the number of electable nodes in the group.
    pub fn electable_count(&self) -> u32 {
        self.nodes.values().filter(|n| n.node_type().is_electable()).count()
            as u32
    }

    /// Returns the Phase 1 (Prepare/Promise) quorum size under the current policy.
    pub fn phase1_quorum(&self) -> usize {
        self.quorum_policy.phase1_quorum(self.electable_count() as usize)
    }

    /// Returns the Phase 2 (Accept/Commit) quorum size under the current policy.
    pub fn phase2_quorum(&self) -> usize {
        self.quorum_policy.phase2_quorum(self.electable_count() as usize)
    }

    /// Returns `true` if `voters` satisfies the Phase 2 quorum requirement.
    pub fn is_valid_phase2_quorum(&self, voters: &HashSet<&str>) -> bool {
        self.quorum_policy
            .is_valid_phase2_quorum(voters, self.electable_count() as usize)
    }

    /// Validate and optionally rebuild the quorum system after a membership
    /// change.  For `SimpleMajority` and `Expression` policies this is always
    /// valid; for `Flexible` it checks `phase1 + phase2 > n`.
    ///
    /// Returns `Err` if the current policy is unsafe for the new group size.
    pub fn rebuild_quorum_system(&self) -> Result<(), String> {
        self.quorum_policy.validate(self.electable_count() as usize)
    }

    /// Returns the quorum size: a simple majority of electable nodes.
    ///
    /// This is a compatibility shim that returns [`phase2_quorum`](Self::phase2_quorum)
    /// cast to `u32`.  New code should call `phase2_quorum()` directly.
    pub fn quorum_size(&self) -> u32 {
        self.phase2_quorum() as u32
    }

    /// Returns `true` if the group contains a node with the given name.
    pub fn contains_node(&self, name: &str) -> bool {
        self.nodes.contains_key(name)
    }

    /// Returns the total number of nodes in the group.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl std::fmt::Display for RepGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RepGroup(name={}, id={}, nodes={})",
            self.name,
            self.group_id,
            self.nodes.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_electable(name: &str, id: u32) -> RepNode {
        RepNode::new(
            name.to_string(),
            NodeType::Electable,
            "localhost".to_string(),
            5000 + id as u16,
            id,
        )
    }

    fn make_monitor(name: &str, id: u32) -> RepNode {
        RepNode::new(
            name.to_string(),
            NodeType::Monitor,
            "localhost".to_string(),
            5000 + id as u16,
            id,
        )
    }

    fn make_secondary(name: &str, id: u32) -> RepNode {
        RepNode::new(
            name.to_string(),
            NodeType::Secondary,
            "localhost".to_string(),
            5000 + id as u16,
            id,
        )
    }

    fn make_arbiter(name: &str, id: u32) -> RepNode {
        RepNode::new(
            name.to_string(),
            NodeType::Arbiter,
            "localhost".to_string(),
            5000 + id as u16,
            id,
        )
    }

    #[test]
    fn test_new_group() {
        let group = RepGroup::new("testgroup".to_string(), 1);
        assert_eq!(group.name(), "testgroup");
        assert_eq!(group.group_id(), 1);
        assert_eq!(group.node_count(), 0);
    }

    #[test]
    fn test_add_and_get_node() {
        let mut group = RepGroup::new("g".to_string(), 1);
        let node = make_electable("n1", 1);
        assert!(group.add_node(node).is_none());
        assert!(group.get_node("n1").is_some());
        assert_eq!(group.get_node("n1").unwrap().name(), "n1");
    }

    #[test]
    fn test_add_replaces_existing() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        let old = group.add_node(make_electable("n1", 2));
        assert!(old.is_some());
        assert_eq!(old.unwrap().node_id(), 1);
        assert_eq!(group.get_node("n1").unwrap().node_id(), 2);
    }

    #[test]
    fn test_remove_node() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        let removed = group.remove_node("n1");
        assert!(removed.is_some());
        assert!(!group.contains_node("n1"));
        assert!(group.remove_node("n1").is_none());
    }

    #[test]
    fn test_contains_node() {
        let mut group = RepGroup::new("g".to_string(), 1);
        assert!(!group.contains_node("n1"));
        group.add_node(make_electable("n1", 1));
        assert!(group.contains_node("n1"));
    }

    #[test]
    fn test_get_nodes() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        group.add_node(make_monitor("m1", 2));
        assert_eq!(group.get_nodes().len(), 2);
    }

    #[test]
    fn test_get_electable_nodes() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        group.add_node(make_electable("n2", 2));
        group.add_node(make_monitor("m1", 3));
        group.add_node(make_secondary("s1", 4));
        group.add_node(make_arbiter("a1", 5));

        let electables = group.get_electable_nodes();
        // Electable + Arbiter = 3
        assert_eq!(electables.len(), 3);
    }

    #[test]
    fn test_get_monitors() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        group.add_node(make_monitor("m1", 2));
        group.add_node(make_monitor("m2", 3));

        let monitors = group.get_monitors();
        assert_eq!(monitors.len(), 2);
    }

    #[test]
    fn test_electable_count() {
        let mut group = RepGroup::new("g".to_string(), 1);
        assert_eq!(group.electable_count(), 0);

        group.add_node(make_electable("n1", 1));
        group.add_node(make_electable("n2", 2));
        group.add_node(make_monitor("m1", 3));
        assert_eq!(group.electable_count(), 2);

        group.add_node(make_arbiter("a1", 4));
        assert_eq!(group.electable_count(), 3);
    }

    #[test]
    fn test_quorum_size_empty() {
        let group = RepGroup::new("g".to_string(), 1);
        assert_eq!(group.quorum_size(), 0);
    }

    #[test]
    fn test_quorum_size_one() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        // 1/2 + 1 = 1
        assert_eq!(group.quorum_size(), 1);
    }

    #[test]
    fn test_quorum_size_two() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        group.add_node(make_electable("n2", 2));
        // 2/2 + 1 = 2
        assert_eq!(group.quorum_size(), 2);
    }

    #[test]
    fn test_quorum_size_three() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        group.add_node(make_electable("n2", 2));
        group.add_node(make_electable("n3", 3));
        // 3/2 + 1 = 2
        assert_eq!(group.quorum_size(), 2);
    }

    #[test]
    fn test_quorum_size_five() {
        let mut group = RepGroup::new("g".to_string(), 1);
        for i in 1..=5 {
            group.add_node(make_electable(&format!("n{}", i), i));
        }
        // 5/2 + 1 = 3
        assert_eq!(group.quorum_size(), 3);
    }

    #[test]
    fn test_quorum_ignores_non_electable() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        group.add_node(make_electable("n2", 2));
        group.add_node(make_electable("n3", 3));
        group.add_node(make_monitor("m1", 4));
        group.add_node(make_secondary("s1", 5));
        // Only 3 electable: 3/2 + 1 = 2
        assert_eq!(group.quorum_size(), 2);
    }

    #[test]
    fn test_display() {
        let mut group = RepGroup::new("mygroup".to_string(), 42);
        group.add_node(make_electable("n1", 1));
        let s = group.to_string();
        assert!(s.contains("mygroup"));
        assert!(s.contains("42"));
        assert!(s.contains("1"));
    }

    #[test]
    fn test_clone() {
        let mut group = RepGroup::new("g".to_string(), 1);
        group.add_node(make_electable("n1", 1));
        let cloned = group.clone();
        assert_eq!(cloned.name(), group.name());
        assert_eq!(cloned.node_count(), group.node_count());
    }

    #[test]
    fn test_get_node_not_found() {
        let group = RepGroup::new("g".to_string(), 1);
        assert!(group.get_node("nonexistent").is_none());
    }
}
