//! Group membership service for replication.
//!
//! manages the replication
//! group membership, tracking which nodes are in the group, their types, and
//! their activity status.

use noxu_sync::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::node_type::NodeType;

/// Manages the replication group membership.
///
///
///
/// The group service tracks all nodes that are members of a replication group,
/// including their type (electable, monitor, secondary), network address, and
/// activity status. It provides quorum calculation and stale node detection.
pub struct GroupService {
    /// Name of the replication group.
    group_name: String,
    /// Unique identifier for this group instance.
    group_id: RwLock<u64>,
    /// Map of node name to node info.
    nodes: RwLock<HashMap<String, NodeInfo>>,
    /// Group version, incremented on each membership change.
    version: RwLock<u64>,
    /// Cleaner Barrier VLSN (CBVLSN) — global minimum committed VLSN across
    /// all active replicas.
    ///
    /// The log cleaner must not remove log files at VLSNs ≤ CBVLSN because
    /// one or more replicas may still need those entries. Updated whenever a
    /// replica's `known_vlsn` is refreshed.
    ///
    /// Corresponds to `LocalCBVLSNUpdater` / `RepGroupImpl.getCBVLSN()` in JE HA.
    cbvlsn: Arc<AtomicU64>,
}

/// Extended node information tracked by the group service.
///
/// Contains the identity, type, network address, and activity state of a
/// node in the replication group.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// The unique name of this node within the group.
    pub name: String,
    /// The type of this node (Electable, Monitor, Secondary).
    pub node_type: NodeType,
    /// The hostname or IP address of this node.
    pub host: String,
    /// The port number of this node.
    pub port: u16,
    /// The unique node ID assigned when the node joined the group.
    pub node_id: u32,
    /// When this node joined the group.
    pub joined_at: Instant,
    /// When this node was last seen (heartbeat or message).
    pub last_seen: Instant,
    /// Whether this node is currently active.
    pub is_active: bool,
    /// The highest VLSN this node has committed (0 = unknown).
    ///
    /// Updated on every heartbeat / ack from the node. Used to compute the
    /// CBVLSN (Cleaner Barrier VLSN) and to select peer feeders.
    pub known_vlsn: u64,
    /// The contiguous VLSN log range this node holds: `(first, last)`.
    ///
    /// A replica that holds `[first, last]` can act as a peer feeder for
    /// any other replica that needs VLSNs within that range.  `None` means
    /// the range is not yet known (node just joined or is restoring).
    ///
    /// Corresponds to `ReplicaState.getRepTxnEndVLSN()` in JE HA.
    pub log_range: Option<(u64, u64)>,
    /// Relative read throughput capacity x 100 (default 100 = 1.0).
    pub read_capacity_pct: u32,
    /// Relative write throughput capacity x 100 (default 100 = 1.0).
    pub write_capacity_pct: u32,
    /// Expected one-way latency hint in milliseconds (default 1).
    pub latency_hint_ms: u64,
}

impl GroupService {
    /// Create a new group service for the named group.
    pub fn new(group_name: String) -> Self {
        Self {
            group_name,
            group_id: RwLock::new(0),
            nodes: RwLock::new(HashMap::new()),
            version: RwLock::new(0),
            cbvlsn: Arc::new(AtomicU64::new(0)),
        }
    }

    // -----------------------------------------------------------------------
    // CBVLSN — Cleaner Barrier VLSN
    // -----------------------------------------------------------------------

    /// Return the current CBVLSN (Cleaner Barrier VLSN).
    ///
    /// This is the global minimum committed VLSN across all active electable
    /// nodes. The log cleaner must retain all log files at VLSNs ≥ CBVLSN
    /// so that lagging replicas can still catch up.
    pub fn get_cbvlsn(&self) -> u64 {
        self.cbvlsn.load(Ordering::Acquire)
    }

    /// Return an `Arc` to the raw CBVLSN counter.
    ///
    /// Useful for wiring the CBVLSN directly into the cleaner without
    /// locking the group service.
    pub fn cbvlsn_arc(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.cbvlsn)
    }

    /// Update the `known_vlsn` for a node and recompute the CBVLSN.
    ///
    /// Call this whenever a heartbeat or ack arrives from the named node.
    /// If the node is not found, the call is silently ignored (the node may
    /// have been removed from the group).
    pub fn update_node_vlsn(&self, name: &str, vlsn: u64) {
        let mut nodes = self.nodes.write();
        if let Some(info) = nodes.get_mut(name)
            && vlsn > info.known_vlsn
        {
            info.known_vlsn = vlsn;
        }
        // Recompute CBVLSN = min(known_vlsn) over active electable nodes.
        let new_cbvlsn = nodes
            .values()
            .filter(|n| n.is_active && n.node_type == crate::node_type::NodeType::Electable)
            .map(|n| n.known_vlsn)
            .min()
            .unwrap_or(0);
        self.cbvlsn.store(new_cbvlsn, Ordering::Release);
    }

    /// Update the VLSN log range `[first, last]` for a node.
    ///
    /// A node that holds a log range can serve as a peer feeder for other
    /// replicas that need entries within that range.
    pub fn update_node_log_range(&self, name: &str, first: u64, last: u64) {
        let mut nodes = self.nodes.write();
        if let Some(info) = nodes.get_mut(name) {
            info.log_range = Some((first, last));
        }
    }

    /// Find nodes that hold log entries covering the given VLSN.
    ///
    /// Returns names of all nodes (sorted by how recently they were seen)
    /// whose `log_range` covers `vlsn`.  These nodes can act as peer feeders.
    pub fn find_peers_with_vlsn(&self, vlsn: u64) -> Vec<String> {
        let nodes = self.nodes.read();
        let mut peers: Vec<(&str, Instant)> = nodes
            .values()
            .filter(|n| {
                n.is_active
                    && n.log_range
                        .is_some_and(|(first, last)| first <= vlsn && vlsn <= last)
            })
            .map(|n| (n.name.as_str(), n.last_seen))
            .collect();
        // Sort by most-recently-seen first (freshest peer first).
        peers.sort_by(|a, b| b.1.cmp(&a.1));
        peers.into_iter().map(|(name, _)| name.to_string()).collect()
    }

    /// Get the group name.
    pub fn get_group_name(&self) -> String {
        self.group_name.clone()
    }

    /// Get the group ID.
    pub fn get_group_id(&self) -> u64 {
        *self.group_id.read()
    }

    /// Set the group ID.
    pub fn set_group_id(&self, id: u64) {
        *self.group_id.write() = id;
    }

    /// Get the current group version. Incremented on each membership change.
    pub fn get_version(&self) -> u64 {
        *self.version.read()
    }

    /// Increment the group version and return the new value.
    fn increment_version(&self) -> u64 {
        let mut v = self.version.write();
        *v += 1;
        *v
    }

    /// Add a node to the group.
    ///
    /// # Errors
    ///
    /// Returns an error if a node with the same name already exists.
    pub fn add_node(&self, info: NodeInfo) -> crate::error::Result<()> {
        let mut nodes = self.nodes.write();
        if nodes.contains_key(&info.name) {
            return Err(crate::error::RepError::NodeAlreadyExists(info.name));
        }
        log::info!(
            "Adding node '{}' to group '{}'",
            info.name,
            self.group_name
        );
        nodes.insert(info.name.clone(), info);
        drop(nodes);
        self.increment_version();
        Ok(())
    }

    /// Remove a node from the group.
    ///
    /// # Errors
    ///
    /// Returns an error if the named node does not exist.
    pub fn remove_node(&self, name: &str) -> crate::error::Result<()> {
        let mut nodes = self.nodes.write();
        if nodes.remove(name).is_none() {
            return Err(crate::error::RepError::NodeNotFound(name.to_string()));
        }
        log::info!("Removed node '{}' from group '{}'", name, self.group_name);
        drop(nodes);
        self.increment_version();
        Ok(())
    }

    /// Update a node's active status.
    ///
    /// # Errors
    ///
    /// Returns an error if the named node does not exist.
    pub fn update_node_status(
        &self,
        name: &str,
        active: bool,
    ) -> crate::error::Result<()> {
        let mut nodes = self.nodes.write();
        match nodes.get_mut(name) {
            Some(info) => {
                info.is_active = active;
                Ok(())
            }
            None => Err(crate::error::RepError::NodeNotFound(name.to_string())),
        }
    }

    /// Record that a node was seen (heartbeat). Updates the `last_seen`
    /// timestamp and marks the node as active.
    ///
    /// # Errors
    ///
    /// Returns an error if the named node does not exist.
    pub fn touch_node(&self, name: &str) -> crate::error::Result<()> {
        let mut nodes = self.nodes.write();
        match nodes.get_mut(name) {
            Some(info) => {
                info.last_seen = Instant::now();
                info.is_active = true;
                Ok(())
            }
            None => Err(crate::error::RepError::NodeNotFound(name.to_string())),
        }
    }

    /// Update the capacity and latency metadata for an existing node.
    ///
    /// Only updates `read_capacity_pct`, `write_capacity_pct`, and
    /// `latency_hint_ms`. The node's identity and network address are preserved.
    ///
    /// # Errors
    ///
    /// Returns an error if the named node does not exist.
    pub fn update_node_metadata(
        &self,
        name: &str,
        read_capacity_pct: u32,
        write_capacity_pct: u32,
        latency_hint_ms: u64,
    ) -> crate::error::Result<()> {
        let mut nodes = self.nodes.write();
        match nodes.get_mut(name) {
            Some(info) => {
                info.read_capacity_pct = read_capacity_pct;
                info.write_capacity_pct = write_capacity_pct;
                info.latency_hint_ms = latency_hint_ms;
                Ok(())
            }
            None => Err(crate::error::RepError::NodeNotFound(name.to_string())),
        }
    }

    /// Get a clone of the node info for the named node.
    pub fn get_node(&self, name: &str) -> Option<NodeInfo> {
        self.nodes.read().get(name).cloned()
    }

    /// Get all nodes in the group.
    pub fn get_all_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.read().values().cloned().collect()
    }

    /// Get active electable nodes.
    ///
    /// Returns nodes that are both active and of type `Electable`.
    pub fn get_active_electable_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.is_active && n.node_type == NodeType::Electable)
            .cloned()
            .collect()
    }

    /// Get the quorum size (majority of electable nodes).
    ///
    /// The quorum is the simple majority: `(electable_count / 2) + 1`.
    /// This counts all electable nodes regardless of active status, matching
    /// `RepGroupImpl.getElectableGroupSize()` behavior.
    pub fn quorum_size(&self) -> u32 {
        let count = self.electable_count() as u32;
        if count == 0 {
            return 0;
        }
        (count / 2) + 1
    }

    /// Get the total number of nodes in the group.
    pub fn node_count(&self) -> usize {
        self.nodes.read().len()
    }

    /// Get the number of electable nodes (regardless of active status).
    pub fn electable_count(&self) -> usize {
        self.nodes
            .read()
            .values()
            .filter(|n| n.node_type == NodeType::Electable)
            .count()
    }

    /// Find nodes that haven't been seen within the given timeout.
    ///
    /// Returns the names of nodes whose `last_seen` timestamp is older than
    /// `now - timeout`.
    pub fn find_stale_nodes(&self, timeout: Duration) -> Vec<String> {
        let now = Instant::now();
        self.nodes
            .read()
            .values()
            .filter(|n| {
                n.is_active
                    && now
                        .checked_duration_since(n.last_seen)
                        .is_some_and(|d| d > timeout)
            })
            .map(|n| n.name.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(name: &str, node_type: NodeType, port: u16) -> NodeInfo {
        NodeInfo {
            name: name.to_string(),
            node_type,
            host: "localhost".to_string(),
            port,
            node_id: port as u32,
            joined_at: Instant::now(),
            last_seen: Instant::now(),
            known_vlsn: 0,
            log_range: None,
            is_active: true,
            read_capacity_pct: 100,
            write_capacity_pct: 100,
            latency_hint_ms: 1,
        }
    }

    fn make_electable(name: &str, port: u16) -> NodeInfo {
        make_node(name, NodeType::Electable, port)
    }

    // --- Basic operations ---

    #[test]
    fn test_new_group() {
        let gs = GroupService::new("test-group".to_string());
        assert_eq!(gs.get_group_name(), "test-group");
        assert_eq!(gs.get_group_id(), 0);
        assert_eq!(gs.get_version(), 0);
        assert_eq!(gs.node_count(), 0);
    }

    #[test]
    fn test_set_group_id() {
        let gs = GroupService::new("g".to_string());
        gs.set_group_id(42);
        assert_eq!(gs.get_group_id(), 42);
    }

    #[test]
    fn test_add_node() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("node1", 5001)).unwrap();
        assert_eq!(gs.node_count(), 1);
        assert_eq!(gs.get_version(), 1);

        let info = gs.get_node("node1").unwrap();
        assert_eq!(info.name, "node1");
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 5001);
        assert!(info.is_active);
    }

    #[test]
    fn test_add_duplicate_node() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("node1", 5001)).unwrap();
        let result = gs.add_node(make_electable("node1", 5002));
        assert!(result.is_err());
        assert_eq!(gs.node_count(), 1);
    }

    #[test]
    fn test_remove_node() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("node1", 5001)).unwrap();
        gs.add_node(make_electable("node2", 5002)).unwrap();
        assert_eq!(gs.get_version(), 2);

        gs.remove_node("node1").unwrap();
        assert_eq!(gs.node_count(), 1);
        assert!(gs.get_node("node1").is_none());
        assert!(gs.get_node("node2").is_some());
        assert_eq!(gs.get_version(), 3);
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let gs = GroupService::new("g".to_string());
        let result = gs.remove_node("ghost");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_node_not_found() {
        let gs = GroupService::new("g".to_string());
        assert!(gs.get_node("missing").is_none());
    }

    // --- Status tracking ---

    #[test]
    fn test_update_node_status() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("node1", 5001)).unwrap();

        gs.update_node_status("node1", false).unwrap();
        assert!(!gs.get_node("node1").unwrap().is_active);

        gs.update_node_status("node1", true).unwrap();
        assert!(gs.get_node("node1").unwrap().is_active);
    }

    #[test]
    fn test_update_nonexistent_node_status() {
        let gs = GroupService::new("g".to_string());
        assert!(gs.update_node_status("ghost", true).is_err());
    }

    #[test]
    fn test_touch_node() {
        let gs = GroupService::new("g".to_string());
        let mut info = make_electable("node1", 5001);
        info.is_active = false;
        // Set last_seen to something old
        info.last_seen = Instant::now() - Duration::from_secs(100);
        gs.add_node(info).unwrap();

        gs.touch_node("node1").unwrap();
        let updated = gs.get_node("node1").unwrap();
        assert!(updated.is_active);
        // last_seen should be very recent
        assert!(updated.last_seen.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn test_touch_nonexistent_node() {
        let gs = GroupService::new("g".to_string());
        assert!(gs.touch_node("ghost").is_err());
    }

    // --- Listing ---

    #[test]
    fn test_get_all_nodes() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        gs.add_node(make_electable("b", 5002)).unwrap();
        gs.add_node(make_node("m", NodeType::Monitor, 5003)).unwrap();

        let all = gs.get_all_nodes();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_get_active_electable_nodes() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        gs.add_node(make_electable("b", 5002)).unwrap();
        gs.add_node(make_node("m", NodeType::Monitor, 5003)).unwrap();

        // All electable are active
        let active = gs.get_active_electable_nodes();
        assert_eq!(active.len(), 2);

        // Deactivate one
        gs.update_node_status("a", false).unwrap();
        let active = gs.get_active_electable_nodes();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].name, "b");
    }

    // --- Quorum calculation ---

    #[test]
    fn test_quorum_zero_nodes() {
        let gs = GroupService::new("g".to_string());
        assert_eq!(gs.quorum_size(), 0);
    }

    #[test]
    fn test_quorum_one_node() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        // 1 electable: quorum = (1/2) + 1 = 1
        assert_eq!(gs.quorum_size(), 1);
    }

    #[test]
    fn test_quorum_two_nodes() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        gs.add_node(make_electable("b", 5002)).unwrap();
        // 2 electable: quorum = (2/2) + 1 = 2
        assert_eq!(gs.quorum_size(), 2);
    }

    #[test]
    fn test_quorum_three_nodes() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        gs.add_node(make_electable("b", 5002)).unwrap();
        gs.add_node(make_electable("c", 5003)).unwrap();
        // 3 electable: quorum = (3/2) + 1 = 2
        assert_eq!(gs.quorum_size(), 2);
    }

    #[test]
    fn test_quorum_four_nodes() {
        let gs = GroupService::new("g".to_string());
        for (i, name) in ["a", "b", "c", "d"].iter().enumerate() {
            gs.add_node(make_electable(name, 5001 + i as u16)).unwrap();
        }
        // 4 electable: quorum = (4/2) + 1 = 3
        assert_eq!(gs.quorum_size(), 3);
    }

    #[test]
    fn test_quorum_five_nodes() {
        let gs = GroupService::new("g".to_string());
        for (i, name) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            gs.add_node(make_electable(name, 5001 + i as u16)).unwrap();
        }
        // 5 electable: quorum = (5/2) + 1 = 3
        assert_eq!(gs.quorum_size(), 3);
    }

    #[test]
    fn test_quorum_ignores_non_electable() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        gs.add_node(make_electable("b", 5002)).unwrap();
        gs.add_node(make_node("m", NodeType::Monitor, 5003)).unwrap();
        gs.add_node(make_node("s", NodeType::Secondary, 5004)).unwrap();
        // Only 2 electable: quorum = 2
        assert_eq!(gs.quorum_size(), 2);
    }

    // --- Electable count ---

    #[test]
    fn test_electable_count() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        gs.add_node(make_node("m", NodeType::Monitor, 5002)).unwrap();
        assert_eq!(gs.electable_count(), 1);
    }

    // --- Stale node detection ---

    #[test]
    fn test_find_stale_nodes_none() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        let stale = gs.find_stale_nodes(Duration::from_secs(60));
        assert!(stale.is_empty());
    }

    #[test]
    fn test_find_stale_nodes_with_old_timestamp() {
        let gs = GroupService::new("g".to_string());
        let mut info = make_electable("old", 5001);
        info.last_seen = Instant::now() - Duration::from_secs(120);
        gs.add_node(info).unwrap();

        gs.add_node(make_electable("fresh", 5002)).unwrap();

        let stale = gs.find_stale_nodes(Duration::from_secs(60));
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0], "old");
    }

    #[test]
    fn test_find_stale_nodes_ignores_inactive() {
        let gs = GroupService::new("g".to_string());
        let mut info = make_electable("old", 5001);
        info.last_seen = Instant::now() - Duration::from_secs(120);
        info.is_active = false; // Already inactive, shouldn't be reported
        gs.add_node(info).unwrap();

        let stale = gs.find_stale_nodes(Duration::from_secs(60));
        assert!(stale.is_empty());
    }

    // --- Version tracking ---

    #[test]
    fn test_version_increments_on_add() {
        let gs = GroupService::new("g".to_string());
        assert_eq!(gs.get_version(), 0);
        gs.add_node(make_electable("a", 5001)).unwrap();
        assert_eq!(gs.get_version(), 1);
        gs.add_node(make_electable("b", 5002)).unwrap();
        assert_eq!(gs.get_version(), 2);
    }

    #[test]
    fn test_version_increments_on_remove() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        assert_eq!(gs.get_version(), 1);
        gs.remove_node("a").unwrap();
        assert_eq!(gs.get_version(), 2);
    }

    #[test]
    fn test_version_not_incremented_on_status_update() {
        let gs = GroupService::new("g".to_string());
        gs.add_node(make_electable("a", 5001)).unwrap();
        let v = gs.get_version();
        gs.update_node_status("a", false).unwrap();
        assert_eq!(gs.get_version(), v);
    }

    // --- Send + Sync ---

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GroupService>();
        assert_send_sync::<NodeInfo>();
    }
}
