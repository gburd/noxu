use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use noxu_sync::RwLock;

/// The INList is a list of in-memory INs for a given environment.
///
/// Tracks all cached tree nodes (INs, BINs) for the evictor.
/// Uses a concurrent set for thread-safe access.
///
/// Port of `com.sleepycat.je.dbi.INList`.
pub struct INList {
    /// Set of node IDs currently in the cache.
    /// In a full implementation, this would hold actual IN references.
    node_ids: RwLock<HashSet<i64>>,
    /// Whether the list is enabled (disabled during recovery).
    enabled: AtomicBool,
    /// Count of cached upper INs (non-BIN).
    n_cached_upper_ins: AtomicI64,
    /// Count of cached BINs.
    n_cached_bins: AtomicI64,
    /// Count of cached BIN-deltas.
    n_cached_bin_deltas: AtomicI64,
}

impl INList {
    pub fn new() -> Self {
        INList {
            node_ids: RwLock::new(HashSet::new()),
            enabled: AtomicBool::new(false),
            n_cached_upper_ins: AtomicI64::new(0),
            n_cached_bins: AtomicI64::new(0),
            n_cached_bin_deltas: AtomicI64::new(0),
        }
    }

    /// Enables the INList (called after recovery).
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    /// Disables the INList.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }

    /// Returns true if enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Adds a node to the list.
    pub fn add(&self, node_id: i64) {
        if !self.is_enabled() {
            return;
        }
        self.node_ids.write().insert(node_id);
    }

    /// Removes a node from the list.
    pub fn remove(&self, node_id: i64) -> bool {
        self.node_ids.write().remove(&node_id)
    }

    /// Returns true if the node is in the list.
    pub fn contains(&self, node_id: i64) -> bool {
        self.node_ids.read().contains(&node_id)
    }

    /// Returns the number of nodes in the list.
    pub fn size(&self) -> usize {
        self.node_ids.read().len()
    }

    /// Returns all node IDs (for iteration by evictor).
    pub fn get_all_node_ids(&self) -> Vec<i64> {
        self.node_ids.read().iter().copied().collect()
    }

    /// Clears the list.
    pub fn clear(&self) {
        self.node_ids.write().clear();
        self.n_cached_upper_ins.store(0, Ordering::Relaxed);
        self.n_cached_bins.store(0, Ordering::Relaxed);
        self.n_cached_bin_deltas.store(0, Ordering::Relaxed);
    }

    // Stats
    pub fn get_n_cached_upper_ins(&self) -> i64 {
        self.n_cached_upper_ins.load(Ordering::Relaxed)
    }

    pub fn get_n_cached_bins(&self) -> i64 {
        self.n_cached_bins.load(Ordering::Relaxed)
    }

    pub fn get_n_cached_bin_deltas(&self) -> i64 {
        self.n_cached_bin_deltas.load(Ordering::Relaxed)
    }

    pub fn increment_cached_upper_ins(&self) {
        self.n_cached_upper_ins.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_cached_upper_ins(&self) {
        self.n_cached_upper_ins.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn increment_cached_bins(&self) {
        self.n_cached_bins.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_cached_bins(&self) {
        self.n_cached_bins.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn increment_cached_bin_deltas(&self) {
        self.n_cached_bin_deltas.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_cached_bin_deltas(&self) {
        self.n_cached_bin_deltas.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Default for INList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_remove_contains() {
        let list = INList::new();
        list.enable();

        list.add(100);
        list.add(200);

        assert!(list.contains(100));
        assert!(list.contains(200));
        assert!(!list.contains(300));

        assert!(list.remove(100));
        assert!(!list.contains(100));
        assert!(!list.remove(100)); // Already removed
    }

    #[test]
    fn test_size_tracking() {
        let list = INList::new();
        list.enable();

        assert_eq!(list.size(), 0);

        list.add(1);
        assert_eq!(list.size(), 1);

        list.add(2);
        assert_eq!(list.size(), 2);

        list.add(1); // Duplicate, shouldn't increase size
        assert_eq!(list.size(), 2);

        list.remove(1);
        assert_eq!(list.size(), 1);
    }

    #[test]
    fn test_enable_disable() {
        let list = INList::new();

        // Disabled by default
        assert!(!list.is_enabled());

        // Add while disabled should be no-op
        list.add(100);
        assert_eq!(list.size(), 0);

        // Enable and add
        list.enable();
        assert!(list.is_enabled());

        list.add(100);
        assert_eq!(list.size(), 1);

        // Disable
        list.disable();
        assert!(!list.is_enabled());

        // Add while disabled should be no-op
        list.add(200);
        assert_eq!(list.size(), 1); // Still just 100
    }

    #[test]
    fn test_clear() {
        let list = INList::new();
        list.enable();

        list.add(1);
        list.add(2);
        list.add(3);
        list.increment_cached_upper_ins();
        list.increment_cached_bins();
        list.increment_cached_bin_deltas();

        list.clear();

        assert_eq!(list.size(), 0);
        assert_eq!(list.get_n_cached_upper_ins(), 0);
        assert_eq!(list.get_n_cached_bins(), 0);
        assert_eq!(list.get_n_cached_bin_deltas(), 0);
    }

    #[test]
    fn test_stats_counters() {
        let list = INList::new();

        assert_eq!(list.get_n_cached_upper_ins(), 0);
        assert_eq!(list.get_n_cached_bins(), 0);
        assert_eq!(list.get_n_cached_bin_deltas(), 0);

        list.increment_cached_upper_ins();
        list.increment_cached_upper_ins();
        assert_eq!(list.get_n_cached_upper_ins(), 2);

        list.increment_cached_bins();
        assert_eq!(list.get_n_cached_bins(), 1);

        list.increment_cached_bin_deltas();
        list.increment_cached_bin_deltas();
        list.increment_cached_bin_deltas();
        assert_eq!(list.get_n_cached_bin_deltas(), 3);

        list.decrement_cached_upper_ins();
        assert_eq!(list.get_n_cached_upper_ins(), 1);

        list.decrement_cached_bins();
        assert_eq!(list.get_n_cached_bins(), 0);

        list.decrement_cached_bin_deltas();
        assert_eq!(list.get_n_cached_bin_deltas(), 2);
    }

    #[test]
    fn test_get_all_node_ids() {
        let list = INList::new();
        list.enable();

        list.add(10);
        list.add(20);
        list.add(30);

        let mut ids = list.get_all_node_ids();
        ids.sort();

        assert_eq!(ids, vec![10, 20, 30]);
    }
}
