//! Off-heap cache support (stub implementation).
//!
//! Port of `com.sleepycat.je.evictor.OffHeapCache`.

/// Off-heap cache for B-tree nodes.
///
/// When enabled, evicted nodes from the main (on-heap) cache are moved to
/// off-heap memory rather than being discarded. This allows a much larger
/// cache without Java GC pressure.
///
/// This is currently a stub implementation. Full off-heap support is deferred
/// to a later phase as it involves:
/// - Native memory allocation
/// - Serialization/deserialization of nodes
/// - Off-heap LRU tracking
/// - Memory-mapped regions or direct ByteBuffers
///
/// Port of `com.sleepycat.je.evictor.OffHeapCache`.
#[derive(Debug)]
pub struct OffHeapCache {
    /// Whether off-heap cache is enabled.
    enabled: bool,

    /// Maximum size of off-heap cache in bytes.
    max_size: usize,

    /// Current usage of off-heap cache in bytes (stub, always 0).
    usage: usize,
}

impl OffHeapCache {
    /// Create a new off-heap cache.
    ///
    /// # Arguments
    /// * `enabled` - Whether off-heap caching is enabled
    /// * `max_size` - Maximum size in bytes (0 = disabled)
    pub fn new(enabled: bool, max_size: usize) -> Self {
        Self { enabled: enabled && max_size > 0, max_size, usage: 0 }
    }

    /// Check if off-heap cache is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the maximum size of the off-heap cache.
    pub fn get_max_size(&self) -> usize {
        self.max_size
    }

    /// Get the current usage of the off-heap cache.
    pub fn get_usage(&self) -> usize {
        self.usage
    }

    /// Check if the off-heap cache is over budget.
    pub fn is_over_budget(&self) -> bool {
        self.enabled && self.usage > self.max_size
    }

    /// Store a node in off-heap cache (stub).
    ///
    /// In the full implementation, this would:
    /// - Serialize the node
    /// - Allocate off-heap memory
    /// - Store the serialized bytes
    /// - Update usage tracking
    ///
    /// # Arguments
    /// * `node_id` - ID of the node to store
    /// * `_data` - Node data (ignored in stub)
    ///
    /// # Returns
    /// True if stored successfully, false if cache is full or disabled.
    pub fn store_node(&mut self, _node_id: u64, _data: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }

        // Stub: would allocate off-heap and store
        false
    }

    /// Load a node from off-heap cache (stub).
    ///
    /// In the full implementation, this would:
    /// - Look up the node in off-heap storage
    /// - Deserialize the bytes
    /// - Return the reconstructed node
    ///
    /// # Arguments
    /// * `_node_id` - ID of the node to load
    ///
    /// # Returns
    /// Node data if found, None otherwise.
    pub fn load_node(&self, _node_id: u64) -> Option<Vec<u8>> {
        if !self.enabled {
            return None;
        }

        // Stub: would fetch from off-heap
        None
    }

    /// Remove a node from off-heap cache (stub).
    ///
    /// # Arguments
    /// * `_node_id` - ID of the node to remove
    ///
    /// # Returns
    /// True if removed, false if not found or cache disabled.
    pub fn remove_node(&mut self, _node_id: u64) -> bool {
        if !self.enabled {
            return false;
        }

        // Stub: would free off-heap memory
        false
    }

    /// Clear all entries from the off-heap cache (stub).
    pub fn clear(&mut self) {
        self.usage = 0;
    }

    /// Get statistics about the off-heap cache.
    pub fn get_stats(&self) -> OffHeapStats {
        OffHeapStats {
            enabled: self.enabled,
            max_size: self.max_size,
            usage: self.usage,
            num_bins: 0,
            num_lns: 0,
        }
    }
}

impl Default for OffHeapCache {
    fn default() -> Self {
        Self::new(false, 0)
    }
}

/// Statistics for the off-heap cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffHeapStats {
    /// Whether off-heap cache is enabled.
    pub enabled: bool,

    /// Maximum size in bytes.
    pub max_size: usize,

    /// Current usage in bytes.
    pub usage: usize,

    /// Number of BINs stored off-heap.
    pub num_bins: usize,

    /// Number of LNs stored off-heap.
    pub num_lns: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_off_heap_cache_disabled() {
        let cache = OffHeapCache::new(false, 1024);
        assert!(!cache.is_enabled());
        assert_eq!(cache.get_max_size(), 1024);
        assert_eq!(cache.get_usage(), 0);
    }

    #[test]
    fn test_off_heap_cache_enabled() {
        let cache = OffHeapCache::new(true, 1024 * 1024);
        assert!(cache.is_enabled());
        assert_eq!(cache.get_max_size(), 1024 * 1024);
        assert_eq!(cache.get_usage(), 0);
    }

    #[test]
    fn test_off_heap_cache_zero_size() {
        // Even if enabled=true, zero size means disabled
        let cache = OffHeapCache::new(true, 0);
        assert!(!cache.is_enabled());
    }

    #[test]
    fn test_is_over_budget() {
        let cache = OffHeapCache::new(true, 1000);
        assert!(!cache.is_over_budget());

        // In stub, usage is always 0, so never over budget
        assert!(!cache.is_over_budget());
    }

    #[test]
    fn test_store_node_disabled() {
        let mut cache = OffHeapCache::new(false, 1024);
        let data = vec![1, 2, 3, 4];
        assert!(!cache.store_node(1, &data));
    }

    #[test]
    fn test_store_node_stub() {
        let mut cache = OffHeapCache::new(true, 1024);
        let data = vec![1, 2, 3, 4];
        // Stub always returns false
        assert!(!cache.store_node(1, &data));
    }

    #[test]
    fn test_load_node_disabled() {
        let cache = OffHeapCache::new(false, 1024);
        assert_eq!(cache.load_node(1), None);
    }

    #[test]
    fn test_load_node_stub() {
        let cache = OffHeapCache::new(true, 1024);
        // Stub always returns None
        assert_eq!(cache.load_node(1), None);
    }

    #[test]
    fn test_remove_node_disabled() {
        let mut cache = OffHeapCache::new(false, 1024);
        assert!(!cache.remove_node(1));
    }

    #[test]
    fn test_remove_node_stub() {
        let mut cache = OffHeapCache::new(true, 1024);
        // Stub always returns false
        assert!(!cache.remove_node(1));
    }

    #[test]
    fn test_clear() {
        let mut cache = OffHeapCache::new(true, 1024);
        cache.clear();
        assert_eq!(cache.get_usage(), 0);
    }

    #[test]
    fn test_get_stats() {
        let cache = OffHeapCache::new(true, 2048);
        let stats = cache.get_stats();
        assert!(stats.enabled);
        assert_eq!(stats.max_size, 2048);
        assert_eq!(stats.usage, 0);
        assert_eq!(stats.num_bins, 0);
        assert_eq!(stats.num_lns, 0);
    }

    #[test]
    fn test_default() {
        let cache = OffHeapCache::default();
        assert!(!cache.is_enabled());
        assert_eq!(cache.get_max_size(), 0);
    }

    #[test]
    fn test_off_heap_stats_equality() {
        let stats1 = OffHeapStats {
            enabled: true,
            max_size: 1024,
            usage: 512,
            num_bins: 10,
            num_lns: 100,
        };

        let stats2 = OffHeapStats {
            enabled: true,
            max_size: 1024,
            usage: 512,
            num_bins: 10,
            num_lns: 100,
        };

        assert_eq!(stats1, stats2);
    }

    #[test]
    fn test_off_heap_stats_clone() {
        let stats = OffHeapStats {
            enabled: true,
            max_size: 1024,
            usage: 512,
            num_bins: 10,
            num_lns: 100,
        };

        let cloned = stats.clone();
        assert_eq!(stats, cloned);
    }
}
