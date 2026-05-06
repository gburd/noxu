//! Off-heap cache support.
//!
//! Port of `com.sleepycat.je.evictor.OffHeapCache`.
//!
//! JE's OffHeapCache stores evicted BIN bytes in a `ConcurrentHashMap<Long, byte[]>`
//! keyed by node ID.  Rust has no GC pressure to avoid, so we use a simple
//! `Mutex<HashMap<u64, Vec<u8>>>` as the equivalent.  The allocator abstraction
//! from the Java version is not needed here.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Off-heap cache for B-tree nodes.
///
/// When enabled, evicted nodes from the main (on-heap) cache are moved here
/// rather than being discarded.  This allows a larger effective cache because
/// the data can be reloaded without disk I/O on the next access.
///
/// JE equivalent: `ConcurrentHashMap<Long nodeId, byte[] serializedBytes>` +
/// `OffHeapAllocator`.  In Rust we use a `Mutex<HashMap>` (no GC pressure to
/// avoid, so a plain heap `Vec<u8>` per node is fine).
///
/// Port of `com.sleepycat.je.evictor.OffHeapCache`.
#[derive(Debug)]
pub struct OffHeapCache {
    /// Whether off-heap cache is enabled.
    enabled: bool,

    /// Maximum size of off-heap cache in bytes.  When `used_bytes` would
    /// exceed this value, `store_node` returns `false` (over budget).
    max_bytes: u64,

    /// Serialised bytes keyed by node_id.
    /// Port of JE's `ConcurrentHashMap<Long, byte[]> inMemIds`.
    store: Mutex<HashMap<u64, Vec<u8>>>,

    /// Running total of bytes currently stored off-heap.
    /// Port of JE's `memoryUsed` / `allocator.totalBytes()`.
    used_bytes: AtomicU64,
}

impl OffHeapCache {
    /// Create a new off-heap cache.
    ///
    /// # Arguments
    /// * `enabled`   - Whether off-heap caching is enabled
    /// * `max_bytes` - Maximum capacity in bytes (0 = disabled regardless of `enabled`)
    pub fn new(enabled: bool, max_bytes: u64) -> Self {
        let actually_enabled = enabled && max_bytes > 0;
        Self {
            enabled: actually_enabled,
            max_bytes,
            store: Mutex::new(HashMap::new()),
            used_bytes: AtomicU64::new(0),
        }
    }

    /// Check if off-heap cache is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the maximum capacity of the off-heap cache in bytes.
    pub fn get_max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Get the maximum size of the off-heap cache (alias kept for API compat).
    pub fn get_max_size(&self) -> usize {
        self.max_bytes as usize
    }

    /// Get the current usage of the off-heap cache in bytes.
    pub fn get_usage(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed) as usize
    }

    /// Check if the off-heap cache is over budget.
    pub fn is_over_budget(&self) -> bool {
        self.enabled && self.used_bytes.load(Ordering::Relaxed) > self.max_bytes
    }

    /// Store serialised node bytes in the off-heap cache.
    ///
    /// Returns `false` when the cache is disabled or the addition would exceed
    /// `max_bytes`.  If a node with the same ID was already present its old
    /// bytes are replaced (usage is adjusted accordingly).
    ///
    /// Port of JE `OffHeapCache.storeEvictedBIN` / the underlying allocator
    /// `storeIN` pattern — key = nodeId, value = serialised bytes.
    pub fn store_node(&self, node_id: u64, data: Vec<u8>) -> bool {
        if !self.enabled {
            return false;
        }

        let new_len = data.len() as u64;

        let mut guard = match self.store.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };

        // Account for eviction of a previous entry for this node.
        let old_len = guard.get(&node_id).map(|v| v.len() as u64).unwrap_or(0);

        let current = self.used_bytes.load(Ordering::Relaxed);
        let projected = current - old_len + new_len;
        if projected > self.max_bytes {
            return false; // over budget
        }

        guard.insert(node_id, data);
        self.used_bytes.store(projected, Ordering::Relaxed);
        true
    }

    /// Load serialised node bytes from the off-heap cache.
    ///
    /// Returns a clone of the stored bytes, leaving the entry in place (the
    /// node will be promoted back to the main cache by the caller).
    ///
    /// Port of JE `OffHeapCache.getBINBytes`.
    pub fn load_node(&self, node_id: u64) -> Option<Vec<u8>> {
        if !self.enabled {
            return None;
        }

        let guard = self.store.lock().ok()?;
        guard.get(&node_id).cloned()
    }

    /// Remove a node from the off-heap cache and free its bytes.
    ///
    /// Returns `true` if the node was present and removed, `false` otherwise.
    ///
    /// Port of JE `OffHeapCache.removeINFromMain` (the part that frees the
    /// off-heap allocation for the BIN itself).
    pub fn remove_node(&self, node_id: u64) -> bool {
        if !self.enabled {
            return false;
        }

        let mut guard = match self.store.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };

        if let Some(old) = guard.remove(&node_id) {
            let freed = old.len() as u64;
            // Saturating sub — should never underflow in correct usage.
            self.used_bytes.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(freed))
            }).ok();
            true
        } else {
            false
        }
    }

    /// Clear all entries from the off-heap cache.
    ///
    /// Port of JE `OffHeapCache.clearCache`.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.store.lock() {
            guard.clear();
        }
        self.used_bytes.store(0, Ordering::Relaxed);
    }

    /// Number of nodes currently stored off-heap.
    pub fn len(&self) -> usize {
        self.store.lock().map(|g| g.len()).unwrap_or(0)
    }

    /// True when no nodes are stored off-heap.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of bytes currently used by the off-heap cache.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// Get statistics about the off-heap cache.
    pub fn get_stats(&self) -> OffHeapStats {
        let (num_bins, usage) = self.store
            .lock()
            .map(|g| (g.len(), g.values().map(|v| v.len()).sum::<usize>()))
            .unwrap_or((0, 0));

        OffHeapStats {
            enabled: self.enabled,
            max_size: self.max_bytes as usize,
            usage,
            num_bins,
            // LN off-heap is not supported; only BIN pages are cached off-heap.
            // Port of JE OffHeapCache which stores both BIN pages and LN values;
            // Noxu stores LNs inline in BIN slots (embedded_ln=true) instead.
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
        let cache = OffHeapCache::new(true, 10);
        assert!(!cache.is_over_budget());

        // Store 8 bytes — still under budget.
        assert!(cache.store_node(1, vec![0u8; 8]));
        assert!(!cache.is_over_budget());

        // Store another 4 bytes for node 2 — would bring total to 12 > 10,
        // so store_node must refuse.
        assert!(!cache.store_node(2, vec![0u8; 4]));
        assert!(!cache.is_over_budget()); // still 8 bytes
    }

    #[test]
    fn test_store_node_disabled() {
        let cache = OffHeapCache::new(false, 1024);
        assert!(!cache.store_node(1, vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_store_and_load_node() {
        let cache = OffHeapCache::new(true, 1024);
        let data = vec![1u8, 2, 3, 4, 5];
        assert!(cache.store_node(42, data.clone()));
        assert_eq!(cache.load_node(42), Some(data));
        assert_eq!(cache.used_bytes(), 5);
    }

    #[test]
    fn test_store_replaces_existing() {
        let cache = OffHeapCache::new(true, 1024);
        cache.store_node(1, vec![0u8; 100]);
        assert_eq!(cache.used_bytes(), 100);

        // Replace with smaller entry — used_bytes should shrink.
        cache.store_node(1, vec![0u8; 40]);
        assert_eq!(cache.used_bytes(), 40);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_load_node_disabled() {
        let cache = OffHeapCache::new(false, 1024);
        assert_eq!(cache.load_node(1), None);
    }

    #[test]
    fn test_load_node_missing() {
        let cache = OffHeapCache::new(true, 1024);
        assert_eq!(cache.load_node(99), None);
    }

    #[test]
    fn test_remove_node_disabled() {
        let cache = OffHeapCache::new(false, 1024);
        assert!(!cache.remove_node(1));
    }

    #[test]
    fn test_remove_node() {
        let cache = OffHeapCache::new(true, 1024);
        cache.store_node(7, vec![0u8; 64]);
        assert_eq!(cache.used_bytes(), 64);
        assert!(cache.remove_node(7));
        assert_eq!(cache.used_bytes(), 0);
        assert_eq!(cache.load_node(7), None);
    }

    #[test]
    fn test_remove_node_missing() {
        let cache = OffHeapCache::new(true, 1024);
        assert!(!cache.remove_node(999));
    }

    #[test]
    fn test_clear() {
        let cache = OffHeapCache::new(true, 1024);
        cache.store_node(1, vec![0u8; 100]);
        cache.store_node(2, vec![0u8; 200]);
        assert_eq!(cache.len(), 2);
        cache.clear();
        assert_eq!(cache.get_usage(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_get_stats() {
        let cache = OffHeapCache::new(true, 2048);
        cache.store_node(1, vec![0u8; 50]);
        cache.store_node(2, vec![0u8; 30]);
        let stats = cache.get_stats();
        assert!(stats.enabled);
        assert_eq!(stats.max_size, 2048);
        assert_eq!(stats.usage, 80);
        assert_eq!(stats.num_bins, 2);
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

        let cloned = stats;
        assert_eq!(stats, cloned);
    }

    #[test]
    fn test_len_and_is_empty() {
        let cache = OffHeapCache::new(true, 4096);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        cache.store_node(10, vec![1, 2, 3]);
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        cache.remove_node(10);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_budget_enforcement_multiple_nodes() {
        // Budget = 20 bytes; two 8-byte nodes fit, a third does not.
        let cache = OffHeapCache::new(true, 20);
        assert!(cache.store_node(1, vec![0u8; 8]));
        assert!(cache.store_node(2, vec![0u8; 8]));
        assert!(!cache.store_node(3, vec![0u8; 8])); // would push to 24 > 20
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.used_bytes(), 16);
    }
}
