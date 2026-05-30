//! Off-heap cache support.
//!
//! `OffHeapCache` stores serialised BIN bytes in an anonymous `mmap` region,
//! keeping them outside the Rust allocator heap.  The OS can page out cold
//! entries under memory pressure while the in-memory index (a `LruCache`) stays
//! resident.  When capacity is exhausted, the least-recently-used node is
//! evicted to make room rather than refusing the insert.
//!
//! Backing: `memmap2::MmapMut` (anonymous, no file backing) + `lru::LruCache`
//! for O(1) LRU get/evict/peek.  A bump allocator advances through the mmap
//! region; compaction is triggered when the free-after-bump space would be
//! exhausted.

use lru::LruCache;
use memmap2::MmapMut;
use std::sync::Mutex;

// ─── internal mmap-backed store ───────────────────────────────────────────────

struct MmapStore {
    /// The anonymous mmap region.
    mmap: MmapMut,
    /// LRU index: node_id → (offset_in_mmap, byte_len).
    /// `get` promotes the entry to MRU; `pop_lru` evicts the LRU tail.
    index: LruCache<u64, (usize, usize)>,
    /// Next write position (bump allocator within mmap).
    write_pos: usize,
    /// Bytes from evicted/removed entries that have not been compacted.
    fragmented: usize,
    /// Logical capacity (== mmap.len()).
    capacity: usize,
    /// Cumulative count of LRU-driven evictions.
    evictions: u64,
}

impl MmapStore {
    fn new(capacity: usize) -> Option<Self> {
        if capacity == 0 {
            return None;
        }
        let mmap = MmapMut::map_anon(capacity).ok()?;
        Some(Self {
            mmap,
            index: LruCache::unbounded(),
            write_pos: 0,
            fragmented: 0,
            capacity,
            evictions: 0,
        })
    }

    fn live_bytes(&self) -> usize {
        self.write_pos - self.fragmented
    }

    /// Store `data` for `node_id`, evicting LRU entries as needed.
    /// Returns `false` only if `data.len() > capacity` (a single entry is too
    /// large for the entire cache).
    fn store(&mut self, node_id: u64, data: &[u8]) -> bool {
        let len = data.len();
        if len > self.capacity {
            return false;
        }

        // Remove any existing entry (replace semantics; count its bytes freed).
        // `LruCache::pop` returns `Option<V>` = `Option<(usize, usize)>`.
        if let Some((_, old_len)) = self.index.pop(&node_id) {
            self.fragmented += old_len;
        }

        // Evict LRU tail entries until `len` bytes fit within capacity.
        while self.live_bytes() + len > self.capacity {
            match self.index.pop_lru() {
                Some((_, (_, evicted_len))) => {
                    self.fragmented += evicted_len;
                    self.evictions += 1;
                }
                // All entries evicted yet still not enough room — `len` must be
                // larger than the entire mmap; already guarded above.
                None => return false,
            }
        }

        // Compact if there is not enough contiguous space at write_pos.
        if self.write_pos + len > self.capacity {
            self.compact();
        }

        self.mmap[self.write_pos..self.write_pos + len].copy_from_slice(data);
        self.index.push(node_id, (self.write_pos, len));
        self.write_pos += len;
        true
    }

    /// Load bytes for `node_id` (marks it as recently used).
    fn load(&mut self, node_id: u64) -> Option<Vec<u8>> {
        let &(offset, len) = self.index.get(&node_id)?;
        Some(self.mmap[offset..offset + len].to_vec())
    }

    /// Remove a node, marking its bytes as fragmented.
    fn remove(&mut self, node_id: u64) -> bool {
        if let Some((_, len)) = self.index.pop(&node_id) {
            self.fragmented += len;
            true
        } else {
            false
        }
    }

    /// Compact the mmap region: move all live entries to the beginning, close
    /// gaps left by evictions/removals, rebuild the LRU index with updated
    /// offsets.  LRU ordering is approximated by insertion-offset order
    /// (entries at lower offsets were generally stored earlier).
    fn compact(&mut self) {
        // Collect live entries sorted by current physical offset (ascending).
        let mut entries: Vec<(u64, usize, usize)> = self
            .index
            .iter()
            .map(|(&id, &(off, len))| (id, off, len))
            .collect();
        entries.sort_by_key(|&(_, off, _)| off);

        // Copy bytes to a contiguous region at the start of mmap.
        let mut new_pos = 0usize;
        for &(_, old_off, len) in &entries {
            if old_off != new_pos {
                self.mmap.copy_within(old_off..old_off + len, new_pos);
            }
            new_pos += len;
        }

        // Rebuild the LRU with updated offsets (offset order ≈ insertion order).
        let mut new_lru: LruCache<u64, (usize, usize)> = LruCache::unbounded();
        let mut pos = 0usize;
        for (id, _, len) in &entries {
            new_lru.push(*id, (pos, *len));
            pos += len;
        }

        self.index = new_lru;
        self.write_pos = new_pos;
        self.fragmented = 0;
    }

    fn len(&self) -> usize {
        self.index.len()
    }
}

// ─── public API ───────────────────────────────────────────────────────────────

/// Off-heap BIN cache backed by an anonymous `mmap` region with LRU eviction.
///
/// Evicted BINs are serialised and placed here by the evictor.  The backing
/// memory lives outside the Rust allocator heap, so the OS can page it out
/// under memory pressure while the compact in-memory LRU index remains hot.
/// When the cache is full, the least-recently-used entry is evicted to make
/// room rather than refusing the new insert.
///
/// Structural equivalent: `OffHeapAllocator` + `ConcurrentHashMap<Long,
/// byte[]>`.
pub struct OffHeapCache {
    enabled: bool,
    max_bytes: u64,
    inner: Mutex<Option<MmapStore>>,
}

impl std::fmt::Debug for OffHeapCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let usage = self
            .inner
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|s| s.live_bytes()))
            .unwrap_or(0);
        f.debug_struct("OffHeapCache")
            .field("enabled", &self.enabled)
            .field("max_bytes", &self.max_bytes)
            .field("used_bytes", &usage)
            .finish()
    }
}

impl OffHeapCache {
    /// Create a new off-heap cache.
    ///
    /// `enabled && max_bytes > 0` must both be true for the cache to be
    /// active.  If the anonymous `mmap` cannot be created, the cache is
    /// silently disabled.
    pub fn new(enabled: bool, max_bytes: u64) -> Self {
        let store = if enabled && max_bytes > 0 {
            MmapStore::new(max_bytes as usize)
        } else {
            None
        };
        let actually_enabled = store.is_some();
        Self { enabled: actually_enabled, max_bytes, inner: Mutex::new(store) }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn get_max_size(&self) -> usize {
        self.max_bytes as usize
    }

    pub fn get_usage(&self) -> usize {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|s| s.live_bytes()))
            .unwrap_or(0)
    }

    /// With LRU eviction the cache never exceeds its budget, so this always
    /// returns `false`.
    pub fn is_over_budget(&self) -> bool {
        false
    }

    /// Store serialised node bytes in the off-heap cache.
    ///
    /// LRU entries are evicted as needed to stay within `max_bytes`.  Returns
    /// `false` only when the cache is disabled or `data.len() > max_bytes`.
    pub fn store_node(&self, node_id: u64, data: Vec<u8>) -> bool {
        if !self.enabled {
            return false;
        }
        self.inner
            .lock()
            .ok()
            .and_then(|mut g| g.as_mut().map(|s| s.store(node_id, &data)))
            .unwrap_or(false)
    }

    /// Load serialised node bytes, marking the entry as recently used.
    ///
    /// Returns `None` when the cache is disabled or the node is not cached.
    pub fn load_node(&self, node_id: u64) -> Option<Vec<u8>> {
        if !self.enabled {
            return None;
        }
        self.inner.lock().ok()?.as_mut()?.load(node_id)
    }

    /// Remove a node from the cache, freeing its mmap space.
    pub fn remove_node(&self, node_id: u64) -> bool {
        if !self.enabled {
            return false;
        }
        self.inner
            .lock()
            .ok()
            .and_then(|mut g| g.as_mut().map(|s| s.remove(node_id)))
            .unwrap_or(false)
    }

    /// Clear all entries.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.inner.lock()
            && let Some(s) = guard.as_mut()
        {
            s.index = LruCache::unbounded();
            s.write_pos = 0;
            s.fragmented = 0;
        }
    }

    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|s| s.len()))
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn used_bytes(&self) -> u64 {
        self.get_usage() as u64
    }

    pub fn get_stats(&self) -> OffHeapStats {
        let (num_bins, usage, evictions) = self
            .inner
            .lock()
            .ok()
            .and_then(|g| {
                g.as_ref().map(|s| (s.len(), s.live_bytes(), s.evictions))
            })
            .unwrap_or((0, 0, 0));
        OffHeapStats {
            enabled: self.enabled,
            max_size: self.max_bytes as usize,
            usage,
            num_bins,
            num_lns: 0,
            evictions,
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

    /// Cumulative LRU-driven evictions since the cache was created.
    pub evictions: u64,
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

        // Store 4 bytes for node 2: LRU evicts node 1 to make room.
        // With LRU eviction the cache never goes over budget.
        assert!(cache.store_node(2, vec![0u8; 4]));
        assert!(!cache.is_over_budget());
        // Node 1 was evicted; node 2 is present.
        assert!(cache.load_node(1).is_none());
        assert!(cache.load_node(2).is_some());
        assert_eq!(cache.used_bytes(), 4);
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
            evictions: 0,
        };

        let stats2 = OffHeapStats {
            enabled: true,
            max_size: 1024,
            usage: 512,
            num_bins: 10,
            num_lns: 100,
            evictions: 0,
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
            evictions: 0,
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
    fn test_budget_enforcement_lru_eviction() {
        // Budget = 20 bytes: two 8-byte nodes fit (16 bytes), a third 8-byte
        // node evicts the LRU (node 1) to stay within budget.
        let cache = OffHeapCache::new(true, 20);
        assert!(cache.store_node(1, vec![0u8; 8]));
        assert!(cache.store_node(2, vec![0u8; 8]));
        // Node 3 is 8 bytes; total would be 24 > 20, so LRU (node 1) is evicted.
        assert!(cache.store_node(3, vec![0u8; 8]));
        assert_eq!(cache.len(), 2); // nodes 2 and 3
        assert_eq!(cache.used_bytes(), 16); // 8 + 8
        assert!(cache.load_node(1).is_none());
        assert!(cache.load_node(2).is_some());
        assert!(cache.load_node(3).is_some());
        let stats = cache.get_stats();
        assert_eq!(stats.evictions, 1);
    }
}
