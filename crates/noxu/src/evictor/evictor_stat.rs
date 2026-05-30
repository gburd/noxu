//! Eviction statistics tracking.
//!

use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics tracked by the evictor.
///
/// These statistics provide insight into cache behavior, eviction patterns,
/// and performance. All counters use atomic operations for thread-safe updates.
///
///
#[derive(Debug)]
pub struct EvictorStats {
    // Eviction runs and targeting
    /// Number of times the background eviction thread is awoken.
    pub eviction_runs: AtomicU64,

    /// Number of nodes (INs) selected as eviction targets.
    pub nodes_targeted: AtomicU64,

    /// Number of target nodes (INs) evicted from the main cache.
    pub nodes_evicted: AtomicU64,

    /// Number of nodes (INs) that did not require any action.
    pub nodes_skipped: AtomicU64,

    /// Number of target BINs mutated to BIN-deltas.
    pub nodes_mutated: AtomicU64,

    /// Number of target BINs whose child LNs were evicted (stripped).
    pub nodes_stripped: AtomicU64,

    /// Number of target nodes (INs) moved to the cold end of the LRU list
    /// without any action taken on them.
    pub nodes_put_back: AtomicU64,

    /// Number of nodes (INs) moved from the mixed/priority-1 to the
    /// dirty/priority-2 LRU list.
    pub nodes_moved_to_pri2_lru: AtomicU64,

    // Special node types
    /// Number of database root nodes (INs) evicted.
    pub root_nodes_evicted: AtomicU64,

    /// Number of dirty target nodes logged and evicted.
    pub dirty_nodes_evicted: AtomicU64,

    /// Number of LNs evicted as a result of LRU-based eviction.
    pub lns_evicted: AtomicU64,

    // Bytes evicted by source
    /// Number of bytes evicted by evictor pool threads.
    pub bytes_evicted_daemon: AtomicU64,

    /// Number of bytes evicted in the application thread because the cache
    /// is over budget (critical eviction).
    pub bytes_evicted_critical: AtomicU64,

    /// Number of bytes evicted by Environment.evictMemory or during
    /// Environment startup (manual eviction).
    pub bytes_evicted_manual: AtomicU64,

    /// Number of bytes evicted by operations for which CacheMode.EVICT_BIN
    /// is specified.
    pub bytes_evicted_cachemode: AtomicU64,

    // Fetch statistics (cache hits/misses)
    /// Number of BINs (bottom internal nodes) and BIN-deltas requested by
    /// btree operations.
    pub bin_fetch: AtomicU64,

    /// Number of full BINs and BIN-deltas fetched to satisfy btree operations
    /// that were not in main cache.
    pub bin_fetch_miss: AtomicU64,

    /// Number of LNs (data records) requested by btree operations.
    pub ln_fetch: AtomicU64,

    /// Number of LNs requested by btree operations that were not in main cache.
    pub ln_fetch_miss: AtomicU64,

    /// Number of Upper INs (non-bottom internal nodes) requested by btree
    /// operations.
    pub upper_in_fetch: AtomicU64,

    /// Number of Upper INs requested by btree operations that were not in
    /// main cache.
    pub upper_in_fetch_miss: AtomicU64,

    /// Number of BIN-deltas (partial BINs) fetched to satisfy btree operations
    /// that were not in main cache.
    pub bin_delta_fetch_miss: AtomicU64,

    /// Number of times a BIN-delta had to be mutated to a full BIN.
    pub full_bin_miss: AtomicU64,

    /// The number of operations performed blindly in BIN deltas.
    pub bin_delta_blind_ops: AtomicU64,

    // LRU sizes (instant stats, not counters)
    /// Number of INs in the mixed/priority-1 LRU.
    pub pri1_lru_size: AtomicU64,

    /// Number of INs in the dirty/priority-2 LRU.
    pub pri2_lru_size: AtomicU64,

    // Thread pool
    /// Number of eviction tasks that were submitted to the background evictor
    /// pool, but were refused because all eviction threads were busy.
    pub thread_unavailable: AtomicU64,

    // Cache composition (instant stats)
    /// Number of upper INs (non-bottom internal nodes) in main cache.
    pub cached_upper_ins: AtomicU64,

    /// Number of BINs (bottom internal nodes) and BIN-deltas in main cache.
    pub cached_bins: AtomicU64,

    /// Number of BIN-deltas (partial BINs) in main cache.
    pub cached_bin_deltas: AtomicU64,

    /// Number of INs that use a compact sparse array representation.
    pub cached_in_sparse_target: AtomicU64,

    /// Number of INs that use a compact representation when none of its
    /// child nodes are in the main cache.
    pub cached_in_no_target: AtomicU64,

    /// Number of INs that use a compact key representation.
    pub cached_in_compact_key: AtomicU64,
}

impl EvictorStats {
    /// Create a new EvictorStats with all counters initialized to zero.
    pub fn new() -> Self {
        Self {
            eviction_runs: AtomicU64::new(0),
            nodes_targeted: AtomicU64::new(0),
            nodes_evicted: AtomicU64::new(0),
            nodes_skipped: AtomicU64::new(0),
            nodes_mutated: AtomicU64::new(0),
            nodes_stripped: AtomicU64::new(0),
            nodes_put_back: AtomicU64::new(0),
            nodes_moved_to_pri2_lru: AtomicU64::new(0),
            root_nodes_evicted: AtomicU64::new(0),
            dirty_nodes_evicted: AtomicU64::new(0),
            lns_evicted: AtomicU64::new(0),
            bytes_evicted_daemon: AtomicU64::new(0),
            bytes_evicted_critical: AtomicU64::new(0),
            bytes_evicted_manual: AtomicU64::new(0),
            bytes_evicted_cachemode: AtomicU64::new(0),
            bin_fetch: AtomicU64::new(0),
            bin_fetch_miss: AtomicU64::new(0),
            ln_fetch: AtomicU64::new(0),
            ln_fetch_miss: AtomicU64::new(0),
            upper_in_fetch: AtomicU64::new(0),
            upper_in_fetch_miss: AtomicU64::new(0),
            bin_delta_fetch_miss: AtomicU64::new(0),
            full_bin_miss: AtomicU64::new(0),
            bin_delta_blind_ops: AtomicU64::new(0),
            pri1_lru_size: AtomicU64::new(0),
            pri2_lru_size: AtomicU64::new(0),
            thread_unavailable: AtomicU64::new(0),
            cached_upper_ins: AtomicU64::new(0),
            cached_bins: AtomicU64::new(0),
            cached_bin_deltas: AtomicU64::new(0),
            cached_in_sparse_target: AtomicU64::new(0),
            cached_in_no_target: AtomicU64::new(0),
            cached_in_compact_key: AtomicU64::new(0),
        }
    }

    /// Increment a counter by 1.
    #[inline]
    pub fn increment(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Add a value to a counter.
    #[inline]
    pub fn add(&self, counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, Ordering::Relaxed);
    }

    /// Set a counter to a specific value (for instant stats).
    #[inline]
    pub fn set(&self, counter: &AtomicU64, value: u64) {
        counter.store(value, Ordering::Relaxed);
    }

    /// Get the current value of a counter.
    #[inline]
    pub fn get(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Calculate the BIN fetch miss ratio.
    pub fn bin_fetch_miss_ratio(&self) -> f64 {
        let fetch = self.bin_fetch.load(Ordering::Relaxed);
        if fetch == 0 {
            0.0
        } else {
            let miss = self.bin_fetch_miss.load(Ordering::Relaxed);
            miss as f64 / fetch as f64
        }
    }

    /// Reset all counters to zero (for testing).
    #[cfg(test)]
    pub fn reset(&self) {
        self.eviction_runs.store(0, Ordering::Relaxed);
        self.nodes_targeted.store(0, Ordering::Relaxed);
        self.nodes_evicted.store(0, Ordering::Relaxed);
        self.nodes_skipped.store(0, Ordering::Relaxed);
        self.nodes_mutated.store(0, Ordering::Relaxed);
        self.nodes_stripped.store(0, Ordering::Relaxed);
        self.nodes_put_back.store(0, Ordering::Relaxed);
        self.nodes_moved_to_pri2_lru.store(0, Ordering::Relaxed);
        self.root_nodes_evicted.store(0, Ordering::Relaxed);
        self.dirty_nodes_evicted.store(0, Ordering::Relaxed);
        self.lns_evicted.store(0, Ordering::Relaxed);
        self.bytes_evicted_daemon.store(0, Ordering::Relaxed);
        self.bytes_evicted_critical.store(0, Ordering::Relaxed);
        self.bytes_evicted_manual.store(0, Ordering::Relaxed);
        self.bytes_evicted_cachemode.store(0, Ordering::Relaxed);
        self.bin_fetch.store(0, Ordering::Relaxed);
        self.bin_fetch_miss.store(0, Ordering::Relaxed);
        self.ln_fetch.store(0, Ordering::Relaxed);
        self.ln_fetch_miss.store(0, Ordering::Relaxed);
        self.upper_in_fetch.store(0, Ordering::Relaxed);
        self.upper_in_fetch_miss.store(0, Ordering::Relaxed);
        self.bin_delta_fetch_miss.store(0, Ordering::Relaxed);
        self.full_bin_miss.store(0, Ordering::Relaxed);
        self.bin_delta_blind_ops.store(0, Ordering::Relaxed);
        self.pri1_lru_size.store(0, Ordering::Relaxed);
        self.pri2_lru_size.store(0, Ordering::Relaxed);
        self.thread_unavailable.store(0, Ordering::Relaxed);
        self.cached_upper_ins.store(0, Ordering::Relaxed);
        self.cached_bins.store(0, Ordering::Relaxed);
        self.cached_bin_deltas.store(0, Ordering::Relaxed);
        self.cached_in_sparse_target.store(0, Ordering::Relaxed);
        self.cached_in_no_target.store(0, Ordering::Relaxed);
        self.cached_in_compact_key.store(0, Ordering::Relaxed);
    }
}

impl Default for EvictorStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let stats = EvictorStats::new();
        assert_eq!(stats.get(&stats.eviction_runs), 0);
        assert_eq!(stats.get(&stats.nodes_evicted), 0);
        assert_eq!(stats.get(&stats.bytes_evicted_daemon), 0);
    }

    #[test]
    fn test_increment() {
        let stats = EvictorStats::new();
        stats.increment(&stats.eviction_runs);
        assert_eq!(stats.get(&stats.eviction_runs), 1);
        stats.increment(&stats.eviction_runs);
        assert_eq!(stats.get(&stats.eviction_runs), 2);
    }

    #[test]
    fn test_add() {
        let stats = EvictorStats::new();
        stats.add(&stats.bytes_evicted_daemon, 100);
        assert_eq!(stats.get(&stats.bytes_evicted_daemon), 100);
        stats.add(&stats.bytes_evicted_daemon, 50);
        assert_eq!(stats.get(&stats.bytes_evicted_daemon), 150);
    }

    #[test]
    fn test_set() {
        let stats = EvictorStats::new();
        stats.set(&stats.pri1_lru_size, 42);
        assert_eq!(stats.get(&stats.pri1_lru_size), 42);
        stats.set(&stats.pri1_lru_size, 100);
        assert_eq!(stats.get(&stats.pri1_lru_size), 100);
    }

    #[test]
    fn test_bin_fetch_miss_ratio_zero_fetch() {
        let stats = EvictorStats::new();
        assert_eq!(stats.bin_fetch_miss_ratio(), 0.0);
    }

    #[test]
    fn test_bin_fetch_miss_ratio() {
        let stats = EvictorStats::new();
        stats.set(&stats.bin_fetch, 100);
        stats.set(&stats.bin_fetch_miss, 25);
        assert_eq!(stats.bin_fetch_miss_ratio(), 0.25);
    }

    #[test]
    fn test_bin_fetch_miss_ratio_all_miss() {
        let stats = EvictorStats::new();
        stats.set(&stats.bin_fetch, 50);
        stats.set(&stats.bin_fetch_miss, 50);
        assert_eq!(stats.bin_fetch_miss_ratio(), 1.0);
    }

    #[test]
    fn test_reset() {
        let stats = EvictorStats::new();
        stats.increment(&stats.eviction_runs);
        stats.add(&stats.bytes_evicted_daemon, 1000);
        stats.set(&stats.pri1_lru_size, 42);

        assert_eq!(stats.get(&stats.eviction_runs), 1);
        assert_eq!(stats.get(&stats.bytes_evicted_daemon), 1000);
        assert_eq!(stats.get(&stats.pri1_lru_size), 42);

        stats.reset();

        assert_eq!(stats.get(&stats.eviction_runs), 0);
        assert_eq!(stats.get(&stats.bytes_evicted_daemon), 0);
        assert_eq!(stats.get(&stats.pri1_lru_size), 0);
    }

    #[test]
    fn test_all_counters_initialized() {
        let stats = EvictorStats::new();

        // Verify all counters start at zero
        assert_eq!(stats.get(&stats.eviction_runs), 0);
        assert_eq!(stats.get(&stats.nodes_targeted), 0);
        assert_eq!(stats.get(&stats.nodes_evicted), 0);
        assert_eq!(stats.get(&stats.nodes_skipped), 0);
        assert_eq!(stats.get(&stats.nodes_mutated), 0);
        assert_eq!(stats.get(&stats.nodes_stripped), 0);
        assert_eq!(stats.get(&stats.nodes_put_back), 0);
        assert_eq!(stats.get(&stats.nodes_moved_to_pri2_lru), 0);
        assert_eq!(stats.get(&stats.root_nodes_evicted), 0);
        assert_eq!(stats.get(&stats.dirty_nodes_evicted), 0);
        assert_eq!(stats.get(&stats.lns_evicted), 0);
        assert_eq!(stats.get(&stats.bytes_evicted_daemon), 0);
        assert_eq!(stats.get(&stats.bytes_evicted_critical), 0);
        assert_eq!(stats.get(&stats.bytes_evicted_manual), 0);
        assert_eq!(stats.get(&stats.bytes_evicted_cachemode), 0);
        assert_eq!(stats.get(&stats.bin_fetch), 0);
        assert_eq!(stats.get(&stats.bin_fetch_miss), 0);
        assert_eq!(stats.get(&stats.ln_fetch), 0);
        assert_eq!(stats.get(&stats.ln_fetch_miss), 0);
        assert_eq!(stats.get(&stats.upper_in_fetch), 0);
        assert_eq!(stats.get(&stats.upper_in_fetch_miss), 0);
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(EvictorStats::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let stats_clone = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    stats_clone.increment(&stats_clone.eviction_runs);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(stats.get(&stats.eviction_runs), 1000);
    }
}
