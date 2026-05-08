#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Cache eviction for Noxu DB.
//!
//! manages eviction of B-tree nodes
//! from the main cache and off-heap cache when they overflow.
//!
//! ## Overview
//!
//! The evictor is responsible for managing the in-memory cache by:
//! - Tracking nodes in LRU (Least Recently Used) lists
//! - Determining when eviction is needed based on memory budget
//! - Selecting and evicting cold nodes from the cache
//! - Providing different cache modes for fine-grained control
//! - Tracking detailed statistics about cache behavior
//!
//! ## Architecture
//!
//! The evictor uses a dual-priority LRU system:
//! - **Priority 1 (mixed)**: Clean and dirty nodes in normal operation
//! - **Priority 2 (dirty)**: Dirty nodes that should be evicted last to
//!   maximize write absorption
//!
//! When memory budget is exceeded, the evictor selects nodes from the cold
//! end of the LRU lists and attempts to evict them. Dirty nodes are logged
//! before eviction.
//!
//! ## Cache Modes
//!
//! Applications can control caching behavior using `CacheMode`:
//! - `Default`: Normal LRU behavior
//! - `Unchanged`: Don't perturb the cache
//! - `EvictLn`: Evict LNs after use
//! - `EvictBin`: Evict BINs after use
//! - `KeepHot`: Pin in cache
//! - `MakeEvictable`: Move to cold end
//!
//! ## Eviction Sources
//!
//! Eviction can be triggered from multiple sources:
//! - **Daemon**: Background evictor threads
//! - **Critical**: Application threads when severely over budget
//! - **Manual**: Explicit API calls
//! - **CacheMode**: Per-operation eviction
//!
//! ## Off-Heap Cache
//!
//! When configured, the off-heap cache extends the main cache into native
//! memory, avoiding Java GC pressure. Noxu uses a `Mutex<HashMap<u64, Vec<u8>>>` — no GC pressure to avoid.

pub mod arbiter;
pub mod cache_mode;
pub mod error;
pub mod evictor;
pub mod evictor_stat;
pub mod lru_list;
pub mod off_heap;

// Re-export main types at crate root
pub use arbiter::Arbiter;
pub use cache_mode::CacheMode;
pub use error::{EvictorError, Result};
pub use evictor::{
    decide_eviction, EvictResult, EvictionDecision, EvictionSource, Evictor,
    NodeEvictionInfo,
};
pub use evictor_stat::EvictorStats;
pub use lru_list::LruList;
pub use off_heap::{OffHeapCache, OffHeapStats};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::AtomicI64};

    #[test]
    fn test_basic_eviction_flow() {
        // Create a memory budget tracker
        let usage = Arc::new(AtomicI64::new(1500)); // Over budget
        let arbiter = Arbiter::new(1000, Arc::clone(&usage), 100, 200);

        // Create evictor
        let evictor = Evictor::new(arbiter, 100, false);

        // Add some nodes to cache
        for i in 1..=10 {
            evictor.note_ins_added(i, CacheMode::Default);
        }

        // Verify nodes are tracked
        assert_eq!(evictor.get_lru_sizes().0, 10);

        // Perform eviction
        let result = evictor.do_evict(EvictionSource::Daemon);

        // Should have evicted something
        assert!(result.nodes_evicted > 0);
        assert!(result.bytes_evicted > 0);

        // Check statistics
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.eviction_runs), 1);
        assert!(stats.get(&stats.nodes_evicted) > 0);
    }

    #[test]
    fn test_cache_mode_integration() {
        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        // Default mode - hot
        evictor.note_ins_added(1, CacheMode::Default);
        evictor.note_ins_accessed(1, CacheMode::Default);

        // Unchanged mode - no LRU update
        evictor.note_ins_added(2, CacheMode::Unchanged);
        evictor.note_ins_accessed(2, CacheMode::Unchanged);

        // Cold mode
        evictor.note_ins_added(3, CacheMode::MakeEvictable);

        assert_eq!(evictor.get_lru_sizes().0, 3);
    }

    #[test]
    fn test_off_heap_cache_stub() {
        let cache = OffHeapCache::new(true, 1024 * 1024);
        assert!(cache.is_enabled());

        let stats = cache.get_stats();
        assert!(stats.enabled);
        assert_eq!(stats.max_size, 1024 * 1024);
    }

    #[test]
    fn test_arbiter_eviction_decision() {
        let usage = Arc::new(AtomicI64::new(800));
        let arbiter = Arbiter::new(1000, Arc::clone(&usage), 100, 200);

        // Under budget
        assert!(!arbiter.is_over_budget());
        assert!(!arbiter.need_critical_eviction());
        assert_eq!(arbiter.get_eviction_pledge(), 0);

        // Over budget
        usage.store(1100, std::sync::atomic::Ordering::Relaxed);
        assert!(arbiter.is_over_budget());
        assert!(!arbiter.need_critical_eviction()); // Not critical yet

        // Critical
        usage.store(1300, std::sync::atomic::Ordering::Relaxed);
        assert!(arbiter.need_critical_eviction());
    }

    #[test]
    fn test_priority_lists() {
        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        // Add to pri1
        evictor.note_ins_added(1, CacheMode::Default);
        assert_eq!(evictor.get_lru_sizes(), (1, 0));

        // Move to pri2
        evictor.move_to_pri2(1);
        assert_eq!(evictor.get_lru_sizes(), (0, 1));

        // Add more to pri1
        evictor.note_ins_added(2, CacheMode::Default);
        evictor.note_ins_added(3, CacheMode::Default);
        assert_eq!(evictor.get_lru_sizes(), (2, 1));
    }

    #[test]
    fn test_error_types() {
        let err = EvictorError::EvictionFailed("test".to_string());
        assert!(err.to_string().contains("eviction failed"));

        let err = EvictorError::CacheOverflow { usage: 1000, budget: 800 };
        assert!(err.to_string().contains("cache overflow"));
    }
}
