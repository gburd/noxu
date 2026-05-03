//! Arbiter for determining when eviction is needed.
//!
//! Port of `com.sleepycat.je.evictor.Arbiter`.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// The Arbiter determines whether eviction should occur, by consulting the
/// memory budget.
///
/// The Arbiter tracks:
/// - Maximum allowed memory (max_memory)
/// - Current cache usage (cache_usage)
/// - Eviction threshold (evict_bytes)
/// - Critical eviction threshold
///
/// Port of `com.sleepycat.je.evictor.Arbiter`.
#[derive(Debug)]
pub struct Arbiter {
    /// Maximum memory budget in bytes.
    max_memory: AtomicI64,

    /// Current cache usage in bytes (shared with MemoryBudget).
    cache_usage: Arc<AtomicI64>,

    /// Number of bytes to evict beyond the over-budget amount.
    /// This provides hysteresis to avoid constant eviction.
    evict_bytes: i64,

    /// Critical threshold: if cache exceeds budget by this amount,
    /// synchronous (critical) eviction is triggered in application threads.
    critical_threshold: i64,
}

impl Arbiter {
    /// Create a new Arbiter.
    ///
    /// # Arguments
    /// * `max_memory` - Maximum allowed memory budget in bytes
    /// * `cache_usage` - Shared atomic tracking current cache usage
    /// * `evict_bytes` - Hysteresis amount to evict beyond over-budget
    /// * `critical_threshold` - Threshold for triggering critical eviction
    pub fn new(
        max_memory: i64,
        cache_usage: Arc<AtomicI64>,
        evict_bytes: i64,
        critical_threshold: i64,
    ) -> Self {
        Self {
            max_memory: AtomicI64::new(max_memory),
            cache_usage,
            evict_bytes,
            critical_threshold,
        }
    }

    /// Return true if the memory budget is overspent.
    pub fn is_over_budget(&self) -> bool {
        self.cache_usage.load(Ordering::Relaxed)
            > self.max_memory.load(Ordering::Relaxed)
    }

    /// Check whether synchronous (critical) eviction is needed.
    ///
    /// This method is intentionally not synchronized to minimize overhead
    /// when checking for critical eviction. It's called from application
    /// threads for every cursor operation.
    pub fn need_critical_eviction(&self) -> bool {
        let usage = self.cache_usage.load(Ordering::Relaxed);
        let max = self.max_memory.load(Ordering::Relaxed);
        let over = usage - max;
        over > self.critical_threshold
    }

    /// Check whether the cache should still be subject to eviction.
    ///
    /// This method is intentionally not synchronized to minimize overhead,
    /// because it's checked on every iteration of the evict batch loop.
    pub fn still_needs_eviction(&self) -> bool {
        let usage = self.cache_usage.load(Ordering::Relaxed);
        let max = self.max_memory.load(Ordering::Relaxed);
        (usage + self.evict_bytes) > max
    }

    /// Return the number of bytes that should be evicted.
    ///
    /// Returns 0 if no eviction is needed. The returned value includes
    /// both the over-budget amount and the evict_bytes hysteresis, but
    /// is capped to avoid evicting more than 50% of the cache.
    pub fn get_eviction_pledge(&self) -> i64 {
        let usage = self.cache_usage.load(Ordering::Relaxed);
        let max = self.max_memory.load(Ordering::Relaxed);
        let over_budget = usage - max;

        if over_budget <= 0 {
            return 0;
        }

        let mut required = over_budget + self.evict_bytes;

        // Don't evict more than 50% of the cache
        if usage - required < max / 2 {
            required = over_budget + (max / 2);
        }

        required
    }

    /// Get the current cache usage in bytes.
    pub fn get_cache_usage(&self) -> i64 {
        self.cache_usage.load(Ordering::Relaxed)
    }

    /// Get the maximum memory budget in bytes.
    pub fn get_max_memory(&self) -> i64 {
        self.max_memory.load(Ordering::Relaxed)
    }

    /// Update the maximum memory budget.
    pub fn set_max_memory(&self, new_max: i64) {
        self.max_memory.store(new_max, Ordering::Relaxed);
    }

    /// Get the evict_bytes setting.
    pub fn get_evict_bytes(&self) -> i64 {
        self.evict_bytes
    }

    /// Get the critical threshold.
    pub fn get_critical_threshold(&self) -> i64 {
        self.critical_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arbiter_new() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);

        assert_eq!(arbiter.get_max_memory(), 1000);
        assert_eq!(arbiter.get_cache_usage(), 0);
        assert_eq!(arbiter.get_evict_bytes(), 100);
        assert_eq!(arbiter.get_critical_threshold(), 200);
    }

    #[test]
    fn test_is_over_budget() {
        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, Arc::clone(&usage), 100, 200);

        assert!(!arbiter.is_over_budget());

        usage.store(1001, Ordering::Relaxed);
        assert!(arbiter.is_over_budget());

        usage.store(1000, Ordering::Relaxed);
        assert!(!arbiter.is_over_budget());
    }

    #[test]
    fn test_need_critical_eviction() {
        let usage = Arc::new(AtomicI64::new(1000));
        let arbiter = Arbiter::new(1000, Arc::clone(&usage), 100, 200);

        assert!(!arbiter.need_critical_eviction());

        // Over budget but not critical
        usage.store(1100, Ordering::Relaxed);
        assert!(!arbiter.need_critical_eviction());

        // Over critical threshold
        usage.store(1201, Ordering::Relaxed);
        assert!(arbiter.need_critical_eviction());
    }

    #[test]
    fn test_still_needs_eviction() {
        let usage = Arc::new(AtomicI64::new(800));
        let arbiter = Arbiter::new(1000, Arc::clone(&usage), 100, 200);

        // 800 + 100 = 900 < 1000
        assert!(!arbiter.still_needs_eviction());

        // 901 + 100 = 1001 > 1000
        usage.store(901, Ordering::Relaxed);
        assert!(arbiter.still_needs_eviction());

        // Exactly at boundary: 900 + 100 = 1000
        usage.store(900, Ordering::Relaxed);
        assert!(!arbiter.still_needs_eviction());
    }

    #[test]
    fn test_get_eviction_pledge_under_budget() {
        let usage = Arc::new(AtomicI64::new(800));
        let arbiter = Arbiter::new(1000, usage, 100, 200);

        assert_eq!(arbiter.get_eviction_pledge(), 0);
    }

    #[test]
    fn test_get_eviction_pledge_over_budget() {
        let usage = Arc::new(AtomicI64::new(1100));
        let arbiter = Arbiter::new(1000, usage, 100, 200);

        // Over by 100, plus evict_bytes 100 = 200
        assert_eq!(arbiter.get_eviction_pledge(), 200);
    }

    #[test]
    fn test_get_eviction_pledge_capped_at_half() {
        // Use a scenario where capping is actually needed
        let usage = Arc::new(AtomicI64::new(1800));
        let _arbiter = Arbiter::new(1000, Arc::clone(&usage), 500, 200);

        // Over by 800, plus evict_bytes 500 = 1300
        // After eviction: 1800 - 1300 = 500
        // Is 500 < 500 (max/2)? No, they're equal, so no capping
        // But if we use 1800 and evict_bytes=600:
        // Over by 800, plus 600 = 1400
        // After: 1800 - 1400 = 400 < 500 (max/2)
        // So cap at: over + (max/2) = 800 + 500 = 1300
        let arbiter2 =
            Arbiter::new(1000, Arc::new(AtomicI64::new(1800)), 600, 200);
        let pledge = arbiter2.get_eviction_pledge();

        // Expected: over + max/2 = 800 + 500 = 1300
        assert_eq!(pledge, 1300);
    }

    #[test]
    fn test_set_max_memory() {
        let usage = Arc::new(AtomicI64::new(800));
        let arbiter = Arbiter::new(1000, usage, 100, 200);

        assert_eq!(arbiter.get_max_memory(), 1000);
        assert!(!arbiter.is_over_budget());

        arbiter.set_max_memory(700);
        assert_eq!(arbiter.get_max_memory(), 700);
        assert!(arbiter.is_over_budget());
    }

    #[test]
    fn test_getters() {
        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 150, 250);

        assert_eq!(arbiter.get_cache_usage(), 500);
        assert_eq!(arbiter.get_max_memory(), 1000);
        assert_eq!(arbiter.get_evict_bytes(), 150);
        assert_eq!(arbiter.get_critical_threshold(), 250);
    }

    #[test]
    fn test_boundary_conditions() {
        let usage = Arc::new(AtomicI64::new(1000));
        let arbiter = Arbiter::new(1000, usage, 0, 0);

        // Exactly at budget
        assert!(!arbiter.is_over_budget());
        assert_eq!(arbiter.get_eviction_pledge(), 0);

        // Just slightly over
        arbiter.cache_usage.store(1001, Ordering::Relaxed);
        assert!(arbiter.is_over_budget());
        assert_eq!(arbiter.get_eviction_pledge(), 1);
    }
}
