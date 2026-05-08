use std::sync::atomic::{AtomicI64, Ordering};

/// MemoryBudget calculates available memory and tracks usage.
///
/// Centralizes all memory calculations. Objects that need memory
/// should get settings from this class.
///
/// 
pub struct MemoryBudget {
    /// Maximum cache size in bytes.
    max_memory: i64,
    /// Current cache usage: tree (IN) memory.
    tree_memory_usage: AtomicI64,
    /// Current cache usage: lock memory.
    lock_memory_usage: AtomicI64,
    /// Current cache usage: admin/misc memory.
    admin_memory_usage: AtomicI64,
    /// Current cache usage: log buffer memory.
    log_buffer_budget: i64,
}

/// Estimated object overheads in bytes (Rust equivalents of constants).
/// These are approximate sizes used for memory accounting.
pub struct MemoryOverhead;

impl MemoryOverhead {
    /// Overhead for a LockImpl.
    pub const LOCKIMPL_OVERHEAD: i64 = 96;
    /// Overhead for a ThinLockImpl.
    pub const THINLOCKIMPL_OVERHEAD: i64 = 32;
    /// Overhead for a LockInfo.
    pub const LOCKINFO_OVERHEAD: i64 = 32;
    /// Overhead for a HashMap entry.
    pub const HASHMAP_ENTRY_OVERHEAD: i64 = 48;
    /// Overhead for a boxed i64.
    pub const LONG_OVERHEAD: i64 = 16;
    /// Overhead for an IN node (approximate).
    pub const IN_OVERHEAD: i64 = 400;
    /// Overhead for a BIN node.
    pub const BIN_OVERHEAD: i64 = 500;
    /// Overhead for an LN.
    pub const LN_OVERHEAD: i64 = 48;
    /// Overhead for a Txn.
    pub const TXN_OVERHEAD: i64 = 200;
    /// Overhead for a BasicLocker.
    pub const BASICLOCKER_OVERHEAD: i64 = 80;
}

impl MemoryBudget {
    /// Creates a new MemoryBudget with the given max cache size.
    pub fn new(max_memory: i64) -> Self {
        // Reserve 7% for log buffers (matching default)
        let log_buffer_budget = max_memory * 7 / 100;

        MemoryBudget {
            max_memory,
            tree_memory_usage: AtomicI64::new(0),
            lock_memory_usage: AtomicI64::new(0),
            admin_memory_usage: AtomicI64::new(0),
            log_buffer_budget,
        }
    }

    /// Returns the maximum cache size.
    pub fn max_memory(&self) -> i64 {
        self.max_memory
    }

    /// Returns the log buffer budget.
    pub fn log_buffer_budget(&self) -> i64 {
        self.log_buffer_budget
    }

    /// Returns total current memory usage.
    pub fn total_usage(&self) -> i64 {
        self.tree_memory_usage.load(Ordering::Relaxed)
            + self.lock_memory_usage.load(Ordering::Relaxed)
            + self.admin_memory_usage.load(Ordering::Relaxed)
    }

    /// Returns the available (free) cache memory.
    pub fn available_memory(&self) -> i64 {
        self.max_memory - self.total_usage()
    }

    /// Returns true if the cache is over budget.
    pub fn is_over_budget(&self) -> bool {
        self.total_usage() > self.max_memory
    }

    // Tree memory
    pub fn get_tree_memory_usage(&self) -> i64 {
        self.tree_memory_usage.load(Ordering::Relaxed)
    }

    pub fn update_tree_memory_usage(&self, delta: i64) {
        self.tree_memory_usage.fetch_add(delta, Ordering::Relaxed);
    }

    // Lock memory
    pub fn get_lock_memory_usage(&self) -> i64 {
        self.lock_memory_usage.load(Ordering::Relaxed)
    }

    pub fn update_lock_memory_usage(&self, delta: i64) {
        self.lock_memory_usage.fetch_add(delta, Ordering::Relaxed);
    }

    // Admin memory
    pub fn get_admin_memory_usage(&self) -> i64 {
        self.admin_memory_usage.load(Ordering::Relaxed)
    }

    pub fn update_admin_memory_usage(&self, delta: i64) {
        self.admin_memory_usage.fetch_add(delta, Ordering::Relaxed);
    }

    /// Resets all usage counters to zero.
    pub fn reset(&self) {
        self.tree_memory_usage.store(0, Ordering::Relaxed);
        self.lock_memory_usage.store(0, Ordering::Relaxed);
        self.admin_memory_usage.store(0, Ordering::Relaxed);
    }

    /// Returns a summary of memory usage.
    pub fn get_stats(&self) -> MemoryBudgetStats {
        MemoryBudgetStats {
            max_memory: self.max_memory,
            total_usage: self.total_usage(),
            tree_memory: self.get_tree_memory_usage(),
            lock_memory: self.get_lock_memory_usage(),
            admin_memory: self.get_admin_memory_usage(),
            log_buffer_budget: self.log_buffer_budget,
        }
    }
}

/// Snapshot of memory budget statistics.
#[derive(Debug, Clone)]
pub struct MemoryBudgetStats {
    pub max_memory: i64,
    pub total_usage: i64,
    pub tree_memory: i64,
    pub lock_memory: i64,
    pub admin_memory: i64,
    pub log_buffer_budget: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_total_usage_is_sum() {
        let budget = MemoryBudget::new(1000);

        budget.update_tree_memory_usage(100);
        budget.update_lock_memory_usage(50);
        budget.update_admin_memory_usage(25);

        assert_eq!(budget.total_usage(), 175);
    }

    #[test]
    fn test_available_memory_calculation() {
        let budget = MemoryBudget::new(1000);

        assert_eq!(budget.available_memory(), 1000);

        budget.update_tree_memory_usage(300);
        assert_eq!(budget.available_memory(), 700);

        budget.update_lock_memory_usage(200);
        assert_eq!(budget.available_memory(), 500);
    }

    #[test]
    fn test_is_over_budget() {
        let budget = MemoryBudget::new(1000);

        assert!(!budget.is_over_budget());

        budget.update_tree_memory_usage(500);
        assert!(!budget.is_over_budget());

        budget.update_lock_memory_usage(500);
        assert!(!budget.is_over_budget());

        budget.update_admin_memory_usage(1);
        assert!(budget.is_over_budget());
    }

    #[test]
    fn test_update_with_positive_and_negative_deltas() {
        let budget = MemoryBudget::new(1000);

        budget.update_tree_memory_usage(500);
        assert_eq!(budget.get_tree_memory_usage(), 500);

        budget.update_tree_memory_usage(-200);
        assert_eq!(budget.get_tree_memory_usage(), 300);

        budget.update_tree_memory_usage(-300);
        assert_eq!(budget.get_tree_memory_usage(), 0);
    }

    #[test]
    fn test_reset_clears_all() {
        let budget = MemoryBudget::new(1000);

        budget.update_tree_memory_usage(100);
        budget.update_lock_memory_usage(200);
        budget.update_admin_memory_usage(300);

        budget.reset();

        assert_eq!(budget.get_tree_memory_usage(), 0);
        assert_eq!(budget.get_lock_memory_usage(), 0);
        assert_eq!(budget.get_admin_memory_usage(), 0);
        assert_eq!(budget.total_usage(), 0);
    }

    #[test]
    fn test_log_buffer_budget_is_7_percent() {
        let budget = MemoryBudget::new(10000);

        // 7% of 10000 = 700
        assert_eq!(budget.log_buffer_budget(), 700);
    }

    #[test]
    fn test_memory_overhead_constants_exist() {
        // Just verify the constants are accessible
        assert_eq!(MemoryOverhead::LOCKIMPL_OVERHEAD, 96);
        assert_eq!(MemoryOverhead::THINLOCKIMPL_OVERHEAD, 32);
        assert_eq!(MemoryOverhead::LOCKINFO_OVERHEAD, 32);
        assert_eq!(MemoryOverhead::HASHMAP_ENTRY_OVERHEAD, 48);
        assert_eq!(MemoryOverhead::LONG_OVERHEAD, 16);
        assert_eq!(MemoryOverhead::IN_OVERHEAD, 400);
        assert_eq!(MemoryOverhead::BIN_OVERHEAD, 500);
        assert_eq!(MemoryOverhead::LN_OVERHEAD, 48);
        assert_eq!(MemoryOverhead::TXN_OVERHEAD, 200);
        assert_eq!(MemoryOverhead::BASICLOCKER_OVERHEAD, 80);
    }

    #[test]
    fn test_get_stats() {
        let budget = MemoryBudget::new(5000);

        budget.update_tree_memory_usage(1000);
        budget.update_lock_memory_usage(500);
        budget.update_admin_memory_usage(250);

        let stats = budget.get_stats();

        assert_eq!(stats.max_memory, 5000);
        assert_eq!(stats.total_usage, 1750);
        assert_eq!(stats.tree_memory, 1000);
        assert_eq!(stats.lock_memory, 500);
        assert_eq!(stats.admin_memory, 250);
        assert_eq!(stats.log_buffer_budget, 350); // 7% of 5000
    }
}
