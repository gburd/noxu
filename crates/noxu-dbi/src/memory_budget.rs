use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// `MemoryBudget` aggregates the engine's per-category cache memory so the
/// evictor's over-budget decision sees **total** memory, not just tree nodes.
///
/// JE `MemoryBudget` (dbi/MemoryBudget.java) keeps four live counters —
/// `treeMemoryUsage`, `lockMemoryUsage`, `txnMemoryUsage`, `adminMemoryUsage`
/// — each updated on every node / lock / txn / tracker allocation, and the
/// over-budget arbiter reads their sum.
///
/// DBI-20/21: previously this struct existed but was never instantiated in
/// production; the engine tracked a single flat `cache_usage` `AtomicI64` fed
/// only by the tree path, so lock-table and txn footprint were invisible to
/// the arbiter.  We now back the **tree** category with that same shared
/// `cache_usage` `Arc` (so the existing tree accounting flows straight
/// through, and the arbiter still reads it), and add live lock / txn / admin
/// categories on top so they can be fed and counted in `total_usage()`.
///
/// Feeding the lock / txn categories from `noxu-txn` is a documented
/// follow-up: it requires either a callback hook (to avoid a `noxu-txn ->
/// noxu-dbi` circular dependency) or moving the counter to a shared crate.
/// The structure is now real (not dead) and ready for that wiring.
pub struct MemoryBudget {
    /// Maximum cache size in bytes.
    max_memory: i64,
    /// Tree (IN/BIN) memory.  Backed by the shared `cache_usage` `Arc` the
    /// tree path increments and the arbiter reads — JE `treeMemoryUsage`.
    tree_memory_usage: Arc<AtomicI64>,
    /// Lock-table memory — JE `lockMemoryUsage`.
    lock_memory_usage: AtomicI64,
    /// Transaction memory — JE `txnMemoryUsage`.
    txn_memory_usage: AtomicI64,
    /// Admin / misc memory (e.g. cleaner utilization tracker) —
    /// JE `adminMemoryUsage`.
    admin_memory_usage: AtomicI64,
    /// Current cache usage: log buffer memory.
    log_buffer_budget: i64,
}

/// Per-object memory overheads, in bytes (DBI-22).
///
/// JE measures these with `Sizeof` on the live JVM and selects a 32-bit /
/// 64-bit / compressed-oops variant at startup (MemoryBudget.java ~242-268).
/// Rust has a fixed struct layout, so where a Rust type maps directly to the
/// JE object we derive the constant with `size_of`; otherwise we cite JE's
/// 64-bit value (the platform Noxu targets).  These replace the previous
/// dead round-number guesses (96 / 200 / …) that matched no real allocation.
pub struct MemoryOverhead;

impl MemoryOverhead {
    /// `LockImpl` overhead.  JE `LOCKIMPL_OVERHEAD_64` = 48.
    pub const LOCKIMPL_OVERHEAD: i64 = 48;
    /// `ThinLockImpl` overhead.  JE `THINLOCKIMPL_OVERHEAD_64` = 32.
    pub const THINLOCKIMPL_OVERHEAD: i64 = 32;
    /// `LockInfo` overhead.  JE `LOCKINFO_OVERHEAD_64` = 32.
    pub const LOCKINFO_OVERHEAD: i64 = 32;
    /// `WriteLockInfo` overhead.  JE `WRITE_LOCKINFO_OVERHEAD_64` = 72.
    pub const WRITE_LOCKINFO_OVERHEAD: i64 = 72;
    /// HashMap entry overhead.  JE `HASHMAP_ENTRY_OVERHEAD_64` = 52.
    pub const HASHMAP_ENTRY_OVERHEAD: i64 = 52;
    /// Boxed `i64` (`Long`) overhead.  JE `LONG_OVERHEAD_64` = 16.
    pub const LONG_OVERHEAD: i64 = 16;
    /// LN (leaf node) fixed overhead.  JE `LN_OVERHEAD_64` = 32.
    pub const LN_OVERHEAD: i64 = 32;
    /// Transaction (`Txn`) overhead.  JE `TXN_OVERHEAD_64` = 361.
    pub const TXN_OVERHEAD: i64 = 361;
}

impl MemoryBudget {
    /// Creates a new MemoryBudget whose tree category is backed by the shared
    /// `cache_usage` counter the tree path increments and the arbiter reads.
    pub fn new(max_memory: i64, tree_memory_usage: Arc<AtomicI64>) -> Self {
        // Reserve 7% for log buffers (matching default)
        let log_buffer_budget = max_memory * 7 / 100;

        MemoryBudget {
            max_memory,
            tree_memory_usage,
            lock_memory_usage: AtomicI64::new(0),
            txn_memory_usage: AtomicI64::new(0),
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

    /// Total current memory usage across ALL categories — the figure JE's
    /// over-budget decision uses (tree + lock + txn + admin).
    pub fn total_usage(&self) -> i64 {
        self.tree_memory_usage.load(Ordering::Relaxed)
            + self.lock_memory_usage.load(Ordering::Relaxed)
            + self.txn_memory_usage.load(Ordering::Relaxed)
            + self.admin_memory_usage.load(Ordering::Relaxed)
    }

    /// Returns the available (free) cache memory.
    pub fn available_memory(&self) -> i64 {
        self.max_memory - self.total_usage()
    }

    /// Returns true if the cache is over budget (total across all categories).
    pub fn is_over_budget(&self) -> bool {
        self.total_usage() > self.max_memory
    }

    // Tree memory (shared with the arbiter's cache_usage).
    pub fn get_tree_memory_usage(&self) -> i64 {
        self.tree_memory_usage.load(Ordering::Relaxed)
    }

    pub fn update_tree_memory_usage(&self, delta: i64) {
        self.tree_memory_usage.fetch_add(delta, Ordering::Relaxed);
    }

    /// The shared tree-memory counter, so the arbiter and tree can clone it.
    pub fn tree_memory_counter(&self) -> Arc<AtomicI64> {
        Arc::clone(&self.tree_memory_usage)
    }

    // Lock memory — JE updateLockMemoryUsage.
    pub fn get_lock_memory_usage(&self) -> i64 {
        self.lock_memory_usage.load(Ordering::Relaxed)
    }

    pub fn update_lock_memory_usage(&self, delta: i64) {
        self.lock_memory_usage.fetch_add(delta, Ordering::Relaxed);
    }

    // Txn memory — JE updateTxnMemoryUsage.
    pub fn get_txn_memory_usage(&self) -> i64 {
        self.txn_memory_usage.load(Ordering::Relaxed)
    }

    pub fn update_txn_memory_usage(&self, delta: i64) {
        self.txn_memory_usage.fetch_add(delta, Ordering::Relaxed);
    }

    // Admin memory — JE updateAdminMemoryUsage (e.g. cleaner tracker detail).
    pub fn get_admin_memory_usage(&self) -> i64 {
        self.admin_memory_usage.load(Ordering::Relaxed)
    }

    pub fn update_admin_memory_usage(&self, delta: i64) {
        self.admin_memory_usage.fetch_add(delta, Ordering::Relaxed);
    }

    /// Resets the non-tree usage counters to zero.  The tree counter is shared
    /// with the arbiter and owned by the tree path, so it is NOT reset here.
    pub fn reset(&self) {
        self.lock_memory_usage.store(0, Ordering::Relaxed);
        self.txn_memory_usage.store(0, Ordering::Relaxed);
        self.admin_memory_usage.store(0, Ordering::Relaxed);
    }

    /// Returns a summary of memory usage.
    pub fn get_stats(&self) -> MemoryBudgetStats {
        MemoryBudgetStats {
            max_memory: self.max_memory,
            total_usage: self.total_usage(),
            tree_memory: self.get_tree_memory_usage(),
            lock_memory: self.get_lock_memory_usage(),
            txn_memory: self.get_txn_memory_usage(),
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
    pub txn_memory: i64,
    pub admin_memory: i64,
    pub log_buffer_budget: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(max: i64) -> MemoryBudget {
        MemoryBudget::new(max, Arc::new(AtomicI64::new(0)))
    }

    #[test]
    fn test_total_usage_is_sum() {
        let budget = budget(1000);

        budget.update_tree_memory_usage(100);
        budget.update_lock_memory_usage(50);
        budget.update_txn_memory_usage(15);
        budget.update_admin_memory_usage(10);

        assert_eq!(budget.total_usage(), 175);
    }

    #[test]
    fn test_available_memory_calculation() {
        let budget = budget(1000);

        assert_eq!(budget.available_memory(), 1000);

        budget.update_tree_memory_usage(300);
        assert_eq!(budget.available_memory(), 700);

        budget.update_lock_memory_usage(200);
        assert_eq!(budget.available_memory(), 500);
    }

    #[test]
    fn test_is_over_budget_sees_all_categories() {
        // DBI-20/21: lock + txn footprint must push the budget over, not just
        // tree memory.
        let budget = budget(1000);

        assert!(!budget.is_over_budget());

        budget.update_tree_memory_usage(500);
        assert!(!budget.is_over_budget());

        budget.update_lock_memory_usage(400);
        budget.update_txn_memory_usage(100);
        assert!(!budget.is_over_budget());

        budget.update_admin_memory_usage(1);
        assert!(
            budget.is_over_budget(),
            "lock + txn + admin must count toward over-budget"
        );
    }

    #[test]
    fn test_tree_category_shares_arc() {
        // The tree counter is the SAME Arc the arbiter reads: an external
        // tree-path update must be visible through the budget.
        let shared = Arc::new(AtomicI64::new(0));
        let budget = MemoryBudget::new(1000, Arc::clone(&shared));
        shared.fetch_add(250, Ordering::Relaxed); // simulate tree-path insert
        assert_eq!(budget.get_tree_memory_usage(), 250);
        assert_eq!(budget.total_usage(), 250);
    }

    #[test]
    fn test_update_with_positive_and_negative_deltas() {
        let budget = budget(1000);

        budget.update_tree_memory_usage(500);
        assert_eq!(budget.get_tree_memory_usage(), 500);

        budget.update_tree_memory_usage(-200);
        assert_eq!(budget.get_tree_memory_usage(), 300);

        budget.update_tree_memory_usage(-300);
        assert_eq!(budget.get_tree_memory_usage(), 0);
    }

    #[test]
    fn test_reset_clears_non_tree_categories() {
        let budget = budget(1000);

        budget.update_tree_memory_usage(100);
        budget.update_lock_memory_usage(200);
        budget.update_txn_memory_usage(50);
        budget.update_admin_memory_usage(300);

        budget.reset();

        // Tree counter is owned by the tree path; reset leaves it alone.
        assert_eq!(budget.get_tree_memory_usage(), 100);
        assert_eq!(budget.get_lock_memory_usage(), 0);
        assert_eq!(budget.get_txn_memory_usage(), 0);
        assert_eq!(budget.get_admin_memory_usage(), 0);
    }

    #[test]
    fn test_log_buffer_budget_is_7_percent() {
        let budget = budget(10000);

        // 7% of 10000 = 700
        assert_eq!(budget.log_buffer_budget(), 700);
    }

    #[test]
    fn test_memory_overhead_constants_match_je_64bit() {
        // DBI-22: these now mirror JE's 64-bit Sizeof constants, not the old
        // round-number guesses.
        assert_eq!(MemoryOverhead::LOCKIMPL_OVERHEAD, 48);
        assert_eq!(MemoryOverhead::THINLOCKIMPL_OVERHEAD, 32);
        assert_eq!(MemoryOverhead::LOCKINFO_OVERHEAD, 32);
        assert_eq!(MemoryOverhead::WRITE_LOCKINFO_OVERHEAD, 72);
        assert_eq!(MemoryOverhead::HASHMAP_ENTRY_OVERHEAD, 52);
        assert_eq!(MemoryOverhead::LONG_OVERHEAD, 16);
        assert_eq!(MemoryOverhead::LN_OVERHEAD, 32);
        assert_eq!(MemoryOverhead::TXN_OVERHEAD, 361);
    }

    #[test]
    fn test_get_stats() {
        let budget = budget(5000);

        budget.update_tree_memory_usage(1000);
        budget.update_lock_memory_usage(500);
        budget.update_txn_memory_usage(100);
        budget.update_admin_memory_usage(250);

        let stats = budget.get_stats();

        assert_eq!(stats.max_memory, 5000);
        assert_eq!(stats.total_usage, 1850);
        assert_eq!(stats.tree_memory, 1000);
        assert_eq!(stats.lock_memory, 500);
        assert_eq!(stats.txn_memory, 100);
        assert_eq!(stats.admin_memory, 250);
        assert_eq!(stats.log_buffer_budget, 350); // 7% of 5000
    }
}
