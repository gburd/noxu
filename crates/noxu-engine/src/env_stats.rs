//! Aggregated environment statistics.

use noxu_cleaner::CleanerStatsSnapshot;
use noxu_evictor::EvictorStats;
use noxu_recovery::CheckpointStatsSnapshot;
use std::sync::atomic::Ordering;

/// Aggregated statistics for the environment.
///
/// Collects statistics from all subsystems into a single snapshot
/// for convenient reporting and monitoring.
#[derive(Debug, Clone)]
pub struct EnvironmentStats {
    /// Evictor statistics snapshot.
    pub evictor: EvictorStatsSnapshot,

    /// Cleaner statistics snapshot.
    pub cleaner: CleanerStatsSnapshot,

    /// Checkpoint statistics snapshot.
    pub checkpoint: CheckpointStatsSnapshot,

    /// Total cache size (budget) in bytes.
    pub cache_size: u64,

    /// Current cache usage in bytes.
    pub cache_usage: u64,

    /// Number of currently open databases.
    pub n_databases: u32,

    /// Number of lock table shards.
    pub n_lock_tables: u32,

    /// Total number of locks held.
    pub n_locks: u64,

    /// Total number of transactions.
    pub n_transactions: u64,
}

impl EnvironmentStats {
    /// Create a new empty statistics snapshot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate cache utilization as a percentage (0-100).
    pub fn cache_utilization_percent(&self) -> f64 {
        if self.cache_size == 0 {
            0.0
        } else {
            (self.cache_usage as f64 / self.cache_size as f64) * 100.0
        }
    }

    /// Check if cache is over budget.
    pub fn is_cache_over_budget(&self) -> bool {
        self.cache_usage > self.cache_size
    }

    /// Get total number of eviction runs across all sources.
    pub fn total_eviction_runs(&self) -> u64 {
        self.evictor.eviction_runs
    }

    /// Get total number of nodes evicted.
    pub fn total_nodes_evicted(&self) -> u64 {
        self.evictor.nodes_evicted
    }

    /// Get total number of bytes evicted.
    pub fn total_bytes_evicted(&self) -> u64 {
        self.evictor.bytes_evicted
    }

    /// Get total number of cleaning runs.
    pub fn total_cleaning_runs(&self) -> u64 {
        self.cleaner.runs
    }

    /// Get total number of files cleaned.
    pub fn total_files_cleaned(&self) -> u64 {
        self.cleaner.deletions
    }

    /// Get total number of checkpoint runs.
    pub fn total_checkpoint_runs(&self) -> u64 {
        self.checkpoint.checkpoints
    }

    /// Get total number of full INs checkpointed.
    pub fn total_full_ins_checkpointed(&self) -> u64 {
        self.checkpoint.full_in_flush
    }
}

impl Default for EnvironmentStats {
    fn default() -> Self {
        Self {
            evictor: EvictorStatsSnapshot::default(),
            cleaner: CleanerStatsSnapshot {
                runs: 0,
                two_pass_runs: 0,
                revisal_runs: 0,
                deletions: 0,
                entries_read: 0,
                disk_reads: 0,
                ins_cleaned: 0,
                ins_dead: 0,
                ins_migrated: 0,
                ins_obsolete: 0,
                lns_cleaned: 0,
                lns_dead: 0,
                lns_migrated: 0,
                lns_obsolete: 0,
                lns_locked: 0,
                lns_marked: 0,
                lns_expired: 0,
                lnqueue_hits: 0,
                pending_lns_processed: 0,
                pending_lns_locked: 0,
                cluster_lns_processed: 0,
                marked_lns_processed: 0,
                to_be_cleaned_lns_processed: 0,
                bin_deltas_cleaned: 0,
                bin_deltas_dead: 0,
                bin_deltas_migrated: 0,
                bin_deltas_obsolete: 0,
                pending_ln_queue_size: 0,
                total_log_size: 0,
                active_log_size: 0,
                reserved_log_size: 0,
                protected_log_size: 0,
                available_log_size: 0,
                min_utilization: 0,
                max_utilization: 0,
            },
            checkpoint: CheckpointStatsSnapshot {
                checkpoints: 0,
                full_in_flush: 0,
                full_bin_flush: 0,
                delta_in_flush: 0,
                last_ckpt_id: 0,
                last_ckpt_start: 0,
                last_ckpt_end: 0,
                last_ckpt_interval: 0,
            },
            cache_size: 0,
            cache_usage: 0,
            n_databases: 0,
            n_lock_tables: 0,
            n_locks: 0,
            n_transactions: 0,
        }
    }
}

/// Snapshot of evictor statistics.
///
/// This is a simplified version that captures key metrics.
#[derive(Debug, Clone, Default)]
pub struct EvictorStatsSnapshot {
    pub eviction_runs: u64,
    pub nodes_evicted: u64,
    pub bytes_evicted: u64,
    pub lru_size: u64,
}

impl From<&EvictorStats> for EvictorStatsSnapshot {
    fn from(stats: &EvictorStats) -> Self {
        Self {
            eviction_runs: stats.eviction_runs.load(Ordering::Relaxed),
            nodes_evicted: stats.nodes_evicted.load(Ordering::Relaxed),
            bytes_evicted: stats.bytes_evicted_daemon.load(Ordering::Relaxed)
                + stats.bytes_evicted_critical.load(Ordering::Relaxed)
                + stats.bytes_evicted_manual.load(Ordering::Relaxed)
                + stats.bytes_evicted_cachemode.load(Ordering::Relaxed),
            lru_size: stats.pri1_lru_size.load(Ordering::Relaxed)
                + stats.pri2_lru_size.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats() {
        let stats = EnvironmentStats::new();
        assert_eq!(stats.cache_size, 0);
        assert_eq!(stats.cache_usage, 0);
        assert_eq!(stats.n_databases, 0);
    }

    #[test]
    fn test_cache_utilization() {
        let mut stats = EnvironmentStats::new();
        stats.cache_size = 1000;
        stats.cache_usage = 500;
        assert_eq!(stats.cache_utilization_percent(), 50.0);

        stats.cache_usage = 750;
        assert_eq!(stats.cache_utilization_percent(), 75.0);

        stats.cache_usage = 1000;
        assert_eq!(stats.cache_utilization_percent(), 100.0);

        stats.cache_usage = 1200;
        assert_eq!(stats.cache_utilization_percent(), 120.0);
    }

    #[test]
    fn test_cache_utilization_zero_size() {
        let mut stats = EnvironmentStats::new();
        stats.cache_size = 0;
        stats.cache_usage = 100;
        assert_eq!(stats.cache_utilization_percent(), 0.0);
    }

    #[test]
    fn test_is_cache_over_budget() {
        let mut stats = EnvironmentStats::new();
        stats.cache_size = 1000;

        stats.cache_usage = 500;
        assert!(!stats.is_cache_over_budget());

        stats.cache_usage = 1000;
        assert!(!stats.is_cache_over_budget());

        stats.cache_usage = 1001;
        assert!(stats.is_cache_over_budget());
    }

    #[test]
    fn test_evictor_aggregates() {
        let mut stats = EnvironmentStats::new();
        stats.evictor.eviction_runs = 10;
        stats.evictor.nodes_evicted = 100;
        stats.evictor.bytes_evicted = 10000;

        assert_eq!(stats.total_eviction_runs(), 10);
        assert_eq!(stats.total_nodes_evicted(), 100);
        assert_eq!(stats.total_bytes_evicted(), 10000);
    }

    #[test]
    fn test_cleaner_aggregates() {
        let mut stats = EnvironmentStats::new();
        stats.cleaner.runs = 5;
        stats.cleaner.deletions = 20;

        assert_eq!(stats.total_cleaning_runs(), 5);
        assert_eq!(stats.total_files_cleaned(), 20);
    }

    #[test]
    fn test_checkpoint_aggregates() {
        let mut stats = EnvironmentStats::new();
        stats.checkpoint.checkpoints = 3;
        stats.checkpoint.full_in_flush = 50;

        assert_eq!(stats.total_checkpoint_runs(), 3);
        assert_eq!(stats.total_full_ins_checkpointed(), 50);
    }

    #[test]
    fn test_default_stats() {
        let stats = EnvironmentStats::default();
        assert_eq!(stats.cache_size, 0);
        assert_eq!(stats.cache_usage, 0);
        assert_eq!(stats.n_databases, 0);
        assert_eq!(stats.n_lock_tables, 0);
        assert_eq!(stats.n_locks, 0);
        assert_eq!(stats.n_transactions, 0);
        assert_eq!(stats.total_eviction_runs(), 0);
        assert_eq!(stats.total_cleaning_runs(), 0);
        assert_eq!(stats.total_checkpoint_runs(), 0);
    }

    #[test]
    fn test_full_stats() {
        let mut stats = EnvironmentStats::new();
        stats.cache_size = 64 * 1024 * 1024;
        stats.cache_usage = 32 * 1024 * 1024;
        stats.n_databases = 5;
        stats.n_lock_tables = 16;
        stats.n_locks = 100;
        stats.n_transactions = 10;

        stats.evictor.eviction_runs = 20;
        stats.evictor.nodes_evicted = 500;
        stats.evictor.bytes_evicted = 50000;

        stats.cleaner.runs = 8;
        stats.cleaner.deletions = 15;

        stats.checkpoint.checkpoints = 4;
        stats.checkpoint.full_in_flush = 200;

        assert_eq!(stats.cache_utilization_percent(), 50.0);
        assert!(!stats.is_cache_over_budget());
        assert_eq!(stats.total_eviction_runs(), 20);
        assert_eq!(stats.total_nodes_evicted(), 500);
        assert_eq!(stats.total_cleaning_runs(), 8);
        assert_eq!(stats.total_checkpoint_runs(), 4);
    }
}
