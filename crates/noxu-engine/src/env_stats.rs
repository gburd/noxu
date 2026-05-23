//! Aggregated environment statistics.

use noxu_cleaner::CleanerStatsSnapshot;
use noxu_dbi::ThroughputStatsSnapshot;
use noxu_evictor::EvictorStats;
use noxu_log::LogManagerStats;
use noxu_recovery::CheckpointStatsSnapshot;
use noxu_txn::{LockStats, TxnStats};
use std::sync::atomic::Ordering;

/// Aggregated statistics for the environment.
///
/// Collects statistics from all subsystems into a single snapshot
/// for convenient reporting and monitoring.  Mirrors 's
/// `EnvironmentStats` grouping: Cache, Evictor, Log, Lock, Txn,
/// Cleaner, Checkpoint, Throughput.
#[derive(Debug, Clone)]
pub struct EnvironmentStats {
    // ── Cache ──────────────────────────────────────────────────────────────
    /// Total cache size (budget) in bytes.
    pub cache_size: u64,
    /// Current cache usage in bytes.
    pub cache_usage: u64,

    // ── B-tree node counts ─────────────────────────────────────────────────
    /// Number of currently open databases.
    pub n_databases: u32,

    // ── Evictor ───────────────────────────────────────────────────────────
    pub evictor: EvictorStatsSnapshot,

    // ── Log / FileManager / FsyncManager ──────────────────────────────────
    pub log: LogStatsSnapshot,

    // ── Lock manager ──────────────────────────────────────────────────────
    pub lock: LockStatsSnapshot,

    // ── Transaction manager ───────────────────────────────────────────────
    pub txn: TxnStatsSnapshot,

    // ── Cleaner ───────────────────────────────────────────────────────────
    pub cleaner: CleanerStatsSnapshot,

    // ── Checkpointer ──────────────────────────────────────────────────────
    pub checkpoint: CheckpointStatsSnapshot,

    // ── Throughput ────────────────────────────────────────────────────────
    pub throughput: ThroughputStatsSnapshot,
}

impl EnvironmentStats {
    pub fn new() -> Self {
        Self::default()
    }

    // ── Convenience aggregators ────────────────────────────────────────────

    pub fn cache_utilization_percent(&self) -> f64 {
        if self.cache_size == 0 {
            0.0
        } else {
            (self.cache_usage as f64 / self.cache_size as f64) * 100.0
        }
    }

    pub fn is_cache_over_budget(&self) -> bool {
        self.cache_usage > self.cache_size
    }

    pub fn total_eviction_runs(&self) -> u64 {
        self.evictor.eviction_runs
    }

    pub fn total_nodes_evicted(&self) -> u64 {
        self.evictor.nodes_evicted
    }

    pub fn total_bytes_evicted(&self) -> u64 {
        self.evictor.bytes_evicted
    }

    pub fn total_cleaning_runs(&self) -> u64 {
        self.cleaner.runs
    }

    pub fn total_files_cleaned(&self) -> u64 {
        self.cleaner.deletions
    }

    pub fn total_checkpoint_runs(&self) -> u64 {
        self.checkpoint.checkpoints
    }

    pub fn total_full_ins_checkpointed(&self) -> u64 {
        self.checkpoint.full_in_flush
    }

    /// Bin fetch miss ratio (0.0 – 1.0).
    pub fn bin_fetch_miss_ratio(&self) -> f64 {
        let total = self.evictor.bin_fetch;
        if total == 0 {
            0.0
        } else {
            self.evictor.bin_fetch_miss as f64 / total as f64
        }
    }
}

impl Default for EnvironmentStats {
    fn default() -> Self {
        Self {
            cache_size: 0,
            cache_usage: 0,
            n_databases: 0,
            evictor: EvictorStatsSnapshot::default(),
            log: LogStatsSnapshot::default(),
            lock: LockStatsSnapshot::default(),
            txn: TxnStatsSnapshot::default(),
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
                probe_runs: 0,
                repeat_iterator_reads: 0,
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
            throughput: ThroughputStatsSnapshot::default(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// EvictorStatsSnapshot — all fields from EvictorStats
// ═══════════════════════════════════════════════════════════════════════════

/// Full snapshot of evictor statistics.
///
/// EvictorStatDefinition.
#[derive(Debug, Clone, Default)]
pub struct EvictorStatsSnapshot {
    // Eviction counts
    pub eviction_runs: u64,
    pub nodes_targeted: u64,
    pub nodes_evicted: u64,
    pub nodes_skipped: u64,
    pub nodes_mutated: u64,
    pub nodes_stripped: u64,
    pub nodes_put_back: u64,
    pub nodes_moved_to_pri2_lru: u64,
    pub root_nodes_evicted: u64,
    pub dirty_nodes_evicted: u64,
    pub lns_evicted: u64,

    // Bytes evicted by source
    pub bytes_evicted_daemon: u64,
    pub bytes_evicted_critical: u64,
    pub bytes_evicted_manual: u64,
    pub bytes_evicted_cachemode: u64,
    /// Sum of all bytes_evicted_* fields.
    pub bytes_evicted: u64,

    // Fetch / miss statistics
    pub bin_fetch: u64,
    pub bin_fetch_miss: u64,
    pub ln_fetch: u64,
    pub ln_fetch_miss: u64,
    pub upper_in_fetch: u64,
    pub upper_in_fetch_miss: u64,
    pub bin_delta_fetch_miss: u64,
    pub full_bin_miss: u64,
    pub bin_delta_blind_ops: u64,

    // LRU sizes (instant)
    pub pri1_lru_size: u64,
    pub pri2_lru_size: u64,
    /// Sum of pri1 + pri2.
    pub lru_size: u64,

    // Thread pool
    /// Number of eviction tasks refused because all threads were busy.
    pub thread_unavailable: u64,

    // Cache composition (instant stats)
    /// Number of upper INs in main cache.
    pub cached_upper_ins: u64,
    /// Number of BINs and BIN-deltas in main cache.
    pub cached_bins: u64,
    /// Number of BIN-deltas in main cache.
    pub cached_bin_deltas: u64,
    /// Number of INs using compact sparse array representation.
    pub cached_in_sparse_target: u64,
    /// Number of INs using compact no-child representation.
    pub cached_in_no_target: u64,
    /// Number of INs using compact key representation.
    pub cached_in_compact_key: u64,
}

impl From<&EvictorStats> for EvictorStatsSnapshot {
    fn from(stats: &EvictorStats) -> Self {
        let d = stats.bytes_evicted_daemon.load(Ordering::Relaxed);
        let c = stats.bytes_evicted_critical.load(Ordering::Relaxed);
        let m = stats.bytes_evicted_manual.load(Ordering::Relaxed);
        let cm = stats.bytes_evicted_cachemode.load(Ordering::Relaxed);
        let p1 = stats.pri1_lru_size.load(Ordering::Relaxed);
        let p2 = stats.pri2_lru_size.load(Ordering::Relaxed);
        Self {
            eviction_runs: stats.eviction_runs.load(Ordering::Relaxed),
            nodes_targeted: stats.nodes_targeted.load(Ordering::Relaxed),
            nodes_evicted: stats.nodes_evicted.load(Ordering::Relaxed),
            nodes_skipped: stats.nodes_skipped.load(Ordering::Relaxed),
            nodes_mutated: stats.nodes_mutated.load(Ordering::Relaxed),
            nodes_stripped: stats.nodes_stripped.load(Ordering::Relaxed),
            nodes_put_back: stats.nodes_put_back.load(Ordering::Relaxed),
            nodes_moved_to_pri2_lru: stats
                .nodes_moved_to_pri2_lru
                .load(Ordering::Relaxed),
            root_nodes_evicted: stats
                .root_nodes_evicted
                .load(Ordering::Relaxed),
            dirty_nodes_evicted: stats
                .dirty_nodes_evicted
                .load(Ordering::Relaxed),
            lns_evicted: stats.lns_evicted.load(Ordering::Relaxed),
            bytes_evicted_daemon: d,
            bytes_evicted_critical: c,
            bytes_evicted_manual: m,
            bytes_evicted_cachemode: cm,
            bytes_evicted: d + c + m + cm,
            bin_fetch: stats.bin_fetch.load(Ordering::Relaxed),
            bin_fetch_miss: stats.bin_fetch_miss.load(Ordering::Relaxed),
            ln_fetch: stats.ln_fetch.load(Ordering::Relaxed),
            ln_fetch_miss: stats.ln_fetch_miss.load(Ordering::Relaxed),
            upper_in_fetch: stats.upper_in_fetch.load(Ordering::Relaxed),
            upper_in_fetch_miss: stats
                .upper_in_fetch_miss
                .load(Ordering::Relaxed),
            bin_delta_fetch_miss: stats
                .bin_delta_fetch_miss
                .load(Ordering::Relaxed),
            full_bin_miss: stats.full_bin_miss.load(Ordering::Relaxed),
            bin_delta_blind_ops: stats
                .bin_delta_blind_ops
                .load(Ordering::Relaxed),
            pri1_lru_size: p1,
            pri2_lru_size: p2,
            lru_size: p1 + p2,
            thread_unavailable: stats
                .thread_unavailable
                .load(Ordering::Relaxed),
            cached_upper_ins: stats.cached_upper_ins.load(Ordering::Relaxed),
            cached_bins: stats.cached_bins.load(Ordering::Relaxed),
            cached_bin_deltas: stats.cached_bin_deltas.load(Ordering::Relaxed),
            cached_in_sparse_target: stats
                .cached_in_sparse_target
                .load(Ordering::Relaxed),
            cached_in_no_target: stats
                .cached_in_no_target
                .load(Ordering::Relaxed),
            cached_in_compact_key: stats
                .cached_in_compact_key
                .load(Ordering::Relaxed),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// LogStatsSnapshot — log manager + file manager + fsync manager
// ═══════════════════════════════════════════════════════════════════════════

/// Snapshot of log subsystem statistics.
///
/// LogStatDefinition (LOGMGR_*, FILEMGR_*, FSYNCMGR_*).
#[derive(Debug, Clone, Default)]
pub struct LogStatsSnapshot {
    /// Current end-of-log LSN (as raw u64).
    pub end_of_log: u64,
    /// LSN of the last completed flush.
    pub last_flush_lsn: u64,
    /// Number of repeated fault reads (log entries re-read on cache miss).
    pub n_repeat_fault_reads: u64,
    /// Number of temporary-buffer writes (oversized entries).
    pub n_temp_buffer_writes: u64,
    /// Number of log buffers in the pool.
    pub n_log_buffers: u64,
    /// Total bytes across all log buffers.
    pub n_log_buffer_bytes: u64,
    /// Number of fdatasync calls completed (after group-commit coalescing).
    pub n_log_fsyncs: u64,
    /// Number of fdatasync requests (before coalescing).
    pub n_fsync_requests: u64,
    /// Number of log files opened (LRU cache miss).
    pub n_file_opens: u64,
    /// Number of sequential read operations.
    pub n_sequential_reads: u64,
    /// Total bytes read sequentially.
    pub n_sequential_read_bytes: u64,
    /// Number of sequential write operations.
    pub n_sequential_writes: u64,
    /// Total bytes written sequentially.
    pub n_sequential_write_bytes: u64,
    /// Number of random (point-lookup) read operations.
    pub n_random_reads: u64,
    /// Total bytes from random reads.
    pub n_random_read_bytes: u64,
    /// Number of fsync requests that timed out.
    pub n_fsync_timeouts: u64,
    /// Number of group-commit batches (leader served ≥1 waiter).
    pub n_group_commits: u64,
    /// Cumulative fsync duration in milliseconds.
    pub fsync_time_ms: u64,
    /// Sum of all group-commit batch sizes (total waiters served across all batches).
    pub n_fsync_batch_size_sum: u64,
}

impl From<&LogManagerStats> for LogStatsSnapshot {
    fn from(s: &LogManagerStats) -> Self {
        Self {
            end_of_log: s.end_of_log.as_u64(),
            last_flush_lsn: s.last_flush_lsn.as_u64(),
            n_repeat_fault_reads: s.n_repeat_fault_reads,
            n_temp_buffer_writes: s.n_temp_buffer_writes,
            n_log_buffers: s.buffer_pool_stats.num_buffers as u64,
            n_log_buffer_bytes: (s.buffer_pool_stats.num_buffers as u64)
                * (s.buffer_pool_stats.buffer_size as u64),
            n_log_fsyncs: s.n_log_fsyncs,
            n_fsync_requests: s.n_fsync_requests,
            n_file_opens: s.n_file_opens,
            n_sequential_reads: s.n_sequential_reads,
            n_sequential_read_bytes: s.n_sequential_read_bytes,
            n_sequential_writes: s.n_sequential_writes,
            n_sequential_write_bytes: s.n_sequential_write_bytes,
            n_random_reads: s.n_random_reads,
            n_random_read_bytes: s.n_random_read_bytes,
            n_fsync_timeouts: s.n_fsync_timeouts,
            n_group_commits: s.n_group_commits,
            fsync_time_ms: s.fsync_time_ms,
            n_fsync_batch_size_sum: s.n_fsync_batch_size_sum,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// LockStatsSnapshot — lock manager statistics
// ═══════════════════════════════════════════════════════════════════════════

/// Snapshot of lock manager statistics.
///
/// LockStatDefinition.
#[derive(Debug, Clone, Default)]
pub struct LockStatsSnapshot {
    /// Total lock requests.
    pub n_requests: u64,
    /// Number of requests that waited (blocked).
    pub n_waits: u64,
    /// Number of distinct lock objects held.
    pub n_total_locks: u64,
    /// Number of read-lock holders.
    pub n_read_locks: u64,
    /// Number of write-lock holders.
    pub n_write_locks: u64,
    /// Number of distinct lock owners.
    pub n_owners: u64,
    /// Number of lockers currently waiting.
    pub n_waiters: u64,
    /// Number of lock tables (shards).
    pub n_lock_tables: u64,
    /// Number of lock acquisitions that timed out.
    pub n_lock_timeouts: u64,
}

impl From<&LockStats> for LockStatsSnapshot {
    fn from(s: &LockStats) -> Self {
        Self {
            n_requests: s.lock_requests,
            n_waits: s.lock_waits,
            n_total_locks: s.n_total_locks,
            n_read_locks: s.n_read_locks,
            n_write_locks: s.n_write_locks,
            n_owners: s.n_owners,
            n_waiters: s.n_waiters,
            n_lock_tables: 0, // filled in by engine.rs
            n_lock_timeouts: s.n_lock_timeouts,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TxnStatsSnapshot — transaction manager statistics
// ═══════════════════════════════════════════════════════════════════════════

/// Snapshot of transaction manager statistics.
#[derive(Debug, Clone, Default)]
pub struct TxnStatsSnapshot {
    pub n_begins: u64,
    pub n_commits: u64,
    pub n_aborts: u64,
    pub n_active: u64,
}

impl From<&TxnStats> for TxnStatsSnapshot {
    fn from(s: &TxnStats) -> Self {
        Self {
            n_begins: s.n_begins,
            n_commits: s.n_commits,
            n_aborts: s.n_aborts,
            n_active: s.n_active,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

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
        stats.cache_usage = 999;
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
    fn test_log_stats_snapshot_default() {
        let s = LogStatsSnapshot::default();
        assert_eq!(s.n_log_fsyncs, 0);
        assert_eq!(s.n_sequential_writes, 0);
    }

    #[test]
    fn test_lock_stats_snapshot_default() {
        let s = LockStatsSnapshot::default();
        assert_eq!(s.n_requests, 0);
        assert_eq!(s.n_total_locks, 0);
    }

    #[test]
    fn test_txn_stats_snapshot_default() {
        let s = TxnStatsSnapshot::default();
        assert_eq!(s.n_begins, 0);
        assert_eq!(s.n_active, 0);
    }

    #[test]
    fn test_throughput_stats_snapshot_default() {
        let s = ThroughputStatsSnapshot::default();
        assert_eq!(s.n_pri_inserts, 0);
        assert_eq!(s.n_pri_searches, 0);
    }

    #[test]
    fn test_bin_fetch_miss_ratio_zero_denominator() {
        let stats = EnvironmentStats::new();
        assert_eq!(stats.bin_fetch_miss_ratio(), 0.0);
    }

    #[test]
    fn test_bin_fetch_miss_ratio() {
        let mut stats = EnvironmentStats::new();
        stats.evictor.bin_fetch = 100;
        stats.evictor.bin_fetch_miss = 25;
        assert_eq!(stats.bin_fetch_miss_ratio(), 0.25);
    }

    #[test]
    fn test_evictor_snapshot_all_fields() {
        let snap = EvictorStatsSnapshot {
            eviction_runs: 1,
            nodes_targeted: 2,
            nodes_evicted: 3,
            nodes_skipped: 4,
            nodes_mutated: 5,
            nodes_stripped: 6,
            nodes_put_back: 7,
            nodes_moved_to_pri2_lru: 8,
            root_nodes_evicted: 9,
            dirty_nodes_evicted: 10,
            lns_evicted: 11,
            bytes_evicted_daemon: 100,
            bytes_evicted_critical: 200,
            bytes_evicted_manual: 300,
            bytes_evicted_cachemode: 400,
            bytes_evicted: 1000,
            bin_fetch: 50,
            bin_fetch_miss: 5,
            ln_fetch: 40,
            ln_fetch_miss: 4,
            upper_in_fetch: 30,
            upper_in_fetch_miss: 3,
            bin_delta_fetch_miss: 2,
            full_bin_miss: 1,
            bin_delta_blind_ops: 20,
            pri1_lru_size: 100,
            pri2_lru_size: 200,
            lru_size: 300,
            thread_unavailable: 0,
            cached_upper_ins: 10,
            cached_bins: 20,
            cached_bin_deltas: 5,
            cached_in_sparse_target: 3,
            cached_in_no_target: 2,
            cached_in_compact_key: 1,
        };
        assert_eq!(snap.bytes_evicted, 1000);
        assert_eq!(snap.lru_size, 300);
        assert_eq!(snap.cached_bins, 20);
    }

    #[test]
    fn test_default_stats() {
        let stats = EnvironmentStats::default();
        assert_eq!(stats.cache_size, 0);
        assert_eq!(stats.n_databases, 0);
        assert_eq!(stats.total_eviction_runs(), 0);
        assert_eq!(stats.total_cleaning_runs(), 0);
        assert_eq!(stats.total_checkpoint_runs(), 0);
    }
}
