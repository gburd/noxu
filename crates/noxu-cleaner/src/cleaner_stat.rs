//! Cleaner statistics tracking.
//!
//! comprehensive statistics for the cleaner
//! daemon, including runs, migrations, deletions, and disk usage metrics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Comprehensive statistics for the cleaner daemon.
///
/// All counters use atomic operations for lock-free updates from multiple
/// FileProcessor threads. The relaxed ordering is sufficient since exact
/// counts are not critical for statistics.
#[derive(Debug)]
pub struct CleanerStats {
    /// Number of cleaner runs, including two-pass runs.
    pub runs: AtomicU64,

    /// Number of cleaner two-pass runs.
    pub two_pass_runs: AtomicU64,

    /// Number of cleaner runs that ended in revising expiration info, but
    /// not in any cleaning.
    pub revisal_runs: AtomicU64,

    /// Number of cleaner file deletions.
    pub deletions: AtomicU64,

    /// Accumulated number of log entries read by the cleaner.
    pub entries_read: AtomicU64,

    /// Number of disk reads by the cleaner.
    pub disk_reads: AtomicU64,

    /// Accumulated number of INs cleaned.
    pub ins_cleaned: AtomicU64,

    /// Accumulated number of INs that were not found in the tree anymore (deleted).
    pub ins_dead: AtomicU64,

    /// Accumulated number of INs migrated.
    pub ins_migrated: AtomicU64,

    /// Accumulated number of INs obsolete.
    pub ins_obsolete: AtomicU64,

    /// Accumulated number of LNs cleaned.
    pub lns_cleaned: AtomicU64,

    /// Accumulated number of LNs that were not found in the tree anymore (deleted).
    pub lns_dead: AtomicU64,

    /// Accumulated number of LNs that were migrated forward in the log by the cleaner.
    pub lns_migrated: AtomicU64,

    /// Accumulated number of LNs obsolete.
    pub lns_obsolete: AtomicU64,

    /// Accumulated number of LNs encountered that were locked.
    pub lns_locked: AtomicU64,

    /// Accumulated number of LNs in temporary DBs that were dirtied by the
    /// cleaner and subsequently logged during checkpoint/eviction.
    pub lns_marked: AtomicU64,

    /// Accumulated number of obsolete LNs that were expired.
    pub lns_expired: AtomicU64,

    /// Accumulated number of LNs processed without a tree lookup.
    pub lnqueue_hits: AtomicU64,

    /// Accumulated number of LNs processed because they were previously locked.
    pub pending_lns_processed: AtomicU64,

    /// Accumulated number of pending LNs that could not be locked for migration
    /// because of a long duration application lock.
    pub pending_lns_locked: AtomicU64,

    /// Accumulated number of LNs processed because they qualify for clustering.
    pub cluster_lns_processed: AtomicU64,

    /// Accumulated number of LNs processed because they were previously marked
    /// for migration.
    pub marked_lns_processed: AtomicU64,

    /// Accumulated number of LNs processed because they are soon to be cleaned.
    pub to_be_cleaned_lns_processed: AtomicU64,

    /// Accumulated number of BIN-deltas cleaned.
    pub bin_deltas_cleaned: AtomicU64,

    /// Accumulated number of BIN-deltas that were not found in the tree anymore (deleted).
    pub bin_deltas_dead: AtomicU64,

    /// Accumulated number of BIN-deltas migrated.
    pub bin_deltas_migrated: AtomicU64,

    /// Accumulated number of BIN-deltas obsolete.
    pub bin_deltas_obsolete: AtomicU64,

    /// Number of LNs pending because they were locked and could not be migrated.
    pub pending_ln_queue_size: AtomicU64,

    /// Bytes used by data files on disk: activeLogSize + reservedLogSize.
    pub total_log_size: AtomicU64,

    /// Bytes used by all active data files: files required for basic operation.
    pub active_log_size: AtomicU64,

    /// Bytes used by all reserved data files: files that have been cleaned and
    /// can be deleted if they are not protected.
    pub reserved_log_size: AtomicU64,

    /// Bytes used by all protected data files: the subset of reserved files that
    /// are temporarily protected and cannot be deleted.
    pub protected_log_size: AtomicU64,

    /// Bytes available for write operations when unprotected reserved files are
    /// deleted: free space + reservedLogSize - protectedLogSize.
    pub available_log_size: AtomicU64,

    /// The current minimum (lower bound) log utilization as a percentage.
    pub min_utilization: AtomicU64,

    /// The current maximum (upper bound) log utilization as a percentage.
    pub max_utilization: AtomicU64,
}

impl CleanerStats {
    /// Creates a new statistics object with all counters initialized to zero.
    pub fn new() -> Self {
        Self {
            runs: AtomicU64::new(0),
            two_pass_runs: AtomicU64::new(0),
            revisal_runs: AtomicU64::new(0),
            deletions: AtomicU64::new(0),
            entries_read: AtomicU64::new(0),
            disk_reads: AtomicU64::new(0),
            ins_cleaned: AtomicU64::new(0),
            ins_dead: AtomicU64::new(0),
            ins_migrated: AtomicU64::new(0),
            ins_obsolete: AtomicU64::new(0),
            lns_cleaned: AtomicU64::new(0),
            lns_dead: AtomicU64::new(0),
            lns_migrated: AtomicU64::new(0),
            lns_obsolete: AtomicU64::new(0),
            lns_locked: AtomicU64::new(0),
            lns_marked: AtomicU64::new(0),
            lns_expired: AtomicU64::new(0),
            lnqueue_hits: AtomicU64::new(0),
            pending_lns_processed: AtomicU64::new(0),
            pending_lns_locked: AtomicU64::new(0),
            cluster_lns_processed: AtomicU64::new(0),
            marked_lns_processed: AtomicU64::new(0),
            to_be_cleaned_lns_processed: AtomicU64::new(0),
            bin_deltas_cleaned: AtomicU64::new(0),
            bin_deltas_dead: AtomicU64::new(0),
            bin_deltas_migrated: AtomicU64::new(0),
            bin_deltas_obsolete: AtomicU64::new(0),
            pending_ln_queue_size: AtomicU64::new(0),
            total_log_size: AtomicU64::new(0),
            active_log_size: AtomicU64::new(0),
            reserved_log_size: AtomicU64::new(0),
            protected_log_size: AtomicU64::new(0),
            available_log_size: AtomicU64::new(0),
            min_utilization: AtomicU64::new(0),
            max_utilization: AtomicU64::new(0),
        }
    }

    /// Resets all statistics counters to zero.
    pub fn reset(&self) {
        self.runs.store(0, Ordering::Relaxed);
        self.two_pass_runs.store(0, Ordering::Relaxed);
        self.revisal_runs.store(0, Ordering::Relaxed);
        self.deletions.store(0, Ordering::Relaxed);
        self.entries_read.store(0, Ordering::Relaxed);
        self.disk_reads.store(0, Ordering::Relaxed);
        self.ins_cleaned.store(0, Ordering::Relaxed);
        self.ins_dead.store(0, Ordering::Relaxed);
        self.ins_migrated.store(0, Ordering::Relaxed);
        self.ins_obsolete.store(0, Ordering::Relaxed);
        self.lns_cleaned.store(0, Ordering::Relaxed);
        self.lns_dead.store(0, Ordering::Relaxed);
        self.lns_migrated.store(0, Ordering::Relaxed);
        self.lns_obsolete.store(0, Ordering::Relaxed);
        self.lns_locked.store(0, Ordering::Relaxed);
        self.lns_marked.store(0, Ordering::Relaxed);
        self.lns_expired.store(0, Ordering::Relaxed);
        self.lnqueue_hits.store(0, Ordering::Relaxed);
        self.pending_lns_processed.store(0, Ordering::Relaxed);
        self.pending_lns_locked.store(0, Ordering::Relaxed);
        self.cluster_lns_processed.store(0, Ordering::Relaxed);
        self.marked_lns_processed.store(0, Ordering::Relaxed);
        self.to_be_cleaned_lns_processed.store(0, Ordering::Relaxed);
        self.bin_deltas_cleaned.store(0, Ordering::Relaxed);
        self.bin_deltas_dead.store(0, Ordering::Relaxed);
        self.bin_deltas_migrated.store(0, Ordering::Relaxed);
        self.bin_deltas_obsolete.store(0, Ordering::Relaxed);
        self.pending_ln_queue_size.store(0, Ordering::Relaxed);
        self.total_log_size.store(0, Ordering::Relaxed);
        self.active_log_size.store(0, Ordering::Relaxed);
        self.reserved_log_size.store(0, Ordering::Relaxed);
        self.protected_log_size.store(0, Ordering::Relaxed);
        self.available_log_size.store(0, Ordering::Relaxed);
        self.min_utilization.store(0, Ordering::Relaxed);
        self.max_utilization.store(0, Ordering::Relaxed);
    }

    /// Creates a non-atomic snapshot of the current statistics.
    pub fn snapshot(&self) -> CleanerStatsSnapshot {
        CleanerStatsSnapshot {
            runs: self.runs.load(Ordering::Relaxed),
            two_pass_runs: self.two_pass_runs.load(Ordering::Relaxed),
            revisal_runs: self.revisal_runs.load(Ordering::Relaxed),
            deletions: self.deletions.load(Ordering::Relaxed),
            entries_read: self.entries_read.load(Ordering::Relaxed),
            disk_reads: self.disk_reads.load(Ordering::Relaxed),
            ins_cleaned: self.ins_cleaned.load(Ordering::Relaxed),
            ins_dead: self.ins_dead.load(Ordering::Relaxed),
            ins_migrated: self.ins_migrated.load(Ordering::Relaxed),
            ins_obsolete: self.ins_obsolete.load(Ordering::Relaxed),
            lns_cleaned: self.lns_cleaned.load(Ordering::Relaxed),
            lns_dead: self.lns_dead.load(Ordering::Relaxed),
            lns_migrated: self.lns_migrated.load(Ordering::Relaxed),
            lns_obsolete: self.lns_obsolete.load(Ordering::Relaxed),
            lns_locked: self.lns_locked.load(Ordering::Relaxed),
            lns_marked: self.lns_marked.load(Ordering::Relaxed),
            lns_expired: self.lns_expired.load(Ordering::Relaxed),
            lnqueue_hits: self.lnqueue_hits.load(Ordering::Relaxed),
            pending_lns_processed: self
                .pending_lns_processed
                .load(Ordering::Relaxed),
            pending_lns_locked: self.pending_lns_locked.load(Ordering::Relaxed),
            cluster_lns_processed: self
                .cluster_lns_processed
                .load(Ordering::Relaxed),
            marked_lns_processed: self
                .marked_lns_processed
                .load(Ordering::Relaxed),
            to_be_cleaned_lns_processed: self
                .to_be_cleaned_lns_processed
                .load(Ordering::Relaxed),
            bin_deltas_cleaned: self.bin_deltas_cleaned.load(Ordering::Relaxed),
            bin_deltas_dead: self.bin_deltas_dead.load(Ordering::Relaxed),
            bin_deltas_migrated: self
                .bin_deltas_migrated
                .load(Ordering::Relaxed),
            bin_deltas_obsolete: self
                .bin_deltas_obsolete
                .load(Ordering::Relaxed),
            pending_ln_queue_size: self
                .pending_ln_queue_size
                .load(Ordering::Relaxed),
            total_log_size: self.total_log_size.load(Ordering::Relaxed),
            active_log_size: self.active_log_size.load(Ordering::Relaxed),
            reserved_log_size: self.reserved_log_size.load(Ordering::Relaxed),
            protected_log_size: self.protected_log_size.load(Ordering::Relaxed),
            available_log_size: self.available_log_size.load(Ordering::Relaxed),
            min_utilization: self.min_utilization.load(Ordering::Relaxed),
            max_utilization: self.max_utilization.load(Ordering::Relaxed),
        }
    }
}

impl Default for CleanerStats {
    fn default() -> Self {
        Self::new()
    }
}

/// A non-atomic snapshot of cleaner statistics.
///
/// Useful for reporting and persistence without holding locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CleanerStatsSnapshot {
    pub runs: u64,
    pub two_pass_runs: u64,
    pub revisal_runs: u64,
    pub deletions: u64,
    pub entries_read: u64,
    pub disk_reads: u64,
    pub ins_cleaned: u64,
    pub ins_dead: u64,
    pub ins_migrated: u64,
    pub ins_obsolete: u64,
    pub lns_cleaned: u64,
    pub lns_dead: u64,
    pub lns_migrated: u64,
    pub lns_obsolete: u64,
    pub lns_locked: u64,
    pub lns_marked: u64,
    pub lns_expired: u64,
    pub lnqueue_hits: u64,
    pub pending_lns_processed: u64,
    pub pending_lns_locked: u64,
    pub cluster_lns_processed: u64,
    pub marked_lns_processed: u64,
    pub to_be_cleaned_lns_processed: u64,
    pub bin_deltas_cleaned: u64,
    pub bin_deltas_dead: u64,
    pub bin_deltas_migrated: u64,
    pub bin_deltas_obsolete: u64,
    pub pending_ln_queue_size: u64,
    pub total_log_size: u64,
    pub active_log_size: u64,
    pub reserved_log_size: u64,
    pub protected_log_size: u64,
    pub available_log_size: u64,
    pub min_utilization: u64,
    pub max_utilization: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats_all_zero() {
        let stats = CleanerStats::new();
        let snap = stats.snapshot();
        assert_eq!(snap.runs, 0);
        assert_eq!(snap.entries_read, 0);
        assert_eq!(snap.lns_migrated, 0);
        assert_eq!(snap.total_log_size, 0);
    }

    #[test]
    fn test_increment_counters() {
        let stats = CleanerStats::new();

        stats.runs.fetch_add(1, Ordering::Relaxed);
        stats.lns_cleaned.fetch_add(100, Ordering::Relaxed);
        stats.lns_migrated.fetch_add(50, Ordering::Relaxed);

        let snap = stats.snapshot();
        assert_eq!(snap.runs, 1);
        assert_eq!(snap.lns_cleaned, 100);
        assert_eq!(snap.lns_migrated, 50);
    }

    #[test]
    fn test_reset() {
        let stats = CleanerStats::new();

        stats.runs.fetch_add(5, Ordering::Relaxed);
        stats.entries_read.fetch_add(1000, Ordering::Relaxed);
        stats.deletions.fetch_add(3, Ordering::Relaxed);

        stats.reset();

        let snap = stats.snapshot();
        assert_eq!(snap.runs, 0);
        assert_eq!(snap.entries_read, 0);
        assert_eq!(snap.deletions, 0);
    }

    #[test]
    fn test_all_fields_tracked() {
        let stats = CleanerStats::new();

        // Increment all fields
        stats.runs.fetch_add(1, Ordering::Relaxed);
        stats.two_pass_runs.fetch_add(1, Ordering::Relaxed);
        stats.revisal_runs.fetch_add(1, Ordering::Relaxed);
        stats.deletions.fetch_add(1, Ordering::Relaxed);
        stats.entries_read.fetch_add(1, Ordering::Relaxed);
        stats.disk_reads.fetch_add(1, Ordering::Relaxed);
        stats.ins_cleaned.fetch_add(1, Ordering::Relaxed);
        stats.ins_dead.fetch_add(1, Ordering::Relaxed);
        stats.ins_migrated.fetch_add(1, Ordering::Relaxed);
        stats.ins_obsolete.fetch_add(1, Ordering::Relaxed);
        stats.lns_cleaned.fetch_add(1, Ordering::Relaxed);
        stats.lns_dead.fetch_add(1, Ordering::Relaxed);
        stats.lns_migrated.fetch_add(1, Ordering::Relaxed);
        stats.lns_obsolete.fetch_add(1, Ordering::Relaxed);
        stats.lns_locked.fetch_add(1, Ordering::Relaxed);
        stats.lns_marked.fetch_add(1, Ordering::Relaxed);
        stats.lns_expired.fetch_add(1, Ordering::Relaxed);
        stats.lnqueue_hits.fetch_add(1, Ordering::Relaxed);
        stats.pending_lns_processed.fetch_add(1, Ordering::Relaxed);
        stats.pending_lns_locked.fetch_add(1, Ordering::Relaxed);
        stats.cluster_lns_processed.fetch_add(1, Ordering::Relaxed);
        stats.marked_lns_processed.fetch_add(1, Ordering::Relaxed);
        stats.to_be_cleaned_lns_processed.fetch_add(1, Ordering::Relaxed);
        stats.bin_deltas_cleaned.fetch_add(1, Ordering::Relaxed);
        stats.bin_deltas_dead.fetch_add(1, Ordering::Relaxed);
        stats.bin_deltas_migrated.fetch_add(1, Ordering::Relaxed);
        stats.bin_deltas_obsolete.fetch_add(1, Ordering::Relaxed);
        stats.pending_ln_queue_size.fetch_add(1, Ordering::Relaxed);

        let snap = stats.snapshot();
        assert_eq!(snap.runs, 1);
        assert_eq!(snap.two_pass_runs, 1);
        assert_eq!(snap.revisal_runs, 1);
        assert_eq!(snap.pending_ln_queue_size, 1);
    }

    #[test]
    fn test_disk_usage_stats() {
        let stats = CleanerStats::new();

        stats.total_log_size.store(1000000, Ordering::Relaxed);
        stats.active_log_size.store(700000, Ordering::Relaxed);
        stats.reserved_log_size.store(300000, Ordering::Relaxed);
        stats.protected_log_size.store(100000, Ordering::Relaxed);
        stats.available_log_size.store(500000, Ordering::Relaxed);

        let snap = stats.snapshot();
        assert_eq!(snap.total_log_size, 1000000);
        assert_eq!(snap.active_log_size, 700000);
        assert_eq!(snap.reserved_log_size, 300000);
        assert_eq!(snap.protected_log_size, 100000);
        assert_eq!(snap.available_log_size, 500000);
    }

    #[test]
    fn test_utilization_stats() {
        let stats = CleanerStats::new();

        stats.min_utilization.store(25, Ordering::Relaxed);
        stats.max_utilization.store(95, Ordering::Relaxed);

        let snap = stats.snapshot();
        assert_eq!(snap.min_utilization, 25);
        assert_eq!(snap.max_utilization, 95);
    }

    #[test]
    fn test_snapshot_consistency() {
        let stats = CleanerStats::new();

        stats.lns_cleaned.store(100, Ordering::Relaxed);
        stats.lns_migrated.store(80, Ordering::Relaxed);
        stats.lns_dead.store(20, Ordering::Relaxed);

        let snap1 = stats.snapshot();
        let snap2 = stats.snapshot();

        assert_eq!(snap1, snap2);
    }

    #[test]
    fn test_default() {
        let stats = CleanerStats::default();
        let snap = stats.snapshot();
        assert_eq!(snap.runs, 0);
        assert_eq!(snap.lns_cleaned, 0);
    }
}
