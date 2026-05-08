//! Checkpoint statistics.
//!

use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics for checkpoint operations.
///
/// Tracks various metrics about checkpoint execution including the number of
/// checkpoints performed, nodes flushed, and timing information.
///
/// 
#[derive(Debug)]
pub struct CheckpointStats {
    /// Total number of checkpoints performed.
    pub checkpoints: AtomicU64,

    /// Number of full INs flushed during checkpoints.
    pub full_in_flush: AtomicU64,

    /// Number of full BINs flushed during checkpoints.
    pub full_bin_flush: AtomicU64,

    /// Number of BIN-deltas flushed during checkpoints.
    pub delta_in_flush: AtomicU64,

    /// ID of the last checkpoint.
    pub last_ckpt_id: AtomicU64,

    /// LSN of the last checkpoint start (as u64).
    pub last_ckpt_start: AtomicU64,

    /// LSN of the last checkpoint end (as u64).
    pub last_ckpt_end: AtomicU64,

    /// Interval between last two checkpoints in milliseconds.
    pub last_ckpt_interval: AtomicU64,
}

impl CheckpointStats {
    /// Creates a new CheckpointStats with all counters initialized to zero.
    pub fn new() -> Self {
        Self {
            checkpoints: AtomicU64::new(0),
            full_in_flush: AtomicU64::new(0),
            full_bin_flush: AtomicU64::new(0),
            delta_in_flush: AtomicU64::new(0),
            last_ckpt_id: AtomicU64::new(0),
            last_ckpt_start: AtomicU64::new(0),
            last_ckpt_end: AtomicU64::new(0),
            last_ckpt_interval: AtomicU64::new(0),
        }
    }

    /// Resets all statistics to zero.
    pub fn reset(&self) {
        self.checkpoints.store(0, Ordering::Relaxed);
        self.full_in_flush.store(0, Ordering::Relaxed);
        self.full_bin_flush.store(0, Ordering::Relaxed);
        self.delta_in_flush.store(0, Ordering::Relaxed);
        self.last_ckpt_id.store(0, Ordering::Relaxed);
        self.last_ckpt_start.store(0, Ordering::Relaxed);
        self.last_ckpt_end.store(0, Ordering::Relaxed);
        self.last_ckpt_interval.store(0, Ordering::Relaxed);
    }

    /// Takes a snapshot of the current statistics.
    ///
    /// Returns a struct containing the current values of all counters.
    pub fn snapshot(&self) -> CheckpointStatsSnapshot {
        CheckpointStatsSnapshot {
            checkpoints: self.checkpoints.load(Ordering::Relaxed),
            full_in_flush: self.full_in_flush.load(Ordering::Relaxed),
            full_bin_flush: self.full_bin_flush.load(Ordering::Relaxed),
            delta_in_flush: self.delta_in_flush.load(Ordering::Relaxed),
            last_ckpt_id: self.last_ckpt_id.load(Ordering::Relaxed),
            last_ckpt_start: self.last_ckpt_start.load(Ordering::Relaxed),
            last_ckpt_end: self.last_ckpt_end.load(Ordering::Relaxed),
            last_ckpt_interval: self.last_ckpt_interval.load(Ordering::Relaxed),
        }
    }
}

impl Default for CheckpointStats {
    fn default() -> Self {
        Self::new()
    }
}

/// A snapshot of checkpoint statistics at a point in time.
///
/// This struct contains copies of the atomic counters for safe reading
/// without worrying about concurrent modifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointStatsSnapshot {
    pub checkpoints: u64,
    pub full_in_flush: u64,
    pub full_bin_flush: u64,
    pub delta_in_flush: u64,
    pub last_ckpt_id: u64,
    pub last_ckpt_start: u64,
    pub last_ckpt_end: u64,
    pub last_ckpt_interval: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let stats = CheckpointStats::new();
        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 0);
        assert_eq!(stats.full_in_flush.load(Ordering::Relaxed), 0);
        assert_eq!(stats.full_bin_flush.load(Ordering::Relaxed), 0);
        assert_eq!(stats.delta_in_flush.load(Ordering::Relaxed), 0);
        assert_eq!(stats.last_ckpt_id.load(Ordering::Relaxed), 0);
        assert_eq!(stats.last_ckpt_start.load(Ordering::Relaxed), 0);
        assert_eq!(stats.last_ckpt_end.load(Ordering::Relaxed), 0);
        assert_eq!(stats.last_ckpt_interval.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_default() {
        let stats = CheckpointStats::default();
        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_increment() {
        let stats = CheckpointStats::new();

        stats.checkpoints.fetch_add(1, Ordering::Relaxed);
        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 1);

        stats.full_in_flush.fetch_add(5, Ordering::Relaxed);
        assert_eq!(stats.full_in_flush.load(Ordering::Relaxed), 5);

        stats.full_bin_flush.fetch_add(10, Ordering::Relaxed);
        assert_eq!(stats.full_bin_flush.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_reset() {
        let stats = CheckpointStats::new();

        stats.checkpoints.store(100, Ordering::Relaxed);
        stats.full_in_flush.store(200, Ordering::Relaxed);
        stats.full_bin_flush.store(300, Ordering::Relaxed);
        stats.delta_in_flush.store(400, Ordering::Relaxed);

        stats.reset();

        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 0);
        assert_eq!(stats.full_in_flush.load(Ordering::Relaxed), 0);
        assert_eq!(stats.full_bin_flush.load(Ordering::Relaxed), 0);
        assert_eq!(stats.delta_in_flush.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_snapshot() {
        let stats = CheckpointStats::new();

        stats.checkpoints.store(10, Ordering::Relaxed);
        stats.full_in_flush.store(20, Ordering::Relaxed);
        stats.full_bin_flush.store(30, Ordering::Relaxed);
        stats.delta_in_flush.store(40, Ordering::Relaxed);
        stats.last_ckpt_id.store(123, Ordering::Relaxed);
        stats.last_ckpt_start.store(1000, Ordering::Relaxed);
        stats.last_ckpt_end.store(2000, Ordering::Relaxed);
        stats.last_ckpt_interval.store(5000, Ordering::Relaxed);

        let snapshot = stats.snapshot();

        assert_eq!(snapshot.checkpoints, 10);
        assert_eq!(snapshot.full_in_flush, 20);
        assert_eq!(snapshot.full_bin_flush, 30);
        assert_eq!(snapshot.delta_in_flush, 40);
        assert_eq!(snapshot.last_ckpt_id, 123);
        assert_eq!(snapshot.last_ckpt_start, 1000);
        assert_eq!(snapshot.last_ckpt_end, 2000);
        assert_eq!(snapshot.last_ckpt_interval, 5000);
    }

    #[test]
    fn test_snapshot_independence() {
        let stats = CheckpointStats::new();

        stats.checkpoints.store(5, Ordering::Relaxed);
        let snapshot1 = stats.snapshot();

        stats.checkpoints.store(10, Ordering::Relaxed);
        let snapshot2 = stats.snapshot();

        // Snapshots should differ
        assert_eq!(snapshot1.checkpoints, 5);
        assert_eq!(snapshot2.checkpoints, 10);
    }

    #[test]
    fn test_last_checkpoint_info() {
        let stats = CheckpointStats::new();

        stats.last_ckpt_id.store(42, Ordering::Relaxed);
        stats.last_ckpt_start.store(0x0000000100000064, Ordering::Relaxed); // LSN(1, 100)
        stats.last_ckpt_end.store(0x00000001000000C8, Ordering::Relaxed); // LSN(1, 200)
        stats.last_ckpt_interval.store(60000, Ordering::Relaxed); // 60 seconds

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.last_ckpt_id, 42);
        assert_eq!(snapshot.last_ckpt_start, 0x0000000100000064);
        assert_eq!(snapshot.last_ckpt_end, 0x00000001000000C8);
        assert_eq!(snapshot.last_ckpt_interval, 60000);
    }

    #[test]
    fn test_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(CheckpointStats::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let stats_clone = Arc::clone(&stats);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    stats_clone.checkpoints.fetch_add(1, Ordering::Relaxed);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn test_snapshot_clone() {
        let stats = CheckpointStats::new();
        stats.checkpoints.store(100, Ordering::Relaxed);

        let snapshot1 = stats.snapshot();
        let snapshot2 = snapshot1;

        assert_eq!(snapshot1, snapshot2);
    }

    #[test]
    fn test_multiple_resets() {
        let stats = CheckpointStats::new();

        stats.checkpoints.store(100, Ordering::Relaxed);
        stats.reset();
        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 0);

        stats.checkpoints.store(200, Ordering::Relaxed);
        stats.reset();
        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 0);
    }
}
