//! Replication statistics.
//!
//! Port of `com.sleepycat.je.rep.ReplicatedEnvironmentStats`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics for replication operations.
pub struct RepStats {
    // Election stats
    pub elections_held: AtomicU64,
    pub elections_won: AtomicU64,
    pub elections_lost: AtomicU64,

    // Feeder stats
    pub feeders_created: AtomicU64,
    pub feeders_shutdown: AtomicU64,

    // Ack stats
    pub acks_received: AtomicU64,
    pub ack_timeouts: AtomicU64,

    // Replication stream stats
    pub entries_replicated: AtomicU64,
    pub entries_applied: AtomicU64,
    pub bytes_replicated: AtomicU64,

    // Lag stats
    pub max_replica_lag_ms: AtomicU64,
}

impl RepStats {
    /// Creates a new stats instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            elections_held: AtomicU64::new(0),
            elections_won: AtomicU64::new(0),
            elections_lost: AtomicU64::new(0),
            feeders_created: AtomicU64::new(0),
            feeders_shutdown: AtomicU64::new(0),
            acks_received: AtomicU64::new(0),
            ack_timeouts: AtomicU64::new(0),
            entries_replicated: AtomicU64::new(0),
            entries_applied: AtomicU64::new(0),
            bytes_replicated: AtomicU64::new(0),
            max_replica_lag_ms: AtomicU64::new(0),
        }
    }

    pub fn increment_elections_held(&self) {
        self.elections_held.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_elections_won(&self) {
        self.elections_won.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_elections_lost(&self) {
        self.elections_lost.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_entries_replicated(&self, count: u64) {
        self.entries_replicated.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_bytes_replicated(&self, bytes: u64) {
        self.bytes_replicated.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_entries_applied(&self, count: u64) {
        self.entries_applied.fetch_add(count, Ordering::Relaxed);
    }

    pub fn update_max_lag(&self, lag_ms: u64) {
        self.max_replica_lag_ms.fetch_max(lag_ms, Ordering::Relaxed);
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.elections_held.store(0, Ordering::Relaxed);
        self.elections_won.store(0, Ordering::Relaxed);
        self.elections_lost.store(0, Ordering::Relaxed);
        self.feeders_created.store(0, Ordering::Relaxed);
        self.feeders_shutdown.store(0, Ordering::Relaxed);
        self.acks_received.store(0, Ordering::Relaxed);
        self.ack_timeouts.store(0, Ordering::Relaxed);
        self.entries_replicated.store(0, Ordering::Relaxed);
        self.entries_applied.store(0, Ordering::Relaxed);
        self.bytes_replicated.store(0, Ordering::Relaxed);
        self.max_replica_lag_ms.store(0, Ordering::Relaxed);
    }

    /// Get a snapshot of all stats as a formatted string.
    pub fn summary(&self) -> String {
        format!(
            "RepStats {{ elections: held={} won={} lost={}, \
             feeders: created={} shutdown={}, \
             acks: received={} timeouts={}, \
             stream: replicated={} applied={} bytes={}, \
             max_lag_ms={} }}",
            self.elections_held.load(Ordering::Relaxed),
            self.elections_won.load(Ordering::Relaxed),
            self.elections_lost.load(Ordering::Relaxed),
            self.feeders_created.load(Ordering::Relaxed),
            self.feeders_shutdown.load(Ordering::Relaxed),
            self.acks_received.load(Ordering::Relaxed),
            self.ack_timeouts.load(Ordering::Relaxed),
            self.entries_replicated.load(Ordering::Relaxed),
            self.entries_applied.load(Ordering::Relaxed),
            self.bytes_replicated.load(Ordering::Relaxed),
            self.max_replica_lag_ms.load(Ordering::Relaxed),
        )
    }
}

impl Default for RepStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats_are_zero() {
        let stats = RepStats::new();
        assert_eq!(stats.elections_held.load(Ordering::Relaxed), 0);
        assert_eq!(stats.entries_replicated.load(Ordering::Relaxed), 0);
        assert_eq!(stats.max_replica_lag_ms.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_increment_elections() {
        let stats = RepStats::new();
        stats.increment_elections_held();
        stats.increment_elections_held();
        stats.increment_elections_won();
        stats.increment_elections_lost();
        assert_eq!(stats.elections_held.load(Ordering::Relaxed), 2);
        assert_eq!(stats.elections_won.load(Ordering::Relaxed), 1);
        assert_eq!(stats.elections_lost.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_add_entries() {
        let stats = RepStats::new();
        stats.add_entries_replicated(10);
        stats.add_entries_replicated(5);
        stats.add_entries_applied(8);
        stats.add_bytes_replicated(1024);
        assert_eq!(stats.entries_replicated.load(Ordering::Relaxed), 15);
        assert_eq!(stats.entries_applied.load(Ordering::Relaxed), 8);
        assert_eq!(stats.bytes_replicated.load(Ordering::Relaxed), 1024);
    }

    #[test]
    fn test_update_max_lag() {
        let stats = RepStats::new();
        stats.update_max_lag(100);
        assert_eq!(stats.max_replica_lag_ms.load(Ordering::Relaxed), 100);
        stats.update_max_lag(50); // should not decrease
        assert_eq!(stats.max_replica_lag_ms.load(Ordering::Relaxed), 100);
        stats.update_max_lag(200);
        assert_eq!(stats.max_replica_lag_ms.load(Ordering::Relaxed), 200);
    }

    #[test]
    fn test_reset() {
        let stats = RepStats::new();
        stats.increment_elections_held();
        stats.add_entries_replicated(100);
        stats.update_max_lag(500);
        stats.reset();
        assert_eq!(stats.elections_held.load(Ordering::Relaxed), 0);
        assert_eq!(stats.entries_replicated.load(Ordering::Relaxed), 0);
        assert_eq!(stats.max_replica_lag_ms.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_summary() {
        let stats = RepStats::new();
        stats.increment_elections_held();
        stats.add_entries_replicated(42);
        let summary = stats.summary();
        assert!(summary.contains("held=1"));
        assert!(summary.contains("replicated=42"));
    }

    #[test]
    fn test_default() {
        let stats = RepStats::default();
        assert_eq!(stats.elections_held.load(Ordering::Relaxed), 0);
    }
}
