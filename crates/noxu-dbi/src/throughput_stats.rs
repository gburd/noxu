//! Throughput statistics for database operations.
//!
//! Tracks per-database operation counts (inserts, updates, deletes, searches,
//! cursor positions) using lock-free atomics so that CursorImpl can increment
//! them on the hot path without acquiring any mutex.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Per-database operation throughput counters.
///
/// Mirrors JE DbiStatDefinition THROUGHPUT_PRI_* statistics.
/// Shared across all CursorImpl instances for a single database via Arc.
#[derive(Debug, Default)]
pub struct ThroughputStats {
    /// Successful primary-database insert operations.
    pub n_pri_inserts: AtomicU64,
    /// Failed primary-database insert operations (key already exists).
    pub n_pri_insert_fails: AtomicU64,
    /// Successful primary-database update operations.
    pub n_pri_updates: AtomicU64,
    /// Successful primary-database delete operations.
    pub n_pri_deletes: AtomicU64,
    /// Failed primary-database delete operations (key not found).
    pub n_pri_delete_fails: AtomicU64,
    /// Successful primary-database search operations.
    pub n_pri_searches: AtomicU64,
    /// Failed primary-database search operations (key not found).
    pub n_pri_search_fails: AtomicU64,
    /// Primary-database cursor position operations (get_next, get_prev, etc.).
    pub n_pri_positions: AtomicU64,

    // ── Secondary-index operations ──────────────────────────────────────────
    /// Successful secondary-index insert operations.
    pub n_sec_inserts: AtomicU64,
    /// Failed secondary-index insert operations (key already exists).
    pub n_sec_insert_fails: AtomicU64,
    /// Successful secondary-index update operations.
    pub n_sec_updates: AtomicU64,
    /// Successful secondary-index delete operations.
    pub n_sec_deletes: AtomicU64,
    /// Failed secondary-index delete operations (key not found).
    pub n_sec_delete_fails: AtomicU64,
    /// Successful secondary-index search operations.
    pub n_sec_searches: AtomicU64,
    /// Failed secondary-index search operations (key not found).
    pub n_sec_search_fails: AtomicU64,
    /// Secondary-index cursor position operations.
    pub n_sec_positions: AtomicU64,
}

impl ThroughputStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Returns a point-in-time snapshot.
    pub fn snapshot(&self) -> ThroughputStatsSnapshot {
        ThroughputStatsSnapshot {
            n_pri_inserts: self.n_pri_inserts.load(Ordering::Relaxed),
            n_pri_insert_fails: self.n_pri_insert_fails.load(Ordering::Relaxed),
            n_pri_updates: self.n_pri_updates.load(Ordering::Relaxed),
            n_pri_deletes: self.n_pri_deletes.load(Ordering::Relaxed),
            n_pri_delete_fails: self.n_pri_delete_fails.load(Ordering::Relaxed),
            n_pri_searches: self.n_pri_searches.load(Ordering::Relaxed),
            n_pri_search_fails: self.n_pri_search_fails.load(Ordering::Relaxed),
            n_pri_positions: self.n_pri_positions.load(Ordering::Relaxed),
            n_sec_inserts: self.n_sec_inserts.load(Ordering::Relaxed),
            n_sec_insert_fails: self.n_sec_insert_fails.load(Ordering::Relaxed),
            n_sec_updates: self.n_sec_updates.load(Ordering::Relaxed),
            n_sec_deletes: self.n_sec_deletes.load(Ordering::Relaxed),
            n_sec_delete_fails: self.n_sec_delete_fails.load(Ordering::Relaxed),
            n_sec_searches: self.n_sec_searches.load(Ordering::Relaxed),
            n_sec_search_fails: self.n_sec_search_fails.load(Ordering::Relaxed),
            n_sec_positions: self.n_sec_positions.load(Ordering::Relaxed),
        }
    }

    /// Adds another snapshot's counts into this snapshot (for aggregation).
    pub fn add_snapshot(&self, other: &ThroughputStatsSnapshot) {
        self.n_pri_inserts.fetch_add(other.n_pri_inserts, Ordering::Relaxed);
        self.n_pri_insert_fails.fetch_add(other.n_pri_insert_fails, Ordering::Relaxed);
        self.n_pri_updates.fetch_add(other.n_pri_updates, Ordering::Relaxed);
        self.n_pri_deletes.fetch_add(other.n_pri_deletes, Ordering::Relaxed);
        self.n_pri_delete_fails.fetch_add(other.n_pri_delete_fails, Ordering::Relaxed);
        self.n_pri_searches.fetch_add(other.n_pri_searches, Ordering::Relaxed);
        self.n_pri_search_fails.fetch_add(other.n_pri_search_fails, Ordering::Relaxed);
        self.n_pri_positions.fetch_add(other.n_pri_positions, Ordering::Relaxed);
        self.n_sec_inserts.fetch_add(other.n_sec_inserts, Ordering::Relaxed);
        self.n_sec_insert_fails.fetch_add(other.n_sec_insert_fails, Ordering::Relaxed);
        self.n_sec_updates.fetch_add(other.n_sec_updates, Ordering::Relaxed);
        self.n_sec_deletes.fetch_add(other.n_sec_deletes, Ordering::Relaxed);
        self.n_sec_delete_fails.fetch_add(other.n_sec_delete_fails, Ordering::Relaxed);
        self.n_sec_searches.fetch_add(other.n_sec_searches, Ordering::Relaxed);
        self.n_sec_search_fails.fetch_add(other.n_sec_search_fails, Ordering::Relaxed);
        self.n_sec_positions.fetch_add(other.n_sec_positions, Ordering::Relaxed);
    }
}

/// Point-in-time snapshot of throughput counters (all u64, no atomics).
#[derive(Debug, Clone, Default)]
pub struct ThroughputStatsSnapshot {
    pub n_pri_inserts: u64,
    pub n_pri_insert_fails: u64,
    pub n_pri_updates: u64,
    pub n_pri_deletes: u64,
    pub n_pri_delete_fails: u64,
    pub n_pri_searches: u64,
    pub n_pri_search_fails: u64,
    pub n_pri_positions: u64,
    pub n_sec_inserts: u64,
    pub n_sec_insert_fails: u64,
    pub n_sec_updates: u64,
    pub n_sec_deletes: u64,
    pub n_sec_delete_fails: u64,
    pub n_sec_searches: u64,
    pub n_sec_search_fails: u64,
    pub n_sec_positions: u64,
}

impl ThroughputStatsSnapshot {
    /// Adds counts from another snapshot into self.
    pub fn add(&mut self, other: &ThroughputStatsSnapshot) {
        self.n_pri_inserts += other.n_pri_inserts;
        self.n_pri_insert_fails += other.n_pri_insert_fails;
        self.n_pri_updates += other.n_pri_updates;
        self.n_pri_deletes += other.n_pri_deletes;
        self.n_pri_delete_fails += other.n_pri_delete_fails;
        self.n_pri_searches += other.n_pri_searches;
        self.n_pri_search_fails += other.n_pri_search_fails;
        self.n_pri_positions += other.n_pri_positions;
        self.n_sec_inserts += other.n_sec_inserts;
        self.n_sec_insert_fails += other.n_sec_insert_fails;
        self.n_sec_updates += other.n_sec_updates;
        self.n_sec_deletes += other.n_sec_deletes;
        self.n_sec_delete_fails += other.n_sec_delete_fails;
        self.n_sec_searches += other.n_sec_searches;
        self.n_sec_search_fails += other.n_sec_search_fails;
        self.n_sec_positions += other.n_sec_positions;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_default_all_zero() {
        let s = ThroughputStats::new();
        let snap = s.snapshot();
        assert_eq!(snap.n_pri_inserts, 0);
        assert_eq!(snap.n_pri_searches, 0);
        assert_eq!(snap.n_pri_positions, 0);
    }

    #[test]
    fn test_increment_and_snapshot() {
        let s = ThroughputStats::new();
        s.n_pri_inserts.fetch_add(10, Ordering::Relaxed);
        s.n_pri_search_fails.fetch_add(3, Ordering::Relaxed);
        let snap = s.snapshot();
        assert_eq!(snap.n_pri_inserts, 10);
        assert_eq!(snap.n_pri_search_fails, 3);
    }

    #[test]
    fn test_snapshot_add() {
        let mut acc = ThroughputStatsSnapshot::default();
        let s1 = ThroughputStatsSnapshot { n_pri_inserts: 5, n_pri_searches: 20, ..Default::default() };
        let s2 = ThroughputStatsSnapshot { n_pri_inserts: 3, n_pri_searches: 10, ..Default::default() };
        acc.add(&s1);
        acc.add(&s2);
        assert_eq!(acc.n_pri_inserts, 8);
        assert_eq!(acc.n_pri_searches, 30);
    }

    #[test]
    fn test_sec_counters() {
        let s = ThroughputStats::new();
        s.n_sec_inserts.fetch_add(10, Ordering::Relaxed);
        s.n_sec_searches.fetch_add(50, Ordering::Relaxed);
        s.n_sec_search_fails.fetch_add(2, Ordering::Relaxed);
        let snap = s.snapshot();
        assert_eq!(snap.n_sec_inserts, 10);
        assert_eq!(snap.n_sec_searches, 50);
        assert_eq!(snap.n_sec_search_fails, 2);
        assert_eq!(snap.n_sec_deletes, 0);
    }

    #[test]
    fn test_add_snapshot_includes_sec() {
        let acc = ThroughputStats::new();
        let other = ThroughputStatsSnapshot {
            n_sec_updates: 7,
            n_sec_positions: 3,
            ..Default::default()
        };
        acc.add_snapshot(&other);
        let snap = acc.snapshot();
        assert_eq!(snap.n_sec_updates, 7);
        assert_eq!(snap.n_sec_positions, 3);
    }

    #[test]
    fn test_sec_snapshot_add() {
        let mut acc = ThroughputStatsSnapshot::default();
        let s = ThroughputStatsSnapshot {
            n_sec_inserts: 4, n_sec_deletes: 2, ..Default::default()
        };
        acc.add(&s);
        assert_eq!(acc.n_sec_inserts, 4);
        assert_eq!(acc.n_sec_deletes, 2);
    }
}
