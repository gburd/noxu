//! Lock statistics definitions.
//!
//! Port of `com.sleepycat.je.txn.LockStatDefinition`.

/// Lock statistics counters.
///
/// Tracks lock manager performance and usage metrics.
///
/// Port of `com.sleepycat.je.txn.LockStatDefinition` and `com.sleepycat.je.LockStats`.
#[derive(Debug, Default, Clone)]
pub struct LockStats {
    /// Total number of lock requests.
    pub lock_requests: u64,

    /// Number of lock requests that had to wait.
    pub lock_waits: u64,

    /// Current number of lock owners across all locks.
    pub n_owners: u64,

    /// Current number of lock waiters across all locks.
    pub n_waiters: u64,

    /// Total number of locks currently held.
    pub n_total_locks: u64,

    /// Number of read locks currently held.
    pub n_read_locks: u64,

    /// Number of write locks currently held.
    pub n_write_locks: u64,
}

impl LockStats {
    /// Creates a new LockStats with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all statistics to zero.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Returns the percentage of lock requests that had to wait.
    pub fn wait_percentage(&self) -> f64 {
        if self.lock_requests == 0 {
            0.0
        } else {
            (self.lock_waits as f64 / self.lock_requests as f64) * 100.0
        }
    }

    /// Adds the counts from another LockStats to this one.
    pub fn add(&mut self, other: &LockStats) {
        self.lock_requests += other.lock_requests;
        self.lock_waits += other.lock_waits;
        self.n_owners += other.n_owners;
        self.n_waiters += other.n_waiters;
        self.n_total_locks += other.n_total_locks;
        self.n_read_locks += other.n_read_locks;
        self.n_write_locks += other.n_write_locks;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let stats = LockStats::new();
        assert_eq!(stats.lock_requests, 0);
        assert_eq!(stats.lock_waits, 0);
        assert_eq!(stats.n_owners, 0);
        assert_eq!(stats.n_waiters, 0);
        assert_eq!(stats.n_total_locks, 0);
        assert_eq!(stats.n_read_locks, 0);
        assert_eq!(stats.n_write_locks, 0);
    }

    #[test]
    fn test_reset() {
        let mut stats = LockStats::new();
        stats.lock_requests = 100;
        stats.lock_waits = 10;
        stats.n_owners = 5;

        stats.reset();
        assert_eq!(stats.lock_requests, 0);
        assert_eq!(stats.lock_waits, 0);
        assert_eq!(stats.n_owners, 0);
    }

    #[test]
    fn test_wait_percentage() {
        let mut stats = LockStats::new();
        assert_eq!(stats.wait_percentage(), 0.0);

        stats.lock_requests = 100;
        stats.lock_waits = 10;
        assert_eq!(stats.wait_percentage(), 10.0);

        stats.lock_requests = 200;
        stats.lock_waits = 50;
        assert_eq!(stats.wait_percentage(), 25.0);
    }

    #[test]
    fn test_add() {
        let mut stats1 = LockStats::new();
        stats1.lock_requests = 100;
        stats1.lock_waits = 10;
        stats1.n_owners = 5;
        stats1.n_waiters = 2;
        stats1.n_total_locks = 50;
        stats1.n_read_locks = 30;
        stats1.n_write_locks = 20;

        let mut stats2 = LockStats::new();
        stats2.lock_requests = 50;
        stats2.lock_waits = 5;
        stats2.n_owners = 3;
        stats2.n_waiters = 1;
        stats2.n_total_locks = 25;
        stats2.n_read_locks = 15;
        stats2.n_write_locks = 10;

        stats1.add(&stats2);
        assert_eq!(stats1.lock_requests, 150);
        assert_eq!(stats1.lock_waits, 15);
        assert_eq!(stats1.n_owners, 8);
        assert_eq!(stats1.n_waiters, 3);
        assert_eq!(stats1.n_total_locks, 75);
        assert_eq!(stats1.n_read_locks, 45);
        assert_eq!(stats1.n_write_locks, 30);
    }
}
