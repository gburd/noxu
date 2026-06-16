//! TTL-aware expiration profile for log file utilization.
//!
//! Noxu's `ExpirationProfile` is a per-file histogram representing the
//! byte distribution of expiration times.  `ExpirationProfileStore` is an
//! in-memory map from file number to `ExpirationTracker`, used by the cleaner
//! to carry per-file expiration data between the two-pass dry run and the
//! subsequent file selection pass.
//!
//! **CLN-9 status**: the in-memory store is implemented here.  JE also
//! persists this data to the `FileSummaryDB` (a dedicated internal BTree
//! database) so that expiration data survives crashes.  The persistent
//! store is deferred (see CLN-11 / known-limitations.md) — it requires a
//! new internal database and recovery integration that is out of scope for
//! this pass.
//!
//! JE: `ExpirationProfile.java` (per-file histogram store, `putFile`,
//! `removeFile`, `getExpiredBytes`).

use crate::expiration_tracker::ExpirationTracker;
use std::collections::{BTreeMap, HashMap};

/// Tracks the distribution of expiration times for entries in a log file.
///
/// Used by the cleaner to predict when a file will become fully obsolete
/// due to TTL expiration, avoiding unnecessary migration work.
#[derive(Debug, Default, Clone)]
pub struct ExpirationProfile {
    /// Map from expiration_time bucket to byte count.
    /// Key: packed hours since epoch (same resolution as BinEntry.expiration_time).
    buckets: BTreeMap<u32, u64>,
    /// Total bytes tracked in this profile.
    total_bytes: u64,
}

impl ExpirationProfile {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `size` bytes that expire at `expiration_time` (0 = never).
    pub fn add(&mut self, expiration_time: u32, size: u64) {
        if expiration_time != 0 {
            *self.buckets.entry(expiration_time).or_default() += size;
        }
        self.total_bytes += size;
    }

    /// Returns true if all tracked bytes are predicted to be expired by `current_time`.
    pub fn is_all_expired(&self, current_time: u32) -> bool {
        if self.total_bytes == 0 {
            return true;
        }
        self.buckets.keys().all(|&t| t <= current_time)
    }

    /// Returns the fraction (0.0-1.0) of bytes predicted expired by `current_time`.
    pub fn expired_fraction(&self, current_time: u32) -> f64 {
        if self.total_bytes == 0 {
            return 1.0;
        }
        let expired: u64 = self
            .buckets
            .iter()
            .filter(|&(&t, _)| t <= current_time)
            .map(|(_, &b)| b)
            .sum();
        expired as f64 / self.total_bytes as f64
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

/// In-memory store mapping file numbers to their `ExpirationTracker` data.
///
/// CLN-9: this is the Noxu equivalent of JE's per-file histogram store in
/// `ExpirationProfile.java`.  Each entry represents the expiration-time
/// distribution of LN records written to a specific log file.
///
/// **Usage**:
/// - `put_file`: called after a two-pass dry-run with the resulting tracker.
/// - `remove_file`: called when a file is cleaned or deleted.
/// - `get_expired_bytes`: returns expired bytes for a file at a given time
///   (hours since epoch).
///
/// **Limitations**: in-memory only; does not survive crashes.  JE persists
/// this to `FileSummaryDB`.  See CLN-11 and `docs/src/operations/
/// known-limitations.md` for the deferral rationale.
///
/// JE: `ExpirationProfile.putFile` / `removeFile` / `getExpiredBytes`.
#[derive(Debug, Default)]
pub struct ExpirationProfileStore {
    /// Map of file_number → ExpirationTracker.
    trackers: HashMap<u32, ExpirationTracker>,
}

impl ExpirationProfileStore {
    /// Creates a new empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the expiration data for `file_number` from an `ExpirationTracker`.
    ///
    /// Replaces any existing entry for this file.
    /// JE: `ExpirationProfile.putFile(tracker, expiredSize)`.
    pub fn put_file(&mut self, tracker: ExpirationTracker) {
        let file_number = tracker.get_file_number();
        self.trackers.insert(file_number, tracker);
    }

    /// Removes all expiration data for `file_number`.
    ///
    /// Called when a file is cleaned or deleted.
    /// JE: `ExpirationProfile.removeFile(fileNum)`.
    pub fn remove_file(&mut self, file_number: u32) {
        self.trackers.remove(&file_number);
    }

    /// Returns the number of expired bytes in `file_number` at
    /// `current_time_hours`.
    ///
    /// Returns 0 if no data is recorded for this file.
    /// JE: `ExpirationProfile.getExpiredBytes(fileNum)` — note JE converts
    /// `TTL.currentSystemTime()` (ms) to hours internally; callers here must
    /// pass hours directly.
    pub fn get_expired_bytes(
        &self,
        file_number: u32,
        current_time_hours: u64,
    ) -> i64 {
        self.trackers
            .get(&file_number)
            .map(|t| t.get_expired_bytes(current_time_hours))
            .unwrap_or(0)
    }

    /// Returns whether any data is recorded for `file_number`.
    pub fn has_file(&self, file_number: u32) -> bool {
        self.trackers.contains_key(&file_number)
    }

    /// Returns the number of files tracked.
    pub fn file_count(&self) -> usize {
        self.trackers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_empty() {
        let ep = ExpirationProfile::new();
        assert_eq!(ep.total_bytes(), 0);
        assert!(ep.is_all_expired(0));
        assert_eq!(ep.expired_fraction(0), 1.0);
    }

    #[test]
    fn test_add_no_expiration() {
        let mut ep = ExpirationProfile::new();
        ep.add(0, 100);
        assert_eq!(ep.total_bytes(), 100);
        // Bucket 0 (never-expires) is not added to buckets map,
        // so is_all_expired returns true (no buckets with future expiry).
        assert!(ep.is_all_expired(0));
    }

    #[test]
    fn test_add_with_expiration() {
        let mut ep = ExpirationProfile::new();
        ep.add(100, 500);
        ep.add(200, 300);
        assert_eq!(ep.total_bytes(), 800);
        // At time 99, nothing is expired.
        assert!(!ep.is_all_expired(99));
        assert_eq!(ep.expired_fraction(99), 0.0);
        // At time 100, first bucket expired.
        assert!(!ep.is_all_expired(100));
        assert!((ep.expired_fraction(100) - 500.0 / 800.0).abs() < 1e-9);
        // At time 200, all expired.
        assert!(ep.is_all_expired(200));
        assert_eq!(ep.expired_fraction(200), 1.0);
    }

    #[test]
    fn test_expired_fraction_mixed_never_and_ttl() {
        let mut ep = ExpirationProfile::new();
        ep.add(0, 400); // never expires
        ep.add(10, 600); // expires at hour 10
        assert_eq!(ep.total_bytes(), 1000);
        // At time 10: 600 bytes expired, 400 never expire.
        let frac = ep.expired_fraction(10);
        assert!((frac - 0.6).abs() < 1e-9);
        // is_all_expired: bucket 10 <= 10, so all buckets expired.
        assert!(ep.is_all_expired(10));
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;

    #[test]
    fn test_cln9_put_and_get_expired_bytes() {
        let mut store = ExpirationProfileStore::new();

        let mut tracker = ExpirationTracker::new(1);
        tracker.track(100, 500); // 500 bytes expire at hour 100
        tracker.track(200, 300);
        store.put_file(tracker);

        assert!(store.has_file(1));
        assert_eq!(store.get_expired_bytes(1, 100), 500);
        assert_eq!(store.get_expired_bytes(1, 200), 800);
    }

    #[test]
    fn test_cln9_remove_file() {
        let mut store = ExpirationProfileStore::new();
        let tracker = ExpirationTracker::new(42);
        store.put_file(tracker);
        assert!(store.has_file(42));

        store.remove_file(42);
        assert!(!store.has_file(42));
        assert_eq!(store.get_expired_bytes(42, 1000), 0);
    }

    #[test]
    fn test_cln9_missing_file_returns_zero() {
        let store = ExpirationProfileStore::new();
        assert_eq!(store.get_expired_bytes(99, 1000), 0);
    }

    #[test]
    fn test_cln9_replaces_on_put() {
        let mut store = ExpirationProfileStore::new();

        let mut t1 = ExpirationTracker::new(5);
        t1.track(100, 1000);
        store.put_file(t1);

        // Replace with new data
        let mut t2 = ExpirationTracker::new(5);
        t2.track(100, 2000); // different size
        store.put_file(t2);

        assert_eq!(store.get_expired_bytes(5, 100), 2000);
    }
}
