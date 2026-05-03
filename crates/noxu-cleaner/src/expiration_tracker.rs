//! TTL expiration tracking for log files.
//!
//! Port of `ExpirationTracker.java` - tracks expired bytes in time windows
//! (histogram) for each log file, used to calculate expired data during cleaning.

use std::collections::HashMap;

/// Tracks the expired bytes in each time window (histogram) for a log file.
///
/// Each tracked file maintains a histogram of expiration times to byte counts.
/// This is used during cleaning to determine how much data in a file will
/// expire and when, allowing the cleaner to prioritize files with more
/// expired data.
///
/// The histogram uses expiration time buckets (in hours since epoch) as keys
/// and tracks both the count of records and total size for each bucket.
#[derive(Debug)]
pub struct ExpirationTracker {
    /// The log file number being tracked.
    file_number: u32,

    /// Histogram bins: expiration_time_hours -> (count, size in bytes)
    bins: HashMap<u64, ExpirationBin>,
}

/// A single bin in the expiration histogram.
#[derive(Debug, Clone, Default)]
pub struct ExpirationBin {
    /// Expiration time bucket (in hours since epoch, 0 = never expires).
    pub expiration_time: u64,

    /// Number of records expiring in this bucket.
    pub count: i32,

    /// Total size in bytes of records expiring in this bucket.
    pub size: i32,
}

impl ExpirationTracker {
    /// Creates a new expiration tracker for the given file.
    pub fn new(file_number: u32) -> Self {
        Self { file_number, bins: HashMap::new() }
    }

    /// Returns the file number being tracked.
    pub fn get_file_number(&self) -> u32 {
        self.file_number
    }

    /// Tracks an entry with the given expiration time and size.
    ///
    /// # Arguments
    /// * `expiration_time` - Expiration time in hours since epoch (0 = never expires)
    /// * `size` - Size of the entry in bytes
    pub fn track(&mut self, expiration_time: u64, size: i32) {
        if expiration_time == 0 {
            // 0 means never expires - don't track
            return;
        }

        self.bins
            .entry(expiration_time)
            .and_modify(|bin| {
                bin.count += 1;
                bin.size += size;
            })
            .or_insert(ExpirationBin { expiration_time, count: 1, size });
    }

    /// Returns the total size of expired bytes as of the given time.
    ///
    /// # Arguments
    /// * `current_time` - Current time in hours since epoch
    ///
    /// # Returns
    /// Total size in bytes of all entries that have expired by current_time
    pub fn get_expired_bytes(&self, current_time: u64) -> i64 {
        let mut expired_size = 0i64;

        for bin in self.bins.values() {
            if bin.expiration_time > 0 && bin.expiration_time <= current_time {
                expired_size += bin.size as i64;
            }
        }

        expired_size
    }

    /// Returns the total tracked size (all bins).
    pub fn get_total_tracked_size(&self) -> i64 {
        self.bins.values().map(|bin| bin.size as i64).sum()
    }

    /// Returns the number of bins in the histogram.
    pub fn get_bin_count(&self) -> usize {
        self.bins.len()
    }

    /// Returns a reference to all bins (for testing/inspection).
    pub fn get_bins(&self) -> &HashMap<u64, ExpirationBin> {
        &self.bins
    }

    /// Clears all tracked data.
    pub fn clear(&mut self) {
        self.bins.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_tracker() {
        let tracker = ExpirationTracker::new(5);
        assert_eq!(tracker.get_file_number(), 5);
        assert_eq!(tracker.get_bin_count(), 0);
        assert_eq!(tracker.get_total_tracked_size(), 0);
    }

    #[test]
    fn test_track_single_entry() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1024);

        assert_eq!(tracker.get_bin_count(), 1);
        assert_eq!(tracker.get_total_tracked_size(), 1024);
    }

    #[test]
    fn test_track_never_expires() {
        let mut tracker = ExpirationTracker::new(1);

        // Expiration time 0 means never expires - should not be tracked
        tracker.track(0, 1024);

        assert_eq!(tracker.get_bin_count(), 0);
        assert_eq!(tracker.get_total_tracked_size(), 0);
    }

    #[test]
    fn test_track_multiple_same_expiration() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 512);
        tracker.track(100, 256);
        tracker.track(100, 128);

        assert_eq!(tracker.get_bin_count(), 1);
        assert_eq!(tracker.get_total_tracked_size(), 896);

        let bin = tracker.get_bins().get(&100).unwrap();
        assert_eq!(bin.count, 3);
        assert_eq!(bin.size, 896);
    }

    #[test]
    fn test_track_different_expirations() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        assert_eq!(tracker.get_bin_count(), 3);
        assert_eq!(tracker.get_total_tracked_size(), 6000);
    }

    #[test]
    fn test_get_expired_bytes_none_expired() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        // Current time is before all expirations
        let expired = tracker.get_expired_bytes(50);
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_get_expired_bytes_some_expired() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        // Current time is after first two expirations
        let expired = tracker.get_expired_bytes(250);
        assert_eq!(expired, 3000); // 1000 + 2000
    }

    #[test]
    fn test_get_expired_bytes_all_expired() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        // Current time is after all expirations
        let expired = tracker.get_expired_bytes(400);
        assert_eq!(expired, 6000);
    }

    #[test]
    fn test_get_expired_bytes_exact_boundary() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);

        // Current time exactly at expiration
        let expired = tracker.get_expired_bytes(100);
        assert_eq!(expired, 1000);

        let expired = tracker.get_expired_bytes(200);
        assert_eq!(expired, 3000);
    }

    #[test]
    fn test_clear() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);

        assert_eq!(tracker.get_bin_count(), 2);

        tracker.clear();

        assert_eq!(tracker.get_bin_count(), 0);
        assert_eq!(tracker.get_total_tracked_size(), 0);
        assert_eq!(tracker.get_expired_bytes(1000), 0);
    }

    #[test]
    fn test_large_values() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(1_000_000, 100_000_000);
        tracker.track(2_000_000, 200_000_000);

        assert_eq!(tracker.get_total_tracked_size(), 300_000_000);
        assert_eq!(tracker.get_expired_bytes(1_500_000), 100_000_000);
        assert_eq!(tracker.get_expired_bytes(3_000_000), 300_000_000);
    }

    #[test]
    fn test_bins_independent() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        // Each bin should maintain its own count and size
        let bin100 = tracker.get_bins().get(&100).unwrap();
        let bin200 = tracker.get_bins().get(&200).unwrap();
        let bin300 = tracker.get_bins().get(&300).unwrap();

        assert_eq!(bin100.size, 1000);
        assert_eq!(bin200.size, 2000);
        assert_eq!(bin300.size, 3000);

        assert_eq!(bin100.count, 1);
        assert_eq!(bin200.count, 1);
        assert_eq!(bin300.count, 1);
    }

    #[test]
    fn test_expiration_bin_default() {
        let bin = ExpirationBin::default();
        assert_eq!(bin.expiration_time, 0);
        assert_eq!(bin.count, 0);
        assert_eq!(bin.size, 0);
    }

    #[test]
    fn test_mixed_tracking() {
        let mut tracker = ExpirationTracker::new(1);

        // Mix of never-expires and timed entries
        tracker.track(0, 1000); // Should be ignored
        tracker.track(100, 500);
        tracker.track(0, 2000); // Should be ignored
        tracker.track(100, 500);
        tracker.track(200, 1000);

        assert_eq!(tracker.get_bin_count(), 2); // Only two bins (100 and 200)
        assert_eq!(tracker.get_total_tracked_size(), 2000); // Ignores never-expires entries
    }
}
