//! TTL-aware expiration profile for log file utilization.
//!
//! Port of `com.sleepycat.je.cleaner.ExpirationProfile`.

use std::collections::BTreeMap;

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
        ep.add(0, 400);  // never expires
        ep.add(10, 600); // expires at hour 10
        assert_eq!(ep.total_bytes(), 1000);
        // At time 10: 600 bytes expired, 400 never expire.
        let frac = ep.expired_fraction(10);
        assert!((frac - 0.6).abs() < 1e-9);
        // is_all_expired: bucket 10 <= 10, so all buckets expired.
        assert!(ep.is_all_expired(10));
    }
}
