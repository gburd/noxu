//! Write operation options.
//!

use crate::cache_mode::CacheMode;

/// Options for write operations.
///
/// Specifies optional parameters that control write behavior, including
/// caching and time-to-live (TTL) expiration.
///
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOptions {
    /// Cache mode for the write operation.
    pub cache_mode: Option<CacheMode>,

    /// Time-to-live in hours (0 = no expiration).
    pub ttl: u64,

    /// Whether to update TTL on existing records.
    pub update_ttl: bool,
}

impl WriteOptions {
    /// Creates a new WriteOptions with default settings.
    pub fn new() -> Self {
        Self { cache_mode: None, ttl: 0, update_ttl: false }
    }

    /// Sets the cache mode.
    pub fn with_cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = Some(cache_mode);
        self
    }

    /// Sets the time-to-live in hours.
    pub fn with_ttl(mut self, ttl_hours: u64) -> Self {
        self.ttl = ttl_hours;
        self
    }

    /// Sets whether to update TTL on existing records.
    pub fn with_update_ttl(mut self, update_ttl: bool) -> Self {
        self.update_ttl = update_ttl;
        self
    }

    /// Creates WriteOptions with evict-after-write cache mode.
    pub fn evict_after_write() -> Self {
        Self { cache_mode: Some(CacheMode::EvictLn), ttl: 0, update_ttl: false }
    }

    /// Creates WriteOptions with a TTL.
    pub fn with_expiration(ttl_hours: u64) -> Self {
        Self { cache_mode: None, ttl: ttl_hours, update_ttl: false }
    }

    /// Returns the expiration time in milliseconds from now, or 0 if no TTL.
    pub fn expiration_time_ms(&self, current_time_ms: u64) -> u64 {
        if self.ttl == 0 {
            0
        } else {
            current_time_ms + (self.ttl * 3600 * 1000)
        }
    }

    /// Returns whether a TTL is configured.
    pub fn has_ttl(&self) -> bool {
        self.ttl > 0
    }

    /// Returns the packed expiration_time (hours since epoch) for use in BinEntry.
    ///
    /// Returns 0 if no TTL is set.  Uses `noxu_util::ttl_hours_to_expiration`
    /// to compute the expiration time relative to now.
    pub fn get_expiration_time(&self) -> u32 {
        noxu_util::ttl_hours_to_expiration(self.ttl as u32)
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let opts = WriteOptions::new();
        assert_eq!(opts.cache_mode, None);
        assert_eq!(opts.ttl, 0);
        assert!(!opts.update_ttl);
    }

    #[test]
    fn test_with_cache_mode() {
        let opts = WriteOptions::new().with_cache_mode(CacheMode::KeepHot);
        assert_eq!(opts.cache_mode, Some(CacheMode::KeepHot));
    }

    #[test]
    fn test_with_ttl() {
        let opts = WriteOptions::new().with_ttl(24);
        assert_eq!(opts.ttl, 24);
    }

    #[test]
    fn test_with_update_ttl() {
        let opts = WriteOptions::new().with_update_ttl(true);
        assert!(opts.update_ttl);
    }

    #[test]
    fn test_evict_after_write() {
        let opts = WriteOptions::evict_after_write();
        assert_eq!(opts.cache_mode, Some(CacheMode::EvictLn));
        assert_eq!(opts.ttl, 0);
    }

    #[test]
    fn test_with_expiration() {
        let opts = WriteOptions::with_expiration(48);
        assert_eq!(opts.ttl, 48);
        assert!(!opts.update_ttl);
    }

    #[test]
    fn test_expiration_time_ms_no_ttl() {
        let opts = WriteOptions::new();
        assert_eq!(opts.expiration_time_ms(1000), 0);
    }

    #[test]
    fn test_expiration_time_ms_with_ttl() {
        let opts = WriteOptions::new().with_ttl(1); // 1 hour
        let current = 1000000;
        let expected = current + (3600 * 1000);
        assert_eq!(opts.expiration_time_ms(current), expected);
    }

    #[test]
    fn test_expiration_time_ms_24_hours() {
        let opts = WriteOptions::new().with_ttl(24);
        let current = 0;
        let expected = 24 * 3600 * 1000;
        assert_eq!(opts.expiration_time_ms(current), expected);
    }

    #[test]
    fn test_has_ttl() {
        let opts_no_ttl = WriteOptions::new();
        let opts_with_ttl = WriteOptions::new().with_ttl(1);
        assert!(!opts_no_ttl.has_ttl());
        assert!(opts_with_ttl.has_ttl());
    }

    #[test]
    fn test_default() {
        let opts = WriteOptions::default();
        assert_eq!(opts.cache_mode, None);
        assert_eq!(opts.ttl, 0);
        assert!(!opts.update_ttl);
    }

    #[test]
    fn test_clone() {
        let opts1 = WriteOptions::with_expiration(12);
        let opts2 = opts1.clone();
        assert_eq!(opts1, opts2);
    }

    #[test]
    fn test_builder_chain() {
        let opts = WriteOptions::new()
            .with_cache_mode(CacheMode::EvictBin)
            .with_ttl(48)
            .with_update_ttl(true);
        assert_eq!(opts.cache_mode, Some(CacheMode::EvictBin));
        assert_eq!(opts.ttl, 48);
        assert!(opts.update_ttl);
    }

    #[test]
    fn test_equality() {
        let opts1 = WriteOptions::new();
        let opts2 = WriteOptions::default();
        assert_eq!(opts1, opts2);
    }

    #[test]
    fn test_debug() {
        let opts = WriteOptions::with_expiration(24);
        let debug = format!("{:?}", opts);
        assert!(debug.contains("cache_mode"));
        assert!(debug.contains("ttl"));
    }
}
