//! Write operation options.
//!

use crate::cache_mode::CacheMode;
pub use noxu_util::TtlUnit;

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

    /// Time-to-live value (0 = no expiration).  Interpreted in [`ttl_unit`].
    ///
    /// [`ttl_unit`]: WriteOptions::ttl_unit
    pub ttl: u64,

    /// Unit of [`ttl`]: [`TtlUnit::Hours`] or [`TtlUnit::Days`].  Default
    /// is [`TtlUnit::Days`] (JE `WriteOptions` default), which minimizes the
    /// per-slot expiration storage.
    ///
    /// [`ttl`]: WriteOptions::ttl
    pub ttl_unit: TtlUnit,

    /// Whether to update TTL on existing records.
    pub update_ttl: bool,
}

impl WriteOptions {
    /// Creates a new WriteOptions with default settings.
    pub fn new() -> Self {
        Self {
            cache_mode: None,
            ttl: 0,
            ttl_unit: TtlUnit::Days,
            update_ttl: false,
        }
    }

    /// Sets the cache mode.
    #[deprecated(
        note = "not yet implemented: cache_mode is advisory/informational and \
                is not consulted by the engine; this setting has no effect"
    )]
    pub fn with_cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = Some(cache_mode);
        self
    }

    /// Sets the time-to-live in hours.
    ///
    /// Convenience for `with_ttl_unit(ttl_hours, TtlUnit::Hours)`.
    pub fn with_ttl(mut self, ttl_hours: u64) -> Self {
        self.ttl = ttl_hours;
        self.ttl_unit = TtlUnit::Hours;
        self
    }

    /// Sets the time-to-live with an explicit unit (hours or days).
    ///
    /// Mirrors JE `WriteOptions.setTTL(int ttl, TimeUnit)`.  `TtlUnit::Days`
    /// is recommended to minimize per-slot expiration storage.
    pub fn with_ttl_unit(mut self, ttl: u64, unit: TtlUnit) -> Self {
        self.ttl = ttl;
        self.ttl_unit = unit;
        self
    }

    /// Sets whether to update TTL on existing records.
    ///
    /// Mirrors JE `WriteOptions.setUpdateTTL(boolean)`: when `true`, an update
    /// to an existing record re-assigns (or clears, if `ttl` is 0) the
    /// record's expiration time; when `false`, an update leaves the existing
    /// expiration unchanged.  Ignored for inserts (a new record always takes
    /// the specified TTL).
    pub fn with_update_ttl(mut self, update_ttl: bool) -> Self {
        self.update_ttl = update_ttl;
        self
    }

    /// Creates WriteOptions with evict-after-write cache mode.
    #[deprecated(
        note = "not yet implemented: cache_mode is advisory/informational and \
                is not consulted by the engine; this constructor has no effect \
                beyond WriteOptions::new()"
    )]
    pub fn evict_after_write() -> Self {
        Self {
            cache_mode: Some(CacheMode::EvictLn),
            ttl: 0,
            ttl_unit: TtlUnit::Days,
            update_ttl: false,
        }
    }

    /// Creates WriteOptions with a TTL expressed in hours.
    pub fn with_expiration(ttl_hours: u64) -> Self {
        Self {
            cache_mode: None,
            ttl: ttl_hours,
            ttl_unit: TtlUnit::Hours,
            update_ttl: false,
        }
    }

    /// Returns the expiration time in milliseconds from now, or 0 if no TTL.
    pub fn expiration_time_ms(&self, current_time_ms: u64) -> u64 {
        if self.ttl == 0 {
            0
        } else {
            let unit_ms = match self.ttl_unit {
                TtlUnit::Hours => 3600 * 1000,
                TtlUnit::Days => 24 * 3600 * 1000,
            };
            current_time_ms + (self.ttl * unit_ms)
        }
    }

    /// Returns whether a TTL is configured.
    pub fn has_ttl(&self) -> bool {
        self.ttl > 0
    }

    /// Returns the packed expiration_time (hours since epoch) for use in the
    /// BIN slot and the LN log entry.
    ///
    /// Returns 0 if no TTL is set.  Uses the JE-faithful
    /// `noxu_util::ttl_to_expiration` which rounds the current time up to the
    /// next hour/day boundary before adding the TTL, and always yields
    /// hours-since-epoch (day-granular TTLs land on a 24-hour boundary).
    pub fn expiration_time(&self) -> u32 {
        noxu_util::ttl_to_expiration(self.ttl as u32, self.ttl_unit)
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
    #[allow(deprecated)]
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
    #[allow(deprecated)]
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
        let opts = WriteOptions::new().with_ttl(24); // 24 hours
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
    #[allow(deprecated)]
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
