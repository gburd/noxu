//! Cache modes for database operations.
//!

/// Cache mode for database operations.
///
/// Specifies how records are cached during database operations.
/// Allows applications to optimize caching behavior based on
/// access patterns.
///
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CacheMode {
    /// Use the default caching behavior.
    ///
    /// Records are cached normally according to the LRU policy.
    #[default]
    Default,

    /// Keep the record in cache.
    ///
    /// The record is pinned in cache and will not be evicted by
    /// the LRU policy. Use for hot data that should remain cached.
    KeepHot,

    /// Make the record most recently used.
    ///
    /// Moves the record to the MRU position in the LRU list, making
    /// it less likely to be evicted. This is the default for most operations.
    MakeCold,

    /// Evict the record after the operation.
    ///
    /// The record is immediately evicted from cache after the operation
    /// completes. Use for one-time access patterns or bulk operations.
    EvictLn,

    /// Evict both the record and its parent BIN.
    ///
    /// More aggressive eviction that also removes the BIN node from cache.
    /// Use for sequential scans where data won't be accessed again.
    EvictBin,

    /// Do not modify the cache position.
    ///
    /// The record is accessed but its position in the LRU list is not changed.
    /// Use when you don't want to affect normal cache management.
    Unchanged,
}

impl CacheMode {
    /// Returns whether this mode evicts the record.
    pub fn evicts_ln(&self) -> bool {
        matches!(self, CacheMode::EvictLn | CacheMode::EvictBin)
    }

    /// Returns whether this mode evicts the BIN.
    pub fn evicts_bin(&self) -> bool {
        matches!(self, CacheMode::EvictBin)
    }

    /// Returns whether this mode keeps the record hot.
    pub fn keeps_hot(&self) -> bool {
        matches!(self, CacheMode::KeepHot)
    }

    /// Returns whether this mode modifies cache position.
    pub fn modifies_cache(&self) -> bool {
        !matches!(self, CacheMode::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        assert_eq!(CacheMode::default(), CacheMode::Default);
    }

    #[test]
    fn test_evicts_ln() {
        assert!(CacheMode::EvictLn.evicts_ln());
        assert!(CacheMode::EvictBin.evicts_ln());
        assert!(!CacheMode::Default.evicts_ln());
        assert!(!CacheMode::KeepHot.evicts_ln());
    }

    #[test]
    fn test_evicts_bin() {
        assert!(CacheMode::EvictBin.evicts_bin());
        assert!(!CacheMode::EvictLn.evicts_bin());
        assert!(!CacheMode::Default.evicts_bin());
    }

    #[test]
    fn test_keeps_hot() {
        assert!(CacheMode::KeepHot.keeps_hot());
        assert!(!CacheMode::Default.keeps_hot());
        assert!(!CacheMode::EvictLn.keeps_hot());
    }

    #[test]
    fn test_modifies_cache() {
        assert!(CacheMode::Default.modifies_cache());
        assert!(CacheMode::KeepHot.modifies_cache());
        assert!(!CacheMode::Unchanged.modifies_cache());
    }

    #[test]
    fn test_equality() {
        assert_eq!(CacheMode::Default, CacheMode::Default);
        assert_ne!(CacheMode::Default, CacheMode::KeepHot);
    }

    #[test]
    fn test_clone() {
        let mode1 = CacheMode::EvictBin;
        let mode2 = mode1;
        assert_eq!(mode1, mode2);
    }

    #[test]
    fn test_copy() {
        let mode1 = CacheMode::MakeCold;
        let mode2 = mode1;
        assert_eq!(mode1, mode2);
    }

    #[test]
    fn test_debug() {
        let mode = CacheMode::KeepHot;
        let debug = format!("{:?}", mode);
        assert_eq!(debug, "KeepHot");
    }
}
