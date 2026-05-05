//! Cache mode configuration for controlling per-operation caching behavior.
//!
//! Port of `com.sleepycat.je.CacheMode`.

/// Modes that can be specified for control over caching of records in the
/// in-memory cache.
///
/// When a record is stored or retrieved, the cache mode determines how long
/// the record is subsequently retained in the in-memory cache, relative to
/// other records in the cache.
///
/// Port of `com.sleepycat.je.CacheMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CacheMode {
    /// The record's hotness is changed to "most recently used" by the operation.
    ///
    /// This cache mode is used when the application does not need explicit
    /// control over the cache and a standard LRU approach is sufficient.
    ///
    /// Specifically:
    /// - The BIN containing the record's LN will remain in the main cache,
    ///   and it is moved to the hot end of its LRU list.
    /// - When an off-heap cache is configured, the record's LN and BIN will
    ///   be loaded into the main cache only. They will be removed from the
    ///   off-heap cache, if they were present there.
    #[default]
    Default,

    /// The record's hotness or coldness is unchanged by the operation.
    ///
    /// This cache mode is normally used when the application prefers that
    /// the operation should not perturb the cache, for example, when scanning
    /// over all records in a database.
    ///
    /// Specifically:
    /// - A record's LN and BIN must be loaded into the main cache in order to
    ///   perform the operation. However, they may be removed from the main
    ///   cache after the operation, to avoid a net change to the cache.
    /// - If the record's LN was not present in the main cache prior to the
    ///   operation, then the LN will be evicted from the main cache after the
    ///   operation.
    /// - When the BIN was present in the main cache prior to the operation,
    ///   its position in the LRU list will not be changed.
    Unchanged,

    /// The record's LN is evicted after the operation, and the containing
    /// BIN is moved to the hot end of the LRU list.
    ///
    /// This cache mode is normally used when not all LNs will fit into the
    /// main cache, and the application prefers to read the LN from the log
    /// file or load it from the off-heap cache when the record is accessed
    /// again, rather than have it take up space in the main cache.
    ///
    /// By using this mode, the file system cache or off-heap cache can be
    /// relied on for holding LNs, which complements the use of the cache to
    /// hold BINs and INs.
    ///
    /// Specifically:
    /// - The record's LN will be evicted from the main cache after the operation.
    /// - The LN will be added to the off-heap cache, if it is not already
    ///   present and an off-heap cache is configured.
    /// - When a cursor is used, the LN is evicted when the cursor is moved to
    ///   a different record or closed.
    EvictLn,

    /// The record's BIN (and its LNs) are evicted after the operation.
    ///
    /// This cache mode is normally used when not all BINs will fit into the
    /// main cache, and the application prefers to read the LN and BIN from
    /// the log file or load it from the off-heap cache when the record is
    /// accessed again.
    ///
    /// Because this mode evicts all LNs in the BIN, even if they are "hot"
    /// from the perspective of a different accessor, this mode should be used
    /// with caution.
    ///
    /// Specifically:
    /// - The record's LN will be evicted from the main cache after the operation.
    /// - The LN will be added to the off-heap cache, if it is not already
    ///   present and an off-heap cache is configured.
    /// - Whether the BIN is evicted depends on whether the BIN is dirty and
    ///   whether an off-heap cache is configured.
    EvictBin,

    /// Pin the record in cache (keep it hot, don't evict).
    ///
    /// This is used internally to prevent eviction of nodes that are actively
    /// being used or have special significance.
    KeepHot,

    /// Move the record to the cold end of the LRU (make it evictable soon).
    ///
    /// This is used internally to mark nodes as good eviction candidates
    /// without actually evicting them immediately.
    MakeEvictable,
}

impl CacheMode {
    /// Returns true if this cache mode causes the node to be moved to the
    /// hot end of the LRU list.
    pub fn is_hot(self) -> bool {
        matches!(self, CacheMode::Default | CacheMode::KeepHot)
    }

    /// Returns true if this cache mode causes the node to be moved to the
    /// cold end of the LRU list.
    pub fn is_cold(self) -> bool {
        matches!(self, CacheMode::MakeEvictable)
    }

    /// Returns true if this cache mode leaves the LRU position unchanged.
    pub fn is_unchanged(self) -> bool {
        matches!(self, CacheMode::Unchanged)
    }

    /// Returns true if this cache mode causes LNs to be evicted.
    pub fn evicts_ln(self) -> bool {
        matches!(self, CacheMode::EvictLn | CacheMode::EvictBin)
    }

    /// Returns true if this cache mode causes BINs to be evicted.
    pub fn evicts_bin(self) -> bool {
        matches!(self, CacheMode::EvictBin)
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
    fn test_is_hot() {
        assert!(CacheMode::Default.is_hot());
        assert!(CacheMode::KeepHot.is_hot());
        assert!(!CacheMode::Unchanged.is_hot());
        assert!(!CacheMode::EvictLn.is_hot());
        assert!(!CacheMode::EvictBin.is_hot());
        assert!(!CacheMode::MakeEvictable.is_hot());
    }

    #[test]
    fn test_is_cold() {
        assert!(CacheMode::MakeEvictable.is_cold());
        assert!(!CacheMode::Default.is_cold());
        assert!(!CacheMode::KeepHot.is_cold());
        assert!(!CacheMode::Unchanged.is_cold());
    }

    #[test]
    fn test_is_unchanged() {
        assert!(CacheMode::Unchanged.is_unchanged());
        assert!(!CacheMode::Default.is_unchanged());
        assert!(!CacheMode::EvictLn.is_unchanged());
    }

    #[test]
    fn test_evicts_ln() {
        assert!(CacheMode::EvictLn.evicts_ln());
        assert!(CacheMode::EvictBin.evicts_ln());
        assert!(!CacheMode::Default.evicts_ln());
        assert!(!CacheMode::Unchanged.evicts_ln());
    }

    #[test]
    fn test_evicts_bin() {
        assert!(CacheMode::EvictBin.evicts_bin());
        assert!(!CacheMode::EvictLn.evicts_bin());
        assert!(!CacheMode::Default.evicts_bin());
    }

    #[test]
    fn test_equality() {
        assert_eq!(CacheMode::Default, CacheMode::Default);
        assert_ne!(CacheMode::Default, CacheMode::EvictLn);
    }

    #[test]
    fn test_clone() {
        let mode = CacheMode::EvictLn;
        let cloned = mode;
        assert_eq!(mode, cloned);
    }

    #[test]
    fn test_copy() {
        let mode = CacheMode::EvictBin;
        let copied = mode;
        assert_eq!(mode, copied);
    }
}
