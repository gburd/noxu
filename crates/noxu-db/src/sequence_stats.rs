//! Sequence statistics.
//!

/// Statistics for a `Sequence` handle.
///
/// 
#[derive(Debug, Clone)]
pub struct SequenceStats {
    /// Total number of successful `get` calls on this handle.
    pub n_gets: u64,

    /// Number of `get` calls that were served from the in-memory cache
    /// without touching the database.
    pub n_cache_hits: u64,

    /// The value most recently written to the database (the "stored value").
    /// Other handles may have already consumed values between `current_value`
    /// and `cache_last`.
    pub current_value: i64,

    /// The next value that will be returned from the local cache.
    pub cache_value: i64,

    /// The last value reserved in the local cache.
    pub cache_last: i64,

    /// Configured minimum of the sequence range.
    pub range_min: i64,

    /// Configured maximum of the sequence range.
    pub range_max: i64,

    /// Configured cache size for this handle.
    pub cache_size: i32,
}
