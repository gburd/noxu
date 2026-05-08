//! Read operation options.
//!

use crate::cache_mode::CacheMode;
use crate::lock_mode::LockMode;

/// Options for read operations.
///
/// Specifies optional parameters that control read behavior, including
/// locking and caching.
///
/// 
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadOptions {
    /// Lock mode for the read operation.
    pub lock_mode: LockMode,

    /// Cache mode for the read operation.
    pub cache_mode: Option<CacheMode>,
}

impl ReadOptions {
    /// Creates a new ReadOptions with default settings.
    pub fn new() -> Self {
        Self { lock_mode: LockMode::Default, cache_mode: None }
    }

    /// Sets the lock mode.
    pub fn with_lock_mode(mut self, lock_mode: LockMode) -> Self {
        self.lock_mode = lock_mode;
        self
    }

    /// Sets the cache mode.
    pub fn with_cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = Some(cache_mode);
        self
    }

    /// Creates ReadOptions for read-uncommitted (dirty read).
    pub fn read_uncommitted() -> Self {
        Self { lock_mode: LockMode::ReadUncommitted, cache_mode: None }
    }

    /// Creates ReadOptions for read-modify-write.
    pub fn read_modify_write() -> Self {
        Self { lock_mode: LockMode::Rmw, cache_mode: None }
    }

    /// Creates ReadOptions with evict-after-read cache mode.
    pub fn evict_after_read() -> Self {
        Self {
            lock_mode: LockMode::Default,
            cache_mode: Some(CacheMode::EvictLn),
        }
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let opts = ReadOptions::new();
        assert_eq!(opts.lock_mode, LockMode::Default);
        assert_eq!(opts.cache_mode, None);
    }

    #[test]
    fn test_with_lock_mode() {
        let opts = ReadOptions::new().with_lock_mode(LockMode::Rmw);
        assert_eq!(opts.lock_mode, LockMode::Rmw);
    }

    #[test]
    fn test_with_cache_mode() {
        let opts = ReadOptions::new().with_cache_mode(CacheMode::KeepHot);
        assert_eq!(opts.cache_mode, Some(CacheMode::KeepHot));
    }

    #[test]
    fn test_read_uncommitted() {
        let opts = ReadOptions::read_uncommitted();
        assert_eq!(opts.lock_mode, LockMode::ReadUncommitted);
        assert_eq!(opts.cache_mode, None);
    }

    #[test]
    fn test_read_modify_write() {
        let opts = ReadOptions::read_modify_write();
        assert_eq!(opts.lock_mode, LockMode::Rmw);
    }

    #[test]
    fn test_evict_after_read() {
        let opts = ReadOptions::evict_after_read();
        assert_eq!(opts.cache_mode, Some(CacheMode::EvictLn));
    }

    #[test]
    fn test_default() {
        let opts = ReadOptions::default();
        assert_eq!(opts.lock_mode, LockMode::Default);
        assert_eq!(opts.cache_mode, None);
    }

    #[test]
    fn test_clone() {
        let opts1 = ReadOptions::read_uncommitted();
        let opts2 = opts1.clone();
        assert_eq!(opts1, opts2);
    }

    #[test]
    fn test_builder_chain() {
        let opts = ReadOptions::new()
            .with_lock_mode(LockMode::ReadCommitted)
            .with_cache_mode(CacheMode::EvictBin);
        assert_eq!(opts.lock_mode, LockMode::ReadCommitted);
        assert_eq!(opts.cache_mode, Some(CacheMode::EvictBin));
    }

    #[test]
    fn test_equality() {
        let opts1 = ReadOptions::new();
        let opts2 = ReadOptions::default();
        assert_eq!(opts1, opts2);
    }

    #[test]
    fn test_debug() {
        let opts = ReadOptions::read_uncommitted();
        let debug = format!("{:?}", opts);
        assert!(debug.contains("lock_mode"));
        assert!(debug.contains("cache_mode"));
    }
}
