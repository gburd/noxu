//! Cursor configuration.
//!

/// Configuration for opening a cursor.
///
/// Specifies the configuration parameters used to open a cursor on a database.
///
/// # Changes in v1.5.1
///
/// Four fields that used to live on this struct (`read_committed`,
/// `non_sticky`, `evict_ln`, `prefix_constraint`) were removed because
/// the engine never consulted them.  See
/// `docs/src/internal/v1.5-decisions-2026-05.md` for the full rationale
/// and migration notes.
///
/// * `read_committed` — to use read-committed isolation, set it on the
///   surrounding [`crate::transaction_config::TransactionConfig`] (it
///   is honoured by [`crate::cursor::Cursor`] via the txn's locker)
///   or pass [`crate::lock_mode::LockMode::ReadCommitted`] to a
///   per-operation [`crate::read_options::ReadOptions`].
/// * `non_sticky` — Rust cursors are bound to their owning scope and
///   are not sticky to a transaction in the JE sense; the flag had
///   no observable effect.
/// * `evict_ln` — use [`crate::cache_mode::CacheMode::Unchanged`] /
///   `EvictLn` on the surrounding `DatabaseConfig` instead.
/// * `prefix_constraint` — application code should compare the
///   returned key against its own prefix and stop iterating; the
///   engine's BIN-level prefix is independent of the user's
///   range-scan termination condition.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CursorConfig {
    /// Use read-uncommitted isolation (dirty reads).
    ///
    /// When `true`, the cursor is opened in read-only mode and skips
    /// read-lock acquisition.  This mirrors JE's
    /// `CursorConfig.setReadUncommitted(true)` shape and is the only
    /// isolation override consulted at cursor-open time.  Per-operation
    /// `LockMode` overrides (passed via
    /// [`crate::read_options::ReadOptions`]) take precedence at the
    /// individual `get` call.
    pub read_uncommitted: bool,
}

impl CursorConfig {
    /// Creates a new CursorConfig with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets read-uncommitted isolation.
    pub fn set_read_uncommitted(
        &mut self,
        read_uncommitted: bool,
    ) -> &mut Self {
        self.read_uncommitted = read_uncommitted;
        self
    }

    /// Builder-style method to set read_uncommitted.
    pub fn with_read_uncommitted(mut self, read_uncommitted: bool) -> Self {
        self.read_uncommitted = read_uncommitted;
        self
    }

    /// Creates a CursorConfig for read-uncommitted isolation.
    pub fn read_uncommitted() -> Self {
        Self::new().with_read_uncommitted(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_defaults_to_no_read_uncommitted() {
        let config = CursorConfig::new();
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_set_read_uncommitted() {
        let mut config = CursorConfig::new();
        config.set_read_uncommitted(true);
        assert!(config.read_uncommitted);
    }

    #[test]
    fn test_with_read_uncommitted() {
        let config = CursorConfig::new().with_read_uncommitted(true);
        assert!(config.read_uncommitted);
    }

    #[test]
    fn test_read_uncommitted_factory() {
        let config = CursorConfig::read_uncommitted();
        assert!(config.read_uncommitted);
    }

    #[test]
    fn test_default() {
        let config = CursorConfig::default();
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_clone_eq() {
        let a = CursorConfig::read_uncommitted();
        let b = a.clone();
        assert_eq!(a, b);

        let c = CursorConfig::new();
        assert_ne!(a, c);
    }

    #[test]
    fn test_debug_format_mentions_field() {
        let config = CursorConfig::read_uncommitted();
        let debug = format!("{:?}", config);
        assert!(debug.contains("read_uncommitted"));
    }
}
