//! Cursor configuration.
//!

/// Configuration for opening a cursor.
///
/// Specifies the configuration parameters used to open a cursor on a database.
///
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorConfig {
    /// Use read-committed isolation.
    ///
    /// Read locks are released when the cursor moves to a new position.
    pub read_committed: bool,

    /// Use read-uncommitted isolation (dirty reads).
    ///
    /// No read locks are acquired, allowing dirty reads.
    pub read_uncommitted: bool,

    /// Whether the cursor is non-sticky.
    ///
    /// Non-sticky cursors are automatically closed when the transaction commits.
    pub non_sticky: bool,

    /// Evict leaf nodes (LNs) from cache after reading.
    ///
    /// When true, fetched LN data is evicted from the cache after the cursor
    /// operation completes. Useful for scans that won't revisit data.
    pub evict_ln: bool,

    /// Key prefix constraint for range scans.
    ///
    /// When set, the cursor will stop advancing (return NotFound) when the
    /// fetched key no longer shares this prefix. Optimizes bounded prefix scans.
    pub prefix_constraint: Option<Vec<u8>>,
}

impl CursorConfig {
    /// Creates a new CursorConfig with default settings.
    pub fn new() -> Self {
        Self {
            read_committed: false,
            read_uncommitted: false,
            non_sticky: false,
            evict_ln: false,
            prefix_constraint: None,
        }
    }

    /// Sets read-committed isolation.
    pub fn set_read_committed(&mut self, read_committed: bool) -> &mut Self {
        self.read_committed = read_committed;
        if read_committed {
            self.read_uncommitted = false;
        }
        self
    }

    /// Sets read-uncommitted isolation.
    pub fn set_read_uncommitted(
        &mut self,
        read_uncommitted: bool,
    ) -> &mut Self {
        self.read_uncommitted = read_uncommitted;
        if read_uncommitted {
            self.read_committed = false;
        }
        self
    }

    /// Sets whether the cursor is non-sticky.
    pub fn set_non_sticky(&mut self, non_sticky: bool) -> &mut Self {
        self.non_sticky = non_sticky;
        self
    }

    /// Builder-style method to set read_committed.
    pub fn with_read_committed(mut self, read_committed: bool) -> Self {
        self.set_read_committed(read_committed);
        self
    }

    /// Builder-style method to set read_uncommitted.
    pub fn with_read_uncommitted(mut self, read_uncommitted: bool) -> Self {
        self.set_read_uncommitted(read_uncommitted);
        self
    }

    /// Builder-style method to set non_sticky.
    pub fn with_non_sticky(mut self, non_sticky: bool) -> Self {
        self.non_sticky = non_sticky;
        self
    }

    /// Sets whether to evict LNs from cache after reading.
    pub fn set_evict_ln(&mut self, evict_ln: bool) -> &mut Self {
        self.evict_ln = evict_ln;
        self
    }

    /// Builder-style method to set evict_ln.
    pub fn with_evict_ln(mut self, evict_ln: bool) -> Self {
        self.evict_ln = evict_ln;
        self
    }

    /// Sets the prefix constraint for bounded scans.
    pub fn set_prefix_constraint(
        &mut self,
        prefix: Option<Vec<u8>>,
    ) -> &mut Self {
        self.prefix_constraint = prefix;
        self
    }

    /// Builder-style method to set prefix_constraint.
    pub fn with_prefix_constraint(mut self, prefix: Vec<u8>) -> Self {
        self.prefix_constraint = Some(prefix);
        self
    }

    /// Creates a CursorConfig for read-committed isolation.
    pub fn read_committed() -> Self {
        Self::new().with_read_committed(true)
    }

    /// Creates a CursorConfig for read-uncommitted isolation.
    pub fn read_uncommitted() -> Self {
        Self::new().with_read_uncommitted(true)
    }
}

impl Default for CursorConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let config = CursorConfig::new();
        assert!(!config.read_committed);
        assert!(!config.read_uncommitted);
        assert!(!config.non_sticky);
    }

    #[test]
    fn test_set_read_committed() {
        let mut config = CursorConfig::new();
        config.set_read_committed(true);
        assert!(config.read_committed);
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_set_read_uncommitted() {
        let mut config = CursorConfig::new();
        config.set_read_uncommitted(true);
        assert!(config.read_uncommitted);
        assert!(!config.read_committed);
    }

    #[test]
    fn test_isolation_mutual_exclusion() {
        let mut config = CursorConfig::new();
        config.set_read_committed(true);
        assert!(config.read_committed);

        config.set_read_uncommitted(true);
        assert!(config.read_uncommitted);
        assert!(!config.read_committed);

        config.set_read_committed(true);
        assert!(config.read_committed);
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_set_non_sticky() {
        let mut config = CursorConfig::new();
        config.set_non_sticky(true);
        assert!(config.non_sticky);
    }

    #[test]
    fn test_with_read_committed() {
        let config = CursorConfig::new().with_read_committed(true);
        assert!(config.read_committed);
    }

    #[test]
    fn test_with_read_uncommitted() {
        let config = CursorConfig::new().with_read_uncommitted(true);
        assert!(config.read_uncommitted);
    }

    #[test]
    fn test_with_non_sticky() {
        let config = CursorConfig::new().with_non_sticky(true);
        assert!(config.non_sticky);
    }

    #[test]
    fn test_read_committed_factory() {
        let config = CursorConfig::read_committed();
        assert!(config.read_committed);
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_read_uncommitted_factory() {
        let config = CursorConfig::read_uncommitted();
        assert!(config.read_uncommitted);
        assert!(!config.read_committed);
    }

    #[test]
    fn test_default() {
        let config = CursorConfig::default();
        assert!(!config.read_committed);
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_clone() {
        let config1 = CursorConfig::read_committed();
        let config2 = config1.clone();
        assert_eq!(config1, config2);
    }

    #[test]
    fn test_equality() {
        let config1 = CursorConfig::new();
        let config2 = CursorConfig::default();
        assert_eq!(config1, config2);

        let config3 = CursorConfig::read_committed();
        assert_ne!(config1, config3);
    }

    #[test]
    fn test_builder_chain() {
        let config =
            CursorConfig::new().with_read_committed(true).with_non_sticky(true);
        assert!(config.read_committed);
        assert!(config.non_sticky);
    }

    #[test]
    fn test_debug() {
        let config = CursorConfig::read_uncommitted();
        let debug = format!("{:?}", config);
        assert!(debug.contains("read_uncommitted"));
    }
}
