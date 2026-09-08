//! Configuration for statistics retrieval operations.
//!
//! Implements `StatsConfig`.

/// Specifies the attributes of a statistics retrieval operation.
///
/// Pass to [`Environment::stats`][crate::environment::Environment::stats] or
/// [`Database::stats`][crate::database::Database::stats].
///
/// # Defaults
///
/// - `fast = false` — collect all statistics, including those that require an
///   expensive action such as a tree traversal or lock-table scan.
/// - `clear = false` — do not reset counters after reading them.
#[derive(Clone, Debug, Default)]
pub struct StatsConfig {
    /// If `true`, return only values that do not require expensive actions
    /// (e.g. skip B-tree traversal counts).  Implements `StatsConfig.setFast(true)`.
    pub fast: bool,
    /// If `true`, reset all counters to zero after reading them.
    /// Implements `StatsConfig.setClear(true)`.
    pub clear: bool,
}

impl StatsConfig {
    /// Creates a `StatsConfig` with all default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenience constructor: `fast = false`, `clear = true`.
    ///
    /// Implements `StatsConfig.CLEAR` constant.
    pub fn clear() -> Self {
        Self { fast: false, clear: true }
    }

    /// Builder: set `fast`.
    pub fn with_fast(mut self, fast: bool) -> Self {
        self.fast = fast;
        self
    }

    /// Builder: set `clear`.
    pub fn with_clear(mut self, clear: bool) -> Self {
        self.clear = clear;
        self
    }

    /// Sets `fast` and returns `&mut self` for chaining.
    pub fn set_fast(&mut self, fast: bool) -> &mut Self {
        self.fast = fast;
        self
    }

    /// Sets `clear` and returns `&mut self` for chaining.
    pub fn set_clear(&mut self, clear: bool) -> &mut Self {
        self.clear = clear;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_config::DatabaseConfig;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use tempfile::TempDir;

    #[test]
    fn defaults_are_full_and_non_clearing() {
        let c = StatsConfig::new();
        assert!(!c.fast, "default must collect the expensive stats too");
        assert!(!c.clear, "default must not reset counters");
        assert_eq!(format!("{c:?}"), format!("{:?}", StatsConfig::default()));
    }

    /// `StatsConfig::clear()` is the named convenience constructor for the
    /// JE `StatsConfig.CLEAR` constant: it must set `clear` WITHOUT also
    /// turning on `fast`, since a clearing read is still a full read.
    #[test]
    fn clear_constructor_sets_only_clear() {
        let c = StatsConfig::clear();
        assert!(c.clear);
        assert!(!c.fast);
    }

    #[test]
    fn builders_are_independent() {
        assert!(StatsConfig::new().with_fast(true).fast);
        assert!(!StatsConfig::new().with_fast(true).clear);
        assert!(StatsConfig::new().with_clear(true).clear);
        assert!(!StatsConfig::new().with_clear(true).fast);

        let mut c = StatsConfig::new();
        c.set_fast(true);
        assert!(c.fast && !c.clear);
        c.set_clear(true);
        assert!(c.fast && c.clear);
        c.set_fast(false);
        assert!(!c.fast && c.clear, "set_fast must not disturb clear");
    }

    /// The behavioural contract, not just the field: `fast = true` takes the
    /// O(1) counter path and therefore reports NO node counts, while
    /// `fast = false` walks the tree and populates them. Both must agree on
    /// the record count, which is the property that makes the fast path
    /// usable at all.
    #[test]
    fn fast_stats_skip_the_tree_walk_but_agree_on_the_record_count() {
        let dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "stats",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        for i in 0u16..64 {
            db.put(i.to_be_bytes(), b"v").unwrap();
        }

        let full = db.stats(Some(&StatsConfig::new())).unwrap();
        let fast = db.stats(Some(&StatsConfig::new().with_fast(true))).unwrap();

        assert_eq!(
            full.btree.leaf_node_count, 64,
            "full stats must count every record"
        );
        assert_eq!(
            fast.btree.leaf_node_count, full.btree.leaf_node_count,
            "the fast counter must agree with the walked count"
        );
        assert!(
            full.btree.bottom_internal_node_count > 0,
            "full stats must report BINs"
        );
        assert_eq!(
            fast.btree.bottom_internal_node_count, 0,
            "fast stats must skip the walk, leaving node counts at zero"
        );

        // `None` must behave as the default (full), not as fast.
        let none = db.stats(None).unwrap();
        assert_eq!(
            none.btree.bottom_internal_node_count,
            full.btree.bottom_internal_node_count
        );
    }
}
