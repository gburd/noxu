//! Database configuration.
//!

use crate::cache_mode::CacheMode;
use std::cmp::Ordering;
use std::sync::Arc;

/// A user-supplied key (or duplicate-data) comparator paired with a stable
/// identity string.
///
/// JE persists the comparator's *class name* in the database record
/// (`DatabaseImpl.comparatorToBytes(comparator, byClassName=true)`) and
/// reconstructs the `Comparator<byte[]>` instance by class name at open
/// (`DatabaseImpl.ComparatorReader`).  A Rust `Fn` has no portable name and
/// cannot be reconstructed from a string, so Noxu's faithful adaptation
/// asks the application to supply that name itself: the `identity` is the
/// stable string persisted in the database record, and it is what the
/// reopen-time mismatch check compares.  See
/// `docs/src/maintainer/design-decisions.md`.
///
/// The `compare` closure receives the two *whole* (uncompressed) byte keys,
/// exactly as JE's `Comparator.compare(byte[] o1, byte[] o2)`.
#[derive(Clone)]
pub struct Comparator {
    identity: String,
    compare: Arc<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>,
}

impl Comparator {
    /// Builds a comparator from a stable `identity` string and a comparison
    /// closure.  The identity is persisted in the database record and must be
    /// re-supplied (with a matching comparator) on every subsequent open, or
    /// the open fails — mirroring JE's class-name persistence + reconstruct
    /// path (`DatabaseImpl.ComparatorReader`).
    pub fn new(
        identity: impl Into<String>,
        compare: impl Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
    ) -> Self {
        Self { identity: identity.into(), compare: Arc::new(compare) }
    }

    /// The stable identity persisted in the database record.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// The comparison closure (cloneable `Arc`).
    pub fn func(&self) -> Arc<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync> {
        Arc::clone(&self.compare)
    }
}

// A comparator's *behaviour* cannot be compared structurally, so equality
// and Debug key on the persisted identity only — the same value that drives
// the reopen-time mismatch check.
impl PartialEq for Comparator {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}
impl Eq for Comparator {}
impl std::fmt::Debug for Comparator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Comparator").field("identity", &self.identity).finish()
    }
}

/// Configuration for opening a database.
///
/// Specifies the configuration parameters used to open a database within
/// an environment. Use the builder pattern to configure individual parameters.
///
///
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct DatabaseConfig {
    /// Allow creation of a new database if it doesn't exist.
    pub allow_create: bool,

    /// Whether the database supports sorted duplicates.
    pub sorted_duplicates: bool,

    /// Whether the database supports transactions.
    pub transactional: bool,

    /// Open the database in read-only mode.
    pub read_only: bool,

    /// Whether this is a temporary database.
    ///
    /// Temporary databases are not logged and are removed when closed.
    pub temporary: bool,

    /// Whether to use deferred write mode.
    ///
    /// Deferred write databases delay writing to disk for better performance.
    pub deferred_write: bool,

    /// Override the B-tree key comparator.
    ///
    /// JE `DatabaseConfig.setOverrideBtreeComparator`: when `true`, the
    /// comparator supplied on *this* open replaces the one persisted in the
    /// database record, instead of being rejected as a mismatch.  When
    /// `false` (the default), supplying a different comparator than the one
    /// persisted is an error.
    pub override_btree_comparator: bool,

    /// Override the duplicate data comparator.
    ///
    /// JE `DatabaseConfig.setOverrideDuplicateComparator` — see
    /// `override_btree_comparator`.
    pub override_duplicate_comparator: bool,

    /// User-supplied B-tree key comparator (DBI-14).
    ///
    /// `None` (the default) uses unsigned-byte lexicographic order, byte for
    /// byte identical to JE's default.  When `Some`, every key comparison in
    /// the tree (search, insert, delete, split, cursor seek, range scan) uses
    /// it.  JE `DatabaseConfig.setBtreeComparator` /
    /// `DatabaseImpl.getBtreeComparator`.
    pub btree_comparator: Option<Comparator>,

    /// User-supplied duplicate-data comparator (DBI-14).
    ///
    /// Orders the *data* of duplicates sharing a primary key in a
    /// `sorted_duplicates` database.  `None` uses unsigned-byte order.  JE
    /// `DatabaseConfig.setDuplicateComparator` /
    /// `DatabaseImpl.getDuplicateComparator`.
    pub duplicate_comparator: Option<Comparator>,

    /// Whether this database is exclusive to a single thread.
    ///
    /// **Inert as of v1.6.0**: the
    /// `noxu_dbi` engine has no per-database thread-affinity
    /// enforcement; this flag is recorded but never consulted.
    pub exclusive: bool,

    /// Node maximum entries (0 = use default).
    pub node_max_entries: u32,

    /// Whether this database participates in replication.
    ///
    /// **Inert as of v1.6.0**: the
    /// `noxu_dbi::DatabaseConfig` has no `replicated` field; the
    /// replication scope is set at the env level via `noxu-rep`.
    pub replicated: bool,

    /// Enable key prefix compression in BIN nodes.
    ///
    /// **Plumbed through to `noxu_dbi::DatabaseConfig` as of v1.6.0**
    ///.
    pub key_prefixing: bool,

    /// Per-database cache eviction hint.
    ///
    /// **Inert as of v1.6.0**: the
    /// per-DB hint is not yet honoured by the evictor; the env-level
    /// cache mode is.
    pub cache_mode: CacheMode,

    /// Write BIN-deltas to the log instead of full BINs (space optimization).
    ///
    /// **Inert as of v1.6.0**: the
    /// engine always emits BIN-deltas where applicable.
    pub bin_delta: bool,

    /// When true, opening an existing database reuses its stored config
    /// rather than applying this config.
    ///
    /// **Inert as of v1.6.0**: the
    /// engine does not yet persist per-DB config across runs in a way
    /// that can be selectively re-applied.
    pub use_existing_config: bool,
}

impl DatabaseConfig {
    /// Creates a new DatabaseConfig with default settings.
    pub fn new() -> Self {
        Self {
            allow_create: false,
            sorted_duplicates: false,
            transactional: false,
            read_only: false,
            temporary: false,
            deferred_write: false,
            override_btree_comparator: false,
            override_duplicate_comparator: false,
            btree_comparator: None,
            duplicate_comparator: None,
            exclusive: false,
            node_max_entries: 0,
            replicated: false,
            key_prefixing: false,
            cache_mode: CacheMode::Default,
            bin_delta: true, // enabled by default (JE default)
            use_existing_config: false,
        }
    }

    /// Sets whether to allow creation of a new database.
    pub fn set_allow_create(&mut self, allow_create: bool) -> &mut Self {
        self.allow_create = allow_create;
        self
    }

    /// Sets whether the database supports sorted duplicates.
    pub fn set_sorted_duplicates(
        &mut self,
        sorted_duplicates: bool,
    ) -> &mut Self {
        self.sorted_duplicates = sorted_duplicates;
        self
    }

    /// Sets whether the database supports transactions.
    pub fn set_transactional(&mut self, transactional: bool) -> &mut Self {
        self.transactional = transactional;
        self
    }

    /// Sets whether the database is read-only.
    pub fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    /// Sets whether this is a temporary database.
    pub fn set_temporary(&mut self, temporary: bool) -> &mut Self {
        self.temporary = temporary;
        self
    }

    /// Sets whether to use deferred write mode.
    pub fn set_deferred_write(&mut self, deferred_write: bool) -> &mut Self {
        self.deferred_write = deferred_write;
        self
    }

    /// Sets whether to override the B-tree comparator.
    pub fn set_override_btree_comparator(
        &mut self,
        override_btree_comparator: bool,
    ) -> &mut Self {
        self.override_btree_comparator = override_btree_comparator;
        self
    }

    /// Sets whether to override the duplicate comparator.
    pub fn set_override_duplicate_comparator(
        &mut self,
        override_duplicate_comparator: bool,
    ) -> &mut Self {
        self.override_duplicate_comparator = override_duplicate_comparator;
        self
    }

    /// Sets the B-tree key comparator (DBI-14).
    ///
    /// JE `DatabaseConfig.setBtreeComparator`.  The comparator's identity is
    /// persisted in the database record; on every subsequent open the same
    /// identity must be re-supplied (or `override_btree_comparator` set), or
    /// the open fails.
    pub fn set_btree_comparator(
        &mut self,
        comparator: Comparator,
    ) -> &mut Self {
        self.btree_comparator = Some(comparator);
        self
    }

    /// Sets the duplicate-data comparator (DBI-14).
    ///
    /// JE `DatabaseConfig.setDuplicateComparator`.
    pub fn set_duplicate_comparator(
        &mut self,
        comparator: Comparator,
    ) -> &mut Self {
        self.duplicate_comparator = Some(comparator);
        self
    }

    /// Builder-style B-tree comparator setter (DBI-14).
    pub fn with_btree_comparator(mut self, comparator: Comparator) -> Self {
        self.btree_comparator = Some(comparator);
        self
    }

    /// Builder-style duplicate-data comparator setter (DBI-14).
    pub fn with_duplicate_comparator(mut self, comparator: Comparator) -> Self {
        self.duplicate_comparator = Some(comparator);
        self
    }

    /// Sets whether the database is exclusive.
    pub fn set_exclusive(&mut self, exclusive: bool) -> &mut Self {
        self.exclusive = exclusive;
        self
    }

    /// Sets the node maximum entries.
    pub fn set_node_max_entries(&mut self, node_max_entries: u32) -> &mut Self {
        self.node_max_entries = node_max_entries;
        self
    }

    /// Builder-style method to set allow_create.
    pub fn with_allow_create(mut self, allow_create: bool) -> Self {
        self.allow_create = allow_create;
        self
    }

    /// Builder-style method to set sorted_duplicates.
    pub fn with_sorted_duplicates(mut self, sorted_duplicates: bool) -> Self {
        self.sorted_duplicates = sorted_duplicates;
        self
    }

    /// Builder-style method to set transactional.
    pub fn with_transactional(mut self, transactional: bool) -> Self {
        self.transactional = transactional;
        self
    }

    /// Builder-style method to set read_only.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Builder-style method to set temporary.
    pub fn with_temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    /// Builder-style method to set deferred_write.
    pub fn with_deferred_write(mut self, deferred_write: bool) -> Self {
        self.deferred_write = deferred_write;
        self
    }

    /// Sets whether this database participates in replication.
    pub fn set_replicated(&mut self, replicated: bool) -> &mut Self {
        self.replicated = replicated;
        self
    }

    /// Builder-style method to set replicated.
    pub fn with_replicated(mut self, replicated: bool) -> Self {
        self.replicated = replicated;
        self
    }

    /// Sets whether key prefix compression is enabled.
    pub fn set_key_prefixing(&mut self, key_prefixing: bool) -> &mut Self {
        self.key_prefixing = key_prefixing;
        self
    }

    /// Builder-style method to set key_prefixing.
    pub fn with_key_prefixing(mut self, key_prefixing: bool) -> Self {
        self.key_prefixing = key_prefixing;
        self
    }

    /// Sets the per-database cache eviction mode.
    pub fn set_cache_mode(&mut self, cache_mode: CacheMode) -> &mut Self {
        self.cache_mode = cache_mode;
        self
    }

    /// Builder-style method to set cache_mode.
    pub fn with_cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = cache_mode;
        self
    }

    /// Sets whether BIN-deltas are written to the log.
    pub fn set_bin_delta(&mut self, bin_delta: bool) -> &mut Self {
        self.bin_delta = bin_delta;
        self
    }

    /// Builder-style method to set bin_delta.
    pub fn with_bin_delta(mut self, bin_delta: bool) -> Self {
        self.bin_delta = bin_delta;
        self
    }

    /// Sets whether to reuse existing config when opening an existing database.
    pub fn set_use_existing_config(&mut self, v: bool) -> &mut Self {
        self.use_existing_config = v;
        self
    }

    /// Builder-style method to set use_existing_config.
    pub fn with_use_existing_config(mut self, v: bool) -> Self {
        self.use_existing_config = v;
        self
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let config = DatabaseConfig::new();
        assert!(!config.allow_create);
        assert!(!config.sorted_duplicates);
        assert!(!config.transactional);
        assert!(!config.read_only);
        assert!(!config.temporary);
        assert!(!config.deferred_write);
    }

    #[test]
    fn test_set_allow_create() {
        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);
        assert!(config.allow_create);
    }

    #[test]
    fn test_set_sorted_duplicates() {
        let mut config = DatabaseConfig::new();
        config.set_sorted_duplicates(true);
        assert!(config.sorted_duplicates);
    }

    #[test]
    fn test_set_transactional() {
        let mut config = DatabaseConfig::new();
        config.set_transactional(true);
        assert!(config.transactional);
    }

    #[test]
    fn test_set_read_only() {
        let mut config = DatabaseConfig::new();
        config.set_read_only(true);
        assert!(config.read_only);
    }

    #[test]
    fn test_set_temporary() {
        let mut config = DatabaseConfig::new();
        config.set_temporary(true);
        assert!(config.temporary);
    }

    #[test]
    fn test_set_deferred_write() {
        let mut config = DatabaseConfig::new();
        config.set_deferred_write(true);
        assert!(config.deferred_write);
    }

    #[test]
    fn test_with_allow_create() {
        let config = DatabaseConfig::new().with_allow_create(true);
        assert!(config.allow_create);
    }

    #[test]
    fn test_with_sorted_duplicates() {
        let config = DatabaseConfig::new().with_sorted_duplicates(true);
        assert!(config.sorted_duplicates);
    }

    #[test]
    fn test_with_transactional() {
        let config = DatabaseConfig::new().with_transactional(true);
        assert!(config.transactional);
    }

    #[test]
    fn test_with_read_only() {
        let config = DatabaseConfig::new().with_read_only(true);
        assert!(config.read_only);
    }

    #[test]
    fn test_with_temporary() {
        let config = DatabaseConfig::new().with_temporary(true);
        assert!(config.temporary);
    }

    #[test]
    fn test_with_deferred_write() {
        let config = DatabaseConfig::new().with_deferred_write(true);
        assert!(config.deferred_write);
    }

    #[test]
    fn test_builder_chain() {
        let config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_transactional(true);
        assert!(config.allow_create);
        assert!(config.sorted_duplicates);
        assert!(config.transactional);
    }

    #[test]
    fn test_default() {
        let config = DatabaseConfig::default();
        assert!(!config.allow_create);
        assert!(!config.transactional);
    }

    #[test]
    fn test_clone() {
        let config1 = DatabaseConfig::new().with_allow_create(true);
        let config2 = config1.clone();
        assert_eq!(config1, config2);
    }

    #[test]
    fn test_equality() {
        let config1 = DatabaseConfig::new();
        let config2 = DatabaseConfig::default();
        assert_eq!(config1, config2);

        let config3 = DatabaseConfig::new().with_allow_create(true);
        assert_ne!(config1, config3);
    }

    #[test]
    fn test_override_comparators() {
        let mut config = DatabaseConfig::new();
        config.set_override_btree_comparator(true);
        config.set_override_duplicate_comparator(true);
        assert!(config.override_btree_comparator);
        assert!(config.override_duplicate_comparator);
    }

    #[test]
    fn test_exclusive() {
        let mut config = DatabaseConfig::new();
        assert!(!config.exclusive);
        config.set_exclusive(true);
        assert!(config.exclusive);
    }

    #[test]
    fn test_node_max_entries() {
        let mut config = DatabaseConfig::new();
        assert_eq!(config.node_max_entries, 0);
        config.set_node_max_entries(128);
        assert_eq!(config.node_max_entries, 128);
    }
}
