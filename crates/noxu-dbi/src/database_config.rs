//! Database configuration.
//!

use std::sync::Arc;

use noxu_tree::KeyComparatorFn;

use crate::trigger::Trigger;

/// A persisted-identity + comparison-function pair threaded from the public
/// `noxu_db::Comparator` down to `DatabaseImpl`.
///
/// The `identity` is the stable string persisted in the database record (the
/// NameLN data) and re-checked at open; the `func` is the actual comparison
/// closure threaded into the `Tree`.  JE `DatabaseImpl.btreeComparator` plus
/// the persisted `btreeComparatorBytes` (the serialized class name).
#[derive(Clone)]
pub struct ConfigComparator {
    /// Stable identity persisted in the database record.
    pub identity: String,
    /// The comparison closure threaded into the tree.
    pub func: KeyComparatorFn,
}

impl std::fmt::Debug for ConfigComparator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigComparator")
            .field("identity", &self.identity)
            .finish()
    }
}

/// Configuration for a database.
///
///
#[derive(Clone)]
pub struct DatabaseConfig {
    /// Allow database creation if it doesn't exist.
    pub allow_create: bool,
    /// Enable sorted duplicates.
    pub sorted_duplicates: bool,
    /// Enable key prefixing compression.
    pub key_prefixing: bool,
    /// Database is temporary (not persisted).
    pub temporary: bool,
    /// Database operations are transactional.
    pub transactional: bool,
    /// Database is read-only.
    pub read_only: bool,
    /// Whether this database participates in replication. Default `true`.
    /// Only takes effect when the owning environment is itself replicated
    /// (`EnvironmentImpl::is_replicated()`); on a plain (non-replicated)
    /// environment every database is non-replicated regardless of this
    /// value, since the environment itself is never marked replicated
    /// there.
    pub replicated: bool,
    /// Maximum entries per node.
    pub node_max_entries: i32,
    /// Deferred write: skip WAL logging; flush only at eviction/checkpoint.
    ///
    ///
    pub deferred_write: bool,
    /// User-supplied B-tree key comparator (DBI-14).
    ///
    /// JE `DatabaseImpl.btreeComparator`.  `None` = unsigned-byte order.
    pub btree_comparator: Option<ConfigComparator>,
    /// User-supplied duplicate-data comparator (DBI-14).
    ///
    /// JE `DatabaseImpl.duplicateComparator`.
    pub duplicate_comparator: Option<ConfigComparator>,
    /// JE `DatabaseConfig.overrideBtreeComparator`: replace a persisted
    /// comparator instead of rejecting a mismatch.
    pub override_btree_comparator: bool,
    /// JE `DatabaseConfig.overrideDuplicateComparator`.
    pub override_duplicate_comparator: bool,
    /// User-supplied database / transaction triggers, fired in registration
    /// order (DB-TRIG).
    ///
    /// JE `DatabaseConfig.setTriggers` / `getTriggers` (a `List<Trigger>`).
    /// Runtime-registered only: not persisted, not replicated — see
    /// [`crate::trigger`].
    pub triggers: Vec<Arc<dyn Trigger>>,
}

impl std::fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("allow_create", &self.allow_create)
            .field("sorted_duplicates", &self.sorted_duplicates)
            .field("key_prefixing", &self.key_prefixing)
            .field("temporary", &self.temporary)
            .field("transactional", &self.transactional)
            .field("read_only", &self.read_only)
            .field("node_max_entries", &self.node_max_entries)
            .field("deferred_write", &self.deferred_write)
            .field("btree_comparator", &self.btree_comparator)
            .field("duplicate_comparator", &self.duplicate_comparator)
            .field("override_btree_comparator", &self.override_btree_comparator)
            .field(
                "override_duplicate_comparator",
                &self.override_duplicate_comparator,
            )
            .field("triggers", &self.triggers.len())
            .finish()
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            allow_create: false,
            sorted_duplicates: false,
            key_prefixing: false,
            temporary: false,
            transactional: false,
            read_only: false,
            replicated: true,
            node_max_entries: 128,
            deferred_write: false,
            btree_comparator: None,
            duplicate_comparator: None,
            override_btree_comparator: false,
            override_duplicate_comparator: false,
            triggers: Vec::new(),
        }
    }
}

impl DatabaseConfig {
    /// Creates a new DatabaseConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the allow_create flag.
    pub fn set_allow_create(&mut self, allow_create: bool) -> &mut Self {
        self.allow_create = allow_create;
        self
    }

    /// Sets the sorted_duplicates flag.
    pub fn set_sorted_duplicates(
        &mut self,
        sorted_duplicates: bool,
    ) -> &mut Self {
        self.sorted_duplicates = sorted_duplicates;
        self
    }

    /// Sets the key_prefixing flag.
    pub fn set_key_prefixing(&mut self, key_prefixing: bool) -> &mut Self {
        self.key_prefixing = key_prefixing;
        self
    }

    /// Sets the temporary flag.
    pub fn set_temporary(&mut self, temporary: bool) -> &mut Self {
        self.temporary = temporary;
        self
    }

    /// Sets the transactional flag.
    pub fn set_transactional(&mut self, transactional: bool) -> &mut Self {
        self.transactional = transactional;
        self
    }

    /// Sets the read_only flag.
    pub fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    /// Sets the replicated flag.
    pub fn set_replicated(&mut self, replicated: bool) -> &mut Self {
        self.replicated = replicated;
        self
    }

    /// Sets the maximum entries per node.
    pub fn set_node_max_entries(&mut self, max: i32) -> &mut Self {
        self.node_max_entries = max;
        self
    }

    /// Appends a trigger to the registration list (DB-TRIG).
    ///
    /// Triggers fire in the order they are added.  JE
    /// `DatabaseConfig.setTriggers` (Noxu allows incremental registration).
    pub fn add_trigger(&mut self, trigger: Arc<dyn Trigger>) -> &mut Self {
        self.triggers.push(trigger);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let config = DatabaseConfig::default();
        assert!(!config.allow_create);
        assert!(!config.sorted_duplicates);
        assert!(!config.key_prefixing);
        assert!(!config.temporary);
        assert!(!config.transactional);
        assert!(!config.read_only);
        assert_eq!(config.node_max_entries, 128);
    }

    #[test]
    fn test_new() {
        let config = DatabaseConfig::new();
        assert!(!config.allow_create);
    }

    #[test]
    fn test_setters() {
        let mut config = DatabaseConfig::new();

        config.set_allow_create(true);
        assert!(config.allow_create);

        config.set_sorted_duplicates(true);
        assert!(config.sorted_duplicates);

        config.set_key_prefixing(true);
        assert!(config.key_prefixing);

        config.set_temporary(true);
        assert!(config.temporary);

        config.set_transactional(true);
        assert!(config.transactional);

        config.set_read_only(true);
        assert!(config.read_only);

        config.set_node_max_entries(256);
        assert_eq!(config.node_max_entries, 256);
    }

    #[test]
    fn test_builder_pattern() {
        let config = DatabaseConfig::new()
            .set_allow_create(true)
            .set_transactional(true)
            .set_node_max_entries(512)
            .clone();

        assert!(config.allow_create);
        assert!(config.transactional);
        assert_eq!(config.node_max_entries, 512);
    }
}
