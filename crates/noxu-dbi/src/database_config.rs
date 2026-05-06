//! Database configuration.
//!
//! Port of `com.sleepycat.je.DatabaseConfig`.

/// Configuration for a database.
///
/// Port of `com.sleepycat.je.DatabaseConfig`.
#[derive(Debug, Clone)]
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
    /// Maximum entries per node.
    pub node_max_entries: i32,
    /// Deferred write: skip WAL logging; flush only at eviction/checkpoint.
    ///
    /// Port of `DatabaseConfig.setDeferredWrite(true)` in JE.
    pub deferred_write: bool,
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
            node_max_entries: 128,
            deferred_write: false,
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

    /// Sets the maximum entries per node.
    pub fn set_node_max_entries(&mut self, max: i32) -> &mut Self {
        self.node_max_entries = max;
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
