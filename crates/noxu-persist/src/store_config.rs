//! Entity store configuration.
//!
//! Port of `com.sleepycat.persist.StoreConfig`.

/// Configuration for an `EntityStore`.
///
/// Controls how the entity store is opened, including whether to allow
/// creation of new databases and whether the store operates in read-only
/// or transactional mode.
///
/// Port of `com.sleepycat.persist.StoreConfig`.
///
/// # Example
///
/// ```
/// use noxu_persist::StoreConfig;
///
/// let config = StoreConfig::new("my_store")
///     .with_allow_create(true)
///     .with_transactional(false);
/// ```
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// The store name, used as a prefix for database names.
    pub store_name: String,

    /// Whether to allow creation of new databases when they don't exist.
    pub allow_create: bool,

    /// Whether the store is read-only.
    pub read_only: bool,

    /// Whether the store operates in transactional mode.
    pub transactional: bool,
}

impl StoreConfig {
    /// Creates a new `StoreConfig` with the given store name.
    ///
    /// Defaults: `allow_create = false`, `read_only = false`, `transactional = false`.
    pub fn new(store_name: impl Into<String>) -> Self {
        Self {
            store_name: store_name.into(),
            allow_create: false,
            read_only: false,
            transactional: false,
        }
    }

    /// Builder-style method to set `allow_create`.
    pub fn with_allow_create(mut self, allow: bool) -> Self {
        self.allow_create = allow;
        self
    }

    /// Builder-style method to set `read_only`.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Builder-style method to set `transactional`.
    pub fn with_transactional(mut self, txn: bool) -> Self {
        self.transactional = txn;
        self
    }

    /// Sets the `allow_create` flag.
    pub fn set_allow_create(&mut self, allow: bool) -> &mut Self {
        self.allow_create = allow;
        self
    }

    /// Sets the `read_only` flag.
    pub fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    /// Sets the `transactional` flag.
    pub fn set_transactional(&mut self, txn: bool) -> &mut Self {
        self.transactional = txn;
        self
    }

    /// Returns the store name.
    pub fn get_store_name(&self) -> &str {
        &self.store_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_defaults() {
        let config = StoreConfig::new("test");
        assert_eq!(config.store_name, "test");
        assert!(!config.allow_create);
        assert!(!config.read_only);
        assert!(!config.transactional);
    }

    #[test]
    fn test_with_allow_create() {
        let config = StoreConfig::new("s").with_allow_create(true);
        assert!(config.allow_create);
    }

    #[test]
    fn test_with_read_only() {
        let config = StoreConfig::new("s").with_read_only(true);
        assert!(config.read_only);
    }

    #[test]
    fn test_with_transactional() {
        let config = StoreConfig::new("s").with_transactional(true);
        assert!(config.transactional);
    }

    #[test]
    fn test_builder_chain() {
        let config = StoreConfig::new("mystore")
            .with_allow_create(true)
            .with_read_only(false)
            .with_transactional(true);
        assert_eq!(config.store_name, "mystore");
        assert!(config.allow_create);
        assert!(!config.read_only);
        assert!(config.transactional);
    }

    #[test]
    fn test_set_allow_create() {
        let mut config = StoreConfig::new("s");
        config.set_allow_create(true);
        assert!(config.allow_create);
    }

    #[test]
    fn test_set_read_only() {
        let mut config = StoreConfig::new("s");
        config.set_read_only(true);
        assert!(config.read_only);
    }

    #[test]
    fn test_set_transactional() {
        let mut config = StoreConfig::new("s");
        config.set_transactional(true);
        assert!(config.transactional);
    }

    #[test]
    fn test_get_store_name() {
        let config = StoreConfig::new("hello");
        assert_eq!(config.get_store_name(), "hello");
    }

    #[test]
    fn test_from_string() {
        let name = String::from("dynamic_name");
        let config = StoreConfig::new(name);
        assert_eq!(config.store_name, "dynamic_name");
    }

    #[test]
    fn test_clone() {
        let config1 = StoreConfig::new("test").with_allow_create(true);
        let config2 = config1.clone();
        assert_eq!(config1.store_name, config2.store_name);
        assert_eq!(config1.allow_create, config2.allow_create);
    }
}
