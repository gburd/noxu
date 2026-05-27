//! Entity store configuration.
//!

use std::sync::Arc;

use crate::evolve::evolve_config::EvolveConfig;
use crate::evolve::mutations::Mutations;

/// Configuration for an `EntityStore`.
///
/// Controls how the entity store is opened, including whether to allow
/// creation of new databases and whether the store operates in read-only
/// or transactional mode.
///
///
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

    /// Schema-evolution mutations applied on the open path (Wave 2C-2).
    ///
    /// When `EntityStore::get_primary_index<E>` is called, the persisted
    /// catalog version is compared against `E::class_version()`.  If they
    /// differ and `mutations` is `Some`, the records of the entity class
    /// are streamed through the catalog evolution under a single
    /// transaction; class-level [`crate::evolve::Renamer`] /
    /// [`crate::evolve::Deleter`] / [`crate::evolve::Converter`] are
    /// applied; per-record envelopes are rewritten with the current
    /// version.
    ///
    /// Wrapped in `Arc` so the same set of mutations can be shared with
    /// `PrimaryIndex` for read-side, version-aware deserialisation
    /// without cloning.
    pub mutations: Option<Arc<Mutations>>,

    /// Filter / progress listener for the open-path eager evolution.
    pub evolve_config: Option<Arc<EvolveConfig>>,
}

impl StoreConfig {
    /// Creates a new `StoreConfig` with the given store name.
    ///
    /// Defaults: `allow_create = false`, `read_only = false`,
    /// `transactional = false`, no mutations registered.
    pub fn new(store_name: impl Into<String>) -> Self {
        Self {
            store_name: store_name.into(),
            allow_create: false,
            read_only: false,
            transactional: false,
            mutations: None,
            evolve_config: None,
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

    /// Registers a [`Mutations`] config to drive open-path schema evolution
    /// (Wave 2C-2).
    ///
    /// The mutations are also exposed to [`EntitySerializer::deserialize_versioned`]
    /// so user serializers can do field-level evolution on read.
    ///
    /// [`EntitySerializer::deserialize_versioned`]: crate::entity_serializer::EntitySerializer::deserialize_versioned
    pub fn with_mutations(mut self, mutations: Mutations) -> Self {
        self.mutations = Some(Arc::new(mutations));
        self
    }

    /// Like [`Self::with_mutations`] but accepts an already-shared `Arc`.
    pub fn with_mutations_arc(mut self, mutations: Arc<Mutations>) -> Self {
        self.mutations = Some(mutations);
        self
    }

    /// Registers an [`EvolveConfig`] used to filter or report progress on
    /// the open-path evolution.
    pub fn with_evolve_config(mut self, cfg: EvolveConfig) -> Self {
        self.evolve_config = Some(Arc::new(cfg));
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

    /// Returns the registered mutations, if any.
    pub fn mutations(&self) -> Option<&Arc<Mutations>> {
        self.mutations.as_ref()
    }

    /// Returns the registered evolve config, if any.
    pub fn evolve_config(&self) -> Option<&Arc<EvolveConfig>> {
        self.evolve_config.as_ref()
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

    #[test]
    fn test_with_mutations_round_trip() {
        use crate::evolve::{Mutations, Renamer};
        let mut m = Mutations::new();
        m.add_renamer(Renamer::for_class("A", 0, "B"));
        let cfg = StoreConfig::new("s").with_mutations(m);
        assert!(cfg.mutations().is_some());
        assert!(cfg.mutations().unwrap().get_renamer("A", 0, None).is_some());
    }

    #[test]
    fn test_with_evolve_config() {
        use crate::evolve::EvolveConfig;
        let cfg = StoreConfig::new("s")
            .with_evolve_config(EvolveConfig::new().with_class_to_evolve("X"));
        assert!(cfg.evolve_config().is_some());
        assert!(cfg.evolve_config().unwrap().should_evolve("X"));
        assert!(!cfg.evolve_config().unwrap().should_evolve("Y"));
    }

    #[test]
    fn test_default_no_mutations_or_evolve_config() {
        let cfg = StoreConfig::new("s");
        assert!(cfg.mutations().is_none());
        assert!(cfg.evolve_config().is_none());
    }
}
