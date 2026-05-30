//! Configuration for eager schema evolution.
//!

use hashbrown::HashSet;

/// A progress listener invoked during eager entity evolution.
///
/// Return `true` to continue evolution; `false` to stop early.
///
///
pub trait EvolveListener: Send + Sync {
    /// Called each time an entity is processed during eager evolution.
    ///
    /// # Arguments
    /// * `entity_class_name` - The name of the entity class being evolved.
    /// * `n_read` - Cumulative entities read so far.
    /// * `n_converted` - Cumulative entities written/converted so far.
    ///
    /// Returns `true` to continue, `false` to abort.
    fn evolve_progress(
        &self,
        entity_class_name: &str,
        n_read: u64,
        n_converted: u64,
    ) -> bool;
}

/// Configuration for eager entity evolution.
///
/// Controls which entity classes are eagerly re-serialized when
/// [`EntityStore::evolve`] is called.  If no classes are added via
/// [`add_class_to_evolve`], **all** entity classes that require evolution
/// are processed.
///
/// # Example
///
/// ```
/// use crate::persist::evolve::EvolveConfig;
///
/// let config = EvolveConfig::new()
///     .with_class_to_evolve("my.package.Person");
/// ```
///
///
///
/// [`EntityStore::evolve`]: crate::persist::entity_store::EntityStore::evolve
/// [`add_class_to_evolve`]: EvolveConfig::add_class_to_evolve
#[derive(Default)]
pub struct EvolveConfig {
    /// Specific entity class names to evolve; empty means "all classes".
    classes_to_evolve: HashSet<String>,
    /// Optional progress listener.
    listener: Option<Box<dyn EvolveListener>>,
}

impl EvolveConfig {
    /// Creates an evolve configuration with default properties.
    ///
    ///
    pub fn new() -> Self {
        Self { classes_to_evolve: HashSet::new(), listener: None }
    }

    /// Adds an entity class name to the set of classes to evolve.
    ///
    /// If no classes are added, all indexes that require evolution will be
    /// converted.
    ///
    ///
    pub fn add_class_to_evolve(
        &mut self,
        entity_class: impl Into<String>,
    ) -> &mut Self {
        self.classes_to_evolve.insert(entity_class.into());
        self
    }

    /// Builder-style version of [`add_class_to_evolve`].
    ///
    /// [`add_class_to_evolve`]: EvolveConfig::add_class_to_evolve
    pub fn with_class_to_evolve(
        mut self,
        entity_class: impl Into<String>,
    ) -> Self {
        self.classes_to_evolve.insert(entity_class.into());
        self
    }

    /// Returns the set of entity class names to be evolved.
    ///
    /// An empty set means "evolve all classes".
    ///
    ///
    pub fn classes_to_evolve(&self) -> &HashSet<String> {
        &self.classes_to_evolve
    }

    /// Returns `true` if the given class name should be evolved, given the
    /// current configuration.
    ///
    /// If the classes set is empty, all classes should be evolved.
    pub fn should_evolve(&self, class_name: &str) -> bool {
        self.classes_to_evolve.is_empty()
            || self.classes_to_evolve.contains(class_name)
    }

    /// Sets a progress listener that is notified each time an entity is read.
    ///
    ///
    pub fn set_listener(
        &mut self,
        listener: impl EvolveListener + 'static,
    ) -> &mut Self {
        self.listener = Some(Box::new(listener));
        self
    }

    /// Builder-style version of [`set_listener`].
    ///
    /// [`set_listener`]: EvolveConfig::set_listener
    pub fn with_listener(
        mut self,
        listener: impl EvolveListener + 'static,
    ) -> Self {
        self.listener = Some(Box::new(listener));
        self
    }

    /// Returns the progress listener, if one was set.
    ///
    ///
    pub fn listener(&self) -> Option<&dyn EvolveListener> {
        self.listener.as_deref()
    }
}

impl std::fmt::Debug for EvolveConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvolveConfig")
            .field("classes_to_evolve", &self.classes_to_evolve)
            .field("listener", &self.listener.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_evolves_all() {
        let cfg = EvolveConfig::new();
        assert!(cfg.should_evolve("any.Class"));
        assert!(cfg.should_evolve("another.Class"));
    }

    #[test]
    fn test_specific_class_filter() {
        let cfg = EvolveConfig::new().with_class_to_evolve("my.pkg.Person");
        assert!(cfg.should_evolve("my.pkg.Person"));
        assert!(!cfg.should_evolve("my.pkg.Other"));
    }

    #[test]
    fn test_add_multiple_classes() {
        let mut cfg = EvolveConfig::new();
        cfg.add_class_to_evolve("A");
        cfg.add_class_to_evolve("B");
        assert!(cfg.should_evolve("A"));
        assert!(cfg.should_evolve("B"));
        assert!(!cfg.should_evolve("C"));
    }

    #[test]
    fn test_classes_to_evolve_returns_set() {
        let cfg = EvolveConfig::new().with_class_to_evolve("X");
        assert!(cfg.classes_to_evolve().contains("X"));
    }

    #[test]
    fn test_no_listener_by_default() {
        let cfg = EvolveConfig::new();
        assert!(cfg.listener().is_none());
    }

    #[test]
    fn test_with_listener() {
        struct TestListener;
        impl EvolveListener for TestListener {
            fn evolve_progress(&self, _: &str, _: u64, _: u64) -> bool {
                true
            }
        }
        let cfg = EvolveConfig::new().with_listener(TestListener);
        assert!(cfg.listener().is_some());
    }

    #[test]
    fn test_listener_called() {
        use std::sync::{Arc, Mutex};

        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let calls_clone = calls.clone();

        struct CountListener {
            calls: Arc<Mutex<Vec<String>>>,
        }
        impl EvolveListener for CountListener {
            fn evolve_progress(
                &self,
                entity_class_name: &str,
                _n_read: u64,
                _n_converted: u64,
            ) -> bool {
                self.calls.lock().unwrap().push(entity_class_name.to_string());
                true
            }
        }

        let cfg = EvolveConfig::new()
            .with_listener(CountListener { calls: calls_clone });
        let result = cfg.listener().unwrap().evolve_progress("my.Entity", 1, 1);
        assert!(result);
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(calls.lock().unwrap()[0], "my.Entity");
    }

    #[test]
    fn test_debug() {
        let cfg = EvolveConfig::new().with_class_to_evolve("X");
        let s = format!("{:?}", cfg);
        assert!(s.contains("EvolveConfig"));
    }
}
