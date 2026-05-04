//! Environment configuration.
//!
//! Port of `com.sleepycat.je.EnvironmentConfig`.

use crate::durability::Durability;
use std::path::PathBuf;

/// Configuration for opening a Noxu DB environment.
///
/// Specifies the configuration parameters used to open an environment.
/// Use the builder pattern to configure individual parameters.
///
/// Port of `com.sleepycat.je.EnvironmentConfig`.
#[derive(Debug, Clone)]
pub struct EnvironmentConfig {
    /// Home directory for the environment.
    pub home: PathBuf,

    /// Allow creation of a new environment if it doesn't exist.
    pub allow_create: bool,

    /// Open the environment for transactional use.
    pub transactional: bool,

    /// Open the environment in read-only mode.
    pub read_only: bool,

    /// Cache size in bytes.
    pub cache_size: u64,

    /// Lock timeout in milliseconds.
    pub lock_timeout_ms: u64,

    /// Transaction timeout in milliseconds.
    pub txn_timeout_ms: u64,

    /// Default durability for transactions.
    pub durability: Durability,

    /// Shared cache across environments.
    pub shared_cache: bool,

    /// Logging level.
    pub logging_level: Option<String>,

    /// Whether to run cleaner threads.
    pub run_cleaner: bool,

    /// Whether to run checkpointer threads.
    pub run_checkpointer: bool,

    /// Whether to run evictor threads.
    pub run_evictor: bool,
}

impl EnvironmentConfig {
    /// Creates a new EnvironmentConfig with the given home directory.
    pub fn new(home: PathBuf) -> Self {
        Self {
            home,
            allow_create: false,
            transactional: false,
            read_only: false,
            cache_size: 64 * 1024 * 1024, // 64 MB default
            lock_timeout_ms: 500,
            txn_timeout_ms: 0, // No timeout
            durability: Durability::default(),
            shared_cache: false,
            logging_level: None,
            run_cleaner: true,
            run_checkpointer: true,
            run_evictor: true,
        }
    }

    /// Sets whether to allow creation of a new environment.
    pub fn set_allow_create(&mut self, allow_create: bool) -> &mut Self {
        self.allow_create = allow_create;
        self
    }

    /// Sets whether the environment is transactional.
    pub fn set_transactional(&mut self, transactional: bool) -> &mut Self {
        self.transactional = transactional;
        self
    }

    /// Sets whether the environment is read-only.
    pub fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    /// Sets the cache size in bytes.
    pub fn set_cache_size(&mut self, cache_size: u64) -> &mut Self {
        self.cache_size = cache_size;
        self
    }

    /// Sets the lock timeout in milliseconds.
    pub fn set_lock_timeout(&mut self, timeout_ms: u64) -> &mut Self {
        self.lock_timeout_ms = timeout_ms;
        self
    }

    /// Sets the transaction timeout in milliseconds.
    pub fn set_txn_timeout(&mut self, timeout_ms: u64) -> &mut Self {
        self.txn_timeout_ms = timeout_ms;
        self
    }

    /// Sets the default durability.
    pub fn set_durability(&mut self, durability: Durability) -> &mut Self {
        self.durability = durability;
        self
    }

    /// Sets whether to use a shared cache.
    pub fn set_shared_cache(&mut self, shared_cache: bool) -> &mut Self {
        self.shared_cache = shared_cache;
        self
    }

    /// Sets the logging level.
    pub fn set_logging_level(&mut self, level: String) -> &mut Self {
        self.logging_level = Some(level);
        self
    }

    /// Sets whether to run the cleaner.
    pub fn set_run_cleaner(&mut self, run_cleaner: bool) -> &mut Self {
        self.run_cleaner = run_cleaner;
        self
    }

    /// Sets whether to run the checkpointer.
    pub fn set_run_checkpointer(
        &mut self,
        run_checkpointer: bool,
    ) -> &mut Self {
        self.run_checkpointer = run_checkpointer;
        self
    }

    /// Sets whether to run the evictor.
    pub fn set_run_evictor(&mut self, run_evictor: bool) -> &mut Self {
        self.run_evictor = run_evictor;
        self
    }

    /// Builder-style method to set allow_create.
    pub fn with_allow_create(mut self, allow_create: bool) -> Self {
        self.allow_create = allow_create;
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

    /// Builder-style method to set cache_size.
    pub fn with_cache_size(mut self, cache_size: u64) -> Self {
        self.cache_size = cache_size;
        self
    }

    /// Builder-style method to set durability.
    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }
}

impl Default for EnvironmentConfig {
    fn default() -> Self {
        Self::new(PathBuf::from("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let config = EnvironmentConfig::new(PathBuf::from("/tmp/test"));
        assert_eq!(config.home, PathBuf::from("/tmp/test"));
        assert!(!config.allow_create);
        assert!(!config.transactional);
        assert!(!config.read_only);
        assert_eq!(config.cache_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_set_allow_create() {
        let mut config = EnvironmentConfig::default();
        config.set_allow_create(true);
        assert!(config.allow_create);
    }

    #[test]
    fn test_set_transactional() {
        let mut config = EnvironmentConfig::default();
        config.set_transactional(true);
        assert!(config.transactional);
    }

    #[test]
    fn test_set_read_only() {
        let mut config = EnvironmentConfig::default();
        config.set_read_only(true);
        assert!(config.read_only);
    }

    #[test]
    fn test_set_cache_size() {
        let mut config = EnvironmentConfig::default();
        config.set_cache_size(128 * 1024 * 1024);
        assert_eq!(config.cache_size, 128 * 1024 * 1024);
    }

    #[test]
    fn test_set_lock_timeout() {
        let mut config = EnvironmentConfig::default();
        config.set_lock_timeout(1000);
        assert_eq!(config.lock_timeout_ms, 1000);
    }

    #[test]
    fn test_set_txn_timeout() {
        let mut config = EnvironmentConfig::default();
        config.set_txn_timeout(5000);
        assert_eq!(config.txn_timeout_ms, 5000);
    }

    #[test]
    fn test_set_durability() {
        let mut config = EnvironmentConfig::default();
        config.set_durability(Durability::COMMIT_NO_SYNC);
        assert_eq!(config.durability, Durability::COMMIT_NO_SYNC);
    }

    #[test]
    fn test_with_allow_create() {
        let config = EnvironmentConfig::default().with_allow_create(true);
        assert!(config.allow_create);
    }

    #[test]
    fn test_with_transactional() {
        let config = EnvironmentConfig::default().with_transactional(true);
        assert!(config.transactional);
    }

    #[test]
    fn test_with_read_only() {
        let config = EnvironmentConfig::default().with_read_only(true);
        assert!(config.read_only);
    }

    #[test]
    fn test_with_cache_size() {
        let config =
            EnvironmentConfig::default().with_cache_size(256 * 1024 * 1024);
        assert_eq!(config.cache_size, 256 * 1024 * 1024);
    }

    #[test]
    fn test_with_durability() {
        let config = EnvironmentConfig::default()
            .with_durability(Durability::COMMIT_WRITE_NO_SYNC);
        assert_eq!(config.durability, Durability::COMMIT_WRITE_NO_SYNC);
    }

    #[test]
    fn test_builder_chain() {
        let config = EnvironmentConfig::new(PathBuf::from("/data"))
            .with_allow_create(true)
            .with_transactional(true)
            .with_cache_size(512 * 1024 * 1024);
        assert_eq!(config.home, PathBuf::from("/data"));
        assert!(config.allow_create);
        assert!(config.transactional);
        assert_eq!(config.cache_size, 512 * 1024 * 1024);
    }

    #[test]
    fn test_default() {
        let config = EnvironmentConfig::default();
        assert_eq!(config.home, PathBuf::from("."));
        assert!(!config.allow_create);
    }

    #[test]
    fn test_clone() {
        let config1 = EnvironmentConfig::default().with_allow_create(true);
        let config2 = config1.clone();
        assert_eq!(config1.allow_create, config2.allow_create);
    }

    #[test]
    fn test_daemon_flags() {
        let mut config = EnvironmentConfig::default();
        assert!(config.run_cleaner);
        assert!(config.run_checkpointer);
        assert!(config.run_evictor);

        config.set_run_cleaner(false);
        config.set_run_checkpointer(false);
        config.set_run_evictor(false);

        assert!(!config.run_cleaner);
        assert!(!config.run_checkpointer);
        assert!(!config.run_evictor);
    }

    #[test]
    fn test_shared_cache() {
        let mut config = EnvironmentConfig::default();
        assert!(!config.shared_cache);
        config.set_shared_cache(true);
        assert!(config.shared_cache);
    }

    #[test]
    fn test_logging_level() {
        let mut config = EnvironmentConfig::default();
        assert_eq!(config.logging_level, None);
        config.set_logging_level("DEBUG".to_string());
        assert_eq!(config.logging_level, Some("DEBUG".to_string()));
    }
}
