//! Main engine implementation for Noxu DB.

use crate::daemon_manager::DaemonManager;
use crate::engine_config::EngineConfig;
use crate::env_stats::{
    EnvironmentStats, EvictorStatsSnapshot, LockStatsSnapshot, LogStatsSnapshot,
    TxnStatsSnapshot,
};
use crate::error::{EngineError, Result};
use noxu_cleaner::{CleanResult, Cleaner};
use noxu_dbi::EnvironmentImpl;
use noxu_evictor::{Arbiter, EvictResult, EvictionSource, Evictor};
use noxu_recovery::{
    CheckpointConfig, CheckpointResult, Checkpointer, RecoveryManager,
    log_scanner::InMemoryLogScanner,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// The Noxu DB engine.
///
/// Wires together all internal subsystems:
/// - EnvironmentImpl (dbi layer)
/// - Evictor (cache management)
/// - Cleaner (log GC)
/// - Checkpointer (durability)
/// - DaemonManager (background threads)
///
/// This is the internal engine that `noxu-db` wraps. It coordinates
/// all subsystems and provides a unified interface for database operations.
pub struct Engine {
    /// Engine configuration.
    config: EngineConfig,

    /// The internal environment implementation (dbi layer).
    env_impl: Arc<Mutex<EnvironmentImpl>>,

    /// The evictor for cache management.
    evictor: Arc<Evictor>,

    /// The cleaner for log garbage collection.
    cleaner: Arc<Cleaner>,

    /// The checkpointer for durability.
    checkpointer: Arc<Checkpointer>,

    /// The daemon manager for background threads.
    daemon_manager: Mutex<DaemonManager>,

    /// Whether the engine is open.
    open: AtomicBool,

    /// Memory budget tracker (shared with arbiter).
    cache_usage: Arc<AtomicI64>,
}

impl Engine {
    /// Opens a Noxu DB environment with the given configuration.
    ///
    /// This is the main entry point for opening an environment. It:
    /// 1. Validates configuration
    /// 2. Creates the environment directory if needed
    /// 3. Creates EnvironmentImpl (dbi layer)
    /// 4. Creates Evictor, Cleaner, Checkpointer
    /// 5. Runs recovery (RecoveryManager)
    /// 6. Starts daemon threads
    /// 7. Returns the Engine
    ///
    /// # Errors
    /// Returns an error if:
    /// - Configuration is invalid
    /// - Environment directory cannot be created
    /// - Recovery fails
    /// - Any subsystem initialization fails
    pub fn open(config: EngineConfig) -> Result<Self> {
        // Validate configuration
        config.validate().map_err(EngineError::InvalidConfig)?;

        // Create environment directory if needed
        if config.allow_create && !config.home.exists() {
            std::fs::create_dir_all(&config.home)?;
        }

        // Verify directory exists
        if !config.home.exists() {
            return Err(EngineError::InvalidConfig(format!(
                "environment directory does not exist: {}",
                config.home.display()
            )));
        }

        // Create EnvironmentImpl (dbi layer)
        let env_impl = EnvironmentImpl::new(
            &config.home,
            config.read_only,
            config.transactional,
        )?;
        let env_impl = Arc::new(Mutex::new(env_impl));

        // Create cache usage tracker
        let cache_usage = Arc::new(AtomicI64::new(0));

        // Create arbiter for eviction decisions
        let arbiter = Arbiter::new(
            config.cache_size as i64,
            Arc::clone(&cache_usage),
            (config.cache_size / 10) as i64, // 10% eviction pledge
            (config.cache_size / 5) as i64,  // 20% critical threshold
        );

        // Create evictor
        let evictor = Arc::new(Evictor::new(arbiter, 100, false));

        // Create cleaner
        let cleaner = Arc::new(Cleaner::new(
            config.cleaner_min_utilization,
            config.cleaner_min_file_count,
            0, // min age
        ));

        // Create checkpointer
        let checkpoint_config = CheckpointConfig::default()
            .bytes_interval(config.checkpoint_bytes_interval);
        let checkpointer = Arc::new(Checkpointer::new(checkpoint_config));

        // Run recovery using an empty in-memory scanner (no log files yet on
        // fresh open; a real LogFileScanner will replace this once the log
        // manager is wired through the engine).
        let mut recovery_manager = RecoveryManager::new();
        let mut scanner = InMemoryLogScanner::new();
        log::info!("Running recovery...");
        let recovery_info = recovery_manager.recover(&mut scanner, None, true)?;
        log::info!(
            "Recovery completed: last_used_lsn={:?}, checkpoint_start_lsn={:?}",
            recovery_info.last_used_lsn,
            recovery_info.checkpoint_start_lsn
        );

        // Create daemon manager
        let mut daemon_manager = DaemonManager::new(&config);

        // Start daemons
        daemon_manager.start_daemons(
            Arc::clone(&evictor),
            Arc::clone(&cleaner),
            Arc::clone(&checkpointer),
        );

        let engine = Engine {
            config,
            env_impl,
            evictor,
            cleaner,
            checkpointer,
            daemon_manager: Mutex::new(daemon_manager),
            open: AtomicBool::new(true),
            cache_usage,
        };

        log::info!("Engine opened successfully");
        Ok(engine)
    }

    /// Closes the environment.
    ///
    /// Performs orderly shutdown:
    /// 1. Stop daemon threads
    /// 2. Flush final checkpoint
    /// 3. Close EnvironmentImpl
    ///
    /// After close(), the Engine cannot be used.
    pub fn close(&self) -> Result<()> {
        if !self.is_open() {
            return Err(EngineError::EnvironmentClosed);
        }

        log::info!("Closing engine...");

        // Mark as closed
        self.open.store(false, Ordering::Relaxed);

        // Stop daemon threads
        self.daemon_manager.lock().shutdown();

        // Flush final checkpoint
        if !self.config.read_only
            && self.config.checkpointer_enabled
            && let Err(e) = self.checkpointer.do_checkpoint("close")
        {
            log::warn!("Final checkpoint failed: {}", e);
        }

        // Close environment impl
        // (EnvironmentImpl doesn't have explicit close yet - would be added in full implementation)

        log::info!("Engine closed successfully");
        Ok(())
    }

    /// Returns whether the engine is open.
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Relaxed)
    }

    /// Gets a reference to the EnvironmentImpl.
    pub fn get_env_impl(&self) -> &Arc<Mutex<EnvironmentImpl>> {
        &self.env_impl
    }

    /// Gets a reference to the Evictor.
    pub fn get_evictor(&self) -> &Arc<Evictor> {
        &self.evictor
    }

    /// Gets a reference to the Cleaner.
    pub fn get_cleaner(&self) -> &Arc<Cleaner> {
        &self.cleaner
    }

    /// Gets a reference to the Checkpointer.
    pub fn get_checkpointer(&self) -> &Arc<Checkpointer> {
        &self.checkpointer
    }

    /// Gets the engine configuration.
    pub fn get_config(&self) -> &EngineConfig {
        &self.config
    }

    /// Performs a checkpoint.
    ///
    /// # Arguments
    /// * `invoker` - Description of who invoked the checkpoint (for logging)
    ///
    /// # Returns
    /// Information about the checkpoint that was performed.
    pub fn checkpoint(&self, invoker: &str) -> Result<CheckpointResult> {
        if !self.is_open() {
            return Err(EngineError::EnvironmentClosed);
        }

        if self.config.read_only {
            return Err(EngineError::InvalidConfig(
                "cannot checkpoint read-only environment".to_string(),
            ));
        }

        let result = self.checkpointer.do_checkpoint(invoker)?;
        Ok(result)
    }

    /// Performs log cleaning.
    ///
    /// # Arguments
    /// * `n_files` - Maximum number of files to clean
    ///
    /// # Returns
    /// Information about the cleaning operation.
    pub fn clean(&self, n_files: u32) -> Result<CleanResult> {
        if !self.is_open() {
            return Err(EngineError::EnvironmentClosed);
        }

        if self.config.read_only {
            return Err(EngineError::InvalidConfig(
                "cannot clean read-only environment".to_string(),
            ));
        }

        let result = self
            .cleaner
            .do_clean(n_files, false)
            .map_err(EngineError::DatabaseError)?;
        Ok(result)
    }

    /// Performs cache eviction.
    ///
    /// # Returns
    /// Information about the eviction operation.
    pub fn evict(&self) -> Result<EvictResult> {
        if !self.is_open() {
            return Err(EngineError::EnvironmentClosed);
        }

        let result = self.evictor.do_evict(EvictionSource::Manual);
        Ok(result)
    }

    /// Collects environment statistics.
    ///
    /// Returns a snapshot of statistics from all subsystems.
    pub fn get_stats(&self) -> EnvironmentStats {
        let evictor_stats = self.evictor.get_stats();
        let cleaner_stats = self.cleaner.get_stats();
        let checkpoint_stats = self.checkpointer.get_stats();

        let env_impl = self.env_impl.lock();
        let n_databases = env_impl.n_databases() as u32;
        let log_stats = env_impl.get_log_manager().map(|lm| lm.get_stats());
        let lock_stats = env_impl.get_lock_manager().get_stats();
        let txn_stats = env_impl.get_txn_manager().get_stats();
        let throughput = env_impl.get_throughput_snapshot();
        drop(env_impl);

        EnvironmentStats {
            cache_size: self.config.cache_size,
            cache_usage: self.cache_usage.load(Ordering::Relaxed) as u64,
            n_databases,
            evictor: EvictorStatsSnapshot::from(evictor_stats),
            log: log_stats.as_ref().map(LogStatsSnapshot::from).unwrap_or_default(),
            lock: LockStatsSnapshot {
                n_lock_tables: self.config.lock_table_count as u64,
                ..LockStatsSnapshot::from(&lock_stats)
            },
            txn: TxnStatsSnapshot::from(&txn_stats),
            cleaner: cleaner_stats.snapshot(),
            checkpoint: checkpoint_stats.snapshot(),
            throughput,
        }
    }

    /// Gets the current cache usage in bytes.
    pub fn get_cache_usage(&self) -> u64 {
        self.cache_usage.load(Ordering::Relaxed) as u64
    }

    /// Gets the cache budget in bytes.
    pub fn get_cache_budget(&self) -> u64 {
        self.config.cache_size
    }

    /// Checks if the cache is over budget.
    pub fn is_cache_over_budget(&self) -> bool {
        self.get_cache_usage() > self.get_cache_budget()
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.is_open()
            && let Err(e) = self.close()
        {
            log::error!("Error closing engine in drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_config() -> (TempDir, EngineConfig) {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(dir.path())
            .allow_create(true)
            .cache_size(10 * 1024 * 1024)
            .evictor_wakeup_interval_ms(100)
            .cleaner_wakeup_interval_ms(100)
            .checkpointer_wakeup_interval_ms(100);
        (dir, config)
    }

    #[test]
    fn test_engine_open_and_close() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();
        assert!(engine.is_open());

        engine.close().unwrap();
        assert!(!engine.is_open());
    }

    #[test]
    fn test_engine_open_creates_directory() {
        let dir = TempDir::new().unwrap();
        let home = dir.path().join("newdb");
        let config = EngineConfig::new(&home).allow_create(true);

        assert!(!home.exists());
        let engine = Engine::open(config).unwrap();
        assert!(home.exists());
        assert!(engine.is_open());
    }

    #[test]
    fn test_engine_open_fails_without_create() {
        let dir = TempDir::new().unwrap();
        let home = dir.path().join("nonexistent");
        let config = EngineConfig::new(&home).allow_create(false);

        let result = Engine::open(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_engine_invalid_config() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(dir.path()).cache_size(1024); // Too small

        let result = Engine::open(config);
        assert!(result.is_err());
        match result {
            Err(EngineError::InvalidConfig(_)) => { /* Expected */ }
            _ => panic!("Expected InvalidConfig error"),
        }
    }

    #[test]
    fn test_engine_double_close() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();

        engine.close().unwrap();
        let result = engine.close();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), EngineError::EnvironmentClosed));
    }

    #[test]
    fn test_engine_get_subsystems() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();

        assert!(engine.get_env_impl().lock().is_open());
        assert_eq!(
            engine.get_evictor().get_lru_sizes().0
                + engine.get_evictor().get_lru_sizes().1,
            0
        );
        // Cleaner and checkpointer don't have simple accessors but we can verify they exist
        let _ = engine.get_cleaner();
        let _ = engine.get_checkpointer();
    }

    #[test]
    fn test_engine_checkpoint() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();

        let result = engine.checkpoint("test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_engine_checkpoint_readonly() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(dir.path())
            .allow_create(true)
            .cache_size(10 * 1024 * 1024)
            .read_only(true)
            .cleaner_enabled(false)
            .checkpointer_enabled(false)
            .evictor_wakeup_interval_ms(100);
        let engine = Engine::open(config).unwrap();

        let result = engine.checkpoint("test");
        assert!(result.is_err());
    }

    #[test]
    fn test_engine_clean() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();

        let result = engine.clean(5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_engine_clean_readonly() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(dir.path())
            .allow_create(true)
            .cache_size(10 * 1024 * 1024)
            .read_only(true)
            .cleaner_enabled(false)
            .checkpointer_enabled(false)
            .evictor_wakeup_interval_ms(100);
        let engine = Engine::open(config).unwrap();

        let result = engine.clean(5);
        assert!(result.is_err());
    }

    #[test]
    fn test_engine_evict() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();

        let result = engine.evict();
        assert!(result.is_ok());
        let _evict_result = result.unwrap();
        // May or may not evict anything depending on cache state
    }

    #[test]
    fn test_engine_get_stats() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();

        let stats = engine.get_stats();
        assert_eq!(stats.cache_size, 10 * 1024 * 1024);
        assert_eq!(stats.lock.n_lock_tables, 16);
    }

    #[test]
    fn test_engine_cache_budget() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();

        assert_eq!(engine.get_cache_budget(), 10 * 1024 * 1024);
        assert_eq!(engine.get_cache_usage(), 0); // Empty initially
        assert!(!engine.is_cache_over_budget());
    }

    #[test]
    fn test_engine_operations_after_close() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();
        engine.close().unwrap();

        assert!(engine.checkpoint("test").is_err());
        assert!(engine.clean(5).is_err());
        assert!(engine.evict().is_err());
    }

    #[test]
    fn test_engine_drop_closes() {
        let (_dir, config) = temp_config();
        let engine = Engine::open(config).unwrap();
        assert!(engine.is_open());
        drop(engine);
        // Engine should have closed cleanly in drop
    }

    #[test]
    fn test_engine_readonly() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(dir.path())
            .allow_create(true)
            .cache_size(10 * 1024 * 1024)
            .read_only(true)
            .cleaner_enabled(false)
            .checkpointer_enabled(false)
            .evictor_wakeup_interval_ms(100);
        let engine = Engine::open(config).unwrap();

        assert!(engine.is_open());
        assert!(engine.get_config().read_only);

        // Read-only operations should work
        let stats = engine.get_stats();
        assert_eq!(stats.cache_size, 10 * 1024 * 1024);

        // Write operations should fail
        assert!(engine.checkpoint("test").is_err());
        assert!(engine.clean(5).is_err());
    }
}
