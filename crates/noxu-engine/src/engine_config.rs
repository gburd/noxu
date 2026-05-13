//! Configuration for the Noxu DB engine.

use std::path::PathBuf;

/// Configuration for the Noxu DB engine.
///
/// Aggregates all configuration that affects environment behavior.
/// This is the primary configuration structure for opening an environment.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Environment home directory.
    ///
    /// All database files are stored in this directory.
    pub home: PathBuf,

    /// Whether to create the environment if it doesn't exist.
    pub allow_create: bool,

    /// Whether transactions are enabled.
    ///
    /// When true, the transaction manager is active and all database
    /// operations can optionally be transactional.
    pub transactional: bool,

    /// Whether the environment is read-only.
    ///
    /// Read-only environments cannot modify the database or log files.
    pub read_only: bool,

    /// Maximum cache size in bytes.
    ///
    /// Controls the memory budget for the in-memory B-tree cache.
    pub cache_size: u64,

    /// Maximum number of lock tables (shards).
    ///
    /// Higher values reduce contention but increase memory overhead.
    pub lock_table_count: u32,

    /// Lock timeout in milliseconds (0 = no timeout).
    ///
    /// Maximum time to wait for a lock before timing out.
    pub lock_timeout_ms: u64,

    /// Transaction timeout in milliseconds (0 = no timeout).
    ///
    /// Maximum time a transaction can run before timing out.
    pub txn_timeout_ms: u64,

    /// Whether to run the evictor daemon.
    ///
    /// The evictor daemon runs in the background evicting nodes
    /// from the cache when memory budget is exceeded.
    pub evictor_enabled: bool,

    /// Whether to run the cleaner daemon.
    ///
    /// The cleaner daemon runs in the background performing log
    /// file garbage collection.
    pub cleaner_enabled: bool,

    /// Whether to run the checkpointer daemon.
    ///
    /// The checkpointer daemon runs in the background performing
    /// periodic checkpoints to bound recovery time.
    pub checkpointer_enabled: bool,

    /// Checkpoint bytes interval.
    ///
    /// A checkpoint is performed after approximately this many bytes
    /// have been written to the log (0 = disabled).
    pub checkpoint_bytes_interval: u64,

    /// Cleaner minimum utilization (0-100).
    ///
    /// Log files below this utilization percentage are candidates
    /// for cleaning.
    pub cleaner_min_utilization: u32,

    /// Cleaner minimum file count.
    ///
    /// The cleaner won't run until at least this many log files exist.
    pub cleaner_min_file_count: u32,

    /// Evictor wakeup interval in milliseconds.
    ///
    /// How often the evictor daemon wakes up to check if eviction is needed.
    pub evictor_wakeup_interval_ms: u64,

    /// Cleaner wakeup interval in milliseconds.
    ///
    /// How often the cleaner daemon wakes up to check if cleaning is needed.
    pub cleaner_wakeup_interval_ms: u64,

    /// Checkpointer wakeup interval in milliseconds.
    ///
    /// How often the checkpointer daemon wakes up to check if checkpoint is needed.
    pub checkpointer_wakeup_interval_ms: u64,

    // -----------------------------------------------------------------------
    // Log parameters (je.log.*)
    // -----------------------------------------------------------------------

    /// Maximum size of each log file in bytes (je.log.fileMax).
    ///
    /// Range: 1 MB – 1 GB. Default: 10 MB.
    pub log_file_max: u64,

    /// Whether the environment uses an in-memory log only (je.log.memOnly).
    ///
    /// When true, no files are written and the log lives entirely in memory.
    pub log_mem_only: bool,

    /// Whether to verify checksums when reading log entries (je.log.checksumRead).
    pub log_checksum_read: bool,

    /// Total bytes to use for log write buffers (je.log.totalBufferBytes).
    ///
    /// 0 means compute automatically from max_memory.
    pub log_total_buffer_bytes: u64,

    // -----------------------------------------------------------------------
    // Evictor parameters (je.evictor.*)
    // -----------------------------------------------------------------------

    /// Number of bytes to evict per eviction pass (je.evictor.evictBytes).
    ///
    /// Default: 512 KB.
    pub evictor_evict_bytes: u64,

    /// Number of evictor core threads (je.evictor.coreThreads).
    pub evictor_core_threads: u32,

    /// Maximum number of evictor threads (je.evictor.maxThreads).
    pub evictor_max_threads: u32,

    /// Number of LRU lists for the evictor (je.evictor.nLRULists).
    ///
    /// More lists reduce contention. Range: 1–32. Default: 4.
    pub evictor_n_lru_lists: u32,

    // -----------------------------------------------------------------------
    // Cleaner parameters (je.cleaner.*)
    // -----------------------------------------------------------------------

    /// Minimum per-file utilization (je.cleaner.minFileUtilization).
    ///
    /// Files below this percentage are cleaned regardless of overall utilization.
    /// Range: 0–50. Default: 5.
    pub cleaner_min_file_utilization: u32,

    /// Number of cleaner threads (je.cleaner.threads).
    ///
    /// Default: 1.
    pub cleaner_threads: u32,

    /// Lock timeout for cleaner operations in milliseconds (je.cleaner.lockTimeout).
    ///
    /// Default: 500 ms.
    pub cleaner_lock_timeout_ms: u64,

    // -----------------------------------------------------------------------
    // Transaction / lock parameters
    // -----------------------------------------------------------------------

    /// If true, all transactions use serializable isolation (je.txn.serializableIsolation).
    pub txn_serializable_isolation: bool,

    /// If true, deadlock detection is enabled (je.lock.deadlockDetect).
    pub lock_deadlock_detect: bool,

    // -----------------------------------------------------------------------
    // Checkpointer parameters
    // -----------------------------------------------------------------------

    /// If true, the checkpointer runs at high priority (je.checkpointer.highPriority).
    pub checkpointer_high_priority: bool,
}

impl EngineConfig {
    /// Create a new EngineConfig with the given home directory.
    pub fn new(home: impl Into<PathBuf>) -> Self {
        Self { home: home.into(), ..Default::default() }
    }

    /// Set whether to create the environment if it doesn't exist.
    pub fn allow_create(mut self, allow: bool) -> Self {
        self.allow_create = allow;
        self
    }

    /// Set whether transactions are enabled.
    pub fn transactional(mut self, enabled: bool) -> Self {
        self.transactional = enabled;
        self
    }

    /// Set whether the environment is read-only.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set the maximum cache size in bytes.
    pub fn cache_size(mut self, size: u64) -> Self {
        self.cache_size = size;
        self
    }

    /// Set the number of lock table shards.
    pub fn lock_table_count(mut self, count: u32) -> Self {
        self.lock_table_count = count;
        self
    }

    /// Set the lock timeout in milliseconds.
    pub fn lock_timeout_ms(mut self, timeout: u64) -> Self {
        self.lock_timeout_ms = timeout;
        self
    }

    /// Set the transaction timeout in milliseconds.
    pub fn txn_timeout_ms(mut self, timeout: u64) -> Self {
        self.txn_timeout_ms = timeout;
        self
    }

    /// Enable or disable the evictor daemon.
    pub fn evictor_enabled(mut self, enabled: bool) -> Self {
        self.evictor_enabled = enabled;
        self
    }

    /// Enable or disable the cleaner daemon.
    pub fn cleaner_enabled(mut self, enabled: bool) -> Self {
        self.cleaner_enabled = enabled;
        self
    }

    /// Enable or disable the checkpointer daemon.
    pub fn checkpointer_enabled(mut self, enabled: bool) -> Self {
        self.checkpointer_enabled = enabled;
        self
    }

    /// Set the checkpoint bytes interval.
    pub fn checkpoint_bytes_interval(mut self, bytes: u64) -> Self {
        self.checkpoint_bytes_interval = bytes;
        self
    }

    /// Set the cleaner minimum utilization percentage.
    pub fn cleaner_min_utilization(mut self, percent: u32) -> Self {
        self.cleaner_min_utilization = percent.min(100);
        self
    }

    /// Set the evictor wakeup interval in milliseconds.
    pub fn evictor_wakeup_interval_ms(mut self, ms: u64) -> Self {
        self.evictor_wakeup_interval_ms = ms;
        self
    }

    /// Set the cleaner wakeup interval in milliseconds.
    pub fn cleaner_wakeup_interval_ms(mut self, ms: u64) -> Self {
        self.cleaner_wakeup_interval_ms = ms;
        self
    }

    /// Set the checkpointer wakeup interval in milliseconds.
    pub fn checkpointer_wakeup_interval_ms(mut self, ms: u64) -> Self {
        self.checkpointer_wakeup_interval_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // Log builder methods
    // -----------------------------------------------------------------------

    /// Set the maximum log file size in bytes (je.log.fileMax).
    pub fn log_file_max(mut self, bytes: u64) -> Self {
        self.log_file_max = bytes;
        self
    }

    /// Enable or disable in-memory-only log (je.log.memOnly).
    pub fn log_mem_only(mut self, mem_only: bool) -> Self {
        self.log_mem_only = mem_only;
        self
    }

    /// Enable or disable log checksum verification on read (je.log.checksumRead).
    pub fn log_checksum_read(mut self, enabled: bool) -> Self {
        self.log_checksum_read = enabled;
        self
    }

    /// Set total log buffer bytes (je.log.totalBufferBytes). 0 = auto.
    pub fn log_total_buffer_bytes(mut self, bytes: u64) -> Self {
        self.log_total_buffer_bytes = bytes;
        self
    }

    // -----------------------------------------------------------------------
    // Evictor builder methods
    // -----------------------------------------------------------------------

    /// Set the number of bytes to evict per pass (je.evictor.evictBytes).
    pub fn evictor_evict_bytes(mut self, bytes: u64) -> Self {
        self.evictor_evict_bytes = bytes;
        self
    }

    /// Set the number of evictor core threads (je.evictor.coreThreads).
    pub fn evictor_core_threads(mut self, n: u32) -> Self {
        self.evictor_core_threads = n;
        self
    }

    /// Set the maximum number of evictor threads (je.evictor.maxThreads).
    pub fn evictor_max_threads(mut self, n: u32) -> Self {
        self.evictor_max_threads = n;
        self
    }

    /// Set the number of LRU lists for the evictor (je.evictor.nLRULists).
    pub fn evictor_n_lru_lists(mut self, n: u32) -> Self {
        self.evictor_n_lru_lists = n.clamp(1, 32);
        self
    }

    // -----------------------------------------------------------------------
    // Cleaner builder methods
    // -----------------------------------------------------------------------

    /// Set the per-file minimum utilization (je.cleaner.minFileUtilization).
    pub fn cleaner_min_file_utilization(mut self, percent: u32) -> Self {
        self.cleaner_min_file_utilization = percent.min(50);
        self
    }

    /// Set the number of cleaner threads (je.cleaner.threads).
    pub fn cleaner_threads(mut self, n: u32) -> Self {
        self.cleaner_threads = n.max(1);
        self
    }

    /// Set the cleaner lock timeout in milliseconds (je.cleaner.lockTimeout).
    pub fn cleaner_lock_timeout_ms(mut self, ms: u64) -> Self {
        self.cleaner_lock_timeout_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // Transaction / lock builder methods
    // -----------------------------------------------------------------------

    /// Enable or disable serializable isolation for all transactions.
    pub fn txn_serializable_isolation(mut self, enabled: bool) -> Self {
        self.txn_serializable_isolation = enabled;
        self
    }

    /// Enable or disable automatic deadlock detection.
    pub fn lock_deadlock_detect(mut self, enabled: bool) -> Self {
        self.lock_deadlock_detect = enabled;
        self
    }

    // -----------------------------------------------------------------------
    // Checkpointer builder methods
    // -----------------------------------------------------------------------

    /// Enable or disable high-priority checkpointing.
    pub fn checkpointer_high_priority(mut self, enabled: bool) -> Self {
        self.checkpointer_high_priority = enabled;
        self
    }

    /// Validate the configuration.
    ///
    /// Returns an error if any configuration parameters are invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.cache_size < 1024 * 1024 {
            return Err("cache_size must be at least 1 MB".to_string());
        }

        if self.lock_table_count == 0 {
            return Err("lock_table_count must be at least 1".to_string());
        }

        if self.cleaner_min_utilization > 100 {
            return Err("cleaner_min_utilization must be 0-100".to_string());
        }

        if self.cleaner_min_file_utilization > 50 {
            return Err(
                "cleaner_min_file_utilization must be 0-50".to_string(),
            );
        }

        if self.cleaner_threads == 0 {
            return Err("cleaner_threads must be at least 1".to_string());
        }

        if !(1..=10_000_000).contains(&self.log_file_max) {
            return Err(
                "log_file_max must be between 1 MB and 1 GB".to_string(),
            );
        }

        if self.evictor_n_lru_lists == 0 || self.evictor_n_lru_lists > 32 {
            return Err(
                "evictor_n_lru_lists must be between 1 and 32".to_string(),
            );
        }

        if self.evictor_max_threads == 0 {
            return Err("evictor_max_threads must be at least 1".to_string());
        }

        if self.read_only && (self.cleaner_enabled || self.checkpointer_enabled)
        {
            return Err(
                "cleaner and checkpointer cannot be enabled in read-only mode"
                    .to_string(),
            );
        }

        Ok(())
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            home: PathBuf::from("."),
            allow_create: true,
            transactional: true,
            read_only: false,
            cache_size: 64 * 1024 * 1024, // 64 MB
            lock_table_count: 16,
            lock_timeout_ms: 500,  // 500 ms — matches default
            txn_timeout_ms: 0,     // 0 = no timeout — matches default
            evictor_enabled: true,
            cleaner_enabled: true,
            checkpointer_enabled: true,
            checkpoint_bytes_interval: 20_000_000, // 20 MB — matches 
            cleaner_min_utilization: 50,           // 50% — matches 
            cleaner_min_file_count: 5,
            evictor_wakeup_interval_ms: 5000, // 5 seconds
            cleaner_wakeup_interval_ms: 10_000, // 10 s — matches 
            checkpointer_wakeup_interval_ms: 0, // 0 = bytes-based — matches 
            // Log defaults — match 
            log_file_max: 10_000_000,      // 10 MB
            log_mem_only: false,
            log_checksum_read: true,
            log_total_buffer_bytes: 0,     // auto-computed
            // Evictor defaults — match 
            evictor_evict_bytes: 524_288,  // 512 KB
            evictor_core_threads: 1,
            evictor_max_threads: 10,
            evictor_n_lru_lists: 4,
            // Cleaner defaults — match 
            cleaner_min_file_utilization: 5, // 5%
            cleaner_threads: 1,
            cleaner_lock_timeout_ms: 500,    // 500 ms
            // Txn/lock defaults — match 
            txn_serializable_isolation: false,
            lock_deadlock_detect: true,
            // Checkpointer defaults — match 
            checkpointer_high_priority: false,
        }
    }
}

#[cfg(test)]
#[expect(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = EngineConfig::default();
        assert_eq!(config.home, PathBuf::from("."));
        assert!(config.allow_create);
        assert!(config.transactional);
        assert!(!config.read_only);
        assert_eq!(config.cache_size, 64 * 1024 * 1024);
        assert_eq!(config.lock_table_count, 16);
        assert!(config.evictor_enabled);
        assert!(config.cleaner_enabled);
        assert!(config.checkpointer_enabled);
        // Check -matched defaults
        assert_eq!(config.lock_timeout_ms, 500);
        assert_eq!(config.txn_timeout_ms, 0);
        assert_eq!(config.cleaner_min_utilization, 50);
        assert_eq!(config.cleaner_min_file_utilization, 5);
        assert_eq!(config.cleaner_threads, 1);
        assert_eq!(config.checkpoint_bytes_interval, 20_000_000);
        assert_eq!(config.log_file_max, 10_000_000);
        assert!(!config.log_mem_only);
        assert!(config.log_checksum_read);
        assert_eq!(config.log_total_buffer_bytes, 0);
        assert_eq!(config.evictor_evict_bytes, 524_288);
        assert_eq!(config.evictor_core_threads, 1);
        assert_eq!(config.evictor_max_threads, 10);
        assert_eq!(config.evictor_n_lru_lists, 4);
        assert!(!config.txn_serializable_isolation);
        assert!(config.lock_deadlock_detect);
        assert!(!config.checkpointer_high_priority);
    }

    #[test]
    fn test_new_config() {
        let config = EngineConfig::new("/tmp/mydb");
        assert_eq!(config.home, PathBuf::from("/tmp/mydb"));
        // Other fields should be default
        assert!(config.allow_create);
        assert!(config.transactional);
    }

    #[test]
    fn test_builder_pattern() {
        let config = EngineConfig::new("/data/db")
            .allow_create(false)
            .transactional(true)
            .read_only(false)
            .cache_size(128 * 1024 * 1024)
            .lock_table_count(32)
            .lock_timeout_ms(10000)
            .txn_timeout_ms(20000)
            .evictor_enabled(true)
            .cleaner_enabled(false)
            .checkpointer_enabled(true)
            .checkpoint_bytes_interval(50_000_000)
            .cleaner_min_utilization(60)
            .log_file_max(20_000_000)
            .log_mem_only(false)
            .log_checksum_read(true)
            .evictor_evict_bytes(1_048_576)
            .evictor_core_threads(2)
            .evictor_max_threads(8)
            .evictor_n_lru_lists(8)
            .cleaner_min_file_utilization(10)
            .cleaner_threads(2)
            .cleaner_lock_timeout_ms(1000)
            .txn_serializable_isolation(true)
            .lock_deadlock_detect(true)
            .checkpointer_high_priority(true);

        assert_eq!(config.home, PathBuf::from("/data/db"));
        assert!(!config.allow_create);
        assert!(config.transactional);
        assert!(!config.read_only);
        assert_eq!(config.cache_size, 128 * 1024 * 1024);
        assert_eq!(config.lock_table_count, 32);
        assert_eq!(config.lock_timeout_ms, 10000);
        assert_eq!(config.txn_timeout_ms, 20000);
        assert!(config.evictor_enabled);
        assert!(!config.cleaner_enabled);
        assert!(config.checkpointer_enabled);
        assert_eq!(config.checkpoint_bytes_interval, 50_000_000);
        assert_eq!(config.cleaner_min_utilization, 60);
        assert_eq!(config.log_file_max, 20_000_000);
        assert!(!config.log_mem_only);
        assert!(config.log_checksum_read);
        assert_eq!(config.evictor_evict_bytes, 1_048_576);
        assert_eq!(config.evictor_core_threads, 2);
        assert_eq!(config.evictor_max_threads, 8);
        assert_eq!(config.evictor_n_lru_lists, 8);
        assert_eq!(config.cleaner_min_file_utilization, 10);
        assert_eq!(config.cleaner_threads, 2);
        assert_eq!(config.cleaner_lock_timeout_ms, 1000);
        assert!(config.txn_serializable_isolation);
        assert!(config.lock_deadlock_detect);
        assert!(config.checkpointer_high_priority);
    }

    #[test]
    fn test_validate_valid_config() {
        let config = EngineConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_cache_too_small() {
        let config = EngineConfig::default().cache_size(1024);
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cache_size"));
    }

    #[test]
    fn test_validate_zero_lock_tables() {
        let config = EngineConfig::default().lock_table_count(0);
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("lock_table_count"));
    }

    #[test]
    fn test_validate_invalid_utilization() {
        let mut config = EngineConfig::default();
        config.cleaner_min_utilization = 150;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("utilization"));
    }

    #[test]
    fn test_validate_readonly_conflicts() {
        let config =
            EngineConfig::default().read_only(true).cleaner_enabled(true);
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("read-only"));

        let config =
            EngineConfig::default().read_only(true).checkpointer_enabled(true);
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("read-only"));
    }

    #[test]
    fn test_cleaner_utilization_clamped() {
        let config = EngineConfig::default().cleaner_min_utilization(150);
        assert_eq!(config.cleaner_min_utilization, 100);

        let config = EngineConfig::default().cleaner_min_utilization(50);
        assert_eq!(config.cleaner_min_utilization, 50);
    }

    #[test]
    fn test_readonly_config() {
        let config = EngineConfig::new("/db")
            .read_only(true)
            .cleaner_enabled(false)
            .checkpointer_enabled(false);
        assert!(config.validate().is_ok());
        assert!(config.read_only);
        assert!(!config.cleaner_enabled);
        assert!(!config.checkpointer_enabled);
    }
}
