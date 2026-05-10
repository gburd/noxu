//! Environment configuration.
//!
//! Mirrors the JE `EnvironmentConfig` / `EnvironmentMutableConfig` classes.
//! Parameters are grouped by subsystem: log, B-tree, cleaner, checkpointer,
//! evictor, locking, and transaction.

use crate::durability::Durability;
use std::path::PathBuf;

/// Configuration for opening a Noxu DB environment.
///
/// Specifies the configuration parameters used to open an environment.
/// Use the builder pattern to configure individual parameters.
///
/// Matches `EnvironmentConfig` from JE (147 total params in JE; this covers
/// the ~55 most operationally significant ones).
#[derive(Debug, Clone)]
pub struct EnvironmentConfig {
    // -----------------------------------------------------------------------
    // Core
    // -----------------------------------------------------------------------

    /// Home directory for the environment.
    pub home: PathBuf,

    /// Allow creation of a new environment if it doesn't exist.
    /// JE: `EnvironmentConfig.setAllowCreate()`
    pub allow_create: bool,

    /// Open the environment for transactional use.
    /// JE: `EnvironmentConfig.setTransactional()`
    pub transactional: bool,

    /// Open the environment in read-only mode.
    /// JE: `EnvironmentConfig.setReadOnly()`
    pub read_only: bool,

    /// Shared cache across environments.
    /// JE: `EnvironmentConfig.setSharedCache()`
    pub shared_cache: bool,

    /// Logging level.
    pub logging_level: Option<String>,

    /// Force a checkpoint after recovery completes.
    /// JE: `ENV_RECOVERY_FORCE_CHECKPOINT` / default false.
    pub env_recovery_force_checkpoint: bool,

    /// Maximum disk space the environment may use in bytes.  0 = unlimited.
    /// JE: `MAX_DISK` / default 0.
    pub max_disk: u64,

    // -----------------------------------------------------------------------
    // Memory / cache
    // -----------------------------------------------------------------------

    /// Maximum number of bytes to use for the B-tree cache.
    /// Replaces the earlier `cache_size` field.
    /// JE: `EnvironmentConfig.setCacheSize()` / `MAX_MEMORY`
    /// Default: 64 MiB.
    pub cache_size: u64,

    /// Cache size as a percentage of the JVM heap (0 = use `cache_size` abs value).
    /// JE: `EnvironmentConfig.setCachePercent()` / `CACHE_PERCENT`
    /// Default: 0 (use `cache_size`).
    pub cache_percent: u32,

    // -----------------------------------------------------------------------
    // Log / I-O
    // -----------------------------------------------------------------------

    /// Maximum size of a single log file in bytes.
    /// JE: `EnvironmentConfig.setConfigParam(LOG_FILE_MAX)` / default 10 MiB.
    pub log_file_max_bytes: u64,

    /// Number of cached open file handles (LRU eviction when full).
    /// JE: `LOG_FILE_CACHE_SIZE` / default 100.
    pub log_file_cache_size: usize,

    /// Validate entry checksums when reading the log (read-path only).
    /// JE: `LOG_CHECKSUM_READ` / default true.
    pub log_checksum_read: bool,

    /// Timeout for a single `fdatasync` call in milliseconds.
    /// JE: `LOG_FSYNC_TIMEOUT` / default 500_000 ms (8.3 min).
    pub log_fsync_timeout_ms: u64,

    /// Number of write buffers in the log buffer pool.
    /// JE: `LOG_NUM_BUFFERS` / default 3.
    pub log_num_buffers: usize,

    /// Total bytes across all log buffers (`log_num_buffers` × per-buffer size).
    /// JE: `LOG_TOTAL_BUFFER_BYTES` / default 7 MiB (≈ 2.3 MiB each × 3).
    pub log_total_buffer_bytes: u64,

    /// Size of the fault-in read buffer for random BIN reads.
    /// JE: `LOG_FAULT_READ_SIZE` / default 2 KiB.
    pub log_fault_read_size: usize,

    /// Group-commit waiter threshold: minimum number of concurrent commit
    /// callers before the leader fsyncs immediately.  0 = disabled.
    /// JE: `LOG_GROUP_COMMIT_THRESHOLD`
    pub log_group_commit_threshold: usize,

    /// Group-commit interval in milliseconds: maximum time the leader waits
    /// for additional concurrent callers before fsyncing.  0 = disabled.
    /// JE: `LOG_GROUP_COMMIT_INTERVAL`
    pub log_group_commit_interval_ms: u64,

    // -----------------------------------------------------------------------
    // B-tree
    // -----------------------------------------------------------------------

    /// Maximum percentage of BIN entries that may be in a delta before a full
    /// BIN is written (0–100).
    /// JE: `TREE_MAX_DELTA` / default 25.
    pub tree_max_delta: u8,

    /// Whether to write BIN-delta log entries (partial BIN updates).
    /// JE: `TREE_BIN_DELTA` / default true.
    pub tree_bin_delta: bool,

    /// Whether to run the background INCompressor daemon.
    /// JE: `ENV_RUN_IN_COMPRESSOR` / default true.
    pub run_in_compressor: bool,

    /// INCompressor wakeup interval in milliseconds.
    /// JE: `COMPRESSOR_WAKEUP_INTERVAL` / default 5000 ms.
    pub in_compressor_wakeup_interval_ms: u64,

    // -----------------------------------------------------------------------
    // Cleaner
    // -----------------------------------------------------------------------

    /// Whether to run the background cleaner daemon.
    /// JE: `ENV_RUN_CLEANER` / default true.
    pub run_cleaner: bool,

    /// Minimum log utilization percentage below which cleaning is triggered.
    /// JE: `CLEANER_MIN_UTILIZATION` / default 50.
    pub cleaner_min_utilization: u8,

    /// Minimum per-file utilization percentage; files below this are always
    /// candidates regardless of overall utilization.
    /// JE: `CLEANER_MIN_FILE_UTILIZATION` / default 5.
    pub cleaner_min_file_utilization: u8,

    /// Number of background cleaner threads.
    /// JE: `CLEANER_THREADS` / default 1.
    pub cleaner_threads: u32,

    /// Minimum number of log files that must exist before cleaning begins.
    /// JE: `CLEANER_MIN_FILES_TO_CLEAN` / default 2.
    pub cleaner_min_file_count: u32,

    /// Minimum age of a log file (in checkpoints) before it becomes a
    /// cleaning candidate.
    /// JE: `CLEANER_MIN_AGE` / default 2.
    pub cleaner_min_age: u32,

    /// Whether TTL-based record expiration is tracked by the cleaner.
    /// JE: `CLEANER_EXPIRATION_ENABLED` / default false.
    pub cleaner_expiration_enabled: bool,

    // -----------------------------------------------------------------------
    // Checkpointer
    // -----------------------------------------------------------------------

    /// Whether to run the background checkpointer daemon.
    /// JE: `ENV_RUN_CHECKPOINTER` / default true.
    pub run_checkpointer: bool,

    /// Number of bytes written between automatic checkpoints.
    /// JE: `CHECKPOINTER_BYTES_INTERVAL` / default 20 MiB.
    pub checkpointer_bytes_interval: u64,

    /// Minimum time between automatic checkpoints in seconds (0 = disabled).
    /// JE: `CHECKPOINTER_HIGH_PRIORITY` (relates to interval) / default 0.
    pub checkpointer_min_interval_secs: u64,

    // -----------------------------------------------------------------------
    // Evictor
    // -----------------------------------------------------------------------

    /// Whether to run the background evictor daemon.
    /// JE: `ENV_RUN_EVICTOR` / default true.
    pub run_evictor: bool,

    /// Number of tree nodes examined per evictor pass.
    /// JE: `EVICTOR_NODES_PER_SCAN` / default 10.
    pub evictor_nodes_per_scan: usize,

    /// Whether to use LRU-only eviction (no priority-1 / priority-2 split).
    /// JE: `EVICTOR_LRU_ONLY` / default false.
    pub evictor_lru_only: bool,

    /// Minimum number of background evictor threads always kept alive.
    /// JE: `EVICTOR_CORE_THREADS` / default 1.
    pub evictor_core_threads: usize,

    /// Maximum number of background evictor threads.
    /// JE: `EVICTOR_MAX_THREADS` / default 10.
    pub evictor_max_threads: usize,

    // -----------------------------------------------------------------------
    // Locking
    // -----------------------------------------------------------------------

    /// Lock timeout in milliseconds.
    /// JE: `LOCK_TIMEOUT` / default 500 ms.
    pub lock_timeout_ms: u64,

    /// Number of lock table shards.  Higher values reduce contention at the
    /// cost of slightly more memory.
    /// JE: `LOCK_N_LOCK_TABLES` / default 1 (Noxu defaults to 16).
    pub lock_n_lock_tables: u32,

    /// Whether to run the deadlock detector on lock waits.
    /// JE: `LOCK_DEADLOCK_DETECT` / default true.
    pub lock_deadlock_detect: bool,

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    /// Transaction timeout in milliseconds.  0 = no timeout.
    /// JE: `TXN_TIMEOUT` / default 0.
    pub txn_timeout_ms: u64,

    /// Default durability for transactions.
    /// JE: `TXN_DURABILITY`
    pub durability: Durability,

    /// If true, commits do not wait for the log to be written to disk.
    /// JE: `TXN_NO_SYNC` / default false.
    pub txn_no_sync: bool,

    /// If true, commits write the log to the OS buffer but do not fdatasync.
    /// JE: `TXN_WRITE_NO_SYNC` / default false.
    pub txn_write_no_sync: bool,

    /// If true, all transactions use serializable (degree-3) isolation by default.
    /// JE: `TXN_SERIALIZABLE_ISOLATION` / default false.
    pub txn_serializable_isolation: bool,

    // -----------------------------------------------------------------------
    // Cleaner (extended)
    // -----------------------------------------------------------------------

    /// Bytes read per cleaner file scan pass.
    /// JE: `CLEANER_READ_SIZE` / default 8 KiB.
    pub cleaner_read_size: usize,

    /// Number of LN records to look ahead during file cleaning.
    /// JE: `CLEANER_LOOK_AHEAD_CACHE_SIZE` / default 32.
    pub cleaner_look_ahead_cache_size: usize,

    // -----------------------------------------------------------------------
    // Background stats
    // -----------------------------------------------------------------------

    /// Whether to collect environment statistics in the background.
    /// JE: `STATS_COLLECT` / default false.
    pub stats_collect: bool,

    /// Interval in seconds between background stats collection passes.
    /// JE: `STATS_COLLECT_INTERVAL` / default 300 s.
    pub stats_collect_interval_secs: u64,
}

impl EnvironmentConfig {
    /// Creates a new EnvironmentConfig with the given home directory.
    pub fn new(home: PathBuf) -> Self {
        Self {
            home,
            allow_create: false,
            transactional: false,
            read_only: false,
            shared_cache: false,
            logging_level: None,
            env_recovery_force_checkpoint: false,
            max_disk: 0,
            // Memory
            cache_size: 64 * 1024 * 1024, // 64 MiB
            cache_percent: 0,
            // Log
            log_file_max_bytes: 10 * 1024 * 1024, // 10 MiB (JE default)
            log_file_cache_size: 100,               // JE LOG_FILE_CACHE_SIZE default
            log_checksum_read: true,
            log_fsync_timeout_ms: 500_000,          // JE LOG_FSYNC_TIMEOUT default (500 s)
            log_num_buffers: 3,
            log_total_buffer_bytes: 7 * 1024 * 1024, // 7 MiB total
            log_fault_read_size: 2048,
            log_group_commit_threshold: 0,
            log_group_commit_interval_ms: 0,
            // B-tree
            tree_max_delta: 25,
            tree_bin_delta: true,
            // INCompressor
            run_in_compressor: true,
            in_compressor_wakeup_interval_ms: 5000, // JE COMPRESSOR_WAKEUP_INTERVAL default
            // Cleaner
            run_cleaner: true,
            cleaner_min_utilization: 50,
            cleaner_min_file_utilization: 5,
            cleaner_threads: 1,
            cleaner_min_file_count: 2,
            cleaner_min_age: 2,
            cleaner_expiration_enabled: false,
            cleaner_read_size: 8192,                // JE CLEANER_READ_SIZE default
            cleaner_look_ahead_cache_size: 32,      // JE CLEANER_LOOK_AHEAD_CACHE_SIZE default
            // Checkpointer
            run_checkpointer: true,
            checkpointer_bytes_interval: 20_000_000, // 20 MiB
            checkpointer_min_interval_secs: 0,
            // Evictor
            run_evictor: true,
            evictor_nodes_per_scan: 10,
            evictor_lru_only: false,
            evictor_core_threads: 1,                // JE EVICTOR_CORE_THREADS default
            evictor_max_threads: 10,                // JE EVICTOR_MAX_THREADS default
            // Locking
            lock_timeout_ms: 500,
            lock_n_lock_tables: 16,                 // Noxu default (JE default is 1)
            lock_deadlock_detect: true,
            // Transactions
            txn_timeout_ms: 0,
            durability: Durability::default(),
            txn_no_sync: false,
            txn_write_no_sync: false,
            txn_serializable_isolation: false,
            // Stats
            stats_collect: false,
            stats_collect_interval_secs: 300,
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

    // -----------------------------------------------------------------------
    // New parameter setters (Log)
    // -----------------------------------------------------------------------

    pub fn set_log_file_max_bytes(&mut self, bytes: u64) -> &mut Self {
        self.log_file_max_bytes = bytes;
        self
    }

    pub fn set_log_num_buffers(&mut self, n: usize) -> &mut Self {
        self.log_num_buffers = n;
        self
    }

    pub fn set_log_total_buffer_bytes(&mut self, bytes: u64) -> &mut Self {
        self.log_total_buffer_bytes = bytes;
        self
    }

    pub fn set_log_fault_read_size(&mut self, size: usize) -> &mut Self {
        self.log_fault_read_size = size;
        self
    }

    pub fn set_log_group_commit_threshold(&mut self, threshold: usize) -> &mut Self {
        self.log_group_commit_threshold = threshold;
        self
    }

    pub fn set_log_group_commit_interval_ms(&mut self, ms: u64) -> &mut Self {
        self.log_group_commit_interval_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // B-tree
    // -----------------------------------------------------------------------

    pub fn set_tree_max_delta(&mut self, pct: u8) -> &mut Self {
        self.tree_max_delta = pct;
        self
    }

    pub fn set_tree_bin_delta(&mut self, enabled: bool) -> &mut Self {
        self.tree_bin_delta = enabled;
        self
    }

    // -----------------------------------------------------------------------
    // Cleaner
    // -----------------------------------------------------------------------

    pub fn set_cleaner_min_utilization(&mut self, pct: u8) -> &mut Self {
        self.cleaner_min_utilization = pct;
        self
    }

    pub fn set_cleaner_min_file_utilization(&mut self, pct: u8) -> &mut Self {
        self.cleaner_min_file_utilization = pct;
        self
    }

    pub fn set_cleaner_threads(&mut self, n: u32) -> &mut Self {
        self.cleaner_threads = n;
        self
    }

    pub fn set_cleaner_min_file_count(&mut self, n: u32) -> &mut Self {
        self.cleaner_min_file_count = n;
        self
    }

    pub fn set_cleaner_min_age(&mut self, checkpoints: u32) -> &mut Self {
        self.cleaner_min_age = checkpoints;
        self
    }

    pub fn set_cleaner_expiration_enabled(&mut self, enabled: bool) -> &mut Self {
        self.cleaner_expiration_enabled = enabled;
        self
    }

    // -----------------------------------------------------------------------
    // Checkpointer
    // -----------------------------------------------------------------------

    pub fn set_checkpointer_bytes_interval(&mut self, bytes: u64) -> &mut Self {
        self.checkpointer_bytes_interval = bytes;
        self
    }

    pub fn set_checkpointer_min_interval_secs(&mut self, secs: u64) -> &mut Self {
        self.checkpointer_min_interval_secs = secs;
        self
    }

    // -----------------------------------------------------------------------
    // Evictor
    // -----------------------------------------------------------------------

    pub fn set_evictor_nodes_per_scan(&mut self, n: usize) -> &mut Self {
        self.evictor_nodes_per_scan = n;
        self
    }

    pub fn set_evictor_lru_only(&mut self, lru_only: bool) -> &mut Self {
        self.evictor_lru_only = lru_only;
        self
    }

    // -----------------------------------------------------------------------
    // Locking
    // -----------------------------------------------------------------------

    pub fn set_lock_n_lock_tables(&mut self, n: u32) -> &mut Self {
        self.lock_n_lock_tables = n;
        self
    }

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    pub fn set_txn_no_sync(&mut self, no_sync: bool) -> &mut Self {
        self.txn_no_sync = no_sync;
        self
    }

    pub fn set_txn_write_no_sync(&mut self, write_no_sync: bool) -> &mut Self {
        self.txn_write_no_sync = write_no_sync;
        self
    }

    pub fn set_txn_serializable_isolation(&mut self, serializable: bool) -> &mut Self {
        self.txn_serializable_isolation = serializable;
        self
    }

    pub fn set_cache_percent(&mut self, pct: u32) -> &mut Self {
        self.cache_percent = pct;
        self
    }

    // -----------------------------------------------------------------------
    // Core (extended)
    // -----------------------------------------------------------------------

    pub fn set_env_recovery_force_checkpoint(&mut self, force: bool) -> &mut Self {
        self.env_recovery_force_checkpoint = force;
        self
    }

    pub fn set_max_disk(&mut self, bytes: u64) -> &mut Self {
        self.max_disk = bytes;
        self
    }

    // -----------------------------------------------------------------------
    // Log (extended)
    // -----------------------------------------------------------------------

    pub fn set_log_file_cache_size(&mut self, n: usize) -> &mut Self {
        self.log_file_cache_size = n;
        self
    }

    pub fn set_log_checksum_read(&mut self, enabled: bool) -> &mut Self {
        self.log_checksum_read = enabled;
        self
    }

    pub fn set_log_fsync_timeout_ms(&mut self, ms: u64) -> &mut Self {
        self.log_fsync_timeout_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // INCompressor
    // -----------------------------------------------------------------------

    pub fn set_run_in_compressor(&mut self, run: bool) -> &mut Self {
        self.run_in_compressor = run;
        self
    }

    pub fn set_in_compressor_wakeup_interval_ms(&mut self, ms: u64) -> &mut Self {
        self.in_compressor_wakeup_interval_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // Cleaner (extended)
    // -----------------------------------------------------------------------

    pub fn set_cleaner_read_size(&mut self, bytes: usize) -> &mut Self {
        self.cleaner_read_size = bytes;
        self
    }

    pub fn set_cleaner_look_ahead_cache_size(&mut self, n: usize) -> &mut Self {
        self.cleaner_look_ahead_cache_size = n;
        self
    }

    // -----------------------------------------------------------------------
    // Evictor (extended)
    // -----------------------------------------------------------------------

    pub fn set_evictor_core_threads(&mut self, n: usize) -> &mut Self {
        self.evictor_core_threads = n;
        self
    }

    pub fn set_evictor_max_threads(&mut self, n: usize) -> &mut Self {
        self.evictor_max_threads = n;
        self
    }

    // -----------------------------------------------------------------------
    // Locking (extended)
    // -----------------------------------------------------------------------

    pub fn set_lock_deadlock_detect(&mut self, enabled: bool) -> &mut Self {
        self.lock_deadlock_detect = enabled;
        self
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    pub fn set_stats_collect(&mut self, enabled: bool) -> &mut Self {
        self.stats_collect = enabled;
        self
    }

    pub fn set_stats_collect_interval_secs(&mut self, secs: u64) -> &mut Self {
        self.stats_collect_interval_secs = secs;
        self
    }

    // -----------------------------------------------------------------------
    // Builder-style equivalents (chained self)
    // -----------------------------------------------------------------------

    pub fn with_log_file_max_bytes(mut self, bytes: u64) -> Self {
        self.log_file_max_bytes = bytes;
        self
    }

    pub fn with_cleaner_min_utilization(mut self, pct: u8) -> Self {
        self.cleaner_min_utilization = pct;
        self
    }

    pub fn with_checkpointer_bytes_interval(mut self, bytes: u64) -> Self {
        self.checkpointer_bytes_interval = bytes;
        self
    }

    pub fn with_evictor_nodes_per_scan(mut self, n: usize) -> Self {
        self.evictor_nodes_per_scan = n;
        self
    }

    pub fn with_log_group_commit(mut self, threshold: usize, interval_ms: u64) -> Self {
        self.log_group_commit_threshold = threshold;
        self.log_group_commit_interval_ms = interval_ms;
        self
    }

    pub fn with_txn_no_sync(mut self, no_sync: bool) -> Self {
        self.txn_no_sync = no_sync;
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

    // -----------------------------------------------------------------------
    // New parameter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_defaults_for_new_params() {
        let config = EnvironmentConfig::default();
        assert_eq!(config.log_file_max_bytes, 10 * 1024 * 1024);
        assert_eq!(config.log_num_buffers, 3);
        assert_eq!(config.log_total_buffer_bytes, 7 * 1024 * 1024);
        assert_eq!(config.log_fault_read_size, 2048);
        assert_eq!(config.log_group_commit_threshold, 0);
        assert_eq!(config.log_group_commit_interval_ms, 0);
        assert_eq!(config.tree_max_delta, 25);
        assert!(config.tree_bin_delta);
        assert!(config.run_cleaner);
        assert_eq!(config.cleaner_min_utilization, 50);
        assert_eq!(config.cleaner_min_file_utilization, 5);
        assert_eq!(config.cleaner_threads, 1);
        assert_eq!(config.cleaner_min_file_count, 2);
        assert_eq!(config.cleaner_min_age, 2);
        assert!(!config.cleaner_expiration_enabled);
        assert!(config.run_checkpointer);
        assert_eq!(config.checkpointer_bytes_interval, 20_000_000);
        assert_eq!(config.checkpointer_min_interval_secs, 0);
        assert!(config.run_evictor);
        assert_eq!(config.evictor_nodes_per_scan, 10);
        assert!(!config.evictor_lru_only);
        assert_eq!(config.lock_n_lock_tables, 16);
        assert!(!config.txn_no_sync);
        assert!(!config.txn_write_no_sync);
        assert_eq!(config.cache_percent, 0);
    }

    #[test]
    fn test_log_file_max() {
        let mut c = EnvironmentConfig::default();
        c.set_log_file_max_bytes(20 * 1024 * 1024);
        assert_eq!(c.log_file_max_bytes, 20 * 1024 * 1024);
    }

    #[test]
    fn test_log_buffers() {
        let mut c = EnvironmentConfig::default();
        c.set_log_num_buffers(5);
        c.set_log_total_buffer_bytes(5 * 1024 * 1024);
        assert_eq!(c.log_num_buffers, 5);
        assert_eq!(c.log_total_buffer_bytes, 5 * 1024 * 1024);
    }

    #[test]
    fn test_group_commit() {
        let c = EnvironmentConfig::default()
            .with_log_group_commit(10, 20);
        assert_eq!(c.log_group_commit_threshold, 10);
        assert_eq!(c.log_group_commit_interval_ms, 20);
    }

    #[test]
    fn test_cleaner_params() {
        let c = EnvironmentConfig::default()
            .with_cleaner_min_utilization(30);
        assert_eq!(c.cleaner_min_utilization, 30);

        let mut c2 = EnvironmentConfig::default();
        c2.set_cleaner_threads(4);
        c2.set_cleaner_min_file_count(5);
        c2.set_cleaner_min_age(3);
        c2.set_cleaner_expiration_enabled(true);
        assert_eq!(c2.cleaner_threads, 4);
        assert_eq!(c2.cleaner_min_file_count, 5);
        assert_eq!(c2.cleaner_min_age, 3);
        assert!(c2.cleaner_expiration_enabled);
    }

    #[test]
    fn test_checkpointer_params() {
        let c = EnvironmentConfig::default()
            .with_checkpointer_bytes_interval(50_000_000);
        assert_eq!(c.checkpointer_bytes_interval, 50_000_000);

        let mut c2 = EnvironmentConfig::default();
        c2.set_checkpointer_min_interval_secs(60);
        assert_eq!(c2.checkpointer_min_interval_secs, 60);
    }

    #[test]
    fn test_evictor_params() {
        let c = EnvironmentConfig::default()
            .with_evictor_nodes_per_scan(50);
        assert_eq!(c.evictor_nodes_per_scan, 50);

        let mut c2 = EnvironmentConfig::default();
        c2.set_evictor_lru_only(true);
        assert!(c2.evictor_lru_only);
    }

    #[test]
    fn test_lock_n_tables() {
        let mut c = EnvironmentConfig::default();
        c.set_lock_n_lock_tables(32);
        assert_eq!(c.lock_n_lock_tables, 32);
    }

    #[test]
    fn test_txn_sync_flags() {
        let c = EnvironmentConfig::default().with_txn_no_sync(true);
        assert!(c.txn_no_sync);
        assert!(!c.txn_write_no_sync);

        let mut c2 = EnvironmentConfig::default();
        c2.set_txn_write_no_sync(true);
        assert!(c2.txn_write_no_sync);
    }

    #[test]
    fn test_extended_params_defaults() {
        let c = EnvironmentConfig::default();
        assert!(!c.env_recovery_force_checkpoint);
        assert_eq!(c.max_disk, 0);
        assert_eq!(c.log_file_cache_size, 100);
        assert!(c.log_checksum_read);
        assert_eq!(c.log_fsync_timeout_ms, 500_000);
        assert!(c.run_in_compressor);
        assert_eq!(c.in_compressor_wakeup_interval_ms, 5000);
        assert_eq!(c.cleaner_read_size, 8192);
        assert_eq!(c.cleaner_look_ahead_cache_size, 32);
        assert_eq!(c.evictor_core_threads, 1);
        assert_eq!(c.evictor_max_threads, 10);
        assert!(c.lock_deadlock_detect);
        assert!(!c.txn_serializable_isolation);
        assert!(!c.stats_collect);
        assert_eq!(c.stats_collect_interval_secs, 300);
    }

    #[test]
    fn test_extended_params_setters() {
        let mut c = EnvironmentConfig::default();
        c.set_env_recovery_force_checkpoint(true);
        c.set_max_disk(10 * 1024 * 1024 * 1024);
        c.set_log_file_cache_size(200);
        c.set_log_checksum_read(false);
        c.set_log_fsync_timeout_ms(1000);
        c.set_run_in_compressor(false);
        c.set_in_compressor_wakeup_interval_ms(1000);
        c.set_cleaner_read_size(16384);
        c.set_cleaner_look_ahead_cache_size(64);
        c.set_evictor_core_threads(2);
        c.set_evictor_max_threads(4);
        c.set_lock_deadlock_detect(false);
        c.set_txn_serializable_isolation(true);
        c.set_stats_collect(true);
        c.set_stats_collect_interval_secs(60);
        assert!(c.env_recovery_force_checkpoint);
        assert_eq!(c.max_disk, 10 * 1024 * 1024 * 1024);
        assert_eq!(c.log_file_cache_size, 200);
        assert!(!c.log_checksum_read);
        assert_eq!(c.log_fsync_timeout_ms, 1000);
        assert!(!c.run_in_compressor);
        assert_eq!(c.in_compressor_wakeup_interval_ms, 1000);
        assert_eq!(c.cleaner_read_size, 16384);
        assert_eq!(c.cleaner_look_ahead_cache_size, 64);
        assert_eq!(c.evictor_core_threads, 2);
        assert_eq!(c.evictor_max_threads, 4);
        assert!(!c.lock_deadlock_detect);
        assert!(c.txn_serializable_isolation);
        assert!(c.stats_collect);
        assert_eq!(c.stats_collect_interval_secs, 60);
    }

    #[test]
    fn test_builder_chain_with_new_params() {
        let c = EnvironmentConfig::new(PathBuf::from("/data"))
            .with_allow_create(true)
            .with_transactional(true)
            .with_cache_size(128 * 1024 * 1024)
            .with_log_file_max_bytes(5 * 1024 * 1024)
            .with_cleaner_min_utilization(40)
            .with_checkpointer_bytes_interval(10_000_000)
            .with_evictor_nodes_per_scan(20)
            .with_log_group_commit(5, 10)
            .with_txn_no_sync(false);
        assert_eq!(c.cache_size, 128 * 1024 * 1024);
        assert_eq!(c.log_file_max_bytes, 5 * 1024 * 1024);
        assert_eq!(c.cleaner_min_utilization, 40);
        assert_eq!(c.checkpointer_bytes_interval, 10_000_000);
        assert_eq!(c.evictor_nodes_per_scan, 20);
        assert_eq!(c.log_group_commit_threshold, 5);
        assert_eq!(c.log_group_commit_interval_ms, 10);
    }
}
