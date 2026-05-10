//! Environment handle.
//!

use crate::database::Database;
use crate::database_config::DatabaseConfig;
use crate::environment_config::EnvironmentConfig;
use crate::error::{NoxuError, Result};
use crate::transaction::Transaction;
use crate::transaction_config::TransactionConfig;
use noxu_dbi::{DbiEnvConfig, EnvironmentImpl};
use noxu_engine::EnvironmentStats;
use noxu_engine::env_stats::{EvictorStatsSnapshot, LockStatsSnapshot, LogStatsSnapshot, TxnStatsSnapshot};
use noxu_log::LogManager;
use noxu_sync::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// A database environment.
///
/// 
///
/// An Environment provides support for caching, locking, logging, and
/// transactions. It is the top-level handle through which databases are
/// opened and transactions are started.
///
/// # Example
/// ```ignore
/// use noxu_db::{Environment, EnvironmentConfig};
/// use std::path::PathBuf;
///
/// let config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
///     .allow_create(true)
///     .transactional(true);
/// let env = Environment::open(config).unwrap();
/// env.close().unwrap();
/// ```
pub struct Environment {
    /// Home directory path
    home: PathBuf,
    /// Configuration used to open this environment
    config: EnvironmentConfig,
    /// Open databases by name (tracks which names are currently open via this handle)
    databases: Mutex<HashMap<String, Arc<DatabaseHandle>>>,
    /// Active transactions
    active_txns: Mutex<HashMap<u64, Arc<TransactionState>>>,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Whether the environment is open
    open: AtomicBool,
    /// Whether the environment is valid (not invalidated by a fatal error).
    ///
    /// JE: `EnvironmentImpl.isValid()` / `envInvalid` AtomicBoolean.
    /// Set to `false` when an `EnvironmentFailure` with `invalidates_environment() == true`
    /// is returned; all subsequent API calls check this and return `EnvironmentFailure`.
    env_valid: AtomicBool,
    /// The real internal environment implementation (B-tree backed).
    env_impl: Arc<Mutex<EnvironmentImpl>>,
    /// Cached log manager — acquired once at open; None for non-transactional envs.
    /// Used by stat_fsync_count() to avoid env_impl.lock() on the stats hot path.
    log_manager: Option<Arc<LogManager>>,
}

/// Internal database handle state.
struct DatabaseHandle {
    name: String,
    #[allow(dead_code)]
    id: u64,
    #[allow(dead_code)]
    config: DatabaseConfig,
    open: AtomicBool,
}

/// Internal transaction state.
struct TransactionState {
    #[allow(dead_code)]
    id: u64,
    #[allow(dead_code)]
    config: TransactionConfig,
    #[allow(dead_code)]
    committed: AtomicBool,
    #[allow(dead_code)]
    aborted: AtomicBool,
}

impl Environment {
    /// Opens or creates a database environment.
    ///
    /// Constructor.
    ///
    /// # Arguments
    /// * `config` - The environment configuration
    ///
    /// # Returns
    /// The opened environment handle
    ///
    /// # Errors
    /// Returns an error if:
    /// - The environment directory does not exist and `allow_create` is false
    /// - The environment directory exists but is not writable and `read_only` is false
    /// - Invalid configuration parameters are provided
    pub fn open(config: EnvironmentConfig) -> Result<Self> {
        let home = config.home.clone();

        // Validate home directory
        if !home.exists() {
            if config.allow_create {
                std::fs::create_dir_all(&home).map_err(|e| {
                    NoxuError::environment(format!(
                        "Failed to create environment directory {:?}: {}",
                        home, e
                    ))
                })?;
            } else {
                return Err(NoxuError::environment(format!(
                    "Environment directory {:?} does not exist and allow_create is false",
                    home
                )));
            }
        }

        if !home.is_dir() {
            return Err(NoxuError::environment(format!(
                "Environment home {:?} is not a directory",
                home
            )));
        }

        // Check write permissions if not read-only
        if !config.read_only {
            // Test write access by creating a temp file
            let test_file = home.join(".noxu_write_test");
            std::fs::write(&test_file, b"test").map_err(|e| {
                NoxuError::environment(format!(
                    "Environment directory {:?} is not writable: {}",
                    home, e
                ))
            })?;
            let _ = std::fs::remove_file(&test_file);
        }

        // Translate EnvironmentConfig into DbiEnvConfig (the noxu-dbi struct)
        // to avoid a circular dependency between the two crates.
        let buf_size = if config.log_buffer_size > 0 {
            config.log_buffer_size
        } else {
            (config.log_total_buffer_bytes as usize)
                .checked_div(config.log_num_buffers)
                .unwrap_or(1024 * 1024)
        };
        let dbi_cfg = DbiEnvConfig {
            // Core
            read_only: config.read_only,
            transactional: config.transactional,
            env_is_locking: config.env_is_locking,
            env_recovery_force_checkpoint: config.env_recovery_force_checkpoint,
            env_recovery_force_checkpoint_field: config.env_recovery_force_checkpoint,
            env_recovery_force_new_file: config.env_recovery_force_new_file,
            halt_on_commit_after_checksum_exception: config
                .halt_on_commit_after_checksum_exception,
            env_check_leaks: config.env_check_leaks,
            env_forced_yield: config.env_forced_yield,
            env_fair_latches: config.env_fair_latches,
            env_latch_timeout_ms: config.env_latch_timeout_ms,
            env_ttl_clock_tolerance_ms: config.env_ttl_clock_tolerance_ms,
            env_expiration_enabled: config.env_expiration_enabled,
            env_db_eviction: config.env_db_eviction,
            // Memory
            cache_size: config.cache_size,
            cache_percent: config.cache_percent,
            max_off_heap_memory: config.max_off_heap_memory,
            max_disk: config.max_disk,
            free_disk: config.free_disk,
            // Log
            log_file_max_bytes: config.log_file_max_bytes,
            log_file_cache_size: config.log_file_cache_size,
            log_checksum_read: config.log_checksum_read,
            log_verify_checksums: config.log_verify_checksums,
            log_fsync_timeout_ms: config.log_fsync_timeout_ms,
            log_fsync_time_limit_ms: config.log_fsync_time_limit_ms,
            log_num_buffers: config.log_num_buffers,
            log_buffer_size: buf_size,
            log_fault_read_size: config.log_fault_read_size,
            log_iterator_read_size: config.log_iterator_read_size,
            log_iterator_max_size: config.log_iterator_max_size,
            log_n_data_directories: config.log_n_data_directories,
            log_mem_only: config.log_mem_only,
            log_detect_file_delete: config.log_detect_file_delete,
            log_detect_file_delete_interval_ms: config.log_detect_file_delete_interval_ms,
            log_flush_sync_interval_ms: config.log_flush_sync_interval_ms,
            log_flush_no_sync_interval_ms: config.log_flush_no_sync_interval_ms,
            log_use_odsync: config.log_use_odsync,
            log_use_write_queue: config.log_use_write_queue,
            log_write_queue_size: config.log_write_queue_size,
            log_group_commit_threshold: config.log_group_commit_threshold,
            log_group_commit_interval_ms: config.log_group_commit_interval_ms,
            // B-tree
            node_max_entries: config.node_max_entries,
            node_dup_tree_max_entries: config.node_dup_tree_max_entries,
            tree_max_embedded_ln: config.tree_max_embedded_ln,
            tree_max_delta: config.tree_max_delta,
            tree_bin_delta: config.tree_bin_delta,
            tree_min_memory: config.tree_min_memory,
            tree_compact_max_key_length: config.tree_compact_max_key_length,
            // INCompressor
            run_in_compressor: config.run_in_compressor,
            in_compressor_wakeup_interval_ms: config.in_compressor_wakeup_interval_ms,
            compressor_deadlock_retry: config.compressor_deadlock_retry,
            compressor_lock_timeout_ms: config.compressor_lock_timeout_ms,
            compressor_purge_root: config.compressor_purge_root,
            // Cleaner
            run_cleaner: config.run_cleaner,
            cleaner_min_utilization: config.cleaner_min_utilization,
            cleaner_min_file_utilization: config.cleaner_min_file_utilization,
            cleaner_threads: config.cleaner_threads,
            cleaner_min_file_count: config.cleaner_min_file_count,
            cleaner_min_age: config.cleaner_min_age,
            cleaner_bytes_interval: config.cleaner_bytes_interval,
            cleaner_wakeup_interval_ms: config.cleaner_wakeup_interval_ms,
            cleaner_fetch_obsolete_size: config.cleaner_fetch_obsolete_size,
            cleaner_adjust_utilization: config.cleaner_adjust_utilization,
            cleaner_deadlock_retry: config.cleaner_deadlock_retry,
            cleaner_lock_timeout_ms: config.cleaner_lock_timeout_ms,
            cleaner_expunge: config.cleaner_expunge,
            cleaner_use_deleted_dir: config.cleaner_use_deleted_dir,
            cleaner_max_batch_files: config.cleaner_max_batch_files,
            cleaner_read_size: config.cleaner_read_size,
            cleaner_detail_max_memory_percentage: config.cleaner_detail_max_memory_percentage,
            cleaner_look_ahead_cache_size: config.cleaner_look_ahead_cache_size,
            cleaner_foreground_proactive_migration: config
                .cleaner_foreground_proactive_migration,
            cleaner_background_proactive_migration: config
                .cleaner_background_proactive_migration,
            cleaner_lazy_migration: config.cleaner_lazy_migration,
            cleaner_expiration_enabled: config.cleaner_expiration_enabled,
            // Checkpointer
            run_checkpointer: config.run_checkpointer,
            checkpointer_bytes_interval: config.checkpointer_bytes_interval,
            checkpointer_wakeup_interval_ms: config.checkpointer_wakeup_interval_ms,
            checkpointer_deadlock_retry: config.checkpointer_deadlock_retry,
            checkpointer_high_priority: config.checkpointer_high_priority,
            // Evictor
            run_evictor: config.run_evictor,
            evictor_nodes_per_scan: config.evictor_nodes_per_scan,
            evictor_evict_bytes: config.evictor_evict_bytes,
            evictor_critical_percentage: config.evictor_critical_percentage,
            evictor_lru_only: config.evictor_lru_only,
            evictor_n_lru_lists: config.evictor_n_lru_lists,
            evictor_deadlock_retry: config.evictor_deadlock_retry,
            evictor_core_threads: config.evictor_core_threads,
            evictor_max_threads: config.evictor_max_threads,
            evictor_keep_alive_ms: config.evictor_keep_alive_ms,
            evictor_allow_bin_deltas: config.evictor_allow_bin_deltas,
            // Off-heap evictor
            run_offheap_evictor: config.run_offheap_evictor,
            offheap_evict_bytes: config.offheap_evict_bytes,
            offheap_n_lru_lists: config.offheap_n_lru_lists,
            offheap_checksum: config.offheap_checksum,
            offheap_core_threads: config.offheap_core_threads,
            offheap_max_threads: config.offheap_max_threads,
            offheap_keep_alive_ms: config.offheap_keep_alive_ms,
            // Locking
            lock_timeout_ms: config.lock_timeout_ms,
            lock_deadlock_detect: config.lock_deadlock_detect,
            lock_deadlock_detect_delay_ms: config.lock_deadlock_detect_delay_ms,
            // Transactions
            txn_timeout_ms: config.txn_timeout_ms,
            txn_serializable_isolation: config.txn_serializable_isolation,
            txn_deadlock_stack_trace: config.txn_deadlock_stack_trace,
            txn_dump_locks: config.txn_dump_locks,
            // Verifier
            run_verifier: config.run_verifier,
            verify_log: config.verify_log,
            verify_log_read_delay_ms: config.verify_log_read_delay_ms,
            verify_btree: config.verify_btree,
            verify_secondaries: config.verify_secondaries,
            verify_data_records: config.verify_data_records,
            verify_obsolete_records: config.verify_obsolete_records,
            verify_btree_batch_size: config.verify_btree_batch_size,
            verify_btree_batch_delay_ms: config.verify_btree_batch_delay_ms,
            // Stats
            stats_collect: config.stats_collect,
            stats_collect_interval_secs: config.stats_collect_interval_secs,
            // Background rate limits
            env_background_read_limit_kb: config.env_background_read_limit_kb,
            env_background_write_limit_kb: config.env_background_write_limit_kb,
            env_background_sleep_interval_us: config.env_background_sleep_interval_us,
        };
        let env_impl =
            EnvironmentImpl::from_dbi_config(home.clone(), &dbi_cfg)
                .map_err(|e| NoxuError::environment(e.to_string()))?;

        let log_manager = env_impl.get_log_manager();
        let env_impl_arc = Arc::new(Mutex::new(env_impl));
        Ok(Environment {
            home,
            config,
            databases: Mutex::new(HashMap::new()),
            active_txns: Mutex::new(HashMap::new()),
            next_txn_id: AtomicU64::new(1),
            open: AtomicBool::new(true),
            env_valid: AtomicBool::new(true),
            env_impl: env_impl_arc,
            log_manager,
        })
    }

    /// Closes the environment handle.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if:
    /// - The environment is already closed
    /// - There are open database handles
    /// - There are active transactions
    pub fn close(&self) -> Result<()> {
        if !self.open.load(Ordering::Acquire) {
            return Err(NoxuError::EnvironmentClosed);
        }

        // Check for open databases
        let databases = self.databases.lock();
        let open_dbs: Vec<String> = databases
            .iter()
            .filter(|(_, db)| db.open.load(Ordering::Acquire))
            .map(|(name, _)| name.clone())
            .collect();

        if !open_dbs.is_empty() {
            return Err(NoxuError::OperationNotAllowed(format!(
                "Cannot close environment with open database handles: {:?}",
                open_dbs
            )));
        }

        // Check for active transactions
        let active_txns = self.active_txns.lock();
        if !active_txns.is_empty() {
            return Err(NoxuError::OperationNotAllowed(format!(
                "Cannot close environment with {} active transactions",
                active_txns.len()
            )));
        }

        self.open.store(false, Ordering::Release);
        let env_impl = self.env_impl.lock();
        let _ = env_impl.close();
        Ok(())
    }

    /// Opens or creates a database.
    ///
    /// 
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `name` - Database name
    /// * `config` - Database configuration
    ///
    /// # Returns
    /// The opened database handle
    ///
    /// # Errors
    /// Returns an error if:
    /// - The environment is closed
    /// - The database name is invalid
    /// - The database does not exist and `allow_create` is false
    pub fn open_database(
        &self,
        _txn: Option<&Transaction>,
        name: &str,
        config: &DatabaseConfig,
    ) -> Result<Database> {
        self.check_open()?;

        if name.is_empty() {
            return Err(NoxuError::IllegalArgument(
                "Database name cannot be empty".to_string(),
            ));
        }

        let mut databases = self.databases.lock();

        // Check if database is already open via this environment handle
        if let Some(db_handle) = databases.get(name)
            && db_handle.open.load(Ordering::Acquire)
        {
            return Err(NoxuError::DatabaseAlreadyExists(format!(
                "Database '{}' is already open",
                name
            )));
        }

        // Build the noxu-dbi config from noxu-db config
        let mut dbi_config = noxu_dbi::DatabaseConfig::new();
        dbi_config.set_allow_create(config.allow_create);
        dbi_config.set_sorted_duplicates(config.sorted_duplicates);
        dbi_config.set_read_only(config.read_only);
        dbi_config.set_temporary(config.temporary);
        dbi_config.set_transactional(config.transactional);
        dbi_config.deferred_write = config.deferred_write;
        if config.node_max_entries > 0 {
            dbi_config.set_node_max_entries(config.node_max_entries as i32);
        }

        // Open the database via EnvironmentImpl (creates if allow_create, else errors)
        let db_impl_arc = {
            let env_impl = self.env_impl.lock();
            env_impl.open_database(name, &dbi_config).map_err(|e| {
                match &e {
                    noxu_dbi::DbiError::DatabaseNotFound(_) => {
                        NoxuError::DatabaseNotFound(format!(
                            "Database '{}' does not exist and allow_create is false",
                            name
                        ))
                    }
                    _ => NoxuError::environment(e.to_string()),
                }
            })?
        };

        let db_id = db_impl_arc.read().get_id().id() as u64;

        let db_handle = Arc::new(DatabaseHandle {
            name: name.to_string(),
            id: db_id,
            config: config.clone(),
            open: AtomicBool::new(true),
        });

        databases.insert(name.to_string(), db_handle);
        drop(databases);

        Ok(Database::new(
            name.to_string(),
            db_id,
            config.clone(),
            db_impl_arc,
            Arc::clone(&self.env_impl),
            self.config.txn_no_sync,
            self.config.txn_write_no_sync,
        ))
    }

    /// Removes a database.
    ///
    /// 
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `name` - Database name
    ///
    /// # Errors
    /// Returns an error if:
    /// - The environment is closed
    /// - The database does not exist
    /// - The database is currently open
    pub fn remove_database(
        &self,
        _txn: Option<&Transaction>,
        name: &str,
    ) -> Result<()> {
        self.check_open()?;

        let mut databases = self.databases.lock();
        {
            let env_impl = self.env_impl.lock();
            env_impl.remove_database(name).map_err(|e| {
                match &e {
                    noxu_dbi::DbiError::DatabaseNotFound(_) => {
                        NoxuError::DatabaseNotFound(format!(
                            "Database '{}' does not exist",
                            name
                        ))
                    }
                    _ => NoxuError::environment(e.to_string()),
                }
            })?;
        }
        databases.remove(name);

        Ok(())
    }

    /// Truncates a database: removes all records while keeping the database
    /// registered and any open handles valid.
    ///
    /// Returns the number of records that were in the database before truncation.
    ///
    /// JE: `Environment.truncateDatabase(txn, dbName, returnCount)`.
    pub fn truncate_database(
        &self,
        _txn: Option<&Transaction>,
        name: &str,
    ) -> Result<u64> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        env_impl.truncate_database(name).map_err(|e| {
            match &e {
                noxu_dbi::DbiError::DatabaseNotFound(_) => {
                    NoxuError::DatabaseNotFound(format!("Database '{}' does not exist", name))
                }
                _ => NoxuError::environment(e.to_string()),
            }
        })
    }

    /// Renames a database.
    ///
    ///
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `old_name` - Current database name
    /// * `new_name` - New database name
    ///
    /// # Errors
    /// Returns an error if:
    /// - The environment is closed
    /// - The source database does not exist
    /// - The destination database already exists
    /// - The source database is currently open
    pub fn rename_database(
        &self,
        _txn: Option<&Transaction>,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        self.check_open()?;

        if old_name == new_name {
            return Ok(());
        }

        let mut databases = self.databases.lock();
        {
            let env_impl = self.env_impl.lock();
            env_impl.rename_database(old_name, new_name).map_err(|e| {
                match &e {
                    noxu_dbi::DbiError::DatabaseNotFound(_) => {
                        NoxuError::DatabaseNotFound(format!(
                            "Database '{}' does not exist",
                            old_name
                        ))
                    }
                    noxu_dbi::DbiError::DatabaseAlreadyExists(_) => {
                        NoxuError::DatabaseAlreadyExists(format!(
                            "Database '{}' already exists",
                            new_name
                        ))
                    }
                    _ => NoxuError::environment(e.to_string()),
                }
            })?;
        }

        if let Some(handle) = databases.remove(old_name) {
            databases.insert(new_name.to_string(), handle);
        }

        Ok(())
    }

    /// Begins a new transaction.
    ///
    /// 
    ///
    /// # Arguments
    /// * `parent` - Optional parent transaction (currently ignored)
    /// * `config` - Optional transaction configuration
    ///
    /// # Returns
    /// A new transaction handle
    ///
    /// # Errors
    /// Returns an error if:
    /// - The environment is closed
    /// - The environment is not transactional
    pub fn begin_transaction(
        &self,
        _parent: Option<&Transaction>,
        config: Option<&TransactionConfig>,
    ) -> Result<Transaction> {
        self.check_open()?;

        if !self.config.transactional {
            return Err(NoxuError::OperationNotAllowed(
                "Cannot begin transaction on non-transactional environment"
                    .to_string(),
            ));
        }

        let txn_id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        let txn_config = config.cloned().unwrap_or_default();

        let txn_state = Arc::new(TransactionState {
            id: txn_id,
            config: txn_config.clone(),
            committed: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
        });

        let mut active_txns = self.active_txns.lock();
        active_txns.insert(txn_id, txn_state);
        drop(active_txns);

        // Wire the transaction to the WAL so commit/abort write log entries.
        // Also create an inner Txn for per-record lock management.
        let env_guard = self.env_impl.lock();
        let is_read_committed = txn_config.read_committed;
        let inner_txn = env_guard.begin_txn()
            .map(|mut t| {
                // Propagate the isolation level from TransactionConfig into the
                // inner Txn so that lock_ln() can release read locks immediately
                // for read-committed transactions.
                if is_read_committed {
                    t.set_read_committed_isolation(true);
                }
                Arc::new(std::sync::Mutex::new(t))
            })
            .ok();
        let txn = if let Some(lm) = env_guard.get_log_manager() {
            Transaction::with_log_manager(txn_id, txn_config, lm)
        } else {
            Transaction::new(txn_id, txn_config)
        };
        drop(env_guard);

        let txn = if let Some(it) = inner_txn {
            txn.with_inner_txn(it)
        } else {
            txn
        };

        // Wire env_impl so Transaction::abort() can apply undo records.
        // Txn environment reference during construction.
        let txn = txn.with_env_impl(Arc::clone(&self.env_impl));

        Ok(txn)
    }

    /// Returns a list of database names.
    ///
    /// 
    ///
    /// # Returns
    /// A vector of database names
    ///
    /// # Errors
    /// Returns an error if the environment is closed
    pub fn get_database_names(&self) -> Result<Vec<String>> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        Ok(env_impl.get_database_names())
    }

    /// Returns the home directory path.
    ///
    /// 
    pub fn get_home(&self) -> &Path {
        &self.home
    }

    /// Returns the environment configuration.
    ///
    /// 
    pub fn get_config(&self) -> &EnvironmentConfig {
        &self.config
    }

    /// Returns `true` if the environment is open and has not been invalidated by a fatal error.
    ///
    /// JE: `Environment.isValid()`.  Returns `false` after the environment is closed
    /// or after an `EnvironmentFailure` whose `reason.invalidates_environment()` returns
    /// `true` (e.g. `LogChecksum`, `BtreeCorruption`, `DiskLimit`).
    /// Once invalidated the environment must be closed and re-opened.
    pub fn is_valid(&self) -> bool {
        self.open.load(Ordering::Acquire) && self.env_valid.load(Ordering::Acquire)
    }

    /// Invalidates the environment in response to a fatal error.
    ///
    /// Called internally when an `EnvironmentFailure` with
    /// `reason.invalidates_environment() == true` propagates out of a
    /// background daemon.  After invalidation `is_valid()` returns `false`
    /// and all subsequent public API calls return `EnvironmentFailure`.
    pub fn invalidate(&self) {
        self.env_valid.store(false, Ordering::Release);
    }

    /// Returns whether the environment is transactional.
    ///
    /// Via environment.
    pub fn is_transactional(&self) -> bool {
        self.config.transactional
    }

    /// Returns whether the environment is read-only.
    ///
    /// Via environment.
    pub fn is_read_only(&self) -> bool {
        self.config.read_only
    }

    /// Returns a snapshot of environment statistics from all subsystems.
    ///
    /// JE: `Environment.getStats(StatsConfig)`.
    pub fn get_stats(&self) -> Result<EnvironmentStats> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let n_databases = env_impl.n_databases() as u32;
        // Use cached log_manager for the log stats to avoid double-locking.
        let log = self.log_manager
            .as_ref()
            .map(|lm| LogStatsSnapshot::from(&lm.get_stats()))
            .unwrap_or_default();
        let lock = LockStatsSnapshot::from(&env_impl.get_lock_manager().get_stats());
        let txn = TxnStatsSnapshot::from(&env_impl.get_txn_manager().get_stats());
        let throughput = env_impl.get_throughput_snapshot();
        let evictor = EvictorStatsSnapshot::from(env_impl.get_evictor().get_stats());
        let cleaner = env_impl
            .get_cleaner()
            .map(|c| c.get_stats().snapshot())
            .unwrap_or_default();
        let checkpoint = env_impl
            .get_checkpointer()
            .map(|cp| cp.get_stats().snapshot())
            .unwrap_or_default();
        Ok(EnvironmentStats {
            cache_size: self.config.cache_size,
            cache_usage: 0,
            n_databases,
            log,
            lock,
            txn,
            throughput,
            evictor,
            cleaner,
            checkpoint,
        })
    }

    /// Returns the total number of fdatasync calls performed by the log manager.
    ///
    /// Useful for benchmarking
    /// and for verifying that group commit is working (fewer fsyncs than commits).
    /// Returns 0 if the environment is non-transactional (no log manager).
    pub fn stat_fsync_count(&self) -> u64 {
        self.log_manager
            .as_ref()
            .map(|lm| lm.fsync_count())
            .unwrap_or(0)
    }

    /// Internal method to mark a database as closed.
    ///
    /// Called by Database::close().
    pub(crate) fn mark_database_closed(&self, name: &str) {
        let databases = self.databases.lock();
        if let Some(db_handle) = databases.get(name) {
            db_handle.open.store(false, Ordering::Release);
        }
    }

    /// Internal method to mark a transaction as complete.
    ///
    /// Called by Transaction::commit() or Transaction::abort().
    pub(crate) fn mark_transaction_complete(&self, txn_id: u64) {
        let mut active_txns = self.active_txns.lock();
        active_txns.remove(&txn_id);
    }

    fn check_open(&self) -> Result<()> {
        if !self.open.load(Ordering::Acquire) {
            return Err(NoxuError::EnvironmentClosed);
        }
        if !self.env_valid.load(Ordering::Acquire) {
            return Err(NoxuError::environment_with_reason(
                crate::error::EnvironmentFailureReason::ForcedShutdown,
                "environment has been invalidated due to a prior fatal error".to_string(),
            ));
        }
        Ok(())
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        // Best effort close on drop
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_env_config() -> (TempDir, EnvironmentConfig) {
        let temp_dir = TempDir::new().unwrap();
        let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        (temp_dir, config)
    }

    #[test]
    fn test_open_environment() {
        let (temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        assert!(env.is_valid());
        assert_eq!(env.get_home(), temp_dir.path());
        env.close().unwrap();
    }

    #[test]
    fn test_open_creates_directory() {
        let temp_dir = TempDir::new().unwrap();
        let home = temp_dir.path().join("subdir");
        let config =
            EnvironmentConfig::new(home.clone()).with_allow_create(true);

        let env = Environment::open(config).unwrap();
        assert!(home.exists());
        assert!(home.is_dir());
        env.close().unwrap();
    }

    #[test]
    fn test_open_fails_without_allow_create() {
        let temp_dir = TempDir::new().unwrap();
        let home = temp_dir.path().join("nonexistent");
        let config =
            EnvironmentConfig::new(home).with_allow_create(false);

        let result = Environment::open(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_close_environment() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        assert!(env.is_valid());
        env.close().unwrap();
        assert!(!env.is_valid());
    }

    #[test]
    fn test_close_twice_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        env.close().unwrap();
        let result = env.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_close_with_open_database_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let _db = env.open_database(None, "testdb", &db_config).unwrap();

        let result = env.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_open_database() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();
        assert_eq!(db.get_database_name(), "testdb");
        assert!(db.is_valid());
    }

    #[test]
    fn test_open_database_twice_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let _db1 = env.open_database(None, "testdb", &db_config).unwrap();
        let result = env.open_database(None, "testdb", &db_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_database_without_create_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(false);
        let result = env.open_database(None, "nonexistent", &db_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_database_empty_name_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let result = env.open_database(None, "", &db_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_database() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();
        db.close().unwrap();

        env.remove_database(None, "testdb").unwrap();
        let names = env.get_database_names().unwrap();
        assert!(!names.contains(&"testdb".to_string()));
    }

    #[test]
    fn test_remove_open_database_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let _db = env.open_database(None, "testdb", &db_config).unwrap();

        let result = env.remove_database(None, "testdb");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_nonexistent_database_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let result = env.remove_database(None, "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_database() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "oldname", &db_config).unwrap();
        db.close().unwrap();

        env.rename_database(None, "oldname", "newname").unwrap();

        let names = env.get_database_names().unwrap();
        assert!(!names.contains(&"oldname".to_string()));
        assert!(names.contains(&"newname".to_string()));
    }

    #[test]
    fn test_rename_to_same_name() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();
        db.close().unwrap();

        env.rename_database(None, "testdb", "testdb").unwrap();
    }

    #[test]
    fn test_rename_open_database_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let _db = env.open_database(None, "testdb", &db_config).unwrap();

        let result = env.rename_database(None, "testdb", "newname");
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_nonexistent_database_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let result = env.rename_database(None, "nonexistent", "newname");
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_to_existing_database_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db1 = env.open_database(None, "db1", &db_config).unwrap();
        let db2 = env.open_database(None, "db2", &db_config).unwrap();
        db1.close().unwrap();
        db2.close().unwrap();

        let result = env.rename_database(None, "db1", "db2");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_database_names() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let _db1 = env.open_database(None, "db1", &db_config).unwrap();
        let _db2 = env.open_database(None, "db2", &db_config).unwrap();

        let names = env.get_database_names().unwrap();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"db1".to_string()));
        assert!(names.contains(&"db2".to_string()));
    }

    #[test]
    fn test_begin_transaction() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let txn = env.begin_transaction(None, None).unwrap();
        assert!(txn.is_valid());
    }

    #[test]
    fn test_begin_transaction_non_transactional_fails() {
        let temp_dir = TempDir::new().unwrap();
        let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(false);
        let env = Environment::open(config).unwrap();

        let result = env.begin_transaction(None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_transactional() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        assert!(env.is_transactional());
    }

    #[test]
    fn test_is_not_transactional() {
        let temp_dir = TempDir::new().unwrap();
        let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(false);
        let env = Environment::open(config).unwrap();
        assert!(!env.is_transactional());
    }

    #[test]
    fn test_is_read_only() {
        let temp_dir = TempDir::new().unwrap();
        let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_read_only(true);
        let env = Environment::open(config).unwrap();
        assert!(env.is_read_only());
    }

    #[test]
    fn test_operations_on_closed_environment_fail() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        env.close().unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        assert!(env.open_database(None, "test", &db_config).is_err());
        assert!(env.remove_database(None, "test").is_err());
        assert!(env.rename_database(None, "a", "b").is_err());
        assert!(env.begin_transaction(None, None).is_err());
        assert!(env.get_database_names().is_err());
    }

    // ========================================================================
    // Additional branch-coverage tests
    // ========================================================================

    /// open() with a path that points to a file (not a directory) fails.
    #[test]
    fn test_open_fails_if_home_is_a_file() {
        use std::io::Write;
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("not_a_dir.txt");
        let mut f = std::fs::File::create(&file_path).unwrap();
        writeln!(f, "data").unwrap();
        drop(f);

        let config = EnvironmentConfig::new(file_path).with_allow_create(false);
        // The path exists but is not a directory — must fail.
        let result = Environment::open(config);
        assert!(result.is_err());
    }

    /// open_database() with node_max_entries > 0 hits the set_node_max_entries branch.
    #[test]
    fn test_open_database_with_node_max_entries() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let mut db_config = DatabaseConfig::new().with_allow_create(true);
        db_config.set_node_max_entries(64);
        let db = env.open_database(None, "testdb_entries", &db_config).unwrap();
        assert!(db.is_valid());
    }

    /// begin_transaction() with an explicit TransactionConfig.
    #[test]
    fn test_begin_transaction_with_explicit_config() {
        use crate::transaction_config::TransactionConfig;
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let txn_config = TransactionConfig::new();
        let txn = env.begin_transaction(None, Some(&txn_config)).unwrap();
        assert!(txn.is_valid());
    }

    /// rename_database() when the old name is not in the databases map
    /// (handle was never registered) still succeeds at the env_impl level and
    /// the missing-handle branch (`if let Some(...)` => false) is taken.
    #[test]
    fn test_rename_database_handle_not_in_map() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        // Create the DB using env_impl directly (bypassing Environment::open_database
        // so the handle is NOT in the databases map), then immediately close it
        // so that reference_count returns to 0 (no open user handles).
        {
            let env_impl = env.env_impl.lock();
            let mut dbi_config = noxu_dbi::DatabaseConfig::new();
            dbi_config.set_allow_create(true);
            let db_arc = env_impl.open_database("ghost_db", &dbi_config).unwrap();
            let db_id = db_arc.read().get_id();
            env_impl.close_database(db_id).unwrap();
        }

        // rename_database should succeed and hit the `if let Some(handle)` false branch.
        env.rename_database(None, "ghost_db", "ghost_db_renamed").unwrap();

        let names = env.get_database_names().unwrap();
        assert!(names.contains(&"ghost_db_renamed".to_string()));
        assert!(!names.contains(&"ghost_db".to_string()));
    }

    /// close() with active transactions returns an error.
    #[test]
    fn test_close_with_active_transactions_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let _txn = env.begin_transaction(None, None).unwrap();

        let result = env.close();
        assert!(result.is_err());
    }

    /// get_config() and get_home() return the correct values.
    #[test]
    fn test_get_config_and_home() {
        let (temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        assert!(env.get_config().allow_create);
        assert_eq!(env.get_home(), temp_dir.path());
        env.close().unwrap();
    }

    /// mark_database_closed() when the database is in the map.
    #[test]
    fn test_mark_database_closed_known_name() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "mydb", &db_config).unwrap();
        // db is open — mark it closed via the internal API.
        env.mark_database_closed("mydb");
        // The database handle is now marked closed in the map; close() should succeed.
        let _ = db.is_valid(); // just use the variable
        env.close().unwrap();
    }

    /// mark_database_closed() for an unknown name is a no-op.
    #[test]
    fn test_mark_database_closed_unknown_name_is_noop() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        // No database named "ghost" — should not panic.
        env.mark_database_closed("ghost");
        env.close().unwrap();
    }

    /// mark_transaction_complete() removes the transaction from the active set.
    #[test]
    fn test_mark_transaction_complete_allows_env_close() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let txn = env.begin_transaction(None, None).unwrap();
        let txn_id = txn.get_id();

        // Without removing the txn, close would fail.
        // Remove it via the internal API.
        env.mark_transaction_complete(txn_id);

        // Now close should succeed.
        env.close().unwrap();
    }
}
