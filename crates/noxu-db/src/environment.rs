//! Environment handle.
//!

use crate::checkpoint_config::CheckpointConfig;
use crate::database::Database;
use crate::database_config::DatabaseConfig;
use crate::environment_config::EnvironmentConfig;
use crate::environment_mutable_config::EnvironmentMutableConfig;
use crate::error::{NoxuError, Result};
use crate::transaction::Transaction;
use crate::transaction_config::TransactionConfig;
use hashbrown::HashMap;
use noxu_dbi::{DbiEnvConfig, EnvironmentImpl};
use noxu_engine::EnvironmentStats;
use noxu_engine::env_stats::{
    EvictorStatsSnapshot, LockStatsSnapshot, LogStatsSnapshot, TxnStatsSnapshot,
};
use noxu_log::LogManager;
use noxu_sync::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

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
    /// Active transactions registry, shared with `Transaction` so that
    /// `Transaction::commit()` / `Transaction::abort()` can prune their
    /// own entry on completion (F1: `mark_transaction_complete` was dead
    /// code, so `env.close()` after `txn.commit()` always failed).
    active_txns: Arc<ActiveTxns>,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Whether the environment is open
    open: AtomicBool,
    /// Whether the environment is valid (not invalidated by a fatal error).
    ///
    /// Mirrors `EnvironmentImpl.isValid()` / `envInvalid` AtomicBoolean.
    /// Set to `false` when an `EnvironmentFailure` with `invalidates_environment() == true`
    /// is returned; all subsequent API calls check this and return `EnvironmentFailure`.
    env_valid: AtomicBool,
    /// The real internal environment implementation (B-tree backed).
    env_impl: Arc<Mutex<EnvironmentImpl>>,
    /// Cached log manager — acquired once at open; None for non-transactional envs.
    /// Used by stat_fsync_count() to avoid env_impl.lock() on the stats hot path.
    log_manager: Option<Arc<LogManager>>,
    /// Bookkeeping for `Environment::checkpoint(CheckpointConfig)` so that
    /// `force` / `k_bytes` / `minutes` can gate whether the call actually
    /// runs a checkpoint.
    last_checkpoint_time: Mutex<Option<Instant>>,
    last_checkpoint_end_lsn: Mutex<noxu_util::Lsn>,
    /// Optional replica-ack coordinator (typically a
    /// `noxu_rep::ReplicatedEnvironment`).  When set via
    /// [`Environment::set_replica_coordinator`], every new
    /// `Transaction` is wired to the coordinator and its
    /// `commit_with_durability` blocks until the configured
    /// `ReplicaAckPolicy` is satisfied (or the configured timeout
    /// elapses, in which case `NoxuError::InsufficientReplicas` is
    /// returned).  Closes finding F1 of
    /// the 2026 review.
    replica_coordinator: Mutex<Option<noxu_dbi::SharedReplicaAckCoordinator>>,
    /// Per-commit timeout for replica acknowledgments.  Mirrors
    /// `noxu_rep::RepConfig::replica_ack_timeout`; defaults to 5s.
    replica_ack_timeout: Mutex<std::time::Duration>,
}

/// Internal database handle state.
struct DatabaseHandle {
    name: String,
    #[expect(dead_code)]
    id: u64,
    #[expect(dead_code)]
    config: DatabaseConfig,
    /// Shared open flag — same `Arc<AtomicBool>` as `Database.open` so that
    /// `Database::close()` setting the flag to false also marks this handle
    /// as closed, letting `Environment::close()` succeed.
    open: Arc<AtomicBool>,
}

/// Internal transaction state.
struct TransactionState {
    #[expect(dead_code)]
    id: u64,
    #[expect(dead_code)]
    config: TransactionConfig,
    #[expect(dead_code)]
    committed: AtomicBool,
    #[expect(dead_code)]
    aborted: AtomicBool,
}

/// Shared registry of active transactions, owned by `Environment` and
/// referenced (via `Arc`) by every `Transaction` so that `commit()` /
/// `abort()` can prune their own entry without a callback into
/// `Environment` itself.
///
/// Resolves F1 of the May 2026 API audit: `Environment::active_txns` was
/// previously a private `Mutex<HashMap>` that no `Transaction` could see,
/// so `mark_transaction_complete` was dead code and `env.close()` after a
/// commit always returned `OperationNotAllowed`.
pub(crate) struct ActiveTxns {
    txns: Mutex<HashMap<u64, Arc<TransactionState>>>,
}

impl ActiveTxns {
    fn new() -> Self {
        Self { txns: Mutex::new(HashMap::new()) }
    }

    fn insert(&self, id: u64, state: Arc<TransactionState>) {
        self.txns.lock().insert(id, state);
    }

    /// Removes the entry for the given transaction id.
    ///
    /// Called by `Transaction::commit_with_durability` and `Transaction::abort`
    /// once the transaction has reached a terminal state.
    pub(crate) fn mark_complete(&self, id: u64) {
        self.txns.lock().remove(&id);
    }

    fn len(&self) -> usize {
        self.txns.lock().len()
    }

    fn is_empty(&self) -> bool {
        self.txns.lock().is_empty()
    }
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

        // Warn about any unimplemented config parameters that have been set
        // to non-default values (re-audit JE F-1: no silently-ignored params).
        crate::unimplemented_params::warn_unimplemented_params(&config);

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
            env_recovery_force_checkpoint_field: config
                .env_recovery_force_checkpoint,
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
            log_detect_file_delete_interval_ms: config
                .log_detect_file_delete_interval_ms,
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
            tree_bin_delta_percent: config.tree_bin_delta_percent,
            tree_min_memory: config.tree_min_memory,
            tree_compact_max_key_length: config.tree_compact_max_key_length,
            // INCompressor
            run_in_compressor: config.run_in_compressor,
            in_compressor_wakeup_interval_ms: config
                .in_compressor_wakeup_interval_ms,
            compressor_deadlock_retry: config.compressor_deadlock_retry,
            compressor_lock_timeout_ms: config.compressor_lock_timeout_ms,
            compressor_purge_root: config.compressor_purge_root,
            // Cleaner
            run_cleaner: config.run_cleaner,
            cleaner_min_utilization: config.cleaner_min_utilization,

            cleaner_two_pass_gap: config.cleaner_two_pass_gap,

            cleaner_two_pass_threshold: config.cleaner_two_pass_threshold,
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
            cleaner_detail_max_memory_percentage: config
                .cleaner_detail_max_memory_percentage,
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
            checkpointer_wakeup_interval_ms: config
                .checkpointer_wakeup_interval_ms,
            checkpointer_deadlock_retry: config.checkpointer_deadlock_retry,
            checkpointer_high_priority: config.checkpointer_high_priority,
            // Evictor
            run_evictor: config.run_evictor,
            evictor_nodes_per_scan: config.evictor_nodes_per_scan,
            evictor_evict_bytes: config.evictor_evict_bytes,
            evictor_critical_percentage: config.evictor_critical_percentage,
            evictor_lru_only: config.evictor_lru_only,
            evictor_use_dirty_lru: config.evictor_use_dirty_lru,
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
            n_lock_tables: config.lock_n_lock_tables as usize,
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
            env_background_sleep_interval_us: config
                .env_background_sleep_interval_us,
        };
        let env_impl = EnvironmentImpl::from_dbi_config(home.clone(), &dbi_cfg)
            .map_err(|e| NoxuError::environment(e.to_string()))?;

        let log_manager = env_impl.get_log_manager();
        let env_impl_arc = Arc::new(Mutex::new(env_impl));
        Ok(Environment {
            home,
            config,
            databases: Mutex::new(HashMap::new()),
            active_txns: Arc::new(ActiveTxns::new()),
            next_txn_id: AtomicU64::new(1),
            open: AtomicBool::new(true),
            env_valid: AtomicBool::new(true),
            env_impl: env_impl_arc,
            log_manager,
            last_checkpoint_time: Mutex::new(None),
            last_checkpoint_end_lsn: Mutex::new(noxu_util::NULL_LSN),
            replica_coordinator: Mutex::new(None),
            replica_ack_timeout: Mutex::new(std::time::Duration::from_secs(5)),
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
        if !self.active_txns.is_empty() {
            return Err(NoxuError::OperationNotAllowed(format!(
                "Cannot close environment with {} active transactions",
                self.active_txns.len()
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
    /// - A handle for `name` is already open in this `Environment`
    ///   (`DatabaseAlreadyExists`)
    pub fn open_database(
        &self,
        txn: Option<&Transaction>,
        name: &str,
        config: &DatabaseConfig,
    ) -> Result<Database> {
        self.check_open()?;

        // Audit transaction-env F5 (Wave 2C-4): on a read-only env,
        // open_database must not create new databases nor open existing
        // ones in writable mode.  Pre-fix the request silently created an
        // in-memory-only database (no WAL backing) which violated the
        // "no write operations" guarantee in the user-facing docs.
        if self.config.read_only {
            if config.allow_create {
                return Err(NoxuError::OperationNotAllowed(
                    "open_database: cannot create a database on a read-only \
                     environment (DatabaseConfig::with_allow_create(true))"
                        .to_string(),
                ));
            }
            if !config.read_only {
                return Err(NoxuError::OperationNotAllowed(
                    "open_database: read-only environment requires the \
                     database to be opened read-only \
                     (DatabaseConfig::with_read_only(true))"
                        .to_string(),
                ));
            }
        }

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
        // Audit database F7 (Wave 2C-4): plumb key_prefixing through;
        // pre-fix the outer flag was silently dropped on the floor.
        dbi_config.set_key_prefixing(config.key_prefixing);
        // DBI-14: thread user comparators (identity + closure) through to
        // DatabaseImpl so the tree honours them on every key comparison.
        dbi_config.btree_comparator =
            config.btree_comparator.as_ref().map(|c| {
                noxu_dbi::ConfigComparator {
                    identity: c.identity().to_string(),
                    func: c.func(),
                }
            });
        dbi_config.duplicate_comparator = config
            .duplicate_comparator
            .as_ref()
            .map(|c| noxu_dbi::ConfigComparator {
                identity: c.identity().to_string(),
                func: c.func(),
            });
        dbi_config.override_btree_comparator = config.override_btree_comparator;
        dbi_config.override_duplicate_comparator =
            config.override_duplicate_comparator;
        if config.node_max_entries > 0 {
            dbi_config.set_node_max_entries(config.node_max_entries as i32);
        }

        // Open the database via EnvironmentImpl (creates if allow_create, else errors)
        // C-4 / JE 1-I: if a transaction is supplied and this is a new
        // creation, use the transactional path so the name registration is
        // deferred until commit.
        let is_transactional_create = txn.is_some() && config.allow_create;
        let db_impl_arc = {
            let env_impl = self.env_impl.lock();
            if is_transactional_create {
                // SAFETY: is_transactional_create implies txn.is_some().
                let txn_id = txn
                    .expect("invariant: txn is Some when is_transactional_create")
                    .get_id();
                env_impl
                    .open_database_transactional(name, &dbi_config, txn_id)
            } else {
                env_impl.open_database(name, &dbi_config)
            }
            .map_err(|e| {
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

        // Register abort/commit callbacks on the transaction so that
        // transactional database creation is properly rolled back or
        // finalised when the transaction resolves (C-4 / JE 1-I).
        if is_transactional_create {
            let env_impl_arc = Arc::clone(&self.env_impl);
            let db_name_abort = name.to_string();
            let db_name_commit = name.to_string();
            let env_impl_arc2 = Arc::clone(&self.env_impl);
            // SAFETY: is_transactional_create implies txn.is_some().
            let txn_ref = txn
                .expect("invariant: txn is Some when is_transactional_create");
            txn_ref.register_abort_callback(move || {
                env_impl_arc.lock().abort_pending_database(&db_name_abort);
            });
            txn_ref.register_commit_callback(move || {
                env_impl_arc2.lock().commit_pending_database(&db_name_commit);
            });
        }

        let db_id = db_impl_arc.read().get_id().id() as u64;

        // Shared open flag: stored in both `DatabaseHandle` and `Database`.
        // When `Database::close()` sets it to false the env-side handle is
        // also marked as closed, so `Environment::close()` can proceed.
        let open_flag = Arc::new(AtomicBool::new(true));

        let db_handle = Arc::new(DatabaseHandle {
            name: name.to_string(),
            id: db_id,
            config: config.clone(),
            open: Arc::clone(&open_flag),
        });

        databases.insert(name.to_string(), db_handle);
        drop(databases);

        Ok(Database::new(
            name.to_string(),
            db_id,
            config.clone(),
            db_impl_arc,
            Arc::clone(&self.env_impl),
            open_flag,
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
        self.check_writable("remove_database")?;

        let mut databases = self.databases.lock();
        {
            let env_impl = self.env_impl.lock();
            env_impl.remove_database(name).map_err(|e| match &e {
                noxu_dbi::DbiError::DatabaseNotFound(_) => {
                    NoxuError::DatabaseNotFound(format!(
                        "Database '{}' does not exist",
                        name
                    ))
                }
                _ => NoxuError::environment(e.to_string()),
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
    /// Mirrors `Environment.truncateDatabase(txn, dbName, returnCount)`.
    pub fn truncate_database(
        &self,
        _txn: Option<&Transaction>,
        name: &str,
    ) -> Result<u64> {
        self.check_writable("truncate_database")?;
        let env_impl = self.env_impl.lock();
        env_impl.truncate_database(name).map_err(|e| match &e {
            noxu_dbi::DbiError::DatabaseNotFound(_) => {
                NoxuError::DatabaseNotFound(format!(
                    "Database '{}' does not exist",
                    name
                ))
            }
            _ => NoxuError::environment(e.to_string()),
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
        self.check_writable("rename_database")?;

        if old_name == new_name {
            return Ok(());
        }

        let mut databases = self.databases.lock();
        {
            let env_impl = self.env_impl.lock();
            env_impl.rename_database(old_name, new_name).map_err(
                |e| match &e {
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
                },
            )?;
        }

        if let Some(handle) = databases.remove(old_name) {
            databases.insert(new_name.to_string(), handle);
        }

        Ok(())
    }

    /// Begins a new transaction.
    ///
    /// # Arguments
    /// * `config` - Optional transaction configuration
    ///
    /// # Returns
    /// A new transaction handle.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The environment is closed
    /// - The environment is not transactional
    ///
    /// # Nested transactions
    /// Nested (child) transactions are not supported.  In v1.5 this method
    /// took an `Option<&Transaction>` `parent` argument that was rejected
    /// at runtime with [`NoxuError::Unsupported`] (Decision 3B in
    /// the 2026 review, audit finding F11).
    /// In v2.0 the parameter has been removed entirely — the
    /// type system now enforces the constraint, so what was a runtime
    /// error is now a compile error.
    #[allow(deprecated)] // Transaction::new / with_log_manager / with_inner_txn / with_env_impl are pub(internal)
    pub fn begin_transaction(
        &self,
        config: Option<&TransactionConfig>,
    ) -> Result<Transaction> {
        self.check_open()?;

        if !self.config.transactional {
            return Err(NoxuError::OperationNotAllowed(
                "Cannot begin transaction on non-transactional environment"
                    .to_string(),
            ));
        }

        // Audit transaction-env F5 (Wave 2C-4): on a read-only env, only
        // explicitly read-only transactions are allowed.  A writable txn
        // on a read-only env was previously accepted but every commit
        // silently no-op'd because `log_manager` was None.
        if self.config.read_only
            && !config.map(|c| c.read_only).unwrap_or(false)
        {
            return Err(NoxuError::OperationNotAllowed(
                "begin_transaction: read-only environment requires the \
                 transaction to be read-only \
                 (TransactionConfig::with_read_only(true))"
                    .to_string(),
            ));
        }

        let txn_id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        // F3: when the caller does not supply a TransactionConfig, the
        // environment-level `Durability` default (`EnvironmentConfig::durability`,
        // settable via `EnvironmentConfig::with_durability`) must be
        // honoured.  Pre-fix `unwrap_or_default()` produced a config with
        // `Durability::COMMIT_SYNC` regardless of the env setting, so a
        // user opening with `.with_durability(COMMIT_NO_SYNC)` and then
        // calling `begin_transaction(None)` still fsynced on every
        // commit.
        // Audit transaction-env F4 (Wave 2C-4): the env-level
        // `txn_no_sync` / `txn_write_no_sync` flags now apply to explicit
        // commits as well as auto-commit.  When neither config nor
        // env-default sets a durability override, derive one from the
        // boolean flags.  An explicit `with_durability(...)` on the
        // TransactionConfig still wins.
        let mut txn_config = match config.cloned() {
            Some(c) => c,
            None => TransactionConfig::default()
                .with_durability(self.config.durability),
        };
        if config.is_none() {
            // No caller config: env flags can override the inherited
            // durability if they request a less-strict sync policy.
            let derived = match (
                self.config.txn_no_sync,
                self.config.txn_write_no_sync,
            ) {
                (true, _) => {
                    Some(crate::durability::Durability::COMMIT_NO_SYNC)
                }
                (_, true) => {
                    Some(crate::durability::Durability::COMMIT_WRITE_NO_SYNC)
                }
                _ => None,
            };
            if let Some(d) = derived {
                txn_config = txn_config.with_durability(d);
            }
        }

        let txn_state = Arc::new(TransactionState {
            id: txn_id,
            config: txn_config.clone(),
            committed: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
        });

        let mut active_txns = self.active_txns.txns.lock();
        active_txns.insert(txn_id, txn_state);
        drop(active_txns);

        // Wire the transaction to the WAL so commit/abort write log entries.
        // Also create an inner Txn for per-record lock management.
        let env_guard = self.env_impl.lock();
        let inner_txn = env_guard
            .begin_txn()
            .map(|mut t| {
                // Propagate all relevant TransactionConfig fields into the
                // inner Txn for lock management and isolation behavior.
                if txn_config.read_committed {
                    t.set_read_committed_isolation(true);
                }
                if txn_config.read_uncommitted {
                    // F2: previously this branch was missing, so the
                    // user-set `with_read_uncommitted(true)` flag was
                    // silently dropped and dirty reads were impossible
                    // at the txn level.
                    t.set_read_uncommitted_default(true);
                }
                if txn_config.serializable_isolation {
                    t.set_serializable_isolation(true);
                }
                if txn_config.importunate {
                    t.set_importunate(true);
                }
                if txn_config.no_wait {
                    t.set_no_wait(true);
                }
                if txn_config.lock_timeout_ms > 0 {
                    t.set_lock_timeout(txn_config.lock_timeout_ms);
                }
                if txn_config.txn_timeout_ms > 0 {
                    t.set_txn_timeout(txn_config.txn_timeout_ms);
                }
                Arc::new(std::sync::Mutex::new(t))
            })
            .ok();
        // TXN-2 (JE TxnManager.registerTxn `nActiveSerializable` path):
        // Increment the serializable counter exactly once per serializable
        // explicit txn.  The counterpart decrement is in
        // `Transaction::unregister_inner_txn`, which is called from every
        // terminal path (commit, abort, resolved_commit/abort_after_prepare).
        // Guard on `inner_txn.is_some()` so read-only or env-less contexts
        // (which have no TxnManager) are not counted.
        if txn_config.serializable_isolation && inner_txn.is_some() {
            env_guard.get_txn_manager().register_serializable();
        }
        let txn = if let Some(lm) = env_guard.get_log_manager() {
            Transaction::with_log_manager(txn_id, txn_config, lm)
        } else {
            Transaction::new(txn_id, txn_config)
        };
        drop(env_guard);

        let txn =
            if let Some(it) = inner_txn { txn.with_inner_txn(it) } else { txn };

        // Wire env_impl so Transaction::abort() can apply undo records.
        // Txn environment reference during construction.
        let txn = txn.with_env_impl(Arc::clone(&self.env_impl));

        // Wire the active-txns registry so commit/abort can prune their
        // own entry (F1).  Without this, every successful txn left an
        // entry in `active_txns` and `env.close()` returned
        // `OperationNotAllowed`.
        let txn = txn.with_active_txns(Arc::clone(&self.active_txns));

        // F1: if a replica-ack coordinator has been installed (via
        // `set_replica_coordinator`), wire it into the transaction so
        // that `commit_with_durability` blocks until the configured
        // `ReplicaAckPolicy` is satisfied.
        let txn = if let Some(coord) = self.replica_coordinator.lock().clone() {
            let timeout = *self.replica_ack_timeout.lock();
            txn.with_replica_coordinator(coord, timeout)
        } else {
            txn
        };

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

    /// Install a replica-ack coordinator on this environment.
    ///
    /// After this call, every transaction begun on this environment
    /// will consult the coordinator on `commit_with_durability` and
    /// block until the configured `ReplicaAckPolicy` is satisfied (or
    /// until `replica_ack_timeout` elapses, in which case
    /// `NoxuError::InsufficientReplicas` is returned).
    ///
    /// `noxu_rep::ReplicatedEnvironment` implements
    /// `noxu_dbi::ReplicaAckCoordinator`; users typically wire it as:
    ///
    /// ```ignore
    /// let rep_env = Arc::new(ReplicatedEnvironment::new(rep_config)?);
    /// env.set_replica_coordinator(rep_env.clone());
    /// rep_env.with_environment(env_impl);
    /// ```
    ///
    /// Closes finding F1 of the 2026 review.
    pub fn set_replica_coordinator(
        &self,
        coord: noxu_dbi::SharedReplicaAckCoordinator,
    ) {
        *self.replica_coordinator.lock() = Some(coord);
    }

    /// Clear any installed replica-ack coordinator.
    ///
    /// Subsequent `commit_with_durability` calls revert to local-only
    /// durability semantics.
    pub fn clear_replica_coordinator(&self) {
        *self.replica_coordinator.lock() = None;
    }

    /// Set the per-commit timeout used when waiting for replica
    /// acknowledgments.
    ///
    /// Default is 5 seconds.  Mirrors
    /// `noxu_rep::RepConfig::replica_ack_timeout`.
    pub fn set_replica_ack_timeout(&self, timeout: std::time::Duration) {
        *self.replica_ack_timeout.lock() = timeout;
    }

    /// Returns the per-commit replica-ack timeout.
    pub fn get_replica_ack_timeout(&self) -> std::time::Duration {
        *self.replica_ack_timeout.lock()
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

    /// Returns the mutable subset of environment configuration.
    ///
    /// Mirrors `Environment.getMutableConfig()`.  The returned struct reflects the
    /// current runtime values; pass it (modified) to `set_mutable_config()` to
    /// apply changes without re-opening the environment.
    pub fn get_mutable_config(&self) -> Result<EnvironmentMutableConfig> {
        self.check_open()?;
        Ok(EnvironmentMutableConfig {
            cache_size: Some(self.config.cache_size as usize),
            durability: None,
            txn_no_sync: self.config.txn_no_sync,
            txn_write_no_sync: self.config.txn_write_no_sync,
            run_cleaner: Some(self.config.run_cleaner),
            run_checkpointer: Some(self.config.run_checkpointer),
            run_evictor: Some(self.config.run_evictor),
            lock_timeout_ms: Some(self.config.lock_timeout_ms),
            txn_timeout_ms: Some(self.config.txn_timeout_ms),
            cleaner_min_utilization: Some(
                self.config.cleaner_min_utilization as u32,
            ),
        })
    }

    /// Applies a set of mutable configuration changes to the running environment.
    ///
    /// Mirrors `Environment.setMutableConfig(EnvironmentMutableConfig)`.
    /// Only the fields that differ from their sentinel "no-change" values are
    /// applied (`None` means unchanged).  `Some(0)` for a timeout clears it
    /// (matches JE: 0 = no timeout).
    ///
    /// # Errors
    /// Returns an error if the environment is closed or invalidated.
    pub fn set_mutable_config(
        &mut self,
        cfg: EnvironmentMutableConfig,
    ) -> Result<()> {
        self.check_open()?;
        if let Some(sz) = cfg.cache_size {
            self.config.cache_size = sz as u64;
            // Audit transaction-env F7 (Wave 2C-4): push the cache-size
            // change to the evictor's Arbiter so it actually takes
            // effect at runtime; pre-fix the value was only recorded in
            // `self.config`.
            let env_impl = self.env_impl.lock();
            let evictor = env_impl.get_evictor();
            evictor.get_arbiter().set_max_memory(sz as i64);
        }
        if let Some(ms) = cfg.lock_timeout_ms {
            self.config.lock_timeout_ms = ms;
            // Push the new default to the live LockManager.
            let env_impl = self.env_impl.lock();
            env_impl.get_lock_manager().set_lock_timeout(ms);
        }
        if let Some(ms) = cfg.txn_timeout_ms {
            self.config.txn_timeout_ms = ms;
            // The TxnManager does not currently track a default txn
            // timeout (each Txn snapshots the value at `begin_txn` from
            // its own TransactionConfig).  We record the new env-level
            // default here so that future `begin_transaction` calls that
            // rely on the env default pick it up; live txns keep their
            // original timeout.  Tracked under transaction-env F7
            // residual; pushing into running txns requires a TxnManager
            // API change beyond Wave 2C-4.
        }
        // DBI-10 / JE EnvConfigObserver: push the mutable cleaner
        // minUtilization to the running cleaner (noxu.cleaner.minUtilization
        // has mutable=true). Mirrors how JE's Cleaner re-reads
        // CLEANER_MIN_UTILIZATION on envConfigUpdate.
        if let Some(pct) = cfg.cleaner_min_utilization {
            self.config.cleaner_min_utilization = pct.min(100) as u8;
            let env_impl = self.env_impl.lock();
            if let Some(cleaner) = env_impl.get_cleaner() {
                cleaner.set_min_utilization(pct);
            }
        }
        self.config.txn_no_sync = cfg.txn_no_sync;
        self.config.txn_write_no_sync = cfg.txn_write_no_sync;
        // Daemon enable/disable flags are advisory at runtime; dbi-level wiring
        // for live daemon pause/resume is future work (mirrors where
        // setMutableConfig re-reads the flag on next daemon wakeup).
        if let Some(v) = cfg.run_cleaner {
            self.config.run_cleaner = v;
        }
        if let Some(v) = cfg.run_checkpointer {
            self.config.run_checkpointer = v;
        }
        if let Some(v) = cfg.run_evictor {
            self.config.run_evictor = v;
        }
        Ok(())
    }

    /// Runs a checkpoint.
    ///
    /// Mirrors `Environment.checkpoint(CheckpointConfig)`.  If the environment has
    /// no checkpointer (e.g. non-transactional or in-memory), this is a no-op.
    ///
    /// # Arguments
    /// * `config` - Optional checkpoint options (force, thresholds, etc.)
    ///
    /// # Errors
    /// Returns an error if the environment is closed, invalidated, or if the
    /// checkpoint itself fails (e.g. disk write error).
    pub fn checkpoint(&self, config: Option<&CheckpointConfig>) -> Result<()> {
        self.check_open()?;

        // Audit transaction-env F6 (Wave 2C-4): honour `force` /
        // `k_bytes` / `minutes` / `minimize_recovery_time` in
        // `CheckpointConfig`.  Pre-fix the entire config was a no-op.
        // Threshold gating happens in the wrapper layer; the underlying
        // `noxu_recovery::Checkpointer::do_checkpoint` is invoker-only.
        let cfg = config.cloned().unwrap_or_default();

        if !cfg.force {
            // k_bytes: skip the checkpoint if not enough log bytes have
            // been written since the last successful checkpoint.
            if cfg.k_bytes > 0 {
                let cur_lsn = self
                    .log_manager
                    .as_ref()
                    .map(|lm| lm.get_end_of_log())
                    .unwrap_or(noxu_util::NULL_LSN);
                let last = *self.last_checkpoint_end_lsn.lock();
                let bytes_written =
                    cur_lsn.as_u64().saturating_sub(last.as_u64());
                let threshold = (cfg.k_bytes as u64) * 1024;
                if bytes_written < threshold {
                    log::debug!(
                        "checkpoint: skipping (k_bytes threshold {} not \
                         met, only {} bytes since last checkpoint)",
                        threshold,
                        bytes_written,
                    );
                    return Ok(());
                }
            }

            // minutes: skip the checkpoint if not enough wall-clock time
            // has elapsed since the last successful checkpoint.
            if cfg.minutes > 0 {
                let last_at = *self.last_checkpoint_time.lock();
                if let Some(at) = last_at {
                    let elapsed = at.elapsed();
                    let threshold =
                        std::time::Duration::from_secs(cfg.minutes as u64 * 60);
                    if elapsed < threshold {
                        log::debug!(
                            "checkpoint: skipping (minutes threshold {:?} \
                             not met, only {:?} since last checkpoint)",
                            threshold,
                            elapsed,
                        );
                        return Ok(());
                    }
                }
            }
        }

        // `minimize_recovery_time` is currently advisory — the recovery
        // checkpointer always writes the full set of dirty BINs; the
        // "minimal" path requires a pluggable BIN-flush filter that is
        // outside the scope of Wave 2C-4.  We surface the request in the
        // invoker label so it shows up in structured logs.
        let invoker = match (cfg.force, cfg.minimize_recovery_time) {
            (true, true) => "manual_force_full",
            (true, false) => "manual_force",
            (false, true) => "manual_full",
            (false, false) => "manual",
        };

        let env_impl = self.env_impl.lock();
        env_impl
            .run_checkpoint_with_invoker(invoker)
            .map_err(|e| NoxuError::environment(e.to_string()))?;
        drop(env_impl);

        // Update bookkeeping so subsequent threshold-gated calls can
        // honour `k_bytes` / `minutes`.
        *self.last_checkpoint_time.lock() = Some(Instant::now());
        if let Some(lm) = &self.log_manager {
            *self.last_checkpoint_end_lsn.lock() = lm.get_end_of_log();
        }
        Ok(())
    }

    /// Returns `true` if the environment is open and has not been invalidated by a fatal error.
    ///
    /// Mirrors `Environment.isValid()`.  Returns `false` after the environment is closed
    /// or after an `EnvironmentFailure` whose `reason.invalidates_environment()` returns
    /// `true` (e.g. `LogChecksum`, `BtreeCorruption`, `DiskLimit`).
    /// Once invalidated the environment must be closed and re-opened.
    pub fn is_valid(&self) -> bool {
        self.open.load(Ordering::Acquire)
            && self.env_valid.load(Ordering::Acquire)
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
    /// Mirrors `Environment.getStats(StatsConfig)`.
    pub fn get_stats(&self) -> Result<EnvironmentStats> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let n_databases = env_impl.n_databases() as u32;
        // Use cached log_manager for the log stats to avoid double-locking.
        let log = self
            .log_manager
            .as_ref()
            .map(|lm| LogStatsSnapshot::from(&lm.get_stats()))
            .unwrap_or_default();
        let lock =
            LockStatsSnapshot::from(&env_impl.get_lock_manager().get_stats());
        let txn =
            TxnStatsSnapshot::from(&env_impl.get_txn_manager().get_stats());
        let throughput = env_impl.get_throughput_snapshot();
        let evictor =
            EvictorStatsSnapshot::from(env_impl.get_evictor().get_stats());
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
        self.log_manager.as_ref().map(|lm| lm.fsync_count()).unwrap_or(0)
    }

    // -------------------------------------------------------------------
    // Wave 3-2: XA crash-durable two-phase commit support
    // -------------------------------------------------------------------

    /// Returns the list of XA in-doubt prepared transactions surfaced by
    /// the most recent recovery pass.
    ///
    /// The XA layer (`noxu_xa::XaEnvironment::xa_recover`) reads this
    /// list to populate its return value with XIDs that completed phase
    /// 1 of two-phase commit but were not committed or aborted before
    /// the previous shutdown / crash.  An empty `Vec` means there are
    /// no in-doubt transactions to resolve.
    ///
    /// See `noxu_xa` for XA two-phase commit.
    pub fn recovered_prepared_txns(
        &self,
    ) -> Vec<noxu_recovery::PreparedTxnInfo> {
        let env_impl = self.env_impl.lock();
        env_impl.recovered_prepared_txns()
    }

    /// Removes and returns the LN replay list for a recovered prepared
    /// transaction.
    ///
    /// Used by `xa_commit(xid)` after locating the txn id from
    /// [`Self::recovered_prepared_txns`].  The XA layer iterates the
    /// returned list and applies each LN to the in-memory tree before
    /// writing the `TxnCommit` WAL frame.
    pub fn take_recovered_prepared_lns(
        &self,
        txn_id: u64,
    ) -> Vec<noxu_recovery::PreparedLnReplay> {
        let env_impl = self.env_impl.lock();
        env_impl.take_recovered_prepared_lns(txn_id)
    }

    /// Removes a recovered prepared txn entry after the XA layer has
    /// successfully resolved it (`xa_commit` or `xa_rollback`).
    /// Idempotent.
    pub fn forget_recovered_prepared_txn(&self, txn_id: u64) {
        let env_impl = self.env_impl.lock();
        env_impl.forget_recovered_prepared_txn(txn_id);
    }

    /// Writes a `TxnCommit` WAL frame for `txn_id` and fsyncs.
    ///
    /// Used by `xa_commit(xid)` to durably resolve a recovered prepared
    /// transaction without requiring an in-memory `Txn` (which the
    /// crash destroyed).  The caller must have already replayed any
    /// LNs into the in-memory tree via
    /// [`Self::take_recovered_prepared_lns`] and applied them.
    ///
    /// See `noxu_xa` for XA two-phase commit.
    pub fn write_txn_commit_for_recovered(&self, txn_id: u64) -> Result<()> {
        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(()), // Non-transactional env (shouldn't happen).
        };
        // R-3: pre-allocate the VLSN BEFORE writing the WAL entry so the
        // TxnCommit record carries it.  On a second crash, the X-14 VLSN
        // rebuild scans TxnCommit records with non-NULL dtvlsn and includes
        // them — fixing the double-crash VLSN loss reported in Keith R-3.
        let pre_vlsn =
            if let Some(coord) = self.replica_coordinator.lock().as_ref() {
                coord.pre_alloc_vlsn_for_recovered_commit()
            } else {
                0
            };

        let commit_lsn = write_txn_end_for_recovered(
            lm, txn_id, true, /* is_commit */
            true, /* fsync */
            true, /* flush */
            pre_vlsn,
        )?;

        // Register the pre-allocated VLSN in the VlsnIndex now that we have
        // the actual commit LSN.  Also keep the legacy alloc path for any
        // coordinator that doesn't implement pre_alloc (returns 0).
        if let Some(coord) = self.replica_coordinator.lock().as_ref() {
            if pre_vlsn > 0 {
                coord.register_recovered_commit_vlsn(pre_vlsn, commit_lsn);
                log::debug!(
                    "write_txn_commit_for_recovered: txn_id={} commit_lsn={:?} \
                     embedded+registered vlsn={} (R-3)",
                    txn_id,
                    commit_lsn,
                    pre_vlsn
                );
            } else {
                // Fallback: coordinator returned 0 for pre_alloc (non-master
                // or non-replicated); try the legacy single-step allocator.
                let vlsn = coord.alloc_vlsn_for_recovered_commit(commit_lsn);
                if vlsn > 0 {
                    log::debug!(
                        "write_txn_commit_for_recovered: txn_id={} commit_lsn={:?} \
                         assigned vlsn={} (X-3 legacy path)",
                        txn_id,
                        commit_lsn,
                        vlsn
                    );
                }
            }
        }
        Ok(())
    }

    /// Writes a `TxnAbort` WAL frame for `txn_id`.  Used by `xa_rollback(xid)`
    /// to durably resolve a recovered prepared transaction.
    pub fn write_txn_abort_for_recovered(&self, txn_id: u64) -> Result<()> {
        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(()),
        };
        write_txn_end_for_recovered(
            lm, txn_id, false, /* is_commit */
            false, /* fsync */
            false, /* flush */
            0,     /* vlsn: NULL_VLSN for abort */
        )
        .map(|_| ())
    }

    /// Replays a recovered prepared transaction’s LNs into the in-memory
    /// tree at `xa_commit` resolution time.
    ///
    /// Iterates the LN list (already removed from the recovered map by
    /// the caller) and applies each insert/update/delete to the
    /// matching `DatabaseImpl`'s tree.  This makes the prepared writes
    /// observable to subsequent reads in the same process — without
    /// this step, a recovered+committed XA branch's writes would only
    /// become visible after a second recovery on the next reopen.
    pub fn apply_recovered_prepared_lns(
        &self,
        lns: &[noxu_recovery::PreparedLnReplay],
    ) -> Result<()> {
        let env_impl = self.env_impl.lock();
        for ln in lns {
            let db_id = noxu_dbi::DatabaseId::new(ln.db_id as i64);
            let Some(db_arc) = env_impl.get_database_by_id(db_id) else {
                continue;
            };
            let db_guard = db_arc.read();
            let Some(tree) = db_guard.get_real_tree() else {
                continue;
            };
            match ln.operation {
                noxu_recovery::PreparedLnOperation::Insert
                | noxu_recovery::PreparedLnOperation::Update => {
                    if let Some(data) = &ln.data {
                        let _ = tree.insert(
                            ln.key.clone(),
                            data.clone(),
                            ln.original_lsn,
                        );
                    }
                }
                noxu_recovery::PreparedLnOperation::Delete => {
                    if tree.delete(&ln.key) {
                        db_guard.decrement_entry_count();
                    }
                }
            }
        }
        Ok(())
    }

    /// Verifies the structural integrity of all databases in this environment.
    ///
    /// Iterates every open `DatabaseImpl` in the environment's db_map and
    /// calls `verify_database_impl()` on each one (B-tree key-order checks,
    /// LSN validity, child-pointer completeness).  Results are merged into a
    /// single `VerifyResult`.
    ///
    /// Mirrors `Environment.verify(VerifyConfig, PrintStream)` in
    /// creates a `BtreeVerifier` and calls `verifier.verifyAll()`.
    ///
    /// # Arguments
    /// * `config` - Verification options (btree, log, checksums, max_errors).
    ///
    /// # Returns
    /// A combined `VerifyResult` over all databases.
    ///
    /// # Errors
    /// Returns an error if the environment is closed or invalidated.
    pub fn verify(
        &self,
        config: &noxu_engine::VerifyConfig,
    ) -> Result<noxu_engine::VerifyResult> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let all_dbs = env_impl.get_all_database_impls();
        // Lock the live utilization tracker for the checkLsns half
        // (VerifyUtils.checkLsns): live tree LSNs must be DISJOINT from the
        // obsolete LSNs recorded in the UtilizationProfile/tracker. Held
        // across the loop (no clone); read-only envs have no tracker.
        let tracker_guard =
            env_impl.get_utilization_tracker().map(|t| t.lock());

        let mut merged = noxu_engine::VerifyResult::new();
        for db_arc in &all_dbs {
            let guard = db_arc.read();
            let result = noxu_engine::verify_database_impl(&guard, config);
            merged.databases_verified += result.databases_verified;
            merged.records_verified += result.records_verified;
            for err in result.errors {
                merged.add_error(err);
                if merged.error_count() >= config.max_errors as usize {
                    return Ok(merged);
                }
            }
            for w in result.warnings {
                merged.add_warning(w);
            }
            // CLN-2 / VerifyUtils.checkLsns: assert live-tree LSNs are
            // disjoint from the obsolete set.
            if let Some(ref t) = tracker_guard {
                noxu_engine::check_lsns_against_tracker(&guard, t, &mut merged);
                if merged.error_count() >= config.max_errors as usize {
                    return Ok(merged);
                }
            }
        }
        Ok(merged)
    }

    /// Explicitly trigger BIN compression for all open databases.
    ///
    /// Mirrors `Environment.compress()` in JE (`Environment.java:1887`).
    /// Synchronously runs one pass of the INCompressor logic: finds every
    /// BIN with known-deleted slots and compresses them.  Useful in tests
    /// to drain the compressor queue before taking a checkpoint, and for
    /// applications that want deterministic memory reclamation after bulk
    /// deletes.
    ///
    /// Returns `Ok(n)` where `n` is the number of BINs compressed.  Returns
    /// `Err` if the environment is closed or invalid.
    pub fn compress(&self) -> Result<usize> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let n = env_impl.compress_all();
        Ok(n)
    }

    /// Explicitly clean (garbage-collect) log files, returning the number of
    /// files that were cleaned.
    ///
    /// Mirrors `Environment.cleanLog()` in JE (`Environment.java`). Drives the
    /// log cleaner synchronously to migrate live LNs forward and mark
    /// low-utilization files for deletion. This is the manual counterpart to
    /// the background cleaner daemon, used to reclaim space on demand.
    ///
    /// Unlike the throttled daemon pass, this performs a forced cleaning pass
    /// (JE's `cleanLog()` likewise cleans regardless of the daemon's
    /// utilization budget), so callers can deterministically reclaim obsolete
    /// files in tests and batch maintenance.
    ///
    /// Returns `Ok(files_cleaned)`. Returns `Err` if the environment is closed
    /// or read-only (no cleaner is available).
    pub fn clean_log(&self) -> Result<u32> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let result = env_impl
            .run_cleaner(u32::MAX, true)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        Ok(result.files_cleaned)
    }

    /// Explicitly trigger the memory evictor.
    ///
    /// Mirrors `Environment.evictMemory()` in JE (`Environment.java:1860`).
    /// Requests that the cache evictor free cache pages down toward the
    /// configured cache size.  Useful after bulk inserts to reclaim memory
    /// proactively rather than waiting for the background daemon.
    ///
    /// Returns `Ok(bytes_evicted)`.  Returns `Err` if the environment is
    /// closed or invalid.
    pub fn evict_memory(&self) -> Result<usize> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let bytes = env_impl.evict_memory();
        Ok(bytes)
    }
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
    /// Historically a no-op call site; now superseded by
    /// `Transaction::commit` / `Transaction::abort` calling
    /// `ActiveTxns::mark_complete` directly via the shared `Arc<ActiveTxns>`.
    /// Kept for backwards compatibility with internal tests.
    pub(crate) fn mark_transaction_complete(&self, txn_id: u64) {
        self.active_txns.mark_complete(txn_id);
    }

    fn check_open(&self) -> Result<()> {
        if !self.open.load(Ordering::Acquire) {
            return Err(NoxuError::EnvironmentClosed);
        }
        if !self.env_valid.load(Ordering::Acquire) {
            return Err(NoxuError::environment_with_reason(
                crate::error::EnvironmentFailureReason::ForcedShutdown,
                "environment has been invalidated due to a prior fatal error"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Every mutating env-layer
    /// operation funnels through this helper so a `read_only=true`
    /// environment cannot create / remove / rename / truncate databases
    /// nor begin a (writable) transaction.
    fn check_writable(&self, what: &str) -> Result<()> {
        self.check_open()?;
        if self.config.read_only {
            return Err(NoxuError::OperationNotAllowed(format!(
                "{what}: environment is read-only",
            )));
        }
        Ok(())
    }
}

/// Helper used by `Environment::write_txn_commit_for_recovered` and
/// `write_txn_abort_for_recovered` to write a `TxnCommit` / `TxnAbort` WAL
/// frame for a transaction id that has no in-memory `Txn` (the original
/// process crashed before it could commit; recovery surfaced it via
/// `recovered_prepared_txns`).
///
/// `vlsn` is the pre-allocated VLSN to embed in the `dtvlsn` field of the
/// TxnEndEntry payload.  Pass `NULL_VLSN` (0) for non-replicated environments.
/// The R-3 fix requires the VLSN to be embedded so the X-14 VLSN rebuild
/// on a second crash can reconstruct the VLSN index from TxnCommit records.
///
/// XA two-phase commit support.
fn write_txn_end_for_recovered(
    lm: &LogManager,
    txn_id: u64,
    is_commit: bool,
    fsync: bool,
    flush: bool,
    vlsn: u64,
) -> Result<noxu_util::Lsn> {
    use bytes::BytesMut;
    use noxu_log::{LogEntryType, Provisional, entry::TxnEndEntry};
    use noxu_util::{
        lsn::NULL_LSN,
        vlsn::{NULL_VLSN, Vlsn},
    };

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // R-3: embed the pre-allocated VLSN (if any) in the dtvlsn field so the
    // X-14 VLSN rebuild on second crash can find it in TxnCommit records.
    let dtvlsn = if vlsn > 0 { Vlsn::new(vlsn as i64) } else { NULL_VLSN };

    let entry = if is_commit {
        TxnEndEntry::new_commit(txn_id as i64, NULL_LSN, timestamp, 0, dtvlsn)
    } else {
        TxnEndEntry::new_abort(txn_id as i64, NULL_LSN, timestamp, 0, NULL_VLSN)
    };

    let entry_type = if is_commit {
        LogEntryType::TxnCommit
    } else {
        LogEntryType::TxnAbort
    };

    let mut buf = BytesMut::with_capacity(entry.log_size());
    entry.write_to_log(&mut buf);

    lm.log(entry_type, &buf, Provisional::No, flush, fsync).map_err(|e| {
        NoxuError::environment_with_reason(
            crate::error::EnvironmentFailureReason::LogWrite,
            e.to_string(),
        )
    })
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
        let config = EnvironmentConfig::new(home).with_allow_create(false);

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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let _db = env.open_database(None, "testdb", &db_config).unwrap();

        let result = env.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_open_database() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();
        assert_eq!(db.get_database_name(), "testdb");
        assert!(db.is_valid());
    }

    #[test]
    fn test_open_database_twice_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let result = env.open_database(None, "", &db_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_database() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();
        db.close().unwrap();

        env.rename_database(None, "testdb", "testdb").unwrap();
    }

    #[test]
    fn test_rename_open_database_fails() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let _db1 = env.open_database(None, "db1", &db_config).unwrap();
        let _db2 = env.open_database(None, "db2", &db_config).unwrap();

        let names = env.get_database_names().unwrap();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"db1".to_string()));
        assert!(names.contains(&"db2".to_string()));
    }

    /// C-4 / JE 1-I: a database opened inside an explicit transaction that is
    /// subsequently aborted must NOT persist after env close + reopen.
    #[test]
    fn test_transactional_open_database_abort_removes_db() {
        let (temp_dir, config) = temp_env_config();
        {
            let env = Environment::open(config).unwrap();
            let txn = env.begin_transaction(None).unwrap();
            let db_config = DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true);
            let _db = env
                .open_database(Some(&txn), "aborted_db", &db_config)
                .unwrap();
            txn.abort().unwrap();
            // After abort the database must not appear in the committed list.
            let names = env.get_database_names().unwrap();
            assert!(
                !names.contains(&"aborted_db".to_string()),
                "aborted database must not appear in get_database_names() \
                 (C-4 committed-only semantics), got: {:?}",
                names
            );
            drop(env);
        }
        // Reopen: the aborted database must NOT have been written to the WAL.
        let env2 = Environment::open(
            EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(false)
                .with_transactional(true),
        )
        .unwrap();
        let names2 = env2.get_database_names().unwrap();
        assert!(
            !names2.contains(&"aborted_db".to_string()),
            "after env reopen, aborted database must not appear: {:?}",
            names2
        );
    }

    /// C-4 / JE 1-I: `get_database_names()` must NOT return a database that
    /// was opened inside a concurrent uncommitted transaction.
    #[test]
    fn test_get_database_names_excludes_uncommitted() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let txn = env.begin_transaction(None).unwrap();
        let _db =
            env.open_database(Some(&txn), "pending_db", &db_config).unwrap();

        // While txn is still uncommitted, another observer must not see
        // the database in the committed-names list.
        let names = env.get_database_names().unwrap();
        assert!(
            !names.contains(&"pending_db".to_string()),
            "uncommitted database must be invisible to get_database_names() \
             (C-4 / JE 1-J): got {:?}",
            names
        );

        // After commit the database must appear.
        txn.commit().unwrap();
        let names_after = env.get_database_names().unwrap();
        assert!(
            names_after.contains(&"pending_db".to_string()),
            "committed database must appear in get_database_names()"
        );
    }

    #[test]
    fn test_begin_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(false);
        let env = Environment::open(config).unwrap();

        let result = env.begin_transaction(None);
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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        assert!(env.open_database(None, "test", &db_config).is_err());
        assert!(env.remove_database(None, "test").is_err());
        assert!(env.rename_database(None, "a", "b").is_err());
        assert!(env.begin_transaction(None).is_err());
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

        let mut db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
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
        let txn = env.begin_transaction(Some(&txn_config)).unwrap();
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
            let db_arc =
                env_impl.open_database("ghost_db", &dbi_config).unwrap();
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

        let _txn = env.begin_transaction(None).unwrap();

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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
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

        let txn = env.begin_transaction(None).unwrap();
        let txn_id = txn.get_id();

        // Without removing the txn, close would fail.
        // Remove it via the internal API.
        env.mark_transaction_complete(txn_id);

        // Now close should succeed.
        env.close().unwrap();
    }

    // ── verify ─────────────────────────────────────────────────────────────

    #[test]
    fn test_verify_empty_environment_passes() {
        use crate::VerifyConfig;
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        let verify_cfg = VerifyConfig::default();
        let result = env.verify(&verify_cfg).unwrap();
        assert!(result.passed, "empty env should pass: {:?}", result.errors);
    }

    #[test]
    fn test_verify_environment_with_data_passes() {
        use crate::{DatabaseConfig, DatabaseEntry, VerifyConfig};
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let mut db_config = DatabaseConfig::new();
        db_config.set_allow_create(true);
        let db = env.open_database(None, "vtest", &db_config).unwrap();
        for i in 0u32..10 {
            let k = DatabaseEntry::from_bytes(&i.to_be_bytes());
            let v = DatabaseEntry::from_bytes(&(i * 3).to_be_bytes());
            db.put(None, &k, &v).unwrap();
        }

        let verify_cfg = VerifyConfig::default();
        let result = env.verify(&verify_cfg).unwrap();
        assert!(
            result.passed,
            "env with data should pass: {:?}",
            result.errors
        );
        assert!(result.records_verified >= 10);
        db.close().unwrap();
        env.close().unwrap();
    }

    #[test]
    fn test_verify_closed_environment_fails() {
        use crate::VerifyConfig;
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        env.close().unwrap();
        let verify_cfg = VerifyConfig::default();
        assert!(env.verify(&verify_cfg).is_err());
    }

    // ── checkpoint ──────────────────────────────────────────────────────────

    #[test]
    fn test_checkpoint_default_succeeds() {
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        // Transactional env has a checkpointer; call with no config.
        env.checkpoint(None).unwrap();
        env.close().unwrap();
    }

    #[test]
    fn test_checkpoint_with_config_succeeds() {
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        let ckpt_cfg = CheckpointConfig {
            force: true,
            k_bytes: 0,
            minutes: 0,
            minimize_recovery_time: false,
        };
        env.checkpoint(Some(&ckpt_cfg)).unwrap();
        env.close().unwrap();
    }

    #[test]
    fn test_checkpoint_closed_env_fails() {
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        env.close().unwrap();
        assert!(env.checkpoint(None).is_err());
    }

    // ── get_mutable_config / set_mutable_config ──────────────────────────────

    #[test]
    fn test_get_mutable_config_returns_current_values() {
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        let mc = env.get_mutable_config().unwrap();
        // cache_size should be Some() with the default value.
        assert!(mc.cache_size.is_some());
        assert!(mc.run_cleaner.is_some());
        assert!(mc.run_checkpointer.is_some());
        assert!(mc.run_evictor.is_some());
        env.close().unwrap();
    }

    #[test]
    fn test_get_mutable_config_closed_env_fails() {
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        env.close().unwrap();
        assert!(env.get_mutable_config().is_err());
    }

    #[test]
    fn test_set_mutable_config_updates_cache_size() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();
        let new_size: usize = 128 * 1024 * 1024; // 128 MiB
        let mc = EnvironmentMutableConfig::new().with_cache_size(new_size);
        env.set_mutable_config(mc).unwrap();
        let updated = env.get_mutable_config().unwrap();
        assert_eq!(updated.cache_size.unwrap(), new_size);
        env.close().unwrap();
    }

    #[test]
    fn test_set_mutable_config_updates_timeouts() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();
        let mc = EnvironmentMutableConfig {
            lock_timeout_ms: Some(5_000),
            txn_timeout_ms: Some(10_000),
            ..EnvironmentMutableConfig::default()
        };
        env.set_mutable_config(mc).unwrap();
        // After setting, values should be reflected (lock_timeout_ms is advisory at
        // the config layer; verify via get_mutable_config).
        let updated = env.get_mutable_config().unwrap();
        assert_eq!(updated.lock_timeout_ms, Some(5_000));
        assert_eq!(updated.txn_timeout_ms, Some(10_000));
        env.close().unwrap();
    }

    #[test]
    fn test_set_mutable_config_none_timeout_unchanged() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();
        let original = env.get_mutable_config().unwrap();
        // None means "unchanged".  See Wave 1C audit cleanup
        // (Transaction-Env F19/F20): the previous implementation used
        // 0 as the sentinel which prevented users from clearing a
        // timeout.
        let mc = EnvironmentMutableConfig {
            lock_timeout_ms: None,
            txn_timeout_ms: None,
            ..EnvironmentMutableConfig::default()
        };
        env.set_mutable_config(mc).unwrap();
        let updated = env.get_mutable_config().unwrap();
        assert_eq!(updated.lock_timeout_ms, original.lock_timeout_ms);
        assert_eq!(updated.txn_timeout_ms, original.txn_timeout_ms);
        env.close().unwrap();
    }

    #[test]
    fn test_set_mutable_config_closed_env_fails() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();
        env.close().unwrap();
        let mc = EnvironmentMutableConfig::new();
        assert!(env.set_mutable_config(mc).is_err());
    }

    // ========================================================================
    // Audit transaction-env F4 / F5 / F6 / F7 / F10 — Wave 2C-4
    // ========================================================================

    /// F5 — read-only env rejects database creation.
    #[test]
    fn test_read_only_env_rejects_create_database() {
        // First create the env writably so the directory exists.
        let temp_dir = TempDir::new().unwrap();
        {
            let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true);
            let _env = Environment::open(config).unwrap();
        }
        // Re-open read-only.
        let ro_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_read_only(true)
            .with_transactional(true);
        let env = Environment::open(ro_config).unwrap();

        let db_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let result = env.open_database(None, "new", &db_cfg);
        assert!(
            result.is_err(),
            "open_database with allow_create on read-only env must fail",
        );
    }

    /// F5 — read-only env rejects remove_database.
    #[test]
    fn test_read_only_env_rejects_remove_database() {
        let temp_dir = TempDir::new().unwrap();
        {
            let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true);
            let _env = Environment::open(config).unwrap();
        }
        let ro_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_read_only(true)
            .with_transactional(true);
        let env = Environment::open(ro_config).unwrap();

        assert!(env.remove_database(None, "test").is_err());
        assert!(env.truncate_database(None, "test").is_err());
        assert!(env.rename_database(None, "a", "b").is_err());
    }

    /// F5 — read-only env rejects writable transactions.
    #[test]
    fn test_read_only_env_rejects_writable_txn() {
        let temp_dir = TempDir::new().unwrap();
        {
            let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true);
            let _env = Environment::open(config).unwrap();
        }
        let ro_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_read_only(true)
            .with_transactional(true);
        let env = Environment::open(ro_config).unwrap();

        // Default txn config is writable — must be rejected.
        let result = env.begin_transaction(None);
        assert!(result.is_err(), "writable txn on read-only env must fail");

        // Read-only txn must be allowed.
        let ro_txn_cfg = TransactionConfig::default().with_read_only(true);
        let _txn = env
            .begin_transaction(Some(&ro_txn_cfg))
            .expect("read-only txn on read-only env must succeed");
    }

    /// F6 — checkpoint() with `force=false` and a fresh `minutes`
    /// threshold skips the checkpoint when it has just run.
    #[test]
    fn test_checkpoint_minutes_threshold_skips() {
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        // First call: runs (no prior checkpoint).
        env.checkpoint(None).unwrap();

        // Second call with minutes=60 and force=false: should skip.
        let cfg = CheckpointConfig::default().with_minutes(60);
        env.checkpoint(Some(&cfg)).unwrap();
        // No assertion-able effect we can read here, but the call must
        // succeed and not error.

        // Third call with force=true must run regardless.
        let cfg = CheckpointConfig::default().with_force(true).with_minutes(60);
        env.checkpoint(Some(&cfg)).unwrap();
        env.close().unwrap();
    }

    /// F7 — set_mutable_config(cache_size) pushes through to the
    /// evictor's Arbiter.
    #[test]
    fn test_set_mutable_config_pushes_cache_size_to_evictor() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();

        let mc = EnvironmentMutableConfig {
            cache_size: Some(64 * 1024 * 1024),
            ..EnvironmentMutableConfig::default()
        };
        env.set_mutable_config(mc).unwrap();

        let env_impl = env.env_impl.lock();
        let evictor = env_impl.get_evictor();
        assert_eq!(
            evictor.get_arbiter().get_max_memory(),
            64 * 1024 * 1024,
            "set_mutable_config(cache_size) must push to Arbiter",
        );
    }

    /// F4 — env-level `txn_no_sync = true` makes explicit-txn commits
    /// inherit COMMIT_NO_SYNC when the caller does not specify a
    /// TransactionConfig.
    #[test]
    #[allow(deprecated)] // tests the deprecated txn_no_sync flag
    fn test_env_txn_no_sync_applies_to_explicit_txn() {
        let temp_dir = TempDir::new().unwrap();
        let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_txn_no_sync(true);
        let env = Environment::open(config).unwrap();

        let txn = env.begin_transaction(None).unwrap();
        // The transaction must have inherited COMMIT_NO_SYNC.
        let dur = txn.get_durability().expect("durability must be set");
        assert_eq!(
            dur,
            crate::durability::Durability::COMMIT_NO_SYNC,
            "env txn_no_sync=true must propagate to explicit-txn durability",
        );
        txn.commit().unwrap();
        env.close().unwrap();
    }

    /// F10 — dropping an open transaction performs an actual abort,
    /// releasing locks instead of leaking them.
    #[test]
    fn test_drop_aborts_open_transaction() {
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let initial_active = env.active_txns.len();
        {
            let _txn = env.begin_transaction(None).unwrap();
            assert_eq!(env.active_txns.len(), initial_active + 1);
            // Drop _txn at scope exit without commit/abort.
        }
        // After drop, the active-txns registry must have pruned the entry.
        assert_eq!(
            env.active_txns.len(),
            initial_active,
            "Transaction::Drop must abort and prune from active_txns",
        );
        // close() must succeed because no txns remain registered.
        env.close().unwrap();
    }

    // ── X-3: recovered XA commit assigns real VLSN ─────────────────────

    /// X-3: after calling `write_txn_commit_for_recovered` on an environment
    /// wired with a mock replica coordinator, the coordinator's
    /// `alloc_vlsn_for_recovered_commit` must be called with a non-NULL LSN.
    #[test]
    fn test_x3_recovered_commit_calls_alloc_vlsn() {
        use noxu_dbi::{
            AckWaitError, ReplicaAckCoordinator, ReplicaAckPolicyKind,
        };
        use noxu_util::Lsn;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering as AO};
        use std::time::Duration;

        // Mock coordinator that records the LSN passed to alloc_vlsn_for_recovered_commit.
        struct MockCoord {
            last_lsn: AtomicU64,
        }
        impl ReplicaAckCoordinator for MockCoord {
            fn await_replica_acks(
                &self,
                _policy: ReplicaAckPolicyKind,
                _timeout: Duration,
            ) -> std::result::Result<u32, AckWaitError> {
                Ok(0)
            }
            fn alloc_vlsn_for_recovered_commit(&self, lsn: Lsn) -> u64 {
                self.last_lsn.store(lsn.as_u64(), AO::SeqCst);
                // Return the file_number as a fake VLSN (non-zero = success).
                lsn.file_number() as u64 + 1
            }
        }

        let (tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        // Wire the mock coordinator.
        let coord = Arc::new(MockCoord { last_lsn: AtomicU64::new(0) });
        env.set_replica_coordinator(coord.clone());

        // Write a fake txn_id=42 commit (simulates recovered XA).
        env.write_txn_commit_for_recovered(42).unwrap();

        // The coordinator must have been called with a non-zero LSN.
        let recorded_lsn = coord.last_lsn.load(AO::SeqCst);
        assert_ne!(
            recorded_lsn, 0,
            "X-3: alloc_vlsn_for_recovered_commit must be called with the commit LSN"
        );

        env.close().unwrap();
        drop(tmp);
    }

    /// CLN-2 / VerifyUtils.checkLsns wired into Environment::verify.
    ///
    /// A healthy env (live tree LSNs disjoint from the obsolete set) passes
    /// verify; after seeding one live LSN into the obsolete tracker the same
    /// verify FAILS with the "Obsolete LSN set contains valid LSN" error.
    #[test]
    fn test_verify_checklsns_detects_live_in_obsolete() {
        use crate::database_entry::DatabaseEntry;
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "cln2db", &db_config).unwrap();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"alpha"),
            &DatabaseEntry::from_bytes(b"1"),
        )
        .unwrap();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"beta"),
            &DatabaseEntry::from_bytes(b"2"),
        )
        .unwrap();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"gamma"),
            &DatabaseEntry::from_bytes(b"3"),
        )
        .unwrap();

        let cfg = noxu_engine::VerifyConfig::default();

        // Healthy: live LSNs disjoint from obsolete -> verify passes.
        let healthy = env.verify(&cfg).unwrap();
        assert!(
            healthy.is_passed(),
            "healthy env must pass checkLsns: {:?}",
            healthy.errors
        );

        // Grab a real live LSN from the db's tree.
        let live_lsn = {
            let guard = db.db_impl.read();
            let tree = guard
                .get_real_tree()
                .expect("invariant: populated db has a real tree");
            noxu_engine::gather_tree_lsns(&tree)
                .into_iter()
                .next()
                .expect("invariant: at least one live LSN")
        };

        // Seed the bug: record that exact live LSN as obsolete in the live
        // tracker (mislabels live data obsolete -> the cleaner would delete
        // live data).
        {
            let env_impl = env.env_impl.lock();
            let tracker = env_impl
                .get_utilization_tracker()
                .expect("invariant: rw env has a tracker");
            tracker.lock().count_obsolete_node(
                live_lsn.file_number(),
                live_lsn.file_offset(),
                10,
                true,
                None,
            );
        }

        // Now verify must DETECT the overlap.
        let bad = env.verify(&cfg).unwrap();
        assert!(
            !bad.is_passed(),
            "verify must detect live LSN in obsolete set"
        );
        assert!(
            bad.errors.iter().any(|e| matches!(
                e,
                noxu_engine::VerifyError::DataInconsistency { description }
                    if description.contains("Obsolete LSN set contains valid LSN")
            )),
            "expected checkLsns error, got: {:?}",
            bad.errors
        );

        drop(db);
        env.close().unwrap();
    }

    /// DBI-10 / JE EnvConfigObserver + MemoryBudget.envConfigUpdate: a runtime
    /// cache-size change must reach the live evictor Arbiter's max-memory.
    #[test]
    fn test_set_mutable_config_pushes_cache_size_to_arbiter() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();

        let new_cache = 256 * 1024 * 1024_usize; // 256 MiB
        let mc = EnvironmentMutableConfig::new().with_cache_size(new_cache);
        env.set_mutable_config(mc).unwrap();

        let arbiter_max = env.env_impl.lock().get_arbiter_max_memory();
        assert_eq!(
            arbiter_max, new_cache as i64,
            "Arbiter max-memory must reflect the new cache size"
        );
        env.close().unwrap();
    }

    /// DBI-10 / JE EnvConfigObserver: a runtime cleaner minUtilization change
    /// must reach the running cleaner (noxu.cleaner.minUtilization is
    /// mutable). FAILS on main (no propagation); PASSES after the push.
    #[test]
    fn test_set_mutable_config_pushes_cleaner_min_utilization() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();

        let cleaner = env
            .env_impl
            .lock()
            .get_cleaner()
            .expect("invariant: transactional env has a cleaner");
        let before = cleaner.get_min_utilization();

        let new_pct = if before == 70 { 40 } else { 70 };
        let mc = EnvironmentMutableConfig::new()
            .with_cleaner_min_utilization(new_pct);
        env.set_mutable_config(mc).unwrap();

        assert_eq!(
            cleaner.get_min_utilization(),
            new_pct,
            "running cleaner must reflect the new minUtilization"
        );
        env.close().unwrap();
    }
}
