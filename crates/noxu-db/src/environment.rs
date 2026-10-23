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
use noxu_txn::{LockType, Locker};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// Diagnostic snapshot returned by [`Environment::cleaner_diagnostics`].
///
/// Not part of the stable public API surface; shape may change without a
/// major version bump.
#[derive(Debug, Clone)]
pub struct CleanerDiagnostics {
    /// File-selector pipeline-state counts (see
    /// [`noxu_cleaner::FileSelectorStats`]).
    pub file_selector: noxu_cleaner::FileSelectorStats,
    /// Merged per-file utilization summary map (cached profile + live
    /// tracker), keyed by file number — the same map `do_clean` scores
    /// against.
    pub file_summaries:
        std::collections::BTreeMap<u32, noxu_cleaner::FileSummary>,
    /// The cleaner's currently configured `min_utilization` percentage.
    pub min_utilization: u32,
}

/// Build an [`EnvironmentStats`] snapshot from a locked [`EnvironmentImpl`].
///
/// Shared by [`Environment::stats`] and the periodic stats-file dumper
/// ([`crate::stats_file`]) so both produce byte-identical snapshots.  The
/// caller holds the `EnvironmentImpl` lock; `log_manager` is the cached handle
/// (avoids a second lock on the stats hot path).
pub(crate) fn build_environment_stats(
    env_impl: &EnvironmentImpl,
    log_manager: Option<&LogManager>,
    cache_size: u64,
) -> EnvironmentStats {
    let n_databases = env_impl.n_databases() as u32;
    let log = log_manager
        .map(|lm| LogStatsSnapshot::from(&lm.get_stats()))
        .unwrap_or_default();
    let lock =
        LockStatsSnapshot::from(&env_impl.get_lock_manager().get_stats());
    let txn = TxnStatsSnapshot::from(&env_impl.get_txn_manager().get_stats());
    let throughput = env_impl.get_throughput_snapshot();
    // Refresh the instantaneous LRU-size gauges (pri1/pri2) from the live
    // eviction policy before snapshotting, mirroring JE's Evictor.loadStats
    // which recomputes cache-composition stats on demand. Without this the
    // exported noxu_evictor_lru_size gauge reads a stale 0: pri1_lru_size /
    // pri2_lru_size are only otherwise written by update_lru_stats, which had
    // no production caller on the env.stats() path.
    env_impl.get_evictor().update_lru_stats();
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
    EnvironmentStats {
        cache_size,
        cache_usage: env_impl.get_cache_usage().max(0) as u64,
        n_databases,
        log,
        lock,
        txn,
        throughput,
        evictor,
        cleaner,
        checkpoint,
    }
}

/// Maps a [`noxu_dbi::DbiError`] from a single-name DDL op (remove/truncate)
/// to the public [`NoxuError`], turning `DatabaseNotFound` into a
/// user-facing message naming the database.  Shared by the auto-commit and
/// deferred (commit-callback) paths so both surface the same error.
fn map_dbi_err(name: &str) -> impl Fn(noxu_dbi::DbiError) -> NoxuError + '_ {
    move |e| match &e {
        noxu_dbi::DbiError::DatabaseNotFound(_) => NoxuError::DatabaseNotFound(
            format!("Database '{}' does not exist", name),
        ),
        // R-LIVEDDL-711: preserve typed lock/conflict errors (the NameLN lock
        // can time out / deadlock / be denied) so callers see a *retryable*
        // `NoxuError::LockTimeout` / `DeadlockDetected` / `LockConflict`
        // rather than an opaque, non-retryable `EnvironmentFailure`.  JE's
        // `LockConflictException` family is retryable; flattening it here
        // would break at-conflict retry (and `is_retryable()`).
        noxu_dbi::DbiError::LockConflict(_)
        | noxu_dbi::DbiError::TxnError(_) => NoxuError::from(e),
        _ => NoxuError::environment(e.to_string()),
    }
}

/// Maps a [`noxu_dbi::DbiError`] from `rename_database` to the public
/// [`NoxuError`], distinguishing missing-source from destination-exists.
fn map_rename_err<'a>(
    old_name: &'a str,
    new_name: &'a str,
) -> impl Fn(noxu_dbi::DbiError) -> NoxuError + 'a {
    move |e| match &e {
        noxu_dbi::DbiError::DatabaseNotFound(_) => NoxuError::DatabaseNotFound(
            format!("Database '{}' does not exist", old_name),
        ),
        noxu_dbi::DbiError::DatabaseAlreadyExists(_) => {
            NoxuError::DatabaseAlreadyExists(format!(
                "Database '{}' already exists",
                new_name
            ))
        }
        // R-LIVEDDL-711: preserve typed lock/conflict errors (see map_dbi_err).
        noxu_dbi::DbiError::LockConflict(_)
        | noxu_dbi::DbiError::TxnError(_) => NoxuError::from(e),
        _ => NoxuError::environment(e.to_string()),
    }
}

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
    databases: Arc<Mutex<HashMap<String, Arc<DatabaseHandle>>>>,
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
    /// elapses, in which case `NoxuError::InsufficientAcks` is returned --
    /// the txn IS committed+durable at the master; FID-DUR-1).  Closes
    /// finding F1 of the 2026 review.
    replica_coordinator: Mutex<Option<noxu_dbi::SharedReplicaAckCoordinator>>,
    /// Per-commit timeout for replica acknowledgments.  Mirrors
    /// `noxu_rep::RepConfig::replica_ack_timeout`; defaults to 5s.
    replica_ack_timeout: Mutex<std::time::Duration>,
    /// Background stats-file dumper (STATS_FILE_*), started at open when
    /// `stats_collect` is enabled and a stats-file destination is configured.
    /// `None` when stats collection is off.  Stopped (joined) at `close`.
    stats_dumper: Mutex<Option<crate::stats_file::StatsFileDumper>>,

    /// Background B-tree verifier daemon (`VERIFY_SCHEDULE` /
    /// `ENV_RUN_VERIFIER`).  `Some` when `run_verifier` is true AND a
    /// non-empty, well-formed `verify_schedule` was supplied — now the
    /// default (both flags default ON to match JE), so a daemon runs unless
    /// the operator opts out; `None` otherwise.  Stopped
    /// (joined) at `close`.
    verify_daemon: Mutex<Option<crate::verify_daemon::VerifyDaemon>>,
}

/// Internal database handle state.
struct DatabaseHandle {
    #[expect(dead_code)]
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
/// Number of registry shards. Mirrors `noxu-txn`'s `LockManager`
/// (`DEFAULT_N_LOCK_TABLES = 64`): 64 shards spread ~470k begin/complete
/// acquisitions/s (64 threads @ ~235k ops/s) across distinct mutexes so
/// transactions with distinct ids no longer serialize on one global lock.
/// JE uses a `ConcurrentHashMap` for `TxnManager.allTxns` (striped locking)
/// for the same reason; this is the same technique with an explicit shard
/// array, matching the in-repo `lock_manager` sharding pattern.
const N_TXN_SHARDS: usize = 64;

pub(crate) struct ActiveTxns {
    /// One `Mutex<HashMap>` per shard. A transaction id maps to exactly one
    /// shard via `id % N_TXN_SHARDS`, so insert/remove touch a single shard.
    /// Aggregate queries (`len`/`is_empty`) iterate all shards.
    shards: Box<[Mutex<HashMap<u64, Arc<TransactionState>>>]>,
}

impl ActiveTxns {
    fn new() -> Self {
        let shards = (0..N_TXN_SHARDS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect::<Vec<_>>();
        Self { shards: shards.into_boxed_slice() }
    }

    /// The shard owning a given transaction id. Txn ids come from a
    /// monotonic `AtomicU64` starting at 1, so `id % N` distributes them
    /// round-robin across shards with no clustering.
    #[inline]
    fn shard_for(
        &self,
        id: u64,
    ) -> &Mutex<HashMap<u64, Arc<TransactionState>>> {
        &self.shards[(id % N_TXN_SHARDS as u64) as usize]
    }

    /// Registers an active transaction. Locks only the owning shard.
    fn insert(&self, id: u64, state: Arc<TransactionState>) {
        self.shard_for(id).lock().insert(id, state);
    }

    /// Removes the entry for the given transaction id.
    ///
    /// Called by `Transaction::commit_with_durability` and `Transaction::abort`
    /// once the transaction has reached a terminal state. Locks only the
    /// owning shard.
    pub(crate) fn mark_complete(&self, id: u64) {
        self.shard_for(id).lock().remove(&id);
    }

    /// Total active transactions across all shards. Used by `close()` to
    /// detect leaked (never-committed/aborted) transactions, so it must sum
    /// every shard.
    fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }

    /// True only when every shard is empty. Used by `close()`'s leaked-txn
    /// guard, so a single non-empty shard must make this false.
    fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.lock().is_empty())
    }
}

/// R-LIVEDDL-711 (S3): RAII release for an auto-commit catalog name lock.
///
/// An auto-commit (`None` txn) catalog op takes the NameLN lock under a
/// throwaway locker id that no `Txn` owns, so nothing releases it at
/// commit/abort.  Before this guard the release was a straight-line call after
/// the inner op; a PANIC inside the op unwound past it and leaked the lock,
/// permanently wedging every future catalog op on that name (the database
/// became un-removable).  Holding the release in `Drop` makes it run on EVERY
/// exit — normal return, `?` early return, and unwind — so a panic can no
/// longer strand the lock.  `release_name_record` is idempotent and also
/// unregisters the throwaway locker's diagnostic label, so arming the guard
/// even when the acquire failed is correct (it only drops the label).
struct AutoNameLockGuard<'a> {
    env_impl: &'a Arc<Mutex<EnvironmentImpl>>,
    name: &'a str,
    locker_id: i64,
}

impl Drop for AutoNameLockGuard<'_> {
    fn drop(&mut self) {
        self.env_impl.lock().release_name_record(self.name, self.locker_id);
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
        let open_start = std::time::Instant::now();
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
            dos_producer_queue_timeout_ms: config.dos_producer_queue_timeout_ms,
            env_db_eviction: config.env_db_eviction,
            // Memory
            cache_size: config.cache_size,
            cache_percent: config.cache_percent,
            shared_cache: config.shared_cache,
            max_off_heap_memory: config.max_off_heap_memory,
            max_disk: config.max_disk,
            free_disk: config.free_disk,
            reserved_disk: config.reserved_disk,
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
            log_fsync_max_leaders: config.log_fsync_max_leaders,
            log_fsync_adaptive_leaders: config.log_fsync_adaptive_leaders,
            log_fsync_adaptive_trigger: config.log_fsync_adaptive_trigger,
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
            evictor_algorithm: config.evictor_algorithm.clone(),
            evictor_evict_bytes: config.evictor_evict_bytes,
            evictor_critical_percentage: config.evictor_critical_percentage,
            evictor_lru_only: config.evictor_lru_only,
            evictor_use_dirty_lru: config.evictor_use_dirty_lru,
            evictor_mutate_bins: config.evictor_mutate_bins,
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
            .map_err(|e| match e {
                // XDB-711 option A (and any future typed open-path reason):
                // a `LnFetchFailure` carries a specific
                // `EnvironmentFailureReason` (here LOG_INTEGRITY for a catalog
                // damaged by the 7.11.0/.1 relog bug). Preserve it through the
                // typed `From<DbiError>` conversion instead of flattening to
                // UnexpectedState, so callers see the real reason
                // (EnvironmentFailureReason::LogIntegrity) and the JE message.
                noxu_dbi::DbiError::LnFetchFailure { .. } => NoxuError::from(e),
                // Every other open failure keeps its historical shape
                // (EnvironmentFailure/UnexpectedState with the stringified
                // cause) — no behaviour change.
                _ => NoxuError::environment(e.to_string()),
            })?;

        // exception_listener (JE ExceptionListener): if the application
        // registered a listener, install a daemon exception sink that adapts
        // the daemon's (source, message) callback into the public
        // `ExceptionEvent`.  Installed here — before any daemon performs work
        // (each daemon sleeps its wakeup interval first) — so no async daemon
        // failure is missed.  Without a listener the sink is never installed
        // and dispatch is a no-op.
        if let Some(listener) = config.exception_listener() {
            env_impl.set_exception_sink(std::sync::Arc::new(
                move |source: &str, message: &str| {
                    let src = match source {
                        "Checkpointer" => {
                            crate::error::ExceptionSource::Checkpointer
                        }
                        "Cleaner" => crate::error::ExceptionSource::Cleaner,
                        "Evictor" => crate::error::ExceptionSource::Evictor,
                        "INCompressor" => {
                            crate::error::ExceptionSource::INCompressor
                        }
                        "Verifier" => crate::error::ExceptionSource::Verifier,
                        other => crate::error::ExceptionSource::Unknown(
                            other.to_string(),
                        ),
                    };
                    let thread_name = std::thread::current()
                        .name()
                        .unwrap_or("<unnamed>")
                        .to_string();
                    let event = crate::error::ExceptionEvent::new(
                        message.to_string(),
                        src,
                        thread_name,
                    );
                    listener.exception_event(&event);
                },
            ));
        }

        let log_manager = env_impl.get_log_manager();
        let env_impl_arc = Arc::new(Mutex::new(env_impl));

        // STATS_FILE_*: start the periodic stats-file dumper (JE StatCapture)
        // when stats collection is enabled.  The dumper samples the same
        // snapshot `stats()` returns and appends a rotating CSV to
        // `stats_file_directory` (default: the env home).  On by default
        // (stats_collect = true, matching JE STATS_COLLECT).
        let stats_dumper = if config.stats_collect {
            let dir = config
                .stats_file_directory
                .clone()
                .unwrap_or_else(|| home.clone());
            let interval = std::time::Duration::from_secs(
                config.stats_collect_interval_secs.max(1),
            );
            Some(crate::stats_file::StatsFileDumper::start(
                Arc::clone(&env_impl_arc),
                log_manager.clone(),
                config.cache_size,
                dir,
                interval,
                config.stats_file_row_count,
                config.stats_max_files,
            ))
        } else {
            None
        };

        // VERIFY_SCHEDULE / ENV_RUN_VERIFIER: start the background B-tree
        // verifier daemon when `run_verifier` is true (now the default, matching
        // JE ENV_RUN_VERIFIER) AND a non-empty, well-formed cron schedule was
        // supplied (the default `verify_schedule` is "0 0 * * *", also matching
        // JE, so the daemon runs by default).  Setting `run_verifier = false`
        // or clearing `verify_schedule` spawns nothing.  The daemon re-uses the
        // same public verification walk `Environment::verify` runs (it does
        // NOT touch verify's internals) on the shared EnvironmentImpl.  It is
        // env-owned (not registered with the engine DaemonManager) so it
        // cannot perturb the daemon-manager shutdown ordering.
        // JE ref: EnvironmentParams.ENV_RUN_VERIFIER / VERIFY_SCHEDULE,
        // com.sleepycat.je.dbi.DataVerifier.
        let verify_daemon =
            if config.run_verifier && !config.verify_schedule.is_empty() {
                match crate::verify_daemon::CronSchedule::parse(
                    &config.verify_schedule,
                ) {
                    Some(schedule) => {
                        let vconfig = noxu_engine::VerifyConfig::new()
                            .with_btree_verification(true);
                        Some(crate::verify_daemon::VerifyDaemon::start(
                            Arc::clone(&env_impl_arc),
                            schedule,
                            vconfig,
                        ))
                    }
                    None => {
                        log::warn!(
                            "verify_schedule={:?} is not a valid 5-field cron \
                         expression; the background verifier will NOT run",
                            config.verify_schedule,
                        );
                        None
                    }
                }
            } else {
                None
            };

        let env = Environment {
            home,
            config,
            databases: Arc::new(Mutex::new(HashMap::new())),
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
            stats_dumper: Mutex::new(stats_dumper),
            verify_daemon: Mutex::new(verify_daemon),
        };

        // STARTUP_DUMP_THRESHOLD: if opening the environment (dominated by
        // crash-recovery: the analysis + redo + undo passes) took longer than
        // the configured threshold, log a one-line startup performance summary
        // plus a stats snapshot so operators can see WHY a slow start happened.
        // A threshold of 0 (the default) disables the dump.
        // JE ref: EnvironmentParams.STARTUP_DUMP_THRESHOLD / EnvironmentImpl
        // dumping the startup RecoveryInfo + EnvironmentStats when startup is
        // slow.
        let threshold_ms = env.config.startup_dump_threshold_ms;
        if threshold_ms > 0 {
            let elapsed_ms = open_start.elapsed().as_millis() as u64;
            if Self::startup_dump_triggered(elapsed_ms, threshold_ms) {
                match env.stats() {
                    Ok(stats) => log::warn!(
                        "startup dump: Environment::open took {elapsed_ms} ms \
                         (threshold {threshold_ms} ms). Startup is dominated by \
                         crash recovery. Stats snapshot after open: {stats:?}"
                    ),
                    Err(e) => log::warn!(
                        "startup dump: Environment::open took {elapsed_ms} ms \
                         (threshold {threshold_ms} ms); stats unavailable: {e}"
                    ),
                }
            }
        }

        Ok(env)
    }

    /// STARTUP_DUMP_THRESHOLD predicate: returns `true` when the measured
    /// `Environment::open` elapsed time (ms) meets or exceeds a non-zero
    /// `threshold_ms`.  Extracted so the decision is unit-testable without a
    /// log-capture harness.  A threshold of 0 disables the dump.
    #[inline]
    fn startup_dump_triggered(elapsed_ms: u64, threshold_ms: u64) -> bool {
        threshold_ms > 0 && elapsed_ms >= threshold_ms
    }

    /// Closes the environment handle.
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
        // Stop the stats-file dumper (join its thread) before tearing down the
        // engine, so it cannot sample a closing EnvironmentImpl.
        if let Some(dumper) = self.stats_dumper.lock().take() {
            dumper.stop();
        }
        // Stop the background verifier daemon (join its thread) before tearing
        // down the engine, so it cannot walk a closing EnvironmentImpl.
        if let Some(verifier) = self.verify_daemon.lock().take() {
            verifier.stop();
        }
        let env_impl = self.env_impl.lock();
        // env_check_leaks (default true): at a clean close all user
        // transactions have released their locks; any lock still held with an
        // owner indicates an application leak (a dropped Transaction, a cursor
        // held open, ...).  Report it (JE EnvironmentImpl leak checking).  This
        // is diagnostic only — it warns, it does not fail the close.
        if self.config.env_check_leaks {
            let leaks = env_impl.get_lock_manager().report_leaked_locks();
            if !leaks.is_empty() {
                let total_locks: usize =
                    leaks.iter().map(|(_, owners)| owners.len()).sum();
                log::warn!(
                    "env_check_leaks: {} lock(s) still held by {} locker(s) at \
                     Environment::close — an application likely leaked a \
                     transaction or cursor. Leaked (lsn, owners): {:?}",
                    leaks.len(),
                    total_locks,
                    leaks,
                );
            }
        }
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

        // R-REPUSER (BUG-REP-USER-WRITES-NOT-REPLICATED, S1): refuse DDL that
        // CREATES a new database on a master-wired env.  A create logs a
        // NameLN/MapLN that is NOT shipped to replicas in 7.11.x (DDL replay is
        // missing), so creating a database here would silently diverge the
        // group.  Scope the refusal to an actual create (allow_create AND the
        // name is not already a committed database) so re-OPENING an existing
        // database (a read) still works -- tests open databases BEFORE wiring
        // the coordinator; only a create AFTER wiring is refused.  JE screens
        // the NameLN write at the operation (`DbTree.doCreateDb` ->
        // `checkReplicaWrite`).
        if config.allow_create {
            let env_impl = self.env_impl.lock();
            if let Some(reason) = env_impl.refuse_user_writes_reason() {
                let already =
                    env_impl.get_database_names().iter().any(|n| n == name);
                drop(env_impl);
                if !already {
                    return Err(NoxuError::ReplicationNotSupported {
                        reason: reason.to_string(),
                    });
                }
            }
        }

        // NEW-TXN-1: reject a closed / non-open transaction, mirroring the
        // data path.  JE rejects ANY operation on a closed Locker/Transaction
        // (TxnEndTest.testClose: `env.openDatabase(closedTxn, ...)` throws).
        // On the Noxu data path, `Txn::check_state` (crates/noxu-txn/src/txn.rs)
        // returns `TxnError::InvalidTransaction` for every state except `Open`,
        // which `put_in` / `get_into` surface as
        // `NoxuError::TransactionAborted`.  `open_database` (a DDL entry point)
        // previously skipped this check, silently accepting a committed/aborted
        // txn.  Mirror `check_state` here: only an `Open` transaction may be
        // used to open/create a database.
        if let Some(t) = txn
            && t.state() != crate::transaction::TransactionState::Open
        {
            return Err(NoxuError::TransactionAborted(format!(
                "txn {}: {:?}",
                t.id(),
                t.state()
            )));
        }

        // ENG-AUTODDL-3 (S3): an auto-commit CREATE (`txn = None`,
        // `allow_create`, name not already committed) must be durable at
        // return, as JE's `openDatabase(txn=null)` is — `DbNameOperation`
        // -> `createAutoTxn` commits the create's NameLN/MapLN at the env
        // default durability (COMMIT_SYNC). The direct auto-commit path wrote
        // the NameLN lazily (`log_name_ln`, flush=false/fsync=false), so a
        // crash after Ok lost the fresh database. Run the create through ONE
        // internal auto-txn and return the resulting handle (its lifetime is
        // independent of the auto-txn, exactly as JE returns the Database from
        // the committed auto-txn). Scope strictly to an actual CREATE of a
        // not-yet-committed name in a transactional env+db: a reopen of an
        // existing database stays on the fast shared-READ path below, and a
        // non-transactional env/db keeps the direct (ThreadLocker) path.
        if txn.is_none()
            && config.allow_create
            && self.config.transactional
            && config.transactional
        {
            let is_new = {
                let env_impl = self.env_impl.lock();
                !env_impl.database_exists(name)
            };
            if is_new {
                return self.run_auto_commit_ddl(|auto| {
                    // auto is always Some here (env is transactional).
                    let t = auto.expect(
                        "invariant: transactional env yields an auto-txn",
                    );
                    self.open_database(Some(t), name, config)
                });
            }
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
        dbi_config.set_replicated(config.replicated);
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
        // DB-TRIG: thread user triggers (runtime-registered, not persisted)
        // through to DatabaseImpl so put/delete/commit/abort fire.  JE
        // `DatabaseConfig.getTriggers` -> `DatabaseImpl.triggers`.
        dbi_config.triggers = config.triggers.0.clone();
        // Resolve the effective fanout (NODE_MAX_ENTRIES): the per-database
        // DatabaseConfig overrides, but when it is unset (0) fall back to the
        // environment-level NODE_MAX_ENTRIES default.  Mirrors JE
        // DatabaseImpl.java:420 ("if maxTreeEntriesPerNode is zero ... set it
        // to the [env] default config value"), so a database created without
        // an explicit per-DB fanout inherits the env default and keeps it
        // across reopen.
        let effective_node_max = if config.node_max_entries > 0 {
            config.node_max_entries
        } else {
            self.config.node_max_entries
        };
        if effective_node_max > 0 {
            dbi_config.set_node_max_entries(effective_node_max as i32);
        }

        // Open the database via EnvironmentImpl (creates if allow_create, else errors)
        // C-4 / JE 1-I: if a transaction is supplied and this is a new
        // creation, use the transactional path so the name registration is
        // deferred until commit.
        let is_transactional_create = txn.is_some() && config.allow_create;
        // NEW-REC-1: LSN of the provisional NameLNTxn written for a
        // transactional create; recorded on the inner Txn below.
        let mut name_ln_lsn: Option<noxu_util::Lsn> = None;

        // R-LIVEDDL-711: the handle-open read path takes a non-blocking READ on
        // the NameLN (JE `DbTree.getDb` + `acquireHandleLock` ->
        // `nonBlockingLock(lsn, READ)`, DbTree.java:706-725/:1524).  While
        // another txn holds the name's WRITE lock for an uncommitted
        // remove/rename, this READ is DENIED and the open conflicts
        // (`LockConflict`, JE `DatabasePreemptedException`) rather than opening
        // a handle on a database about to vanish.  An open under the SAME txn
        // that holds the WRITE (e.g. a transactional truncate opening the db)
        // shares its own lock and does not conflict.  The probe is released
        // immediately after the open for the auto-commit locker; for
        // `Some(txn)` the lock is held by the txn (JE's handle lock lives for
        // the handle's life, and a create's NameLN WRITE lives to txn end).
        let open_probe_guard: Option<AutoNameLockGuard<'_>> =
            if is_transactional_create {
                // XDB-711 #2b: a TRANSACTIONAL create MUST take the catalog
                // NameLN lock under its own txn BEFORE binding the name, held
                // to commit -- JE `DbTree.doCreateDb` takes `lockNameLN(WRITE)`
                // under the creating locker before `nameCursor.insertRecord`
                // (DbTree.java:586-609) and holds it until the txn ends.  Prior
                // to this fix the create took NO name lock (this arm set
                // `open_probe_guard = None`), so a transactional create of a
                // name could bind it concurrently with a rename-to (or another
                // create) of the SAME name: two committed bindings for one
                // name, and recovery's latest-NameLN-wins scan cross-bound them
                // (XDB-711 #2b).  LOCK FIRST, then `open_database_transactional`
                // re-checks existence under the held lock and binds or reopens.
                //
                // JE `Environment.setupDatabase`: `getDb` takes a READ on the
                // NameLN, and only `createDb` (name absent) upgrades to WRITE.
                // Mirror that WRITE/READ split off the committed-existence
                // decision so a transactional REOPEN of an existing db takes a
                // shared READ (no mutual blocking between concurrent openers)
                // and only a fresh CREATE (name absent) takes WRITE.  Taking
                // WRITE unconditionally (the earlier WIP) serialized every
                // reopen and was the source of the ~5x same-name slowdown;
                // distinct-name creates still take WRITE (one record lock per
                // DDL, JE's inherent cost).  Blocking (JE's create blocks
                // behind an in-doubt holder); the txn's `release_all_locks`
                // frees it at commit/abort -- no extra teardown.
                //
                // SAFETY: is_transactional_create implies txn.is_some().
                let t = txn
                    .expect("invariant: txn is Some for transactional create");
                let want_write = {
                    let env_impl = self.env_impl.lock();
                    !env_impl.database_exists(name)
                };
                self.lock_name_record_under_txn_typed(
                    t,
                    name,
                    if want_write { LockType::Write } else { LockType::Read },
                    /*non_blocking=*/ false,
                )?;
                None
            } else if let Some(t) = txn {
                // Probe under the opener's own txn so a truncate/remove that holds
                // the WRITE under this same txn is not self-blocked.
                self.lock_name_record_under_txn_typed(
                    t,
                    name,
                    LockType::Read,
                    /*non_blocking=*/ true,
                )?;
                None
            } else {
                // Auto-commit open/create.  XDB-711 + R-LIVEDDL-711: serialize
                // this registration against a concurrent catalog op on the
                // same name by holding the name record lock across the whole
                // open (JE holds the NameLN lock across `getDb`/`createDb`):
                //   * a fresh CREATE (allow_create && name absent) takes WRITE;
                //   * an open of an existing name takes a blocking READ.
                // The lock is taken BLOCKING but LATCH-FREE -- we clone the
                // `LockManager` + mint the locker id under a BRIEF `env_impl`
                // lock, DROP `env_impl`, then block on the lock manager with no
                // env/structural latch held (v8 Rule 2b: never wait on the lock
                // manager while holding `env_impl`; the remover/renamer needs
                // `env_impl` in its precommit to resolve and release this
                // lock).  A concurrent uncommitted remove/rename holding the
                // name's WRITE lock makes an open block until that txn resolves
                // (then NotFound on commit, or the handle on abort) or time out
                // with `LockTimeout` -- JE's blocking `getDb` semantics,
                // resolving the former fail-fast `FID-711-OPEN-FAILFAST`
                // deviation.
                let want_write = config.allow_create && {
                    let env_impl = self.env_impl.lock();
                    !env_impl.database_exists(name)
                };
                let (lm, auto_id) = {
                    let env_impl = self.env_impl.lock();
                    (
                        env_impl.get_lock_manager().clone(),
                        env_impl.next_auto_locker_id(),
                    )
                };
                // Latch-free blocking acquire (no env_impl held here).
                let res = lm.lock(
                    noxu_util::Lsn::name_lock_id(name),
                    auto_id,
                    if want_write { LockType::Write } else { LockType::Read },
                    /*non_blocking=*/ false,
                    false,
                );
                // Arm the guard before checking `res` so a failed acquire still
                // drops the throwaway locker's label (idempotent release).
                let guard = AutoNameLockGuard {
                    env_impl: &self.env_impl,
                    name,
                    locker_id: auto_id,
                };
                if let Err(e) = res {
                    return Err(map_dbi_err(name)(noxu_dbi::DbiError::from(e)));
                }
                Some(guard)
            };

        let db_impl_arc = {
            let env_impl = self.env_impl.lock();
            if is_transactional_create {
                // SAFETY: is_transactional_create implies txn.is_some().
                let txn_id = txn
                    .expect("invariant: txn is Some when is_transactional_create")
                    .id();
                env_impl
                    .open_database_transactional(name, &dbi_config, txn_id)
                    .map(|(db, lsn)| {
                        name_ln_lsn = lsn;
                        db
                    })
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
                    // GAP-REPLAY-3 (merge-prep follow-up): a create on a
                    // replica is screened in `open_database_inner` (JE
                    // `DbTree.doCreateDb` -> `checkReplicaWrite` ->
                    // `disallowReplicaWrite`).  Surface the typed
                    // `ReplicaWrite` here rather than flattening it to a
                    // stringy `environment(...)`, matching the data path.
                    noxu_dbi::DbiError::ReplicaWrite => {
                        NoxuError::ReplicaWrite
                    }
                    _ => NoxuError::environment(e.to_string()),
                }
            })
        };
        // R-LIVEDDL-711: release the auto-commit open READ probe now that the
        // DatabaseImpl is resolved (or the open failed); its job was the
        // conflict check against an in-doubt WRITE holder.  Dropped before the
        // `?` so an open error cannot leak the throwaway locker's lock; the
        // guard's `Drop` also covers an unwind if the open above panicked
        // (R-LIVEDDL-711 S3).
        drop(open_probe_guard);
        let db_impl_arc = db_impl_arc?;

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
            // NEW-REC-1: record the provisional NameLNTxn's LSN on the inner
            // Txn so it reports has_logged_entries() == true.  Without this,
            // an empty (data-less) DB-create txn logs the NameLNTxn directly
            // through the LogManager but never advances the Txn's last_lsn, so
            // Txn::commit() skips writing the durable TxnCommit record and
            // recovery drops the database (its provisional NameLNTxn pairs with
            // no committed txn).  JE: NameLN.log() inside DbTree.createDb runs
            // through the txn, advancing Txn.lastLoggedLsn.
            if let Some(lsn) = name_ln_lsn
                && let Some(inner) = txn_ref.get_inner_txn()
            {
                inner
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .note_log_entry(lsn.as_u64());
            }
            txn_ref.register_abort_callback(move || {
                env_impl_arc.lock().abort_pending_database(&db_name_abort);
            });
            // XDB-711: publish a committed transactional CREATE to `name_map`
            // as a PRECOMMIT catalog callback -- after the durable TxnCommit
            // but BEFORE the create's locks are released (JE
            // `setDeletedDatabaseState`-side ordering, mirror of the
            // remove/rename visibility).  The old post-commit callback
            // published AFTER lock release, widening the window in which a
            // checkpoint relog / `database_names()` could observe the catalog
            // between a durable create-commit and its `name_map` publish.
            // `commit_pending_database` is a fast `pending_names`->`name_map`
            // move (no `DatabaseImpl` teardown), safe under the barrier.
            txn_ref.register_precommit_catalog_callback(move || {
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
            self.effective_no_sync(),
            self.effective_write_no_sync(),
        ))
    }

    /// Effective `no_sync` for auto-commit: honours `with_durability(
    /// COMMIT_NO_SYNC)` (the recommended API) as well as the deprecated
    /// `set_txn_no_sync(true)` boolean.
    ///
    /// Fidelity fix: previously the auto-commit `put()` path read ONLY the
    /// legacy `txn_no_sync` boolean, so `with_durability(COMMIT_NO_SYNC)` was
    /// silently ignored and every auto-commit `put()` still fdatasync'd
    /// (Noxu ~1,646 fdatasync/s where JE NO_SYNC does ~12/s). JE resolves the
    /// commit SyncPolicy from the effective `Durability` (Txn.getCommitDurability
    /// / EnvironmentConfig.getDurabilityFromSync). Now the durability's
    /// `local_sync` drives the decision, with the legacy boolean as an OR.
    fn effective_no_sync(&self) -> bool {
        self.config.txn_no_sync
            || self.config.durability.local_sync
                == crate::durability::SyncPolicy::NoSync
    }

    /// Effective `write_no_sync` for auto-commit (see [`effective_no_sync`]).
    fn effective_write_no_sync(&self) -> bool {
        self.config.txn_write_no_sync
            || self.config.durability.local_sync
                == crate::durability::SyncPolicy::WriteNoSync
    }

    /// R-LIVEDDL-711: acquire the catalog NameLN lock for `name` on behalf of a
    /// transactional DDL op, held by `txn` until it commits or aborts.
    ///
    /// This is JE's `DbTree.lockNameLN` WRITE (`searchExact(key=name, WRITE)`,
    /// `DbTree.java:952`): while `txn` holds the uncommitted remove / rename /
    /// truncate of `name`, a concurrent catalog op on the same name BLOCKS (or
    /// times out / deadlock-fails with a typed error).  The lock is taken
    /// THROUGH the owning `Txn` (`Locker::lock`), so it is tracked in the
    /// txn's lock set and released by the normal `release_all_locks` at
    /// commit/abort — no extra teardown.  WRITE, blocking.
    fn lock_name_record_under_txn(
        &self,
        txn: &Transaction,
        name: &str,
    ) -> Result<()> {
        self.lock_name_record_under_txn_typed(txn, name, LockType::Write, false)
    }

    /// R-LIVEDDL-711: as [`Self::lock_name_record_under_txn`] but with an
    /// explicit lock type and blocking mode — WRITE/blocking for
    /// remove/rename/truncate, READ/non-blocking for the handle-open path (JE
    /// `acquireHandleLock`).  A READ request from a locker that already holds
    /// the WRITE lock on the same name (e.g. a transactional truncate that
    /// then opens the db under the same txn) is satisfied by its own lock and
    /// does not conflict.
    fn lock_name_record_under_txn_typed(
        &self,
        txn: &Transaction,
        name: &str,
        lock_type: LockType,
        non_blocking: bool,
    ) -> Result<()> {
        let Some(inner) = txn.get_inner_txn() else {
            return Ok(());
        };
        let lock_id = noxu_util::Lsn::name_lock_id(name);
        let r = inner.lock().unwrap_or_else(|p| p.into_inner()).lock(
            lock_id,
            lock_type,
            non_blocking,
        );
        match r {
            Ok(_) => Ok(()),
            // A non-blocking denial surfaces as LockNotAvailable from the
            // Txn/lock-manager; present it as the catalog name-conflict error
            // JE's preempted handle open raises.
            Err(noxu_txn::TxnError::LockNotAvailable { .. }) => {
                Err(NoxuError::LockConflict(name.to_string()))
            }
            Err(e) => Err(NoxuError::from(e)),
        }
    }

    /// R-LIVEDDL-711: run `op` while holding the catalog NameLN lock for `name`
    /// under a throwaway auto-commit locker — JE's null-txn auto-commit locker
    /// taking `DbTree.lockNameLN` for the duration of one auto-commit catalog
    /// op.  `lock_type` is WRITE for remove/rename/truncate (blocking) and READ
    /// for the handle-open path (non-blocking → `LockConflict` on a conflict).
    /// The lock is released after `op` returns, win or lose.  A concurrent
    /// transactional DDL holding the name conflicts here.
    ///
    /// R-LIVEDDL-711 (S3, review finding): the release runs through an RAII
    /// [`AutoNameLockGuard`] so a PANIC in `op` cannot leak the auto-commit
    /// name lock.  Before the guard, a panic unwinding past the manual release
    /// left the NameLN lock held by a dead throwaway locker, which permanently
    /// wedged every future catalog op on that name (an un-removable database).
    /// The transactional paths get this for free via `release_all_locks`;
    /// JE's auto-commit `Locker.operationEnd` likewise releases on any exit.
    fn with_name_record_lock<T>(
        &self,
        name: &str,
        lock_type: LockType,
        non_blocking: bool,
        op: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let (locker_id, acquired) = {
            let env_impl = self.env_impl.lock();
            let locker_id = env_impl.next_auto_locker_id();
            let acquired = env_impl
                .lock_name_record(name, lock_type, locker_id, non_blocking)
                .map_err(map_dbi_err(name));
            (locker_id, acquired)
        };
        // Arm the release guard whether or not the acquire succeeded: on a
        // failed acquire there is no lock to drop but the throwaway locker's
        // diagnostic label still needs unregistering (both are handled by
        // `release_name_record`, which is idempotent).  On success the guard
        // releases the held lock when it drops — on the normal return, on an
        // early `?` return, AND on an unwind if `op` panics.
        let _guard =
            AutoNameLockGuard { env_impl: &self.env_impl, name, locker_id };
        acquired?;
        op()
    }

    /// Removes a database.
    ///
    /// When `txn` is `Some`, the removal participates in that transaction:
    /// the existence / not-in-use checks run immediately (mirroring JE's
    /// `DbTree.lockNameLN`, which locks and validates the NameLN at operation
    /// time), but the physical deletion of the database is **deferred to
    /// commit**.  Aborting the transaction therefore rolls the removal back —
    /// the database and its records survive.  This mirrors JE, where
    /// `Environment.removeDatabase` runs under a `Locker` derived from the
    /// passed `Transaction` (`Environment.java:988` -> `DbNameOperation` ->
    /// `DbTree.dbRemove`, `DbTree.java:1230`), the NameLN delete is
    /// transactional, and the physical `MapLN`/tree deletion is scheduled for
    /// commit via `Txn.markDeleteAtTxnEnd(dbImpl, /*deleteAtCommit=*/true)`
    /// (`DbTree.java:1214`, `Txn.java:1481`).
    ///
    /// When `txn` is `None`, the removal is applied immediately (auto-commit),
    /// matching JE's null-txn auto-commit locker.
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle; if supplied, the removal is
    ///   undone on abort and applied on commit.
    /// * `name` - Database name
    ///
    /// # Errors
    /// Returns an error if:
    /// - The environment is closed
    /// - The database does not exist
    /// - The database is currently open
    ///
    /// # Fidelity note
    /// The removed name remains visible to `database_names()` (and to
    /// concurrent openers) until the transaction commits, whereas JE deletes
    /// the NameLN transactionally and hides it within the txn.  See
    /// `docs/src/reference/known-limitations.md` ("Transactional DDL").
    pub fn remove_database(
        &self,
        txn: Option<&Transaction>,
        name: &str,
    ) -> Result<()> {
        self.check_writable("remove_database", name)?;

        // Validate up front (existence + not-in-use) so the caller gets a
        // synchronous error, mirroring JE's `DbTree.lockNameLN` check at
        // operation time.  The authoritative `db_id` the deferred remove binds
        // to is RE-captured under the held WRITE lock in the `Some(txn)` arm
        // (XDB-711 #2b -- the up-front value is pre-lock and may be stale);
        // the auto-commit `None` path re-validates inside `remove_database`.
        {
            let env_impl = self.env_impl.lock();
            env_impl.can_remove_database(name).map_err(map_dbi_err(name))?;
        }

        match txn {
            None => {
                // ENG-AUTODDL-2 (S2): run the auto-commit remove through JE's
                // `createAutoTxn` path — ONE internal auto-txn that logs the
                // NameLN tombstone transactionally and commits SYNC, so a log
                // error propagates (the former direct path discarded it with
                // `let _ = log_name_ln_delete(..)`). The removal itself was
                // already durable on the base (the tombstone was fsync'd), but
                // the dropped error made a failed remove report Ok. A
                // non-transactional env takes the direct path (`None`).
                self.run_auto_commit_ddl(|auto| match auto {
                    Some(t) => self.remove_database(Some(t), name),
                    None => {
                        // R-LIVEDDL-711: hold the NameLN WRITE lock for the op
                        // (JE `DbTree.doRemoveDb` -> `lockNameLN` WRITE,
                        // DbTree.java:952).
                        self.with_name_record_lock(
                            name,
                            LockType::Write,
                            /*non_blocking=*/ false,
                            || {
                                let mut databases = self.databases.lock();
                                {
                                    let env_impl = self.env_impl.lock();
                                    env_impl
                                        .remove_database(name)
                                        .map_err(map_dbi_err(name))?;
                                }
                                databases.remove(name);
                                Ok(())
                            },
                        )
                    }
                })
            }
            Some(txn) => {
                // R-LIVEDDL-711: take the NameLN WRITE lock under `txn`, held
                // until it commits/aborts (JE `DbTree.doRemoveDb` ->
                // `lockNameLN` WRITE, DbTree.java:952).  A concurrent catalog
                // op on `name` now blocks/typed-conflicts until `txn`
                // resolves, so an abort can no longer lose records a
                // concurrent remove/truncate destroyed in the race window.
                self.lock_name_record_under_txn(txn, name)?;
                // XDB-711 #2b: the up-front `can_remove_database` validated
                // `db_id` BEFORE this WRITE lock was taken -- a concurrent
                // committed rename/remove/recreate could have re-bound `name`
                // in that window, so the validated `db_id` may be stale.  A
                // stale `db_id` makes the precommit identity guard
                // (`remove_database_make_visible_if_id`) NO-OP, leaving the
                // durable committed NameLN-delete tombstone logged below
                // UNPUBLISHED in `name_map` -- then a later relog keeps logging
                // the live binding past the committed tombstone and recovery
                // resurrects the removed db (the XDB-711 #2b residual).
                // RE-VALIDATE under the HELD lock so `db_id` is lock-stable:
                // the WRITE lock we now hold forbids any concurrent rebind, so
                // the precommit guard will always match and publish.  JE
                // validates the NameLN AFTER `lockNameLN` (DbTree.java:952+).
                let db_id = {
                    let env_impl = self.env_impl.lock();
                    env_impl
                        .can_remove_database(name)
                        .map_err(map_dbi_err(name))?
                };
                // ordinary TRANSACTIONAL LN inside the user txn BEFORE commit
                // (JE DbTree.doRemoveDb deletes the NameLN through a cursor
                // under the user txn, DbTree.java:1182-1206), and record its
                // LSN on the inner Txn.  That makes has_logged_data() true, so
                // commit_with_durability writes a durable TxnCommit (SYNC =
                // flush+fsync, Txn.java:983-986) that covers the tombstone —
                // the removal survives a crash with no special per-op fsync.
                // The PHYSICAL cleanup (map removal, obsolete counting) stays
                // deferred to the commit callback (JE Txn.deletedDatabases,
                // Txn.java:1485-1489).  Abort writes TxnAbort and never runs
                // the callback, and recovery's mapping-tree undo pass restores
                // the name because the deleting txn did not commit -> the
                // database survives.
                //
                // F-DDL-1: bind to the validated `db_id`, not the name, so a
                // concurrent same-name recreate committed before us is not
                // silently destroyed (JE binds to the `DatabaseImpl`
                // identity, DbTree.java:1214 / Txn.java:1481).
                {
                    let env_impl = self.env_impl.lock();
                    // R-RELOG (S1) TOCTOU-hardening (review merge-prep): mark the
                    // pending remove FIRST, before relog-once/tombstone, so a
                    // background checkpoint's lock-free relog_live_catalog (which
                    // does NOT take env_impl.lock) can never observe the victim
                    // UNMARKED between the tombstone write and the mark and thus
                    // never re-logs a live NameLN past the tombstone.  db_id was
                    // validated up front (can_remove_database).
                    env_impl.mark_pending_ddl(name, db_id);
                    // R-RELOG (S1): re-log a known-LSN LIVE copy of the victim's
                    // committed NameLN BEFORE the tombstone, and anchor the
                    // first-active clamp there (note_log_entry, earliest-wins)
                    // so the file holding that live copy is kept while the txn
                    // is open.  On abort it is the latest live NameLN (name
                    // survives); on commit the tombstone below supersedes it.
                    if let Some(lsn) = env_impl
                        .relog_name_ln_once(name)
                        .map_err(map_dbi_err(name))?
                        && let Some(inner) = txn.get_inner_txn()
                    {
                        inner
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .note_log_entry(lsn.as_u64());
                    }
                    if let Some(lsn) = env_impl
                        .log_remove_name_ln_txn(name, txn.id())
                        .map_err(map_dbi_err(name))?
                        && let Some(inner) = txn.get_inner_txn()
                    {
                        inner
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .note_log_entry(lsn.as_u64());
                    }
                }
                let env_impl_arc = Arc::clone(&self.env_impl);
                let databases_arc = Arc::clone(&self.databases);
                // R-RELOG (S1): clear the pending-DDL marker on BOTH commit and
                // abort so relog resumes once the txn resolves.
                let env_impl_abort = Arc::clone(&self.env_impl);
                let name_abort = name.to_string();
                txn.register_abort_callback(move || {
                    env_impl_abort.lock().clear_pending_ddl(&name_abort);
                });
                // XDB-711: PRECOMMIT catalog-visibility callback -- runs after
                // the durable TxnCommit barrier but BEFORE the inner Txn
                // releases the name WRITE lock (JE `setDeletedDatabaseState`).
                // Publishes the `name_map` removal (and the noxu-db handle-map
                // removal) while the name lock is still held, so the
                // per-checkpoint relog's non-blocking READ on the name is
                // DENIED for the whole window and the F-DDL-1 identity guard
                // sees the still-current binding (no commit-window TOCTOU
                // no-op).  Fast map updates only -- no `DatabaseImpl`
                // teardown.  Order mirrors the relog read order: publish
                // `name_map` FIRST, then clear `pending_ddl_names`.
                let env_impl_vis = Arc::clone(&self.env_impl);
                let databases_vis = Arc::clone(&self.databases);
                let name_vis = name.to_string();
                let did_remove: Arc<AtomicBool> =
                    Arc::new(AtomicBool::new(false));
                let did_remove_vis = Arc::clone(&did_remove);
                txn.register_precommit_catalog_callback(move || {
                    let mut databases = databases_vis.lock();
                    let env = env_impl_vis.lock();
                    if let Ok(Some(_id)) =
                        env.remove_database_make_visible_if_id(&name_vis, db_id)
                    {
                        databases.remove(&name_vis);
                        did_remove_vis.store(true, Ordering::SeqCst);
                    }
                    env.clear_pending_ddl(&name_vis);
                });
                // Post-release: the heavy `DatabaseImpl` teardown + obsolete
                // counting, OUTSIDE the name lock (JE `cleanupDatabaseImpls`).
                txn.register_commit_callback(move || {
                    if did_remove.load(Ordering::SeqCst) {
                        env_impl_arc
                            .lock()
                            .remove_database_finish_cleanup(db_id);
                    }
                    let _ = &databases_arc;
                });
                Ok(())
            }
        }
    }

    /// Truncates a database: removes all records while keeping the database
    /// registered and any open handles valid.
    ///
    /// Returns the number of records that were in the database before
    /// truncation.
    ///
    /// When `txn` is `Some`, the truncation participates in that transaction:
    /// the existence / not-in-use checks and the pre-truncate record count run
    /// immediately (so the count is returned synchronously, as JE's
    /// `returnCount` result is), but the physical tree replacement is
    /// **deferred to commit**.  Aborting the transaction rolls the truncation
    /// back — the records survive.  Mirrors JE, where
    /// `Environment.truncateDatabase` runs under a `Locker` from the passed
    /// `Transaction` (`Environment.java:1125` -> `DbTree.truncate`,
    /// `DbTree.java:1392`) and schedules the old DB for commit-time deletion.
    ///
    /// When `txn` is `None`, the truncation is applied immediately
    /// (auto-commit).
    ///
    /// Mirrors `Environment.truncateDatabase(txn, dbName, returnCount)`.
    pub fn truncate_database(
        &self,
        txn: Option<&Transaction>,
        name: &str,
    ) -> Result<u64> {
        self.check_writable("truncate_database", name)?;

        match txn {
            None => {
                // ENG-AUTODDL-2 (S2) + durability: run the auto-commit
                // truncate through JE's `createAutoTxn` path — ONE internal
                // auto-txn that deletes every record transactionally and
                // commits SYNC, so the truncation is durable at return and a
                // log error propagates (the former direct path logged lazy
                // per-record DeleteLNs with `let _ = log_delete_ln(..)`). A
                // non-transactional env takes the direct path (`None`).
                self.run_auto_commit_ddl(|auto| match auto {
                    Some(t) => self.truncate_database(Some(t), name),
                    None => {
                        // R-LIVEDDL-711: hold the NameLN WRITE lock for the op
                        // (JE `DbTree.doTruncateDb` -> `lockNameLN` WRITE,
                        // DbTree.java:1282/952).
                        self.with_name_record_lock(
                            name,
                            LockType::Write,
                            /*non_blocking=*/ false,
                            || {
                                let env_impl = self.env_impl.lock();
                                env_impl
                                    .truncate_database(name)
                                    .map_err(map_dbi_err(name))
                            },
                        )
                    }
                })
            }
            Some(txn) => {
                // R-LIVEDDL-711: take the NameLN WRITE lock under `txn`, held
                // until it commits/aborts (JE `DbTree.doTruncateDb` ->
                // `lockNameLN` WRITE, DbTree.java:1282).  A concurrent catalog
                // op on `name` now blocks/typed-conflicts until `txn`
                // resolves.  The open-under-`txn` below shares this locker, so
                // it does not self-conflict.
                self.lock_name_record_under_txn(txn, name)?;

                // NEW-TRUNCATE-1 (data-loss fix): do the truncate EAGERLY under
                // `txn`, not via a deferred commit callback.  The old code
                // registered a commit callback that swapped in a fresh empty
                // tree AFTER `Transaction::commit` had already written the
                // txn's data-log — so any same-txn insert issued *after* the
                // truncate went into the still-live old tree and was wiped by
                // the callback on commit (silent data loss; the count was a
                // correct but stale up-front value).
                //
                // JE `DbTree.doTruncateDb` (`DbTree.java:1273`) installs a
                // fresh empty tree with a NEW `DatabaseId` at truncate time, so
                // subsequent same-txn inserts land in the post-truncate tree
                // and survive commit; abort rolls back the truncate and the
                // inserts.  Noxu keeps the `DatabaseId` stable, so the faithful
                // same-`DatabaseId` equivalent is to delete every record
                // transactionally right now: the `DeleteLN`s are logged under
                // `txn` (ordered BEFORE any later same-txn insert, which
                // recovery's in-log-order redo requires) and the txn's own undo
                // pass restores every before-image on abort.
                //
                // The up-front count (JE `returnCount`) is read first, WHILE no
                // handle is open (so the not-in-use check passes), then the DB
                // is opened under `txn`, drained, and closed.  F-DDL-1: the
                // count read validates existence; the delete then runs under
                // the same lock ecosystem so a concurrent recreate cannot slip
                // between validation and drain.
                let count = {
                    let env_impl = self.env_impl.lock();
                    let (count, _db_id) = env_impl
                        .count_for_truncate(name)
                        .map_err(map_dbi_err(name))?;
                    count
                };
                let dbcfg = DatabaseConfig::new().with_transactional(true);
                let db = self.open_database(Some(txn), name, &dbcfg)?;
                let res = db.delete_all_under_txn(txn);
                // Always drop the internal handle so the reference count
                // returns to zero regardless of the delete outcome.
                let _ = db.close();
                res?;
                Ok(count)
            }
        }
    }

    /// Renames a database.
    ///
    /// When `txn` is `Some`, the rename participates in that transaction: the
    /// existence / not-in-use / destination-free checks run immediately, but
    /// the physical rename is **deferred to commit**.  Aborting the
    /// transaction rolls the rename back — the original name survives and the
    /// new name is not created.  Mirrors JE, where `Environment.renameDatabase`
    /// runs under a `Locker` from the passed `Transaction`
    /// (`Environment.java:1047` -> `DbTree.dbRename`, `DbTree.java:1136`) and
    /// the NameLN update is transactional.
    ///
    /// When `txn` is `None`, the rename is applied immediately (auto-commit).
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle; if supplied, the rename is
    ///   undone on abort and applied on commit.
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
        txn: Option<&Transaction>,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        self.check_writable("rename_database", old_name)?;

        if old_name == new_name {
            return Ok(());
        }

        // Validate up front so the caller gets a synchronous error (JE
        // `DbTree.lockNameLN` locks/validates at operation time).  The
        // authoritative source `DatabaseId` the deferred rename binds to is
        // RE-captured under the held source WRITE lock in the `Some(txn)` arm
        // (XDB-711 #2b -- the up-front value is pre-lock and may be stale);
        // the auto-commit `None` path re-validates inside `rename_database`.
        {
            let env_impl = self.env_impl.lock();
            env_impl
                .can_rename_database(old_name, new_name)
                .map_err(map_rename_err(old_name, new_name))?;
        }

        match txn {
            None => {
                // ENG-AUTODDL-1/-2 (S1/S2): run the auto-commit rename through
                // JE's `createAutoTxn` path — ONE internal auto-txn that logs
                // both NameLN changes transactionally and commits at the env
                // default durability (COMMIT_SYNC), so the rename is atomic,
                // durable at return, and a log error propagates. The former
                // direct `env_impl.rename_database` path logged the new-name
                // NameLN lazily (flush=false/fsync=false) and discarded errors
                // (`let _ = log_name_ln(..)`), losing the whole db on a crash
                // after Ok. A non-transactional env has no auto-txn: `op` gets
                // `None` and takes the direct (durable) path below.
                self.run_auto_commit_ddl(|auto| match auto {
                    Some(t) => {
                        self.rename_database(Some(t), old_name, new_name)
                    }
                    None => {
                        // R-LIVEDDL-711: hold the NameLN WRITE lock for BOTH
                        // source and destination for the op (JE
                        // `DbTree.doRenameDb` -> `lockNameLN` WRITE on source +
                        // create WRITE on dest).
                        self.with_rename_record_locks(
                            old_name,
                            new_name,
                            || {
                                let mut databases = self.databases.lock();
                                {
                                    let env_impl = self.env_impl.lock();
                                    env_impl
                                        .rename_database(old_name, new_name)
                                        .map_err(map_rename_err(
                                            old_name, new_name,
                                        ))?;
                                }
                                if let Some(handle) = databases.remove(old_name)
                                {
                                    databases
                                        .insert(new_name.to_string(), handle);
                                }
                                Ok(())
                            },
                        )
                    }
                })
            }
            Some(txn) => {
                // R-LIVEDDL-711: take the NameLN WRITE lock under `txn` on the
                // source AND destination, held until it commits/aborts (JE
                // `DbTree.doRenameDb` -> `lockNameLN` WRITE on source, create
                // WRITE on dest).  A concurrent catalog op on either name now
                // blocks/typed-conflicts until `txn` resolves.
                self.lock_name_record_under_txn(txn, old_name)?;
                self.lock_name_record_under_txn(txn, new_name)?;
                // XDB-711 #2b: RE-VALIDATE the source + capture the
                // authoritative `db_id` UNDER the held source WRITE lock (the
                // up-front `can_rename_database` ran pre-lock and its `db_id`
                // may be stale after a concurrent committed rebind).  A stale
                // `db_id` makes the precommit `rename_database_if_id` NO-OP,
                // leaving the durable committed rename NameLNs UNPUBLISHED in
                // `name_map`, which a later relog then re-logs past the
                // committed rename and recovery resurrects the old name /
                // cross-binds (the #2b residual).  The held WRITE lock forbids
                // any concurrent rebind, so the re-captured `db_id` is
                // lock-stable and the precommit guard always matches.  Also
                // RE-VALIDATE the destination is still free under the HELD
                // new-name WRITE lock (closing the `can_rename_database`
                // dest-TOCTOU): a rename onto a name a concurrent op bound in
                // the window -> `DatabaseAlreadyExists` (JE `DbTree.doRenameDb`
                // holds the dest NameLN WRITE across the check + insert).
                let db_id = {
                    let env_impl = self.env_impl.lock();
                    if env_impl.database_exists(new_name) {
                        return Err(NoxuError::DatabaseAlreadyExists(
                            new_name.to_string(),
                        ));
                    }
                    env_impl
                        .can_rename_database(old_name, new_name)
                        .map_err(map_rename_err(old_name, new_name))?
                };
                // BUG-DDL-DURABILITY-1: log the rename's NameLN entries
                // (delete old + create new) as ordinary TRANSACTIONAL LNs
                // inside the user txn BEFORE commit (JE DbTree.dbRename re-logs
                // the NameLN under the txn), and record the LSN on the inner
                // Txn so has_logged_data() is true and the commit's SYNC makes
                // the rename durable.  Only the in-memory name-map swap stays
                // in the commit callback; abort leaves the original name in
                // place and recovery's mapping-tree undo pass restores it.
                {
                    let env_impl = self.env_impl.lock();
                    // R-RELOG (S1) TOCTOU-hardening (review merge-prep): mark the
                    // pending rename-away of the OLD name FIRST, before relog-
                    // once/tombstone, so a background lock-free relog can never
                    // observe the old name UNMARKED between tombstone and mark.
                    // (The NEW name is not yet in name_map, so relog never
                    // touches it.)  db_id was validated up front.
                    env_impl.mark_pending_ddl(old_name, db_id);
                    // R-RELOG (S1): re-log a known-LSN LIVE copy of the OLD
                    // name's committed NameLN BEFORE the rename tombstone, and
                    // anchor the first-active clamp there, so the file holding
                    // that live copy is kept while the txn is open.  On abort it
                    // is the latest live NameLN for the old name (which thus
                    // survives); on commit the delete-old tombstone below
                    // supersedes it.
                    if let Some(lsn) = env_impl
                        .relog_name_ln_once(old_name)
                        .map_err(map_rename_err(old_name, new_name))?
                        && let Some(inner) = txn.get_inner_txn()
                    {
                        inner
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .note_log_entry(lsn.as_u64());
                    }
                    if let Some(lsn) = env_impl
                        .log_rename_name_ln_txn(old_name, new_name, txn.id())
                        .map_err(map_rename_err(old_name, new_name))?
                        && let Some(inner) = txn.get_inner_txn()
                    {
                        inner
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .note_log_entry(lsn.as_u64());
                    }
                }
                let env_impl_arc = Arc::clone(&self.env_impl);
                let databases_arc = Arc::clone(&self.databases);
                let old_owned = old_name.to_string();
                let new_owned = new_name.to_string();
                // R-RELOG (S1): clear the pending-DDL marker on BOTH commit and
                // abort of the rename txn.
                let env_impl_abort = Arc::clone(&self.env_impl);
                let old_abort = old_name.to_string();
                txn.register_abort_callback(move || {
                    env_impl_abort.lock().clear_pending_ddl(&old_abort);
                });
                // XDB-711: PRECOMMIT catalog-visibility callback -- publish the
                // rename's `name_map` swap AFTER the durable TxnCommit but
                // BEFORE the name WRITE lock is released (JE
                // `setDeletedDatabaseState`), so the relog cannot observe a
                // stale old->id live after the committed rename and the
                // F-DDL-1 identity guard sees the still-current binding.  The
                // rename swap is a fast map update (no `DatabaseImpl`
                // teardown), so it is safe to run while the name lock is held.
                // Order mirrors the relog read order: publish `name_map`
                // FIRST, then clear `pending_ddl_names`.
                txn.register_precommit_catalog_callback(move || {
                    let mut databases = databases_arc.lock();
                    let env = env_impl_arc.lock();
                    // F-DDL-1: bind to the validated source identity.
                    if let Ok(true) =
                        env.rename_database_if_id(&old_owned, &new_owned, db_id)
                        && let Some(handle) = databases.remove(&old_owned)
                    {
                        databases.insert(new_owned.clone(), handle);
                    }
                    env.clear_pending_ddl(&old_owned);
                });
                Ok(())
            }
        }
    }

    /// ENG-AUTODDL (S1/S2/S3): run an auto-commit (`txn = None`) catalog DDL
    /// as JE's `DbNameOperation.run()` does — through a single internal
    /// auto-txn committed at the environment's default durability.
    ///
    /// JE `Environment.renameDatabase` / `removeDatabase` / `truncateDatabase`
    /// / `openDatabase(txn=null)` build a `DbNameOperation(txn=null)` and call
    /// `run()` → `getWritableLocker(false)`
    /// (`Environment.java:1176-1243`). For a TRANSACTIONAL environment
    /// `LockerFactory.getWritableLocker` returns `Txn.createAutoTxn(envImpl,
    /// defaultTxnConfig, ...)` (`LockerFactory.java:72-83`): ONE auto-txn that
    /// holds every NameLN / MapLN change and commits at the env default
    /// durability (`config.getDurability()`, COMMIT_SYNC by default —
    /// `Txn.java:324-356,625`). `runOnce`'s `finally
    /// locker.operationEnd(success)` commits on success and aborts on failure
    /// (`Environment.java:1225`), so the op is atomic, durable at return, and
    /// any log-write error propagates. For a NON-transactional environment
    /// `getWritableLocker` returns a `ThreadLocker` and the NameLN write is
    /// non-transactional; `op` runs directly (there is no txn to open — the
    /// direct `env_impl` path writes the NameLN with flush+fsync and
    /// propagates its error).
    ///
    /// `op` receives the auto-txn (`Some`) for a transactional env, or `None`
    /// for a non-transactional env, and must perform the DDL through the
    /// corresponding arm.
    fn run_auto_commit_ddl<T>(
        &self,
        op: impl FnOnce(Option<&Transaction>) -> Result<T>,
    ) -> Result<T> {
        if !self.config.transactional {
            // Non-transactional env: JE's ThreadLocker path. No auto-txn to
            // open; the direct `env_impl` path logs the NameLN durably and
            // surfaces its error.
            return op(None);
        }
        // Transactional env: JE `Txn.createAutoTxn` + `operationEnd(success)`.
        //
        // JE `DbNameOperation.run()` first takes `getWritableLocker(false)` =
        // `autoTxnIsReplicated=false` (Environment.java:1204), so the
        // auto-commit DDL txn is created with `ReplicationContext.NO_REPLICATE`
        // (LockerFactory.java:77-82) and does NOT drive replica acknowledgement
        // by default. Match that by forcing `ReplicaAckPolicy::None` on the
        // auto-txn's durability so the commit does NOT consult the replica-ack
        // coordinator for a DDL (DDL replay is not shipped in 7.11.x; routing a
        // DDL through the coordinator would change DPL/open replication
        // semantics -- dpl_rep_probe). The env's local sync policy is preserved
        // (COMMIT_SYNC's fsync keeps the op durable at return).
        //
        // NOT `local_write`: on a replicated env a `local_write` txn is
        // forbidden from writing a REPLICATED database, which would break a
        // DDL on a replicated db (gap_replay_3). `ReplicaAckPolicy::None` alone
        // suffices to keep the coordinator out of the commit.
        let local_dur = crate::durability::Durability::new(
            self.config.durability.local_sync,
            self.config.durability.replica_sync,
            crate::durability::ReplicaAckPolicy::None,
        );
        let auto_cfg = TransactionConfig::default().with_durability(local_dur);
        let auto = self.begin_transaction(Some(&auto_cfg))?;
        match op(Some(&auto)) {
            Ok(v) => {
                // operationEnd(true): commit at the local-only durability.
                auto.commit_with_durability(local_dur)?;
                Ok(v)
            }
            Err(e) => {
                // operationEnd(false): abort, then propagate the original
                // error (a best-effort abort failure must not mask it).
                let _ = auto.abort();
                Err(e)
            }
        }
    }

    /// R-LIVEDDL-711: run `op` while holding the catalog NameLN WRITE lock for
    /// BOTH names of an auto-commit rename (source + destination), under one
    /// throwaway auto-commit locker.  JE `DbTree.doRenameDb` WRITE-locks the
    /// source NameLN (`lockNameLN`, DbTree.java:952) and creates the
    /// destination NameLN (also WRITE).  The two ids are taken in a canonical
    /// (sorted) order so two renames racing in opposite directions cannot
    /// deadlock against each other.  Both are released after `op`.
    fn with_rename_record_locks<T>(
        &self,
        old_name: &str,
        new_name: &str,
        op: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        // Canonical acquire order (sorted) — deadlock-free across directions.
        let (first, second) = if old_name <= new_name {
            (old_name, new_name)
        } else {
            (new_name, old_name)
        };
        self.with_name_record_lock(first, LockType::Write, false, move || {
            self.with_name_record_lock(second, LockType::Write, false, op)
        })
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

        // A local-write transaction that is also read-only is a
        // contradiction (local-write only matters for writes), so reject
        // the combination up front rather than silently ignoring one flag.
        if let Some(c) = config
            && c.local_write
            && c.read_only
        {
            return Err(NoxuError::IllegalArgument(
                "begin_transaction: local_write and read_only may not both \
                 be true (TransactionConfig)"
                    .to_string(),
            ));
        }

        // JE `Environment.checkTxnConfig` (Environment.java:1501): at most one
        // of SerializableIsolation, ReadCommitted or ReadUncommitted may be
        // set on a TransactionConfig; any pair is an IllegalArgumentException.
        // (BUG-PHANTOM-1b / PhantomTest.testIllegalTransactionConfig.)
        if let Some(c) = config {
            let ser_ru = c.serializable_isolation && c.read_uncommitted;
            let ser_rc = c.serializable_isolation && c.read_committed;
            let ru_rc = c.read_uncommitted && c.read_committed;
            if ser_ru || ser_rc || ru_rc {
                return Err(NoxuError::IllegalArgument(
                    "Only one may be specified: SerializableIsolation, \
                     ReadCommitted or ReadUncommitted"
                        .to_string(),
                ));
            }
        }

        // JE uses ONE transaction-id allocator (`TxnManager`) for every
        // transaction, explicit and auto-commit alike, so a transaction's id
        // in the WAL (LN `txn_id` + `TxnCommit`) is globally unique within a
        // recovery window.  Noxu historically had a SEPARATE
        // `Environment::next_txn_id` counter for the explicit-txn outer id,
        // which was only safe while auto-commit logged its LNs in the
        // `txn_id = 0` form.  The v7.11.1 single-locker fix makes auto-commit
        // write real-id LNs + a `TxnCommit` (drawn from `TxnManager`), so the
        // explicit outer id MUST come from the same allocator or the two id
        // spaces collide and recovery mis-resolves one as the other.  The
        // outer id is derived from the inner `Txn` below (allocated by
        // `TxnManager`, which recovery seeds via `set_last_txn_id`); this
        // standalone counter is only the fallback for a WAL-less env.
        let fallback_txn_id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
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
            // JE `Environment.beginTransactionInternal` uses the env's
            // `defaultTxnConfig` verbatim when the caller passes a null
            // TransactionConfig (Environment.java:1430).  So an env opened with
            // `EnvironmentConfig.setTxnSerializableIsolation(true)` makes
            // `begin_transaction(None)` serializable.  A caller-supplied config
            // (the `Some` arm above) is used as-is and does NOT inherit the env
            // default.  (A15 / PhantomTest.testEnvironmentConfig.)
            if self.config.txn_serializable_isolation {
                txn_config = txn_config.with_serializable_isolation(true);
            }
        }

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
                // A locker's writes default to local (non-replicated)
                // unless the owning environment is itself replicated, in
                // which case an explicit transaction defaults to
                // replicating normally — the caller must opt in via
                // TransactionConfig::with_local_write(true) for the local-
                // only escape hatch. On a non-replicated environment the
                // setting has no observable effect either way, so the
                // common case (no replication at all) is unaffected.
                if env_guard.is_replicated() {
                    t.set_local_write(txn_config.local_write);
                } else if txn_config.local_write {
                    t.set_local_write(true);
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
        // Derive the outer transaction id from the inner `Txn` (TxnManager
        // allocator) so the explicit-txn and auto-commit id spaces are one
        // and the same (see the `fallback_txn_id` comment above).  A WAL-less
        // env has no inner `Txn`; use the standalone counter there.
        let txn_id = inner_txn
            .as_ref()
            .map(|it| it.lock().unwrap().id_as_locker() as u64)
            .unwrap_or(fallback_txn_id);

        let txn_state = Arc::new(TransactionState {
            id: txn_id,
            config: txn_config.clone(),
            committed: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
        });
        self.active_txns.insert(txn_id, txn_state);

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
        // Capture the general env-invalid flag while env_guard is ALREADY held
        // (no extra lock) so the txn's check_env can fail closed on any
        // invalidation source — matching EnvironmentImpl::is_valid() / JE
        // checkIfInvalid().
        let env_invalid_flag = env_guard.is_invalid_flag();
        drop(env_guard);

        let txn =
            if let Some(it) = inner_txn { txn.with_inner_txn(it) } else { txn };

        // Wire env_impl so Transaction::abort() can apply undo records.
        // Txn environment reference during construction.
        let txn = txn
            .with_env_impl(Arc::clone(&self.env_impl))
            .with_env_invalid(env_invalid_flag);

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
    pub fn database_names(&self) -> Result<Vec<String>> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        Ok(env_impl.get_database_names())
    }

    /// Recovery lazy-fetch counters from the most recent open of this
    /// environment: `(lns_redone, lns_gated)`.
    ///
    /// `lns_redone` is the number of committed / non-transactional LNs the
    /// redo phase actually replayed; `lns_gated` is the number of
    /// pre-checkpoint-start LNs the seeded-root redo gate skipped because
    /// their records were materialised by lazily fetching checkpointed BINs
    /// (`fetchTarget`-in-recovery) instead.  A large `lns_gated` relative to
    /// `lns_redone` confirms recovery used lazy fetch rather than full LN
    /// redo.  Both zero for a fresh environment or when no database was
    /// checkpoint-seeded (full-redo fallback).
    ///
    /// Primarily an observability / test hook.
    pub fn recovery_redo_counts(&self) -> (u64, u64) {
        let env_impl = self.env_impl.lock();
        (env_impl.recovery_lns_redone, env_impl.recovery_lns_gated)
    }

    /// Install a replica-ack coordinator on this environment.
    ///
    /// After this call, every transaction begun on this environment
    /// will consult the coordinator on `commit_with_durability` and
    /// block until the configured `ReplicaAckPolicy` is satisfied (or
    /// until `replica_ack_timeout` elapses, in which case
    /// `NoxuError::InsufficientAcks` is returned -- the txn IS
    /// committed+durable at the master; FID-DUR-1).
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
    /// **R-REPUSER (v7.11.x): fail-closed.** Installing a
    /// `noxu_rep::ReplicatedEnvironment` here does NOT make ordinary user
    /// writes replicate — the user write path never assigns a VLSN, so the
    /// feeder skips every user write (`BUG-REP-USER-WRITES-NOT-REPLICATED`,
    /// S1). To avoid silently misleading an application, EVERY subsequent
    /// public user write on this environment — auto-commit `put`/`delete`, an
    /// explicit transaction's `put_in`, cursor writes, non-transactional
    /// writes, and DDL (create/remove/rename/truncate) — is **refused at the
    /// operation, before anything is logged**, with
    /// [`NoxuError::ReplicationNotSupported`]. Elections, group membership,
    /// and master transfer still work on the `ReplicatedEnvironment` itself;
    /// only the shipping of user writes does not. The complete fix lands in
    /// v8.0.0. See `docs/src/operations/known-limitations.md`.
    ///
    /// Closes finding F1 of the 2026 review.
    pub fn set_replica_coordinator(
        &self,
        coord: noxu_dbi::SharedReplicaAckCoordinator,
    ) {
        // R-REPUSER (BUG-REP-USER-WRITES-NOT-REPLICATED, S1): if the installed
        // coordinator reports it CANNOT replicate ordinary user writes in this
        // release (the real `noxu_rep::ReplicatedEnvironment` does in 7.11.x),
        // arm the env-level fail-closed flag so EVERY public user-write path --
        // explicit-txn commit, auto-commit `db.put`/`delete`, cursor writes,
        // non-transactional writes, DDL, sequences, DPL, collections, XA --
        // refuses the write BEFORE any LN is logged, not just the explicit-txn
        // commit path.  A test-double / honest coordinator returns `None` and
        // nothing is armed.  See `EnvironmentImpl::refuse_user_writes_reason`.
        if let Some(reason) = coord.refuse_user_write_commit() {
            self.env_impl.lock().set_refuse_user_writes(reason);
        } else {
            self.env_impl.lock().clear_refuse_user_writes();
        }
        *self.replica_coordinator.lock() = Some(coord);
    }

    /// Clear any installed replica-ack coordinator.
    ///
    /// Subsequent `commit_with_durability` calls revert to local-only
    /// durability semantics.
    pub fn clear_replica_coordinator(&self) {
        // R-REPUSER: dropping the coordinator also disarms the fail-closed
        // user-write refusal (back to local-only durability).
        self.env_impl.lock().clear_refuse_user_writes();
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
    pub fn replica_ack_timeout(&self) -> std::time::Duration {
        *self.replica_ack_timeout.lock()
    }

    /// Returns the home directory path.
    ///
    ///
    pub fn home(&self) -> &Path {
        &self.home
    }

    /// Returns the environment configuration.
    ///
    ///
    pub fn config(&self) -> &EnvironmentConfig {
        &self.config
    }

    /// Returns the mutable subset of environment configuration.
    ///
    /// Mirrors `Environment.getMutableConfig()`.  The returned struct reflects the
    /// current runtime values; pass it (modified) to `set_mutable_config()` to
    /// apply changes without re-opening the environment.
    pub fn mutable_config(&self) -> Result<EnvironmentMutableConfig> {
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
            max_disk: Some(self.config.max_disk),
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
            // Push the new env-level txn-timeout default to the live
            // LockManager so future `begin_transaction` / auto-commit Txns
            // inherit it (JE Locker ctor: txnTimeoutMillis =
            // envImpl.getTxnTimeout(), Locker.java:136). Live txns keep their
            // original timeout (they snapshot it at `Txn::new`); mirrors the
            // lock-timeout path just above and the EnvConfigObserver pattern.
            let env_impl = self.env_impl.lock();
            env_impl.get_lock_manager().set_default_txn_timeout(ms);
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
        // Runtime MAX_DISK change (JE EnvironmentMutableConfig.setMaxDisk):
        // push it to the live DiskLimitTracker (shared with the write path,
        // cleaner and checkpointer) and re-evaluate the violation flag
        // immediately.  Passing 0 disables the max-disk cap so a write blocked
        // purely by MAX_DISK resumes -- the Noxu analogue of DiskLimitTest's
        // allowWrites() -> setMaxDisk(0).
        if let Some(bytes) = cfg.max_disk {
            self.config.max_disk = bytes;
            let env_impl = self.env_impl.lock();
            env_impl.get_disk_limit().set_max_disk(bytes);
        }
        Ok(())
    }

    /// Synchronously flushes dirty data to the log and performs a
    /// checkpoint, making all preceding writes durable.
    ///
    /// Mirrors JE `Environment.sync()` (Environment.java:1603): it runs a
    /// forced checkpoint with `minimizeRecoveryTime=true`.  This is the
    /// supported way to make a **non-transactional** write durable — a
    /// non-transactional write is logged with no commit-time fsync (JE
    /// `LN.logInternal` logs `flushRequired=false, fsyncRequired=false`), so
    /// its durability comes from a checkpoint, a later SYNC commit that
    /// drains the buffer, env close, or this call.  See FID-NONTXN-SYNC-FSYNC.
    ///
    /// # Errors
    /// Returns an error if the environment is closed, invalidated, or the
    /// checkpoint fails (e.g. a disk write error).
    pub fn sync(&self) -> Result<()> {
        // JE Environment.sync(): CheckpointConfig.setForce(true) +
        // setMinimizeRecoveryTime(true), then invokeCheckpoint(config, "sync").
        let cfg = CheckpointConfig::default()
            .with_force(true)
            .with_minimize_recovery_time(true);
        self.checkpoint(Some(&cfg))
    }

    /// Writes buffered log data to the file system and, when `fsync` is true,
    /// performs an fsync so the data reaches the physical device.
    ///
    /// Mirrors JE `Environment.flushLog(boolean fsync)` (Environment.java:1644):
    /// it makes durable all preceding non-transactional writes and any
    /// preceding `NO_SYNC` commits.  Like JE, it does **not** flush unwritten
    /// data in deferred-write databases — use [`Database::sync`] or a
    /// checkpoint for those.
    ///
    /// # Errors
    /// Returns an error if the environment is closed, invalidated, or the
    /// flush fails.
    pub fn flush_log(&self, fsync: bool) -> Result<()> {
        self.check_open()?;
        // JE EnvironmentImpl.flushLog(fsync) -> LogManager.flushNoSync() or
        // flushSync() depending on `fsync`.
        if let Some(lm) = &self.log_manager {
            if fsync {
                lm.flush_sync()
                    .map_err(|e| NoxuError::environment(e.to_string()))?;
            } else {
                lm.flush_no_sync()
                    .map_err(|e| NoxuError::environment(e.to_string()))?;
            }
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
            // D1: a cursor-detected LN fetch failure sets the shared
            // EnvironmentImpl invalid flag; surface it on the public handle.
            && self.env_impl.lock().is_valid()
            // A-prime public fail-stop: a critical WAL write/drain/rotation/
            // sync failure poisons the shared LogManager `io_invalid` flag.
            // Surface it here so `Environment::is_valid` reports fail-stop for
            // the whole environment (JE invalidates the environment, not just
            // the logger).  Handles without their own logger (e.g. read-only
            // cursors) still observe it through this shared flag.
            && !self
                .log_manager
                .as_ref()
                .is_some_and(|lm| lm.is_io_invalid())
    }

    /// Invalidates the environment in response to a fatal error.
    ///
    /// Called internally when an `EnvironmentFailure` with
    /// `reason.invalidates_environment() == true` propagates out of a
    /// background daemon.  After invalidation `is_valid()` returns `false`
    /// and all subsequent public API calls return `EnvironmentFailure`.
    ///
    /// Sets BOTH invalidity flags, which is load-bearing. `Environment` keeps
    /// its own `env_valid` bool, but every open `Database` and `Cursor` caches
    /// `EnvironmentImpl`'s `Arc<AtomicBool>` at open time so their `check_open`
    /// fast path never has to take `env_impl.lock()` (X-13). Setting only the
    /// local bool would make `is_valid()` report false while already-open
    /// `Database` handles kept serving reads and accepting writes.
    pub fn invalidate(&self) {
        self.env_valid.store(false, Ordering::Release);
        // Propagate to the shared flag the open handles actually read.
        self.env_impl.lock().invalidate(
            noxu_dbi::EnvironmentFailureReason::UnexpectedStateFatal,
        );
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
    pub fn stats(&self) -> Result<EnvironmentStats> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        Ok(build_environment_stats(
            &env_impl,
            self.log_manager.as_deref(),
            self.config.cache_size,
        ))
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
    /// R-REPUSER: `Some(reason)` when ordinary user writes (and recovered-XA
    /// commits) must be refused on this environment because the installed
    /// replica-ack coordinator cannot replicate user writes in this release;
    /// `None` otherwise.  The public view of
    /// `EnvironmentImpl::refuse_user_writes_reason`, so a higher layer (XA)
    /// can consult the SAME env-level gate BEFORE it applies recovered LNs,
    /// rather than only at the final durable `TxnCommit` write.
    ///
    /// `#[doc(hidden)]`: internal cross-crate wiring for `noxu-xa`, not part of
    /// the stable surface.
    #[doc(hidden)]
    pub fn user_writes_refused(&self) -> Option<String> {
        self.env_impl.lock().refuse_user_writes_reason().map(|r| r.to_string())
    }

    pub fn write_txn_commit_for_recovered(&self, txn_id: u64) -> Result<()> {
        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(()), // Non-transactional env (shouldn't happen).
        };
        // R-REPUSER (BUG-REP-USER-WRITES-NOT-REPLICATED, S1): committing a
        // recovered prepared (XA) branch writes a `TxnCommit` WAL frame that,
        // in a working replicated system, is itself a REPLICATED commit (JE
        // routes a recovered prepared txn through `Txn.commit` ->
        // `preLogCommitHook` -> the master ack hook, so it ships and waits for
        // acks like any other master commit; XAEnvironment.commit ->
        // Txn.commit(xid), Txn.java:730-765).  Noxu cannot ship it in 7.11.x,
        // so refuse fail-closed on a master-wired env rather than silently
        // committing a prepared branch whose resolution would never reach a
        // replica.  A standalone (non-wired) env resolves it normally.  This is
        // the same env-level predicate every other user-write path consults.
        if let Some(reason) = self.env_impl.lock().refuse_user_writes_reason() {
            return Err(NoxuError::ReplicationNotSupported {
                reason: reason.to_string(),
            });
        }
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
        // R-XADDL-711 (S2): resolve any CATALOG (DDL) action this recovered
        // prepared branch carried -- run the SAME live-DDL commit callback
        // (commit_pending_database / remove_database_if_id /
        // rename_database_if_id + clear_pending_ddl).  After the durable
        // TxnCommit above, mirroring the live callback order (physical catalog
        // cleanup runs post-commit).
        self.env_impl
            .lock()
            .resolve_recovered_prepared_ddl(txn_id, /* commit = */ true)
            .map_err(NoxuError::from)?;
        Ok(())
    }

    /// Writes a `TxnAbort` WAL frame for `txn_id`.  Used by `xa_rollback(xid)`
    /// to durably resolve a recovered prepared transaction.
    pub fn write_txn_abort_for_recovered(&self, txn_id: u64) -> Result<()> {
        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(()),
        };
        // R-XAREC F1 (S2 durability): an XA rollback must FORCE-FLUSH the
        // resolving TxnAbort, exactly as JE `Txn.abort(Xid)` = `abort(true
        // /*forceFlush*/)` (Txn.java:611-617) and `XAEnvironment.rollback`
        // (XAEnvironment.java:284-299) which force-flush every branch,
        // recovered or not.  Symmetric with `write_txn_commit_for_recovered`
        // (COMMIT_SYNC).  Without this fsync+flush a crash after a reported-
        // successful `xa_rollback` lost the TxnAbort: the next recovery
        // re-surfaced the branch in-doubt and REDID + RE-LOCKED its LNs.
        write_txn_end_for_recovered(
            lm, txn_id, false, /* is_commit */
            true,  /* fsync */
            true,  /* flush */
            0,     /* vlsn: NULL_VLSN for abort */
        )?;
        // R-XADDL-711 (S2): resolve any CATALOG (DDL) action this recovered
        // branch carried as a ROLLBACK -- the live-DDL abort callback
        // (abort_pending_database for a pending create / in-doubt rename
        // target; clear_pending_ddl for remove/rename -- old name survives).
        self.env_impl
            .lock()
            .resolve_recovered_prepared_ddl(txn_id, /* commit = */ false)
            .map_err(NoxuError::from)?;
        Ok(())
    }

    /// Resolves a recovered prepared transaction's LNs at `xa_commit` time.
    ///
    /// R-XAREC (S1): the prepared txn's LNs were already **redone into the
    /// tree** during the FIRST recovery and the recovered prepared txn holds
    /// the WRITE record locks on those slots (JE routes a `PreparedTxn`
    /// through `redoOneLN` -> `relock` -> `redo`, RecoveryManager.java:2208).
    /// So the committed values are already present and durable in a redoable
    /// log position; `xa_commit` only has to write the durable `TxnCommit`
    /// frame (the caller does that next) and release the relocks (done by
    /// `forget_recovered_prepared_txn` -> `resolve_recovered_prepared`).
    ///
    /// This method is therefore now a **no-op** over the LN list — kept for
    /// API compatibility with the XA layer (which still calls it before
    /// `write_txn_commit_for_recovered`).  Before this fix it re-applied the
    /// LNs from owned bytes at commit time, which left the slots out of every
    /// checkpointed BIN and lost them to the redo gate after a second crash.
    pub fn apply_recovered_prepared_lns(
        &self,
        _lns: &[noxu_recovery::PreparedLnReplay],
    ) -> Result<()> {
        // Slots are already live (redone during recovery); nothing to apply.
        Ok(())
    }

    /// Undoes a recovered prepared transaction's redone LNs at `xa_rollback`
    /// time, restoring each slot's before-image.
    ///
    /// R-XAREC (S1): because the prepared txn's LNs are now redone into the
    /// tree during recovery (so they survive checkpoints and a second crash),
    /// `xa_rollback` must UNDO them — exactly as JE aborts the resurrected
    /// `PreparedTxn`, whose write-lock set carries the before-image captured
    /// by `relock` -> `setAbortInfo` (RecoveryManager.java:2354).  For each LN,
    /// in reverse log order:
    ///   * `abort_known_deleted` or no before-image => the slot was a first
    ///     write / insert over an absent key: DELETE the slot.
    ///   * otherwise => restore the embedded before-image value at
    ///     `abort_lsn` (an update's or delete's prior committed value).
    ///
    /// The caller writes the durable `TxnAbort` frame next and releases the
    /// relocks via `forget_recovered_prepared_txn`.
    pub fn undo_recovered_prepared_lns(
        &self,
        lns: &[noxu_recovery::PreparedLnReplay],
    ) -> Result<()> {
        let env_impl = self.env_impl.lock();
        // Reverse log order so a key written more than once by the prepared
        // txn is reverted to its earliest before-image (JE undoes newest
        // first; the oldest before-image is the pre-txn committed value).
        for ln in lns.iter().rev() {
            let db_id = noxu_dbi::DatabaseId::new(ln.db_id as i64);
            let Some(db_arc) = env_impl.get_database_by_id(db_id) else {
                continue;
            };
            let db_guard = db_arc.read();
            let Some(tree) = db_guard.get_real_tree() else {
                continue;
            };
            let had_slot = tree.slot_lsn_any(&ln.key).is_some();
            if ln.abort_known_deleted
                || (ln.abort_lsn == noxu_util::NULL_LSN
                    && ln.abort_data.is_none())
            {
                // First write / insert over an absent key: delete the slot.
                if tree.delete(&ln.key) && had_slot {
                    db_guard.decrement_entry_count();
                }
            } else if let Some(before) = &ln.abort_data {
                // Restore the embedded before-image value at abort_lsn.
                let key =
                    ln.abort_key.clone().unwrap_or_else(|| ln.key.clone());
                let was_present = had_slot;
                if let Err(e) = tree.insert(key, before.clone(), ln.abort_lsn) {
                    log::error!(
                        "R-XAREC: xa_rollback undo (before-image) failed for \
                         db={} abort_lsn={:?}: {e:?}",
                        ln.db_id,
                        ln.abort_lsn,
                    );
                } else if !was_present {
                    // Re-inserting a before-image for a prepared DELETE of an
                    // existing key brings the record back.
                    db_guard.increment_entry_count();
                }
            } else {
                // Non-embedded before-image (rare in modern logs, which embed
                // the abort value).  Delete the in-memory slot now; the
                // durable `TxnAbort` the caller writes next means the NEXT
                // recovery's normal undo pass restores the exact before-image
                // from `abort_lsn` via the log scanner's `read_at_lsn`.
                log::warn!(
                    "R-XAREC: xa_rollback of a non-embedded before-image \
                     (db={} abort_lsn={:?}); the in-memory slot is dropped and \
                     restored exactly on the next recovery via the undo path",
                    ln.db_id,
                    ln.abort_lsn,
                );
                if tree.delete(&ln.key) && had_slot {
                    db_guard.decrement_entry_count();
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
        self.verify_inner(config, None)
    }

    /// Test seam for **BUG-VERIFY-TRACKER-LATCH-ABBA**: run [`verify`] but
    /// invoke `prewalk_hook` once at the per-DB race window — the point where
    /// pre-fix code held the `UtilizationTracker` leaf lock while it was about
    /// to latch tree nodes.  The witness installs a hook here that blocks
    /// until a concurrent thread (holding a BIN latch, then taking the tracker,
    /// the checkpointer's legal order) makes progress.  With the ABBA bug the
    /// tracker is held across this point, so that thread cannot acquire it and
    /// both wedge; with the fix the tracker is not held here, so the thread
    /// completes and verify proceeds.
    ///
    /// [`verify`]: Self::verify
    #[doc(hidden)]
    pub fn verify_with_prewalk_hook_for_test(
        &self,
        config: &noxu_engine::VerifyConfig,
        prewalk_hook: &dyn Fn(),
    ) -> Result<noxu_engine::VerifyResult> {
        self.verify_inner(config, Some(prewalk_hook))
    }

    /// Test seam for **BUG-VERIFY-TRACKER-LATCH-ABBA**: clone the live
    /// `UtilizationTracker` Arc so the witness thread can reproduce the
    /// checkpointer's legal lock order (BIN write latch -> tracker lock).
    /// `None` on a read-only / memory-only env (no tracker).
    #[doc(hidden)]
    pub fn utilization_tracker_for_test(
        &self,
    ) -> Option<
        std::sync::Arc<noxu_sync::Mutex<noxu_cleaner::UtilizationTracker>>,
    > {
        self.env_impl.lock().get_utilization_tracker().cloned()
    }

    /// Test seam for **BUG-VERIFY-TRACKER-LATCH-ABBA**: return the root tree
    /// node of a named database so the witness thread can take a real tree
    /// write latch (the "BIN latch" leg of the ABBA cycle).  `None` if the DB
    /// is absent or has no resident root.
    #[doc(hidden)]
    pub fn root_tree_node_for_test(
        &self,
        db_name: &str,
    ) -> Option<std::sync::Arc<noxu_dbi::NodeRwLock<noxu_dbi::TreeNode>>> {
        let env_impl = self.env_impl.lock();
        // v7.11.1 has no by-name DatabaseImpl lookup; scan the open set.
        for db_arc in env_impl.get_all_database_impls() {
            let guard = db_arc.read();
            if guard.get_name() == db_name {
                return guard.get_real_tree()?.get_root();
            }
        }
        None
    }

    /// Test seam (BUG-VERIFY-TRACKER-LATCH-ABBA remedy teeth): the live LSN
    /// set of a named database's tree, so an inverse-teeth test can plant one
    /// into the tracker's obsolete set and confirm `verify` still reports it.
    #[doc(hidden)]
    pub fn live_lsns_for_test(&self, db_name: &str) -> Vec<noxu_util::Lsn> {
        let env_impl = self.env_impl.lock();
        for db_arc in env_impl.get_all_database_impls() {
            let guard = db_arc.read();
            if guard.get_name() == db_name {
                return noxu_engine::gather_db_live_lsns(&guard)
                    .into_iter()
                    .collect();
            }
        }
        Vec::new()
    }

    /// Test seam for **BUG-VERIFY-TRACKER-LATCH-ABBA** (daemon path): clone
    /// the shared `EnvironmentImpl` Arc so a witness can drive
    /// `verify_daemon::run_once_with_prewalk_hook_for_test` against the same
    /// environment the real daemon walks.
    #[doc(hidden)]
    pub fn env_impl_arc_for_test(
        &self,
    ) -> Arc<Mutex<noxu_dbi::EnvironmentImpl>> {
        Arc::clone(&self.env_impl)
    }

    /// XDB-711 deterministic-witness seam (`#[doc(hidden)]`, test-only): drive
    /// one catalog relog, invoking `after_snapshot` in the window between the
    /// relog's `name_map` snapshot and its first per-name NameLN append.
    ///
    /// Obtains a self-contained relog closure (cloned `Arc`s) while briefly
    /// holding `env_impl.lock()`, then DROPS the guard before running the
    /// relog, so `after_snapshot` may itself perform a DDL (which re-enters
    /// `env_impl.lock()`) without deadlocking.  This forces the exact XDB-711
    /// hazard interleaving: relog snapshots `(name, old_id)` -> `after_snapshot`
    /// COMMITS a rename/remove on `name` -> relog appends.  With the fix the
    /// relog refuses to re-log the now-stale binding.
    #[doc(hidden)]
    pub fn relog_live_catalog_with_snapshot_hook_for_test(
        &self,
        after_snapshot: &dyn Fn(),
    ) {
        let runner = {
            let g = self.env_impl.lock();
            g.relog_snapshot_hook_runner()
        };
        if let Some(runner) = runner {
            runner(after_snapshot);
        }
    }

    /// Test seam for the JE cleaner `FileSelectionTest` port: force-clean
    /// EXACTLY ONE file and ensure that file is physically deleted, returning
    /// the number of files cleaned (0 or 1).
    ///
    /// This is the Noxu analogue of JE's `FileSelectionTest.forceCleanOne()`
    /// (`cleaner.doClean(false /*cleanMultipleFiles*/, true /*forceCleaning*/)`
    /// followed by a forced checkpoint to delete the cleaned file).  The
    /// public [`Self::clean_log`] / [`Self::clean_log_forced`] both clean ALL
    /// eligible files (`n_files = u32::MAX`); this seam caps the pass at one
    /// file via `run_cleaner(1, true)`.
    ///
    /// JE deletes a cleaned file during the single checkpoint that follows
    /// cleaning.  Noxu uses a longer, two-checkpoint `FileSelector`
    /// safe-to-delete barrier (X-5): a cleaned file goes `cleaned_files` ->
    /// (checkpoint) `checkpointed` -> (checkpoint) `safe_to_delete`, and is
    /// physically removed by the next cleaner pass's `delete_safe_files()`.
    /// To reproduce JE `forceCleanOne`'s *observable* (one file cleaned AND
    /// deleted) this seam crosses that barrier: clean one file, two forced
    /// checkpoints, then a no-clean deletion pass (`run_cleaner(0, false)`,
    /// which cleans nothing but runs `delete_safe_files`).  Only the single
    /// forced file is cleaned; the deletion pass selects no new file.
    /// `#[doc(hidden)]` and `*_for_test`: additive, not part of the stable
    /// surface (patch-safe).
    #[doc(hidden)]
    pub fn force_clean_one_for_test(&self) -> Result<u32> {
        self.check_open()?;
        let files_cleaned = {
            let env_impl = self.env_impl.lock();
            env_impl
                .run_cleaner(1, true)
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
                .files_cleaned
        };
        // Cross Noxu's two-checkpoint safe-to-delete barrier so the
        // just-cleaned file is actually removed from disk (JE forceCleanOne
        // deletes during its single post-clean checkpoint).
        let force = CheckpointConfig::new().with_force(true);
        self.checkpoint(Some(&force))?;
        self.checkpoint(Some(&force))?;
        {
            let env_impl = self.env_impl.lock();
            env_impl
                .run_cleaner(0, false)
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        }
        Ok(files_cleaned)
    }

    /// Test seam (R-XARECOVERED): the cleaner's first-active-txn-file clamp
    /// value, so a witness can assert the clamp HOLDS the unresolved
    /// recovered-prepared window across several checkpoints/cleaner passes and
    /// RELEASES (returns `None`) once the branch is resolved — no stale pin.
    /// `None` == `NULL_LSN` (no active/in-doubt txn clamping any file).
    #[doc(hidden)]
    pub fn first_active_lsn_for_test(&self) -> Option<u64> {
        let v = self.env_impl.lock().get_txn_manager().get_first_active_lsn();
        if v == noxu_util::NULL_LSN.as_u64() { None } else { Some(v) }
    }

    fn verify_inner(
        &self,
        config: &noxu_engine::VerifyConfig,
        prewalk_hook: Option<&dyn Fn()>,
    ) -> Result<noxu_engine::VerifyResult> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let all_dbs = env_impl.get_all_database_impls();
        // The live utilization tracker (used for the checkLsns half, below).
        // It is a LEAF lock: the log-write observer takes it while holding a
        // BIN latch (LogManager::notify_observer ->
        // UtilizationTrackerObserver::count_new_entry), so the legal direction
        // is tree-latch -> tracker.  We must therefore NEVER hold the tracker
        // across the latching tree walk; doing so is the ABBA inversion
        // BUG-VERIFY-TRACKER-LATCH-ABBA (a checkpoint flushing BINs would block
        // on the tracker while verify blocks on the BIN latch).  Faithful to
        // JE VerifyUtils.checkLsns (VerifyUtils.java:70-160): gather the live
        // tree LSNs FIRST with no utilization state held, then take the
        // tracker briefly to read the obsolete offsets.  read-only envs have
        // no tracker.
        let tracker = env_impl.get_utilization_tracker().cloned();

        let mut merged = noxu_engine::VerifyResult::new();
        for db_arc in &all_dbs {
            // Race window for the ABBA witness: pre-fix the tracker leaf lock
            // was already held here (acquired before the loop) while we are
            // about to latch tree nodes; post-fix only an Arc clone is held.
            if let Some(hook) = prewalk_hook {
                hook();
            }
            let guard = db_arc.read();
            // Phase 1 (NO tracker lock held): structural verify + gather the
            // live tree LSNs.  Both read-latch tree nodes.
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
            // disjoint from the obsolete set.  Gather the live LSNs with NO
            // tracker lock held (the latching walk), THEN lock the tracker for
            // the brief disjointness comparison only.
            if let Some(ref t) = tracker {
                // BUG-VERIFY-TRACKER-LATCH-ABBA remedy (JE-faithful): take the
                // tracker leaf lock ONLY to snapshot the obsolete LSN set
                // (JE ObsoleteOffsetsCache), then DROP it before the latching
                // walk.  The walk that gathers the live LSNs holds NO leaf
                // lock (deadlock fix) and checks each CURRENT slot LSN against
                // the earlier snapshot: an LSN that goes obsolete after the
                // snapshot is absent from it (no false positive), an obsolete
                // LSN never becomes live again (a hit is real corruption).
                let obsolete_snapshot = {
                    let tg = t.lock();
                    noxu_cleaner::obsolete_lsn_set(&tg)
                };
                let live = noxu_engine::gather_db_live_lsns(&guard);
                let db_name = guard.get_name().to_string();
                drop(guard);
                noxu_engine::check_lsns_against_tracker(
                    &db_name,
                    &live,
                    &obsolete_snapshot,
                    &mut merged,
                );
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
    /// This runs the cleaner with `forceCleaning = false`, matching JE
    /// `Environment.cleanLog()` -> `invokeCleaner(false)` -> `doClean(...,
    /// false /*forceCleaning*/)` (EnvironmentImpl.java:2299-2300): only files
    /// whose utilization is below the configured threshold are selected, so a
    /// manual clean does NOT re-migrate 100%-live files on every pass (which
    /// would be an unbounded migration treadmill — REG-CLEANER-DISKLIMIT).
    /// For the JE test-internal forced pass (`doClean(..., true)`), which
    /// cleans regardless of utilization, use [`Self::clean_log_forced`].
    ///
    /// Returns `Ok(files_cleaned)`. Returns `Err` if the environment is closed
    /// or read-only (no cleaner is available).
    pub fn clean_log(&self) -> Result<u32> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let result = env_impl
            .run_cleaner(u32::MAX, false)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        Ok(result.files_cleaned)
    }

    /// Forced log-cleaning pass: clean every age-eligible file regardless of
    /// its utilization, returning the number of files cleaned.
    ///
    /// This is the Noxu equivalent of JE's test/maintenance forced clean —
    /// `envImpl.getCleaner().doClean(cleanMultipleFiles, true /*forceCleaning*/)`
    /// (the `forceCleaning` branch of `UtilizationCalculator.getBestFile`,
    /// labelled "forced for testing" in JE).  JE's cleaner tests
    /// (`CleanerTest`, `FileSelectionTest`, `TruncateAndRemoveTest`, ...) drive
    /// exactly this path to deterministically reclaim files that hold obsolete
    /// space but still sit at or above the utilization threshold (e.g. a
    /// single-generation overwrite churn, or a truncated/removed database
    /// whose per-file obsolete accounting has not yet dropped below the cap).
    ///
    /// Prefer [`Self::clean_log`] (`forceCleaning = false`) for normal manual
    /// reclamation: forcing a clean of 100%-live files re-migrates them every
    /// pass (the migration treadmill REG-CLEANER-DISKLIMIT fixed), so this
    /// forced variant is intended for tests and one-off batch maintenance, not
    /// steady-state use.
    ///
    /// Returns `Ok(files_cleaned)`. Returns `Err` if the environment is closed
    /// or read-only (no cleaner is available).
    pub fn clean_log_forced(&self) -> Result<u32> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let result = env_impl
            .run_cleaner(u32::MAX, true)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        Ok(result.files_cleaned)
    }

    /// Starts a hot backup and returns a [`Backup`](crate::Backup) handle.
    ///
    /// This is the JE `DbBackup` contract (`util/DbBackup.java`): it does
    /// **not** copy any files.  It pins the current log-file set so the cleaner
    /// cannot delete it, and returns the exact list of `.ndb` files you must
    /// copy.  You copy them with any tool (`std::fs::copy`, `rsync`, an
    /// object-store client, …), then call
    /// [`Backup::end_backup`](crate::Backup::end_backup) — or drop the handle —
    /// to re-enable cleaning.
    ///
    /// For a fast restore, force a checkpoint immediately before calling this
    /// (JE recommends the same, `DbBackup.java:108`).
    ///
    /// ```no_run
    /// use noxu_db::{CheckpointConfig, Environment, EnvironmentConfig};
    /// # fn run(env: &Environment) -> noxu_db::error::Result<()> {
    /// // Reduce recovery time after a restore.
    /// env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))?;
    ///
    /// let backup = env.start_backup()?;
    /// for src in backup.log_files_in_backup_set() {
    ///     let name = src.file_name().expect("log file name");
    ///     std::fs::copy(&src, std::path::Path::new("/backup/noxu").join(name))?;
    /// }
    /// backup.end_backup()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns an error if the environment is closed, or if it was opened
    /// read-only / without a cleaner (a hot backup needs the running cleaner's
    /// file protector to pin the set).
    ///
    /// JE: `new DbBackup(env).startBackup()` (`DbBackup.java:480`).
    pub fn start_backup(&self) -> Result<crate::Backup> {
        self.check_open()?;
        let env_impl = self.env_impl.lock();
        let inner = env_impl
            .start_backup()
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        Ok(crate::Backup::new(inner))
    }

    /// Recomputes the cached disk-limit violation state immediately (JE:
    /// `Cleaner.freshenLogSizeStats`).
    ///
    /// The background checkpointer daemon refreshes this on its interval and
    /// the cleaner refreshes after each pass, so most callers never need this.
    /// It is useful in tests and for tools that want the disk-limit decision
    /// to reflect the current log size / free space right now rather than at
    /// the next daemon wakeup.  Cheap no-op when `MAX_DISK`/`FREE_DISK` are 0.
    pub fn refresh_disk_limit(&self) -> Result<()> {
        self.check_open()?;
        self.env_impl.lock().refresh_disk_limit();
        Ok(())
    }

    /// Diagnostic snapshot of the cleaner's file-selector pipeline state
    /// (`to_be_cleaned`, `being_cleaned`, `cleaned`, `checkpointed`,
    /// `safe_to_delete` file counts) plus the merged per-file utilization
    /// summary map used for selection.  `None` if this environment has no
    /// cleaner (read-only).
    ///
    /// Used by the space-amplification probe (Phase 2,
    /// docs/src/internal/space-amplification-2026-09.md) to decompose an
    /// observed on-disk-bytes gap into: genuinely-below-the-floor garbage
    /// (derivable from the summary map + `min_utilization`), a real cleaner
    /// backlog (`to_be_cleaned` non-zero), and files cleaned but not yet
    /// past the two-checkpoint deletion barrier (`cleaned`/`checkpointed`
    /// non-zero). Not part of the stable public API surface (diagnostic
    /// only); may change shape without a major version bump.
    pub fn cleaner_diagnostics(&self) -> Option<CleanerDiagnostics> {
        let env_impl = self.env_impl.lock();
        let cleaner = env_impl.get_cleaner()?;
        Some(CleanerDiagnostics {
            file_selector: cleaner.get_file_selector_stats(),
            file_summaries: cleaner.get_merged_file_summary_map(),
            min_utilization: cleaner.get_min_utilization(),
        })
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

    /// The cache eviction algorithm in effect ("LRU"|"Clock"|"ARC"|"CAR"|"LIRS").
    /// Reflects the runtime-selected policy, used to verify `EVICTOR_ALGORITHM`
    /// wiring took effect.
    pub fn evictor_algorithm_name(&self) -> Result<&'static str> {
        self.check_open()?;
        Ok(self.env_impl.lock().evictor_algorithm_name())
    }

    /// Current cache memory in use (bytes), as tracked by the evictor arbiter.
    /// Unlike [`stats`](Self::stats)`.cache_usage` (a placeholder), this
    /// reflects the live tree-memory budget the evictor drives down.
    pub fn cache_usage_bytes(&self) -> Result<i64> {
        self.check_open()?;
        Ok(self.env_impl.lock().get_cache_usage())
    }
    ///
    /// Called by Database::close().
    #[allow(dead_code)] // exercised by in-crate tests; close() path lives in DBI
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
    #[allow(dead_code)] // exercised by in-crate tests
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
        // A-prime public fail-stop: a critical WAL failure maps to a typed
        // fatal `EnvironmentFailure(LogWrite)` for user operations funnelled
        // through the environment (open_database / begin transaction / etc.),
        // including handles that have no logger of their own.
        if self.log_manager.as_ref().is_some_and(|lm| lm.is_io_invalid()) {
            return Err(NoxuError::environment_with_reason(
                crate::error::EnvironmentFailureReason::LogWrite,
                "environment invalidated by a fatal log write failure"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Every mutating env-layer
    /// operation funnels through this helper so a `read_only=true`
    /// environment cannot create / remove / rename / truncate databases
    /// nor begin a (writable) transaction.
    fn check_writable(&self, what: &str, db_name: &str) -> Result<()> {
        self.check_open()?;
        if self.config.read_only {
            return Err(NoxuError::OperationNotAllowed(format!(
                "{what}: environment is read-only",
            )));
        }
        // R-REPUSER (BUG-REP-USER-WRITES-NOT-REPLICATED, S1): on a MASTER-wired
        // env, DDL (remove / truncate / rename) logs a NameLN / MapLN that is
        // NOT shipped to replicas in 7.11.x (DDL replay is missing), so refuse
        // it fail-closed FIRST, same as data writes.
        if let Some(reason) = self.env_impl.lock().refuse_user_writes_reason() {
            return Err(NoxuError::ReplicationNotSupported {
                reason: reason.to_string(),
            });
        }
        // GAP-REPLAY-3: a DDL write is a NameLN write.  JE screens it on a
        // replica via `DbTree.lockNameLN` -> `checkReplicaWrite`, gated on the
        // TARGET DB's `allow_replica_write` (`isReplicated() && dbType != NAME`),
        // so a DDL op on a NON-replicated (local) DB is permitted on a replica.
        // One guard, both reads (`env_impl` is a non-reentrant Mutex).
        {
            let ei = self.env_impl.lock();
            if ei.is_replica_write_screened()
                && !ei.allow_replica_write_for_name(db_name)
            {
                return Err(NoxuError::ReplicaWrite);
            }
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
        // Best effort close on drop.
        let _ = self.close();
        // `close()` returns early (without stopping the background daemons) if
        // the environment still has open databases or active transactions.
        // The verifier / stats daemons each hold an `Arc<EnvironmentImpl>`
        // clone, so a daemon left running would keep the environment — and its
        // FileManager file lock — alive past this `Drop`, blocking a subsequent
        // reopen of the same directory. Both daemons default ON (JE parity), so
        // we must join them unconditionally here. `stop()` joins the thread and
        // drops the daemon's `Arc`; taking from the `Mutex<Option<_>>` makes it
        // idempotent with the `close()` path above.
        if let Some(dumper) = self.stats_dumper.lock().take() {
            dumper.stop();
        }
        if let Some(verifier) = self.verify_daemon.lock().take() {
            verifier.stop();
        }
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
        assert_eq!(env.home(), temp_dir.path());
        env.close().unwrap();
    }

    #[test]
    fn test_env_check_leaks_reports_leaked_lock() {
        use noxu_txn::LockType;
        let (_temp_dir, config) = temp_env_config();
        // env_check_leaks defaults to true.
        assert!(config.env_check_leaks);
        let env = Environment::open(config).unwrap();

        // Deliberately leak a lock directly on the shared lock manager: a
        // locker (id 999) acquires a write lock and never releases it. This is
        // exactly the leak env_check_leaks is meant to surface (an application
        // that dropped a transaction/cursor without releasing its locks).
        {
            let env_impl = env.env_impl.lock();
            let lm = env_impl.get_lock_manager();
            lm.lock(0xDEAD_BEEF, 999, LockType::Write, false, false).unwrap();
            // The leak is observable before close.
            let leaks = lm.report_leaked_locks();
            assert!(
                leaks.iter().any(|(lsn, owners)| *lsn == 0xDEAD_BEEF
                    && owners.contains(&999)),
                "the deliberately-leaked lock must be reported"
            );
        }

        // close() must still succeed; env_check_leaks only warns (side effect).
        // There are no *tracked* active transactions, so the active-txn guard
        // does not block the close.
        env.close().unwrap();
    }

    // R-LIVEDDL-711 (S3) WITNESS: a panic inside the inner op of an
    // auto-commit catalog operation must NOT leak the NameLN lock.
    //
    // RED (pre-RAII, `let result = op(); release(); result;`): the panic
    // unwinds past the manual release, so the throwaway auto-commit locker
    // keeps the WRITE lock on `name_lock_id("x")` forever.  A subsequent
    // auto-commit `remove_database(None, "x")` then takes the same WRITE lock
    // blocking, finds it held by the dead locker, and BLOCKS to the lock
    // timeout (`LockTimeout`) -- the database is permanently un-removable.
    //
    // GREEN (with `AutoNameLockGuard`): the guard releases on unwind, the lock
    // is not leaked, and the later remove succeeds promptly.
    #[test]
    fn test_name_lock_released_on_panic_in_auto_commit_op() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        let db_cfg = DatabaseConfig::new().with_allow_create(true);
        // Create "x" so the later remove targets a real database.
        {
            let db = env.open_database(None, "x", &db_cfg).unwrap();
            drop(db);
        }

        let name_lock = noxu_util::Lsn::name_lock_id("x");

        // Drive a panic through the exact auto-commit lock path (private
        // method, reachable from this in-module test).  The op panics while
        // the throwaway locker holds the WRITE lock on name_lock_id("x").
        let caught = catch_unwind(AssertUnwindSafe(|| {
            let _ = env.with_name_record_lock::<()>(
                "x",
                LockType::Write,
                /*non_blocking=*/ false,
                || {
                    panic!(
                        "injected panic while holding the auto-commit name lock"
                    )
                },
            );
        }));
        assert!(caught.is_err(), "the injected op must have panicked");

        // The name lock must NOT be leaked: no owner holds name_lock_id("x").
        {
            let env_impl = env.env_impl.lock();
            let leaks = env_impl.get_lock_manager().report_leaked_locks();
            assert!(
                !leaks.iter().any(|(lsn, _owners)| *lsn == name_lock),
                "the auto-commit name lock leaked after a panic in op(): {leaks:?}"
            );
        }

        // And the database must still be removable -- promptly, not after a
        // lock-timeout block.  (On the buggy build this call blocks on the
        // stranded WRITE lock and fails with LockTimeout / hangs.)
        let start = std::time::Instant::now();
        let rc = env.remove_database(None, "x");
        let elapsed = start.elapsed();
        assert!(
            rc.is_ok(),
            "remove_database(None, x) after a panicked op must succeed, \
             got {rc:?} (the name lock leaked)"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "remove_database took {elapsed:?} -- it blocked on a leaked name lock"
        );

        env.close().unwrap();
    }

    #[test]
    fn test_env_check_leaks_clean_close_no_leak() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        // A clean env with no outstanding locks reports zero leaks.
        {
            let env_impl = env.env_impl.lock();
            assert!(
                env_impl.get_lock_manager().report_leaked_locks().is_empty(),
                "a freshly opened env must have no leaked locks"
            );
        }
        env.close().unwrap();
    }

    // exception_listener (JE ExceptionListener)
    // JE: ExceptionListenerTest.testExceptionListener -- a listener registered
    // on the env config is invoked (with the failing daemon thread name) when
    // a daemon thread throws; this drives the same dispatch path.  The
    // "open without a listener -> null/uninstalled" half of the JE test is
    // covered by test_no_exception_listener_leaves_sink_uninstalled below.
    // (Noxu dispatches the error synchronously rather than spinning a real
    // daemon that yields until shutdown -- language/API deviation, same intent.)
    #[test]
    fn test_exception_listener_fires_on_daemon_error() {
        use crate::error::{
            ExceptionEvent, ExceptionListener, ExceptionSource,
        };
        use std::sync::Arc as StdArc;
        use std::sync::Mutex as StdMutex;

        // A listener that records every event it receives.
        #[derive(Default)]
        struct Recorder {
            events: StdMutex<Vec<ExceptionEvent>>,
        }
        impl ExceptionListener for Recorder {
            fn exception_event(&self, event: &ExceptionEvent) {
                self.events.lock().unwrap().push(event.clone());
            }
        }

        let recorder = StdArc::new(Recorder::default());
        let (_temp_dir, config) = temp_env_config();
        let config = config.with_exception_listener(recorder.clone());
        let env = Environment::open(config).unwrap();

        // Drive the SAME dispatch path a background daemon uses when its
        // do_checkpoint()/do_clean() returns an error.  This exercises the
        // full production wiring: config listener -> set_exception_sink ->
        // ExceptionDispatcher::dispatch -> adapter -> ExceptionListener.
        {
            let env_impl = env.env_impl.lock();
            let d = env_impl.exception_dispatcher();
            assert!(
                d.is_installed(),
                "the registered listener must install a daemon sink"
            );
            d.dispatch("Cleaner", "simulated background cleaner failure");
            d.dispatch("Checkpointer", "simulated checkpoint failure");
        }

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 2, "listener must receive both daemon errors");
        assert_eq!(events[0].source, ExceptionSource::Cleaner);
        assert!(events[0].message.contains("cleaner failure"));
        assert_eq!(events[1].source, ExceptionSource::Checkpointer);
        assert!(events[1].message.contains("checkpoint failure"));
        drop(events);

        env.close().unwrap();
    }

    // JE: ExceptionListenerTest.testExceptionListener (no-listener half:
    // envConfig.setExceptionListener(null) -> assertNull getExceptionListener).
    #[test]
    fn test_no_exception_listener_leaves_sink_uninstalled() {
        let (_temp_dir, config) = temp_env_config();
        // No listener registered (the default).
        let env = Environment::open(config).unwrap();
        {
            let env_impl = env.env_impl.lock();
            assert!(
                !env_impl.exception_dispatcher().is_installed(),
                "no listener means no sink installed; dispatch is a no-op"
            );
            // dispatch must be a safe no-op with no sink.
            env_impl.exception_dispatcher().dispatch("Cleaner", "ignored");
        }
        env.close().unwrap();
    }

    // STARTUP_DUMP_THRESHOLD (startup_dump_threshold_ms)
    #[test]
    fn test_startup_dump_triggered_predicate() {
        // Disabled: threshold 0 never triggers regardless of elapsed.
        assert!(!Environment::startup_dump_triggered(1_000_000, 0));
        // Below threshold: no dump.
        assert!(!Environment::startup_dump_triggered(9, 10));
        // At threshold: dump (>=).
        assert!(Environment::startup_dump_triggered(10, 10));
        // Above threshold: dump.
        assert!(Environment::startup_dump_triggered(50, 10));
    }

    #[test]
    fn test_open_with_startup_dump_threshold_smoke() {
        // A 1 ms threshold makes any real open exceed it, exercising the
        // dump path (stats snapshot + warn!).  We only assert open still
        // succeeds and the env is valid; the log line is a side effect.
        let temp_dir = TempDir::new().unwrap();
        let config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_startup_dump_threshold_ms(1);
        let env = Environment::open(config).unwrap();
        assert!(env.is_valid());
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
        assert_eq!(db.name(), "testdb");
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
        let names = env.database_names().unwrap();
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

        let names = env.database_names().unwrap();
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
    fn test_database_names() {
        let (_temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let _db1 = env.open_database(None, "db1", &db_config).unwrap();
        let _db2 = env.open_database(None, "db2", &db_config).unwrap();

        let names = env.database_names().unwrap();
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
            let names = env.database_names().unwrap();
            assert!(
                !names.contains(&"aborted_db".to_string()),
                "aborted database must not appear in database_names() \
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
        let names2 = env2.database_names().unwrap();
        assert!(
            !names2.contains(&"aborted_db".to_string()),
            "after env reopen, aborted database must not appear: {:?}",
            names2
        );
    }

    /// C-4 / JE 1-I: `database_names()` must NOT return a database that
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
        let names = env.database_names().unwrap();
        assert!(
            !names.contains(&"pending_db".to_string()),
            "uncommitted database must be invisible to database_names() \
             (C-4 / JE 1-J): got {:?}",
            names
        );

        // After commit the database must appear.
        txn.commit().unwrap();
        let names_after = env.database_names().unwrap();
        assert!(
            names_after.contains(&"pending_db".to_string()),
            "committed database must appear in database_names()"
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
        assert!(env.database_names().is_err());
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

        let names = env.database_names().unwrap();
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

    /// config() and home() return the correct values.
    #[test]
    fn test_get_config_and_home() {
        let (temp_dir, config) = temp_env_config();
        let env = Environment::open(config).unwrap();

        assert!(env.config().allow_create);
        assert_eq!(env.home(), temp_dir.path());
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
        let txn_id = txn.id();

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
            db.put(&k, &v).unwrap();
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
        let mc = env.mutable_config().unwrap();
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
        assert!(env.mutable_config().is_err());
    }

    #[test]
    fn test_set_mutable_config_updates_cache_size() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();
        let new_size: usize = 128 * 1024 * 1024; // 128 MiB
        let mc = EnvironmentMutableConfig::new().with_cache_size(new_size);
        env.set_mutable_config(mc).unwrap();
        let updated = env.mutable_config().unwrap();
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
        let updated = env.mutable_config().unwrap();
        assert_eq!(updated.lock_timeout_ms, Some(5_000));
        assert_eq!(updated.txn_timeout_ms, Some(10_000));
        env.close().unwrap();
    }

    #[test]
    fn test_set_mutable_config_none_timeout_unchanged() {
        let (_tmp, config) = temp_env_config();
        let mut env = Environment::open(config).unwrap();
        let original = env.mutable_config().unwrap();
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
        let updated = env.mutable_config().unwrap();
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
        let dur = txn.durability().expect("durability must be set");
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

    /// Sharded `ActiveTxns` must stay correct under concurrent begin/commit
    /// from many threads: every txn is tracked while open and pruned on
    /// commit, so the final registry len is 0 (no lost/duplicated entries
    /// across shards). Guards the shard-keying + aggregate-len paths.
    #[test]
    fn test_concurrent_registry_no_corruption() {
        use std::sync::Arc as StdArc;
        let (_tmp, config) = temp_env_config();
        let env = StdArc::new(Environment::open(config).unwrap());
        let initial = env.active_txns.len();

        let threads: Vec<_> = (0..16)
            .map(|_| {
                let env = StdArc::clone(&env);
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        let txn = env.begin_transaction(None).unwrap();
                        txn.commit().unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(
            env.active_txns.len(),
            initial,
            "every concurrently-committed txn must be pruned from its shard",
        );
        assert!(env.active_txns.is_empty());
        StdArc::try_unwrap(env).ok().unwrap().close().unwrap();
    }

    /// close()'s leaked-txn guard must still fire when the leaked txn lands
    /// in any shard: begin without commit, forget it, and close() must
    /// report the active txn (aggregate len across shards is non-zero).
    #[test]
    fn test_close_detects_leaked_txn_across_shards() {
        let (_tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        // Leak a txn: begin, then std::mem::forget so Drop can't prune it.
        let txn = env.begin_transaction(None).unwrap();
        std::mem::forget(txn);
        assert!(!env.active_txns.is_empty());
        let err = env.close().unwrap_err();
        assert!(
            matches!(err, NoxuError::OperationNotAllowed(_)),
            "close() must refuse to close with a leaked active txn, got {err:?}",
        );
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
        // Values > max_embedded_ln (16) so the LNs are NON-embedded and
        // their slot LSNs are checked by checkLsns.  JE
        // BtreeVerifier.verifyObsoleteRecords (BtreeVerifier.java:960-965)
        // exempts EMBEDDED LNs (their logrec is immediately obsolete), so a
        // 1-byte embedded value would not be checked (and must not be).
        let big = [b'x'; 32];
        db.put(
            DatabaseEntry::from_bytes(b"alpha"),
            DatabaseEntry::from_bytes(&big),
        )
        .unwrap();
        db.put(
            DatabaseEntry::from_bytes(b"beta"),
            DatabaseEntry::from_bytes(&big),
        )
        .unwrap();
        db.put(
            DatabaseEntry::from_bytes(b"gamma"),
            DatabaseEntry::from_bytes(&big),
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
            noxu_engine::gather_db_live_lsns(&guard)
                .into_iter()
                .next()
                .expect("invariant: at least one non-embedded live LSN")
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

    /// Test helper: a REPLICATED environment with the screen initially off.
    /// `env_impl.set_replicated(true)` makes `is_replicated()` true, so every
    /// user DB opened here is treated as replicated for write-screening
    /// (7.11.x safety gate; see `local-db-design.md`).  Arming the
    /// replica-write screen on top then models a replica node.
    fn replicated_env() -> (TempDir, Environment) {
        let (tmp, config) = temp_env_config();
        let env = Environment::open(config).unwrap();
        env.env_impl_arc_for_test().lock().set_replicated(true);
        (tmp, env)
    }

    /// GAP-REPLAY-3 WITNESS: on a REPLICA, a user write to a user database is
    /// refused with `NoxuError::ReplicaWrite` (JE
    /// `ReadonlyTxn.lockInternal` -> `disallowReplicaWrite`); reads still
    /// succeed; clearing the screen (become_master) re-enables writes.
    ///
    /// 7.11.x SAFETY GATE: in a replicated env every user DB is treated as
    /// replicated (`!allow_replica_write()` is true for it), so a replica
    /// refuses ALL local-origin user writes — including to a DB opened with
    /// `with_replicated(false)` — because Noxu lacks the JE machinery
    /// (NO_REPLICATE path, separate local db-id space) that makes a local DB
    /// on a replica safe.  See `local-db-design.md`.
    #[test]
    #[allow(deprecated)] // with_replicated: exercising the 7.11.x safety gate
    fn gap_replay_3_replica_screens_user_writes() {
        let (_tmp, env) = replicated_env();
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        // A DB the caller TRIED to mark local; the replicated env still treats
        // it as replicated (safety gate).
        let local_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_replicated(false);

        let db = env.open_database(None, "repdb", &cfg).unwrap();
        let localdb = env.open_database(None, "localdb", &local_cfg).unwrap();

        // Writes while MASTER (screen off) succeed.
        db.put(b"k", b"v").unwrap();
        localdb.put(b"lk", b"lv").unwrap();
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));

        // Flip the replica-write screen on, as become_replica does.
        env.env_impl_arc_for_test().lock().set_replica_write_screen(true);

        // Every user write is refused with ReplicaWrite — including to the
        // would-be-local DB (safety gate: a replica originates no user write).
        let w = db.put(b"k2", b"v2");
        assert!(
            matches!(w, Err(NoxuError::ReplicaWrite)),
            "a write on a replica must return ReplicaWrite, got {w:?}"
        );
        let d = db.delete(b"k");
        assert!(
            matches!(d, Err(NoxuError::ReplicaWrite)),
            "a delete on a replica must return ReplicaWrite, got {d:?}"
        );
        let lw = localdb.put(b"lk2", b"lv2");
        assert!(
            matches!(lw, Err(NoxuError::ReplicaWrite)),
            "a write to a would-be-local DB on a replica must ALSO return \
             ReplicaWrite under the 7.11.x safety gate, got {lw:?}"
        );

        // Reads still work.
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));

        // Clearing the screen (become_master) re-enables writes.
        env.env_impl_arc_for_test().lock().set_replica_write_screen(false);
        db.put(b"k3", b"v3").unwrap();
        assert_eq!(db.get(b"k3").unwrap().as_deref(), Some(&b"v3"[..]));

        drop(db);
        drop(localdb);
        env.close().unwrap();
    }

    /// BUG-REP-SCREEN-RACE (in-flight write across a demotion): a transaction
    /// that logged its writes while this node was the MASTER, and only reaches
    /// `commit()` AFTER the node stepped down (the replica-write screen armed),
    /// must be REFUSED at commit with `ReplicaWrite` and leave nothing durable.
    ///
    /// The op-level screen (`check_writable`) runs when each write op executes;
    /// here the `put_in` runs while the screen is clear (Master), so it is
    /// correctly allowed. Only the commit-time re-check catches the demotion.
    /// JE re-checks at exactly this point: `MasterTxn.preLogCommitHook` ->
    /// `RepImpl.preLogCommitHook` (RepImpl.java:1257) -> `checkIfMaster`
    /// (RepImpl.java:1076-1098), which throws before logging the commit entry;
    /// `MasterTxn.lockInternal` (MasterTxn.java:261-287) likewise re-reads the
    /// role on every post-begin lock. The armed screen stands in for the
    /// non-Master role; the refused commit aborts the txn with nothing durable.
    ///
    /// RED before this commit (the commit path only checked the R-REPUSER
    /// refuse-flag, not the screen, so the stepped-down write committed
    /// durably); GREEN after.
    #[test]
    fn gap_replay_3_in_flight_write_refused_at_commit_after_demotion() {
        let (tmp, env) = replicated_env();
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "repdb", &cfg).unwrap();

        // Begin a txn and write WHILE MASTER (screen clear): the op is allowed.
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(&txn, b"inflight", b"v").unwrap();

        // The node steps down to a non-Master role between the write and the
        // commit: `become_replica` / loss-of-master arms the screen.
        env.env_impl_arc_for_test().lock().set_replica_write_screen(true);

        // Commit must be refused BEFORE the TxnCommit frame (JE checkIfMaster).
        let c = txn.commit();
        assert!(
            matches!(c, Err(NoxuError::ReplicaWrite)),
            "an in-flight write committed after demotion must be refused with \
             ReplicaWrite (JE RepImpl.preLogCommitHook -> checkIfMaster), got \
             {c:?}"
        );
        // A refused commit leaves the txn for the caller to abort (JE's
        // throwPreCommitException runs the abort path); abort releases the
        // write lock and rolls back the buffered write.
        txn.abort().expect("abort after refused commit");

        // Nothing durable: the key is absent now, and absent after reopen.
        env.env_impl_arc_for_test().lock().set_replica_write_screen(false);
        assert_eq!(
            db.get(b"inflight").unwrap(),
            None,
            "the refused in-flight commit must not be visible"
        );
        drop(db);
        env.close().unwrap();

        let reopen_cfg = EnvironmentConfig::new(tmp.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env2 = Environment::open(reopen_cfg).unwrap();
        let db2 = env2.open_database(None, "repdb", &cfg).unwrap();
        assert_eq!(
            db2.get(b"inflight").unwrap(),
            None,
            "the refused in-flight commit must not be durable after reopen"
        );
        drop(db2);
        env2.close().unwrap();
    }

    /// BUG-REP-SCREEN-RACE (per-lock re-check fidelity): JE re-reads the role
    /// on EVERY post-begin write-lock acquisition, not just at commit --
    /// `MasterTxn.lockInternal` (MasterTxn.java:261-287) reads
    /// `((RepImpl)envImpl).getState()` and `throwNotMaster` (ReplicaWrite /
    /// UnknownMaster) if the node is no longer the Master, and
    /// `preLogWithoutLock` (MasterTxn.java:293-302) does the same for the
    /// log-before-lock case.  A long transaction that keeps WRITING after the
    /// node is demoted must therefore be refused at the NEXT write op, not
    /// deferred to commit.
    ///
    /// In Noxu the per-op write pre-flight is the dual of JE's per-lock gate:
    /// every user write op re-reads the replica-write screen
    /// (`Database::check_writable` / `CursorImpl::check_local_write_allowed`),
    /// taken as part of the write op exactly as JE acquires the write lock as
    /// part of the op.  This witness pins that equivalence so a future refactor
    /// cannot silently drop the per-op re-check and leave only the commit-time
    /// guard (`gap_replay_3_in_flight_write_refused_at_commit_after_demotion`),
    /// which would reintroduce the JE-divergence the coordinator asked about: a
    /// write issued after demotion that is only caught at commit.
    ///
    /// Scenario: write #1 while Master (op allowed); demote (screen armed);
    /// write #2 on the SAME open txn -> refused AT THE OP with `ReplicaWrite`
    /// (JE `MasterTxn.lockInternal` -> `throwNotMaster`); the txn then aborts
    /// with nothing durable (neither key present after abort + reopen).
    ///
    /// RED if the per-op screen is removed from the write path (write #2 would
    /// be accepted and only the commit guard would fire); GREEN with the per-op
    /// screen in place.
    #[test]
    fn gap_replay_3_write_after_demotion_refused_at_op_not_deferred_to_commit()
    {
        let (tmp, env) = replicated_env();
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "repdb", &cfg).unwrap();

        let txn = env.begin_transaction(None).unwrap();
        // Write #1 WHILE MASTER (screen clear): the op is allowed.
        db.put_in(&txn, b"first", b"1").unwrap();

        // The node steps down between write #1 and write #2 (screen armed).
        env.env_impl_arc_for_test().lock().set_replica_write_screen(true);

        // Write #2 on the SAME open txn, AFTER demotion, must be refused AT THE
        // OP -- JE `MasterTxn.lockInternal` re-checks the role on this lock and
        // throws before the LN is logged; it is NOT deferred to commit.
        let r = db.put_in(&txn, b"second", b"2");
        assert!(
            matches!(r, Err(NoxuError::ReplicaWrite)),
            "a write issued AFTER demotion must be refused at the OP (JE \
             MasterTxn.lockInternal -> throwNotMaster), not deferred to \
             commit, got {r:?}"
        );

        // The txn aborts; nothing durable from either write.
        txn.abort().expect("abort after refused op");
        env.env_impl_arc_for_test().lock().set_replica_write_screen(false);
        assert_eq!(db.get(b"first").unwrap(), None, "write #1 rolled back");
        assert_eq!(db.get(b"second").unwrap(), None, "write #2 never applied");
        drop(db);
        env.close().unwrap();

        let reopen_cfg = EnvironmentConfig::new(tmp.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env2 = Environment::open(reopen_cfg).unwrap();
        let db2 = env2.open_database(None, "repdb", &cfg).unwrap();
        assert_eq!(
            db2.get(b"first").unwrap(),
            None,
            "the aborted txn must leave nothing durable after reopen"
        );
        assert_eq!(db2.get(b"second").unwrap(), None);
        drop(db2);
        env2.close().unwrap();
    }

    /// GAP-REPLAY-3 WITNESS: the screen is driven by the screen FLAG (armed
    /// only on a replica by `become_replica`), not by anything a standalone
    /// environment ever sets.  A standalone (never-replicated) env never arms
    /// the screen, so a user write always succeeds -- the common no-
    /// replication case is unaffected.  The predicate retains its JE shape
    /// (`DatabaseImpl::allow_replica_write` = `!is_replicated() || dbType ==
    /// NAME`); the 7.11.x gate narrows it to
    /// `allow_replica_write_7_11_x` (internal DBs only) ONLY when the screen
    /// is armed, which only a replica does.  This pins that the standalone
    /// write path carries no replica screening.
    #[test]
    fn gap_replay_3_standalone_env_never_screens() {
        let (_tmp, config) = temp_env_config(); // standalone (non-rep) env
        let env = Environment::open(config).unwrap();
        assert!(
            !env.env_impl_arc_for_test().lock().is_replicated(),
            "a plain Environment is not replicated"
        );
        assert!(
            !env.env_impl_arc_for_test().lock().is_replica_write_screened(),
            "a standalone env never arms the replica-write screen"
        );
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "standalone", &cfg).unwrap();
        // Writes and creates always succeed on a standalone env.
        assert!(db.put(b"k", b"v").is_ok());
        assert!(db.delete(b"k").is_ok());
        drop(db);
        assert!(env.truncate_database(None, "standalone").is_ok());
        env.close().unwrap();
    }

    /// GAP-REPLAY-3 (merge-prep) WITNESS: DDL (truncate / remove / rename) of a
    /// user database on a REPLICA is refused with `NoxuError::ReplicaWrite`.
    /// JE screens the DDL NameLN path via `DbTree.lockNameLN` ->
    /// `checkReplicaWrite(getOperationRepContext(op))` gated on the target DB's
    /// replicated status; the 7.11.x safety gate treats every user DB as
    /// replicated, so all user-DB DDL on a replica is refused.
    #[test]
    #[allow(deprecated)]
    fn gap_replay_3_merge_prep_ddl_screened_on_replica() {
        let (_tmp, env) = replicated_env();
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let repdb = env.open_database(None, "ddldb", &cfg).unwrap();
        repdb.put(b"k", b"v").unwrap();
        drop(repdb);
        // A would-be-local DB is still screened for DDL under the safety gate.
        let local_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_replicated(false);
        let localdb = env.open_database(None, "localddl", &local_cfg).unwrap();
        localdb.put(b"k", b"v").unwrap();
        drop(localdb);

        env.env_impl_arc_for_test().lock().set_replica_write_screen(true);

        for (op, res) in [
            ("truncate", env.truncate_database(None, "ddldb").map(|_| ())),
            ("rename", env.rename_database(None, "ddldb", "ddldb2")),
            ("remove", env.remove_database(None, "ddldb")),
            (
                "truncate-local",
                env.truncate_database(None, "localddl").map(|_| ()),
            ),
            (
                "rename-local",
                env.rename_database(None, "localddl", "localddl2"),
            ),
            ("remove-local", env.remove_database(None, "localddl")),
        ] {
            assert!(
                matches!(res, Err(NoxuError::ReplicaWrite)),
                "{op} on a replica must return ReplicaWrite, got {res:?}"
            );
        }

        // Clearing the screen (become_master) re-enables DDL.
        env.env_impl_arc_for_test().lock().set_replica_write_screen(false);
        assert!(env.truncate_database(None, "ddldb").is_ok());

        let _ = env.close();
    }

    /// GAP-REPLAY-3 (merge-prep follow-up) WITNESS: on a REPLICA,
    /// `open_database(allow_create=true)` that would CREATE a user database is
    /// refused with `NoxuError::ReplicaWrite`; opening an EXISTING database is
    /// allowed (a pure NameLN read).  7.11.x safety gate: every user DB in a
    /// replicated env is treated as replicated, so all user-DB creation on a
    /// replica is refused (JE `DbTree.doCreateDb` -> `checkReplicaWrite` ->
    /// `mustGenerateVLSN`).
    #[test]
    #[allow(deprecated)]
    fn gap_replay_3_merge_prep_open_database_create_screened_on_replica() {
        let (_tmp, env) = replicated_env();
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);

        // Create an existing database while still the master.
        let db = env.open_database(None, "existing", &cfg).unwrap();
        db.put(b"k", b"v").unwrap();
        drop(db);

        env.env_impl_arc_for_test().lock().set_replica_write_screen(true);

        // Opening the EXISTING database on a replica still works (NameLN read).
        let existing = env.open_database(None, "existing", &cfg);
        assert!(
            existing.is_ok(),
            "opening an EXISTING DB on a replica must succeed, got {:?}",
            existing.err()
        );
        {
            let edb = existing.unwrap();
            assert_eq!(edb.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
            drop(edb);
        }

        // Creating a NEW user DB on a replica is refused (safety gate), whether
        // or not the caller tried to mark it local.
        for (name, c) in [
            ("brand_new", cfg.clone()),
            (
                "brand_new_local",
                DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true)
                    .with_replicated(false),
            ),
        ] {
            let created = env.open_database(None, name, &c);
            assert!(
                matches!(created, Err(NoxuError::ReplicaWrite)),
                "creating a NEW DB '{name}' on a replica must return \
                 ReplicaWrite, got {:?}",
                created.map(|_| "Ok(db)")
            );
            assert!(
                !env.env_impl_arc_for_test()
                    .lock()
                    .get_database_names()
                    .iter()
                    .any(|n| n == name),
                "a screened create must not register '{name}' on the replica"
            );
        }

        // Clearing the screen re-enables creation.
        env.env_impl_arc_for_test().lock().set_replica_write_screen(false);
        let now_ok = env.open_database(None, "brand_new", &cfg);
        assert!(
            now_ok.is_ok(),
            "after clearing the screen, creating the DB must succeed, got {:?}",
            now_ok.err()
        );
        drop(now_ok);

        let _ = env.close();
    }

    /// GAP-REPLAY-3 (merge-prep follow-up) WITNESS: the transactional create
    /// path is screened on a replica too.
    #[test]
    fn gap_replay_3_merge_prep_open_database_txn_create_screened_on_replica() {
        let (_tmp, env) = replicated_env();
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);

        env.env_impl_arc_for_test().lock().set_replica_write_screen(true);

        let txn = env.begin_transaction(None).unwrap();
        let created = env.open_database(Some(&txn), "txn_new", &cfg);
        assert!(
            matches!(created, Err(NoxuError::ReplicaWrite)),
            "transactional create of a NEW DB on a replica must return \
             ReplicaWrite, got {:?}",
            created.map(|_| "Ok(db)")
        );
        let _ = txn.abort();

        let _ = env.close();
    }
}

#[cfg(test)]
#[path = "cleaner_fetch_failclosed_probe.rs"]
mod cleaner_fetch_failclosed_probe;

#[cfg(test)]
#[path = "writer_oldlsn_probe.rs"]
mod writer_oldlsn_probe;

#[cfg(test)]
#[path = "del_race_probe.rs"]
mod del_race_probe;

#[cfg(test)]
#[path = "rc_scan_writer_order_probe.rs"]
mod rc_scan_writer_order_probe;
