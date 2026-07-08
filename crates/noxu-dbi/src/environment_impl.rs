//! Internal environment implementation.
//!

use hashbrown::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use bytes::BytesMut;
// DST: the `Arc<RwLock<DatabaseImpl>>` this env hands out (via `open_database`,
// `get_database_impl`, `db_map`) is passed into `CursorImpl`, whose `db_impl`
// RwLock routes through the `noxu_util::dst_sync_pl` seam (cursor shuttle
// gate). Route this env's RwLock-typed fields through the SAME seam so the
// types match under `--cfg noxu_shuttle`. Under the default cfg
// `dst_sync_pl::RwLock` *is* `noxu_sync::RwLock` (transparent re-export), so
// production is byte-identical. (The B-tree `Arc`s use `std::sync::RwLock`
// explicitly and are unaffected.)
use noxu_util::dst_sync_pl::RwLock;

use crate::database_impl::DatabaseImpl;
use crate::dbi_config::DbiEnvConfig;
use crate::file_manager_scanner::FileManagerLogScanner;
use crate::throughput_stats::ThroughputStatsSnapshot;
use crate::{
    DatabaseConfig, DatabaseId, DbType, DbiError, EnvState,
    EnvironmentFailureReason, NodeSequence,
};
use noxu_cleaner::{Cleaner, UtilizationTracker, UtilizationTrackerObserver};
use noxu_evictor::{Arbiter, EvictionSource, Evictor};
use noxu_log::{
    FileManager, LogEntryType, LogManager, Provisional,
    entry::{LnLogEntry, TxnEndEntry},
};
use noxu_recovery::RecoveryManager;
use noxu_sync::Mutex as NoxuMutex;
use noxu_txn::{LockManager, Txn, TxnManager};
use noxu_util::{lsn::NULL_LSN, vlsn::NULL_VLSN};

/// Interruptible-sleep signal for a daemon thread.
///
/// A daemon waits on [`DaemonSignal::wait_timeout`] for its poll interval;
/// [`DaemonSignal::shutdown`] sets the flag and wakes the thread immediately
/// (via the condvar), so `join()` returns without waiting for the current
/// sleep interval to elapse.  This replaces the previous chunked
/// `thread::sleep(100ms)` poll loops, which added up to one chunk of latency
/// (~100ms per daemon) to `close()` / `drop()` and inflated the W11 recovery
/// benchmark.  Mirrors the checkpointer's `wait_for_shutdown_or_timeout`.
struct DaemonSignal {
    shutdown: AtomicBool,
    lock: Mutex<()>,
    cv: Condvar,
}

impl DaemonSignal {
    fn new() -> Arc<Self> {
        Arc::new(DaemonSignal {
            shutdown: AtomicBool::new(false),
            lock: Mutex::new(()),
            cv: Condvar::new(),
        })
    }

    /// True once shutdown has been requested.
    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Request shutdown and wake the daemon immediately.
    fn shutdown(&self) {
        let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.shutdown.store(true, Ordering::Relaxed);
        self.cv.notify_all();
    }

    /// Sleep up to `dur`, returning early if shutdown is requested.
    /// Returns `true` if shutdown was requested (caller should exit).
    fn wait_timeout(&self, dur: std::time::Duration) -> bool {
        if self.shutdown.load(Ordering::Relaxed) {
            return true;
        }
        let guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        // Re-check under the lock to avoid missing a notify between the
        // unlocked check and acquiring the lock.
        if self.shutdown.load(Ordering::Relaxed) {
            return true;
        }
        let (_g, _timed_out) =
            self.cv.wait_timeout(guard, dur).unwrap_or_else(|e| e.into_inner());
        self.shutdown.load(Ordering::Relaxed)
    }
}

/// The internal representation of an environment.
///
/// Owns all subsystems: log manager, B-tree, transaction manager,
/// lock manager, evictor, cleaner, checkpointer, and INCompressor daemon.
///
///
pub struct EnvironmentImpl {
    /// Path to the environment home directory.
    env_home: PathBuf,
    /// Current environment state.
    state: RwLock<EnvState>,
    /// Whether this is a read-only environment.
    is_read_only: bool,
    /// Whether transactions are enabled.
    is_transactional: bool,
    /// Whether this environment is part of a replication group. Set once,
    /// after construction, by `noxu-rep`'s `ReplicatedEnvironment::new` (a
    /// plain `EnvironmentImpl` has no `noxu-rep` dependency, so this is a
    /// plain flag rather than a constructor parameter). Gates whether
    /// `DatabaseConfig::replicated` (default `true`) actually marks a
    /// database as replicated at open time — in a non-replicated
    /// environment every database is non-replicated regardless of the
    /// config value.
    is_replicated: std::sync::atomic::AtomicBool,

    /// Node ID and transient LSN generator.
    ///
    /// `Arc`-shared so the checkpointer can read the current max node-id at
    /// checkpoint time (REC-S) without re-plumbing ownership.
    node_sequence: Arc<NodeSequence>,
    /// Next database ID.
    ///
    /// `Arc`-shared so the checkpointer can read the current max db-id at
    /// checkpoint time (REC-S).
    next_db_id: Arc<AtomicI64>,

    /// The lock manager (shared across all lockers/txns).
    lock_manager: Arc<LockManager>,
    /// The transaction manager.
    txn_manager: Arc<TxnManager>,

    /// All open databases, keyed by DatabaseId.
    db_map: Arc<RwLock<HashMap<DatabaseId, Arc<RwLock<DatabaseImpl>>>>>,
    /// Name -> DatabaseId mapping.
    ///
    /// Only contains **committed** database registrations.  Databases opened
    /// inside an uncommitted transaction are held in `pending_names` until
    /// the transaction commits; on abort they are removed without ever
    /// appearing here.  This gives `get_database_names()` committed-only
    /// visibility (JE `DbTree.getDbNames()` / 1-J fix).
    name_map: RwLock<HashMap<String, DatabaseId>>,

    /// DBI-14: persisted comparator identities `(btree, dup)` per database
    /// name, recovered from NameLN data.  At open, a database whose name has
    /// a persisted identity here requires the caller to supply a comparator
    /// with the matching identity (or set the override flag), else open
    /// fails — mirroring JE's comparator mismatch semantics
    /// (`DatabaseImpl.ComparatorReader`).
    recovered_comparators:
        RwLock<HashMap<String, (Option<String>, Option<String>)>>,

    /// Names of databases whose creating transaction has not yet committed.
    ///
    /// Maps database name → the `DatabaseId` that was allocated at
    /// `open_database_inner` time.  Storing the id here (rather than only in
    /// `db_map`) enables:
    /// - O(1) lookup in `commit_pending_database` / `abort_pending_database`
    ///   (eliminates the former O(N) `db_map` scan).
    /// - Atomic pending→committed transition: the write lock acquired to
    ///   remove the name can be held until `name_map` is updated, closing
    ///   the TOCTOU window described in re-audit-keith R-4.
    ///
    /// `get_database_names()` excludes names in this map so callers see only
    /// committed databases (C-4 / JE 1-I / 1-J fix).
    pending_names: RwLock<hashbrown::HashMap<String, DatabaseId>>,

    /// Whether the environment has been invalidated.
    ///
    /// Stored as `Arc<AtomicBool>` so that `Database` and `CursorImpl`
    /// can cache a cheap clone and check validity on every hot-path
    /// operation without acquiring `env_impl.lock()`.
    is_invalid: Arc<AtomicBool>,
    /// If invalidated, the reason.
    invalid_reason: RwLock<Option<EnvironmentFailureReason>>,

    /// Creation time in milliseconds.
    creation_time_ms: u64,

    /// Write-ahead log manager (None for read-only environments).
    log_manager: Option<Arc<LogManager>>,

    /// Disk-limit violation tracker (JE: the cached disk-usage state on
    /// `Cleaner`).  Shared (`Arc`) into every user cursor so the write path
    /// can consult the cached violation flag with a single atomic load, and
    /// refreshed by the env after each cleaner/checkpointer run (JE:
    /// `Cleaner.freshenLogSizeStats`).  Inert when both `maxDisk` and
    /// `freeDisk` are 0.
    disk_limit: Arc<crate::disk_limit::DiskLimitTracker>,

    /// `DOS_PRODUCER_QUEUE_TIMEOUT`: max time the DiskOrderedScan producer
    /// blocks trying to enqueue an item before failing the scan (ms).
    dos_producer_queue_timeout_ms: u64,

    /// The cache evictor (shared with the background daemon thread).
    ///
    /// For a PRIVATE env this is a per-env evictor with its own daemon.  For a
    /// SHARED_CACHE env this is the process-global shared evictor (the same
    /// `Arc` every sharing env holds) and `evictor_handle` is `None` (the
    /// shared daemon runs elsewhere).
    evictor: Arc<Evictor>,

    /// Handle to the process-global shared evictor when `shared_cache=true`.
    ///
    /// `Some` only for shared-cache envs.  Its `deregister` is called on
    /// `close`/`Drop` to remove THIS env's trees from the shared LRU before
    /// the env's tree `Arc`s drop (no dangling trees / use-after-close), and
    /// to tear the shared evictor down when the last member leaves.
    shared_evictor_handle: Option<noxu_evictor::SharedEvictorHandle>,

    /// Background evictor daemon thread handle.
    ///
    /// Wrapped in `Mutex<Option<…>>` so that `close()` (which takes `&self`)
    /// can take ownership of the handle to join it.
    evictor_handle: Mutex<Option<std::thread::JoinHandle<()>>>,

    /// B-trees recovered from the log during startup, keyed by database ID
    /// (the `u64` form of `DatabaseId::id()`).
    ///
    /// Recovery replays committed LN records into a per-database tree.
    /// When `open_database()` is called and a matching entry exists here, the
    /// recovered tree is transplanted into the new `DatabaseImpl` via
    /// `set_recovered_tree()` instead of starting with an empty tree —
    /// giving crash-restart durability for in-memory state.
    ///
    /// `RecoveryManager.getDbIdToDbMap()` /
    /// `EnvironmentImpl.setupDbEnvironment()` tree population.
    recovered_trees: Mutex<HashMap<u64, noxu_tree::Tree>>,

    /// Wave 3-2: XA in-doubt transactions surfaced by recovery.
    ///
    /// `recovered_prepared_txns` is the list of `(xid, txn_id,
    /// first_lsn, last_lsn)` tuples that completed phase 1 of two-phase
    /// commit but were not committed or aborted before the crash.  The
    /// XA layer (`noxu_xa::XaEnvironment`) reads this via
    /// `recovered_prepared_txns()` to populate `xa_recover()` results
    /// and to resolve subsequent `xa_commit(xid)` / `xa_rollback(xid)`
    /// calls.
    ///
    /// `recovered_prepared_lns` is keyed by txn_id and holds the LN
    /// records that belong to each prepared txn.  `xa_commit` replays
    /// these into the in-memory tree at resolution time; `xa_rollback`
    /// discards them.
    recovered_prepared_txns: Mutex<Vec<noxu_recovery::PreparedTxnInfo>>,
    recovered_prepared_lns:
        Mutex<HashMap<u64, Vec<noxu_recovery::PreparedLnReplay>>>,

    /// VLSN→LSN pairs replayed during the redo phase (X-14 / X-1).
    ///
    /// Populated from `RecoveryInfo::recovered_vlsns` during `new()`.  Read
    /// by `ReplicatedEnvironment::with_environment()` to rebuild the
    /// in-memory VLSN index after crash recovery.
    pub recovery_vlsns: Vec<(u64, u64)>,

    /// Minimum rollback matchpoint LSN from recovery (X-1).
    ///
    /// `Some(lsn_u64)` when recovery detected a completed rollback; the
    /// VLSN index should be truncated to the VLSN at or before this LSN.
    pub recovery_rollback_matchpoint: Option<u64>,

    /// The primary (db_id=1) shared tree used for LN migration during
    /// log cleaning.
    ///
    /// This `Arc<RwLock<…>>` wraps the live B-tree for the default single
    /// database.  The `Cleaner` holds a clone of this Arc so that when
    /// `run_cleaner()` is called, live LN entries are migrated via
    /// `SharedTreeLookup`.
    ///
    /// Database tree access for file processing.
    primary_tree: Arc<std::sync::RwLock<noxu_tree::Tree>>,

    /// Per-database tree registry for secondary databases (X-7 fix).
    ///
    /// Maps `db_id.id() as i64` → `Arc<RwLock<Tree>>` for every non-primary
    /// database that has been opened.  The cleaner's `SharedTreeLookup`
    /// dispatches liveness checks for non-primary LNs to the correct tree
    /// via `with_extra_trees`.
    ///
    /// The `Arc<Mutex<…>>` wrapper lets `open_database_inner` insert entries
    /// after the cleaner has already been constructed.
    db_trees_registry: Arc<
        std::sync::Mutex<
            std::collections::HashMap<
                i64,
                Arc<std::sync::RwLock<noxu_tree::Tree>>,
            >,
        >,
    >,

    /// The log-file garbage collector.
    ///
    /// Created in `new()` for writable environments via
    /// `Cleaner::with_file_manager_and_tree()`.  For read-only environments
    /// `cleaner` is `None`.
    ///
    ///
    cleaner: Option<Arc<Cleaner>>,

    /// The checkpoint daemon.
    ///
    /// Created for writable environments; wired to the LogManager and the
    /// primary tree so that `do_checkpoint()` flushes dirty BINs to the log.
    ///
    ///
    checkpointer: Option<Arc<noxu_recovery::checkpointer::Checkpointer>>,

    /// Background checkpointer daemon thread handle.
    ///
    /// Wrapped in `Mutex<Option<…>>` so that `close()` (which takes `&self`)
    /// can take ownership of the handle to join it.
    checkpointer_handle: Mutex<Option<std::thread::JoinHandle<()>>>,

    /// Interval in milliseconds between periodic checkpoints.
    ///
    /// Default: 30,000 ms (30 seconds).  Passed to the checkpointer thread
    /// at environment creation time.
    checkpoint_interval_ms: u64,

    /// INCompressor daemon shutdown flag.
    ///
    /// Shared with the `in_compressor_handle` thread so that `close()` can
    /// signal the thread to exit.
    ///
    ///
    in_compressor_shutdown: Arc<DaemonSignal>,

    /// Background INCompressor daemon thread handle.
    ///
    /// The INCompressor processes BINs that have known-deleted slots,
    /// compressing them and pruning empty subtrees.  Mirrors the daemon
    /// pattern used by the evictor and checkpointer.
    ///
    /// / `INCompressor.run()`.
    in_compressor_handle: Mutex<Option<std::thread::JoinHandle<()>>>,

    /// Cleaner daemon shutdown flag (CleanerDaemon).
    cleaner_shutdown: Arc<DaemonSignal>,

    /// Background cleaner daemon thread handle.
    cleaner_handle: Mutex<Option<std::thread::JoinHandle<()>>>,

    // =========================================================================
    // X-11: LogFlushTask — background no-sync flush daemon
    // =========================================================================
    /// Shutdown flag for the `noxu-log-flusher` daemon.
    ///
    /// When `log_flush_no_sync_interval_ms > 0` in the environment config the
    /// daemon wakes on the configured interval and calls
    /// `LogManager::flush_no_sync()`.  This ensures that data committed with
    /// `CommitNoSync` durability reaches the OS page cache within the bounded
    /// interval even if no subsequent commit triggers a flush.
    log_flush_no_sync_shutdown: Arc<DaemonSignal>,

    /// Background log-flush-no-sync daemon thread handle.
    log_flush_no_sync_handle: Mutex<Option<std::thread::JoinHandle<()>>>,

    // =========================================================================
    // extended fork: additional background services
    // =========================================================================
    /// Background data-erasure daemon.
    ///
    /// Physically overwrites obsolete user data on disk so that sensitive
    /// data is unrecoverable.  Started lazily when the first erasure request
    /// is enqueued.
    ///
    /// (extended fork).
    data_eraser: Mutex<noxu_cleaner::DataEraser>,

    /// Background record-extinction scanner.
    ///
    /// Asynchronously removes extinct records from the B-tree after a
    /// `discard_extinct_records()` call commits.
    ///
    /// (extended fork).
    extinction_scanner: Mutex<noxu_cleaner::ExtinctionScanner>,

    /// Automatic backup manager.
    ///
    /// Copies closed log files to a configured archive destination on a
    /// cron-style schedule.
    ///
    /// (extended fork).
    backup_manager: Mutex<crate::backup_manager::BackupManager>,

    /// Per-file utilization tracker shared between the LogManager write path
    /// and the Cleaner.
    ///
    /// The `LogManager` holds a `LogWriteObserver` that calls into this tracker
    /// under the LWL for every log write.  The Cleaner reads the accumulated
    /// counts when choosing which files to clean.
    ///
    /// / `getUtilizationTracker()`
    ///.
    utilization_tracker: Option<Arc<NoxuMutex<UtilizationTracker>>>,

    /// Shared memory-usage counter wired into every user database tree and
    /// into the Arbiter.  Each BIN insert/delete updates this counter via
    /// `Tree::set_memory_counter`; the Arbiter reads it for eviction decisions.
    ///
    /// MemoryBudget.updateTreeMemoryUsage(delta) path.
    cache_usage: Arc<AtomicI64>,

    /// Per-category memory budget (DBI-20/21).  Its `tree` category is backed
    /// by `cache_usage` (the same `Arc` the arbiter reads), and it adds live
    /// `lock` / `txn` / `admin` categories so the over-budget view can account
    /// total memory, not just tree nodes.  JE `EnvironmentImpl.memoryBudget`.
    memory_budget: Arc<crate::memory_budget::MemoryBudget>,

    /// Optional VLSN counter installed by `ReplicatedEnvironment::with_environment`.
    ///
    /// When `Some`, `log_txn_commit` atomically increments this counter and
    /// writes the commit WAL entry with the 22-byte VLSN-tagged header so
    /// that `EnvironmentLogScanner` can discover and stream committed entries
    /// automatically.  `None` for standalone (non-replicated) environments —
    /// those always write the 14-byte header and are byte-unchanged.
    replication_vlsn_counter: Mutex<Option<Arc<std::sync::atomic::AtomicU64>>>,

    /// T-5: `TREE_COMPACT_MAX_KEY_LENGTH` (`EnvironmentParams
    /// .TREE_COMPACT_MAX_KEY_LENGTH`).  Threaded into every BIN tree this
    /// environment opens via `Tree::set_compact_max_key_length`
    /// (`IN.getCompactMaxKeyLength`).  Default 16.
    compact_max_key_length: i32,

    /// Background-daemon exception dispatcher (JE `ExceptionListener`
    /// substrate).  A shared, late-bindable slot handed to each daemon at
    /// spawn time; the higher layer (`noxu-db`) installs a sink via
    /// [`set_exception_sink`](Self::set_exception_sink) right after
    /// construction (before any daemon does work).  Daemon error sites call
    /// `dispatch` so an application can observe recoverable async failures.
    exception_dispatcher: noxu_config::ExceptionDispatcher,
}

impl EnvironmentImpl {
    /// Default interval between periodic checkpoints: 30 seconds.
    pub const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 30_000;

    /// Creates a new EnvironmentImpl.
    ///
    /// In a full implementation, this would:
    /// 1. Open/create the environment directory
    /// 2. Acquire the environment lock file
    /// 3. Initialize the log subsystem (FileManager, LogManager)
    /// 4. Run recovery
    /// 5. Open internal databases (id, name, utilization)
    /// 6. Start daemon threads (evictor, cleaner, checkpointer)
    pub fn new(
        env_home: impl Into<PathBuf>,
        read_only: bool,
        transactional: bool,
    ) -> Result<Self, DbiError> {
        Self::new_with_config(
            env_home,
            read_only,
            transactional,
            Self::DEFAULT_CHECKPOINT_INTERVAL_MS,
        )
    }

    /// Opens an environment using a fully specified `DbiEnvConfig`.
    ///
    /// The caller (typically `noxu_db::Environment::open`) translates its
    /// own `EnvironmentConfig` into this struct, which avoids a circular
    /// dependency between `noxu-db` and `noxu-dbi`.
    pub fn from_dbi_config(
        env_home: impl Into<PathBuf>,
        cfg: &DbiEnvConfig,
    ) -> Result<Self, DbiError> {
        Self::new_with_config_inner(env_home, cfg)
    }

    /// Like `new()` but allows overriding the checkpoint interval for testing.
    pub fn new_with_config(
        env_home: impl Into<PathBuf>,
        read_only: bool,
        transactional: bool,
        checkpoint_interval_ms: u64,
    ) -> Result<Self, DbiError> {
        Self::new_with_config_inner(
            env_home,
            &DbiEnvConfig {
                read_only,
                transactional,
                checkpointer_wakeup_interval_ms: checkpoint_interval_ms,
                ..DbiEnvConfig::default()
            },
        )
    }

    fn new_with_config_inner(
        env_home: impl Into<PathBuf>,
        cfg: &DbiEnvConfig,
    ) -> Result<Self, DbiError> {
        // JE latch knobs (ENV_LATCH_TIMEOUT / ENV_FORCED_YIELD): install the
        // process-global latch config BEFORE any latch is constructed during
        // env build-up.  Only touch the globals if the operator opted out of
        // the defaults, so an env that leaves these unset preserves the exact
        // historical latch behaviour (5 s timeout, no forced yield) —
        // byte-identical.  `env_latch_timeout_ms == 300_000` is the unset
        // sentinel (the JE default); `env_forced_yield == false` is the unset
        // sentinel.  See `noxu_latch::config`.
        if cfg.env_latch_timeout_ms != 300_000 || cfg.env_forced_yield {
            noxu_latch::configure(
                cfg.env_latch_timeout_ms,
                cfg.env_forced_yield,
            );
        }

        let read_only = cfg.read_only;
        let transactional = cfg.transactional;
        let checkpoint_interval_ms = cfg.checkpointer_wakeup_interval_ms;
        let env_home = env_home.into();
        let lock_manager = Arc::new(LockManager::with_config(
            cfg.lock_timeout_ms,
            cfg.n_lock_tables,
        ));
        let txn_manager = Arc::new(TxnManager::new(lock_manager.clone()));

        // REC-S/REC-C/L-30: the env's three id sequences.  Created here as
        // Arc so the checkpointer can read the current maxima at checkpoint
        // time (REC-S) and so they can be seeded from the recovered maxima
        // after recovery (REC-C).  Node-ids come from the single tree-wide
        // counter (L-30); `node_sequence` tracks the env's last_local_node_id
        // for the CheckpointEnd id field.
        let node_sequence = Arc::new(NodeSequence::new());
        let next_db_id = Arc::new(AtomicI64::new(1));

        // Ensure the environment directory exists (create if needed).
        if !env_home.exists() {
            std::fs::create_dir_all(&env_home).map_err(|e| {
                DbiError::EnvironmentFailure {
                    reason: format!(
                        "cannot create environment directory {}: {}",
                        env_home.display(),
                        e
                    ),
                }
            })?;
        }

        // Trees recovered from the log, keyed by database ID.
        // Populated during the recovery pass below (writable envs only).
        let mut recovered: HashMap<u64, noxu_tree::Tree> = HashMap::new();
        // Database name → id mappings recovered from NameLN entries.
        let mut recovered_names: HashMap<String, DatabaseId> = HashMap::new();
        // DBI-14: persisted comparator identities recovered from NameLN data.
        let mut recovered_comparators: HashMap<
            String,
            (Option<String>, Option<String>),
        > = HashMap::new();
        // Wave 3-2: prepared (XA in-doubt) transactions surfaced by
        // recovery.  Empty for fresh / clean-shutdown environments.
        let mut recovered_prepared: Vec<noxu_recovery::PreparedTxnInfo> =
            Vec::new();
        let mut recovered_prepared_lns: HashMap<
            u64,
            Vec<noxu_recovery::PreparedLnReplay>,
        > = HashMap::new();
        // X-14 / X-1: VLSN pairs and rollback matchpoint from recovery.
        let mut recovery_vlsns: Vec<(u64, u64)> = Vec::new();
        let mut recovery_rollback_matchpoint: Option<u64> = None;
        // REC-C: id maxima recovered from the log (CheckpointEnd id fields +
        // live scan).  Used to seed the env's sequences so post-restart
        // allocation never reuses an id present in the recovered log.
        let mut recovered_max_node_id: u64 = 0;
        let mut recovered_max_db_id: u64 = 0;
        let mut recovered_max_txn_id: u64 = 0;
        // REC-G / REC-H: checkpoint baselines recovered from the WAL, applied
        // to the checkpointer once it is built so the first post-recovery
        // checkpoint interval and ID continue from the recovered CkptEnd
        // instead of resetting to NULL_LSN / 1.
        let mut recovered_ckpt_start: noxu_util::Lsn = NULL_LSN;
        let mut recovered_ckpt_end: noxu_util::Lsn = NULL_LSN;
        let mut recovered_ckpt_id: Option<u64> = None;
        // CLN-4: per-file utilization summaries rebuilt from persisted
        // FileSummaryLN records, used to seed the cleaner's profile so it
        // sees real utilization immediately after restart.
        let mut rebuilt_file_summaries: HashMap<
            u32,
            noxu_recovery::RebuiltFileSummary,
        > = HashMap::new();

        // Initialize the WAL (LogManager) for writable environments.
        // Read-only environments don't need to write log entries.
        let log_manager_and_tracker = if !read_only {
            let fm = Arc::new(
                FileManager::new(
                    &env_home,
                    false,
                    cfg.log_file_max_bytes,
                    cfg.log_file_cache_size,
                )
                .map_err(|e| DbiError::EnvironmentFailure {
                    reason: format!("failed to init FileManager: {e}"),
                })?,
            );

            // Run 3-phase recovery before the first write.
            //
            // This scans the existing log files to:
            //   1. Find the true end-of-log and restore FileManager LSN state
            //      so new writes continue after the last valid entry.
            //   2. Build the committed/aborted transaction sets for analysis.
            //   3. Report max IDs seen (node, db, txn) for ID allocation.
            //   4. (P1b) Replay committed LN writes into a real B-tree so
            //      the in-memory state is reconstructed after a crash/reopen.
            //
            // Recovery replays LN records into a per-database Tree keyed by
            // db_id.  Each recovered tree is stashed in `recovered_trees` so
            // that `open_database()` can transplant it into the new
            // `DatabaseImpl` instead of starting from an empty tree.
            //
            // Multi-database recovery: recover_all() builds a HashMap<db_id, Tree>
            // and routes each LN/BIN entry to the correct database's tree.
            // Which populates
            // DbTree.dbIdToDb (a Map<DatabaseId, DatabaseImpl>) during the analysis
            // phase, then RecoveryManager.recoverDatabases() hands the recovered
            // DbTree to each DatabaseImpl.
            let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm))
                .with_halt_on_commit(
                    cfg.halt_on_commit_after_checksum_exception,
                );
            let mut rmgr = RecoveryManager::new();
            // REC-P: give recovery a LogManager so it can lazily fetch
            // checkpoint-seeded pre-checkpoint BINs during redo
            // (`fetchTarget`-in-recovery).  This is a read-only view over the
            // same FileManager the real LogManager (built below) will use;
            // recovery only calls `read_entry` on it.  Wiring it enables the
            // AfterCheckpointStart redo gate for checkpoint-seeded trees; if
            // it were absent, recovery would leave trees unseeded and
            // full-redo (the safe fallback).
            {
                let recovery_lm = Arc::new(LogManager::new(
                    Arc::clone(&fm),
                    cfg.log_num_buffers,
                    cfg.log_buffer_size,
                    cfg.log_fault_read_size,
                ));
                rmgr.set_log_manager(recovery_lm);
            }
            // Multi-DB recovery: discover every db_id in the log and build
            // a Tree for each one. During the analysis phase, each LN/BIN is
            // routed to the correct database by its db_id.
            //
            // We seed the map with db_id=1 (the primary user database) and let
            // recover_all() auto-insert entries for any other db_ids it discovers.
            let mut recovery_trees: HashMap<u64, noxu_tree::Tree> =
                HashMap::new();
            recovery_trees.insert(1u64, noxu_tree::Tree::new(1, 256));

            let recovery_info =
                match rmgr.recover_all(&mut scanner, &mut recovery_trees, true)
                {
                    Ok(info) => info,
                    Err(e) => {
                        return Err(DbiError::RecoveryFailure {
                            reason: e.to_string(),
                        });
                    }
                };

            // L-14 [#18307]: if end-of-log discovery found a committed txn
            // AFTER a mid-file corruption point (and the
            // haltOnCommitAfterChecksumException param was enabled), REFUSE
            // to mount the silently-truncated log — surface the fatal
            // FoundCommittedTxn reason. Faithful to JE LastFileReader
            // throwing EnvironmentFailureException(FOUND_COMMITTED_TXN, ...)
            // (LastFileReader.java:313).
            if let Some((corrupt_lsn, commit_lsn)) =
                scanner.take_found_committed_txn()
            {
                return Err(DbiError::RecoveryFailure {
                    reason: format!(
                        "{}: corrupt entry at LSN {corrupt_lsn}, committed \
                         txn at LSN {commit_lsn}",
                        crate::env_failure_reason::EnvironmentFailureReason::FoundCommittedTxn
                    ),
                });
            }

            // REP-1 STEP 4 (JE RollbackTracker.singlePassSetInvisible /
            // recoveryEndFsyncInvisible): (re-)mark rolled-back entries from
            // OPEN-ENDED rollback periods invisible on disk, in file order,
            // and fsync the affected files. A replica that crashed
            // mid-rollback before its invisible bits were durable would
            // otherwise re-apply the rolled-back entries on the next redo;
            // re-marking + fsync here closes that window.
            {
                let mut lsns: Vec<noxu_util::Lsn> =
                    rmgr.invisible_lsns_to_mark().to_vec();
                if !lsns.is_empty() {
                    // Sort into disk order (JE sorts rollbackLsns so the bit
                    // is flipped in file order for efficiency).
                    lsns.sort_by_key(|l| l.as_u64());
                    let mut files_to_fsync: Vec<u32> = Vec::new();
                    let mut i = 0;
                    while i < lsns.len() {
                        let file_num = lsns[i].file_number();
                        let mut offsets: Vec<u32> = Vec::new();
                        while i < lsns.len()
                            && lsns[i].file_number() == file_num
                        {
                            offsets.push(lsns[i].file_offset());
                            i += 1;
                        }
                        if let Err(e) = fm.make_invisible(file_num, &offsets) {
                            return Err(DbiError::RecoveryFailure {
                                reason: format!(
                                    "failed to re-mark rolled-back entries \
                                     invisible in file {file_num}: {e}"
                                ),
                            });
                        }
                        files_to_fsync.push(file_num);
                    }
                    if let Err(e) = fm.force(&files_to_fsync) {
                        return Err(DbiError::RecoveryFailure {
                            reason: format!(
                                "failed to fsync invisible-marked files: {e}"
                            ),
                        });
                    }
                }
            }

            // Wave 3-2: capture in-doubt prepared (XA) transactions so
            // the XA layer can surface them via xa_recover() and resolve
            // them via xa_commit / xa_rollback.
            recovered_prepared = recovery_info.recovered_prepared_txns.clone();
            // REC-G / REC-H: capture checkpoint baselines BEFORE recovery_info
            // is partially moved below.  `checkpoint_end` carries the recovered
            // CkptEnd whose `.get_id()` continues the checkpoint-ID sequence;
            // the start/end LSNs seed the interval baselines.  All are
            // NULL_LSN / None when the log had no prior checkpoint.
            recovered_ckpt_start = recovery_info.checkpoint_start_lsn;
            recovered_ckpt_end = recovery_info.checkpoint_end_lsn;
            recovered_ckpt_id = recovery_info.recovered_checkpoint_id;
            recovered_prepared_lns = recovery_info.prepared_txn_lns;
            // CLN-4: stash the rebuilt per-file utilization summaries.
            rebuilt_file_summaries = recovery_info.rebuilt_file_summaries;
            // X-14 / X-1: stash VLSN rebuild data.
            recovery_vlsns = recovery_info.recovered_vlsns;
            recovery_rollback_matchpoint =
                recovery_info.rollback_matchpoint_lsn;

            // REC-C: capture the id maxima the recovery pass computed so we
            // can seed the env's sequences below.  JE applies these as a
            // recovery contract: DbTree.setLastDbId, TxnManager.setLastTxnId,
            // NodeSequence.initRealNodeId / setLastNodeId — so post-restart
            // allocation never reuses an id present in the recovered log.
            recovered_max_node_id = recovery_info.use_max_node_id;
            recovered_max_db_id = recovery_info.use_max_db_id;
            recovered_max_txn_id = recovery_info.use_max_txn_id;

            // Install all recovered trees keyed by db_id so that
            // open_database() can transplant each into the matching DatabaseImpl.
            // Per-database tree population from
            // RecoveryManager.getDbIdToDbMap().
            for (db_id, tree) in recovery_trees {
                recovered.insert(db_id, tree);
            }

            // Restore the database name → id map from NameLN entries in the
            // WAL so that subsequent open_database() calls (including
            // read-only reopens with allow_create=false) can find registered
            // databases without needing to recreate them.
            for (name, db_id) in recovery_info.recovered_db_names {
                recovered_names.insert(name, DatabaseId::new(db_id as i64));
            }
            // DBI-14: capture persisted comparator identities.
            for (name, ids) in recovery_info.recovered_db_comparators {
                recovered_comparators.insert(name, ids);
            }

            let mut lm = LogManager::new(
                fm,
                cfg.log_num_buffers,
                cfg.log_buffer_size,
                cfg.log_fault_read_size,
            );
            lm.set_group_commit(
                cfg.log_group_commit_threshold,
                cfg.log_group_commit_interval_ms,
            );
            // Wire the LOG_CHECKSUM_READ knob (JE
            // LogManager.getChecksumOnRead). Default true matches JE.
            lm.set_checksum_on_read(cfg.log_checksum_read);

            // Wire the UtilizationTracker into the LogManager write path.
            // The observer is called under the LWL for every log write so
            // that utilization statistics are always consistent with the
            // on-disk log.
            // LogManager.logItem() calls envImpl.getUtilizationTracker()
            // and passes it to serialLogWork().
            //
            // DBI-24: cap the tracker's obsolete-offset detail at
            // CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE (default 2%) of the cache
            // size. JE: MemoryBudget.reset computes
            //   trackerBudget = cachePortion
            //                   * CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE / 100
            // (DbConfigManager.getInt(CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE)).
            // When the tracked detail exceeds this, evict_memory drops it
            // (keeping aggregate counts).
            let tracker_budget = (cfg.cache_size as i64).saturating_mul(
                cfg.cleaner_detail_max_memory_percentage as i64,
            ) / 100;
            let util_tracker = Arc::new(NoxuMutex::new(
                UtilizationTracker::with_budget(true, tracker_budget),
            ));
            let observer = Arc::new(UtilizationTrackerObserver::new(
                Arc::clone(&util_tracker),
            ));
            lm.set_write_observer(observer);

            Some((Arc::new(lm), util_tracker))
        } else {
            // Read-only environment: run a read-only scan to recover both
            // the name map and the trees so that open_database() can find
            // registered databases and their committed data.
            if let Ok(fm) = FileManager::new(
                &env_home, true, // read_only
                0,    // default log file size
                0,    // default cache size
            ) {
                let fm_arc = Arc::new(fm);
                let mut scanner =
                    FileManagerLogScanner::new(Arc::clone(&fm_arc));
                let mut rmgr = noxu_recovery::RecoveryManager::new();
                let mut recovery_trees: HashMap<u64, noxu_tree::Tree> =
                    HashMap::new();
                recovery_trees.insert(1u64, noxu_tree::Tree::new(1, 256));
                if let Ok(info) =
                    rmgr.recover_all(&mut scanner, &mut recovery_trees, true)
                {
                    for (name, db_id) in info.recovered_db_names {
                        recovered_names
                            .insert(name, DatabaseId::new(db_id as i64));
                    }
                    // DBI-14: capture persisted comparator identities
                    // (read-only reopen path).
                    for (name, ids) in info.recovered_db_comparators {
                        recovered_comparators.insert(name, ids);
                    }
                }
                for (db_id, tree) in recovery_trees {
                    recovered.insert(db_id, tree);
                }
            }
            None
        };
        let (log_manager, utilization_tracker): (
            Option<Arc<LogManager>>,
            Option<Arc<NoxuMutex<UtilizationTracker>>>,
        ) = match log_manager_and_tracker {
            Some((lm, ut)) => (Some(lm), Some(ut)),
            None => (None, None),
        };

        // Disk-limit tracker (JE: cached disk-usage state on Cleaner).
        // Probes through the LogManager's FileManager; inert when both limits
        // are 0 (the common default), in which case the write-path check is a
        // single branch with no statvfs.  Read-only envs never write, so the
        // tracker has no file manager and is permanently non-violating.
        let disk_limit = Arc::new(crate::disk_limit::DiskLimitTracker::new(
            cfg.max_disk,
            cfg.free_disk,
            cfg.reserved_disk,
            log_manager.as_ref().map(|lm| lm.file_manager().clone()),
        ));
        // Compute the initial violation state before any user write is served,
        // so an env reopened already over-limit refuses writes immediately
        // (JE calls freshenLogSizeStats during recovery).
        disk_limit.refresh();

        // REC-C / L-30: seed the env's three id sequences from the maxima the
        // recovery pass recovered from the log, so a freshly allocated
        // db-id / txn-id / node-id is always strictly greater than every id
        // present in the recovered log.  Without this the counters restart at
        // 1 and a prepared-XA txn-id or an un-reopened db-id can be reused
        // after restart (catalog / in-doubt-XA corruption).
        //
        // JE recovery contract: DbTree.setLastDbId / TxnManager.setLastTxnId /
        // NodeSequence.initRealNodeId (initRealNodeId seeds the node-id
        // counter from CheckpointEnd.lastLocalNodeId).
        if !read_only {
            // db-id: ensure next_db_id > max recovered db-id.
            let want_db = (recovered_max_db_id as i64).saturating_add(1);
            if next_db_id.load(Ordering::Relaxed) < want_db {
                next_db_id.store(want_db, Ordering::Relaxed);
            }
            // txn-id: the helper already ensures next_txn_id > id.
            if recovered_max_txn_id > 0 {
                txn_manager.set_last_txn_id(recovered_max_txn_id as i64);
            }
            // node-id: seed the env's NodeSequence and the single tree-wide
            // node-id counter (L-30) past the recovered max.
            if recovered_max_node_id > 0 {
                node_sequence.set_last_node_id(
                    NodeSequence::FIRST_REPLICATED_NODE_ID,
                    recovered_max_node_id as i64,
                );
                noxu_tree::seed_node_id_counter(recovered_max_node_id);
            }
        }

        // Primary shared tree (db_id=1) used for LN migration by the cleaner
        // and as the backing store for the checkpointer.
        //
        // This Arc is shared with the cleaner and checkpointer.  After the
        // first open_database() call for db_id=1, the DatabaseImpl receives
        // the recovered tree via set_recovered_tree(); the primary_tree here
        // starts empty but is kept in sync by subsequent writes through
        // the cursor layer.
        //
        // EnvironmentImpl.dbMapTree / getDbTree() used by the
        // FileProcessor (cleaner) to look up live BINs during LN migration.
        // Shared memory counter for evictor/MemoryBudget feedback.
        // Linked to the primary tree and to the Arbiter so that BIN entry
        // insertions/deletions are visible to the evictor.
        // IN.updateMemorySize(delta) → MemoryBudget.updateTreeMemoryUsage(delta).
        // SHARED_CACHE: when this env opts into the process-global shared
        // cache it joins a single Arc<Evictor> + one budget counter shared by
        // every sharing env (JE EnvironmentConfig.setSharedCache(true) ->
        // process-global SharedEvictor + shared MemoryBudget).  The first
        // joiner's cache_size sizes the ONE budget (JE-faithful); later
        // joiners' cache_size is ignored for the budget.
        //
        // We compute the budget/threshold FIRST (they are cheap and identical
        // to the private path) so the shared-join params are ready.
        let cache_bytes = cfg.cache_size as i64;
        // X-12: cache_size is the TOTAL memory budget. Subtract the log
        // write-buffer pool and off-heap reservation so that the three
        // independent pools (BIN tree, log buffers, off-heap) together
        // do not exceed cache_size. This matches JE semantics where
        // cache_size is the ceiling for total memory use.
        //
        // log_buffer_size is the per-buffer size; log_num_buffers is the count.
        let log_buf_total = (cfg.log_num_buffers * cfg.log_buffer_size) as i64;
        let off_heap_reserved = cfg.max_off_heap_memory as i64;
        // Floor at 1 MiB so the arbiter remains functional even if the user
        // sets a cache_size smaller than the buffer + off-heap reservations.
        let arbiter_budget = (cache_bytes - log_buf_total - off_heap_reserved)
            .max(1024 * 1024_i64);
        // F10 (JE EVICTOR_EVICT_BYTES / EVICTOR_CRITICAL_PERCENTAGE): read the
        // eviction hysteresis and critical threshold from config instead of
        // hardcoding 128 KiB / budget/16. Default evict_bytes = 512 KiB
        // (matches JE's EVICTOR_EVICT_BYTES default); critical_threshold =
        // budget * critical_percentage / 100 (JE
        // `maxMemory * criticalPercentage / 100`).
        let evict_bytes = (cfg.evictor_evict_bytes as i64).max(1024);
        let critical_threshold = arbiter_budget
            .saturating_mul(cfg.evictor_critical_percentage as i64)
            / 100;

        // Join the process-global shared cache if requested.  When shared, the
        // returned handle carries the ONE shared cache_usage counter and the
        // ONE shared Arc<Evictor>; when NOT shared this stays None and the
        // env builds a private evictor exactly as before (zero change).
        let shared_evictor_handle: Option<noxu_evictor::SharedEvictorHandle> =
            if cfg.shared_cache {
                Some(noxu_evictor::SharedEvictorHandle::join(
                    noxu_evictor::SharedCacheParams {
                        budget_bytes: arbiter_budget,
                        evict_bytes,
                        critical_threshold,
                        nodes_per_scan: cfg.evictor_nodes_per_scan,
                        lru_only: cfg.evictor_lru_only,
                        algorithm: noxu_evictor::EvictionAlgorithm::from_name(
                            &cfg.evictor_algorithm,
                        ),
                    },
                ))
            } else {
                None
            };

        // The tree memory counter: the shared budget counter when shared, else
        // a fresh per-env counter (unchanged private behaviour).
        let cache_usage = match &shared_evictor_handle {
            Some(h) => h.cache_usage(),
            None => Arc::new(AtomicI64::new(0)),
        };

        let mut primary_tree_inner = noxu_tree::Tree::new(1, 256);
        primary_tree_inner.set_memory_counter(Arc::clone(&cache_usage));
        // EV-14: wire the log manager so an evicted root IN can be re-fetched
        // from its persisted LSN (Tree::fetch_root_from_log).
        if let Some(ref lm) = log_manager {
            primary_tree_inner.set_log_manager(Arc::clone(lm));
        }
        let primary_tree: Arc<std::sync::RwLock<noxu_tree::Tree>> =
            Arc::new(std::sync::RwLock::new(primary_tree_inner));

        let arbiter = Arbiter::new(
            arbiter_budget,
            Arc::clone(&cache_usage),
            evict_bytes,
            critical_threshold,
        );

        // DBI-20/21: the real per-category memory budget.  Its tree category
        // shares `cache_usage` with the arbiter; lock / txn / admin categories
        // are added on top so total memory (not just tree nodes) is visible.
        let memory_budget = Arc::new(crate::memory_budget::MemoryBudget::new(
            arbiter_budget,
            Arc::clone(&cache_usage),
        ));
        // Build optional off-heap cache from config ( MAX_OFF_HEAP_MEMORY).
        let off_heap_cache = Arc::new(noxu_evictor::OffHeapCache::new(
            cfg.max_off_heap_memory > 0,
            cfg.max_off_heap_memory,
        ));

        // When shared, reuse the process-global Arc<Evictor> and DO NOT spawn
        // a private daemon (the shared daemon evicts across all envs).  When
        // private, build and own a per-env evictor + daemon exactly as before.
        let (evictor, evictor_thread) = if let Some(h) = &shared_evictor_handle
        {
            // The private `arbiter` we just built is discarded for shared
            // envs; the shared evictor owns the one shared arbiter.  Keep the
            // off-heap wiring off the shared evictor (a shared cache does not
            // mix per-env off-heap caches).
            let _ = arbiter; // shared budget lives in the shared evictor
            (h.evictor(), None)
        } else {
            let evictor_builder = Evictor::new(
                arbiter,
                cfg.evictor_nodes_per_scan,
                cfg.evictor_lru_only,
            )
            // Select the eviction algorithm (JE EVICTOR is LRU; Noxu defaults
            // to "lru" but allows clock/arc/car/lirs via EVICTOR_ALGORITHM).
            // Sets both the primary and scan policy slots.
            .with_algorithm(noxu_evictor::EvictionAlgorithm::from_name(
                &cfg.evictor_algorithm,
            ))
            // JE EVICTOR_USE_DIRTY_LRU; with_off_heap below forces it false if
            // the off-heap cache is enabled (JE Evictor.java:1705).
            .with_use_dirty_lru(cfg.evictor_use_dirty_lru)
            .with_mutate_bins(cfg.evictor_mutate_bins)
            .with_off_heap(Arc::clone(&off_heap_cache));
            // EVICTOR-LOG-1: wire the LogManager so `flush_dirty_node_to_log`
            // actually LOGS a dirty BIN before `detach_node_by_id` removes it
            // and stamps the BIN's `last_full_lsn` into the parent slot.
            // Without this the evictor field is `None`, so
            // `flush_dirty_node_to_log` short-circuits `return true` WITHOUT
            // logging; a dirty BIN is then detached with `last_full_lsn ==
            // NULL`, leaving the parent slot pointing at a stale LN LSN.  A
            // later re-fetch tries to parse that LN entry as a BIN
            // (`InLogEntry::read_from_log`) and fails -> the whole BIN's keys
            // are silently lost.  JE reaches the log via
            // `database.getEnv().getLogManager()` in `Evictor.evict` ->
            // `target.log(...)` (Evictor.java:3027); Noxu installs it
            // directly.  The checkpointer (line ~1233) was already wired this
            // way; the evictor was the missing sibling.
            let evictor_builder = match &log_manager {
                Some(lm) => evictor_builder.with_log_manager(Arc::clone(lm)),
                None => evictor_builder,
            };
            log::info!(
                "evictor eviction algorithm: {} (requested {:?})",
                evictor_builder.primary_algorithm_name(),
                cfg.evictor_algorithm
            );
            let evictor = Arc::new(evictor_builder);

            // Start the background daemon thread.  The thread loops as long as
            // `evictor.is_shutdown()` returns false, sleeping 5 ms between
            // passes so it is not a CPU hog when the cache is under budget.
            let evictor_clone = Arc::clone(&evictor);
            let handle = std::thread::Builder::new()
                .name("noxu-evictor".to_string())
                .spawn(move || {
                    while !evictor_clone.is_shutdown() {
                        evictor_clone.do_evict(EvictionSource::Daemon);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                })
                .expect("failed to spawn noxu-evictor thread");
            (evictor, Some(handle))
        };

        // Background-daemon exception dispatcher (JE ExceptionListener).
        // Created before spawning daemons so each daemon captures a clone and
        // can dispatch recoverable errors to the (later-installed) sink.
        let exception_dispatcher = noxu_config::ExceptionDispatcher::new();

        // Build the cleaner wired to the FileManager, primary tree, and
        // LogManager for writable environments.  Read-only envs get None.
        //
        // Build the db_trees_registry that will be shared between this
        // EnvironmentImpl and the Cleaner (X-7 fix).  Created here so we can
        // pass it to the cleaner constructor before EnvironmentImpl is built.
        let db_trees_registry: Arc<
            std::sync::Mutex<
                std::collections::HashMap<
                    i64,
                    Arc<std::sync::RwLock<noxu_tree::Tree>>,
                >,
            >,
        > = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

        // EVICTOR-RECLAIM-1: wire the SAME registry into the evictor so its
        // node lookups (strip/flush/detach/evict_root) search every database
        // tree, not just the primary slot.  JE's evictor walks one env-wide
        // INList covering all DBs and resolves each target's owning DB via
        // `target.getDatabase()` (Evictor.processTarget, Evictor.java:2374).
        // Set after construction because the registry is built here, after
        // the evictor builder above.
        //
        // SHARED_CACHE: for a shared env the evictor ALREADY holds the
        // process-global shared registry (union of all envs' trees); we must
        // NOT overwrite it with this env's local registry.  Instead we mirror
        // this env's trees INTO the shared registry (below and in
        // open_database_inner) while the cleaner/checkpointer keep using the
        // env-local `db_trees_registry` so they only ever see THIS env's
        // trees.  For a private env, behaviour is unchanged.
        if shared_evictor_handle.is_none() {
            evictor.set_db_trees_registry(Arc::clone(&db_trees_registry));
        } else if let Some(h) = &shared_evictor_handle {
            // Mirror the primary tree into the shared LRU immediately so
            // pre-open-database resident nodes are evictable across envs.
            h.register_tree(Arc::clone(&primary_tree));
        }

        // Cleaner initialization.
        // constructor (called after RecoveryManager.recover()).
        let cleaner = log_manager.as_ref().map(|lm| {
            let fm = Arc::clone(lm.file_manager());
            // Pass the environment's shared LockManager so that cleaner-held
            // locks contend with user transactions for correct deadlock
            // detection. The cleaner uses the environment's shared lock manager.
            let mut c = Cleaner::with_file_manager_tree_and_lock_manager(
                cfg.cleaner_min_utilization as u32,
                cfg.cleaner_min_file_count,
                cfg.cleaner_min_age as u64,
                fm,
                Arc::clone(&primary_tree),
                Arc::clone(lm),
                Arc::clone(&lock_manager),
            )
            // X-7: wire the shared db-tree registry so the cleaner
            // dispatches secondary-LN liveness checks to the correct tree.
            .with_tree_registry(Arc::clone(&db_trees_registry));
            // CLN-F1: wire the minFileUtilization second-tier threshold so the
            // faithful getBestFile decision can clean a single below-threshold
            // file even when the aggregate gate passes.
            c = c.with_min_file_utilization(
                cfg.cleaner_min_file_utilization as u32,
            );
            // Wire the live UtilizationTracker so that do_clean can build
            // the merged fileSummaryMap autonomously without needing manual
            // add_file_to_clean calls.
            // JE: FileProcessor.doClean calls
            //   profile.getFileSummaryMap(true /*includeTrackedFiles*/)
            //   which merges UtilizationProfile + UtilizationTracker
            //   (FileProcessor.java doClean ~line 340).
            if let Some(ref tracker) = utilization_tracker {
                c = c.with_utilization_tracker(Arc::clone(tracker));
            }
            // CLN-4: wire the TxnManager so do_clean clamps file selection to
            // the first-active-transaction window — the cleaner must not clean
            // a file whose log entries an open transaction may still need.
            // JE: UtilizationCalculator.getBestFile uses
            //   firstActiveFile = min(newestFile, firstActiveTxnFile).
            // Without this the clamp is inert (first_active_txn_file == None).
            c = c.with_txn_manager(Arc::clone(&txn_manager));
            // CFG-TWOPASS-1: wire the two-pass cleaning gate config.
            c = c.with_two_pass_params(
                cfg.cleaner_two_pass_gap,
                cfg.cleaner_two_pass_threshold,
            );
            Arc::new(c)
        });

        // CLN-4: seed the cleaner's UtilizationProfile from the per-file
        // summaries recovery rebuilt from persisted FileSummaryLN records.
        // After this the cleaner's get_file_summary_map sees real utilization
        // IMMEDIATELY after restart (no re-warm-from-live-writes lag).
        //
        // JE: UtilizationProfile.populateCache installs the FileSummaryLN
        // records into fileSummaryMap during recovery; the cleaner then reads
        // them via getFileSummaryMap.
        if let Some(ref c) = cleaner
            && !rebuilt_file_summaries.is_empty()
        {
            let mut seed: hashbrown::HashMap<u32, noxu_cleaner::FileSummary> =
                hashbrown::HashMap::with_capacity(rebuilt_file_summaries.len());
            for (file, r) in &rebuilt_file_summaries {
                // CLN-24: restore the cleaner's TTL expiration band from the
                // persisted histogram so the two-pass gate sees the same
                // expiration prediction it had before the restart, instead of
                // an empty band.  JE: ExpirationProfile.populateCache reads the
                // serialized histograms back into the profile's map.
                let (expired_lower, expired_gradual) =
                    if r.expiration_histogram.is_empty() {
                        (0, 0)
                    } else {
                        let tracker =
                            noxu_cleaner::ExpirationTracker::deserialize(
                                *file,
                                &r.expiration_histogram,
                            );
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let (lo, grad) = tracker.get_expired_bytes_band(
                            now_ms / 3_600_000,
                            now_ms % 3_600_000,
                        );
                        (
                            lo.min(i32::MAX as i64) as i32,
                            grad.min(i32::MAX as i64) as i32,
                        )
                    };
                seed.insert(
                    *file,
                    noxu_cleaner::FileSummary {
                        total_count: r.total_count,
                        total_size: r.total_size,
                        total_in_count: r.total_in_count,
                        total_in_size: r.total_in_size,
                        total_ln_count: r.total_ln_count,
                        total_ln_size: r.total_ln_size,
                        max_ln_size: r.max_ln_size,
                        obsolete_in_count: r.obsolete_in_count,
                        obsolete_ln_count: r.obsolete_ln_count,
                        obsolete_ln_size: r.obsolete_ln_size,
                        obsolete_ln_size_counted: r.obsolete_ln_size_counted,
                        obsolete_expired_size: expired_lower,
                        obsolete_expired_gradual_size: expired_gradual,
                        ..Default::default()
                    },
                );
            }
            c.seed_profile(seed);
        }

        // Build the checkpointer, wired to the LogManager and the primary
        // tree, for writable environments.  The db_id=1 convention matches
        // the default single database used by the primary tree.
        //
        // Constructor calling
        // `Checkpointer(env, DbEnvPool.CHECKPOINT_TIMEOUT_MS)`.
        let checkpointer = log_manager.as_ref().map(|lm| {
            use noxu_recovery::checkpointer::{CheckpointConfig, Checkpointer};
            let mut builder = Checkpointer::new(
                CheckpointConfig::new()
                    .bytes_interval(cfg.checkpointer_bytes_interval)
                    // T-17: thread the configurable BIN-delta percent
                    // (TREE_BIN_DELTA / BIN_DELTA_PERCENT) into the
                    // count-based delta-vs-full decision
                    // (BinStub::should_log_delta / JE BIN.shouldLogDelta).
                    .bin_delta_percent(cfg.tree_bin_delta_percent as i32)
                    // EVICTOR_ALLOW_BIN_DELTAS: master switch that can force
                    // full-BIN logging even when the count-based decision
                    // would pick a delta.
                    .allow_bin_deltas(cfg.evictor_allow_bin_deltas),
            )
            // REC-D: thread the configured CHECKPOINTER_BYTES_INTERVAL into
            // the runnable gate. Without this the gate used the hardcoded
            // 10 MiB default regardless of config. JE Checkpointer ctor reads
            // EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL into
            // logSizeBytesInterval, which isRunnable() consults.
            .with_bytes_interval(cfg.checkpointer_bytes_interval)
            // REC-D: wire the time interval (CHECKPOINTER_WAKEUP_INTERVAL) so
            // the bytes-OR-time runnable gate matches JE getWakeupPeriod
            // (bytes takes precedence when non-zero).
            .with_time_interval(cfg.checkpointer_wakeup_interval_ms)
            .with_log_manager(Arc::clone(lm))
            .with_tree(Arc::clone(&primary_tree), 1)
            // Stage-1: wire the db_trees_registry so the checkpointer flushes
            // ALL open user-database dirty BINs, not just the primary tree.
            // JE processINList walks a single env-wide INList covering all
            // databases; this achieves the same effect.
            .with_db_trees_registry(Arc::clone(&db_trees_registry));
            // X-5: wire the cleaner so do_checkpoint calls after_checkpoint()
            // and activates the three-state deletion barrier.
            if let Some(ref c) = cleaner {
                builder = builder.with_cleaner(Arc::clone(c));
            }
            // Stage-2 / T-F3: wire the txn manager so do_checkpoint can
            // compute the real first_active_lsn for CkptEnd.  Safe now that
            // Stage 1 checkpoints ALL user-database BINs.
            builder = builder.with_txn_manager(Arc::clone(&txn_manager));
            // REC-S: wire the db-id counter so do_checkpoint writes the real
            // last node/db/txn ids into CheckpointEnd (txn-id via the wired
            // txn_manager, node-id via the tree-wide counter).
            builder = builder.with_id_sources(Arc::clone(&next_db_id));
            // C7/CLN-4: wire the live UtilizationTracker so persist_file_summaries
            // writes real FileSummaryLN WAL entries at each checkpoint, which
            // recovery reads back to rebuild the profile after restart.
            if let Some(ref tracker) = utilization_tracker {
                builder = builder.with_utilization_tracker(Arc::clone(tracker));
            }
            Arc::new(builder)
        });

        // CLN-14: wire the cleaner's wakeupAfterNoWrites callback to the
        // checkpointer.  The cleaner is built before the checkpointer (the
        // checkpointer needs the cleaner for the X-5 deletion barrier), so
        // the cross-subsystem edge is registered here, once both exist.
        // After each successful cleaning pass the cleaner calls this, waking
        // the checkpointer daemon early so cleaned files are deleted promptly
        // instead of waiting the full checkpointer wakeup interval
        // (default 60 s) when write activity has stopped.
        //
        // JE: FileProcessor.doClean -> envImpl.getCheckpointer().wakeupAfterNoWrites().
        if let (Some(c), Some(ckpt)) = (&cleaner, &checkpointer) {
            // Weak, NOT Arc: the checkpointer already holds an Arc<Cleaner>
            // (with_cleaner, for the X-5 barrier), so an Arc here would form a
            // cleaner<->checkpointer reference cycle that leaks both on env
            // drop — and with them the env's file lock.  Upgrade at call time;
            // a dropped checkpointer simply skips the wakeup.
            let ckpt_for_wakeup = Arc::downgrade(ckpt);
            c.set_checkpoint_wakeup_fn(Arc::new(move || {
                if let Some(ckpt) = ckpt_for_wakeup.upgrade() {
                    ckpt.wakeup_after_no_writes();
                }
            }));
        }

        // F13: wire the checkpointer into the evictor so eviction of a dirty
        // BIN coordinates its `Provisional` flag with an in-progress
        // checkpoint.  The checkpointer is built AFTER the evictor (it needs
        // the tree + LogManager), so the evictor's `checkpointer` slot is still
        // `None` at this point; without this wire the evictor would always log
        // `Provisional::No` even for a BIN below the checkpoint's max flush
        // level, which can cause a recovery mismatch when an eviction races an
        // in-progress checkpoint.  For a SHARED_CACHE env the process-global
        // evictor is shared across environments (each with its OWN
        // checkpointer + max-flush-level), so a single per-env checkpointer
        // cannot be wired here without a cross-env coordination design; that
        // path retains the always-`Provisional::No` behaviour for now (see
        // CHANGELOG F13 note).  We wire the PRIVATE per-env evictor, which is
        // the default and the case the recovery-race affects in practice.
        //
        // JE ref: `Evictor.coordinateEvictionWithCheckpoint` ->
        // `Checkpointer.coordinateEvictionWithCheckpoint` ->
        // `DirtyINMap.coordinateEvictionWithCheckpoint` /
        // `getHighestFlushLevel(db)`.
        if shared_evictor_handle.is_none()
            && let Some(ckpt) = &checkpointer
        {
            evictor.set_checkpointer(Arc::downgrade(ckpt));
        }

        // REC-G / REC-H: seed the checkpointer's interval baselines and
        // checkpoint-ID sequence from the recovered CkptEnd, so the first
        // post-recovery checkpoint continues from the recovered state instead
        // of measuring from process start / restarting the ID at 1.  No-op
        // when the log had no prior checkpoint (NULL_LSN / None).  JE:
        // RecoveryManager.recover() calls Checkpointer.initIntervals(...) and
        // setCheckpointId(...) after the recovery scan.
        if let Some(ckpt) = &checkpointer {
            // REC-G
            ckpt.init_intervals(recovered_ckpt_start, recovered_ckpt_end);
            // REC-H
            if let Some(id) = recovered_ckpt_id {
                ckpt.set_checkpoint_id(id);
            }
        }

        // Start the background checkpointer daemon thread.
        //
        // Mirrors the evictor pattern: the thread holds an Arc clone of the
        // Checkpointer and loops with `thread::sleep(interval)` until the
        // shutdown flag is set.
        //
        // Gated on `cfg.run_checkpointer` (default true), matching how the
        // cleaner and INCompressor daemons honour their run flags.  Without
        // this gate the daemon ran even when a caller set
        // `run_checkpointer=false`, so a background checkpoint could race an
        // explicit `Environment::checkpoint` (surfacing "Checkpoint already in
        // progress").  Honouring the flag also makes the CLN-14 cleaner wakeup
        // a no-op when the daemon is disabled, as intended.
        //
        // `run()` → periodic checkpoint loop.
        let run_checkpointer_daemon = cfg.run_checkpointer;
        let checkpointer_thread = checkpointer.as_ref().map(|ckpt| {
            let ckpt_clone = Arc::clone(ckpt);
            let interval =
                std::time::Duration::from_millis(checkpoint_interval_ms);
            let disk_limit_for_ckpt = Arc::clone(&disk_limit);
            let dispatcher_for_ckpt = exception_dispatcher.clone();
            std::thread::Builder::new()
                .name("noxu-checkpointer".to_string())
                .spawn(move || {
                    if !run_checkpointer_daemon {
                        return;
                    }
                    while !ckpt_clone.is_shutdown() {
                        // Use condvar-based interruptible sleep so that
                        // request_shutdown() wakes the thread immediately.
                        ckpt_clone.wait_for_shutdown_or_timeout(interval);
                        if ckpt_clone.is_shutdown() {
                            break;
                        }
                        // Periodic disk-limit refresh (JE: the manageDiskUsage
                        // daemon updates the cached usage stats frequently
                        // enough to prevent violating the limits by a large
                        // amount). This is the wakeup that arms the limit as
                        // the log grows, and clears it once space is freed.
                        // Cheap no-op when enforcement is disabled.
                        disk_limit_for_ckpt.refresh();
                        // REC-D / REC-F: gate the daemon checkpoint on
                        // is_runnable so an idle environment is not
                        // checkpointed every wakeup (wasted I/O), while a
                        // cleaner with files pending reclaim still triggers an
                        // idle checkpoint. JE Checkpointer.doCheckpoint returns
                        // early when !isRunnable(config).
                        if !ckpt_clone.is_runnable(false) {
                            continue;
                        }
                        // Ignore checkpoint errors in the daemon — the
                        // environment may be closing or a concurrent
                        // checkpoint may be in progress.  But surface a
                        // recoverable error to the exception listener so an
                        // application can observe async checkpoint failures
                        // (JE ExceptionListener).
                        if let Err(e) = ckpt_clone.do_checkpoint("daemon") {
                            dispatcher_for_ckpt
                                .dispatch("Checkpointer", &e.to_string());
                        }
                    }
                })
                .expect("failed to spawn noxu-checkpointer thread")
        });

        // Shared db_map Arc — also given to the INCompressor daemon so it can
        // iterate open databases without holding a reference to `self`.
        let db_map: Arc<
            RwLock<HashMap<DatabaseId, Arc<RwLock<DatabaseImpl>>>>,
        > = Arc::new(RwLock::new(HashMap::new()));

        // Start the background INCompressor daemon thread (INCompressor).
        // Controlled by cfg.run_in_compressor; wakeup interval from
        // cfg.in_compressor_wakeup_interval_ms ( COMPRESSOR_WAKEUP_INTERVAL).
        let in_compressor_shutdown = DaemonSignal::new();
        let in_compressor_shutdown_clone = Arc::clone(&in_compressor_shutdown);
        let db_map_for_compressor = Arc::clone(&db_map);
        let lm_for_compressor = Arc::clone(&lock_manager);
        let compressor_interval_ms = cfg.in_compressor_wakeup_interval_ms;
        let run_in_compressor = cfg.run_in_compressor;
        let in_compressor_handle = std::thread::Builder::new()
            .name("noxu-in-compressor".to_string())
            .spawn(move || {
                if !run_in_compressor {
                    return;
                }
                while !in_compressor_shutdown_clone.is_shutdown() {
                    // Interruptible sleep: shutdown() wakes us immediately via
                    // the condvar, so close()/drop() doesn't wait out the poll
                    // interval (previously a chunked thread::sleep that added
                    // ~100ms to teardown and inflated the W11 benchmark).
                    if in_compressor_shutdown_clone.wait_timeout(
                        std::time::Duration::from_millis(
                            compressor_interval_ms,
                        ),
                    ) {
                        break;
                    }
                    // Iterate all open databases and compress any BINs that
                    // have known-deleted slots (INCompressor.processQueue path).
                    let db_list: Vec<Arc<RwLock<DatabaseImpl>>> =
                        db_map_for_compressor
                            .read()
                            .values()
                            .cloned()
                            .collect();
                    for db_arc in db_list {
                        let db = db_arc.read();
                        if let Some(tree) = db.get_real_tree() {
                            let bins = tree.collect_bins_with_known_deleted();
                            // IC-3: supply the lock-state predicate so the
                            // compressor SKIPS any known_deleted slot still
                            // write-locked by an in-flight txn, mirroring JE
                            // BIN.compress's isLockUncontended(lsn) check.
                            // `get_lock_info(lsn) != (0, 0)` is the inverse of
                            // JE's isLockUncontended (nWaiters==0 && nOwners==0,
                            // LockManager.java:692).
                            //
                            // Lock ordering: the predicate runs while
                            // compress_bin holds the BIN write latch; it takes
                            // a LockManager shard mutex for one short,
                            // non-blocking critical section and releases it
                            // before returning.  The LockManager never latches
                            // a BIN, so the only edge is BIN-latch ->
                            // shard-mutex (acyclic) — no deadlock.
                            let lm = &lm_for_compressor;
                            let is_locked =
                                move |lsn: u64| lm.get_lock_info(lsn) != (0, 0);
                            for bin_arc in bins {
                                tree.compress_bin_with_lock_check(
                                    &bin_arc,
                                    Some(&is_locked),
                                );
                            }
                        }
                    }
                }
            })
            .expect("failed to spawn noxu-in-compressor thread");

        // Start the background log-cleaner daemon thread (CleanerDaemon).
        // Sleeps for throttle.current_sleep_ms() between cleaning passes so
        // the sleep interval adapts to the current log write rate.
        let cleaner_shutdown = DaemonSignal::new();
        let cleaner_shutdown_clone = Arc::clone(&cleaner_shutdown);
        let cleaner_for_daemon = cleaner.as_ref().map(Arc::clone);
        let run_cleaner_daemon = cfg.run_cleaner;
        let disk_limit_for_cleaner = Arc::clone(&disk_limit);
        let dispatcher_for_cleaner = exception_dispatcher.clone();
        let cleaner_handle = std::thread::Builder::new()
            .name("noxu-cleaner".to_string())
            .spawn(move || {
                if !run_cleaner_daemon {
                    return;
                }
                while !cleaner_shutdown_clone.is_shutdown() {
                    let sleep_ms = if let Some(ref c) = cleaner_for_daemon {
                        if let Err(e) =
                            c.do_clean(c.throttle.current_n_files(), false)
                        {
                            // Surface recoverable cleaner errors to the
                            // exception listener (JE ExceptionListener).
                            dispatcher_for_cleaner
                                .dispatch("Cleaner", &e.to_string());
                        }
                        // JE: Cleaner.manageDiskUsage refreshes the cached disk
                        // usage stats after each pass so writes blocked by a
                        // disk-limit violation resume as soon as space frees.
                        disk_limit_for_cleaner.refresh();
                        c.throttle.current_sleep_ms()
                    } else {
                        5_000 // no cleaner — sleep 5 s
                    };
                    // Interruptible sleep: shutdown() wakes immediately.
                    if cleaner_shutdown_clone.wait_timeout(
                        std::time::Duration::from_millis(sleep_ms),
                    ) {
                        break;
                    }
                }
            })
            .expect("failed to spawn noxu-cleaner thread");

        // X-11: Start the background LogFlushTask daemon (LogFlushTask).
        // When log_flush_no_sync_interval_ms > 0 the daemon wakes on the
        // configured interval and calls flush_no_sync() so data committed
        // with CommitNoSync (SyncPolicy::NoSync) is drained from the in-process
        // write buffers to the OS page cache within a bounded time.
        // If the interval is 0 the thread exits immediately (disabled path).
        let log_flush_no_sync_shutdown = DaemonSignal::new();
        let log_flush_no_sync_shutdown_clone =
            Arc::clone(&log_flush_no_sync_shutdown);
        let flush_interval_ms = cfg.log_flush_no_sync_interval_ms;
        let lm_for_flush = log_manager.as_ref().map(Arc::clone);
        let dispatcher_for_flush = exception_dispatcher.clone();
        let log_flush_no_sync_handle = std::thread::Builder::new()
            .name("noxu-log-flusher".to_string())
            .spawn(move || {
                if flush_interval_ms == 0 {
                    return; // disabled
                }
                while !log_flush_no_sync_shutdown_clone.is_shutdown() {
                    // Interruptible sleep: shutdown() wakes immediately.
                    if log_flush_no_sync_shutdown_clone.wait_timeout(
                        std::time::Duration::from_millis(flush_interval_ms),
                    ) {
                        break;
                    }
                    if let Some(ref lm) = lm_for_flush
                        && let Err(e) = lm.flush_no_sync()
                    {
                        // Surface background fsync/flush errors (JE
                        // ExceptionListener): the app can observe I/O errors
                        // that would otherwise be silently swallowed here.
                        dispatcher_for_flush
                            .dispatch("LogFlusher", &e.to_string());
                    }
                }
            })
            .expect("failed to spawn noxu-log-flusher thread");

        let env = EnvironmentImpl {
            env_home,
            state: RwLock::new(EnvState::Init),
            is_read_only: read_only,
            is_transactional: transactional,
            is_replicated: std::sync::atomic::AtomicBool::new(false),
            node_sequence: Arc::clone(&node_sequence),
            next_db_id: Arc::clone(&next_db_id),
            lock_manager,
            txn_manager,
            db_map,
            name_map: RwLock::new(recovered_names),
            recovered_comparators: RwLock::new(recovered_comparators),
            pending_names: RwLock::new(hashbrown::HashMap::new()),
            is_invalid: Arc::new(AtomicBool::new(false)),
            invalid_reason: RwLock::new(None),
            creation_time_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            log_manager,
            disk_limit,
            dos_producer_queue_timeout_ms: cfg.dos_producer_queue_timeout_ms,
            evictor,
            shared_evictor_handle,
            // `evictor_thread` is `None` for a shared-cache env (the shared
            // daemon runs process-globally); `Some(handle)` for a private env.
            evictor_handle: Mutex::new(evictor_thread),
            recovered_trees: Mutex::new(recovered),
            recovered_prepared_txns: Mutex::new(recovered_prepared),
            recovered_prepared_lns: Mutex::new(recovered_prepared_lns),
            recovery_vlsns,
            recovery_rollback_matchpoint,
            primary_tree,
            db_trees_registry,
            cleaner,
            checkpointer,
            checkpointer_handle: Mutex::new(checkpointer_thread),
            checkpoint_interval_ms,
            in_compressor_shutdown,
            in_compressor_handle: Mutex::new(Some(in_compressor_handle)),
            cleaner_shutdown,
            cleaner_handle: Mutex::new(Some(cleaner_handle)),
            log_flush_no_sync_shutdown,
            log_flush_no_sync_handle: Mutex::new(Some(
                log_flush_no_sync_handle,
            )),
            data_eraser: Mutex::new(noxu_cleaner::DataEraser::new()),
            extinction_scanner: Mutex::new(
                noxu_cleaner::ExtinctionScanner::new(),
            ),
            backup_manager: Mutex::new(
                crate::backup_manager::BackupManager::new(),
            ),
            utilization_tracker,
            cache_usage,
            memory_budget,
            replication_vlsn_counter: Mutex::new(None),
            // T-5: TREE_COMPACT_MAX_KEY_LENGTH from the env config.
            compact_max_key_length: cfg.tree_compact_max_key_length as i32,
            exception_dispatcher,
        };

        // Mark as open
        *env.state.write() = EnvState::Open;

        Ok(env)
    }

    // Getters
    pub fn get_env_home(&self) -> &Path {
        &self.env_home
    }

    /// Install a VLSN counter for replicated commit tagging.
    ///
    /// Called by `ReplicatedEnvironment::with_environment` to enable automatic
    /// VLSN assignment on every commit.  Once set, `log_txn_commit` atomically
    /// increments the counter and writes the WAL entry with the 22-byte
    /// VLSN-tagged header so that `EnvironmentLogScanner` discovers it
    /// without any manual `replicate_entry` calls.
    ///
    /// Non-replicated (standalone) environments never call this; their commit
    /// path is byte-unchanged (14-byte header, no VLSN field).
    pub fn set_replication_vlsn_counter(
        &self,
        counter: Arc<std::sync::atomic::AtomicU64>,
    ) {
        *self.replication_vlsn_counter.lock().unwrap() = Some(counter);
    }

    /// Install the background-daemon exception sink (JE `ExceptionListener`).
    ///
    /// Called by `noxu_db::Environment::open` right after construction — before
    /// any daemon performs work (each daemon sleeps its wakeup interval first).
    /// Once installed, recoverable errors in the checkpointer / cleaner /
    /// log-flusher daemons are dispatched to `sink` as `(source, message)`.
    pub fn set_exception_sink(&self, sink: noxu_config::DaemonExceptionSink) {
        self.exception_dispatcher.set(sink);
    }

    /// Test/diagnostic accessor: the daemon exception dispatcher.
    pub fn exception_dispatcher(&self) -> &noxu_config::ExceptionDispatcher {
        &self.exception_dispatcher
    }
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }
    pub fn is_transactional(&self) -> bool {
        self.is_transactional
    }
    /// Returns whether this environment is part of a replication group.
    /// See [`Self::set_replicated`].
    pub fn is_replicated(&self) -> bool {
        self.is_replicated.load(std::sync::atomic::Ordering::Relaxed)
    }
    /// Marks this environment as replicated. Called exactly once by
    /// `noxu-rep`'s `ReplicatedEnvironment::new`, immediately after
    /// constructing the underlying `EnvironmentImpl` and before any
    /// database is opened, so every subsequent `open_database` sees the
    /// correct value when resolving `DatabaseConfig::replicated`. A plain
    /// (non-`noxu-rep`) `EnvironmentImpl` never calls this and stays
    /// `false`, so every database in a standalone environment is correctly
    /// non-replicated regardless of the config's `replicated` value.
    pub fn set_replicated(&self, replicated: bool) {
        self.is_replicated.store(replicated, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn get_creation_time(&self) -> u64 {
        self.creation_time_ms
    }

    // State management
    pub fn get_state(&self) -> EnvState {
        *self.state.read()
    }
    pub fn is_open(&self) -> bool {
        self.state.read().is_open()
    }
    pub fn is_valid(&self) -> bool {
        if self.is_invalid.load(Ordering::Relaxed) {
            return false;
        }
        // C-2: also check whether the log manager has detected an I/O failure
        // (fsyncgate — fdatasync returned EIO and the environment must not
        // accept further commits).  We read the shared Arc<AtomicBool> that
        // LogManager::io_invalid points to; no circular Arc reference.
        if let Some(lm) = &self.log_manager
            && lm.io_invalid.load(std::sync::atomic::Ordering::Acquire)
        {
            return false;
        }
        true
    }

    /// Checks that the environment is open and valid.
    pub fn check_open(&self) -> Result<(), DbiError> {
        // Check validity first - if invalidated, that takes precedence
        if !self.is_valid() {
            let reason = self
                .invalid_reason
                .read()
                .map(|r| format!("{:?}", r))
                .unwrap_or_else(|| "unknown".to_string());
            return Err(DbiError::EnvironmentFailure { reason });
        }
        if !self.is_open() {
            return Err(DbiError::EnvironmentNotOpen);
        }
        Ok(())
    }

    /// Invalidates the environment due to a failure.
    pub fn invalidate(&self, reason: EnvironmentFailureReason) {
        self.is_invalid.store(true, Ordering::Relaxed);
        *self.invalid_reason.write() = Some(reason);
        *self.state.write() = EnvState::Invalid;
    }

    /// Returns a cheap `Arc<AtomicBool>` clone of the invalidation flag.
    ///
    /// Callers (`Database`, `CursorImpl`) cache this at open time so
    /// `check_open` / `check_state` can detect environment invalidity
    /// without acquiring `env_impl.lock()` on every operation.
    /// X-13 fix.
    pub fn is_invalid_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.is_invalid)
    }

    // Database operations

    /// Creates or opens a database.
    ///
    /// `creating_txn_id`: when `Some`, the database is created under a
    /// transaction and the name registration is deferred (C-4 / JE 1-I fix):
    /// - A `NameLNTxn` WAL entry (`Provisional::Yes`) is written inside the
    ///   creating transaction (C-6).
    /// - The name and its `DatabaseId` are stored in `pending_names` (a
    ///   `HashMap<String, DatabaseId>`) but NOT in `name_map` yet.
    /// - `get_database_names()` excludes pending names.
    /// - Call `commit_pending_database(name)` when the txn commits to move
    ///   the name into `name_map` atomically (no additional WAL write — the
    ///   TxnCommit record makes the provisional NameLNTxn durable).
    /// - Call `abort_pending_database(name)` when the txn aborts to remove
    ///   the db from `db_map` and `pending_names`.  Recovery's
    ///   `run_mapping_tree_undo_pass` removes the NameLNTxn.
    ///
    /// When `None` (non-transactional), behaviour is unchanged: the name
    /// is inserted into `name_map` immediately and a NameLN is written.
    pub fn open_database(
        &self,
        name: &str,
        config: &DatabaseConfig,
    ) -> Result<Arc<RwLock<DatabaseImpl>>, DbiError> {
        self.open_database_inner(name, config, None)
    }

    /// Transactional variant: creates the database under a transaction.
    ///
    /// The name is added to `pending_names` (not yet to `name_map`).  The
    /// caller is responsible for calling `commit_pending_database` or
    /// `abort_pending_database` when the transaction resolves.
    ///
    /// `txn_id` is the ID of the creating transaction.  A `NameLNTxn` WAL
    /// entry (with `Provisional::Yes`) is written **inside** the transaction
    /// so that crash recovery can undo it if the transaction never commits.
    pub fn open_database_transactional(
        &self,
        name: &str,
        config: &DatabaseConfig,
        txn_id: u64,
    ) -> Result<Arc<RwLock<DatabaseImpl>>, DbiError> {
        self.open_database_inner(name, config, Some(txn_id))
    }

    fn open_database_inner(
        &self,
        name: &str,
        config: &DatabaseConfig,
        creating_txn_id: Option<u64>,
    ) -> Result<Arc<RwLock<DatabaseImpl>>, DbiError> {
        self.check_open()?;

        // Check if database already exists in the open db_map.
        if let Some(db_id) = self.name_map.read().get(name)
            && let Some(db) = self.db_map.read().get(db_id)
        {
            db.read().increment_reference_count();
            return Ok(db.clone());
        }

        // R-4 TOCTOU guard: if the name is currently being committed from
        // another transaction (name in pending_names but not yet in
        // name_map), treat it as "already exists" rather than creating a
        // second DatabaseImpl with a conflicting DatabaseId.  This check
        // closes the race window complemented by commit_pending_database
        // holding pending_names.write() across the name_map insert.
        if self.pending_names.read().contains_key(name) {
            return Err(DbiError::DatabaseAlreadyExists(name.to_string()));
        }

        // Check if the name was recovered from the WAL (name in name_map but
        // no live db_map entry yet).  Use the recovered db_id so that the
        // correct recovered tree is transplanted below.
        let recovered_db_id: Option<i64> =
            self.name_map.read().get(name).copied().map(|id| id.id());

        if recovered_db_id.is_none() && !config.allow_create {
            return Err(DbiError::DatabaseNotFound(name.to_string()));
        }

        // DBI-14: comparator mismatch check.  If this database was recovered
        // with a persisted comparator identity, the caller must supply a
        // comparator whose identity matches (or set the override flag), else
        // the open fails — reinterpreting a comparator-ordered tree as
        // byte-ordered (or under the wrong comparator) silently corrupts the
        // sort.  Mirrors JE `DatabaseImpl.ComparatorReader` /
        // `setOverrideBtreeComparator`.
        if recovered_db_id.is_some()
            && let Some((persisted_btree, persisted_dup)) =
                self.recovered_comparators.read().get(name).cloned()
        {
            let cfg_btree =
                config.btree_comparator.as_ref().map(|c| c.identity.clone());
            let cfg_dup = config
                .duplicate_comparator
                .as_ref()
                .map(|c| c.identity.clone());
            if !config.override_btree_comparator && persisted_btree != cfg_btree
            {
                return Err(DbiError::ComparatorMismatch {
                    name: name.to_string(),
                    kind: "btree",
                    persisted: persisted_btree,
                    configured: cfg_btree,
                });
            }
            if !config.override_duplicate_comparator && persisted_dup != cfg_dup
            {
                return Err(DbiError::ComparatorMismatch {
                    name: name.to_string(),
                    kind: "duplicate",
                    persisted: persisted_dup,
                    configured: cfg_dup,
                });
            }
        }

        let db_id = if let Some(rid) = recovered_db_id {
            // Reopen: use the recovered db_id and ensure the counter is
            // at least rid + 1 so fresh creations don't reuse it.
            let next = self.next_db_id.load(Ordering::Relaxed);
            if next <= rid {
                self.next_db_id.store(rid + 1, Ordering::Relaxed);
            }
            DatabaseId::new(rid)
        } else {
            // New creation: allocate a fresh ID.
            DatabaseId::new(self.next_db_id.fetch_add(1, Ordering::Relaxed))
        };

        let mut db_impl =
            DatabaseImpl::new(db_id, name.to_string(), DbType::User, config);
        // The config's `replicated` (default `true`) only takes effect
        // when the owning environment is itself replicated; a plain
        // environment never marks a database replicated regardless of the
        // config value.
        if self.is_replicated() {
            db_impl.set_replicated(config.replicated);
        }

        // Wire the environment's shared memory counter into the new database
        // tree so that BIN insertions/deletions are visible to the Arbiter
        // (MemoryBudget.updateTreeMemoryUsage path).
        db_impl.set_memory_counter(Arc::clone(&self.cache_usage));
        // T-5: thread TREE_COMPACT_MAX_KEY_LENGTH into the tree so the BIN
        // compact-key rep uses the configured threshold
        // (IN.getCompactMaxKeyLength).
        db_impl.set_tree_compact_max_key_length(self.compact_max_key_length);

        // If recovery populated a tree for this db_id, transplant it so the
        // database starts from its recovered (crash-consistent) state rather
        // than an empty tree.  We use `remove` so the tree is transferred
        // (not cloned) and the map entry is consumed — each recovered tree is
        // used at most once.
        if let Some(recovered_tree) =
            self.recovered_trees.lock().unwrap().remove(&(db_id.id() as u64))
        {
            db_impl.set_recovered_tree(recovered_tree);
            // Re-wire counter since set_recovered_tree replaces the tree.
            db_impl.set_memory_counter(Arc::clone(&self.cache_usage));
        }

        let db = Arc::new(RwLock::new(db_impl));
        db.read().increment_reference_count();

        // X-7: register this database's tree in the shared registry so the
        // cleaner can dispatch secondary-LN liveness checks to the correct
        // tree.  Since the cleaner holds an Arc clone of db_trees_registry,
        // it will automatically see this tree on its next clean cycle.
        if let Some(tree_arc) = db.read().get_real_tree_arc() {
            if let Ok(mut reg) = self.db_trees_registry.lock() {
                reg.insert(db_id.id(), Arc::clone(&tree_arc));
            }

            // F1: feed this database tree's node add/access/remove events to
            // the evictor's LRU lists, and point the evictor's eviction walk
            // at this tree, so that nodes inserted/accessed through the
            // production cursor path actually populate the lists the evictor
            // drains.  Without this the policy lists stay empty, every
            // evict_batch phase quota is 0, and the evictor selects nothing.
            //
            // EVICTOR-RECLAIM-1: the evictor now resolves a targeted node to
            // its owning tree via the shared `db_trees_registry` (wired in
            // `EnvironmentImpl::new`), so eviction reclaims memory across ALL
            // database trees — not just the one in the primary slot.  We still
            // set the primary slot to this tree as the first lookup target,
            // and the registry insert above makes every other DB reachable.
            //
            // JE: the database's INs are registered in the environment-wide
            // INList, which feeds Evictor.addBack/moveBack/remove and which
            // evictBatch drains; processTarget resolves each IN's owning DB.
            if let Ok(mut tree_guard) = tree_arc.write() {
                tree_guard.set_in_list_listener(Arc::clone(&self.evictor)
                    as Arc<dyn noxu_tree::InListListener>);
                // EV-14: wire the log manager so this DB's evicted root IN can
                // be re-fetched from its persisted LSN on next access.
                if let Some(ref lm) = self.log_manager {
                    tree_guard.set_log_manager(Arc::clone(lm));
                }
            }
            // SHARED_CACHE: a shared evictor's primary slot must NOT be
            // overwritten per-env (it would dangle when that env closes and
            // races with other envs).  Instead mirror this DB's tree into the
            // process-global shared registry so the shared LRU spans it; the
            // env's SharedEvictorHandle::deregister removes it on close.
            // A PRIVATE env keeps the existing single-slot fast path.
            if let Some(h) = &self.shared_evictor_handle {
                h.register_tree(Arc::clone(&tree_arc));
            } else {
                self.evictor.set_tree(tree_arc, db_id.id() as u64);
            }
        }

        self.db_map.write().insert(db_id, db.clone());

        if let Some(txn_id) = creating_txn_id {
            if recovered_db_id.is_none() {
                // C-4 / JE 1-I fix + C-6: defer name_map insertion until commit.
                // The name is visible within the creating txn (via db_map) but
                // not to other callers of get_database_names() until committed.
                self.pending_names.write().insert(name.to_string(), db_id);
                // C-6: write NameLNTxn (Provisional::Yes) inside the creating
                // transaction so crash recovery can undo it if the transaction
                // aborts or the process crashes before commit.
                if let Some(lm) = &self.log_manager {
                    let _ = Self::log_name_ln_txn(
                        lm,
                        name,
                        db_id.id() as u64,
                        txn_id,
                        config
                            .btree_comparator
                            .as_ref()
                            .map(|c| c.identity.as_str()),
                        config
                            .duplicate_comparator
                            .as_ref()
                            .map(|c| c.identity.as_str()),
                    );
                }
            }
            // recovered_db_id.is_some(): db already in name_map from recovery;
            // name_map insertion is a no-op and no new WAL entry is needed.
        } else {
            // Non-transactional path (or reopening a recovered db): insert
            // into name_map immediately, as before.
            self.name_map.write().insert(name.to_string(), db_id);

            // Persist the name → id mapping to the WAL (only for new
            // creations and writable environments) so that subsequent
            // reopens can reconstruct name_map.
            if recovered_db_id.is_none()
                && let Some(lm) = &self.log_manager
            {
                let _ = Self::log_name_ln(
                    lm,
                    name,
                    db_id.id() as u64,
                    config
                        .btree_comparator
                        .as_ref()
                        .map(|c| c.identity.as_str()),
                    config
                        .duplicate_comparator
                        .as_ref()
                        .map(|c| c.identity.as_str()),
                );
            }
        }

        Ok(db)
    }

    /// Called when the transaction that created `name` commits.
    ///
    /// Moves the name from `pending_names` to `name_map` atomically under the
    /// `pending_names` write lock, eliminating the TOCTOU window described
    /// in re-audit-keith R-4: there is no gap during which the name is absent
    /// from both maps.  Also uses the O(1) stored `DatabaseId` rather than an
    /// O(N) `db_map` linear scan.
    ///
    /// No-op if the name is not in `pending_names` (safe to call idempotently).
    pub fn commit_pending_database(&self, name: &str) {
        // Hold the pending_names write lock across the name_map insert so
        // that a concurrent open_database cannot observe the name missing
        // from both maps (R-4 TOCTOU fix).
        let mut pending = self.pending_names.write();
        let db_id = match pending.remove(name) {
            Some(id) => id,
            None => return, // not a pending transactional creation
        };
        // C-6: do NOT write a second NameLN here.  The NameLNTxn was
        // already written inside the transaction; the TxnCommit record
        // (written by Transaction::commit) is the durability marker.
        self.name_map.write().insert(name.to_string(), db_id);
        // Release pending write lock only after name_map is updated.
        drop(pending);
    }

    /// Called when the transaction that created `name` aborts.
    ///
    /// Removes the name from `pending_names` and removes the corresponding
    /// `DatabaseImpl` from `db_map` using the O(1) stored `DatabaseId`
    /// (eliminates the former O(N) `db_map` linear scan — R-4 fix).
    /// No-op if the name is not pending.
    pub fn abort_pending_database(&self, name: &str) {
        let db_id = {
            let mut pending = self.pending_names.write();
            match pending.remove(name) {
                Some(id) => id,
                None => return, // not a pending transactional creation
            }
        };
        self.db_map.write().remove(&db_id);
    }

    /// Write a transactional NameLNTxn entry to the WAL mapping `name` → `db_id`.
    ///
    /// Written **inside** the creating transaction with `Provisional::Yes`
    /// so that crash recovery treats it as uncommitted until a `TxnCommit`
    /// is seen.  `txn_id` must be the ID of the creating transaction.
    ///
    /// Format: `LnLogEntry` with key=name bytes, data=8-byte LE db_id,
    /// `txn_id=Some(txn_id as i64)`, `LogEntryType::NameLNTxn`.
    fn log_name_ln_txn(
        lm: &Arc<LogManager>,
        name: &str,
        db_id: u64,
        txn_id: u64,
        btree_comparator_id: Option<&str>,
        dup_comparator_id: Option<&str>,
    ) -> Result<(), DbiError> {
        let key = name.as_bytes().to_vec();
        let mut data = db_id.to_le_bytes().to_vec();
        // DBI-14: append the persisted comparator identities (empty for
        // byte-ordered databases, preserving the pre-DBI-14 wire format).
        data.extend_from_slice(&crate::name_ln_codec::encode_comparator_ids(
            btree_comparator_id,
            dup_comparator_id,
        ));
        let entry = LnLogEntry::new(
            0,                   // db_id header field (unused for NameLN)
            Some(txn_id as i64), // txn_id: creating transaction
            NULL_LSN,
            false,
            None,
            None,
            noxu_util::vlsn::NULL_VLSN,
            0,
            false,
            key,
            Some(data),
            0,
            noxu_util::vlsn::NULL_VLSN,
        );
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        lm.log(
            LogEntryType::NameLNTxn,
            &buf,
            Provisional::Yes,
            false, // flush: lazy
            false, // fsync: lazy
        )
        .map(|_| ())
        .map_err(DbiError::from)
    }

    /// Write a NameLN entry to the WAL mapping `name` → `db_id`.
    ///
    /// Format: `LnLogEntry` with key=name bytes, data=8-byte LE db_id.
    /// Uses `LogEntryType::NameLN` (non-transactional).
    fn log_name_ln(
        lm: &Arc<LogManager>,
        name: &str,
        db_id: u64,
        btree_comparator_id: Option<&str>,
        dup_comparator_id: Option<&str>,
    ) -> Result<(), DbiError> {
        let key = name.as_bytes().to_vec();
        let mut data = db_id.to_le_bytes().to_vec();
        // DBI-14: append the persisted comparator identities.
        data.extend_from_slice(&crate::name_ln_codec::encode_comparator_ids(
            btree_comparator_id,
            dup_comparator_id,
        ));
        let entry = LnLogEntry::new(
            0,    // db_id header field (unused for NameLN, use 0)
            None, // txn_id: non-transactional
            NULL_LSN,
            false,
            None,
            None,
            noxu_util::vlsn::NULL_VLSN,
            0,
            false,
            key,
            Some(data),
            0,
            noxu_util::vlsn::NULL_VLSN,
        );
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        lm.log(
            LogEntryType::NameLN,
            &buf,
            Provisional::No,
            false, // flush: lazy
            false, // fsync: lazy
        )
        .map(|_| ())
        .map_err(DbiError::from)
    }

    /// Returns the `Arc<RwLock<DatabaseImpl>>` for `db_id`, or `None` if not found.
    ///
    /// Used by `Transaction::abort()` to look up each modified database for
    /// undo application.
    ///
    /// called from
    /// `Txn.undoLNs()`.
    pub fn get_database_by_id(
        &self,
        db_id: DatabaseId,
    ) -> Option<Arc<RwLock<DatabaseImpl>>> {
        self.db_map.read().get(&db_id).cloned()
    }

    /// Returns all open `DatabaseImpl` arcs for iteration (e.g., verification).
    ///
    /// The returned `Vec` holds cloned `Arc`s so the db_map lock is released
    /// before the caller accesses individual databases.
    pub fn get_all_database_impls(&self) -> Vec<Arc<RwLock<DatabaseImpl>>> {
        self.db_map.read().values().cloned().collect()
    }

    /// Closes a database handle.
    pub fn close_database(&self, db_id: DatabaseId) -> Result<(), DbiError> {
        if let Some(db) = self.db_map.read().get(&db_id) {
            db.read().decrement_reference_count();
            if db.read().reference_count() <= 0 {
                // Could remove from maps, but keep for now
            }
        }
        Ok(())
    }

    /// Removes (deletes) a database by name.
    ///
    /// Returns an error if any open
    /// handles exist for the database (reference_count > 0).
    pub fn remove_database(&self, name: &str) -> Result<(), DbiError> {
        self.check_open()?;

        let db_id = self
            .name_map
            .read()
            .get(name)
            .copied()
            .ok_or_else(|| DbiError::DatabaseNotFound(name.to_string()))?;

        // "must not have any open Database handles" — enforce here.
        if let Some(db) = self.db_map.read().get(&db_id)
            && db.read().reference_count() > 0
        {
            return Err(DbiError::DatabaseInUse(name.to_string()));
        }

        self.name_map.write().remove(name);
        if let Some(db) = self.db_map.write().remove(&db_id) {
            db.write().start_delete();
            db.write().finish_delete();
        }

        Ok(())
    }

    /// Renames a database.
    ///
    /// Returns an error if any open
    /// handles exist for the database (reference_count > 0).
    pub fn rename_database(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), DbiError> {
        self.check_open()?;

        let db_id =
            self.name_map.read().get(old_name).copied().ok_or_else(|| {
                DbiError::DatabaseNotFound(old_name.to_string())
            })?;

        // "must not have any open Database handles" — enforce here.
        if let Some(db) = self.db_map.read().get(&db_id)
            && db.read().reference_count() > 0
        {
            return Err(DbiError::DatabaseInUse(old_name.to_string()));
        }

        if self.name_map.read().contains_key(new_name) {
            return Err(DbiError::DatabaseAlreadyExists(new_name.to_string()));
        }

        self.name_map.write().remove(old_name);
        self.name_map.write().insert(new_name.to_string(), db_id);

        // In a full implementation, would log the rename

        Ok(())
    }

    /// Truncates a database: removes all records while keeping the database
    /// registered and any open handles valid.
    ///
    /// Returns the number of records that were in the database before truncation.
    ///
    /// Mirrors `Environment.truncateDatabase(txn, dbName, returnCount)`.
    pub fn truncate_database(&self, name: &str) -> Result<u64, DbiError> {
        self.check_open()?;

        let db_id = self
            .name_map
            .read()
            .get(name)
            .copied()
            .ok_or_else(|| DbiError::DatabaseNotFound(name.to_string()))?;

        // Audit database F12 (Wave 2C-4): match `remove_database` /
        // `rename_database` and reject truncate when any open
        // `Database` handle still exists.  Pre-fix the tree was
        // replaced underneath live cursors, leaving them positioned
        // on a now-unreachable BIN.
        if let Some(db) = self.db_map.read().get(&db_id)
            && db.read().reference_count() > 0
        {
            return Err(DbiError::DatabaseInUse(name.to_string()));
        }

        let count = {
            let db_map_guard = self.db_map.read();
            let db_arc = db_map_guard
                .get(&db_id)
                .ok_or_else(|| DbiError::DatabaseNotFound(name.to_string()))?;
            let mut db_guard = db_arc.write();
            let old_count = db_guard.entry_count();

            // Write DeleteLN entries to the WAL for every key in the
            // database before clearing the tree.  On recovery, these
            // non-transactional deletes are replayed after the original
            // committed inserts, resulting in an empty tree.  Without
            // this WAL fence the truncation is invisible after reopen.
            if let Some(lm) = &self.log_manager
                && let Some(tree) = db_guard.get_real_tree()
            {
                let all_nodes = tree.rebuild_in_list();
                for node_arc in all_nodes {
                    let node_guard = node_arc.read();
                    if let noxu_tree::tree::TreeNode::Bottom(bin) = &*node_guard
                    {
                        for idx in 0..bin.entries.len() {
                            if let Some(full_key) = bin.get_full_key(idx)
                                && !full_key.is_empty()
                            {
                                let _ = Self::log_delete_ln(
                                    lm,
                                    db_id.id() as u64,
                                    &full_key,
                                );
                            }
                        }
                    }
                }
            }

            // Replace the real tree with a fresh empty tree, preserving config.
            let max_entries = db_guard.max_tree_entries_per_node() as usize;
            let new_tree =
                noxu_tree::Tree::new(db_id.as_i64() as u64, max_entries);
            db_guard.set_recovered_tree(new_tree); // resets entry_count to 0
            // Audit database F13 (Wave 2C-4): re-wire the per-env
            // memory counter onto the fresh tree so subsequent inserts
            // continue to update the Arbiter / cleaner budget.
            // `set_recovered_tree` does not preserve the counter wiring.
            db_guard.set_memory_counter(Arc::clone(&self.cache_usage));
            // F1: `set_recovered_tree` also drops the evictor listener wiring;
            // re-install it on the fresh tree (and re-point the evictor walk)
            // so eviction keeps working after a truncate.
            if let Some(tree_arc) = db_guard.get_real_tree_arc() {
                if let Ok(mut reg) = self.db_trees_registry.lock() {
                    reg.insert(db_id.as_i64(), Arc::clone(&tree_arc));
                }
                if let Ok(mut tg) = tree_arc.write() {
                    tg.set_in_list_listener(Arc::clone(&self.evictor)
                        as Arc<dyn noxu_tree::InListListener>);
                }
                self.evictor.set_tree(tree_arc, db_id.as_i64() as u64);
            }
            old_count
        };

        Ok(count)
    }

    /// Write a non-transactional `DeleteLN` entry to the WAL.
    fn log_delete_ln(
        lm: &Arc<LogManager>,
        db_id: u64,
        key: &[u8],
    ) -> Result<(), DbiError> {
        use noxu_util::vlsn::NULL_VLSN;
        let entry = LnLogEntry::new(
            db_id,
            None,     // txn_id: non-transactional
            NULL_LSN, // abort_lsn
            false,
            None,
            None,
            NULL_VLSN,
            0,
            false,
            key.to_vec(),
            None, // data = None → DeleteLN
            0,
            NULL_VLSN,
        );
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        lm.log(LogEntryType::DeleteLN, &buf, Provisional::No, false, false)
            .map(|_| ())
            .map_err(DbiError::from)
    }

    /// Returns the list of committed database names.
    ///
    /// Only databases whose creating transaction has committed are returned
    /// (C-4 / JE 1-J fix).  Databases created inside an active transaction
    /// are held in `pending_names` and excluded until the transaction commits.
    pub fn get_database_names(&self) -> Vec<String> {
        self.name_map.read().keys().cloned().collect()
    }

    // Transaction operations

    /// Begins a new transaction.
    pub fn begin_txn(&self) -> Result<Txn, DbiError> {
        self.check_open()?;
        Ok(self.txn_manager.begin_txn())
    }

    /// Returns a reference to the lock manager.
    pub fn get_lock_manager(&self) -> &Arc<LockManager> {
        &self.lock_manager
    }

    /// Returns a reference to the txn manager.
    pub fn get_txn_manager(&self) -> &Arc<TxnManager> {
        &self.txn_manager
    }

    /// Returns the utilization tracker shared with the LogManager observer.
    ///
    ///
    pub fn get_utilization_tracker(
        &self,
    ) -> Option<&Arc<NoxuMutex<UtilizationTracker>>> {
        self.utilization_tracker.as_ref()
    }

    /// Returns a reference to the node sequence generator.
    pub fn get_node_sequence(&self) -> &NodeSequence {
        &self.node_sequence
    }

    /// Returns the next database ID that would be allocated (without
    /// allocating it).  Used by tests to assert that post-recovery allocation
    /// never reuses a db-id present in the recovered log.
    pub fn peek_next_db_id(&self) -> i64 {
        self.next_db_id.load(Ordering::Relaxed)
    }

    /// Returns a clone of the shared LogManager, if any.
    ///
    /// Returns `None` for read-only environments.
    pub fn get_log_manager(&self) -> Option<Arc<LogManager>> {
        self.log_manager.clone()
    }

    /// Returns the shared disk-limit tracker (JE: cursors read the env's
    /// cached `getDiskLimitViolation()`).  `Database` caches this Arc so the
    /// write path can check the violation flag without locking the env.
    pub fn get_disk_limit(&self) -> Arc<crate::disk_limit::DiskLimitTracker> {
        Arc::clone(&self.disk_limit)
    }

    /// `DOS_PRODUCER_QUEUE_TIMEOUT` in milliseconds (JE
    /// `EnvironmentParams.DOS_PRODUCER_QUEUE_TIMEOUT`): the max time a
    /// DiskOrderedScan producer blocks trying to enqueue an item before it
    /// fails the scan.
    pub fn get_dos_producer_queue_timeout_ms(&self) -> u64 {
        self.dos_producer_queue_timeout_ms
    }

    /// Recomputes the cached disk-limit violation state (JE:
    /// `Cleaner.freshenLogSizeStats`).  Called after the cleaner/checkpointer
    /// may have changed total log size or free space, so writes can resume as
    /// soon as space is reclaimed.  Cheap no-op when enforcement is disabled.
    pub fn refresh_disk_limit(&self) {
        self.disk_limit.refresh();
    }

    /// REP-7: return the live in-memory B-tree for `db_id`, if the database
    /// has been opened on this (replica) node.
    ///
    /// The returned `Arc<RwLock<Tree>>` is the SAME tree that opened
    /// `Database`/`Cursor` handles read through (it is the entry the
    /// `db_trees_registry` holds, which `open_database_inner` installs from
    /// the database's `get_real_tree_arc()`).  The replica live-apply path
    /// ([`crate::replica_replay::ReplicaReplay`]) write-locks this tree and
    /// applies streamed committed LNs to it, so a read on the replica sees
    /// the replicated data without a restart.
    ///
    /// JE: the replica's `Replay.applyLN` resolves the target tree via
    /// `repNode.getReplica().getDbCache().get(dbId, repTxn)` and applies
    /// through a `Cursor` on it; here the tree IS the cursor-backing tree.
    pub fn replica_tree_for_db(
        &self,
        db_id: u64,
    ) -> Option<Arc<std::sync::RwLock<noxu_tree::Tree>>> {
        self.db_trees_registry.lock().ok()?.get(&(db_id as i64)).cloned()
    }

    /// Wave 3-2: Returns the list of XA in-doubt prepared transactions
    /// surfaced by the most recent recovery pass.
    ///
    /// Each entry holds the txn id, the first/last LSN logged by the
    /// transaction, the prepare-frame LSN, and the encoded XID
    /// components.  The XA layer (`noxu_xa::XaEnvironment::xa_recover`)
    /// reads this list to populate its return value and to seed the
    /// recovered-branches map so that `xa_commit(xid)` /
    /// `xa_rollback(xid)` can resolve the in-doubt transaction.
    ///
    /// Calling this method does NOT clear the list — it returns clones
    /// so multiple `xa_recover()` calls (e.g. across re-scans) all see
    /// the same set until
    /// [`Self::take_recovered_prepared_lns`] is called as part of
    /// resolution.
    pub fn recovered_prepared_txns(
        &self,
    ) -> Vec<noxu_recovery::PreparedTxnInfo> {
        self.recovered_prepared_txns.lock().unwrap().clone()
    }

    /// Wave 3-2: Removes and returns the LN replay list for a prepared
    /// transaction.
    ///
    /// Called by `xa_commit(xid)` after locating the txn id from
    /// [`Self::recovered_prepared_txns`].  The XA layer iterates the
    /// returned list and applies each LN to the in-memory tree, then
    /// writes a `TxnCommit` WAL frame.
    ///
    /// Returns an empty `Vec` if the txn id is not in the recovered
    /// set (e.g. it was already resolved in this process, or it was
    /// never prepared).
    pub fn take_recovered_prepared_lns(
        &self,
        txn_id: u64,
    ) -> Vec<noxu_recovery::PreparedLnReplay> {
        self.recovered_prepared_lns
            .lock()
            .unwrap()
            .remove(&txn_id)
            .unwrap_or_default()
    }

    /// Wave 3-2: Removes a recovered prepared txn entry from the
    /// EnvironmentImpl after the XA layer has successfully resolved it.
    ///
    /// After this call, [`Self::recovered_prepared_txns`] no longer
    /// includes this txn id.  Idempotent.
    pub fn forget_recovered_prepared_txn(&self, txn_id: u64) {
        self.recovered_prepared_txns
            .lock()
            .unwrap()
            .retain(|info| info.txn_id != txn_id);
        self.recovered_prepared_lns.lock().unwrap().remove(&txn_id);
    }

    /// Returns a clone of the shared Evictor.
    pub fn get_evictor(&self) -> Arc<Evictor> {
        Arc::clone(&self.evictor)
    }

    /// X-12: Returns the Arbiter's configured maximum memory budget in bytes.
    ///
    /// Under the total-budget model the Arbiter budget equals
    /// `cache_size - log_buf_total - off_heap_reserved`.
    /// Used in tests to assert the allocation formula is correct.
    pub fn get_arbiter_max_memory(&self) -> i64 {
        self.evictor.get_arbiter().get_max_memory()
    }

    /// Returns the current shared cache-usage counter (bytes), the value the
    /// Arbiter reads to decide whether to evict.  Inserts `fetch_add` to it;
    /// eviction `fetch_sub`s from it (F2).  Used by tests to assert eviction
    /// actually drives usage down.
    pub fn get_cache_usage(&self) -> i64 {
        self.evictor.get_arbiter().get_cache_usage()
    }

    /// The per-category memory budget (DBI-20/21).  Tree category is shared
    /// with the arbiter's `cache_usage`; lock / txn / admin categories can be
    /// fed by their owning subsystems so the over-budget view sees total
    /// memory.  JE `EnvironmentImpl.getMemoryBudget()`.
    pub fn get_memory_budget(
        &self,
    ) -> &Arc<crate::memory_budget::MemoryBudget> {
        &self.memory_budget
    }
    ///
    /// Returns `None` for read-only environments.
    pub fn get_cleaner(&self) -> Option<Arc<Cleaner>> {
        self.cleaner.as_ref().map(Arc::clone)
    }

    /// Returns the `CleanerThrottle` from the active cleaner, if one exists.
    ///
    /// Used by the write path to apply backpressure when the log write rate
    /// significantly exceeds the cleaner's capacity.
    pub fn get_cleaner_throttle(
        &self,
    ) -> Option<Arc<noxu_cleaner::CleanerThrottle>> {
        self.cleaner.as_ref().map(|c| Arc::clone(&c.throttle))
    }

    /// Returns the cleaner's shared `FileProtector`, if a cleaner exists.
    ///
    /// A `DiskOrderedCursor` producer uses this to protect the log files it
    /// scans from being deleted by the cleaner mid-scan (CLN-7).  Faithful to
    /// JE `DiskOrderedScanner.scan` calling
    /// `env.getFileProtector().protectActiveFiles(...)`
    /// (DiskOrderedScanner.java:704) before walking the log.
    pub fn get_file_protector(
        &self,
    ) -> Option<Arc<noxu_cleaner::FileProtector>> {
        self.cleaner.as_ref().map(|c| Arc::clone(c.get_file_protector()))
    }

    /// Returns the checkpointer, if one was created.
    ///
    /// Returns `None` for read-only environments.
    pub fn get_checkpointer(
        &self,
    ) -> Option<Arc<noxu_recovery::checkpointer::Checkpointer>> {
        self.checkpointer.as_ref().map(Arc::clone)
    }

    /// Runs a manual checkpoint synchronously.
    ///
    /// Bridges `Environment::checkpoint()` in `noxu-db` without exposing
    /// `noxu_recovery` as a direct dependency of that crate.
    /// Returns `Ok(())` if there is no checkpointer (read-only / non-txn env).
    pub fn run_checkpoint(&self) -> Result<(), DbiError> {
        self.run_checkpoint_with_invoker("manual")
    }

    /// Variant of `run_checkpoint` that lets the caller supply a
    /// non-default invoker label for structured logs / observability.
    /// Used by `noxu-db::Environment::checkpoint` to thread the
    /// `CheckpointConfig` semantics through to the recovery layer
    /// (audit transaction-env F6, Wave 2C-4).
    pub fn run_checkpoint_with_invoker(
        &self,
        invoker: &str,
    ) -> Result<(), DbiError> {
        match &self.checkpointer {
            None => Ok(()),
            Some(ckpt) => {
                ckpt.do_checkpoint(invoker).map(|_| ()).map_err(|e| {
                    DbiError::EnvironmentFailure { reason: e.to_string() }
                })
            }
        }
    }

    /// Runs one pass of the log cleaner.
    ///
    /// Selects up to `n_files` least-utilized log files, processes them
    /// (migrating live LN entries via `SharedTreeLookup`), and deletes the
    /// cleaned files.
    ///
    /// Returns `Ok(CleanResult)` on success or `Err(String)` if the cleaner
    /// is already running, is shut down, or this is a read-only environment.
    ///
    /// / `Cleaner.doClean()`.
    pub fn run_cleaner(
        &self,
        n_files: u32,
        force: bool,
    ) -> Result<noxu_cleaner::CleanResult, DbiError> {
        match &self.cleaner {
            None => Err(DbiError::EnvironmentFailure {
                reason: "cleaner is not available (read-only environment)"
                    .to_string(),
            }),
            Some(cleaner) => cleaner
                .do_clean(n_files, force)
                .map_err(|e| DbiError::EnvironmentFailure { reason: e })
                .inspect(|_| {
                    // The cleaner deleted log files, freeing space.  Refresh
                    // the cached disk-limit state so writes resume as soon as
                    // we are back within the limits (JE: Cleaner.manageDiskUsage
                    // calls freshenLogSizeStats after deleting files).
                    self.disk_limit.refresh();
                }),
        }
    }

    /// Writes a TxnCommit entry to the WAL and flushes according to durability.
    ///
    /// - `SyncPolicy::Sync`        → fsync after writing (default, safest)
    /// - `SyncPolicy::WriteNoSync` → flush to OS buffers, no fsync
    /// - `SyncPolicy::NoSync`      → write to log buffer only, no flush
    ///
    /// → `LogManager.flushTo()` → `FileManager.syncLogEnd()`.
    pub fn log_txn_commit(
        &self,
        txn_id: i64,
        fsync: bool,
        flush: bool,
    ) -> Result<(), DbiError> {
        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(()), // read-only env: nothing to log
        };

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // C-C2b: when a VLSN counter is installed (replicated env), assign the
        // next VLSN and write a 22-byte VLSN-tagged header so that
        // EnvironmentLogScanner can discover and auto-feed this commit to
        // replicas.  Standalone envs take the else branch — byte-unchanged.
        let vlsn_opt = {
            let guard = self.replication_vlsn_counter.lock().unwrap();
            guard.as_ref().map(|c| {
                c.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1
            })
        };

        if let Some(vlsn) = vlsn_opt {
            use noxu_util::vlsn::Vlsn;
            let entry = TxnEndEntry::new_commit(
                txn_id,
                NULL_LSN,
                timestamp,
                0,
                Vlsn::new(vlsn as i64),
            );
            let mut buf = BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            lm.log_with_vlsn(LogEntryType::TxnCommit, &buf, vlsn, flush, fsync)
                .map(|_| ())
                .map_err(DbiError::from)
        } else {
            let entry = TxnEndEntry::new_commit(
                txn_id, NULL_LSN, timestamp, 0, NULL_VLSN,
            );
            let mut buf = BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, flush, fsync)
                .map(|_| ())
                .map_err(DbiError::from)
        }
    }

    /// Writes a TxnAbort entry to the WAL (no fsync needed on abort).
    ///
    /// → log abort entry.
    pub fn log_txn_abort(&self, txn_id: i64) -> Result<(), DbiError> {
        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(()),
        };

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry =
            TxnEndEntry::new_abort(txn_id, NULL_LSN, timestamp, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);

        lm.log(LogEntryType::TxnAbort, &buf, Provisional::No, false, false)
            .map(|_| ())
            .map_err(DbiError::from)
    }

    /// Returns the number of active transactions.
    pub fn n_active_txns(&self) -> usize {
        self.txn_manager.n_active_txns()
    }

    /// Synchronously compress all BINs that have known-deleted slots.
    ///
    /// Mirrors `Environment.compress()` in JE (`Environment.java:1887`).
    /// Iterates every open database and calls `Tree::compress_bin` on each
    /// BIN that has at least one known-deleted slot, exactly as the
    /// background INCompressor daemon would.  Useful in tests to drain the
    /// compression queue before taking a checkpoint, and in applications
    /// that want deterministic memory reclamation after bulk deletes.
    ///
    /// Returns the total number of BINs that were compressed.
    pub fn compress_all(&self) -> usize {
        let mut total = 0usize;
        let db_list: Vec<Arc<RwLock<DatabaseImpl>>> =
            self.db_map.read().values().cloned().collect();
        for db_arc in db_list {
            let db = db_arc.read();
            if let Some(tree) = db.get_real_tree() {
                let bins = tree.collect_bins_with_known_deleted();
                total += bins.len();
                // IC-3: skip slots still write-locked by an in-flight txn
                // (JE BIN.compress isLockUncontended check).  See the
                // INCompressor daemon above for the lock-ordering rationale.
                let lm = &self.lock_manager;
                let is_locked = move |lsn: u64| lm.get_lock_info(lsn) != (0, 0);
                for bin_arc in bins {
                    tree.compress_bin_with_lock_check(
                        &bin_arc,
                        Some(&is_locked),
                    );
                }
            }
        }
        total
    }

    /// Explicitly trigger the cache evictor to free memory.
    ///
    /// Mirrors `Environment.evictMemory()` in JE (`Environment.java:1860`).
    /// Forwards the request to the `Evictor` / `Arbiter` subsystem.  The
    /// evictor selects and evicts nodes from the in-memory B+tree cache
    /// down toward the configured cache size, then returns the number of
    /// bytes freed.  Useful when the application has just completed a bulk
    /// load and wants to reclaim memory proactively.
    ///
    /// Returns the number of cache bytes evicted (0 if nothing was evicted
    /// or no cache is active).
    pub fn evict_memory(&self) -> usize {
        self.evictor.do_evict(EvictionSource::Manual).bytes_evicted as usize
    }

    /// The eviction algorithm name actually in effect (primary policy slot).
    /// Used to verify `EVICTOR_ALGORITHM` wiring took effect at runtime.
    pub fn evictor_algorithm_name(&self) -> &'static str {
        self.evictor.primary_algorithm_name()
    }

    /// EV-15: per-operation synchronous critical eviction (write back-pressure).
    ///
    /// JE `EnvironmentImpl.criticalEviction` (EnvironmentImpl.java:3012) is
    /// called from application threads before every cursor operation; it
    /// forwards to `Evictor.doCriticalEviction`, which makes the calling
    /// thread itself evict when the cache is *critically* over budget so a
    /// writer filling the cache blocks before continuing.  Returns bytes
    /// evicted (0 when no critical eviction was needed).
    pub fn critical_eviction(&self) -> u64 {
        self.evictor.do_critical_eviction()
    }

    /// Returns the number of open databases.
    pub fn n_databases(&self) -> usize {
        self.db_map.read().len()
    }

    /// Aggregates throughput statistics across all open databases.
    pub fn get_throughput_snapshot(&self) -> ThroughputStatsSnapshot {
        let mut agg = ThroughputStatsSnapshot::default();
        for db in self.db_map.read().values() {
            let snap = db.read().throughput.snapshot();
            agg.add(&snap);
        }
        agg
    }

    /// Closes the environment.
    pub fn close(&self) -> Result<(), DbiError> {
        let mut state = self.state.write();
        if state.is_closed() {
            return Ok(());
        }
        *state = EnvState::Closing;

        // F12: daemon shutdown ORDER faithful to JE `EnvironmentImpl.close()`
        // (EnvironmentImpl.java:1873 `requestShutdownDaemons()` → final
        // checkpoint → :1915 `shutdownDaemons()`).
        //
        // Phase 1 — REQUEST shutdown of the non-flush daemons before the final
        // checkpoint.  JE comment (EnvironmentImpl.java:1870-1872): "Begin
        // shutdown of the daemons before checkpointing.  Cleaning during the
        // checkpoint is wasted and slows down the checkpoint."  The evictor and
        // checkpointer stay ALIVE across the final checkpoint so its dirty-node
        // flushes still happen.
        self.in_compressor_shutdown.shutdown();
        self.cleaner_shutdown.shutdown();
        self.log_flush_no_sync_shutdown.shutdown();

        // Final (forced) checkpoint before WAL sync so recovery can restart
        // from the checkpoint rather than replaying the full log.
        // `EnvironmentImpl.close()` calling
        // `checkpointer.doCheckpoint(CheckpointConfig.FORCE)` — run while the
        // evictor is still alive so any dirty BIN it holds is flushed (F12).
        if let Some(ckpt) = &self.checkpointer {
            let _ = ckpt.do_checkpoint("close");
        }

        // Phase 2 — JOIN daemons in JE `shutdownDaemons()` order
        // (EnvironmentImpl.java:2328-2374):
        //   inCompressor → cleaner → checkpointer → evictor → logFlusher.
        // "Cleaner has to be shutdown before checkpointer because former calls
        // the latter" and "The evictors have to be shutdown last because the
        // other daemons might create changes to the memory usage which result
        // in a notify to eviction."

        // inCompressor (already signalled above).
        if let Some(handle) = self.in_compressor_handle.lock().unwrap().take() {
            let _ = handle.join();
        }

        // cleaner — joined before the checkpointer because the cleaner may
        // request a checkpoint.
        if let Some(handle) = self.cleaner_handle.lock().unwrap().take() {
            let _ = handle.join();
        }

        // checkpointer — its final flush must complete before the evictor
        // stops.
        if let Some(ckpt) = &self.checkpointer {
            ckpt.request_shutdown();
        }
        if let Some(handle) = self.checkpointer_handle.lock().unwrap().take() {
            let _ = handle.join();
        }

        // evictor LAST so final dirty-node flushes still happen.
        //
        // SHARED_CACHE: for a shared env we must NOT call
        // `self.evictor.shutdown()` — that would stop the process-global
        // shared daemon and break every OTHER sharing env.  Instead we
        // deregister THIS env's trees from the shared LRU (before its trees
        // drop — no dangling trees) and let the shared handle tear the daemon
        // down only when the last member leaves.  A private env owns its
        // evictor + daemon, so it shuts down as before.
        if let Some(h) = &self.shared_evictor_handle {
            h.deregister();
        } else {
            self.evictor.shutdown();
        }
        if let Some(handle) = self.evictor_handle.lock().unwrap().take() {
            // Best-effort join: ignore a panic in the evictor thread.
            let _ = handle.join();
        }
        // EVICTOR-LOG-1 teardown: drop the private evictor's `Arc<LogManager>`
        // now that its daemon is joined, breaking the
        // `Tree -> Arc<dyn InListListener>(=Evictor) -> Arc<LogManager> ->
        // FileManager` chain so the on-disk env lock is released even if a
        // Tree `Arc` outlives this struct.  Skipped for a shared evictor (it
        // outlives this env and serves others).
        if self.shared_evictor_handle.is_none() {
            self.evictor.clear_log_manager();
        }

        // X-11: log-flush-no-sync daemon (already signalled above), joined last
        // like JE's `logFlusher.shutdown()`.
        if let Some(handle) =
            self.log_flush_no_sync_handle.lock().unwrap().take()
        {
            let _ = handle.join();
        }

        // Flush and fsync the WAL so no buffered data is lost on close.
        if let Some(lm) = &self.log_manager {
            // Best-effort: ignore flush errors on close (env is shutting down).
            let _ = lm.flush_sync();
        }

        // Shut down the extended-fork background services.
        self.extinction_scanner.lock().unwrap().shutdown();
        self.data_eraser.lock().unwrap().shutdown();
        self.backup_manager.lock().unwrap().shutdown();

        *state = EnvState::Closed;
        Ok(())
    }

    // =========================================================================
    // extended fork: Record Extinction, Data Erasure, Auto-Backup
    // =========================================================================

    /// Schedules asynchronous removal of extinct records.
    ///
    /// Records in the specified key range are permanently removed from the
    /// B-tree without per-record delete log entries. The caller must ensure
    /// that the records will never be accessed again (see `ExtinctionFilter`).
    ///
    /// `Environment.discardExtinctRecords(Transaction, DatabaseImpl,
    ///   DatabaseEntry startKey, DatabaseEntry endKey, ScanFilter)` (extended fork).
    pub fn discard_extinct_records(
        &self,
        db_name: &str,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
    ) -> u64 {
        let task = noxu_cleaner::ExtinctionTask {
            db_name: db_name.to_string(),
            start_key,
            end_key,
            dups: false,
        };
        self.extinction_scanner.lock().unwrap().discard_extinct_records(task)
    }

    /// Returns `true` if an extinction scan is in progress.
    ///
    /// (extended fork).
    pub fn is_record_extinction_active(&self) -> bool {
        self.extinction_scanner.lock().unwrap().is_active()
    }

    /// Returns the total number of LN records discarded by extinction scans.
    ///
    /// (extended fork).
    pub fn n_lns_extinct(&self) -> u64 {
        self.extinction_scanner.lock().unwrap().n_lns_extinct()
    }

    /// Enqueues a disk region for physical data erasure.
    ///
    /// (extended fork).
    pub fn enqueue_erase(&self, request: noxu_cleaner::EraseRequest) {
        self.data_eraser.lock().unwrap().enqueue_erase(request);
    }
}

impl Drop for EnvironmentImpl {
    fn drop(&mut self) {
        // Poison-safe teardown: a panic elsewhere may have poisoned any of these
        // mutexes.  In Drop we MUST NOT unwrap() a poisoned lock — that turns a
        // recoverable poison into a double-panic and aborts the whole process.
        // Every lock on the teardown path recovers the guard with into_inner().
        //
        // F12: teardown join order faithful to JE `shutdownDaemons()`
        // (EnvironmentImpl.java:2328-2374): inCompressor → cleaner →
        // checkpointer → evictor.  Drop takes no final checkpoint (this is the
        // test/no-close teardown path), but the JOIN ORDER still matters:
        // "Cleaner has to be shutdown before checkpointer because former calls
        // the latter" and the evictor is joined last so any dirty node other
        // daemons touch on the way down can still be flushed.

        // Shut down the INCompressor daemon thread.
        self.in_compressor_shutdown.shutdown();
        if let Some(handle) = self
            .in_compressor_handle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let _ = handle.join();
        }

        // Shut down the cleaner daemon thread (before the checkpointer).
        self.cleaner_shutdown.shutdown();
        if let Some(handle) =
            self.cleaner_handle.lock().unwrap_or_else(|p| p.into_inner()).take()
        {
            let _ = handle.join();
        }

        // Shut down the checkpointer daemon thread.
        if let Some(ckpt) = &self.checkpointer {
            ckpt.request_shutdown();
        }
        if let Some(handle) = self
            .checkpointer_handle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let _ = handle.join();
        }

        // Shut down the evictor daemon LAST so its thread exits cleanly and any
        // final dirty node is still flushable while it is alive.
        //
        // SHARED_CACHE: deregister (not shutdown) so the shared daemon keeps
        // serving other envs; the handle's own Drop is a further safety net.
        if let Some(h) = &self.shared_evictor_handle {
            h.deregister();
        } else {
            self.evictor.shutdown();
        }
        if let Some(handle) =
            self.evictor_handle.lock().unwrap_or_else(|p| p.into_inner()).take()
        {
            let _ = handle.join();
        }
        // EVICTOR-LOG-1 teardown: drop the private evictor's `Arc<LogManager>`
        // (see the sibling in `close()`).  The daemon is joined above, so no
        // background flush can be in flight.
        if self.shared_evictor_handle.is_none() {
            self.evictor.clear_log_manager();
        }

        // Shut down the extended-fork background services.
        self.extinction_scanner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .shutdown();
        self.data_eraser.lock().unwrap_or_else(|p| p.into_inner()).shutdown();
        self.backup_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .shutdown();

        // EV-14 teardown: break the `Tree -> Arc<LogManager> -> FileManager`
        // chain so the FileManager's on-disk exclusive lock is released on
        // env drop, even if a Tree `Arc` momentarily outlives this struct.
        // Without this a reopened environment (e.g. recovery tests) fails with
        // "Environment locked".  Daemons are already joined above, so no
        // background thread can be mid-re-fetch.
        if let Ok(mut t) = self.primary_tree.write() {
            t.clear_log_manager();
        }
        if let Ok(reg) = self.db_trees_registry.lock() {
            for tree_arc in reg.values() {
                if let Ok(mut t) = tree_arc.write() {
                    t.clear_log_manager();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_env(read_only: bool) -> (TempDir, EnvironmentImpl) {
        let dir = TempDir::new().unwrap();
        let env = EnvironmentImpl::new(dir.path(), read_only, true).unwrap();
        (dir, env)
    }

    #[test]
    fn test_environment_creation() {
        let (dir, env) = make_env(false);
        assert_eq!(env.get_env_home(), dir.path());
        assert!(!env.is_read_only());
        assert!(env.is_transactional());
        assert!(env.is_open());
        assert!(env.is_valid());
        assert!(matches!(env.get_state(), EnvState::Open));
    }

    #[test]
    fn test_open_database_with_create() {
        let (_dir, env) = make_env(false);

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db = env.open_database("test_db", &config).unwrap();
        assert_eq!(db.read().get_name(), "test_db");
        assert_eq!(db.read().reference_count(), 1);
    }

    #[test]
    fn test_open_database_without_create() {
        let (_dir, env) = make_env(false);

        let config = DatabaseConfig::new();
        let result = env.open_database("test_db", &config);

        assert!(matches!(result, Err(DbiError::DatabaseNotFound(_))));
    }

    #[test]
    fn test_open_same_database_twice() {
        let (_dir, env) = make_env(false);

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db1 = env.open_database("test_db", &config).unwrap();
        let db2 = env.open_database("test_db", &config).unwrap();

        // Should return the same database
        assert_eq!(db1.read().get_id(), db2.read().get_id());
        assert_eq!(db1.read().reference_count(), 2);
    }

    #[test]
    fn test_remove_database() {
        let (_dir, env) = make_env(false);

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db = env.open_database("test_db", &config).unwrap();
        env.close_database(db.read().get_id()).unwrap();

        env.remove_database("test_db").unwrap();

        let result = env.open_database("test_db", &DatabaseConfig::new());
        assert!(matches!(result, Err(DbiError::DatabaseNotFound(_))));
    }

    #[test]
    fn test_rename_database() {
        let (_dir, env) = make_env(false);

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db = env.open_database("old_name", &config).unwrap();
        let db_id = db.read().get_id();
        env.close_database(db_id).unwrap();

        env.rename_database("old_name", "new_name").unwrap();

        // Old name should not exist
        let result = env.open_database("old_name", &DatabaseConfig::new());
        assert!(matches!(result, Err(DbiError::DatabaseNotFound(_))));

        // New name should exist and point to same database
        let db2 = env.open_database("new_name", &config).unwrap();
        assert_eq!(db2.read().get_id(), db_id);
    }

    #[test]
    fn test_get_database_names() {
        let (_dir, env) = make_env(false);

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        env.open_database("db1", &config).unwrap();
        env.open_database("db2", &config).unwrap();
        env.open_database("db3", &config).unwrap();

        let names = env.get_database_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"db1".to_string()));
        assert!(names.contains(&"db2".to_string()));
        assert!(names.contains(&"db3".to_string()));
    }

    #[test]
    fn test_begin_txn() {
        let (_dir, env) = make_env(false);
        let _txn = env.begin_txn().unwrap();
        assert_eq!(env.n_active_txns(), 1);
    }

    #[test]
    fn test_invalidate_environment() {
        let (_dir, env) = make_env(false);

        assert!(env.is_valid());

        env.invalidate(EnvironmentFailureReason::LogChecksum);

        assert!(!env.is_valid());
        assert!(matches!(env.get_state(), EnvState::Invalid));

        let result = env.begin_txn();
        assert!(matches!(result, Err(DbiError::EnvironmentFailure { .. })));
    }

    /// C-2 regression: an I/O failure reported by the LogManager must also
    /// cause `is_valid()` to return `false`, so that all subsequent commit
    /// attempts are rejected even before `EnvironmentImpl::invalidate()` is
    /// called explicitly.
    #[test]
    fn test_log_io_failure_invalidates_environment() {
        use std::sync::atomic::Ordering;
        let (_dir, env) = make_env(false);

        assert!(env.is_valid(), "env starts valid");

        // Directly flip the io_invalid flag on the underlying LogManager,
        // simulating what `flush_sync()` does on EIO.
        let lm =
            env.get_log_manager().expect("writable env must have log manager");
        lm.io_invalid.store(true, Ordering::Release);

        // is_valid() must now return false even though invalidate() was never called.
        assert!(
            !env.is_valid(),
            "is_valid() must return false after log I/O failure"
        );
    }

    #[test]
    fn test_close_environment() {
        let (_dir, env) = make_env(false);

        assert!(env.is_open());

        env.close().unwrap();

        assert!(!env.is_open());
        assert!(matches!(env.get_state(), EnvState::Closed));

        // Second close should be ok
        env.close().unwrap();
    }

    #[test]
    fn test_operations_on_closed_environment() {
        let (_dir, env) = make_env(false);

        env.close().unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let result = env.open_database("test_db", &config);
        assert!(matches!(result, Err(DbiError::EnvironmentNotOpen)));

        let result = env.begin_txn();
        assert!(matches!(result, Err(DbiError::EnvironmentNotOpen)));
    }

    #[test]
    fn test_read_only_mode() {
        let (_dir, env) = make_env(true);
        assert!(env.is_read_only());
    }

    #[test]
    fn test_multiple_databases_coexist() {
        let (_dir, env) = make_env(false);

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db1 = env.open_database("db1", &config).unwrap();
        let db2 = env.open_database("db2", &config).unwrap();
        let db3 = env.open_database("db3", &config).unwrap();

        assert_eq!(env.n_databases(), 3);
        assert_ne!(db1.read().get_id(), db2.read().get_id());
        assert_ne!(db2.read().get_id(), db3.read().get_id());
    }

    #[test]
    fn test_n_active_txns() {
        let (_dir, env) = make_env(false);

        assert_eq!(env.n_active_txns(), 0);

        let _txn1 = env.begin_txn().unwrap();
        assert_eq!(env.n_active_txns(), 1);

        let _txn2 = env.begin_txn().unwrap();
        assert_eq!(env.n_active_txns(), 2);
    }

    #[test]
    fn test_log_txn_commit() {
        let (_dir, env) = make_env(false);
        // Should succeed without error (fsync = true, flush = true)
        env.log_txn_commit(1, true, true).unwrap();
    }

    #[test]
    fn test_log_txn_abort() {
        let (_dir, env) = make_env(false);
        env.log_txn_abort(2).unwrap();
    }

    #[test]
    fn test_log_txn_commit_read_only_is_noop() {
        let (_dir, env) = make_env(true);
        // Read-only env has no log manager; commit is a no-op.
        env.log_txn_commit(3, true, true).unwrap();
    }

    /// P0 integration test: reopen an existing environment and verify
    /// that recovery runs successfully and the LSN state is restored.
    #[test]
    fn test_recovery_on_reopen() {
        let dir = TempDir::new().unwrap();

        // First open: write some commit entries and close cleanly.
        {
            let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();
            env.log_txn_commit(1, true, true).unwrap();
            env.log_txn_commit(2, true, true).unwrap();
            env.log_txn_abort(3).unwrap();
            env.close().unwrap();
        }

        // Second open: recovery should run over the existing log files.
        // If recovery fails (e.g. LSN state restored incorrectly), `new()`
        // will return an error.
        let env2 = EnvironmentImpl::new(dir.path(), false, true).unwrap();
        assert!(env2.is_open());

        // The log manager should be at a position *after* the entries
        // written in the first session (not overwriting them from offset 20).
        let lm = env2.get_log_manager().unwrap();
        let end_of_log = lm.get_end_of_log();
        assert!(
            end_of_log.file_offset()
                > noxu_log::file_header::FILE_HEADER_SIZE as u32,
            "end-of-log offset {:#x} should be past the file header ({})",
            end_of_log.file_offset(),
            noxu_log::file_header::FILE_HEADER_SIZE
        );
    }

    /// P0 safety test: writing after reopen must not overwrite existing data.
    #[test]
    fn test_write_after_reopen_does_not_overwrite() {
        let dir = TempDir::new().unwrap();

        // Session 1: write txn 100.
        {
            let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();
            env.log_txn_commit(100, true, true).unwrap();
            env.close().unwrap();
        }

        // Session 2: write txn 200 after recovery.
        {
            let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();
            env.log_txn_commit(200, true, true).unwrap();
            env.close().unwrap();
        }

        // Session 3: recover and scan; both txns should be visible.
        use crate::file_manager_scanner::FileManagerLogScanner;
        use noxu_log::FileManager;
        use noxu_recovery::{LogEntry, LogScanner};
        use std::sync::Arc;

        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (_, end) = scanner.find_end_of_log();
        let entries = scanner.scan_forward(noxu_util::NULL_LSN, end);

        let commit_txn_ids: Vec<u64> = entries
            .iter()
            .filter_map(|e| match &e.entry {
                LogEntry::TxnCommit(r) => Some(r.txn_id),
                _ => None,
            })
            .collect();

        assert!(
            commit_txn_ids.contains(&100),
            "txn 100 from session 1 must still be readable"
        );
        assert!(
            commit_txn_ids.contains(&200),
            "txn 200 from session 2 must be readable"
        );
    }

    /// P1b test: recovery with `tree=Some` runs without panic on reopen.
    ///
    /// Session 1: open, write txn commits, close.
    /// Session 2: reopen — recovery runs with a real tree (P1b wiring).
    /// The test passes if `new()` succeeds, proving the recovery path with
    /// `tree=Some` does not panic or return an error.
    #[test]
    fn test_recovery_replays_committed_ln() {
        let dir = TempDir::new().unwrap();

        // Session 1: write some entries and close cleanly.
        {
            let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();
            env.log_txn_commit(1, true, true).unwrap();
            env.log_txn_commit(2, true, true).unwrap();
            env.close().unwrap();
        }

        // Session 2: reopen — recovery runs with a real B-tree.
        // If tree replay panics or fails, new() returns Err here.
        {
            let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();
            // Database can be opened after recovery.
            let mut config = DatabaseConfig::new();
            config.set_allow_create(true);
            let _db = env.open_database("mydb", &config).unwrap();
            env.close().unwrap();
        }
        // Reaching here proves recovery with tree=Some succeeded.
    }

    /// P1b test: the real_tree in a freshly opened database has the correct
    /// database ID (matches the DatabaseId assigned by the environment).
    #[test]
    fn test_recovery_tree_initialized_with_correct_db_id() {
        let dir = TempDir::new().unwrap();
        let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);
        let db_arc = env.open_database("test", &config).unwrap();
        let db = db_arc.read();

        // The real_tree's database_id must equal db.get_id().id() as u64.
        if let Some(tree) = db.get_real_tree() {
            assert_eq!(
                tree.get_database_id(),
                db.get_id().id() as u64,
                "real_tree database_id must match the DatabaseId"
            );
        }
        drop(db);
        env.close().unwrap();
    }

    /// P3 smoke test: verify the evictor daemon starts and stops cleanly.
    ///
    /// Opens an environment, writes a commit entry so the daemon has a real
    /// environment to run against, then closes the environment.  The test
    /// passes as long as there is no panic — confirming the thread starts,
    /// runs at least one eviction pass, and is joined successfully on close.
    #[test]
    fn test_evictor_daemon_starts_and_stops() {
        let dir = TempDir::new().unwrap();
        let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();

        // Verify the evictor is accessible and not yet shut down.
        let evictor = env.get_evictor();
        assert!(!evictor.is_shutdown(), "evictor should be running after open");

        // Write some data so the daemon has a live env to run against.
        env.log_txn_commit(42, false, false).unwrap();

        // Give the daemon thread at least one sleep cycle to execute.
        std::thread::sleep(std::time::Duration::from_millis(250));

        // close() must signal shutdown, join the thread, and not panic.
        env.close().unwrap();

        // After close the evictor's shutdown flag must be set.
        assert!(
            evictor.is_shutdown(),
            "evictor should be shut down after close"
        );
    }

    /// Verify that the checkpointer daemon thread starts and stops cleanly.
    ///
    /// Uses a very short checkpoint interval (50 ms) so the daemon wakes up
    /// at least once before `close()`.  The test passes as long as no panic
    /// occurs and the checkpointer's shutdown flag is set after `close()`.
    #[test]
    fn test_checkpointer_daemon_starts_and_stops() {
        let dir = TempDir::new().unwrap();
        // Use a short interval so the daemon fires during the test.
        let env = EnvironmentImpl::new_with_config(
            dir.path(),
            false,
            true,
            50, // 50 ms interval
        )
        .unwrap();

        // Checkpointer should exist for a writable environment.
        assert!(
            env.checkpointer.is_some(),
            "checkpointer should be created for writable env"
        );

        // Write some data so there is a live log.
        env.log_txn_commit(1, false, false).unwrap();

        // Give the daemon at least two sleep cycles (100+ ms).
        std::thread::sleep(std::time::Duration::from_millis(200));

        // close() must signal shutdown, join the thread, and not panic.
        env.close().unwrap();

        // After close the shutdown flag must be set.
        let ckpt = env.checkpointer.as_ref().unwrap();
        assert!(
            ckpt.is_shutdown(),
            "checkpointer should be shut down after close"
        );
    }

    /// Verify that `wakeup_after_write` on the environment's Checkpointer
    /// triggers a checkpoint when the accumulated bytes exceed the threshold.
    ///
    /// We reach into the checkpointer directly to call `wakeup_after_write`
    /// with a tiny threshold (already configured on the Checkpointer via
    /// `with_bytes_interval`) and verify the checkpoint count increases.
    ///
    /// Note: the Checkpointer built by EnvironmentImpl uses the default
    /// 10 MiB threshold, so here we test the method on a standalone
    /// Checkpointer with a tiny threshold, which is the correct unit-level
    /// test for this behaviour (integration-level coverage is in
    /// `noxu-recovery`).
    #[test]
    fn test_wakeup_after_write_triggers_checkpoint_via_env() {
        use noxu_recovery::checkpointer::{CheckpointConfig, Checkpointer};
        use std::sync::atomic::Ordering;

        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_bytes_interval(64); // 64-byte threshold

        // Below threshold — no checkpoint.
        checkpointer.wakeup_after_write(32);
        assert_eq!(
            checkpointer.get_stats().checkpoints.load(Ordering::Relaxed),
            0
        );

        // Cross threshold — checkpoint fires.
        checkpointer.wakeup_after_write(32);
        assert_eq!(
            checkpointer.get_stats().checkpoints.load(Ordering::Relaxed),
            1,
            "checkpoint should fire when threshold is crossed"
        );
    }

    /// CLN-14: after env open, the engine must have wired the cleaner's
    /// checkpoint wakeup callback into the checkpointer, AND the wakeup must
    /// release the checkpointer daemon PROMPTLY (not after the full wakeup
    /// interval).  Together with the cleaner-side callback tests
    /// (noxu-cleaner) and the daemon-wake unit test (noxu-recovery), this
    /// proves cleaned files are deleted promptly when writes stop.
    ///
    /// JE: FileProcessor.doClean -> envImpl.getCheckpointer().wakeupAfterNoWrites().
    #[test]
    fn test_cln14_engine_wires_checkpoint_wakeup() {
        use std::time::{Duration, Instant};

        let dir = TempDir::new().unwrap();
        let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();

        // The engine must have wired the wakeup callback at open.
        let cleaner = env.get_cleaner().expect("writable env has a cleaner");
        assert!(
            cleaner.has_checkpoint_wakeup_fn(),
            "CLN-14: engine must wire the cleaner's checkpoint wakeup callback"
        );

        // The callback the cleaner fires targets the checkpointer's
        // `wakeup_after_no_writes`, which must release a daemon parked on the
        // sleep condvar well under a 60 s interval.  Exercise it through the
        // real checkpointer the env built.
        let ckpt =
            env.get_checkpointer().expect("writable env has a checkpointer");
        let ckpt_for_wait = std::sync::Arc::clone(&ckpt);
        let start = Instant::now();
        let handle = std::thread::spawn(move || {
            ckpt_for_wait.wait_for_shutdown_or_timeout(Duration::from_secs(60));
        });
        std::thread::sleep(Duration::from_millis(50));
        ckpt.wakeup_after_no_writes();

        handle.join().unwrap();
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "CLN-14: checkpointer daemon must wake promptly, not after 60 s"
        );

        env.close().unwrap();
    }
}
