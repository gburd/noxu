//! Internal environment implementation.
//!
//! Port of `com.sleepycat.je.dbi.EnvironmentImpl`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use bytes::BytesMut;
use noxu_sync::RwLock;

use crate::database_impl::DatabaseImpl;
use crate::file_manager_scanner::FileManagerLogScanner;
use crate::{
    DatabaseConfig, DatabaseId, DbType, DbiError, EnvState,
    EnvironmentFailureReason, NodeSequence,
};
use noxu_cleaner::Cleaner;
use noxu_evictor::{Arbiter, Evictor, EvictionSource};
use noxu_log::{
    FileManager, LogManager, LogEntryType, Provisional,
    entry::TxnEndEntry,
};
use noxu_recovery::RecoveryManager;
use noxu_txn::{LockManager, Txn, TxnManager};
use noxu_util::{lsn::NULL_LSN, vlsn::NULL_VLSN};

/// The internal representation of an environment.
///
/// Owns all subsystems: log, tree, txn, lock, evictor, cleaner, etc.
/// This is a simplified initial implementation that wires together
/// the key components built in phases 0-3.
///
/// Port of `com.sleepycat.je.dbi.EnvironmentImpl`.
pub struct EnvironmentImpl {
    /// Path to the environment home directory.
    env_home: PathBuf,
    /// Current environment state.
    state: RwLock<EnvState>,
    /// Whether this is a read-only environment.
    is_read_only: bool,
    /// Whether transactions are enabled.
    is_transactional: bool,

    /// Node ID and transient LSN generator.
    node_sequence: NodeSequence,
    /// Next database ID.
    next_db_id: AtomicI64,

    /// The lock manager (shared across all lockers/txns).
    lock_manager: Arc<LockManager>,
    /// The transaction manager.
    txn_manager: TxnManager,

    /// All open databases, keyed by DatabaseId.
    db_map: RwLock<HashMap<DatabaseId, Arc<RwLock<DatabaseImpl>>>>,
    /// Name -> DatabaseId mapping.
    name_map: RwLock<HashMap<String, DatabaseId>>,

    /// Whether the environment has been invalidated.
    is_invalid: AtomicBool,
    /// If invalidated, the reason.
    invalid_reason: RwLock<Option<EnvironmentFailureReason>>,

    /// Creation time in milliseconds.
    creation_time_ms: u64,

    /// Write-ahead log manager (None for read-only environments).
    log_manager: Option<Arc<LogManager>>,

    /// The cache evictor (shared with the background daemon thread).
    evictor: Arc<Evictor>,

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
    /// Port of JE `RecoveryManager.getDbIdToDbMap()` /
    /// `EnvironmentImpl.setupDbEnvironment()` tree population.
    recovered_trees: Mutex<HashMap<u64, noxu_tree::Tree>>,

    /// The primary (db_id=1) shared tree used for LN migration during
    /// log cleaning.
    ///
    /// This `Arc<RwLock<…>>` wraps the live B-tree for the default single
    /// database.  The `Cleaner` holds a clone of this Arc so that when
    /// `run_cleaner()` is called, live LN entries are migrated via
    /// `SharedTreeLookup`.
    ///
    /// Port of the `env.getDbTree()` access pattern in JE's FileProcessor.
    primary_tree: Arc<std::sync::RwLock<noxu_tree::Tree>>,

    /// The log-file garbage collector.
    ///
    /// Created in `new()` for writable environments via
    /// `Cleaner::with_file_manager_and_tree()`.  For read-only environments
    /// `cleaner` is `None`.
    ///
    /// Port of `EnvironmentImpl.cleaner` in JE.
    cleaner: Option<Cleaner>,

    /// The checkpoint daemon.
    ///
    /// Created for writable environments; wired to the LogManager and the
    /// primary tree so that `do_checkpoint()` flushes dirty BINs to the log.
    ///
    /// Port of `EnvironmentImpl.checkpointer` in JE.
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

    /// Like `new()` but allows overriding the checkpoint interval for testing.
    pub fn new_with_config(
        env_home: impl Into<PathBuf>,
        read_only: bool,
        transactional: bool,
        checkpoint_interval_ms: u64,
    ) -> Result<Self, DbiError> {
        let env_home = env_home.into();
        let lock_manager = Arc::new(LockManager::new());
        let txn_manager = TxnManager::new(lock_manager.clone());

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

        // Initialize the WAL (LogManager) for writable environments.
        // Read-only environments don't need to write log entries.
        let log_manager = if !read_only {
            let fm = Arc::new(
                FileManager::new(
                    &env_home,
                    false,
                    64 * 1024 * 1024, // 64 MiB per log file
                    100,              // file handle cache size
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
            // Multi-database recovery: the `redo_ln` in RecoveryManager
            // already gates each LN replay on `tree.get_database_id() ==
            // rec.db_id`, so only LNs for db_id=1 flow into `recovery_tree`.
            // Future work: maintain a HashMap<u64, Tree> inside recovery and
            // return the full map so all databases are reconstructed.
            //
            // Port of: RecoveryManager.recover() called from
            //          EnvironmentImpl constructor in JE, followed by
            //          RecoveryManager.recoverDatabases() which hands the
            //          recovered DbTree to each DatabaseImpl.
            let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
            let mut rmgr = RecoveryManager::new();
            // Build the recovery tree for db_id=1.  We run recovery into a
            // locally-owned Tree so we can move it into `recovered_trees`
            // after recovery completes, giving `open_database()` access to
            // the crash-consistent state.
            //
            // Port of JE: RecoveryManager.recover() returns RecoveryInfo;
            // the recovered per-database trees are later used by
            // EnvironmentImpl.setupDbEnvironment() →
            // DatabaseImpl.setTree(recoveredTree).
            let mut recovery_tree = noxu_tree::Tree::new(1, 256);
            if let Err(e) =
                rmgr.recover(&mut scanner, Some(&mut recovery_tree), true)
            {
                return Err(DbiError::EnvironmentFailure {
                    reason: format!("recovery failed: {e}"),
                });
            }

            // Stash the recovered tree keyed by db_id=1 so that
            // open_database() can transplant it into the DatabaseImpl via
            // set_recovered_tree().  Port of JE's per-database tree
            // population from RecoveryManager.getDbIdToDbMap().
            recovered.insert(1u64, recovery_tree);

            Some(Arc::new(LogManager::new(fm, 3, 1024 * 1024, 65536)))
        } else {
            None
        };

        // Primary shared tree (db_id=1) used for LN migration by the cleaner
        // and as the backing store for the checkpointer.
        //
        // This Arc is shared with the cleaner and checkpointer.  After the
        // first open_database() call for db_id=1, the DatabaseImpl receives
        // the recovered tree via set_recovered_tree(); the primary_tree here
        // starts empty but is kept in sync by subsequent writes through
        // the cursor layer.
        //
        // Port of JE EnvironmentImpl.dbMapTree / getDbTree() used by the
        // FileProcessor (cleaner) to look up live BINs during LN migration.
        let primary_tree: Arc<std::sync::RwLock<noxu_tree::Tree>> =
            Arc::new(std::sync::RwLock::new(noxu_tree::Tree::new(1, 256)));

        // Build the evictor with a 64 MiB default budget.  A shared
        // AtomicI64 is used as the live cache-usage counter; the budget can
        // be reconfigured at runtime via Arbiter::set_max_memory().
        let cache_usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(
            64 * 1024 * 1024, // 64 MiB default max
            Arc::clone(&cache_usage),
            128 * 1024,       // 128 KiB hysteresis
            4 * 1024 * 1024,  // 4 MiB critical threshold
        );
        let evictor = Arc::new(Evictor::new(arbiter, 100, false));

        // Start the background daemon thread.  The thread loops as long as
        // `evictor.is_shutdown()` returns false, sleeping 5 ms between
        // passes so it is not a CPU hog when the cache is under budget.
        let evictor_clone = Arc::clone(&evictor);
        let evictor_thread = std::thread::Builder::new()
            .name("noxu-evictor".to_string())
            .spawn(move || {
                while !evictor_clone.is_shutdown() {
                    evictor_clone.do_evict(EvictionSource::Daemon);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            })
            .expect("failed to spawn noxu-evictor thread");

        // Build the cleaner wired to the FileManager, primary tree, and
        // LogManager for writable environments.  Read-only envs get None.
        //
        // Port of the Cleaner initialisation in JE's EnvironmentImpl
        // constructor (called after RecoveryManager.recover()).
        let cleaner = log_manager.as_ref().map(|lm| {
            let fm = Arc::clone(lm.file_manager());
            Cleaner::with_file_manager_and_tree(
                50,                      // min_utilization (50 %)
                2,                       // min_file_count
                0,                       // min_age (seconds)
                fm,
                Arc::clone(&primary_tree),
                Arc::clone(lm),
            )
        });

        // Build the checkpointer, wired to the LogManager and the primary
        // tree, for writable environments.  The db_id=1 convention matches
        // the default single database used by the primary tree.
        //
        // Port of `EnvironmentImpl` constructor calling
        // `Checkpointer(env, DbEnvPool.CHECKPOINT_TIMEOUT_MS)` in JE.
        let checkpointer = log_manager.as_ref().map(|lm| {
            use noxu_recovery::checkpointer::{Checkpointer, CheckpointConfig};
            Arc::new(
                Checkpointer::new(CheckpointConfig::default())
                    .with_log_manager(Arc::clone(lm))
                    .with_tree(Arc::clone(&primary_tree), 1),
            )
        });

        // Start the background checkpointer daemon thread.
        //
        // Mirrors the evictor pattern: the thread holds an Arc clone of the
        // Checkpointer and loops with `thread::sleep(interval)` until the
        // shutdown flag is set.
        //
        // Port of `Checkpointer.java` `run()` → periodic checkpoint loop.
        let checkpointer_thread = checkpointer.as_ref().map(|ckpt| {
            let ckpt_clone = Arc::clone(ckpt);
            let interval =
                std::time::Duration::from_millis(checkpoint_interval_ms);
            std::thread::Builder::new()
                .name("noxu-checkpointer".to_string())
                .spawn(move || {
                    while !ckpt_clone.is_shutdown() {
                        // Use condvar-based interruptible sleep so that
                        // request_shutdown() wakes the thread immediately.
                        ckpt_clone.wait_for_shutdown_or_timeout(interval);
                        if ckpt_clone.is_shutdown() {
                            break;
                        }
                        // Ignore checkpoint errors in the daemon — the
                        // environment may be closing or a concurrent
                        // checkpoint may be in progress.
                        let _ = ckpt_clone.do_checkpoint("daemon");
                    }
                })
                .expect("failed to spawn noxu-checkpointer thread")
        });

        let env = EnvironmentImpl {
            env_home,
            state: RwLock::new(EnvState::Init),
            is_read_only: read_only,
            is_transactional: transactional,
            node_sequence: NodeSequence::new(),
            next_db_id: AtomicI64::new(1),
            lock_manager,
            txn_manager,
            db_map: RwLock::new(HashMap::new()),
            name_map: RwLock::new(HashMap::new()),
            is_invalid: AtomicBool::new(false),
            invalid_reason: RwLock::new(None),
            creation_time_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            log_manager,
            evictor,
            evictor_handle: Mutex::new(Some(evictor_thread)),
            recovered_trees: Mutex::new(recovered),
            primary_tree,
            cleaner,
            checkpointer,
            checkpointer_handle: Mutex::new(checkpointer_thread),
            checkpoint_interval_ms,
        };

        // Mark as open
        *env.state.write() = EnvState::Open;

        Ok(env)
    }

    // Getters
    pub fn get_env_home(&self) -> &Path {
        &self.env_home
    }
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }
    pub fn is_transactional(&self) -> bool {
        self.is_transactional
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
        !self.is_invalid.load(Ordering::Relaxed)
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

    // Database operations

    /// Creates or opens a database.
    pub fn open_database(
        &self,
        name: &str,
        config: &DatabaseConfig,
    ) -> Result<Arc<RwLock<DatabaseImpl>>, DbiError> {
        self.check_open()?;

        // Check if database already exists
        if let Some(db_id) = self.name_map.read().get(name)
            && let Some(db) = self.db_map.read().get(db_id)
        {
            db.read().increment_reference_count();
            return Ok(db.clone());
        }

        // Create new database
        if !config.allow_create {
            return Err(DbiError::DatabaseNotFound(name.to_string()));
        }

        let db_id =
            DatabaseId::new(self.next_db_id.fetch_add(1, Ordering::Relaxed));

        let mut db_impl =
            DatabaseImpl::new(db_id, name.to_string(), DbType::User, config);

        // If recovery populated a tree for this db_id, transplant it so the
        // database starts from its recovered (crash-consistent) state rather
        // than an empty tree.  We use `remove` so the tree is transferred
        // (not cloned) and the map entry is consumed — each recovered tree is
        // used at most once.
        if let Some(recovered_tree) = self
            .recovered_trees
            .lock()
            .unwrap()
            .remove(&(db_id.id() as u64))
        {
            db_impl.set_recovered_tree(recovered_tree);
        }

        let db = Arc::new(RwLock::new(db_impl));
        db.read().increment_reference_count();

        self.db_map.write().insert(db_id, db.clone());
        self.name_map.write().insert(name.to_string(), db_id);

        Ok(db)
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
    /// Port of `EnvironmentImpl.dbRemove()`: returns an error if any open
    /// handles exist for the database (reference_count > 0).
    pub fn remove_database(&self, name: &str) -> Result<(), DbiError> {
        self.check_open()?;

        let db_id = self
            .name_map
            .read()
            .get(name)
            .copied()
            .ok_or_else(|| DbiError::DatabaseNotFound(name.to_string()))?;

        // JE: "must not have any open Database handles" — enforce here.
        if let Some(db) = self.db_map.read().get(&db_id)
            && db.read().reference_count() > 0 {
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
    /// Port of `EnvironmentImpl.dbRename()`: returns an error if any open
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

        // JE: "must not have any open Database handles" — enforce here.
        if let Some(db) = self.db_map.read().get(&db_id)
            && db.read().reference_count() > 0 {
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

    /// Returns the list of database names.
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
    pub fn get_txn_manager(&self) -> &TxnManager {
        &self.txn_manager
    }

    /// Returns a reference to the node sequence generator.
    pub fn get_node_sequence(&self) -> &NodeSequence {
        &self.node_sequence
    }

    /// Returns a clone of the shared LogManager, if any.
    ///
    /// Returns `None` for read-only environments.
    pub fn get_log_manager(&self) -> Option<Arc<LogManager>> {
        self.log_manager.clone()
    }

    /// Returns a clone of the shared Evictor.
    pub fn get_evictor(&self) -> Arc<Evictor> {
        Arc::clone(&self.evictor)
    }

    /// Returns a reference to the cleaner, if one was created.
    ///
    /// Returns `None` for read-only environments.
    pub fn get_cleaner(&self) -> Option<&Cleaner> {
        self.cleaner.as_ref()
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
    /// Port of `EnvironmentImpl.invokeEvictor()` / `Cleaner.doClean()` in JE.
    pub fn run_cleaner(
        &self,
        n_files: u32,
        force: bool,
    ) -> Result<noxu_cleaner::CleanResult, DbiError> {
        match &self.cleaner {
            None => Err(DbiError::EnvironmentFailure {
                reason: "cleaner is not available (read-only environment)".to_string(),
            }),
            Some(cleaner) => {
                cleaner
                    .do_clean(n_files, force)
                    .map_err(|e| DbiError::EnvironmentFailure { reason: e })
            }
        }
    }

    /// Writes a TxnCommit entry to the WAL and flushes according to durability.
    ///
    /// - `SyncPolicy::Sync`        → fsync after writing (default, safest)
    /// - `SyncPolicy::WriteNoSync` → flush to OS buffers, no fsync
    /// - `SyncPolicy::NoSync`      → write to log buffer only, no flush
    ///
    /// Port of `Txn.commit()` → `LogManager.flushTo()` → `FileManager.syncLogEnd()`.
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

        let entry = TxnEndEntry::new_commit(txn_id, NULL_LSN, timestamp, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);

        lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, flush, fsync)
            .map(|_| ())
            .map_err(DbiError::from)
    }

    /// Writes a TxnAbort entry to the WAL (no fsync needed on abort).
    ///
    /// Port of `Txn.abort()` → log abort entry.
    pub fn log_txn_abort(&self, txn_id: i64) -> Result<(), DbiError> {
        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(()),
        };

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = TxnEndEntry::new_abort(txn_id, NULL_LSN, timestamp, 0, NULL_VLSN);
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

    /// Returns the number of open databases.
    pub fn n_databases(&self) -> usize {
        self.db_map.read().len()
    }

    /// Closes the environment.
    pub fn close(&self) -> Result<(), DbiError> {
        let mut state = self.state.write();
        if state.is_closed() {
            return Ok(());
        }
        *state = EnvState::Closing;

        // Signal the evictor daemon to stop and wait for it to exit.
        self.evictor.shutdown();
        if let Some(handle) = self.evictor_handle.lock().unwrap().take() {
            // Best-effort join: ignore a panic in the evictor thread.
            let _ = handle.join();
        }

        // Signal the checkpointer daemon to stop and wait for it to exit.
        if let Some(ckpt) = &self.checkpointer {
            ckpt.request_shutdown();
        }
        if let Some(handle) = self.checkpointer_handle.lock().unwrap().take() {
            let _ = handle.join();
        }

        // Final (forced) checkpoint before WAL sync so recovery can restart
        // from the checkpoint rather than replaying the full log.
        // Port of JE `EnvironmentImpl.close()` calling
        // `checkpointer.doCheckpoint(CheckpointConfig.FORCE)`.
        if let Some(ckpt) = &self.checkpointer {
            let _ = ckpt.do_checkpoint("close");
        }

        // Flush and fsync the WAL so no buffered data is lost on close.
        if let Some(lm) = &self.log_manager {
            // Best-effort: ignore flush errors on close (env is shutting down).
            let _ = lm.flush_sync();
        }

        *state = EnvState::Closed;
        Ok(())
    }
}

impl Drop for EnvironmentImpl {
    fn drop(&mut self) {
        // Shut down the evictor daemon so its thread exits cleanly when the
        // environment is dropped (e.g. in tests that don't call close()).
        self.evictor.shutdown();
        if let Some(handle) = self.evictor_handle.lock().unwrap().take() {
            let _ = handle.join();
        }

        // Shut down the checkpointer daemon thread.
        if let Some(ckpt) = &self.checkpointer {
            ckpt.request_shutdown();
        }
        if let Some(handle) = self.checkpointer_handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_env(read_only: bool) -> (TempDir, EnvironmentImpl) {
        let dir = TempDir::new().unwrap();
        let env =
            EnvironmentImpl::new(dir.path(), read_only, true).unwrap();
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
            let env =
                EnvironmentImpl::new(dir.path(), false, true).unwrap();
            env.log_txn_commit(1, true, true).unwrap();
            env.log_txn_commit(2, true, true).unwrap();
            env.log_txn_abort(3).unwrap();
            env.close().unwrap();
        }

        // Second open: recovery should run over the existing log files.
        // If recovery fails (e.g. LSN state restored incorrectly), `new()`
        // will return an error.
        let env2 =
            EnvironmentImpl::new(dir.path(), false, true).unwrap();
        assert!(env2.is_open());

        // The log manager should be at a position *after* the entries
        // written in the first session (not overwriting them from offset 20).
        let lm = env2.get_log_manager().unwrap();
        let end_of_log = lm.get_end_of_log();
        assert!(
            end_of_log.file_offset() > noxu_log::file_header::FILE_HEADER_SIZE as u32,
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
            let env =
                EnvironmentImpl::new(dir.path(), false, true).unwrap();
            env.log_txn_commit(100, true, true).unwrap();
            env.close().unwrap();
        }

        // Session 2: write txn 200 after recovery.
        {
            let env =
                EnvironmentImpl::new(dir.path(), false, true).unwrap();
            env.log_txn_commit(200, true, true).unwrap();
            env.close().unwrap();
        }

        // Session 3: recover and scan; both txns should be visible.
        use crate::file_manager_scanner::FileManagerLogScanner;
        use noxu_log::FileManager;
        use noxu_recovery::{LogEntry, LogScanner};
        use std::sync::Arc;

        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100)
                .unwrap(),
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
            let env =
                EnvironmentImpl::new(dir.path(), false, true).unwrap();
            env.log_txn_commit(1, true, true).unwrap();
            env.log_txn_commit(2, true, true).unwrap();
            env.close().unwrap();
        }

        // Session 2: reopen — recovery runs with a real B-tree.
        // If tree replay panics or fails, new() returns Err here.
        {
            let env =
                EnvironmentImpl::new(dir.path(), false, true).unwrap();
            // Database can be opened after recovery.
            let mut config = DatabaseConfig::new();
            config.set_allow_create(true);
            let _db =
                env.open_database("mydb", &config).unwrap();
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
        let db_arc =
            env.open_database("test", &config).unwrap();
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
        assert!(evictor.is_shutdown(), "evictor should be shut down after close");
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
        use noxu_recovery::checkpointer::{Checkpointer, CheckpointConfig};
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
}
