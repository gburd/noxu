//! Database handle.
//!

use crate::cursor::Cursor;
use crate::cursor_config::CursorConfig;
use crate::database_config::DatabaseConfig;
use crate::database_entry::DatabaseEntry;
use crate::database_stats::{BtreeStats, DatabaseStats};
use crate::error::{NoxuError, Result};
use crate::join_config::JoinConfig;
use crate::join_cursor::JoinCursor;
use crate::lock_mode::LockMode;
use crate::operation_status::OperationStatus;
use crate::preload::{PreloadConfig, PreloadStats};
use crate::read_options::ReadOptions;
use crate::secondary_cursor::SecondaryCursor;
use crate::sequence::Sequence;
use crate::sequence_config::SequenceConfig;
use crate::stats_config::StatsConfig;
use crate::transaction::Transaction;
use crate::write_options::WriteOptions;
use noxu_dbi::{
    CursorImpl, DatabaseImpl, EnvironmentImpl, GetMode, PutMode, SearchMode,
    ThroughputStats,
};
use noxu_log::LogManager;
use noxu_sync::{Mutex, RwLock};
use noxu_txn::{Durability, LockManager, Txn, TxnManager, UndoRecord};
use noxu_util::lsn::Lsn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A database handle.
///
///
///
/// Database handles provide methods for inserting, retrieving, and
/// deleting records. A database belongs to a single environment.
///
/// # Example
/// ```ignore
/// use noxu_db::{Environment, EnvironmentConfig, DatabaseConfig, DatabaseEntry};
/// use std::path::PathBuf;
///
/// let env_config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
///     .allow_create(true);
/// let env = Environment::open(env_config).unwrap();
///
/// let db_config = DatabaseConfig::new().allow_create(true);
/// let db = env.open_database(None, "mydb", &db_config).unwrap();
///
/// let key = DatabaseEntry::from_bytes(b"key1");
/// let value = DatabaseEntry::from_bytes(b"value1");
/// db.put(None, &key, &value).unwrap();
///
/// db.close().unwrap();
/// env.close().unwrap();
/// ```
pub struct Database {
    /// Name of this database
    name: String,
    /// Database ID
    id: u64,
    /// Configuration
    config: DatabaseConfig,
    /// The underlying DatabaseImpl (shared with the EnvironmentImpl).
    pub(crate) db_impl: Arc<RwLock<DatabaseImpl>>,
    /// Back-reference to the owning EnvironmentImpl (for close/cleanup).
    env_impl: Arc<Mutex<EnvironmentImpl>>,
    /// Shared open flag — same `Arc<AtomicBool>` as the environment's
    /// `DatabaseHandle.open`, so that `Database::close()` automatically
    /// marks the environment-side handle as closed too.
    open: Arc<AtomicBool>,
    /// Throughput counters for this database's operations.
    ///
    /// Cloned from `DatabaseImpl.throughput` at open time so that
    /// `get()`, `put()`, `delete()` can increment stats without
    /// locking `db_impl`.
    throughput: Arc<ThroughputStats>,
    /// Cached lock manager — acquired once at open, never changes.
    /// Eliminates per-operation `env_impl.lock()` on the hot read/write path.
    lock_manager: Arc<LockManager>,
    /// Cached log manager — acquired once at open, None for no-WAL envs.
    /// Eliminates per-operation `env_impl.lock()` on the hot read/write path.
    log_manager: Option<Arc<LogManager>>,
    /// Cached cleaner throttle — acquired once at open, None when no cleaner.
    /// Used by put() for write-path backpressure without locking env_impl.
    cleaner_throttle: Option<Arc<noxu_cleaner::CleanerThrottle>>,
    /// Cached transaction manager — acquired once at open.
    ///
    /// Used by [`Self::with_auto_txn`] to allocate a synthetic auto-commit
    /// `Txn` per `txn = None` write so the lock manager sees a typed
    /// locker id from the explicit-txn id space (`"auto-txn:<id>"` in
    /// deadlock messages) and the auto-commit op gets full abort-undo
    /// semantics on any error path.  Closes the F12 residuals.
    txn_manager: Arc<TxnManager>,
    /// If true, auto-commit writes skip the log flush entirely (: TXN_NO_SYNC).
    no_sync: bool,
    /// If true, auto-commit writes flush to OS but skip fdatasync (: TXN_WRITE_NO_SYNC).
    write_no_sync: bool,
    /// Registered secondary indexes that automatically maintain themselves
    /// when this primary is written.  v1.6 (Decision 1B / audit C3 — the
    /// associate()-style hook): every [`SecondaryDatabase`] opened against
    /// this primary downgrades its `Arc<SecondaryHookState>` to a
    /// `Weak<dyn SecondaryHook>` and pushes it here.  `Database::put` and
    /// `Database::delete` walk the list under the same caller-supplied
    /// txn so primary writes and secondary index updates commit / abort
    /// atomically.
    ///
    /// Stored behind an `Arc<RwLock<…>>` (rather than directly on the
    /// `Database` body) so registrations performed through one of the
    /// `Arc<Mutex<Database>>` clones the user typically holds become
    /// visible to every other clone of the same primary.
    pub(crate) secondaries: Arc<
        RwLock<
            Vec<
                std::sync::Weak<
                    dyn crate::secondary_database::SecondaryHook + Send + Sync,
                >,
            >,
        >,
    >,
    /// Foreign-key referrer registry: every child secondary whose
    /// `foreign_key_database` points at *this* primary downgrades its
    /// hook to a `Weak<dyn FkReferrer>` and pushes it here.  When this
    /// primary is deleted, every entry is consulted to apply
    /// `ForeignKeyDeleteAction::Abort` (v1.6 step 8) /
    /// `Cascade` (step 9) / `Nullify` (step 10).
    pub(crate) fk_referrers: Arc<
        RwLock<
            Vec<
                std::sync::Weak<
                    dyn crate::secondary_database::FkReferrer + Send + Sync,
                >,
            >,
        >,
    >,
}

/// State of a database handle.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbState {
    /// Database is open and operational
    Open,
    /// Database has been closed
    Closed,
    /// Database is in an invalid state
    Invalid,
}

impl Database {
    /// Creates a CursorImpl, wired to the WAL and lock manager when the
    /// environment has them.
    ///
    /// Uses cached `lock_manager` / `log_manager` to avoid acquiring
    /// `env_impl.lock()` on every operation.
    fn make_cursor(&self) -> CursorImpl {
        self.make_cursor_with_locker(0)
    }

    /// Creates a CursorImpl with an explicit `locker_id`.
    ///
    /// Auto-commit cursors use `0`; transactional cursors must use the
    /// owning `Transaction::id` so that the LN log entries written by
    /// `cursor.put` / `cursor.delete` carry the txn id and recovery's
    /// commit/abort tracking can correctly skip aborted txns.  Without
    /// this, every LN entry was written with `txn_id = None` (the
    /// auto-commit form) and recovery treated aborted-txn writes as
    /// committed once `env_impl.close()` started running on `env.close()`
    /// after a successful commit/abort (F1).
    fn make_cursor_with_locker(&self, locker_id: i64) -> CursorImpl {
        match &self.log_manager {
            Some(lm) => CursorImpl::with_log_manager(
                Arc::clone(&self.db_impl),
                locker_id,
                Arc::clone(lm),
            )
            .with_lock_manager(Arc::clone(&self.lock_manager)),
            None => CursorImpl::new(Arc::clone(&self.db_impl), locker_id)
                .with_lock_manager(Arc::clone(&self.lock_manager)),
        }
    }

    /// Creates a CursorImpl without a lock manager (dirty-read / read-uncommitted).
    ///
    /// Used by `get_with_options()` when `ReadOptions.lock_mode == ReadUncommitted`.
    /// Skips all lock acquisition so the cursor reads directly from the BIN
    /// without blocking on write locks — mirrors 's read-uncommitted cursor.
    fn make_cursor_no_lock(&self) -> CursorImpl {
        match &self.log_manager {
            Some(lm) => CursorImpl::with_log_manager(
                Arc::clone(&self.db_impl),
                0,
                Arc::clone(lm),
            ),
            None => CursorImpl::new(Arc::clone(&self.db_impl), 0),
        }
    }

    /// Creates a CursorImpl wired to the given transaction for write-lock tracking.
    ///
    /// Behaves like `make_cursor()` but additionally calls `.with_txn()` so
    /// that write operations acquire locks via the transaction's `Txn` and
    /// record abort before-images in `WriteLockInfo`.
    ///
    /// In which passes the
    /// transaction's `Locker` to the new `CursorImpl`.
    fn make_cursor_for_txn(&self, txn: &Transaction) -> CursorImpl {
        // Use the transaction id as the cursor's locker_id so that LN
        // log entries written under this cursor carry the txn id
        // (recovery's analysis pass uses LN.txn_id together with
        // TxnCommit / TxnAbort records to decide whether to redo or
        // undo the LN). Pre-fix this was hardcoded to 0, which made
        // every txn-LN look like an auto-commit LN and caused
        // recovery to redo aborted writes.
        let cursor = self.make_cursor_with_locker(txn.get_id() as i64);
        if let Some(inner) = txn.get_inner_txn() {
            cursor.with_txn(inner)
        } else {
            cursor
        }
    }

    /// Allocates a synthetic auto-commit `Txn` and runs `op` under it.
    ///
    /// `op` receives an auto-commit-wired [`CursorImpl`] (locker_id = 0
    /// so the LN is logged with the auto-commit `InsertLN` / `DeleteLN`
    /// form, txn_ref set so the lock manager sees the synthetic auto-txn
    /// as the owner).
    ///
    /// On `Ok(value)`:
    ///   * Drops the cursor (closing it).
    ///   * Calls [`Txn::commit_with_durability`] on the synthetic auto-txn
    ///     with a [`Durability`] derived from the database's
    ///     `no_sync` / `write_no_sync` config; this releases all locks
    ///     and (for `CommitSync`) fsyncs up to the LN's LSN via
    ///     `LogManager::flush_sync_if_needed` for many-to-one fsync
    ///     coalescing under concurrent write load.
    ///   * Records the commit in the txn manager statistics and removes
    ///     the diagnostic locker label.
    ///
    /// On `Err(e)`:
    ///   * Drops the cursor.
    ///   * Calls [`Txn::abort_collect_undo`] to harvest before-image undo
    ///     records WITHOUT releasing the held write locks (so a reader
    ///     blocked on a write lock cannot observe the in-flight value
    ///     before we restore the before-image).
    ///   * Applies the undo records to the in-memory B-tree.
    ///   * Calls [`Txn::release_all_locks`] to drain the held locks.
    ///   * Records the abort in the txn manager statistics and removes
    ///     the diagnostic locker label.
    ///   * Returns `Err(e)`.
    ///
    /// Closes the first F12 residual: an auto-commit op now goes through
    /// the same lock-tracking and abort-undo machinery as an explicit
    /// transaction, so two concurrent inserts of the same brand-new key
    /// serialise through the lock manager and a forced mid-write failure
    /// rolls back the in-memory tree mutation.
    fn with_auto_txn<F, T>(&self, op: F) -> Result<T>
    where
        F: FnOnce(&mut CursorImpl) -> Result<T>,
    {
        let auto_txn =
            self.txn_manager.begin_auto_txn(self.log_manager.clone());
        let synthetic_id = auto_txn.id_as_locker();
        let auto_txn_arc = Arc::new(std::sync::Mutex::new(auto_txn));

        let mut cursor = self.make_cursor();
        cursor.attach_txn(Arc::clone(&auto_txn_arc));

        let result = op(&mut cursor);
        // Drop the cursor handle (un-pins BIN, drops Arc<Mutex<Txn>> ref).
        drop(cursor);

        // Reclaim sole ownership of the synthetic auto-txn so we can
        // finalise it.  All cursors and their Arcs were dropped above.
        let mut auto_txn = match Arc::try_unwrap(auto_txn_arc) {
            Ok(m) => m.into_inner().unwrap_or_else(|p| p.into_inner()),
            Err(_arc) => {
                // A cursor escaped the closure with a clone of the txn
                // Arc.  This is a caller-side bug — leak the auto-txn
                // (its Drop calls `close()` which aborts) and surface a
                // typed error.  No undo applied because we cannot
                // safely take the Txn out of the shared Arc.
                return Err(NoxuError::OperationNotAllowed(
                    "with_auto_txn: synthetic auto-txn outlived cursor scope"
                        .to_string(),
                ));
            }
        };
        let txn_manager = Arc::clone(&self.txn_manager);

        match result {
            Ok(value) => {
                let durability = if self.no_sync {
                    Durability::CommitNoSync
                } else if self.write_no_sync {
                    Durability::CommitWriteNoSync
                } else {
                    Durability::CommitSync
                };
                if let Err(e) = auto_txn.commit_with_durability(durability) {
                    // Commit failed (e.g. log fsync error).  Undo the
                    // in-memory tree write so we are not left in an
                    // inconsistent state, then surface the error.
                    let undo_records =
                        auto_txn.abort_collect_undo().unwrap_or_default();
                    self.apply_auto_txn_undo(undo_records);
                    auto_txn.release_all_locks();
                    txn_manager.abort_txn(synthetic_id);
                    return Err(NoxuError::OperationNotAllowed(format!(
                        "auto-commit fsync failed: {e}"
                    )));
                }
                txn_manager.commit_txn(synthetic_id);
                Ok(value)
            }
            Err(e) => {
                // Phase 1: collect undo without releasing write locks.
                let undo_records =
                    auto_txn.abort_collect_undo().unwrap_or_default();
                // Phase 2: apply undo while write locks are still held
                // so any concurrent reader blocked on a write lock
                // cannot observe the in-flight value.
                self.apply_auto_txn_undo(undo_records);
                // Phase 3: drain locks.
                auto_txn.release_all_locks();
                txn_manager.abort_txn(synthetic_id);
                Err(e)
            }
        }
    }

    /// Applies undo records collected from a synthetic auto-txn to the
    /// in-memory B-tree of `self`.  Mirrors the per-`undo_record` block
    /// in `Transaction::abort()` but specialised for the
    /// single-database `with_auto_txn` case so we do not need to thread
    /// the env-impl in.
    fn apply_auto_txn_undo(&self, undo_records: Vec<UndoRecord>) {
        let db_id_match = self.id;
        let db_guard = self.db_impl.read();
        let Some(tree) = db_guard.get_real_tree() else { return };
        for undo in undo_records {
            // The synthetic auto-txn touches only this database, but be
            // defensive in case future changes broaden the contract.
            if undo.database_id != db_id_match {
                continue;
            }
            let Some(abort_key) = undo.abort_key else { continue };
            if undo.abort_known_deleted {
                if tree.delete(&abort_key) {
                    db_guard.decrement_entry_count();
                }
            } else if let Some(abort_data) = undo.abort_data {
                let lsn = noxu_util::Lsn::from_u64(undo.abort_lsn);
                let _ = tree.insert(abort_key, abort_data, lsn);
            }
        }
    }

    /// Auto-commit flush: when `txn` is `None` (auto-commit mode), flush and
    /// fsync the log before returning to the caller.
    ///
    /// `write_lsn` is the LSN assigned to the write operation just performed.
    /// Port of`LogManager.flushTo(lsn)`: if a concurrent committer already
    /// flushed past `write_lsn`, the fdatasync is skipped entirely, giving
    /// natural many:1 fsync coalescing under concurrent write load with no
    /// explicit group-commit configuration required.
    fn auto_commit_sync(
        &self,
        txn: Option<&Transaction>,
        write_lsn: Lsn,
    ) -> Result<()> {
        if txn.is_some() {
            return Ok(()); // explicit txn handles its own commit/fsync
        }
        if self.no_sync {
            return Ok(()); // : TXN_NO_SYNC — skip log flush entirely
        }
        if let Some(lm) = &self.log_manager {
            if self.write_no_sync {
                // : TXN_WRITE_NO_SYNC — flush to OS buffer, no fdatasync
                lm.flush_no_sync().map_err(|e| {
                    NoxuError::OperationNotAllowed(e.to_string())
                })?;
            } else {
                // : flushTo(lsn) — skip if already covered by another flush.
                lm.flush_sync_if_needed(write_lsn).map_err(|e| {
                    NoxuError::OperationNotAllowed(e.to_string())
                })?;
            }
        }
        Ok(())
    }

    /// Creates a new database handle.
    ///
    /// Internal constructor called by Environment.
    ///
    /// `open_flag` is a shared `Arc<AtomicBool>` that is also stored in the
    /// environment's `DatabaseHandle` for this database.  Setting it to `false`
    /// (via `Database::close()`) simultaneously marks the env-side handle as
    /// closed, allowing `Environment::close()` to succeed without a separate
    /// callback.
    pub(crate) fn new(
        name: String,
        id: u64,
        config: DatabaseConfig,
        db_impl: Arc<RwLock<DatabaseImpl>>,
        env_impl: Arc<Mutex<EnvironmentImpl>>,
        open_flag: Arc<AtomicBool>,
        no_sync: bool,
        write_no_sync: bool,
    ) -> Self {
        let throughput = db_impl.read().throughput.clone();
        // Cache the manager Arcs at construction so hot-path operations
        // (get/put/delete) never need to re-acquire env_impl.lock().
        let (lock_manager, log_manager, cleaner_throttle, txn_manager) = {
            let env = env_impl.lock();
            let lm = Arc::clone(env.get_lock_manager());
            let logm = env.get_log_manager();
            let ct = env.get_cleaner_throttle();
            let txnm = Arc::clone(env.get_txn_manager());
            (lm, logm, ct, txnm)
        };
        Database {
            name,
            id,
            config,
            db_impl,
            env_impl,
            open: open_flag,
            throughput,
            lock_manager,
            log_manager,
            cleaner_throttle,
            txn_manager,
            no_sync,
            write_no_sync,
            secondaries: Arc::new(RwLock::new(Vec::new())),
            fk_referrers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Retrieves a record by key.
    ///
    ///
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `key` - The search key
    /// * `data` - Output parameter to receive the data
    ///
    /// # Returns
    /// `OperationStatus::Success` if found, `OperationStatus::NotFound` otherwise
    ///
    /// # Errors
    /// Returns an error if the database is closed
    pub fn get(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        observe_span!(
            "db_get",
            db_name = self.name.as_str(),
            key_size = key.get_data().map_or(0, |k| k.len()),
        );
        let _obs_timer = observe_timer_start!();
        observe_counter!("noxu_db_operations_total", "op" => "get");

        let key_bytes = match key.get_data() {
            Some(k) => k,
            None => return Ok(OperationStatus::NotFound),
        };

        let mut cursor = match txn {
            Some(t) => self.make_cursor_for_txn(t),
            None => self.make_cursor(),
        };
        match cursor
            .search(key_bytes, None, SearchMode::Set)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
        {
            noxu_dbi::OperationStatus::Success => {
                let (_, value) = cursor.get_current().map_err(|e| {
                    NoxuError::OperationNotAllowed(e.to_string())
                })?;
                // Partial get: return only the requested slice.
                // DatabaseEntry partial-read logic.
                if data.is_partial() {
                    let off = data.get_partial_offset();
                    let len = data.get_partial_length();
                    let end = (off + len).min(value.len());
                    let slice =
                        if off < value.len() { &value[off..end] } else { &[] };
                    data.set_data(slice);
                } else {
                    data.set_data(&value);
                }
                self.throughput.n_pri_searches.fetch_add(1, Ordering::Relaxed);
                observe_timer_record!(_obs_timer, "noxu_db_operation_duration_seconds", "op" => "get");
                Ok(OperationStatus::Success)
            }
            _ => {
                self.throughput
                    .n_pri_search_fails
                    .fetch_add(1, Ordering::Relaxed);
                observe_timer_record!(_obs_timer, "noxu_db_operation_duration_seconds", "op" => "get");
                Ok(OperationStatus::NotFound)
            }
        }
    }

    /// Retrieves a record with per-operation read options.
    ///
    /// Mirrors `Cursor.get()` with `ReadOptions` applied:
    /// - `LockMode::ReadUncommitted` — dirty read, no lock acquired ( read-uncommitted)
    /// - `LockMode::ReadCommitted` — read-committed isolation (standard locking)
    /// - `LockMode::Rmw` — acquire write lock for read-modify-write
    /// - `LockMode::Default` — environment default isolation
    ///
    /// `CacheMode` in `ReadOptions` is advisory (currently informational).
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle
    /// * `key` - The search key
    /// * `data` - Output parameter to receive the data
    /// * `opts` - Per-operation read options (isolation, cache hints)
    ///
    /// # Returns
    /// `OperationStatus::Success` if found, `OperationStatus::NotFound` otherwise
    pub fn get_with_options(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        data: &mut DatabaseEntry,
        opts: &ReadOptions,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        // Wave 1C audit cleanup (database Low "asymmetric observability
        // between *_with_options paths and the basic ones"): mirror
        // the span / counter / timer instrumentation that
        // `Database::get` emits so dashboards do not lose the
        // per-operation `op = get_with_options` slice when callers
        // opt for the richer surface.
        observe_span!(
            "db_get_with_options",
            db_name = self.name.as_str(),
            key_size = key.get_data().map_or(0, |k| k.len()),
            lock_mode = format!("{:?}", opts.lock_mode),
        );
        let _obs_timer = observe_timer_start!();
        observe_counter!("noxu_db_operations_total", "op" => "get_with_options");

        let key_bytes = match key.get_data() {
            Some(k) => k,
            None => return Ok(OperationStatus::NotFound),
        };

        let mut cursor = match opts.lock_mode {
            LockMode::ReadUncommitted => self.make_cursor_no_lock(),
            _ => match txn {
                Some(t) => self.make_cursor_for_txn(t),
                None => self.make_cursor(),
            },
        };

        match cursor
            .search(key_bytes, None, SearchMode::Set)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
        {
            noxu_dbi::OperationStatus::Success => {
                let (_, value) = cursor.get_current().map_err(|e| {
                    NoxuError::OperationNotAllowed(e.to_string())
                })?;
                if data.is_partial() {
                    let off = data.get_partial_offset();
                    let len = data.get_partial_length();
                    let end = (off + len).min(value.len());
                    let slice =
                        if off < value.len() { &value[off..end] } else { &[] };
                    data.set_data(slice);
                } else {
                    data.set_data(&value);
                }
                self.throughput.n_pri_searches.fetch_add(1, Ordering::Relaxed);
                observe_timer_record!(
                    _obs_timer,
                    "noxu_db_operation_duration_seconds",
                    "op" => "get_with_options"
                );
                Ok(OperationStatus::Success)
            }
            _ => {
                self.throughput
                    .n_pri_search_fails
                    .fetch_add(1, Ordering::Relaxed);
                observe_timer_record!(
                    _obs_timer,
                    "noxu_db_operation_duration_seconds",
                    "op" => "get_with_options"
                );
                Ok(OperationStatus::NotFound)
            }
        }
    }

    /// Inserts or updates a record.
    ///
    ///
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `key` - The key to insert/update
    /// * `data` - The data to store
    ///
    /// # Returns
    /// `OperationStatus::Success` on success
    ///
    /// # Errors
    /// Returns an error if the database is closed or read-only
    pub fn put(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_writable()?;
        observe_span!(
            "db_put",
            db_name = self.name.as_str(),
            key_size = key.get_data().map_or(0, |k| k.len()),
            data_size = data.get_data().map_or(0, |d| d.len()),
        );
        let _obs_timer = observe_timer_start!();
        observe_counter!("noxu_db_operations_total", "op" => "put");

        let key_bytes = key.get_data().unwrap_or(&[]);

        // Partial put: read-modify-write using the partial offset/length.
        // LN.combinePuts() — existing bytes outside [offset..offset+length]
        // are preserved; only the specified range is replaced with new data.
        //
        // Wave 1C audit cleanup (database Low "partial-put length mismatch
        // silent truncation"): when the user supplies a `data` value
        // whose byte length does not match the configured partial
        // length, JE rejects the call; the v1.5.0 implementation here
        // silently min'd the two lengths and truncated the user's
        // bytes.  We now return a typed [`NoxuError::IllegalArgument`]
        // so the mismatch is visible at the call site instead of
        // corrupting the on-disk record.
        let write_bytes: Vec<u8>;
        let data_bytes: &[u8] = if data.is_partial() {
            let new_bytes = data.get_data().unwrap_or(&[]);
            let off = data.get_partial_offset();
            let len = data.get_partial_length();
            if new_bytes.len() != len {
                return Err(NoxuError::IllegalArgument(format!(
                    "partial put: data length {} does not match \
                     partial_length {} (partial_offset={}); JE \
                     requires exact equality",
                    new_bytes.len(),
                    len,
                    off
                )));
            }
            // Fetch the existing record to splice into.
            let existing = {
                let mut tmp_entry = DatabaseEntry::new();
                let mut tmp_cursor = self.make_cursor();
                match tmp_cursor
                    .search(key_bytes, None, noxu_dbi::SearchMode::Set)
                    .map_err(|e| {
                        NoxuError::OperationNotAllowed(e.to_string())
                    })? {
                    noxu_dbi::OperationStatus::Success => {
                        let (_, v) = tmp_cursor.get_current().map_err(|e| {
                            NoxuError::OperationNotAllowed(e.to_string())
                        })?;
                        tmp_entry.set_data(&v);
                        tmp_entry.get_data().unwrap_or(&[]).to_vec()
                    }
                    _ => vec![0u8; off + len],
                }
            };
            let total_len = (off + len).max(existing.len());
            let mut patched = existing;
            patched.resize(total_len, 0);
            patched[off..off + len].copy_from_slice(new_bytes);
            write_bytes = patched;
            &write_bytes
        } else {
            data.get_data().unwrap_or(&[])
        };

        // v1.6 (audit C3 / step 6): if any secondaries are registered,
        // capture the pre-put value of this key (if it exists) BEFORE
        // the overwrite so we can pass it as `old_data` to the
        // secondary key creator below.  When the put is a fresh
        // insert the read returns NotFound and `old_data_for_secondaries`
        // remains `None`.  For partial puts the pre-put value already
        // lives in `write_bytes` indirectly; we re-read here to keep
        // the code paths uniform and to use the caller's txn for the
        // read so isolation is honoured.
        let secondaries_pre = self.live_secondaries();
        let old_data_for_secondaries: Option<Vec<u8>> =
            if secondaries_pre.is_empty() {
                None
            } else {
                let mut existing = DatabaseEntry::new();
                match self.get(txn, key, &mut existing)? {
                    OperationStatus::Success => {
                        existing.get_data().map(<[u8]>::to_vec)
                    }
                    _ => None,
                }
            };

        match txn {
            Some(t) => {
                let mut cursor = self.make_cursor_for_txn(t);
                cursor.put(key_bytes, data_bytes, PutMode::Overwrite).map_err(
                    |e| NoxuError::OperationNotAllowed(e.to_string()),
                )?;
            }
            None => {
                // Wrap the write in a synthetic auto-commit `Txn` so the
                // lock manager sees a typed locker id ("auto-txn:<id>")
                // and any error rolls back the in-memory tree write
                // through `Txn::abort_collect_undo`.
                self.with_auto_txn(|cursor| {
                    cursor
                        .put(key_bytes, data_bytes, PutMode::Overwrite)
                        .map_err(|e| {
                            NoxuError::OperationNotAllowed(e.to_string())
                        })?;
                    Ok(())
                })?;
            }
        }

        // v1.6 (audit C3 — the associate()-style hook): drive every
        // registered secondary index under the same caller-supplied
        // txn so the primary record and its secondary entries commit /
        // abort together.
        //
        // Step 4 + Step 6 (this path): we capture the pre-put value
        // before issuing the primary write so the put-existing-key
        // update path can pass it as `old_data` and the secondary key
        // creator can compute every stale (sec_key, pri_key) pair to
        // delete in addition to inserting the new ones.  Pre-Step-6
        // an update over an existing key leaked the previous
        // secondary entries (audit C3 sub-case).
        let secondaries = self.live_secondaries();
        if !secondaries.is_empty() {
            // Build a fresh DatabaseEntry around the data we just
            // wrote so the secondary key creator sees the bytes the
            // user asked for (and not the partial-put scratch buffer).
            let new_entry = DatabaseEntry::from_bytes(data_bytes);
            let old_entry: Option<DatabaseEntry> = old_data_for_secondaries
                .as_deref()
                .map(DatabaseEntry::from_bytes);
            for hook in secondaries {
                hook.maintain(txn, key, old_entry.as_ref(), Some(&new_entry))?;
            }
        }

        // Apply cleaner write-path backpressure for auto-commit (no-txn) bulk
        // writes: if the log write rate exceeds the cleaner's capacity, sleep
        // briefly so the cleaner can keep up.  Transactional paths handle this
        // in Transaction::commit_with_durability() instead.
        if txn.is_none()
            && let Some(delay) = self
                .cleaner_throttle
                .as_ref()
                .and_then(|t| t.should_throttle_writer())
        {
            std::thread::sleep(delay);
        }

        self.throughput.n_pri_updates.fetch_add(1, Ordering::Relaxed);
        observe_timer_record!(_obs_timer, "noxu_db_operation_duration_seconds", "op" => "put");
        Ok(OperationStatus::Success)
    }

    /// Inserts or updates a record with per-operation write options.
    ///
    /// Extends `put()` with `WriteOptions` support:
    /// - `ttl` — if > 0, sets a per-record TTL expiration (hours from now); the
    ///   record will be treated as expired and invisible after the TTL elapses.
    ///   Stored in the BIN slot as absolute hours since Unix epoch, matching
    ///   the `BIN.expirationInHours` / `IN.entryExpiration` path.
    /// - `update_ttl` — if true and the record already exists, refreshes its TTL
    ///   to the new value rather than leaving the original expiration.
    /// - `cache_mode` — advisory cache hint (currently informational).
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle
    /// * `key` - The key to insert/update
    /// * `data` - The data to store
    /// * `opts` - Per-operation write options (TTL, cache hints)
    ///
    /// # Returns
    /// `OperationStatus::Success` on success
    pub fn put_with_options(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
        opts: &WriteOptions,
    ) -> Result<OperationStatus> {
        let result = self.put(txn, key, data)?;

        // Apply TTL to the just-written BIN slot when requested.
        if opts.ttl > 0 {
            let key_bytes = key.get_data().unwrap_or(&[]);
            let expiration_hours =
                noxu_util::current_time_hours().saturating_add(opts.ttl as u32);
            self.db_impl
                .read()
                .update_key_expiration(key_bytes, expiration_hours);
        }

        Ok(result)
    }

    /// Inserts a record, failing if the key already exists.
    ///
    ///
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `key` - The key to insert
    /// * `data` - The data to store
    ///
    /// # Returns
    /// `OperationStatus::Success` if inserted, `OperationStatus::KeyExists` if key already exists
    ///
    /// # Errors
    /// Returns an error if the database is closed or read-only
    pub fn put_no_overwrite(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_writable()?;

        let key_bytes = key.get_data().unwrap_or(&[]);
        let data_bytes = data.get_data().unwrap_or(&[]);

        let status = match txn {
            Some(t) => {
                let mut cursor = self.make_cursor_for_txn(t);
                match cursor
                    .put(key_bytes, data_bytes, PutMode::NoOverwrite)
                    .map_err(|e| {
                        NoxuError::OperationNotAllowed(e.to_string())
                    })? {
                    noxu_dbi::OperationStatus::KeyExist => {
                        OperationStatus::KeyExists
                    }
                    _ => OperationStatus::Success,
                }
            }
            None => self.with_auto_txn(|cursor| {
                cursor
                    .put(key_bytes, data_bytes, PutMode::NoOverwrite)
                    .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))
                    .map(|s| match s {
                        noxu_dbi::OperationStatus::KeyExist => {
                            OperationStatus::KeyExists
                        }
                        _ => OperationStatus::Success,
                    })
            })?,
        };
        if status == OperationStatus::Success {
            self.throughput.n_pri_inserts.fetch_add(1, Ordering::Relaxed);
        } else {
            self.throughput.n_pri_insert_fails.fetch_add(1, Ordering::Relaxed);
        }
        Ok(status)
    }

    /// Deletes a record by key.
    ///
    ///
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// `OperationStatus::Success` if deleted, `OperationStatus::NotFound` if key didn't exist
    ///
    /// # Errors
    /// Returns an error if the database is closed or read-only
    pub fn delete(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_writable()?;
        observe_span!(
            "db_delete",
            db_name = self.name.as_str(),
            key_size = key.get_data().map_or(0, |k| k.len()),
        );
        let _obs_timer = observe_timer_start!();
        observe_counter!("noxu_db_operations_total", "op" => "delete");

        let key_bytes = match key.get_data() {
            Some(k) => k,
            None => return Ok(OperationStatus::NotFound),
        };

        // v1.6 (audit C3): if any secondaries are registered we must
        // capture the pre-delete primary data on each iteration so the
        // secondary key creator can recompute every (sec_key, pri_key)
        // pair to remove.  Collected outside the cursor closure so
        // the auto-commit and explicit-txn paths share one buffer.
        let secondaries = self.live_secondaries();
        let track_old_data = !secondaries.is_empty();
        let mut deleted_old_values: Vec<Vec<u8>> = Vec::new();

        // v1.6 (audit C2 / Decision 2C — step 8): consult any FK
        // referrers BEFORE the delete is applied.  An Abort action
        // raises a typed error and prevents the foreign delete from
        // happening at all (matching JE's `ForeignConstraintException`
        // semantics).  Cascade / Nullify (steps 9 / 10) mutate child
        // records under the same caller-supplied txn so the foreign
        // delete and its consequences commit / abort together.
        let fk_referrers = self.live_fk_referrers();
        if !fk_referrers.is_empty() {
            for referrer in &fk_referrers {
                referrer.on_foreign_key_deleted(txn, key)?;
            }
        }

        // Inner closure shared between the explicit-txn and synthetic
        // auto-txn paths: scans + deletes every duplicate of `key_bytes`
        // through the supplied `cursor`.  See comment in pre-Wave-1A
        // delete for the dup-loop rationale (BDB-JE
        // `Database.delete(key)` semantics).
        let mut run_delete = |cursor: &mut CursorImpl| -> Result<bool> {
            let mut deleted_any = false;
            while let noxu_dbi::OperationStatus::Success = cursor
                .search(key_bytes, None, SearchMode::Set)
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
            {
                if track_old_data {
                    let (_, v) = cursor.get_current().map_err(|e| {
                        NoxuError::OperationNotAllowed(e.to_string())
                    })?;
                    deleted_old_values.push(v);
                }
                cursor.delete().map_err(|e| {
                    NoxuError::OperationNotAllowed(e.to_string())
                })?;
                deleted_any = true;
            }
            Ok(deleted_any)
        };

        let deleted_any = match txn {
            Some(t) => {
                let mut cursor = self.make_cursor_for_txn(t);
                run_delete(&mut cursor)?
            }
            None => self.with_auto_txn(&mut run_delete)?,
        };

        // v1.6 (audit C3): fan out the secondary cleanup under the
        // caller's txn so the primary delete and every secondary
        // tombstone commit / abort together.
        if deleted_any && !secondaries.is_empty() {
            for old_bytes in &deleted_old_values {
                let old_entry = DatabaseEntry::from_bytes(old_bytes);
                for hook in &secondaries {
                    hook.maintain(txn, key, Some(&old_entry), None)?;
                }
            }
        }

        let status = if deleted_any {
            OperationStatus::Success
        } else {
            OperationStatus::NotFound
        };
        if status == OperationStatus::Success {
            self.throughput.n_pri_deletes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.throughput.n_pri_delete_fails.fetch_add(1, Ordering::Relaxed);
        }
        observe_timer_record!(_obs_timer, "noxu_db_operation_duration_seconds", "op" => "delete");
        Ok(status)
    }

    /// Opens a cursor for iterating over database records.
    ///
    /// When a `Some(&txn)` is passed, the cursor binds to the transaction's
    /// `Locker`: every cursor `get` acquires shared locks tracked by the txn,
    /// every cursor `put`/`delete` acquires exclusive locks and is rolled
    /// back if the txn aborts.  When `txn` is `None` the cursor runs in
    /// auto-commit mode — each write is its own transaction and is fsynced
    /// before returning.
    ///
    /// **You must close the cursor before committing or aborting the
    /// transaction it was opened under.**
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle that the cursor should
    ///   participate in.
    /// * `config` - Optional cursor configuration
    ///
    /// # Returns
    /// A new cursor handle
    ///
    /// # Errors
    /// Returns an error if the database is closed
    pub fn open_cursor(
        &self,
        txn: Option<&Transaction>,
        config: Option<&CursorConfig>,
    ) -> Result<Cursor> {
        self.check_open()?;

        let read_only = config.map(|c| c.read_uncommitted).unwrap_or(false)
            || self.config.read_only;

        let cursor_impl = if read_only {
            CursorImpl::new(Arc::clone(&self.db_impl), 0)
        } else {
            // Plumb the caller's txn through to the cursor so that
            // cursor reads acquire shared locks via the txn's locker and
            // cursor writes acquire exclusive locks (and roll back on
            // txn.abort()) rather than auto-committing.  See API audit
            // 2026-05 cursor finding C1.
            match txn {
                Some(t) => self.make_cursor_for_txn(t),
                None => self.make_cursor(),
            }
        };

        Ok(Cursor::from_impl(cursor_impl, read_only))
    }

    /// Opens (and optionally creates) a sequence backed by this database.
    ///
    ///
    ///
    /// # Arguments
    /// * `key`    - The database key under which the sequence record is stored.
    /// * `config` - Sequence configuration (use `SequenceConfig::new()` for defaults).
    ///
    /// # Errors
    /// Returns an error if the database is closed, the config is invalid, or
    /// `allow_create` is false and the sequence does not exist.
    pub fn open_sequence<'db>(
        &'db self,
        key: &DatabaseEntry,
        config: SequenceConfig,
    ) -> Result<Sequence<'db>> {
        self.check_open()?;
        Sequence::open(self, key, config)
    }

    /// Closes the database handle.
    ///
    ///
    ///
    /// # Errors
    /// Returns an error if the database is already closed
    pub fn close(&self) -> Result<()> {
        if !self.open.load(Ordering::Acquire) {
            return Err(NoxuError::DatabaseClosed);
        }

        self.open.store(false, Ordering::Release);
        let _ = self
            .env_impl
            .lock()
            .close_database(noxu_dbi::DatabaseId::new(self.id as i64));
        Ok(())
    }

    /// Returns the database name.
    ///
    ///
    pub fn get_database_name(&self) -> &str {
        &self.name
    }

    /// Returns the database configuration.
    ///
    ///
    pub fn get_config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Returns the underlying database ID.  Used by FK cascade guards
    /// to disambiguate `(db, key)` frames when several databases
    /// participate in a cycle.
    pub(crate) fn db_id_for_fk_guard(&self) -> u64 {
        self.id
    }

    /// Registers a secondary index for automatic maintenance.
    ///
    /// v1.6 (audit C3 — associate() hook): every [`SecondaryDatabase`]
    /// downgrades its inner `Arc<SecondaryHookState>` to a `Weak` and
    /// stores it here.  Subsequent `put` / `delete` calls iterate the
    /// list and forward the same txn to every live secondary, dropping
    /// dead `Weak` entries on the fly.
    pub(crate) fn register_secondary(
        &self,
        hook: std::sync::Weak<
            dyn crate::secondary_database::SecondaryHook + Send + Sync,
        >,
    ) {
        let mut guard = self.secondaries.write();
        // Compact dead Weak entries lazily on every registration so the
        // list does not grow unboundedly with churn.
        guard.retain(|w| w.strong_count() > 0);
        guard.push(hook);
    }

    /// Returns a snapshot of every live registered secondary.  Used by
    /// the automatic-maintenance plumbing in `put` / `delete` to drive
    /// secondaries without holding the registry lock across the
    /// secondary call.  Dead `Weak` entries are dropped from the
    /// returned list (and — because we re-acquire the registry write
    /// lock at registration time — lazily compacted from the registry
    /// itself).
    pub(crate) fn live_secondaries(
        &self,
    ) -> Vec<Arc<dyn crate::secondary_database::SecondaryHook + Send + Sync>>
    {
        self.secondaries.read().iter().filter_map(|w| w.upgrade()).collect()
    }

    /// Registers an FK referrer that points at this primary as its
    /// foreign-key target (v1.6 audit C2 / Decision 2C — Abort hook).
    pub(crate) fn register_fk_referrer(
        &self,
        referrer: std::sync::Weak<
            dyn crate::secondary_database::FkReferrer + Send + Sync,
        >,
    ) {
        let mut guard = self.fk_referrers.write();
        guard.retain(|w| w.strong_count() > 0);
        guard.push(referrer);
    }

    /// Snapshot of every live FK referrer.
    pub(crate) fn live_fk_referrers(
        &self,
    ) -> Vec<Arc<dyn crate::secondary_database::FkReferrer + Send + Sync>> {
        self.fk_referrers.read().iter().filter_map(|w| w.upgrade()).collect()
    }

    /// Returns an approximate count of records in the database.
    ///
    /// reads the per-database `AtomicU64` entry
    /// counter, giving O(1) performance analogous to an O(1) counter.
    ///
    /// The counter is incremented on every new insert and decremented on every
    /// delete (including transaction aborts that undo inserts).
    ///
    /// # Errors
    /// Returns an error if the database is closed
    pub fn count(&self) -> Result<u64> {
        self.check_open()?;
        Ok(self.db_impl.read().entry_count())
    }

    /// Returns all records as `(key_bytes, data_bytes)` pairs in key order.
    ///
    /// This is a helper for schema evolution: it uses the lower-level
    /// `CursorImpl` directly so each iteration yields raw `Vec<u8>` pairs
    /// without allocating a pair of `DatabaseEntry` values per record.
    ///
    /// # Errors
    /// Returns an error if the database is closed or a cursor operation fails.
    pub fn scan_all_kv(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_open()?;

        let mut cursor = CursorImpl::new(Arc::clone(&self.db_impl), 0);
        let first_status = cursor
            .get_first()
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;

        if first_status != noxu_dbi::OperationStatus::Success {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        loop {
            let (k, v) = cursor
                .get_current()
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
            records.push((k, v));

            let status = cursor
                .retrieve_next(GetMode::Next)
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
            if status != noxu_dbi::OperationStatus::Success {
                break;
            }
        }

        Ok(records)
    }

    /// Returns whether the database handle is valid.
    ///
    ///
    pub fn is_valid(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Returns the current state of the database handle.
    pub fn state(&self) -> DbState {
        if self.open.load(Ordering::Acquire) {
            DbState::Open
        } else {
            DbState::Closed
        }
    }

    /// Flushes all pending writes for this database to stable storage.
    ///
    /// Implements `Database.sync()` — issues an fdatasync on the log file,
    /// ensuring that all writes made by non-transactional or deferred-sync
    /// operations are durable before returning.
    ///
    /// # Returns
    /// `Ok(())` on success. Acts as a no-op for non-transactional /
    /// in-memory environments where no log manager is configured.
    ///
    /// # Errors
    /// Returns an error if the database is closed or the underlying
    /// log-manager flush fails.
    pub fn sync(&self) -> Result<()> {
        self.check_open()?;
        if let Some(lm) = &self.log_manager {
            lm.flush_sync()
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        }
        Ok(())
    }

    /// Preloads the database into cache by scanning the B-tree.
    ///
    /// Walks the tree, touching each node to bring pages into the cache.
    /// Useful for warming the cache before a workload begins.
    ///
    /// # Arguments
    /// * `config` - Controls limits on preload duration and memory
    ///
    /// # Returns
    /// Statistics about what was preloaded.
    pub fn preload(&self, config: &PreloadConfig) -> Result<PreloadStats> {
        self.check_open()?;
        let start = std::time::Instant::now();
        let mut stats =
            PreloadStats { bins_loaded: 0, lns_loaded: 0, elapsed_ms: 0 };

        let guard = self.db_impl.read();
        if let Some(tree_stats) = guard.collect_btree_stats() {
            // collect_btree_stats() walks every node in the tree, which has
            // the side effect of pulling all BINs/INs into memory (cache).
            stats.bins_loaded = tree_stats.n_bins;
            if config.load_lns {
                stats.lns_loaded = tree_stats.n_entries;
            }
        }

        let _ = config.max_millis; // reserved for future time-bounded preload
        stats.elapsed_ms = start.elapsed().as_millis() as u64;
        Ok(stats)
    }

    /// Returns B-tree statistics for this database.
    ///
    /// Implements `Database.getStats(StatsConfig)`.
    ///
    /// When `config.fast` is `true`, only the O(1) entry-count is returned
    /// and no tree traversal is performed.  When `fast` is `false` (default),
    /// the full tree is walked to populate all node-count fields.
    ///
    /// # Errors
    /// Returns an error if the database is closed.
    pub fn get_stats(
        &self,
        config: Option<&StatsConfig>,
    ) -> Result<DatabaseStats> {
        self.check_open()?;
        let fast = config.map(|c| c.fast).unwrap_or(false);

        let btree = if fast {
            // Fast path: O(1) counter only; skip tree traversal.
            BtreeStats {
                leaf_node_count: self.db_impl.read().entry_count(),
                ..Default::default()
            }
        } else {
            // Full path: walk the tree.
            let guard = self.db_impl.read();
            match guard.collect_btree_stats() {
                Some(ts) => BtreeStats {
                    leaf_node_count: ts.n_entries,
                    deleted_leaf_node_count: 0,
                    bottom_internal_node_count: ts.n_bins,
                    internal_node_count: ts.n_ins,
                    main_tree_max_depth: ts.height,
                },
                None => BtreeStats {
                    leaf_node_count: guard.entry_count(),
                    ..Default::default()
                },
            }
        };

        Ok(DatabaseStats { btree })
    }

    /// Verifies the structural integrity of this database's B-tree.
    ///
    /// Walks the B-tree from root to BIN leaves and checks:
    /// - Each upper IN's children are accessible (non-null child references).
    /// - Each BIN entry that is not known-deleted has a valid (non-NULL) LSN.
    /// - The BIN's first key is >= the parent routing key (key-range containment).
    ///
    /// Mirrors `Database.verify(VerifyConfig)` — calls `BtreeVerifier` on
    /// the underlying tree.
    ///
    /// # Arguments
    /// * `config` - Verification options (which checks to run, max errors, etc.)
    ///
    /// # Returns
    /// A `VerifyResult` with any structural errors and the count of records verified.
    ///
    /// # Errors
    /// Returns an error if the database is closed.
    pub fn verify(
        &self,
        config: &noxu_engine::VerifyConfig,
    ) -> Result<noxu_engine::VerifyResult> {
        self.check_open()?;
        let guard = self.db_impl.read();
        Ok(noxu_engine::verify_database_impl(&guard, config))
    }

    /// Creates a join cursor that returns records matching all secondary-key
    /// constraints expressed by the pre-positioned `cursors`.
    ///
    /// Mirrors `Database.join(SecondaryCursor[], JoinConfig)`.
    ///
    /// Each cursor in `cursors` must already be positioned at the desired
    /// secondary key value (e.g. via `SecondaryCursor::get_search_key`).
    /// The join algorithm iterates through all candidate primary keys from
    /// `cursors[0]` and probes `cursors[1..n]` to confirm each candidate
    /// also appears in their secondary keys.  Candidates that pass all
    /// probes are returned by [`JoinCursor::get_next`].
    ///
    /// Unless `config.no_sort` is `true`, the cursor array is re-ordered by
    /// ascending duplicate-count estimate before the join starts, matching
    /// JE's optimisation for minimum candidate-set size.
    ///
    /// The returned `JoinCursor` owns the `cursors` for its lifetime.
    ///
    /// # Errors
    /// Returns an error if this database handle is closed.
    pub fn join<'db>(
        &'db self,
        cursors: Vec<SecondaryCursor<'db>>,
        config: Option<JoinConfig>,
    ) -> Result<JoinCursor<'db>> {
        self.check_open()?;
        JoinCursor::new(self, cursors, config)
    }

    /// Checks if the database is open, returns an error if not.
    fn check_open(&self) -> Result<()> {
        if !self.open.load(Ordering::Acquire) {
            return Err(NoxuError::DatabaseClosed);
        }
        Ok(())
    }

    /// Public-ish accessor for the cached log manager, used by
    /// [`crate::disk_ordered_cursor::open_disk_ordered_cursor_multi`].
    /// Returns `None` for non-WAL environments.
    pub(crate) fn cached_log_manager(
        &self,
    ) -> Option<&std::sync::Arc<noxu_log::LogManager>> {
        self.log_manager.as_ref()
    }

    /// Public-ish accessor used by the disk-ordered-cursor helper to
    /// validate that the database is still open before scanning.
    pub(crate) fn check_open_for_doc(&self) -> Result<()> {
        self.check_open()
    }

    /// Returns this database's `DatabaseId` for use by the disk-ordered
    /// cursor producer.
    pub(crate) fn database_id_for_doc(&self) -> noxu_dbi::DatabaseId {
        noxu_dbi::DatabaseId::new(self.id as i64)
    }

    /// Checks if the database is writable, returns an error if not.
    fn check_writable(&self) -> Result<()> {
        if self.config.read_only {
            return Err(NoxuError::ReadOnly);
        }
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Best effort close on drop
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use tempfile::TempDir;

    fn temp_env_and_db() -> (TempDir, Environment, Database) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();

        (temp_dir, env, db)
    }

    #[test]
    fn test_database_name() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        assert_eq!(db.get_database_name(), "testdb");
    }

    #[test]
    fn test_put_and_get() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");

        let result = db.put(None, &key, &value).unwrap();
        assert_eq!(result, OperationStatus::Success);

        let mut retrieved = DatabaseEntry::new();
        let result = db.get(None, &key, &mut retrieved).unwrap();
        assert_eq!(result, OperationStatus::Success);
        assert_eq!(retrieved.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_get_nonexistent() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"nonexistent");
        let mut data = DatabaseEntry::new();

        let result = db.get(None, &key, &mut data).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    /// Wave 1C audit cleanup (database Low "partial-put length
    /// mismatch silent truncation"): a partial put whose `data` slice
    /// differs in length from the configured partial-length must be
    /// rejected with a typed error instead of silently truncating or
    /// padding the splice.
    #[test]
    fn test_partial_put_length_mismatch_rejected() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"k");
        db.put(None, &key, &DatabaseEntry::from_bytes(b"hello world")).unwrap();

        // Partial offset=6, partial_length=5 ("world"), but only 3 bytes
        // supplied.  Used to silently truncate; now rejected.
        let mut patch = DatabaseEntry::from_bytes(b"abc");
        patch.set_partial(6, 5, true);
        let err = db.put(None, &key, &patch).unwrap_err();
        assert!(
            matches!(err, NoxuError::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert!(
            err.to_string().contains("partial"),
            "expected partial-related message, got {}",
            err
        );

        // The on-disk record is unchanged because the call returned
        // before any write.
        let mut buf = DatabaseEntry::new();
        let status = db.get(None, &key, &mut buf).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(buf.get_data().unwrap(), b"hello world");
    }

    /// Companion: when data.len() == partial_length the partial put
    /// patches the slice in place and other bytes are preserved.
    #[test]
    fn test_partial_put_exact_length_patches_in_place() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"k");
        db.put(None, &key, &DatabaseEntry::from_bytes(b"hello world")).unwrap();

        let mut patch = DatabaseEntry::from_bytes(b"WORLD");
        patch.set_partial(6, 5, true);
        db.put(None, &key, &patch).unwrap();

        let mut buf = DatabaseEntry::new();
        db.get(None, &key, &mut buf).unwrap();
        assert_eq!(buf.get_data().unwrap(), b"hello WORLD");
    }

    #[test]
    fn test_put_updates_existing() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value1 = DatabaseEntry::from_bytes(b"value1");
        let value2 = DatabaseEntry::from_bytes(b"value2");

        db.put(None, &key, &value1).unwrap();
        db.put(None, &key, &value2).unwrap();

        let mut retrieved = DatabaseEntry::new();
        db.get(None, &key, &mut retrieved).unwrap();
        assert_eq!(retrieved.get_data().unwrap(), b"value2");
    }

    #[test]
    fn test_put_no_overwrite_success() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");

        let result = db.put_no_overwrite(None, &key, &value).unwrap();
        assert_eq!(result, OperationStatus::Success);
    }

    #[test]
    fn test_put_no_overwrite_key_exists() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value1 = DatabaseEntry::from_bytes(b"value1");
        let value2 = DatabaseEntry::from_bytes(b"value2");

        db.put(None, &key, &value1).unwrap();
        let result = db.put_no_overwrite(None, &key, &value2).unwrap();
        assert_eq!(result, OperationStatus::KeyExists);

        // Verify original value is unchanged
        let mut retrieved = DatabaseEntry::new();
        db.get(None, &key, &mut retrieved).unwrap();
        assert_eq!(retrieved.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_delete() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");

        db.put(None, &key, &value).unwrap();
        let result = db.delete(None, &key).unwrap();
        assert_eq!(result, OperationStatus::Success);

        let mut retrieved = DatabaseEntry::new();
        let result = db.get(None, &key, &mut retrieved).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    #[test]
    fn test_delete_nonexistent() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"nonexistent");
        let result = db.delete(None, &key).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    #[test]
    fn test_count() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        assert_eq!(db.count().unwrap(), 0);

        let key1 = DatabaseEntry::from_bytes(b"key1");
        let value1 = DatabaseEntry::from_bytes(b"value1");
        db.put(None, &key1, &value1).unwrap();
        assert_eq!(db.count().unwrap(), 1);

        let key2 = DatabaseEntry::from_bytes(b"key2");
        let value2 = DatabaseEntry::from_bytes(b"value2");
        db.put(None, &key2, &value2).unwrap();
        assert_eq!(db.count().unwrap(), 2);

        db.delete(None, &key1).unwrap();
        assert_eq!(db.count().unwrap(), 1);
    }

    #[test]
    fn test_close() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        assert!(db.is_valid());
        db.close().unwrap();
        assert!(!db.is_valid());
    }

    #[test]
    fn test_close_twice_fails() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.close().unwrap();
        let result = db.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_operations_on_closed_database_fail() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.close().unwrap();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");
        let mut data = DatabaseEntry::new();

        assert!(db.get(None, &key, &mut data).is_err());
        assert!(db.put(None, &key, &value).is_err());
        assert!(db.put_no_overwrite(None, &key, &value).is_err());
        assert!(db.delete(None, &key).is_err());
        assert!(db.count().is_err());
        assert!(db.open_cursor(None, None).is_err());
    }

    #[test]
    fn test_state() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        assert_eq!(db.state(), DbState::Open);
        db.close().unwrap();
        assert_eq!(db.state(), DbState::Closed);
    }

    #[test]
    fn test_read_only_database() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();

        let db_config =
            DatabaseConfig::new().with_allow_create(true).with_read_only(true);
        let db = env.open_database(None, "readonly_db", &db_config).unwrap();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");

        // Write operations should fail
        assert!(db.put(None, &key, &value).is_err());
        assert!(db.put_no_overwrite(None, &key, &value).is_err());
        assert!(db.delete(None, &key).is_err());
    }

    #[test]
    fn test_multiple_databases() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db1 = env.open_database(None, "db1", &db_config).unwrap();
        let db2 = env.open_database(None, "db2", &db_config).unwrap();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value1 = DatabaseEntry::from_bytes(b"value1");
        let value2 = DatabaseEntry::from_bytes(b"value2");

        db1.put(None, &key, &value1).unwrap();
        db2.put(None, &key, &value2).unwrap();

        let mut retrieved1 = DatabaseEntry::new();
        let mut retrieved2 = DatabaseEntry::new();

        db1.get(None, &key, &mut retrieved1).unwrap();
        db2.get(None, &key, &mut retrieved2).unwrap();

        assert_eq!(retrieved1.get_data().unwrap(), b"value1");
        assert_eq!(retrieved2.get_data().unwrap(), b"value2");
    }

    #[test]
    fn test_empty_keys_and_values() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let empty_key = DatabaseEntry::from_bytes(b"");
        let empty_value = DatabaseEntry::from_bytes(b"");

        let result = db.put(None, &empty_key, &empty_value).unwrap();
        assert_eq!(result, OperationStatus::Success);

        let mut retrieved = DatabaseEntry::new();
        let result = db.get(None, &empty_key, &mut retrieved).unwrap();
        assert_eq!(result, OperationStatus::Success);
        assert_eq!(retrieved.get_data().unwrap(), b"");
    }

    #[test]
    fn test_large_keys_and_values() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let large_key = DatabaseEntry::from_bytes(&vec![b'k'; 1000]);
        let large_value = DatabaseEntry::from_bytes(&vec![b'v'; 10000]);

        db.put(None, &large_key, &large_value).unwrap();

        let mut retrieved = DatabaseEntry::new();
        db.get(None, &large_key, &mut retrieved).unwrap();
        assert_eq!(retrieved.get_data().unwrap().len(), 10000);
        assert!(retrieved.get_data().unwrap().iter().all(|&b| b == b'v'));
    }

    #[test]
    fn test_binary_keys_and_values() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let binary_key = DatabaseEntry::from_bytes(&[0u8, 1, 2, 255, 254, 253]);
        let binary_value = DatabaseEntry::from_bytes(&[255u8, 0, 128, 64, 32]);

        db.put(None, &binary_key, &binary_value).unwrap();

        let mut retrieved = DatabaseEntry::new();
        db.get(None, &binary_key, &mut retrieved).unwrap();
        assert_eq!(retrieved.get_data().unwrap(), &[255u8, 0, 128, 64, 32]);
    }

    #[test]
    fn test_scan_all_kv_empty() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        let kv = db.scan_all_kv().unwrap();
        assert!(kv.is_empty());
    }

    #[test]
    fn test_scan_all_kv_returns_records() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.put(
            None,
            &DatabaseEntry::from_vec(vec![1]),
            &DatabaseEntry::from_vec(vec![10]),
        )
        .unwrap();
        db.put(
            None,
            &DatabaseEntry::from_vec(vec![2]),
            &DatabaseEntry::from_vec(vec![20]),
        )
        .unwrap();
        let kv = db.scan_all_kv().unwrap();
        assert_eq!(kv.len(), 2);
    }

    #[test]
    fn test_scan_all_kv_then_delete() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.put(
            None,
            &DatabaseEntry::from_vec(vec![1]),
            &DatabaseEntry::from_vec(vec![10]),
        )
        .unwrap();
        db.put(
            None,
            &DatabaseEntry::from_vec(vec![2]),
            &DatabaseEntry::from_vec(vec![20]),
        )
        .unwrap();

        let kv = db.scan_all_kv().unwrap();
        assert_eq!(kv.len(), 2);

        for (k, _v) in &kv {
            let status =
                db.delete(None, &DatabaseEntry::from_vec(k.clone())).unwrap();
            assert_eq!(
                status,
                OperationStatus::Success,
                "delete failed for key {:?}",
                k
            );
        }

        let count = db.count().unwrap();
        assert_eq!(count, 0, "expected 0 records after deletes, got {}", count);
    }

    #[test]
    fn test_scan_all_kv_then_delete_u64_be_keys() {
        // Simulate the exact pattern used in EntityStore::evolve: big-endian u64 keys.
        let (_temp_dir, _env, db) = temp_env_and_db();
        for id in [1u64, 2u64] {
            let key_bytes = id.to_be_bytes().to_vec();
            let val_bytes = format!("user{}", id).into_bytes();
            db.put(
                None,
                &DatabaseEntry::from_vec(key_bytes),
                &DatabaseEntry::from_vec(val_bytes),
            )
            .unwrap();
        }
        assert_eq!(db.count().unwrap(), 2);

        let records = db.scan_all_kv().unwrap();
        assert_eq!(records.len(), 2);

        for (k, _v) in records {
            let status =
                db.delete(None, &DatabaseEntry::from_vec(k.clone())).unwrap();
            assert_eq!(
                status,
                OperationStatus::Success,
                "delete failed for u64 key {:?}",
                k
            );
        }
        assert_eq!(db.count().unwrap(), 0);
    }

    // ========================================================================
    // Additional branch-coverage tests
    // ========================================================================

    /// get() with a None-data DatabaseEntry returns NotFound.
    #[test]
    fn test_get_with_none_key_data_returns_not_found() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        let key_none = DatabaseEntry::new(); // no data set
        let mut data = DatabaseEntry::new();

        let result = db.get(None, &key_none, &mut data).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    /// delete() with a None-data DatabaseEntry returns NotFound.
    #[test]
    fn test_delete_with_none_key_data_returns_not_found() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        let key_none = DatabaseEntry::new();

        let result = db.delete(None, &key_none).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    /// open_cursor() with a CursorConfig that has read_uncommitted=true makes
    /// the cursor read-only.
    #[test]
    fn test_open_cursor_read_uncommitted_config_makes_read_only() {
        use crate::cursor_config::CursorConfig;
        let (_temp_dir, _env, db) = temp_env_and_db();

        let config = CursorConfig::new().with_read_uncommitted(true);
        let cursor = db.open_cursor(None, Some(&config)).unwrap();
        assert!(cursor.is_read_only());
    }

    /// open_cursor() with no config and a non-read-only database produces a
    /// writable cursor.
    #[test]
    fn test_open_cursor_no_config_writable_db_is_writable() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        let cursor = db.open_cursor(None, None).unwrap();
        assert!(!cursor.is_read_only());
    }

    /// scan_all_kv() on a closed database returns an error.
    #[test]
    fn test_scan_all_kv_on_closed_database_fails() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.close().unwrap();
        let result = db.scan_all_kv();
        assert!(result.is_err());
    }

    /// put_no_overwrite() on a read-only database returns an error.
    #[test]
    fn test_put_no_overwrite_on_read_only_database_fails() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();

        let db_config =
            DatabaseConfig::new().with_allow_create(true).with_read_only(true);
        let db = env.open_database(None, "ro_db", &db_config).unwrap();

        let key = DatabaseEntry::from_bytes(b"k");
        let val = DatabaseEntry::from_bytes(b"v");
        let result = db.put_no_overwrite(None, &key, &val);
        assert!(result.is_err());
    }

    // =====================================================================
    // cursor-failure map_err coverage: use the test hook in noxu-dbi to
    // force cursor operations to return Err, exercising the map_err closures
    // in Database::get / put / put_no_overwrite / delete / count / scan_all_kv.
    // =====================================================================

    /// Covers the map_err closure on `cursor.search(...)` inside `get()`.
    #[test]
    fn test_get_search_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1); // fail on the 1st check_state (search)
        let key = DatabaseEntry::from_bytes(b"any");
        let mut data = DatabaseEntry::new();
        let result = db.get(None, &key, &mut data);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.get_current()` inside `get()`.
    #[test]
    fn test_get_get_current_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        // Insert a key so search can succeed.
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        // fail on the 2nd check (check_initialized inside get_current).
        noxu_dbi::set_cursor_fail_after(2);
        let key = DatabaseEntry::from_bytes(b"k");
        let mut data = DatabaseEntry::new();
        let result = db.get(None, &key, &mut data);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.put(...)` inside `put()`.
    #[test]
    fn test_put_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let key = DatabaseEntry::from_bytes(b"k");
        let val = DatabaseEntry::from_bytes(b"v");
        let result = db.put(None, &key, &val);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.put(...)` inside `put_no_overwrite()`.
    #[test]
    fn test_put_no_overwrite_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let key = DatabaseEntry::from_bytes(b"k");
        let val = DatabaseEntry::from_bytes(b"v");
        let result = db.put_no_overwrite(None, &key, &val);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.search(...)` inside `delete()`.
    #[test]
    fn test_delete_search_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let key = DatabaseEntry::from_bytes(b"k");
        let result = db.delete(None, &key);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.delete()` inside `delete()`.
    #[test]
    fn test_delete_delete_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        // fail on the 2nd check_state (the delete() call, after search succeeds).
        noxu_dbi::set_cursor_fail_after(2);
        let key = DatabaseEntry::from_bytes(b"k");
        let result = db.delete(None, &key);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// count() uses the O(1) AtomicU64 counter; cursor-fail hooks do not affect it.
    /// Verify the counter is correct across insert/update/delete.
    #[test]
    fn test_count_atomic_counter_insert_update_delete() {
        let (_tmp, _env, db) = temp_env_and_db();

        // Empty database starts at 0.
        assert_eq!(db.count().unwrap(), 0);

        // Insert three distinct keys.
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"a"),
            &DatabaseEntry::from_bytes(b"1"),
        )
        .unwrap();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"b"),
            &DatabaseEntry::from_bytes(b"2"),
        )
        .unwrap();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"c"),
            &DatabaseEntry::from_bytes(b"3"),
        )
        .unwrap();
        assert_eq!(db.count().unwrap(), 3);

        // Overwrite an existing key — count must NOT change.
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"a"),
            &DatabaseEntry::from_bytes(b"updated"),
        )
        .unwrap();
        assert_eq!(db.count().unwrap(), 3);

        // Delete one key — count decrements.
        db.delete(None, &DatabaseEntry::from_bytes(b"b")).unwrap();
        assert_eq!(db.count().unwrap(), 2);
    }

    /// count() is O(1): verify it still works even when the cursor fail-hook
    /// is active (the hook only affects cursor operations, not the atomic read).
    #[test]
    fn test_count_unaffected_by_cursor_fail_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        noxu_dbi::set_cursor_fail_after(1);
        // count() must succeed (no cursor used).
        let result = db.count();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    /// Covers the map_err closure on `cursor.get_first()` inside `scan_all_kv()`.
    #[test]
    fn test_scan_all_kv_get_first_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let result = db.scan_all_kv();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.get_current()` inside `scan_all_kv()`.
    #[test]
    fn test_scan_all_kv_get_current_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        // fail on the 2nd check (check_initialized inside get_current, after get_first succeeds).
        noxu_dbi::set_cursor_fail_after(2);
        let result = db.scan_all_kv();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.retrieve_next(...)` inside `scan_all_kv()`.
    #[test]
    fn test_scan_all_kv_retrieve_next_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        // fail on the 3rd check (retrieve_next, after get_first and get_current succeed).
        noxu_dbi::set_cursor_fail_after(3);
        let result = db.scan_all_kv();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    #[test]
    fn test_sync_on_open_database_succeeds() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"key"),
            &DatabaseEntry::from_bytes(b"val"),
        )
        .unwrap();
        assert!(db.sync().is_ok());
    }

    #[test]
    fn test_sync_on_closed_database_fails() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.close().unwrap();
        assert!(db.sync().is_err());
    }

    // ── verify ─────────────────────────────────────────────────────────────

    #[test]
    fn test_verify_empty_database_passes() {
        use noxu_engine::VerifyConfig;
        let (_tmp, _env, db) = temp_env_and_db();
        let config = VerifyConfig::default();
        let result = db.verify(&config).unwrap();
        assert!(result.passed, "empty db should pass: {:?}", result.errors);
    }

    #[test]
    fn test_verify_populated_database_passes() {
        use noxu_engine::VerifyConfig;
        let (_tmp, _env, db) = temp_env_and_db();
        for i in 0u32..20 {
            let k = DatabaseEntry::from_bytes(&i.to_be_bytes());
            let v = DatabaseEntry::from_bytes(&(i * 2).to_be_bytes());
            db.put(None, &k, &v).unwrap();
        }
        let config = VerifyConfig::default();
        let result = db.verify(&config).unwrap();
        assert!(result.passed, "populated db should pass: {:?}", result.errors);
        assert!(result.records_verified > 0);
    }

    #[test]
    fn test_verify_closed_database_fails() {
        use noxu_engine::VerifyConfig;
        let (_tmp, _env, db) = temp_env_and_db();
        db.close().unwrap();
        let config = VerifyConfig::default();
        assert!(db.verify(&config).is_err());
    }

    // ── get_with_options / put_with_options ────────────────────────────────

    #[test]
    fn test_get_with_options_default_reads_written_record() {
        use crate::read_options::ReadOptions;
        let (_tmp, _env, db) = temp_env_and_db();
        let key = DatabaseEntry::from_bytes(b"ropt_key");
        let val = DatabaseEntry::from_bytes(b"ropt_val");
        db.put(None, &key, &val).unwrap();

        let opts = ReadOptions::new();
        let mut out = DatabaseEntry::new();
        let status = db.get_with_options(None, &key, &mut out, &opts).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), b"ropt_val");
    }

    #[test]
    fn test_get_with_options_read_uncommitted_sees_written_record() {
        use crate::read_options::ReadOptions;
        let (_tmp, _env, db) = temp_env_and_db();
        let key = DatabaseEntry::from_bytes(b"ru_key");
        let val = DatabaseEntry::from_bytes(b"ru_val");
        db.put(None, &key, &val).unwrap();

        let opts = ReadOptions::read_uncommitted();
        let mut out = DatabaseEntry::new();
        let status = db.get_with_options(None, &key, &mut out, &opts).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), b"ru_val");
    }

    #[test]
    fn test_get_with_options_not_found() {
        use crate::read_options::ReadOptions;
        let (_tmp, _env, db) = temp_env_and_db();
        let key = DatabaseEntry::from_bytes(b"missing");
        let opts = ReadOptions::new();
        let mut out = DatabaseEntry::new();
        let status = db.get_with_options(None, &key, &mut out, &opts).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_put_with_options_no_ttl_behaves_like_put() {
        use crate::write_options::WriteOptions;
        let (_tmp, _env, db) = temp_env_and_db();
        let key = DatabaseEntry::from_bytes(b"wopt_key");
        let val = DatabaseEntry::from_bytes(b"wopt_val");
        let opts = WriteOptions::new();
        let status = db.put_with_options(None, &key, &val, &opts).unwrap();
        assert_eq!(status, OperationStatus::Success);

        let mut out = DatabaseEntry::new();
        db.get(None, &key, &mut out).unwrap();
        assert_eq!(out.get_data().unwrap(), b"wopt_val");
    }

    #[test]
    fn test_put_with_options_with_ttl_stores_record() {
        use crate::write_options::WriteOptions;
        let (_tmp, _env, db) = temp_env_and_db();
        let key = DatabaseEntry::from_bytes(b"ttl_key");
        let val = DatabaseEntry::from_bytes(b"ttl_val");
        // TTL of 1 hour — the record is not yet expired so it should be readable
        let opts = WriteOptions::with_expiration(1);
        let status = db.put_with_options(None, &key, &val, &opts).unwrap();
        assert_eq!(status, OperationStatus::Success);

        let mut out = DatabaseEntry::new();
        let read_status = db.get(None, &key, &mut out).unwrap();
        assert_eq!(read_status, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), b"ttl_val");
    }

    #[test]
    fn test_put_with_options_closed_db_fails() {
        use crate::write_options::WriteOptions;
        let (_tmp, _env, db) = temp_env_and_db();
        db.close().unwrap();
        let key = DatabaseEntry::from_bytes(b"k");
        let val = DatabaseEntry::from_bytes(b"v");
        let opts = WriteOptions::new();
        assert!(db.put_with_options(None, &key, &val, &opts).is_err());
    }
}
