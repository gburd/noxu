//! Transaction handle for Noxu DB.
//!

use crate::durability::{Durability, SyncPolicy};
use crate::environment::ActiveTxns;
use crate::error::{NoxuError, Result};
use crate::transaction_config::TransactionConfig;
use noxu_dbi::{DatabaseId, EnvironmentImpl};
use noxu_log::LogManager;
use noxu_sync::Mutex as SyncMutex;
use noxu_txn::Txn;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is open and can be used for operations.
    Open,
    /// Transaction has been prepared (XA two-phase commit phase 1).
    ///
    /// Locks are still held; the only valid transitions are
    /// [`Transaction::resolved_commit_after_prepare`] and
    /// [`Transaction::resolved_abort_after_prepare`].  Direct
    /// `commit()` / `abort()` are protocol errors.
    Prepared,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been aborted.
    Aborted,
    /// Transaction must be aborted (error occurred).
    MustAbort,
}

/// A transaction handle.
///
///
///
/// Transaction handles are used to protect database operations.
/// A single Transaction may be used for operations on multiple databases
/// within the same environment.
///
/// Transaction handles are free-threaded; they may be used concurrently
/// by multiple threads. Once committed or aborted, the handle must not
/// be used for any further operations.
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
/// let txn = env.begin_transaction(None, None).unwrap();
/// // ... do operations ...
/// txn.commit().unwrap();
/// ```
pub struct Transaction {
    /// Transaction ID
    id: u64,
    /// Current state
    state: Mutex<TransactionState>,
    /// When this transaction was created
    start_time: Instant,
    /// Whether this is read-only
    read_only: bool,
    /// Optional caller-supplied transaction name (JE
    /// `Transaction.setName(String)`).
    ///
    /// The name is purely diagnostic: it is included in `Debug`
    /// output and structured logs, and may be queried via
    /// [`Transaction::get_name`].  Wave 1C audit cleanup
    /// (transaction-env F22 `setName/getName missing`).
    name: Mutex<Option<String>>,
    /// Durability override (None = use environment default)
    durability: Option<Durability>,
    /// Lock timeout in milliseconds (0 = use environment default)
    lock_timeout_ms: Mutex<u64>,
    /// Transaction timeout in milliseconds (0 = use environment default)
    txn_timeout_ms: Mutex<u64>,
    /// Write-ahead log manager (None when created outside of an Environment).
    log_manager: Option<Arc<LogManager>>,
    /// Internal transaction for lock management and write-set tracking.
    ///
    /// When `Some`, write operations on cursors acquire per-record write locks
    /// via this `Txn` and record abort before-images.  On `abort()`, this `Txn`
    /// releases all locks and collects `UndoRecord`s.
    ///
    /// Relationship between `Transaction` (public) and `Txn` (internal)
    /// in the: `Transaction.txnImpl` field.
    inner_txn: Option<Arc<Mutex<Txn>>>,
    /// Reference to the owning `EnvironmentImpl`.
    ///
    /// Used by `abort()` to look up each modified database by ID and apply
    /// undo records to the B-tree.
    ///
    /// which is used by `Txn.undoLNs()` to call
    /// `EnvironmentImpl.getDatabase(dbId).abort(undoLsn, locker)`.
    env_impl: Option<Arc<SyncMutex<EnvironmentImpl>>>,
    /// Shared registry of active transactions on the owning
    /// `Environment`.  When the transaction reaches a terminal state
    /// (`commit`, `commit_with_durability`, or `abort`) we prune our
    /// entry here so that `Environment::close()` can succeed.
    ///
    /// Resolves F1 of the May 2026 API audit.
    active_txns: Option<Arc<ActiveTxns>>,
}

impl Transaction {
    /// Create a new transaction handle.
    ///
    /// # Arguments
    /// * `id` - Unique transaction ID
    /// * `config` - Transaction configuration
    pub fn new(id: u64, config: TransactionConfig) -> Self {
        observe_gauge_inc!("noxu_db_active_transactions");
        Self {
            id,
            state: Mutex::new(TransactionState::Open),
            start_time: Instant::now(),
            read_only: config.read_only,
            name: Mutex::new(None),
            durability: Some(config.durability),
            lock_timeout_ms: Mutex::new(config.lock_timeout_ms),
            txn_timeout_ms: Mutex::new(config.txn_timeout_ms),
            log_manager: None,
            inner_txn: None,
            env_impl: None,
            active_txns: None,
        }
    }

    /// Create a new transaction backed by a real WAL.
    ///
    /// Called by `Environment::begin_transaction()` to wire the transaction to
    /// the environment's log manager so that commit/abort write WAL entries.
    pub fn with_log_manager(
        id: u64,
        config: TransactionConfig,
        log_manager: Arc<LogManager>,
    ) -> Self {
        observe_gauge_inc!("noxu_db_active_transactions");
        Self {
            id,
            state: Mutex::new(TransactionState::Open),
            start_time: Instant::now(),
            read_only: config.read_only,
            name: Mutex::new(None),
            durability: Some(config.durability),
            lock_timeout_ms: Mutex::new(config.lock_timeout_ms),
            txn_timeout_ms: Mutex::new(config.txn_timeout_ms),
            log_manager: Some(log_manager),
            inner_txn: None,
            env_impl: None,
            active_txns: None,
        }
    }

    /// Wires the `EnvironmentImpl` so that `abort()` can apply undo records.
    ///
    /// Called by `Environment::begin_transaction()` after constructing the
    /// `Transaction`.
    ///
    /// Wiring in the equivalent `Txn` constructor.
    pub fn with_env_impl(
        mut self,
        env_impl: Arc<SyncMutex<EnvironmentImpl>>,
    ) -> Self {
        self.env_impl = Some(env_impl);
        self
    }

    /// Sets the inner `Txn` for lock management and write-set tracking.
    ///
    /// Called by `Environment::begin_transaction()` to wire the transaction to
    /// the environment's `TxnManager` / `LockManager`.
    pub fn with_inner_txn(mut self, txn: Arc<Mutex<Txn>>) -> Self {
        self.inner_txn = Some(txn);
        self
    }

    /// Wires the shared active-transactions registry so that `commit` /
    /// `abort` can prune their own entry on completion.
    ///
    /// Resolves F1 of the May 2026 API audit.
    pub(crate) fn with_active_txns(
        mut self,
        registry: Arc<ActiveTxns>,
    ) -> Self {
        self.active_txns = Some(registry);
        self
    }

    /// Returns a clone of the `Arc<Mutex<Txn>>` inner transaction, if any.
    ///
    /// Used by `Database::make_cursor_for_txn()` to wire the cursor to the
    /// same `Txn` so that write operations lock via the transaction.
    pub fn get_inner_txn(&self) -> Option<Arc<Mutex<Txn>>> {
        self.inner_txn.clone()
    }

    /// Commit the transaction.
    ///
    /// All operations performed under this transaction are made durable
    /// and visible to other transactions.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The transaction is not in `Open` state.
    /// - Writing the `TxnCommit` WAL entry fails (`EnvironmentFailure`
    ///   with reason `LogWrite`, propagated from `write_txn_end`).
    /// - The inner-`Txn` commit fails after the WAL entry has been
    ///   fsynced (e.g. open cursors held against this transaction, or
    ///   inner-state inconsistency surfaced by `check_state`). When
    ///   this happens the transaction is still durably committed; the
    ///   error is propagated so the caller can react to the leak.
    pub fn commit(&self) -> Result<()> {
        observe_span!("txn_commit", txn_id = self.id);
        let _obs_timer = observe_timer_start!();
        observe_counter!("noxu_db_operations_total", "op" => "commit");
        let durability = self.durability.unwrap_or(Durability::COMMIT_SYNC);
        let result = self.commit_with_durability(durability);
        observe_timer_record!(_obs_timer, "noxu_db_operation_duration_seconds", "op" => "commit");
        result
    }

    /// Commit the transaction with specific durability.
    ///
    /// # Arguments
    /// * `durability` - Durability settings for this commit
    ///
    /// # Errors
    /// Returns an error if:
    /// - The transaction is not in `Open` state.
    /// - Writing the `TxnCommit` WAL entry fails (`EnvironmentFailure`
    ///   with reason `LogWrite`, propagated from `write_txn_end`).
    /// - The inner-`Txn` commit fails after the WAL entry has been
    ///   fsynced (e.g. open cursors held against this transaction, or
    ///   inner-state inconsistency surfaced by `check_state`). When
    ///   this happens the transaction is still durably committed; the
    ///   error is propagated so the caller can react to the leak.
    pub fn commit_with_durability(&self, durability: Durability) -> Result<()> {
        self.check_open()?;

        // Write TxnCommit to the WAL before marking committed.
        // Durability controls whether we fsync, flush, or just buffer.
        if !self.read_only
            && let Some(lm) = &self.log_manager
        {
            let (fsync, flush) = match durability.local_sync {
                SyncPolicy::Sync => (true, true),
                SyncPolicy::WriteNoSync => (false, true),
                SyncPolicy::NoSync => (false, false),
            };
            self.write_txn_end(lm, true, fsync, flush)?;
        }

        // Apply cleaner write-path backpressure: if the log write rate exceeds
        // the cleaner's capacity, sleep briefly to let cleaning catch up.
        // Implements CleanerThrottle.getWriteDelay() path in Txn.commit().
        // Extract the throttle Arc while holding the env lock, then
        // drop the lock BEFORE sleeping to avoid blocking other threads.
        if !self.read_only
            && let Some(ref env) = self.env_impl
        {
            let throttle = env.lock().get_cleaner_throttle();
            if let Some(delay) =
                throttle.and_then(|t| t.should_throttle_writer())
            {
                std::thread::sleep(delay);
            }
        }

        // Release per-record locks held by the inner Txn.
        // The inner Txn has no log_manager so it won't write duplicate WAL records.
        //
        // At this point the commit is *durable* on disk: `write_txn_end`
        // above has already fsynced the TxnCommit WAL entry, and recovery
        // will replay this commit on the next environment open. The inner
        // commit only releases `lock_manager` locks and flips the inner
        // state Open → Committed; the data path mutations were applied to
        // the in-memory tree at `db.put()` time.
        //
        // Possible failure modes for `inner.commit()`:
        //   * `has_open_cursors`: a user bug — a cursor on this transaction
        //     was not closed before `commit()`. The data is still
        //     durably committed; the cursor's lifetime contract is
        //     violated. Surfacing this lets the caller find the leak.
        //   * `check_state` (state != Open): the inner txn was flipped to
        //     `MustAbort` by the deadlock detector after our WAL fsync,
        //     or somehow advanced to `Committed`/`Aborted` already. Both
        //     indicate a state-machine inconsistency that needs to be
        //     visible.
        //
        // Either way, we mark the outer state `Committed` first because
        // the durable record says so — returning early before that would
        // leave the outer in `Open`, and a retried `commit()` would
        // append a *second* `TxnCommit` record to the WAL. We then
        // propagate the inner error so the caller can react.
        //
        // The inner Txn's `commit_with_durability` now drains all
        // read and write locks on every error return path, so a
        // failed inner.commit() no longer leaks lock-manager entries
        // until environment close. See `Txn::commit_with_durability`
        // and `Txn::release_all_locks` in noxu-txn for the
        // implementation of this guarantee.
        let inner_err = if let Some(inner) = &self.inner_txn {
            match inner.lock().unwrap().commit() {
                Ok(_) => None,
                Err(e) => {
                    log::error!(
                        "Transaction::commit_with_durability: inner txn \
                         commit failed after WAL fsync (txn is durably \
                         committed; lock_manager locks may be leaked): {e}"
                    );
                    Some(e)
                }
            }
        } else {
            None
        };

        let mut state = self.state.lock().unwrap();
        *state = TransactionState::Committed;
        drop(state);

        // Prune our entry from the environment's active-txns registry so
        // that `Environment::close()` can succeed (F1).  Decrement the
        // active-transactions gauge here (rather than in `commit()`) so
        // that callers of `commit_with_durability` directly are also
        // accounted for (resolves F9 as a side effect).
        if let Some(registry) = &self.active_txns {
            registry.mark_complete(self.id);
        }
        observe_gauge_dec!("noxu_db_active_transactions");

        if let Some(e) = inner_err {
            return Err(NoxuError::from(e));
        }
        Ok(())
    }

    /// Abort the transaction.
    ///
    /// All operations performed under this transaction are rolled back.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The transaction is already committed or aborted.
    /// - Writing the `TxnAbort` WAL entry fails (`EnvironmentFailure`
    ///   with reason `LogWrite`, propagated from `write_txn_end`).
    ///   This path is taken only when the transaction is not read-only
    ///   and a `LogManager` is configured on the environment.
    pub fn abort(&self) -> Result<()> {
        observe_span!("txn_abort", txn_id = self.id);
        observe_counter!("noxu_db_operations_total", "op" => "abort");
        {
            let state = self.state.lock().unwrap();
            match *state {
                TransactionState::Committed => {
                    return Err(NoxuError::OperationNotAllowed(
                        "Cannot abort a committed transaction".to_string(),
                    ));
                }
                TransactionState::Aborted => {
                    return Err(NoxuError::OperationNotAllowed(
                        "Transaction already aborted".to_string(),
                    ));
                }
                TransactionState::Open | TransactionState::MustAbort => {}
                TransactionState::Prepared => {
                    return Err(NoxuError::OperationNotAllowed(
                        "Cannot abort a prepared transaction directly; \
                         use xa_rollback / resolved_abort_after_prepare"
                            .to_string(),
                    ));
                }
            }
        }

        // Write TxnAbort to WAL before marking aborted (no fsync needed).
        if !self.read_only
            && let Some(lm) = &self.log_manager
        {
            self.write_txn_end(lm, false, false, false)?;
        }

        // Apply undo records to the B-tree to restore before-images, then
        // release write locks.  The two steps must happen in this order: while
        // write locks are still held, no reader can observe the in-flight value;
        // once release_all_locks() is called, blocked readers unblock and must
        // already see the restored before-image.
        if let Some(inner) = &self.inner_txn {
            // Phase 1: collect undo records without releasing write locks.
            let undo_records =
                inner.lock().unwrap().abort_collect_undo().unwrap_or_default();

            // Phase 2: apply undo to the B-tree (write locks still held).
            if let Some(env) = &self.env_impl {
                let env_guard = env.lock();
                for undo in undo_records {
                    let Some(abort_key) = undo.abort_key else { continue };
                    let db_id = DatabaseId::new(undo.database_id as i64);
                    let Some(db_arc) = env_guard.get_database_by_id(db_id)
                    else {
                        continue;
                    };
                    let db_guard = db_arc.read();
                    if let Some(tree) = db_guard.get_real_tree() {
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
            }

            // Phase 3: release write locks — blocked readers now unblock and
            // see the restored before-image.
            inner.lock().unwrap().release_all_locks();
        }

        let mut state = self.state.lock().unwrap();
        *state = TransactionState::Aborted;
        // Prune our entry from the environment's active-txns registry so
        // that `Environment::close()` can succeed (F1).
        if let Some(registry) = &self.active_txns {
            registry.mark_complete(self.id);
        }
        observe_gauge_dec!("noxu_db_active_transactions");
        Ok(())
    }

    /// Prepares the transaction for the second phase of XA two-phase
    /// commit.
    ///
    /// Implements the crash-durable contract introduced in wave 3-2:
    ///
    /// 1. Writes a `TxnPrepare` WAL frame containing the txn id, the
    ///    first / last LSN logged by this transaction, and the supplied
    ///    XID components (format_id, gtrid, bqual).  The frame is
    ///    fsynced before this method returns, so a crash immediately
    ///    afterwards still allows recovery to resurrect the prepared
    ///    state.
    /// 2. Marks the inner `Txn` as PREPARED — direct `commit()` and
    ///    `abort()` calls now return `OperationNotAllowed`; only
    ///    `resolved_commit_after_prepare` and
    ///    `resolved_abort_after_prepare` may finalise the transaction.
    /// 3. Locks are RETAINED — prepared transactions hold every lock
    ///    until xa_commit / xa_rollback so concurrent readers cannot
    ///    observe in-flight state.
    /// 4. The persistent prepared-log entry (the `noxu-xa::PreparedLog`
    ///    XID -> timestamp record) is the responsibility of the XA
    ///    layer and is written *after* this method returns.  The WAL
    ///    `TxnPrepare` frame is the source of truth for crash
    ///    durability; the prepared-log database is a convenience for
    ///    operators inspecting in-doubt XIDs without scanning the WAL.
    ///
    /// Returns `Ok(())` on success.  Read-only transactions (no LN
    /// frames written) still take a code path here so the inner Txn
    /// flips to PREPARED, but no `TxnPrepare` frame is emitted — the
    /// XA layer should take its `PrepareResult::ReadOnly` shortcut
    /// rather than calling this method on read-only branches.
    ///
    /// # Errors
    /// * `OperationNotAllowed` if the transaction is not Open.
    /// * `EnvironmentFailure { reason: LogWrite }` if the WAL write or
    ///   fsync fails.
    pub fn prepare(
        &self,
        xid_format_id: i32,
        xid_gtrid: &[u8],
        xid_bqual: &[u8],
    ) -> Result<()> {
        self.check_open()?;

        // Capture first / last LSN from the inner Txn so the recovery
        // code can chain them.  For read-only branches both are
        // NULL_LSN, in which case we skip writing the frame entirely
        // (the prepared XID will appear in the persistent prepared-log
        // database but recovery has nothing to do for it).
        let (first_lsn, last_lsn) = match &self.inner_txn {
            Some(inner) => {
                let g = inner.lock().unwrap();
                (g.first_lsn(), g.last_lsn())
            }
            None => (
                noxu_util::NULL_LSN.as_u64(),
                noxu_util::NULL_LSN.as_u64(),
            ),
        };

        // Write the durable TxnPrepare frame.  Skipped for read-only
        // txns (no inner Txn or no LN frames) to avoid recording an
        // empty prepare that recovery would have nothing to do with.
        if !self.read_only
            && let Some(lm) = &self.log_manager
            && first_lsn != noxu_util::NULL_LSN.as_u64()
        {
            self.write_txn_prepare(
                lm,
                first_lsn,
                last_lsn,
                xid_format_id,
                xid_gtrid,
                xid_bqual,
            )?;
        }

        // Flip the inner Txn into PREPARED state so direct
        // `inner.commit()` / `inner.abort()` are protocol errors.
        // The inner Txn has no `log_manager` of its own (the outer
        // Transaction owns the only LM reference), so its `prepare`
        // call is a pure flag-flip; it does not write a duplicate
        // TxnPrepare frame.
        if let Some(inner) = &self.inner_txn {
            inner
                .lock()
                .unwrap()
                .prepare(
                    xid_format_id,
                    xid_gtrid.to_vec(),
                    xid_bqual.to_vec(),
                )
                .map_err(NoxuError::from)?;
        }

        let mut state = self.state.lock().unwrap();
        *state = TransactionState::Prepared;
        Ok(())
    }

    /// Resolves a prepared transaction with a commit.
    ///
    /// Used by the XA `xa_commit` path.  Bypasses the
    /// `TransactionState::Prepared` guard in `commit_with_durability`
    /// because the prepare already established the commit decision.
    ///
    /// Steps:
    /// 1. Verifies the txn is Prepared.
    /// 2. Writes a `TxnCommit` WAL frame (mirrors `commit()`).
    /// 3. Releases the inner Txn's locks via `resolved_commit_after_prepare`.
    /// 4. Transitions state to Committed and prunes from the active-txns
    ///    registry.
    pub fn resolved_commit_after_prepare(&self) -> Result<()> {
        {
            let state = self.state.lock().unwrap();
            if !matches!(*state, TransactionState::Prepared) {
                return Err(NoxuError::OperationNotAllowed(format!(
                    "resolved_commit_after_prepare: expected Prepared, got {:?}",
                    *state
                )));
            }
        }

        // Write the TxnCommit frame.
        if !self.read_only
            && let Some(lm) = &self.log_manager
        {
            self.write_txn_end(lm, true /* is_commit */, true, true)?;
        }

        // Inner-side resolution: clear IS_PREPARED and run the standard
        // commit path (which releases locks and flips inner state).
        if let Some(inner) = &self.inner_txn {
            inner
                .lock()
                .unwrap()
                .resolved_commit_after_prepare()
                .map_err(NoxuError::from)?;
        }

        let mut state = self.state.lock().unwrap();
        *state = TransactionState::Committed;
        if let Some(registry) = &self.active_txns {
            registry.mark_complete(self.id);
        }
        observe_gauge_dec!("noxu_db_active_transactions");
        Ok(())
    }

    /// Resolves a prepared transaction with an abort.
    pub fn resolved_abort_after_prepare(&self) -> Result<()> {
        {
            let state = self.state.lock().unwrap();
            if !matches!(*state, TransactionState::Prepared) {
                return Err(NoxuError::OperationNotAllowed(format!(
                    "resolved_abort_after_prepare: expected Prepared, got {:?}",
                    *state
                )));
            }
        }

        if !self.read_only
            && let Some(lm) = &self.log_manager
        {
            self.write_txn_end(lm, false /* is_commit */, false, false)?;
        }

        if let Some(inner) = &self.inner_txn {
            inner
                .lock()
                .unwrap()
                .resolved_abort_after_prepare()
                .map_err(NoxuError::from)?;
        }

        let mut state = self.state.lock().unwrap();
        *state = TransactionState::Aborted;
        if let Some(registry) = &self.active_txns {
            registry.mark_complete(self.id);
        }
        observe_gauge_dec!("noxu_db_active_transactions");
        Ok(())
    }

    /// Serializes a `TxnPrepareEntry` and writes it to the WAL with fsync.
    fn write_txn_prepare(
        &self,
        lm: &LogManager,
        first_lsn: u64,
        last_lsn: u64,
        xid_format_id: i32,
        xid_gtrid: &[u8],
        xid_bqual: &[u8],
    ) -> Result<()> {
        use noxu_log::{
            LogEntryType, Provisional, entry::TxnPrepareEntry,
        };

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = TxnPrepareEntry::new(
            self.id as i64,
            timestamp_ms,
            first_lsn,
            last_lsn,
            xid_format_id,
            xid_gtrid.to_vec(),
            xid_bqual.to_vec(),
        )
        .map_err(|e| {
            NoxuError::environment_with_reason(
                crate::error::EnvironmentFailureReason::LogWrite,
                format!("prepare entry encode: {e}"),
            )
        })?;

        let mut buf = Vec::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);

        // fsync=true, flush=true: prepare must be durable before
        // returning so a subsequent crash sees the prepare frame.
        lm.log(LogEntryType::TxnPrepare, &buf, Provisional::No, true, true)
            .map(|_| ())
            .map_err(|e| {
                NoxuError::environment_with_reason(
                    crate::error::EnvironmentFailureReason::LogWrite,
                    e.to_string(),
                )
            })
    }

    /// Serializes a TxnCommit or TxnAbort entry and writes it to `lm`.
    fn write_txn_end(
        &self,
        lm: &LogManager,
        is_commit: bool,
        fsync: bool,
        flush: bool,
    ) -> Result<()> {
        use bytes::BytesMut;
        use noxu_log::{LogEntryType, Provisional, entry::TxnEndEntry};
        use noxu_util::{lsn::NULL_LSN, vlsn::NULL_VLSN};

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = if is_commit {
            TxnEndEntry::new_commit(
                self.id as i64,
                NULL_LSN,
                timestamp,
                0,
                NULL_VLSN,
            )
        } else {
            TxnEndEntry::new_abort(
                self.id as i64,
                NULL_LSN,
                timestamp,
                0,
                NULL_VLSN,
            )
        };

        let entry_type = if is_commit {
            LogEntryType::TxnCommit
        } else {
            LogEntryType::TxnAbort
        };

        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);

        lm.log(entry_type, &buf, Provisional::No, flush, fsync)
            .map(|_| ())
            .map_err(|e| {
                NoxuError::environment_with_reason(
                    crate::error::EnvironmentFailureReason::LogWrite,
                    e.to_string(),
                )
            })
    }

    /// Get the transaction ID.
    pub fn get_id(&self) -> u64 {
        self.id
    }

    /// Set the human-readable name of this transaction.
    ///
    /// Mirrors `Transaction.setName(String)`.  The name is purely
    /// diagnostic — it appears in `Debug` output, structured log
    /// records, and lock-conflict reports.  Wave 1C audit cleanup
    /// (transaction-env F22).
    pub fn set_name<S: Into<String>>(&self, name: S) {
        *self.name.lock().unwrap() = Some(name.into());
    }

    /// Returns the caller-supplied transaction name, if any.
    ///
    /// Mirrors `Transaction.getName()`.
    pub fn get_name(&self) -> Option<String> {
        self.name.lock().unwrap().clone()
    }

    /// Returns the number of locks currently held by this transaction.
    ///
    /// Mirrors `Transaction.getLockStat()` / `Transaction.getNumWriteLocks() +
    /// getNumReadLocks()` (the JE API exposes both counts; we return
    /// the sum because the lock manager partitions reads / writes per
    /// LSN rather than per record).  Returns `0` for transactions that
    /// have not acquired any locks (or for read-only transactions
    /// running with read-uncommitted isolation, which skip lock
    /// acquisition entirely).  Wave 1C audit cleanup
    /// (transaction-env F23 "lock-stat reporting missing").
    pub fn lock_count(&self) -> usize {
        match &self.inner_txn {
            Some(txn) => {
                let g = txn.lock().unwrap();
                g.read_lock_count() + g.write_lock_count()
            }
            None => 0,
        }
    }

    /// Returns `(read_lock_count, write_lock_count)` for this
    /// transaction's lock set.
    ///
    /// Mirrors JE's `Transaction.getNumReadLocks()` /
    /// `getNumWriteLocks()` accessors.  Returns `(0, 0)` for a
    /// transaction that has not acquired any locks.
    pub fn lock_counts(&self) -> (usize, usize) {
        match &self.inner_txn {
            Some(txn) => {
                let g = txn.lock().unwrap();
                (g.read_lock_count(), g.write_lock_count())
            }
            None => (0, 0),
        }
    }

    /// Get the current transaction state.
    pub fn get_state(&self) -> TransactionState {
        *self.state.lock().unwrap()
    }

    /// Check if the transaction is valid (in Open state).
    pub fn is_valid(&self) -> bool {
        matches!(self.get_state(), TransactionState::Open)
    }

    /// Set the lock timeout for this transaction.
    ///
    /// # Arguments
    /// * `timeout_ms` - Lock timeout in milliseconds (0 = use environment default)
    pub fn set_lock_timeout(&self, timeout_ms: u64) {
        *self.lock_timeout_ms.lock().unwrap() = timeout_ms;
    }

    /// Get the lock timeout for this transaction.
    pub fn get_lock_timeout(&self) -> u64 {
        *self.lock_timeout_ms.lock().unwrap()
    }

    /// Set the transaction timeout.
    ///
    /// # Arguments
    /// * `timeout_ms` - Transaction timeout in milliseconds (0 = use environment default)
    pub fn set_txn_timeout(&self, timeout_ms: u64) {
        *self.txn_timeout_ms.lock().unwrap() = timeout_ms;
    }

    /// Get the transaction timeout for this transaction.
    pub fn get_txn_timeout(&self) -> u64 {
        *self.txn_timeout_ms.lock().unwrap()
    }

    /// Get the durability setting for this transaction.
    pub fn get_durability(&self) -> Option<Durability> {
        self.durability
    }

    /// Check if this is a read-only transaction.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Get the elapsed time since transaction start.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Check that the transaction is in Open state.
    ///
    /// # Errors
    /// Returns error if the transaction is not Open.
    fn check_open(&self) -> Result<()> {
        let state = self.get_state();
        match state {
            TransactionState::Open => Ok(()),
            TransactionState::Prepared => Err(NoxuError::OperationNotAllowed(
                "Transaction has been prepared; use xa_commit / xa_rollback"
                    .to_string(),
            )),
            TransactionState::Committed => Err(NoxuError::OperationNotAllowed(
                "Transaction has been committed".to_string(),
            )),
            TransactionState::Aborted => Err(NoxuError::OperationNotAllowed(
                "Transaction has been aborted".to_string(),
            )),
            TransactionState::MustAbort => Err(NoxuError::OperationNotAllowed(
                "Transaction must be aborted due to previous error".to_string(),
            )),
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Warn if transaction wasn't explicitly committed or aborted
        let state = *self.state.lock().unwrap();
        if matches!(state, TransactionState::Open | TransactionState::MustAbort)
        {
            log::warn!(
                "Transaction {} dropped without commit or abort, implicitly aborting",
                self.id
            );
        } else if matches!(state, TransactionState::Prepared) {
            // Prepared txns dropped without resolution simulate a crash
            // — the durable TxnPrepare frame on disk is recovered on the
            // next environment open, where xa_recover() will surface the
            // XID for resolution.  This is intentional and supports the
            // crash-durable XA contract introduced in wave 3-2.
            log::info!(
                "Transaction {} dropped while prepared; XID will be \
                 surfaced via xa_recover() on next open",
                self.id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_transaction() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(1, config);
        assert_eq!(txn.get_id(), 1);
        assert_eq!(txn.get_state(), TransactionState::Open);
        assert!(txn.is_valid());
        assert!(!txn.is_read_only());
    }

    /// Wave 1C audit cleanup (transaction-env F22): set_name / get_name
    /// round-trip and survives commit (the JE shape stays valid until
    /// the txn is dropped).
    #[test]
    fn test_set_name_get_name_round_trip() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(1, config);
        assert_eq!(txn.get_name(), None);

        txn.set_name("workload-import");
        assert_eq!(txn.get_name().as_deref(), Some("workload-import"));

        // Setting again replaces.
        txn.set_name("workload-import-2");
        assert_eq!(txn.get_name().as_deref(), Some("workload-import-2"));
    }

    /// Wave 1C audit cleanup (transaction-env F23): lock_count and
    /// lock_counts return zero when there is no inner Txn (i.e., the
    /// transaction is decorative — unit-test mode without an
    /// EnvironmentImpl wired in).
    #[test]
    fn test_lock_counts_without_inner_txn_are_zero() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(1, config);
        assert_eq!(txn.lock_count(), 0);
        assert_eq!(txn.lock_counts(), (0, 0));
    }

    #[test]
    fn test_read_only_transaction() {
        let config = TransactionConfig::default().with_read_only(true);
        let txn = Transaction::new(2, config);
        assert!(txn.is_read_only());
        assert!(txn.is_valid());
    }

    #[test]
    fn test_commit() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(3, config);
        assert!(txn.commit().is_ok());
        assert_eq!(txn.get_state(), TransactionState::Committed);
        assert!(!txn.is_valid());
    }

    #[test]
    fn test_commit_twice_fails() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(4, config);
        assert!(txn.commit().is_ok());
        let result = txn.commit();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NoxuError::OperationNotAllowed(_)
        ));
    }

    #[test]
    fn test_abort() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(5, config);
        assert!(txn.abort().is_ok());
        assert_eq!(txn.get_state(), TransactionState::Aborted);
        assert!(!txn.is_valid());
    }

    #[test]
    fn test_abort_twice_fails() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(6, config);
        assert!(txn.abort().is_ok());
        let result = txn.abort();
        assert!(result.is_err());
    }

    #[test]
    fn test_commit_after_abort_fails() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(7, config);
        assert!(txn.abort().is_ok());
        let result = txn.commit();
        assert!(result.is_err());
    }

    #[test]
    fn test_abort_after_commit_fails() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(8, config);
        assert!(txn.commit().is_ok());
        let result = txn.abort();
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_timeout() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(9, config);
        assert_eq!(txn.get_lock_timeout(), 0);
        txn.set_lock_timeout(5000);
        assert_eq!(txn.get_lock_timeout(), 5000);
    }

    #[test]
    fn test_txn_timeout() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(10, config);
        assert_eq!(txn.get_txn_timeout(), 0);
        txn.set_txn_timeout(10000);
        assert_eq!(txn.get_txn_timeout(), 10000);
    }

    #[test]
    fn test_durability() {
        let dur = Durability::COMMIT_SYNC;
        let config = TransactionConfig::default().with_durability(dur);
        let txn = Transaction::new(11, config);
        assert_eq!(txn.get_durability(), Some(dur));
    }

    #[test]
    fn test_elapsed_time() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(12, config);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = txn.elapsed();
        assert!(elapsed.as_millis() >= 10);
    }

    #[test]
    fn test_commit_with_durability() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(13, config);
        let dur = Durability::COMMIT_NO_SYNC;
        assert!(txn.commit_with_durability(dur).is_ok());
        assert_eq!(txn.get_state(), TransactionState::Committed);
    }

    #[test]
    fn test_must_abort_state() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(14, config);
        {
            let mut state = txn.state.lock().unwrap();
            *state = TransactionState::MustAbort;
        }
        assert_eq!(txn.get_state(), TransactionState::MustAbort);
        assert!(!txn.is_valid());

        // Can still abort a MustAbort transaction
        assert!(txn.abort().is_ok());
        assert_eq!(txn.get_state(), TransactionState::Aborted);
    }

    #[test]
    fn test_must_abort_cannot_commit() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(15, config);
        {
            let mut state = txn.state.lock().unwrap();
            *state = TransactionState::MustAbort;
        }

        let result = txn.commit();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            NoxuError::OperationNotAllowed(_)
        ));
    }

    #[test]
    fn test_state_transitions() {
        let config = TransactionConfig::default();
        // Open -> Committed
        let txn1 = Transaction::new(16, config.clone());
        assert_eq!(txn1.get_state(), TransactionState::Open);
        txn1.commit().unwrap();
        assert_eq!(txn1.get_state(), TransactionState::Committed);

        // Open -> Aborted
        let txn2 = Transaction::new(17, config);
        assert_eq!(txn2.get_state(), TransactionState::Open);
        txn2.abort().unwrap();
        assert_eq!(txn2.get_state(), TransactionState::Aborted);
    }

    #[test]
    fn test_transaction_id_uniqueness() {
        let config = TransactionConfig::default();
        let txn1 = Transaction::new(100, config.clone());
        let txn2 = Transaction::new(101, config);
        assert_ne!(txn1.get_id(), txn2.get_id());
    }
}
