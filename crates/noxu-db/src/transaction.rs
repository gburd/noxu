//! Transaction handle for Noxu DB.
//!

use crate::durability::{Durability, SyncPolicy};
use crate::environment::ActiveTxns;
use crate::error::{NoxuError, Result};
use crate::transaction_config::TransactionConfig;
use noxu_dbi::{
    AckWaitErrorKind, DatabaseId, EnvironmentImpl, ReplicaAckPolicyKind,
    SharedReplicaAckCoordinator, Trigger,
};
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
/// ```no_run
/// use noxu_db::{Environment, EnvironmentConfig};
/// use std::path::PathBuf;
///
/// let config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
///     .with_allow_create(true)
///     .with_transactional(true);
/// let env = Environment::open(config).unwrap();
/// let txn = env.begin_transaction(None).unwrap();
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
    /// [`Transaction::get_name`].
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
    /// Optional replica-ack coordinator (typically a
    /// `noxu_rep::ReplicatedEnvironment`).  When `Some`, a successful
    /// `commit_with_durability` blocks until the configured
    /// `ReplicaAckPolicy` is satisfied or the durability ack-timeout
    /// elapses, in which case `NoxuError::InsufficientReplicas` is
    /// returned.  Closes finding F1 of
    /// the 2026 review.
    replica_coordinator: Option<SharedReplicaAckCoordinator>,
    /// Per-commit timeout for replica acknowledgments.  Default 5s; set
    /// from the environment's `replica_ack_timeout_ms` (see
    /// `EnvironmentConfig::replica_ack_timeout_ms`) when the
    /// coordinator is installed.
    replica_ack_timeout: std::time::Duration,

    /// Callbacks to run when this transaction aborts.
    ///
    /// C-4 / JE 1-I: used to undo transactional database registrations
    /// when `open_database(Some(txn), ...)` is followed by `txn.abort()`.
    /// Each callback is a `Box<dyn FnOnce() + Send>` so it can capture
    /// shared state without requiring the caller to hold locks.
    abort_callbacks: Mutex<Vec<Box<dyn FnOnce() + Send>>>,

    /// Callbacks to run when this transaction commits.
    ///
    /// C-4 / JE 1-I: used to finalise transactional database registrations
    /// by moving the database name from `pending_names` to `name_map`.
    commit_callbacks: Mutex<Vec<Box<dyn FnOnce() + Send>>>,

    /// Databases modified under this transaction that carry user triggers,
    /// keyed by database id so each is recorded at most once (DB-TRIG).
    ///
    /// JE `Txn.triggerDbs` (a `Set<DatabaseImpl>`) populated by
    /// `TriggerManager.runTriggers` -> `noteTriggerDb`.  On `commit` / `abort`
    /// every recorded database's triggers fire (`runCommitTriggers` /
    /// `runAbortTriggers`), in registration order.
    trigger_dbs: Mutex<Vec<(u64, Vec<Arc<dyn Trigger>>)>>,
}

impl Transaction {
    /// Create a new unconnected transaction handle.
    ///
    /// **Internal** — `pub(crate)` for the no-WAL / in-memory environment
    /// path inside `Environment::begin_transaction`.  Such a handle is not
    /// wired to a WAL when constructed alone, so it is deliberately not part
    /// of the public surface; downstream callers obtain a fully operational
    /// handle via
    /// [`Environment::begin_transaction`][crate::environment::Environment::begin_transaction].
    ///
    /// # Arguments
    /// * `id` - Unique transaction ID
    /// * `config` - Transaction configuration
    pub(crate) fn new(id: u64, config: TransactionConfig) -> Self {
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
            replica_coordinator: None,
            replica_ack_timeout: std::time::Duration::from_secs(5),
            abort_callbacks: Mutex::new(Vec::new()),
            commit_callbacks: Mutex::new(Vec::new()),
            trigger_dbs: Mutex::new(Vec::new()),
        }
    }

    /// Create a new transaction backed by a real WAL.
    ///
    /// Called by `Environment::begin_transaction()` to wire the transaction to
    /// the environment's log manager so that commit/abort write WAL entries.
    ///
    /// **Internal** — `pub(crate)` for cross-module wiring within the
    /// Noxu DB engine; not part of the public surface.
    /// `LogManager` is not re-exported by `noxu-db`.
    pub(crate) fn with_log_manager(
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
            replica_coordinator: None,
            replica_ack_timeout: std::time::Duration::from_secs(5),
            abort_callbacks: Mutex::new(Vec::new()),
            commit_callbacks: Mutex::new(Vec::new()),
            trigger_dbs: Mutex::new(Vec::new()),
        }
    }

    /// Wires the `EnvironmentImpl` so that `abort()` can apply undo records.
    ///
    /// Called by `Environment::begin_transaction()` after constructing the
    /// `Transaction`.
    ///
    /// **Internal** — `EnvironmentImpl` is not re-exported by `noxu-db`.
    pub(crate) fn with_env_impl(
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
    ///
    /// **Internal** — `noxu_txn::Txn` is not re-exported by `noxu-db`.
    pub(crate) fn with_inner_txn(mut self, txn: Arc<Mutex<Txn>>) -> Self {
        self.inner_txn = Some(txn);
        self
    }

    /// Removes the inner `Txn` from the environment's `TxnManager` (its
    /// `all_txns` map and the lock manager's locker-label map) — the
    /// counterpart to `TxnManager::begin_txn`, which the explicit-transaction
    /// commit/abort paths previously never called (review F-5).
    ///
    /// Without this, `TxnManager::all_txns` and the locker-label map grow
    /// without bound for the process lifetime, `n_active_txns()` reports a
    /// monotonically increasing (wrong) count, and `n_commits`/`n_aborts`
    /// undercount. The inner `Txn`'s locker id (a separate id space from
    /// `Transaction::id`) is the `all_txns` key, so we use it here.
    ///
    /// Lock discipline: the inner-txn lock is read in a tight scope and
    /// released before the (separate) environment lock is taken, and both
    /// commit/abort paths have already released any env lock by this point.
    fn unregister_inner_txn(&self, committed: bool) {
        let Some(inner) = self.inner_txn.as_ref() else {
            return;
        };
        let g = inner.lock().unwrap();
        let inner_id = g.id_as_locker();
        // TXN-2: mirror JE TxnManager.unRegisterTxn nActiveSerializable path.
        // Read the isolation level before releasing the txn from the manager.
        let was_serializable = g.is_serializable();
        drop(g);
        if let Some(env) = self.env_impl.as_ref() {
            let guard = env.lock();
            let tm = guard.get_txn_manager();
            if committed {
                tm.commit_txn(inner_id);
            } else {
                tm.abort_txn(inner_id);
            }
            // Decrement the serializable counter exactly once, after the
            // all_txns entry is removed, so n_active_serializable ≤
            // n_active at all times.  Matches JE TxnManager.unRegisterTxn
            // `nActiveSerializable.decrementAndGet()` ordering.
            if was_serializable {
                tm.unregister_serializable();
            }
        }
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

    /// Wires the replica-ack coordinator from the owning `Environment`.
    ///
    /// When set, a successful `commit_with_durability` blocks until the
    /// configured `ReplicaAckPolicy` is satisfied or `replica_ack_timeout`
    /// elapses, in which case `NoxuError::InsufficientReplicas` is
    /// returned.
    ///
    /// Closes finding F1 of the 2026 review.
    pub(crate) fn with_replica_coordinator(
        mut self,
        coord: SharedReplicaAckCoordinator,
        ack_timeout: std::time::Duration,
    ) -> Self {
        self.replica_coordinator = Some(coord);
        self.replica_ack_timeout = ack_timeout;
        self
    }

    /// Returns a clone of the `Arc<Mutex<Txn>>` inner transaction, if any.
    ///
    /// Used by `Database::make_cursor_for_txn()` and the XA layer to wire a
    /// cursor/branch to the same `Txn` so that write operations lock via the
    /// transaction.
    ///
    /// **Internal** — `#[doc(hidden)]` cross-crate wiring point.
    /// `noxu_txn::Txn` is not re-exported by `noxu-db`, so the return type is
    /// effectively un-nameable by downstream users; this is not part of the
    /// stable surface.
    #[doc(hidden)]
    pub fn get_inner_txn(&self) -> Option<Arc<Mutex<Txn>>> {
        self.inner_txn.clone()
    }

    /// Register a callback to run when this transaction aborts.
    ///
    /// Used by `Environment::open_database()` to roll back a transactional
    /// database creation if the owning transaction is aborted (C-4 / JE 1-I).
    /// The callback is invoked from within `abort()`, after WAL writes but
    /// before the outer state is marked `Aborted`.
    pub fn register_abort_callback<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.abort_callbacks.lock().unwrap().push(Box::new(f));
    }

    /// Register a callback to run when this transaction commits.
    ///
    /// Used by `Environment::open_database()` to finalise a transactional
    /// database creation when the owning transaction commits (C-4 / JE 1-I).
    /// The callback is invoked from within `commit_with_durability()`, after
    /// the WAL entry is written and locks are released.
    pub fn register_commit_callback<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.commit_callbacks.lock().unwrap().push(Box::new(f));
    }

    /// Record that a triggered database was modified under this transaction
    /// (DB-TRIG).
    ///
    /// Idempotent per database id: the first write to a given database under
    /// this transaction records its triggers; subsequent writes are no-ops.
    /// On `commit` / `abort` every recorded database's triggers fire in
    /// registration order.
    ///
    /// JE `Txn.noteTriggerDb` (a `Set<DatabaseImpl>` populated from
    /// `TriggerManager.runTriggers`).
    pub(crate) fn note_trigger_db(
        &self,
        db_id: u64,
        triggers: &[Arc<dyn Trigger>],
    ) {
        if triggers.is_empty() {
            return;
        }
        let mut dbs = self.trigger_dbs.lock().unwrap();
        if dbs.iter().any(|(id, _)| *id == db_id) {
            return;
        }
        dbs.push((db_id, triggers.to_vec()));
    }

    /// Fire `TransactionTrigger.commit` for every recorded triggered database,
    /// in registration order (DB-TRIG).  JE
    /// `TriggerManager.runCommitTriggers`.
    fn run_commit_triggers(&self) {
        let dbs = std::mem::take(&mut *self.trigger_dbs.lock().unwrap());
        for (_db_id, triggers) in dbs {
            for trigger in triggers {
                trigger.commit(self.id);
            }
        }
    }

    /// Fire `TransactionTrigger.abort` for every recorded triggered database,
    /// in registration order (DB-TRIG).  JE
    /// `TriggerManager.runAbortTriggers`.
    fn run_abort_triggers(&self) {
        let dbs = std::mem::take(&mut *self.trigger_dbs.lock().unwrap());
        for (_db_id, triggers) in dbs {
            for trigger in triggers {
                trigger.abort(self.id);
            }
        }
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

        // F1 (rep audit): wait for replica acknowledgments before returning
        // success.  This wait happens AFTER the local WAL is durable but
        // BEFORE the inner txn releases its locks, mirroring BDB-JE
        // `Txn.preLogCommitHook` / `commit(Durability)` ordering: the
        // master is durable locally and replicas are notified, but the
        // commit only "returns" once `replica_ack` is satisfied.
        //
        // If no coordinator is wired (non-replicated env) or the policy
        // is `None`, the wait is skipped.  Read-only commits never need
        // replica acks.  Captured failure is propagated at the end of
        // the function after lock release so the caller observes a
        // typed `NoxuError::InsufficientReplicas` rather than a state
        // leak.
        let ack_err: Option<NoxuError> = if !self.read_only
            && durability.replica_ack
                != crate::durability::ReplicaAckPolicy::None
            && let Some(coord) = &self.replica_coordinator
        {
            match coord.await_replica_acks(
                durability.replica_ack.as_kind(),
                self.replica_ack_timeout,
            ) {
                Ok(_received) => None,
                Err(e) => match e.kind {
                    AckWaitErrorKind::NotMaster => {
                        Some(NoxuError::ReplicaWrite)
                    }
                    AckWaitErrorKind::Timeout | AckWaitErrorKind::Shutdown => {
                        Some(NoxuError::InsufficientReplicas {
                            required: e.needed,
                            available: e.received,
                        })
                    }
                },
            }
        } else {
            None
        };

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

        // C-4 / JE 1-I: run commit callbacks (transactional database
        // registration finalisation).
        let callbacks: Vec<Box<dyn FnOnce() + Send>> =
            std::mem::take(&mut *self.commit_callbacks.lock().unwrap());
        for cb in callbacks {
            cb();
        }

        // DB-TRIG: fire TransactionTrigger.commit for every database modified
        // under this transaction, in registration order.  JE
        // `TriggerManager.runCommitTriggers(txn)`.
        self.run_commit_triggers();

        // Prune our entry from the environment's active-txns registry so
        // that `Environment::close()` can succeed (F1).  Decrement the
        // active-transactions gauge here (rather than in `commit()`) so
        // that callers of `commit_with_durability` directly are also
        // accounted for (resolves F9 as a side effect).
        if let Some(registry) = &self.active_txns {
            registry.mark_complete(self.id);
        }
        // F-5: counterpart to begin_txn — remove the inner Txn from
        // TxnManager (all_txns + locker label) to avoid an unbounded leak.
        self.unregister_inner_txn(true);
        observe_gauge_dec!("noxu_db_active_transactions");

        if let Some(e) = inner_err {
            return Err(NoxuError::from(e));
        }
        // F1: surface any replica-ack failure last, after the local
        // commit has fully released locks.  The local commit is durable;
        // returning this error tells the caller the durability policy
        // was not satisfied so they can retry or rollback at the
        // application layer.
        if let Some(e) = ack_err {
            return Err(e);
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
            // Poison-safe: abort is reachable from Drop and a prior panic
            // on this txn may have poisoned its own locks; recover and abort.
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
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
            let mut undo_records = inner
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .abort_collect_undo()
                .unwrap_or_default();

            // Apply undo in reverse-operation order (newest LSN first).
            //
            // The in-memory write-lock map is a HashMap (no order), so the
            // raw `undo_records` order is non-deterministic.  When the same
            // key is touched multiple times in one txn (e.g. delete →
            // re-insert in SR9465), the undo records carry conflicting
            // intents:
            //   - DELETE undo  (abort_data=orig)        : restore the slot
            //   - INSERT undo  (abort_known_deleted=t)  : remove the slot
            // Applying these in arbitrary order can leave the tree in either
            // "correct" or "slot deleted" depending on iteration luck.
            //
            // The recovery path's backward log scan already applies undo
            // newest-first; we mirror that here so the in-memory abort and
            // crash-recovery undo are observationally identical.  Sorting by
            // `current_lsn` descending is sufficient because LSNs are
            // monotonic per-WAL-write.
            undo_records.sort_by_key(|r| std::cmp::Reverse(r.current_lsn));

            // Phase 2: apply undo to the B-tree (write locks still held).
            //
            // H-1 (the 2026 review F-2.2): acquire env lock only for
            // the fast database-handle lookup, then drop it immediately.
            // This prevents the entire abort undo loop from serialising all
            // concurrent readers/writers against the EnvironmentImpl mutex.
            //
            // Algorithm:
            //   a) Collect unique database IDs referenced by the undo set.
            //   b) For each ID, briefly lock env, clone the Arc<RwLock<DatabaseImpl>>,
            //      and immediately release the env lock.
            //   c) Apply all undo records without ever holding the env lock.
            //
            // Safety: the database Arcs are ref-counted; even if a concurrent
            // `env.remove_database()` call drops the EnvironmentImpl's own Arc,
            // our cloned Arc keeps the DatabaseImpl alive for the duration of
            // the undo loop.
            if let Some(env) = &self.env_impl {
                // Step (a+b): collect database handles with minimal lock hold time.
                use std::collections::HashMap;
                let mut db_handles: HashMap<
                    i64,
                    Arc<noxu_sync::RwLock<noxu_dbi::DatabaseImpl>>,
                > = HashMap::new();
                for undo in &undo_records {
                    let db_id_raw = undo.database_id as i64;
                    if db_handles.contains_key(&db_id_raw) {
                        continue;
                    }
                    // Brief env lock: lookup only.
                    let guard = env.lock();
                    if let Some(arc) =
                        guard.get_database_by_id(DatabaseId::new(db_id_raw))
                    {
                        db_handles.insert(db_id_raw, arc);
                    }
                    // env lock released here — drop(guard) implicit at end of block
                }

                // Step (c): apply undo records without holding env lock.
                for undo in undo_records {
                    let Some(abort_key) = undo.abort_key else { continue };
                    let db_id_raw = undo.database_id as i64;
                    let Some(db_arc) = db_handles.get(&db_id_raw) else {
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
                            if let Ok(is_new) =
                                tree.insert(abort_key, abort_data, lsn)
                                && is_new
                            {
                                // Restoring a slot that the aborted txn had
                                // deleted: the in-memory delete already
                                // decremented the counter, so the restore
                                // must re-bump it.
                                db_guard.increment_entry_count();
                            }
                        }
                    }
                }
            }

            // Phase 3: release write locks — blocked readers now unblock and
            // see the restored before-image.
            inner.lock().unwrap_or_else(|p| p.into_inner()).release_all_locks();
        }

        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        *state = TransactionState::Aborted;
        drop(state);

        // C-4 / JE 1-I: run abort callbacks (transactional database
        // registration rollback).
        let callbacks: Vec<Box<dyn FnOnce() + Send>> = std::mem::take(
            &mut *self
                .abort_callbacks
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        for cb in callbacks {
            cb();
        }

        // DB-TRIG: fire TransactionTrigger.abort for every database modified
        // under this transaction, in registration order.  The data-change
        // undo above has already restored the before-images, so an abort
        // trigger observes the rolled-back state.  JE
        // `TriggerManager.runAbortTriggers(txn)`.
        self.run_abort_triggers();

        // Prune our entry from the environment's active-txns registry so
        // that `Environment::close()` can succeed (F1).
        if let Some(registry) = &self.active_txns {
            registry.mark_complete(self.id);
        }
        // F-5: counterpart to begin_txn — remove the inner Txn from
        // TxnManager (all_txns + locker label) to avoid an unbounded leak.
        self.unregister_inner_txn(false);
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
            None => {
                (noxu_util::NULL_LSN.as_u64(), noxu_util::NULL_LSN.as_u64())
            }
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
                .prepare(xid_format_id, xid_gtrid.to_vec(), xid_bqual.to_vec())
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
        drop(state);
        // Run commit callbacks (e.g. transactional database registration).
        let cbs: Vec<Box<dyn FnOnce() + Send>> =
            std::mem::take(&mut *self.commit_callbacks.lock().unwrap());
        for cb in cbs {
            cb();
        }
        if let Some(registry) = &self.active_txns {
            registry.mark_complete(self.id);
        }
        // F-5: counterpart to begin_txn — remove the inner Txn from
        // TxnManager (all_txns + locker label) to avoid an unbounded leak.
        self.unregister_inner_txn(true);
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

        // Apply undo records to the B-tree to restore before-images, then
        // release write locks.  Same 3-phase ordering as `Transaction::abort()`:
        // collect undo → apply → release locks.  See the matching comment
        // in `Transaction::abort` for the rationale (no reader sees the
        // in-flight value until the before-image is back in the tree).
        if let Some(inner) = &self.inner_txn {
            // First, clear the IS_PREPARED flag on the inner Txn so that
            // `abort_collect_undo()` (which calls into `Txn::abort`) does
            // not refuse with InvalidTransaction { state: PREPARED }.
            // We do this via the inner's resolved-abort path, which
            // performs `txn_flags &= !IS_PREPARED` and then runs `abort()`.
            // Unfortunately that consumes the locks; we want the
            // pre-release behaviour instead, so flip the flag manually
            // and then run abort_collect_undo.
            let mut undo_records = {
                let mut g = inner.lock().unwrap();
                // Undo the IS_PREPARED flag so abort_collect_undo doesn't
                // refuse.  Inner state is still Open at this point.
                g.clear_prepared_flag();
                g.abort_collect_undo().unwrap_or_default()
            };

            // See `abort()` above: undo must be applied newest-LSN first so
            // that delete-then-reinsert sequences in the same txn are
            // unwound in reverse-operation order, matching the recovery
            // path's backward log scan.
            undo_records.sort_by_key(|r| std::cmp::Reverse(r.current_lsn));

            if let Some(env) = &self.env_impl {
                let env_guard = env.lock();
                for undo in undo_records {
                    let Some(abort_key) = undo.abort_key else { continue };
                    let db_id =
                        noxu_dbi::DatabaseId::new(undo.database_id as i64);
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
                            if let Ok(is_new) =
                                tree.insert(abort_key, abort_data, lsn)
                                && is_new
                            {
                                db_guard.increment_entry_count();
                            }
                        }
                    }
                }
            }

            inner.lock().unwrap().release_all_locks();
        }

        let mut state = self.state.lock().unwrap();
        *state = TransactionState::Aborted;
        drop(state);
        // Run abort callbacks (e.g. transactional database registration rollback).
        let cbs: Vec<Box<dyn FnOnce() + Send>> =
            std::mem::take(&mut *self.abort_callbacks.lock().unwrap());
        for cb in cbs {
            cb();
        }
        if let Some(registry) = &self.active_txns {
            registry.mark_complete(self.id);
        }
        // F-5: counterpart to begin_txn — remove the inner Txn from
        // TxnManager (all_txns + locker label) to avoid an unbounded leak.
        self.unregister_inner_txn(false);
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
        use noxu_log::{LogEntryType, Provisional, entry::TxnPrepareEntry};

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

    /// Returns the transaction ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Set the human-readable name of this transaction.
    ///
    /// Mirrors `Transaction.setName(String)`.  The name is purely
    /// diagnostic — it appears in `Debug` output, structured log
    /// records, and lock-conflict reports.
    /// (transaction-env F22).
    pub fn set_name<S: Into<String>>(&self, name: S) {
        *self.name.lock().unwrap() = Some(name.into());
    }

    /// Returns the caller-supplied transaction name, if any.
    ///
    /// Mirrors `Transaction.getName()`.
    pub fn name(&self) -> Option<String> {
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
    /// acquisition entirely).
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

    /// Returns the current transaction state.
    pub fn state(&self) -> TransactionState {
        *self.state.lock().unwrap()
    }

    /// Check if the transaction is valid (in Open state).
    pub fn is_valid(&self) -> bool {
        matches!(self.state(), TransactionState::Open)
    }

    /// Set the lock timeout for this transaction.
    ///
    /// # Arguments
    /// * `timeout_ms` - Lock timeout in milliseconds (0 = use environment default)
    pub fn set_lock_timeout(&self, timeout_ms: u64) {
        *self.lock_timeout_ms.lock().unwrap() = timeout_ms;
    }

    /// Returns the lock timeout for this transaction.
    pub fn lock_timeout(&self) -> u64 {
        *self.lock_timeout_ms.lock().unwrap()
    }

    /// Set the transaction timeout.
    ///
    /// # Arguments
    /// * `timeout_ms` - Transaction timeout in milliseconds (0 = use environment default)
    pub fn set_txn_timeout(&self, timeout_ms: u64) {
        *self.txn_timeout_ms.lock().unwrap() = timeout_ms;
    }

    /// Returns the transaction timeout for this transaction.
    pub fn txn_timeout(&self) -> u64 {
        *self.txn_timeout_ms.lock().unwrap()
    }

    /// Returns the durability setting for this transaction.
    pub fn durability(&self) -> Option<Durability> {
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
        let state = self.state();
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
        // Audit transaction-env F10 (Wave 2C-4): if the txn is still in
        // a non-terminal state at drop time, perform an actual abort
        // (release locks, apply undo, prune from active-txn registry,
        // decrement gauge) instead of just logging a warning.
        //
        // Poison-safe: a panic elsewhere may have poisoned `state`.  In Drop we
        // MUST NOT unwrap() a poisoned lock — that would turn a recoverable
        // poison into a double-panic and abort the whole process.  Recover the
        // guard with into_inner() and proceed with a best-effort abort.
        let state =
            *self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(state, TransactionState::Open | TransactionState::MustAbort)
        {
            log::warn!(
                "Transaction {} dropped without commit or abort, \
                 implicitly aborting",
                self.id
            );
            // Best-effort abort.  Errors are swallowed because Drop
            // cannot return Result; any failure (e.g., WAL write error)
            // is still observable through the abort path's logging.
            if let Err(e) = self.abort() {
                log::error!(
                    "Transaction {} implicit abort on drop failed: {e}",
                    self.id,
                );
            }
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
        assert_eq!(txn.id(), 1);
        assert_eq!(txn.state(), TransactionState::Open);
        assert!(txn.is_valid());
        assert!(!txn.is_read_only());
    }

    /// `set_name` / `get_name`
    /// round-trip and survives commit (the JE shape stays valid until
    /// the txn is dropped).
    #[test]
    fn test_set_name_get_name_round_trip() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(1, config);
        assert_eq!(txn.name(), None);

        txn.set_name("workload-import");
        assert_eq!(txn.name().as_deref(), Some("workload-import"));

        // Setting again replaces.
        txn.set_name("workload-import-2");
        assert_eq!(txn.name().as_deref(), Some("workload-import-2"));
    }

    /// `lock_count` and
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
        assert_eq!(txn.state(), TransactionState::Committed);
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
        assert_eq!(txn.state(), TransactionState::Aborted);
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
        assert_eq!(txn.lock_timeout(), 0);
        txn.set_lock_timeout(5000);
        assert_eq!(txn.lock_timeout(), 5000);
    }

    #[test]
    fn test_txn_timeout() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(10, config);
        assert_eq!(txn.txn_timeout(), 0);
        txn.set_txn_timeout(10000);
        assert_eq!(txn.txn_timeout(), 10000);
    }

    #[test]
    fn test_durability() {
        let dur = Durability::COMMIT_SYNC;
        let config = TransactionConfig::default().with_durability(dur);
        let txn = Transaction::new(11, config);
        assert_eq!(txn.durability(), Some(dur));
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
        assert_eq!(txn.state(), TransactionState::Committed);
    }

    #[test]
    fn test_must_abort_state() {
        let config = TransactionConfig::default();
        let txn = Transaction::new(14, config);
        {
            let mut state = txn.state.lock().unwrap();
            *state = TransactionState::MustAbort;
        }
        assert_eq!(txn.state(), TransactionState::MustAbort);
        assert!(!txn.is_valid());

        // Can still abort a MustAbort transaction
        assert!(txn.abort().is_ok());
        assert_eq!(txn.state(), TransactionState::Aborted);
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
        assert_eq!(txn1.state(), TransactionState::Open);
        txn1.commit().unwrap();
        assert_eq!(txn1.state(), TransactionState::Committed);

        // Open -> Aborted
        let txn2 = Transaction::new(17, config);
        assert_eq!(txn2.state(), TransactionState::Open);
        txn2.abort().unwrap();
        assert_eq!(txn2.state(), TransactionState::Aborted);
    }

    #[test]
    fn test_transaction_id_uniqueness() {
        let config = TransactionConfig::default();
        let txn1 = Transaction::new(100, config.clone());
        let txn2 = Transaction::new(101, config);
        assert_ne!(txn1.id(), txn2.id());
    }
}
