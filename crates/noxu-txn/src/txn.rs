//! Transaction implementation.
//!

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Instant;

use noxu_log::{LogEntryType, LogManager, Provisional};
use noxu_util::lsn::{Lsn, NULL_LSN};

use crate::group_commit::GroupCommit;
use crate::txn_abort::TxnAbort;
use crate::txn_commit::TxnCommit;
use crate::txn_state::TxnState;
use crate::{
    LockManager, LockResult, LockType, Locker, TxnError, WriteLockInfo,
};

/// A single undo record produced when a transaction aborts.
///
/// Corresponds to the information extracted from `WriteLockInfo` during
/// `Txn.undo()`. The engine/recovery layer uses these records to restore
/// the before-image of each modified record.
///
/// Per-entry undo information.
#[derive(Debug, Clone)]
pub struct UndoRecord {
    /// LSN of the log entry that must be marked obsolete (the current version).
    pub current_lsn: u64,
    /// LSN of the abort (before-image) version.
    pub abort_lsn: u64,
    /// True if the abort version was a known-deleted record (i.e. the record
    /// did not exist before this transaction).
    pub abort_known_deleted: bool,
    /// Embedded data of the abort version, when the LN data is stored directly
    /// in the BIN slot ("embedded LN" / BIN-delta path).
    pub abort_data: Option<Vec<u8>>,
    /// Key of the abort version (only set when key updates are allowed).
    pub abort_key: Option<Vec<u8>>,
    /// ID of the database that was modified.
    ///
    /// Used by the engine layer to route undo to the correct database's tree.
    pub database_id: u64,
}

/// Durability policy for transaction commit.
///
/// Controls whether the log is flushed/fsynced on commit.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Flush and fsync before returning from commit.  Guarantees data is on
    /// durable storage.  This is the default.
    ///
    /// 
    CommitSync,
    /// Flush write buffers (OS page cache) but do not fsync.  Data survives
    /// process crash but not OS/power failure.
    ///
    /// 
    CommitWriteNoSync,
    /// Do not flush or fsync.  Fastest; data may be lost on crash.
    ///
    /// 
    CommitNoSync,
}

/// Internal transaction flags.
const IS_PREPARED: u8 = 1;
const PAST_ROLLBACK: u8 = 4;
const IMPORTUNATE: u8 = 8;

/// A Txn is the internal representation of a transaction.
///
/// This class must support multi-threaded use. A single Txn can be used
/// by multiple threads via cursor operations.
///
/// 
pub struct Txn {
    /// Transaction ID.
    id: i64,
    /// Reference to the lock manager.
    lock_manager: Arc<LockManager>,
    /// Current transaction state.
    state: TxnState,
    /// Internal flags.
    txn_flags: u8,

    /// Set of LSNs holding read locks.
    /// In this is a TinyHashSet for memory efficiency.
    read_locks: HashSet<u64>,
    /// Map of LSN -> WriteLockInfo for write locks.
    /// The write lock info is needed for undo operations on abort.
    write_locks: HashMap<u64, WriteLockInfo>,

    /// The LSN of the last log entry written by this txn.
    /// Used to chain undo records.
    last_lsn: u64,

    /// The first LSN written by this txn.
    first_lsn: u64,

    /// Number of cursors currently using this txn.
    cursor_count: AtomicI32,

    /// Lock timeout in milliseconds.
    lock_timeout_ms: u64,
    /// Transaction timeout in milliseconds (0 = no timeout).
    txn_timeout_ms: u64,
    /// When this txn started (for timeout checking).
    txn_start: Instant,

    /// Read-uncommitted default.
    read_uncommitted_default: bool,
    /// Whether this txn can preempt other lockers' locks.
    importunate: bool,

    /// Serializable (repeatable-read) isolation level.
    ///
    /// When true, read locks are retained through commit/abort.
    ///
    serializable_isolation: bool,

    /// Read-committed isolation level.
    ///
    /// When true, read locks are released as soon as the cursor moves.
    ///
    read_committed_isolation: bool,

    /// Undo records collected during `abort()`.
    ///
    /// Populated by `abort()` from the `WriteLockInfo` of each write lock.
    /// Consumed by `take_undo_records()`.
    undo_records: Vec<UndoRecord>,

    /// Optional reference to the LogManager.
    ///
    /// When `Some`, `commit()` and `abort()` write TxnCommit/TxnAbort records
    /// to the persistent log.  When `None` (e.g. in unit tests) the log-write
    /// step is skipped and `NULL_LSN` is returned.
    ///
    /// Option A from the task spec: simpler than holding a full EnvironmentImpl
    /// and avoids circular dependencies.
    log_manager: Option<Arc<LogManager>>,

    /// LSN of the TxnCommit record written during `commit()`.
    /// `NULL_LSN` until commit is called.
    commit_lsn: u64,

    /// LSN of the TxnAbort record written during `abort()`.
    /// `NULL_LSN` until abort is called (and if the txn had logged entries).
    abort_lsn: u64,

    /// Hook called immediately before writing the TxnCommit log entry.
    ///
    /// Used by replication (`MasterTxn`) to pre-register the commit in VLSN
    /// tracking before it becomes durable.
    ///
    /// 
    pre_commit_hook: Option<Box<dyn Fn() + Send + Sync>>,

    /// Hook called immediately after the TxnCommit log entry is written.
    ///
    /// Used by replication to queue the commit LSN for ACK tracking.
    ///
    /// 
    post_commit_hook: Option<Box<dyn Fn(Lsn) + Send + Sync>>,

    /// Optional group-commit handler (Master or Replica).
    ///
    /// When `Some` and enabled, `commit()` calls `buffer_commit()` after
    /// writing the TxnCommit WAL entry to decide whether to fsync or defer.
    ///
    /// Commit()`.
    group_commit: Option<Arc<dyn GroupCommit>>,
}

impl Txn {
    /// Creates a new transaction without a log manager.
    ///
    /// Commits and aborts will not write to the persistent log.  Use this
    /// constructor in unit tests or when a LogManager is not available.
    pub fn new(id: i64, lock_manager: Arc<LockManager>) -> Self {
        Txn {
            id,
            lock_manager,
            state: TxnState::Open,
            txn_flags: 0,
            read_locks: HashSet::new(),
            write_locks: HashMap::new(),
            last_lsn: NULL_LSN.as_u64(),
            first_lsn: NULL_LSN.as_u64(),
            cursor_count: AtomicI32::new(0),
            lock_timeout_ms: 500, // default 500ms
            txn_timeout_ms: 0,    // no timeout
            txn_start: Instant::now(),
            read_uncommitted_default: false,
            importunate: false,
            serializable_isolation: false,
            read_committed_isolation: false,
            undo_records: Vec::new(),
            log_manager: None,
            commit_lsn: NULL_LSN.as_u64(),
            abort_lsn: NULL_LSN.as_u64(),
            pre_commit_hook: None,
            post_commit_hook: None,
            group_commit: None,
        }
    }

    /// Attaches a group-commit handler to this transaction.
    ///
    /// When set, `commit()` calls `buffer_commit()` after writing the
    /// TxnCommit WAL entry:
    /// - if `buffer_commit()` returns `true` (batched): skip the fsync.
    /// - if `buffer_commit()` returns `false` (flush now): call `flush_sync()`.
    ///
    /// `TxnManager.groupCommit` wiring in the equivalent `Txn.commit()`.
    pub fn with_group_commit(mut self, gc: Arc<dyn GroupCommit>) -> Self {
        self.group_commit = Some(gc);
        self
    }

    /// Sets the group-commit handler on an existing transaction.
    ///
    /// Same semantics as `with_group_commit()` but works on `&mut self`.
    pub fn set_group_commit(&mut self, gc: Arc<dyn GroupCommit>) {
        self.group_commit = Some(gc);
    }

    /// Creates a new transaction wired to a LogManager.
    ///
    /// When this constructor is used, `commit()` writes a `TxnCommit` record
    /// and `abort()` writes a `TxnAbort` record to `log_manager`, making the
    /// transaction durable.
    ///
    /// `Txn` holds a reference to
    /// `EnvironmentImpl` (which owns the `LogManager`).
    pub fn with_log_manager(
        id: i64,
        lock_manager: Arc<LockManager>,
        log_manager: Arc<LogManager>,
    ) -> Self {
        let mut txn = Self::new(id, lock_manager);
        txn.log_manager = Some(log_manager);
        txn
    }

    /// Returns the commit LSN (`NULL_LSN` if not yet committed or read-only).
    pub fn commit_lsn(&self) -> Lsn {
        Lsn::from_u64(self.commit_lsn)
    }

    /// Returns the abort LSN (`NULL_LSN` if not yet aborted or read-only).
    pub fn abort_lsn(&self) -> Lsn {
        Lsn::from_u64(self.abort_lsn)
    }

    /// Commits with an explicit durability policy.
    ///
    /// 
    ///
    /// - `CommitSync` (default): flush and fsync before returning.
    /// - `CommitWriteNoSync`: write to OS page cache but don't fsync.
    /// - `CommitNoSync`: don't flush; fastest but least durable.
    pub fn commit_with_durability(&mut self, durability: Durability) -> Result<Lsn, TxnError> {
        self.check_state()?;
        if self.has_open_cursors() {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "has open cursors".into(),
            });
        }
        for lsn in self.read_locks.drain().collect::<Vec<_>>() {
            let _ = self.lock_manager.release(lsn, self.id);
        }
        let want_sync = matches!(durability, Durability::CommitSync);
        let assigned_lsn = if self.has_logged_entries() {
            if let Some(ref hook) = self.pre_commit_hook {
                hook();
            }
            let commit = TxnCommit::new(self.id, self.last_lsn, 0, 0);
            let mut payload = Vec::with_capacity(commit.log_size());
            commit.write_to_log(&mut payload);

            // Write the TxnCommit WAL entry. The fsync is always deferred
            // here so that group commit can decide whether to coalesce it.
            // Txn.commit() writes the entry then calls flushTo(commitLsn)
            // separately, which is how GroupCommit intercepts the fsync.
            let commit_lsn = self.log_entry(LogEntryType::TxnCommit, &payload, false)?;

            if let Some(ref hook) = self.post_commit_hook {
                hook(commit_lsn);
            }

            // Step: decide whether to fsync now or defer via GroupCommit.
            //
            // (NoSQL fork): after writing the WAL entry, Txn.commit()
            // calls GroupCommit.bufferCommit(nowNs, txn, commitVLSN).
            // - returns true  → commit is batched; skip fsync (another
            //                   commit will flush for us).
            // - returns false → flush now (timeout or buffer limit reached).
            //
            // Without GroupCommit: fsync according to the durability policy.
            if want_sync {
                let should_skip_fsync = match &self.group_commit {
                    Some(gc) if gc.is_enabled() => {
                        // Use the txn id as a proxy for commit VLSN in
                        // non-replicated environments (single-node
                        // path where VLSN is not assigned for local txns).
                        gc.buffer_commit(self.id)
                    }
                    _ => false,
                };
                if !should_skip_fsync && let Some(ref lm) = self.log_manager {
                    // Port of JE LogManager.flushTo(commitLsn): skip fsync
                    // if a concurrent committer already flushed past our LSN.
                    lm.flush_sync_if_needed(commit_lsn)
                        .map_err(TxnError::LogError)?;
                }
            } else if matches!(durability, Durability::CommitWriteNoSync)
                && let Some(ref lm) = self.log_manager
            {
                lm.flush_no_sync()
                    .map_err(TxnError::LogError)?;
            }
            // CommitNoSync: neither flush nor fsync.

            commit_lsn
        } else {
            NULL_LSN
        };
        self.commit_lsn = assigned_lsn.as_u64();
        // Release write locks AFTER the log flush (so lock holders are
        // not visible to readers until the commit is durable).
        for lsn in self.write_locks.keys().copied().collect::<Vec<_>>() {
            let _ = self.lock_manager.release(lsn, self.id);
        }
        self.write_locks.clear();
        self.state = TxnState::Committed;
        Ok(assigned_lsn)
    }

    /// Sets the pre-commit hook called before writing the TxnCommit log entry.
    ///
    pub fn set_pre_commit_hook<F>(&mut self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.pre_commit_hook = Some(Box::new(hook));
    }

    /// Sets the post-commit hook called after writing the TxnCommit log entry.
    ///
    /// The hook receives the LSN of the committed TxnCommit record.
    ///
    pub fn set_post_commit_hook<F>(&mut self, hook: F)
    where
        F: Fn(Lsn) + Send + Sync + 'static,
    {
        self.post_commit_hook = Some(Box::new(hook));
    }

    /// Returns the current transaction state.
    pub fn get_state(&self) -> TxnState {
        self.state
    }

    /// Sets the transaction to MUST_ABORT state.
    ///
    /// After this call the transaction can only be aborted; any further
    /// operation attempt except abort() will return an error.
    ///
    /// 
    pub fn set_only_abortable(&mut self) {
        if self.state == TxnState::Open {
            self.state = TxnState::MustAbort;
        }
    }

    /// Returns the total number of locks held by this transaction.
    ///
    /// Used by deadlock victim selection: transactions holding fewer locks are
    /// preferred as victims (lighter transactions are cheaper to abort).
    ///
    /// Lock-count approach.
    pub fn n_locks(&self) -> usize {
        self.read_locks.len() + self.write_locks.len()
    }

    /// Returns true if this transaction is importunate (can steal locks).
    pub fn get_importunate(&self) -> bool {
        self.importunate
    }

    /// Sets whether this transaction is importunate.
    pub fn set_importunate(&mut self, v: bool) {
        self.importunate = v;
    }

    /// Configures serializable (repeatable-read) isolation.
    ///
    /// When true, read locks are held until commit/abort.
    ///
    pub fn set_serializable_isolation(&mut self, v: bool) {
        self.serializable_isolation = v;
    }

    /// Configures read-committed isolation.
    ///
    /// When true, read locks are released as the cursor advances.
    ///
    pub fn set_read_committed_isolation(&mut self, v: bool) {
        self.read_committed_isolation = v;
    }

    /// Sets the transaction-level timeout in milliseconds.
    ///
    /// A non-zero value causes `is_timed_out()` to return `true` after that
    /// many milliseconds, even if individual lock requests haven't timed out.
    ///
    pub fn set_txn_timeout(&mut self, timeout_ms: u64) {
        self.txn_timeout_ms = timeout_ms;
    }

    /// Returns the first LSN written by this transaction.
    ///
    pub fn first_active_lsn(&self) -> u64 {
        self.first_lsn
    }

    /// Returns true if any log entries have been written for this transaction.
    ///
    /// only transactions that have
    /// written log entries need a TxnCommit / TxnAbort log record.
    pub fn has_logged_entries(&self) -> bool {
        self.last_lsn != NULL_LSN.as_u64()
    }

    /// Records a new log entry written by this transaction.
    ///
    /// maintains `lastLoggedLsn` (chain of undo log entries) and
    /// `firstLoggedLsn` (checkpointing). We update both here.
    pub fn note_log_entry(&mut self, lsn: u64) {
        if self.first_lsn == NULL_LSN.as_u64() {
            self.first_lsn = lsn;
        }
        self.last_lsn = lsn;
    }

    /// Returns the last LSN logged by this transaction (for TxnCommit/Abort).
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    /// Returns the first LSN logged by this transaction (for checkpointing).
    pub fn first_lsn(&self) -> u64 {
        self.first_lsn
    }

    /// Helper: serialise `entry` and write it to the log manager.
    ///
    /// Returns the assigned LSN, or `NULL_LSN` when no log manager is
    /// configured (read-only test contexts).
    ///
    /// `logManager.log(params)` call inside `logCommitEntry` /
    /// `abortInternal` in the equivalent `Txn.java`.
    fn log_entry(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
        fsync: bool,
    ) -> Result<Lsn, TxnError> {
        match &self.log_manager {
            None => Ok(NULL_LSN),
            Some(lm) => {
                // Provisional.NO for commit/abort records (they are
                // never provisional — they mark the end of a transaction).
                // fsync behaviour follows the durability SyncPolicy:
                //   SYNC            -> flush_required=true, fsync_required=true
                //   WRITE_NO_SYNC   -> flush_required=true, fsync_required=false
                //   NO_SYNC (default) -> flush_required=false, fsync_required=false
                // We default to SYNC (safest) and expose `fsync` to callers.
                let flush = true; // always at least flush on commit (fsync implies flush)
                let lsn = lm.log(
                    entry_type,
                    payload,
                    Provisional::No,
                    flush,
                    fsync,
                )?;
                Ok(lsn)
            }
        }
    }

    /// Returns true if there are open cursors on this transaction.
    ///
    /// 
    pub fn has_open_cursors(&self) -> bool {
        self.cursor_count.load(Ordering::Relaxed) > 0
    }

    /// Commits the transaction.
    ///
    ///
    /// 1. Check state and that there are no open cursors.
    /// 2. Release all read locks (the: `clearReadLocks`).
    /// 3. If this txn has written log entries, serialise a `TxnCommit` record
    ///    and write it to the `LogManager` via `log()`.  The assigned LSN is
    ///    stored in `self.commit_lsn` and returned to the caller.
    ///    Per the: "If nothing was written to log for this txn, no need to log
    ///    a commit." (Txn.commit lines 764-785)
    /// 4. Release all write locks.
    /// 5. Set state to `Committed`.
    ///
    /// # Returns
    /// The `Lsn` of the `TxnCommit` log record, or `NULL_LSN` for read-only
    /// transactions or when no `LogManager` is configured.
    ///
    /// # Errors
    /// Returns `TxnError::LogError` if the log write fails.
    pub fn commit(&mut self) -> Result<Lsn, TxnError> {
        self.check_state()?;

        if self.has_open_cursors() {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "has open cursors".into(),
            });
        }

        // Step 2: release read locks first (the: clearReadLocks).
        for lsn in self.read_locks.drain().collect::<Vec<_>>() {
            let _ = self.lock_manager.release(lsn, self.id);
        }

        // Step 3: log TxnCommit if this txn made any writes.
        //
        // Per the: "If nothing was written to log for this txn, no need to
        // log a commit." (Txn.commit lines 764-785)
        //
        // logCommitEntry() calls preLogCommitHook() before and
        // postLogCommitHook() after writing the TxnCommit entry.
        let assigned_lsn = if self.has_logged_entries() {
            // Pre-commit hook (the: preLogCommitHook).
            if let Some(ref hook) = self.pre_commit_hook {
                hook();
            }

            let commit =
                TxnCommit::new(self.id, self.last_lsn, 0 /* master_id */, 0 /* dtvlsn */);
            let mut payload = Vec::with_capacity(commit.log_size());
            commit.write_to_log(&mut payload);

            // Write the TxnCommit entry without fsync; we decide below
            // whether to fsync based on the GroupCommit handler.
            let commit_lsn = self.log_entry(LogEntryType::TxnCommit, &payload, false /* fsync deferred */)?;

            // Post-commit hook (the: postLogCommitHook).
            if let Some(ref hook) = self.post_commit_hook {
                hook(commit_lsn);
            }

            // Decide whether to fsync now or defer to GroupCommit.
            // commit() defaults to CommitSync (default durability).
            let should_skip_fsync = match &self.group_commit {
                Some(gc) if gc.is_enabled() => gc.buffer_commit(self.id),
                _ => false,
            };
            if !should_skip_fsync && let Some(ref lm) = self.log_manager {
                lm.flush_sync()
                    .map_err(TxnError::LogError)?;
            }

            commit_lsn
        } else {
            NULL_LSN
        };

        self.commit_lsn = assigned_lsn.as_u64();

        // Step 4: release write locks AFTER the log flush (the: clearLocks
        // is called after logManager.flushTo(commitLsn)).
        for lsn in self.write_locks.keys().copied().collect::<Vec<_>>() {
            let _ = self.lock_manager.release(lsn, self.id);
        }
        self.write_locks.clear();

        // Step 5: mark committed.
        self.state = TxnState::Committed;
        Ok(assigned_lsn)
    }

    /// Aborts the transaction.
    ///
    ///
    /// 1. Set state to ABORTED immediately (blocks other threads from seeing
    ///    a partially-undone transaction — see comment at line 1192).
    /// 2. If this txn wrote log entries, serialise a `TxnAbort` record and
    ///    write it to the `LogManager`.  The abort LSN is stored in
    ///    `self.abort_lsn` and returned to the caller.
    /// 3. Collect undo records from `WriteLockInfo` (before-images).
    /// 4. Release all locks (write first, then read).
    ///
    /// # Returns
    /// The `Lsn` of the `TxnAbort` log record, or `NULL_LSN` for read-only
    /// transactions or when no `LogManager` is configured.
    ///
    /// # Errors
    /// Returns `TxnError::LogError` if the log write fails.
    pub fn abort(&mut self) -> Result<Lsn, TxnError> {
        // Idempotent for already-terminated transactions.
        if self.state == TxnState::Aborted {
            return Ok(NULL_LSN);
        }
        if self.state == TxnState::Committed {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "COMMITTED".into(),
            });
        }

        // Step 1: set ABORTED state before undo so other threads see this
        // txn as finished.  Per line 1192: "State is set to ABORTED before
        // undo, so that other threads cannot access this txn in the middle of
        // undo."
        self.state = TxnState::Aborted;

        // Step 2: log TxnAbort if this txn wrote any log entries.
        //
        // abortInternal() calls logManager.logForceFlush(abortEntry,
        // fsyncRequired, repContext) when forceFlush is true (i.e. durability
        // SyncPolicy.SYNC), or logManager.log() otherwise.  We write with
        // fsync=false (NO_SYNC default for aborts) to match default.
        let assigned_lsn = if self.has_logged_entries() {
            let abort =
                TxnAbort::new(self.id, self.last_lsn, 0 /* master_id */, 0 /* dtvlsn */);
            let mut payload = Vec::with_capacity(abort.log_size());
            abort.write_to_log(&mut payload);
            self.log_entry(LogEntryType::TxnAbort, &payload, false /* fsync */)?
        } else {
            NULL_LSN
        };

        self.abort_lsn = assigned_lsn.as_u64();

        // Step 3: collect undo records from WriteLockInfo for tree undo.
        //
        // `Txn.undoLNs()` walks the write-lock chain and calls
        // `DatabaseImpl.abort(undoLsn, locker)` for each LN.  In Noxu, the
        // caller (`Transaction::abort()`) is responsible for applying the undo
        // to the B-tree after draining `take_undo_records()`.
        //
        // Collects before-image records from
        // each `WriteLockInfo` entry, including new inserts where
        // `abort_known_deleted=true` and `abort_lsn == NULL_LSN`.
        for (lsn, wli) in &self.write_locks {
            if wli.abort_known_deleted || wli.abort_lsn != NULL_LSN.as_u64() {
                let record = UndoRecord {
                    current_lsn: *lsn,
                    abort_lsn: wli.abort_lsn,
                    abort_known_deleted: wli.abort_known_deleted,
                    abort_data: wli.abort_data.clone(),
                    abort_key: wli.abort_key.clone(),
                    database_id: wli.database_id,
                };
                self.undo_records.push(record);
            }
        }

        // Step 4: release all write locks then read locks.
        self.release_all_locks();

        Ok(assigned_lsn)
    }

    /// Performs steps 1-3 of abort (state transition, log, undo collection)
    /// WITHOUT releasing write locks.
    ///
    /// Used by the higher-level `Transaction::abort()` so it can apply tree
    /// undo while write locks are still held, then call `release_all_locks()`
    /// after the before-images are restored.  Readers blocked on a write lock
    /// will not unblock until `release_all_locks()` is called, so they always
    /// observe the restored before-image rather than the in-flight value.
    pub fn abort_collect_undo(&mut self) -> Result<Vec<UndoRecord>, TxnError> {
        if self.state == TxnState::Aborted {
            return Ok(std::mem::take(&mut self.undo_records));
        }
        if self.state == TxnState::Committed {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "COMMITTED".into(),
            });
        }

        self.state = TxnState::Aborted;

        let assigned_lsn = if self.has_logged_entries() {
            let abort =
                TxnAbort::new(self.id, self.last_lsn, 0 /* master_id */, 0 /* dtvlsn */);
            let mut payload = Vec::with_capacity(abort.log_size());
            abort.write_to_log(&mut payload);
            self.log_entry(LogEntryType::TxnAbort, &payload, false /* fsync */)?
        } else {
            NULL_LSN
        };

        self.abort_lsn = assigned_lsn.as_u64();

        let mut records = Vec::new();
        for (lsn, wli) in &self.write_locks {
            if wli.abort_known_deleted || wli.abort_lsn != NULL_LSN.as_u64() {
                records.push(UndoRecord {
                    current_lsn: *lsn,
                    abort_lsn: wli.abort_lsn,
                    abort_known_deleted: wli.abort_known_deleted,
                    abort_data: wli.abort_data.clone(),
                    abort_key: wli.abort_key.clone(),
                    database_id: wli.database_id,
                });
            }
        }
        // Also store in self.undo_records so take_undo_records() still works.
        self.undo_records.extend(records.iter().cloned());
        Ok(records)
    }

    /// Releases all write locks then read locks held by this transaction.
    ///
    /// Called by `abort()` and by the higher-level `Transaction::abort()` after
    /// tree undo has been applied.
    pub fn release_all_locks(&mut self) {
        for lsn in self.write_locks.keys().copied().collect::<Vec<_>>() {
            let _ = self.lock_manager.release(lsn, self.id);
        }
        self.write_locks.clear();

        for lsn in self.read_locks.drain().collect::<Vec<_>>() {
            let _ = self.lock_manager.release(lsn, self.id);
        }
    }

    /// Returns (and clears) the list of undo records produced by `abort()`.
    ///
    /// Each `UndoRecord` describes one write operation that must be undone.
    /// The caller (engine or recovery layer) is responsible for applying the
    /// undo to the B-tree.
    pub fn take_undo_records(&mut self) -> Vec<UndoRecord> {
        std::mem::take(&mut self.undo_records)
    }

    /// Checks that the txn is in a valid state for operations.
    fn check_state(&self) -> Result<(), TxnError> {
        match self.state {
            TxnState::Open => Ok(()),
            TxnState::MustAbort => Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "MUST_ABORT".into(),
            }),
            TxnState::Committed => Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "COMMITTED".into(),
            }),
            TxnState::Aborted => Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "ABORTED".into(),
            }),
        }
    }

    /// Downgrades a write lock to a read lock.
    ///
    /// 1. Calls `LockManager.demote()` to downgrade the lock at the table level.
    /// 2. Moves the LSN from `write_locks` to `read_locks` in this txn.
    ///
    /// # Errors
    /// Returns `TxnError::InvalidTransaction` if the txn is not open.
    /// Returns `TxnError::LogError` if the lock manager fails.
    pub fn demote_lock(&mut self, lsn: u64) -> Result<(), TxnError> {
        self.check_state()?;

        // Remove from write locks first; only demote if we actually hold it.
        if self.write_locks.remove(&lsn).is_some() {
            // Downgrade at the LockManager level.
            self.lock_manager.demote(lsn, self.id)?;
            // Track as a read lock.
            self.read_locks.insert(lsn);
        }

        Ok(())
    }

    /// Moves the write lock from `old_lsn` to `new_lsn`.
    ///
    /// Called after logging a new LN entry for an existing record:
    /// 1. Removes the `WriteLockInfo` from `write_locks[old_lsn]`.
    /// 2. Releases the old LSN lock at the `LockManager` level.
    /// 3. Acquires a write lock on `new_lsn` at the `LockManager` level.
    /// 4. Moves the `WriteLockInfo` into `write_locks[new_lsn]`.
    ///
    /// 
    pub fn move_write_lock_to_new_lsn(&mut self, old_lsn: u64, new_lsn: u64) {
        if let Some(wli) = self.write_locks.remove(&old_lsn) {
            let _ = self.lock_manager.release(old_lsn, self.id);
            let _ = self.lock_manager.lock(new_lsn, self.id, LockType::Write, false, false);
            self.write_locks.insert(new_lsn, wli);
        }
    }

    /// Records abort (before-image) information for a write lock.
    ///
    /// Must be called after acquiring the write lock on `lsn` (via `lock()` or
    /// `move_write_lock_to_new_lsn()`).  Only sets the abort information the
    /// first time (`never_locked == true`); subsequent calls are no-ops so that
    /// the original before-image is preserved across multiple writes to the
    /// same record within one transaction.
    ///
    /// / `WriteLockInfo.setAbortInfo()`.
    pub fn set_write_lock_abort_info(
        &mut self,
        lsn: u64,
        abort_lsn: u64,
        abort_key: Option<Vec<u8>>,
        abort_data: Option<Vec<u8>>,
        abort_known_deleted: bool,
        database_id: u64,
    ) {
        if let Some(wli) = self.write_locks.get_mut(&lsn)
            && wli.never_locked
        {
            wli.set_abort_info(abort_lsn, abort_key, abort_data, -1, 0, abort_known_deleted, 0, false);
            wli.never_locked = false;
            wli.database_id = database_id;
        }
    }

    /// Returns the number of read locks held.
    pub fn n_read_locks(&self) -> usize {
        self.read_locks.len()
    }

    /// Returns the number of write locks held.
    pub fn n_write_locks(&self) -> usize {
        self.write_locks.len()
    }

    /// Register a cursor with this txn.
    pub fn register_cursor(&self) {
        self.cursor_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Unregister a cursor from this txn.
    pub fn unregister_cursor(&self) {
        self.cursor_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the number of active cursors on this txn.
    pub fn cursor_count(&self) -> i32 {
        self.cursor_count.load(Ordering::Relaxed)
    }
}

impl Locker for Txn {
    fn id(&self) -> i64 {
        self.id
    }

    fn lock(
        &mut self,
        lsn: u64,
        lock_type: LockType,
        non_blocking: bool,
    ) -> Result<LockResult, TxnError> {
        self.check_state()?;

        let grant = self.lock_manager.lock(
            lsn,
            self.id,
            lock_type,
            non_blocking,
            self.importunate,
        )?;

        // Track the lock.
        // When a write lock is acquired (new or via promotion), the LSN
        // must be removed from read_locks if it was there, because a write lock
        // supersedes the read lock.  This mirrors LockManager.lock()
        // behaviour where PROMOTION moves the entry from the read set to the
        // write set.
        let wli = if lock_type.is_write_lock() {
            // Remove from read set if this is a promotion.
            self.read_locks.remove(&lsn);
            let wli = self.write_locks.entry(lsn).or_default();
            Some(wli.clone())
        } else {
            self.read_locks.insert(lsn);
            None
        };

        Ok(LockResult { grant, write_lock_info: wli })
    }

    fn release_lock(&mut self, lsn: u64) -> Result<(), TxnError> {
        // Txns don't release individual locks during the txn  -  they hold until commit/abort
        // This is called only for cursor-level lock release in read-committed mode
        if self.read_locks.remove(&lsn) {
            self.lock_manager.release(lsn, self.id)?;
        }
        Ok(())
    }

    fn owns_write_lock(&self, lsn: u64) -> bool {
        self.write_locks.contains_key(&lsn)
    }

    fn is_transactional(&self) -> bool {
        true
    }

    fn lock_timeout_ms(&self) -> u64 {
        self.lock_timeout_ms
    }

    fn is_preemptable(&self) -> bool {
        !self.importunate
    }

    fn is_importunate(&self) -> bool {
        self.importunate
    }

    fn is_read_uncommitted_default(&self) -> bool {
        self.read_uncommitted_default
    }

    /// Returns the transaction-level timeout.
    fn txn_timeout_ms(&self) -> u64 {
        self.txn_timeout_ms
    }

    /// Returns true if the transaction-level timeout has expired.
    ///
    /// Checks `txnTimeoutMillis` vs elapsed time.
    fn is_timed_out(&self) -> bool {
        if self.txn_timeout_ms == 0 {
            false
        } else {
            self.txn_start.elapsed().as_millis() as u64 >= self.txn_timeout_ms
        }
    }

    fn is_serializable_isolation(&self) -> bool {
        self.serializable_isolation
    }

    fn is_read_committed_isolation(&self) -> bool {
        self.read_committed_isolation
    }

    /// Returns the ID of this Txn (since Txn IS the transactional locker).
    ///
    /// `Txn.getTxnLocker()` returns `this`.
    fn get_txn_locker_id(&self) -> Option<i64> {
        Some(self.id)
    }

    fn retains_locks_on_commit(&self) -> bool {
        self.serializable_isolation
    }

    /// Re-acquires a lock on `new_lsn` when an LN is moved without prior
    /// write-lock acquisition (eviction / cleaning path).
    ///
    /// Every locker
    /// holding `old_lsn` must acquire the new LSN.
    fn lock_after_lsn_change(
        &mut self,
        old_lsn: u64,
        new_lsn: u64,
    ) -> Result<(), TxnError> {
        // If we hold a write lock on old_lsn, migrate it to new_lsn.
        if let Some(wli) = self.write_locks.remove(&old_lsn) {
            let _ = self.lock_manager.release(old_lsn, self.id);
            let _ = self.lock_manager.lock(new_lsn, self.id, LockType::Write, false, false);
            self.write_locks.insert(new_lsn, wli);
        } else if self.read_locks.remove(&old_lsn) {
            // Migrate read lock.
            let _ = self.lock_manager.release(old_lsn, self.id);
            let _ = self.lock_manager.lock(new_lsn, self.id, LockType::Read, false, false);
            self.read_locks.insert(new_lsn);
        }
        Ok(())
    }

    fn close(&mut self) {
        if self.state == TxnState::Open || self.state == TxnState::MustAbort {
            let _ = self.abort();
        }
    }

    fn is_open(&self) -> bool {
        self.state.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LockGrantType;

    fn create_test_txn() -> Txn {
        let lock_manager = Arc::new(LockManager::new());
        Txn::new(1, lock_manager)
    }

    #[test]
    fn test_create_txn() {
        let txn = create_test_txn();
        assert_eq!(txn.id(), 1);
        assert_eq!(txn.get_state(), TxnState::Open);
        assert!(txn.is_transactional());
        assert!(txn.is_open());
    }

    #[test]
    fn test_lock_and_commit() {
        let mut txn = create_test_txn();

        // Acquire a write lock
        let result = txn.lock(100, LockType::Write, false).unwrap();
        assert_eq!(result.grant, LockGrantType::New);
        assert!(result.write_lock_info.is_some());
        assert_eq!(txn.n_write_locks(), 1);

        // Acquire a read lock
        let result = txn.lock(200, LockType::Read, false).unwrap();
        assert_eq!(result.grant, LockGrantType::New);
        assert!(result.write_lock_info.is_none());
        assert_eq!(txn.n_read_locks(), 1);

        // Commit should release all locks; no log entries written so NULL_LSN.
        // (No log manager configured — Txn::new.)
        let lsn = txn.commit().unwrap();
        assert!(lsn.is_null()); // read-only txn: no log entry
        assert_eq!(txn.get_state(), TxnState::Committed);
        assert_eq!(txn.n_write_locks(), 0);
        assert_eq!(txn.n_read_locks(), 0);
    }

    #[test]
    fn test_lock_and_abort() {
        let mut txn = create_test_txn();

        // Acquire locks
        txn.lock(100, LockType::Write, false).unwrap();
        txn.lock(200, LockType::Read, false).unwrap();
        assert_eq!(txn.n_write_locks(), 1);
        assert_eq!(txn.n_read_locks(), 1);

        // Abort should release all locks; no log entries written so NULL_LSN.
        let lsn = txn.abort().unwrap();
        assert!(lsn.is_null()); // no writes logged => no abort record
        assert_eq!(txn.get_state(), TxnState::Aborted);
        assert_eq!(txn.n_write_locks(), 0);
        assert_eq!(txn.n_read_locks(), 0);
    }

    #[test]
    fn test_write_lock_tracking() {
        let mut txn = create_test_txn();

        let result = txn.lock(100, LockType::Write, false).unwrap();
        assert!(result.write_lock_info.is_some());
        assert!(txn.owns_write_lock(100));
        assert!(!txn.owns_write_lock(200));
    }

    #[test]
    fn test_read_lock_tracking() {
        let mut txn = create_test_txn();

        txn.lock(100, LockType::Read, false).unwrap();
        assert_eq!(txn.n_read_locks(), 1);
        assert!(!txn.owns_write_lock(100));
    }

    #[test]
    fn test_state_transitions() {
        let mut txn = create_test_txn();

        assert_eq!(txn.get_state(), TxnState::Open);

        txn.commit().unwrap();
        assert_eq!(txn.get_state(), TxnState::Committed);
    }

    #[test]
    fn test_state_transitions_abort() {
        let mut txn = create_test_txn();

        assert_eq!(txn.get_state(), TxnState::Open);

        txn.abort().unwrap();
        assert_eq!(txn.get_state(), TxnState::Aborted);
    }

    #[test]
    fn test_abort_idempotent() {
        let mut txn = create_test_txn();
        txn.abort().unwrap();
        // Calling abort a second time on an already-aborted txn should be OK.
        let result = txn.abort();
        assert!(result.is_ok());
        assert_eq!(txn.get_state(), TxnState::Aborted);
    }

    #[test]
    fn test_must_abort_state() {
        let mut txn = create_test_txn();

        txn.set_only_abortable();
        assert_eq!(txn.get_state(), TxnState::MustAbort);

        // Operations should fail
        let result = txn.lock(100, LockType::Write, false);
        assert!(result.is_err());

        // Can still abort
        let _ = txn.abort().unwrap();
        assert_eq!(txn.get_state(), TxnState::Aborted);
    }

    #[test]
    fn test_operations_on_committed_fail() {
        let mut txn = create_test_txn();
        txn.commit().unwrap();

        let result = txn.lock(100, LockType::Write, false);
        assert!(result.is_err());
        if let Err(TxnError::InvalidTransaction { state, .. }) = result {
            assert_eq!(state, "COMMITTED");
        } else {
            panic!("Expected InvalidTransaction error");
        }
    }

    #[test]
    fn test_operations_on_aborted_fail() {
        let mut txn = create_test_txn();
        txn.abort().unwrap();  // returns Lsn

        let result = txn.lock(100, LockType::Write, false);
        assert!(result.is_err());
        if let Err(TxnError::InvalidTransaction { state, .. }) = result {
            assert_eq!(state, "ABORTED");
        } else {
            panic!("Expected InvalidTransaction error");
        }
    }

    #[test]
    fn test_cursor_registration() {
        let txn = create_test_txn();

        assert_eq!(txn.cursor_count(), 0);

        txn.register_cursor();
        assert_eq!(txn.cursor_count(), 1);

        txn.register_cursor();
        assert_eq!(txn.cursor_count(), 2);

        txn.unregister_cursor();
        assert_eq!(txn.cursor_count(), 1);

        txn.unregister_cursor();
        assert_eq!(txn.cursor_count(), 0);
    }

    #[test]
    fn test_close_aborts_open_txn() {
        let mut txn = create_test_txn();

        txn.lock(100, LockType::Write, false).unwrap();
        assert_eq!(txn.get_state(), TxnState::Open);

        txn.close();
        assert_eq!(txn.get_state(), TxnState::Aborted);
        assert_eq!(txn.n_write_locks(), 0);
    }

    // -----------------------------------------------------------------------
    // Tests for the ported commit/abort protocol (no log manager — Txn::new)
    // -----------------------------------------------------------------------

    /// When a transaction has written log entries but no log manager is
    /// configured, commit() returns NULL_LSN (no persistence).
    #[test]
    fn test_commit_no_log_manager_returns_null_lsn() {
        let mut txn = create_test_txn();

        // Simulate a write to the log (note_log_entry records that this txn
        // has actually logged something — in production this is done by the
        // LogManager).
        txn.note_log_entry(1000);
        assert!(txn.has_logged_entries());

        txn.lock(100, LockType::Write, false).unwrap();
        let lsn = txn.commit().unwrap();

        // No log manager: commit LSN is NULL_LSN.
        assert!(lsn.is_null(), "no log manager: commit returns NULL_LSN");
    }

    /// A read-only transaction (no log entries written) should return NULL_LSN.
    #[test]
    fn test_commit_read_only_txn_no_log_entry() {
        let mut txn = create_test_txn();
        assert!(!txn.has_logged_entries());

        txn.lock(100, LockType::Read, false).unwrap();
        let lsn = txn.commit().unwrap();
        assert!(lsn.is_null(), "read-only commit: no TxnCommit record");
    }

    /// When a transaction that has logged entries aborts but has no log
    /// manager, abort() returns NULL_LSN.
    #[test]
    fn test_abort_no_log_manager_returns_null_lsn() {
        let mut txn = create_test_txn();
        txn.note_log_entry(2000);
        assert!(txn.has_logged_entries());

        txn.lock(100, LockType::Write, false).unwrap();
        let lsn = txn.abort().unwrap();

        assert!(lsn.is_null(), "no log manager: abort returns NULL_LSN");
    }

    /// Abort should collect undo records for write locks that have abort_lsn.
    #[test]
    fn test_abort_collects_undo_records() {
        let mut txn = create_test_txn();
        txn.note_log_entry(3000);

        // Acquire a write lock and set its abort information (simulates the
        // before-image set by Cursor.put before writing the new LN).
        txn.lock(100, LockType::Write, false).unwrap();
        {
            let wli = txn.write_locks.get_mut(&100).unwrap();
            wli.set_abort_info(
                50,                     // abort_lsn (before-image LSN)
                Some(b"key1".to_vec()), // abort_key
                Some(b"old".to_vec()),  // abort_data (before-image data)
                -1,                     // abort_vlsn
                0,                      // abort_log_size
                false,                  // abort_known_deleted
                0,                      // abort_expiration
                false,                  // abort_expiration_in_hours
            );
        }

        // Also lock an LSN with no abort_lsn (newly inserted, no prior version).
        txn.lock(200, LockType::Write, false).unwrap();

        let _ = txn.abort().unwrap();

        // Should have exactly one undo record (for lsn 100 which had abort_lsn=50).
        let records = txn.take_undo_records();
        assert_eq!(records.len(), 1, "one undo record for lsn 100");
        assert_eq!(records[0].current_lsn, 100);
        assert_eq!(records[0].abort_lsn, 50);
        assert_eq!(records[0].abort_data, Some(b"old".to_vec()));
        assert!(!records[0].abort_known_deleted);
    }

    /// Abort should set abort_known_deleted correctly for "insert undo" records
    /// (the record did not exist before this transaction, so the undo is a delete).
    #[test]
    fn test_abort_known_deleted_undo_record() {
        let mut txn = create_test_txn();
        txn.note_log_entry(4000);

        txn.lock(300, LockType::Write, false).unwrap();
        {
            let wli = txn.write_locks.get_mut(&300).unwrap();
            // abort_known_deleted=true means: before this txn, the slot was
            // known-deleted (i.e. this txn inserted a brand-new record).
            // On abort, the record must be deleted again.
            wli.abort_lsn = 150;
            wli.abort_known_deleted = true;
        }

        let _ = txn.abort().unwrap();
        let records = txn.take_undo_records();
        assert_eq!(records.len(), 1);
        assert!(records[0].abort_known_deleted);
    }

    /// Committing a transaction with open cursors must fail.
    #[test]
    fn test_commit_with_open_cursors_fails() {
        let txn = create_test_txn();
        txn.register_cursor();

        // We need a mutable reference; create via a separate fn.
        let mut txn2 = create_test_txn();
        txn2.register_cursor();

        let result = txn2.commit();
        assert!(result.is_err());
        if let Err(TxnError::InvalidTransaction { state, .. }) = result {
            assert!(state.contains("cursors"), "error should mention cursors");
        } else {
            panic!("Expected InvalidTransaction error");
        }
    }

    /// note_log_entry tracks first and last LSN.
    #[test]
    fn test_note_log_entry_tracking() {
        let mut txn = create_test_txn();

        assert_eq!(txn.first_lsn(), NULL_LSN.as_u64());
        assert_eq!(txn.last_lsn(), NULL_LSN.as_u64());

        txn.note_log_entry(100);
        assert_eq!(txn.first_lsn(), 100);
        assert_eq!(txn.last_lsn(), 100);

        txn.note_log_entry(200);
        assert_eq!(txn.first_lsn(), 100); // first never changes
        assert_eq!(txn.last_lsn(), 200);

        txn.note_log_entry(300);
        assert_eq!(txn.first_lsn(), 100);
        assert_eq!(txn.last_lsn(), 300);
    }

    /// has_logged_entries() returns false when no entries logged.
    #[test]
    fn test_has_logged_entries() {
        let mut txn = create_test_txn();
        assert!(!txn.has_logged_entries());
        txn.note_log_entry(42);
        assert!(txn.has_logged_entries());
    }

    /// take_undo_records() is idempotent — second call returns empty vec.
    #[test]
    fn test_take_undo_records_idempotent() {
        let mut txn = create_test_txn();
        txn.note_log_entry(5000);
        txn.lock(400, LockType::Write, false).unwrap();
        {
            let wli = txn.write_locks.get_mut(&400).unwrap();
            wli.abort_lsn = 10;
        }
        let _ = txn.abort().unwrap();

        let records1 = txn.take_undo_records();
        assert_eq!(records1.len(), 1);

        let records2 = txn.take_undo_records();
        assert!(records2.is_empty(), "second take should be empty");
    }

    #[test]
    fn test_importunate_flag() {
        let mut txn = create_test_txn();

        assert!(!txn.get_importunate());
        assert!(txn.is_preemptable());

        txn.set_importunate(true);
        assert!(txn.get_importunate());
        assert!(!txn.is_preemptable());
    }

    // -----------------------------------------------------------------------
    // Tests that exercise the real LogManager integration (Txn::with_log_manager)
    // -----------------------------------------------------------------------

    /// Helper: build a real LogManager backed by a temp directory.
    fn make_log_manager_in_tempdir() -> (Arc<LogManager>, tempfile::TempDir) {
        use noxu_log::FileManager;
        let dir = tempfile::TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
        );
        let lm = Arc::new(LogManager::new(fm, 3, 1024 * 1024, 4096));
        (lm, dir)
    }

    /// commit() on a txn that has logged entries must write a TxnCommit record
    /// to the log and return a non-null LSN.
    #[test]
    fn test_commit_writes_to_log() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();

        let mut txn = Txn::with_log_manager(42, lock_manager, lm.clone());

        // Simulate that this txn actually logged an LN (last_lsn is set).
        txn.note_log_entry(100);
        txn.lock(100, LockType::Write, false).unwrap();

        let eol_before = lm.get_end_of_log();

        let commit_lsn = txn.commit().unwrap();

        // The returned LSN must not be null — a TxnCommit record was written.
        assert!(!commit_lsn.is_null(), "commit_lsn should not be NULL_LSN");

        // The log must have grown.
        let eol_after = lm.get_end_of_log();
        assert!(
            eol_after.as_u64() > eol_before.as_u64(),
            "log must have grown after commit"
        );

        // commit_lsn accessor must return the same value.
        assert_eq!(txn.commit_lsn(), commit_lsn);
        assert_eq!(txn.get_state(), TxnState::Committed);
    }

    /// abort() on a txn that has logged entries must write a TxnAbort record
    /// to the log and return a non-null LSN.
    #[test]
    fn test_abort_writes_to_log() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();

        let mut txn = Txn::with_log_manager(99, lock_manager, lm.clone());

        txn.note_log_entry(200);
        txn.lock(200, LockType::Write, false).unwrap();

        let eol_before = lm.get_end_of_log();

        let abort_lsn_val = txn.abort().unwrap();

        assert!(!abort_lsn_val.is_null(), "abort_lsn should not be NULL_LSN");

        let eol_after = lm.get_end_of_log();
        assert!(
            eol_after.as_u64() > eol_before.as_u64(),
            "log must have grown after abort"
        );

        // abort_lsn accessor must match.
        assert_eq!(txn.abort_lsn(), abort_lsn_val);
        assert_eq!(txn.get_state(), TxnState::Aborted);
    }

    /// A read-only transaction (has_logged_entries() == false) must NOT write
    /// any record to the log — the log end-of-log position must not advance.
    #[test]
    fn test_read_only_txn_does_not_write_to_log() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();

        let mut txn = Txn::with_log_manager(7, lock_manager, lm.clone());

        // Read lock only — no note_log_entry call.
        txn.lock(300, LockType::Read, false).unwrap();
        assert!(!txn.has_logged_entries());

        let eol_before = lm.get_end_of_log();

        let commit_lsn = txn.commit().unwrap();

        // No log entry should have been written.
        assert!(commit_lsn.is_null(), "read-only commit: LSN must be NULL_LSN");
        let eol_after = lm.get_end_of_log();
        assert_eq!(
            eol_before.as_u64(),
            eol_after.as_u64(),
            "log must not grow for read-only txn"
        );
    }

    // -----------------------------------------------------------------------
    // Ported from TxnTest.java — testBasicLocking
    // -----------------------------------------------------------------------

    /// Acquire a read lock, verify it is
    /// held, release it, and verify the count returns to zero.
    #[test]
    fn test_je_basic_read_lock_release() {
        let mut txn = create_test_txn();

        let result = txn.lock(100, LockType::Read, false).unwrap();
        assert_eq!(result.grant, LockGrantType::New);
        assert_eq!(txn.n_read_locks(), 1);
        assert_eq!(txn.n_write_locks(), 0);

        txn.release_lock(100).unwrap();
        assert_eq!(txn.n_read_locks(), 0);
    }

    /// Acquire a read lock then promote to
    /// write (PROMOTION grant), then demote back to read.
    #[test]
    fn test_je_promote_and_demote() {
        let mut txn = create_test_txn();

        let r1 = txn.lock(100, LockType::Read, false).unwrap();
        assert_eq!(r1.grant, LockGrantType::New);
        assert_eq!(txn.n_read_locks(), 1);
        assert_eq!(txn.n_write_locks(), 0);

        let r2 = txn.lock(100, LockType::Write, false).unwrap();
        assert_eq!(r2.grant, LockGrantType::Promotion);
        // After promotion: moves from read_locks to write_locks.
        assert_eq!(txn.n_read_locks(), 0);
        assert_eq!(txn.n_write_locks(), 1);

        // Demote write → read.
        txn.demote_lock(100).unwrap();
        assert_eq!(txn.n_read_locks(), 1);
        assert_eq!(txn.n_write_locks(), 0);
    }

    /// Existing grant when requesting a lock
    /// that is already held at the same or stronger level.
    #[test]
    fn test_je_existing_lock_grant() {
        let mut txn = create_test_txn();

        txn.lock(100, LockType::Read, false).unwrap();
        // Re-requesting the same read lock must return EXISTING.
        let r = txn.lock(100, LockType::Read, false).unwrap();
        assert_eq!(r.grant, LockGrantType::Existing);
        assert_eq!(txn.n_read_locks(), 1);
    }

    // -----------------------------------------------------------------------
    // Ported from TxnTest.java — testCommit (lock-focused part)
    // -----------------------------------------------------------------------

    /// Commit releases all locks held by the txn.
    #[test]
    fn test_je_commit_releases_all_locks() {
        let mut txn = create_test_txn();

        txn.lock(100, LockType::Read, false).unwrap();
        txn.lock(200, LockType::Read, false).unwrap();
        assert_eq!(txn.n_read_locks(), 2);

        // Upgrade lsn 200 to write.
        let r = txn.lock(200, LockType::Write, false).unwrap();
        assert_eq!(r.grant, LockGrantType::Promotion);
        assert_eq!(txn.n_read_locks(), 1);
        assert_eq!(txn.n_write_locks(), 1);

        // Re-requesting lsn 100 yields EXISTING.
        let r2 = txn.lock(100, LockType::Read, false).unwrap();
        assert_eq!(r2.grant, LockGrantType::Existing);

        txn.commit().unwrap();
        assert_eq!(txn.n_read_locks(), 0);
        assert_eq!(txn.n_write_locks(), 0);
        assert_eq!(txn.get_state(), TxnState::Committed);
    }

    // -----------------------------------------------------------------------
    // Ported from TxnTest.java — txn state transitions
    // -----------------------------------------------------------------------

    /// Begin → commit creates/destroys txn state correctly.
    #[test]
    fn test_je_begin_commit_state() {
        let mut txn = create_test_txn();
        assert!(txn.is_open());
        assert_eq!(txn.get_state(), TxnState::Open);

        txn.commit().unwrap();
        assert!(!txn.is_open());
        assert_eq!(txn.get_state(), TxnState::Committed);
    }

    /// Begin → abort destroys txn state and releases locks.
    #[test]
    fn test_je_begin_abort_releases_locks() {
        let mut txn = create_test_txn();
        txn.lock(100, LockType::Write, false).unwrap();
        txn.lock(200, LockType::Read, false).unwrap();
        assert_eq!(txn.n_write_locks(), 1);
        assert_eq!(txn.n_read_locks(), 1);

        txn.abort().unwrap();
        assert_eq!(txn.n_write_locks(), 0);
        assert_eq!(txn.n_read_locks(), 0);
        assert_eq!(txn.get_state(), TxnState::Aborted);
        assert!(!txn.is_open());
    }

    // -----------------------------------------------------------------------
    // Ported from TxnTest — abort rolls back locks, allowing another txn to
    // acquire what was held
    // -----------------------------------------------------------------------

    /// After txn1 aborts, txn2 can immediately acquire the
    /// same lock that txn1 held.
    #[test]
    fn test_je_abort_releases_lock_for_other_txn() {
        let lm = Arc::new(LockManager::new());
        let mut txn1 = Txn::new(1, Arc::clone(&lm));
        let mut txn2 = Txn::new(2, Arc::clone(&lm));

        txn1.lock(500, LockType::Write, false).unwrap();
        assert!(txn1.owns_write_lock(500));

        // txn2 would conflict if txn1 is still alive; after abort it must succeed.
        txn1.abort().unwrap();
        assert!(!txn1.owns_write_lock(500));

        let r = txn2.lock(500, LockType::Write, false).unwrap();
        assert_eq!(r.grant, LockGrantType::New);
        assert!(txn2.owns_write_lock(500));
        txn2.commit().unwrap();
    }

    // -----------------------------------------------------------------------
    // Ported from TxnTest — nested/shared lock manager (two txns, one manager)
    // -----------------------------------------------------------------------

    /// Two independent txns sharing the same lockmanager can
    /// hold compatible locks concurrently.
    #[test]
    fn test_je_two_txns_shared_read_locks() {
        let lm = Arc::new(LockManager::new());
        let mut txn1 = Txn::new(1, Arc::clone(&lm));
        let mut txn2 = Txn::new(2, Arc::clone(&lm));

        txn1.lock(600, LockType::Read, false).unwrap();
        txn2.lock(600, LockType::Read, false).unwrap();

        // Both txns hold read locks on the same LSN simultaneously.
        assert!(!txn1.owns_write_lock(600));
        assert!(!txn2.owns_write_lock(600));

        txn1.commit().unwrap();
        txn2.commit().unwrap();
    }

    /// A write lock held by txn1 prevents txn2 from
    /// immediately acquiring a write lock on the same LSN (non-blocking).
    #[test]
    fn test_je_write_blocks_other_write_nonblocking() {
        let lm = Arc::new(LockManager::new());
        let mut txn1 = Txn::new(1, Arc::clone(&lm));
        let mut txn2 = Txn::new(2, Arc::clone(&lm));

        txn1.lock(700, LockType::Write, false).unwrap();
        assert!(txn1.owns_write_lock(700));

        // Non-blocking request: txn2 should get LockNotAvailable.
        let r = txn2.lock(700, LockType::Write, true);
        assert!(
            r.is_err(),
            "expected error for blocked non-blocking write, got {:?}",
            r
        );

        txn1.abort().unwrap();
        txn2.abort().unwrap();
    }

    /// N_locks() returns the total read + write lock count.
    #[test]
    fn test_je_n_locks_totals() {
        let mut txn = create_test_txn();

        assert_eq!(txn.n_locks(), 0);
        txn.lock(100, LockType::Read, false).unwrap();
        assert_eq!(txn.n_locks(), 1);
        txn.lock(200, LockType::Write, false).unwrap();
        assert_eq!(txn.n_locks(), 2);
        txn.lock(300, LockType::Read, false).unwrap();
        assert_eq!(txn.n_locks(), 3);

        txn.abort().unwrap();
        assert_eq!(txn.n_locks(), 0);
    }
}
