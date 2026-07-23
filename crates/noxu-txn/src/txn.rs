//! Transaction implementation.
//!

use hashbrown::{HashMap, HashSet};
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
    LockGrantType, LockManager, LockResult, LockType, Locker, TxnError,
    WriteLockInfo,
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
/// This `Txn` was allocated by [`crate::TxnManager::begin_auto_txn`] to wrap
/// a single auto-commit operation.
///
/// Auto-txns share the explicit-txn id space (allocated by
/// `TxnManager::next_txn_id`) so the lock manager can render typed locker
/// IDs in deadlock messages, but they intentionally **do not** write
/// `TxnCommit` / `TxnAbort` log records on commit/abort — the underlying
/// LN entry is logged with `txn_id = 0` (i.e. as `InsertLN` /
/// `DeleteLN`) so the on-disk WAL format stays identical to pre-Wave-1A
/// auto-commit and recovery does not need to look up a synthetic
/// commit/abort record for every auto-commit op.  Closes the first F12
/// residual.
const IS_AUTO_TXN: u8 = 16;

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
    /// When true, all lock requests are non-blocking (fail immediately).
    no_wait: bool,

    /// Writes stay local rather than being replicated. Only meaningful in
    /// a replicated environment; the write path permits a write through
    /// this txn only against a database whose replicated-ness disagrees
    /// with this flag — i.e. a locally-writing txn may only write to a
    /// non-replicated database, and an ordinarily-replicating txn may only
    /// write to a replicated one. The default is `true` (writes are local
    /// unless told otherwise), which is also the only value that matters
    /// outside replication. On a replicated environment, an explicit
    /// transaction is instead given `false` by default (replicate
    /// normally, the common case) unless the caller opts into local-only
    /// writes; that resolution happens where the txn is created (it
    /// requires knowing whether the environment is replicated, which this
    /// struct does not track itself). Auto-commit txns have this derived
    /// automatically from the target database's replicated flag at
    /// creation time, so a mismatch never occurs for them.
    local_write: bool,

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

/// Outcome of the commit *append* phase (Fix 3a): the LSN to report to the
/// caller plus the fsync still owed by the *durable* phase.  Threading this
/// between the two phases lets `commit_with_durability` release the write
/// locks after the WAL append but before the fsync.
struct PendingCommit {
    /// LSN returned to the caller (`NULL_LSN` when nothing was logged).
    assigned_lsn: Lsn,
    /// The durability barrier still owed after the write locks are released.
    pending_sync: PendingSync,
}

/// The fsync still owed by the commit *durable* phase after the WAL append.
enum PendingSync {
    /// Explicit txn: fsync (or GroupCommit-coalesce) up to this commit LSN.
    Commit { commit_lsn: Lsn },
    /// Synthetic auto-txn: no TxnCommit record; honour durability for the LN.
    Ln { ln_lsn: Lsn },
    /// Nothing was logged; no durability work is owed.
    None,
}

impl Txn {
    /// Creates a new transaction without a log manager.
    ///
    /// Commits and aborts will not write to the persistent log.  Use this
    /// constructor in unit tests or when a LogManager is not available.
    pub fn new(id: i64, lock_manager: Arc<LockManager>) -> Self {
        Self::new_with_flags(id, lock_manager, 0)
    }

    /// Creates a synthetic auto-commit transaction.
    ///
    /// Returned by [`crate::TxnManager::begin_auto_txn`] to wrap a single
    /// auto-commit cursor operation.  The auto-txn:
    ///
    /// * Carries the `IS_AUTO_TXN` flag, so [`Self::is_auto_txn`] returns
    ///   `true` and [`Self::commit_with_durability`] / [`Self::abort`] skip
    ///   the `TxnCommit` / `TxnAbort` WAL entry (the underlying LN was
    ///   logged as auto-commit by the cursor, so no synthetic commit
    ///   record is needed for recovery).
    /// * Tracks per-record write locks via `WriteLockInfo` exactly like an
    ///   explicit `Txn`, so [`Self::abort`] / [`Self::abort_collect_undo`]
    ///   can roll back the in-memory tree write on any error path —
    ///   closing the first F12 residual.
    /// * Allocates its locker id from the SAME counter as explicit txns,
    ///   so deadlocks involving auto-commit and explicit txns no longer
    ///   report opaque cursor ids.
    pub fn new_auto(id: i64, lock_manager: Arc<LockManager>) -> Self {
        Self::new_with_flags(id, lock_manager, IS_AUTO_TXN)
    }

    /// Returns `true` if this `Txn` was created by
    /// [`crate::TxnManager::begin_auto_txn`].
    ///
    /// Used by [`Self::commit_with_durability`] and [`Self::abort`] to skip
    /// the `TxnCommit` / `TxnAbort` WAL entry, and by
    /// [`crate::LockManager::register_locker_label`] callers to label the
    /// locker as `"auto-txn"` in deadlock diagnostics.
    pub fn is_auto_txn(&self) -> bool {
        self.txn_flags & IS_AUTO_TXN != 0
    }

    /// Returns the locker id of this `Txn`.
    ///
    /// Inherent helper so callers do not need the `Locker` trait in scope.
    pub fn id_as_locker(&self) -> i64 {
        self.id
    }

    fn new_with_flags(
        id: i64,
        lock_manager: Arc<LockManager>,
        txn_flags: u8,
    ) -> Self {
        Txn {
            id,
            lock_manager,
            state: TxnState::Open,
            txn_flags,
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
            no_wait: false,
            local_write: true,
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

    /// Creates a synthetic auto-commit transaction wired to a LogManager.
    ///
    /// Like [`Self::new_auto`] but exposes the LogManager so the auto-txn
    /// can perform the auto-commit fsync via
    /// `LogManager::flush_sync_if_needed` from within
    /// [`Self::commit_with_durability`].  The auto-txn still skips the
    /// `TxnCommit` / `TxnAbort` WAL entries by virtue of the
    /// `IS_AUTO_TXN` flag.
    pub fn with_log_manager_auto(
        id: i64,
        lock_manager: Arc<LockManager>,
        log_manager: Arc<LogManager>,
    ) -> Self {
        let mut txn = Self::new_auto(id, lock_manager);
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

    /// If this txn holds a `RangeInsert` next-key lock on `lsn`, release it so a
    /// subsequent `Write` request on the same LSN is a fresh grant rather than
    /// an illegal `(RangeInsert, Write)` upgrade.
    ///
    /// An insert of key A takes a `RangeInsert` lock on A's successor B's LSN
    /// (phantom prevention).  If the SAME txn then writes key B (an existing
    /// key locked by its real LSN), it would request `Write` on the LSN it
    /// already holds as `RangeInsert` — ILLEGAL in JE's upgrade matrix
    /// (RANGE_INSERT is the next-key lock, not a lock on the key itself; JE
    /// asserts this upgrade is never requested).  Noxu can co-locate them
    /// because its next-key lock targets the successor's real LSN.  Releasing
    /// the txn's OWN RangeInsert here is safe: it only guarded against OTHER
    /// txns' phantom inserts at B, and the Write about to be taken provides
    /// at-least-as-strong protection.  Returns true if a lock was released.
    /// A plain `Read`-held lock is left alone (Read->Write is a legal
    /// WritePromote).
    pub fn release_range_insert_for_write(&mut self, lsn: u64) -> bool {
        if self.write_locks.contains_key(&lsn)
            || !self.read_locks.contains(&lsn)
        {
            return false;
        }
        if matches!(
            self.lock_manager.get_owned_lock_type(lsn, self.id),
            Some(LockType::RangeInsert)
        ) && self.read_locks.remove(&lsn)
            && self.lock_manager.release(lsn, self.id).is_ok()
        {
            return true;
        }
        false
    }

    /// Returns true if this txn currently holds a `RangeInsert` (next-key) lock
    /// on `lsn`.  Used by the read path to skip a `Read`/`RangeRead` request on
    /// an LSN the txn already next-key-locked: `(RangeInsert, Read)` /
    /// `(RangeInsert, RangeRead)` is ILLEGAL in JE's upgrade matrix (JE never
    /// reads the successor it next-key-locked, so it's unreachable there).
    /// Noxu's split lock locus can reach it (insert A range-locks existing
    /// successor B's real LSN, then the same txn reads B).  The existing
    /// RangeInsert lock entry stands; the read proceeds against the BIN.
    pub fn holds_range_insert(&self, lsn: u64) -> bool {
        self.read_locks.contains(&lsn)
            && matches!(
                self.lock_manager.get_owned_lock_type(lsn, self.id),
                Some(LockType::RangeInsert)
            )
    }

    /// Read-committed / read-uncommitted fast-path probe.
    ///
    /// A read-committed cursor read acquires a `Read` lock and releases it
    /// immediately after the operation (see `CursorImpl::lock_ln`), so the
    /// only thing the lock buys is detecting whether a *writer* currently
    /// holds the slot — in which case the read must wait for that writer and
    /// re-read the committed value.  When the slot is unlocked (the common
    /// case under a read-heavy workload) the acquire+release pair is two
    /// shard-mutex round-trips plus a `read_locks` insert/remove of pure
    /// overhead.
    ///
    /// This probe runs `check_state` (so an Aborted / MustAbort txn is still
    /// caught here rather than silently returning dirty data — the same
    /// invariant `lock()` enforces) and then asks the lock manager whether
    /// the slot has a foreign write owner or any waiter with a SINGLE shard
    /// access.  It returns `true` (uncontended, caller may skip the formal
    /// acquire+release) iff the txn's state is Open AND no foreign writer
    /// owns the slot AND there are no waiters.  It inserts NOTHING into
    /// `read_locks`, so nothing needs releasing.
    ///
    /// Behaviour-identical to acquiring a `Read` lock and releasing it
    /// immediately, because (a) read-committed never holds the read lock past
    /// the operation, so no `read_locks` entry would have survived to commit;
    /// and (b) the BIN write-latch a writer must hold to mutate the slot
    /// serialises it against the reader — "no foreign write owner now" means
    /// the slot LSN the reader observed is committed data.
    ///
    /// ONLY sound for the immediate-release isolation levels
    /// (read-committed).  Repeatable-read / serializable hold the read lock
    /// for the txn and MUST use the full `lock()` path so a concurrent writer
    /// blocks on the held read lock.  The caller (`lock_ln`) gates on
    /// `is_read_committed_isolation()` before calling this.
    ///
    /// Mirrors `LockManager::probe_read_uncontended`, which the auto-commit
    /// read path already uses (CHANGELOG 6.4.1); this extends the same
    /// optimisation to the read-committed *txn* path.
    pub fn probe_read_uncontended(&self, lsn: u64) -> Result<bool, TxnError> {
        self.check_state()?;
        Ok(self.lock_manager.probe_read_uncontended(lsn, self.id))
    }

    /// Commits with an explicit durability policy.
    ///
    ///
    ///
    /// - `CommitSync` (default): flush and fsync before returning.
    /// - `CommitWriteNoSync`: write to OS page cache but don't fsync.
    /// - `CommitNoSync`: don't flush; fastest but least durable.
    pub fn commit_with_durability(
        &mut self,
        durability: Durability,
    ) -> Result<Lsn, TxnError> {
        // Prepared txns must be resolved through
        // `resolved_commit_after_prepare` so the XA layer is the only path
        // that can finalise them.  A direct `commit()` on a prepared txn
        // would skip the prepare→commit ordering invariant and is a
        // protocol error.
        if self.is_prepared() {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "PREPARED".into(),
            });
        }
        // Drain locks on every error return path so that an early
        // return (state-check failure, open cursors, log fsync error)
        // never leaves entries in `lock_manager` until environment
        // close. `release_all_locks` is idempotent.
        if let Err(e) = self.check_state() {
            // State-check failure means the txn is no longer in
            // `Open` (typically `MustAbort` flipped by the deadlock
            // detector). Drain so we don't leak the held locks until
            // environment close — `release_all_locks` is idempotent.
            self.release_all_locks();
            return Err(e);
        }
        if self.has_open_cursors() {
            // Caller's bug: at least one cursor outlived this
            // commit() call. Preserve retry semantics — the caller
            // can close cursors and call `commit()` again — by NOT
            // draining locks here. The locks are still held by this
            // (Open) txn, and will be released by the eventual
            // successful commit() or abort(). Since the txn stays in
            // `Open`, no lock leak occurs as long as the caller
            // either retries or aborts.
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "has open cursors".into(),
            });
        }
        for lsn in self.read_locks.drain().collect::<Vec<_>>() {
            if let Err(e) = self.lock_manager.release(lsn, self.id) {
                log::error!(
                    "noxu-txn: lock_manager.release({lsn}, txn={}) failed \
                     during commit_with_durability read-lock drain: {e}",
                    self.id
                );
            }
        }
        // The commit is split into two phases so the write locks can be
        // released between the WAL append and the fsync (Fix 3a).  Phase 1
        // (append) can fail with `?`; on Err the epilogue drains the still-held
        // write locks rather than leaking them to environment close, and flips
        // the txn to `MustAbort` so a caller who ignores the error and re-calls
        // commit() hits `check_state` (a clear `InvalidTransaction` Err)
        // instead of silently writing a second TxnCommit record.
        // ── Phase 1: append the commit record to the WAL buffer ──────────
        // Marshals + appends the TxnCommit entry under the log-write latch
        // (LSN assigned + buffer slot reserved), but does NOT yet fsync.  On
        // any `?` failure here the write locks are still held, so the epilogue
        // drains them and flips to MustAbort.
        let PendingCommit { assigned_lsn, pending_sync } =
            match self.commit_append_phase() {
                Ok(pending) => pending,
                Err(e) => {
                    self.release_all_locks();
                    self.state = TxnState::MustAbort;
                    return Err(e);
                }
            };
        self.commit_lsn = assigned_lsn.as_u64();

        // ── Fix 3a: release write locks BEFORE the commit fsync ──────────
        //
        // The commit record is already durably in the WAL *buffer* with a
        // monotonically-assigned LSN (Phase 1 above); the fsync barrier is
        // still ahead of us and we still wait on it (Phase 2 below) before
        // returning success to the caller.  Releasing the write locks here —
        // between WAL append and fsync — shrinks the lock-hold window from the
        // whole fsync syscall (100µs–2ms) to microseconds, dissolving the
        // hot-key convoy under high contention.
        //
        // ISOLATION + DURABILITY INVARIANT (why this is safe on Noxu's WAL):
        //   "Locks guard *logical* conflict; durability is a *separate*
        //    barrier the committer still waits on before returning success."
        //
        // A second txn may now acquire a just-released record lock and commit
        // *before* this txn's fsync completes.  That is safe because Noxu has
        // a single WAL with a single monotonic durable watermark
        // (`LogManager::last_synced_lsn`, advanced only by the fsync leader
        // after a successful fdatasync):
        //   * LSN assignment is serialized under the log-write latch, so the
        //     second txn's commit record gets a strictly-higher LSN than ours
        //     (its append happens-after ours).
        //   * A single fdatasync drains the whole buffer, so making LSN Y
        //     durable implies every LSN < Y (including ours) is durable — the
        //     durable point is monotonic (`flush_sync_if_needed` /
        //     `flush_sync` in noxu-log/log_manager.rs).
        // Therefore a dependent write can never become durable *ahead of* the
        // write it depends on: if our commit is lost to a crash/fsync-failure,
        // any later (higher-LSN) commit that read our released lock is lost
        // too, and recovery replays in LSN order.  No torn/inconsistent state.
        //
        // Read locks were already drained pre-append.  Repeatable-read /
        // serializable readers hold their own read locks (or a snapshot) and
        // are unaffected.  Read-committed is the case this protects: a reader
        // that acquires the released lock observes the new value, whose commit
        // is ordered before the reader's own commit in the same monotonic WAL.
        //
        // DEVIATION FROM JE (documented, not JE-faithful): JE releases write
        // locks AFTER the commit fsync — its `Txn.commit` calls
        // `logCommitEntry(SYNC)` (which fsyncs inside `LogManager.log`) and
        // only then `releaseWriteLocks()` (Txn.java).  Noxu deliberately
        // reorders these two steps to cut tail latency on hot-contention
        // workloads; the durability guarantee to the caller is unchanged
        // (success is still returned only after the fsync in Phase 2).
        for lsn in self.write_locks.keys().copied().collect::<Vec<_>>() {
            if let Err(e) = self.lock_manager.release(lsn, self.id) {
                log::error!(
                    "noxu-txn: lock_manager.release({lsn}, txn={}) failed \
                     during commit_with_durability write-lock drain: {e}",
                    self.id
                );
            }
        }
        self.write_locks.clear();

        // ── Phase 2: durability barrier ─────────────────────────────────
        // The committer STILL blocks here until the fsync makes the commit
        // record durable — success is returned to the caller only after this.
        // Locks are already released (above), so this wait no longer holds up
        // any waiter on the hot key.
        if let Err(e) = self.commit_durable_phase(durability, pending_sync) {
            // The WAL append already succeeded but the fsync failed; the log
            // is now invalidated (noxu-log sets `io_invalid`).  Locks are
            // already released; flip to MustAbort and surface the error.  The
            // commit is NOT durable and MUST NOT be reported as committed.
            self.state = TxnState::MustAbort;
            return Err(e);
        }

        self.state = TxnState::Committed;
        Ok(assigned_lsn)
    }

    /// Phase 1 of commit: append the TxnCommit record to the WAL buffer and
    /// decide (via GroupCommit) whether a fsync is still owed.  Does NOT fsync
    /// — that is [`Self::commit_durable_phase`], which runs AFTER write locks
    /// are released (Fix 3a).  Any `?` failure leaves write locks held so the
    /// caller's epilogue can drain them.
    fn commit_append_phase(&mut self) -> Result<PendingCommit, TxnError> {
        if self.has_logged_entries() && !self.is_auto_txn() {
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
            let commit_lsn =
                self.log_entry(LogEntryType::TxnCommit, &payload, false)?;

            // TXN-1: the prior versions of every record this txn overwrote
            // become reclaimable on commit.  Count each write-lock's abort
            // LSN obsolete through the tracker.
            // JE Txn.getObsoleteLsnInfo -> LogManager counts each
            // obsoleteWriteLockInfo via countObsoleteNode under the LWL.
            //
            // Runs BEFORE the write locks are released (Fix 3a) so the
            // WriteLockInfo abort-LSN set is still intact here.
            self.count_obsolete_abort_lsns();

            if let Some(ref hook) = self.post_commit_hook {
                hook(commit_lsn);
            }
            Ok(PendingCommit {
                assigned_lsn: commit_lsn,
                pending_sync: PendingSync::Commit { commit_lsn },
            })
        } else if self.is_auto_txn() && self.has_logged_entries() {
            // Auto-commit (synthetic auto-txn): no `TxnCommit` WAL entry is
            // written, but we still honour the caller's durability policy for
            // the LN entry the cursor already wrote.  `last_lsn` is the LN's
            // LSN.  The fsync is likewise deferred to the durable phase.
            Ok(PendingCommit {
                assigned_lsn: NULL_LSN,
                pending_sync: PendingSync::Ln {
                    ln_lsn: Lsn::from_u64(self.last_lsn),
                },
            })
        } else {
            Ok(PendingCommit {
                assigned_lsn: NULL_LSN,
                pending_sync: PendingSync::None,
            })
        }
    }

    /// Phase 2 of commit: the durability barrier.  Runs AFTER write locks are
    /// released (Fix 3a) but BEFORE `commit_with_durability` returns success,
    /// so the caller never observes a committed result that is not durable.
    fn commit_durable_phase(
        &mut self,
        durability: Durability,
        pending_sync: PendingSync,
    ) -> Result<(), TxnError> {
        let want_sync = matches!(durability, Durability::CommitSync);
        match pending_sync {
            PendingSync::Commit { commit_lsn } => {
                // Step: fsync now or defer via GroupCommit.
                //
                // (extended fork): after writing the WAL entry, Txn.commit()
                // calls GroupCommit.bufferCommit(nowNs, txn, commitVLSN).
                // - returns true  → commit is batched; skip fsync (another
                //                   commit will flush for us).
                // - returns false → flush now (timeout or buffer limit).
                if want_sync {
                    let should_skip_fsync = match &self.group_commit {
                        Some(gc) if gc.is_enabled() => {
                            // Use the txn id as a proxy for commit VLSN in
                            // non-replicated environments (single-node path
                            // where VLSN is not assigned for local txns).
                            gc.buffer_commit(self.id)
                        }
                        _ => false,
                    };
                    if !should_skip_fsync && let Some(ref lm) = self.log_manager
                    {
                        // Skip fsync if a concurrent committer already flushed
                        // past our LSN (the coalescing fast path).
                        lm.flush_sync_if_needed(commit_lsn)
                            .map_err(TxnError::LogError)?;
                    }
                } else if matches!(durability, Durability::CommitWriteNoSync)
                    && let Some(ref lm) = self.log_manager
                {
                    lm.flush_no_sync().map_err(TxnError::LogError)?;
                }
                // CommitNoSync: neither flush nor fsync.
            }
            PendingSync::Ln { ln_lsn } => {
                if want_sync && let Some(ref lm) = self.log_manager {
                    lm.flush_sync_if_needed(ln_lsn)
                        .map_err(TxnError::LogError)?;
                } else if matches!(durability, Durability::CommitWriteNoSync)
                    && let Some(ref lm) = self.log_manager
                {
                    lm.flush_no_sync().map_err(TxnError::LogError)?;
                }
            }
            PendingSync::None => {}
        }
        Ok(())
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

    /// Returns the number of *read* locks currently held by this
    /// transaction.  Wave 1C audit cleanup (transaction-env F23): the
    /// public `noxu_db::Transaction::lock_counts()` accessor needs a
    /// way to ask the inner `Txn` for read/write lock totals
    /// separately so it can mirror JE's
    /// `Transaction.getNumReadLocks()`.
    pub fn read_lock_count(&self) -> usize {
        self.read_locks.len()
    }

    /// Returns the number of *write* locks currently held by this
    /// transaction.  Mirrors JE's `Transaction.getNumWriteLocks()`.
    pub fn write_lock_count(&self) -> usize {
        self.write_locks.len()
    }

    /// Returns true if this transaction is importunate (can steal locks).
    pub fn get_importunate(&self) -> bool {
        self.importunate
    }

    /// Sets whether this transaction is importunate.
    ///
    /// An importunate locker is non-preemptable (`is_preemptable()` returns
    /// `!importunate`).  Mirror that into the lock manager's non-preemptable
    /// registry so a *different* importunate locker's steal leaves this
    /// txn's locks alone (JE `LockImpl.stealLock` skips non-preemptable
    /// owners, LockImpl.java:543).
    pub fn set_importunate(&mut self, v: bool) {
        self.importunate = v;
        if v {
            self.lock_manager.register_non_preemptable(self.id);
        } else {
            self.lock_manager.unregister_non_preemptable(self.id);
        }
    }

    /// Configures serializable (repeatable-read) isolation.
    ///
    /// When true, read locks are held until commit/abort.
    ///
    /// See `TxnManager::register_serializable` / `TxnManager::unregister_serializable`
    /// (JE `TxnManager.registerTxn` / `unRegisterTxn` `nActiveSerializable` path).
    pub fn set_serializable_isolation(&mut self, v: bool) {
        self.serializable_isolation = v;
    }

    /// Returns true if this transaction uses serializable (repeatable-read)
    /// isolation.
    ///
    /// Used by `TxnManager` bookkeeping: the manager increments
    /// `n_active_serializable` when a serializable txn begins and decrements
    /// it on commit / abort, mirroring JE `TxnManager.registerTxn` /
    /// `unRegisterTxn` (`nActiveSerializable` path).
    pub fn is_serializable(&self) -> bool {
        self.serializable_isolation
    }

    /// Configures read-committed isolation.
    ///
    /// When true, read locks are released as the cursor advances.
    ///
    pub fn set_read_committed_isolation(&mut self, v: bool) {
        self.read_committed_isolation = v;
    }

    /// Configures the default read-uncommitted isolation level for this
    /// transaction.
    ///
    /// When true, cursors created on this transaction skip read-lock
    /// acquisition entirely, allowing dirty reads of uncommitted data
    /// from other transactions.  This is the transaction-level
    /// counterpart of the per-operation `LockMode::ReadUncommitted`
    /// flag on `ReadOptions`.
    ///
    /// Resolves F2 of the May 2026 API audit: previously the
    /// `read_uncommitted_default` field was settable only by
    /// constructing a fresh `Txn` and there was no public setter, so
    /// `TransactionConfig::with_read_uncommitted(true)` was silently
    /// dropped by `Environment::begin_transaction`.
    pub fn set_read_uncommitted_default(&mut self, v: bool) {
        self.read_uncommitted_default = v;
    }

    /// Sets the per-lock timeout in milliseconds.
    ///
    /// Controls how long a lock request waits before returning a timeout error.
    pub fn set_lock_timeout(&mut self, timeout_ms: u64) {
        self.lock_timeout_ms = timeout_ms;
    }

    /// Sets the transaction-level timeout in milliseconds.
    ///
    /// A non-zero value causes `is_timed_out()` to return `true` after that
    /// many milliseconds, even if individual lock requests haven't timed out.
    ///
    pub fn set_txn_timeout(&mut self, timeout_ms: u64) {
        self.txn_timeout_ms = timeout_ms;
    }

    /// Sets the no-wait flag. When true, all lock requests are non-blocking
    /// and fail immediately if the lock is not available.
    pub fn set_no_wait(&mut self, v: bool) {
        self.no_wait = v;
    }

    /// Sets the local-write flag. See the `local_write` field doc for the
    /// write-path contract this enforces.
    ///
    /// `Txn` itself has no notion of "is my environment replicated", so its
    /// own default (see [`Self::is_local_write`]) is the common case
    /// (`true`); callers that DO know the environment is replicated are
    /// responsible for calling this to override the default for an
    /// explicit transaction that should replicate rather than write
    /// locally.
    pub fn set_local_write(&mut self, v: bool) {
        self.local_write = v;
    }

    /// Returns whether this txn is configured for local (non-replicated)
    /// writes.
    ///
    /// The natural default for a locker with no explicit configuration is
    /// `true` ("writes are local unless configured otherwise") — this holds
    /// for auto-commit lockers and for any locker in a standalone (non-
    /// replicated) environment, where the setting has no observable effect
    /// anyway since no database is replicated there. An explicit
    /// transaction on a replicated environment defaults instead to `false`
    /// (replicate normally, the common case) unless the caller opted into
    /// local-only writes via [`Self::set_local_write`].
    pub fn is_local_write(&self) -> bool {
        self.local_write
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
                let lsn =
                    lm.log(entry_type, payload, Provisional::No, flush, fsync)?;
                Ok(lsn)
            }
        }
    }

    /// TXN-1: counts the abort versions of this txn's write locks obsolete.
    ///
    /// JE `Txn.getObsoleteLsnInfo` + `maybeCountObsoleteLSN`: for each write
    /// lock, the prior (abort) version becomes reclaimable on commit and is
    /// counted obsolete via `countObsoleteNode`, EXCEPT when it was already
    /// counted at logging time.  The filters applied here:
    ///
    /// - skip NULL abort LSN or `abort_known_deleted` (nothing reclaimable);
    /// - skip `abort_data.is_some()` (embedded — already counted at logging);
    /// - de-duplicate by abort LSN (a txn may touch the same record twice).
    ///
    /// ponytail: the JE `db.isLNImmediatelyObsolete()` filter (dup-DB LNs
    /// already counted at logging) is omitted because the txn layer has no
    /// `DatabaseImpl` handle — only a `database_id`.  For a dup DB this can
    /// double-count the abort LSN obsolete, which only makes a file *more*
    /// cleanable (never deletes live data); it is conservative.  Restore the
    /// filter when the txn can resolve `database_id -> isLNImmediatelyObsolete`.
    fn count_obsolete_abort_lsns(&self) {
        let Some(lm) = &self.log_manager else {
            return;
        };
        let mut seen: HashSet<u64> = HashSet::new();
        let mut infos: Vec<(Lsn, Option<u32>, i32)> = Vec::new();
        for wli in self.write_locks.values() {
            if wli.abort_lsn == NULL_LSN.as_u64() || wli.abort_known_deleted {
                continue;
            }
            if wli.abort_data.is_some() {
                // Embedded abort version — already counted obsolete at
                // logging time. (JE maybeCountObsoleteLSN: abortData != null.)
                continue;
            }
            if !seen.insert(wli.abort_lsn) {
                continue;
            }
            infos.push((
                Lsn::from_u64(wli.abort_lsn),
                Some(wli.database_id as u32),
                wli.abort_log_size,
            ));
        }
        if !infos.is_empty() {
            lm.count_obsolete_commit_lsns(&infos);
        }
    }

    /// Returns true if there are open cursors on this transaction.
    ///
    ///
    pub fn has_open_cursors(&self) -> bool {
        self.cursor_count.load(Ordering::Relaxed) > 0
    }

    /// Returns `true` if this transaction has been prepared
    /// ([`Self::prepare`]).
    ///
    /// Prepared transactions retain their write locks and cannot be
    /// committed or aborted via [`Self::commit`] / [`Self::abort`] — the
    /// XA layer must call them through the resolved code paths.
    pub fn is_prepared(&self) -> bool {
        self.txn_flags & IS_PREPARED != 0
    }

    /// Clears the IS_PREPARED flag without writing any log entry.
    ///
    /// Used by the higher-level `noxu_db::Transaction::resolved_*_after_prepare`
    /// methods so they can invoke `abort_collect_undo()` (which has
    /// its own `is_prepared` guard) without first writing a duplicate
    /// log frame.  This is a no-op if the txn was not prepared.
    pub fn clear_prepared_flag(&mut self) {
        self.txn_flags &= !IS_PREPARED;
    }

    /// Prepares the transaction for the second phase of XA two-phase commit.
    ///
    /// 1. Checks state (must be `Open`, no open cursors).
    /// 2. Serialises a `TxnPrepareEntry` carrying (txn_id, first_lsn,
    ///    last_lsn, xid_format_id, xid_gtrid, xid_bqual) and writes it to
    ///    the WAL via the configured `LogManager`.  The frame is **fsynced**
    ///    before this method returns so a crash immediately afterwards still
    ///    allows recovery to resurrect the prepared state.
    /// 3. Marks the transaction as prepared (`IS_PREPARED` flag), which
    ///    blocks subsequent direct `commit()` / `abort()` calls.  After
    ///    this point only the XA layer's resolved paths
    ///    (`Txn::resolved_commit_after_prepare` /
    ///    `Txn::resolved_abort_after_prepare`) may finalise the txn.
    /// 4. Read locks are NOT released here — prepared txns hold every
    ///    lock until resolution so that no other txn can observe the
    ///    in-flight state.
    ///
    /// Returns the LSN of the `TxnPrepare` record, or `NULL_LSN` if the
    /// txn has no logged entries (read-only txn — the XA layer should take
    /// the `PrepareResult::ReadOnly` optimisation rather than calling
    /// this method).
    ///
    /// # Arguments
    /// * `xid_format_id`, `xid_gtrid`, `xid_bqual` — the components of the
    ///   `noxu_xa::Xid` being prepared.  Encoded the same way the XA
    ///   `PreparedLog` encodes them so recovery can round-trip the XID.
    ///
    /// # Errors
    /// * `InvalidTransaction` if the txn is not `Open` (already committed,
    ///   aborted, prepared, or flipped to MUST_ABORT).
    /// * `InvalidTransaction` if cursors remain open on this txn.
    /// * `LogError` if writing the `TxnPrepare` frame fails.
    pub fn prepare(
        &mut self,
        xid_format_id: i32,
        xid_gtrid: Vec<u8>,
        xid_bqual: Vec<u8>,
    ) -> Result<Lsn, TxnError> {
        // State machine: prepare requires Open.  IS_PREPARED transitions
        // are NOT idempotent — a second prepare() returns InvalidTransaction
        // so the XA layer cannot accidentally write two TxnPrepare frames
        // for the same txn.
        if self.is_prepared() {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "PREPARED".into(),
            });
        }
        self.check_state()?;
        if self.has_open_cursors() {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "has open cursors".into(),
            });
        }

        // No logged entries — caller should have taken the read-only
        // optimisation.  We still mark prepared (defensive), but do not
        // emit a TxnPrepare frame: there is nothing to resolve.
        if !self.has_logged_entries() {
            self.txn_flags |= IS_PREPARED;
            return Ok(NULL_LSN);
        }

        // Serialise and write the TxnPrepare frame, with fsync.
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let entry = noxu_log::entry::TxnPrepareEntry::new(
            self.id,
            timestamp_ms,
            self.first_lsn,
            self.last_lsn,
            xid_format_id,
            xid_gtrid,
            xid_bqual,
        )
        .map_err(|e| TxnError::StateError(format!("prepare: {e}")))?;
        let mut payload = Vec::with_capacity(entry.log_size());
        entry.write_to_log(&mut payload);

        // fsync=true: the prepare contract is durable-by-the-time-we-return.
        let prepare_lsn =
            self.log_entry(LogEntryType::TxnPrepare, &payload, true)?;

        // Belt-and-braces: ensure the write reached the platter even if
        // the LogManager's group-commit batched our entry.  `flush_sync_if_needed`
        // is a no-op when another committer already flushed past this LSN.
        if let Some(ref lm) = self.log_manager {
            lm.flush_sync_if_needed(prepare_lsn).map_err(TxnError::LogError)?;
        }

        // Track the prepare LSN as the new last_lsn so that, after a
        // crash, recovery can chain undo / redo correctly off it.
        self.note_log_entry(prepare_lsn.as_u64());
        self.txn_flags |= IS_PREPARED;

        Ok(prepare_lsn)
    }

    /// Resolves a prepared transaction with a commit.
    ///
    /// Used by the XA `xa_commit` path after `prepare()` has succeeded.
    /// Bypasses the IS_PREPARED guard in [`Self::commit_with_durability`]
    /// because the prepare already established the commit decision; we
    /// just need to write the durable `TxnCommit` record and release
    /// locks.
    pub fn resolved_commit_after_prepare(&mut self) -> Result<Lsn, TxnError> {
        if !self.is_prepared() {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "not prepared".into(),
            });
        }
        // Clear the prepared flag so commit_with_durability's check_state
        // path takes the normal Open route.
        self.txn_flags &= !IS_PREPARED;
        self.commit_with_durability(Durability::CommitSync)
    }

    /// Resolves a prepared transaction with an abort.
    pub fn resolved_abort_after_prepare(&mut self) -> Result<Lsn, TxnError> {
        if !self.is_prepared() {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "not prepared".into(),
            });
        }
        self.txn_flags &= !IS_PREPARED;
        self.abort()
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
        // Delegate to `commit_with_durability` with the default
        // policy. The historical inline body was nearly identical
        // and shared every silent-lock-leak defect; consolidating
        // means new error-path improvements only have to be applied
        // once.
        self.commit_with_durability(Durability::CommitSync)
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
        // Prepared txns must be resolved through
        // `resolved_abort_after_prepare`; see the matching guard in
        // `commit_with_durability` for the rationale.
        if self.is_prepared() {
            return Err(TxnError::InvalidTransaction {
                txn_id: self.id,
                state: "PREPARED".into(),
            });
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
        //
        // Synthetic auto-txns skip the `TxnAbort` WAL entry: the underlying
        // LN was logged as auto-commit (`InsertLN` / `DeleteLN` with
        // `txn_id=0`), so no synthetic abort record is required and the
        // on-disk WAL format stays identical to pre-Wave-1A auto-commit.
        // Closes the first F12 residual.
        let assigned_lsn = if self.has_logged_entries() && !self.is_auto_txn() {
            let abort = TxnAbort::new(
                self.id,
                self.last_lsn,
                0, /* master_id */
                0, /* dtvlsn */
            );
            let mut payload = Vec::with_capacity(abort.log_size());
            abort.write_to_log(&mut payload);
            self.log_entry(
                LogEntryType::TxnAbort,
                &payload,
                false, /* fsync */
            )?
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

        // Synthetic auto-txns skip the `TxnAbort` WAL entry; see
        // [`Self::abort`] for rationale.
        let assigned_lsn = if self.has_logged_entries() && !self.is_auto_txn() {
            let abort = TxnAbort::new(
                self.id,
                self.last_lsn,
                0, /* master_id */
                0, /* dtvlsn */
            );
            let mut payload = Vec::with_capacity(abort.log_size());
            abort.write_to_log(&mut payload);
            self.log_entry(
                LogEntryType::TxnAbort,
                &payload,
                false, /* fsync */
            )?
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
    /// Release every read and write lock the txn is currently
    /// holding in the lock manager and clear the per-txn maps.
    ///
    /// Called from `commit_with_durability` on every Ok and Err
    /// return path, and from `abort` after applying undo records.
    /// Idempotent — a second call after a successful first call is
    /// a no-op because `write_locks` has been cleared and
    /// `read_locks` drained.
    ///
    /// Per-lock release failures are logged via `log::error!` but do
    /// not abort the drain — losing one lock release leaks a single
    /// lock-manager entry, but losing the *whole* drain leaks every
    /// lock the txn ever held until the environment is closed.
    pub fn release_all_locks(&mut self) {
        for lsn in self.write_locks.keys().copied().collect::<Vec<_>>() {
            if let Err(e) = self.lock_manager.release(lsn, self.id) {
                log::error!(
                    "noxu-txn: lock_manager.release({lsn}, txn={}) failed \
                     during release_all_locks (write lock): {e}",
                    self.id
                );
            }
        }
        self.write_locks.clear();

        for lsn in self.read_locks.drain().collect::<Vec<_>>() {
            if let Err(e) = self.lock_manager.release(lsn, self.id) {
                log::error!(
                    "noxu-txn: lock_manager.release({lsn}, txn={}) failed \
                     during release_all_locks (read lock): {e}",
                    self.id
                );
            }
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
    /// 1. Releases the old LSN lock at the `LockManager` level.
    /// 2. Acquires a write lock on `new_lsn` at the `LockManager` level.
    /// 3. Atomically moves the `WriteLockInfo` entry from
    ///    `write_locks[old_lsn]` to `write_locks[new_lsn]`.
    ///
    /// Returns `Ok(())` if the txn held no write lock on `old_lsn`
    /// (a no-op).
    ///
    /// # Errors
    ///
    /// Either the `release(old_lsn)` or the `lock(new_lsn)` step can
    /// fail. The `WriteLockInfo` (which holds the abort image used
    /// by `abort()` to undo the write) is NEVER dropped on the error
    /// path — losing it would silently break rollback for the row:
    ///
    ///   - `release(old_lsn)` failure: WLI is still at `old_lsn`,
    ///     and the lock_manager still holds the old lock. A
    ///     subsequent `abort()` re-attempts `release(old_lsn)`,
    ///     which is logged again but does not corrupt state.
    ///   - `lock(new_lsn)` failure (after release succeeded): WLI
    ///     remains at `old_lsn` even though the lock_manager
    ///     released old_lsn. `abort()` will iterate `write_locks`,
    ///     generate the undo record, then attempt `release(old_lsn)`
    ///     which logs (lock_manager has nothing there) and
    ///     continues.
    ///
    /// In both cases the error is surfaced so the caller can abort
    /// the cursor operation rather than silently desynchronise the
    /// per-txn and lock-manager views.
    pub fn move_write_lock_to_new_lsn(
        &mut self,
        old_lsn: u64,
        new_lsn: u64,
    ) -> Result<(), TxnError> {
        // No-op if the txn doesn't hold a write lock at `old_lsn`.
        if !self.write_locks.contains_key(&old_lsn) {
            return Ok(());
        }
        // Phase 1: release old. WLI stays in write_locks until both
        // phases succeed, so an Err leaves abort info reachable.
        if let Err(e) = self.lock_manager.release(old_lsn, self.id) {
            log::error!(
                "noxu-txn: lock_manager.release({old_lsn}, txn={}) failed \
                 during move_write_lock_to_new_lsn (WLI preserved at \
                 old_lsn so abort can roll back): {e}",
                self.id
            );
            return Err(e);
        }
        // Phase 2: acquire new.
        if let Err(e) = self.lock_manager.lock(
            new_lsn,
            self.id,
            LockType::Write,
            false,
            false,
        ) {
            log::error!(
                "noxu-txn: lock_manager.lock({new_lsn}, txn={}, Write) \
                 failed during move_write_lock_to_new_lsn (WLI preserved \
                 at old_lsn so abort can roll back; lock_manager no \
                 longer holds old_lsn): {e}",
                self.id
            );
            return Err(e);
        }
        // Both phases succeeded — atomic move.
        let wli = self
            .write_locks
            .remove(&old_lsn)
            .expect("contains_key check above guarantees Some");
        self.write_locks.insert(new_lsn, wli);
        Ok(())
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
            wli.set_abort_info(
                abort_lsn,
                abort_key,
                abort_data,
                -1,
                0,
                abort_known_deleted,
                0,
                false,
            );
            wli.never_locked = false;
            wli.database_id = database_id;
        }
    }

    /// Returns true if this txn already recorded abort info for the write
    /// lock at `lsn` (i.e. `never_locked == false`).
    ///
    /// TXN-1: the cursor calls this at LN-write time to apply JE's
    /// `currLsn != abortLsn` predicate (LN.java:685).  On the FIRST write to
    /// a record the lock is freshly acquired (`never_locked == true`) and the
    /// prior version IS the abort version — counted obsolete at COMMIT.  On a
    /// SUBSEQUENT write the WriteLockInfo already carries abort info
    /// (`never_locked == false`), so the prior version differs from the abort
    /// version and is counted obsolete now, at write time.
    pub fn write_lock_abort_recorded(&self, lsn: u64) -> bool {
        self.write_locks.get(&lsn).map(|w| !w.never_locked).unwrap_or(false)
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

        // Normal Locker.lock always passes jumpAheadOfWaiters=false
        // (Locker.java:503).  Importunate (HA ReplayTxn) is NOT jump-ahead:
        // it is handled by the lock-steal path inside the lock manager
        // (LockManager.waitForLock -> stealLock, LockManager.java:552), so
        // route importunate requests through `lock_importunate_with_timeout`.
        let grant = if self.importunate {
            self.lock_manager.lock_importunate_with_timeout(
                lsn,
                self.id,
                lock_type,
                non_blocking || self.no_wait,
                self.lock_timeout_ms,
            )?
        } else {
            self.lock_manager.lock_with_timeout(
                lsn,
                self.id,
                lock_type,
                non_blocking || self.no_wait,
                false, // jumpAheadOfWaiters
                self.lock_timeout_ms,
            )?
        };

        // Track the lock.
        // NoneNeeded means lock_type was None — check_state ran above but
        // no lock was acquired; skip tracking so we don't insert a phantom
        // entry into read_locks that would be released at commit.
        if grant == LockGrantType::NoneNeeded {
            return Ok(LockResult { grant, write_lock_info: None });
        }
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

    fn owns_any_lock(&self, lsn: u64) -> bool {
        self.read_locks.contains(&lsn) || self.write_locks.contains_key(&lsn)
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
            let _ = self.lock_manager.lock(
                new_lsn,
                self.id,
                LockType::Write,
                false,
                false,
            );
            self.write_locks.insert(new_lsn, wli);
        } else if self.read_locks.remove(&old_lsn) {
            // Migrate read lock.
            let _ = self.lock_manager.release(old_lsn, self.id);
            let _ = self.lock_manager.lock(
                new_lsn,
                self.id,
                LockType::Read,
                false,
                false,
            );
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
        txn.abort().unwrap(); // returns Lsn

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

    /// TXN-1: a recording observer that captures every obsolete LSN fired
    /// through the log write path, so the commit-time abort-LSN counting can
    /// be asserted.
    #[derive(Default)]
    struct RecordingObserver {
        obsolete: std::sync::Mutex<Vec<(u64, Option<u32>, i32)>>,
    }
    impl noxu_log::LogWriteObserver for RecordingObserver {
        fn count_new_entry(
            &self,
            _f: u32,
            _o: u32,
            _s: u32,
            _ln: bool,
            _in: bool,
            _db: Option<u32>,
        ) {
        }
        fn count_obsolete(&self, ob: noxu_log::ObsoleteLsn) {
            self.obsolete.lock().unwrap().push((
                ob.lsn.as_u64(),
                ob.db_id,
                ob.size,
            ));
        }
    }

    /// TXN-1: `Txn::commit` counts each write-lock's abort LSN obsolete,
    /// applying JE's `maybeCountObsoleteLSN` filters and de-duplicating.
    ///
    /// JE `Txn.getObsoleteLsnInfo` -> `LogManager` counts each
    /// `obsoleteWriteLockInfo` via `countObsoleteNode`.
    #[test]
    fn test_commit_counts_abort_lsns_obsolete() {
        use noxu_log::FileManager;
        let dir = tempfile::TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
        );
        let mut lm_inner = LogManager::new(fm, 3, 1024 * 1024, 4096);
        let observer = Arc::new(RecordingObserver::default());
        lm_inner.set_write_observer(observer.clone());
        let lm = Arc::new(lm_inner);

        let lock_manager = Arc::new(LockManager::new());
        let mut txn = Txn::with_log_manager(7, lock_manager, lm);

        // Three write locks on three records.  Record A has a real,
        // reclaimable abort version; B is a fresh insert (abort_known_deleted);
        // C has an embedded abort version (abort_data Some -> already counted).
        txn.note_log_entry(1000);
        txn.lock(1000, LockType::Write, false).unwrap();
        txn.set_write_lock_abort_info(
            1000, /*abort_lsn=*/ 500, None, None, false, 9,
        );
        txn.lock(2000, LockType::Write, false).unwrap();
        txn.set_write_lock_abort_info(
            2000,
            NULL_LSN.as_u64(),
            None,
            None,
            true,
            9,
        );
        txn.lock(3000, LockType::Write, false).unwrap();
        txn.set_write_lock_abort_info(
            3000,
            /*abort_lsn=*/ 700,
            None,
            Some(vec![1, 2, 3]),
            false,
            9,
        );

        txn.commit().unwrap();

        let obsolete = observer.obsolete.lock().unwrap();
        // Only record A's abort LSN (500) is counted: B is known-deleted, C is
        // embedded (already counted at logging).
        assert_eq!(
            obsolete.len(),
            1,
            "only the one reclaimable abort version should be counted; got {obsolete:?}"
        );
        assert_eq!(obsolete[0].0, 500, "abort LSN of record A");
        assert_eq!(obsolete[0].1, Some(9), "per-DB axis carries the db id");
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

    // ============================================================
    // Prepare path (XA crash-durable two-phase commit, wave 3-2)
    // ============================================================

    #[test]
    fn test_prepare_writes_txn_prepare_frame() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();
        let mut txn = Txn::with_log_manager(77, lock_manager, lm.clone());
        txn.note_log_entry(500);
        txn.lock(500, LockType::Write, false).unwrap();

        let eol_before = lm.get_end_of_log();
        let prep_lsn =
            txn.prepare(1, b"gtrid".to_vec(), b"bqual".to_vec()).unwrap();
        let eol_after = lm.get_end_of_log();

        assert!(!prep_lsn.is_null());
        assert!(eol_after.as_u64() > eol_before.as_u64());
        assert!(txn.is_prepared());
        // Locks are retained across prepare.
        assert_eq!(txn.n_locks(), 1);
    }

    #[test]
    fn test_prepare_blocks_direct_commit() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();
        let mut txn = Txn::with_log_manager(78, lock_manager, lm);
        txn.note_log_entry(501);
        txn.lock(501, LockType::Write, false).unwrap();
        txn.prepare(1, b"g".to_vec(), b"b".to_vec()).unwrap();

        let res = txn.commit();
        assert!(matches!(
            res,
            Err(TxnError::InvalidTransaction { state, .. }) if state == "PREPARED"
        ));
        // Same for direct abort.
        let res = txn.abort();
        assert!(matches!(
            res,
            Err(TxnError::InvalidTransaction { state, .. }) if state == "PREPARED"
        ));
    }

    #[test]
    fn test_resolved_commit_after_prepare_completes() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();
        let mut txn = Txn::with_log_manager(79, lock_manager, lm);
        txn.note_log_entry(502);
        txn.lock(502, LockType::Write, false).unwrap();
        txn.prepare(1, b"g".to_vec(), b"b".to_vec()).unwrap();

        let commit_lsn = txn.resolved_commit_after_prepare().unwrap();
        assert!(!commit_lsn.is_null());
        assert_eq!(txn.get_state(), TxnState::Committed);
        // Locks released.
        assert_eq!(txn.n_locks(), 0);
        // Prepared flag cleared.
        assert!(!txn.is_prepared());
    }

    #[test]
    fn test_resolved_abort_after_prepare_completes() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();
        let mut txn = Txn::with_log_manager(80, lock_manager, lm);
        txn.note_log_entry(503);
        txn.lock(503, LockType::Write, false).unwrap();
        txn.prepare(1, b"g".to_vec(), b"b".to_vec()).unwrap();

        let abort_lsn = txn.resolved_abort_after_prepare().unwrap();
        assert!(!abort_lsn.is_null());
        assert_eq!(txn.get_state(), TxnState::Aborted);
        assert_eq!(txn.n_locks(), 0);
    }

    #[test]
    fn test_prepare_twice_is_protocol_error() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();
        let mut txn = Txn::with_log_manager(81, lock_manager, lm);
        txn.note_log_entry(504);
        txn.lock(504, LockType::Write, false).unwrap();
        txn.prepare(1, b"g".to_vec(), b"b".to_vec()).unwrap();
        let res = txn.prepare(1, b"g".to_vec(), b"b".to_vec());
        assert!(matches!(res, Err(TxnError::InvalidTransaction { .. })));
    }

    #[test]
    fn test_prepare_read_only_returns_null_lsn() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();
        let mut txn = Txn::with_log_manager(82, lock_manager, lm);
        // No note_log_entry: this is a read-only txn.
        let prep = txn.prepare(1, b"g".to_vec(), b"b".to_vec()).unwrap();
        assert!(prep.is_null());
        assert!(txn.is_prepared());
    }

    #[test]
    fn test_prepare_after_commit_is_protocol_error() {
        let lock_manager = Arc::new(LockManager::new());
        let (lm, _dir) = make_log_manager_in_tempdir();
        let mut txn = Txn::with_log_manager(83, lock_manager, lm);
        txn.note_log_entry(505);
        txn.lock(505, LockType::Write, false).unwrap();
        txn.commit().unwrap();
        let res = txn.prepare(1, b"g".to_vec(), b"b".to_vec());
        assert!(matches!(res, Err(TxnError::InvalidTransaction { .. })));
    }

    /// TXN-4 regression test: `lock(NONE)` on a MustAbort txn must return
    /// `InvalidTransaction`, not silently succeed.
    ///
    /// This exercises the state-check path that `CursorImpl::lock_ln` calls
    /// for read-uncommitted transactions: it calls `guard.lock(lsn,
    /// LockType::None, false)` which runs `check_state()` before returning
    /// `NoneNeeded`. Pre-fix, read-uncommitted early-returned before calling
    /// `lock` at all, so an Aborted/MustAbort txn doing a dirty read was not
    /// caught. Post-fix, `lock(NONE)` surfaces the state error.
    #[test]
    fn test_txn4_lock_none_on_must_abort_returns_error() {
        let mut txn = create_test_txn();

        // Force into MustAbort state.
        txn.set_only_abortable();
        assert_eq!(txn.get_state(), TxnState::MustAbort);

        // lock(NONE) must run check_state and return InvalidTransaction.
        let result = txn.lock(999, LockType::None, false);
        assert!(
            matches!(result, Err(TxnError::InvalidTransaction { .. })),
            "TXN-4: lock(NONE) on MustAbort must return InvalidTransaction; got {result:?}"
        );
    }

    /// TXN-4: lock(NONE) on an Open txn returns NoneNeeded without tracking.
    #[test]
    fn test_txn4_lock_none_on_open_returns_none_needed() {
        let mut txn = create_test_txn();
        let result = txn.lock(999, LockType::None, false).unwrap();
        assert_eq!(
            result.grant,
            LockGrantType::NoneNeeded,
            "TXN-4: lock(NONE) on Open must return NoneNeeded"
        );
    }
}
