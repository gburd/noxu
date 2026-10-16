//! REP-7 (A): live replay-apply on a streaming read replica.
//!
//! Port of `com.sleepycat.je.rep.impl.node.Replay` (the replica-side apply of
//! the master's replication stream).
//!
//! ## What this is
//!
//! Today a streaming replica writes the master's log entries to its own WAL
//! (a byte-shadow) and advances the VLSN index, but it does NOT materialise a
//! live in-memory tree — the tree is only built by crash-recovery on restart,
//! so the replica cannot serve fresh reads.
//!
//! `ReplicaReplay` closes that gap: as each entry streams in (AFTER it has
//! been written to the WAL and registered in the VLSN index by
//! `EnvironmentLogWriter`), the replica APPLIES committed operations to its
//! LIVE in-memory tree — exactly as JE `Replay.replayEntry` applies the
//! master's operations through a `Cursor`.  A read on the replica then reads
//! the live tree (the same `Arc<RwLock<Tree>>` that opened cursors traverse),
//! so it returns the master's committed data WITHOUT a restart.
//!
//! ## Faithful reuse, no fork
//!
//! The tree mutation itself is performed by [`noxu_recovery::apply_redo_ln`] —
//! the SAME function the crash-recovery redo pass uses.  This is a
//! crash-consistency requirement: the replica's WAL is the source of truth and
//! a subsequent crash-recovery must reproduce the SAME tree the live-apply
//! produced.  Forking the mutation logic between live-apply and recovery-redo
//! would be a correctness bug (a divergence is worse than warm-standby).  JE
//! `Replay.applyLN` likewise re-uses the cursor put/delete machinery rather
//! than reimplementing tree mutation.
//!
//! ## Transaction model (provisional-apply, resolved at commit)
//!
//! JE `Replay.replayEntry` tracks active replica transactions in a
//! `ReplayTxn` that is **importunate** (`setImportunate(true)`,
//! ReplayTxn.java:113) and applies each LN under that txn's write lock,
//! through a `Cursor` (`getReplayTxn` / `applyLN` / `repTxn.commit`).  A local
//! reader that holds a lock on the record is PREEMPTED (its locker gets a
//! `LockPreemptedException`); a reader that arrives while the replay txn holds
//! the write lock BLOCKS until the replay commits/aborts.  That — not an
//! MVCC snapshot — is how a replica read sees a replayed transaction
//! all-or-nothing.
//!
//! Noxu is lock-based too, and the importunate machinery already exists
//! (`Txn::set_importunate` → `LockManager::lock_importunate_with_timeout` /
//! `steal_lock`; readers get `TxnError::LockPreempted`).  We reproduce JE's
//! lock protocol faithfully (FID-REPLAY-1), with ONE deliberate adaptation of
//! the tree *mutation* to preserve crash-consistency (see below):
//!
//! - A transactional LN (`txn_id = Some`) is write-LOCKED EAGERLY as it
//!   streams in, under a per-master-txn importunate [`Txn`] (the ReplayTxn):
//!   the lock is taken on the record's CURRENT lock id — the slot LSN when the
//!   key exists (read under the BIN latch), else the synthetic key id for a
//!   brand-new key, exactly as the master's writer locks (D-LOCK-LOCUS).  A
//!   concurrent reader locking that same id now conflicts with / is preempted
//!   by the replay.  The LN is also BUFFERED; its tree mutation is applied at
//!   the txn-COMMIT entry, in log order, and each write lock is MOVED to the
//!   applied slot's new LSN (JE/C2 `move_write_lock_to_new_lsn`).  The commit
//!   then releases the txn's locks (`repTxn.commit`), so a reader proceeds and
//!   sees the committed data.
//! - At the txn-ABORT entry the buffered LNs are DISCARDED, never applied, and
//!   the txn's write locks are released (`repTxn.abort`).
//! - A non-transactional LN (`txn_id = None`) is committed by definition, so
//!   it is applied immediately under a short-lived importunate ReplayTxn (lock
//!   the slot, mutate, move the lock to the new slot, release).
//!
//! ## Why the mutation is still buffered-to-commit (crash-consistency)
//!
//! JE applies each LN to the tree AS it streams in (immediate), relying on
//! recovery UNDO to roll back a replica that crashes mid-txn.  Noxu's recovery
//! also undoes uncommitted LNs, so immediate apply would be crash-safe too —
//! but buffering the MUTATION to commit lets us keep using
//! [`noxu_recovery::apply_redo_ln`], the SAME function the crash-recovery redo
//! pass uses, so a live-applied tree and a recovery-rebuilt tree are
//! bit-identical (a hard invariant: a divergence is worse than warm-standby).
//! The owner-approved choice is therefore: hold JE's importunate write LOCK
//! for the full stream→commit window (the isolation half is fully faithful),
//! but perform the tree MUTATION via the recovery-redo path at commit (the
//! crash-consistency half).  Visibility to a lock-taking reader is identical
//! to JE: excluded from the first streamed LN until commit.
//!
//! ## Residual (mid-operation preemption) — documented, not a regression
//!
//! JE's `checkPreempted` runs at each lock op, so a reader is preempted when
//! it NEXT touches the lock manager; a reader that already returned a value
//! and does no further lock op is not retro-actively preempted.  Noxu matches
//! this exactly (the preempted flag is observed on the reader's next
//! lock-taking op).  See the replay fidelity report for the full note.
//!
//! ## REP-10 seam
//!
//! `ReplicaReplay` advances `last_applied_vlsn` after each apply/commit.  That
//! is the hook a future REP-10 (consistency policies) will gate reads on (a
//! `ConsistencyPolicy` blocks a read until `last_applied_vlsn >=
//! required_vlsn`).  REP-10 is a SEPARATE follow-up; this module only exposes
//! the value, it does not gate on it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use bytes::Bytes;
use noxu_recovery::{LnRecord, LogEntry, apply_redo_ln};
use noxu_txn::{LockManager, LockType, Locker, Txn};
use noxu_util::Lsn;

use crate::environment_impl::EnvironmentImpl;
use crate::error::DbiError;
use crate::file_manager_scanner::FileManagerLogScanner;

/// Locker-id base for replay transactions.
///
/// JE's `ReplayTxn` is a real `Txn` carrying the MASTER's transaction id as a
/// mandated id (`ReplayTxn(envImpl, config, txnId, logger)`).  Noxu keys the
/// lock manager on a plain `i64` locker id; to guarantee a replay locker never
/// collides with a locally-begun txn (whose ids come from the positive
/// `TxnManager::next_txn_id` space) we offset the master txn id into a
/// dedicated negative range.  A non-transactional LN (no master txn id) gets a
/// fresh ephemeral id from `REPLAY_EPHEMERAL_LOCKER`, which also lives in the
/// negative range.
const REPLAY_LOCKER_BASE: i64 = i64::MIN / 2;

/// Per-master-txn replay state: the importunate `ReplayTxn` plus the buffered
/// LNs that will be drained into the tree at commit.
///
/// Port of JE `Replay.activeTxns` entries (`ReplayTxn`).  The `Txn` here IS
/// the ReplayTxn: created with `set_importunate(true)` so its write-lock
/// requests STEAL from (preempt) local reader lockers, exactly as JE
/// `ReplayTxn` constructor calls `setImportunate(true)` (ReplayTxn.java:113).
struct ReplayTxnState {
    /// The importunate replay transaction holding this txn's write locks.
    txn: Txn,
    /// Buffered `(record, wal_lsn)` ops, applied to the tree in log order at
    /// commit (crash-consistency: the drain reproduces exactly the tree the
    /// recovery-redo pass would — see the module doc).
    ops: Vec<(LnRecord, Lsn)>,
}

/// Live replica-apply driver.
///
/// Holds a reference to the replica's [`EnvironmentImpl`] (to resolve the live
/// tree per db_id and the shared [`LockManager`]) and the per-txn replay
/// state.  Driven by the replica receive loop (one call per streamed entry,
/// after the WAL write).
///
/// Port of `Replay` (the per-replica replay state).
pub struct ReplicaReplay {
    /// The replica's live environment — used to resolve the per-db tree and
    /// the shared lock manager.
    env: Arc<EnvironmentImpl>,

    /// Shared lock manager (the SAME one replica readers lock against), so a
    /// replay write lock conflicts with / preempts a concurrent reader.  Port
    /// of the env-wide `LockManager` JE `ReplayTxn` locks through.
    lock_manager: Arc<LockManager>,

    /// Active replica transactions: master txn_id → replay state (the
    /// importunate ReplayTxn + its buffered LNs).
    ///
    /// Port of `Replay.activeTxns` / `ReplayTxn`.  An LN is write-LOCKED
    /// eagerly as it streams in (so a concurrent reader blocks on / is
    /// preempted by it) and BUFFERED here; its tree mutation is applied on
    /// commit, dropped on abort.
    active_txns: HashMap<u64, ReplayTxnState>,

    /// Monotonically-decreasing allocator for ephemeral (non-txn LN) replay
    /// locker ids, kept disjoint from the master-txn-id-derived ids and from
    /// the positive local-txn id space.
    ephemeral_locker: AtomicI64,

    /// Highest VLSN whose effects are now visible in the live tree.
    ///
    /// Advanced after a committed/non-transactional apply.  REP-10 gates reads
    /// on this; this module only publishes it.  Shared as an
    /// `Arc<AtomicU64>` so a reader thread can observe it without locking the
    /// replay driver.
    last_applied_vlsn: Arc<AtomicU64>,

    /// GAP-REPLAY-7: the replica's in-memory Durable-Txn VLSN, extracted from
    /// each replayed commit/abort TxnEnd record.  Port of JE
    /// `Replay.updateDTVLSN` + `ReplayTxn.validateDTVLSN`
    /// (Replay.java:629-651, ReplayTxn.java:166-190): DTVLSNs are
    /// non-decreasing (except the one post->pre-DTVLSN-stream transition to
    /// UNINITIALIZED) and `DTVLSN(vlsn) <= vlsn`.  Starts UNINITIALIZED (0).
    last_replayed_dtvlsn: i64,
}

impl ReplicaReplay {
    /// Create a new replay driver for `env`.
    pub fn new(env: Arc<EnvironmentImpl>) -> Self {
        let lock_manager = Arc::clone(env.get_lock_manager());
        Self {
            env,
            lock_manager,
            active_txns: HashMap::new(),
            ephemeral_locker: AtomicI64::new(REPLAY_LOCKER_BASE / 2),
            last_applied_vlsn: Arc::new(AtomicU64::new(0)),
            last_replayed_dtvlsn: 0,
        }
    }

    /// Return a shared handle to the last-applied-VLSN counter.
    ///
    /// REP-10 seam: a consistency policy reads this to decide whether a read
    /// may proceed.
    pub fn last_applied_vlsn_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.last_applied_vlsn)
    }

    /// Clear buffered provisional transactions and RELEASE their write locks.
    ///
    /// Called by the replica's outer reconnect loop on every full re-sync
    /// (HA Gap B).  Closer in spirit to JE `Replay.abortOldTxns()` ("When
    /// mastership changes, all inflight replay transactions are aborted",
    /// Replay.java:388): any txn still buffered here when a stream ends without
    /// a matching commit/abort was, by construction, never made visible in the
    /// tree (its LNs are still buffered), so dropping it is a complete, safe
    /// discard equivalent to an abort — but we MUST also release the write
    /// locks it holds, or they would pin records against local readers forever.
    /// Does NOT touch `last_applied_vlsn` (a `ConsistencyTracker` installed at
    /// replica-thread spawn keeps a reference to that handle).
    pub fn reset(&mut self) {
        for (_txn_id, mut state) in self.active_txns.drain() {
            // Release every write lock the aborted replay txn holds (JE
            // ReplayTxn.abort releases locks); do not apply the buffered ops.
            state.txn.release_all_locks();
        }
    }

    /// The highest VLSN whose effects are visible in the live tree.
    pub fn last_applied_vlsn(&self) -> u64 {
        self.last_applied_vlsn.load(Ordering::Acquire)
    }

    /// Apply one streamed log entry to the live replica state.
    ///
    /// Called by the replica receive loop AFTER the entry has been written to
    /// the WAL and registered in the VLSN index.  `lsn` is the LSN the WAL
    /// write returned (the slot LSN the tree mutation + lock-move key on, so
    /// live-apply and recovery agree on slot LSNs).
    ///
    /// Port of `Replay.replayEntry`:
    /// - LOG_TXN_COMMIT  → drain & apply the txn's buffered LNs, move+hold then
    ///   release its write locks, advance VLSN.
    /// - LOG_TXN_ABORT   → discard the txn's buffered LNs, release its locks.
    /// - data LN         → eagerly write-LOCK the record (importunate), buffer
    ///   it (transactional) or apply+release it (non-txn).
    /// - everything else → note only.
    pub fn apply_entry(
        &mut self,
        vlsn: u64,
        entry_type: u8,
        payload: &[u8],
        lsn: Lsn,
    ) -> Result<(), DbiError> {
        let decoded = FileManagerLogScanner::parse_payload(
            entry_type,
            Bytes::copy_from_slice(payload),
            if vlsn > 0 { Some(vlsn) } else { None },
            0,
        );

        match decoded {
            // Replay.replayEntry: LOG_TXN_COMMIT branch (repTxn.commit).
            Some(LogEntry::TxnCommit(rec)) => {
                // GAP-REPLAY-7: validate + track the DTVLSN before applying
                // (JE updateDTVLSN runs before repTxn.commit).
                self.validate_and_track_dtvlsn(rec.dtvlsn, vlsn)?;
                self.commit_txn(rec.txn_id);
                self.advance_vlsn(vlsn);
            }
            // Replay.replayEntry: LOG_TXN_ABORT branch (repTxn.abort).
            Some(LogEntry::TxnAbort(rec)) => {
                self.validate_and_track_dtvlsn(rec.dtvlsn, vlsn)?;
                self.abort_txn(rec.txn_id);
                self.advance_vlsn(vlsn);
            }
            // Replay.replayEntry: data-operation branch (applyLN).
            Some(LogEntry::Ln(rec)) => {
                match rec.txn_id {
                    Some(txn_id) => {
                        // Transactional: take the importunate write lock NOW
                        // (so a concurrent reader blocks / is preempted) and
                        // buffer the op; the tree mutation + lock-move happen
                        // when the commit entry streams in.  `lock_txn_ln`
                        // guarantees the per-txn ReplayTxn state exists, so the
                        // buffer push below always finds it.
                        self.lock_txn_ln(txn_id, &rec);
                        if let Some(state) = self.active_txns.get_mut(&txn_id) {
                            state.ops.push((rec, lsn));
                        }
                    }
                    None => {
                        // Non-transactional LN: already committed by
                        // definition.  Apply immediately under a short-lived
                        // importunate ReplayTxn (lock the slot, mutate, move
                        // the lock to the new slot, then release).
                        self.apply_non_txn_ln(&rec, lsn);
                        self.advance_vlsn(vlsn);
                    }
                }
            }
            // Checkpoint markers, NameLN, rollback markers, INs, etc.
            _ => {
                self.advance_vlsn(vlsn);
            }
        }
        Ok(())
    }

    /// GAP-REPLAY-7: validate + track the DTVLSN from a replayed commit/abort.
    ///
    /// Port of JE `Replay.updateDTVLSN` + `ReplayTxn.validateDTVLSN`
    /// (Replay.java:629-651, ReplayTxn.java:166-190):
    /// - `DTVLSN(vlsn) <= vlsn` MUST hold (a durable VLSN can never exceed the
    ///   commit's own VLSN); a violation is fatal (`IllegalStateException` ->
    ///   here a `DbiError` the replica receive loop surfaces, tearing the
    ///   stream down -- the EnvironmentFailureException-equivalent restart).
    /// - DTVLSNs are non-decreasing, EXCEPT the single post->pre-DTVLSN-stream
    ///   transition (a positive value -> UNINITIALIZED/0), which is allowed.
    ///
    /// A `dtvlsn` of `None` is the pre-DTVLSN (UNINITIALIZED) case: track 0,
    /// never a violation.
    fn validate_and_track_dtvlsn(
        &mut self,
        dtvlsn: Option<u64>,
        vlsn: u64,
    ) -> Result<(), DbiError> {
        let Some(dtvlsn) = dtvlsn else {
            // Pre-DTVLSN stream (UNINITIALIZED): JE sets the in-memory DTVLSN
            // to UNINITIALIZED; a post->pre transition is the one allowed
            // decrease.
            self.last_replayed_dtvlsn = 0;
            return Ok(());
        };
        // DTVLSN(vlsn) <= vlsn (ReplayTxn.validateDTVLSN).
        if vlsn > 0 && dtvlsn > vlsn {
            return Err(DbiError::EnvironmentFailure {
                reason: format!(
                    "GAP-REPLAY-7: replayed DTVLSN({dtvlsn}) > its own                      VLSN({vlsn}); the replication stream is corrupt                      (fail-closed)"
                ),
            });
        }
        // Non-decreasing (post->post): a decrease to a smaller positive value
        // is a corrupt stream (JE: DTVLSNs should not decrease; the only
        // allowed decrease is the post->pre transition handled above).
        let prev = self.last_replayed_dtvlsn;
        if prev > 0 && (dtvlsn as i64) < prev {
            return Err(DbiError::EnvironmentFailure {
                reason: format!(
                    "GAP-REPLAY-7: replayed DTVLSN({dtvlsn}) decreased below                      the last replayed DTVLSN({prev}); the replication stream                      is corrupt (fail-closed)"
                ),
            });
        }
        self.last_replayed_dtvlsn = dtvlsn as i64;
        Ok(())
    }

    /// Stable per-master-txn replay locker id (disjoint from local txn ids).
    fn txn_locker_id(master_txn_id: u64) -> i64 {
        REPLAY_LOCKER_BASE.wrapping_add(master_txn_id as i64)
    }

    /// Construct an importunate ReplayTxn with the given locker id.
    ///
    /// Port of JE `ReplayTxn(envImpl, config, txnId, logger)` which calls
    /// `setImportunate(true)` — its write-lock requests STEAL from preemptable
    /// (reader) lockers rather than wait, and the locker is registered
    /// non-preemptable so a different importunate steal leaves it alone.
    fn new_replay_txn(&self, locker_id: i64) -> Txn {
        self.lock_manager.register_locker_label(locker_id, "replay");
        let mut txn = Txn::new(locker_id, Arc::clone(&self.lock_manager));
        txn.set_importunate(true);
        txn
    }

    /// The lock id a writer/reader of `key` in `db_id` agrees on: the slot's
    /// CURRENT LSN when the key exists (read under the BIN latch via
    /// `slot_lsn_any`), or the synthetic key-coordination id when it does not
    /// yet exist.  Mirrors the master's `lock_write_before_log` /
    /// `lock_slot_current_lsn` choice (D-LOCK-LOCUS: real LSN for an existing
    /// key, synthetic id for a brand-new one).
    fn lock_id_for(&self, rec: &LnRecord) -> u64 {
        let null = noxu_util::NULL_LSN.as_u64();
        if let Some(tree_arc) = self.env.replica_tree_for_db(rec.db_id) {
            let slot = match tree_arc.read() {
                Ok(t) => t.slot_lsn_any(&rec.key),
                Err(p) => p.into_inner().slot_lsn_any(&rec.key),
            };
            if let Some((slot_lsn, _kd)) = slot
                && !slot_lsn.is_null()
            {
                return slot_lsn.as_u64();
            }
        }
        let _ = null;
        Lsn::synthetic_key_lock_id(rec.db_id, &rec.key)
    }

    /// Take the importunate WRITE lock for a buffered transactional LN on the
    /// record's CURRENT lock id (JE `ReplayTxn` locks each LN via the cursor
    /// as it is applied; here we lock eagerly at stream time and defer the
    /// tree mutation to commit, so a reader is excluded for the whole window).
    fn lock_txn_ln(&mut self, txn_id: u64, rec: &LnRecord) {
        let lock_id = self.lock_id_for(rec);
        // Ensure the ReplayTxn exists so the lock is held under it.
        if !self.active_txns.contains_key(&txn_id) {
            let txn = self.new_replay_txn(Self::txn_locker_id(txn_id));
            self.active_txns
                .insert(txn_id, ReplayTxnState { txn, ops: Vec::new() });
        }
        let state = self.active_txns.get_mut(&txn_id).expect("inserted above");
        if let Err(e) = state.txn.lock(lock_id, LockType::Write, false) {
            // Importunate locks steal rather than wait, so a failure here is a
            // genuine fault (a different importunate/non-preemptable owner, or
            // a wedged lock table).  Log and continue applying: dropping the
            // lock does not corrupt the tree (the buffered mutation still
            // happens at commit), it only weakens the isolation window.
            log::error!(
                "replica-replay: importunate write-lock {lock_id:#x} for \
                 replay txn {txn_id} failed: {e}"
            );
        }
    }

    /// Drain and apply a committed transaction's buffered LNs, in log order,
    /// moving each write lock to the applied slot's new LSN, then release all
    /// of the txn's locks.
    ///
    /// Port of `Replay.replayEntry` LOG_TXN_COMMIT → `repTxn.commit`.
    fn commit_txn(&mut self, txn_id: u64) {
        if let Some(mut state) = self.active_txns.remove(&txn_id) {
            for (rec, lsn) in &state.ops {
                let old_lock_id = self.lock_id_for(rec);
                self.apply_ln_mutation(rec, *lsn);
                // Move the write lock from the pre-apply lock id to the new
                // slot LSN (JE/C2 `move_write_lock_to_new_lsn`): the old lock
                // is retained so an abort before-image stays matchable; the
                // new slot is now covered until this txn releases.
                let new_lock_id = lsn.as_u64();
                if new_lock_id != old_lock_id
                    && new_lock_id != noxu_util::NULL_LSN.as_u64()
                    && let Err(e) = state
                        .txn
                        .move_write_lock_to_new_lsn(old_lock_id, new_lock_id)
                {
                    log::error!(
                        "replica-replay: move write lock \
                         {old_lock_id:#x}->{new_lock_id:#x} for replay \
                         txn {txn_id} failed: {e}"
                    );
                }
            }
            // Commit the ReplayTxn: release every write lock it holds so local
            // readers can now proceed and SEE the committed data (JE
            // repTxn.commit releases locks).
            state.txn.release_all_locks();
        }
    }

    /// Discard a rolled-back transaction's buffered LNs and release its locks.
    ///
    /// Port of `Replay.replayEntry` LOG_TXN_ABORT → `repTxn.abort`.
    fn abort_txn(&mut self, txn_id: u64) {
        if let Some(mut state) = self.active_txns.remove(&txn_id) {
            // Never apply the buffered ops; release the write locks the aborted
            // txn held (JE ReplayTxn.abort).
            state.txn.release_all_locks();
        }
    }

    /// Apply a non-transactional LN immediately under a short-lived
    /// importunate ReplayTxn: lock the current slot id, mutate the tree, move
    /// the lock to the new slot, then release.  Non-txn LNs are committed by
    /// definition so the lock is held only for the apply window.
    fn apply_non_txn_ln(&mut self, rec: &LnRecord, lsn: Lsn) {
        let locker_id = self.ephemeral_locker.fetch_sub(1, Ordering::Relaxed);
        let mut txn = self.new_replay_txn(locker_id);
        let old_lock_id = self.lock_id_for(rec);
        if let Err(e) = txn.lock(old_lock_id, LockType::Write, false) {
            log::error!(
                "replica-replay: importunate write-lock {old_lock_id:#x} for \
                 non-txn LN failed: {e}"
            );
        }
        self.apply_ln_mutation(rec, lsn);
        let new_lock_id = lsn.as_u64();
        if new_lock_id != old_lock_id
            && new_lock_id != noxu_util::NULL_LSN.as_u64()
            && let Err(e) =
                txn.move_write_lock_to_new_lsn(old_lock_id, new_lock_id)
        {
            log::error!(
                "replica-replay: move non-txn write lock \
                 {old_lock_id:#x}->{new_lock_id:#x} failed: {e}"
            );
        }
        txn.release_all_locks();
    }

    /// Mutate the live tree for one LN via the shared recovery-redo mutation.
    ///
    /// Port of `Replay.applyLN` tree effect.  Uses
    /// [`noxu_recovery::apply_redo_ln`] — the SAME function crash-recovery redo
    /// uses — so live-apply and a subsequent recovery produce the identical
    /// tree (the crash-consistency invariant in the module doc).  The write
    /// lock excluding readers is taken by the callers around this mutation.
    fn apply_ln_mutation(&self, rec: &LnRecord, lsn: Lsn) {
        let Some(tree_arc) = self.env.replica_tree_for_db(rec.db_id) else {
            log::trace!(
                "replica-replay: db {} not open on replica; LN buffered in \
                 WAL only (vlsn={:?})",
                rec.db_id,
                rec.vlsn,
            );
            return;
        };
        match tree_arc.write() {
            Ok(mut tree) => apply_redo_ln(&mut tree, rec, lsn),
            Err(poisoned) => {
                let mut tree = poisoned.into_inner();
                apply_redo_ln(&mut tree, rec, lsn);
            }
        }
    }

    /// Advance the last-applied VLSN high-water mark (monotone).
    fn advance_vlsn(&self, vlsn: u64) {
        if vlsn == 0 {
            return;
        }
        self.last_applied_vlsn.fetch_max(vlsn, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_log::entry::LnLogEntry;
    use noxu_util::{NULL_LSN, NULL_VLSN};

    // Entry-type bytes used by the stream wire format.
    fn ln_payload(
        db_id: u64,
        txn_id: Option<i64>,
        key: &[u8],
        data: Option<&[u8]>,
    ) -> Vec<u8> {
        use bytes::BytesMut;
        let entry = LnLogEntry::new(
            db_id,
            txn_id,
            NULL_LSN,
            false,
            None,
            None,
            NULL_VLSN,
            0,
            false,
            key.to_vec(),
            data.map(|d| d.to_vec()),
            0,
            NULL_VLSN,
        );
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        buf.to_vec()
    }

    fn txn_commit_payload(txn_id: i64) -> Vec<u8> {
        use bytes::BytesMut;
        use noxu_log::entry::TxnEndEntry;
        let e = TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    fn txn_abort_payload(txn_id: i64) -> Vec<u8> {
        use bytes::BytesMut;
        use noxu_log::entry::TxnEndEntry;
        let e = TxnEndEntry::new_abort(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    /// Open an env, open a database, return (env, db_id, tree_arc).
    fn open_env_with_db()
    -> (Arc<EnvironmentImpl>, u64, Arc<std::sync::RwLock<noxu_tree::Tree>>)
    {
        use crate::database_config::DatabaseConfig;
        let dir = tempfile::TempDir::new().unwrap();
        let env =
            Arc::new(EnvironmentImpl::new(dir.path(), false, true).unwrap());
        let mut cfg = DatabaseConfig::new();
        cfg.set_allow_create(true).set_transactional(true);
        // open_database registers the tree in db_trees_registry.
        let db = env.open_database("repl_db", &cfg).unwrap();
        let db_id = db.read().get_id().id() as u64;
        let tree = env.replica_tree_for_db(db_id).unwrap();
        (env, db_id, tree)
    }

    fn insert_ln_txn() -> u8 {
        noxu_log::LogEntryType::InsertLNTxn.type_num()
    }
    fn insert_ln() -> u8 {
        noxu_log::LogEntryType::InsertLN.type_num()
    }
    fn txn_commit_type() -> u8 {
        noxu_log::LogEntryType::TxnCommit.type_num()
    }
    fn txn_abort_type() -> u8 {
        noxu_log::LogEntryType::TxnAbort.type_num()
    }

    /// HEADLINE (A): a transactional LN is NOT visible until its commit
    /// streams in, then it IS visible (provisional-apply resolved at commit).
    ///
    /// JE: `ReplayTest.testResumedTransaction` (portable core) — an op that is
    /// still provisional stays invisible until its own commit streams in
    /// (`Replay.replayEntry` LOG_TXN_COMMIT -> `repTxn.commit`). The
    /// resumed-across-reopen variant is ported in
    /// `noxu-rep/tests/je_replay_node_tck.rs`.
    #[test]
    fn test_txn_ln_invisible_until_commit() {
        let (env, db_id, tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        // Stream a transactional insert (txn 7) — buffered, not visible.
        let p = ln_payload(db_id, Some(7), b"k1", Some(b"v1"));
        replay.apply_entry(1, insert_ln_txn(), &p, Lsn::new(0, 100)).unwrap();
        assert!(
            tree.read().unwrap().search(b"k1").is_none(),
            "uncommitted txn LN must NOT be visible before commit"
        );
        assert_eq!(replay.last_applied_vlsn(), 0, "no commit yet");

        // Stream the commit — now visible.
        let c = txn_commit_payload(7);
        replay.apply_entry(2, txn_commit_type(), &c, Lsn::new(0, 200)).unwrap();
        assert!(
            tree.read().unwrap().search(b"k1").is_some(),
            "committed txn LN must be visible after commit"
        );
        assert_eq!(replay.last_applied_vlsn(), 2);
    }

    /// An aborted txn's LNs are never applied.
    ///
    /// JE: `Replay.replayEntry` LOG_TXN_ABORT -> `repTxn.abort` (the abort
    /// half of the `ReplayTest` replay contract).
    #[test]
    fn test_txn_abort_discards_lns() {
        let (env, db_id, tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        let p = ln_payload(db_id, Some(9), b"gone", Some(b"x"));
        replay.apply_entry(1, insert_ln_txn(), &p, Lsn::new(0, 100)).unwrap();
        let a = txn_abort_payload(9);
        replay.apply_entry(2, txn_abort_type(), &a, Lsn::new(0, 200)).unwrap();

        assert!(
            tree.read().unwrap().search(b"gone").is_none(),
            "aborted txn LN must never be visible"
        );
    }

    /// A non-transactional LN is applied immediately.
    #[test]
    fn test_non_txn_ln_applied_immediately() {
        let (env, db_id, tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        let p = ln_payload(db_id, None, b"now", Some(b"here"));
        replay.apply_entry(1, insert_ln(), &p, Lsn::new(0, 100)).unwrap();
        assert!(
            tree.read().unwrap().search(b"now").is_some(),
            "non-txn LN must be visible immediately"
        );
        assert_eq!(replay.last_applied_vlsn(), 1);
    }

    /// Multiple LNs in one txn all become visible atomically at commit.
    ///
    /// JE: `ReplayTest.testBasicDatabaseOperations` (portable core) — the
    /// replica converges to the master's state after replaying a committed
    /// multi-op transaction. Full LN insert/update/delete convergence +
    /// VLSN-order + the DDL-catalog deviation are in
    /// `noxu-rep/tests/je_replay_node_tck.rs`.
    #[test]
    fn test_multi_ln_txn_commit() {
        let (env, db_id, tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        for (i, k) in [b"a", b"b", b"c"].iter().enumerate() {
            let p = ln_payload(db_id, Some(3), *k, Some(b"v"));
            replay
                .apply_entry(
                    (i + 1) as u64,
                    insert_ln_txn(),
                    &p,
                    Lsn::new(0, 100 + i as u32),
                )
                .unwrap();
            // None visible until commit.
            assert!(tree.read().unwrap().search(*k).is_none());
        }
        let c = txn_commit_payload(3);
        replay.apply_entry(4, txn_commit_type(), &c, Lsn::new(0, 200)).unwrap();
        for k in [b"a", b"b", b"c"].iter() {
            assert!(
                tree.read().unwrap().search(*k).is_some(),
                "all committed LNs visible after commit"
            );
        }
    }

    /// FID-REPLAY-1 WITNESS: while a multi-LN replay transaction is open, the
    /// record it is applying is WRITE-LOCKED under the importunate ReplayTxn,
    /// so a would-be reader taking a read lock on that record CONFLICTS (sees
    /// nothing of the half-applied txn); once the commit entry streams in the
    /// lock is released and the reader proceeds.  All-or-nothing.
    ///
    /// RED at 81b2f048: ReplicaReplay buffered the LNs with NO lock-manager
    /// interaction, so a reader's non-blocking read lock on the record was
    /// granted immediately mid-replay (the record was not protected at all).
    #[test]
    fn test_replay_txn_write_locks_record_until_commit() {
        use noxu_txn::LockType;

        let (env, db_id, _tree) = open_env_with_db();
        let lm = Arc::clone(env.get_lock_manager());
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        // The lock id the replay takes for a brand-new key (absent slot) is
        // the synthetic key-coordination id -- the SAME id a reader of that
        // absent key would probe (D-LOCK-LOCUS).
        let key = b"k1";
        let lock_id = noxu_util::Lsn::synthetic_key_lock_id(db_id, key);
        let reader_id: i64 = 999_001;

        // Stream a transactional insert (txn 7) -- buffered + write-locked.
        let p = ln_payload(db_id, Some(7), key, Some(b"v1"));
        replay.apply_entry(1, insert_ln_txn(), &p, Lsn::new(0, 100)).unwrap();

        // A reader taking a NON-BLOCKING read lock on the record must be
        // DENIED (the importunate replay write lock holds it).
        let r = lm.lock(lock_id, reader_id, LockType::Read, true, false);
        assert!(
            matches!(r, Err(noxu_txn::TxnError::LockNotAvailable { .. })),
            "reader must conflict with the open replay txn write lock, got {r:?}"
        );

        // Stream the commit -- releases the replay txn locks.
        let c = txn_commit_payload(7);
        replay.apply_entry(2, txn_commit_type(), &c, Lsn::new(0, 200)).unwrap();

        // Now the reader's read lock is granted (the record is committed and
        // the replay txn released its locks).  Note the committed slot moved
        // to the new LSN, so an existing-key reader would lock that; here the
        // synthetic id is free again, which is what a fresh probe sees.
        let r2 = lm.lock(lock_id, reader_id, LockType::Read, true, false);
        assert!(
            r2.is_ok(),
            "after commit the replay lock is released; reader proceeds, got {r2:?}"
        );
        let _ = lm.release(lock_id, reader_id);
    }

    /// FID-REPLAY-1 WITNESS (preemption): a reader that already HOLDS a read
    /// lock on a record is PREEMPTED when the importunate replay write-locks
    /// it -- the reader's next lock op surfaces `LockPreempted`
    /// (JE LockPreemptedException via the importunate steal, checkPreempted on
    /// the next op).
    ///
    /// RED at 81b2f048: the replay never touched the lock manager, so the
    /// prior reader was never preempted (its lock stood, and the replay
    /// mutated the tree underneath it).
    #[test]
    fn test_replay_preempts_prior_reader() {
        use noxu_txn::LockType;

        let (env, db_id, _tree) = open_env_with_db();
        let lm = Arc::clone(env.get_lock_manager());
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        let key = b"pk";
        let lock_id = noxu_util::Lsn::synthetic_key_lock_id(db_id, key);
        let reader_id: i64 = 999_002;

        // A reader takes and HOLDS a read lock on the record first.
        lm.lock(lock_id, reader_id, LockType::Read, false, false)
            .expect("reader acquires read lock");

        // The importunate replay now applies a txnal write to the same key:
        // it STEALS the lock (importunate), preempting the reader.
        let p = ln_payload(db_id, Some(8), key, Some(b"v"));
        replay.apply_entry(1, insert_ln_txn(), &p, Lsn::new(0, 100)).unwrap();

        // The reader's NEXT lock op surfaces LockPreempted (JE
        // checkPreempted). We probe with another read-lock request.
        let r = lm.lock(lock_id, reader_id, LockType::Read, false, false);
        assert!(
            matches!(r, Err(noxu_txn::TxnError::LockPreempted { .. })),
            "preempted reader's next lock op must be LockPreempted, got {r:?}"
        );

        // Clean up: abort the replay txn (releases its stolen lock).
        let a = txn_abort_payload(8);
        replay.apply_entry(2, txn_abort_type(), &a, Lsn::new(0, 200)).unwrap();
    }

    fn txn_commit_payload_dtvlsn(txn_id: i64, dtvlsn: u64) -> Vec<u8> {
        use bytes::BytesMut;
        use noxu_log::entry::TxnEndEntry;
        let e = TxnEndEntry::new_commit(
            txn_id,
            NULL_LSN,
            0,
            0,
            noxu_util::Vlsn::new(dtvlsn as i64),
        );
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    /// GAP-REPLAY-7 WITNESS: a replayed commit whose DTVLSN exceeds its own
    /// VLSN is FATAL (JE `ReplayTxn.validateDTVLSN`: `DTVLSN(vlsn) <= vlsn`,
    /// else IllegalStateException).  Noxu returns a `DbiError::EnvironmentFailure`
    /// the replica receive loop surfaces, tearing the stream down.
    ///
    /// RED at 81b2f048: ReplicaReplay discarded the DTVLSN on replay entirely
    /// (no extraction, no validation), so a bad DTVLSN was accepted silently.
    #[test]
    fn test_replay_rejects_dtvlsn_greater_than_vlsn() {
        let (env, db_id, _tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        // Buffer a txnal insert at vlsn=1.
        let p = ln_payload(db_id, Some(7), b"k", Some(b"v"));
        replay.apply_entry(1, insert_ln_txn(), &p, Lsn::new(0, 100)).unwrap();

        // Commit at vlsn=2 carrying DTVLSN=99 (> 2): must fail closed.
        let c = txn_commit_payload_dtvlsn(7, 99);
        let r = replay.apply_entry(2, txn_commit_type(), &c, Lsn::new(0, 200));
        assert!(
            matches!(r, Err(DbiError::EnvironmentFailure { .. })),
            "DTVLSN > VLSN on a replayed commit must be fatal, got {r:?}"
        );
    }

    /// GAP-REPLAY-7 WITNESS (non-decreasing): a replayed DTVLSN that decreases
    /// below the last replayed positive DTVLSN is fatal (JE: DTVLSNs should
    /// not decrease; the only allowed decrease is post->pre-DTVLSN-stream).
    #[test]
    fn test_replay_rejects_decreasing_dtvlsn() {
        let (env, db_id, _tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        // Commit 1 @ vlsn=5 carries DTVLSN=5 (valid: 5 <= 5).
        let p1 = ln_payload(db_id, Some(1), b"a", Some(b"v"));
        replay.apply_entry(4, insert_ln_txn(), &p1, Lsn::new(0, 90)).unwrap();
        let c1 = txn_commit_payload_dtvlsn(1, 5);
        replay
            .apply_entry(5, txn_commit_type(), &c1, Lsn::new(0, 100))
            .unwrap();

        // Commit 2 @ vlsn=9 carries DTVLSN=3 (< last 5): must fail closed.
        let p2 = ln_payload(db_id, Some(2), b"b", Some(b"v"));
        replay.apply_entry(8, insert_ln_txn(), &p2, Lsn::new(0, 190)).unwrap();
        let c2 = txn_commit_payload_dtvlsn(2, 3);
        let r = replay.apply_entry(9, txn_commit_type(), &c2, Lsn::new(0, 200));
        assert!(
            matches!(r, Err(DbiError::EnvironmentFailure { .. })),
            "a decreasing DTVLSN must be fatal, got {r:?}"
        );
    }
}
