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
//! `ReplayTxn` and applies each LN under a write lock that readers cannot see
//! through until the txn `commit`s (`getReplayTxn` / `repTxn.commit`).  Our
//! engine is lock-based, not MVCC, but we get the same *visibility* contract
//! with a simpler, faithful model:
//!
//! - A transactional LN (`txn_id = Some`) is BUFFERED in the active-txn map,
//!   not applied to the tree yet (this is the "provisional" state — a read
//!   cannot see it).
//! - At the txn-COMMIT entry the buffered LNs are applied to the tree in log
//!   order (`Replay.replayEntry` LOG_TXN_COMMIT → `repTxn.commit`); now reads
//!   see them.
//! - At the txn-ABORT entry the buffered LNs are DISCARDED, never applied
//!   (`Replay.replayEntry` LOG_TXN_ABORT → `repTxn.abort`).
//! - A non-transactional LN (`txn_id = None`) is applied immediately (it is
//!   already committed by definition).
//!
//! This produces exactly the tree the crash-recovery redo pass would: recovery
//! redoes only committed LNs (uncommitted ones are undone), so live-apply and
//! recovery agree.
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
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use noxu_recovery::{LnRecord, LogEntry, apply_redo_ln};
use noxu_util::Lsn;

use crate::environment_impl::EnvironmentImpl;
use crate::file_manager_scanner::FileManagerLogScanner;

/// Live replica-apply driver.
///
/// Holds a reference to the replica's [`EnvironmentImpl`] (to resolve the live
/// tree per db_id) and the per-txn buffer of provisional LNs.  Driven by the
/// replica receive loop (one call per streamed entry, after the WAL write).
///
/// Port of `Replay` (the per-replica replay state).
pub struct ReplicaReplay {
    /// The replica's live environment — used to resolve the per-db tree.
    env: Arc<EnvironmentImpl>,

    /// Active replica transactions: txn_id → buffered (LnRecord, lsn) ops.
    ///
    /// Port of `Replay.activeTxns` / `ReplayTxn`.  An LN is buffered here
    /// (NOT applied to the tree) until its txn commits; on commit the buffer
    /// is drained into the tree, on abort it is dropped.
    active_txns: HashMap<u64, Vec<(LnRecord, Lsn)>>,

    /// Highest VLSN whose effects are now visible in the live tree.
    ///
    /// Advanced after a committed/non-transactional apply.  REP-10 will gate
    /// reads on this; this module only publishes it.  Shared as an
    /// `Arc<AtomicU64>` so a reader thread can observe it without locking the
    /// replay driver.
    last_applied_vlsn: Arc<AtomicU64>,
}

impl ReplicaReplay {
    /// Create a new replay driver for `env`.
    pub fn new(env: Arc<EnvironmentImpl>) -> Self {
        Self {
            env,
            active_txns: HashMap::new(),
            last_applied_vlsn: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Return a shared handle to the last-applied-VLSN counter.
    ///
    /// REP-10 seam: a consistency policy reads this to decide whether a read
    /// may proceed.
    pub fn last_applied_vlsn_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.last_applied_vlsn)
    }

    /// The highest VLSN whose effects are visible in the live tree.
    pub fn last_applied_vlsn(&self) -> u64 {
        self.last_applied_vlsn.load(Ordering::Acquire)
    }

    /// Apply one streamed log entry to the live replica state.
    ///
    /// Called by the replica receive loop AFTER the entry has been written to
    /// the WAL and registered in the VLSN index.  `lsn` is the LSN the WAL
    /// write returned (used as the slot LSN for the redo currency check, so
    /// live-apply and recovery agree on slot LSNs).
    ///
    /// Port of `Replay.replayEntry`:
    /// - LOG_TXN_COMMIT  → drain & apply the txn's buffered LNs, advance VLSN.
    /// - LOG_TXN_ABORT   → discard the txn's buffered LNs.
    /// - data LN         → buffer (transactional) or apply (non-txn).
    /// - everything else → note only (checkpoint markers, NameLN, …).
    pub fn apply_entry(
        &mut self,
        vlsn: u64,
        entry_type: u8,
        payload: &[u8],
        lsn: Lsn,
    ) {
        // Decode the streamed payload into a recovery `LogEntry`, reusing the
        // SAME decoder the file-backed recovery scanner uses
        // (FileManagerLogScanner::parse_payload).  No fork.  We do not have
        // the on-disk header flags here (the master already validated them),
        // so pass flags=0 and the streamed VLSN.
        let decoded = FileManagerLogScanner::parse_payload(
            entry_type,
            Bytes::copy_from_slice(payload),
            if vlsn > 0 { Some(vlsn) } else { None },
            0,
        );

        match decoded {
            // Replay.replayEntry: LOG_TXN_COMMIT branch (repTxn.commit).
            Some(LogEntry::TxnCommit(rec)) => {
                self.commit_txn(rec.txn_id);
                self.advance_vlsn(vlsn);
            }
            // Replay.replayEntry: LOG_TXN_ABORT branch (repTxn.abort).
            Some(LogEntry::TxnAbort(rec)) => {
                self.abort_txn(rec.txn_id);
                // An abort still advances the stream position; the txn's
                // effects are simply never made visible.
                self.advance_vlsn(vlsn);
            }
            // Replay.replayEntry: data-operation branch (applyLN).
            Some(LogEntry::Ln(rec)) => {
                match rec.txn_id {
                    Some(txn_id) => {
                        // Transactional: buffer as provisional; a read cannot
                        // see it until the commit entry streams in.
                        self.active_txns
                            .entry(txn_id)
                            .or_default()
                            .push((rec, lsn));
                    }
                    None => {
                        // Non-transactional LN: already committed, apply now.
                        self.apply_ln(&rec, lsn);
                        self.advance_vlsn(vlsn);
                    }
                }
            }
            // Checkpoint markers, NameLN, rollback markers, INs, etc.: the WAL
            // write + VLSN index update already happened in
            // EnvironmentLogWriter; the live tree does not need them to serve
            // LN reads.  (JE notes these but the LN tree apply is the part
            // that matters for read replicas.)  Still advance the position.
            _ => {
                self.advance_vlsn(vlsn);
            }
        }
    }

    /// Drain and apply a committed transaction's buffered LNs, in log order.
    ///
    /// Port of `Replay.replayEntry` LOG_TXN_COMMIT → `repTxn.commit`: the
    /// txn's write-locked records become visible.
    fn commit_txn(&mut self, txn_id: u64) {
        if let Some(ops) = self.active_txns.remove(&txn_id) {
            for (rec, lsn) in &ops {
                self.apply_ln(rec, *lsn);
            }
        }
    }

    /// Discard a rolled-back transaction's buffered LNs.
    ///
    /// Port of `Replay.replayEntry` LOG_TXN_ABORT → `repTxn.abort`: the
    /// provisional operations are never made visible.
    fn abort_txn(&mut self, txn_id: u64) {
        self.active_txns.remove(&txn_id);
    }

    /// Apply one LN to its database's live tree via the shared recovery-redo
    /// mutation.
    ///
    /// Port of `Replay.applyLN` (`DbInternal.putForReplay` /
    /// `deleteForReplay`).  Resolves the live tree for `rec.db_id`; if the
    /// database has not been opened on this replica there is no tree to apply
    /// to yet (the WAL still holds the entry, so a later open + recovery — or
    /// a later open that transplants the recovered tree — will materialise
    /// it).
    fn apply_ln(&mut self, rec: &LnRecord, lsn: Lsn) {
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
                // Mutex poisoning is treated as fatal elsewhere; here we still
                // apply so the replica tree does not silently lag the WAL.
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
        // Monotone CAS-free advance: the receive loop already enforces
        // strictly-increasing VLSNs (LOG-7), so a plain store after a load
        // is safe for the single replay thread; use fetch_max for safety.
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
    fn open_env_with_db(
    ) -> (Arc<EnvironmentImpl>, u64, Arc<std::sync::RwLock<noxu_tree::Tree>>)
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
    #[test]
    fn test_txn_ln_invisible_until_commit() {
        let (env, db_id, tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        // Stream a transactional insert (txn 7) — buffered, not visible.
        let p = ln_payload(db_id, Some(7), b"k1", Some(b"v1"));
        replay.apply_entry(1, insert_ln_txn(), &p, Lsn::new(0, 100));
        assert!(
            tree.read().unwrap().search(b"k1").is_none(),
            "uncommitted txn LN must NOT be visible before commit"
        );
        assert_eq!(replay.last_applied_vlsn(), 0, "no commit yet");

        // Stream the commit — now visible.
        let c = txn_commit_payload(7);
        replay.apply_entry(2, txn_commit_type(), &c, Lsn::new(0, 200));
        assert!(
            tree.read().unwrap().search(b"k1").is_some(),
            "committed txn LN must be visible after commit"
        );
        assert_eq!(replay.last_applied_vlsn(), 2);
    }

    /// An aborted txn's LNs are never applied.
    #[test]
    fn test_txn_abort_discards_lns() {
        let (env, db_id, tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        let p = ln_payload(db_id, Some(9), b"gone", Some(b"x"));
        replay.apply_entry(1, insert_ln_txn(), &p, Lsn::new(0, 100));
        let a = txn_abort_payload(9);
        replay.apply_entry(2, txn_abort_type(), &a, Lsn::new(0, 200));

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
        replay.apply_entry(1, insert_ln(), &p, Lsn::new(0, 100));
        assert!(
            tree.read().unwrap().search(b"now").is_some(),
            "non-txn LN must be visible immediately"
        );
        assert_eq!(replay.last_applied_vlsn(), 1);
    }

    /// Multiple LNs in one txn all become visible atomically at commit.
    #[test]
    fn test_multi_ln_txn_commit() {
        let (env, db_id, tree) = open_env_with_db();
        let mut replay = ReplicaReplay::new(Arc::clone(&env));

        for (i, k) in [b"a", b"b", b"c"].iter().enumerate() {
            let p = ln_payload(db_id, Some(3), *k, Some(b"v"));
            replay.apply_entry(
                (i + 1) as u64,
                insert_ln_txn(),
                &p,
                Lsn::new(0, 100 + i as u32),
            );
            // None visible until commit.
            assert!(tree.read().unwrap().search(*k).is_none());
        }
        let c = txn_commit_payload(3);
        replay.apply_entry(4, txn_commit_type(), &c, Lsn::new(0, 200));
        for k in [b"a", b"b", b"c"].iter() {
            assert!(
                tree.read().unwrap().search(*k).is_some(),
                "all committed LNs visible after commit"
            );
        }
    }
}
