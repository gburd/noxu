//! HA gap item 2b, headline: a DIVERGED replica is reconciled against a new
//! master by the **bilateral syncup matchpoint protocol over the wire**, before
//! any replication stream starts.
//!
//! ```text
//!   replica ──SYNCUP (EntryRequest/Entry/StartStream)──► master
//!           ◄── matchpoint agreed ──
//!   replica: roll divergent tail back to matchpoint   (or REFUSE, see below)
//!   replica ──PEER_FEEDER──► master   (stream from matchpoint + 1)
//! ```
//!
//! ## What the gap was
//!
//! `negotiate_syncup` is a VLSN **range-availability** check: it compares
//! numbers (`first <= needed <= last`) and is structurally blind to *contents*.
//! A replica whose log contains entries past the matchpoint that the master's
//! history does not include — an old master, or a node that kept accepting
//! writes through a partition — passes the range check and then streams the
//! master's history *on top of* its own divergent records. Its log and tree
//! then permanently disagree with the cluster's accepted history.
//!
//! ## What closes it
//!
//! The replica negotiates a matchpoint against the master's real log over the
//! `SYNCUP` service (JE `ReplicaFeederSyncup` ↔ `FeederReplicaSyncup`), then
//! either rolls its divergent tail back to that matchpoint or REFUSES to
//! replicate at all. Both outcomes are asserted here.
//!
//! ## Two outcomes, both proven
//!
//! - `test_diverged_replica_rolls_back_over_the_wire`: a tail of PROVISIONAL
//!   (transactional) LNs — never applied to the live tree by `ReplicaReplay`,
//!   which buffers them until their commit arrives — is discarded. The
//!   divergent entries are gone; everything at/below the matchpoint survives;
//!   the replica then converges on the master's history.
//! - `test_diverged_replica_with_applied_tail_is_refused`: a tail containing a
//!   NON-transactional LN was already applied to the live B-tree, and this
//!   build performs JE `Replay.rollback` steps 1 and 3–5 but not step 2 (the
//!   in-memory `TxnChain` revert). Truncating the log alone would leave the
//!   tree holding a record the cluster never accepted, so the syncup is
//!   REFUSED with an operator-visible reason and **nothing is truncated**. Log
//!   truncation is irreversible; a refusal is recoverable via network restore.
//!
//! Both tests also assert the **safety invariant** directly: no VLSN at or
//! below the negotiated matchpoint is ever discarded, in either outcome.

use std::sync::Arc;

use noxu_dbi::EnvironmentImpl;
use noxu_log::LogEntryType;
use noxu_rep::stream::{SyncupLogView, SyncupView};
use noxu_rep::{RepConfig, ReplicatedEnvironment, SyncupAction};
use noxu_util::Vlsn;

/// TxnCommit: a sync point AND a txn end — a matchpoint candidate.
const COMMIT: LogEntryType = LogEntryType::TxnCommit;
/// Transactional LN: buffered by `ReplicaReplay` until its commit streams in,
/// so it is never applied to the live tree on its own → safe to roll back.
const TXN_LN: LogEntryType = LogEntryType::InsertLNTxn;
/// Non-transactional LN: applied to the live tree IMMEDIATELY → NOT safe to
/// roll back in this build (no in-memory revert).
const PLAIN_LN: LogEntryType = LogEntryType::InsertLN;

fn cfg(name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder("gap2b_group", name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home.to_path_buf())
        .build()
}

/// Write a VLSN-tagged entry to the node's real log and register it in its
/// shared VLSN index — the two halves of applying a replicated entry.
fn apply(
    rep: &ReplicatedEnvironment,
    env: &EnvironmentImpl,
    vlsn: u64,
    ty: LogEntryType,
    payload: &[u8],
) {
    let lm = env.get_log_manager().expect("log manager");
    let lsn = lm
        .log_with_vlsn(ty, payload, vlsn, true, false)
        .expect("log_with_vlsn");
    rep.register_vlsn_typed(vlsn, lsn.file_number(), lsn.file_offset(), ty);
}

/// Append local, non-replicated (un-VLSN'd) entries so this node's log LAYOUT
/// differs from its peer's. Real cluster members always differ this way, and it
/// is what makes the matchpoint search's content-based record comparison
/// (JE `OutputWireRecord.match`) load-bearing rather than accidentally
/// satisfied by identical logs.
fn add_local_history(env: &EnvironmentImpl, n: u8) {
    let lm = env.get_log_manager().expect("log manager");
    for i in 0..n {
        lm.log(
            PLAIN_LN,
            &[0xE0 | i; 24],
            noxu_log::Provisional::No,
            true,
            false,
        )
        .expect("local history entry");
    }
}

/// A master + a replica, each with a live env, a bound TCP dispatcher, and a
/// common replicated history of `COMMIT` sync points at VLSNs `1..=common`.
struct Pair {
    master_dir: tempfile::TempDir,
    replica_dir: tempfile::TempDir,
    master_env: Arc<EnvironmentImpl>,
    replica_env: Arc<EnvironmentImpl>,
    master: Arc<ReplicatedEnvironment>,
    replica: Arc<ReplicatedEnvironment>,
}

impl Pair {
    fn new(common: u64) -> Self {
        let master_dir = tempfile::TempDir::new().unwrap();
        let replica_dir = tempfile::TempDir::new().unwrap();
        let master_env = Arc::new(
            EnvironmentImpl::new(master_dir.path(), false, true).unwrap(),
        );
        let replica_env = Arc::new(
            EnvironmentImpl::new(replica_dir.path(), false, true).unwrap(),
        );
        let master = Arc::new(
            ReplicatedEnvironment::new(cfg("m", master_dir.path())).unwrap(),
        );
        let replica = Arc::new(
            ReplicatedEnvironment::new(cfg("r", replica_dir.path())).unwrap(),
        );
        // with_environment registers the SYNCUP service on each node's
        // dispatcher — that registration is what makes the handshake below
        // reachable at all.
        master.with_environment(Arc::clone(&master_env));
        replica.with_environment(Arc::clone(&replica_env));

        // Divergent log LAYOUTS (different local histories, different lengths)
        // so the same replicated record lands at a different LSN on each node.
        add_local_history(&master_env, 2);
        add_local_history(&replica_env, 5);

        // Common accepted history: VLSNs 1..=common on BOTH nodes.
        for v in 1..=common {
            let payload = [v as u8; 8];
            apply(&master, &master_env, v, COMMIT, &payload);
            apply(&replica, &replica_env, v, COMMIT, &payload);
        }
        Self {
            master_dir,
            replica_dir,
            master_env,
            replica_env,
            master,
            replica,
        }
    }

    fn flush(&self) {
        self.master_env.get_log_manager().unwrap().flush_sync().ok();
        self.replica_env.get_log_manager().unwrap().flush_sync().ok();
    }

    fn master_addr(&self) -> std::net::SocketAddr {
        self.master.bound_addr().expect("master dispatcher bound")
    }

    fn replica_view(&self) -> SyncupLogView {
        SyncupLogView::scan(self.replica_dir.path()).unwrap()
    }

    fn master_view(&self) -> SyncupLogView {
        SyncupLogView::scan(self.master_dir.path()).unwrap()
    }

    /// Assert the two nodes hold the same record contents at every VLSN in
    /// `1..=common` but at DIFFERENT LSNs — the precondition that makes the
    /// content-based matchpoint comparison necessary.
    fn assert_layouts_diverged(&self, common: u64) {
        self.flush();
        let (m, r) = (self.master_view(), self.replica_view());
        let mut any_lsn_differs = false;
        for v in 1..=common {
            let vl = Vlsn::new(v as i64);
            let (me, re) = (m.entry(vl).unwrap(), r.entry(vl).unwrap());
            assert_eq!(
                me.fingerprint, re.fingerprint,
                "VLSN {v} must hold the same record contents on both nodes"
            );
            any_lsn_differs |= me.lsn != re.lsn;
        }
        assert!(
            any_lsn_differs,
            "the two logs must differ in LAYOUT, else an (incorrect) \
             LSN-equality matchpoint predicate would pass this test"
        );
    }

    fn close(self) {
        let _ = self.replica.close();
        let _ = self.master.close();
    }
}

// ---------------------------------------------------------------------------
// Outcome 1: divergent tail of provisional LNs → rolled back over the wire.
// ---------------------------------------------------------------------------

/// A replica whose log diverged past the matchpoint negotiates that matchpoint
/// with the master over the real `SYNCUP` service, rolls the divergent tail
/// back, and then converges on the master's history. Nothing at or below the
/// matchpoint is lost.
#[test]
fn test_diverged_replica_rolls_back_over_the_wire() {
    let p = Pair::new(5);
    p.assert_layouts_diverged(5);

    // DIVERGENCE. The replica accepted writes at VLSNs 6,7 from an old master
    // (or during a partition); the new master's history has DIFFERENT records
    // at 6,7 and a further entry at 8. The replica's 6,7 were never accepted by
    // the cluster.
    //
    // They are transactional LNs whose commits never arrived, so `ReplicaReplay`
    // buffered them and never applied them to the live tree: the safety gate
    // (`classify_tail`) permits discarding them.
    apply(&p.replica, &p.replica_env, 6, TXN_LN, b"OLD-6-AA");
    apply(&p.replica, &p.replica_env, 7, TXN_LN, b"OLD-7-BB");
    apply(&p.master, &p.master_env, 6, TXN_LN, b"NEW-6-CC");
    apply(&p.master, &p.master_env, 7, TXN_LN, b"NEW-7-DD");
    apply(&p.master, &p.master_env, 8, TXN_LN, b"NEW-8-EE");
    p.flush();

    assert_eq!(p.replica.get_current_vlsn(), 7, "replica diverged to VLSN 7");
    assert_eq!(p.master.get_current_vlsn(), 8, "master at VLSN 8");

    // Record what the replica held at/below the matchpoint, to prove none of
    // it is lost by the rollback (the safety invariant).
    let before = p.replica_view();
    let kept: Vec<(i64, u64)> = (1..=5)
        .map(|v| {
            (
                v,
                before
                    .entry(Vlsn::new(v))
                    .expect("pre-matchpoint entry")
                    .fingerprint,
            )
        })
        .collect();

    // THE HANDSHAKE, over the real wire against the master's SYNCUP service.
    let action = p
        .replica
        .syncup_with_feeder_at(p.master_addr())
        .expect("syncup handshake");

    assert_eq!(
        action,
        SyncupAction::RolledBack { matchpoint_vlsn: 5, start_vlsn: 6 },
        "the bilateral protocol must agree on matchpoint 5 and roll the \
         divergent tail back to it — not merely report range availability"
    );

    // The divergent tail is GONE: dropped from the VLSN index...
    assert_eq!(
        p.replica.get_current_vlsn(),
        5,
        "VLSN index truncated to the matchpoint"
    );
    // ...and made invisible in the log, so a re-scan (and a later recovery
    // redo pass) no longer sees it.
    let after = p.replica_view();
    for gone in [6i64, 7] {
        assert!(
            after.entry(Vlsn::new(gone)).is_none(),
            "divergent VLSN {gone} must be discarded by the rollback"
        );
    }

    // SAFETY INVARIANT: everything at or below the matchpoint is untouched.
    for (v, fingerprint) in &kept {
        assert_eq!(
            after.entry(Vlsn::new(*v)).map(|e| e.fingerprint),
            Some(*fingerprint),
            "VLSN {v} is at/below the matchpoint and MUST survive: the \
             rollback may only discard entries outside the accepted history"
        );
    }

    // CONVERGENCE: streaming resumes at matchpoint + 1 and the replica takes
    // the master's winning history for 6,7,8.
    for (v, payload) in
        [(6u64, &b"NEW-6-CC"[..]), (7, b"NEW-7-DD"), (8, b"NEW-8-EE")]
    {
        apply(&p.replica, &p.replica_env, v, TXN_LN, payload);
    }
    p.flush();

    assert_eq!(p.replica.get_current_vlsn(), 8, "replica caught up to VLSN 8");

    // NO CORRUPTION, NO LOSS: the replica and master now hold the identical
    // record at every VLSN of the master's history.
    let (rf, mf) = (p.replica_view(), p.master_view());
    for v in 1i64..=8 {
        assert_eq!(
            rf.entry(Vlsn::new(v)).map(|e| e.fingerprint),
            mf.entry(Vlsn::new(v)).map(|e| e.fingerprint),
            "converged: same record at VLSN {v} on replica and master"
        );
    }

    p.close();
}

// ---------------------------------------------------------------------------
// Outcome 2: divergent tail already applied to the tree → REFUSED.
// ---------------------------------------------------------------------------

/// A replica whose divergent tail contains a NON-transactional LN — already
/// applied to its live B-tree by `ReplicaReplay`, with no in-memory revert in
/// this build — must have its syncup REFUSED, with the log left intact.
///
/// This is the conservative half of the design: the divergence is DETECTED and
/// reported to the operator rather than silently truncated. A refusal is
/// recoverable (network restore); an unsafe truncation is not.
#[test]
fn test_diverged_replica_with_applied_tail_is_refused() {
    let p = Pair::new(5);
    p.assert_layouts_diverged(5);

    // DIVERGENCE with an APPLIED entry: VLSN 6 is transactional (buffered, safe
    // to discard) but VLSN 7 is a plain LN, which `ReplicaReplay::apply_ln`
    // pushed straight into the live tree.
    apply(&p.replica, &p.replica_env, 6, TXN_LN, b"OLD-6-AA");
    apply(&p.replica, &p.replica_env, 7, PLAIN_LN, b"OLD-7-APPLIED");
    apply(&p.master, &p.master_env, 6, TXN_LN, b"NEW-6-CC");
    apply(&p.master, &p.master_env, 7, TXN_LN, b"NEW-7-DD");
    p.flush();

    let before = p.replica_view();
    let vlsn_count_before =
        (1i64..=7).filter(|v| before.entry(Vlsn::new(*v)).is_some()).count();
    assert_eq!(vlsn_count_before, 7, "replica holds VLSNs 1..=7 pre-syncup");

    let action = p
        .replica
        .syncup_with_feeder_at(p.master_addr())
        .expect("syncup handshake");

    match action {
        SyncupAction::DivergedRefused { matchpoint_vlsn, tail_len, reason } => {
            assert_eq!(
                matchpoint_vlsn, 5,
                "the matchpoint is still correctly negotiated (5); only the \
                 TRUNCATION is refused"
            );
            assert_eq!(tail_len, 2, "the diverged tail is VLSNs 6 and 7");
            assert!(
                reason.contains("NON-transactional")
                    && reason.contains("network restore"),
                "the refusal must name the hazard and the remedy: {reason}"
            );
        }
        other => panic!(
            "a tail already applied to the live tree must be REFUSED, not \
             truncated; got {other:?}"
        ),
    }

    // NOTHING was truncated: the log and the VLSN index are exactly as before.
    // The node is diverged-but-intact, awaiting a network restore.
    assert_eq!(
        p.replica.get_current_vlsn(),
        7,
        "a refused syncup must not advance or truncate the VLSN index"
    );
    let after = p.replica_view();
    for v in 1i64..=7 {
        assert_eq!(
            after.entry(Vlsn::new(v)).map(|e| e.fingerprint),
            before.entry(Vlsn::new(v)).map(|e| e.fingerprint),
            "refused syncup must leave VLSN {v} byte-identical (no \
             make-invisible, no truncation)"
        );
    }

    p.close();
}

// ---------------------------------------------------------------------------
// Gap A step 2: the computed revert set is correct EVEN for a tail that
// stays refused. This does not change the decision -- it proves the data
// source `syncup_with_feeder` runs internally (diagnostically) is right.
// ---------------------------------------------------------------------------

/// The exact tail from `test_diverged_replica_with_applied_tail_is_refused`
/// (a still-active transactional LN at VLSN 6, an applied non-transactional
/// LN at VLSN 7) still yields `DivergedRefused` -- but the live
/// `TxnChain` this build computes internally (`live_txn_chain::
/// build_tail_chains`, wired diagnostically into
/// `ReplicatedEnvironment::syncup_with_feeder`) for the transactional LN's
/// txn id is independently verifiable as CORRECT via the same public API,
/// against the replica's real on-disk WAL, proving Gap A step 2: "wired but
/// nothing admitted" with a tested, correct data source underneath.
#[test]
fn test_computed_revert_set_is_correct_for_a_still_refused_tail() {
    let p = Pair::new(5);
    p.assert_layouts_diverged(5);

    // Same tail shape as test_diverged_replica_with_applied_tail_is_refused:
    // VLSN 6 is a transactional LN for txn 42 (still active, never
    // committed -- ReplicaReplay buffers it, never applies it); VLSN 7 is a
    // non-transactional LN (applied immediately) which is what forces the
    // refusal.
    let db_id = 1u64; // matches the plain payload's db_id below; TxnChain's
    // CompareSlot only needs a stable id, not a real open database.
    let txn6_payload = {
        use bytes::BytesMut;
        use noxu_log::entry::LnLogEntry;
        use noxu_util::{NULL_LSN, NULL_VLSN};
        let entry = LnLogEntry::new(
            db_id,
            Some(42),
            NULL_LSN,
            true, // abort_known_deleted: first write of this slot by txn 42
            None,
            None,
            NULL_VLSN,
            0,
            true,
            b"only-in-txn-42".to_vec(),
            Some(b"OLD-6-AA".to_vec()),
            0,
            NULL_VLSN,
        );
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        buf.to_vec()
    };
    apply(&p.replica, &p.replica_env, 6, TXN_LN, &txn6_payload);
    apply(&p.replica, &p.replica_env, 7, PLAIN_LN, b"OLD-7-APPLIED");
    apply(&p.master, &p.master_env, 6, TXN_LN, b"NEW-6-CC");
    apply(&p.master, &p.master_env, 7, TXN_LN, b"NEW-7-DD");
    p.flush();

    // THE PRODUCTION DECISION: still refused, exactly as before -- the
    // diagnostic chain computation inside syncup_with_feeder must not have
    // changed the verdict.
    let action = p
        .replica
        .syncup_with_feeder_at(p.master_addr())
        .expect("syncup handshake");
    match &action {
        SyncupAction::DivergedRefused { matchpoint_vlsn, .. } => {
            assert_eq!(*matchpoint_vlsn, 5);
        }
        other => panic!("expected DivergedRefused, got {other:?}"),
    }

    // INDEPENDENT VERIFICATION: call the exact same public data source the
    // diagnostic block calls, against the replica's own real WAL, and prove
    // its output is the CORRECT chain for txn 42 -- one rolled-back logrec
    // (VLSN 6's LSN) reverting to the pre-txn abort info embedded in that
    // same logrec (abort_known_deleted=true -> delete the slot).
    let lm = p.replica_env.get_log_manager().expect("log manager");
    let fm = lm.file_manager();
    let matchpoint_lsn = {
        let view = p.replica_view();
        view.entry(Vlsn::new(5)).expect("matchpoint entry present").lsn
    };
    let mut chains = noxu_rep::stream::build_tail_chains(
        fm,
        &[noxu_util::Lsn::from_u64({
            let view = p.replica_view();
            view.entry(Vlsn::new(6)).expect("vlsn 6 present").lsn
        })],
        noxu_util::Lsn::from_u64(matchpoint_lsn),
        &|a: &[u8], b: &[u8]| a.cmp(b),
    );

    let mut chain = chains.remove(&42).expect("txn 42 must be discovered");
    assert_eq!(chain.len(), 1, "txn 42 logged exactly one LN above matchpoint");
    let ri = chain.pop().unwrap();
    assert!(
        ri.revert_kd,
        "txn 42's ONLY write was the first write of this slot: reverting \
         it must delete the slot (revert-to-known-deleted), matching the \
         abort_known_deleted=true embedded in the logrec itself"
    );

    p.close();
}

// ---------------------------------------------------------------------------
// The gap itself: the range check cannot do this.
// ---------------------------------------------------------------------------

/// FAIL-PRE: the pre-existing `negotiate_syncup` range check reports
/// `CanServe` for the very same diverged replica that the matchpoint protocol
/// rolls back — it compares VLSN *numbers* and cannot see divergent contents.
/// This is the gap the tests above close, pinned so it cannot silently be
/// treated as sufficient again.
#[test]
fn test_range_check_alone_cannot_detect_divergence() {
    use noxu_rep::stream::{SyncupResult, negotiate_syncup};

    // Master holds 1..=8, the diverged replica needs 8 onwards. The range check
    // sees only that 8 is inside [1,8] and happily streams — regardless of the
    // replica holding non-accepted records at 6,7.
    assert_eq!(
        negotiate_syncup(Some((1, 8)), 8),
        SyncupResult::CanServe { start_vlsn: 8 },
        "the range check is content-blind: this is exactly why the bilateral \
         matchpoint protocol is required"
    );
}
