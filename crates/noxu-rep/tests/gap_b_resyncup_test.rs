//! HA Gap B headline test: a replica re-syncs to a NEW master mid-stream
//! instead of streaming forever against a stale channel.
//!
//! ## The gap
//!
//! Before this branch, `become_replica` ran the SYNCUP handshake **once at
//! spawn** and then streamed forever against that original channel. Nothing
//! watched for a master change, so after a mid-stream failover the replica
//! kept streaming from (or trying to reconnect to) the OLD master instead of
//! re-syncing to the new one.
//!
//! JE equivalent: `Replica` registers a `MasterChangeListener` with
//! `RepNode`; on notification it breaks out of `runReplicaLoop`'s inner
//! streaming loop and re-enters `ReplicaFeederSyncup` against the new master.
//!
//! ## What this test proves
//!
//! 1. `test_replica_resyncs_to_new_master_mid_stream`: a replica streaming
//!    from master1 is told (via `become_replica`, exactly as the election
//!    driver would call it after a real election) that master2 is now the
//!    master. The replica's OUTER RECONNECT LOOP must notice the
//!    `MasterTracker` generation bump, drop the master1 channel, run a FRESH
//!    syncup against master2, and stream master2's data — even though
//!    master1 is still alive and still willing to serve.
//!    - **FAIL-PRE (before this branch)**: `become_replica` either
//!      double-spawns a second receive thread (racing with the first against
//!      the same live `EnvironmentImpl`) or the single existing thread keeps
//!      streaming from master1 and never touches master2. Either way the
//!      replica does NOT converge on master2's post-failover value.
//!    - **PASS-POST**: the replica's tree converges on master2's value.
//!
//! 2. `test_resync_still_refuses_on_divergent_unsafe_tail`: the SAME
//!    mid-stream master-change path, but the replica's tail above the common
//!    matchpoint contains an entry that the default-deny safety gate
//!    (`classify_tail`/`verify_rollback`) cannot safely truncate (a
//!    NON-transactional LN, streamed from master1 and applied to the
//!    replica's live tree via real replication). Proves the Gap B re-syncup
//!    path still goes through the SAME safety gate as the at-spawn syncup: it
//!    must REFUSE and stop streaming, not silently continue with a stale or
//!    corrupted view. This is HA Gap A's territory (the gate itself) — this
//!    test only proves Gap B's fix does not bypass it.
//!
//! Records are written as transactional-LN + TxnCommit pairs (matching
//! `rep1_step5_live_syncup_test.rs` / `syncup_matchpoint_rollback_test.rs`):
//! `TxnCommit` is both a sync point (a matchpoint candidate) and a txn end,
//! so it establishes a real common matchpoint for the syncup handshake to
//! find. A bare `InsertLN` (used only for the deliberately-unsafe tail in
//! headline 2) is neither.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use noxu_dbi::{DatabaseConfig, EnvironmentImpl};
use noxu_log::entry::{LnLogEntry, TxnEndEntry};
use noxu_log::{LogEntryType, LogManager};
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};
use noxu_util::{NULL_LSN, NULL_VLSN};

/// Generous bound for anything this test waits on: real TCP + real timers,
/// no fixed iteration counts.  See AGENTS.md timing discipline.
const WAIT: Duration = Duration::from_secs(20);

/// Build the on-log payload for a transactional InsertLN record.
fn txn_ln_payload(db_id: u64, txn_id: i64, key: &[u8], data: &[u8]) -> Vec<u8> {
    let entry = LnLogEntry::new(
        db_id,
        Some(txn_id),
        NULL_LSN,
        false,
        None,
        None,
        NULL_VLSN,
        0,
        false,
        key.to_vec(),
        Some(data.to_vec()),
        0,
        NULL_VLSN,
    );
    let mut buf = BytesMut::new();
    entry.write_to_log(&mut buf);
    buf.to_vec()
}

/// Build the on-log payload for a NON-transactional InsertLN record.
fn plain_ln_payload(db_id: u64, key: &[u8], data: &[u8]) -> Vec<u8> {
    let entry = LnLogEntry::new(
        db_id,
        None,
        NULL_LSN,
        false,
        None,
        None,
        NULL_VLSN,
        0,
        false,
        key.to_vec(),
        Some(data.to_vec()),
        0,
        NULL_VLSN,
    );
    let mut buf = BytesMut::new();
    entry.write_to_log(&mut buf);
    buf.to_vec()
}

fn txn_commit_payload(txn_id: i64) -> Vec<u8> {
    let e = TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
    let mut buf = BytesMut::new();
    e.write_to_log(&mut buf);
    buf.to_vec()
}

/// Open a live `EnvironmentImpl` in `dir`, open the replicated database, and
/// return the env + db id + live tree + log manager.
fn open_node(
    dir: &std::path::Path,
) -> (
    Arc<EnvironmentImpl>,
    u64,
    Arc<std::sync::RwLock<noxu_tree::Tree>>,
    Arc<LogManager>,
) {
    let env = Arc::new(EnvironmentImpl::new(dir, false, true).unwrap());
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true).set_transactional(true);
    let db = env.open_database("gapb_db", &cfg).unwrap();
    let db_id = db.read().get_id().id() as u64;
    let tree = env.replica_tree_for_db(db_id).unwrap();
    let log_mgr = env.get_log_manager().expect("log manager");
    (env, db_id, tree, log_mgr)
}

/// Write one VLSN-tagged entry to `log_mgr` and register it in `rep`'s VLSN
/// index, exactly as a replicated master's committed write does.
fn write_vlsn_entry(
    rep: &ReplicatedEnvironment,
    log_mgr: &LogManager,
    vlsn: u64,
    ty: LogEntryType,
    payload: &[u8],
) {
    let lsn = log_mgr
        .log_with_vlsn(ty, payload, vlsn, true, false)
        .expect("log_with_vlsn");
    rep.register_vlsn_typed(vlsn, lsn.file_number(), lsn.file_offset(), ty);
}

/// Commit one record as a (transactional LN, TxnCommit) pair at
/// `(ln_vlsn, ln_vlsn + 1)`.  `TxnCommit` is a sync point AND a txn end, so
/// it is a real matchpoint candidate for the syncup handshake.
fn commit_record(
    rep: &ReplicatedEnvironment,
    log_mgr: &LogManager,
    db_id: u64,
    ln_vlsn: u64,
    txn_id: i64,
    key: &[u8],
    data: &[u8],
) {
    write_vlsn_entry(
        rep,
        log_mgr,
        ln_vlsn,
        LogEntryType::InsertLNTxn,
        &txn_ln_payload(db_id, txn_id, key, data),
    );
    write_vlsn_entry(
        rep,
        log_mgr,
        ln_vlsn + 1,
        LogEntryType::TxnCommit,
        &txn_commit_payload(txn_id),
    );
}

/// Poll `tree` until `key` resolves to `Some(data)`, or the deadline passes.
fn poll_read(
    tree: &Arc<std::sync::RwLock<noxu_tree::Tree>>,
    key: &[u8],
    timeout: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(fetch) = tree.read().unwrap().search_with_data(key)
            && fetch.found
        {
            return fetch.data.map(|d| d.to_vec());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

/// Wait until `f()` returns `true`, or the deadline passes.  Returns whether
/// it converged.
fn wait_until(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    f()
}

// ---------------------------------------------------------------------------
// HEADLINE 1: mid-stream master change -> re-syncup -> converge on new master
// ---------------------------------------------------------------------------

#[test]
fn test_replica_resyncs_to_new_master_mid_stream() {
    // ── master1 ──────────────────────────────────────────────────────────
    let m1_dir = tempfile::TempDir::new().unwrap();
    let (m1_env, m1_db_id, _m1_tree, m1_log) = open_node(m1_dir.path());
    let m1_cfg = RepConfig::builder("gapb_grp", "master1", "127.0.0.1")
        .node_port(0)
        .env_home(m1_dir.path())
        .build();
    let master1 = Arc::new(ReplicatedEnvironment::new(m1_cfg).unwrap());
    master1.init_self_weak();
    master1.with_environment(Arc::clone(&m1_env));

    // ── master2 (the node that will WIN a failover) ────────────────────
    let m2_dir = tempfile::TempDir::new().unwrap();
    let (m2_env, m2_db_id, _m2_tree, m2_log) = open_node(m2_dir.path());
    assert_eq!(m2_db_id, m1_db_id, "shared db id across the group");
    let m2_cfg = RepConfig::builder("gapb_grp", "master2", "127.0.0.1")
        .node_port(0)
        .env_home(m2_dir.path())
        .build();
    let master2 = Arc::new(ReplicatedEnvironment::new(m2_cfg).unwrap());
    master2.init_self_weak();
    master2.with_environment(Arc::clone(&m2_env));

    // ── replica ──────────────────────────────────────────────────────────
    let r_dir = tempfile::TempDir::new().unwrap();
    let (r_env, r_db_id, r_tree, _r_log) = open_node(r_dir.path());
    assert_eq!(r_db_id, m1_db_id, "shared db id across the group");
    let r_cfg = RepConfig::builder("gapb_grp", "replica", "127.0.0.1")
        .node_port(0)
        .env_home(r_dir.path())
        .build();
    let replica = Arc::new(ReplicatedEnvironment::new(r_cfg).unwrap());
    replica.init_self_weak();
    replica.with_environment(Arc::clone(&r_env));

    let m1_addr = master1.bound_addr().expect("master1 binds");
    let m2_addr = master2.bound_addr().expect("master2 binds");

    // The replica knows BOTH masters (as a real group member would, from
    // GroupService membership independent of who currently holds
    // mastership).
    replica
        .add_peer(RepNode::new(
            "master1".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            m1_addr.port(),
            1,
        ))
        .unwrap();
    replica
        .add_peer(RepNode::new(
            "master2".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            m2_addr.port(),
            2,
        ))
        .unwrap();

    master1.become_master(1).unwrap();
    master2.become_master(2).unwrap();

    // ── common accepted history: 3 committed records, identical on both
    //    masters (VLSNs 1-6: 3 x (transactional LN + TxnCommit) pairs). Each
    //    TxnCommit is a sync point AND a txn end, giving the syncup
    //    handshake a real matchpoint to find.
    for (i, (k, v)) in
        [(b"a" as &[u8], b"1" as &[u8]), (b"b", b"2"), (b"c", b"3")]
            .into_iter()
            .enumerate()
    {
        let ln_vlsn = (i as u64) * 2 + 1;
        let txn_id = 100 + i as i64;
        commit_record(&master1, &m1_log, m1_db_id, ln_vlsn, txn_id, k, v);
        commit_record(&master2, &m2_log, m2_db_id, ln_vlsn, txn_id, k, v);
    }

    // ── replica becomes a replica of master1 and streams the common prefix ─
    replica.become_replica("master1").unwrap();
    for (k, v) in [(b"a" as &[u8], b"1" as &[u8]), (b"b", b"2"), (b"c", b"3")] {
        let got = poll_read(&r_tree, k, WAIT);
        assert_eq!(
            got.as_deref(),
            Some(v),
            "replica must receive the common prefix from master1 before \
             the mid-stream master change"
        );
    }
    assert_eq!(
        replica.get_current_vlsn(),
        6,
        "replica must be caught up to the common prefix's last VLSN (6)"
    );

    // ── FAILOVER: master2 commits a NEW record (VLSNs 7-8) that the
    //    replica has not yet seen from anyone. The replica's own tail above
    //    the common matchpoint (VLSN 6) is EMPTY at this point -- this test
    //    isolates the reconnect + re-syncup mechanism itself; the
    //    interaction between a Gap-B re-sync and an unsafe divergent tail is
    //    covered separately by `test_resync_still_refuses_on_divergent_
    //    unsafe_tail` below.
    //
    // Master1 is STILL ALIVE and STILL WILLING TO SERVE -- this is the
    // realistic mid-stream failover shape (the old master does not
    // necessarily crash; it can simply lose an election).
    assert!(master1.is_master(), "master1 must remain reachable");

    // ── SIMULATED FAILOVER NOTIFICATION: exactly what the election driver
    //    calls on every node that lost the new election (see
    //    run_election_loop's `become_replica(&winner_node.name)` call).
    //    The replica is ALREADY streaming from master1 when this arrives.
    replica.become_replica("master2").unwrap();

    // Give the reconnect loop a generous window to notice the master change,
    // tear down the master1 channel, run a fresh syncup against master2, and
    // start streaming from it -- BEFORE either master writes anything new.
    // This ordering is what makes the assertions below deterministic rather
    // than racing the reconnect against a frame already in flight: nothing
    // is written on EITHER master during this window, so there is nothing
    // for a still-connected stale thread to have already buffered.
    std::thread::sleep(Duration::from_secs(3));

    // NOW write the post-failover records. `master1_stale` is written on
    // master1 -- the node the replica must have ABANDONED. `fresh` is
    // written on master2 -- the node the replica must now be streaming
    // from.
    commit_record(
        &master1,
        &m1_log,
        m1_db_id,
        7,
        201,
        b"master1-stale",
        b"should-never-arrive",
    );
    commit_record(
        &master2,
        &m2_log,
        m2_db_id,
        7,
        200,
        b"fresh",
        b"from-master2",
    );

    // ── HEADLINE ASSERTION (positive): the replica converges on MASTER2's
    //    value -- proving it ran a fresh syncup against master2 and is now
    //    streaming from it.
    let converged = wait_until(
        || {
            r_tree
                .read()
                .unwrap()
                .search_with_data(b"fresh")
                .is_some_and(|f| f.found)
        },
        WAIT,
    );
    assert!(
        converged,
        "FAIL-PRE: replica never received master2's post-failover write; \
         without Gap B's reconnect loop the replica thread either keeps \
         streaming master1 forever, or (if become_replica double-spawned) \
         races two receive threads against the same EnvironmentImpl"
    );
    let got_fresh = r_tree.read().unwrap().search_with_data(b"fresh").unwrap();
    assert_eq!(got_fresh.data.as_deref(), Some(&b"from-master2"[..]));

    // ── HEADLINE ASSERTION (negative, the actual Gap B defect): the
    //    replica must NEVER receive master1's post-failover write. Master1
    //    is STILL ALIVE and STILL PUSHING new entries over its feeder
    //    connection; the ONLY thing that can prevent this record from
    //    landing is the replica having actually TORN DOWN its channel to
    //    master1 and stopped listening to it. A generous wait here would
    //    still catch a slow-but-eventual leak; give it the same window as
    //    the positive check.
    std::thread::sleep(Duration::from_secs(2));
    let stale_arrived = r_tree
        .read()
        .unwrap()
        .search_with_data(b"master1-stale")
        .is_some_and(|f| f.found);
    assert!(
        !stale_arrived,
        "FAIL-PRE: the replica applied a write from master1 AFTER being \
         told master2 is the new master -- this is the exact defect Gap B \
         closes: without the reconnect loop, become_replica either leaves \
         the ORIGINAL receive thread listening to the stale master \
         forever, or double-spawns a second thread while the first keeps \
         consuming master1's live feed"
    );

    // The replica's reported VLSN must have advanced past the common
    // prefix -- it must not be stuck at 6, which would mean the reconnect
    // never actually happened.
    let final_vlsn = replica.get_current_vlsn();
    assert!(
        final_vlsn >= 8,
        "replica VLSN must advance past the common prefix once it \
         re-syncs to master2 and streams VLSNs 7-8; got {final_vlsn}"
    );

    replica.close().unwrap();
    master1.close().unwrap();
    master2.close().unwrap();
}

// ---------------------------------------------------------------------------
// HEADLINE 2: the default-deny safety gate still applies on a Gap B re-sync
// ---------------------------------------------------------------------------

#[test]
fn test_resync_still_refuses_on_divergent_unsafe_tail() {
    // ── master1 (the node the replica streams from FIRST) ───────────────
    let m1_dir = tempfile::TempDir::new().unwrap();
    let (m1_env, m1_db_id, _m1_tree, m1_log) = open_node(m1_dir.path());
    let m1_cfg = RepConfig::builder("gapb_grp2", "master1", "127.0.0.1")
        .node_port(0)
        .env_home(m1_dir.path())
        .build();
    let master1 = Arc::new(ReplicatedEnvironment::new(m1_cfg).unwrap());
    master1.init_self_weak();
    master1.with_environment(Arc::clone(&m1_env));

    // ── master2 (the new master the replica is told to follow) ──────────
    let m2_dir = tempfile::TempDir::new().unwrap();
    let (m2_env, m2_db_id, _m2_tree, m2_log) = open_node(m2_dir.path());
    assert_eq!(m2_db_id, m1_db_id);
    let m2_cfg = RepConfig::builder("gapb_grp2", "master2", "127.0.0.1")
        .node_port(0)
        .env_home(m2_dir.path())
        .build();
    let master2 = Arc::new(ReplicatedEnvironment::new(m2_cfg).unwrap());
    master2.init_self_weak();
    master2.with_environment(Arc::clone(&m2_env));

    // ── replica ──────────────────────────────────────────────────────────
    let r_dir = tempfile::TempDir::new().unwrap();
    let (r_env, r_db_id, r_tree, _r_log) = open_node(r_dir.path());
    assert_eq!(r_db_id, m1_db_id);
    let r_cfg = RepConfig::builder("gapb_grp2", "replica", "127.0.0.1")
        .node_port(0)
        .env_home(r_dir.path())
        .build();
    let replica = Arc::new(ReplicatedEnvironment::new(r_cfg).unwrap());
    replica.init_self_weak();
    replica.with_environment(Arc::clone(&r_env));

    let m1_addr = master1.bound_addr().expect("master1 binds");
    let m2_addr = master2.bound_addr().expect("master2 binds");
    replica
        .add_peer(RepNode::new(
            "master1".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            m1_addr.port(),
            1,
        ))
        .unwrap();
    replica
        .add_peer(RepNode::new(
            "master2".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            m2_addr.port(),
            2,
        ))
        .unwrap();

    master1.become_master(1).unwrap();
    master2.become_master(2).unwrap();

    // Common accepted history: 2 committed records, identical on both
    // masters (VLSNs 1-4).
    for (i, (k, v)) in
        [(b"a" as &[u8], b"1" as &[u8]), (b"b", b"2")].into_iter().enumerate()
    {
        let ln_vlsn = (i as u64) * 2 + 1;
        let txn_id = 100 + i as i64;
        commit_record(&master1, &m1_log, m1_db_id, ln_vlsn, txn_id, k, v);
        commit_record(&master2, &m2_log, m2_db_id, ln_vlsn, txn_id, k, v);
    }

    replica.become_replica("master1").unwrap();
    for (k, v) in [(b"a" as &[u8], b"1" as &[u8]), (b"b", b"2")] {
        let got = poll_read(&r_tree, k, WAIT);
        assert_eq!(got.as_deref(), Some(v));
    }
    assert_eq!(replica.get_current_vlsn(), 4);

    // ── master1 (still the current master, still streaming to the
    //    replica) commits ONE MORE record above the common matchpoint --
    //    but as a NON-transactional LN (VLSN 5), which the replica applies
    //    to its live tree IMMEDIATELY on receipt (real replication, not a
    //    direct injection). This is exactly the tail `classify_tail` cannot
    //    safely discard: its effects are already visible in the live
    //    B-tree, and this build has no in-memory `TxnChain` revert (JE
    //    `Replay.rollback` step 2).
    write_vlsn_entry(
        &master1,
        &m1_log,
        5,
        LogEntryType::InsertLN,
        &plain_ln_payload(m1_db_id, b"replica-only", b"unsafe-tail"),
    );
    assert!(
        wait_until(
            || r_tree
                .read()
                .unwrap()
                .search_with_data(b"replica-only")
                .is_some_and(|f| f.found),
            WAIT,
        ),
        "the replica must receive + apply master1's non-transactional LN \
         via real streaming before the failover"
    );

    // master2 does NOT have this entry: its log stops at VLSN 4 (the
    // common matchpoint). When the replica re-syncs to master2, the
    // matchpoint search finds VLSN 4 (both sides agree there), and the
    // replica's tail above it (VLSN 5, the non-transactional LN) is the
    // unsafe tail.

    // ── SIMULATED FAILOVER NOTIFICATION: tell the (already-streaming)
    //    replica that master2 is now the master.
    replica.become_replica("master2").unwrap();

    // Give the reconnect loop a generous window to run syncup against
    // master2 and observe the refusal.
    std::thread::sleep(Duration::from_secs(3));

    // master1 (the ABANDONED master) keeps writing. If the replica's Gap-B
    // reconnect loop is working, this write must NEVER reach the replica --
    // the replica has torn down its channel to master1 and is (correctly)
    // refusing to stream from master2 due to the unsafe tail. If Gap B is
    // NOT wired up, a still-running stale thread happily keeps consuming
    // master1's live feed forever, since the safety gate was only
    // evaluated once at spawn and the ONGOING stream never re-checks it.
    write_vlsn_entry(
        &master1,
        &m1_log,
        6,
        LogEntryType::InsertLN,
        &plain_ln_payload(m1_db_id, b"master1-kept-streaming", b"leaked"),
    );

    // ── HEADLINE ASSERTION: the safety gate REFUSES. The replica must NOT
    //    silently continue streaming (which would either corrupt its view
    //    or silently diverge), and it must NOT advance past VLSN 5, since
    //    the syncup was refused before any streaming resumed. This also
    //    catches the plain reconnect-loop defect: if the replica kept
    //    streaming from the abandoned master1 (Gap B unfixed), VLSN 6
    //    above would land and the VLSN would advance to 6.
    let stayed_at_5 =
        !wait_until(|| replica.get_current_vlsn() > 5, Duration::from_secs(8));
    assert!(
        stayed_at_5,
        "FAIL-PRE: the replica's VLSN advanced past 5 after being told \
         master2 is the new master. Either (a) the default-deny safety \
         gate failed to REFUSE a re-sync whose tail contains an \
         already-applied non-transactional LN, or (b) -- the Gap B defect \
         this test also exercises -- the replica kept streaming from the \
         ABANDONED master1 because nothing tore its channel down"
    );

    // The replica's own pre-failover state must still be intact -- REFUSING
    // must not have torn anything down.
    let still_there = r_tree.read().unwrap().search_with_data(b"replica-only");
    assert!(
        still_there.is_some_and(|f| f.found),
        "a refused re-sync must leave the replica's existing state intact \
         (no truncation happened)"
    );
    assert_eq!(
        replica.get_current_vlsn(),
        5,
        "VLSN must remain exactly at the pre-failover value"
    );

    replica.close().unwrap();
    master1.close().unwrap();
    master2.close().unwrap();
}
