//! REP-1 STEP 5 headline test: a diverged replica auto-reconciles via LIVE
//! syncup rollback (NOT a network restore).
//!
//! Port of the JE `ReplicaFeederSyncup` scenario: a replica that applied
//! entries PAST a matchpoint (diverged from a new master) runs live syncup,
//! rolls its divergent tail back to the highest common matchpoint, and resumes
//! streaming from `matchpoint + 1` — converging on the master's state without
//! copying log files.
//!
//! These tests use REAL on-disk logs (VLSN-tagged entries written via
//! `LogManager::log_with_vlsn`) so the per-VLSN fingerprint is the actual
//! record checksum the backward `SyncupLogView` re-reads — exactly as JE's
//! `ReplicaSyncupReader` does (the sparse VLSN index alone cannot supply
//! per-record checksums).
//!
//! - **fail-pre** (the pre-STEP-5 behaviour): the only available path was the
//!   range-check `negotiate_syncup`, which sees only VLSN *numbers* and cannot
//!   detect the diverged tail, so a diverged replica would (wrongly) stream
//!   the master's history on top of its own divergent records, or be sent to a
//!   full network restore.
//! - **pass-post**: `syncup_with_feeder` returns `RolledBack { matchpoint, +1 }`,
//!   makes the divergent log entries invisible + fsyncs, truncates the
//!   divergent VLSN tail, and the replica converges after re-streaming
//!   `matchpoint + 1 ..` from the master.

use std::sync::Arc;

use noxu_dbi::EnvironmentImpl;
use noxu_log::LogEntryType;
use noxu_rep::stream::{
    SyncupLogView, SyncupResult, SyncupView, negotiate_syncup,
};
use noxu_rep::{RepConfig, ReplicatedEnvironment, SyncupAction};

const COMMIT: LogEntryType = LogEntryType::TxnCommit; // sync point + txn end
const LN: LogEntryType = LogEntryType::InsertLNTxn; // not sync, not txn end

fn cfg(name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder("step5_group", name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home.to_path_buf())
        .build()
}

/// Write a VLSN-tagged entry to the env's real log AND register it in the rep
/// env's shared VLSN index (the two halves of applying a replicated entry).
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

/// A diverged replica rolls its divergent tail back to the matchpoint via live
/// syncup and converges — without a network restore.
#[test]
fn test_diverged_replica_converges_via_live_syncup_rollback() {
    let master_dir = tempfile::TempDir::new().unwrap();
    let replica_dir = tempfile::TempDir::new().unwrap();

    let master_env =
        Arc::new(EnvironmentImpl::new(master_dir.path(), false, true).unwrap());
    let replica_env = Arc::new(
        EnvironmentImpl::new(replica_dir.path(), false, true).unwrap(),
    );
    let master = Arc::new(
        ReplicatedEnvironment::new(cfg("m", master_dir.path())).unwrap(),
    );
    let replica = Arc::new(
        ReplicatedEnvironment::new(cfg("r", replica_dir.path())).unwrap(),
    );
    master.with_environment(Arc::clone(&master_env));
    replica.with_environment(Arc::clone(&replica_env));

    // Common prefix: both nodes hold VLSNs 1..=5 (TxnCommit sync points).
    for v in 1u64..=5 {
        let payload = [v as u8; 8];
        apply(&master, &master_env, v, COMMIT, &payload);
        apply(&replica, &replica_env, v, COMMIT, &payload);
    }

    // DIVERGENCE: the replica applied uncommitted LN writes at VLSNs 6,7 from
    // an OLD master; the NEW master applied DIFFERENT uncommitted LN writes at
    // 6,7 plus a fresh entry at 8. VLSNs 6,7 are LN (NOT txn ends), so rolling
    // them back is a NORMAL soft rollback, not hard recovery. The matchpoint
    // is the highest common sync point, VLSN 5.
    apply(&replica, &replica_env, 6, LN, b"OLD-6-AA");
    apply(&replica, &replica_env, 7, LN, b"OLD-7-BB");
    apply(&master, &master_env, 6, LN, b"NEW-6-CC");
    apply(&master, &master_env, 7, LN, b"NEW-7-DD");
    apply(&master, &master_env, 8, LN, b"NEW-8-EE");

    // Pre-syncup: replica diverged to VLSN 7, master at VLSN 8.
    assert_eq!(replica.get_current_vlsn(), 7, "replica diverged to VLSN 7");
    assert_eq!(master.get_current_vlsn(), 8, "master at VLSN 8");

    // FAIL-PRE: the range check sees only VLSN numbers; it reports CanServe
    // for a divergent replica, unable to detect or repair the diverged tail.
    assert_eq!(
        negotiate_syncup(Some((1, 8)), 8),
        SyncupResult::CanServe { start_vlsn: 8 },
        "range check cannot detect the diverged tail at 6,7 — that is exactly \
         why STEP 5 is needed"
    );

    // PASS-POST: run the live syncup against the master's real-log view.
    master_env.get_log_manager().unwrap().flush_sync().ok();
    let feeder_view = SyncupLogView::scan(master_dir.path()).unwrap();
    // Sanity: the feeder really holds VLSN 5 at a matching record.
    assert!(feeder_view.entry(noxu_util::Vlsn::new(5)).is_some());

    let action = replica.syncup_with_feeder(&feeder_view).expect("syncup");
    assert_eq!(
        action,
        SyncupAction::RolledBack { matchpoint_vlsn: 5, start_vlsn: 6 },
        "live syncup must roll the divergent tail back to matchpoint 5 and \
         resume at 6 — NOT a network restore"
    );

    // The divergent tail (VLSN 6,7) is gone from the index.
    assert_eq!(
        replica.get_current_vlsn(),
        5,
        "replica's divergent tail rolled back to matchpoint 5"
    );

    // The divergent log entries were made invisible (STEP 4): a fresh
    // SyncupLogView of the replica's log no longer sees VLSN 6,7.
    let replica_view_after = SyncupLogView::scan(replica_dir.path()).unwrap();
    assert!(
        replica_view_after.entry(noxu_util::Vlsn::new(6)).is_none(),
        "divergent VLSN 6 must be invisible after rollback"
    );
    assert!(
        replica_view_after.entry(noxu_util::Vlsn::new(7)).is_none(),
        "divergent VLSN 7 must be invisible after rollback"
    );
    assert!(
        replica_view_after.entry(noxu_util::Vlsn::new(5)).is_some(),
        "matchpoint VLSN 5 must survive the rollback"
    );

    // CONVERGENCE: streaming resumes from matchpoint + 1 (VLSN 6). Re-apply
    // the master's winning history 6,7,8 on the replica.
    apply(&replica, &replica_env, 6, LN, b"NEW-6-CC");
    apply(&replica, &replica_env, 7, LN, b"NEW-7-DD");
    apply(&replica, &replica_env, 8, LN, b"NEW-8-EE");

    assert_eq!(
        replica.get_current_vlsn(),
        8,
        "replica converged to the master's VLSN 8 after rollback + restream"
    );

    // The replica now holds the SAME visible record at every VLSN as the
    // master — re-read both logs and compare the matchpoint: it is the
    // replica's last VLSN (8), so no divergence remains.
    replica_env.get_log_manager().unwrap().flush_sync().ok();
    let replica_final = SyncupLogView::scan(replica_dir.path()).unwrap();
    let master_final = SyncupLogView::scan(master_dir.path()).unwrap();
    for v in 1u64..=8 {
        let rv = replica_final.entry(noxu_util::Vlsn::new(v as i64));
        let mv = master_final.entry(noxu_util::Vlsn::new(v as i64));
        assert_eq!(
            rv.map(|e| e.fingerprint),
            mv.map(|e| e.fingerprint),
            "converged: replica and master hold the same record at VLSN {v}"
        );
    }

    let _ = replica.close();
    let _ = master.close();
}

/// No common matchpoint → NeedsRestore fallback still triggers (STEP 5
/// preserves JE's truth-table fallback).
#[test]
fn test_no_common_matchpoint_falls_back_to_restore() {
    let master_dir = tempfile::TempDir::new().unwrap();
    let replica_dir = tempfile::TempDir::new().unwrap();
    let master_env =
        Arc::new(EnvironmentImpl::new(master_dir.path(), false, true).unwrap());
    let replica_env = Arc::new(
        EnvironmentImpl::new(replica_dir.path(), false, true).unwrap(),
    );
    let master = Arc::new(
        ReplicatedEnvironment::new(cfg("m", master_dir.path())).unwrap(),
    );
    let replica = Arc::new(
        ReplicatedEnvironment::new(cfg("r", replica_dir.path())).unwrap(),
    );
    master.with_environment(Arc::clone(&master_env));
    replica.with_environment(Arc::clone(&replica_env));

    // Every VLSN holds a DIFFERENT record on each side: no matchpoint.
    for v in 1u64..=4 {
        apply(&master, &master_env, v, COMMIT, b"MASTER-X");
        apply(&replica, &replica_env, v, COMMIT, b"REPLICA-Y");
    }

    master_env.get_log_manager().unwrap().flush_sync().ok();
    let feeder_view = SyncupLogView::scan(master_dir.path()).unwrap();
    let action = replica.syncup_with_feeder(&feeder_view).expect("syncup");
    assert_eq!(
        action,
        SyncupAction::NeedsRestore,
        "no common matchpoint must fall back to network restore"
    );
    // The replica's range is untouched (no rollback).
    assert_eq!(replica.get_current_vlsn(), 4);

    let _ = replica.close();
    let _ = master.close();
}

/// HardRecovery fallback: a matchpoint exists but rolling back to it would
/// cross a committed transaction (a txn end above the matchpoint) → the
/// replica needs hard recovery / network restore, NOT a soft rollback.
#[test]
fn test_rollback_past_commit_needs_restore() {
    let master_dir = tempfile::TempDir::new().unwrap();
    let replica_dir = tempfile::TempDir::new().unwrap();
    let master_env =
        Arc::new(EnvironmentImpl::new(master_dir.path(), false, true).unwrap());
    let replica_env = Arc::new(
        EnvironmentImpl::new(replica_dir.path(), false, true).unwrap(),
    );
    let master = Arc::new(
        ReplicatedEnvironment::new(cfg("m", master_dir.path())).unwrap(),
    );
    let replica = Arc::new(
        ReplicatedEnvironment::new(cfg("r", replica_dir.path())).unwrap(),
    );
    master.with_environment(Arc::clone(&master_env));
    replica.with_environment(Arc::clone(&replica_env));

    // Common prefix 1..=3 (commits). Replica then COMMITS at VLSN 4 and 5
    // (its own history), diverging from the master which commits DIFFERENT
    // records at 4,5. The replica's matchpoint is 3, but rolling back its tail
    // crosses the committed txn end at VLSN 4 → HardRecovery.
    for v in 1u64..=3 {
        let p = [v as u8; 8];
        apply(&master, &master_env, v, COMMIT, &p);
        apply(&replica, &replica_env, v, COMMIT, &p);
    }
    apply(&replica, &replica_env, 4, COMMIT, b"R-COMMIT4");
    apply(&replica, &replica_env, 5, COMMIT, b"R-COMMIT5");
    apply(&master, &master_env, 4, COMMIT, b"M-COMMIT4");
    apply(&master, &master_env, 5, COMMIT, b"M-COMMIT5");

    master_env.get_log_manager().unwrap().flush_sync().ok();
    let feeder_view = SyncupLogView::scan(master_dir.path()).unwrap();
    let action = replica.syncup_with_feeder(&feeder_view).expect("syncup");
    assert_eq!(
        action,
        SyncupAction::NeedsRestore,
        "rolling back past a committed txn (VLSN 4 commit) must NOT soft-roll \
         back — it needs hard recovery / network restore"
    );
    // No rollback happened.
    assert_eq!(replica.get_current_vlsn(), 5);

    let _ = replica.close();
    let _ = master.close();
}
