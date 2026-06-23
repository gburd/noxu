//! REP-10 headline integration tests: replica read-consistency policies are
//! ENFORCED on the replica read path.
//!
//! Port of the JE replica consistency-wait:
//! `ReplicaConsistencyPolicy.ensureConsistency` →
//! `Replica.ConsistencyTracker.awaitVLSN` / `lagAwait`, invoked from a replica
//! `beginTransaction` (`RepImpl.checkConsistency`).
//!
//! The wait predicate is the REP-7 `last_applied_vlsn` handle
//! (`ReplicaReplay::last_applied_vlsn_handle`) — the SAME `Arc<AtomicU64>` the
//! replay driver advances after each committed apply.  These tests drive a
//! real `ReplicaReplay` + `EnvironmentLogWriter` (the REP-7 receive path) and
//! a `ReplicatedEnvironment` whose `ConsistencyTracker` is installed over that
//! handle exactly as `become_replica` does, then call
//! `begin_read_consistency` and assert the gate's behaviour.
//!
//! ## Headline gates
//!
//! 1. `test_commit_point_blocks_then_sees_data` — master commits (mints a
//!    CommitToken); the read on the replica with that token BLOCKS until the
//!    replica has applied up to that VLSN, then returns and the data is
//!    visible in the live tree.
//!    - **Fail-pre (origin/main):** the policy is never invoked on a read, so
//!      the read returns immediately ignoring the token (stale/nothing).
//!    - **Pass-post:** `begin_read_consistency` blocks until the replay
//!      advances `last_applied_vlsn` past the token, then the live read sees
//!      the value.
//! 2. `test_time_consistency_blocks_lagging_replica` — a lagging replica
//!    blocks a time-consistency read until it catches up within the lag.
//! 3. `test_no_consistency_never_blocks` — NoConsistency returns immediately.
//! 4. `test_commit_point_timeout_is_clean_error` — a token the replica never
//!    reaches yields a clean ConsistencyTimeout, NOT a hang.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use noxu_dbi::{DatabaseConfig, EnvironmentImpl, ReplicaReplay};
use noxu_log::LogEntryType;
use noxu_log::entry::LnLogEntry;
use noxu_rep::{
    CommitToken, ConsistencyPolicy, RepConfig, ReplicatedEnvironment,
};
use noxu_util::{Lsn, NULL_LSN, NULL_VLSN};

fn cfg(name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder("rep10_group", name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home.to_path_buf())
        .build()
}

fn ln_payload(db_id: u64, key: &[u8], data: &[u8]) -> Vec<u8> {
    // Non-transactional committed LN (applied immediately on the replica).
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

/// Build a replica env + a live ReplicaReplay + the env's ConsistencyTracker
/// installed over the replay's REP-7 last_applied_vlsn handle (exactly as
/// `become_replica` wires it).
struct Setup {
    rep_env: Arc<ReplicatedEnvironment>,
    _env: Arc<EnvironmentImpl>,
    replay: ReplicaReplay,
    handle: Arc<AtomicU64>,
    db_id: u64,
    tree: Arc<std::sync::RwLock<noxu_tree::Tree>>,
}

fn replica_setup() -> Setup {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.keep();
    let env_impl = Arc::new(EnvironmentImpl::new(&path, false, true).unwrap());
    let mut dbcfg = DatabaseConfig::new();
    dbcfg.set_allow_create(true).set_transactional(true);
    let db = env_impl.open_database("repl_db", &dbcfg).unwrap();
    let db_id = db.read().get_id().id() as u64;
    let tree = env_impl.replica_tree_for_db(db_id).unwrap();

    let rep_env =
        Arc::new(ReplicatedEnvironment::new(cfg("replica", &path)).unwrap());
    rep_env.with_environment(Arc::clone(&env_impl));

    let replay = ReplicaReplay::new(Arc::clone(&env_impl));
    let handle = replay.last_applied_vlsn_handle();
    // Install the tracker over the SAME handle become_replica would use.
    rep_env.install_consistency_tracker_for_test(Arc::clone(&handle));

    Setup { rep_env, _env: env_impl, replay, handle, db_id, tree }
}

// ── HEADLINE 1: commit-point read blocks until applied, then sees data ──────

#[test]
fn test_commit_point_blocks_then_sees_data() {
    let Setup { rep_env, mut replay, handle, db_id, tree, .. } =
        replica_setup();
    let insert_ln = LogEntryType::InsertLN.type_num();

    // A client wrote on the master and got a CommitToken for commit VLSN 5.
    let token = CommitToken::new("rep10_group", 5).unwrap();
    let policy =
        ConsistencyPolicy::commit_point(&token, Duration::from_secs(5));

    // Replica is behind (nothing applied yet).
    assert_eq!(handle.load(Ordering::Acquire), 0);

    // Background: stream the entry up to VLSN 5 after a delay (the replay
    // thread advancing last_applied_vlsn).  This is what the receive loop
    // does as the master's commit streams in.
    let payload = ln_payload(db_id, b"k", b"v");
    let bg = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(60));
        // Apply VLSNs 1..=5; the LN with the data lands at VLSN 5.
        for v in 1..=5u64 {
            replay.apply_entry(
                v,
                insert_ln,
                &payload,
                Lsn::new(0, 100 + v as u32),
            );
        }
        // Wake any reader parked on the tracker.
        replay
    });

    // The read on the replica BLOCKS in begin_read_consistency until the
    // replica has applied up to the token's VLSN.
    let start = Instant::now();
    rep_env
        .begin_read_consistency(Some(&policy))
        .expect("commit-point read must succeed once replica catches up");
    let waited = start.elapsed();

    // It actually blocked (did not return on the fast path).
    assert!(
        waited >= Duration::from_millis(40),
        "FAIL-PRE: read returned immediately ({waited:?}) ignoring the \
         CommitToken; the policy was not enforced on the read path"
    );
    assert!(handle.load(Ordering::Acquire) >= 5, "replica caught up to token");

    // ...and the data is now visible in the live tree.
    let fetch = tree.read().unwrap().search_with_data(b"k").unwrap();
    assert!(fetch.found);
    assert_eq!(fetch.data.as_deref(), Some(&b"v"[..]));

    bg.join().unwrap();
}

// ── HEADLINE 2: time-consistency blocks a lagging replica ───────────────────

#[test]
fn test_time_consistency_blocks_lagging_replica() {
    let Setup { rep_env, mut replay, handle, db_id, .. } = replica_setup();
    let insert_ln = LogEntryType::InsertLN.type_num();

    // Master is at VLSN 1000; permissible lag 100ms (≈100 VLSN proxy) means
    // the replica must reach >= 900.  Surface the master high-water on the
    // replica stream so the time policy can compute the lag.
    rep_env.replica_stream().update_master_vlsn(1000);

    let policy = ConsistencyPolicy::TimeConsistency {
        max_lag: Duration::from_millis(100),
        timeout: Duration::from_secs(5),
    };

    let payload = ln_payload(db_id, b"k", b"v");
    let bg = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        for v in 1..=920u64 {
            replay.apply_entry(v, insert_ln, &payload, Lsn::new(0, 1));
        }
        replay
    });

    let start = Instant::now();
    rep_env
        .begin_read_consistency(Some(&policy))
        .expect("time-consistency read must succeed once lag is within bound");
    assert!(
        start.elapsed() >= Duration::from_millis(30),
        "time-consistency read must block a lagging replica"
    );
    assert!(handle.load(Ordering::Acquire) >= 900);
    bg.join().unwrap();
}

// ── HEADLINE 3: NoConsistency never blocks ──────────────────────────────────

#[test]
fn test_no_consistency_never_blocks() {
    let Setup { rep_env, .. } = replica_setup();
    // Replica fully behind, master far ahead — NoConsistency returns at once.
    rep_env.replica_stream().update_master_vlsn(10_000);
    let start = Instant::now();
    rep_env
        .begin_read_consistency(Some(&ConsistencyPolicy::NoConsistency))
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(50));

    // The default (config) policy is also NoConsistency, so a None override
    // is likewise non-blocking — existing behaviour is unchanged.
    let start = Instant::now();
    rep_env.begin_read_consistency(None).unwrap();
    assert!(start.elapsed() < Duration::from_millis(50));
}

// ── HEADLINE 4: timeout is a clean error, not a hang ────────────────────────

#[test]
fn test_commit_point_timeout_is_clean_error() {
    let Setup { rep_env, .. } = replica_setup();
    // Token for a VLSN the replica will never reach within the timeout.
    let token = CommitToken::new("rep10_group", 1_000_000).unwrap();
    let policy =
        ConsistencyPolicy::commit_point(&token, Duration::from_millis(80));

    let start = Instant::now();
    let err = rep_env
        .begin_read_consistency(Some(&policy))
        .expect_err("an unreachable token must time out");
    // Returned promptly (no hang) and is a consistency error.
    assert!(start.elapsed() < Duration::from_secs(2), "must not hang");
    assert!(
        matches!(err, noxu_rep::RepError::ConsistencyTimeout(_)),
        "expected ConsistencyTimeout, got {err:?}"
    );
}
