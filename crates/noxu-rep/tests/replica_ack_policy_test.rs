//! F1: ReplicaAckPolicy is honoured on commit.
//!
//! Without F1, a `noxu_db::Transaction` configured with
//! `Durability::COMMIT_SYNC` (which carries
//! `ReplicaAckPolicy::SimpleMajority`) committed silently even when no
//! replicas were connected. Worse, a user explicitly configuring
//! `ReplicaAckPolicy::All` on a master with no peers saw `Ok(())`
//! returned from commit.
//!
//! Wave 3-3 wires `ReplicatedEnvironment` as a
//! `noxu_dbi::ReplicaAckCoordinator`, installs it on the
//! `noxu_db::Environment`, and makes `Transaction::commit_with_durability`
//! block on the configured policy until the configured timeout fires.
//!
//! See the 2026 review finding F1.
//!
//! These tests build an `Arc<ReplicatedEnvironment>` directly (the
//! coordinator does not require a real `noxu_db::Environment` to
//! verify its own contract — the F1 trait is exercised at the rep
//! layer and noxu-db wiring is verified separately by
//! `f1_commit_blocks_on_replica_acks` below).

use std::sync::Arc;
use std::time::{Duration, Instant};

use noxu_dbi::{AckWaitErrorKind, ReplicaAckCoordinator, ReplicaAckPolicyKind};
use noxu_rep::{NodeType, RepConfig, ReplicatedEnvironment, rep_node::RepNode};

fn build_master_env(node_name: &str) -> Arc<ReplicatedEnvironment> {
    let cfg = RepConfig::builder("test_group_f1", node_name, "127.0.0.1")
        .node_port(0)
        .node_type(NodeType::Electable)
        .build();
    Arc::new(ReplicatedEnvironment::new(cfg).unwrap())
}

fn add_peers(env: &ReplicatedEnvironment, n: u32) {
    for i in 1..=n {
        let peer = RepNode::new(
            format!("peer{}", i),
            NodeType::Electable,
            "127.0.0.1".into(),
            6_000 + i as u16,
            10 + i,
        );
        env.add_peer(peer).unwrap();
    }
}

/// `ReplicaAckPolicy::All` on a master with two peer replicas (none of
/// which ack) must NOT silently succeed.  The coordinator must wait
/// the full timeout and return `AckWaitErrorKind::Timeout`.
///
// JE: ReplicatedTransactionTest.testReplicaAckPolicy — the master enforces
// the configured ReplicaAckPolicy on commit; a policy that cannot be met
// (no acking replicas) does not silently succeed. Same intent at the
// coordinator layer Noxu exposes (AckWaitErrorKind::Timeout ~
// InsufficientAcksException).
//
// JE: ReplicatedTransactionTest.testMasterTxnBegin — a txn under SYNC_SYNC_ALL
// (ReplicaAckPolicy::All) with a replica missing must NOT enter its scope /
// succeed; JE throws InsufficientReplicasException. Same intent: the All
// policy that cannot be met with no acking replicas does not silently
// succeed — it returns AckWaitErrorKind::Timeout with needed > received.
// (JE's SYNC_SYNC_QUORUM-still-succeeds-with-one-down half maps to
// f1_simple_majority_with_no_acks_times_out's quorum arithmetic.)
#[test]
fn f1_all_policy_with_no_acks_times_out() {
    let env = build_master_env("master_f1_all");
    env.become_master(1).unwrap();
    add_peers(&env, 2);

    let started = Instant::now();
    let timeout = Duration::from_millis(200);
    let res = env.await_replica_acks(ReplicaAckPolicyKind::All, timeout);
    let elapsed = started.elapsed();

    let err = res.expect_err("commit must NOT succeed without acks (F1)");
    assert_eq!(err.kind, AckWaitErrorKind::Timeout);
    // 3 electable peers (master + 2) → All requires 2 acks; needed > 0.
    assert!(err.needed >= 1, "expected non-zero needed, got {:?}", err);
    assert_eq!(err.received, 0);
    assert!(
        elapsed >= timeout,
        "expected to wait at least the full timeout; waited {:?}",
        elapsed
    );

    let _ = env.close();
}

/// `ReplicaAckPolicy::SimpleMajority` on a master with one peer
/// (single peer means 2 electables, majority=2, master counts as 1, so
/// needed=1 ack) blocks for the full timeout when no acks arrive.
///
// JE: ReplicatedTransactionTest.testReadonlyTxnBasic (commit-under-quorum
// subset) — that test commits 100 txns under SYNC_SYNC_QUORUM with one
// replica down; the SimpleMajority quorum arithmetic that governs whether
// such a commit can proceed is exactly what this test pins (needed acks
// under SimpleMajority with one peer). The consistency read-back half
// (TimeConsistency returns the master's last value on the lagging replica)
// is covered by rep10_consistency_policy_test::
// test_time_consistency_blocks_lagging_replica.
#[test]
fn f1_simple_majority_with_no_acks_times_out() {
    let env = build_master_env("master_f1_maj");
    env.become_master(1).unwrap();
    add_peers(&env, 1);

    let timeout = Duration::from_millis(150);
    let res =
        env.await_replica_acks(ReplicaAckPolicyKind::SimpleMajority, timeout);

    let err = res.expect_err("commit must time out");
    assert_eq!(err.kind, AckWaitErrorKind::Timeout);
    assert!(err.needed >= 1);

    let _ = env.close();
}

/// `ReplicaAckPolicy::None` is the documented "fire-and-forget"
/// policy.  It must short-circuit and return success even when no
/// replicas are connected.
#[test]
fn f1_none_policy_returns_immediately() {
    let env = build_master_env("master_f1_none");
    env.become_master(1).unwrap();

    let started = Instant::now();
    let res = env.await_replica_acks(
        ReplicaAckPolicyKind::None,
        Duration::from_secs(60),
    );
    let elapsed = started.elapsed();

    assert!(res.is_ok(), "ReplicaAckPolicy::None must succeed");
    assert!(
        elapsed < Duration::from_millis(50),
        "ReplicaAckPolicy::None must not block; waited {:?}",
        elapsed
    );

    let _ = env.close();
}

/// Calling on a non-master node returns `NotMaster`.  This is the
/// path that maps to `NoxuError::ReplicaWrite` in the noxu-db commit.
#[test]
fn f1_replica_node_returns_not_master() {
    let env = build_master_env("replica_f1");
    env.become_replica("the_master").unwrap();

    let res = env.await_replica_acks(
        ReplicaAckPolicyKind::SimpleMajority,
        Duration::from_millis(50),
    );
    let err = res.expect_err("non-master must reject");
    assert_eq!(err.kind, AckWaitErrorKind::NotMaster);

    let _ = env.close();
}

/// When the configured policy can be satisfied (e.g. peer ack arrives
/// during the wait), the coordinator returns `Ok` promptly without
/// waiting the full timeout.
#[test]
fn f1_acks_within_timeout_succeed() {
    let env = build_master_env("master_f1_ok");
    env.become_master(1).unwrap();
    add_peers(&env, 1);

    // Spawn a thread that records an ack shortly after the commit
    // starts waiting. With one peer the SimpleMajority policy needs
    // exactly 1 ack.
    let env_for_ack = Arc::clone(&env);
    let ack_thread = std::thread::spawn(move || {
        // Wait long enough for the coordinator to register the commit
        // VLSN, then ack it. The commit_seq used by the coordinator
        // starts at 1 and increments per call.
        std::thread::sleep(Duration::from_millis(20));
        env_for_ack.record_ack(1, "peer1");
    });

    let started = Instant::now();
    let res = env.await_replica_acks(
        ReplicaAckPolicyKind::SimpleMajority,
        Duration::from_secs(2),
    );
    let elapsed = started.elapsed();

    ack_thread.join().unwrap();

    assert!(res.is_ok(), "expected ack to satisfy policy; got {:?}", res);
    assert!(
        elapsed < Duration::from_millis(500),
        "should have returned promptly after ack; waited {:?}",
        elapsed
    );

    let _ = env.close();
}

/// End-to-end: install the rep coordinator on a real `noxu_db::Environment`
/// and verify that `Transaction::commit_with_durability` actually
/// blocks on replica acks. Without F1 the commit returned `Ok(())`
/// silently; with F1 it returns `NoxuError::InsufficientReplicas`.
///
// JE: ReplicatedTransactionTest.testReplicaCommitDurability — a replicated
// commit blocks until the durability (ack) policy is satisfied and fails
// cleanly (InsufficientReplicasException) when it cannot be, rather than
// returning success without the required acks.
//
// JE: ReplicatedTransactionTest.testTxnCommitException — commit(SYNC_SYNC_ALL)
// with a replica missing throws InsufficientReplicasException from the
// pre/post-log-commit hook. This end-to-end test is exactly that: commit
// with ReplicaAckPolicy::All and non-acking peers returns
// NoxuError::InsufficientReplicas (Noxu's InsufficientReplicasException).
///
/// This test now writes data (a `put`) before committing so the txn is a
/// real ack-requiring commit.  An EMPTY / read-only txn correctly returns
/// `Ok(())` WITHOUT waiting for acks (JE-faithful: a txn that logged no
/// entry assigns no commit VLSN and has nothing to replicate — see
/// `Txn.commit` which invokes the commit hooks only when
/// `updateLoggedForTxn()`, and
/// `f1_empty_commit_returns_ok_without_acks` below which pins that).
#[test]
fn f1_commit_blocks_on_replica_acks() {
    use noxu_db::durability::{Durability, ReplicaAckPolicy, SyncPolicy};
    use noxu_db::error::NoxuError;
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use std::path::PathBuf;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let rep_env = build_master_env("master_e2e");
    rep_env.become_master(1).unwrap();
    add_peers(&rep_env, 2);

    // Install coordinator. With ReplicaAckPolicy::All and 2 peers,
    // the commit needs 2 acks. Nobody acks → it must time out and
    // return InsufficientReplicas.
    env.set_replica_coordinator(rep_env.clone());
    env.set_replica_ack_timeout(Duration::from_millis(200));

    let txn = env.begin_transaction(None).unwrap();
    // Write a record so this is a data-logging commit that actually
    // requires replica acks.  Without the put the txn logs nothing and
    // correctly commits Ok without waiting (see the empty-txn test below).
    db.put_in(&txn, b"k", b"v").unwrap();
    let durability = Durability::new(
        SyncPolicy::Sync,
        SyncPolicy::Sync,
        ReplicaAckPolicy::All,
    );
    let started = Instant::now();
    let res = txn.commit_with_durability(durability);
    let elapsed = started.elapsed();

    match res {
        Err(NoxuError::InsufficientReplicas { required, available }) => {
            assert!(required >= 1, "required acks must be > 0");
            assert_eq!(available, 0);
        }
        other => panic!(
            "expected InsufficientReplicas, got {:?} after {:?}",
            other, elapsed
        ),
    }
    assert!(
        elapsed >= Duration::from_millis(150),
        "commit must wait for the configured timeout; waited {:?}",
        elapsed
    );

    let _ = db.close();
    let _ = env.close();
    let _ = rep_env.close();
}

/// An EMPTY (read-only-in-practice) txn under `ReplicaAckPolicy::All`
/// with 2 non-acking peers must return `Ok(())` promptly WITHOUT waiting
/// for replica acks.
///
/// This pins the JE-faithful behaviour introduced by the read-only-commit
/// fix: a txn that logged no entry (`has_logged_entries() == false`,
/// matching JE `updateLoggedForTxn()` == `lastLoggedLsn != NULL_LSN`)
/// assigns no commit VLSN and has nothing to replicate, so JE's
/// `Txn.commit` never invokes `preLogCommitHook`/`postLogCommitHook` and
/// therefore never calls `RepImpl.postLogCommitHook` →
/// `feederTxns.awaitReplicaAcks`.  The old (pre-`da6a2008`) behaviour of
/// blocking an empty commit on acks was the bug: replicas have nothing to
/// ack for a txn that wrote nothing.
#[test]
fn f1_empty_commit_returns_ok_without_acks() {
    use noxu_db::durability::{Durability, ReplicaAckPolicy, SyncPolicy};
    use noxu_db::{Environment, EnvironmentConfig};
    use std::path::PathBuf;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();

    let rep_env = build_master_env("master_empty");
    rep_env.become_master(1).unwrap();
    add_peers(&rep_env, 2);

    env.set_replica_coordinator(rep_env.clone());
    env.set_replica_ack_timeout(Duration::from_millis(200));

    // begin + commit with NO put: the txn logs nothing.
    let txn = env.begin_transaction(None).unwrap();
    let durability = Durability::new(
        SyncPolicy::Sync,
        SyncPolicy::Sync,
        ReplicaAckPolicy::All,
    );
    let started = Instant::now();
    let res = txn.commit_with_durability(durability);
    let elapsed = started.elapsed();

    assert!(
        res.is_ok(),
        "an empty txn must commit Ok without acks (JE-faithful); got {:?}",
        res
    );
    assert!(
        elapsed < Duration::from_millis(50),
        "an empty commit must NOT block on the ack timeout; waited {:?}",
        elapsed
    );

    let _ = env.close();
    let _ = rep_env.close();
}

// ---------------------------------------------------------------------------
// BUG-ARB-01: an ARBITER's ack is a valid durable witness for SIMPLE_MAJORITY
// at RF=2, but NEVER for ALL (JE ArbiterTest.testReplicaDown; JE
// DurabilityQuorum + RepImpl.useArbiter). These runtime tests drive the ack
// *satisfaction* side end-to-end through `await_replica_acks` + `record_ack`.
// ---------------------------------------------------------------------------

/// Add a single arbiter peer to the group (the RF=2 witness).
fn add_arbiter(env: &ReplicatedEnvironment, name: &str, id: u32) {
    let arb = RepNode::new(
        name.into(),
        NodeType::Arbiter,
        "127.0.0.1".into(),
        7_000 + id as u16,
        100 + id,
    );
    env.add_peer(arb).unwrap();
}

/// JE: ArbiterTest.testReplicaDown (SIMPLE_MAJORITY half) — with the data
/// replica down, an RF=2 SIMPLE_MAJORITY commit is still made durable by the
/// arbiter's ack. Here the group is {master, arbiter} (the data replica is
/// down/absent); the arbiter acks and the commit reaches quorum via the
/// arbiter — required=1, satisfied by the arbiter feeder's ack (JE useArbiter,
/// getNumCurrentAckFeeders counts the arbiter because isElectable()).
///
/// Before BUG-ARB-01 fix the master's ack group collapsed to {master} and
/// required 0 acks (durable with no witness). Now it requires 1 and the
/// arbiter supplies it.
#[test]
fn bug_arb_01_simple_majority_reaches_quorum_via_arbiter_ack() {
    let env = build_master_env("master_arb_sm");
    env.become_master(1).unwrap();
    // RF=2 arbiter config: one arbiter, no live data replica.
    add_arbiter(&env, "arb1", 1);

    // Required acks under SIMPLE_MAJORITY must be exactly 1 (the arbiter),
    // NOT 0 — the durability-safety guarantee.
    let env_for_ack = Arc::clone(&env);
    let ack_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        // The arbiter acks the commit VLSN (>= 0). This is the witness.
        env_for_ack.record_ack(1, "arb1");
    });

    let started = Instant::now();
    let res = env.await_replica_acks(
        ReplicaAckPolicyKind::SimpleMajority,
        Duration::from_secs(2),
    );
    let elapsed = started.elapsed();
    ack_thread.join().unwrap();

    assert!(
        res.is_ok(),
        "RF=2 SIMPLE_MAJORITY must reach quorum via the arbiter's ack; got {:?}",
        res
    );
    // needed==1 proves it did NOT collapse to a zero-witness commit.
    assert_eq!(res.unwrap(), 1, "the arbiter's ack is the required witness");
    assert!(
        elapsed < Duration::from_millis(500),
        "should return promptly after the arbiter ack; waited {:?}",
        elapsed
    );

    let _ = env.close();
}

/// JE: ArbiterTest.testReplicaDown (ALL half) — "Insertion with ACK durability
/// of ALL should have failed." With the data replica down in an RF=2 arbiter
/// group, an ALL commit must NOT be satisfiable by the arbiter: the arbiter's
/// ack does not qualify for ALL (JE useArbiter is SIMPLE_MAJORITY only), so
/// only the (down) data replica could satisfy it. The commit must time out
/// (fail), never silently succeed.
///
/// Before BUG-ARB-01 fix the master required 0 acks for ALL and returned Ok
/// with no witness — the durability violation. Now it requires 1 and, since
/// the arbiter cannot satisfy ALL, it times out.
#[test]
fn bug_arb_01_all_durability_is_not_satisfied_by_arbiter() {
    let env = build_master_env("master_arb_all");
    env.become_master(1).unwrap();
    add_arbiter(&env, "arb1", 1);

    // The arbiter acks — but it must NOT satisfy ALL.
    let env_for_ack = Arc::clone(&env);
    let ack_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        env_for_ack.record_ack(1, "arb1");
    });

    let timeout = Duration::from_millis(300);
    let started = Instant::now();
    let res = env.await_replica_acks(ReplicaAckPolicyKind::All, timeout);
    let elapsed = started.elapsed();
    ack_thread.join().unwrap();

    let err = res.expect_err(
        "ALL must FAIL when only the arbiter is up (JE testReplicaDown: \
         'ALL should have failed'); it must not silently succeed",
    );
    assert_eq!(err.kind, AckWaitErrorKind::Timeout);
    // needed==1 proves ALL required a witness (not the buggy 0); received==0
    // proves the arbiter's ack did NOT count for ALL.
    assert_eq!(err.needed, 1, "ALL over the RF=2 group needs one data ack");
    assert_eq!(
        err.received, 0,
        "the arbiter ack must NOT count toward ALL (JE useArbiter is \
         SIMPLE_MAJORITY only)"
    );
    assert!(
        elapsed >= timeout,
        "ALL must block the full timeout, not short-circuit; waited {:?}",
        elapsed
    );

    let _ = env.close();
}
