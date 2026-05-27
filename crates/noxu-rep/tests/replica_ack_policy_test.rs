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
//! See `docs/src/internal/api-audit-2026-05-rep.md` finding F1.
//!
//! These tests build an `Arc<ReplicatedEnvironment>` directly (the
//! coordinator does not require a real `noxu_db::Environment` to
//! verify its own contract — the F1 trait is exercised at the rep
//! layer and noxu-db wiring is verified separately by
//! `f1_commit_blocks_on_replica_acks` below).

use std::sync::Arc;
use std::time::{Duration, Instant};

use noxu_dbi::{
    AckWaitErrorKind, ReplicaAckCoordinator, ReplicaAckPolicyKind,
};
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
#[test]
fn f1_simple_majority_with_no_acks_times_out() {
    let env = build_master_env("master_f1_maj");
    env.become_master(1).unwrap();
    add_peers(&env, 1);

    let timeout = Duration::from_millis(150);
    let res = env
        .await_replica_acks(ReplicaAckPolicyKind::SimpleMajority, timeout);

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
#[test]
fn f1_commit_blocks_on_replica_acks() {
    use noxu_db::durability::{Durability, ReplicaAckPolicy, SyncPolicy};
    use noxu_db::error::NoxuError;
    use noxu_db::{Environment, EnvironmentConfig};
    use std::path::PathBuf;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();

    let rep_env = build_master_env("master_e2e");
    rep_env.become_master(1).unwrap();
    add_peers(&rep_env, 2);

    // Install coordinator. With ReplicaAckPolicy::All and 2 peers,
    // the commit needs 2 acks. Nobody acks → it must time out and
    // return InsufficientReplicas.
    env.set_replica_coordinator(rep_env.clone());
    env.set_replica_ack_timeout(Duration::from_millis(200));

    let txn = env.begin_transaction(None, None).unwrap();
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

    let _ = env.close();
    let _ = rep_env.close();
}
