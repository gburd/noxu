//! Test-parity ports for `com.sleepycat.je.rep.impl.node` behaviors that were
//! not yet directly cited by a Noxu test.
//!
//! This suite carries FAITHFUL ports of the portable, deterministic pieces of
//! the JE `rep.impl.node` test package. The heavier multi-JVM / thread-hook
//! MasterTransfer scenarios, the JE-version-negotiation tests, and the
//! JE-internal cache/handle tests are recorded as N/A in the package report
//! (`tp-je-rep-impl-node.md`) with their deviation rationale; the rep-node
//! *behaviors* (role transitions, master-transfer argument validation,
//! catch-up hand-off, CBVLSN file protection, group shutdown) are cited there
//! against the merged B5/B6/C5 suites and the tests below.
//!
//! Determinism: every test here drives the state machine directly through the
//! in-process `ReplicatedEnvironment` API (`become_master` / `become_replica`
//! / `transfer_master`) with `node_port(0)` ephemeral binds and no real
//! timing dependence, matching the `group_admin_test` / `transfer_master_*`
//! harness style.

use std::sync::Arc;
use std::time::Duration;

use noxu_rep::master_transfer::MasterTransferConfig;
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};
use tempfile::TempDir;

fn config(name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder("g1", name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home)
        .build()
}

/// Build a `ReplicatedEnvironment` with the ADMIN service registered (matches
/// `group_admin_test::admin_env` / `transfer_master_catchup_test::admin_env`).
fn admin_env(
    name: &str,
    env_home: &std::path::Path,
) -> Arc<ReplicatedEnvironment> {
    let env =
        Arc::new(ReplicatedEnvironment::new(config(name, env_home)).unwrap());
    env.register_admin_service();
    env
}

// ---------------------------------------------------------------------------
// MasterTransferTest.testStupidCodingMistakes
// ---------------------------------------------------------------------------

/// JE: `MasterTransferTest.testStupidCodingMistakes` — argument validation for
/// `ReplicatedEnvironment.transferMaster`. JE rejects (with
/// `IllegalArgumentException` / `IllegalStateException`) each of: a null/empty
/// candidate set; a bogus (non-member) replica name; a candidate that is a
/// MONITOR or SECONDARY (neither can become master); and invocation on a node
/// that is not the master. It defines a candidate set that *contains the
/// master itself* to complete immediately and successfully.
///
/// Noxu adapts JE's exceptions to `Result::Err`. The valid-argument invariant
/// (a real, electable replica) is enforced in `transfer_master` before any
/// hand-off work; the checks below would each pass on the pre-validation code
/// path only vacuously (empty / secondary / monitor targets would proceed to a
/// blind hand-off), so they are non-vacuous guards against a real regression.
///
/// Deviation from JE: JE also validates the `TimeUnit`/negative-timeout
/// arguments; Noxu's `MasterTransferConfig` takes a typed `Duration`, so a
/// negative or unit-less timeout is not representable (language/API
/// difference — see contract exception (a)).
#[test]
fn transfer_master_argument_validation() {
    let dir = TempDir::new().unwrap();
    let sec_dir = TempDir::new().unwrap();
    let env = admin_env("master", dir.path());
    env.become_master(1).unwrap();

    // Register a genuine electable replica, a SECONDARY, and a MONITOR so the
    // node-type guards are exercised against real members (not just
    // "unknown").
    env.add_peer(RepNode::new(
        "electable_replica".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        6_801,
        2,
    ))
    .unwrap();
    env.add_peer(RepNode::new(
        "secondary_node".to_string(),
        NodeType::Secondary,
        "127.0.0.1".to_string(),
        6_802,
        3,
    ))
    .unwrap();
    env.add_peer(RepNode::new(
        "monitor_node".to_string(),
        NodeType::Monitor,
        "127.0.0.1".to_string(),
        6_803,
        4,
    ))
    .unwrap();

    // Empty target set: JE rejects an empty `replicas` set.
    let empty =
        MasterTransferConfig::new(String::new(), Duration::from_secs(1));
    assert!(
        Arc::clone(&env).transfer_master(empty).is_err(),
        "empty target must be rejected (JE: empty replicas set)"
    );

    // Bogus (non-member) name: JE rejects a candidate that is not in the group.
    let bogus =
        MasterTransferConfig::new("venus".to_string(), Duration::from_secs(1));
    assert!(
        Arc::clone(&env).transfer_master(bogus).is_err(),
        "unknown target must be rejected (JE: bogus replica name)"
    );

    // SECONDARY target: a secondary can never become master.
    let to_secondary = MasterTransferConfig::new(
        "secondary_node".to_string(),
        Duration::from_secs(1),
    );
    assert!(
        Arc::clone(&env).transfer_master(to_secondary).is_err(),
        "transfer to a SECONDARY must be rejected (JE: SECONDARY cannot become master)"
    );

    // MONITOR target: a monitor is not a data node and cannot become master.
    let to_monitor = MasterTransferConfig::new(
        "monitor_node".to_string(),
        Duration::from_secs(1),
    );
    assert!(
        Arc::clone(&env).transfer_master(to_monitor).is_err(),
        "transfer to a MONITOR must be rejected (JE: MONITOR cannot become master)"
    );

    // Self-in-candidate-set: JE defines transferring to the master itself to
    // complete immediately and successfully; the node stays master.
    let to_self =
        MasterTransferConfig::new("master".to_string(), Duration::from_secs(1));
    assert!(
        Arc::clone(&env).transfer_master(to_self).is_ok(),
        "transfer to self must complete immediately and successfully (JE)"
    );
    assert!(
        env.is_master(),
        "master must remain master after a self-transfer (got {:?})",
        env.get_state()
    );

    // Invoked on a NON-master: JE throws IllegalStateException.
    let replica_env = admin_env("replica_side", sec_dir.path());
    replica_env.become_replica("master").unwrap();
    let from_replica = MasterTransferConfig::new(
        "electable_replica".to_string(),
        Duration::from_secs(1),
    );
    assert!(
        Arc::clone(&replica_env).transfer_master(from_replica).is_err(),
        "transfer_master invoked on a non-master must be rejected (JE: IllegalStateException)"
    );

    Arc::clone(&env).close().unwrap();
    Arc::clone(&replica_env).close().unwrap();
}

// ---------------------------------------------------------------------------
// ReplicaMasterStateTransitionsTest.testMasterReplicaTransition
// ---------------------------------------------------------------------------

/// JE: `ReplicaMasterStateTransitionsTest.testMasterReplicaTransition`
/// (motivated by SR 18212). node1 starts as master, relinquishes mastership to
/// node2 (`RepNode.forceMaster(true)`), then RESUMES as a *replica* with node2
/// as the master and successfully reads a value node2 wrote after the switch —
/// proving the old master's handle transitioned cleanly to the replica role
/// and re-followed the new master without a recovery.
///
/// Noxu drives the same role sequence deterministically through the state
/// machine: master → replica-of-node2 → (node2 forced to master) → old master
/// re-follows node2. We assert the same INTENT as JE — that a node can move
/// Master → Replica and correctly re-target the new master — using
/// `get_state` / `get_master_name` rather than a full cross-node commit-token
/// read (the streamed-read half is covered by
/// `chained_replication_test` / `cluster_integration_test`; this test pins the
/// ROLE-TRANSITION half that JE's test is motivated by).
///
/// Non-vacuous: if `become_replica` failed to leave the Master state or failed
/// to record the new master, the assertions below fail.
#[test]
fn master_relinquishes_then_resumes_as_replica() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    // node1 starts as master.
    let node1 = admin_env("node1", dir1.path());
    let node1_addr = node1.bound_addr().expect("node1 must bind");
    let node2 = admin_env("node2", dir2.path());
    let node2_addr = node2.bound_addr().expect("node2 must bind");

    // Wire the peers both ways so the demoted master can re-follow node2.
    node1
        .add_peer(RepNode::new(
            "node2".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            node2_addr.port(),
            2,
        ))
        .unwrap();
    node2
        .add_peer(RepNode::new(
            "node1".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            node1_addr.port(),
            1,
        ))
        .unwrap();

    node1.become_master(1).unwrap();
    assert!(node1.is_master(), "node1 starts as master");
    assert!(!node2.is_master(), "node2 starts as a non-master");

    // node1 relinquishes mastership to node2 (JE `forceMaster(true)` on node2
    // is modeled as node2 taking master at the next term and node1 demoting;
    // with no writes on node1, `transfer_master` skips the catch-up wait and
    // performs the hand-off directly).
    let cfg =
        MasterTransferConfig::new("node2".to_string(), Duration::from_secs(5));
    Arc::clone(&node1)
        .transfer_master(cfg)
        .expect("relinquish to node2 must succeed for an idle master");

    // node1 has transitioned to the REPLICA role, following node2.
    assert!(
        node1.is_replica(),
        "old master must resume as a replica (got {:?})",
        node1.get_state()
    );
    assert_eq!(
        node1.get_master_name(),
        Some("node2".to_string()),
        "the demoted node must re-target node2 as its master"
    );

    // node2 became master (grace window for the ADMIN handler to apply).
    let mut node2_master = node2.is_master();
    for _ in 0..50 {
        if node2_master {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
        node2_master = node2.is_master();
    }
    assert!(
        node2_master,
        "node2 must become master after the relinquish (got {:?})",
        node2.get_state()
    );

    Arc::clone(&node1).close().unwrap();
    Arc::clone(&node2).close().unwrap();
}
