//! F6: Election driver wired into ReplicatedEnvironment::open().
//!
//! Pre-Wave-3-3, opening a `ReplicatedEnvironment` per the docs ("creating
//! a `ReplicatedEnvironment` will trigger an election") never actually
//! held an election. The node sat in `Detached`. Production deployments
//! had to call `become_master` / `become_replica` by hand.
//!
//! Wave 3-3 adds:
//!   - An ELECTION service registered on the TCP dispatcher in
//!     `ReplicatedEnvironment::new`.
//!   - A `start_election_driver()` method that spawns a background
//!     thread driving Paxos rounds against known peers.
//!   - `ReplicatedEnvironment::open(config) -> Arc<Self>`, the
//!     production entry point that does both.
//!
//! See the 2026 review finding F6.

use std::time::{Duration, Instant};

use noxu_rep::rep_node::RepNode;
use noxu_rep::{NodeState, NodeType, RepConfig, ReplicatedEnvironment};

fn config(node_name: &str) -> RepConfig {
    RepConfig::builder("group_f6", node_name, "127.0.0.1")
        .node_port(0)
        .node_type(NodeType::Electable)
        .election_timeout(Duration::from_millis(100))
        .build()
}

/// A single-node group bootstraps as Master without external help.
/// The election driver runs `run_election` with no peers; quorum is
/// 1, the self-vote suffices, so the proposer (self) wins.
#[test]
fn f6_single_node_open_becomes_master() {
    let env = ReplicatedEnvironment::open(config("solo")).unwrap();

    // Wait up to 2 seconds for the driver to settle.
    let deadline = Instant::now() + Duration::from_secs(2);
    while env.get_state() != NodeState::Master && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(
        env.get_state(),
        NodeState::Master,
        "single-node open() must elect self as master (F6)"
    );
    assert!(env.is_master());
    assert_eq!(env.get_master_name().as_deref(), Some("solo"),);

    let _ = env.close();
}

/// Two-node cluster: the first node bootstraps as Master, then the
/// second node opens with the first as a peer and converges to either
/// Master (if it has higher VLSN \u2014 it doesn't, both are 0, but VLSN ties
/// are broken by lexicographic name) or Replica.
///
/// We allow either resolution, since the test verifies the driver wires
/// through to ELECTION end-to-end, not VLSN ordering details.
#[test]
fn f6_two_node_cluster_resolves_via_election_driver() {
    // Node A opens first and bootstraps the group.
    let env_a = ReplicatedEnvironment::open(config("node_a")).unwrap();
    let addr_a = env_a.bound_addr().expect("node_a must bind");

    // Wait for node_a to become master.
    let deadline = Instant::now() + Duration::from_secs(2);
    while env_a.get_state() != NodeState::Master && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(env_a.get_state(), NodeState::Master);

    // Tell node_a about node_b and vice versa via add_peer.
    let env_b = ReplicatedEnvironment::open(config("node_b")).unwrap();
    let addr_b = env_b.bound_addr().expect("node_b must bind");

    env_a
        .add_peer(RepNode::new(
            "node_b".into(),
            NodeType::Electable,
            addr_b.ip().to_string(),
            addr_b.port(),
            2,
        ))
        .unwrap();
    env_b
        .add_peer(RepNode::new(
            "node_a".into(),
            NodeType::Electable,
            addr_a.ip().to_string(),
            addr_a.port(),
            1,
        ))
        .unwrap();

    // The driver loop should eventually resolve node_b into either
    // Master or Replica.  Wait up to 5 seconds.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let s = env_b.get_state();
        if matches!(s, NodeState::Master | NodeState::Replica) {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "node_b stuck in {:?} after 5 s; election driver did not \
                 wire through (F6 not closed)",
                s
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let _ = env_b.close();
    let _ = env_a.close();
}

/// `ReplicatedEnvironment::new` (without `open`) must NOT spawn the
/// driver \u2014 it preserves the explicit-control entry point used by
/// existing tests and recovery tooling.
#[test]
fn f6_new_does_not_auto_start_driver() {
    let env = ReplicatedEnvironment::new(config("manual")).unwrap();

    // Give a hypothetical driver plenty of time to fire.
    std::thread::sleep(Duration::from_millis(500));

    assert_eq!(
        env.get_state(),
        NodeState::Detached,
        "new() must leave the env in Detached without explicit driver start"
    );

    let _ = env.close();
}

/// `start_election_driver` is idempotent.
#[test]
fn f6_start_election_driver_idempotent() {
    use std::sync::Arc;
    let env = Arc::new(ReplicatedEnvironment::new(config("idem")).unwrap());
    env.start_election_driver();
    env.start_election_driver();
    env.start_election_driver();

    let deadline = Instant::now() + Duration::from_secs(2);
    while env.get_state() != NodeState::Master && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(env.get_state(), NodeState::Master);

    let _ = env.close();
}
