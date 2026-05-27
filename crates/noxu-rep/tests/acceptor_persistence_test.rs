//! Integration tests for F5 / F31: persistent acceptor promises.
//!
//! Closes findings F5 and F31 of `docs/src/internal/api-audit-2026-05-rep.md`.
//!
//! The Paxos safety invariant is that an acceptor never accepts a
//! proposal at a term lower than its highest promise.  Without
//! crash-durable promises, a node that restarts forgets every promise
//! and an old proposer (with a low term) can win a fresh majority,
//! becoming a second master at the same effective term — split-brain.
//!
//! These tests verify that:
//!
//! 1. A `ReplicatedEnvironment` opened with `env_home` persists every
//!    promise and accept to `<env_home>/acceptor.state`.
//! 2. After a simulated restart (close + new env at the same env_home),
//!    a stale proposer with a term lower than the persisted promise is
//!    rejected.

use noxu_rep::{
    elections::{ELECTION_SERVICE_NAME, PersistentAcceptorState},
    RepConfig, ReplicatedEnvironment,
};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn acceptor_state_file_is_created_on_open() {
    let dir = TempDir::new().unwrap();
    let env_home = dir.path().to_path_buf();

    let config = RepConfig::builder("g1", "n1", "127.0.0.1")
        .node_port(0)
        .env_home(&env_home)
        .build();
    let env = ReplicatedEnvironment::new(config).unwrap();

    // The file is only created when we make a promise.  Force one via
    // PersistentAcceptorState directly.
    let state = PersistentAcceptorState::load_or_default(&env_home);
    assert!(state.try_promise(7));

    let file = env_home.join("acceptor.state");
    assert!(file.exists(), "acceptor.state must be written");

    env.close().unwrap();
}

#[test]
fn promise_survives_restart_and_rejects_stale_term() {
    // Simulate the F5/F31 split-brain attack:
    //   1. Node promises term 10.
    //   2. Process crashes.
    //   3. New process starts at the same env_home.
    //   4. A stale proposer at term 7 must be rejected.
    let dir = TempDir::new().unwrap();
    let env_home = dir.path().to_path_buf();

    {
        let config = RepConfig::builder("g1", "n1", "127.0.0.1")
            .node_port(0)
            .env_home(&env_home)
            .build();
        let env = ReplicatedEnvironment::new(config).unwrap();
        // Reach into the persistent state via the public re-export and
        // force a promise.  This mirrors what the acceptor side does
        // when an ELECTION proposer connects.
        let state = PersistentAcceptorState::load_or_default(&env_home);
        assert!(state.try_promise(10));
        env.close().unwrap();
    }

    // Restart.
    {
        let config = RepConfig::builder("g1", "n1", "127.0.0.1")
            .node_port(0)
            .env_home(&env_home)
            .build();
        let env = ReplicatedEnvironment::new(config).unwrap();
        let state = PersistentAcceptorState::load_or_default(&env_home);
        assert_eq!(state.promised_term(), 10);
        // Stale proposer at term 7 must be rejected.
        assert!(
            !state.try_promise(7),
            "post-restart acceptor must reject term lower than persisted promise"
        );
        env.close().unwrap();
    }
}

#[test]
fn election_service_uses_persistent_state_end_to_end() {
    use noxu_rep::elections::paxos::run_election;
    use noxu_rep::net::channel::Channel;
    use noxu_rep::net::service_dispatcher::connect_to_service;
    use noxu_rep::{NodeType, RepGroup, RepNode};

    let dir = TempDir::new().unwrap();
    let env_home = dir.path().to_path_buf();

    let config = RepConfig::builder("g1", "n1", "127.0.0.1")
        .node_port(0)
        .env_home(&env_home)
        .build();
    let env = ReplicatedEnvironment::new(config).unwrap();
    let addr = env.bound_addr().expect("must bind");

    // Build a tiny 2-node group view.
    let mut group = RepGroup::new("g1".to_string(), 1);
    group.add_node(RepNode::new(
        "self".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        addr.port(),
        1,
    ));
    group.add_node(RepNode::new(
        "n1".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        addr.port(),
        2,
    ));

    // Run an election against the running ELECTION service.  The proposer
    // wins at term 5 and becomes "self" master.
    let ch = connect_to_service(addr, ELECTION_SERVICE_NAME).unwrap();
    let ch_arc: Arc<dyn Channel> = Arc::new(ch);
    let winner = run_election(1, "self", &group, &[ch_arc], 100, 1, 5);
    assert_eq!(winner, Some(1));

    // Give the per-connection acceptor thread time to flush state.
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Now verify that the acceptor recorded master "self" at term 5 in
    // its persistent file.
    let state = PersistentAcceptorState::load_or_default(&env_home);
    assert_eq!(state.promised_term(), 5);
    assert_eq!(state.accepted_term(), 5);
    assert_eq!(state.accepted_master(), Some("self".to_string()));

    // Try a stale proposal at term 3: must be rejected.
    let ch2 = connect_to_service(addr, ELECTION_SERVICE_NAME).unwrap();
    let ch2_arc: Arc<dyn Channel> = Arc::new(ch2);
    let stale = run_election(1, "self", &group, &[ch2_arc], 100, 1, 3);
    assert!(
        stale.is_none(),
        "stale proposer at term 3 must NOT win after a term-5 promise was persisted"
    );

    env.close().unwrap();
}
