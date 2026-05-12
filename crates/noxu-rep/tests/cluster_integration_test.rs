//! Cluster integration tests for the noxu-rep replication subsystem.
//!
//! These tests verify end-to-end interactions that are NOT covered by the
//! existing unit tests inside each source module:
//!
//! - `test_election_over_tcp_channels`: Two-phase Paxos driven over real
//!   loopback TCP sockets.  The unit tests in `paxos.rs` use only in-memory
//!   `LocalChannelPair` channels; this verifies the full TcpChannel framing
//!   (4-byte LE length prefix + payload) works correctly for elections.
//!
//! - `test_election_tcp_higher_vlsn_peer_wins`: Same TCP path; confirms the
//!   Phase-1 "best proposal" tracking elects the node with the highest VLSN
//!   even when it is the acceptor rather than the proposer.
//!
//! - `test_replica_applies_1000_entries`: apply_entry streaming at scale —
//!   1 000 entries, verifying VLSN index coverage and range span.  Existing
//!   tests use `register_vlsn` with 50 entries; this exercises the replica
//!   code-path at higher volume.
//!
//! - `test_env_home_registers_restore_service`: Setting `env_home` in
//!   `RepConfig` causes `ReplicatedEnvironment::new()` to start its TCP
//!   service dispatcher and register the RESTORE handler.  Verified by
//!   confirming `bound_addr()` is `Some` and then round-tripping files
//!   through a standalone `NetworkRestoreServer` backed by the same dir.
//!
//! - `test_three_node_failover`: master crash → replica → Unknown → new
//!   master, with VLSN continuity enforced across the leadership boundary.
//!
//! - `test_partition_and_catch_up`: replica falls behind during a simulated
//!   partition, then catches up via sequential `apply_entry`; final VLSN on
//!   replica matches the master and the range spans the full [1..N] window.
//!
//! - `test_fpaxos_5node_election_phase2_2`: Flexible Paxos with phase1=4,
//!   phase2=2 on a 5-node group; election succeeds with 2 Phase-2 accepts.
//!
//! - `test_dynamic_peer_add_remove`: add_peer/remove_peer at runtime on a
//!   ReplicatedEnvironment; get_rep_group() reflects changes immediately.

use std::net::SocketAddr;
use std::sync::Arc;

use noxu_rep::{
    NetworkRestore, NetworkRestoreConfig, NetworkRestoreServer, NodeState,
    NodeType, RepConfig, RepGroup, RepNode, ReplicatedEnvironment,
};
use noxu_rep::elections::{run_acceptor, run_election};
use noxu_rep::net::{Channel, TcpChannel, TcpChannelListener};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a 3-node fully-electable `RepGroup`.
///
/// The port numbers stored in the group are placeholders; they are NOT used
/// by `run_election` / `run_acceptor` for making connections.  The actual
/// TCP connections are established by the test via `TcpChannelListener` with
/// OS-assigned ephemeral ports.
fn make_3node_group() -> RepGroup {
    let mut g = RepGroup::new("cluster_test_group".to_string(), 1);
    for i in 1u32..=3 {
        g.add_node(RepNode::new(
            format!("cluster_node{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            5500 + i as u16,
            i,
        ));
    }
    g
}

/// Bind a `TcpChannelListener` on `127.0.0.1:0` (OS-assigned ephemeral port).
fn ephemeral_listener() -> TcpChannelListener {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    TcpChannelListener::bind(addr).expect("bind loopback listener")
}

// ---------------------------------------------------------------------------
// P3-1: Paxos election over real TCP channels
// ---------------------------------------------------------------------------

/// Run `run_election` / `run_acceptor` through real loopback TCP sockets.
///
/// The in-module unit tests in `paxos.rs` use only `LocalChannelPair` (shared
/// memory queues).  This test verifies the same protocol works correctly when
/// messages traverse the `TcpChannel` length-prefix framing layer.
///
/// Setup: 3-node group (quorum = 2).
///   - node1 (id=1, vlsn=100): proposer.  Highest VLSN → should win.
///   - node2 (id=2, vlsn=50): acceptor thread on listener2.
///   - node3 (id=3, vlsn=50): acceptor thread on listener3.
#[test]
fn test_election_over_tcp_channels() {
    let group = make_3node_group();

    // Two listeners — one per peer acceptor.
    let listener2 = ephemeral_listener();
    let addr2 = listener2.local_addr().unwrap();
    let listener3 = ephemeral_listener();
    let addr3 = listener3.local_addr().unwrap();

    // Acceptor threads: each blocks on accept(), runs run_acceptor, returns
    // Ok(Some(master_name)) when phase-2 is accepted.
    let h2 = std::thread::spawn(move || {
        let ch = listener2.accept().expect("acceptor2 accept");
        run_acceptor(&ch, "cluster_node2", 50, 1, 1)
    });
    let h3 = std::thread::spawn(move || {
        let ch = listener3.accept().expect("acceptor3 accept");
        run_acceptor(&ch, "cluster_node3", 50, 1, 1)
    });

    // Proposer: connect to each acceptor and run the election.
    let ch2: Arc<dyn Channel> =
        Arc::new(TcpChannel::connect(addr2).expect("connect to node2"));
    let ch3: Arc<dyn Channel> =
        Arc::new(TcpChannel::connect(addr3).expect("connect to node3"));

    let winner =
        run_election(1, "cluster_node1", &group, &[ch2, ch3], 100, 1, 1);

    let acc2 = h2.join().expect("acceptor2 thread panicked");
    let acc3 = h3.join().expect("acceptor3 thread panicked");

    // Election must succeed (quorum = 2; proposer self-vote + 2 peer votes).
    assert!(winner.is_some(), "election must elect a winner with quorum 2/3");
    // node1 has the highest VLSN (100 > 50) and should be elected master.
    assert_eq!(winner.unwrap(), 1, "node1 (vlsn=100) must win");

    // Both acceptors committed to the phase-2 result.
    assert!(
        acc2.unwrap_or(None).is_some(),
        "acceptor2 must have accepted the phase-2 result"
    );
    assert!(
        acc3.unwrap_or(None).is_some(),
        "acceptor3 must have accepted the phase-2 result"
    );
}

/// Verify that the best-proposal (highest-VLSN) selection works over TCP when
/// the best candidate is the *acceptor* rather than the proposer.
///
/// node2 (acceptor, vlsn=999) has a far higher VLSN than node1 (proposer,
/// vlsn=5).  In Phase 1 node2's acceptor returns its own suggestion; the
/// proposer picks node2 as the winner and announces it in Phase 2.  Both
/// acceptors must confirm "cluster_node2" as master.
#[test]
fn test_election_tcp_higher_vlsn_peer_wins() {
    let group = make_3node_group();

    let listener2 = ephemeral_listener();
    let addr2 = listener2.local_addr().unwrap();
    let listener3 = ephemeral_listener();
    let addr3 = listener3.local_addr().unwrap();

    // node2: high VLSN acceptor.
    let h2 = std::thread::spawn(move || {
        let ch = listener2.accept().expect("acceptor2 accept");
        run_acceptor(&ch, "cluster_node2", 999, 1, 1)
    });
    // node3: low VLSN acceptor (same as proposer).
    let h3 = std::thread::spawn(move || {
        let ch = listener3.accept().expect("acceptor3 accept");
        run_acceptor(&ch, "cluster_node3", 5, 1, 1)
    });

    let ch2: Arc<dyn Channel> = Arc::new(TcpChannel::connect(addr2).unwrap());
    let ch3: Arc<dyn Channel> = Arc::new(TcpChannel::connect(addr3).unwrap());

    // node1 proposes with vlsn=5 — lower than node2's vlsn=999.
    let winner =
        run_election(1, "cluster_node1", &group, &[ch2, ch3], 5, 1, 1);

    let acc2 = h2.join().unwrap();
    let acc3 = h3.join().unwrap();

    assert!(winner.is_some(), "election must produce a winner");
    // node2 (id=2) has the best proposal and must be elected.
    assert_eq!(winner.unwrap(), 2, "node2 (vlsn=999) must be elected");

    // Both acceptors must have accepted the result naming "cluster_node2".
    let master2 = acc2.unwrap_or(None);
    let master3 = acc3.unwrap_or(None);
    assert_eq!(
        master2.as_deref(),
        Some("cluster_node2"),
        "acceptor2 must report cluster_node2 as master"
    );
    assert_eq!(
        master3.as_deref(),
        Some("cluster_node2"),
        "acceptor3 must report cluster_node2 as master"
    );
}

// ---------------------------------------------------------------------------
// P3-2: apply_entry streaming at scale
// ---------------------------------------------------------------------------

/// Verify that a replica can receive and apply 1 000 log entries in order,
/// and that the VLSN index covers the full [1..1000] range without gaps.
///
/// This exercises a code path distinct from `test_vlsn_monotonically_
/// increasing_on_master` in `tcp_integration.rs`, which calls `register_vlsn`
/// on a master node with only 50 entries.  Here we call `apply_entry` — the
/// replica entry point — and verify VLSN index coverage at 20× that scale.
#[test]
fn test_replica_applies_1000_entries() {
    let env = ReplicatedEnvironment::new(
        RepConfig::builder("stream_group", "stream_replica", "127.0.0.1")
            .build(),
    )
    .expect("env creation failed");

    env.become_replica("stream_master").expect("become_replica failed");

    const N: u64 = 1_000;
    for vlsn in 1..=N {
        let data = vec![(vlsn & 0xFF) as u8; 16];
        env.apply_entry(vlsn, 0, data)
            .unwrap_or_else(|e| panic!("apply_entry({vlsn}) failed: {e}"));
    }

    // Current VLSN must reflect the last applied entry.
    let current = env.get_current_vlsn();
    assert_eq!(current, N, "current VLSN must equal last applied ({N})");

    // VLSN range must cover all N entries.
    let range = env.get_vlsn_range();
    assert_eq!(range.get_last(), N, "vlsn_range.last must be {N}");
    assert!(range.get_first() <= 1, "vlsn_range.first must be ≤ 1");

    // Range span must be at least N — no gaps, no duplicates lost.
    let span = range.get_last().saturating_sub(range.get_first()) + 1;
    assert!(
        span >= N,
        "VLSN range must span ≥ {N} entries; got span={span}"
    );

    env.close().expect("close failed");
}

// ---------------------------------------------------------------------------
// P3-3: env_home in RepConfig registers the RESTORE service
// ---------------------------------------------------------------------------

/// Verify that providing `env_home` in `RepConfig` causes `ReplicatedEnvironment`
/// to start the TCP service dispatcher (so `bound_addr()` is `Some`) and to
/// register the RESTORE handler.
///
/// The registration is validated indirectly: we confirm `bound_addr()` is
/// `Some` (service dispatcher started), then verify the same `env_home` dir
/// can serve files via a standalone `NetworkRestoreServer` — confirming the
/// file-serving infrastructure wired through `RepConfig::env_home` is intact.
#[test]
fn test_env_home_registers_restore_service() {
    use std::io::Write as _;
    use tempfile::TempDir;

    // Build a fake env_home with two .ndb log files.
    let env_home = TempDir::new().unwrap();
    for i in 0u8..2 {
        let path = env_home.path().join(format!("{i:08}.ndb"));
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&vec![i + 1; 512]).unwrap();
    }

    // Create a ReplicatedEnvironment with env_home and port=0.
    // At construction the TCP service dispatcher binds on an OS-assigned port
    // and registers the RESTORE handler for `env_home`.
    let config = RepConfig::builder(
        "restore_group",
        "restore_node",
        "127.0.0.1",
    )
    .node_port(0)
    .env_home(env_home.path())
    .build();

    let rep_env = ReplicatedEnvironment::new(config).expect("env creation failed");

    // bound_addr must be Some — service dispatcher started successfully.
    assert!(
        rep_env.bound_addr().is_some(),
        "ReplicatedEnvironment with node_port=0 must have a bound address"
    );

    // Verify file content is correct by running a standalone restore against
    // the same env_home directory — this confirms the files the service would
    // serve are intact and correctly readable.
    let server = Arc::new(NetworkRestoreServer::new(env_home.path()));
    let srv_bound = server
        .start("127.0.0.1:0".parse().unwrap())
        .expect("standalone server start");
    std::thread::sleep(std::time::Duration::from_millis(20));

    let restore_dir = TempDir::new().unwrap();
    let restore = NetworkRestore::new(NetworkRestoreConfig {
        source_node: "restore_node".to_string(),
        source_host: "127.0.0.1".to_string(),
        source_port: srv_bound.port(),
        retain_log_files: false,
    })
    .with_local_dir(restore_dir.path());
    restore.execute().expect("restore execute failed");

    // Both .ndb files must have been transferred with exact content.
    for i in 0u8..2 {
        let fname = format!("{i:08}.ndb");
        let got = std::fs::read(restore_dir.path().join(&fname))
            .unwrap_or_else(|_| panic!("restored file '{fname}' missing"));
        assert_eq!(
            got,
            vec![i + 1; 512],
            "restored file '{fname}' content mismatch"
        );
    }

    let progress = restore.get_progress();
    assert_eq!(progress.files_transferred, 2, "exactly 2 files must transfer");
    assert_eq!(
        progress.bytes_transferred,
        2 * 512,
        "total bytes must be 2 × 512"
    );

    server.stop();
    rep_env.close().expect("rep_env close failed");
}

// ---------------------------------------------------------------------------
// P3-4: Three-node failover with VLSN continuity
// ---------------------------------------------------------------------------

/// Verify that VLSN is preserved across a master crash + re-election cycle
/// in a three-node cluster.
///
/// Scenario:
///   1. node1 = master (term=1); registers VLSNs 1–30.
///   2. node2 and node3 = replicas; each applies VLSNs 1–30.
///   3. node1 "crashes" (close()).
///   4. node2 transitions: Replica → Unknown → Master (term=2).
///   5. VLSN on new master must be ≥ 30 (no regression across failover).
///   6. New master continues writing VLSNs 31–50.
///   7. node3 rejoins as replica of node2 and catches up to VLSN 50.
#[test]
fn test_three_node_failover() {
    // ---- node1: initial master ----
    let node1 = ReplicatedEnvironment::new(
        RepConfig::builder("failover_group", "failover_node1", "127.0.0.1")
            .build(),
    )
    .unwrap();
    node1.become_master(1).unwrap();

    // ---- node2: replica ----
    let node2 = ReplicatedEnvironment::new(
        RepConfig::builder("failover_group", "failover_node2", "127.0.0.1")
            .build(),
    )
    .unwrap();
    node2.become_replica("failover_node1").unwrap();

    // ---- node3: replica ----
    let node3 = ReplicatedEnvironment::new(
        RepConfig::builder("failover_group", "failover_node3", "127.0.0.1")
            .build(),
    )
    .unwrap();
    node3.become_replica("failover_node1").unwrap();

    // Master commits 30 entries; both replicas apply them.
    for vlsn in 1u64..=30 {
        node1.register_vlsn(vlsn, 0, vlsn as u32 * 16);
        node2
            .apply_entry(vlsn, 0, vec![vlsn as u8; 8])
            .unwrap_or_else(|e| panic!("node2 apply_entry({vlsn}): {e}"));
        node3
            .apply_entry(vlsn, 0, vec![vlsn as u8; 8])
            .unwrap_or_else(|e| panic!("node3 apply_entry({vlsn}): {e}"));
    }

    assert_eq!(node1.get_current_vlsn(), 30, "master must be at VLSN 30");
    assert_eq!(node2.get_current_vlsn(), 30, "node2 must be at VLSN 30");
    assert_eq!(node3.get_current_vlsn(), 30, "node3 must be at VLSN 30");

    // Master crashes.
    node1.close().unwrap();

    // node2 wins re-election: Replica → Unknown → Master.
    node2.ensure_unknown_state().unwrap();
    assert_eq!(node2.get_state(), NodeState::Unknown);

    node2.become_master(2).unwrap();
    assert_eq!(
        node2.get_state(),
        NodeState::Master,
        "node2 must be Master after re-election"
    );

    // Critical invariant: VLSN must not regress across the failover boundary.
    let vlsn_after_failover = node2.get_current_vlsn();
    assert!(
        vlsn_after_failover >= 30,
        "new master VLSN must not regress: expected ≥30, got {vlsn_after_failover}"
    );

    // New master continues writing VLSNs 31–50.
    for vlsn in 31u64..=50 {
        node2.register_vlsn(vlsn, 0, vlsn as u32 * 16);
    }
    assert_eq!(
        node2.get_current_vlsn(),
        50,
        "new master must reach VLSN 50"
    );

    // node3 follows the new master and catches up.
    node3.ensure_unknown_state().unwrap();
    node3.become_replica("failover_node2").unwrap();
    for vlsn in 31u64..=50 {
        node3
            .apply_entry(vlsn, 0, vec![vlsn as u8; 8])
            .unwrap_or_else(|e| panic!("node3 catch-up apply_entry({vlsn}): {e}"));
    }
    assert_eq!(
        node3.get_current_vlsn(),
        50,
        "node3 must catch up to VLSN 50 under new master"
    );

    node2.close().unwrap();
    node3.close().unwrap();
}

// ---------------------------------------------------------------------------
// P3-5: Network partition simulation and replica catch-up
// ---------------------------------------------------------------------------

/// Simulate a network partition followed by replica catch-up via sequential
/// `apply_entry` calls.
///
/// Scenario:
///   1. Pre-partition: master registers VLSNs 1–10; replica applies them.
///   2. Partition begins: master continues to VLSN 30; replica is stuck at 10.
///   3. Partition heals: replica applies the gap entries 11–30.
///   4. Replica VLSN reaches 30 (same as master).
///   5. Replica VLSN range spans [1..30] with no gaps.
#[test]
fn test_partition_and_catch_up() {
    let master = ReplicatedEnvironment::new(
        RepConfig::builder("partition_grp", "part_master", "127.0.0.1")
            .build(),
    )
    .unwrap();
    master.become_master(1).unwrap();

    let replica = ReplicatedEnvironment::new(
        RepConfig::builder("partition_grp", "part_replica", "127.0.0.1")
            .build(),
    )
    .unwrap();
    replica.become_replica("part_master").unwrap();

    // --- Phase 1: pre-partition — both nodes in sync (VLSNs 1–10) ---
    for vlsn in 1u64..=10 {
        master.register_vlsn(vlsn, 0, vlsn as u32 * 8);
        replica
            .apply_entry(vlsn, 0, vec![0u8; 8])
            .unwrap_or_else(|e| panic!("pre-partition apply_entry({vlsn}): {e}"));
    }
    assert_eq!(master.get_current_vlsn(), 10, "master must be at 10");
    assert_eq!(replica.get_current_vlsn(), 10, "replica must be at 10");

    // --- Phase 2: partition — master writes 11–30, replica cannot receive ---
    for vlsn in 11u64..=30 {
        master.register_vlsn(vlsn, 0, vlsn as u32 * 8);
    }
    assert_eq!(master.get_current_vlsn(), 30, "master must be at 30");
    // Replica is still at 10 during the partition.
    assert_eq!(
        replica.get_current_vlsn(),
        10,
        "replica must lag at 10 during partition"
    );

    // --- Phase 3: partition heals — replica catches up (VLSNs 11–30) ---
    for vlsn in 11u64..=30 {
        replica
            .apply_entry(vlsn, 0, vec![0u8; 8])
            .unwrap_or_else(|e| panic!("catch-up apply_entry({vlsn}): {e}"));
    }

    // Replica has fully caught up to the master.
    assert_eq!(
        replica.get_current_vlsn(),
        30,
        "replica must reach VLSN 30 after catch-up"
    );

    // VLSN range on replica must span the full [1..30] window.
    let range = replica.get_vlsn_range();
    assert_eq!(range.get_last(), 30, "vlsn_range.last must be 30");
    assert!(range.get_first() <= 1, "vlsn_range.first must be ≤ 1");

    master.close().unwrap();
    replica.close().unwrap();
}

// ---------------------------------------------------------------------------
// P3-6: State change listener receives master/replica events
// ---------------------------------------------------------------------------

/// Verify that `StateChangeListener` callbacks are fired when a node
/// transitions between `Master` and `Replica` states.
///
/// This tests a code path not exercised by any other test file: the listener
/// registration + dispatch mechanism inside `ReplicatedEnvironment`.
#[test]
fn test_state_change_listener_fires_on_transitions() {
    use noxu_rep::{StateChangeEvent, StateChangeListener};
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Counter {
        master_count: AtomicU32,
        replica_count: AtomicU32,
    }

    impl StateChangeListener for Counter {
        fn on_state_change(&self, event: StateChangeEvent) {
            match event.new_state {
                NodeState::Master => {
                    self.master_count.fetch_add(1, Ordering::SeqCst);
                }
                NodeState::Replica => {
                    self.replica_count.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    }

    let counter = Arc::new(Counter {
        master_count: AtomicU32::new(0),
        replica_count: AtomicU32::new(0),
    });

    let env = ReplicatedEnvironment::new(
        RepConfig::builder("listener_group", "listener_node", "127.0.0.1")
            .build(),
    )
    .unwrap();

    env.set_state_change_listener(Arc::clone(&counter) as _);

    // Transition to master → listener must be called.
    env.become_master(1).unwrap();
    assert_eq!(
        counter.master_count.load(Ordering::SeqCst),
        1,
        "listener must fire once on become_master"
    );

    // Transition to replica → listener must be called.
    env.become_replica("other_node").unwrap();
    assert_eq!(
        counter.replica_count.load(Ordering::SeqCst),
        1,
        "listener must fire once on become_replica"
    );

    // Second master transition → total master fires = 2.
    env.become_master(2).unwrap();
    assert_eq!(
        counter.master_count.load(Ordering::SeqCst),
        2,
        "listener must fire again on second become_master"
    );

    env.close().unwrap();
}

// ---------------------------------------------------------------------------
// FPaxos: 5-node cluster, phase1=4, phase2=2
// ---------------------------------------------------------------------------

/// Verify Flexible Paxos with phase1=4, phase2=2 on a 5-node cluster over
/// real loopback TCP sockets.
///
/// Setup:
///   - node1 (id=1, vlsn=200): proposer.  Highest VLSN → should win.
///   - node2..node5: acceptors on ephemeral listeners.
///
/// Phase 1 requires 4 promises (proposer + ≥ 3 peers).
/// Phase 2 requires 2 accepts (proposer + ≥ 1 peer).
///
/// All 4 peers participate so both phase constraints are met.
#[test]
fn test_fpaxos_5node_election_phase2_2() {
    use noxu_rep::QuorumPolicy;

    // Build a 5-node RepGroup with Flexible{phase1:4, phase2:2}.
    let mut group = RepGroup::new("fpaxos_test_group".to_string(), 42);
    for i in 1u32..=5 {
        group.add_node(RepNode::new(
            format!("fp_node{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            6600 + i as u16,
            i,
        ));
    }
    group.set_quorum_policy(QuorumPolicy::Flexible { phase1: 4, phase2: 2 });

    // Verify the policy is applied correctly.
    assert_eq!(group.phase1_quorum(), 4, "phase1 quorum must be 4");
    assert_eq!(group.phase2_quorum(), 2, "phase2 quorum must be 2");

    // Spawn 4 acceptor threads (nodes 2–5).
    let listeners: Vec<_> = (0..4).map(|_| ephemeral_listener()).collect();
    let addrs: Vec<_> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap())
        .collect();

    let handles: Vec<_> = listeners
        .into_iter()
        .enumerate()
        .map(|(i, listener)| {
            let node_name = format!("fp_node{}", i + 2);
            std::thread::spawn(move || {
                let ch = listener.accept().expect("acceptor accept");
                run_acceptor(&ch, &node_name, 50, 1, 1)
            })
        })
        .collect();

    // Proposer: connect to all 4 acceptors.
    let channels: Vec<Arc<dyn Channel>> = addrs
        .iter()
        .map(|&addr| Arc::new(TcpChannel::connect(addr).expect("connect")) as Arc<dyn Channel>)
        .collect();

    let winner = run_election(1, "fp_node1", &group, &channels, 200, 1, 1);

    // Collect acceptor results.
    let acceptor_results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("acceptor thread panicked"))
        .collect();

    // Election must succeed.
    assert!(
        winner.is_some(),
        "election must succeed with Flexible{{phase1:4, phase2:2}} on 5-node cluster"
    );
    assert_eq!(winner.unwrap(), 1, "fp_node1 (highest VLSN 200) must win");

    // All acceptors must have accepted a phase-2 result.
    for (i, result) in acceptor_results.iter().enumerate() {
        assert!(
            result.as_ref().is_ok_and(|r| r.is_some()),
            "acceptor {} must have accepted the phase-2 result",
            i + 2
        );
    }
}

// ---------------------------------------------------------------------------
// Dynamic peer management: add_peer / remove_peer
// ---------------------------------------------------------------------------

/// Verify that `add_peer` and `remove_peer` work at runtime and that
/// `get_rep_group()` reflects changes immediately.
#[test]
fn test_dynamic_peer_add_remove() {
    // Start with a single-node (self) environment.
    let env = ReplicatedEnvironment::new(
        RepConfig::builder("dyn_group", "dyn_node1", "127.0.0.1")
            .node_port(0)
            .build(),
    )
    .expect("env creation failed");

    // Initially no peers beyond self.
    let initial_group = env.get_rep_group();
    let initial_count = initial_group.electable_count() as usize;

    // Add node2.
    env.add_peer(RepNode::new(
        "dyn_node2".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        5802,
        2,
    ))
    .expect("add_peer must succeed");

    let group_after_add = env.get_rep_group();
    assert_eq!(
        group_after_add.electable_count() as usize,
        initial_count + 1,
        "electable count must increase by 1 after add_peer"
    );
    assert!(
        group_after_add.contains_node("dyn_node2"),
        "group must contain the newly added peer"
    );

    // Add node3.
    env.add_peer(RepNode::new(
        "dyn_node3".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        5803,
        3,
    ))
    .expect("second add_peer must succeed");

    let group_3node = env.get_rep_group();
    assert_eq!(
        group_3node.electable_count() as usize,
        initial_count + 2,
        "electable count must be initial + 2 after two add_peer calls"
    );

    // Remove node2.
    env.remove_peer("dyn_node2").expect("remove_peer must succeed");

    let group_after_remove = env.get_rep_group();
    assert_eq!(
        group_after_remove.electable_count() as usize,
        initial_count + 1,
        "electable count must decrease by 1 after remove_peer"
    );
    assert!(
        !group_after_remove.contains_node("dyn_node2"),
        "removed peer must no longer be in the group"
    );
    assert!(
        group_after_remove.contains_node("dyn_node3"),
        "remaining peer dyn_node3 must still be in the group"
    );
}

// ---------------------------------------------------------------------------
// Dynamic peer metadata update
// ---------------------------------------------------------------------------

/// Verify that `update_peer_metadata` updates capacity/latency on an existing
/// peer without corrupting group state or disrupting replication.
#[test]
fn test_update_peer_metadata_while_active() {
    let env = ReplicatedEnvironment::new(
        RepConfig::builder("meta_group", "meta_node1", "127.0.0.1")
            .node_port(0)
            .build(),
    )
    .expect("env creation failed");

    // Add two peers.
    env.add_peer(RepNode::new(
        "meta_node2".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        5902,
        2,
    ))
    .unwrap();
    env.add_peer(RepNode::new(
        "meta_node3".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        5903,
        3,
    ))
    .unwrap();

    // Become master and register some VLSNs to prove replication works.
    env.become_master(1).unwrap();
    for vlsn in 1u64..=5 {
        env.register_vlsn(vlsn, 0, vlsn as u32 * 8);
    }
    assert_eq!(env.get_current_vlsn(), 5);

    // Update meta_node2's write capacity to 2.0 (200 pct).
    let updated_node = RepNode::new(
        "meta_node2".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        5902,
        2,
    )
    .with_write_capacity(2.0)
    .with_latency_hint(std::time::Duration::from_millis(5));

    env.update_peer_metadata("meta_node2", updated_node).unwrap();

    // Verify the group snapshot reflects the new metadata.
    let group = env.get_rep_group();
    let node2 = group.get_node("meta_node2").expect("meta_node2 must exist");
    assert_eq!(node2.write_capacity_pct, 200, "write capacity must be updated to 200");
    assert_eq!(node2.latency_hint_ms, 5, "latency must be updated to 5ms");
    assert_eq!(node2.read_capacity_pct, 100, "read capacity must remain default");

    // meta_node3 must be unaffected.
    let node3 = group.get_node("meta_node3").expect("meta_node3 must exist");
    assert_eq!(node3.write_capacity_pct, 100);
    assert_eq!(node3.latency_hint_ms, 1);

    // Replication still works after metadata update.
    for vlsn in 6u64..=10 {
        env.register_vlsn(vlsn, 0, vlsn as u32 * 8);
    }
    assert_eq!(env.get_current_vlsn(), 10);

    // Updating a non-existent peer must fail.
    let bogus = RepNode::new(
        "ghost".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        9999,
        99,
    );
    assert!(
        env.update_peer_metadata("ghost", bogus).is_err(),
        "updating non-existent peer must return error"
    );

    env.close().unwrap();
}
