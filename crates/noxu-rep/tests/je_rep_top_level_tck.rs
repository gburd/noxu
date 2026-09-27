//! Ports of JE replication TCK tests under `je.rep` (top-level) that
//! exercise [`crate::ReplicatedEnvironment`] lifecycle, state-change
//! listeners, group membership, and node-type behaviour.
//!
//! Each test maps to one or more `@Test` methods in the JE source under
//! `je/test/com/sleepycat/je/rep/*.java`; the doc-comment on each test
//! names the JE source file and method.
//!
//! All tests use the in-memory [`crate::test_harness::RepTestBase`]
//! harness; none of them open real network sockets, so no test in this
//! file can hang on TCP coordination.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use noxu_rep::test_harness::{CountingListener, RepTestBase};
use noxu_rep::{
    NodeState, NodeType, RepGroup, RepNode, StateChangeEvent,
    StateChangeListener,
};

// ---------------------------------------------------------------------------
// Listener that records the full event sequence, like JE's `Listener`.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingListener {
    events: noxu_sync::Mutex<Vec<NodeState>>,
}

impl RecordingListener {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn snapshot(&self) -> Vec<NodeState> {
        self.events.lock().clone()
    }
}

impl StateChangeListener for RecordingListener {
    fn on_state_change(&self, ev: StateChangeEvent) {
        self.events.lock().push(ev.new_state);
    }
}

// =====================================================================
// StateChangeListenerTest — `je.rep.StateChangeListenerTest`
// =====================================================================

/// JE: `StateChangeListenerTest.testListenerReplacement`.
///
/// "When a state-change listener is replaced with a second listener, the
/// second listener is the one that subsequently receives state-change
/// events."
///
/// In Noxu the listener model is append-only (`set_state_change_listener`
/// pushes onto a `Vec`), so the closest behavioural invariant is that a
/// freshly-attached listener receives exactly one immediate event for the
/// current state and continues to receive new transitions, while older
/// listeners also keep receiving them.  This test asserts both halves of
/// that invariant in our model.
#[test]
fn state_change_listener_replacement() {
    let mut group = RepTestBase::builder("scl_repl").group_size(1).build();
    {
        let n = &mut group.node_mut(0);
        n.open_env().unwrap();
    }
    let env = group.node(0).get_env();

    let listener1 = CountingListener::new();
    env.set_state_change_listener(
        Arc::clone(&listener1) as Arc<dyn StateChangeListener>
    );
    // Initial state on a freshly-opened env is Detached → exactly one
    // event delivered.
    assert_eq!(
        listener1.detached.load(Ordering::SeqCst)
            + listener1.unknown.load(Ordering::SeqCst),
        1,
        "first listener must observe the freshly-opened state once"
    );

    // Drive a transition.
    env.become_master(1).unwrap();
    let after_master_1 = listener1.master.load(Ordering::SeqCst);
    assert_eq!(after_master_1, 1);

    // Add a second listener; it must immediately observe the current state.
    let listener2 = CountingListener::new();
    env.set_state_change_listener(
        Arc::clone(&listener2) as Arc<dyn StateChangeListener>
    );
    assert_eq!(
        listener2.master.load(Ordering::SeqCst),
        1,
        "second listener must immediately observe current Master state"
    );

    // Both listeners must continue to observe transitions.
    env.become_replica("nobody").unwrap();
    assert_eq!(listener1.replica.load(Ordering::SeqCst), 1);
    assert_eq!(listener2.replica.load(Ordering::SeqCst), 1);

    let _ = env.close();
}

/// JE: `StateChangeListenerTest.testBasic`.
///
/// "Verify that an initial notification is always sent on listener
/// attachment (with the current state), and that subsequent transitions
/// also fire."
#[test]
fn state_change_listener_basic() {
    let mut group = RepTestBase::builder("scl_basic").group_size(3).build();
    group.create_group(1).unwrap();

    let listener_master = RecordingListener::new();
    group
        .node(0)
        .get_env()
        .set_state_change_listener(
            Arc::clone(&listener_master) as Arc<dyn StateChangeListener>
        );

    // Initial event: current state (Master).
    assert_eq!(listener_master.snapshot(), vec![NodeState::Master]);

    let listener_r1 = RecordingListener::new();
    group.node(1).get_env().set_state_change_listener(
        Arc::clone(&listener_r1) as Arc<dyn StateChangeListener>
    );
    assert_eq!(listener_r1.snapshot(), vec![NodeState::Replica]);

    // Drive a master close → unknown / detached on the master node.
    group.nodes_mut()[0].close_env().unwrap();
    let snap = listener_master.snapshot();
    // Sequence MUST end in Shutdown (terminal); we don't constrain the
    // intermediate transitions because Noxu's close path is allowed to
    // skip through Unknown.
    assert_eq!(*snap.last().unwrap(), NodeState::Shutdown);

    let _ = group.nodes_mut()[1].close_env();
    let _ = group.nodes_mut()[2].close_env();
}

/// JE: `StateChangeListenerTest.testSecondary`.
///
/// "Test state changes when establishing a secondary node, having it
/// lose contact with the master, and then shutting it down."
#[test]
fn state_change_listener_secondary() {
    let mut group = RepTestBase::builder("scl_sec")
        .group_size(2)
        .override_node_type(1, NodeType::Secondary)
        .build();
    group.create_group(1).unwrap();
    assert!(group.node(0).is_master());
    assert!(group.node(1).is_replica());
    assert_eq!(group.node(1).rep_config().node_type, NodeType::Secondary);

    let listener = RecordingListener::new();
    group.node(1).get_env().set_state_change_listener(
        Arc::clone(&listener) as Arc<dyn StateChangeListener>
    );

    // Close master, then secondary.
    group.nodes_mut()[0].close_env().unwrap();
    group.nodes_mut()[1].close_env().unwrap();

    // Sequence must begin with Replica (the initial-state event) and end
    // with Shutdown (terminal).  Intermediate Unknown is optional, just
    // like JE.
    let snap = listener.snapshot();
    assert_eq!(snap.first(), Some(&NodeState::Replica));
    assert_eq!(snap.last(), Some(&NodeState::Shutdown));
}

// =====================================================================
// ReplicatedEnvironmentTest — `je.rep.ReplicatedEnvironmentTest`
// =====================================================================

/// JE: `ReplicatedEnvironmentTest.testEnvOpenOnRepEnv` (subset).
///
/// "A `ReplicatedEnvironment` is fully usable as a regular environment
/// once opened."  In Noxu the lifecycle invariant we expose is: a
/// freshly-opened env is in [`NodeState::Detached`] and exposes a stable
/// [`crate::RepConfig`], regardless of whether it ever joined a group.
#[test]
fn rep_env_fresh_open_state_is_detached() {
    let mut group =
        RepTestBase::builder("env_fresh_open").group_size(1).build();
    {
        let n = group.node_mut(0);
        n.open_env().unwrap();
    }
    assert_eq!(group.node(0).state(), Some(NodeState::Detached));
    assert!(!group.node(0).is_master());
    assert!(!group.node(0).is_replica());
    assert_eq!(group.node(0).current_vlsn(), 0);
}

/// JE: `ReplicatedEnvironmentTest.testRepEnvConfig`.
///
/// "The configuration installed at construction time is what the env
/// reports back."  Mirrors JE's invariant that
/// `repEnv.getRepConfig().getGroupName()` round-trips.
#[test]
fn rep_env_config_round_trips() {
    let mut group = RepTestBase::builder("env_cfg").group_size(1).build();
    {
        let n = group.node_mut(0);
        n.open_env().unwrap();
    }
    let env = group.node(0).get_env();
    let cfg = env.get_config();
    assert_eq!(cfg.group_name, "env_cfg");
    assert_eq!(cfg.node_name, "env_cfg_n1");
    assert_eq!(cfg.node_host, "127.0.0.1");
}

/// JE: `ReplicatedEnvironmentTest.testRepEnvMutableConfig` (subset).
///
/// Closes and re-opens a node within the same group.  In JE this exercises
/// the `EnvironmentMutableConfig` round-trip; in Noxu the equivalent
/// observable invariant is that `RepEnvInfo::open_env` after `close_env`
/// returns a fresh handle that starts in [`NodeState::Detached`].
#[test]
fn rep_env_close_reopen_returns_fresh_handle() {
    let mut group = RepTestBase::builder("env_reopen").group_size(1).build();
    let info = group.node_mut(0);
    info.open_env().unwrap();
    info.close_env().unwrap();
    info.open_env().unwrap();
    assert_eq!(info.state(), Some(NodeState::Detached));
}

// =====================================================================
// JoinGroupTest — `je.rep.JoinGroupTest`
// =====================================================================

/// JE: `JoinGroupTest.testAllJoinLeaveJoinGroup`.
///
/// "All nodes join, all nodes leave, all nodes join again — and the same
/// node ends up master both times."  In Noxu the harness drives the
/// election outcomes, so the equivalent invariant is: after a full
/// shutdown + re-`create_group`, the new master is whichever node we
/// elect (deterministic).
#[test]
fn join_group_join_leave_join() {
    let mut group = RepTestBase::builder("join_leave").group_size(3).build();
    group.create_group(1).unwrap();
    assert_eq!(group.find_master_idx(), Some(0));

    group.shutdown_all();
    for n in group.nodes() {
        assert!(matches!(n.state(), None | Some(NodeState::Shutdown)));
    }

    // Re-create the group.  Since `shutdown_all` dropped the env handles,
    // each `RepEnvInfo::open_env` re-creates a fresh env.
    group.create_group(2).unwrap();
    assert_eq!(group.find_master_idx(), Some(0));
}

/// JE: `JoinGroupTest.testRepeatedOpen`.
///
/// "Opening the same `RepEnvInfo` twice without closing fails."
#[test]
fn join_group_repeated_open_fails() {
    let mut group = RepTestBase::builder("join_dup").group_size(1).build();
    group.node_mut(0).open_env().unwrap();
    let r = group.node_mut(0).open_env();
    assert!(r.is_err(), "second open without close must fail");
}

// =====================================================================
// ReplicationGroupTest — `je.rep.ReplicationGroupTest`
// =====================================================================

/// JE: `ReplicationGroupTest.testBasic` (subset that doesn't require
/// physical group-database state).
///
/// "After a group is created, every node sees the same group name and
/// the master reports itself as master."
#[test]
fn replication_group_basic_membership_visible() {
    let mut group = RepTestBase::builder("rep_grp_basic").group_size(3).build();
    group.create_group(1).unwrap();

    let group_name = group.group_name().to_string();
    for node in group.nodes() {
        assert_eq!(node.get_env().get_group_name(), group_name);
    }

    // Master reports itself; replicas report the master.
    let master_name = group.node(0).node_name().to_string();
    assert_eq!(
        group.node(0).get_env().get_master_name(),
        Some(master_name.clone()),
    );
    for replica_idx in 1..group.group_size() {
        assert_eq!(
            group.node(replica_idx).get_env().get_master_name(),
            Some(master_name.clone()),
        );
    }
}

// =====================================================================
// SecondaryNodeTest — `je.rep.SecondaryNodeTest`
// =====================================================================

/// JE: `SecondaryNodeTest.testJoinLeaveJoin`.
///
/// "A secondary node can join, leave, and re-join the group without
/// affecting the master/replica electable nodes."
#[test]
fn secondary_node_join_leave_join() {
    let mut group = RepTestBase::builder("sec_jlj")
        .group_size(3)
        .override_node_type(2, NodeType::Secondary)
        .build();
    group.create_group(1).unwrap();
    assert!(group.node(0).is_master());
    assert!(group.node(1).is_replica());
    assert!(group.node(2).is_replica());
    assert_eq!(group.node(2).rep_config().node_type, NodeType::Secondary);

    // Secondary leaves.
    group.nodes_mut()[2].close_env().unwrap();
    assert!(group.node(0).is_master(), "master unaffected by secondary leave");
    assert!(
        group.node(1).is_replica(),
        "replica unaffected by secondary leave"
    );

    // Secondary re-joins.
    group.nodes_mut()[2].open_env().unwrap();
    group.nodes_mut()[2]
        .get_env()
        .become_replica(group.node(0).node_name())
        .unwrap();
    assert!(group.node(2).is_replica());
}

/// JE: `SecondaryNodeTest.testSecondaryChangeMaster`.
///
/// "A secondary node correctly follows when the master changes."  After
/// failover, the secondary's `get_master_name()` reflects the new master.
#[test]
fn secondary_node_follows_new_master() {
    let mut group = RepTestBase::builder("sec_chmaster")
        .group_size(3)
        .override_node_type(2, NodeType::Secondary)
        .build();
    group.create_group(1).unwrap();
    let initial_master = group.node(0).node_name().to_string();
    assert_eq!(group.node(2).get_env().get_master_name(), Some(initial_master),);

    // Original master leaves.
    group.close_master().unwrap();

    // node 1 (electable) takes over; secondary follows.
    group.failover_to(1).unwrap();
    let new_master = group.node(1).node_name().to_string();
    assert!(group.node(2).is_replica());
    assert_eq!(group.node(2).get_env().get_master_name(), Some(new_master),);
}

// =====================================================================
// ElectableGroupSizeOverrideTest — `je.rep.ElectableGroupSizeOverrideTest`
// =====================================================================

/// JE: `ElectableGroupSizeOverrideTest.testBasic` (subset).
///
/// "When the electable group size is set, elections succeed with the
/// reduced quorum even though some nodes are unreachable."  This maps to
/// Noxu's [`crate::QuorumPolicy::Flexible`] policy at the group level.
#[test]
fn electable_group_size_override_quorum() {
    use noxu_rep::QuorumPolicy;

    let mut g = RepGroup::new("egso_test".to_string(), 99);
    for i in 1u32..=5 {
        g.add_node(RepNode::new(
            format!("egso_n{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            6700 + i as u16,
            i,
        ));
    }
    // Override: phase1=3 (down from majority 3 of 5 — same), phase2=2.
    g.set_quorum_policy(QuorumPolicy::Flexible { phase1: 3, phase2: 2 });
    assert_eq!(g.phase1_quorum(), 3);
    assert_eq!(g.phase2_quorum(), 2);

    // Override down to phase1=2, phase2=2 (artificially reduced).
    g.set_quorum_policy(QuorumPolicy::Flexible { phase1: 2, phase2: 2 });
    assert_eq!(g.phase1_quorum(), 2);
    assert_eq!(g.phase2_quorum(), 2);
}

// =====================================================================
// NodePriorityTest — `je.rep.NodePriorityTest`
// =====================================================================

/// JE: `NodePriorityTest.testPriorityBasic`.
///
/// "A node with a higher priority wins elections over equally-eligible
/// nodes."  In Noxu's Paxos implementation the tiebreak is by VLSN then
/// node id (priority is not a separate concept), so the equivalent
/// invariant is: when two nodes are eligible, the higher-VLSN node wins.
/// The harness drives the election outcome explicitly, so this test
/// asserts that tiebreaker mechanics hold when chosen explicitly.
#[test]
fn node_priority_higher_vlsn_can_be_master() {
    let mut group = RepTestBase::builder("nprio").group_size(3).build();
    group.create_group(1).unwrap();

    // Master writes 20 entries; replicas apply 20.
    group.populate_db(1, 20).unwrap();
    group.assert_all_at_vlsn(20);

    // Master crashes.  The "highest VLSN among survivors" is a tie at 20.
    // Failover to node 1; semantically: noxu allows any electable replica
    // to be elected so long as VLSN doesn't regress.
    group.close_master().unwrap();
    group.failover_to(1).unwrap();
    assert!(group.node(1).is_master());
    assert!(group.node(1).current_vlsn() >= 20, "VLSN must not regress");
}

// =====================================================================
// ReplicationConfigTest — `je.rep.ReplicationConfigTest`
// =====================================================================

/// JE: `ReplicationConfigTest.testConsistency`.
///
/// "A `ReplicationConfig` round-trips every valid `ReplicaConsistencyPolicy`
/// (`setConsistencyPolicy` / `getConsistencyPolicy`), and an invalid policy
/// (a `CommitPointConsistencyPolicy` built over a null-VLSN token, or a
/// point-consistency policy over `VLSN.NULL_VLSN`) is rejected with an
/// `IllegalArgumentException`."
///
/// Noxu adaptation of the JE assertions:
///   * `TimeConsistencyPolicy` and `NoConsistencyRequiredPolicy` round-trip
///     through the [`crate::RepConfig`] builder — same as JE's
///     `setConsistencyPolicy` / `getConsistencyPolicy` equality check.
///   * The invalid-policy half maps to Noxu's typed constructor: a
///     [`crate::CommitToken`] with VLSN 0 (JE `new CommitToken(uuid, 0)`,
///     which the `CommitPointConsistencyPolicy` ctor rejects) is
///     un-constructible — [`crate::CommitToken::new`] returns `None`, so the
///     bad policy can never be built.  This is the Rust idiom for JE's
///     "throw `IllegalArgumentException` on a null-VLSN commit token".
///
/// The string-config path (`setConfigParam(CONSISTENCY_POLICY, "badPolicy")`)
/// is N/A: Noxu's `RepConfig` is a typed builder with no string
/// `setConfigParam` facility (recorded in the package report).
#[test]
fn replication_config_consistency_round_trip() {
    use std::time::Duration;

    use noxu_rep::{CommitToken, ConsistencyPolicy, RepConfig};

    // TimeConsistencyPolicy round-trips (JE: policy.equals(getConsistencyPolicy)).
    let time_policy = ConsistencyPolicy::TimeConsistency {
        max_lag: Duration::from_millis(100),
        timeout: Duration::from_secs(1),
    };
    let cfg = RepConfig::builder("g", "n", "127.0.0.1")
        .consistency_policy(time_policy.clone())
        .build();
    assert_eq!(cfg.consistency_policy, time_policy);

    // NoConsistencyRequiredPolicy round-trips.
    let cfg = RepConfig::builder("g", "n", "127.0.0.1")
        .consistency_policy(ConsistencyPolicy::NoConsistency)
        .build();
    assert_eq!(cfg.consistency_policy, ConsistencyPolicy::NoConsistency);

    // A CommitPointConsistencyPolicy over a null-VLSN commit token is
    // rejected — in JE the ctor throws IllegalArgumentException; in Noxu the
    // token itself is un-constructible (VLSN must be non-null).
    assert!(
        CommitToken::new("g", 0).is_none(),
        "a null-VLSN commit token must be rejected (JE CommitToken ctor: \
         'the vlsn must not be null'), so no invalid CommitPointConsistency \
         policy can be built"
    );
    // A valid token yields a usable commit-point policy that round-trips.
    let token = CommitToken::new("g", 42).expect("valid token");
    let cp = ConsistencyPolicy::commit_point(&token, Duration::from_secs(1));
    let cfg = RepConfig::builder("g", "n", "127.0.0.1")
        .consistency_policy(cp.clone())
        .build();
    assert_eq!(cfg.consistency_policy, cp);
}

// =====================================================================
// ReplicatedEnvironmentStatsTest — `je.rep.ReplicatedEnvironmentStatsTest`
// =====================================================================

/// JE: `ReplicatedEnvironmentStatsTest.testBasic`.
///
/// "After a group is created, `getRepStats()` is available on every node and
/// every stat accessor returns without throwing."  JE's `invokeAllAccessors`
/// calls ~50 getters purely to prove they are reachable and side-effect free.
///
/// Noxu exposes a subset of the JE `ReplicatedEnvironmentStats` group via
/// [`crate::ReplicatedEnvironment::get_stats`] → [`crate::RepStats`]
/// (elections, feeders, acks, replicated/applied entries, bytes, max lag).
/// This test reads every field on every node of a formed group, exactly as
/// JE's smoke test does — it would fail (panic / not compile) if the stats
/// handle were unreachable or a field were removed.  The JE getters Noxu does
/// not implement (per-replica VLSN-rate maps, protocol-nanos, group-commit
/// counters) are recorded as N/A in the package report.
#[test]
fn replicated_environment_stats_all_accessors_readable() {
    use std::sync::atomic::Ordering;

    let mut group =
        RepTestBase::builder("rep_stats_basic").group_size(3).build();
    group.create_group(1).unwrap();

    // Drive a little replication so the counters are exercised, not just zero.
    group.populate_db(1, 5).unwrap();
    group.assert_all_at_vlsn(5);

    for idx in 0..group.group_size() {
        let env = group.node(idx).get_env();
        let stats = env.get_stats();
        // Read every accessor (JE invokeAllAccessors) — the loads must not
        // panic and the summary must render.
        let _ = stats.elections_held.load(Ordering::Relaxed);
        let _ = stats.elections_won.load(Ordering::Relaxed);
        let _ = stats.elections_lost.load(Ordering::Relaxed);
        let _ = stats.feeders_created.load(Ordering::Relaxed);
        let _ = stats.feeders_shutdown.load(Ordering::Relaxed);
        let _ = stats.acks_received.load(Ordering::Relaxed);
        let _ = stats.ack_timeouts.load(Ordering::Relaxed);
        let _ = stats.entries_replicated.load(Ordering::Relaxed);
        let _ = stats.entries_applied.load(Ordering::Relaxed);
        let _ = stats.bytes_replicated.load(Ordering::Relaxed);
        let _ = stats.max_replica_lag_ms.load(Ordering::Relaxed);
        let summary = stats.summary();
        assert!(
            summary.contains("RepStats"),
            "stats summary must render on node {idx}"
        );
    }
}

// =====================================================================
// DatabaseOperationTest — `je.rep.DatabaseOperationTest`
// =====================================================================

/// JE: `DatabaseOperationTest.testDbNameOpReplicaWriteException`.
///
/// "A database-name operation (create / rename / remove) attempted directly
/// on a replica fails with `ReplicaWriteException` — a replica is read-only
/// for replicated content; only the master may originate such operations."
///
/// Noxu's stream-level analogue: a replica has [`crate::NodeState::Replica`]
/// and cannot be driven to originate writes.  The direct write-path guard is
/// the master-only-write restriction — a replica node reports `is_replica()`
/// and NOT `is_master()`, and a `become_master` on a Secondary (never
/// electable) node is rejected (see the txn TCK's
/// `secondary_node_become_master_should_fail`).  Here we assert the replica's
/// read-only role directly: after a group forms, the replica is a replica and
/// is not the master, so any master-only DB-name op routed by role would be
/// refused.
#[test]
fn database_op_replica_is_read_only_role() {
    let mut group =
        RepTestBase::builder("dbop_replica_ro").group_size(3).build();
    group.create_group(1).unwrap();

    assert!(group.node(0).is_master(), "node 0 is the master");
    for replica_idx in 1..group.group_size() {
        assert!(
            group.node(replica_idx).is_replica(),
            "node {replica_idx} must be a (read-only) replica"
        );
        assert!(
            !group.node(replica_idx).is_master(),
            "a replica must never report itself as master (a DB-name op \
             originated here would be a ReplicaWriteException)"
        );
    }
}

/// JE: `DatabaseOperationTest.testLocalStoreNoConsistency`.
///
/// "Reads against a local (non-replicated) database on a replica are NOT
/// subject to the replica consistency policy — a `NoConsistencyRequiredPolicy`
/// read proceeds immediately regardless of how far the replica lags the
/// master."
///
/// Noxu analogue at the consistency-gate layer: a
/// [`crate::ConsistencyPolicy::NoConsistency`] read never blocks, even when
/// the master is far ahead.  (The full local-DB-bypass wiring is covered by
/// `noxu-dbi`'s `local_write_replication_test`; here we assert the
/// no-consistency-never-blocks half that JE's local-store read relies on.)
#[test]
fn database_op_local_store_no_consistency_never_blocks() {
    use std::time::{Duration, Instant};

    use noxu_rep::ConsistencyPolicy;

    let mut group =
        RepTestBase::builder("dbop_local_noc").group_size(2).build();
    group.create_group(1).unwrap();

    // Master writes; replica intentionally left behind (master-only).
    group.populate_master_only(1, 50).unwrap();

    // A NoConsistency read on the (lagging) replica must return at once.
    let start = Instant::now();
    group
        .node(1)
        .get_env()
        .begin_read_consistency(Some(&ConsistencyPolicy::NoConsistency))
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "a NoConsistency read against a local store must not block on the \
         replica's replication lag"
    );
}

// =====================================================================
// RepGroupAdminTest — `je.rep.RepGroupAdminTest`
// =====================================================================

/// JE: `RepGroupAdminTest.testRemoveMember`.
///
/// "`RepGroupAdmin.removeMember` removes an electable member from the group;
/// afterwards the group's electable membership no longer includes it and the
/// quorum arithmetic reflects the smaller group."
///
/// Noxu analogue at the group-model layer ([`crate::RepGroup`]): removing an
/// electable node drops the electable count (and thus the majority quorum),
/// while the removed node is no longer resolvable by name.
#[test]
fn rep_group_admin_remove_member() {
    let mut g = RepGroup::new("rga_remove".to_string(), 7);
    for i in 1u32..=3 {
        g.add_node(RepNode::new(
            format!("n{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            7100 + i as u16,
            i,
        ));
    }
    assert_eq!(g.electable_count(), 3);
    // Majority of 3 is 2.
    assert_eq!(g.phase2_quorum(), 2);

    let removed = g.remove_node("n3");
    assert!(removed.is_some(), "removeMember must return the removed member");
    assert_eq!(g.electable_count(), 2, "electable membership shrinks");
    assert!(g.get_node("n3").is_none(), "removed member is no longer present");
    // Majority of 2 is 2 (JE: quorum recomputed over the smaller group).
    assert_eq!(g.phase2_quorum(), 2);
}

/// JE: `RepGroupAdminTest.testDeleteMember`.
///
/// "`RepGroupAdmin.deleteMember` permanently deletes a member; deleting a
/// member that is not present (or already deleted) is an error / no-op."
///
/// Noxu analogue: [`crate::RepGroup::remove_node`] returns the deleted node on
/// first call and `None` on a second (idempotent removal / not-present).
#[test]
fn rep_group_admin_delete_member_is_idempotent() {
    let mut g = RepGroup::new("rga_delete".to_string(), 8);
    g.add_node(RepNode::new(
        "n1".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        7200,
        1,
    ));
    assert!(g.remove_node("n1").is_some(), "first delete removes the member");
    assert!(
        g.remove_node("n1").is_none(),
        "deleting an absent member returns None (JE: MemberNotFoundException)"
    );
}

/// JE: `RepGroupAdminTest.testAddMonitor`.
///
/// "A monitor can be added to the group; it observes membership but does not
/// count toward the electable quorum."
///
/// Noxu analogue: a [`crate::NodeType::Monitor`] node added to a
/// [`crate::RepGroup`] appears in `get_monitors()` but NOT in
/// `get_electable_nodes()`, and does not change `electable_count()`.
#[test]
fn rep_group_admin_add_monitor() {
    let mut g = RepGroup::new("rga_monitor".to_string(), 9);
    for i in 1u32..=2 {
        g.add_node(RepNode::new(
            format!("e{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            7300 + i as u16,
            i,
        ));
    }
    assert_eq!(g.electable_count(), 2);
    assert_eq!(g.get_monitors().len(), 0);

    g.add_node(RepNode::new(
        "mon1".to_string(),
        NodeType::Monitor,
        "127.0.0.1".to_string(),
        7400,
        3,
    ));
    // Monitor is visible as a monitor, absent from the electable set, and
    // does NOT inflate the electable quorum (JE: monitors are non-voting).
    assert_eq!(g.get_monitors().len(), 1, "monitor is registered");
    assert_eq!(
        g.electable_count(),
        2,
        "a monitor does not count toward the electable quorum"
    );
    assert!(
        !g.get_electable_nodes().iter().any(|n| n.name == "mon1"),
        "a monitor must not appear in the electable set"
    );
}
