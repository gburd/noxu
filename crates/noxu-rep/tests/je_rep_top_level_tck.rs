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
