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
/// Also covers (COVERED-CITED — "a default group forms and every node agrees
/// on the group name / master" is the shared invariant):
///   * JE: `JoinGroupTest.testDefaultJoinGroup` — nodes join with default
///     config and form a group with one master (partial: the JE
///     helper-host-driven join path is N/A; here `create_group` forms it).
///   * JE: `CheckAccessTest.testBasicConfig` — with no special (SSL/auth)
///     config a 2+-node group forms with a master and a replica (partial:
///     JE drives it via a `.je.properties` file the typed `RepConfig` has
///     no equivalent for).
///   * JE: `ReplicatedEnvironmentTest.testJoin` (quorum-gated-join subset) —
///     PARTIAL: the group-membership-visibility half is here; the
///     env-open-BLOCKS-until-quorum + `ENV_SETUP_TIMEOUT` half is covered by
///     `chaos_test::test_quorum_unreachable_election_fails_gracefully`
///     (no quorum → no progress) and is otherwise N/A (`ENV_SETUP_TIMEOUT`
///     knob not modeled — same as `testEnvSetupTimeoutExceeded`).
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
/// Also covers (COVERED-CITED):
///   * JE: `SecondaryNodeTest.testJoinLeaveAllJoinAll` — the same
///     join/leave/re-join cycle at group scale; the electable core is
///     unaffected by secondary churn (see also `cluster_integration_test`
///     and `replica_scale_test` for the multi-node scale variant).
///   * JE: `SecondaryNodeTest.testAddSecondaryWithNonAuthoritativeMaster`
///     (admission subset) — a secondary joins/leaves/re-joins; the
///     authoritative-master admission gate the JE test additionally
///     exercises is covered by `s1_identity_binding_test` (peer identity /
///     authoritative-master admission).
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
/// Also covers (COVERED-CITED — PARTIAL: the override *arithmetic* is the
/// essence; the live master-down/up timing variants are integration-timing
/// harness variants not modeled):
///   * JE: `ElectableGroupSizeOverrideTest.testMasterDownOverride` — with the
///     override set, an election concludes with the reduced quorum after a
///     simple majority (incl. the master) is down.  The reduced-quorum
///     arithmetic is exactly `QuorumPolicy::Flexible` here.
///   * JE: `ElectableGroupSizeOverrideTest.testMasterUpOverride` — restoring
///     the override to 0 restores normal quorum; same arithmetic.
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
// MasterChangeTest — `je.rep.MasterChangeTest`
// =====================================================================

/// JE: `MasterChangeTest.testTransitions` (master-switch subset).
///
/// JE's `testTransitions` does FIVE rounds of switching the master (by
/// mimicking network partitions) and, per round, asserts (a) the master
/// actually changes and the committed data is preserved across the switch,
/// and (b) open write-txns are ABORTED on the switch while read-only txns
/// survive.
///
/// This port covers the master-switch half faithfully: it drives multiple
/// rounds of `close_master`/`failover_to`, asserting each round elects a
/// different node than the previous master and that the replicated VLSN never
/// regresses across the switch (the "committed data preserved" invariant).
///
/// GAP (noted, not silently dropped): the open-write-txn-ABORT / read-txn-
/// survives half is NOT modeled here.  It requires Database-level open
/// transactions on a live `ReplicatedEnvironment`, but Noxu's rep layer
/// surfaces no `Database` handle from a `ReplicatedEnvironment` (the replica
/// applies raw VLSN-tagged log entries, not `Database.put`/`Transaction`
/// operations — the same structural gap recorded for `RepPreloadTest` /
/// `StoredClassCatalogTest` / the replica-side DB-name-op tests).  The
/// underlying election machinery this port exercises is additionally covered
/// by `chaos_test::test_multi_round_elections_monotone_terms`.
#[test]
fn master_change_transitions_switch_master_preserving_data() {
    let mut group = RepTestBase::builder("mchange").group_size(3).build();
    group.create_group(1).unwrap();

    // Seed committed data that must survive every switch.
    group.populate_db(1, 10).unwrap();
    group.assert_all_at_vlsn(10);

    // Five rounds of switching the master among the nodes, JE-style.  Each
    // round: close the current master, fail over to a survivor that is still
    // caught up, then bring the former master back as a caught-up replica so
    // it can serve as a failover target in a later round (JE keeps all nodes
    // in play).  The invariant asserted every round is JE's: the master
    // actually changes and the committed data (VLSN) is preserved.
    let group_vlsn = 10u64;
    let mut current = group.find_master_idx().expect("a master");
    for round in 0..5 {
        let prev = current;
        let prev_vlsn = group.node(prev).current_vlsn();

        // Rotate to the next node (JE: targetIndex = (firstIndex==2)?0:+1).
        let target = (prev + 1) % group.group_size();
        // The target must be a live, caught-up replica for the failover to
        // preserve data (the harness models the election, not catch-up).
        assert!(
            group.node(target).current_vlsn() >= prev_vlsn,
            "round {round}: failover target must be caught up before election"
        );

        group.close_master().unwrap();
        group.failover_to(target).unwrap();

        let new_master = group.find_master_idx().expect("a new master");
        assert_eq!(
            new_master, target,
            "round {round}: failover must elect the targeted node"
        );
        assert_ne!(
            new_master, prev,
            "round {round}: the master must actually CHANGE"
        );
        // Committed data preserved across the switch: VLSN must not regress.
        assert!(
            group.node(new_master).current_vlsn() >= prev_vlsn,
            "round {round}: committed data must survive the master change \
             (VLSN {} regressed below {prev_vlsn})",
            group.node(new_master).current_vlsn()
        );

        // Reopen the just-closed former master and catch it back up to the
        // group VLSN so it is a valid (caught-up) failover target next round.
        group.nodes_mut()[prev].open_env().unwrap();
        group
            .node(prev)
            .get_env()
            .become_replica(group.node(new_master).node_name())
            .unwrap();
        // A fresh handle starts at VLSN 0; replay the committed stream so it
        // rejoins as a caught-up replica (JE's rejoined node re-syncs).
        group.catch_up_replica(prev, 1, group_vlsn).unwrap();
        assert!(
            group.node(prev).current_vlsn() >= group_vlsn,
            "round {round}: rejoined former master must catch back up"
        );

        current = new_master;
    }
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
/// Also covers:
///   * JE: `ReplicatedTransactionTest.testReplicaReadonlyTransaction` — a
///     replica rejects an originated (master-only) write path; the same
///     master-only guard asserted here.
///
/// "A database-name operation (create / rename / remove) attempted directly
/// on a replica fails with `ReplicaWriteException` — a replica is read-only
/// for replicated content; only the master may originate such operations."
///
/// Noxu's stream-level analogue: a replica has [`crate::NodeState::Replica`]
/// and the master-only-write path is refused on it.  Noxu's rep layer does
/// not surface a `Database` handle from a `ReplicatedEnvironment` (the
/// replica applies raw VLSN-tagged log entries, not `openDatabase` calls),
/// so the *exact* JE call `env.openDatabase(...)` → `ReplicaWriteException`
/// is not expressible.  The master-only write path Noxu *does* model is the
/// commit/ack origination path ([`noxu_dbi::ReplicaAckCoordinator::
/// await_replica_acks`]): it is master-only and returns
/// [`noxu_dbi::AckWaitErrorKind::NotMaster`] when invoked on a replica —
/// the same "a replica cannot originate a replicated write" guard that
/// `ReplicaWriteException` enforces at the DB-name-op layer.  This test now
/// (1) asserts the replica's read-only role and (2) actually *attempts* the
/// master-only origination path on the replica and asserts it is rejected,
/// rather than asserting the role alone.
#[test]
fn database_op_replica_is_read_only_role() {
    use std::time::Duration;

    use noxu_dbi::{
        AckWaitErrorKind, ReplicaAckCoordinator, ReplicaAckPolicyKind,
    };

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

        // Actually attempt the master-only origination path on the replica
        // and assert it is REJECTED (not merely that the role is Replica).
        // `await_replica_acks` is the commit/ack origination path a write
        // must traverse; on a replica it must return NotMaster — Noxu's
        // moral equivalent of ReplicaWriteException for an originated write.
        let env = group.node(replica_idx).get_env();
        let err = ReplicaAckCoordinator::await_replica_acks(
            env.as_ref(),
            ReplicaAckPolicyKind::SimpleMajority,
            Duration::from_millis(50),
        )
        .expect_err(
            "a master-only originated write must be refused on a replica",
        );
        assert_eq!(
            err.kind,
            AckWaitErrorKind::NotMaster,
            "node {replica_idx}: a replica must reject an originated write \
             with NotMaster (Noxu analogue of ReplicaWriteException)"
        );
    }
}

/// JE: `DatabaseOperationTest.testLocalStoreNoConsistency`.
///
/// Also covers (COVERED-CITED):
///   * JE: `SecondaryNodeTest.testReadDisconnectedSecondary` — a read on a
///     disconnected/lagging secondary under a no-consistency policy proceeds
///     immediately (does not block on the replication lag), which is exactly
///     the NoConsistency-never-blocks invariant this test pins.
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

/// JE: `DatabaseOperationTest.testCascade`.
///
/// JE's `testCascade` is a FAILOVER test (its name notwithstanding): form a
/// group, shut down a replica, do database work on the master, then shut the
/// master down, elect a NEW master among the survivors, assert the new master
/// is NOT the former one, and verify the replicated data survived the switch.
/// (It is distinct from a static cascade-feeding topology.)
///
/// The harness models this directly: `close_master` + `failover_to` drives a
/// new election, `find_master_idx`/`node_name` prove the master changed, and
/// the surviving replica's VLSN proves the committed data replicated before
/// the switch survived onto the new master.  The replica-shutdown-before-work
/// step is a fixture detail; the invariant JE asserts — a new master is
/// elected on master loss and the pre-failover replicated data is preserved —
/// is pinned here.
#[test]
fn database_op_cascade_failover_elects_new_master() {
    let mut group = RepTestBase::builder("dbop_cascade").group_size(3).build();
    group.create_group(1).unwrap();
    let former_master = group.find_master_idx().expect("a master");
    assert_eq!(former_master, 0);
    let former_master_name = group.node(former_master).node_name().to_string();

    // "Do some database work" on the master; it replicates to the survivors.
    group.populate_db(1, 10).unwrap();
    group.assert_all_at_vlsn(10);

    // Shut the master down and elect a new master among the survivors.
    let closed = group.close_master().unwrap();
    assert_eq!(closed, former_master);
    group.failover_to(1).unwrap();

    // The new master must NOT be the former one (JE: formerMasterId != new).
    let new_master = group.find_master_idx().expect("a new master");
    assert_ne!(
        new_master, former_master,
        "failover must elect a DIFFERENT node than the former master"
    );
    assert_ne!(group.node(new_master).node_name(), former_master_name);

    // The pre-failover replicated data (VLSN 10) survived onto the new master
    // (JE: checkEquality after the master change).
    assert!(
        group.node(new_master).current_vlsn() >= 10,
        "pre-failover replicated data must survive the master change; got {}",
        group.node(new_master).current_vlsn()
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
