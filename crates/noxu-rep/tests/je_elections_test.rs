//! Faithful port of
//! `je/test/com/sleepycat/je/rep/elections/ElectionsTest.java` (the full
//! election flow: propose / accept / learn, master selection, quorum).
//!
//! JE's `ElectionsTest` runs real `Elections` nodes over `ServiceDispatcher`
//! sockets and asserts, via `getStats()`, PROMISE_COUNT / PHASE1_NO_QUORUM /
//! PHASE1_NO_NON_ZERO_PRIO plus listener notifications. Noxu's `run_election`
//! /`run_acceptor` are the same two-phase Paxos over channels, but expose the
//! *outcome* (`Option<node_id>`), not JE's stat accessors. These ports assert
//! the observable behaviour each JE stat is a proxy for:
//!
//!   * a successful election concludes with a specific master (listener
//!     notification, winningValue),
//!   * a lack of quorum yields NO master (PHASE1_NO_QUORUM / listenerLatch
//!     untouched),
//!   * an all-zero-priority survivor set yields NO master
//!     (PHASE1_NO_NON_ZERO_PRIO),
//!   * fewer acceptors reduce the promises collected (PROMISE_COUNT drops) but
//!     the election still concludes while quorum holds.
//!
//! DEVIATION (stat accessors): JE's `Elections.getStats().getInt(PROMISE_COUNT
//! / PHASE1_NO_QUORUM / PHASE1_NO_NON_ZERO_PRIO)` are per-round counters on the
//! live Elections object. Noxu's `run_election` returns only the outcome; the
//! stat-accessor surface is not ported (a diagnostic-only API, JE-internal).
//! The behaviour each stat pins IS asserted here through the election outcome.

use std::sync::Arc;

use noxu_rep::QuorumPolicy;
use noxu_rep::elections::paxos::{run_acceptor, run_election};
use noxu_rep::net::{Channel, LocalChannelPair};
use noxu_rep::node_type::NodeType;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;

/// Build an electable `n`-node group (node1..nodeN) with the given quorum
/// policy. Priorities are all 1 unless overridden by the caller via
/// `set_priority`.
fn group_n(n: u32, policy: QuorumPolicy) -> RepGroup {
    let mut g = RepGroup::with_policy("TEST_GROUP".into(), 1, policy);
    for i in 1..=n {
        g.add_node(RepNode::new(
            format!("n{i}"),
            NodeType::Electable,
            "127.0.0.1".into(),
            5000 + i as u16,
            i,
        ));
    }
    g
}

/// Spawn `count` acceptor threads, each on the B side of a fresh channel pair.
/// The returned proposer-side channels are what `run_election` broadcasts to.
/// Each acceptor reports `(vlsn, priority)` as its own suggestion.
fn spawn_peers(
    specs: &[(&str, u64, u32)], // (name, vlsn, priority)
) -> (Vec<Arc<dyn Channel>>, Vec<std::thread::JoinHandle<Option<String>>>) {
    let mut chans: Vec<Arc<dyn Channel>> = Vec::new();
    let mut handles = Vec::new();
    for (name, vlsn, prio) in specs {
        let pair = LocalChannelPair::new();
        let ch_a: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let ch_b: Arc<dyn Channel> = Arc::new(pair.channel_b);
        chans.push(ch_a);
        let name = name.to_string();
        let (v, p) = (*vlsn, *prio);
        handles.push(std::thread::spawn(move || {
            run_acceptor(&*ch_b, &name, v, p, 1).unwrap_or(None)
        }));
    }
    (chans, handles)
}

fn join_all(handles: Vec<std::thread::JoinHandle<Option<String>>>) -> usize {
    // Count acceptors that granted the Phase-2 accept (JE PROMISE_COUNT proxy:
    // acceptors that participated through to the accept).
    handles.into_iter().filter_map(|h| h.join().unwrap()).count()
}

// --------------------------------------------------------------------------
// JE: ElectionsTest.testBasicAllNodes
// A basic election with everything normal: a master is elected; a re-run
// elects the same master (no change).
// --------------------------------------------------------------------------
#[test]
fn test_basic_all_nodes() {
    let group = group_n(3, QuorumPolicy::SimpleMajority);
    // Two peers respond (n2, n3); n1 proposes. All equal priority; n1 has the
    // highest VLSN so it wins deterministically.
    let (chans, handles) = spawn_peers(&[("n2", 10, 1), ("n3", 10, 1)]);
    let winner = run_election(1, "n1", &group, &chans, 100, 1, 1);
    join_all(handles);
    assert_eq!(winner, Some(1), "a normal 3-node election must elect a master");

    // Re-run: same inputs -> same master (JE: master unchanged, monitor not
    // invoked).
    let (chans2, handles2) = spawn_peers(&[("n2", 10, 1), ("n3", 10, 1)]);
    let winner2 = run_election(1, "n1", &group, &chans2, 100, 1, 2);
    join_all(handles2);
    assert_eq!(winner2, Some(1), "re-run must elect the same master");
}

// --------------------------------------------------------------------------
// JE: ElectionsTest.testBasicAllPrioNodes
// With testPriority, priority = groupSize - nodeNum + 1, so n1 has the HIGHEST
// priority; winningValue = n1. Pins that priority (not just VLSN) steers the
// outcome.
// --------------------------------------------------------------------------
#[test]
fn test_basic_all_prio_nodes() {
    let group = group_n(3, QuorumPolicy::SimpleMajority);
    // n1 highest priority (3), n2 (2), n3 (1); ALL equal VLSN so priority
    // breaks the tie. n1 must win.
    let (chans, handles) = spawn_peers(&[("n2", 100, 2), ("n3", 100, 1)]);
    let winner = run_election(1, "n1", &group, &chans, 100, 3, 1);
    join_all(handles);
    assert_eq!(
        winner,
        Some(1),
        "highest-priority node (n1) must win on equal VLSN (testPriority)"
    );
}

// --------------------------------------------------------------------------
// JE: ElectionsTest.testBasicAllButOneNode
// One node never came up, but enough for a quorum: election still concludes.
// (3-node group, 2 present -> quorum 2 met.)
// --------------------------------------------------------------------------
#[test]
fn test_basic_all_but_one_node() {
    let group = group_n(3, QuorumPolicy::SimpleMajority);
    // Only ONE peer present (n2); n1 self + n2 promise = 2 = quorum.
    let (chans, handles) = spawn_peers(&[("n2", 10, 1)]);
    let winner = run_election(1, "n1", &group, &chans, 100, 1, 1);
    join_all(handles);
    assert_eq!(
        winner,
        Some(1),
        "election must conclude with a quorum even if one node is absent"
    );
}

// --------------------------------------------------------------------------
// JE: ElectionsTest.testBasicOneNodeCrash
// All nodes -> PROMISE_COUNT == nodes; crash one acceptor; re-run ->
// PROMISE_COUNT == nodes-1 but the election STILL concludes (quorum holds).
// --------------------------------------------------------------------------
#[test]
fn test_basic_one_node_crash() {
    let group = group_n(3, QuorumPolicy::SimpleMajority);

    // Round 1: both peers up -> 2 acceptors accept (PROMISE_COUNT proxy = 2).
    let (chans, handles) = spawn_peers(&[("n2", 10, 1), ("n3", 10, 1)]);
    let winner = run_election(1, "n1", &group, &chans, 100, 1, 1);
    let accepted_round1 = join_all(handles);
    assert_eq!(winner, Some(1));
    assert_eq!(accepted_round1, 2, "both peers accept when all are up");

    // Round 2: one acceptor "crashed" (only n2 present). Quorum (2) still met
    // via self + n2. Election concludes; fewer promises collected.
    let (chans2, handles2) = spawn_peers(&[("n2", 10, 1)]);
    let winner2 = run_election(1, "n1", &group, &chans2, 100, 1, 2);
    let accepted_round2 = join_all(handles2);
    assert_eq!(winner2, Some(1), "election still concludes after a crash");
    assert!(
        accepted_round2 < accepted_round1,
        "PROMISE_COUNT must drop after a node crash (got {accepted_round2} \
         vs {accepted_round1})"
    );
}

// --------------------------------------------------------------------------
// JE: ElectionsTest.testBasicZeroPrio
// A simple majority with a mix of zero- and non-zero-priority nodes still
// elects a (non-zero) master; once ALL non-zero-priority nodes are gone the
// election gives up (PHASE1_NO_NON_ZERO_PRIO).
// --------------------------------------------------------------------------
#[test]
fn test_basic_zero_prio() {
    let group = group_n(3, QuorumPolicy::SimpleMajority);

    // Mix: n1 (proposer, prio 1) + two zero-prio peers. n1 is the only
    // master-eligible node; it wins with the self+2 quorum.
    let (chans, handles) = spawn_peers(&[("n2", 200, 0), ("n3", 300, 0)]);
    let winner = run_election(1, "n1", &group, &chans, 100, 1, 1);
    join_all(handles);
    assert_eq!(
        winner,
        Some(1),
        "a non-zero-priority node wins even amid higher-VLSN zero-prio peers"
    );

    // Now the ONLY non-zero node would itself be zero-prio: no candidate can
    // be master (PHASE1_NO_NON_ZERO_PRIO). Proposer prio 0 refuses outright.
    let (chans2, handles2) = spawn_peers(&[("n2", 200, 0), ("n3", 300, 0)]);
    let winner2 = run_election(1, "n1", &group, &chans2, 100, 0, 2);
    join_all(handles2);
    assert!(
        winner2.is_none(),
        "an all-zero-priority survivor set must elect no master"
    );
}

// --------------------------------------------------------------------------
// JE: ElectionsTest.testQuorumPolicyAll
// QuorumPolicy.ALL: all nodes must respond. All present -> concludes. Remove
// one -> the election gives up (PHASE1_NO_QUORUM).
// --------------------------------------------------------------------------
#[test]
fn test_quorum_policy_all() {
    // ALL modelled as Flexible{phase1 = n, phase2 = 1}: every node must
    // promise in Phase 1 (phase1+phase2 = 4 > 3, a valid quorum system).
    let group = group_n(3, QuorumPolicy::Flexible { phase1: 3, phase2: 1 });

    // All present: self + 2 peers = 3 promises = phase1 quorum.
    let (chans, handles) = spawn_peers(&[("n2", 10, 1), ("n3", 10, 1)]);
    let winner = run_election(1, "n1", &group, &chans, 100, 1, 1);
    join_all(handles);
    assert_eq!(winner, Some(1), "QuorumPolicy.ALL with all present concludes");

    // Remove one: only self + 1 = 2 < phase1 quorum(3) -> no election.
    let (chans2, handles2) = spawn_peers(&[("n2", 10, 1)]);
    let winner2 = run_election(1, "n1", &group, &chans2, 100, 1, 2);
    join_all(handles2);
    assert!(
        winner2.is_none(),
        "QuorumPolicy.ALL must give up when any node is absent \
         (PHASE1_NO_QUORUM)"
    );
}

// --------------------------------------------------------------------------
// JE: ElectionsTest.testNoQuorum
// Only nodes/2 active in a group of `nodes`: no quorum -> no master, no
// listener notification (PHASE1_NO_QUORUM).
// --------------------------------------------------------------------------
#[test]
fn test_no_quorum() {
    // 4-node group, only 1 peer present (self + 1 = 2 < majority 3).
    let group = group_n(4, QuorumPolicy::SimpleMajority);
    let (chans, handles) = spawn_peers(&[("n2", 10, 1)]);
    let winner = run_election(1, "n1", &group, &chans, 100, 1, 1);
    join_all(handles);
    assert!(
        winner.is_none(),
        "fewer than a majority of active nodes must elect no master \
         (PHASE1_NO_QUORUM)"
    );
}
