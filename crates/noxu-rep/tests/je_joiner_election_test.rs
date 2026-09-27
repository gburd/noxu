//! Port of the core safety invariant of
//! `je/test/com/sleepycat/je/rep/elections/JoinerElectionTest.testPartialGroupDB`
//! (JE bug #21915).
//!
//! JE scenario: a joining node (Node3) whose copy of the group DB was only
//! PARTIALLY applied (it crashed mid-syncup, via injected test hooks) is then
//! restarted WITHOUT being able to reach the master. The node must throw
//! `UnknownMasterException` and, crucially, must NOT start an election
//! (`assertEquals(0, repNode.getElections().getElectionCount())`).
//!
//! The safety invariant #21915 pins: a node that has NOT established a
//! complete, quorum-capable view of the group — because it is isolated /
//! joining with partial state — must not run an election that could elect a
//! wrong master (or elect itself off a partial membership view). It waits for
//! the master instead.
//!
//! DEVIATION (JE-internal test scaffolding): JE reproduces the *partial group
//! DB* precondition with `DbEnvPool.setBeforeFinishInitHook` +
//! `Replica.setInitialReplayHook` (a `HalfBacklogSink` that dies after 100
//! commits) and asserts via `getElectionCount()`. Those are JE-internal
//! syncup/replay test hooks and a per-Elections stat accessor with no Noxu
//! equivalent. The portable INVARIANT — an isolated joiner that cannot reach a
//! quorum does not start/win an election — is asserted here directly against
//! `run_election`, which already refuses when (a) the proposer is not a known
//! group member (closed-world guard) or (b) it cannot reach a phase-1 quorum.

use noxu_rep::elections::paxos::run_election;
use noxu_rep::node_type::NodeType;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;

/// A full 3-node group as the joiner *believes* it should be.
fn full_group() -> RepGroup {
    let mut g = RepGroup::new("joiner_group".into(), 1);
    for i in 1..=3 {
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

/// #21915 core: an isolated joiner (no reachable peers) in a multi-node group
/// must NOT elect itself master — quorum (2 of 3) cannot be reached with a
/// lone self-vote, so the node stays master-less (JE: UnknownMasterException,
/// electionCount == 0).
#[test]
fn isolated_joiner_does_not_elect_itself() {
    let group = full_group();
    // n3 is a member, but it can reach NO peers (isolated join). Highest
    // possible VLSN must not let it self-elect: a lone vote is not a quorum.
    let winner = run_election(3, "n3", &group, &[], u64::MAX, 1, 1);
    assert!(
        winner.is_none(),
        "an isolated joiner must not elect itself master with a partial \
         group view (#21915)"
    );
}

/// #21915 companion: a node whose membership view does NOT include itself as a
/// known member (a *partial* group DB, mid-syncup) must refuse to run an
/// election at all — it cannot make a sound quorum decision on an incomplete
/// view. `run_election`'s closed-world guard refuses an unknown proposer.
#[test]
fn joiner_with_partial_group_view_refuses_election() {
    // The joiner's local group DB was only partially applied: it does NOT yet
    // contain its own membership record (JE's HalfBacklogSink dies before
    // Node3's addition is replayed). Model that as a group that does not know
    // "n3".
    let mut partial = RepGroup::new("joiner_group".into(), 1);
    partial.add_node(RepNode::new(
        "n1".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        5001,
        1,
    ));
    partial.add_node(RepNode::new(
        "n2".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        5002,
        2,
    ));
    // "n3" is absent from the partial view.

    let winner = run_election(3, "n3", &partial, &[], u64::MAX, 1, 1);
    assert!(
        winner.is_none(),
        "a joiner not present in its own (partial) group view must refuse to \
         start an election (#21915 closed-world guard)"
    );
}
