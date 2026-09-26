//! C5/V14: operator-configurable election priority (`NODE_PRIORITY`).
//!
//! JE exposes `NODE_PRIORITY` as a mutable per-node rep parameter
//! (`ReplicationMutableConfig.java:165`, `RepParams.NODE_PRIORITY`) so an
//! operator can steer mastership:
//!
//!  * a **higher** priority node is preferred as master when election
//!    progress (DTVLSN / VLSN) is otherwise equal, and
//!  * priority **0** means "electable / participates in quorum but is
//!    **never chosen** as master" — the same not-master-eligible semantics
//!    JE already applies to arbiters (which Noxu models with `priority == 0`
//!    in the ranking layer, see `je_ranking_proposer_test.rs`).
//!
//! Before the C5 fix, `RepConfig` had no `node_priority` field and the live
//! election driver hard-coded `/* priority */ 1`
//! (`replicated_environment.rs:1300`), so `NODE_PRIORITY` had no effect and a
//! node could not be made not-master-eligible via priority 0. These tests
//! FAIL on base `72f60240`:
//!  * the config-plumbing tests do not compile (no `node_priority` field), and
//!  * `test_priority_zero_node_refuses_to_propose_itself` fails because the
//!    base `run_election` lets a priority-0 self-proposer win a self-quorum.

use noxu_rep::RepConfig;
use noxu_rep::elections::Proposal;
use noxu_rep::elections::paxos::run_election;
use noxu_rep::node_type::NodeType;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;

// ---------------------------------------------------------------------------
// Config plumbing: NODE_PRIORITY is a real RepConfig field with JE's default.
// ---------------------------------------------------------------------------

/// JE `RepParams.NODE_PRIORITY` default is `1` (an ordinary electable node).
#[test]
fn node_priority_default_is_one() {
    let config = RepConfig::builder("g", "n", "h").build();
    assert_eq!(
        config.node_priority, 1,
        "JE RepParams.NODE_PRIORITY default is 1"
    );
}

/// The builder sets an operator-chosen priority (steer mastership).
#[test]
fn node_priority_builder_sets_it() {
    let high = RepConfig::builder("g", "n", "h").node_priority(10).build();
    assert_eq!(high.node_priority, 10);
    let zero = RepConfig::builder("g", "n", "h").node_priority(0).build();
    assert_eq!(zero.node_priority, 0, "priority 0 = not master-eligible");
}

// ---------------------------------------------------------------------------
// Ranking: a higher-priority node is preferred when progress is equal.
// (This is the operator lever the driver must actually feed the config into.)
// ---------------------------------------------------------------------------

/// Two candidates with identical DTVLSN/VLSN/term: the higher `node_priority`
/// wins. This is the JE Ranking tiebreaker the driver must feed the config
/// value into rather than a constant.
#[test]
fn higher_priority_preferred_on_equal_progress() {
    // Same dtvlsn (0 -> falls back to vlsn), same vlsn, same term.
    let hi = Proposal::with_timestamp("hi".into(), 100, 5, 1, 0);
    let lo = Proposal::with_timestamp("lo".into(), 100, 1, 1, 0);
    assert!(
        hi.is_better_than(&lo),
        "higher node_priority must be preferred on equal progress"
    );
    assert!(!lo.is_better_than(&hi));
}

// ---------------------------------------------------------------------------
// Priority 0 = not master-eligible (JE): must not be elected even with the
// best VLSN. Mirrors the F22 arbiter guard (see arbiter_election_test.rs).
// ---------------------------------------------------------------------------

/// Single-node group: SimpleMajority quorum == 1, so a self-vote alone is a
/// winning quorum. This isolates the priority-0 guard from the (unrelated)
/// quorum arithmetic — with a 2+ node group `run_election` would return
/// `None` because a lone self-vote is not a majority, masking the priority
/// behaviour we are testing.
fn one_node_group() -> RepGroup {
    let mut g = RepGroup::new("prio_group".into(), 1);
    g.add_node(RepNode::new(
        "node1".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        5001,
        1,
    ));
    g
}

/// A priority-0 Electable node must NOT win an election even with a self-
/// quorum and the highest possible VLSN. It participates as an acceptor but
/// refuses to propose itself as master (JE NODE_PRIORITY == 0).
///
/// On base this FAILS: `run_election` with `priority = 0` and a self-quorum
/// wins and returns the proposer's id (the priority-0 guard is absent).
#[test]
fn priority_zero_node_refuses_to_propose_itself() {
    let group = one_node_group();
    // node1 is Electable (can_be_master() == true by TYPE), but priority 0.
    // Highest VLSN, self-only quorum: without the priority-0 guard it wins.
    let winner = run_election(1, "node1", &group, &[], u64::MAX, 0, 1);
    assert!(
        winner.is_none(),
        "a priority-0 node must not propose/elect itself as master \
         (JE NODE_PRIORITY == 0 = not master-eligible)"
    );
}

/// A priority >= 1 Electable node DOES win the self-quorum (control: the
/// guard is specific to priority 0, not a blanket refusal).
#[test]
fn priority_one_node_still_elects_itself() {
    let group = one_node_group();
    let winner = run_election(1, "node1", &group, &[], 100, 1, 1);
    assert_eq!(
        winner,
        Some(1),
        "an ordinary priority-1 node must still be able to become master"
    );
}
