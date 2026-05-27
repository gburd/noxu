//! F22: Arbiters cannot win elections.
//!
//! Without a guard, `run_election` resolves the winner by `best_proposal`
//! ordering (highest VLSN wins). An Arbiter has no data and `can_be_master()
//! == false`, but it does participate in elections (`is_electable() ==
//! true`). When the Arbiter happens to share or exceed the highest VLSN
//! \u2014 e.g., right after a fresh group is provisioned \u2014 the Arbiter wins
//! and the cluster is wedged: an Arbiter cannot serve reads and cannot
//! generate VLSNs.
//!
//! The Wave 3-3 fix:
//!  1. A non-electable-as-master node refuses to start an election round.
//!  2. The proposer's `best_proposal` only considers counter-proposals
//!     from peers whose `node_type.can_be_master()` is true. Arbiter
//!     promises still count toward Phase 1 quorum but never as the
//!     candidate value.
//!
//! See docs/src/internal/api-audit-2026-05-rep.md finding F22.

use std::sync::Arc;

use noxu_rep::elections::paxos::{run_acceptor, run_election};
use noxu_rep::net::{Channel, LocalChannelPair};
use noxu_rep::node_type::NodeType;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;

fn make_group() -> RepGroup {
    let mut g = RepGroup::new("testgroup".into(), 1);
    // node1: Electable proposer, low VLSN.
    g.add_node(RepNode::new(
        "node1".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        5001,
        1,
    ));
    // node2: Electable peer, low VLSN.
    g.add_node(RepNode::new(
        "node2".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        5002,
        2,
    ));
    // node3: Arbiter at the highest VLSN. Without F22 guard, this
    // would win the election and wedge the cluster.
    g.add_node(RepNode::new(
        "arbiter".into(),
        NodeType::Arbiter,
        "127.0.0.1".into(),
        5003,
        3,
    ));
    g
}

/// An Arbiter at the highest VLSN must NOT win the election. The proposer
/// (node1) has a low VLSN; an Electable peer (node2) has a low VLSN; the
/// Arbiter peer claims the highest VLSN. Outcome: Phase 1 still hits
/// quorum (Arbiter promises count), but the candidate value is the best
/// Electable proposal, so `node1` wins (it is a tied Electable, with
/// proposer's self-vote breaking the tie via Phase 2 quorum).
#[test]
fn f22_arbiter_with_highest_vlsn_does_not_win() {
    let group = make_group();

    // Two peer channels: node2 (Electable, vlsn=10) and arbiter (vlsn=999).
    let pair_e = LocalChannelPair::new();
    let pair_a = LocalChannelPair::new();

    let proposer_chs: Vec<Arc<dyn Channel>> =
        vec![Arc::new(pair_e.channel_a), Arc::new(pair_a.channel_a)];

    let acceptor_e: Arc<dyn Channel> = Arc::new(pair_e.channel_b);
    let acceptor_a: Arc<dyn Channel> = Arc::new(pair_a.channel_b);

    // node2 acceptor: same low VLSN as node1.
    let h_e = std::thread::spawn(move || {
        run_acceptor(&*acceptor_e, "node2", 10, 1, 1).unwrap_or(None)
    });
    // arbiter acceptor: HIGHEST VLSN.
    let h_a = std::thread::spawn(move || {
        run_acceptor(&*acceptor_a, "arbiter", 999, 1, 1).unwrap_or(None)
    });

    // node1 proposes, vlsn=10. Without F22 the Arbiter's vlsn=999
    // counter-proposal would override `best_proposal` and win Phase 2.
    let winner = run_election(1, "node1", &group, &proposer_chs, 10, 1, 1);

    let _ = h_e.join();
    let _ = h_a.join();

    let winner_id = winner.expect("election should reach quorum");
    let winner_node = group
        .get_nodes()
        .into_iter()
        .find(|n| n.node_id() == winner_id)
        .expect("winner id must resolve to a known node");

    assert!(
        winner_node.can_be_master(),
        "elected master must be can_be_master(); got {:?} ({})",
        winner_node.node_type(),
        winner_node.name(),
    );
    assert_ne!(
        winner_node.name(),
        "arbiter",
        "Arbiter must never win elections (F22)"
    );
}

/// An Arbiter must refuse to even propose itself. If an Arbiter
/// somehow calls `run_election`, the function returns `None` rather
/// than driving the protocol that would advertise the Arbiter as a
/// candidate.
#[test]
fn f22_arbiter_refuses_to_propose_itself() {
    let group = make_group();

    // Arbiter "arbiter" tries to start an election. No peer channels
    // needed \u2014 the function must short-circuit before sending anything.
    let winner = run_election(3, "arbiter", &group, &[], 999, 1, 1);
    assert!(winner.is_none(), "Arbiter must not start an election round (F22)");
}

/// A node not in the group at all should also be refused (closed-world
/// guard).
#[test]
fn f22_unknown_node_refuses_to_propose() {
    let group = make_group();
    let winner = run_election(99, "ghost", &group, &[], 0, 1, 1);
    assert!(
        winner.is_none(),
        "unknown proposer must not run an election (F22 closed-world)"
    );
}
