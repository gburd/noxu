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
//! See the 2026 review finding F22.

use std::sync::Arc;

use noxu_rep::elections::paxos::{
    run_acceptor, run_election, run_election_with_phi_dtvlsn,
};
use noxu_rep::net::{Channel, LocalChannelPair};
use noxu_rep::node_type::NodeType;
use noxu_rep::protocol::ProtocolMessage;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;
use std::time::Duration;

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
/// JE: ArbiterTest.testMasterDown / ArbiterTest.testFlipMaster /
/// ArbiterTest.testQuadElection (arbiter-provides-quorum half): an arbiter
/// contributes to the election quorum so a real electable node (not the
/// arbiter) becomes master. The arbiter's Phase-1 promise counts toward
/// quorum while the arbiter itself never wins.
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

/// RF=2 [#25311] arbiter veto (end-to-end through `run_election`).
///
/// JE `RankingProposer.choosePhase2Value` returns null when there is at most
/// one non-arbiter candidate and an arbiter remembers a *strictly higher*
/// ranking (DTVLSN) than that candidate: the sole surviving node has lost
/// durable data the arbiter witnessed, so electing it would silently lose
/// committed transactions. JE test `RankingProposerTest.testPhase2ArbOneNode`
/// asserts the two `assertEquals(null, ...)` cases; this test proves the same
/// veto fires through the production election path, not just the value chooser.
///
/// Scenario: an RF=2 group (node1 electable + arbiter). node1 proposes with
/// DTVLSN 100; the arbiter answers Phase 1 with a counter-proposal carrying
/// DTVLSN 200 (it remembers a more-durable commit from the departed master).
/// `run_election_with_phi_dtvlsn` must return `None` (no master this round).
/// JE: ArbiterTest.testOneMaster / ArbiterTest.testQuad (arbiter-prevents-
/// stale-master half): "the Arbiter prevents a node from becoming Master due
/// to its VLSN lower than the Arbiter's". Same RF=2 veto, driven end-to-end.
#[test]
fn arbiter_veto_blocks_election_when_sole_node_lags_dtvlsn() {
    let mut group = RepGroup::new("rf2".into(), 1);
    group.add_node(RepNode::new(
        "node1".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        6001,
        1,
    ));
    group.add_node(RepNode::new(
        "arbiter".into(),
        NodeType::Arbiter,
        "127.0.0.1".into(),
        6002,
        2,
    ));

    let pair = LocalChannelPair::new();
    let proposer_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
    let arb_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

    // Hand-rolled arbiter acceptor: promise Phase 1 by returning a
    // counter-proposal with a HIGHER DTVLSN than node1's, then reject Phase 2
    // (an arbiter cannot be master and does not grant the accept for itself;
    // the point is the veto aborts the round before Phase 2 quorum anyway).
    let h = std::thread::spawn(move || {
        // Phase 1: receive the proposal.
        let msg = arb_ch
            .receive(Duration::from_secs(2))
            .unwrap()
            .expect("arbiter should receive a proposal");
        if let ProtocolMessage::ElectionProposal { .. } =
            ProtocolMessage::decode(&msg).unwrap()
        {
            // Reply with a counter-proposal: arbiter DTVLSN = 200 > node1's 100.
            let counter = ProtocolMessage::ElectionProposal {
                node_name: "arbiter".into(),
                vlsn: 200,
                priority: 1,
                term: 1,
                dtvlsn: 200,
            };
            arb_ch.send(&counter.encode()).unwrap();
        }
        // Phase 2 (if it ever comes): GRANT it. This makes the test NON-VACUOUS
        // w.r.t. the arbiter veto: if the veto were removed, node1 would win the
        // Phase-2 quorum here and the election would return Some(1) (electing a
        // node below the arbiter's durable point). The ONLY reason the result is
        // None is the choose_phase2_value arbiter veto aborting the round before
        // Phase 2 (JE RankingProposer.choosePhase2Value). A quorum-reject would
        // mask the veto, so we deliberately grant.
        if let Ok(Some(bytes)) = arb_ch.receive(Duration::from_millis(300))
            && let Ok(ProtocolMessage::ElectionResult { term, .. }) =
                ProtocolMessage::decode(&bytes)
        {
            let vote = ProtocolMessage::ElectionVote {
                voter: "arbiter".into(),
                granted: true,
                term,
            };
            let _ = arb_ch.send(&vote.encode());
        }
    });

    // node1 proposes with own DTVLSN = 100 (lags the arbiter's 200).
    let winner = run_election_with_phi_dtvlsn(
        1,
        "node1",
        &group,
        &[proposer_ch],
        100, // proposed_vlsn
        1,   // priority
        1,   // term
        100, // own_dtvlsn (< arbiter's 200)
        None,
        Duration::from_millis(500),
    );

    let _ = h.join();

    assert!(
        winner.is_none(),
        "RF=2 arbiter veto: a node lagging the arbiter's DTVLSN must NOT be \
         elected ([#25311]); got {winner:?}"
    );
}

/// Control for the veto: when the sole node is AHEAD of (or equal to) the
/// arbiter's DTVLSN, the veto does NOT fire and the node is elected. Proves
/// the veto is specific to the lagging case, not a blanket arbiter-present
/// refusal.
/// JE: ArbiterTest.testOneMaster (control) — once the surviving node is
/// caught up to (or ahead of) the arbiter's durable point it IS elected;
/// the veto is specific to the lagging case, not a blanket arbiter block.
#[test]
fn arbiter_veto_does_not_fire_when_node_leads_dtvlsn() {
    let mut group = RepGroup::new("rf2b".into(), 1);
    group.add_node(RepNode::new(
        "node1".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        6101,
        1,
    ));
    group.add_node(RepNode::new(
        "arbiter".into(),
        NodeType::Arbiter,
        "127.0.0.1".into(),
        6102,
        2,
    ));

    let pair = LocalChannelPair::new();
    let proposer_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
    let arb_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

    let h = std::thread::spawn(move || {
        let msg = arb_ch.receive(Duration::from_secs(2)).unwrap().unwrap();
        if let ProtocolMessage::ElectionProposal { .. } =
            ProtocolMessage::decode(&msg).unwrap()
        {
            // Arbiter DTVLSN = 50 < node1's 100 -> no veto.
            let counter = ProtocolMessage::ElectionProposal {
                node_name: "arbiter".into(),
                vlsn: 50,
                priority: 1,
                term: 1,
                dtvlsn: 50,
            };
            arb_ch.send(&counter.encode()).unwrap();
        }
        // Phase 2: the arbiter's promise counts toward quorum; grant so the
        // 2-node quorum (self + arbiter) is met.
        if let Ok(Some(bytes)) = arb_ch.receive(Duration::from_millis(500))
            && let Ok(ProtocolMessage::ElectionResult { term, .. }) =
                ProtocolMessage::decode(&bytes)
        {
            let vote = ProtocolMessage::ElectionVote {
                voter: "arbiter".into(),
                granted: true,
                term,
            };
            let _ = arb_ch.send(&vote.encode());
        }
    });

    let winner = run_election_with_phi_dtvlsn(
        1,
        "node1",
        &group,
        &[proposer_ch],
        100,
        1,
        1,
        100, // own_dtvlsn (> arbiter's 50)
        None,
        Duration::from_millis(500),
    );

    let _ = h.join();

    assert_eq!(
        winner,
        Some(1),
        "node ahead of the arbiter's DTVLSN must still be elected (veto is \
         specific to the lagging case)"
    );
}
