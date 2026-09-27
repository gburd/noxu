//! Faithful port of
//! `je/test/com/sleepycat/je/rep/elections/RankingProposerTest.java` (C5 /
//! ranking proposer). Exercises the PRODUCTION value-selection function
//! `noxu_rep::elections::paxos::choose_phase2_value`, the direct analogue of
//! JE `RankingProposer.choosePhase2Value`.
//!
//! JE's `choosePhase2Value` picks the highest-ranking non-arbiter, non-zero-
//! priority suggestion, with two null cases:
//!   * no non-zero-priority node responded (`NoNonZeroPriority`), and
//!   * the RF=2 `[#25311]` arbiter veto: with <=1 non-arbiter candidate, if an
//!     arbiter remembers a strictly higher ranking than the chosen value, no
//!     master is safely electable (`ArbiterVeto`).
//!
//! JE's `promise(nodeName, dtvlsn)` uses `new Ranking(dtvlsn, 0)` as the
//! ranking and `MasterValue(nodeName, ..., NULL)` for an arbiter (null
//! nodeName). Noxu models the ranking's major key with `Proposal.dtvlsn`, an
//! arbiter with `is_arbiter = true`, and priority 1 for real nodes.
//!
//! FIDELITY NOTE: this replaces the earlier wave-6 port, which (a) reimplemented
//! selection in a test-local helper instead of production code, and (b)
//! deferred the arbiter-DTVLSN null cases citing "DTVLSN ranking not yet
//! ported". DTVLSN ranking IS now the major key and the veto IS implemented in
//! `run_election_with_phi_dtvlsn`, so those cases are asserted here in full.

use noxu_rep::elections::Proposal;
use noxu_rep::elections::paxos::{
    Phase2Value, PromiseRecord, choose_phase2_value,
};

const NODE_NAME: &str = "node1";

/// JE `promise(nodeName, dtvlsn)`: a non-arbiter suggestion for `NODE_NAME`
/// with the given DTVLSN as its ranking major key, priority 1.
fn node_promise(dtvlsn: u64) -> PromiseRecord {
    PromiseRecord {
        // dtvlsn is the major ranking key; vlsn mirrors it; priority 1.
        proposal: Proposal::with_timestamp(NODE_NAME.into(), dtvlsn, 1, 1, 0)
            .with_dtvlsn(dtvlsn),
        is_arbiter: false,
    }
}

/// JE `promise(null, dtvlsn)`: an arbiter suggestion (null node name) with the
/// given DTVLSN ranking.
fn arb_promise(dtvlsn: u64) -> PromiseRecord {
    PromiseRecord {
        proposal: Proposal::with_timestamp("arb".into(), dtvlsn, 1, 1, 0)
            .with_dtvlsn(dtvlsn),
        is_arbiter: true,
    }
}

/// JE's `choosePhase2Value` returns the chosen node name, or null. Map the
/// Noxu tri-state onto that: `Value -> Some(name)`, both null cases -> None.
fn choose(promises: &[PromiseRecord]) -> Option<String> {
    match choose_phase2_value(promises) {
        Phase2Value::Value(p) => Some(p.node_name),
        Phase2Value::NoNonZeroPriority | Phase2Value::ArbiterVeto => None,
    }
}

// --------------------------------------------------------------------------
// JE: RankingProposerTest.testPhase2TwoNodes
// Two non-arbiter promises; highest DTVLSN wins; always NODE_NAME here.
// --------------------------------------------------------------------------
#[test]
fn test_phase2_two_nodes() {
    assert_eq!(
        choose(&[node_promise(100), node_promise(100)]).as_deref(),
        Some(NODE_NAME)
    );
    assert_eq!(
        choose(&[node_promise(100), node_promise(200)]).as_deref(),
        Some(NODE_NAME)
    );
    assert_eq!(
        choose(&[node_promise(200), node_promise(100)]).as_deref(),
        Some(NODE_NAME)
    );
}

// --------------------------------------------------------------------------
// JE: RankingProposerTest.testPhase2ThreeNodes
// --------------------------------------------------------------------------
#[test]
fn test_phase2_three_nodes() {
    assert_eq!(
        choose(&[node_promise(100), node_promise(100), node_promise(100)])
            .as_deref(),
        Some(NODE_NAME)
    );
    assert_eq!(
        choose(&[node_promise(100), node_promise(200), node_promise(300)])
            .as_deref(),
        Some(NODE_NAME)
    );
}

// --------------------------------------------------------------------------
// JE: RankingProposerTest.testPhase2ArbOneNode
// One node + one arbiter. The node wins UNLESS the arbiter has a STRICTLY
// higher DTVLSN, in which case JE returns null (the RF=2 [#25311] veto).
// --------------------------------------------------------------------------
#[test]
fn test_phase2_arb_one_node() {
    // (NODE,100)+(arb,100) -> NODE
    assert_eq!(
        choose(&[node_promise(100), arb_promise(100)]).as_deref(),
        Some(NODE_NAME)
    );
    // (arb,100)+(NODE,100) -> NODE (order independence)
    assert_eq!(
        choose(&[arb_promise(100), node_promise(100)]).as_deref(),
        Some(NODE_NAME)
    );
    // (NODE,100)+(arb,200) -> null: arbiter remembers MORE than the sole node.
    assert_eq!(choose(&[node_promise(100), arb_promise(200)]), None);
    // (arb,200)+(NODE,100) -> null (order independence)
    assert_eq!(choose(&[arb_promise(200), node_promise(100)]), None);
    // (NODE,200)+(arb,100) -> NODE (node ahead of arbiter)
    assert_eq!(
        choose(&[node_promise(200), arb_promise(100)]).as_deref(),
        Some(NODE_NAME)
    );
    // (arb,100)+(NODE,200) -> NODE
    assert_eq!(
        choose(&[arb_promise(100), node_promise(200)]).as_deref(),
        Some(NODE_NAME)
    );
}

// --------------------------------------------------------------------------
// JE: RankingProposerTest.testPhase2ArbTwoNodes
// Two non-arbiters + one arbiter: the arbiter is ALWAYS ignored (>=2 non-arbs),
// regardless of its DTVLSN or position. Result is always NODE_NAME.
// --------------------------------------------------------------------------
#[test]
fn test_phase2_arb_two_nodes() {
    let cases: &[&[PromiseRecord]] = &[
        &[node_promise(100), node_promise(100), arb_promise(100)],
        &[node_promise(100), arb_promise(100), node_promise(100)],
        &[arb_promise(100), node_promise(100), node_promise(100)],
        &[node_promise(100), node_promise(200), arb_promise(100)],
        &[node_promise(100), arb_promise(100), node_promise(200)],
        &[arb_promise(100), node_promise(100), node_promise(200)],
        &[node_promise(200), node_promise(100), arb_promise(100)],
        &[node_promise(200), arb_promise(100), node_promise(100)],
        &[arb_promise(100), node_promise(200), node_promise(100)],
        // Arbiter with the HIGHEST DTVLSN must still be ignored (>=2 non-arbs).
        &[node_promise(100), node_promise(200), arb_promise(300)],
        &[node_promise(100), arb_promise(300), node_promise(200)],
        &[arb_promise(300), node_promise(100), node_promise(200)],
        &[node_promise(200), node_promise(100), arb_promise(300)],
        &[node_promise(200), arb_promise(300), node_promise(100)],
        &[arb_promise(300), node_promise(200), node_promise(100)],
    ];
    for (i, c) in cases.iter().enumerate() {
        assert_eq!(
            choose(c).as_deref(),
            Some(NODE_NAME),
            "case {i}: arbiter must be ignored when >=2 non-arbiters respond"
        );
    }
}

// --------------------------------------------------------------------------
// JE: RankingProposerTest.testPhase2TwoArbs
// Two arbiters + two non-arbs: both arbiters ignored (>=2 non-arbs).
// --------------------------------------------------------------------------
#[test]
fn test_phase2_two_arbs() {
    assert_eq!(
        choose(&[
            node_promise(100),
            arb_promise(300),
            arb_promise(400),
            node_promise(200)
        ])
        .as_deref(),
        Some(NODE_NAME),
        "both arbiters ignored even at the highest DTVLSN when >=2 non-arbs"
    );
}

// --------------------------------------------------------------------------
// Edge (JE null-return coverage): an all-arbiter promise set yields no
// candidate (JE `choosePhase2Value` returns null with no non-arbiter).
// --------------------------------------------------------------------------
#[test]
fn test_phase2_all_arbs_returns_none() {
    assert_eq!(choose(&[arb_promise(100), arb_promise(200)]), None);
}
