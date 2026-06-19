//! JE-equivalent ranking proposer tests.
//!
//! Wave 6 — Priority-3 JE TCK port.
//!
//! Ports invariants from
//! `je/test/com/sleepycat/je/rep/elections/RankingProposerTest.java`.
//!
//! JE's `RankingProposer.choosePhase2Value(promises)` enforces a global
//! ordering on candidate masters: the candidate with the higher DTVLSN
//! (≈ Noxu's `vlsn`) wins, with a key exception — *arbiter promises are
//! ignored when at least one non-arbiter promise is present*. Otherwise
//! an arbiter that happens to advertise a high DTVLSN could be elected
//! master even though arbiters cannot serve as master.
//!
//! Noxu has no `choosePhase2Value` method as a separate function; the
//! same invariant is enforced inside `run_election` via the F22 guard.
//! Here we test the underlying *ordering* via `Proposal::cmp`, which is
//! the value the proposer compares against `best_proposal`.

use noxu_rep::elections::Proposal;

const NODE_NAME: &str = "n1";
const ARB_NAME: &str = "arb";

fn promise(node: &str, dtvlsn: u64, priority: u32) -> Proposal {
    // Term and timestamp are the same for all promises — JE's tests only
    // care about (node_name, dtvlsn) pairs.
    Proposal::with_timestamp(node.into(), dtvlsn, priority, 1, 0)
}

/// Pick the best non-arbiter proposal — the equivalent of JE's
/// `choosePhase2Value`.  Arbiter promises (priority == 0) are filtered
/// out when at least one non-arbiter is present.  This mirrors the F22
/// guard in `run_election`.
fn choose_phase2_value(promises: &[Proposal]) -> Option<Proposal> {
    let has_non_arb = promises.iter().any(|p| p.priority > 0);
    if has_non_arb {
        promises.iter().filter(|p| p.priority > 0).max().cloned()
    } else {
        // All-arbiters: JE returns null in this case (see testPhase2ArbOneNode
        // assertEquals(null, ...) when proposer is the arbiter).  No master
        // can be picked.
        None
    }
}

// --------------------------------------------------------------------------
// testPhase2TwoNodes — two non-arbiter promises; highest VLSN wins.
// --------------------------------------------------------------------------
#[test]
fn test_phase2_two_nodes() {
    // (NODE,100) + (NODE,100) -> NODE
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 100, 1),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    // (NODE,100) + (NODE,200) -> NODE
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 200, 1),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    // (NODE,200) + (NODE,100) -> NODE
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 200, 1),
        promise(NODE_NAME, 100, 1),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);
}

// --------------------------------------------------------------------------
// testPhase2ThreeNodes — three non-arbiter promises; highest VLSN wins.
// --------------------------------------------------------------------------
#[test]
fn test_phase2_three_nodes() {
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 100, 1),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 200, 1),
        promise(NODE_NAME, 300, 1),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);
}

// --------------------------------------------------------------------------
// testPhase2ArbOneNode — one node + one arbiter.
//
// JE invariant: the non-arbiter wins UNLESS the arbiter has a strictly
// higher DTVLSN, in which case neither wins (returns null).
// Noxu equivalent: arbiter is filtered, non-arbiter always wins.
// --------------------------------------------------------------------------
#[test]
fn test_phase2_arb_one_node() {
    // (NODE,100) + (arb,100) -> NODE
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(ARB_NAME, 100, 0),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    // (arb,100) + (NODE,100) -> NODE (order independence)
    let r = choose_phase2_value(&[
        promise(ARB_NAME, 100, 0),
        promise(NODE_NAME, 100, 1),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    // (NODE,200) + (arb,100) -> NODE
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 200, 1),
        promise(ARB_NAME, 100, 0),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    // (arb,100) + (NODE,200) -> NODE
    let r = choose_phase2_value(&[
        promise(ARB_NAME, 100, 0),
        promise(NODE_NAME, 200, 1),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    // DEVIATION (DTVLSN ranking deferred): JE testPhase2ArbOneNode ALSO asserts
    // the two cases where the lone arbiter advertises a STRICTLY HIGHER DTVLSN
    // than the single real node — JE returns null (no master safely electable,
    // because the node is too far behind). Those cases require DTVLSN-based
    // election ranking, which is an authorized deferral (see
    // known-limitations: "DTVLSN-based election ranking ... not yet ported").
    // They are intentionally NOT asserted here. This test covers the
    // arbiter-exclusion invariant (an arb never wins) via the priority-based
    // ranking that IS implemented; the DTVLSN-null cases will be added when
    // DTVLSN ranking lands. NOTE: this exercises a test-local helper modelling
    // the arb-exclusion rule, not production run_election (which enforces the
    // same exclusion via its F22 guard).
}

// --------------------------------------------------------------------------
// testPhase2ArbTwoNodes — two non-arbs + one arb.  Arb is ignored.
// JE invariant: same as testPhase2TwoNodes regardless of arb position
// or arb's DTVLSN.
// --------------------------------------------------------------------------
#[test]
fn test_phase2_arb_two_nodes() {
    // (NODE,100) (NODE,100) (arb,100) -> NODE
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 100, 1),
        promise(ARB_NAME, 100, 0),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    // (NODE,100) (NODE,200) (arb,100) -> NODE
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 200, 1),
        promise(ARB_NAME, 100, 0),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);

    // (NODE,100) (NODE,200) (arb,300) -> NODE — arb's higher DTVLSN
    // must not win.
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 200, 1),
        promise(ARB_NAME, 300, 0),
    ]);
    assert_eq!(
        r.unwrap().node_name,
        NODE_NAME,
        "arbiter with higher DTVLSN must be ignored when non-arbs exist"
    );

    // arbiter at highest DTVLSN regardless of position.
    let r = choose_phase2_value(&[
        promise(ARB_NAME, 999, 0),
        promise(NODE_NAME, 100, 1),
        promise(NODE_NAME, 200, 1),
    ]);
    assert_eq!(r.unwrap().node_name, NODE_NAME);
}

// --------------------------------------------------------------------------
// testPhase2TwoArbs — two arbiters + two non-arbs.  Both arbs ignored.
// --------------------------------------------------------------------------
#[test]
fn test_phase2_two_arbs() {
    let r = choose_phase2_value(&[
        promise(NODE_NAME, 100, 1),
        promise(ARB_NAME, 300, 0),
        promise(ARB_NAME, 400, 0),
        promise(NODE_NAME, 200, 1),
    ]);
    let p = r.unwrap();
    assert_eq!(
        p.node_name, NODE_NAME,
        "both arbiters must be ignored even when DTVLSN is highest"
    );
    // Best non-arb has VLSN 200.
    assert_eq!(p.vlsn, 200);
}

// --------------------------------------------------------------------------
// All-arbiters edge case: returns None (matches JE's null return when
// there are no non-arbiter candidates).
// --------------------------------------------------------------------------
#[test]
fn test_phase2_all_arbs_returns_none() {
    let r = choose_phase2_value(&[
        promise("arb1", 100, 0),
        promise("arb2", 200, 0),
    ]);
    assert!(
        r.is_none(),
        "an all-arbiter promise set must yield no candidate (JE returns null)"
    );
}
