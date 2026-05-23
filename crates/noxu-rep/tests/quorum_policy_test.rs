//! Tests for `QuorumPolicy`: SimpleMajority, Flexible Paxos, and quoracle
//! Expression policies.

use noxu_rep::QuorumPolicy;

// ---------------------------------------------------------------------------
// SimpleMajority
// ---------------------------------------------------------------------------

#[test]
fn test_simple_majority_3_5_7() {
    let p = QuorumPolicy::SimpleMajority;

    // 3-node cluster: quorum = 2
    assert_eq!(p.phase1_quorum(3), 2);
    assert_eq!(p.phase2_quorum(3), 2);

    // 5-node cluster: quorum = 3
    assert_eq!(p.phase1_quorum(5), 3);
    assert_eq!(p.phase2_quorum(5), 3);

    // 7-node cluster: quorum = 4
    assert_eq!(p.phase1_quorum(7), 4);
    assert_eq!(p.phase2_quorum(7), 4);

    // Edge: 1-node cluster: quorum = 1
    assert_eq!(p.phase1_quorum(1), 1);

    // Edge: 0-node cluster: quorum = 0
    assert_eq!(p.phase1_quorum(0), 0);

    // Validation always succeeds for SimpleMajority.
    assert!(p.validate(5).is_ok());
}

// ---------------------------------------------------------------------------
// Flexible
// ---------------------------------------------------------------------------

#[test]
fn test_flexible_5node_phase1_4_phase2_2() {
    let p = QuorumPolicy::Flexible { phase1: 4, phase2: 2 };

    // Explicit sizes are returned regardless of cluster size argument.
    assert_eq!(p.phase1_quorum(5), 4);
    assert_eq!(p.phase2_quorum(5), 2);

    // Safety: 4 + 2 = 6 > 5 → valid.
    assert!(p.validate(5).is_ok(), "4+2>5 must be valid");
}

#[test]
fn test_flexible_invalid_rejected() {
    // phase1=1, phase2=1, n=3 → 1+1=2 NOT > 3 → must fail.
    let p = QuorumPolicy::Flexible { phase1: 1, phase2: 1 };
    let result = p.validate(3);
    assert!(result.is_err(), "1+1 NOT > 3 must be rejected");
    let msg = result.unwrap_err();
    assert!(
        msg.contains("Safety requires"),
        "error message must explain safety: {msg}"
    );
}

#[test]
fn test_flexible_classic_majority_is_valid() {
    // phase1=3, phase2=3, n=5 → 3+3=6 > 5 → valid.
    let p = QuorumPolicy::Flexible { phase1: 3, phase2: 3 };
    assert!(p.validate(5).is_ok());
    assert_eq!(p.phase1_quorum(5), 3);
    assert_eq!(p.phase2_quorum(5), 3);
}

// ---------------------------------------------------------------------------
// Expression (quoracle integration)
// ---------------------------------------------------------------------------

#[test]
fn test_quoracle_choose_expression() {
    // 5-node names.
    let names: Vec<String> = (1u8..=5).map(|i| format!("node{i}")).collect();

    // choose(4 of 5) reads × choose(2 of 5) writes: 4+2=6>5, valid.
    let policy = QuorumPolicy::build_expression(&names, 4, 2)
        .expect("choose(4,5)/choose(2,5) must form a valid quorum system");

    // Phase 1 min quorum = 4 (smallest choose-4-of-5 quorum has 4 members).
    assert_eq!(policy.phase1_quorum(5), 4);
    // Phase 2 min quorum = 2.
    assert_eq!(policy.phase2_quorum(5), 2);

    // Validation succeeds for Expression (guaranteed by construction).
    assert!(policy.validate(5).is_ok());
}

#[test]
fn test_quoracle_choose_invalid_rejected() {
    // choose(1 of 3) reads × choose(1 of 3) writes: 1+1=2 NOT > 3 → quoracle
    // must reject at QuorumSystem::new() time (not intersection property).
    let names: Vec<String> = (1u8..=3).map(|i| format!("node{i}")).collect();

    let result = QuorumPolicy::build_expression(&names, 1, 1);
    assert!(
        result.is_err(),
        "choose(1,3)/choose(1,3) must be rejected (non-intersecting)"
    );
}

#[test]
fn test_build_majority_expression() {
    let names: Vec<String> = (1u8..=5).map(|i| format!("node{i}")).collect();

    let policy = QuorumPolicy::build_majority_expression(&names)
        .expect("majority expression must be constructible");

    // Majority of 5 = 3.
    assert_eq!(policy.phase1_quorum(5), 3);
    assert_eq!(policy.phase2_quorum(5), 3);
    assert!(policy.validate(5).is_ok());
}
