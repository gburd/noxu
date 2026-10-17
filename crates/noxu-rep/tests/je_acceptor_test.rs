//! JE-equivalent acceptor protocol tests.
//!
//! Wave 6 — Priority-3 JE TCK port.
//!
//! Ports the core invariants of
//! `je/test/com/sleepycat/je/rep/elections/AcceptorTest.java::testAcceptor`,
//! adapted to Noxu's `PersistentAcceptorState`.  JE's `Acceptor.process(Propose)`
//! and `Acceptor.process(Accept)` map onto Noxu's `try_promise_proposal(t, proposer)`
//! and `try_accept_proposal(t, proposer, master)`; PROMISE/ACCEPTED responses become
//! `true`, REJECT responses become `false`.  The protocol invariants are
//! identical.
//!
//! Mapping table (JE -> Noxu):
//! - Proposal#compareTo is total-ordered  -> (term, proposer) lexicographic
//! - PROMISE                               -> try_promise_proposal(t, p) == true
//! - ACCEPTED                              -> try_accept_proposal(t, p, master) == true
//! - REJECT (Propose / Accept)             -> try_promise/try_accept == false
//! - StringValue("VALUE")                  -> master string
//!
//! These single-proposer tests fix one proposer name `P`, so the proposal
//! number varies only in its term component and the JE term ordering is
//! preserved verbatim.

use noxu_rep::elections::PersistentAcceptorState;
use tempfile::TempDir;

/// Direct port of `AcceptorTest.testAcceptor`.
///
/// Sequence (term names changed to integers for clarity):
///   pn0 = 100, pn1 = 200, pn2 = 300; pn1 > pn0; pn2 > pn1.
///
///   1. Propose(pn1) -> PROMISE
///   2. Propose(pn0) -> REJECT (lower than promised)
///   3. Accept(pn1, V) -> ACCEPTED
///   4. Propose(pn0) -> REJECT (still, after accept)
///   5. Propose(pn2) -> PROMISE (higher accepted)
///   6. Accept(pn2, V) -> ACCEPTED
///   7. Accept(pn0, V) -> REJECT
///   8. Accept(pn1, V) -> REJECT
#[test]
fn test_acceptor_je_equivalent() {
    let dir = TempDir::new().unwrap();
    let acc = PersistentAcceptorState::load_or_default(dir.path());

    // Proposal numbers ascending.
    let pn0: u64 = 100;
    let pn1: u64 = 200;
    let pn2: u64 = 300;
    assert!(pn1 > pn0, "proposal numbers must be ascending");
    assert!(pn2 > pn1, "proposal numbers must be ascending");

    // Propose(pn1) -> PROMISE
    assert!(
        acc.try_promise_proposal(pn1, "P"),
        "first promise at pn1 must succeed"
    );

    // Propose(pn0) -> REJECT (lower than promised)
    assert!(
        !acc.try_promise_proposal(pn0, "P"),
        "promise at pn0 < promised(pn1) must be REJECTed"
    );

    // Accept(pn1, V) -> ACCEPTED
    assert!(
        acc.try_accept_proposal(pn1, "P", "VALUE"),
        "accept at promised term must succeed"
    );

    // Propose(pn0) -> REJECT (still, after accept)
    assert!(
        !acc.try_promise_proposal(pn0, "P"),
        "promise at pn0 must remain REJECTed after accept"
    );

    // Propose(pn2) -> PROMISE
    assert!(
        acc.try_promise_proposal(pn2, "P"),
        "promise at pn2 (higher) must succeed"
    );

    // Accept(pn2, V) -> ACCEPTED
    assert!(
        acc.try_accept_proposal(pn2, "P", "VALUE"),
        "accept at pn2 (=promised) must succeed"
    );

    // Accept(pn0, V) -> REJECT
    assert!(
        !acc.try_accept_proposal(pn0, "P", "VALUE"),
        "accept at pn0 must be REJECTed after promise(pn2)"
    );

    // Accept(pn1, V) -> REJECT
    assert!(
        !acc.try_accept_proposal(pn1, "P", "VALUE"),
        "accept at pn1 must be REJECTed after promise(pn2)"
    );
}

/// JE invariant: a Promise at the same proposal as the previous promise
/// must succeed (Paxos: reject only a STRICTLY greater promise).
#[test]
fn test_acceptor_repeated_promise_at_same_term() {
    let dir = TempDir::new().unwrap();
    let acc = PersistentAcceptorState::load_or_default(dir.path());
    assert!(acc.try_promise_proposal(50, "P"));
    assert!(
        acc.try_promise_proposal(50, "P"),
        "re-promise of the SAME proposal must still succeed"
    );
    assert_eq!(acc.promised_term(), 50);
}

/// JE invariant (from AcceptorTest's tear-down/setup pattern): an Acceptor
/// reset (close + re-open at same env_home) must preserve the promise.
#[test]
fn test_acceptor_persisted_promise_survives_reload() {
    let dir = TempDir::new().unwrap();
    {
        let acc = PersistentAcceptorState::load_or_default(dir.path());
        assert!(acc.try_promise_proposal(42, "P"));
        assert!(acc.try_accept_proposal(42, "P", "winner"));
    }
    // "Re-open" the acceptor.
    let acc2 = PersistentAcceptorState::load_or_default(dir.path());
    assert_eq!(acc2.promised_term(), 42);
    assert_eq!(acc2.accepted_term(), 42);
    assert_eq!(acc2.accepted_master().as_deref(), Some("winner"));

    // A stale Propose at term 41 must be REJECTed.
    assert!(!acc2.try_promise_proposal(41, "P"));
    // A stale Accept at term 41 must be REJECTed.
    assert!(!acc2.try_accept_proposal(41, "P", "loser"));
}

/// JE Acceptor.process(Accept) (Acceptor.java:210-211): an Accept is rejected
/// unless its proposal EQUALS the promised proposal
/// (`promisedProposal.compareTo(accept.getProposal()) != 0` -> Reject). There
/// is NO implicit promise-bump on accept — an Accept at a term higher than the
/// promise (never promised in a phase 1) must be rejected. Accepting it would
/// admit two proposers at different terms both reaching phase-2 quorum
/// (split-brain). (Previously this test asserted the opposite, citing an
/// implicit-bump that JE does not perform — a re-invention; corrected here.)
#[test]
fn test_acceptor_rejects_accept_at_unpromised_term() {
    let dir = TempDir::new().unwrap();
    let acc = PersistentAcceptorState::load_or_default(dir.path());

    assert!(acc.try_promise_proposal(10, "P"));
    // Accept at term 20 (> promised 10) must be REJECTED: 20 was never
    // promised in a phase 1, so the phase-2 value is not fixed to it.
    assert!(
        !acc.try_accept_proposal(20, "P", "n2"),
        "JE: accept at a term != promised must be rejected"
    );
    assert_eq!(acc.promised_term(), 10, "promise unchanged by rejected accept");
    assert_eq!(acc.accepted_term(), 0, "nothing accepted");
    // Accept at the promised term 10 succeeds.
    assert!(acc.try_accept_proposal(10, "P", "n2"));
    assert_eq!(acc.accepted_term(), 10);
}
