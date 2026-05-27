//! JE-equivalent acceptor protocol tests.
//!
//! Wave 6 — Priority-3 JE TCK port.
//!
//! Ports the core invariants of
//! `je/test/com/sleepycat/je/rep/elections/AcceptorTest.java::testAcceptor`,
//! adapted to Noxu's `PersistentAcceptorState`.  JE's `Acceptor.process(Propose)`
//! and `Acceptor.process(Accept)` map onto Noxu's `try_promise(t)` and
//! `try_accept(t, master)`; PROMISE/ACCEPTED responses become `true`, REJECT
//! responses become `false`.  The protocol invariants are identical.
//!
//! Mapping table (JE -> Noxu):
//! - Proposal#compareTo is total-ordered  -> u64 term
//! - PROMISE                               -> try_promise(t) == true
//! - ACCEPTED                              -> try_accept(t, master) == true
//! - REJECT (Propose / Accept)             -> try_promise/try_accept == false
//! - StringValue("VALUE")                  -> master string

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
    assert!(acc.try_promise(pn1), "first promise at pn1 must succeed");

    // Propose(pn0) -> REJECT (lower than promised)
    assert!(
        !acc.try_promise(pn0),
        "promise at pn0 < promised(pn1) must be REJECTed"
    );

    // Accept(pn1, V) -> ACCEPTED
    assert!(
        acc.try_accept(pn1, "VALUE"),
        "accept at promised term must succeed"
    );

    // Propose(pn0) -> REJECT (still, after accept)
    assert!(
        !acc.try_promise(pn0),
        "promise at pn0 must remain REJECTed after accept"
    );

    // Propose(pn2) -> PROMISE
    assert!(acc.try_promise(pn2), "promise at pn2 (higher) must succeed");

    // Accept(pn2, V) -> ACCEPTED
    assert!(
        acc.try_accept(pn2, "VALUE"),
        "accept at pn2 (=promised) must succeed"
    );

    // Accept(pn0, V) -> REJECT
    assert!(
        !acc.try_accept(pn0, "VALUE"),
        "accept at pn0 must be REJECTed after promise(pn2)"
    );

    // Accept(pn1, V) -> REJECT
    assert!(
        !acc.try_accept(pn1, "VALUE"),
        "accept at pn1 must be REJECTed after promise(pn2)"
    );
}

/// JE invariant: a Promise at the same term as the previous promise
/// must succeed (Paxos `>=` semantics for promises).
#[test]
fn test_acceptor_repeated_promise_at_same_term() {
    let dir = TempDir::new().unwrap();
    let acc = PersistentAcceptorState::load_or_default(dir.path());
    assert!(acc.try_promise(50));
    assert!(acc.try_promise(50), "promise at == promised must still succeed");
    assert_eq!(acc.promised_term(), 50);
}

/// JE invariant (from AcceptorTest's tear-down/setup pattern): an Acceptor
/// reset (close + re-open at same env_home) must preserve the promise.
#[test]
fn test_acceptor_persisted_promise_survives_reload() {
    let dir = TempDir::new().unwrap();
    {
        let acc = PersistentAcceptorState::load_or_default(dir.path());
        assert!(acc.try_promise(42));
        assert!(acc.try_accept(42, "winner"));
    }
    // "Re-open" the acceptor.
    let acc2 = PersistentAcceptorState::load_or_default(dir.path());
    assert_eq!(acc2.promised_term(), 42);
    assert_eq!(acc2.accepted_term(), 42);
    assert_eq!(acc2.accepted_master().as_deref(), Some("winner"));

    // A stale Propose at term 41 must be REJECTed.
    assert!(!acc2.try_promise(41));
    // A stale Accept at term 41 must be REJECTed.
    assert!(!acc2.try_accept(41, "loser"));
}

/// Adapted from JE's AcceptorTest end-to-end protocol invariant:
/// `Accept(t, V)` when t > promised must atomically bump promised to t
/// (JE: AcceptorImpl bumps highestPromisedProposal inside the accept path).
#[test]
fn test_acceptor_accept_implicitly_promises() {
    let dir = TempDir::new().unwrap();
    let acc = PersistentAcceptorState::load_or_default(dir.path());

    assert!(acc.try_promise(10));
    // Accept at term 20 (> promised) succeeds and must implicitly bump
    // promised_term to 20.  JE's AcceptorImpl does this implicitly.
    assert!(acc.try_accept(20, "n2"));
    assert_eq!(acc.promised_term(), 20);
    // Now a Propose at 15 must be rejected, even though 15 > original 10.
    assert!(!acc.try_promise(15));
}
