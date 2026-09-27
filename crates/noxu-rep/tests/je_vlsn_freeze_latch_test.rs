//! Faithful port of
//! `je/test/com/sleepycat/je/rep/elections/VLSNFreezeLatchTest.java` (B5).
//!
//! JE's `CommitFreezeLatch` freezes VLSN advancement on a node for the
//! duration of an election round so the VLSN/DTVLSN it advertised in its
//! Paxos Promise stays valid until the election concludes. The four JE
//! `@Test` methods pin the *sequential* API contract the latch must honour:
//!
//! - `testTimeout`   — freeze(p2); an OLDER event (p1) does NOT release the
//!   waiter; `awaitThaw()` times out -> false,
//!   `awaitTimeoutCount == 1`.
//! - `testElection`  — freeze(p2); the SAME-round event (p2) releases the
//!   waiter; `awaitThaw()` -> true, `awaitElectionCount == 1`.
//! - `testNewerElection` — freeze(p2); a NEWER event (p3) releases the
//!   waiter; `awaitThaw()` -> true, `awaitElectionCount == 1`.
//! - `testNoFreeze`  — no freeze in effect; `vlsnEvent(p1)` is a no-op;
//!   `awaitThaw()` -> false, `awaitTimeoutCount == 0`.
//!
//! JE uses `TimebasedProposalGenerator` to make three sequential proposals
//! p1 < p2 < p3. Noxu's `round_proposal(term)` is monotone in `term`
//! (dtvlsn=vlsn=term with a constant node name), so `round_proposal(1) <
//! round_proposal(2) < round_proposal(3)` is the exact analogue.
//!
//! FIDELITY NOTE: JE calls `vlsnEvent` then `awaitThaw` on the SAME thread
//! (the event has already counted the latch down before the wait begins).
//! These ports preserve that sequential ordering, exercising the contract
//! JE pins: a same-or-newer-round event that arrives *before* the replay
//! thread waits must still be observed as an election-driven thaw
//! (`awaitThaw() == true`, election count incremented), not silently lost.

use noxu_rep::elections::commit_freeze_latch::{
    CommitFreezeLatch, round_proposal,
};
use std::time::Duration;

/// The three sequential proposals p1 < p2 < p3 (JE
/// `TimebasedProposalGenerator.nextProposal()` x3).
fn proposals() -> (
    noxu_rep::elections::Proposal,
    noxu_rep::elections::Proposal,
    noxu_rep::elections::Proposal,
) {
    (round_proposal(1), round_proposal(2), round_proposal(3))
}

/// JE: `VLSNFreezeLatchTest.testTimeout`.
///
/// `freeze(p2)` then an OLDER event `vlsnEvent(p1)` — the older event must
/// NOT release the waiter — so `awaitThaw()` blocks until the freeze times
/// out and returns `false`; the timeout counter is 1.
#[test]
fn test_timeout() {
    let latch = CommitFreezeLatch::with_timeout(Duration::from_millis(10));
    let (p1, p2, _p3) = proposals();

    latch.freeze(p2);
    // Earlier event does not release waiters.
    latch.vlsn_event(&p1);

    assert!(!latch.await_thaw(), "older event must not thaw; await times out");
    assert_eq!(latch.stats().await_timeout_count, 1);
}

/// JE: `VLSNFreezeLatchTest.testElection`.
///
/// `freeze(p2)` then a SAME-round event `vlsnEvent(p2)` releases the waiter;
/// `awaitThaw()` returns `true` and the election counter is 1 — even though
/// the event arrived before the wait began.
#[test]
fn test_election() {
    let latch = CommitFreezeLatch::with_timeout(Duration::from_millis(10));
    let (_p1, p2, _p3) = proposals();

    latch.freeze(p2.clone());
    latch.vlsn_event(&p2);
    assert!(latch.await_thaw(), "same-round event must thaw the freeze");
    assert_eq!(latch.stats().await_election_count, 1);
}

/// JE: `VLSNFreezeLatchTest.testNewerElection`.
///
/// `freeze(p2)` then a NEWER event `vlsnEvent(p3)` releases the waiter;
/// `awaitThaw()` returns `true` and the election counter is 1.
#[test]
fn test_newer_election() {
    let latch = CommitFreezeLatch::with_timeout(Duration::from_millis(10));
    let (_p1, p2, p3) = proposals();

    latch.freeze(p2);
    latch.vlsn_event(&p3);
    assert!(latch.await_thaw(), "newer event must thaw the freeze");
    assert_eq!(latch.stats().await_election_count, 1);
}

/// JE: `VLSNFreezeLatchTest.testNoFreeze`.
///
/// No freeze in effect: `vlsnEvent(p1)` is a no-op, `awaitThaw()` returns
/// `false` immediately, and NEITHER counter moves (no timeout was in play).
#[test]
fn test_no_freeze() {
    let latch = CommitFreezeLatch::with_timeout(Duration::from_millis(10));
    let (p1, _p2, _p3) = proposals();

    latch.vlsn_event(&p1);

    assert!(!latch.await_thaw(), "no freeze -> await returns false");
    assert_eq!(latch.stats().await_timeout_count, 0);
}
