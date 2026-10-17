//! FID-ELECT-1 regression witnesses (ported from the rep-core fidelity audit).
//!
//! The split-brain hole: Noxu's election "proposal number" was a
//! per-node-local `term: u64` counter (NOT a globally-unique, ascending
//! proposal number like JE `TimebasedProposalGenerator` =
//! `ms_time|machineId|uniqueId`).  The Paxos acceptor keyed promise/accept on
//! the bare `term`, so two DIFFERENT proposers that both used the same term
//! value could each be promised AND accepted at that term on the SAME
//! acceptor -> two masters.
//!
//! JE comparison (`Acceptor.java:177-185` Propose, `:210-215` Accept):
//!   - Propose rejected only if `promisedProposal.compareTo(propose) > 0`
//!   - Accept  granted only if `promisedProposal.compareTo(accept) == 0`
//!
//! Because proposal numbers are globally unique AND ascending (JE `ms_time`),
//! a second proposer's Propose either overwrites the promise (its unique
//! number differs) and the 1st's Accept is then rejected, or the 2nd's
//! Propose is rejected.  Never both.
//!
//! The fix (see `elections::acceptor_state` and
//! `elections::proposal_generator`):
//!   1. the proposal number is `(term, proposer)` and the acceptor compares
//!      the WHOLE pair (JE's full-proposal compare);
//!   2. the driver's `term` is drawn from a `TimebasedProposalGenerator`
//!      (JE ms_time), so it is globally monotone and two proposers never
//!      collide; and
//!   3. because Noxu's `RankingProposer` does not adopt a previously-accepted
//!      value (JE's base `Proposer` does), the acceptor additionally refuses
//!      to accept a DIFFERENT value at an already-accepted term.
//!
//! These two tests were RED on the audited RC (81b2f048) and are GREEN after
//! the fix.  They are the auditor's L1 (acceptor state) and L2 (two concurrent
//! sessions) witnesses, ported to the fixed `(term, proposer)` acceptor API.

use noxu_rep::elections::PersistentAcceptorState;
use noxu_rep::elections::paxos::run_acceptor_with_state;
use noxu_rep::net::channel::{Channel, LocalChannelPair};
use noxu_rep::protocol::ProtocolMessage;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// LEVEL 1 (acceptor state): a single acceptor state must NOT accept TWO
/// DIFFERENT masters at the SAME term.
///
/// Both proposers A and B drive term 1 (as every node's local counter once
/// did), with distinct proposer identities.  A promises and accepts at
/// `(1, node-A)`.  B then re-promises the shared acceptor at `(1, node-B)`
/// (`(1,B) > (1,A)`, so JE's Propose rule re-promises).  B's Accept must be
/// REJECTED: the acceptor has already accepted a different value (node-A) at
/// term 1, and Noxu does not adopt accepted values.  The accepted master
/// stays node-A — no split brain.
#[test]
fn fid_elect_1_acceptor_rejects_second_master_same_term() {
    let st = PersistentAcceptorState::in_memory();

    // Proposer A promises + accepts at (1, node-A).
    assert!(st.try_promise_proposal(1, "node-A"), "A promise at (1, node-A)");
    assert!(
        st.try_accept_proposal(1, "node-A", "node-A"),
        "A accepted at (1, node-A)"
    );
    assert_eq!(st.accepted_master().as_deref(), Some("node-A"));

    // Proposer B, using the SAME local term value 1 (its own counter also
    // started at 1), re-promises + tries to accept at (1, node-B).
    assert!(
        st.try_promise_proposal(1, "node-B"),
        "B re-promise at (1, node-B)"
    );
    let b_accepted = st.try_accept_proposal(1, "node-B", "node-B");

    // JE-faithful invariant: an acceptor that already accepted node-A at
    // term 1 must NOT also accept node-B at term 1.
    assert!(
        !b_accepted && st.accepted_master().as_deref() == Some("node-A"),
        "FID-ELECT-1: acceptor accepted a SECOND, DIFFERENT master (node-B) \
         at the same term 1 it already accepted node-A for (accepted_master \
         now = {:?}). This is the split-brain hole.",
        st.accepted_master(),
    );
}

/// LEVEL 2 (protocol, concurrent sessions): two concurrent acceptor sessions
/// on ONE shared `PersistentAcceptorState` (as the service dispatcher runs,
/// one handler per inbound proposer connection) must NOT both grant
/// `granted:true` to two DIFFERENT masters at the same term.  Each
/// `granted:true` is a phase-2 vote the proposer counts toward quorum, so two
/// granted proposers at the same term is split brain.
#[test]
fn fid_elect_1_two_sessions_at_most_one_master_same_term() {
    // Run the two sessions many times with varied interleavings (the OS
    // scheduler decides the order); every run must grant at most one.
    for _ in 0..200 {
        let state = Arc::new(PersistentAcceptorState::in_memory());

        let pair_a = LocalChannelPair::new();
        let pair_b = LocalChannelPair::new();
        let acc_a: Arc<dyn Channel> = Arc::new(pair_a.channel_a);
        let prop_a: Arc<dyn Channel> = Arc::new(pair_a.channel_b);
        let acc_b: Arc<dyn Channel> = Arc::new(pair_b.channel_a);
        let prop_b: Arc<dyn Channel> = Arc::new(pair_b.channel_b);

        // Two acceptor sessions sharing the SAME state.
        let st_a = Arc::clone(&state);
        let st_b = Arc::clone(&state);
        let h_a = thread::spawn(move || {
            run_acceptor_with_state(
                &*acc_a,
                "acceptor-X",
                50,
                1,
                0,
                0,
                &st_a,
                None,
            )
        });
        let h_b = thread::spawn(move || {
            run_acceptor_with_state(
                &*acc_b,
                "acceptor-X",
                50,
                1,
                0,
                0,
                &st_b,
                None,
            )
        });

        // Drive both proposers at the SAME term=1 with DIFFERENT master
        // values.  Each proposer proposes itself (proposer == master).
        let drive = |ch: &Arc<dyn Channel>, master: &str| -> bool {
            ch.send(
                &ProtocolMessage::ElectionProposal {
                    node_name: master.to_string(),
                    vlsn: 100,
                    priority: 1,
                    term: 1,
                    dtvlsn: 0,
                }
                .encode(),
            )
            .unwrap();
            let _promise = ch.receive(Duration::from_secs(2)).unwrap();
            ch.send(
                &ProtocolMessage::ElectionResult {
                    master: master.to_string(),
                    term: 1,
                }
                .encode(),
            )
            .unwrap();
            let vote = ch.receive(Duration::from_secs(2)).unwrap();
            matches!(
                vote.map(|b| ProtocolMessage::decode(&b).unwrap()),
                Some(ProtocolMessage::ElectionVote { granted: true, .. })
            )
        };

        // Run the two proposers concurrently so the acceptor sessions
        // interleave arbitrarily.
        let prop_a2 = Arc::clone(&prop_a);
        let prop_b2 = Arc::clone(&prop_b);
        let d_a = thread::spawn(move || drive(&prop_a2, "node-A"));
        let d_b = thread::spawn(move || drive(&prop_b2, "node-B"));
        let granted_a = d_a.join().unwrap();
        let granted_b = d_b.join().unwrap();

        let _ = h_a.join().unwrap();
        let _ = h_b.join().unwrap();

        assert!(
            !(granted_a && granted_b),
            "FID-ELECT-1: acceptor-X granted phase-2 accepts to BOTH node-A \
             and node-B at the same term 1 (granted_a={granted_a}, \
             granted_b={granted_b}) -> split brain."
        );
    }
}
