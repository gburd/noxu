//! FID-ELECT-1 split-brain DETERMINISTIC witness (rc2, JE 3-node topology).
//!
//! rc2 keys the acceptor on `(term, proposer)` and carries a PER-ACCEPTOR
//! value guard ("no different value at an accepted term", `try_accept_proposal`
//! in acceptor_state.rs).  But the proposer historically added a FREE synthetic
//! self-vote (`self_needed = 1`) that NEVER consulted its own acceptor, so the
//! value guard could not protect a node's own acceptor from a value the node
//! never routed through it.  In the JE 3-node topology {A,B,C} (every electable
//! node is BOTH a proposer and an acceptor) this is a split brain at one term:
//!   * C's acceptor accepts (1, A); the guard then REJECTS (1, B) on C.
//!   * A's acceptor accepts (1, B) — A never accepted its OWN value (its free
//!     self-vote bypassed its acceptor), so the guard does not fire on A.
//!   * A wins free-self + C; B wins free-self + A's acceptor -> TWO masters,
//!     with NO acceptor accepting two values.
//!
//! The fix (f49d5db6-equivalent, patch-safe) routes the proposer's own vote
//! through its own `PersistentAcceptorState` (JE: self is in
//! getAllAcceptorSockets, RepGroupImpl.java:1296).  This witness forces exactly
//! the losing case: node A's OWN acceptor has already promised a strictly
//! higher number `(1, "B")` (as if B's phase 1 reached A's acceptor first); A
//! then runs a full election in which two external acceptors would grant A's
//! `(1, "A")`.  With the fix A's self-vote is denied (its own acceptor promised
//! higher), so A reaches only 2 < quorum 3 and must return None.  Without the
//! fix the free self-vote makes 3 and A (wrongly) wins.

use noxu_rep::elections::PersistentAcceptorState;
use noxu_rep::elections::paxos::{
    run_acceptor_with_state, run_election_with_phi_dtvlsn_self,
};
use noxu_rep::net::channel::{Channel, LocalChannelPair};
use noxu_rep::node_type::NodeType;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;

use std::sync::Arc;
use std::time::Duration;

fn make_group(names: &[(&str, u16)]) -> RepGroup {
    let mut g = RepGroup::new("fid-elect-1".into(), 1);
    for (idx, (name, port)) in names.iter().enumerate() {
        g.add_node(RepNode::new(
            (*name).into(),
            NodeType::Electable,
            "127.0.0.1".into(),
            *port,
            idx as u32 + 1,
        ));
    }
    g
}

/// A fresh in-process acceptor on one channel end; returns the proposer end.
fn spawn_acceptor(
    name: &'static str,
) -> (Arc<dyn Channel>, std::thread::JoinHandle<()>) {
    let pair = LocalChannelPair::new();
    let acc_end: Arc<dyn Channel> = Arc::new(pair.channel_a);
    let prop_end: Arc<dyn Channel> = Arc::new(pair.channel_b);
    let state = Arc::new(PersistentAcceptorState::in_memory());
    let h = std::thread::spawn(move || {
        let _ = run_acceptor_with_state(
            acc_end.as_ref(),
            name,
            10,
            1,
            1,
            0,
            &state,
            None,
        );
    });
    (prop_end, h)
}

#[test]
fn fid_elect_1_proposer_with_higher_promised_self_must_not_win() {
    // JE 3-node group {A, B, C}: quorum = majority(3) = 2.
    let group = make_group(&[("A", 60001), ("B", 60002), ("C", 60003)]);
    assert_eq!(group.phase2_quorum(), 2, "3-node SimpleMajority quorum is 2");

    // A's OWN acceptor has already promised the strictly-higher (1, "B") —
    // exactly what happens when a concurrent higher proposer B ran phase 1
    // against A's acceptor first (JE: self is in getAllAcceptorSockets).
    let self_state = Arc::new(PersistentAcceptorState::in_memory());
    assert!(
        self_state.try_promise_proposal(1, "B"),
        "pre-promise A's own acceptor to (1, B)"
    );

    // One external acceptor (B) that WILL promise+accept A's (1, "A").
    let (ch_b, hb) = spawn_acceptor("B");
    let channels = vec![ch_b];

    let winner = run_election_with_phi_dtvlsn_self(
        1,
        "A",
        &group,
        &channels,
        100, // proposed_vlsn (beats acceptors' 10)
        1,   // priority
        1,   // term
        0,
        None,
        Duration::from_millis(500),
        Some(&self_state),
    );
    let _ = hb.join();

    // A got 1 external accept (B) but its OWN acceptor already promised (1, B),
    // so its self-accept of (1, A) is denied: 1 < quorum 2 -> no master.
    // RED before the fix: the free synthetic self-vote made 2 and A "won".
    assert_eq!(
        winner, None,
        "a proposer whose own acceptor already promised a strictly-higher \
         number must NOT reach quorum (JE: self-Accept rejected, self-vote \
         lost); a non-None winner is the FID-ELECT-1 free-self-vote split brain"
    );
}
