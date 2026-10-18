//! FID-ELECT-1: 3-node concurrent-election safety (JE topology).
//!
//! Three nodes {A, B, C}, each both a PROPOSER and an ACCEPTOR (JE: every
//! electable member runs an Acceptor and the proposer dials
//! `getAllAcceptorSockets()`, which includes self, RepGroupImpl.java:1296).
//! Two proposers A and B run a full Paxos election CONCURRENTLY at the SAME
//! term (1), SAME VLSN (100), SAME priority (1) — a dead tie — against the
//! shared acceptors.  The fix must guarantee **at most one master per
//! election**.
//!
//! ## Why the JE topology (FID-ELECT-1 split brain)
//!
//! rc2 keys the acceptor on `(term, proposer)` and carries a PER-ACCEPTOR
//! value guard ("no different value at an accepted term").  But the proposer's
//! own vote must NOT be a free synthetic `+1`: JE routes it through the node's
//! own `Acceptor` (self is in `getAllAcceptorSockets`), so a concurrent higher
//! proposal that bumped this node's own promise denies its self-vote.  Without
//! that interlock, the per-acceptor guard cannot protect a node's own acceptor
//! from a value the node itself never routed through it, and {A,B,C} split
//! brain at one term (C accepts A, A's acceptor accepts B, A wins free-self+C,
//! B wins free-self+A).  So each proposer here dials the other two electable
//! peers AND routes its own vote through its own `PersistentAcceptorState`
//! (`run_election_with_phi_dtvlsn_self(.., Some(self_state))`).
//!
//! Run 200 times to shake out scheduler-dependent interleavings (the auditor's
//! requirement), on top of the deterministic witness in
//! `fid_elect_1_split_brain_witness.rs`.

use noxu_rep::elections::ElectionAcceptorState;
use noxu_rep::elections::election_service::{
    ELECTION_SERVICE_NAME, ElectionService,
};
use noxu_rep::elections::paxos::run_election_with_phi_dtvlsn_self;
use noxu_rep::net::channel::Channel;
use noxu_rep::net::service_dispatcher::{
    TcpServiceDispatcher, connect_to_service,
};
use noxu_rep::node_type::NodeType;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Build the 3-member electable group {A, B, C}; SimpleMajority quorum = 2.
fn make_group(addrs: &[(String, u16)]) -> RepGroup {
    let mut g = RepGroup::new("fid-elect-1".into(), 1);
    for (idx, (name, port)) in addrs.iter().enumerate() {
        g.add_node(RepNode::new(
            name.clone(),
            NodeType::Electable,
            "127.0.0.1".into(),
            *port,
            idx as u32 + 1,
        ));
    }
    g
}

#[test]
fn fid_elect_1_three_node_concurrent_election_at_most_one_master() {
    const ITERS: usize = 200;

    for iter in 0..ITERS {
        // Three electable nodes, each running a live ELECTION acceptor service
        // with its own persistent-backed state (JE: every electable member is
        // an Acceptor).  A and B are the two concurrent proposers; C is a plain
        // acceptor.  Give each acceptor a DISTINCT lower VLSN so neither
        // proposer is out-ranked by an acceptor's own suggestion.
        let names = ["A", "B", "C"];
        let mut dispatchers = Vec::new();
        let mut states: Vec<Arc<ElectionAcceptorState>> = Vec::new();
        let mut addrs: Vec<(String, u16)> = Vec::new();
        for name in names {
            let st = Arc::new(ElectionAcceptorState::new(name.to_string(), 1));
            st.set_vlsn(10); // below the proposers' 100
            st.set_term(1);
            let svc = Arc::new(ElectionService::new(Arc::clone(&st)));
            let sd = TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap())
                .unwrap();
            sd.register(ELECTION_SERVICE_NAME, svc);
            let bound = sd.start().unwrap();
            addrs.push((name.to_string(), bound.port()));
            states.push(st);
            dispatchers.push((sd, bound));
        }

        let group = Arc::new(make_group(&addrs));

        // Let every acceptor listener bind before the proposers dial.
        std::thread::sleep(Duration::from_millis(5));

        let addr_by_name: Vec<(String, SocketAddr)> = dispatchers
            .iter()
            .enumerate()
            .map(|(i, (_, b))| (names[i].to_string(), *b))
            .collect();

        let masters = Arc::new(AtomicUsize::new(0));
        let run_proposer =
            |node_id: u32,
             name: &'static str,
             self_state: Arc<ElectionAcceptorState>| {
                let group = Arc::clone(&group);
                let addr_by_name = addr_by_name.clone();
                let masters = Arc::clone(&masters);
                std::thread::spawn(move || {
                    // Dial every electable peer EXCEPT self (self votes through its
                    // own acceptor state, JE self-in-acceptor-sockets).
                    let mut channels: Vec<Arc<dyn Channel>> = Vec::new();
                    for (peer, a) in &addr_by_name {
                        if peer == name {
                            continue;
                        }
                        if let Ok(ch) =
                            connect_to_service(*a, ELECTION_SERVICE_NAME)
                        {
                            channels.push(Arc::new(ch));
                        }
                    }
                    // term=1 for BOTH proposers (the collision the fix must
                    // tolerate).  The proposer's own vote routes through its OWN
                    // acceptor state; a concurrent higher proposal that bumped this
                    // node's own promise denies the self-vote.  Winner is Some(id)
                    // iff this proposer reached a phase-2 quorum of REAL accepts
                    // for its own id.
                    let winner = run_election_with_phi_dtvlsn_self(
                        node_id,
                        name,
                        &group,
                        &channels,
                        100,
                        1,
                        1,
                        0,
                        None,
                        Duration::from_millis(500),
                        Some(&self_state.persistent),
                    );
                    if winner == Some(node_id) {
                        masters.fetch_add(1, Ordering::SeqCst);
                    }
                })
            };

        // states[0]=A (id 1), states[1]=B (id 2) are the two proposers.
        let ha = run_proposer(1, "A", Arc::clone(&states[0]));
        let hb = run_proposer(2, "B", Arc::clone(&states[1]));
        ha.join().unwrap();
        hb.join().unwrap();

        let count = masters.load(Ordering::SeqCst);
        assert!(
            count <= 1,
            "FID-ELECT-1 iter {iter}: {count} proposers became master at \
             term 1 (expected at most 1 — split brain if 2)"
        );

        for (sd, _) in &dispatchers {
            sd.stop();
        }
        // Let per-connection acceptor threads drain before the next iter.
        std::thread::sleep(Duration::from_millis(2));
    }
}
