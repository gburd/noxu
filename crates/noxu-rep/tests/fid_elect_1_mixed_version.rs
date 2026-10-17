//! FID-ELECT-1 follow-up: mixed-version election safety.
//!
//! Paxos safety does not depend on the Phase-2 value *ranking* — it is
//! enforced by the acceptor (the `(term, proposer)` promise/accept keying plus
//! the interim value guard), which is identical on a v7.11.0 node and a
//! v7.11.1 node.  The value ranking only decides WHICH value a proposer
//! proposes, never whether two proposers can both reach a Phase-2 quorum.
//!
//! This witness runs a mixed group where one candidate ranks its Phase-2 value
//! with the pre-fix v7.11.0 order (term BEFORE node name — the removed
//! tiebreak, modelled via the `paxos::test_support` thread-local switch) and
//! the other ranks with the fixed v7.11.1 order, racing the same three real
//! acceptors.  200x: at most one master per election.
//!
//! (The "old" node is modelled at the value-ranking layer only; the on-disk
//! and wire formats are unchanged by this fix, so a real 7.11.0 and 7.11.1
//! node already interoperate byte-for-byte — the only behavioural difference
//! between the two versions is exactly this value ranking.)

use noxu_rep::elections::ElectionAcceptorState;
use noxu_rep::elections::election_service::{
    ELECTION_SERVICE_NAME, ElectionService,
};
use noxu_rep::elections::paxos::{run_election, test_support};
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

fn make_group(addrs: &[(String, u16)]) -> RepGroup {
    let mut g = RepGroup::new("fid-elect-1-mixed".into(), 1);
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
fn fid_elect_1_mixed_version_group_at_most_one_master() {
    const ITERS: usize = 200;

    for iter in 0..ITERS {
        // Five nodes each run an ElectionService.  a1,a2,a3 are plain
        // acceptors (suggestion VLSN 10).  p-A (id 4) is the "old" v7.11.0
        // node (ranks with term before name); p-B (id 5) is the "new" v7.11.1
        // node (fixed ranking).  Both candidates are tied at VLSN 100,
        // priority 1; acceptors advertise a stale own_term 0 so the removed
        // term tiebreak is active on the old node.
        let node_names =
            ["a1", "a2", "a3", "p-A", "p-B"].map(|s| s.to_string());
        let mut dispatchers = Vec::new();
        let mut addrs: Vec<(String, u16)> = Vec::new();
        for (i, name) in node_names.iter().enumerate() {
            let st = Arc::new(ElectionAcceptorState::new(name.clone(), 1));
            st.set_vlsn(if i >= 3 { 100 } else { 10 });
            st.set_term(0);
            let svc = Arc::new(ElectionService::new(st));
            let sd = TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap())
                .unwrap();
            sd.register(ELECTION_SERVICE_NAME, svc);
            let bound = sd.start().unwrap();
            addrs.push((name.clone(), bound.port()));
            dispatchers.push((sd, bound));
        }

        let group = Arc::new(make_group(&addrs));
        std::thread::sleep(Duration::from_millis(5));

        let sockets: Vec<SocketAddr> =
            dispatchers.iter().map(|(_, b)| *b).collect();

        let masters = Arc::new(AtomicUsize::new(0));
        let run_candidate = |node_id: u32, name: &'static str, old: bool| {
            let group = Arc::clone(&group);
            let sockets = sockets.clone();
            let masters = Arc::clone(&masters);
            std::thread::spawn(move || {
                // The "old" node ranks its Phase-2 value with the pre-fix
                // v7.11.0 order on THIS proposer thread.
                test_support::set_v7110_ranking(old);
                let mut channels: Vec<Arc<dyn Channel>> = Vec::new();
                for (i, a) in sockets.iter().enumerate() {
                    if (i as u32 + 1) == node_id {
                        continue; // full mesh, skip self
                    }
                    if let Ok(ch) =
                        connect_to_service(*a, ELECTION_SERVICE_NAME)
                    {
                        channels.push(Arc::new(ch));
                    }
                }
                // Both candidates run at the SAME term 1 (the original
                // collision) so both pass phase 1 and the value ranking
                // actually decides the proposed value.
                let winner =
                    run_election(node_id, name, &group, &channels, 100, 1, 1);
                if winner == Some(node_id) {
                    masters.fetch_add(1, Ordering::SeqCst);
                }
                test_support::set_v7110_ranking(false);
            })
        };

        let ha = run_candidate(4, "p-A", true); // old (v7.11.0) ranking
        let hb = run_candidate(5, "p-B", false); // new (v7.11.1) ranking
        ha.join().unwrap();
        hb.join().unwrap();

        let count = masters.load(Ordering::SeqCst);
        assert!(
            count <= 1,
            "iter {iter}: {count} masters elected in a mixed v7.11.0/v7.11.1 \
             group (expected at most 1 — Paxos safety must not depend on the \
             value ranking)"
        );

        for (sd, _) in &dispatchers {
            sd.stop();
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}
