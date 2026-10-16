//! FID-ELECT-1: 3-node concurrent-election safety.
//!
//! Two proposer nodes run `run_election` CONCURRENTLY against the SAME three
//! acceptor nodes, in a tied situation (identical VLSN / priority) AND at the
//! SAME election term — exactly the collision that the per-node-local term
//! counter produced (every node's driver started at term 1).  The fix must
//! guarantee **at most one master per election**: with the `(term, proposer)`
//! proposal number and the acceptor's "no different value at an accepted
//! term" guard, two proposers sharing a term can never both reach a phase-2
//! quorum on the shared acceptors.
//!
//! This is run 200 times to shake out scheduler-dependent interleavings
//! (the auditor's requirement), on top of the deterministic L1/L2 witnesses
//! in `fid_elect_1_witness.rs`.

use noxu_rep::elections::ElectionAcceptorState;
use noxu_rep::elections::election_service::{
    ELECTION_SERVICE_NAME, ElectionService,
};
use noxu_rep::elections::paxos::run_election;
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

/// Build a 5-member group view: three acceptor nodes (a1,a2,a3) plus the two
/// proposer nodes (p-A, p-B), so each proposer's quorum is computed over a
/// stable 5-node group.  phase1/phase2 quorum = 3.
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
        // Three acceptor nodes, each with its own persistent-backed state.
        // Give each a DISTINCT lower VLSN so neither proposer is out-ranked
        // by an acceptor's own suggestion (proposers tie at the top).
        let mut dispatchers = Vec::new();
        let mut acc_addrs: Vec<(String, u16)> = Vec::new();
        for i in 0..3 {
            let name = format!("a{}", i + 1);
            let st = Arc::new(ElectionAcceptorState::new(name.clone(), 1));
            st.set_vlsn(10); // below the proposers' 100
            st.set_term(1);
            let svc = Arc::new(ElectionService::new(st));
            let sd = TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap())
                .unwrap();
            sd.register(ELECTION_SERVICE_NAME, svc);
            let bound = sd.start().unwrap();
            acc_addrs.push((name, bound.port()));
            dispatchers.push((sd, bound));
        }

        // The two proposers share the group view with the acceptors.
        let mut group_specs = acc_addrs.clone();
        group_specs.push(("p-A".into(), 60001));
        group_specs.push(("p-B".into(), 60002));
        let group = Arc::new(make_group(&group_specs));

        std::thread::sleep(Duration::from_millis(5));

        let acc_addr_list: Vec<SocketAddr> =
            dispatchers.iter().map(|(_, b)| *b).collect();

        // Each proposer dials all three acceptors and runs an election at the
        // SAME term (1), SAME VLSN (100), SAME priority (1) — a dead tie.
        let masters = Arc::new(AtomicUsize::new(0));
        let run_proposer = |node_id: u32, name: &'static str| {
            let group = Arc::clone(&group);
            let addrs = acc_addr_list.clone();
            let masters = Arc::clone(&masters);
            std::thread::spawn(move || {
                let mut channels: Vec<Arc<dyn Channel>> = Vec::new();
                for a in &addrs {
                    if let Ok(ch) =
                        connect_to_service(*a, ELECTION_SERVICE_NAME)
                    {
                        channels.push(Arc::new(ch));
                    }
                }
                // term=1 for BOTH proposers (the collision the fix must
                // tolerate).  Winner is Some(id) iff this proposer reached a
                // phase-2 quorum of accepts.
                let winner =
                    run_election(node_id, name, &group, &channels, 100, 1, 1);
                if winner == Some(node_id) {
                    masters.fetch_add(1, Ordering::SeqCst);
                }
            })
        };

        let ha = run_proposer(4, "p-A");
        let hb = run_proposer(5, "p-B");
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
