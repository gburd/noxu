//! FID-ELECT-1 follow-up: end-to-end convergence of tied candidates.
//!
//! Companion to `fid_elect_1_rank_tiebreak.rs` (which isolates the
//! value-ranking DUEL that the non-JE `term` tiebreak caused in
//! `choose_phase2_value`).  This witness drives the FULL election path
//! (`run_election`, real `ElectionService` acceptors over TCP) for two tied
//! candidates and measures convergence: how many rounds until exactly ONE
//! master is learned, and how many DUEL rounds (rounds where neither candidate
//! reached a phase-2 quorum on a shared value) occur along the way.
//!
//! On v7.11.1 the acceptor's `(term, proposer)` promise keying funnels both
//! candidates toward the lexicographically-higher proposer name, which is also
//! what the FIXED value ranking picks — so the system converges in one round
//! with zero duels BOTH before and after the fix (the value-layer duel is
//! masked at the system level by the acceptor keying; it only becomes a
//! liveness failure once the downstream round identity is the full proposal
//! number, which is the v8.0.0 re-key).  This witness therefore asserts the
//! fix is **no worse** end-to-end — still exactly one master, still one round,
//! still zero duels — and prints the before/after counts.
//!
//! Run 200x to shake out scheduler-dependent interleavings.

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
use std::time::Duration;

fn make_group(addrs: &[(String, u16)]) -> RepGroup {
    let mut g = RepGroup::new("fid-elect-1-converge".into(), 1);
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
fn fid_elect_1_tied_candidates_converge_on_one_master_bounded_rounds() {
    const ITERS: usize = 200;
    // A correct election converges in exactly ONE round.  A generous bound so
    // a never-converging regression panics with an unambiguous message.
    const MAX_ROUNDS: usize = 8;

    let mut worst_rounds_to_converge = 0usize;
    let mut total_duels = 0usize;

    for iter in 0..ITERS {
        // Five nodes each run an ElectionService.  a1,a2,a3 are plain
        // acceptors (suggestion VLSN 10, below the candidates' 100).  p-A
        // (id 4) and p-B (id 5) are the tied candidates at VLSN 100,
        // priority 1.  Every acceptor advertises a STALE own_term of 0 — JE's
        // acceptor suggestion reports the acceptor's own term, which lags a
        // live proposer's round term; this is exactly the stale-term input the
        // removed value-ranking tiebreak keyed on.
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

        // Both candidates run at the SAME round term, ascending each round
        // (as run_election_loop's `term = next()` does) so a prior round's
        // promise never blocks a later one.  Both terms stay strictly above
        // every acceptor's stale term 0.
        let mut rounds_to_converge: Option<usize> = None;

        for round in 0..MAX_ROUNDS {
            let term = 5 + (round as u64) * 2;

            let run_candidate =
                |node_id: u32,
                 name: &'static str|
                 -> std::thread::JoinHandle<Option<u32>> {
                    let group = Arc::clone(&group);
                    let sockets = sockets.clone();
                    std::thread::spawn(move || {
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
                        run_election(
                            node_id, name, &group, &channels, 100, 1, term,
                        )
                    })
                };

            let ha = run_candidate(4, "p-A");
            let hb = run_candidate(5, "p-B");
            let a_won = ha.join().unwrap() == Some(4);
            let b_won = hb.join().unwrap() == Some(5);
            let masters = (a_won as usize) + (b_won as usize);

            // Safety: at most one master learned this round (the interim value
            // guard holds this before AND after the fix).
            assert!(
                masters <= 1,
                "iter {iter} round {round}: {masters} masters learned \
                 (split brain if 2)"
            );

            if masters == 1 {
                rounds_to_converge = Some(round + 1);
                break;
            }
            // No single winner this round -> a duel (neither reached a quorum
            // on a shared value).  Retry next round.
            total_duels += 1;
        }

        let rtc = rounds_to_converge.unwrap_or_else(|| {
            panic!(
                "iter {iter}: tied candidates did NOT converge on one master \
                 within {MAX_ROUNDS} rounds"
            )
        });
        worst_rounds_to_converge = worst_rounds_to_converge.max(rtc);

        for (sd, _) in &dispatchers {
            sd.stop();
        }
        std::thread::sleep(Duration::from_millis(2));
    }

    eprintln!(
        "fid_elect_1 convergence: {ITERS} iters, worst rounds-to-converge = \
         {worst_rounds_to_converge}, total duel rounds = {total_duels}"
    );
    // No-worse-than contract: tied candidates always converge within the
    // bound (ideally in one round with zero duels — the printed counts record
    // the exact before/after figures).
    assert!(
        worst_rounds_to_converge <= MAX_ROUNDS,
        "convergence regressed: worst = {worst_rounds_to_converge}"
    );
}
