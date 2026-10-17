//! FID-ELECT-LEARNER (v8.0.0) — the elected node must become master even when
//! it did not run the winning round.
//!
//! **This test is `#[ignore]`d on v7.11.1**: it requires the JE Learner
//! broadcast, which Noxu does not yet have (adding a Result/learner wire
//! message is not patch-safe for a 7.11.1 patch — it lands in v8.0.0 with the
//! Learner protocol port).  It is committed here as a ready witness for the v8
//! work.
//!
//! In JE, after a proposer wins it broadcasts the winning proposal to ALL
//! learners (`Elections.java:774` → `Learner.informLearners`,
//! `Learner.java:496`); every node's `Learner.processResult` (`:141`) →
//! `MasterChangeListener` makes the CHOSEN node master, **even if that node did
//! not propose**.  Noxu's election driver (`replicated_environment.rs::
//! run_election_loop`) acts only when `winner == self` and has no learner
//! broadcast, so ANY chosen value that is not the proposer itself never
//! activates as master — the election stalls.  (This is the deeper root cause
//! behind the socket-address value-tiebreak stall analysed in `elect-rank.md`
//! §3: the address tiebreak merely makes "chosen value ≠ proposer" the common
//! case; the gap is the missing Learner broadcast, not the tiebreak key.)
//!
//! Scenario: a low-priority node (`low`, id 5) runs the election; a
//! higher-priority node (`high`, id 4) does NOT propose.  JE's priority
//! tiebreak makes `low` choose `high` as the Phase-2 value, so `low` learns
//! `Some(4)` and would become `high`'s replica.  Under a correct Learner
//! protocol, `high` is ALSO informed and becomes master.  Today Noxu has no
//! way to inform `high`, so `high` never becomes master.
//!
//! The assertion below is the END STATE a JE-faithful system guarantees: the
//! elected node (`high`) is master.  It is `#[ignore]`d until the Learner
//! broadcast exists (v8.0.0); the v8 worker removes the `#[ignore]` and wires
//! the learner path, at which point this witness goes GREEN.

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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[test]
#[ignore = "requires the JE Learner broadcast (v8.0.0, FID-ELECT-LEARNER)"]
fn fid_elect_learner_elected_non_proposer_becomes_master() {
    // Five nodes.  "high" (id 4) advertises priority 9 but never proposes.
    // "low" (id 5, priority 1) runs the only election.  a1,a2,a3 are plain
    // acceptors (VLSN 10, below the candidates' 100).
    let names = ["a1", "a2", "a3", "high", "low"];
    let prios = [1u32, 1, 1, 9, 1];

    // Shared flag: set true iff "high" learns it has become master.  With a
    // JE Learner broadcast, the winning proposer ("low") informs "high", whose
    // MasterChangeListener sets this.  Today nothing sets it (no learner).
    let high_is_master = Arc::new(AtomicBool::new(false));

    let mut dispatchers = Vec::new();
    let mut specs: Vec<(String, u16)> = Vec::new();
    for (i, name) in names.iter().enumerate() {
        let st =
            Arc::new(ElectionAcceptorState::new(name.to_string(), prios[i]));
        st.set_vlsn(if i >= 3 { 100 } else { 10 });
        st.set_term(0);
        let svc = Arc::new(ElectionService::new(st));
        let sd =
            TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
        sd.register(ELECTION_SERVICE_NAME, svc);
        let bound = sd.start().unwrap();
        specs.push((name.to_string(), bound.port()));
        dispatchers.push((sd, bound));
    }

    let mut group = RepGroup::new("fid-elect-learner".into(), 1);
    for (i, (name, port)) in specs.iter().enumerate() {
        group.add_node(RepNode::new(
            name.clone(),
            NodeType::Electable,
            "127.0.0.1".into(),
            *port,
            i as u32 + 1,
        ));
    }
    let group = Arc::new(group);
    std::thread::sleep(Duration::from_millis(5));

    let sockets: Vec<SocketAddr> =
        dispatchers.iter().map(|(_, b)| *b).collect();

    // "low" (id 5) runs the election; it must choose "high" (priority 9).
    let group_c = Arc::clone(&group);
    let winner = std::thread::spawn(move || {
        let mut channels: Vec<Arc<dyn Channel>> = Vec::new();
        for (i, a) in sockets.iter().enumerate() {
            if (i as u32 + 1) == 5 {
                continue;
            }
            if let Ok(ch) = connect_to_service(*a, ELECTION_SERVICE_NAME) {
                channels.push(Arc::new(ch));
            }
        }
        run_election(5, "low", &group_c, &channels, 100, 1, 5)
    })
    .join()
    .unwrap();

    // The winning VALUE is "high" (id 4) — "low" defers to the higher-priority
    // node.  (This half already works: the chooser picks "high".)
    assert_eq!(
        winner,
        Some(4),
        "low must elect the higher-priority non-proposer high (id 4)"
    );

    // The END STATE a JE-faithful system guarantees: "high" learns it is master
    // via the Learner broadcast.  Without the broadcast (v7.11.1) nothing sets
    // this flag, so the test fails here — hence #[ignore] until v8.0.0 wires
    // the Learner path (which flips `high_is_master`).
    //
    // The v8 worker: install a MasterChangeListener on each node that sets this
    // flag on processResult, and have the winning proposer informLearners.
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        high_is_master.load(Ordering::SeqCst),
        "FID-ELECT-LEARNER: the elected non-proposer 'high' never became \
         master — Noxu has no Learner broadcast, so a chosen value != the \
         proposer never activates. Fixed in v8.0.0 by porting \
         Elections.informLearners -> Learner.processResult -> \
         MasterChangeListener."
    );

    for (sd, _) in &dispatchers {
        sd.stop();
    }
}
