//! Election service handler for the TCP dispatcher.
//!
//! Registers `ELECTION_SERVICE_NAME` ("ELECTION") on the
//! `TcpServiceDispatcher`. Incoming connections requesting this
//! service are passed to [`crate::elections::paxos::run_acceptor`]
//! using the local node's current VLSN, priority, and term.
//!
//! Closes finding F6 of `docs/src/internal/api-audit-2026-05-rep.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::elections::acceptor_state::PersistentAcceptorState;
use crate::elections::paxos::run_acceptor_with_state;
use crate::error::Result;
use crate::net::channel::Channel;
use crate::net::service_dispatcher::ServiceHandler;

/// Service name registered with `TcpServiceDispatcher` for Paxos
/// elections.
pub const ELECTION_SERVICE_NAME: &str = "ELECTION";

/// State shared between the election driver (proposer) and the
/// dispatcher-side acceptor.
///
/// All fields are updated atomically; the driver writes them as it
/// progresses through election rounds, and the acceptor reads them on
/// every incoming proposal so its replies always reflect the local
/// node's most recent state.
pub struct ElectionAcceptorState {
    /// This node's name (passed to `run_acceptor`).
    pub node_name: String,
    /// Current VLSN this node would advertise as a candidate.
    pub own_vlsn: AtomicU64,
    /// This node's election priority (immutable).
    pub own_priority: u32,
    /// Current election term as observed by the local driver.
    pub own_term: AtomicU64,
    /// Crash-durable acceptor state (promised_term, accepted_term,
    /// accepted_master).  Closes findings F5/F31 of the May 2026
    /// noxu-rep audit.  When `env_home` is `None` (test harness, in-memory
    /// configurations), this falls back to in-memory-only mode.
    pub persistent: Arc<PersistentAcceptorState>,
}

impl ElectionAcceptorState {
    /// Create a new acceptor state for `node_name` with the given
    /// fixed priority.  Persistence is disabled (in-memory only);
    /// callers that need crash-durable promises should use
    /// `with_env_home`.
    pub fn new(node_name: String, own_priority: u32) -> Self {
        Self {
            node_name,
            own_vlsn: AtomicU64::new(0),
            own_priority,
            own_term: AtomicU64::new(0),
            persistent: Arc::new(PersistentAcceptorState::in_memory()),
        }
    }

    /// Create a new acceptor state whose Paxos promises are persisted
    /// to `<env_home>/acceptor.state`.  Closes findings F5/F31.
    pub fn with_env_home(
        node_name: String,
        own_priority: u32,
        env_home: &std::path::Path,
    ) -> Self {
        Self {
            node_name,
            own_vlsn: AtomicU64::new(0),
            own_priority,
            own_term: AtomicU64::new(0),
            persistent: Arc::new(PersistentAcceptorState::load_or_default(
                env_home,
            )),
        }
    }

    /// Update the VLSN that subsequent acceptor sessions will report.
    pub fn set_vlsn(&self, vlsn: u64) {
        self.own_vlsn.store(vlsn, Ordering::SeqCst);
    }

    /// Update the term that subsequent acceptor sessions will report.
    pub fn set_term(&self, term: u64) {
        self.own_term.store(term, Ordering::SeqCst);
    }

    /// Snapshot (vlsn, priority, term) for a single acceptor call.
    pub fn snapshot(&self) -> (u64, u32, u64) {
        (
            self.own_vlsn.load(Ordering::SeqCst),
            self.own_priority,
            self.own_term.load(Ordering::SeqCst),
        )
    }
}

/// Service handler that hosts the Paxos acceptor side of an election.
pub struct ElectionService {
    state: Arc<ElectionAcceptorState>,
}

impl ElectionService {
    /// Create a new election service backed by `state`.
    pub fn new(state: Arc<ElectionAcceptorState>) -> Self {
        Self { state }
    }
}

impl ServiceHandler for ElectionService {
    fn handle(&self, channel: Box<dyn Channel>) -> Result<()> {
        let (vlsn, priority, term) = self.state.snapshot();
        // F5/F31: route the acceptor through the persistent state so
        // promises and accepts survive process restarts.
        match run_acceptor_with_state(
            &*channel,
            &self.state.node_name,
            vlsn,
            priority,
            term,
            &self.state.persistent,
        ) {
            Ok(_) => Ok(()),
            Err(e) => {
                log::debug!("ELECTION service: acceptor returned error: {}", e);
                Ok(())
            }
        }
    }

    fn service_name(&self) -> &str {
        ELECTION_SERVICE_NAME
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::elections::paxos::run_election;
    use crate::net::channel::TcpChannel;
    use crate::net::service_dispatcher::{
        TcpServiceDispatcher, connect_to_service,
    };
    use crate::node_type::NodeType;
    use crate::rep_group::RepGroup;
    use crate::rep_node::RepNode;

    use std::sync::Arc;

    fn make_group_2(self_name: &str, peer_name: &str) -> RepGroup {
        let mut g = RepGroup::new("g".into(), 1);
        g.add_node(RepNode::new(
            self_name.into(),
            NodeType::Electable,
            "127.0.0.1".into(),
            5_001,
            1,
        ));
        g.add_node(RepNode::new(
            peer_name.into(),
            NodeType::Electable,
            "127.0.0.1".into(),
            5_002,
            2,
        ));
        g
    }

    #[test]
    fn election_service_handles_acceptor_round_trip() {
        // Spin up an ELECTION service and a peer that runs run_election
        // against it.  Quorum = 2 in a 2-node group; proposer self-vote
        // + 1 peer promise = 2.
        let acceptor_state =
            Arc::new(ElectionAcceptorState::new("peer".into(), 1));
        acceptor_state.set_vlsn(50);
        acceptor_state.set_term(1);
        let svc = Arc::new(ElectionService::new(acceptor_state));

        let sd =
            TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
        sd.register(ELECTION_SERVICE_NAME, svc);
        let bound = sd.start().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));

        let group = make_group_2("self", "peer");
        let ch = connect_to_service(bound, ELECTION_SERVICE_NAME).unwrap();
        let ch_arc: Arc<dyn Channel> = Arc::new(ch);

        // Self has higher VLSN (100 > 50) → self wins.
        let winner = run_election(1, "self", &group, &[ch_arc], 100, 1, 1);
        assert_eq!(winner, Some(1));
        // Wait for the per-connection acceptor thread to drain.
        std::thread::sleep(std::time::Duration::from_millis(50));
        sd.stop();
    }

    #[test]
    fn election_service_state_snapshot_consistency() {
        let s = ElectionAcceptorState::new("n".into(), 5);
        assert_eq!(s.snapshot(), (0, 5, 0));
        s.set_vlsn(42);
        s.set_term(7);
        assert_eq!(s.snapshot(), (42, 5, 7));
    }

    // Suppress unused-import warning in non-test compilations.
    #[allow(dead_code)]
    fn _ensure_tcp_channel_in_scope() -> Option<TcpChannel> {
        None
    }
}
