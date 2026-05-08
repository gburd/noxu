//! Election subsystem for Noxu DB replication.
//!
//! implements the Paxos-based
//! master election protocol used by replication layer. The subsystem
//! includes:
//!
//! - [`ElectionConfig`]  -  tunable election parameters (timeout, retries,
//!   priority, designated-primary).
//! - [`Proposal`]  -  a candidate's election proposal, with ordering that
//!   determines the winner (highest VLSN, then priority, then term, then name).
//! - [`Election`]  -  the election state machine: start, collect votes, evaluate
//!   competing proposals, check quorum, and complete.
//! - [`MasterTracker`]  -  tracks the current known master and heartbeat
//!   liveness.

pub mod election;
pub mod election_config;
pub mod master_tracker;
pub mod paxos;
pub mod proposal;

pub use election::{Election, ElectionOutcome, ElectionState};
pub use election_config::ElectionConfig;
pub use master_tracker::MasterTracker;
pub use paxos::{run_acceptor, run_election, NodeId};
pub use proposal::Proposal;
