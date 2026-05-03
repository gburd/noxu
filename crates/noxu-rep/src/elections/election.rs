//! Election state machine.
//!
//! Port of `com.sleepycat.je.rep.elections.Elections`  -  orchestrates a single
//! election round: proposing candidacy, collecting votes, evaluating competing
//! proposals, checking quorum, and recording the outcome.
//!
//! The state machine progresses through [`ElectionState`] values:
//!
//! ```text
//! Idle -> Proposing -> Voting -> Complete | Failed
//! ```
//!
//! Thread safety is achieved via `parking_lot::Mutex` on interior fields so
//! that vote recording can happen concurrently from multiple network threads.

use std::collections::HashMap;

use parking_lot::Mutex;

use super::election_config::ElectionConfig;
use super::proposal::Proposal;

/// Lifecycle state of an election round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionState {
    /// No election in progress.
    Idle,
    /// This node has broadcast its proposal and is collecting promises.
    Proposing,
    /// Votes are being collected (phase 2 in Paxos terminology).
    Voting,
    /// The election completed successfully (won or lost).
    Complete,
    /// The election failed (timeout, insufficient votes, etc.).
    Failed,
}

/// The result of an election round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionOutcome {
    /// This node (or the specified node) won the election.
    Won {
        /// Name of the new master.
        master: String,
        /// The term in which the election was won.
        term: u64,
    },
    /// Another node won the election.
    Lost {
        /// Name of the new master.
        master: String,
        /// The term in which the election was won.
        term: u64,
    },
    /// Not enough votes were received to reach quorum.
    NoQuorum {
        /// Number of affirmative votes received.
        votes_received: u32,
        /// Number of affirmative votes required.
        votes_needed: u32,
    },
    /// The election timed out before reaching a conclusion.
    Timeout,
}

/// Manages a single election round.
///
/// An `Election` is created for each election attempt. It holds the
/// configuration, the proposal this node is running on, collected votes, and
/// the final outcome.
///
/// All public methods are safe to call from multiple threads concurrently.
pub struct Election {
    config: ElectionConfig,
    state: Mutex<ElectionState>,
    term: Mutex<u64>,
    /// Map of voter name -> whether they granted their vote.
    votes: Mutex<HashMap<String, bool>>,
    /// Our proposal for this election round.
    proposal: Mutex<Option<Proposal>>,
    /// Final outcome once the election completes.
    outcome: Mutex<Option<ElectionOutcome>>,
}

impl Election {
    /// Create a new election with the given configuration.
    pub fn new(config: ElectionConfig) -> Self {
        Self {
            config,
            state: Mutex::new(ElectionState::Idle),
            term: Mutex::new(0),
            votes: Mutex::new(HashMap::new()),
            proposal: Mutex::new(None),
            outcome: Mutex::new(None),
        }
    }

    /// Returns the current election state.
    pub fn get_state(&self) -> ElectionState {
        *self.state.lock()
    }

    /// Returns the current term number.
    pub fn get_term(&self) -> u64 {
        *self.term.lock()
    }

    /// Increments the term number and returns the new value.
    pub fn increment_term(&self) -> u64 {
        let mut term = self.term.lock();
        *term += 1;
        *term
    }

    /// Start an election with the given proposal.
    ///
    /// Transitions state from `Idle` to `Proposing` and records the proposal.
    ///
    /// # Errors
    ///
    /// Returns an error if the election is not in the `Idle` state.
    pub fn start_election(
        &self,
        proposal: Proposal,
    ) -> crate::error::Result<()> {
        let mut state = self.state.lock();
        if *state != ElectionState::Idle {
            return Err(crate::error::RepError::ElectionFailed(format!(
                "cannot start election: state is {:?}, expected Idle",
                *state
            )));
        }
        *state = ElectionState::Proposing;

        let mut term = self.term.lock();
        *term = proposal.term;

        *self.proposal.lock() = Some(proposal);
        self.votes.lock().clear();
        *self.outcome.lock() = None;

        Ok(())
    }

    /// Record a vote from another node.
    ///
    /// The election must be in `Proposing` or `Voting` state. After recording
    /// the first vote the state transitions to `Voting`.
    ///
    /// # Errors
    ///
    /// Returns an error if the election is in `Idle`, `Complete`, or `Failed`
    /// state.
    pub fn record_vote(
        &self,
        voter: &str,
        granted: bool,
    ) -> crate::error::Result<()> {
        let mut state = self.state.lock();
        match *state {
            ElectionState::Proposing | ElectionState::Voting => {
                // Transition to Voting on first vote.
                if *state == ElectionState::Proposing {
                    *state = ElectionState::Voting;
                }
            }
            other => {
                return Err(crate::error::RepError::ElectionFailed(format!(
                    "cannot record vote: state is {:?}",
                    other
                )));
            }
        }
        drop(state);

        self.votes.lock().insert(voter.to_string(), granted);
        Ok(())
    }

    /// Check whether enough affirmative votes have been received to meet
    /// quorum.
    ///
    /// Returns `Some(ElectionOutcome)` when a determination can be made:
    /// - `Won` if affirmative votes >= `quorum_size`
    /// - `NoQuorum` is NOT returned here (the caller should use this together
    ///   with a timeout to decide when to give up).
    ///
    /// Returns `None` if quorum has not yet been reached.
    pub fn check_quorum(&self, quorum_size: u32) -> Option<ElectionOutcome> {
        let votes = self.votes.lock();
        let yes_count = votes.values().filter(|&&v| v).count() as u32;

        if yes_count >= quorum_size {
            let proposal = self.proposal.lock();
            if let Some(ref p) = *proposal {
                return Some(ElectionOutcome::Won {
                    master: p.node_name.clone(),
                    term: p.term,
                });
            }
        }

        // Check if it's impossible to reach quorum (all votes in, not enough
        // yes). This is an optimization  -  in practice the caller also uses a
        // timeout.
        let total = votes.len() as u32;
        let no_count = total - yes_count;
        // If remaining possible votes can't make up the deficit, report
        // NoQuorum. But we don't know the total electorate size here, so we
        // only report when we have enough yes votes. The caller handles
        // timeout-based NoQuorum.
        let _ = no_count; // acknowledge but don't act on it here

        None
    }

    /// Evaluate an incoming proposal from another candidate.
    ///
    /// Returns `true` (vote "yes") if:
    /// - We have no proposal of our own, OR
    /// - The incoming proposal is better than ours according to the
    ///   [`Proposal`] ordering.
    ///
    /// Returns `false` (vote "no") otherwise.
    pub fn evaluate_proposal(&self, incoming: &Proposal) -> bool {
        let proposal = self.proposal.lock();
        match &*proposal {
            None => true,
            Some(ours) => incoming.is_better_than(ours),
        }
    }

    /// Complete the election with the given outcome.
    ///
    /// Transitions state to `Complete` (for `Won`/`Lost`) or `Failed` (for
    /// `NoQuorum`/`Timeout`).
    ///
    /// # Errors
    ///
    /// Returns an error if the election is already in `Complete` or `Failed`
    /// state.
    pub fn complete(
        &self,
        outcome: ElectionOutcome,
    ) -> crate::error::Result<()> {
        let mut state = self.state.lock();
        match *state {
            ElectionState::Complete | ElectionState::Failed => {
                return Err(crate::error::RepError::ElectionFailed(format!(
                    "election already concluded: state is {:?}",
                    *state
                )));
            }
            _ => {}
        }

        *state = match &outcome {
            ElectionOutcome::Won { .. } | ElectionOutcome::Lost { .. } => {
                ElectionState::Complete
            }
            ElectionOutcome::NoQuorum { .. } | ElectionOutcome::Timeout => {
                ElectionState::Failed
            }
        };

        *self.outcome.lock() = Some(outcome);
        Ok(())
    }

    /// Reset the election for a new round.
    ///
    /// Clears all state and returns to `Idle`.
    pub fn reset(&self) {
        *self.state.lock() = ElectionState::Idle;
        self.votes.lock().clear();
        *self.proposal.lock() = None;
        *self.outcome.lock() = None;
    }

    /// Returns the election outcome, if the election has concluded.
    pub fn get_outcome(&self) -> Option<ElectionOutcome> {
        self.outcome.lock().clone()
    }

    /// Returns the election configuration.
    pub fn config(&self) -> &ElectionConfig {
        &self.config
    }

    /// Returns the number of affirmative votes received so far.
    pub fn yes_votes(&self) -> u32 {
        self.votes.lock().values().filter(|&&v| v).count() as u32
    }

    /// Returns the total number of votes received so far.
    pub fn total_votes(&self) -> u32 {
        self.votes.lock().len() as u32
    }
}

// Safety: all interior mutability is behind parking_lot Mutexes.
unsafe impl Send for Election {}
unsafe impl Sync for Election {}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> ElectionConfig {
        ElectionConfig::new()
    }

    fn make_proposal(
        name: &str,
        vlsn: u64,
        priority: u32,
        term: u64,
    ) -> Proposal {
        Proposal::with_timestamp(name.into(), vlsn, priority, term, 0)
    }

    // --- State transitions ---

    #[test]
    fn test_initial_state_is_idle() {
        let e = Election::new(default_config());
        assert_eq!(e.get_state(), ElectionState::Idle);
        assert_eq!(e.get_term(), 0);
        assert!(e.get_outcome().is_none());
    }

    #[test]
    fn test_start_election_transitions_to_proposing() {
        let e = Election::new(default_config());
        let p = make_proposal("node1", 100, 1, 1);
        e.start_election(p).unwrap();
        assert_eq!(e.get_state(), ElectionState::Proposing);
        assert_eq!(e.get_term(), 1);
    }

    #[test]
    fn test_start_election_fails_if_not_idle() {
        let e = Election::new(default_config());
        let p = make_proposal("node1", 100, 1, 1);
        e.start_election(p.clone()).unwrap();
        assert!(e.start_election(p).is_err());
    }

    #[test]
    fn test_record_vote_transitions_to_voting() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.record_vote("voter1", true).unwrap();
        assert_eq!(e.get_state(), ElectionState::Voting);
    }

    #[test]
    fn test_record_vote_fails_if_idle() {
        let e = Election::new(default_config());
        assert!(e.record_vote("voter1", true).is_err());
    }

    // --- Vote counting ---

    #[test]
    fn test_yes_and_total_votes() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.record_vote("v1", true).unwrap();
        e.record_vote("v2", false).unwrap();
        e.record_vote("v3", true).unwrap();

        assert_eq!(e.yes_votes(), 2);
        assert_eq!(e.total_votes(), 3);
    }

    #[test]
    fn test_duplicate_voter_overwrites() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.record_vote("v1", false).unwrap();
        e.record_vote("v1", true).unwrap();

        assert_eq!(e.yes_votes(), 1);
        assert_eq!(e.total_votes(), 1);
    }

    // --- Quorum ---

    #[test]
    fn test_check_quorum_reached() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 5)).unwrap();
        e.record_vote("v1", true).unwrap();
        e.record_vote("v2", true).unwrap();

        let result = e.check_quorum(2);
        assert!(result.is_some());
        match result.unwrap() {
            ElectionOutcome::Won { master, term } => {
                assert_eq!(master, "node1");
                assert_eq!(term, 5);
            }
            other => panic!("expected Won, got {:?}", other),
        }
    }

    #[test]
    fn test_check_quorum_not_yet_reached() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.record_vote("v1", true).unwrap();

        assert!(e.check_quorum(3).is_none());
    }

    #[test]
    fn test_check_quorum_no_votes_false_dont_count() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.record_vote("v1", true).unwrap();
        e.record_vote("v2", false).unwrap();
        e.record_vote("v3", false).unwrap();

        assert!(e.check_quorum(2).is_none());
    }

    // --- Evaluate proposals ---

    #[test]
    fn test_evaluate_no_own_proposal_votes_yes() {
        let e = Election::new(default_config());
        let incoming = make_proposal("other", 100, 1, 1);
        assert!(e.evaluate_proposal(&incoming));
    }

    #[test]
    fn test_evaluate_better_incoming_votes_yes() {
        let e = Election::new(default_config());
        let ours = make_proposal("node1", 100, 1, 1);
        e.start_election(ours).unwrap();

        let better = make_proposal("node2", 200, 1, 1); // higher VLSN
        assert!(e.evaluate_proposal(&better));
    }

    #[test]
    fn test_evaluate_worse_incoming_votes_no() {
        let e = Election::new(default_config());
        let ours = make_proposal("node1", 200, 5, 2);
        e.start_election(ours).unwrap();

        let worse = make_proposal("node2", 100, 1, 1); // lower VLSN
        assert!(!e.evaluate_proposal(&worse));
    }

    #[test]
    fn test_evaluate_equal_proposal_votes_no() {
        let e = Election::new(default_config());
        let ours = make_proposal("node1", 100, 1, 1);
        e.start_election(ours).unwrap();

        let same = make_proposal("node1", 100, 1, 1);
        assert!(!e.evaluate_proposal(&same));
    }

    // --- Complete ---

    #[test]
    fn test_complete_won() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();

        let outcome = ElectionOutcome::Won { master: "node1".into(), term: 1 };
        e.complete(outcome.clone()).unwrap();
        assert_eq!(e.get_state(), ElectionState::Complete);
        assert_eq!(e.get_outcome(), Some(outcome));
    }

    #[test]
    fn test_complete_lost() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();

        let outcome = ElectionOutcome::Lost { master: "node2".into(), term: 1 };
        e.complete(outcome).unwrap();
        assert_eq!(e.get_state(), ElectionState::Complete);
    }

    #[test]
    fn test_complete_no_quorum() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();

        let outcome =
            ElectionOutcome::NoQuorum { votes_received: 1, votes_needed: 3 };
        e.complete(outcome).unwrap();
        assert_eq!(e.get_state(), ElectionState::Failed);
    }

    #[test]
    fn test_complete_timeout() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.complete(ElectionOutcome::Timeout).unwrap();
        assert_eq!(e.get_state(), ElectionState::Failed);
    }

    #[test]
    fn test_complete_twice_fails() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.complete(ElectionOutcome::Timeout).unwrap();
        assert!(e.complete(ElectionOutcome::Timeout).is_err());
    }

    // --- Reset ---

    #[test]
    fn test_reset() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.record_vote("v1", true).unwrap();
        e.complete(ElectionOutcome::Won { master: "node1".into(), term: 1 })
            .unwrap();

        e.reset();
        assert_eq!(e.get_state(), ElectionState::Idle);
        assert!(e.get_outcome().is_none());
        assert_eq!(e.total_votes(), 0);
    }

    #[test]
    fn test_reset_allows_new_election() {
        let e = Election::new(default_config());
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.complete(ElectionOutcome::Timeout).unwrap();

        e.reset();
        // Should be able to start a new election.
        e.start_election(make_proposal("node1", 100, 1, 2)).unwrap();
        assert_eq!(e.get_state(), ElectionState::Proposing);
        assert_eq!(e.get_term(), 2);
    }

    // --- Term management ---

    #[test]
    fn test_increment_term() {
        let e = Election::new(default_config());
        assert_eq!(e.get_term(), 0);
        assert_eq!(e.increment_term(), 1);
        assert_eq!(e.increment_term(), 2);
        assert_eq!(e.get_term(), 2);
    }

    // --- Full election simulation ---

    #[test]
    fn test_full_election_three_node_cluster() {
        let e = Election::new(default_config());

        // Node1 starts an election in term 1.
        let proposal = make_proposal("node1", 150, 5, 1);
        e.start_election(proposal).unwrap();

        // Node1 votes for itself.
        e.record_vote("node1", true).unwrap();

        // Node2 votes yes (its VLSN is lower).
        e.record_vote("node2", true).unwrap();

        // Quorum is 2 out of 3.
        let result = e.check_quorum(2).unwrap();
        match &result {
            ElectionOutcome::Won { master, term } => {
                assert_eq!(master, "node1");
                assert_eq!(*term, 1);
            }
            other => panic!("expected Won, got {:?}", other),
        }

        e.complete(result).unwrap();
        assert_eq!(e.get_state(), ElectionState::Complete);
    }

    #[test]
    fn test_full_election_lost() {
        let e = Election::new(default_config());

        // Our node has low VLSN.
        let our_proposal = make_proposal("node1", 50, 1, 1);
        e.start_election(our_proposal).unwrap();

        // A competing proposal from node2 with higher VLSN arrives.
        let competing = make_proposal("node2", 200, 1, 1);
        assert!(e.evaluate_proposal(&competing)); // we'd vote yes for them

        // We get rejected by other voters.
        e.record_vote("node2", false).unwrap();
        e.record_vote("node3", false).unwrap();

        // No quorum reached.
        assert!(e.check_quorum(2).is_none());

        // Complete as lost.
        e.complete(ElectionOutcome::Lost { master: "node2".into(), term: 1 })
            .unwrap();
        assert_eq!(e.get_state(), ElectionState::Complete);
    }

    #[test]
    fn test_designated_primary_self_election() {
        // In a 2-node group with designated_primary, the primary can
        // self-elect with just its own vote (quorum of 1).
        let config = ElectionConfig::builder().designated_primary(true).build();
        let e = Election::new(config);

        let proposal = make_proposal("primary", 100, 1, 1);
        e.start_election(proposal).unwrap();
        e.record_vote("primary", true).unwrap();

        // Designated primary: quorum of 1 in a 2-node group.
        let result = e.check_quorum(1).unwrap();
        match result {
            ElectionOutcome::Won { master, term } => {
                assert_eq!(master, "primary");
                assert_eq!(term, 1);
            }
            other => panic!("expected Won, got {:?}", other),
        }

        assert!(e.config().designated_primary());
    }

    #[test]
    fn test_multiple_rounds() {
        let e = Election::new(default_config());

        // Round 1: timeout.
        e.start_election(make_proposal("node1", 100, 1, 1)).unwrap();
        e.complete(ElectionOutcome::Timeout).unwrap();
        assert_eq!(e.get_state(), ElectionState::Failed);

        // Round 2: success.
        e.reset();
        let new_term = e.increment_term();
        e.start_election(make_proposal("node1", 100, 1, new_term)).unwrap();
        e.record_vote("node1", true).unwrap();
        e.record_vote("node2", true).unwrap();

        let result = e.check_quorum(2).unwrap();
        e.complete(result).unwrap();
        assert_eq!(e.get_state(), ElectionState::Complete);
    }

    // --- Send + Sync ---

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Election>();
    }
}
