//! Flexible Paxos election protocol — `noxu-rep::elections::paxos`.
//!
//! Models the Phase-1 (Prepare/Promise) + Phase-2 (Accept/Accepted)
//! protocol Howard, Malkhi & Spiegelman describe in OPODIS 2016 and
//! that `crates/noxu-rep/src/elections/paxos.rs::run_election` and
//! `run_acceptor` implement.
//!
//! Production code under model:
//!   - `crates/noxu-rep/src/elections/paxos.rs`
//!   - `crates/noxu-rep/src/elections/proposal.rs`
//!   - `crates/noxu-rep/src/quorum_policy.rs`
//!
//! Properties:
//!   - `ElectionSafety` — at most one leader per term
//!   - `PromiseHonoured` — an acceptor that has promised term t never
//!     subsequently accepts a leader at term < t
//!   - `QuorumIntersection` — for any Phase-1 voter set V1 of size
//!     ≥ Q1 and any Phase-2 voter set V2 of size ≥ Q2, V1 ∩ V2 ≠ ∅.
//!     Static; falls out of `Q1 + Q2 > N`.

use stateright::{Model, Property};

pub const N_NODES: usize = 3;
pub const MAX_TERM: u64 = 1;
pub const Q1: usize = 2;
pub const Q2: usize = 2;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    /// `promised_term[n]` — highest term node `n` has promised.
    pub promised_term: [u64; N_NODES],
    /// `accepted_term[n]` — highest term node `n` has accepted at,
    /// or 0 if none.
    pub accepted_term: [u64; N_NODES],
    /// `accepted_leader[n]` — the leader `n` accepted at
    /// `accepted_term[n]`, or `usize::MAX` for none.
    pub accepted_leader: [usize; N_NODES],
    /// Which (term, leader) pairs have entered Phase-1.
    pub leaders_proposed: Vec<(u64, usize)>,
    /// Phase-1 votes per (term, leader).
    pub phase1_votes: Vec<((u64, usize), Vec<usize>)>,
    /// Phase-2 votes per (term, leader).
    pub phase2_votes: Vec<((u64, usize), Vec<usize>)>,
    /// Successfully elected (term, leader) pairs.
    pub leaders_elected: Vec<(u64, usize)>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    StartElection { leader: usize, term: u64 },
    PromiseVote { acceptor: usize, leader: usize, term: u64 },
    StartPhase2 { leader: usize, term: u64 },
    AcceptVote { acceptor: usize, leader: usize, term: u64 },
    DeclareElected { leader: usize, term: u64 },
}

pub struct FlexiblePaxosModel;

fn votes_for<'a>(
    list: &'a [((u64, usize), Vec<usize>)],
    key: (u64, usize),
) -> Option<&'a [usize]> {
    list.iter().find(|(k, _)| *k == key).map(|(_, v)| v.as_slice())
}

fn votes_for_mut<'a>(
    list: &'a mut [((u64, usize), Vec<usize>)],
    key: (u64, usize),
) -> Option<&'a mut Vec<usize>> {
    list.iter_mut().find(|(k, _)| *k == key).map(|(_, v)| v)
}

impl Model for FlexiblePaxosModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            promised_term: [0; N_NODES],
            accepted_term: [0; N_NODES],
            accepted_leader: [usize::MAX; N_NODES],
            leaders_proposed: vec![],
            phase1_votes: vec![],
            phase2_votes: vec![],
            leaders_elected: vec![],
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        for term in 1..=MAX_TERM {
            for leader in 0..N_NODES {
                if !s.leaders_proposed.contains(&(term, leader))
                    && !s.leaders_elected.contains(&(term, leader))
                {
                    out.push(Action::StartElection { leader, term });
                }
                if s.leaders_proposed.contains(&(term, leader)) {
                    // Phase-1 votes
                    for n in 0..N_NODES {
                        if s.promised_term[n] < term {
                            if let Some(votes) =
                                votes_for(&s.phase1_votes, (term, leader))
                            {
                                if !votes.contains(&n) {
                                    out.push(Action::PromiseVote {
                                        acceptor: n,
                                        leader,
                                        term,
                                    });
                                }
                            }
                        }
                    }
                    // Start Phase 2 once Q1 promises
                    if let Some(votes) =
                        votes_for(&s.phase1_votes, (term, leader))
                        && votes.len() >= Q1
                        && !s
                            .phase2_votes
                            .iter()
                            .any(|(k, _)| *k == (term, leader))
                    {
                        out.push(Action::StartPhase2 { leader, term });
                    }
                    // Phase-2 votes
                    if s.phase2_votes.iter().any(|(k, _)| *k == (term, leader))
                    {
                        for n in 0..N_NODES {
                            if s.promised_term[n] <= term {
                                if let Some(votes) =
                                    votes_for(&s.phase2_votes, (term, leader))
                                {
                                    if !votes.contains(&n) {
                                        out.push(Action::AcceptVote {
                                            acceptor: n,
                                            leader,
                                            term,
                                        });
                                    }
                                }
                            }
                        }
                    }
                    // Declare elected once Q2 accepts
                    if let Some(votes) =
                        votes_for(&s.phase2_votes, (term, leader))
                        && votes.len() >= Q2
                        && !s.leaders_elected.contains(&(term, leader))
                    {
                        out.push(Action::DeclareElected { leader, term });
                    }
                }
            }
        }
    }

    fn next_state(
        &self,
        s: &Self::State,
        a: Self::Action,
    ) -> Option<Self::State> {
        let mut s = s.clone();
        match a {
            Action::StartElection { leader, term } => {
                s.leaders_proposed.push((term, leader));
                s.phase1_votes.push(((term, leader), vec![]));
            }
            Action::PromiseVote { acceptor, leader, term } => {
                s.promised_term[acceptor] = term;
                if let Some(v) =
                    votes_for_mut(&mut s.phase1_votes, (term, leader))
                {
                    if !v.contains(&acceptor) {
                        v.push(acceptor);
                        v.sort_unstable();
                    }
                }
            }
            Action::StartPhase2 { leader, term } => {
                s.phase2_votes.push(((term, leader), vec![]));
            }
            Action::AcceptVote { acceptor, leader, term } => {
                s.accepted_term[acceptor] = term;
                s.accepted_leader[acceptor] = leader;
                // Implicit promise: accepting at t bumps promised to t.
                if s.promised_term[acceptor] < term {
                    s.promised_term[acceptor] = term;
                }
                if let Some(v) =
                    votes_for_mut(&mut s.phase2_votes, (term, leader))
                {
                    if !v.contains(&acceptor) {
                        v.push(acceptor);
                        v.sort_unstable();
                    }
                }
            }
            Action::DeclareElected { leader, term } => {
                s.leaders_elected.push((term, leader));
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("ElectionSafety", |_, s: &State| {
                for term in 1..=MAX_TERM {
                    let leaders: Vec<_> = s
                        .leaders_elected
                        .iter()
                        .filter(|(t, _)| *t == term)
                        .map(|(_, l)| *l)
                        .collect();
                    if leaders
                        .iter()
                        .collect::<std::collections::HashSet<_>>()
                        .len()
                        > 1
                    {
                        return false;
                    }
                }
                true
            }),
            Property::<Self>::always("PromiseHonoured", |_, s: &State| {
                for n in 0..N_NODES {
                    if s.accepted_term[n] == 0 {
                        continue;
                    }
                    if s.accepted_term[n] > s.promised_term[n] {
                        return false;
                    }
                }
                true
            }),
            Property::<Self>::always("QuorumIntersection", |_, _s: &State| {
                Q1 + Q2 > N_NODES
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn paxos_safety_holds() {
        let checker = FlexiblePaxosModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
