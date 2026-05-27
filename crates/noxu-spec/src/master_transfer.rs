//! Master transfer — `noxu-rep::master_transfer`.
//!
//! Models a graceful master handoff: master M1 stops accepting new
//! writes, drains in-flight commits, broadcasts a
//! "MasterTransferProposal" to a quorum, and a designated successor
//! M2 takes over.
//!
//! Production code under model:
//!   - `crates/noxu-rep/src/master_transfer.rs`
//!   - `crates/noxu-rep/src/elections/master_tracker.rs`
//!     (audit rep F32 (Wave 2C-4): the spec previously pointed at
//!     `master_term.rs`, which has never existed; the term state
//!     lives in `master_tracker.rs::MasterTracker::master_term`).
//!
//! Properties:
//!   - `AtMostOneMaster` — across all reachable states, at most one
//!     node is in `MasterActive` at any time.
//!   - `AtMostOneDraining` — likewise for `MasterDraining`; rules
//!     out a split-brain hand-off race.
//!   - `MasterTermsMonotone` — `current_master_term` is at least
//!     the highest per-node `master_term` ever recorded, so terms
//!     never re-use earlier values.

use stateright::{Model, Property};

pub const N_NODES: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NodeRole {
    Replica,
    MasterDraining,
    MasterActive,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub roles: [NodeRole; N_NODES],
    pub master_term: [u64; N_NODES],
    pub commit_point: u64,
    pub current_master_term: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    BecomeMaster { node: usize },
    StartDrain { node: usize },
    HandoffComplete { from: usize, to: usize },
    AdvanceCommitPoint { delta: u64 },
}

pub struct MasterTransferModel;

/// Cap on `current_master_term` so BFS terminates. Three terms is
/// enough to exercise both the initial master and at least one
/// successful handoff plus a follow-up handoff back.
pub const MAX_MASTER_TERM: u64 = 3;
/// Cap on `commit_point` for the same reason.
pub const MAX_COMMIT_POINT: u64 = 3;

impl Model for MasterTransferModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut roles = [NodeRole::Replica; N_NODES];
        roles[0] = NodeRole::MasterActive;
        let mut master_term = [0; N_NODES];
        master_term[0] = 1;
        vec![State {
            roles,
            master_term,
            commit_point: 0,
            current_master_term: 1,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        // No more transitions once we've hit the bounds — keeps the
        // state space finite.
        if s.current_master_term >= MAX_MASTER_TERM
            && s.commit_point >= MAX_COMMIT_POINT
        {
            return;
        }
        for n in 0..N_NODES {
            match s.roles[n] {
                NodeRole::MasterActive => {
                    out.push(Action::StartDrain { node: n })
                }
                NodeRole::MasterDraining => {
                    if s.current_master_term < MAX_MASTER_TERM {
                        for to in 0..N_NODES {
                            if to != n
                                && matches!(s.roles[to], NodeRole::Replica)
                            {
                                out.push(Action::HandoffComplete {
                                    from: n,
                                    to,
                                });
                            }
                        }
                    }
                }
                NodeRole::Replica => {}
            }
        }
        if s.commit_point < MAX_COMMIT_POINT {
            out.push(Action::AdvanceCommitPoint { delta: 1 });
        }
    }

    fn next_state(
        &self,
        s: &Self::State,
        a: Self::Action,
    ) -> Option<Self::State> {
        let mut s = s.clone();
        match a {
            Action::BecomeMaster { node } => {
                s.roles[node] = NodeRole::MasterActive;
                s.current_master_term += 1;
                s.master_term[node] = s.current_master_term;
            }
            Action::StartDrain { node } => {
                if !matches!(s.roles[node], NodeRole::MasterActive) {
                    return None;
                }
                s.roles[node] = NodeRole::MasterDraining;
            }
            Action::HandoffComplete { from, to } => {
                if !matches!(s.roles[from], NodeRole::MasterDraining) {
                    return None;
                }
                if !matches!(s.roles[to], NodeRole::Replica) {
                    return None;
                }
                s.roles[from] = NodeRole::Replica;
                s.roles[to] = NodeRole::MasterActive;
                s.current_master_term += 1;
                s.master_term[to] = s.current_master_term;
            }
            Action::AdvanceCommitPoint { delta } => {
                s.commit_point += delta;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("AtMostOneMaster", |_, s: &State| {
                s.roles
                    .iter()
                    .filter(|r| matches!(r, NodeRole::MasterActive))
                    .count()
                    <= 1
            }),
            Property::<Self>::always("AtMostOneDraining", |_, s: &State| {
                s.roles
                    .iter()
                    .filter(|r| matches!(r, NodeRole::MasterDraining))
                    .count()
                    <= 1
            }),
            Property::<Self>::always("MasterTermsMonotone", |_, s: &State| {
                let max_term = *s.master_term.iter().max().unwrap_or(&0);
                s.current_master_term >= max_term
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn master_transfer_safety_holds() {
        let checker = MasterTransferModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
