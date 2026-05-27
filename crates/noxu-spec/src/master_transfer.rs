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
//!   - `crates/noxu-rep/src/replicated_environment.rs::become_master`
//!     (audit finding F9: spawning a `Feeder` tracker per electable
//!     replica when this node enters `MasterActive`. Modelled by
//!     `current_master_feeders` and the `MasterHasFeeders`
//!     invariant.)
//!
//! Properties:
//!   - `AtMostOneMaster` — across all reachable states, at most one
//!     node is in `MasterActive` at any time.
//!   - `AtMostOneDraining` — likewise for `MasterDraining`; rules
//!     out a split-brain hand-off race.
//!   - `MasterTermsMonotone` — `current_master_term` is at least
//!     the highest per-node `master_term` ever recorded, so terms
//!     never re-use earlier values.
//!   - `MasterHasFeeders` — whenever a node is in `MasterActive`,
//!     `current_master_feeders` is exactly the set of other
//!     electable peers. Closes audit finding F9: a master without
//!     feeders cannot push entries to the replicas pulling from
//!     `PEER_FEEDER`, even though the role state alone (which the
//!     pre-Wave-4-A spec validated) looked correct.

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
    /// Set of peer node indices the *current* master has spawned a
    /// `Feeder` tracker for. Empty when no node is `MasterActive`.
    /// Encoded as a fixed-length bitfield indexed by node id so the
    /// state remains `Hash + Eq`.
    pub current_master_feeders: [bool; N_NODES],
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
        // F9: node 0 is the initial master, so it has feeders
        // spawned for every other (electable) peer.
        let mut feeders = [false; N_NODES];
        for (i, slot) in feeders.iter_mut().enumerate() {
            *slot = i != 0;
        }
        vec![State {
            roles,
            master_term,
            commit_point: 0,
            current_master_term: 1,
            current_master_feeders: feeders,
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
                // F9: become_master spawns a Feeder per electable
                // peer; clears any feeders left over from a prior
                // role.
                for (i, slot) in s.current_master_feeders.iter_mut().enumerate()
                {
                    *slot = i != node;
                }
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
                // F9: the successor's `become_master` re-creates the
                // feeder map. The drain path on `from` has already
                // dropped its feeders (modelled here by recomputing
                // from `to`'s perspective).
                for (i, slot) in s.current_master_feeders.iter_mut().enumerate()
                {
                    *slot = i != to;
                }
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
            Property::<Self>::always("MasterHasFeeders", |_, s: &State| {
                // F9: whenever some node is `MasterActive` or
                // `MasterDraining`, it has a feeder tracker for
                // every other peer. (`StartDrain` does not tear
                // down the feeders — they keep pushing entries
                // until `HandoffComplete` hands the role to the
                // successor.) When no node holds the role, no
                // feeders are expected.
                let in_charge = s.roles.iter().position(|r| {
                    matches!(
                        r,
                        NodeRole::MasterActive | NodeRole::MasterDraining
                    )
                });
                match in_charge {
                    Some(m) => {
                        for (i, &spawned) in
                            s.current_master_feeders.iter().enumerate()
                        {
                            let expected = i != m;
                            if spawned != expected {
                                return false;
                            }
                        }
                        true
                    }
                    None => s.current_master_feeders.iter().all(|f| !*f),
                }
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
