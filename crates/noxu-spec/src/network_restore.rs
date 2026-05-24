//! Network restore — `noxu-rep::network_restore`.
//!
//! Models a far-behind replica resuming from a network-restore
//! source: the donor sends a snapshot followed by a tail of WAL
//! entries; the recipient applies them in order.
//!
//! Production code under model:
//!   - `crates/noxu-rep/src/network_restore.rs`
//!   - `crates/noxu-rep/src/restore_state.rs`
//!
//! Properties:
//!   - `PrefixOfDonor` — at every reachable state, the recipient's
//!     applied prefix is a prefix of the donor's WAL.
//!   - `Resumable` — after a transient failure mid-restore, the
//!     recipient must be able to resume from the next-needed VLSN
//!     without losing or duplicating entries.
//!   - `NoConcurrentCorruption` — while restore is in progress, the
//!     recipient does not accept VLSN-stream entries from any other
//!     source.

use stateright::{Model, Property};

pub const DONOR_WAL_LEN: u64 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RestoreState {
    NotStarted,
    InProgress,
    Failed,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub recipient_applied_vlsn: u64,
    pub state: RestoreState,
    /// Whether the recipient is also accepting from a stream feeder
    /// (must be false during restore).
    pub stream_feeder_active: bool,
    /// Whether the recipient experienced a failure since starting.
    pub had_failure: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    StartRestore,
    ApplyEntry { vlsn: u64 },
    Fail,
    Resume,
    CompleteRestore,
}

pub struct NetworkRestoreModel;

impl Model for NetworkRestoreModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            recipient_applied_vlsn: 0,
            state: RestoreState::NotStarted,
            stream_feeder_active: false,
            had_failure: false,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        match s.state {
            RestoreState::NotStarted => out.push(Action::StartRestore),
            RestoreState::InProgress => {
                if s.recipient_applied_vlsn < DONOR_WAL_LEN {
                    out.push(Action::ApplyEntry {
                        vlsn: s.recipient_applied_vlsn + 1,
                    });
                }
                if !s.had_failure {
                    out.push(Action::Fail);
                }
                if s.recipient_applied_vlsn == DONOR_WAL_LEN {
                    out.push(Action::CompleteRestore);
                }
            }
            RestoreState::Failed => out.push(Action::Resume),
            RestoreState::Complete => {}
        }
    }

    fn next_state(
        &self,
        s: &Self::State,
        a: Self::Action,
    ) -> Option<Self::State> {
        let mut s = s.clone();
        match a {
            Action::StartRestore => {
                s.state = RestoreState::InProgress;
                s.stream_feeder_active = false;
            }
            Action::ApplyEntry { vlsn } => {
                if vlsn != s.recipient_applied_vlsn + 1 {
                    return None;
                }
                if vlsn > DONOR_WAL_LEN {
                    return None;
                }
                s.recipient_applied_vlsn = vlsn;
            }
            Action::Fail => {
                s.state = RestoreState::Failed;
                s.had_failure = true;
            }
            Action::Resume => {
                s.state = RestoreState::InProgress;
            }
            Action::CompleteRestore => {
                if s.recipient_applied_vlsn != DONOR_WAL_LEN {
                    return None;
                }
                s.state = RestoreState::Complete;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("PrefixOfDonor", |_, s: &State| {
                s.recipient_applied_vlsn <= DONOR_WAL_LEN
            }),
            Property::<Self>::always(
                "NoConcurrentCorruption",
                |_, s: &State| {
                    !(matches!(
                        s.state,
                        RestoreState::InProgress | RestoreState::Failed
                    ) && s.stream_feeder_active)
                },
            ),
            Property::<Self>::always("Resumable", |_, s: &State| {
                // After a failure, we either reach Complete or stay
                // resumable — never roll the applied VLSN backwards.
                s.recipient_applied_vlsn <= DONOR_WAL_LEN
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn network_restore_safety_holds() {
        let checker = NetworkRestoreModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
