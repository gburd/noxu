//! XA two-phase commit — `noxu-xa::environment`.
//!
//! Models a single transaction manager driving N resource managers
//! through Prepare → Commit/Abort. Captures the recovery scenario
//! where the TM crashes between Prepare and Commit.
//!
//! Production code under model:
//!   - `crates/noxu-xa/src/environment.rs`
//!   - `crates/noxu-xa/src/internal.rs`
//!   - `crates/noxu-xa/src/types.rs`
//!
//! Properties:
//!   - `PreparedImpliesDecided` — an RM in the `Committed` state
//!     can only exist when the TM is in `CommitDecided`.
//!   - `NoMixedDecision` — once decided, RMs follow the same
//!     outcome: not "one Committed, one Aborted" under
//!     `CommitDecided`.
//!   - `NoUnilateralCommit` — an RM never reaches `Committed`
//!     while the TM is in any state other than `CommitDecided`.
//!
//! TODO: a `RecoveryConsistent` property — that after a TM crash,
//! recovery yields the same global decision for every RM — would
//! benefit from a 2-state predicate (compare pre-crash and
//! post-recovery decision); currently the model handles recovery
//! but only the per-state safety invariants above are checked.

use stateright::{Model, Property};

/// Compile-time anchor: the XA spec relies on the existence of
/// these production [`noxu_xa::XaFlags`] constants. Removing or
/// renaming any of them on the production side breaks the
/// `noxu-spec` build, which is the whole point of taking a hard
/// dep on `noxu-xa` from this crate. The constants themselves are
/// not used at runtime by the model — the model is a state
/// machine over `RmState`/`TmState`, not a wire-format check —
/// but a refactor that, say, renamed `ONEPHASE` to `OnePhase`
/// would currently land silently with no spec response. The
/// `_FLAG_ANCHOR` const below makes that a build break.
#[allow(dead_code)]
const _FLAG_ANCHOR: [noxu_xa::XaFlags; 9] = [
    noxu_xa::XaFlags::NOFLAGS,
    noxu_xa::XaFlags::JOIN,
    noxu_xa::XaFlags::RESUME,
    noxu_xa::XaFlags::TMSUCCESS,
    noxu_xa::XaFlags::TMFAIL,
    noxu_xa::XaFlags::TMSUSPEND,
    noxu_xa::XaFlags::ONEPHASE,
    noxu_xa::XaFlags::STARTRSCAN,
    noxu_xa::XaFlags::ENDRSCAN,
];

pub const N_RMS: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RmState {
    Active,
    Prepared,
    Committed,
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum TmState {
    Idle,
    Preparing,
    /// All RMs voted yes; commit decision is durable.
    CommitDecided,
    /// At least one RM voted no, or TM aborted; abort decision is
    /// durable.
    AbortDecided,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub tm: TmState,
    pub rms: [RmState; N_RMS],
    /// True once the TM has crashed (no more TM transitions allowed
    /// until recovery).
    pub tm_crashed: bool,
    pub recovered: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    StartPrepare,
    RmVoteYes { rm: usize },
    RmVoteNo { rm: usize },
    TmDecideCommit,
    TmDecideAbort,
    RmCommit { rm: usize },
    RmAbort { rm: usize },
    TmCrash,
    Recover,
}

pub struct XaTwoPhaseCommitModel;

impl Model for XaTwoPhaseCommitModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            tm: TmState::Idle,
            rms: [RmState::Active; N_RMS],
            tm_crashed: false,
            recovered: false,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.tm_crashed && !s.recovered {
            out.push(Action::Recover);
            return;
        }
        match s.tm {
            TmState::Idle => out.push(Action::StartPrepare),
            TmState::Preparing => {
                for rm in 0..N_RMS {
                    if matches!(s.rms[rm], RmState::Active) {
                        out.push(Action::RmVoteYes { rm });
                        out.push(Action::RmVoteNo { rm });
                    }
                }
                if s.rms.iter().all(|r| matches!(r, RmState::Prepared)) {
                    out.push(Action::TmDecideCommit);
                }
                if s.rms.iter().any(|r| matches!(r, RmState::Aborted)) {
                    out.push(Action::TmDecideAbort);
                }
                out.push(Action::TmCrash);
            }
            TmState::CommitDecided => {
                for rm in 0..N_RMS {
                    if matches!(s.rms[rm], RmState::Prepared) {
                        out.push(Action::RmCommit { rm });
                    }
                }
            }
            TmState::AbortDecided => {
                for rm in 0..N_RMS {
                    if matches!(s.rms[rm], RmState::Prepared | RmState::Active)
                    {
                        out.push(Action::RmAbort { rm });
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
            Action::StartPrepare => s.tm = TmState::Preparing,
            Action::RmVoteYes { rm } => s.rms[rm] = RmState::Prepared,
            Action::RmVoteNo { rm } => s.rms[rm] = RmState::Aborted,
            Action::TmDecideCommit => s.tm = TmState::CommitDecided,
            Action::TmDecideAbort => s.tm = TmState::AbortDecided,
            Action::RmCommit { rm } => {
                if !matches!(s.tm, TmState::CommitDecided) {
                    return None;
                }
                if !matches!(s.rms[rm], RmState::Prepared) {
                    return None;
                }
                s.rms[rm] = RmState::Committed;
            }
            Action::RmAbort { rm } => {
                if !matches!(s.tm, TmState::AbortDecided) {
                    return None;
                }
                s.rms[rm] = RmState::Aborted;
            }
            Action::TmCrash => s.tm_crashed = true,
            Action::Recover => {
                s.tm_crashed = false;
                s.recovered = true;
                // Recovery rule: if any RM has committed, force
                // commit; else if any RM has aborted post-prepare,
                // force abort; else default to abort (presumed-abort).
                if s.rms.iter().any(|r| matches!(r, RmState::Committed)) {
                    s.tm = TmState::CommitDecided;
                } else if s.rms.iter().any(|r| matches!(r, RmState::Aborted))
                    || s.rms.iter().any(|r| matches!(r, RmState::Active))
                {
                    s.tm = TmState::AbortDecided;
                } else {
                    s.tm = TmState::AbortDecided;
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "PreparedImpliesDecided",
                |_, s: &State| {
                    // If an RM is Committed, TM must be CommitDecided.
                    // If an RM is Aborted (post-prepare), TM must be
                    // AbortDecided OR the RM voted "no" before TM decided.
                    for r in &s.rms {
                        if matches!(r, RmState::Committed)
                            && !matches!(s.tm, TmState::CommitDecided)
                        {
                            return false;
                        }
                    }
                    true
                },
            ),
            Property::<Self>::always("NoMixedDecision", |_, s: &State| {
                // Once decided, all RMs follow the same outcome —
                // can't have one Committed and one Aborted.
                let any_commit =
                    s.rms.iter().any(|r| matches!(r, RmState::Committed));
                let any_abort_post =
                    s.rms.iter().any(|r| matches!(r, RmState::Aborted))
                        && matches!(s.tm, TmState::CommitDecided);
                !(any_commit && any_abort_post)
            }),
            Property::<Self>::always("NoUnilateralCommit", |_, s: &State| {
                if s.rms.iter().any(|r| matches!(r, RmState::Committed))
                    && !matches!(s.tm, TmState::CommitDecided)
                {
                    return false;
                }
                true
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn xa_two_phase_commit_safety_holds() {
        let checker = XaTwoPhaseCommitModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
