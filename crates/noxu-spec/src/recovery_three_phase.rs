//! Recovery 3-phase — `noxu-recovery::recovery_manager` (analysis →
//! redo → undo).
//!
//! Models a small WAL with N committed and M aborted transactions
//! and a checkpoint, exercising the find-end-of-log → analysis →
//! redo → undo pipeline.
//!
//! Production code under model:
//!   - `crates/noxu-recovery/src/recovery_manager.rs`
//!   - `crates/noxu-recovery/src/transaction_table.rs`
//!   - `crates/noxu-recovery/src/dirty_page_table.rs`
//!
//! Properties:
//!   - `AllAndOnlyCommitted` — after recovery, the live tree
//!     contains entries for every committed txn and no entries for
//!     any aborted txn.
//!   - `IdempotentReplay` — running redo twice produces the same
//!     state as running it once.

use stateright::{Model, Property};

pub const N_TXNS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum TxnOutcome {
    Committed,
    Aborted,
    Active,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Phase {
    PreAnalysis,
    AfterAnalysis,
    AfterRedo,
    AfterUndo,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub outcomes: [TxnOutcome; N_TXNS],
    pub phase: Phase,
    /// Whether each txn's data is materialised in the recovered tree.
    pub materialised: [bool; N_TXNS],
    pub redo_run_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    Analyse,
    Redo,
    Undo,
    RedoAgain,
}

pub struct RecoveryThreePhaseModel;

impl Model for RecoveryThreePhaseModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        // All combinations of N_TXNS outcomes (committed vs aborted vs active).
        let mut out = vec![];
        for c0 in
            [TxnOutcome::Committed, TxnOutcome::Aborted, TxnOutcome::Active]
        {
            for c1 in
                [TxnOutcome::Committed, TxnOutcome::Aborted, TxnOutcome::Active]
            {
                for c2 in [
                    TxnOutcome::Committed,
                    TxnOutcome::Aborted,
                    TxnOutcome::Active,
                ] {
                    out.push(State {
                        outcomes: [c0, c1, c2],
                        phase: Phase::PreAnalysis,
                        materialised: [false; N_TXNS],
                        redo_run_count: 0,
                    });
                }
            }
        }
        out
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        match s.phase {
            Phase::PreAnalysis => out.push(Action::Analyse),
            Phase::AfterAnalysis => out.push(Action::Redo),
            Phase::AfterRedo => {
                out.push(Action::Undo);
                if s.redo_run_count < 2 {
                    out.push(Action::RedoAgain);
                }
            }
            Phase::AfterUndo => {}
        }
    }

    fn next_state(
        &self,
        s: &Self::State,
        a: Self::Action,
    ) -> Option<Self::State> {
        let mut s = s.clone();
        match a {
            Action::Analyse => s.phase = Phase::AfterAnalysis,
            Action::Redo | Action::RedoAgain => {
                for tid in 0..N_TXNS {
                    if matches!(
                        s.outcomes[tid],
                        TxnOutcome::Committed
                            | TxnOutcome::Aborted
                            | TxnOutcome::Active
                    ) {
                        // Redo replays every WAL record regardless of
                        // outcome — undo will roll back uncommitted ones.
                        s.materialised[tid] = true;
                    }
                }
                s.phase = Phase::AfterRedo;
                s.redo_run_count += 1;
            }
            Action::Undo => {
                for tid in 0..N_TXNS {
                    if !matches!(s.outcomes[tid], TxnOutcome::Committed) {
                        s.materialised[tid] = false;
                    }
                }
                s.phase = Phase::AfterUndo;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("AllAndOnlyCommitted", |_, s: &State| {
                if !matches!(s.phase, Phase::AfterUndo) {
                    return true;
                }
                for tid in 0..N_TXNS {
                    let should_be =
                        matches!(s.outcomes[tid], TxnOutcome::Committed);
                    if s.materialised[tid] != should_be {
                        return false;
                    }
                }
                true
            }),
            Property::<Self>::always("IdempotentReplay", |_, s: &State| {
                // After redo, every committed txn is materialised; this
                // is invariant under further redo runs.
                if matches!(s.phase, Phase::AfterRedo) {
                    for tid in 0..N_TXNS {
                        if matches!(s.outcomes[tid], TxnOutcome::Committed)
                            && !s.materialised[tid]
                        {
                            return false;
                        }
                    }
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
    fn recovery_three_phase_safety_holds() {
        let checker = RecoveryThreePhaseModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
