//! Recovery 3-phase — `noxu-recovery::recovery_manager` (analysis →
//! redo → undo).
//!
//! Models a small WAL with N committed and M aborted transactions
//! and a checkpoint, exercising the find-end-of-log → analysis →
//! redo → undo pipeline.
//!
//! Production code under model:
//!   - `crates/noxu-recovery/src/recovery_manager.rs`
//!   - `crates/noxu-recovery/src/analysis_result.rs`   (≈ ARIES transaction table)
//!   - `crates/noxu-recovery/src/dirty_in_map.rs`       (≈ ARIES dirty page table)
//!
//! VALIDATED-AS-OF: v3.1.0 — Re-stamped after Wave-ZB re-audit (2026-05-30).
//! The production phases (analysis, redo, undo) are unchanged.
//! IdempotentReplay and AllAndOnlyCommitted still hold for the modelled
//! protocol.
//!
//! NOTE: Wave 11-U / 11-Y (C-6) added a mapping-tree undo pass between
//! analysis and data-LN redo for multi-DB environments. This pass enforces
//! the invariant: "after recovery, the database name registry contains only
//! databases whose creation was committed."
//!
//! TODO: model CatalogConsistency (C-6) — a `CatalogConsistency` property
//! analogous to `AllAndOnlyCommitted` but for NameLNTxn entries would
//! catch bugs in the undo predicate (e.g. a predicate that fails to remove
//! aborted database registrations). The mapping-tree undo pass is not yet
//! modelled here; the production correctness relies on the unit tests in
//! `recovery_manager.rs::test_c6_mapping_tree_undo_*` and the integration
//! test `test_c6_aborted_db_creation_not_recovered`. Tracked as a follow-up
//! spec update.
//!
//! Properties:
//!   - `AllAndOnlyCommitted` — after recovery, the live tree
//!     contains entries for every committed txn and no entries for
//!     any aborted txn.
//!   - `IdempotentReplay` — the materialisation produced by redo
//!     equals the materialisation produced by redo run twice.

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
    /// Snapshot of `materialised` immediately after the first
    /// `Action::Redo`. None until the first redo runs. Used by the
    /// `IdempotentReplay` invariant: after `Action::RedoAgain` the
    /// post-state must equal this snapshot.
    pub materialised_after_first_redo: Option<[bool; N_TXNS]>,
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
                        materialised_after_first_redo: None,
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
                if s.materialised_after_first_redo.is_none() {
                    s.materialised_after_first_redo = Some(s.materialised);
                }
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
                // 2-state idempotency: after Action::RedoAgain (i.e.
                // when redo_run_count > 1 and we are AfterRedo), the
                // current materialisation must equal the snapshot
                // taken after the first redo.
                if matches!(s.phase, Phase::AfterRedo) && s.redo_run_count > 1 {
                    if let Some(snap) = s.materialised_after_first_redo {
                        if snap != s.materialised {
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
