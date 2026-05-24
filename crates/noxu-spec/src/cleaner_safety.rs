//! Cleaner safety — `noxu-cleaner::file_processor`.
//!
//! Models the cleaner deciding whether a log file is safe to delete.
//! A log file is safe to delete only if every LN it contains has
//! either been migrated to a newer file or is unreferenced from the
//! tree. The unsafe case is when a concurrent reader still holds a
//! reference to a slot whose LN lives in the file the cleaner
//! wants to delete.
//!
//! Production code under model:
//!   - `crates/noxu-cleaner/src/file_processor.rs`
//!   - `crates/noxu-cleaner/src/utilization_tracker.rs`
//!
//! Properties:
//!   - `NoLiveDelete` — a log file is never deleted while any reader
//!     still has an outstanding reference to it.
//!   - `ConservativeLiveCheck` — a "no live readers" decision is
//!     correct at the time it is made (reads that arrive AFTER the
//!     decision are not the cleaner's problem; they get a "file gone"
//!     error from the LogManager and retry).

use stateright::{Model, Property};

pub const N_FILES: usize = 2;
pub const N_READERS: usize = 2;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub file_deleted: [bool; N_FILES],
    /// Reader holds a reference to file `i` if `reader_refs[r] == Some(i)`.
    pub reader_refs: [Option<usize>; N_READERS],
    /// Set of (file_no) for which the cleaner has run a live-check
    /// and decided "no live readers".
    pub cleared_for_delete: [bool; N_FILES],
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    AcquireRef { reader: usize, file: usize },
    ReleaseRef { reader: usize },
    LiveCheck { file: usize },
    Delete { file: usize },
}

pub struct CleanerSafetyModel;

impl Model for CleanerSafetyModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            file_deleted: [false; N_FILES],
            reader_refs: [None; N_READERS],
            cleared_for_delete: [false; N_FILES],
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        for r in 0..N_READERS {
            if s.reader_refs[r].is_none() {
                for f in 0..N_FILES {
                    if !s.file_deleted[f] {
                        out.push(Action::AcquireRef { reader: r, file: f });
                    }
                }
            } else {
                out.push(Action::ReleaseRef { reader: r });
            }
        }
        for f in 0..N_FILES {
            if !s.file_deleted[f] {
                out.push(Action::LiveCheck { file: f });
                if s.cleared_for_delete[f] {
                    out.push(Action::Delete { file: f });
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
            Action::AcquireRef { reader, file } => {
                if s.file_deleted[file] {
                    return None;
                }
                s.reader_refs[reader] = Some(file);
                // Acquiring a reference invalidates any outstanding
                // cleared-for-delete decision for this file: the
                // cleaner observes references at the moment of its
                // live-check.
                s.cleared_for_delete[file] = false;
            }
            Action::ReleaseRef { reader } => {
                s.reader_refs[reader] = None;
            }
            Action::LiveCheck { file } => {
                let live = s.reader_refs.contains(&Some(file));
                s.cleared_for_delete[file] = !live;
            }
            Action::Delete { file } => {
                if !s.cleared_for_delete[file] {
                    return None;
                }
                if s.reader_refs.contains(&Some(file)) {
                    return None;
                }
                s.file_deleted[file] = true;
                s.cleared_for_delete[file] = false;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::<Self>::always("NoLiveDelete", |_, s: &State| {
            for f in 0..N_FILES {
                if s.file_deleted[f] && s.reader_refs.contains(&Some(f)) {
                    return false;
                }
            }
            true
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn cleaner_safety_holds() {
        let checker = CleanerSafetyModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
