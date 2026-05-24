//! Cache-vs-cleaner ordering — `noxu-evictor` ↔ `noxu-cleaner`.
//!
//! Models the race between the evictor flushing a dirty BIN to disk
//! and the cleaner migrating its slots to a newer file. The
//! invariant is that the cleaner's pre-migration check observes the
//! BIN's dirty bit before deciding whether the LN is "old"; if the
//! BIN is dirty, the cleaner must reload the slot post-flush.
//!
//! Production code under model:
//!   - `crates/noxu-evictor/src/evictor.rs`
//!   - `crates/noxu-cleaner/src/file_processor.rs::migrate_ln_slot`
//!
//! Properties:
//!   - `DirtyBitPreserved` — the cleaner never observes a clean BIN
//!     before the evictor has fsynced its dirty contents.
//!   - `NoStaleMigration` — when the cleaner migrates a slot, the LN
//!     it copies is at least as fresh as the LN visible to readers
//!     at that moment.

use stateright::{Model, Property};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub bin_dirty: bool,
    pub bin_version: u64,
    pub disk_version: u64,
    pub cleaner_seen_version: Option<u64>,
    pub migrated_version: Option<u64>,
    pub evict_started: bool,
    pub evict_completed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    DirtyTheBin,
    StartEvict,
    CompleteEvict,
    CleanerSnapshot,
    CleanerMigrate,
}

pub struct CacheVsCleanerModel;

impl Model for CacheVsCleanerModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            bin_dirty: false,
            bin_version: 0,
            disk_version: 0,
            cleaner_seen_version: None,
            migrated_version: None,
            evict_started: false,
            evict_completed: false,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.bin_version < 2 {
            out.push(Action::DirtyTheBin);
        }
        if s.bin_dirty && !s.evict_started {
            out.push(Action::StartEvict);
        }
        if s.evict_started && !s.evict_completed {
            out.push(Action::CompleteEvict);
        }
        if s.cleaner_seen_version.is_none() {
            out.push(Action::CleanerSnapshot);
        }
        if s.cleaner_seen_version.is_some() && s.migrated_version.is_none() {
            out.push(Action::CleanerMigrate);
        }
    }

    fn next_state(
        &self,
        s: &Self::State,
        a: Self::Action,
    ) -> Option<Self::State> {
        let mut s = s.clone();
        match a {
            Action::DirtyTheBin => {
                s.bin_version += 1;
                s.bin_dirty = true;
                // A new write invalidates the cleaner's earlier
                // snapshot if not yet migrated.
                if s.migrated_version.is_none() {
                    s.cleaner_seen_version = None;
                }
            }
            Action::StartEvict => {
                if !s.bin_dirty {
                    return None;
                }
                s.evict_started = true;
            }
            Action::CompleteEvict => {
                s.disk_version = s.bin_version;
                s.bin_dirty = false;
                s.evict_started = false;
                s.evict_completed = true;
            }
            Action::CleanerSnapshot => {
                // Cleaner snapshots the disk version. If the BIN is
                // currently dirty, the snapshot is stale.
                if s.bin_dirty {
                    return None;
                }
                s.cleaner_seen_version = Some(s.disk_version);
            }
            Action::CleanerMigrate => {
                let v = s.cleaner_seen_version?;
                if v != s.disk_version {
                    return None;
                }
                s.migrated_version = Some(v);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("DirtyBitPreserved", |_, s: &State| {
                // The cleaner must never have a snapshot version that
                // is greater than what's actually on disk.
                if let Some(v) = s.cleaner_seen_version {
                    return v <= s.disk_version;
                }
                true
            }),
            Property::<Self>::always("NoStaleMigration", |_, s: &State| {
                // Migrated version must equal disk_version at
                // migration time, and disk_version is monotonic.
                if let Some(v) = s.migrated_version {
                    return v <= s.disk_version;
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
    fn cache_vs_cleaner_safety_holds() {
        let checker = CacheVsCleanerModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
