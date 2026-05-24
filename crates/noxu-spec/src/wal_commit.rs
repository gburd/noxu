//! WAL commit + group commit — `noxu-log::LogManager` +
//! `noxu-txn::Txn::commit_with_durability`.
//!
//! Models concurrent transactions writing TxnCommit records to the
//! WAL, with the group-commit handler coalescing fsyncs.
//!
//! Production code under model:
//!   - `crates/noxu-log/src/log_manager.rs`
//!   - `crates/noxu-log/src/log_buffer.rs`
//!   - `crates/noxu-txn/src/group_commit.rs`
//!   - `crates/noxu-txn/src/txn.rs::commit_with_durability`
//!
//! Properties:
//!   - `DurableImpliesLogged` — every transaction reported as
//!     committed by its caller has its TxnCommit record at an LSN
//!     ≤ the most recently fsynced LSN.
//!   - `LsnMonotone` — assigned LSNs strictly increase across
//!     commits.
//!   - `FsyncedNeverDecreases` — the fsynced high-water mark stays
//!     within `[0, next_lsn)` (a coarse termination check; a
//!     dedicated 2-state monotonicity check is left as future work).

use stateright::{Model, Property};

pub const N_TXNS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum TxnState {
    Pending,
    LogWritten { lsn: u64 },
    InGroup { lsn: u64 },
    Committed { lsn: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub txns: [TxnState; N_TXNS],
    /// LSN allocator — strictly monotonic.
    pub next_lsn: u64,
    /// Highest LSN that has been fsynced to disk.
    pub fsynced_lsn: u64,
    /// LSNs currently buffered in the group-commit handler.
    pub group_buffer: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    /// Txn writes its TxnCommit to the WAL buffer.
    WriteTxnCommit { tid: usize },
    /// Txn enters the group-commit handler (defer fsync).
    EnterGroup { tid: usize },
    /// Group-commit handler fsyncs everything buffered.
    FlushGroup,
    /// Txn commits without group-commit (immediate fsync).
    DirectFsync { tid: usize },
    /// Txn marks itself Committed after its LSN is fsynced.
    MarkCommitted { tid: usize },
}

pub struct WalCommitModel;

fn lsn_of(s: &State, tid: usize) -> Option<u64> {
    match s.txns[tid] {
        TxnState::Pending => None,
        TxnState::LogWritten { lsn }
        | TxnState::InGroup { lsn }
        | TxnState::Committed { lsn } => Some(lsn),
    }
}

impl Model for WalCommitModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            txns: [TxnState::Pending; N_TXNS],
            next_lsn: 1,
            fsynced_lsn: 0,
            group_buffer: vec![],
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        for tid in 0..N_TXNS {
            match s.txns[tid] {
                TxnState::Pending => out.push(Action::WriteTxnCommit { tid }),
                TxnState::LogWritten { .. } => {
                    out.push(Action::EnterGroup { tid });
                    out.push(Action::DirectFsync { tid });
                }
                TxnState::InGroup { lsn } => {
                    if lsn <= s.fsynced_lsn {
                        out.push(Action::MarkCommitted { tid });
                    }
                }
                TxnState::Committed { .. } => {}
            }
        }
        if !s.group_buffer.is_empty() {
            out.push(Action::FlushGroup);
        }
    }

    fn next_state(
        &self,
        s: &Self::State,
        a: Self::Action,
    ) -> Option<Self::State> {
        let mut s = s.clone();
        match a {
            Action::WriteTxnCommit { tid } => {
                let lsn = s.next_lsn;
                s.next_lsn += 1;
                s.txns[tid] = TxnState::LogWritten { lsn };
            }
            Action::EnterGroup { tid } => {
                let lsn = lsn_of(&s, tid)?;
                s.txns[tid] = TxnState::InGroup { lsn };
                s.group_buffer.push(lsn);
                s.group_buffer.sort_unstable();
            }
            Action::FlushGroup => {
                if let Some(&top) = s.group_buffer.last() {
                    s.fsynced_lsn = s.fsynced_lsn.max(top);
                }
                s.group_buffer.clear();
            }
            Action::DirectFsync { tid } => {
                let lsn = lsn_of(&s, tid)?;
                s.fsynced_lsn = s.fsynced_lsn.max(lsn);
                s.txns[tid] = TxnState::Committed { lsn };
            }
            Action::MarkCommitted { tid } => {
                let lsn = lsn_of(&s, tid)?;
                if lsn > s.fsynced_lsn {
                    return None;
                }
                s.txns[tid] = TxnState::Committed { lsn };
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("DurableImpliesLogged", |_, s: &State| {
                s.txns.iter().all(|t| match t {
                    TxnState::Committed { lsn } => *lsn <= s.fsynced_lsn,
                    _ => true,
                })
            }),
            Property::<Self>::always("LsnMonotone", |_, s: &State| {
                let mut lsns: Vec<u64> = s
                    .txns
                    .iter()
                    .filter_map(|t| match t {
                        TxnState::LogWritten { lsn }
                        | TxnState::InGroup { lsn }
                        | TxnState::Committed { lsn } => Some(*lsn),
                        _ => None,
                    })
                    .collect();
                lsns.sort_unstable();
                lsns.windows(2).all(|w| w[0] < w[1])
            }),
            Property::<Self>::always(
                "FsyncedNeverDecreases",
                |_, s: &State| s.fsynced_lsn < s.next_lsn,
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn wal_commit_safety_holds() {
        let checker = WalCommitModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
