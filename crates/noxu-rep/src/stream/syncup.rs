//! Replica-feeder syncup: diverged-tail matchpoint search and rollback
//! decision.
//!
//! Port of the decision core of `com.sleepycat.je.rep.stream.ReplicaFeederSyncup`
//! (`findMatchpoint` + `verifyRollback`) and the protocol exchange documented
//! in `FeederReplicaSyncup.java`.
//!
//! When a replica reconnects to a (possibly new) master, the two must agree on
//! the latest COMMON log entry — the *matchpoint* — defined as the highest VLSN
//! that both nodes hold at the *same LSN with a matching log record*. If the
//! replica has applied entries after that matchpoint (a *diverged tail*, e.g.
//! after a failed election), those entries must be ROLLED BACK to the
//! matchpoint before the stream resumes from `matchpoint + 1`.
//!
//! This module implements the *decision* core as pure, testable functions over
//! the VLSN→LSN substrate (the matchpoint search and the `verifyRollback`
//! truth table). The networked wire exchange (`EntryRequest` /
//! `EntryNotFound` / `AlternateMatchpoint`), the backward `ReplicaSyncupReader`,
//! and the live `replay.rollback` log/tree truncation are layered on top by the
//! replica stream driver (see the module-level note in `peer_feeder.rs` and the
//! REP-1 STEP 5 design note). The rollback EXECUTION reuses the durable
//! recovery machinery built in REP-1 STEPS 1-4 (RollbackStart/End entries,
//! RollbackTracker, TxnChain revert, invisible re-marking).

use noxu_util::{NULL_VLSN, Vlsn};

/// One node's per-VLSN log identity used for matchpoint comparison: the LSN
/// the entry lives at and a record fingerprint (checksum) used to confirm the
/// two nodes hold the *same* record at that VLSN, not merely the same VLSN.
///
/// JE compares full `OutputWireRecord.match(InputWireRecord)`; here the
/// fingerprint stands in for that record equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VlsnEntry {
    /// LSN where this VLSN's log entry resides.
    pub lsn: u64,
    /// Record fingerprint (e.g. entry checksum) for record-equality.
    pub fingerprint: u64,
    /// Whether this VLSN is a sync-point (a valid matchpoint candidate). JE
    /// only matches at sync points (`range.getLastSync()` and earlier sync
    /// entries scanned by `scanMatchpointEntries`).
    pub is_sync: bool,
}

/// A node's view of its replicated log for syncup: the VLSN range plus a way
/// to look up each VLSN's [`VlsnEntry`]. Models the replica's `VLSNIndex` +
/// log and the feeder's responses to `EntryRequest`.
pub trait SyncupView {
    /// Highest sync-point VLSN (`VLSNRange.getLastSync`) — the first
    /// matchpoint candidate.
    fn last_sync(&self) -> Vlsn;
    /// Highest commit/abort VLSN (`VLSNRange.getLastTxnEnd`) — the rollback
    /// safety boundary.
    fn last_txn_end(&self) -> Vlsn;
    /// First contiguous VLSN available (`VLSNRange.getFirst`). The search may
    /// not go below this.
    fn first_vlsn(&self) -> Vlsn;
    /// Look up the entry at `vlsn`, or `None` if this node does not hold it.
    fn entry(&self, vlsn: Vlsn) -> Option<VlsnEntry>;
}

/// Outcome of the matchpoint search (`ReplicaFeederSyncup.findMatchpoint`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Matchpoint {
    /// A common matchpoint was found at this VLSN and LSN.
    Found { vlsn: Vlsn, lsn: u64 },
    /// No common matchpoint exists within the replica's contiguous range; the
    /// replica must fall back to a network restore.
    None,
}

/// Search for the highest VLSN that BOTH the replica and the feeder hold with
/// a matching record at the same LSN, scanning the replica's sync points
/// backward from its `lastSync`.
///
/// Port of `ReplicaFeederSyncup.findMatchpoint`:
/// - The first candidate is `replica.last_sync()`.
/// - For each candidate, ask the feeder for the record at that VLSN; if the
///   feeder holds it and the records match, that is the matchpoint.
/// - Otherwise scan to the replica's previous sync point and retry.
/// - If the scan goes below the replica's first contiguous VLSN, there is no
///   matchpoint (network restore).
///
/// Records "match" when the feeder holds the VLSN and its fingerprint equals
/// the replica's (JE `OutputWireRecord.match`).
pub fn find_matchpoint(
    replica: &dyn SyncupView,
    feeder: &dyn SyncupView,
) -> Matchpoint {
    let candidate = replica.last_sync();

    // If the replica has no sync-able entries, the matchpoint is NULL and the
    // stream starts at VLSN 1 — provided the feeder holds VLSN 1.
    if candidate.is_null() {
        // JE getFeederRecord(range, FIRST_VLSN): if the feeder lacks VLSN 1,
        // it is a network restore.
        return Matchpoint::None;
    }

    let first = replica.first_vlsn();
    let mut candidate = candidate;

    loop {
        // Look at the replica's record at the candidate VLSN.
        let replica_entry = match replica.entry(candidate) {
            Some(e) => e,
            None => {
                // Went past the replica's contiguous range → no matchpoint.
                return Matchpoint::None;
            }
        };

        // Ask the feeder for the same VLSN and compare records.
        if let Some(feeder_entry) = feeder.entry(candidate)
            && feeder_entry.fingerprint == replica_entry.fingerprint
            && feeder_entry.lsn == replica_entry.lsn
        {
            return Matchpoint::Found {
                vlsn: candidate,
                lsn: replica_entry.lsn,
            };
        }

        // No match at this candidate; scan to the previous sync point.
        match prev_sync_candidate(replica, candidate, first) {
            Some(prev) => candidate = prev,
            None => return Matchpoint::None,
        }
    }
}

/// Find the replica's next sync-point VLSN strictly below `from`, not going
/// below `first`. Models `ReplicaFeederSyncup.scanMatchpointEntries`, which
/// scans the replica's log backward for the previous sync entry.
fn prev_sync_candidate(
    replica: &dyn SyncupView,
    from: Vlsn,
    first: Vlsn,
) -> Option<Vlsn> {
    let mut v = from.prev();
    while !v.is_null() && v >= first {
        if let Some(e) = replica.entry(v)
            && e.is_sync
        {
            return Some(v);
        }
        v = v.prev();
    }
    None
}

/// The action `verifyRollback` selects once a matchpoint search has completed.
///
/// Port of the `ReplicaFeederSyncup.verifyRollback` truth table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RollbackDecision {
    /// Roll the diverged tail back to the matchpoint, then resume streaming
    /// from `matchpoint + 1`. A "normal" rollback that does not cross a
    /// committed/aborted transaction end (`lastTxnEnd <= matchpoint`).
    RollbackToMatchpoint { matchpoint_vlsn: Vlsn, start_vlsn: Vlsn },
    /// The matchpoint would require rolling back past a transaction end that
    /// the replica has acknowledged. JE does a *hard recovery* (log truncation
    /// + restart) when truncation is permissible.
    HardRecovery { matchpoint_vlsn: Vlsn, last_txn_end: Vlsn },
    /// No safe matchpoint / truncation not permissible — the replica must do a
    /// full network restore from the master.
    NetworkRestore,
}

/// Decide what to do given the matchpoint search result and the replica's VLSN
/// range, faithful to `ReplicaFeederSyncup.verifyRollback`'s truth table.
///
/// `num_passed_commits` is JE `searchResults.getNumPassedCommits()`: the count
/// of commit/abort records the backward matchpoint scan stepped over. A
/// non-zero count means the matchpoint lies before a txn end even when
/// `lastTxnEnd <= matchpoint` numerically (the txn end was logged at a VLSN
/// the scan passed), forcing hard recovery rather than a normal rollback.
pub fn verify_rollback(
    matchpoint: &Matchpoint,
    last_txn_end: Vlsn,
    last_sync: Vlsn,
    num_passed_commits: u64,
) -> RollbackDecision {
    let matchpoint_vlsn = match matchpoint {
        Matchpoint::Found { vlsn, .. } => *vlsn,
        Matchpoint::None => NULL_VLSN,
    };

    // Row group 1: lastTxnEnd is NULL — no acknowledged txn end to protect, so
    // a normal rollback is always safe (rollback everything / to M).
    if last_txn_end.is_null() {
        // (NULL txn end, NULL sync, found M) "can't occur" — but if it does,
        // treat as no matchpoint → network restore is the conservative path.
        if last_sync.is_null() && !matchpoint_vlsn.is_null() {
            return RollbackDecision::NetworkRestore;
        }
        return rollback_to(matchpoint_vlsn);
    }

    // lastTxnEnd is non-null but no matchpoint was found: JE chooses network
    // restore (could hard-recover, but copying files is assumed cheaper).
    if matchpoint_vlsn.is_null() {
        return RollbackDecision::NetworkRestore;
    }

    // The matchpoint is at or after the last txn end AND the backward scan
    // passed no commits: a normal rollback (`lastTxnEnd <= matchpointVLSN`).
    if last_txn_end <= matchpoint_vlsn && num_passed_commits == 0 {
        return rollback_to(matchpoint_vlsn);
    }

    // Otherwise we would roll back past a commit/abort → hard recovery (JE
    // truncates the log and runs hard recovery when truncation is permissible;
    // the not-permissible / disabled cases degrade to network restore, which
    // the live driver decides with checkpoint-deleted-file information not
    // modelled here).
    RollbackDecision::HardRecovery { matchpoint_vlsn, last_txn_end }
}

fn rollback_to(matchpoint_vlsn: Vlsn) -> RollbackDecision {
    RollbackDecision::RollbackToMatchpoint {
        matchpoint_vlsn,
        start_vlsn: matchpoint_vlsn.next(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A simple map-backed [`SyncupView`] for tests.
    struct MapView {
        last_sync: Vlsn,
        last_txn_end: Vlsn,
        first: Vlsn,
        entries: HashMap<i64, VlsnEntry>,
    }

    impl MapView {
        fn new(first: i64, last_sync: i64, last_txn_end: i64) -> Self {
            Self {
                last_sync: Vlsn::new(last_sync),
                last_txn_end: Vlsn::new(last_txn_end),
                first: Vlsn::new(first),
                entries: HashMap::new(),
            }
        }
        fn put(
            mut self,
            vlsn: i64,
            lsn: u64,
            fingerprint: u64,
            is_sync: bool,
        ) -> Self {
            self.entries.insert(vlsn, VlsnEntry { lsn, fingerprint, is_sync });
            self
        }
    }

    impl SyncupView for MapView {
        fn last_sync(&self) -> Vlsn {
            self.last_sync
        }
        fn last_txn_end(&self) -> Vlsn {
            self.last_txn_end
        }
        fn first_vlsn(&self) -> Vlsn {
            self.first
        }
        fn entry(&self, vlsn: Vlsn) -> Option<VlsnEntry> {
            self.entries.get(&vlsn.sequence()).copied()
        }
    }

    /// The replica's last sync point matches the feeder directly: matchpoint =
    /// lastSync, no rollback of divergent tail beyond it.
    #[test]
    fn test_matchpoint_at_last_sync() {
        let replica = MapView::new(1, 5, 5)
            .put(5, 0x500, 0xAA, true)
            .put(4, 0x400, 0xBB, true);
        let feeder = MapView::new(1, 7, 7)
            .put(5, 0x500, 0xAA, true)
            .put(6, 0x600, 0xCC, true);

        let mp = find_matchpoint(&replica, &feeder);
        assert_eq!(mp, Matchpoint::Found { vlsn: Vlsn::new(5), lsn: 0x500 });
    }

    /// Diverged tail: the replica applied VLSN 6/7 that the feeder never had
    /// (its 6/7 differ or are absent). The search walks back to the highest
    /// common sync point (VLSN 4).
    #[test]
    fn test_diverged_tail_walks_back() {
        // Replica's sync points: 6 (divergent), 4 (common), with 5 a non-sync.
        let replica = MapView::new(1, 6, 6)
            .put(6, 0x600, 0xDEAD, true) // divergent — feeder lacks/differs
            .put(5, 0x500, 0x55, false) // not a sync point
            .put(4, 0x400, 0x44, true); // common sync
        let feeder = MapView::new(1, 8, 8)
            .put(6, 0x600, 0xBEEF, true) // different record at VLSN 6
            .put(4, 0x400, 0x44, true); // same record at VLSN 4

        let mp = find_matchpoint(&replica, &feeder);
        assert_eq!(mp, Matchpoint::Found { vlsn: Vlsn::new(4), lsn: 0x400 });
    }

    /// No common matchpoint within the replica's contiguous range → network
    /// restore.
    #[test]
    fn test_no_matchpoint_needs_restore() {
        let replica = MapView::new(4, 6, 6)
            .put(6, 0x600, 0x11, true)
            .put(5, 0x500, 0x22, true)
            .put(4, 0x400, 0x33, true);
        // Feeder holds none of the replica's records (all differ).
        let feeder = MapView::new(1, 8, 8)
            .put(6, 0x600, 0x99, true)
            .put(5, 0x500, 0x88, true)
            .put(4, 0x400, 0x77, true);

        assert_eq!(find_matchpoint(&replica, &feeder), Matchpoint::None);
    }

    /// verifyRollback: matchpoint at/after lastTxnEnd, no passed commits →
    /// normal rollback to the matchpoint, resume at matchpoint+1.
    #[test]
    fn test_verify_normal_rollback() {
        let mp = Matchpoint::Found { vlsn: Vlsn::new(5), lsn: 0x500 };
        let d = verify_rollback(&mp, Vlsn::new(5), Vlsn::new(5), 0);
        assert_eq!(
            d,
            RollbackDecision::RollbackToMatchpoint {
                matchpoint_vlsn: Vlsn::new(5),
                start_vlsn: Vlsn::new(6),
            }
        );
    }

    /// verifyRollback: rolling back past a committed txn end → hard recovery.
    #[test]
    fn test_verify_hard_recovery_past_commit() {
        let mp = Matchpoint::Found { vlsn: Vlsn::new(3), lsn: 0x300 };
        // lastTxnEnd (5) > matchpoint (3) → would roll back past a commit.
        let d = verify_rollback(&mp, Vlsn::new(5), Vlsn::new(6), 0);
        assert_eq!(
            d,
            RollbackDecision::HardRecovery {
                matchpoint_vlsn: Vlsn::new(3),
                last_txn_end: Vlsn::new(5),
            }
        );
    }

    /// verifyRollback: matchpoint == lastTxnEnd numerically but the scan
    /// passed a commit → still hard recovery (numPassedCommits != 0).
    #[test]
    fn test_verify_passed_commit_forces_hard_recovery() {
        let mp = Matchpoint::Found { vlsn: Vlsn::new(5), lsn: 0x500 };
        let d = verify_rollback(&mp, Vlsn::new(5), Vlsn::new(5), 1);
        assert!(matches!(d, RollbackDecision::HardRecovery { .. }));
    }

    /// verifyRollback: no acknowledged txn end → normal rollback regardless.
    #[test]
    fn test_verify_null_txn_end_normal_rollback() {
        let mp = Matchpoint::Found { vlsn: Vlsn::new(4), lsn: 0x400 };
        let d = verify_rollback(&mp, NULL_VLSN, Vlsn::new(4), 3);
        assert_eq!(
            d,
            RollbackDecision::RollbackToMatchpoint {
                matchpoint_vlsn: Vlsn::new(4),
                start_vlsn: Vlsn::new(5),
            }
        );
    }

    /// verifyRollback: non-null txn end but no matchpoint → network restore.
    #[test]
    fn test_verify_no_matchpoint_with_txn_end_restore() {
        let d =
            verify_rollback(&Matchpoint::None, Vlsn::new(5), Vlsn::new(5), 0);
        assert_eq!(d, RollbackDecision::NetworkRestore);
    }
}
