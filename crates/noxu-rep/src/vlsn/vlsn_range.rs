//! VLSN range tracking.
//!
//! Tracks the range of
//! VLSNs available on this node, including the first and last VLSN in the
//! contiguous range, as well as the last committed and last synced VLSNs.

use noxu_log::LogEntryType;

/// Tracks the range of VLSNs available on this node.
///
/// All range values must be viewed together to ensure a consistent set of
/// values. A VLSN value of 0 is treated as NULL/empty (equivalent to the
/// `VLSN.NULL_VLSN`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlsnRange {
    /// First available VLSN (inclusive). 0 means empty.
    first: u64,
    /// Last available VLSN (inclusive). 0 means empty.
    last: u64,
    /// `lastTxnEnd` (JE VLSNRange.lastTxnEnd): the highest VLSN of a
    /// commit/abort log entry. This is the rollback safety boundary used by
    /// the syncup `verifyRollback` decision (a replica must not roll back
    /// past a transaction end it has acknowledged). NOT the same as the sync
    /// matchpoint — see `sync_vlsn`.
    commit_vlsn: u64,
    /// `lastSync` (JE VLSNRange.lastSync): the highest VLSN of a sync-point
    /// log entry. This is the initial matchpoint candidate in
    /// `ReplicaFeederSyncup.findMatchpoint`. JE notes lastSync and lastTxnEnd
    /// are currently the same value but are kept distinct because the
    /// Matchpoint log entry may make lastSync run ahead of lastTxnEnd.
    sync_vlsn: u64,
}

impl Default for VlsnRange {
    fn default() -> Self {
        Self::new()
    }
}

impl VlsnRange {
    /// Create an empty range with all fields set to 0 (NULL_VLSN).
    pub fn new() -> Self {
        VlsnRange { first: 0, last: 0, commit_vlsn: 0, sync_vlsn: 0 }
    }

    /// Create a range with the given first and last VLSNs.
    /// The commit and sync VLSNs are initialized to 0.
    pub fn with_range(first: u64, last: u64) -> Self {
        assert!(
            first == 0 || first <= last,
            "first ({}) must be <= last ({})",
            first,
            last
        );
        VlsnRange { first, last, commit_vlsn: 0, sync_vlsn: 0 }
    }

    /// Return the first available VLSN (inclusive).
    pub fn get_first(&self) -> u64 {
        self.first
    }

    /// Return the last available VLSN (inclusive).
    pub fn get_last(&self) -> u64 {
        self.last
    }

    /// Return `lastTxnEnd`: the highest commit/abort VLSN (rollback boundary).
    pub fn get_commit_vlsn(&self) -> u64 {
        self.commit_vlsn
    }

    /// JE-faithful alias for `get_commit_vlsn` (JE VLSNRange.getLastTxnEnd).
    pub fn get_last_txn_end(&self) -> u64 {
        self.commit_vlsn
    }

    /// Return `lastSync`: the highest sync-point VLSN (matchpoint candidate).
    pub fn get_sync_vlsn(&self) -> u64 {
        self.sync_vlsn
    }

    /// JE-faithful alias for `get_sync_vlsn` (JE VLSNRange.getLastSync).
    pub fn get_last_sync(&self) -> u64 {
        self.sync_vlsn
    }

    /// Alias for `get_first`  -  return the first available VLSN.
    pub fn first(&self) -> u64 {
        self.first
    }

    /// Alias for `get_last`  -  return the last available VLSN.
    pub fn last(&self) -> u64 {
        self.last
    }

    /// Return true if this range is empty (first == 0).
    pub fn is_empty(&self) -> bool {
        self.first == 0
    }

    /// Return true if this VLSN is within the range described by this struct.
    ///
    ///
    pub fn contains(&self, vlsn: u64) -> bool {
        if self.first == 0 {
            return false;
        }
        self.first <= vlsn && vlsn <= self.last
    }

    /// Return the number of VLSNs in this range.
    /// Returns 0 if the range is empty.
    pub fn len(&self) -> u64 {
        if self.first == 0 {
            return 0;
        }
        self.last - self.first + 1
    }

    /// Extend the range to include a new VLSN.
    ///
    /// If the range is empty, the VLSN becomes both first and last.
    /// Otherwise, first is updated if the new VLSN is smaller, and last
    /// is updated if the new VLSN is larger.
    ///
    ///
    ///
    /// Note: does not track per-entry-type commit/barrier VLSNs (those are
    /// managed by `VlsnIndex` at a higher level).
    pub fn extend(&mut self, vlsn: u64) {
        assert!(vlsn > 0, "Cannot extend with NULL_VLSN (0)");
        if self.first == 0 || vlsn < self.first {
            self.first = vlsn;
        }
        if vlsn > self.last {
            self.last = vlsn;
        }
    }

    /// Update `lastTxnEnd` (the commit/abort boundary).
    ///
    /// Advances forward only; a VLSN below the current value is a no-op.
    pub fn update_commit(&mut self, vlsn: u64) {
        if vlsn > self.commit_vlsn {
            self.commit_vlsn = vlsn;
        }
    }

    /// Update `lastSync` (the sync-point matchpoint candidate).
    ///
    /// Advances forward only; a VLSN below the current value is a no-op.
    pub fn update_sync(&mut self, vlsn: u64) {
        if vlsn > self.sync_vlsn {
            self.sync_vlsn = vlsn;
        }
    }

    /// JE-faithful update: extend the range for a new (vlsn, entry-type)
    /// mapping, dispatching `lastSync`/`lastTxnEnd` by entry type.
    ///
    /// Mirrors `VLSNRange.getUpdateForNewMapping` (VLSNRange.java:162-190):
    ///   - always extend `first`/`last`;
    ///   - if `entry_type.is_sync_point()` advance `lastSync` (`sync_vlsn`);
    ///   - if the entry is a commit or abort advance `lastTxnEnd`
    ///     (`commit_vlsn`).
    ///
    /// This is the canonical path that keeps `lastSync` and `lastTxnEnd`
    /// distinct as JE intends; the syncup matchpoint protocol (the consumer)
    /// is tracked separately as a parity gap.
    pub fn update_for_new_mapping(
        &mut self,
        vlsn: u64,
        entry_type: LogEntryType,
    ) {
        self.extend(vlsn);
        if entry_type.is_sync_point() && vlsn > self.sync_vlsn {
            self.sync_vlsn = vlsn;
        }
        if matches!(
            entry_type,
            LogEntryType::TxnCommit | LogEntryType::TxnAbort
        ) && vlsn > self.commit_vlsn
        {
            self.commit_vlsn = vlsn;
        }
    }

    /// Truncate VLSNs after the given value (for rollback).
    ///
    /// Sets the last VLSN to the given value. If the truncation point is
    /// before the first VLSN, the range becomes empty. The commit and sync
    /// VLSNs are clamped to the new last VLSN.
    ///
    ///
    pub fn truncate_after(&mut self, vlsn: u64) {
        if vlsn == 0 || (self.first > 0 && vlsn < self.first) {
            // Truncation point is before the range start; empty the range.
            self.first = 0;
            self.last = 0;
            self.commit_vlsn = 0;
            self.sync_vlsn = 0;
            return;
        }
        if self.first == 0 {
            // Already empty, nothing to truncate.
            return;
        }
        self.last = vlsn;
        // Clamp commit and sync to new last.
        if self.commit_vlsn > vlsn {
            self.commit_vlsn = vlsn;
        }
        if self.sync_vlsn > vlsn {
            self.sync_vlsn = vlsn;
        }
    }

    /// Merge with another range (union).
    ///
    /// The resulting range spans the union of both ranges. The commit and
    /// sync VLSNs are set to the maximum of the two ranges. NULL (0) values
    /// are handled: a non-null value always takes precedence over null.
    ///
    /// And `VLSNRange.getUpdate()`.
    pub fn merge(&mut self, other: &VlsnRange) {
        // Merge first: take the smaller non-zero value.
        self.first = match (self.first, other.first) {
            (0, b) => b,
            (a, 0) => a,
            (a, b) => a.min(b),
        };
        // Merge last: take the larger non-zero value.
        self.last = match (self.last, other.last) {
            (0, b) => b,
            (a, 0) => a,
            (a, b) => a.max(b),
        };
        // Merge commit_vlsn: take the larger non-zero value.
        self.commit_vlsn = match (self.commit_vlsn, other.commit_vlsn) {
            (0, b) => b,
            (a, 0) => a,
            (a, b) => a.max(b),
        };
        // Merge sync_vlsn: take the larger non-zero value.
        self.sync_vlsn = match (self.sync_vlsn, other.sync_vlsn) {
            (0, b) => b,
            (a, 0) => a,
            (a, b) => a.max(b),
        };
    }
}

impl std::fmt::Display for VlsnRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "first={} last={} commit={} sync={}",
            self.first, self.last, self.commit_vlsn, self.sync_vlsn
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_for_new_mapping_dispatch() {
        use noxu_log::LogEntryType;
        let mut r = VlsnRange::new();
        // A non-txn insert LN: extends first/last only, leaves lastSync /
        // lastTxnEnd at 0.
        r.update_for_new_mapping(5, LogEntryType::InsertLN);
        assert_eq!(r.get_last(), 5);
        assert_eq!(r.get_last_sync(), 0, "InsertLN is not a sync point");
        assert_eq!(r.get_last_txn_end(), 0, "InsertLN is not a commit/abort");
        // A commit: advances BOTH lastTxnEnd and lastSync (commit is a sync
        // point in JE — is_sync_point() includes TxnCommit).
        r.update_for_new_mapping(8, LogEntryType::TxnCommit);
        assert_eq!(r.get_last(), 8);
        assert_eq!(r.get_last_txn_end(), 8, "commit advances lastTxnEnd");
        assert_eq!(r.get_last_sync(), 8, "commit is a sync point -> lastSync");
        // A Matchpoint at 12: a sync point but NOT a commit/abort, so lastSync
        // runs AHEAD of lastTxnEnd (the exact JE scenario the two distinct
        // fields exist for).
        r.update_for_new_mapping(12, LogEntryType::Matchpoint);
        assert_eq!(r.get_last_sync(), 12, "matchpoint advances lastSync");
        assert_eq!(
            r.get_last_txn_end(),
            8,
            "matchpoint is not a txn end -> lastTxnEnd unchanged"
        );
    }

    #[test]
    fn test_new_empty() {
        let range = VlsnRange::new();
        assert!(range.is_empty());
        assert_eq!(range.get_first(), 0);
        assert_eq!(range.get_last(), 0);
        assert_eq!(range.get_commit_vlsn(), 0);
        assert_eq!(range.get_sync_vlsn(), 0);
        assert_eq!(range.len(), 0);
    }

    #[test]
    fn test_with_range() {
        let range = VlsnRange::with_range(5, 10);
        assert!(!range.is_empty());
        assert_eq!(range.get_first(), 5);
        assert_eq!(range.get_last(), 10);
        assert_eq!(range.len(), 6);
    }

    #[test]
    fn test_with_range_single() {
        let range = VlsnRange::with_range(7, 7);
        assert!(!range.is_empty());
        assert_eq!(range.len(), 1);
    }

    #[test]
    fn test_contains() {
        let range = VlsnRange::with_range(5, 10);
        assert!(!range.contains(4));
        assert!(range.contains(5));
        assert!(range.contains(7));
        assert!(range.contains(10));
        assert!(!range.contains(11));
    }

    #[test]
    fn test_contains_empty() {
        let range = VlsnRange::new();
        assert!(!range.contains(0));
        assert!(!range.contains(1));
        assert!(!range.contains(100));
    }

    #[test]
    fn test_extend_from_empty() {
        let mut range = VlsnRange::new();
        range.extend(5);
        assert!(!range.is_empty());
        assert_eq!(range.get_first(), 5);
        assert_eq!(range.get_last(), 5);
        assert_eq!(range.len(), 1);
    }

    #[test]
    fn test_extend_forward() {
        let mut range = VlsnRange::with_range(5, 10);
        range.extend(15);
        assert_eq!(range.get_first(), 5);
        assert_eq!(range.get_last(), 15);
        assert_eq!(range.len(), 11);
    }

    #[test]
    fn test_extend_backward() {
        let mut range = VlsnRange::with_range(5, 10);
        range.extend(2);
        assert_eq!(range.get_first(), 2);
        assert_eq!(range.get_last(), 10);
    }

    #[test]
    fn test_extend_within() {
        let mut range = VlsnRange::with_range(5, 10);
        range.extend(7);
        assert_eq!(range.get_first(), 5);
        assert_eq!(range.get_last(), 10);
    }

    #[test]
    fn test_commit_vlsn() {
        let mut range = VlsnRange::with_range(1, 10);
        assert_eq!(range.get_commit_vlsn(), 0);
        range.update_commit(5);
        assert_eq!(range.get_commit_vlsn(), 5);
        range.update_commit(8);
        assert_eq!(range.get_commit_vlsn(), 8);
        // Commit VLSN should not go backwards.
        range.update_commit(3);
        assert_eq!(range.get_commit_vlsn(), 8);
    }

    #[test]
    fn test_sync_vlsn() {
        let mut range = VlsnRange::with_range(1, 10);
        assert_eq!(range.get_sync_vlsn(), 0);
        range.update_sync(4);
        assert_eq!(range.get_sync_vlsn(), 4);
        range.update_sync(9);
        assert_eq!(range.get_sync_vlsn(), 9);
        // Sync VLSN should not go backwards.
        range.update_sync(2);
        assert_eq!(range.get_sync_vlsn(), 9);
    }

    #[test]
    fn test_truncate_after_middle() {
        let mut range = VlsnRange::with_range(5, 20);
        range.update_commit(15);
        range.update_sync(18);
        range.truncate_after(12);
        assert_eq!(range.get_first(), 5);
        assert_eq!(range.get_last(), 12);
        assert_eq!(range.get_commit_vlsn(), 12);
        assert_eq!(range.get_sync_vlsn(), 12);
        assert_eq!(range.len(), 8);
    }

    #[test]
    fn test_truncate_after_before_first() {
        let mut range = VlsnRange::with_range(5, 10);
        range.truncate_after(3);
        assert!(range.is_empty());
        assert_eq!(range.len(), 0);
    }

    #[test]
    fn test_truncate_after_at_last() {
        let mut range = VlsnRange::with_range(5, 10);
        range.truncate_after(10);
        assert_eq!(range.get_first(), 5);
        assert_eq!(range.get_last(), 10);
        assert_eq!(range.len(), 6);
    }

    #[test]
    fn test_truncate_empty() {
        let mut range = VlsnRange::new();
        range.truncate_after(5);
        assert!(range.is_empty());
    }

    #[test]
    fn test_truncate_to_zero() {
        let mut range = VlsnRange::with_range(1, 10);
        range.update_commit(5);
        range.update_sync(7);
        range.truncate_after(0);
        assert!(range.is_empty());
        assert_eq!(range.get_commit_vlsn(), 0);
        assert_eq!(range.get_sync_vlsn(), 0);
    }

    #[test]
    fn test_merge_both_non_empty() {
        let mut range_a = VlsnRange::with_range(5, 10);
        range_a.update_commit(8);
        range_a.update_sync(7);

        let mut range_b = VlsnRange::with_range(8, 15);
        range_b.update_commit(12);
        range_b.update_sync(14);

        range_a.merge(&range_b);
        assert_eq!(range_a.get_first(), 5);
        assert_eq!(range_a.get_last(), 15);
        assert_eq!(range_a.get_commit_vlsn(), 12);
        assert_eq!(range_a.get_sync_vlsn(), 14);
    }

    #[test]
    fn test_merge_with_empty() {
        let mut range_a = VlsnRange::with_range(5, 10);
        range_a.update_commit(8);
        let range_b = VlsnRange::new();

        range_a.merge(&range_b);
        assert_eq!(range_a.get_first(), 5);
        assert_eq!(range_a.get_last(), 10);
        assert_eq!(range_a.get_commit_vlsn(), 8);
    }

    #[test]
    fn test_merge_empty_with_non_empty() {
        let mut range_a = VlsnRange::new();
        let mut range_b = VlsnRange::with_range(3, 7);
        range_b.update_commit(5);

        range_a.merge(&range_b);
        assert_eq!(range_a.get_first(), 3);
        assert_eq!(range_a.get_last(), 7);
        assert_eq!(range_a.get_commit_vlsn(), 5);
    }

    #[test]
    fn test_merge_disjoint_ranges() {
        let mut range_a = VlsnRange::with_range(1, 5);
        let range_b = VlsnRange::with_range(10, 15);

        range_a.merge(&range_b);
        assert_eq!(range_a.get_first(), 1);
        assert_eq!(range_a.get_last(), 15);
    }

    #[test]
    fn test_display() {
        let mut range = VlsnRange::with_range(1, 10);
        range.update_commit(5);
        range.update_sync(8);
        let s = format!("{}", range);
        assert!(s.contains("first=1"));
        assert!(s.contains("last=10"));
        assert!(s.contains("commit=5"));
        assert!(s.contains("sync=8"));
    }

    #[test]
    fn test_default() {
        let range = VlsnRange::default();
        assert!(range.is_empty());
    }

    #[test]
    fn test_clone_eq() {
        let mut range = VlsnRange::with_range(1, 10);
        range.update_commit(5);
        range.update_sync(8);
        let cloned = range.clone();
        assert_eq!(range, cloned);
    }

    // -------------------------------------------------------------------------
    // Ported from VLSNConsistencyTest.java — ordering invariants
    // -------------------------------------------------------------------------

    /// The commit VLSN must never exceed the last VLSN in the range.
    /// Ported from VLSNConsistencyTest — VLSN ordering invariant.
    #[test]
    fn test_commit_vlsn_le_last() {
        let mut range = VlsnRange::with_range(1, 20);
        range.update_commit(20);
        assert!(range.get_commit_vlsn() <= range.get_last());

        // A commit beyond last must be rejected (update_commit only advances
        // within the range semantics — but the invariant is enforced by
        // callers; here we verify that truncation clamps it).
        range.truncate_after(15);
        assert!(
            range.get_commit_vlsn() <= range.get_last(),
            "commit vlsn must be clamped after truncation"
        );
    }

    /// The sync VLSN must never exceed the last VLSN in the range.
    #[test]
    fn test_sync_vlsn_le_last() {
        let mut range = VlsnRange::with_range(1, 20);
        range.update_sync(18);
        range.truncate_after(12);
        assert!(
            range.get_sync_vlsn() <= range.get_last(),
            "sync vlsn must be clamped after truncation"
        );
    }

    /// After any number of extend() calls, first must remain <= last.
    #[test]
    fn test_extend_maintains_first_le_last() {
        let mut range = VlsnRange::new();
        for v in [10u64, 3, 20, 7, 15, 1, 25] {
            range.extend(v);
            assert!(
                range.get_first() <= range.get_last(),
                "first <= last violated after extend({})",
                v
            );
        }
        assert_eq!(range.get_first(), 1);
        assert_eq!(range.get_last(), 25);
    }

    /// Truncation to a point within the range preserves first <= last.
    #[test]
    fn test_truncate_preserves_first_le_last() {
        for last in 1u64..=20 {
            let mut range = VlsnRange::with_range(1, 20);
            range.truncate_after(last);
            if !range.is_empty() {
                assert!(
                    range.get_first() <= range.get_last(),
                    "first <= last violated when truncate_after({})",
                    last
                );
            }
        }
    }

    /// Merging two ranges always yields first <= last (if non-empty).
    #[test]
    fn test_merge_maintains_first_le_last() {
        let cases: &[(u64, u64, u64, u64)] =
            &[(1, 10, 5, 15), (5, 5, 5, 5), (1, 1, 100, 100), (3, 7, 1, 4)];
        for &(af, al, bf, bl) in cases {
            let mut a = VlsnRange::with_range(af, al);
            let b = VlsnRange::with_range(bf, bl);
            a.merge(&b);
            assert!(
                a.get_first() <= a.get_last(),
                "first <= last violated after merge ({},{}) + ({},{})",
                af,
                al,
                bf,
                bl
            );
        }
    }

    /// Commit VLSN must never move backwards (monotonically non-decreasing).
    #[test]
    fn test_commit_vlsn_monotone() {
        let mut range = VlsnRange::with_range(1, 100);
        let updates = [5u64, 10, 8, 20, 15, 30];
        let mut prev = 0u64;
        for &v in &updates {
            range.update_commit(v);
            let current = range.get_commit_vlsn();
            assert!(current >= prev, "commit vlsn must not decrease");
            prev = current;
        }
    }

    /// Sync VLSN must never move backwards.
    #[test]
    fn test_sync_vlsn_monotone() {
        let mut range = VlsnRange::with_range(1, 100);
        let updates = [3u64, 12, 7, 25, 18, 40];
        let mut prev = 0u64;
        for &v in &updates {
            range.update_sync(v);
            let current = range.get_sync_vlsn();
            assert!(current >= prev, "sync vlsn must not decrease");
            prev = current;
        }
    }

    /// An empty range contains no vlsn.
    #[test]
    fn test_empty_range_contains_nothing() {
        let range = VlsnRange::new();
        for v in [0u64, 1, 100, u64::MAX / 2] {
            assert!(!range.contains(v));
        }
    }

    /// Verify that the length formula is always correct relative to
    /// first and last.
    #[test]
    fn test_len_formula() {
        let cases: &[(u64, u64)] = &[(1, 1), (1, 10), (5, 20), (100, 100)];
        for &(f, l) in cases {
            let range = VlsnRange::with_range(f, l);
            assert_eq!(
                range.len(),
                l - f + 1,
                "len mismatch for ({},{})",
                f,
                l
            );
        }
        assert_eq!(VlsnRange::new().len(), 0);
    }
}
