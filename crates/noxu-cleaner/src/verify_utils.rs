//! Cleaner data-structure verification.
//!
//! Faithful port of `VerifyUtils.checkLsns()`
//! (`je/src/com/sleepycat/je/cleaner/VerifyUtils.java`).
//!
//! `checkLsns` compares the LSNs referenced by a live B-tree against the
//! obsolete LSNs recorded in the `UtilizationProfile` / `UtilizationTracker`.
//! It asserts that the two sets are DISJOINT: a live tree LSN must never
//! appear in the obsolete set. A violation means the utilization profile has
//! mislabelled a live LSN as obsolete, which would let the cleaner delete
//! live data — a `LOG_INTEGRITY` failure in JE.
//!
//! JE walks the tree with a `SortedLSNTreeWalker` + `GatherLSNs` processor to
//! collect the live LSN set, then per file pulls the obsolete offsets from
//! `UtilizationProfile.getObsoleteDetailPacked` and rebuilds obsolete LSNs
//! with `DbLsn.makeLsn(fileNum, offset)`. Here the caller supplies the live
//! LSN set (gathered by the engine-side tree walk) and we pull the obsolete
//! offsets from the `UtilizationTracker`.

use crate::utilization_tracker::UtilizationTracker;
use noxu_util::{Lsn, NULL_LSN};
use std::collections::HashSet;

/// Outcome of `check_lsns`: the live LSNs that were wrongly recorded as
/// obsolete (the disjointness violations). Empty == healthy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckLsnsResult {
    /// Live tree LSNs that are also present in the obsolete set.
    /// In JE these trigger "Obsolete LSN set contains valid LSN" and a
    /// `LOG_INTEGRITY` `EnvironmentFailureException`.
    pub obsolete_contains_live: Vec<Lsn>,
}

impl CheckLsnsResult {
    /// Whether the check passed (no live LSN was found in the obsolete set).
    pub fn is_ok(&self) -> bool {
        self.obsolete_contains_live.is_empty()
    }
}

/// Gather the obsolete LSN set recorded in the `UtilizationTracker`.
///
/// Mirrors the JE loop that, per file, walks `getObsoleteDetailPacked` and
/// rebuilds `DbLsn.makeLsn(fileNum, offset)`. Here the offsets come from each
/// `TrackedFileSummary`'s obsolete-offset list (only populated when detail
/// tracking is enabled).
pub fn obsolete_lsn_set(tracker: &UtilizationTracker) -> HashSet<Lsn> {
    let mut obsolete = HashSet::new();
    for (&file_num, tracked) in tracker.get_tracked_files() {
        for &offset in tracked.get_obsolete_offsets() {
            obsolete.insert(Lsn::new(file_num, offset));
        }
    }
    obsolete
}

/// Compare the live tree LSNs against the obsolete LSNs and assert
/// disjointness.
///
/// Faithful port of the core of `VerifyUtils.checkLsns()`: a live tree LSN
/// must NOT be in the obsolete set. `live_lsns` is the set gathered by the
/// caller's tree walk (JE's `GatherLSNs`); the obsolete set is derived from
/// `tracker` (JE's `UtilizationProfile.getObsoleteDetailPacked` per file).
///
/// `NULL_LSN` entries are ignored on both sides (JE's `GatherLSNs.processLSN`
/// skips `DbLsn.NULL_LSN`).
pub fn check_lsns<I>(
    live_lsns: I,
    tracker: &UtilizationTracker,
) -> CheckLsnsResult
where
    I: IntoIterator<Item = Lsn>,
{
    let obsolete = obsolete_lsn_set(tracker);
    let mut result = CheckLsnsResult::default();

    // JE: "Check that none of the LSNs in the tree is in the UP."
    for lsn in live_lsns {
        if lsn == NULL_LSN {
            continue;
        }
        if obsolete.contains(&lsn) {
            result.obsolete_contains_live.push(lsn);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilization_tracker::UtilizationTracker;

    /// On a healthy env the live LSNs are disjoint from the obsolete set,
    /// so `check_lsns` passes.
    #[test]
    fn test_check_lsns_healthy_passes() {
        let mut tracker = UtilizationTracker::new(true);
        // File 1: offsets 100, 200 are obsolete.
        tracker.count_obsolete_node(1, 100, 50, true, None);
        tracker.count_obsolete_node(1, 200, 50, true, None);

        // Live tree LSNs point at DIFFERENT offsets — disjoint.
        let live = vec![Lsn::new(1, 300), Lsn::new(1, 400), Lsn::new(2, 10)];
        let result = check_lsns(live, &tracker);
        assert!(result.is_ok(), "healthy env must pass: {result:?}");
    }

    /// A seeded violation — a live LSN wrongly recorded as obsolete — is
    /// DETECTED (JE: "Obsolete LSN set contains valid LSN" -> LOG_INTEGRITY).
    #[test]
    fn test_check_lsns_detects_live_in_obsolete() {
        let mut tracker = UtilizationTracker::new(true);
        // Seed the bug: offset 300 in file 1 is recorded obsolete...
        tracker.count_obsolete_node(1, 300, 50, true, None);

        // ...but the live tree still references LSN (1, 300).
        let live = vec![Lsn::new(1, 300), Lsn::new(1, 400)];
        let result = check_lsns(live, &tracker);
        assert!(!result.is_ok(), "violation must be detected");
        assert_eq!(result.obsolete_contains_live, vec![Lsn::new(1, 300)]);
    }

    /// NULL_LSN is ignored on the live side (JE GatherLSNs.processLSN skips
    /// DbLsn.NULL_LSN).
    #[test]
    fn test_check_lsns_ignores_null_lsn() {
        let tracker = UtilizationTracker::new(true);
        let live = vec![NULL_LSN, Lsn::new(1, 10)];
        let result = check_lsns(live, &tracker);
        assert!(result.is_ok());
    }
}
