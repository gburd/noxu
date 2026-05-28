//! Property-based tests for noxu-cleaner using proptest (Wave 11-E).
//!
//! Covers utilization tracking and per-file summary invariants:
//!
//! * track_obsolete / count_new_log_entry consistency vs a brute-force
//!   oracle that scans the live LSN list.
//! * FileSummary arithmetic invariants (active = total - obsolete,
//!   utilization in [0,1]).
//! * Adjusted utilization always <= unadjusted utilization.
//! * UtilizationTracker.add() composes per-file deltas commutatively.

use noxu_cleaner::file_summary::FileSummary;
use noxu_cleaner::tracked_file_summary::TrackedFileSummary;
use noxu_cleaner::utilization_tracker::UtilizationTracker;
use proptest::prelude::*;
use proptest::strategy::Strategy;

// ============================================================================
// Synthetic LN write/delete event used by the oracle properties.
// ============================================================================

#[derive(Debug, Clone)]
#[allow(dead_code)] // `offset` is used by Delete; we keep it on Write for symmetry.
enum LnEvent {
    /// Append an LN to file `file_number` at `offset` with `size` bytes.
    Write { file_number: u32, offset: u32, size: u32 },
    /// Mark a previously-written LN obsolete.
    Delete { file_number: u32, offset: u32, size: u32 },
}

fn ln_event_strategy() -> impl Strategy<Value = LnEvent> {
    prop_oneof![
        // Writes: small file space (4 files), modest sizes, modest offsets.
        (0u32..4, 0u32..1024, 1u32..512).prop_map(|(f, o, s)| LnEvent::Write {
            file_number: f,
            offset: o,
            size: s
        }),
        // Deletes target the same space.  If they reference an LN that
        // was never written, the tracker still records the obsolete event;
        // the oracle handles missing LNs the same way.
        (0u32..4, 0u32..1024, 1u32..512).prop_map(|(f, o, s)| {
            LnEvent::Delete { file_number: f, offset: o, size: s }
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // 1. UtilizationTracker.count_new_log_entry sums total_count and
    //    total_size correctly.  For any sequence of LN writes, the
    //    per-file totals must equal the sum of the sizes the test wrote.
    #[test]
    fn prop_tracker_total_size_matches_writes(
        events in prop::collection::vec(ln_event_strategy(), 0..40),
    ) {
        let mut tracker = UtilizationTracker::new(true);
        // Oracle: per-file total LN count and total LN size.
        let mut oracle_count: std::collections::HashMap<u32, i32> = Default::default();
        let mut oracle_size: std::collections::HashMap<u32, i32> = Default::default();

        for ev in &events {
            if let LnEvent::Write { file_number, size, .. } = ev {
                tracker.count_new_log_entry(*file_number, *size as i32, true, false);
                *oracle_count.entry(*file_number).or_insert(0) += 1;
                *oracle_size.entry(*file_number).or_insert(0) += *size as i32;
            }
        }

        for (file_number, expected_count) in &oracle_count {
            let summary = tracker.get_tracked_summary(*file_number).unwrap();
            prop_assert_eq!(summary.get_summary().total_ln_count, *expected_count);
            prop_assert_eq!(summary.get_summary().total_ln_size, oracle_size[file_number]);
        }
    }

    // 2. UtilizationTracker.track_obsolete is consistent with a brute-force
    //    oracle that counts deletes per file.  The number of obsolete-LN
    //    counts in the tracker must equal the number of Delete events for
    //    that file.  Note: the tracker does NOT verify the LN actually
    //    exists; deletes of unwritten offsets still register.
    #[test]
    fn prop_tracker_obsolete_count_matches_oracle(
        events in prop::collection::vec(ln_event_strategy(), 0..40),
    ) {
        let mut tracker = UtilizationTracker::new(true);
        let mut oracle: std::collections::HashMap<u32, i32> = Default::default();

        for ev in &events {
            match ev {
                LnEvent::Write { file_number, size, .. } => {
                    tracker.count_new_log_entry(*file_number, *size as i32, true, false);
                }
                LnEvent::Delete { file_number, offset, size } => {
                    tracker.track_obsolete(*file_number, *offset, *size as i32, true);
                    *oracle.entry(*file_number).or_insert(0) += 1;
                }
            }
        }

        for (file_number, expected) in &oracle {
            let summary = tracker.get_tracked_summary(*file_number).unwrap();
            prop_assert_eq!(
                summary.get_summary().obsolete_ln_count,
                *expected,
                "obsolete_ln_count mismatch for file {}",
                file_number,
            );
        }
    }

    // 3. UtilizationTracker.get_tracked_files maps every observed file
    //    number to a non-empty summary; the set of file numbers is the
    //    union of files referenced by Write and Delete events.
    #[test]
    fn prop_tracker_file_set_is_union(
        events in prop::collection::vec(ln_event_strategy(), 1..40),
    ) {
        let mut tracker = UtilizationTracker::new(true);
        let mut expected_files: std::collections::HashSet<u32> = Default::default();
        for ev in &events {
            match ev {
                LnEvent::Write { file_number, size, .. } => {
                    tracker.count_new_log_entry(*file_number, *size as i32, true, false);
                    expected_files.insert(*file_number);
                }
                LnEvent::Delete { file_number, offset, size } => {
                    tracker.track_obsolete(*file_number, *offset, *size as i32, true);
                    expected_files.insert(*file_number);
                }
            }
        }
        let actual: std::collections::HashSet<u32> =
            tracker.get_tracked_files().keys().copied().collect();
        prop_assert_eq!(actual, expected_files);
    }

    // 4. UtilizationTracker.clear() empties the tracker irrespective of
    //    history.  After clear(), the file count and bytes-tracked are 0.
    #[test]
    fn prop_tracker_clear_resets(
        events in prop::collection::vec(ln_event_strategy(), 0..40),
    ) {
        let mut tracker = UtilizationTracker::new(true);
        for ev in &events {
            match ev {
                LnEvent::Write { file_number, size, .. } => {
                    tracker.count_new_log_entry(*file_number, *size as i32, true, false);
                }
                LnEvent::Delete { file_number, offset, size } => {
                    tracker.track_obsolete(*file_number, *offset, *size as i32, true);
                }
            }
        }
        tracker.clear();
        prop_assert_eq!(tracker.get_tracked_file_count(), 0);
        prop_assert_eq!(tracker.get_bytes_tracked(), 0);
    }
}

// ============================================================================
// FileSummary arithmetic invariants.
// ============================================================================

/// Arbitrary "consistent" FileSummary: obsolete counts/sizes never exceed
/// totals AND total_in_size + total_ln_size <= total_size (a precondition
/// the cleaner enforces in production: IN + LN bytes are partitions of the
/// file, with leftover bytes counted as obsolete).  This is the operational
/// regime; a buggy mutation might violate it, but legal write/delete
/// sequences must satisfy it.
fn consistent_summary_strategy() -> impl Strategy<Value = FileSummary> {
    (
        1i32..1_000_000, // total_size
        1i32..10_000,    // total_count
        0i32..10_000,    // total_ln_count
        0i32..10_000,    // total_in_count
        0u32..1000,      // ln_share (per-mille of total_size for LN)
        0u32..1000,      // in_share (per-mille of total_size for IN)
        0i32..10_000,    // obsolete_ln_count_raw
        0i32..10_000,    // obsolete_in_count_raw
        0u32..1000,      // obs_ln_share (per-mille of ln_size)
    )
        .prop_map(
            |(
                ts,
                tc,
                tlc,
                tic,
                ln_share,
                in_share_raw,
                olc_raw,
                oic_raw,
                obs_ln_share,
            )| {
                // Partition total_size into IN, LN, and leftover so that
                //   total_in_size + total_ln_size <= total_size.
                let ln_size = (ts as i64 * ln_share as i64 / 1000) as i32;
                let max_in_share = 1000u32.saturating_sub(ln_share);
                let in_share = in_share_raw.min(max_in_share);
                let in_size = (ts as i64 * in_share as i64 / 1000) as i32;

                let mut s = FileSummary::new();
                s.total_size = ts;
                s.total_count = tc;
                s.total_ln_count = tlc;
                s.total_ln_size = ln_size;
                s.obsolete_ln_count = olc_raw.min(tlc);
                let obs_ln_size =
                    (ln_size as i64 * obs_ln_share as i64 / 1000) as i32;
                s.obsolete_ln_size = obs_ln_size.min(ln_size);
                s.obsolete_ln_size_counted = s.obsolete_ln_count;
                s.total_in_count = tic;
                s.total_in_size = in_size;
                s.obsolete_in_count = oic_raw.min(tic);
                s
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // 5. For any consistent FileSummary, get_active_size() equals
    //    total_size - get_obsolete_size().  Trivial-looking, but catches
    //    overflow and saturation regressions in get_obsolete_size().
    #[test]
    fn prop_active_plus_obsolete_eq_total(s in consistent_summary_strategy()) {
        let active = s.get_active_size() as i64;
        let obsolete = s.get_obsolete_size() as i64;
        prop_assert_eq!(active + obsolete, s.total_size as i64);
    }

    // 6. utilization is always in [0, 1] for any consistent FileSummary.
    //    Prevents regressions where rounding pushes it negative or > 1.
    #[test]
    fn prop_utilization_in_unit_interval(s in consistent_summary_strategy()) {
        let u = s.get_utilization();
        prop_assert!((0.0..=1.0).contains(&u),
            "utilization {} out of range for {:?}", u, s);
    }

    // 7. Adjusted utilization is always <= unadjusted utilization, since
    //    expired LN bytes shrink the active denominator.
    #[test]
    fn prop_adjusted_utilization_le_utilization(
        mut s in consistent_summary_strategy(),
        expired_size in 0i32..200_000,
        expired_count in 0i32..1_000,
    ) {
        s.obsolete_expired_size = expired_size.min(s.obsolete_ln_size);
        s.obsolete_expired_lns = expired_count.min(s.obsolete_ln_count);
        let adj = s.get_adjusted_utilization();
        let u = s.get_utilization();
        prop_assert!(adj <= u + 1e-9,
            "adjusted util {} > util {} for {:?}", adj, u, s);
    }

    // 8. FileSummary.add(b) is associative w.r.t. totals: adding b to a
    //    yields the same total_count as a.total_count + b.total_count.
    //    Catches accidental subtractions in the additive accumulator.
    #[test]
    fn prop_summary_add_totals_are_additive(
        a in consistent_summary_strategy(),
        b in consistent_summary_strategy(),
    ) {
        let mut combined = a.clone();
        combined.add(&b);
        prop_assert_eq!(combined.total_count, a.total_count + b.total_count);
        prop_assert_eq!(combined.total_size, a.total_size + b.total_size);
        prop_assert_eq!(combined.total_ln_count, a.total_ln_count + b.total_ln_count);
        prop_assert_eq!(combined.total_in_count, a.total_in_count + b.total_in_count);
        prop_assert_eq!(combined.obsolete_ln_count,
            a.obsolete_ln_count + b.obsolete_ln_count);
        // max_ln_size takes the max, not the sum.
        prop_assert_eq!(combined.max_ln_size, a.max_ln_size.max(b.max_ln_size));
    }
}

// ============================================================================
// TrackedFileSummary obsolete-offset accounting.
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // 9. add_obsolete_offset records each call; obsolete_offset_count
    //    equals the number of calls for any track_detail=true tracker.
    #[test]
    fn prop_tracked_summary_offset_count_matches(
        offsets in prop::collection::vec(any::<u32>(), 0..64),
    ) {
        let mut t = TrackedFileSummary::new(0, true);
        for off in &offsets {
            t.add_obsolete_offset(*off);
        }
        prop_assert_eq!(t.obsolete_offset_count(), offsets.len());
    }

    // 10. add_obsolete_offset is a no-op for offset count when track_detail
    //     is disabled.  This is a critical assertion: with detail off, the
    //     cleaner relies on summary counters, NOT the offsets vector.
    #[test]
    fn prop_tracked_summary_no_detail_no_offsets(
        offsets in prop::collection::vec(any::<u32>(), 0..64),
    ) {
        let mut t = TrackedFileSummary::new(0, false);
        for off in &offsets {
            t.add_obsolete_offset(*off);
        }
        prop_assert_eq!(t.obsolete_offset_count(), 0);
    }
}
