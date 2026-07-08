//! Property-based tests for noxu-recovery (Hegel / hegeltest).
//!
//! Covers replay-relevant invariants on the rollback-period tracker and the
//! analysis-result transaction-state machine:
//!
//! * For any random sequence of (matchpoint, start, end) triples representing
//!   RollbackStart/RollbackEnd events seen during analysis, the tracker's
//!   `is_in_rollback_period` agrees with a brute-force scan over the periods.
//! * `record_commit` / `record_abort` / `record_active_txn` produce a
//!   transaction state machine equivalent to "apply each event in order then
//!   inspect the final state".  In particular, a txn is only `is_active` if
//!   it never committed/aborted/prepared in the trace.
//! * `RollbackPeriod::contains` is a half-open interval (matchpoint_lsn,
//!   rollback_start_lsn).

use hegel::generators;
use noxu_recovery::analysis_result::AnalysisResult;
use noxu_recovery::rollback_tracker::{
    RollbackPeriod, RollbackScanner, RollbackTracker,
};
use noxu_util::{Lsn, NULL_LSN};

// ============================================================================
// Helper generators.
// ============================================================================

#[hegel::composite]
fn lsn_gen(tc: hegel::TestCase) -> Lsn {
    let f = tc.draw(generators::integers::<u32>().max_value(15));
    let o = tc.draw(generators::integers::<u32>().max_value(999_999));
    Lsn::new(f, o)
}

/// Generator producing well-formed (matchpoint < start < end) triples.
#[hegel::composite]
fn rollback_triple_gen(tc: hegel::TestCase) -> (Lsn, Lsn, Lsn) {
    let base = tc.draw(generators::integers::<u64>().max_value(999_999));
    let d1 = tc.draw(generators::integers::<u64>().min_value(1).max_value(999));
    let d2 = tc.draw(generators::integers::<u64>().min_value(1).max_value(999));
    let m = base;
    let s = base + d1;
    let e = s + d2;
    (Lsn::from_u64(m), Lsn::from_u64(s), Lsn::from_u64(e))
}

// ============================================================================
// 1. RollbackPeriod.contains is a strict half-open interval.
// ============================================================================

/// For any well-formed triple, contains(matchpoint) and contains(start)
/// must both be false; every LSN strictly between is true.  Catches off-
/// by-one regressions in the boundary checks.
#[hegel::test(test_cases = 256)]
fn prop_rollback_period_boundaries_excluded(tc: hegel::TestCase) {
    let (mp, start, end) = tc.draw(rollback_triple_gen());
    let p = RollbackPeriod::new(mp, start, end);
    assert!(!p.contains(mp), "matchpoint must not be contained");
    assert!(!p.contains(start), "start must not be contained");
}

/// LSNs strictly between matchpoint and start are contained.
#[hegel::test(test_cases = 256)]
fn prop_rollback_period_interior_contained(tc: hegel::TestCase) {
    let (mp, start, end) = tc.draw(rollback_triple_gen());
    let bias =
        tc.draw(generators::integers::<u64>().min_value(1).max_value(999));
    // Pick a sample LSN strictly between mp and start (assuming start > mp + 1).
    tc.assume(start.as_u64() > mp.as_u64() + 1);
    let mid_raw = mp.as_u64() + 1 + (bias % (start.as_u64() - mp.as_u64() - 1));
    let mid = Lsn::from_u64(mid_raw);
    let p = RollbackPeriod::new(mp, start, end);
    assert!(
        p.contains(mid),
        "interior LSN {:?} must be contained in period {:?}",
        mid,
        p
    );
}

// ============================================================================
// 2. RollbackTracker oracle: is_in_rollback_period agrees with brute-force.
// ============================================================================

/// For any sequence of well-formed RollbackStart/End event pairs, the
/// tracker's `is_in_rollback_period` agrees with a direct scan over the
/// completed periods.  The oracle ignores incomplete pairs (which the
/// tracker also excludes from query results until completed).
#[hegel::test(test_cases = 64)]
fn prop_rollback_tracker_matches_oracle(tc: hegel::TestCase) {
    let triples = tc.draw(generators::vecs(rollback_triple_gen()).max_size(15));
    let probe = tc.draw(lsn_gen());

    let mut tracker = RollbackTracker::new();
    for (mp, start, end) in &triples {
        tracker.register_rollback_start(*mp, *start);
        tracker.register_rollback_end(*mp, *end);
    }

    // Oracle: probe is in some completed period iff strict-interval
    // containment holds for any (mp, start, end) triple.
    let oracle = triples.iter().any(|(mp, start, _)| {
        probe.as_u64() > mp.as_u64() && probe.as_u64() < start.as_u64()
    });
    assert_eq!(
        tracker.is_in_rollback_period(probe),
        oracle,
        "tracker disagrees with brute-force oracle for probe={:?}, periods={:?}",
        probe,
        triples,
    );
}

/// After registering N completed pairs, period_count == N (assuming the
/// pairs use distinct matchpoints — duplicates would key-collide).
#[hegel::test(test_cases = 64)]
fn prop_rollback_tracker_period_count(tc: hegel::TestCase) {
    let bases = tc.draw(
        generators::vecs(generators::integers::<u64>().max_value(999_999))
            .max_size(15),
    );
    // Deduplicate base LSNs to ensure distinct matchpoints.
    let mut uniq: Vec<u64> = bases;
    uniq.sort();
    uniq.dedup();

    let mut tracker = RollbackTracker::new();
    for (i, base) in uniq.iter().enumerate() {
        let mp = Lsn::from_u64(*base);
        let start = Lsn::from_u64(*base + 100);
        let end = Lsn::from_u64(*base + 200 + i as u64);
        tracker.register_rollback_start(mp, start);
        tracker.register_rollback_end(mp, end);
    }
    assert_eq!(tracker.period_count(), uniq.len());
    assert_eq!(tracker.pending_count(), 0);
    assert!(!tracker.has_incomplete_rollbacks());
}

/// `RollbackTracker::get_rollback_periods` returns periods sorted by
/// matchpoint_lsn, regardless of insertion order.
#[hegel::test(test_cases = 64)]
fn prop_rollback_tracker_periods_sorted(tc: hegel::TestCase) {
    let bases = tc.draw(
        generators::vecs(generators::integers::<u64>().max_value(999_999))
            .max_size(11),
    );
    let mut uniq: Vec<u64> = bases;
    uniq.sort();
    uniq.dedup();
    tc.assume(uniq.len() >= 2);

    // Insert in REVERSED order (which differs from the natural sort order).
    let mut tracker = RollbackTracker::new();
    for (i, base) in uniq.iter().rev().enumerate() {
        let mp = Lsn::from_u64(*base);
        let start = Lsn::from_u64(*base + 100);
        let end = Lsn::from_u64(*base + 200 + i as u64);
        tracker.register_rollback_start(mp, start);
        tracker.register_rollback_end(mp, end);
    }

    let periods = tracker.get_rollback_periods();
    for w in periods.windows(2) {
        assert!(
            w[0].matchpoint_lsn < w[1].matchpoint_lsn,
            "periods not sorted: {:?} >= {:?}",
            w[0],
            w[1]
        );
    }
}

/// RollbackScanner.is_rolled_back agrees with the same oracle.  Scanner
/// is the post-analysis structure used during redo/undo passes.
#[hegel::test(test_cases = 64)]
fn prop_rollback_scanner_matches_oracle(tc: hegel::TestCase) {
    let triples = tc.draw(generators::vecs(rollback_triple_gen()).max_size(7));
    let probes = tc.draw(generators::vecs(lsn_gen()).max_size(7));

    let periods: Vec<RollbackPeriod> = triples
        .iter()
        .map(|(mp, start, end)| RollbackPeriod::new(*mp, *start, *end))
        .collect();
    let mut scanner = RollbackScanner::new(periods);

    for probe in &probes {
        let oracle = triples.iter().any(|(mp, start, _)| {
            probe.as_u64() > mp.as_u64() && probe.as_u64() < start.as_u64()
        });
        assert_eq!(scanner.is_rolled_back(*probe), oracle);
    }
}

// ============================================================================
// 3. AnalysisResult txn state machine: replay invariants.
// ============================================================================

#[derive(Debug, Clone)]
enum TxnEvent {
    /// Saw an LN belonging to txn_id without a commit/abort yet.
    SawActive(u64),
    /// Saw a commit record at lsn for txn_id.
    Commit(u64, Lsn),
    /// Saw an abort record for txn_id.
    Abort(u64),
}

#[hegel::composite]
fn txn_event_gen(tc: hegel::TestCase, max_txn_id: u64) -> TxnEvent {
    let kind = tc.draw(generators::integers::<u8>().max_value(2));
    let id = tc
        .draw(generators::integers::<u64>().min_value(1).max_value(max_txn_id));
    match kind {
        0 => TxnEvent::SawActive(id),
        1 => TxnEvent::Commit(id, tc.draw(lsn_gen())),
        _ => TxnEvent::Abort(id),
    }
}

/// Recovery invariant: after replaying any sequence of (active, commit,
/// abort) events for a fixed set of txn_ids, the partition
///   active = {txns that saw active and never committed/aborted}
///   committed = {txns whose last seen event was Commit}
///   aborted = {txns whose last seen event was Abort}
/// must hold.  This is the "applying-then-aborting-uncommitted"
/// equivalence the recovery design asserts.
///
/// Respects the documented precondition of `record_active_txn`
/// (the caller must not invoke it after commit/abort — see the ignored
/// test `prop_active_txn_after_terminal_resurrects` below).
#[hegel::test(test_cases = 128)]
fn prop_analysis_txn_state_partition(tc: hegel::TestCase) {
    let events = tc.draw(generators::vecs(txn_event_gen(8)).max_size(39));

    let mut analysis = AnalysisResult::new();

    // Oracle: per-txn last terminal event.
    // None = active (saw an active record but no commit/abort)
    // Some(true) = committed
    // Some(false) = aborted
    let mut oracle: std::collections::HashMap<u64, Option<bool>> =
        Default::default();

    for ev in &events {
        match ev {
            TxnEvent::SawActive(id) => {
                if matches!(oracle.get(id), Some(Some(_))) {
                    continue; // honor record_active_txn precondition
                }
                analysis.record_active_txn(*id);
                oracle.entry(*id).or_insert(None);
            }
            TxnEvent::Commit(id, lsn) => {
                analysis.record_commit(*id, *lsn);
                oracle.insert(*id, Some(true));
            }
            TxnEvent::Abort(id) => {
                analysis.record_abort(*id);
                oracle.insert(*id, Some(false));
            }
        }
    }

    for (id, state) in &oracle {
        match state {
            None => {
                assert!(
                    analysis.is_active(*id),
                    "txn {} should be active, oracle says active",
                    id
                );
                assert!(!analysis.is_committed(*id));
                assert!(!analysis.is_aborted(*id));
            }
            Some(true) => {
                assert!(
                    analysis.is_committed(*id),
                    "txn {} should be committed",
                    id
                );
                assert!(!analysis.is_active(*id));
            }
            Some(false) => {
                assert!(
                    analysis.is_aborted(*id),
                    "txn {} should be aborted",
                    id
                );
                assert!(!analysis.is_active(*id));
            }
        }
    }
}

/// `record_commit` removes the txn from `active_txn_ids`.  So
/// has_active_txns() is true iff at least one observed txn never saw
/// a commit/abort.  This property is what the "skip undo phase entirely
/// on clean shutdown" optimization relies on.
///
/// Note: respects the documented precondition of `record_active_txn`
/// ("txn neither committed nor aborted yet") by skipping SawActive
/// events that occur after a terminal event for the same txn.  Without
/// this filter the property finds a counterexample: a SawActive recorded
/// after a Commit re-introduces the txn into `active_txn_ids` (the
/// production analysis pass doesn't violate the precondition because it
/// processes events in chronological order).
#[hegel::test(test_cases = 128)]
fn prop_analysis_has_active_iff_oracle(tc: hegel::TestCase) {
    let events = tc.draw(generators::vecs(txn_event_gen(8)).max_size(29));

    let mut analysis = AnalysisResult::new();
    let mut oracle: std::collections::HashMap<u64, Option<bool>> =
        Default::default();

    for ev in &events {
        match ev {
            TxnEvent::SawActive(id) => {
                // Respect the precondition.
                if matches!(oracle.get(id), Some(Some(_))) {
                    continue;
                }
                analysis.record_active_txn(*id);
                oracle.entry(*id).or_insert(None);
            }
            TxnEvent::Commit(id, lsn) => {
                analysis.record_commit(*id, *lsn);
                oracle.insert(*id, Some(true));
            }
            TxnEvent::Abort(id) => {
                analysis.record_abort(*id);
                oracle.insert(*id, Some(false));
            }
        }
    }

    let oracle_has_active = oracle.values().any(|v| v.is_none());
    assert_eq!(analysis.has_active_txns(), oracle_has_active);
}

/// `max_txn_id` is monotone: it only ever grows, regardless of event
/// type.  This is necessary for ID-allocation reservations after recovery.
#[hegel::test(test_cases = 128)]
fn prop_analysis_max_txn_id_monotone(tc: hegel::TestCase) {
    let events = tc.draw(generators::vecs(txn_event_gen(1_000)).max_size(29));

    let mut analysis = AnalysisResult::new();
    let mut prev_max = 0u64;
    for ev in &events {
        match ev {
            TxnEvent::SawActive(id) => analysis.record_active_txn(*id),
            TxnEvent::Commit(id, lsn) => analysis.record_commit(*id, *lsn),
            TxnEvent::Abort(id) => analysis.record_abort(*id),
        }
        assert!(
            analysis.max_txn_id >= prev_max,
            "max_txn_id moved backwards: {} -> {}",
            prev_max,
            analysis.max_txn_id
        );
        prev_max = analysis.max_txn_id;
    }
}

#[allow(dead_code)]
fn _force_use_imports() {
    let _ = NULL_LSN;
}

// ============================================================================
// Bug observation surfaced by Wave 11-E (committed `#[ignore]` per the
// wave's discipline; bug fixes are routed to a separate wave).
// ============================================================================

/// `record_active_txn` re-introduces a txn into `active_txn_ids` even after
/// `record_commit` or `record_abort` has been called for the same id.  The
/// docstring on `record_active_txn` says the caller must only invoke it for
/// txns "neither committed nor aborted yet" — in production the analysis
/// pass enforces this implicitly via chronological order.  But there is no
/// defensive check inside the method, so a buggy or out-of-order caller can
/// produce a state where:
///
///   * `is_committed(txn)` returns `true`
///   * `is_active(txn)` returns `false` (because `is_active` defers to
///     `!is_committed && !is_aborted && !is_prepared`)
///   * BUT `active_txn_ids.contains(&txn)` is true, so `has_active_txns()`
///     reports a phantom active txn that the undo phase will then attempt
///     to undo (or refuse to skip the entire undo phase on what is
///     otherwise a clean shutdown).
///
/// Counterexample: events = [Commit(1, lsn), SawActive(1)].  Oracle says
/// has_active_txns should be false (the only txn committed); the impl says
/// it's true.
///
/// TODO: decide whether `record_active_txn` should be
/// hardened with a defensive `if is_committed || is_aborted { return; }`,
/// or whether the precondition should be promoted to a `debug_assert!` and
/// callers audited.  Tracked under the post-v2.3.0 roadmap.
#[test]
fn prop_active_txn_after_terminal_resurrects_phantom_active() {
    let mut a = AnalysisResult::new();
    a.record_commit(1, Lsn::from_u64(0));
    a.record_active_txn(1);
    // Bug: txn 1 has committed, but has_active_txns reports true.
    assert!(a.is_committed(1));
    assert!(!a.is_active(1));
    // The next assertion is what surfaces the gap: oracle says
    // has_active_txns should be false, but the impl returns true.
    assert!(
        !a.has_active_txns(),
        "phantom active txn after Commit then record_active_txn"
    );
}
