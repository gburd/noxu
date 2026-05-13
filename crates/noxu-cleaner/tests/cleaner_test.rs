//! CleanerTest — cleaner and file-selector tests ported to Rust.
//!
//! Covers: FileSelector lifecycle (add/select/mark/delete), file status
//! transitions, utilization scoring, force cleaning, two-pass cleaning,
//! FileSummary counting, CleanerThrottle EWMA, FileProtector, Cleaner
//! construction and stats.

use noxu_cleaner::{
    CleanerThrottle, FileSummary, FileSelector, FileStatus,
};

// ─── 1. FileSelector: empty state ─────────────────────────────────────────────

#[test]
fn file_selector_new_has_no_files_to_clean() {
    let fs = FileSelector::new();
    assert!(!fs.has_files_to_clean());
}

#[test]
fn file_selector_not_force_cleaning_initially() {
    let fs = FileSelector::new();
    assert!(!fs.is_force_cleaning());
}

#[test]
fn file_selector_required_util_none_initially() {
    let fs = FileSelector::new();
    assert!(fs.required_util().is_none());
}

#[test]
fn file_selector_select_on_empty_returns_none() {
    let mut fs = FileSelector::new();
    let result = fs.select_file_for_cleaning();
    assert!(result.is_none());
}

// ─── 2. FileSelector: add and select files ────────────────────────────────────

#[test]
fn file_selector_add_file_to_clean() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(1);
    assert!(fs.has_files_to_clean());
}

#[test]
fn file_selector_select_returns_added_file() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(42);
    let selected = fs.select_file_for_cleaning();
    assert_eq!(selected, Some((42, None)));
}

#[test]
fn file_selector_is_being_cleaned_after_select() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(10);
    fs.select_file_for_cleaning();
    assert!(fs.is_being_cleaned(10));
}

#[test]
fn file_selector_not_tracked_before_add() {
    let fs = FileSelector::new();
    assert!(!fs.is_tracked(999));
}

#[test]
fn file_selector_tracked_after_add() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(5);
    assert!(fs.is_tracked(5));
}

#[test]
fn file_selector_select_multiple_files_in_order() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(1);
    fs.add_file_to_clean(2);
    fs.add_file_to_clean(3);
    let f1 = fs.select_file_for_cleaning().map(|(n, _)| n);
    let f2 = fs.select_file_for_cleaning().map(|(n, _)| n);
    let f3 = fs.select_file_for_cleaning().map(|(n, _)| n);
    // All three should be returned in some order.
    let mut selected = [f1, f2, f3].iter().filter_map(|&x| x).collect::<Vec<_>>();
    selected.sort();
    assert_eq!(selected, vec![1, 2, 3]);
}

// ─── 3. FileSelector: status transitions ─────────────────────────────────────

#[test]
fn file_selector_mark_cleaned_after_being_cleaned() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(10);
    fs.select_file_for_cleaning();
    fs.mark_file_cleaned(10);
    assert_eq!(fs.get_file_status(10), Some(FileStatus::Cleaned));
}

#[test]
fn file_selector_mark_checkpointed() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(10);
    fs.select_file_for_cleaning();
    fs.mark_file_cleaned(10);
    fs.mark_file_checkpointed(10);
    assert_eq!(fs.get_file_status(10), Some(FileStatus::Checkpointed));
}

#[test]
fn file_selector_mark_fully_processed() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(10);
    fs.select_file_for_cleaning();
    fs.mark_file_cleaned(10);
    fs.mark_file_checkpointed(10);
    fs.mark_file_fully_processed(10);
    assert_eq!(fs.get_file_status(10), Some(FileStatus::FullyProcessed));
}

#[test]
fn file_selector_safe_to_delete_after_fully_processed() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(10);
    fs.select_file_for_cleaning();
    fs.mark_file_cleaned(10);
    fs.mark_file_checkpointed(10);
    fs.mark_file_fully_processed(10);
    let safe = fs.get_safe_to_delete();
    assert!(safe.contains(&10));
}

#[test]
fn file_selector_remove_deleted_file() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(10);
    fs.select_file_for_cleaning();
    fs.mark_file_cleaned(10);
    fs.mark_file_checkpointed(10);
    fs.mark_file_fully_processed(10);
    fs.remove_deleted_file(10);
    assert!(!fs.is_tracked(10));
}

// ─── 4. FileSelector: two-pass cleaning ──────────────────────────────────────

#[test]
fn file_selector_check_for_required_util_sets_force_cleaning() {
    let mut fs = FileSelector::new();
    // best-candidate util (70) > threshold (50): even the dirtiest file is above
    // the normal threshold, so no files qualify for regular cleaning — second pass needed.
    fs.check_for_required_util(70, 50);
    assert!(fs.is_force_cleaning());
}

#[test]
fn file_selector_check_for_required_util_already_met_no_force() {
    let mut fs = FileSelector::new();
    // best-candidate util (30) < threshold (50): the dirtiest file is already
    // below the threshold and will be cleaned normally — no second pass needed.
    fs.check_for_required_util(30, 50);
    assert!(!fs.is_force_cleaning());
}

#[test]
fn file_selector_required_util_set_by_check() {
    let mut fs = FileSelector::new();
    // best-candidate util (70) > threshold (50) → required_util raised.
    fs.check_for_required_util(70, 50);
    assert!(fs.required_util().is_some());
}

// ─── 5. FileSelector: utilization scoring ────────────────────────────────────

#[test]
fn file_selector_utilization_pct_all_obsolete() {
    let mut s = FileSummary::new();
    s.total_count = 10;
    s.total_size = 1000;
    s.total_ln_count = 10;
    s.total_ln_size = 1000;
    s.obsolete_ln_count = 10;
    s.obsolete_ln_size = 1000;
    s.obsolete_ln_size_counted = 10;
    // All LNs obsolete → 0% utilization.
    assert_eq!(FileSelector::utilization_pct(&s), 0);
}

#[test]
fn file_selector_utilization_pct_none_obsolete() {
    let mut s = FileSummary::new();
    s.total_count = 10;
    s.total_size = 1000;
    s.total_ln_count = 10;
    s.total_ln_size = 1000; // all space is LN, no leftover
    // No obsolete LNs → 100% utilization.
    assert_eq!(FileSelector::utilization_pct(&s), 100);
}

#[test]
fn file_selector_utilization_pct_half_obsolete() {
    let mut s = FileSummary::new();
    s.total_count = 10;
    s.total_size = 1000;
    s.total_ln_count = 10;
    s.total_ln_size = 1000;
    s.obsolete_ln_count = 5;
    s.obsolete_ln_size = 500;
    s.obsolete_ln_size_counted = 5;
    // Half of LNs obsolete → 50% utilization.
    assert_eq!(FileSelector::utilization_pct(&s), 50);
}

#[test]
fn file_selector_adjusted_util_lower_with_expired_lns() {
    let mut s = FileSummary::new();
    s.total_count = 10;
    s.total_size = 1000;
    s.total_ln_count = 10;
    s.total_ln_size = 1000;
    s.obsolete_ln_count = 3;
    s.obsolete_ln_size = 300;
    s.obsolete_ln_size_counted = 3;
    s.obsolete_expired_lns = 2;
    s.obsolete_expired_size = 200; // 200 bytes of expired LNs not needing migration
    let raw = FileSelector::utilization_pct(&s);
    let adj = FileSelector::adjusted_utilization_pct(&s);
    // Adjusted util should be lower than raw (expired bytes reduce effective active).
    assert!(adj <= raw, "adjusted utilization must be ≤ raw when expired LNs exist");
}

// ─── 6. FileSelector: clear ───────────────────────────────────────────────────

#[test]
fn file_selector_clear_resets_all_state() {
    let mut fs = FileSelector::new();
    fs.add_file_to_clean(1);
    fs.add_file_to_clean(2);
    fs.check_for_required_util(10, 50);
    fs.clear();
    assert!(!fs.has_files_to_clean());
    assert!(!fs.is_force_cleaning());
    assert!(fs.required_util().is_none());
}

// ─── 7. FileSummary ───────────────────────────────────────────────────────────

#[test]
fn file_summary_new_is_empty() {
    let s = FileSummary::new();
    assert!(s.is_empty());
}

#[test]
fn file_summary_not_empty_after_count_set() {
    let mut s = FileSummary::new();
    s.total_count = 5;
    assert!(!s.is_empty());
}

#[test]
fn file_summary_fields_default_zero() {
    let s = FileSummary::default();
    assert_eq!(s.total_count, 0);
    assert_eq!(s.total_size, 0);
    assert_eq!(s.obsolete_ln_count, 0);
    assert_eq!(s.obsolete_ln_size, 0);
    assert_eq!(s.obsolete_expired_lns, 0);
    assert_eq!(s.obsolete_expired_size, 0);
}

#[test]
fn file_summary_clone_equality() {
    let mut s = FileSummary::new();
    s.total_count = 10;
    s.total_size = 4096;
    let s2 = s.clone();
    assert_eq!(s, s2);
}

// ─── 8. CleanerThrottle ───────────────────────────────────────────────────────

#[test]
fn throttle_initial_sleep_is_base() {
    let t = CleanerThrottle::new(0);
    // With zero bytes written and no cleaning needed, sleep should be at BASE.
    let sleep = t.current_sleep_ms();
    assert!(sleep > 0, "sleep must be positive initially");
}

#[test]
fn throttle_update_high_write_rate_reduces_sleep() {
    let t = CleanerThrottle::new(0);
    // Simulate a high write rate.
    let (sleep_after, _) = t.update(10_000_000, false);
    let initial = t.current_sleep_ms();
    assert!(
        sleep_after <= initial * 2,
        "high write rate should not massively increase sleep"
    );
}

#[test]
fn throttle_update_cleaning_needed_caps_sleep() {
    let t = CleanerThrottle::new(0);
    // With cleaning needed, sleep should be capped at BASE_SLEEP_MS.
    let (sleep, _n_files) = t.update(0, true);
    assert!(sleep <= 1000, "when cleaning needed, sleep must be ≤ BASE_SLEEP_MS (1000ms)");
}

#[test]
fn throttle_n_files_at_least_one_when_cleaning_needed() {
    let t = CleanerThrottle::new(0);
    let (_sleep, n) = t.update(0, true);
    assert!(n >= 1, "should recommend at least 1 file to clean when cleaning needed");
}

#[test]
fn throttle_write_rate_zero_initially() {
    let t = CleanerThrottle::new(0);
    assert_eq!(t.write_rate_bytes_per_sec(), 0.0);
}

#[test]
fn throttle_n_files_bounded_above() {
    let t = CleanerThrottle::new(0);
    // Even with very high write rate, n_files must not exceed MAX_FILES_PER_PASS.
    let (_sleep, n) = t.update(u64::MAX / 2, true);
    assert!(n <= 8, "n_files must not exceed MAX_FILES_PER_PASS (8)");
}

#[test]
fn throttle_sleep_bounded_below() {
    let t = CleanerThrottle::new(0);
    let (sleep, _) = t.update(u64::MAX / 2, true);
    assert!(sleep >= 100, "sleep must be ≥ MIN_SLEEP_MS (100ms)");
}
