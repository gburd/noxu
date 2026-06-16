//! CleanerTest — cleaner and file-selector tests ported to Rust.
//!
//! Covers: FileSelector lifecycle (add/select/mark/delete), file status
//! transitions, utilization scoring, force cleaning, two-pass cleaning,
//! FileSummary counting, CleanerThrottle EWMA, FileProtector, Cleaner
//! construction and stats.

use noxu_cleaner::{CleanerThrottle, FileSelector, FileStatus, FileSummary};

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
    let mut selected =
        [f1, f2, f3].iter().filter_map(|&x| x).collect::<Vec<_>>();
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
    assert!(
        adj <= raw,
        "adjusted utilization must be ≤ raw when expired LNs exist"
    );
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
    assert!(
        sleep <= 1000,
        "when cleaning needed, sleep must be ≤ BASE_SLEEP_MS (1000ms)"
    );
}

#[test]
fn throttle_n_files_at_least_one_when_cleaning_needed() {
    let t = CleanerThrottle::new(0);
    let (_sleep, n) = t.update(0, true);
    assert!(
        n >= 1,
        "should recommend at least 1 file to clean when cleaning needed"
    );
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

// ─── X-5: Cleaner checkpoint barrier ──────────────────────────────────────────

/// X-5: after cleaning, a file must NOT move to safe_to_delete until
/// process_checkpoint_end has been called TWICE.  Verifies the two-checkpoint
/// deletion barrier works end-to-end.
#[test]
fn x5_file_not_safe_to_delete_before_checkpoint() {
    use noxu_cleaner::FileSelector;

    let mut fs = FileSelector::new();

    // Simulate: file 1 is tracked and cleaned.
    fs.add_file_to_clean(1);
    let _ = fs.select_file_for_cleaning(); // transitions to BeingCleaned
    fs.mark_file_cleaned(1);

    // Before any checkpoint: safe_to_delete must be empty.
    assert!(
        fs.get_safe_to_delete().is_empty(),
        "file must not be safe_to_delete before any checkpoint"
    );

    // First checkpoint (no pending LNs/DBs): snapshot state at start, then call process_checkpoint_end.
    // JE optimization: if anyPendingDuringCheckpoint = false, CLEANED goes directly to
    // reserved (FullyProcessed) without needing a second checkpoint.
    let state1 = fs.get_checkpoint_state();
    assert_eq!(
        state1.cleaned_files,
        vec![1],
        "checkpoint state should capture cleaned file"
    );
    fs.process_checkpoint_end(&state1);

    // After first checkpoint with no pending items: file is immediately safe_to_delete.
    assert_eq!(
        fs.get_file_status(1),
        Some(noxu_cleaner::FileStatus::FullyProcessed)
    );
    let safe = fs.get_safe_to_delete();
    assert_eq!(
        safe,
        vec![1],
        "file must be safe_to_delete after one checkpoint when no pending items"
    );
}

/// X-5: files cleaned AFTER the first checkpoint start are NOT advanced to
/// safe_to_delete after the first checkpoint end — they wait for the next cycle.
#[test]
fn x5_file_cleaned_after_checkpoint_start_waits() {
    use noxu_cleaner::FileSelector;

    let mut fs = FileSelector::new();

    // First checkpoint: no cleaned files at start.
    let state1 = fs.get_checkpoint_state();
    assert!(state1.cleaned_files.is_empty());

    // File 2 is cleaned AFTER the checkpoint start snapshot.
    fs.add_file_to_clean(2);
    let _ = fs.select_file_for_cleaning(); // transitions to BeingCleaned
    fs.mark_file_cleaned(2);

    fs.process_checkpoint_end(&state1);

    // File 2 is NOT in the checkpoint state snapshot, so it stays in cleaned.
    assert_eq!(fs.get_file_status(2), Some(noxu_cleaner::FileStatus::Cleaned));
    assert!(fs.get_safe_to_delete().is_empty());

    // Second checkpoint captures file 2 (no pending items).
    // JE optimization: CLEANED → FullyProcessed directly when no pending.
    let state2 = fs.get_checkpoint_state();
    assert_eq!(state2.cleaned_files, vec![2]);
    fs.process_checkpoint_end(&state2);

    // File 2 is safe to delete after this checkpoint (no-pending fast path).
    assert_eq!(
        fs.get_file_status(2),
        Some(noxu_cleaner::FileStatus::FullyProcessed)
    );
    let safe = fs.get_safe_to_delete();
    assert_eq!(safe, vec![2]);
}

// ─── CLN-1: pending LN gates file deletion ───────────────────────────────────

/// CLN-1 regression: a cleaned file must NOT become safe-to-delete while a
/// pending LN (lock-denied during migration) remains unresolved.
///
/// Reproduction path (data-loss scenario on origin/main before this fix):
///
/// 1. The cleaner processes a file containing a live LN.
/// 2. The LN's BIN slot is locked by a concurrent writer — migration is denied.
/// 3. On pre-fix code: `lns_locked` was counted but the LN was NOT recorded
///    anywhere, so the file advanced toward deletion with the BIN slot still
///    pointing at it.  After a crash the slot would be dangling → data loss.
/// 4. On post-fix code: the LN is added to `FileSelector::pending_lns`; the
///    checkpoint barrier checks `any_pending_during_checkpoint` and keeps the
///    file in CHECKPOINTED state until `remove_pending_ln` drains the set and
///    `update_processed_files` promotes it.
///
/// This test verifies the gating behavior using the `FileSelector` API directly.
#[test]
fn cln1_pending_ln_gates_file_deletion() {
    use noxu_cleaner::{FileSelector, FileStatus, LnInfo};
    use noxu_util::Lsn;

    let mut fs = FileSelector::new();

    // ── Step 1: file 10 is cleaned.
    fs.add_file_to_clean(10);
    let _ = fs.select_file_for_cleaning();
    fs.mark_file_cleaned(10);

    // ── Step 2: during processing, one LN lock was denied.  Add to pending.
    // This is what FileProcessor now does when process_found_ln → Locked.
    let lock_denied_lsn = Lsn::new(10, 128);
    fs.add_pending_ln(
        lock_denied_lsn,
        LnInfo::new(lock_denied_lsn, 1, vec![0xAA, 0xBB], 64, false, 0),
    );

    // Verify: pending sets cause any_pending_during_checkpoint = true.
    assert!(!fs.all_pending_drained(), "pending LN must block file deletion");

    // ── Step 3: checkpoint starts and ends.  Because any_pending_during_checkpoint
    // is true, the CLEANED file must only advance to CHECKPOINTED, not FullyProcessed.
    let state = fs.get_checkpoint_state();
    assert!(
        fs.any_pending_during_checkpoint(),
        "any_pending_during_checkpoint must be true before checkpoint"
    );
    fs.process_checkpoint_end(&state);

    assert_eq!(
        fs.get_file_status(10),
        Some(FileStatus::Checkpointed),
        "file must stay CHECKPOINTED while pending LN exists (CLN-1 gate)"
    );
    assert!(
        fs.get_safe_to_delete().is_empty(),
        "file must NOT be safe_to_delete while pending LN is unresolved"
    );

    // ── Step 4: pending LN is successfully retried (lock released by writer).
    // remove_pending_ln calls update_processed_files which promotes CHECKPOINTED → FullyProcessed.
    fs.remove_pending_ln(lock_denied_lsn);

    assert!(
        fs.all_pending_drained(),
        "pending sets must be empty after removal"
    );
    assert_eq!(
        fs.get_file_status(10),
        Some(FileStatus::FullyProcessed),
        "file must be FullyProcessed once pending LN is resolved"
    );
    assert_eq!(
        fs.get_safe_to_delete(),
        vec![10],
        "file must be safe_to_delete after pending LN drains"
    );
}

/// CLN-1 regression (pre-fix behavior demonstration):
/// With no pending LNs, one checkpoint is sufficient (fast path).
/// This is the normal case — we verify the fast path still works.
#[test]
fn cln1_no_pending_lns_fast_path_one_checkpoint() {
    use noxu_cleaner::{FileSelector, FileStatus};

    let mut fs = FileSelector::new();

    fs.add_file_to_clean(5);
    let _ = fs.select_file_for_cleaning();
    fs.mark_file_cleaned(5);

    // No pending LNs: one checkpoint is enough (JE's anyPendingDuringCheckpoint = false path).
    let state = fs.get_checkpoint_state();
    assert!(!fs.any_pending_during_checkpoint());
    fs.process_checkpoint_end(&state);

    assert_eq!(fs.get_file_status(5), Some(FileStatus::FullyProcessed));
    assert_eq!(fs.get_safe_to_delete(), vec![5]);
}

/// CLN-1: a pending LN added AFTER checkpoint snapshot but BEFORE
/// checkpoint end still gates the cleaned file, because
/// `any_pending_during_checkpoint` is a running flag that accumulates
/// throughout the checkpoint window.
#[test]
fn cln1_pending_ln_added_mid_checkpoint_keeps_file_blocked() {
    use noxu_cleaner::{FileSelector, FileStatus, LnInfo};
    use noxu_util::Lsn;

    let mut fs = FileSelector::new();

    fs.add_file_to_clean(7);
    let _ = fs.select_file_for_cleaning();
    fs.mark_file_cleaned(7);

    // Snapshot checkpoint start — no pending yet.
    let state = fs.get_checkpoint_state();
    assert!(!fs.any_pending_during_checkpoint());

    // A pending LN is added AFTER the snapshot but BEFORE process_checkpoint_end.
    // (This is the real scenario: file processing discovers a locked LN during
    // the interval between checkpoint start and checkpoint end.)
    let lsn = Lsn::new(7, 200);
    fs.add_pending_ln(lsn, LnInfo::new(lsn, 2, vec![0x01], 32, false, 0));

    // process_checkpoint_end reads self.any_pending_during_checkpoint which is now
    // true (set by add_pending_ln) — the file stays CHECKPOINTED, not FullyProcessed.
    fs.process_checkpoint_end(&state);

    assert_eq!(
        fs.get_file_status(7),
        Some(FileStatus::Checkpointed),
        "pending LN added after snapshot but before checkpoint end must still gate file"
    );
    assert!(fs.get_safe_to_delete().is_empty());

    // Drain the pending LN — update_processed_files promotes CHECKPOINTED → FullyProcessed.
    fs.remove_pending_ln(lsn);
    assert_eq!(fs.get_file_status(7), Some(FileStatus::FullyProcessed));
    assert_eq!(fs.get_safe_to_delete(), vec![7]);
}

// ─── CLN-3: put_back_file_for_cleaning (stuck-state fix) ─────────────────────

/// CLN-3 regression: when processing a file errors or is interrupted,
/// the file must be returned to TO_BE_CLEANED (not stuck in BEING_CLEANED).
///
/// Pre-fix: `process_single_file` errors left the file in BEING_CLEANED
/// indefinitely; the cleaner would never retry it.
/// Post-fix: `put_back_file_for_cleaning` returns BEING_CLEANED → TO_BE_CLEANED.
///
/// JE: FileProcessor.java doClean() finally block (~line 591):
///   `if (!finished && !fileDeleted) { fileSelector.putBackFileForCleaning(fileNum); }`
#[test]
fn cln3_failed_processing_puts_file_back_for_retry() {
    use noxu_cleaner::{FileSelector, FileStatus};

    let mut fs = FileSelector::new();

    // File is queued and selected (now BEING_CLEANED).
    fs.add_file_to_clean(20);
    let selected = fs.select_file_for_cleaning();
    assert_eq!(selected, Some((20, None)));
    assert_eq!(fs.get_file_status(20), Some(FileStatus::BeingCleaned));

    // Simulate processing failure: call put_back_file_for_cleaning.
    fs.put_back_file_for_cleaning(20);

    // File must be back in TO_BE_CLEANED, not stuck in BEING_CLEANED.
    assert_eq!(
        fs.get_file_status(20),
        Some(FileStatus::ToBeCleaned),
        "failed processing must return file to ToBeCleaned for retry"
    );
    assert!(
        fs.has_files_to_clean(),
        "file must be in the to_be_cleaned queue after put_back"
    );

    // File can be re-selected on the next pass.
    let retry = fs.select_file_for_cleaning();
    assert_eq!(
        retry,
        Some((20, None)),
        "file must be re-selectable after put_back"
    );
}

/// CLN-3: put_back is a no-op if the file is not in BEING_CLEANED.
#[test]
fn cln3_put_back_noop_if_not_being_cleaned() {
    use noxu_cleaner::{FileSelector, FileStatus};

    let mut fs = FileSelector::new();
    fs.add_file_to_clean(21);
    // File is in TO_BE_CLEANED, not BEING_CLEANED.
    assert_eq!(fs.get_file_status(21), Some(FileStatus::ToBeCleaned));

    // Calling put_back on a file that is not BEING_CLEANED must not panic or corrupt state.
    fs.put_back_file_for_cleaning(21);
    // Status unchanged.
    assert_eq!(fs.get_file_status(21), Some(FileStatus::ToBeCleaned));
}

// ─── CLN-2: fully_processed_files in checkpoint snapshot ─────────────────────

/// CLN-2: CheckpointStartCleanerState now captures both cleaned_files and
/// fully_processed_files.  FULLY_PROCESSED files are already safe to delete
/// and remain so regardless of the checkpoint; they are not subject to the
/// pending-LN gate.
///
/// JE: getFilesAtCheckpointStart captures both CLEANED and FULLY_PROCESSED sets.
#[test]
fn cln2_checkpoint_state_captures_fully_processed_files() {
    use noxu_cleaner::{FileSelector, FileStatus};

    let mut fs = FileSelector::new();

    // File 30: advance to FullyProcessed.
    fs.add_file_to_clean(30);
    let _ = fs.select_file_for_cleaning();
    fs.mark_file_cleaned(30);
    let state = fs.get_checkpoint_state();
    fs.process_checkpoint_end(&state); // no pending → goes to FullyProcessed

    assert_eq!(fs.get_file_status(30), Some(FileStatus::FullyProcessed));

    // File 31: cleaned but not yet checkpointed.
    fs.add_file_to_clean(31);
    let _ = fs.select_file_for_cleaning();
    fs.mark_file_cleaned(31);

    // Snapshot: should capture file 30 in fully_processed_files and file 31 in cleaned_files.
    let snap = fs.get_checkpoint_state();
    assert!(
        snap.fully_processed_files.contains(&30),
        "fully_processed_files must include file 30"
    );
    assert!(
        snap.cleaned_files.contains(&31),
        "cleaned_files must include file 31"
    );
}

/// CLN-2: FULLY_PROCESSED files in the snapshot are immediately reserved
/// (already in safe_to_delete) regardless of pending LNs.
#[test]
fn cln2_fully_processed_files_always_safe_to_delete() {
    use noxu_cleaner::{FileSelector, FileStatus, LnInfo};
    use noxu_util::Lsn;

    let mut fs = FileSelector::new();

    // File 32: advance to FullyProcessed.
    fs.add_file_to_clean(32);
    let _ = fs.select_file_for_cleaning();
    fs.mark_file_cleaned(32);
    let s = fs.get_checkpoint_state();
    fs.process_checkpoint_end(&s);
    assert_eq!(fs.get_file_status(32), Some(FileStatus::FullyProcessed));

    // Add a pending LN for file 33.
    let lsn = Lsn::new(33, 100);
    fs.add_pending_ln(lsn, LnInfo::new(lsn, 1, vec![0x01], 32, false, 0));

    // File 32 is already in safe_to_delete.  The pending LN must not remove it.
    assert!(
        fs.get_safe_to_delete().contains(&32),
        "fully_processed file must remain safe_to_delete even with pending LNs"
    );
}

/// CLN-2: a file with no pending items is freed after ONE checkpoint
/// (the fast-path optimization).
#[test]
fn cln2_two_checkpoint_barrier_only_needed_when_pending() {
    use noxu_cleaner::FileSelector;

    let mut fs = FileSelector::new();

    fs.add_file_to_clean(40);
    let _ = fs.select_file_for_cleaning();
    fs.mark_file_cleaned(40);

    // One checkpoint, no pending items → immediately FullyProcessed.
    let s = fs.get_checkpoint_state();
    assert!(!fs.any_pending_during_checkpoint());
    fs.process_checkpoint_end(&s);

    assert_eq!(fs.get_safe_to_delete(), vec![40]);
}

// ─── CLN-4: first-active-txn file clamping ───────────────────────────────────

/// CLN-4: a long-running open transaction prevents the cleaner from selecting
/// a file within its active-log window.
///
/// Pre-fix: file selection ignored firstActiveTxnLsn, so files within the
/// oldest open transaction's log range could be selected for cleaning.
/// Post-fix: `select_file_for_cleaning_with_profile_and_txn` clamps
/// `effective_newest = min(newest_file, first_active_txn_file)` so files
/// at or above `first_active_txn_file` are excluded.
///
/// JE: `UtilizationCalculator.getBestFile` clamps firstActiveFile.
#[test]
fn cln4_long_running_txn_prevents_cleaning_within_active_window() {
    use noxu_cleaner::{FileSelector, FileSummary};
    use std::collections::BTreeMap;

    // Files 1..5 with low utilization (20% each), all qualify for cleaning.
    // File 5 is newest. With min_age=0, all are candidates.
    let profile: BTreeMap<u32, FileSummary> = (1u32..=5)
        .map(|n| {
            (
                n,
                FileSummary {
                    total_count: 10,
                    total_size: 1000,
                    total_ln_count: 10,
                    total_ln_size: 1000,
                    obsolete_ln_count: 8,
                    obsolete_ln_size: 800,
                    obsolete_ln_size_counted: 8,
                    ..Default::default()
                },
            )
        })
        .collect();

    let mut fs = FileSelector::new();

    // With no txn window clamping: file 1 (lowest util) is selected.
    let result =
        fs.select_file_for_cleaning_with_profile(&profile, 50, 0, false);
    assert_eq!(
        result.map(|(f, _)| f),
        Some(1),
        "without clamping, file 1 should be selected"
    );
    fs.clear(); // reset

    // With first_active_txn_file = 3: files 3, 4, 5 are protected by the txn.
    // effective_newest = min(5, 3) = 3; last_file_to_clean = 3 - 0 = 3.
    // Files 1, 2, 3 are candidates; file 1 wins (lowest util).
    let result2 = fs.select_file_for_cleaning_with_profile_and_txn(
        &profile,
        50,
        0,
        false,
        Some(3),
    );
    assert_eq!(
        result2.map(|(f, _)| f),
        Some(1),
        "file 1 should still be selected (below txn window)"
    );
    fs.clear();

    // With first_active_txn_file = 1: all files are in the txn window.
    // effective_newest = min(5, 1) = 1; last_file_to_clean = 1 - 0 = 1.
    // Only file 1 is at the boundary — but JE excludes files >= txn_file.
    // Since effective_newest = 1 and min_age = 0, last_file_to_clean = 1.
    // File 1 is still <= 1 so it qualifies.
    let result3 = fs.select_file_for_cleaning_with_profile_and_txn(
        &profile,
        50,
        0,
        false,
        Some(1),
    );
    assert_eq!(result3.map(|(f, _)| f), Some(1));
    fs.clear();

    // With first_active_txn_file = 1 and min_age = 1:
    // effective_newest = 1; last_file_to_clean = 0. No file qualifies.
    let result4 = fs.select_file_for_cleaning_with_profile_and_txn(
        &profile,
        50,
        1,
        false,
        Some(1),
    );
    assert_eq!(
        result4, None,
        "no file should be selected when all are in txn window with min_age=1"
    );
}

/// CLN-4: files within the txn window are excluded even if they have the
/// lowest utilization.
#[test]
fn cln4_txn_window_excludes_best_candidate() {
    use noxu_cleaner::{FileSelector, FileSummary};
    use std::collections::BTreeMap;

    // File 1: 10% util (best candidate), but inside txn window.
    // File 2: 50% util, outside txn window.
    // File 3: newest (95% util).
    let mut profile: BTreeMap<u32, FileSummary> = BTreeMap::new();
    profile.insert(
        1,
        FileSummary {
            total_count: 10,
            total_size: 1000,
            total_ln_count: 10,
            total_ln_size: 1000,
            obsolete_ln_count: 9,
            obsolete_ln_size: 900,
            obsolete_ln_size_counted: 9,
            ..Default::default()
        },
    );
    profile.insert(
        2,
        FileSummary {
            total_count: 10,
            total_size: 1000,
            total_ln_count: 10,
            total_ln_size: 1000,
            obsolete_ln_count: 5,
            obsolete_ln_size: 500,
            obsolete_ln_size_counted: 5,
            ..Default::default()
        },
    );
    profile.insert(
        3,
        FileSummary {
            total_count: 1,
            total_size: 1000,
            total_ln_count: 1,
            total_ln_size: 1000,
            ..Default::default()
        },
    );

    let mut fs = FileSelector::new();

    // first_active_txn_file = 2 means file 1 is below the txn window,
    // but effective_newest = min(3, 2) = 2, last_file_to_clean = 2 - 1 = 1.
    // Only file 1 qualifies by age. With min_age=1 and txn_file=2, file 1 is selected.
    let r1 = fs.select_file_for_cleaning_with_profile_and_txn(
        &profile,
        50,
        1,
        false,
        Some(2),
    );
    assert_eq!(r1.map(|(f, _)| f), Some(1));
    fs.clear();

    // first_active_txn_file = 1 and min_age=1: last_file_to_clean = 0. Nothing.
    let r2 = fs.select_file_for_cleaning_with_profile_and_txn(
        &profile,
        50,
        1,
        false,
        Some(1),
    );
    assert_eq!(r2, None, "all files excluded: txn window covers everything");
}
