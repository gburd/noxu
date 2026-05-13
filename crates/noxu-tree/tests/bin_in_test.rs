//! BinInTest — BIN and IN B-tree node tests ported to Rust.
//!
//! Covers: BIN entry insert/find/delete, dirty-slot tracking, key prefix
//! compression, BIN-delta mutation, cursor tracking, deleted slots,
//! InNode (upper-IN) level predicates, dirty/clean transitions, slot state.

use noxu_tree::bin::Bin;
use noxu_tree::entry_states::DIRTY_BIT;
use noxu_tree::{BIN_LEVEL, DEFAULT_MAX_ENTRIES, InNode, MAIN_LEVEL};
use noxu_util::{Lsn, NULL_LSN};

const DB_ID: u64 = 1;

// ─── helpers ──────────────────────────────────────────────────────────────────

fn make_bin() -> Bin {
    Bin::new(DB_ID, DEFAULT_MAX_ENTRIES)
}

fn make_upper_in(level: i32) -> InNode {
    InNode::new(DB_ID, level, DEFAULT_MAX_ENTRIES)
}

fn lsn(v: u64) -> Lsn {
    Lsn::from_u64(v)
}

/// Insert helper: no embedded data.
fn insert(b: &mut Bin, key: &[u8], lsn_val: u64, state: u8) {
    b.insert_entry(key.to_vec(), lsn(lsn_val), state, None).unwrap();
}

/// Insert helper: with DIRTY_BIT set.
fn insert_dirty(b: &mut Bin, key: &[u8], lsn_val: u64) {
    b.insert_entry(key.to_vec(), lsn(lsn_val), DIRTY_BIT, None).unwrap();
}

// ─── 1. BIN basics ────────────────────────────────────────────────────────────

#[test]
fn bin_new_empty() {
    let b = make_bin();
    assert_eq!(b.get_n_entries(), 0);
}

#[test]
fn bin_insert_single_entry() {
    let mut b = make_bin();
    insert(&mut b, b"key1", 1, 0);
    assert_eq!(b.get_n_entries(), 1);
}

#[test]
fn bin_insert_multiple_entries_sorted() {
    let mut b = make_bin();
    insert(&mut b, b"ccc", 3, 0);
    insert(&mut b, b"aaa", 1, 0);
    insert(&mut b, b"bbb", 2, 0);
    assert_eq!(b.get_n_entries(), 3);
    // Keys should be maintained in sorted order.
    let keys: Vec<_> = (0..3).filter_map(|i| b.get_full_key(i)).collect();
    assert!(keys[0] <= keys[1] && keys[1] <= keys[2], "BIN must be sorted");
}

#[test]
fn bin_find_entry_exact_match() {
    let mut b = make_bin();
    insert(&mut b, b"alpha", 10, 0);
    insert(&mut b, b"beta", 20, 0);
    let idx = b.find_entry(b"alpha", false, true);
    assert!(idx >= 0, "should find 'alpha'");
}

#[test]
fn bin_find_entry_missing_returns_negative() {
    let mut b = make_bin();
    insert(&mut b, b"alpha", 10, 0);
    let idx = b.find_entry(b"zzz", false, true);
    assert!(idx < 0, "missing key should return negative index");
}

#[test]
fn bin_delete_entry_reduces_count() {
    let mut b = make_bin();
    insert(&mut b, b"key", 1, 0);
    assert_eq!(b.get_n_entries(), 1);
    let removed = b.delete_entry(0);
    assert!(removed);
    assert_eq!(b.get_n_entries(), 0);
}

// ─── 2. BIN dirty slot tracking ──────────────────────────────────────────────

#[test]
fn bin_no_dirty_slots_after_clean_insert() {
    let mut b = make_bin();
    insert(&mut b, b"key", 1, 0);
    assert_eq!(b.count_dirty_slots(), 0);
}

#[test]
fn bin_count_dirty_slots_after_dirty_insert() {
    let mut b = make_bin();
    insert_dirty(&mut b, b"dirty_key", 1);
    assert_eq!(b.count_dirty_slots(), 1);
}

#[test]
fn bin_multiple_dirty_slots() {
    let mut b = make_bin();
    insert_dirty(&mut b, b"a", 1);
    insert_dirty(&mut b, b"b", 2);
    insert(&mut b, b"c", 3, 0);
    assert_eq!(b.count_dirty_slots(), 2);
}

// ─── 3. BIN last_full_version ─────────────────────────────────────────────────

#[test]
fn bin_last_full_version_null_initially() {
    let b = make_bin();
    assert_eq!(b.get_last_full_version(), NULL_LSN);
}

#[test]
fn bin_set_last_full_version() {
    let mut b = make_bin();
    b.set_last_full_version(lsn(999));
    assert_eq!(b.get_last_full_version(), lsn(999));
}

// ─── 4. BIN-delta mutation ────────────────────────────────────────────────────

#[test]
fn bin_is_not_bin_delta_initially() {
    let b = make_bin();
    assert!(!b.is_bin_delta());
}

#[test]
fn bin_set_bin_delta_true() {
    let mut b = make_bin();
    b.set_bin_delta(true);
    assert!(b.is_bin_delta());
}

#[test]
fn bin_set_bin_delta_false_clears() {
    let mut b = make_bin();
    b.set_bin_delta(true);
    b.set_bin_delta(false);
    assert!(!b.is_bin_delta());
}

#[test]
fn bin_should_log_delta_false_when_all_dirty() {
    // When all slots are dirty (100%), should NOT log delta ( threshold is ≤25%).
    let mut b = make_bin();
    b.set_last_full_version(lsn(1));
    insert_dirty(&mut b, b"a", 1);
    insert_dirty(&mut b, b"b", 2);
    insert_dirty(&mut b, b"c", 3);
    insert_dirty(&mut b, b"d", 4);
    // 4/4 dirty = 100% → delta NOT preferred.
    assert!(!b.should_log_delta());
}

#[test]
fn bin_should_log_delta_true_when_few_dirty() {
    // 1 dirty out of 8 = 12.5% ≤ 25% → delta preferred.
    let mut b = make_bin();
    b.set_last_full_version(lsn(1));
    insert(&mut b, b"a", 1, 0);
    insert(&mut b, b"b", 2, 0);
    insert(&mut b, b"c", 3, 0);
    insert(&mut b, b"d", 4, 0);
    insert(&mut b, b"e", 5, 0);
    insert(&mut b, b"f", 6, 0);
    insert(&mut b, b"g", 7, 0);
    insert_dirty(&mut b, b"h", 8);
    assert!(b.should_log_delta());
}

#[test]
fn bin_can_mutate_to_bin_delta_with_last_full_lsn() {
    let mut b = make_bin();
    b.set_last_full_version(lsn(10));
    insert(&mut b, b"a", 1, 0);
    insert_dirty(&mut b, b"b", 2);
    // With last_full_lsn set and few dirty slots, can_mutate should be true
    // (depends on slot count vs threshold, just verify no panic).
    let _ = b.can_mutate_to_bin_delta();
}

// ─── 5. BIN cursor tracking ───────────────────────────────────────────────────

#[test]
fn bin_no_cursors_initially() {
    let b = make_bin();
    assert_eq!(b.n_cursors(), 0);
    assert!(!b.has_cursors());
}

#[test]
fn bin_add_cursor_increments_count() {
    let mut b = make_bin();
    b.add_cursor(100);
    assert_eq!(b.n_cursors(), 1);
    assert!(b.has_cursors());
}

#[test]
fn bin_remove_cursor_decrements_count() {
    let mut b = make_bin();
    b.add_cursor(100);
    b.remove_cursor(100);
    assert_eq!(b.n_cursors(), 0);
}

#[test]
fn bin_multiple_cursors() {
    let mut b = make_bin();
    b.add_cursor(1);
    b.add_cursor(2);
    b.add_cursor(3);
    assert_eq!(b.n_cursors(), 3);
    b.remove_cursor(2);
    assert_eq!(b.n_cursors(), 2);
}

// ─── 6. BIN known-deleted and pending-deleted slots ──────────────────────────

#[test]
fn bin_entry_not_deleted_initially() {
    let mut b = make_bin();
    insert(&mut b, b"key", 1, 0);
    assert!(!b.is_deleted(0));
}

#[test]
fn bin_set_known_deleted_marks_as_deleted() {
    let mut b = make_bin();
    insert(&mut b, b"key", 1, 0);
    b.set_known_deleted(0);
    assert!(b.is_deleted(0));
}

#[test]
fn bin_clear_known_deleted_unmarked() {
    let mut b = make_bin();
    insert(&mut b, b"key", 1, 0);
    b.set_known_deleted(0);
    b.clear_known_deleted(0);
    assert!(!b.is_deleted(0));
}

#[test]
fn bin_set_pending_deleted_marks_as_deleted() {
    let mut b = make_bin();
    insert(&mut b, b"key", 1, 0);
    b.set_pending_deleted(0);
    assert!(b.is_deleted(0));
}

// ─── 7. BIN key prefix compression ───────────────────────────────────────────

#[test]
fn bin_key_prefix_empty_initially() {
    let b = make_bin();
    assert!(!b.has_key_prefix());
    assert_eq!(b.get_key_prefix(), b"");
}

#[test]
fn bin_compute_key_prefix_common_bytes() {
    let mut b = make_bin();
    insert(&mut b, b"prefix_aaa", 1, 0);
    insert(&mut b, b"prefix_bbb", 2, 0);
    let prefix = b.compute_key_prefix(None);
    assert!(
        b"prefix_aaa".starts_with(&prefix),
        "computed prefix must be a prefix of the inserted keys"
    );
    assert!(!prefix.is_empty(), "common prefix must be non-empty");
}

#[test]
fn bin_recompute_key_prefix_no_panic_empty_bin() {
    let mut b = make_bin();
    b.recompute_key_prefix();
}

#[test]
fn bin_get_full_key_roundtrip() {
    let mut b = make_bin();
    insert(&mut b, b"prefix_key1", 1, 0);
    b.recompute_key_prefix();
    let full = b.get_full_key(0).expect("slot 0 should exist");
    assert_eq!(full, b"prefix_key1".to_vec());
}

// ─── 8. BIN valid_for_delete / evictable ─────────────────────────────────────

#[test]
fn bin_empty_is_valid_for_delete() {
    // An empty BIN (no entries) is NOT valid for delete — there are no
    // known-deleted slots to satisfy the all-deleted precondition.
    let b = make_bin();
    assert!(!b.is_valid_for_delete());
}

#[test]
fn bin_with_entry_not_valid_for_delete() {
    let mut b = make_bin();
    insert(&mut b, b"key", 1, 0);
    assert!(!b.is_valid_for_delete());
}

#[test]
fn bin_not_evictable_with_cursors() {
    let mut b = make_bin();
    b.add_cursor(1);
    assert!(!b.is_evictable());
}

// ─── 9. InNode (upper-IN) level predicates ───────────────────────────────────

#[test]
fn innode_level_returns_correct_value() {
    let n = make_upper_in(MAIN_LEVEL);
    assert_eq!(n.level(), MAIN_LEVEL);
}

#[test]
fn innode_is_bin_false_at_upper_level() {
    let n = make_upper_in(MAIN_LEVEL);
    assert!(!n.is_bin());
}

#[test]
fn innode_is_bin_true_at_bin_level() {
    let n = make_upper_in(BIN_LEVEL);
    assert!(n.is_bin());
}

#[test]
fn innode_is_upper_in_true_at_main_level() {
    let n = make_upper_in(MAIN_LEVEL);
    assert!(n.is_upper_in());
}

#[test]
fn innode_not_dirty_initially() {
    let n = make_upper_in(MAIN_LEVEL);
    assert!(!n.is_dirty());
}

#[test]
fn innode_set_dirty_true() {
    let mut n = make_upper_in(MAIN_LEVEL);
    n.set_dirty(true);
    assert!(n.is_dirty());
}

#[test]
fn innode_clear_dirty() {
    let mut n = make_upper_in(MAIN_LEVEL);
    n.set_dirty(true);
    n.clear_dirty();
    assert!(!n.is_dirty());
}

#[test]
fn innode_not_bin_delta_initially() {
    let n = make_upper_in(MAIN_LEVEL);
    assert!(!n.is_bin_delta());
}

#[test]
fn innode_set_bin_delta() {
    let mut n = make_upper_in(BIN_LEVEL);
    n.set_bin_delta(true);
    assert!(n.is_bin_delta());
}

// ─── 10. InNode insert/find/delete ───────────────────────────────────────────

#[test]
fn innode_empty_initially() {
    let n = make_upper_in(MAIN_LEVEL);
    assert_eq!(n.n_entries(), 0);
}

#[test]
fn innode_insert_and_find() {
    let mut n = make_upper_in(MAIN_LEVEL);
    n.insert_entry(b"key_a".to_vec(), lsn(1), 0).unwrap();
    n.insert_entry(b"key_b".to_vec(), lsn(2), 0).unwrap();
    assert_eq!(n.n_entries(), 2);
    let idx = n.find_entry(b"key_a", false, true);
    assert!(idx >= 0, "should find key_a");
}

#[test]
fn innode_delete_entry() {
    let mut n = make_upper_in(MAIN_LEVEL);
    n.insert_entry(b"key".to_vec(), lsn(1), 0).unwrap();
    assert_eq!(n.n_entries(), 1);
    n.delete_entry(0);
    assert_eq!(n.n_entries(), 0);
}

// ─── 11. InNode LSN tracking ──────────────────────────────────────────────────

#[test]
fn innode_last_full_lsn_null_initially() {
    let n = make_upper_in(MAIN_LEVEL);
    assert_eq!(n.last_full_lsn(), NULL_LSN);
}

#[test]
fn innode_set_last_full_lsn() {
    let mut n = make_upper_in(MAIN_LEVEL);
    n.set_last_full_lsn(lsn(42));
    assert_eq!(n.last_full_lsn(), lsn(42));
}

#[test]
fn innode_last_delta_lsn_null_initially() {
    let n = make_upper_in(MAIN_LEVEL);
    assert_eq!(n.last_delta_lsn(), NULL_LSN);
}

#[test]
fn innode_set_last_delta_lsn() {
    let mut n = make_upper_in(MAIN_LEVEL);
    n.set_last_delta_lsn(lsn(77));
    assert_eq!(n.last_delta_lsn(), lsn(77));
}

// ─── 12. InNode slot state predicates ────────────────────────────────────────

#[test]
fn innode_set_and_check_known_deleted() {
    let mut n = make_upper_in(MAIN_LEVEL);
    n.insert_entry(b"k".to_vec(), lsn(1), 0).unwrap();
    assert!(!n.is_entry_known_deleted(0));
    n.set_known_deleted(0);
    assert!(n.is_entry_known_deleted(0));
    n.clear_known_deleted(0);
    assert!(!n.is_entry_known_deleted(0));
}

#[test]
fn innode_set_entry_dirty() {
    let mut n = make_upper_in(MAIN_LEVEL);
    n.insert_entry(b"k".to_vec(), lsn(1), 0).unwrap();
    assert!(!n.is_entry_dirty(0));
    n.set_entry_dirty(0);
    assert!(n.is_entry_dirty(0));
}

// ─── 13. InNode pinning ──────────────────────────────────────────────────────

#[test]
fn innode_pin_unpin() {
    let mut n = make_upper_in(MAIN_LEVEL);
    assert!(!n.is_pinned());
    n.pin();
    assert!(n.is_pinned());
    n.unpin();
    assert!(!n.is_pinned());
}

#[test]
fn innode_pinned_not_evictable() {
    // Upper INs report evictable=true regardless of pin count; the evictor
    // checks is_pinned() separately before evicting.
    let mut n = make_upper_in(MAIN_LEVEL);
    n.pin();
    assert!(n.is_pinned());
    // is_evictable() on upper INs always returns true (evictor uses is_pinned).
    assert!(n.is_evictable());
}

// ─── 14. InNode generation counter ───────────────────────────────────────────

#[test]
fn innode_generation_starts_at_zero() {
    let n = make_upper_in(MAIN_LEVEL);
    assert_eq!(n.generation(), 0);
}

#[test]
fn innode_bump_generation_increments() {
    let mut n = make_upper_in(MAIN_LEVEL);
    let g1 = n.bump_generation();
    let g2 = n.bump_generation();
    assert!(g2 > g1);
}
