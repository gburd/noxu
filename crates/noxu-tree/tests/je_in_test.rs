//! JE-equivalent IN findEntry / insertEntry tests.
//!
//! Wave 6 — Priority-4 JE TCK port.
//!
//! Ports invariants from `je/test/com/sleepycat/je/tree/INTest.java`,
//! adapted to Noxu's `noxu_tree::in_node::IN`.
//!
//! JE's INTest exercises:
//! * `findEntry` on an upper IN with a "virtual entry 0" (any search key
//!   matches slot 0 when `exact==false && indicate_if_duplicate==false`).
//! * `insertEntry` returning the new slot index combined with
//!   `INSERT_SUCCESS`.
//! * `EXACT_MATCH` flag returned when `indicate_if_duplicate==true`
//!   and the key matches.
//!
//! Mapping (JE -> Noxu):
//! * `IN.findEntry(key, indicateIfDup, exact)` -> `IN::find_entry(key, indicate_if_duplicate, exact)`
//! * `IN.insertEntry1(...)` -> `IN::insert_entry(key, lsn, state)`
//! * `IN.EXACT_MATCH`        -> `EXACT_MATCH`
//! * `IN.INSERT_SUCCESS`     -> `INSERT_SUCCESS`
//! * `IN.getNEntries()`      -> `IN::n_entries()`

use noxu_tree::in_node::{EXACT_MATCH, INSERT_SUCCESS, InNode};
use noxu_util::NULL_LSN;

const N_BYTES_IN_KEY: usize = 3;
// Upper IN level (matches JE's `new IN(..., 7)` in the test).
const TEST_LEVEL: i32 = 0x10000 | 7;
const TEST_CAPACITY: usize = 6;

fn make_upper_in() -> InNode {
    // JE's `new IN(db, identifierKey=byte[0], capacity, level=7)` starts with
    // n_entries=0.  The "virtual entry 0" is conceptual: when an upper IN's
    // find_entry is called with (exact=false, indicate_if_duplicate=false),
    // slot 0 is always treated as less than the search key.  Noxu's IN
    // matches this: an empty IN returns -1, after inserting the first real
    // key it occupies slot 0 and the virtual-0 trick fires for that same slot.
    InNode::new(/* database_id = */ 11, TEST_LEVEL, TEST_CAPACITY)
}

fn zero_bytes() -> Vec<u8> {
    vec![0x00; N_BYTES_IN_KEY]
}

fn max_bytes() -> Vec<u8> {
    // 0xFF bytes: tests unsigned byte comparison.
    vec![0xFF; N_BYTES_IN_KEY]
}

// --------------------------------------------------------------------------
// testFindEntry — direct port.
// --------------------------------------------------------------------------
#[test]
fn test_find_entry() {
    let mut in_node = make_upper_in();

    let zb = zero_bytes();
    let mb = max_bytes();

    // Initial state: no entries.  All findEntry variants return -1.
    assert_eq!(in_node.find_entry(&zb, false, false), -1);
    assert_eq!(in_node.find_entry(&mb, false, false), -1);
    assert_eq!(in_node.find_entry(&zb, false, true), -1);
    assert_eq!(in_node.find_entry(&mb, false, true), -1);
    assert_eq!(in_node.find_entry(&zb, true, false), -1);
    assert_eq!(in_node.find_entry(&mb, true, false), -1);
    assert_eq!(in_node.find_entry(&zb, true, true), -1);
    assert_eq!(in_node.find_entry(&mb, true, true), -1);

    // JE's loop: i = 0..initialINCapacity inserting key=[0x01, i, 0x10].
    for i in 0..TEST_CAPACITY {
        let mut key_bytes = vec![0u8; N_BYTES_IN_KEY];
        key_bytes[0] = 0x01;
        key_bytes[1] = i as u8;
        key_bytes[2] = 0x10;
        let mut next_key_bytes = key_bytes.clone();
        let mut prev_key_bytes = key_bytes.clone();
        next_key_bytes[2] = next_key_bytes[2].wrapping_add(1);
        prev_key_bytes[2] = prev_key_bytes[2].wrapping_sub(1);

        let flags = in_node.insert_entry(key_bytes.clone(), NULL_LSN, 0).unwrap();
        assert!(flags & INSERT_SUCCESS != 0, "INSERT_SUCCESS flag set");
        assert_eq!(
            flags & !INSERT_SUCCESS,
            i as i32,
            "inserted at slot {}",
            i
        );

        // After insert, the JE asserts (verbatim):
        //   findEntry(zeroBytes, false, false) == 0   // virtual slot 0
        //   findEntry(maxBytes,  false, false) == i   // greatest real slot
        assert_eq!(in_node.find_entry(&zb, false, false), 0);
        assert_eq!(in_node.find_entry(&mb, false, false), i as i32);

        // exact=true on a key that doesn't exist: -1.
        assert_eq!(in_node.find_entry(&zb, false, true), -1);
        assert_eq!(in_node.find_entry(&mb, false, true), -1);

        // indicate_if_duplicate=true disables the virtual-zero trick:
        //   zeroBytes < slot 0 = [0x01,...] -> insertion point is high=-1.
        //   maxBytes > all slots -> high = i.
        assert_eq!(in_node.find_entry(&zb, true, false), -1);
        assert_eq!(in_node.find_entry(&mb, true, false), i as i32);

        assert_eq!(in_node.find_entry(&zb, true, true), -1);
        assert_eq!(in_node.find_entry(&mb, true, true), -1);

        // For each real entry j > 0, JE asserts that findEntry on the
        // entry's own key yields j (with EXACT_MATCH set when dup is
        // requested).  In Noxu, slot 0 is also a real entry (no separate
        // virtual-key cell), so we sweep j from 1 upward to mirror JE
        // ("// 0th key is virtual").
        for j in 1..in_node.n_entries() {
            let kj = in_node.get_key(j).to_vec();
            assert_eq!(in_node.find_entry(&kj, false, false), j as i32);
            assert_eq!(in_node.find_entry(&kj, false, true), j as i32);
            assert_eq!(
                in_node.find_entry(&kj, true, false),
                (j as i32) | EXACT_MATCH
            );
            assert_eq!(
                in_node.find_entry(&kj, true, true),
                (j as i32) | EXACT_MATCH
            );
            // The "next" key (one byte greater than getKey(j)) and "prev"
            // key locate insertion points i (max real slot) and i-1.
            assert_eq!(in_node.find_entry(&next_key_bytes, false, false), i as i32);
            assert_eq!(in_node.find_entry(&prev_key_bytes, false, false), (i as i32) - 1);
            assert_eq!(in_node.find_entry(&next_key_bytes, false, true), -1);
            assert_eq!(in_node.find_entry(&prev_key_bytes, false, true), -1);
        }
    }
}

// --------------------------------------------------------------------------
// testInsertEntry — port the unsigned-byte insertion-order property.
//
// JE asserts that inserting random N-byte keys preserves sorted order in
// the slot array.  We test with a deterministic sequence of bytes plus
// the unsigned-comparison edge cases.
// --------------------------------------------------------------------------
#[test]
fn test_insert_entry_preserves_sorted_order() {
    let mut in_node = make_upper_in();

    // Insert keys in *reverse* sorted order — the IN's binary search
    // should still place them in ascending order via insertion-point.
    let keys: Vec<Vec<u8>> = vec![
        vec![0x80, 0x00, 0x00],
        vec![0x40, 0x00, 0x00],
        vec![0x20, 0x00, 0x00],
        vec![0x10, 0x00, 0x00],
        vec![0x08, 0x00, 0x00],
    ];
    for k in &keys {
        let flags = in_node.insert_entry(k.clone(), NULL_LSN, 0).unwrap();
        assert!(flags & INSERT_SUCCESS != 0);
    }

    // After all inserts, slots must be in ascending unsigned-byte order.
    let n = in_node.n_entries();
    for i in 0..n - 1 {
        let a = in_node.get_key(i).to_vec();
        let b = in_node.get_key(i + 1).to_vec();
        assert!(a < b, "slots must be sorted: a={:?} b={:?}", a, b);
    }
}

// --------------------------------------------------------------------------
// Inserting a duplicate key returns the existing slot index without
// INSERT_SUCCESS — JE's invariant for `insertEntry`.
// --------------------------------------------------------------------------
#[test]
fn test_insert_duplicate_returns_existing_slot_no_success_flag() {
    let mut in_node = make_upper_in();

    let k: Vec<u8> = vec![0x42, 0x42, 0x42];
    let first = in_node.insert_entry(k.clone(), NULL_LSN, 0).unwrap();
    assert!(first & INSERT_SUCCESS != 0, "first insert sets INSERT_SUCCESS");
    let first_slot = first & !INSERT_SUCCESS;

    let second = in_node.insert_entry(k.clone(), NULL_LSN, 0).unwrap();
    assert_eq!(
        second & INSERT_SUCCESS,
        0,
        "duplicate insert must not set INSERT_SUCCESS"
    );
    assert_eq!(second, first_slot);
}

// --------------------------------------------------------------------------
// findEntry must use UNSIGNED byte comparison (high-bit-set bytes are
// greater than 0x7F, not less).  JE's findEntry test specifically
// exercises this with 0xFF bytes.
// --------------------------------------------------------------------------
#[test]
fn test_find_entry_uses_unsigned_byte_comparison() {
    let mut in_node = make_upper_in();
    in_node.insert_entry(vec![0x7F, 0x7F, 0x7F], NULL_LSN, 0).unwrap();
    in_node.insert_entry(vec![0xFF, 0xFF, 0xFF], NULL_LSN, 0).unwrap();

    // Both real entries must be findable; 0xFF must locate slot > 0x7F.
    let high = in_node.find_entry(&vec![0xFF, 0xFF, 0xFF], true, true);
    assert!(high & EXACT_MATCH != 0, "0xFF entry must be findable exactly");
    let mid = in_node.find_entry(&vec![0x7F, 0x7F, 0x7F], true, true);
    assert!(mid & EXACT_MATCH != 0);
    assert!(
        (high & 0xffff) > (mid & 0xffff),
        "0xFF entry's slot must be greater than 0x7F entry's (unsigned byte order)"
    );
}
