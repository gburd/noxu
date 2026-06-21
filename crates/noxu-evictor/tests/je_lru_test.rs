//! JE-equivalent evictor LRU semantics tests.
//!
//! Wave 6 — Priority-4 JE TCK port.
//!
//! Ports invariants from `je/test/com/sleepycat/je/evictor/LRUTest.java`,
//! adapted to Noxu's live `LruPolicy` (the `EvictionPolicy` the evictor
//! actually runs, backed by `slab::SlabList`).  JE's LRUTest exercises
//! eviction at the Environment/Cursor level (with DB cache size + multiple
//! cursors using different `CacheMode`s); the underlying behaviour they
//! validate is the LRU/MRU semantics of the eviction list.  We port those
//! semantics directly against `LruPolicy`:
//!
//! * `testBaseline`               -> insertion order is preserved, the
//!   LRU end is the one popped first.
//! * `testCacheMode_KEEP_HOT`     -> `touch()` moves a node to the MRU
//!   end; subsequent pops return the un-touched (cold) nodes.
//! * `testCacheMode_UNCHANGED`    -> a node not touched stays where it
//!   was inserted; LRU pops are stable.
//! * `testCacheMode_EVICT_LN`     -> `remove(node)` immediately removes
//!   a node (regardless of position), reducing the list size.
//!
//! Method mapping (former `LruList` -> live `EvictionPolicy`):
//!   `insert`  -> `insert`, `pop_lru` -> `evict_candidate`
//!   (`remove_front`), `touch` -> `touch`, `remove` -> `remove`,
//!   `contains` -> `contains`, `len` -> `len`.

use noxu_evictor::policies::LruPolicy;
use noxu_evictor::policy::EvictionPolicy;

// --------------------------------------------------------------------------
// testBaseline-equivalent: insertion order = LRU order; evict_candidate
// drains in insertion order until empty.
// --------------------------------------------------------------------------
#[test]
fn test_baseline_insertion_then_pop_lru() {
    let lru = LruPolicy::new();

    // Insert 5 nodes 1..=5.  Each `insert` puts at MRU (back) so the
    // resulting LRU order (front->back) is 1, 2, 3, 4, 5.
    for n in 1u64..=5 {
        lru.insert(n);
    }
    assert_eq!(lru.len(), 5, "all 5 nodes must be tracked");

    // evict_candidate must return them in insertion order: 1, 2, 3, 4, 5.
    for expected in 1u64..=5 {
        let got = lru.evict_candidate();
        assert_eq!(got, Some(expected), "pop order: expected {}", expected);
    }
    assert_eq!(lru.evict_candidate(), None);
    assert_eq!(lru.len(), 0);
}

// --------------------------------------------------------------------------
// testCacheMode_KEEP_HOT-equivalent: `touch(n)` marks n as recently used
// (moved to MRU).  Subsequent pops return the un-touched nodes first.
// --------------------------------------------------------------------------
#[test]
fn test_keep_hot_via_touch() {
    let lru = LruPolicy::new();
    for n in 1u64..=5 {
        lru.insert(n);
    }

    // Touch nodes 1 and 3.  These are now at the MRU end.
    // LRU order becomes: 2, 4, 5, 1, 3.
    assert!(lru.touch(1), "touch on existing node must succeed");
    assert!(lru.touch(3));

    assert_eq!(lru.evict_candidate(), Some(2));
    assert_eq!(lru.evict_candidate(), Some(4));
    assert_eq!(lru.evict_candidate(), Some(5));
    assert_eq!(lru.evict_candidate(), Some(1));
    assert_eq!(lru.evict_candidate(), Some(3));
    assert_eq!(lru.evict_candidate(), None);
}

// --------------------------------------------------------------------------
// testCacheMode_UNCHANGED-equivalent: nodes that were never touched
// stay in insertion order.  A second insert of an already-present node
// is rejected (the index already maps it).
// --------------------------------------------------------------------------
#[test]
fn test_unchanged_nodes_stay_in_insertion_order() {
    let lru = LruPolicy::new();
    for n in 10u64..=14 {
        lru.insert(n);
    }
    // No touches.  Pops must drain in insertion order.
    for expected in 10u64..=14 {
        assert_eq!(lru.evict_candidate(), Some(expected));
    }
    assert_eq!(lru.len(), 0);
}

// --------------------------------------------------------------------------
// testCacheMode_EVICT_LN-equivalent: an immediate `remove(n)` is
// equivalent to evicting that node from the list, regardless of its
// position.  Subsequent pops do not return n.
// --------------------------------------------------------------------------
#[test]
fn test_evict_ln_via_remove() {
    let lru = LruPolicy::new();
    for n in 1u64..=5 {
        lru.insert(n);
    }

    // Remove the middle node — equivalent to EVICT_LN cursor mode for it.
    assert!(lru.remove(3), "remove must succeed for present node");
    assert_eq!(lru.len(), 4);
    assert!(!lru.contains(3), "removed node must not be in list");

    // Remaining LRU order: 1, 2, 4, 5.
    assert_eq!(lru.evict_candidate(), Some(1));
    assert_eq!(lru.evict_candidate(), Some(2));
    assert_eq!(lru.evict_candidate(), Some(4));
    assert_eq!(lru.evict_candidate(), Some(5));
    assert_eq!(lru.evict_candidate(), None);
}

// --------------------------------------------------------------------------
// touch() on an absent node must return false (no-op), matching JE's
// "no-op cache mode" semantics for nodes that aren't in the LRU.
// --------------------------------------------------------------------------
#[test]
fn test_touch_absent_node_is_no_op() {
    let lru = LruPolicy::new();
    lru.insert(1);
    lru.insert(2);
    assert!(!lru.touch(99), "touch on absent node must return false");
    // Order unchanged: pops still 1, 2.
    assert_eq!(lru.evict_candidate(), Some(1));
    assert_eq!(lru.evict_candidate(), Some(2));
}

// --------------------------------------------------------------------------
// remove() on absent node returns false; double-remove is a no-op.
// --------------------------------------------------------------------------
#[test]
fn test_remove_absent_or_already_removed() {
    let lru = LruPolicy::new();
    lru.insert(1);
    assert!(!lru.remove(99));
    assert!(lru.remove(1));
    assert!(!lru.remove(1), "double-remove must return false");
    assert_eq!(lru.len(), 0);
}

// --------------------------------------------------------------------------
// Repeated touch on the same node is idempotent at the tail (still MRU).
// --------------------------------------------------------------------------
#[test]
fn test_repeated_touch_is_idempotent() {
    let lru = LruPolicy::new();
    lru.insert(1);
    lru.insert(2);
    lru.insert(3);

    // Touch 1 twice.
    assert!(lru.touch(1));
    assert!(lru.touch(1));
    // After touches, LRU order: 2, 3, 1.
    assert_eq!(lru.evict_candidate(), Some(2));
    assert_eq!(lru.evict_candidate(), Some(3));
    assert_eq!(lru.evict_candidate(), Some(1));
}
