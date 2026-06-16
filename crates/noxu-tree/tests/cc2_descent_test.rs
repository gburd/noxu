//! CC-2 regression test: `first_entry_at_or_after_with_index` coupled descent.
//!
//! Before the fix the method did:
//!   ```text
//!   let is_bin = arc.read().is_bin();   // lock acquired, then released
//!   if is_bin {
//!       let g = arc.read();             // second, independent acquisition
//!   ```
//! A concurrent split between those two `read()` calls can:
//! * promote the node from BIN → upper IN (split root), or
//! * move the sought key to a new sibling BIN.
//!
//! The fix uses `read_arc()` hand-over-hand (same as `search`,
//! `first_entry_at_or_after`, `get_first_node`, `get_adjacent_bin_attempt`)
//! so the guard is held continuously from the `is_bin()` test through the
//! entry lookup with no unlocked gap.
//!
//! JE reference: `Tree.searchSubTree` / `Tree.search` in
//! `com/sleepycat/je/tree/Tree.java` — the latch is held across the
//! `isBIN()` check and the subsequent slot search.
//!
//! Deterministic race: because the race window is one memory barrier wide,
//! we cannot reliably reproduce it without injecting a delay.  Instead the
//! tests verify:
//!   1. Boundary correctness — keys at split boundaries are found correctly.
//!   2. Stress — `first_entry_at_or_after_with_index` is correct under
//!      concurrent inserts that force splits.
//!   3. Structural argument — the method now matches the coupled-descent
//!      pattern of its siblings.

use noxu_tree::Tree;
use noxu_util::Lsn;
use std::sync::Arc;

fn lsn(v: u64) -> Lsn {
    Lsn::from_u64(v)
}

// ---------------------------------------------------------------------------
// Boundary correctness test
// ---------------------------------------------------------------------------

/// Insert enough keys to force at least one BIN split, then verify that
/// `first_entry_at_or_after_with_index` finds keys at the split boundary.
///
/// This is the case the check-then-lock gap mishandles: after a split the
/// old BIN becomes an upper-IN parent whose first child BIN holds the keys
/// below the split point.  A second `arc.read()` after the lock gap could
/// have observed a now-promoted node and returned None for an existing key.
#[test]
fn test_split_boundary_key_found() {
    // Use a small max_entries so splits happen early.
    let tree = Arc::new(Tree::new(1, 4));

    // Insert 20 entries to force several splits.
    for i in 0u64..20 {
        let key = format!("key{:03}", i);
        tree.insert(key.into_bytes(), vec![i as u8], lsn(i + 1)).unwrap();
    }

    // Every inserted key must be found by first_entry_at_or_after_with_index.
    for i in 0u64..20 {
        let key = format!("key{:03}", i).into_bytes();
        let result = tree.first_entry_at_or_after_with_index(&key);
        assert!(
            result.is_some(),
            "key{:03} not found — check-then-lock gap would cause this",
            i
        );
        let (found_key, _data, _idx, _lsn, _arc) = result.unwrap();
        assert_eq!(found_key, key, "wrong key returned for key{:03}", i);
    }
}

/// A key that lands between two BINs (i.e. the first key of a newly-created
/// sibling after a split) must be found.
#[test]
fn test_key_at_exact_split_point_found() {
    let tree = Arc::new(Tree::new(1, 4));

    // Insert keys that will cause a mid-point split.
    let keys: Vec<Vec<u8>> = (0u8..16).map(|i| vec![i]).collect();
    for (i, key) in keys.iter().enumerate() {
        tree.insert(key.clone(), vec![i as u8], lsn(i as u64 + 1)).unwrap();
    }

    // All keys must be reachable.
    for key in &keys {
        let r = tree.first_entry_at_or_after_with_index(key);
        assert!(
            r.is_some(),
            "key {:?} not found after split",
            key
        );
        let (found, ..) = r.unwrap();
        assert_eq!(&found, key);
    }
}

/// The returned `idx` must be the correct slot within the BIN for the key,
/// not 0 (the pre-fix behaviour of `search_dup` that CC-2 also fixed).
#[test]
fn test_returned_index_matches_slot() {
    let tree = Arc::new(Tree::new(1, 16));

    let keys: Vec<Vec<u8>> = (0u8..10).map(|i| vec![i * 10]).collect();
    for (i, key) in keys.iter().enumerate() {
        tree.insert(key.clone(), vec![i as u8], lsn(i as u64 + 1)).unwrap();
    }

    for key in &keys {
        let r = tree.first_entry_at_or_after_with_index(key);
        assert!(r.is_some(), "key {:?} not found", key);
        let (found_key, found_data, idx, _lsn, arc) = r.unwrap();
        assert_eq!(&found_key, key);

        // Cross-check: the entry at `idx` in the BIN agrees with what was returned.
        let guard = arc.read();
        if let noxu_tree::TreeNode::Bottom(bin) = &*guard {
            let entry_key = bin.get_full_key(idx).expect("entry at idx");
            assert_eq!(entry_key, found_key, "idx mismatch: got {:?} at slot {}", entry_key, idx);
            assert_eq!(bin.entries[idx].data.as_deref().unwrap_or(&[]), found_data.as_slice());
        } else {
            panic!("expected BIN node from returned Arc");
        }
    }
}

// ---------------------------------------------------------------------------
// Stress test: concurrent inserts that force splits
// ---------------------------------------------------------------------------

/// Spawn writer threads that continuously insert new keys, while a reader
/// thread concurrently calls `first_entry_at_or_after_with_index`.
///
/// The check-then-lock gap would cause occasional None returns for existing
/// keys or panics; the coupled-descent fix must be race-free.
///
/// Note: because the race window is sub-instruction-wide this test may not
/// reproduce the pre-fix bug deterministically.  It documents the structural
/// invariant and serves as a canary under a concurrent load that triggers
/// many splits.
#[test]
fn test_stress_concurrent_splits() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let tree = Arc::new(Tree::new(1, 4)); // small node size → many splits
    let done = Arc::new(AtomicBool::new(false));

    // Pre-populate so reader has something to find.
    for i in 0u64..8 {
        let key = format!("s{:04}", i).into_bytes();
        tree.insert(key, vec![i as u8], lsn(i + 1)).unwrap();
    }

    // Writer threads.
    let writers: Vec<_> = (0u64..2)
        .map(|t| {
            let tree = tree.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let mut i = 100u64 + t * 10000;
                while !done.load(Ordering::Relaxed) {
                    let key = format!("s{:04}", i).into_bytes();
                    let _ = tree.insert(key, vec![0u8], lsn(i));
                    i += 1;
                }
            })
        })
        .collect();

    // Reader: must always find the pre-populated keys.
    let errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reader = {
        let tree = tree.clone();
        let errors = errors.clone();
        let done = done.clone();
        std::thread::spawn(move || {
            for _ in 0..200 {
                for i in 0u64..8 {
                    let key = format!("s{:04}", i).into_bytes();
                    if tree.first_entry_at_or_after_with_index(&key).is_none() {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                std::thread::yield_now();
            }
            done.store(true, Ordering::Relaxed);
        })
    };

    reader.join().unwrap();
    for w in writers {
        w.join().unwrap();
    }

    assert_eq!(
        errors.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "first_entry_at_or_after_with_index returned None for a key that exists"
    );
}
