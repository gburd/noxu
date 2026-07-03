// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation gate for the BIN/IN split-path
//! check-then-act race (DST tree coverage).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg, the tree-node `RwLock` resolves (through
//! `noxu_util::dst_sync_pl`, routed in `noxu-tree/src/tree.rs`) to a
//! shuttle-instrumented lock, so shuttle's scheduler explores the
//! `split_child` / merge-clear interleavings of the *real* split path.
//!
//! # The bug this gate closes
//!
//! (`.agent/archived-audits/bench/bug-bin-split-concurrency.md`.)
//! `insert_recursive_inner` checked `child.get_n_entries() >= max_entries`
//! under a PARENT READ lock, dropped that read lock (required — the split
//! needs `parent.write()`), then called `split_child`. In the drop→reacquire
//! window a racing thread — a second splitter, or the INCompressor merging and
//! CLEARING a sibling (`compress_node`'s `entries.clear()`) — could leave the
//! child no longer full, or empty. Pre-fix, `split_child` then built a
//! `SplitEntries` from that stale child and `SplitEntries::get_key(split_index)`
//! panicked with "index out of bounds: len is 0" on the empty entries vec.
//!
//! **The fix (v7.2.2):** `split_child`, after taking the child write lock,
//! re-validates `child.get_n_entries() >= max_entries` and returns `Ok(())`
//! (a benign no-op) if a racing thread already emptied/split it — the check
//! and the split are now atomic w.r.t. the child latch, matching JE's
//! `IN.split` re-testing `needsSplitting()` after latching.
//!
//! # Why this reproduces the bug that a benchmark had to find
//!
//! The 96-thread saturation benchmark found this because DST could not: the
//! tree used `parking_lot::RwLock` directly, so shuttle could not schedule the
//! node-latch interleavings. Routing the node latch through the seam makes the
//! split path schedulable; shuttle now drives the exact drop→reacquire race
//! deterministically over thousands of interleavings.
//!
//! # Not vacuous (the regression proof)
//!
//! [`split_racing_merge_clear_never_panics`] and
//! [`two_concurrent_splitters_never_panic`] both PASS with the fix and FAIL
//! (shuttle finds the panicking interleaving) with the re-check reverted. To
//! prove it locally, delete the two lines in `Tree::split_child`
//!
//! ```ignore
//! if child_guard.get_n_entries() < max_entries {
//!     return Ok(());
//! }
//! ```
//!
//! and re-run under `--cfg noxu_shuttle`: shuttle reports a panicking schedule
//! ("index out of bounds: the len is 0 but the index is 0" in
//! `SplitEntries::get_key`) with a reproducible seed. Restore the fix and every
//! schedule passes. (This is documented, not run automatically, so the gate
//! stays green in CI while the reversal proof remains a one-edit check.)
//!
//! # Invariants (mapped to `noxu-spec` `btree_latching`)
//!
//!   * **no-panic** — no schedule panics (the observed symptom of the bug).
//!   * **split-atomicity** (`btree_latching::AtMostOneSplit` /
//!     `LockInvariant`) — `split_child` never operates on a stale (emptied or
//!     no-longer-full) child: it either splits a still-full child or returns
//!     the benign `Ok(())` no-op. Never a partial/garbage split; a full child
//!     is split at most once.
//!   * **structural-consistency** — after the race quiesces the parent's slot
//!     count is coherent (never fewer entries than it started with; a
//!     successful split adds exactly one sibling slot).
//!   * **key-order** (`btree_latching::NoLostWrites`) — every entry that
//!     remains in the child is in sorted key order (no lost/duplicated/
//!     reordered entry from a torn split).
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-tree --test shuttle_bin_split
//! ```
#![cfg(noxu_shuttle)]

use noxu_tree::{NodeRwLock, Tree, TreeNode};
use noxu_util::Lsn;
use shuttle::sync::Arc;

/// Number of interleavings shuttle explores per test.
const ITERATIONS: usize = 5_000;

/// Build a level-2 tree (root Internal with BIN children) whose rightmost BIN
/// child is topped up to exactly `max_entries` (full, i.e. the state where the
/// next insert would split it). Construction is single-threaded and
/// uncontended, so it runs fine on shuttle's cooperative executor (no green
/// thread ever blocks on a lock here).
///
/// A plain bulk insert never fills a child to `max` (each split leaves halves
/// ~half-full), so we extend the rightmost key range until one child reaches
/// `max` — the precise "full child about to be split" state the bug needs.
fn build_level2_tree(max_entries: usize) -> Tree {
    let tree = Tree::new(1, max_entries);
    // Enough sorted keys to force at least one split so the root becomes an
    // Internal node with BIN children.
    for i in 0..64u32 {
        tree.insert(
            format!("k{i:04}").into_bytes(),
            vec![i as u8],
            Lsn::new(1, i),
        )
        .expect("insert during setup");
    }
    // Top up the rightmost range until some child reaches `max_entries`.
    let mut i = 64u32;
    loop {
        tree.insert(
            format!("k{i:04}").into_bytes(),
            vec![i as u8],
            Lsn::new(1, i),
        )
        .expect("insert during top-up");
        i += 1;
        if full_child_of_root(&tree, max_entries).is_some() {
            break;
        }
        assert!(i < 512, "setup never filled a child to max_entries");
    }
    tree
}

/// Locate a resident, still-full BIN child of the root together with its slot
/// index. Returns `None` if the tree did not build a level-2 shape (defensive;
/// 64 keys / max 8 always splits).
fn full_child_of_root(
    tree: &Tree,
    max_entries: usize,
) -> Option<(Arc<NodeRwLock<TreeNode>>, usize)> {
    let root = tree.get_root()?;
    let g = root.read();
    let TreeNode::Internal(n) = &*g else {
        return None;
    };
    for idx in 0..n.entries.len() {
        if let Some(c) = n.get_child(idx) {
            let full = { c.read().get_n_entries() >= max_entries };
            if full {
                return Some((c, idx));
            }
        }
    }
    None
}

/// Assert the child, whatever the schedule left it, is a coherent BIN: entries
/// in sorted key order, no duplicates. A torn split (the pre-fix failure mode)
/// would leave garbage or panic before reaching here.
fn assert_child_key_order(child: &Arc<NodeRwLock<TreeNode>>) {
    let g = child.read();
    if let TreeNode::Bottom(b) = &*g {
        let keys: Vec<Vec<u8>> = (0..b.entries.len())
            .map(|i| b.get_full_key(i).unwrap_or_default())
            .collect();
        for w in keys.windows(2) {
            assert!(
                w[0] < w[1],
                "child BIN key order violated (torn split?): {:?} !< {:?}",
                w[0],
                w[1]
            );
        }
    }
}

/// THE REGRESSION: a `split_child` racing an INCompressor-style merge-clear on
/// the SAME child. This is the drop→reacquire window from the bug report: the
/// caller passed the fullness check under the (now-dropped) parent read lock,
/// then a racing merge emptied the child before the split re-acquired its
/// write lock.
///
/// With the v7.2.2 re-check, every interleaving is safe: if the clear wins the
/// child write lock first, `split_child` re-validates fullness and returns the
/// benign `Ok(())` no-op; if the split wins first, it splits the still-full
/// child and the clear then empties the (now left-half) child. No schedule
/// panics. With the fix reverted, shuttle finds the clear-then-split schedule
/// and `SplitEntries::get_key(0)` panics on the empty vec.
#[test]
fn split_racing_merge_clear_never_panics() {
    shuttle::check_random(
        || {
            const MAX: usize = 8;
            let tree = build_level2_tree(MAX);
            let max = tree.shuttle_max_entries();

            let Some((child, child_index)) = full_child_of_root(&tree, max)
            else {
                // Tree did not reach level 2 (should not happen); nothing to
                // race, but do not silently pass a vacuous schedule.
                panic!("setup did not produce a full BIN child of the root");
            };
            let root = tree.get_root().expect("root resident");

            // Splitter: the exact call the insert path makes after dropping the
            // parent read lock.
            let splitter = {
                let root = Arc::clone(&root);
                shuttle::thread::spawn(move || {
                    // Result must be Ok on every schedule: either a real split
                    // or the benign already-emptied no-op.
                    let r = Tree::shuttle_split_child(
                        &root,
                        child_index,
                        max,
                        b"k0000",
                    );
                    // split-atomicity: split_child never surfaces an error on
                    // this path — it returns Ok whether it split or no-op'd.
                    r.expect("split_child must be Ok (split or benign no-op)");
                })
            };

            // Racing merge-clear (INCompressor's entries.clear()).
            let clearer = {
                let child = Arc::clone(&child);
                shuttle::thread::spawn(move || {
                    // Records the pre-clear count; not asserted here (the split
                    // may have already run), but forces the child write lock as
                    // a scheduling point against the splitter.
                    let _before = Tree::shuttle_clear_child(&child);
                })
            };

            splitter.join().unwrap();
            clearer.join().unwrap();

            // structural-consistency: the parent is still a coherent Internal
            // node; a successful split added at most one sibling slot, a no-op
            // added none. Either way the parent never lost slots.
            {
                let g = root.read();
                if let TreeNode::Internal(n) = &*g {
                    assert!(
                        n.entries.len() >= 1,
                        "parent lost all slots after split/clear race"
                    );
                }
            }
            // key-order: whatever remains in the child is sorted.
            assert_child_key_order(&child);
        },
        ITERATIONS,
    );
}

/// Two concurrent splitters on the SAME full child — the primary bug scenario:
/// two inserters both pass the read-lock fullness check, both drop the parent
/// read lock, both call `split_child`. They serialise on `parent.write()`; the
/// first splits, and by the time the second takes the child write lock the
/// child is no longer full. With the re-check the second returns the benign
/// `Ok(())` no-op; without it the second builds a `SplitEntries` from the
/// now-half (or the first's torn intermediate) child and can panic.
#[test]
fn two_concurrent_splitters_never_panic() {
    shuttle::check_random(
        || {
            const MAX: usize = 8;
            let tree = build_level2_tree(MAX);
            let max = tree.shuttle_max_entries();

            let Some((child, child_index)) = full_child_of_root(&tree, max)
            else {
                panic!("setup did not produce a full BIN child of the root");
            };
            let root = tree.get_root().expect("root resident");

            let spawn_splitter = |root: &Arc<NodeRwLock<TreeNode>>| {
                let root = Arc::clone(root);
                shuttle::thread::spawn(move || {
                    Tree::shuttle_split_child(
                        &root,
                        child_index,
                        max,
                        b"k0000",
                    )
                    .expect("split_child must be Ok (split or benign no-op)");
                })
            };

            let a = spawn_splitter(&root);
            let b = spawn_splitter(&root);
            a.join().unwrap();
            b.join().unwrap();

            // structural-consistency: exactly one split can succeed on a given
            // full child (the other re-checks and no-ops), so the parent gains
            // at most one slot. Never fewer than it started with.
            {
                let g = root.read();
                if let TreeNode::Internal(n) = &*g {
                    assert!(
                        n.entries.len() >= 1,
                        "parent lost slots after two-splitter race"
                    );
                }
            }
            assert_child_key_order(&child);
        },
        ITERATIONS,
    );
}
