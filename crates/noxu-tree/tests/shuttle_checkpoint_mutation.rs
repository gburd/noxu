// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation gate for the recovery-vs-mutation race:
//! the checkpointer's dirty-BIN flush pass racing concurrent inserts that
//! dirty and split BINs (DST recovery coverage).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg, the tree-node `RwLock` resolves (through
//! `noxu_util::dst_sync_pl`, routed in `noxu-tree/src/tree.rs`) to a
//! shuttle-instrumented lock, so shuttle's scheduler explores the
//! checkpoint-flush / insert interleavings of the *real* checkpoint path.
//!
//! # What is modelled
//!
//! [`Tree::shuttle_checkpoint_flush_bins`] is a faithful copy of the
//! `noxu_recovery::Checkpointer::flush_one_tree_bins` full-BIN path, MINUS the
//! `LogManager` WAL write this pure-tree harness cannot build:
//!
//!   1. `collect_dirty_bins(db_id)` under a tree/node READ lock — the snapshot
//!      of dirty-BIN `Arc`s taken at checkpoint start.
//!   2. per BIN: take the node WRITE lock; apply the JE X-8 early-exit guard
//!      (`!b.dirty && dirty_count()==0` → skip a node an evictor or a racing
//!      pass already flushed+cleared); otherwise capture every key (what
//!      `serialize_full` would have logged) and `clear_dirty_after_full_log`.
//!
//! The capture-then-clear runs under the SAME node write lock a concurrent
//! `insert` takes, so the flush and the insert serialise on that per-BIN
//! latch — exactly JE's ordering (the per-IN latch, not a global one, orders
//! the snapshot-clear against concurrent tree mutation).
//!
//! # The invariant this gate proves — LOST-DIRTY-NODE
//!
//! For every key inserted concurrently with a checkpoint, after the race
//! quiesces:
//!
//! ```text
//! captured(k)  OR  still_dirty(k)
//! ```
//!
//! i.e. the key was either made durable by this checkpoint (in the captured
//! full-log set) OR is still dirty in the tree (its slot or its BIN carries
//! the dirty flag, so `collect_dirty_bins` will hand it to the NEXT
//! checkpoint). What must NEVER happen: a key present in the tree but NOT
//! captured AND NOT dirty — a "silently clean-but-unflushed" slot. That is the
//! lost-dirty-node bug: a checkpoint clears the dirty flag on a slot it did
//! not actually log, so the node is never written and the insert is lost on
//! crash recovery.
//!
//! Also asserted: no schedule panics (no half-flushed split), and every key
//! present in the tree is in coherent sorted key order per BIN.
//!
//! # Not vacuous (the regression proof)
//!
//! The captured/dirty split is only meaningful because the capture and the
//! clear are ATOMIC under the per-BIN write lock. To prove the gate is not
//! vacuous, break that atomicity in `Tree::shuttle_checkpoint_flush_bins` by
//! clearing dirty WITHOUT capturing — replace the capture loop + clear with a
//! bare `b.clear_dirty_after_full_log(Lsn::new(1, 1));` (skip the
//! `captured.push` loop):
//!
//! ```ignore
//! // BROKEN: clear dirty without capturing the keys.
//! b.clear_dirty_after_full_log(Lsn::new(1, 1));
//! ```
//!
//! Under `--cfg noxu_shuttle` shuttle then finds a schedule where a key that
//! was inserted into a dirty BIN just before the checkpoint write-locked it is
//! cleared-but-not-captured, and [`checkpoint_racing_insert_no_lost_dirty`]
//! fails with "lost dirty node: key … present but neither captured nor
//! dirty" (verified: seed `4701725966304036809`, key `k0000a`). Restore the
//! capture-before-clear and every schedule passes. (Also reproducible by
//! moving `clear_dirty_after_full_log` BEFORE the X-8 guard so a slot inserted
//! between snapshot and write-lock is cleared without being in the snapshot —
//! same lost-dirty symptom.) The gate deliberately leaves the baseline BINs
//! DIRTY (no priming flush) and inserts keys that fall BETWEEN existing keys,
//! so the concurrently-inserted slot lands in a BIN the checkpoint has
//! snapshotted and is about to clear — without that overlap the race is
//! vacuous (the insert's BIN is never in the checkpoint's dirty set).
//!
//! # Invariants (mapped to `noxu-spec` recovery / checkpoint)
//!
//!   * **no-lost-dirty** (`checkpoint` / recovery durability) — every inserted
//!     key is captured or still dirty; never silently clean-but-unflushed.
//!   * **no-panic** — no schedule panics (no half-flushed / torn split during
//!     a concurrent checkpoint pass).
//!   * **key-order** — every present key is in sorted order within its BIN (no
//!     torn split left garbage).
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-tree --test shuttle_checkpoint_mutation
//! ```
#![cfg(noxu_shuttle)]

use noxu_tree::Tree;
use noxu_util::Lsn;
use shuttle::sync::Arc;

/// Number of interleavings shuttle explores per test.
const ITERATIONS: usize = 5_000;

/// Database id used for the whole gate (single tree).
const DB_ID: u64 = 1;

/// Build a level-2 tree (root Internal with BIN children) from a baseline
/// key set. The baseline BINs are left DIRTY (no priming flush): this is the
/// state at the start of a checkpoint — dirty BINs waiting to be logged.
/// `collect_dirty_bins` therefore snapshots them, and a concurrent insert that
/// lands in one of those dirty BINs races the per-BIN capture-then-clear. That
/// is precisely the window the lost-dirty-node bug lives in: an insert adds a
/// slot to a dirty BIN, then the checkpoint clears that BIN's dirty flag; if
/// the clear did not first capture the slot, the insert is silently lost.
///
/// Construction is single-threaded/uncontended, so it runs fine on shuttle's
/// cooperative executor (no green thread ever blocks on a lock here).
fn build_dirty_tree() -> Tree {
    let tree = Tree::new(DB_ID, 8);
    for i in 0..64u32 {
        tree.insert(
            format!("k{i:04}").into_bytes(),
            vec![i as u8],
            Lsn::new(1, i),
        )
        .expect("insert during setup");
    }
    tree
}

/// Assert every BIN in the tree holds its keys in sorted order (a torn split
/// during a concurrent checkpoint would leave garbage or an out-of-order key).
fn assert_all_bins_sorted(states: &[(Vec<u8>, bool)]) {
    // `shuttle_key_dirty_states` walks BINs left-to-right, and within a BIN
    // returns keys in slot order — which is sorted key order for a coherent
    // tree. A torn/duplicated key would break global sortedness.
    for w in states.windows(2) {
        assert!(
            w[0].0 <= w[1].0,
            "tree key order violated (torn split during checkpoint?): {:?} !< {:?}",
            w[0].0,
            w[1].0
        );
    }
}

/// THE LOST-DIRTY-NODE GATE: a checkpoint dirty-BIN flush pass racing
/// concurrent inserts that dirty (and may split) BINs.
///
/// One thread runs the real checkpoint flush sequence (snapshot dirty BINs
/// under read, then per-BIN write-lock + X-8 guard + capture + clear). Another
/// thread inserts a batch of fresh keys, dirtying the BINs they land in and
/// possibly splitting a full BIN. The two serialise on the per-BIN write lock.
///
/// After the race, for every inserted key exactly one of:
///   * it is in the checkpoint's captured set (flushed this checkpoint), or
///   * it is still dirty in the tree (reflushed by the next checkpoint).
///
/// Never present-but-clean-and-uncaptured (the lost-dirty-node bug).
#[test]
fn checkpoint_racing_insert_no_lost_dirty() {
    shuttle::check_random(
        || {
            let tree = Arc::new(build_dirty_tree());

            // Fresh keys inserted concurrently with the checkpoint. Each falls
            // BETWEEN two existing baseline keys, so it lands in an existing
            // (dirty, hence snapshotted) BIN — the exact overlap the
            // lost-dirty-node race needs: the insert adds a slot to a BIN the
            // checkpoint is about to write-lock and clear.
            let inserted: Vec<Vec<u8>> = (0..8u32)
                .map(|j| format!("k{:04}a", j * 7).into_bytes())
                .collect();

            let checkpointer = {
                let tree = Arc::clone(&tree);
                shuttle::thread::spawn(move || {
                    // The real checkpoint flush sequence. Returns the captured
                    // (durable) key set.
                    tree.shuttle_checkpoint_flush_bins(DB_ID)
                })
            };

            let inserter = {
                let tree = Arc::clone(&tree);
                let inserted = inserted.clone();
                shuttle::thread::spawn(move || {
                    for (j, k) in inserted.iter().enumerate() {
                        tree.insert(
                            k.clone(),
                            vec![j as u8],
                            Lsn::new(2, 100 + j as u32),
                        )
                        .expect("concurrent insert during checkpoint");
                    }
                })
            };

            let captured = checkpointer.join().unwrap();
            inserter.join().unwrap();

            // Post-race tree state: every present key + whether it is still
            // dirty (reflushed next checkpoint).
            let states = tree.shuttle_key_dirty_states();

            // no-panic + key-order: BINs coherent and sorted.
            assert_all_bins_sorted(&states);

            // LOST-DIRTY-NODE invariant: every inserted key is captured OR
            // still dirty. Build a lookup of present-key -> dirty.
            for k in &inserted {
                let captured_here = captured.iter().any(|c| c == k);
                // Find the key's dirty state in the tree (it MUST be present:
                // insert cannot lose it — separate from the durability check).
                let present_dirty: Option<bool> = states
                    .iter()
                    .find(|(sk, _)| sk == k)
                    .map(|(_, dirty)| *dirty);

                match present_dirty {
                    // Present and dirty: fine — next checkpoint reflushes it.
                    Some(true) => {}
                    // Present but clean: only OK if THIS checkpoint captured it.
                    Some(false) => {
                        assert!(
                            captured_here,
                            "lost dirty node: key {:?} present but neither \
                             captured nor dirty (clean-but-unflushed)",
                            k
                        );
                    }
                    // Not present at all: insert lost the key entirely — a
                    // different (insert) bug, but still a failure.
                    None => {
                        panic!(
                            "inserted key {:?} vanished from the tree \
                             (insert lost during checkpoint race)",
                            k
                        );
                    }
                }
            }
        },
        ITERATIONS,
    );
}

/// A second checkpoint pass after the race must make durable everything the
/// first pass left dirty — i.e. iterating checkpoints converges: no key stays
/// dirty forever, and after two checkpoints with no further mutation every
/// inserted key is captured by SOME checkpoint. This asserts the recovery
/// guarantee end-to-end: a key dirtied during checkpoint N is captured by
/// checkpoint N+1.
#[test]
fn second_checkpoint_captures_leftover_dirty() {
    shuttle::check_random(
        || {
            let tree = Arc::new(build_dirty_tree());
            let inserted: Vec<Vec<u8>> = (0..6u32)
                .map(|j| format!("k{:04}a", j * 9).into_bytes())
                .collect();

            let checkpointer = {
                let tree = Arc::clone(&tree);
                shuttle::thread::spawn(move || {
                    tree.shuttle_checkpoint_flush_bins(DB_ID)
                })
            };
            let inserter = {
                let tree = Arc::clone(&tree);
                let inserted = inserted.clone();
                shuttle::thread::spawn(move || {
                    for (j, k) in inserted.iter().enumerate() {
                        tree.insert(
                            k.clone(),
                            vec![j as u8],
                            Lsn::new(2, 200 + j as u32),
                        )
                        .expect("concurrent insert during checkpoint");
                    }
                })
            };

            let captured1 = checkpointer.join().unwrap();
            inserter.join().unwrap();

            // No more mutation: a second checkpoint must capture everything the
            // first left dirty.
            let captured2 = tree.shuttle_checkpoint_flush_bins(DB_ID);

            // Convergence: every inserted key is captured by checkpoint 1 or 2.
            for k in &inserted {
                let in1 = captured1.iter().any(|c| c == k);
                let in2 = captured2.iter().any(|c| c == k);
                assert!(
                    in1 || in2,
                    "key {:?} never made durable by two consecutive \
                     checkpoints (dirty flag lost?)",
                    k
                );
            }

            // And nothing is left dirty after the quiescent second pass.
            let states = tree.shuttle_key_dirty_states();
            for (k, dirty) in &states {
                assert!(
                    !dirty,
                    "key {:?} still dirty after two quiescent checkpoints",
                    k
                );
            }
        },
        ITERATIONS,
    );
}
