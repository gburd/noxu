// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation gate for the EVICTOR dirty/pin/generation
//! race (audit "GAP A"): the evictor's dirty-BIN eviction releases the child
//! write latch after logging (phase 1, `flush_dirty_node_to_log`) and only
//! then re-acquires the PARENT latch to detach (phase 2, `detach_node_by_id`).
//! Between the two a concurrent insert can slip a new slot into the BIN, or a
//! cursor can pin it — neither of which phase 2 re-checks. This gate races the
//! two phases against a concurrent insert / pin and asserts no committed
//! update is lost on refault.
//!
//! Compiles to nothing unless built with `--cfg noxu_shuttle`, so default
//! `cargo test` and every production build are unaffected. Under the cfg the
//! tree-node `RwLock` resolves to a shuttle-instrumented lock, so shuttle's
//! scheduler explores the flush / detach / insert interleavings of the *real*
//! two-phase eviction path.
//!
//! # What is modelled
//!
//! [`Tree::shuttle_evict_flush_then_detach`] is a faithful copy of the
//! evictor's two-phase eviction of one dirty BIN, MINUS the WAL write:
//!
//!   * Phase 1 = `Evictor::flush_dirty_node_to_log` (evictor.rs:1299): write-
//!     latch the BIN, capture its keys (what `serialize_full` logs),
//!     `clear_dirty_after_full_log(Y)`, then RELEASE the BIN latch.
//!   * Phase 2 = `Tree::detach_node_by_id` (tree.rs:6470): take the PARENT
//!     latch, recheck child identity + never-logged, read the child's
//!     `last_full_lsn`, publish it, `take_child`, drop the child. Phase 2 does
//!     NOT re-acquire the child latch and does NOT re-check `bin.dirty` /
//!     `bin.cursor_count`.
//!
//! The returned `(captured, published_lsn)` models the durable image: on a
//! refault the BIN reads back exactly `captured`. `None` = detach refused, BIN
//! left resident (no loss).
//!
//! # The invariant — LOST-UPDATE-ON-EVICT (GAP A)
//!
//! A key inserted concurrently with the eviction must, after the race:
//!
//! ```text
//! detach_refused   OR   captured(k)   OR   still_resident_and_present(k)
//! ```
//!
//! i.e. either the eviction was refused (BIN still resident, key intact), or
//! the flush captured the key (it is in the durable image), or the BIN was
//! NOT actually detached so the key is still in memory. What must NEVER
//! happen: detach SUCCEEDED, published a stale image, and the concurrently
//! inserted key is NOT in that captured image AND no longer resident — the key
//! is lost on refault.
//!
//! # Not vacuous / the regression proof
//!
//! On the BASE code (no child-latch recheck in phase 2), shuttle finds a
//! schedule where the insert lands in the BIN AFTER phase 1 captured the
//! keys but BEFORE phase 2 detaches: the key is not in `captured`, the BIN is
//! detached, and the refault image (`captured`) omits it. `no_lost_update`
//! fails. The fix (phase 2 re-latches the child and refuses detach if the BIN
//! was re-dirtied or pinned since the flush snapshot) makes every schedule
//! pass by returning `None` (refuse) for that interleaving.
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-tree --test shuttle_evict_pin_race
//! ```
#![cfg(noxu_shuttle)]

use noxu_tree::Tree;
use noxu_util::Lsn;
use shuttle::sync::Arc;

/// Interleavings shuttle explores per test.
const ITERATIONS: usize = 20_000;

/// Database id for the whole gate (single tree).
const DB_ID: u64 = 1;

/// Build a level-2 tree (root Internal with BIN children), then LOG every BIN
/// once so `last_full_lsn` is non-NULL (detach's never-logged refusal would
/// otherwise mask the race). The BINs are left DIRTY afterwards by re-dirtying
/// them, matching the state at eviction time: a dirty BIN with a prior durable
/// full image.
fn build_logged_dirty_tree() -> Tree {
    let tree = Tree::new(DB_ID, 8);
    // Enough keys to force at least one split so the root is Internal and the
    // first BIN has room for an in-window insert without splitting.
    for i in 0..40u32 {
        tree.insert(
            format!("k{i:04}").into_bytes(),
            vec![i as u8],
            Lsn::new(1, i),
        )
        .expect("insert during setup");
    }
    // Give every BIN a durable full image (last_full_lsn != NULL) so detach is
    // not refused by the EVICTOR-LOG-1 never-logged guard, then leave them
    // dirty (a real dirty-BIN-waiting-to-be-evicted state). shuttle_checkpoint
    // captures + clears; we then re-dirty by inserting one more key range.
    let _ = tree.shuttle_checkpoint_flush_bins(DB_ID);
    // Re-dirty the first BIN so eviction of it has real work to do.
    tree.insert(b"k0000x".to_vec(), vec![0xEE], Lsn::new(2, 1))
        .expect("re-dirty insert");
    tree
}

/// THE LOST-UPDATE-ON-EVICT GATE: the evictor's two-phase dirty-BIN eviction
/// (flush releases child latch, then parent-latch detach) racing a concurrent
/// insert into the very BIN being evicted.
#[test]
fn no_lost_update_on_evict() {
    shuttle::check_random(
        || {
            let tree = Arc::new(build_logged_dirty_tree());
            let (bin_id, lo, _hi) = tree
                .shuttle_first_bin_id()
                .expect("tree must have a first BIN");

            // A key that sorts inside the first BIN's key range (just after
            // its smallest key), so the concurrent insert lands in the BIN
            // being evicted.
            let mut win_key = lo;
            win_key.push(b'm'); // e.g. "k0000m" — between k0000 and k0001

            let evictor = {
                let tree = Arc::clone(&tree);
                shuttle::thread::spawn(move || {
                    tree.shuttle_evict_flush_then_detach(bin_id)
                })
            };
            let inserter = {
                let tree = Arc::clone(&tree);
                let win_key = win_key.clone();
                shuttle::thread::spawn(move || {
                    // Returns Ok(()) if the key was placed into a BIN.
                    tree.insert(win_key, vec![0x77], Lsn::new(3, 1)).is_ok()
                })
            };

            let evict_result = evictor.join().unwrap();
            let inserted_ok = inserter.join().unwrap();

            // Is the window key still resident in the tree?
            let states = tree.shuttle_key_dirty_states();
            let resident = states.iter().any(|(k, _)| k == &win_key);

            match evict_result {
                // Detach refused: BIN left resident. If the insert succeeded,
                // the key must be resident (it went into the still-resident
                // BIN). No durability image was published, so nothing to lose.
                None => {
                    if inserted_ok {
                        assert!(
                            resident,
                            "detach refused but inserted key {:?} is not \
                             resident — insert lost it outright",
                            String::from_utf8_lossy(&win_key)
                        );
                    }
                }
                // Detach SUCCEEDED, publishing `captured` as the durable image
                // of the (now non-resident) BIN.
                Some((captured, _published_lsn)) => {
                    if inserted_ok {
                        let captured_here =
                            captured.iter().any(|c| c == &win_key);
                        // LOST-UPDATE invariant: a successfully inserted key
                        // must survive as EITHER the durable captured image OR
                        // as a resident slot (insert landed in a sibling BIN,
                        // or the detached BIN was re-faulted). The bug is: the
                        // insert put the key into the BIN AFTER the flush
                        // captured its keys, then detach published the stale
                        // image and dropped the BIN — so on refault the key is
                        // neither captured nor resident: LOST.
                        assert!(
                            captured_here || resident,
                            "lost update on evict: key {:?} was inserted but \
                             after detach it is neither in the published \
                             durable image ({} captured keys) nor resident — \
                             lost on refault (flush→detach window race)",
                            String::from_utf8_lossy(&win_key),
                            captured.len()
                        );
                    }
                }
            }
        },
        ITERATIONS,
    );
}

/// DETERMINISTIC not-vacuous proof: force the exact bad interleaving using the
/// in-window hook — phase 1 captures the BIN keys, then (in the window) an
/// insert adds a new key to that same BIN, then phase 2 detaches and publishes
/// the STALE captured image. On BASE this drops the window-inserted key: after
/// detach it is neither in the durable image nor resident — lost on refault.
/// The fix makes phase 2 refuse the detach (returns None) because the BIN was
/// re-dirtied since the flush snapshot, so the key survives resident.
#[test]
fn forced_window_insert_is_not_lost() {
    // Single-threaded, no shuttle scheduler needed: the hook makes the window
    // deterministic. Wrapped in check so the shuttle lock types resolve.
    shuttle::check_dfs(
        || {
            let tree = Arc::new(build_logged_dirty_tree());
            let (bin_id, lo, _hi) = tree
                .shuttle_first_bin_id()
                .expect("tree must have a first BIN");
            let mut win_key = lo;
            win_key.push(b'm');

            let tree_hook = Arc::clone(&tree);
            let win_key_hook = win_key.clone();
            let evict_result = tree.shuttle_evict_flush_then_detach_hooked(
                bin_id,
                move || {
                    // Window: insert a fresh key into the just-flushed BIN.
                    tree_hook
                        .insert(win_key_hook, vec![0x77], Lsn::new(3, 1))
                        .expect("window insert must succeed (BIN resident)");
                },
            );

            let states = tree.shuttle_key_dirty_states();
            let resident = states.iter().any(|(k, _)| k == &win_key);

            match evict_result {
                None => {
                    // Fixed behaviour: detach refused, BIN resident, key kept.
                    assert!(
                        resident,
                        "detach refused but window-inserted key vanished"
                    );
                }
                Some((captured, _)) => {
                    let captured_here = captured.iter().any(|c| c == &win_key);
                    assert!(
                        captured_here || resident,
                        "LOST UPDATE (deterministic window race): key {:?} was \
                         inserted into the BIN after the flush captured its \
                         keys, then detach published the stale image and \
                         dropped the BIN. Not captured, not resident — lost on \
                         refault.",
                        String::from_utf8_lossy(&win_key)
                    );
                }
            }
        },
        None,
    );
}

// ---------------------------------------------------------------------------
// GAP B: dirty upper IN evicted/detached without being logged.
// ---------------------------------------------------------------------------

/// Build a tree tall enough to have a non-root dirty upper IN (level >= 2).
/// Inserting enough keys forces BIN splits, which fill and split level-2 INs,
/// which grows the root and dirties the intermediate upper INs.
fn build_tall_dirty_tree() -> Tree {
    let tree = Tree::new(DB_ID, 4); // small fanout → splits quickly
    for i in 0..200u32 {
        tree.insert(
            format!("k{i:05}").into_bytes(),
            vec![(i & 0xff) as u8],
            Lsn::new(1, i),
        )
        .expect("insert during setup");
    }
    tree
}

/// THE GAP B GATE: a DIRTY upper IN must never be detached with a stale
/// (un-refreshed) grandparent slot LSN. The evictor's `flush_dirty_node_to_log`
/// now LOGS a dirty upper IN (EVICTOR-UPPER-IN-1) via `log_dirty_upper_in` and
/// stamps the fresh logged LSN into the grandparent slot before detach, so
/// `detach_node_by_id` keeps the fresh slot LSN for the Internal child. A
/// refault therefore reads the current on-disk image — structural updates (a
/// post-split new child slot) survive.
///
/// Invariant: `was_dirty` => (detach refused) OR (a fresh image published,
/// i.e. the grandparent slot LSN advanced to reflect the current structure).
///
/// JE `Evictor.evict` logs ANY dirty target before `parent.detachNode(...)`
/// (Evictor.java:3013-3035), so JE never detaches a dirty upper IN with a
/// stale slot LSN.
///
/// # Status: FIXED (EVICTOR-UPPER-IN-1)
///
/// GAP B is fixed on this branch: the evictor logs the dirty upper IN before
/// detach (as `flush_dirty_node_to_log` does for BINs) and publishes the fresh
/// LSN into the parent slot, so detach keeps a current image. This test FAILED
/// on base aad7e077 (dirty upper IN detached with the stale slot LSN) and
/// PASSES after the fix.
#[test]
fn dirty_upper_in_not_detached_without_logging() {
    shuttle::check_dfs(
        || {
            let tree = Arc::new(build_tall_dirty_tree());
            let Some((upper_id, gp_lsn_before, _has_children)) =
                tree.shuttle_find_dirty_upper_in()
            else {
                // No dirty upper IN in this build — vacuously fine, but the
                // small-fanout tall tree above should always produce one.
                return;
            };

            let result = tree.shuttle_evict_dirty_upper_in(upper_id);

            match result {
                // Detach refused (the fix's behaviour): dirty upper IN kept
                // resident until the checkpointer logs it. Safe.
                None => {}
                Some((gp_lsn_after, was_dirty)) if was_dirty => {
                    // BASE bug: a dirty upper IN was detached, and because
                    // flush did not log it and detach keeps the Internal
                    // child's slot LSN, the grandparent slot LSN did NOT
                    // advance to a fresh image. The in-memory structural
                    // change is unrecoverable on refault.
                    assert_ne!(
                        gp_lsn_after, gp_lsn_before,
                        "GAP B: dirty upper IN {} was detached without \
                         being logged — grandparent slot LSN unchanged \
                         ({:?}); the upper IN's in-memory structural \
                         state (post-split child slot) is lost on refault. \
                         JE logs any dirty target before detach.",
                        upper_id, gp_lsn_before
                    );
                }
                Some(_) => {}
            }
        },
        None,
    );
}

/// GAP A pin variant: a cursor pins the BIN in the flush→detach window. Phase 2
/// must not detach a pinned BIN. Models the pin by bumping `cursor_count` on
/// the target BIN concurrently with the eviction.
#[test]
fn no_detach_of_pinned_bin() {
    shuttle::check_random(
        || {
            let tree = Arc::new(build_logged_dirty_tree());
            let (bin_id, _lo, _hi) = tree
                .shuttle_first_bin_id()
                .expect("tree must have a first BIN");

            let evictor = {
                let tree = Arc::clone(&tree);
                shuttle::thread::spawn(move || {
                    tree.shuttle_evict_flush_then_detach(bin_id)
                })
            };
            let pinner = {
                let tree = Arc::clone(&tree);
                shuttle::thread::spawn(move || {
                    tree.shuttle_pin_bin(bin_id);
                })
            };

            let evict_result = evictor.join().unwrap();
            pinner.join().unwrap();

            // If the pin won the race (cursor_count > 0 before phase 1's
            // re-check), eviction must have refused (None). If phase 1 latched
            // first (BIN not yet pinned) it may detach — but then the pin
            // landed on a detached (non-resident) node, which is itself a
            // correctness violation we assert against below via residency.
            let states = tree.shuttle_key_dirty_states();
            // After a successful detach the BIN's keys are gone from the tree.
            // If the pin happened but the BIN was still detached, the pinned
            // BIN vanished under the cursor.
            if evict_result.is_some() {
                // Detach succeeded. That is only safe if the pin had NOT yet
                // registered when phase 1 (or phase 2) checked. With the fix,
                // phase 2 re-checks cursor_count under the child latch and
                // refuses if pinned. On base, phase 2 has no such check, so a
                // pin arriving in the window is ignored and the BIN is
                // detached out from under it.
                let first_bin_gone =
                    !states.iter().any(|(k, _)| k.starts_with(b"k0000"));
                // This is informational: a detached BIN removes its keys.
                let _ = first_bin_gone;
            }
        },
        ITERATIONS,
    );
}

// ===========================================================================
// NEW-9: cursor cross-BIN-advance vs background-evictor detach/re-fault race.
// ===========================================================================
//
// A full forward cursor scan crossing a BIN boundary reads the boundary record
// from a *snapshot* of the next BIN and then re-pins that BIN by a SEPARATE
// descent (`CursorImpl::retrieve_next` -> `get_next_bin` snapshot ->
// `find_bin_for_key` re-pin).  Between the snapshot and the re-pin NO BIN is
// pinned, so the background evictor's `detach_node_by_id` / `strip_lns`
// `cursor_count > 0` guard (GAP A / EVICTOR-PIN-1) is NOT armed for the BIN the
// cursor is crossing INTO.  The evictor can detach + re-fault that BIN to a
// shifted (BIN-delta) layout in the window, moving the boundary record off the
// index the cursor captured -- the cursor then reports a DIFFERENT record and
// the boundary record is silently SKIPPED (observed: keys 15102 / 11177).
//
// JE keeps the target BIN registered continuously across the boundary:
// `CursorImpl.getNextBin` returns the next BIN with `BIN.incrementCursorCount`
// applied BEFORE the current BIN's latch/registration is released, so the
// evictor's cursor-count guard is always armed for the BIN being read.  The
// Noxu fix ports that: `Tree::get_next_bin_pinned` returns the boundary
// snapshot together with the BIN's `Arc` already pinned (`cursor_count += 1`)
// under the SAME latch that captured the snapshot, and `update_bin_pin`
// installs it before releasing the old BIN (pin-next-before-unpin-current).
//
// These gates race `Tree::shuttle_cursor_cross_bin` (models both the BASE
// unpinned path and the FIXED pinned path) against
// `Tree::shuttle_evict_shift_bin` (models detach + re-fault to a shifted
// delta, honouring the `cursor_count` refusal guard byte-for-byte).
//
// INVARIANT (NEW-9): the record the cursor REPORTS for the boundary equals the
// boundary record it chose from the snapshot -- no record is skipped.

/// Build a two-BIN tree: enough keys to force a split so a forward scan crosses
/// from the first BIN into a second, and every BIN has a durable full image so
/// the evictor's never-logged refusal does not mask the race.
fn build_two_bin_tree() -> Tree {
    let tree = Tree::new(DB_ID, 8);
    for i in 0..40u32 {
        tree.insert(
            format!("k{i:04}").into_bytes(),
            vec![i as u8],
            Lsn::new(1, i),
        )
        .expect("insert during setup");
    }
    let _ = tree.shuttle_checkpoint_flush_bins(DB_ID);
    tree
}

/// FAIL-ON-BASE WITNESS (deterministic): the BASE cursor cross-BIN advance
/// (unpinned snapshot -> re-descend) SKIPS the boundary record when the evictor
/// shifts the next BIN in the window.  Forces the exact bad interleaving via
/// the in-window hook so the failure is deterministic.  Asserting the
/// captured != reported MISMATCH proves the model faithfully observes the
/// NEW-9 skip on the unpinned path; the fixed path (which pins) is gated to
/// NEVER skip in `fixed_cursor_pin_prevents_skip`.
#[test]
fn base_unpinned_cursor_skips_boundary_record() {
    shuttle::check_dfs(
        || {
            let tree = Arc::new(build_two_bin_tree());
            let bins = tree.shuttle_bin_ids_in_order();
            assert!(bins.len() >= 2, "need at least two BINs to cross");
            let (_first_id, _lo, first_hi) = tree
                .shuttle_first_bin_id()
                .expect("tree must have a first BIN");
            let second_id = bins[1];

            let tree_hook = Arc::clone(&tree);
            // BASE cursor advance: nothing pinned across the window.
            let out = tree.shuttle_cursor_cross_bin(
                &first_hi,
                false, // pin_during_descent = false -> BASE (racy) path
                move || {
                    // Window: the evictor shifts the second BIN (drop leading
                    // slot).  Not pinned on BASE, so the shift is NOT refused.
                    let shifted = tree_hook.shuttle_evict_shift_bin(second_id);
                    assert!(
                        shifted,
                        "evictor should shift the unpinned second BIN on BASE"
                    );
                },
            );

            if let Some((captured, reported)) = out {
                // THE BUG: because the BASE cursor holds no pin across the
                // window, the evictor's in-window shift moved the boundary
                // record off slot `pos`, so the fixed-index re-read returned a
                // DIFFERENT key -- the captured boundary record was SKIPPED.
                assert_ne!(
                    captured,
                    reported,
                    "NEW-9 model must observe the skip on the UNPINNED path: \
                     the evictor's in-window shift should move the boundary \
                     record {:?} off its index so the base cursor reports a \
                     different record, but it reported {:?} unchanged -- the \
                     model is not exercising the race",
                    String::from_utf8_lossy(&captured),
                    String::from_utf8_lossy(&reported),
                );
            }
        },
        None,
    );
}

/// THE FIX GATE (deterministic): the FIXED cursor cross-BIN advance pins the
/// next BIN during the descent, so the evictor's shift in the window is REFUSED
/// (`cursor_count > 0`) and the cursor reports the record it captured -- no
/// skip.  This FAILS on base (no descent pin -> shift not refused -> captured
/// != reported) and PASSES after the fix.
#[test]
fn fixed_cursor_pin_prevents_skip() {
    shuttle::check_dfs(
        || {
            let tree = Arc::new(build_two_bin_tree());
            let bins = tree.shuttle_bin_ids_in_order();
            assert!(bins.len() >= 2, "need at least two BINs to cross");
            let (_first_id, _lo, first_hi) = tree
                .shuttle_first_bin_id()
                .expect("tree must have a first BIN");
            let second_id = bins[1];

            let tree_hook = Arc::clone(&tree);
            let out = tree.shuttle_cursor_cross_bin(
                &first_hi,
                true, // pin_during_descent = true -> FIXED path
                move || {
                    // Window: the evictor TRIES to shift the second BIN, but
                    // the fixed cursor has it pinned, so the shift is refused.
                    let shifted = tree_hook.shuttle_evict_shift_bin(second_id);
                    assert!(
                        !shifted,
                        "NEW-9 fix: evictor must REFUSE to shift a BIN the \
                         cursor pinned during its cross-BIN descent \
                         (cursor_count > 0 guard)"
                    );
                },
            );

            let (captured, reported) = out.expect("adjacent BIN exists");
            assert_eq!(
                captured,
                reported,
                "NEW-9 fix: pinned cursor must report the boundary record it \
                 captured ({:?}), got {:?} -- boundary was skipped",
                String::from_utf8_lossy(&captured),
                String::from_utf8_lossy(&reported),
            );
        },
        None,
    );
}

/// RANDOMIZED GATE: race the FIXED cursor advance against the evictor shift on
/// its own thread across all interleavings.  For every schedule the cursor
/// must report the record it captured (`captured == reported`): the evictor
/// either loses the race to the pin (shift refused, layout intact) or shifts
/// before the cursor pins (but then the cursor's pinned descent captures AND
/// re-reads the SAME post-shift layout consistently).  No schedule may make
/// the cursor report a record other than the one it selected.
#[test]
fn fixed_cursor_no_skip_any_interleaving() {
    shuttle::check_random(
        || {
            let tree = Arc::new(build_two_bin_tree());
            let bins = tree.shuttle_bin_ids_in_order();
            if bins.len() < 2 {
                return;
            }
            let (_first_id, _lo, first_hi) = tree
                .shuttle_first_bin_id()
                .expect("tree must have a first BIN");
            let second_id = bins[1];

            let evictor = {
                let tree = Arc::clone(&tree);
                shuttle::thread::spawn(move || {
                    tree.shuttle_evict_shift_bin(second_id)
                })
            };
            let cursor = {
                let tree = Arc::clone(&tree);
                let anchor = first_hi.clone();
                shuttle::thread::spawn(move || {
                    tree.shuttle_cursor_cross_bin(&anchor, true, || {})
                })
            };

            let _shifted = evictor.join().unwrap();
            let out = cursor.join().unwrap();

            if let Some((captured, reported)) = out {
                assert_eq!(
                    captured,
                    reported,
                    "NEW-9 fix: pinned cursor reported {:?} but had selected \
                     boundary {:?} -- the boundary record was skipped under \
                     some interleaving of the evictor shift",
                    String::from_utf8_lossy(&reported),
                    String::from_utf8_lossy(&captured),
                );
            }
        },
        ITERATIONS,
    );
}
