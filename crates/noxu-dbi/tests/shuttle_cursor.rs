// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation gate for `CursorImpl` reposition vs a
//! concurrent BIN split (DST cursor coverage).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg:
//!
//!   * the cursor's `db_impl` RwLock resolves (through
//!     `noxu_util::dst_sync_pl`, routed in `noxu-dbi/src/cursor_impl.rs`) to a
//!     shuttle-instrumented lock, and
//!   * the tree node latch is *already* seamed (noxu-tree routes
//!     `NodeRwLock` through the same seam, with the hand-over-hand
//!     `read_arc()` descent backed by `noxu_latch::dst_arc_guard`),
//!
//! so shuttle's scheduler explores the interleavings of a cursor
//! stepping/repositioning against an insert that splits the BIN under it — on
//! the *real* `CursorImpl` / `Tree` code, not a re-implementation.
//!
//! # The class of bug this closes
//!
//! This mirrors the sequential regression tests
//! `test_cc1_cursor_repositioned_after_bin_split_upper_half` /
//! `test_cc1_cursor_stays_in_old_bin_after_split` (in `cursor_impl.rs`), but
//! *concurrently*: the split now happens on another thread while the cursor is
//! mid-step, so the CC-1 split-adjustment in `retrieve_next` (the `stale_split`
//! re-anchor: `current_index >= bin.entries.len()` OR the key at
//! `current_index` no longer matches `current_key`) is exercised under every
//! interleaving of the drop→reacquire window that the BIN-split bug
//! (`bug-bin-split-concurrency.md`) hid in.  It is the same class of
//! interleaving bug DST could not previously reach in the cursor layer.
//!
//! # Invariants (JE `CursorImpl` reposition semantics; mapped to `noxu-spec`
//! `btree_latching`)
//!
//!   * **no-panic** — no schedule panics (index-out-of-bounds on a stale
//!     `current_index`, or a torn read of a splitting BIN).
//!   * **position-valid** — after the race the cursor is still on a coherent
//!     slot: `get_current()` either returns its key or the scan has ended; it
//!     never returns garbage.
//!   * **no-skip / no-double-return** (`btree_latching::NoLostWrites`) — a
//!     full forward scan from the cursor's start position, taken across the
//!     concurrent split, returns each still-live key that is `>= start` at
//!     most once and skips none of them.  A split that migrated the cursor's
//!     slot to the new sibling must NOT cause `get_next_bin` to jump over the
//!     sibling (the exact CC-1(i) skip the sequential test guards).
//!   * **no lost wakeup** — both threads complete (the join gate would hang
//!     shuttle otherwise).
//!
//! # Not vacuous
//!
//! [`stepping_cursor_vs_split_no_skip`] asserts the scanned key set equals the
//! full expected live tail.  If the CC-1 re-anchor in `retrieve_next` were
//! removed (so a split-migrated cursor kept its stale `current_bin_arc` and
//! `get_next_bin` skipped the new sibling), the collected set would be MISSING
//! the migrated keys and the set-equality assert would fire — see the
//! `revert-to-prove` note below.
//!
//! To prove non-vacuous locally, in `cursor_impl.rs::retrieve_next` force the
//! `stale_split` branch off (`let stale_split = false;`) and re-run under
//! `--cfg noxu_shuttle`: shuttle finds a schedule where the split runs before
//! the cursor's step and the scan misses "03"/"04", failing the
//! set-equality assert. Restore the re-anchor and every schedule passes.
//! (Documented, not run automatically, so the gate stays green in CI while the
//! reversal proof remains a one-edit check — the same convention as
//! `shuttle_bin_split.rs`.)
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-dbi --test shuttle_cursor
//! ```
#![cfg(noxu_shuttle)]

use std::collections::BTreeSet;

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DatabaseId, DatabaseImpl, DbType, GetMode,
    OperationStatus, PutMode, SearchMode,
};
use noxu_util::dst_sync_pl::RwLock;
use shuttle::sync::Arc;

/// Number of interleavings shuttle explores per test.  The cursor step +
/// split descent is a deep schedule, so fewer iterations than a flat lock gate.
const ITERATIONS: usize = 1_500;

/// Build a small-fanout (max 4 entries/node) database with keys "00".."03"
/// filling exactly one BIN to capacity — the pre-split state the CC-1
/// sequential tests use.  Single-threaded, uncontended setup (fine on
/// shuttle's cooperative executor).
fn build_full_bin_db(id: i64) -> Arc<RwLock<DatabaseImpl>> {
    let db_id = DatabaseId::new(id);
    let mut config = DatabaseConfig::default();
    config.set_node_max_entries(4);
    let db_impl = DatabaseImpl::new(
        db_id,
        format!("cursor_dst_{id}"),
        DbType::User,
        &config,
    );
    let db = Arc::new(RwLock::new(db_impl));
    {
        let mut c = CursorImpl::new(db.clone(), 1);
        for i in 0u32..4 {
            let key = format!("{i:02}").into_bytes();
            c.put(&key, b"v", PutMode::Overwrite)
                .expect("setup insert must succeed");
        }
    }
    db
}

/// no-skip / no-double-return + position-valid + no-panic: a cursor positioned
/// at "02" (upper half of the full 4-entry BIN) steps forward while another
/// thread inserts "04", which splits the BIN (left=[00,01], right=[02,03],
/// then "04" lands in the right sibling).
///
/// The invariant is scan-completeness, which holds regardless of interleave
/// order: after both threads join, continuing the cursor's forward scan to the
/// end must have visited exactly the still-live keys strictly greater than the
/// start key "02" — i.e. {"03","04"} — each once, none skipped, none repeated.
/// (If the split ran after the step, the cursor already advanced to "03" on its
/// concurrent step; if before, the CC-1 re-anchor repositions it into the new
/// sibling.  Either way the *union* of keys the cursor returns from its start
/// must be the complete tail.)
#[test]
fn stepping_cursor_vs_split_no_skip() {
    shuttle::check_random(
        || {
            let db = build_full_bin_db(201);

            // The stepping cursor, positioned at "02".
            let mut cursor = CursorImpl::new(db.clone(), 2);
            let status =
                cursor.search(b"02", Some(b"v"), SearchMode::Set).unwrap();
            assert_eq!(status, OperationStatus::Success, "search 02 failed");
            assert_eq!(cursor.get_current_key(), Some(b"02".as_slice()));

            // Thread B: insert "04" — triggers split of the full BIN.
            let inserter = {
                let db = db.clone();
                shuttle::thread::spawn(move || {
                    let mut c = CursorImpl::new(db, 3);
                    c.put(b"04", b"v", PutMode::Overwrite)
                        .expect("concurrent split-insert must succeed");
                })
            };

            // Thread A (this thread): step once concurrently with the split,
            // collecting whatever key the step lands on, then drain the rest of
            // the scan.  Collect into a set for the completeness check.
            let mut visited: BTreeSet<Vec<u8>> = BTreeSet::new();
            // First concurrent step (races the split).
            if cursor.retrieve_next(GetMode::Next).unwrap()
                == OperationStatus::Success
            {
                // position-valid: get_current must succeed on a live slot.
                let (k, _v) = cursor.get_current().unwrap();
                assert!(visited.insert(k), "no-double-return: repeated key");
            }

            // Ensure the split has completed before draining the tail so the
            // final expected set is deterministic.
            inserter.join().unwrap();

            // Drain the remainder of the forward scan.
            while cursor.retrieve_next(GetMode::Next).unwrap()
                == OperationStatus::Success
            {
                let (k, _v) = cursor.get_current().unwrap();
                assert!(
                    visited.insert(k),
                    "no-double-return: a key was returned twice by the scan"
                );
            }

            // no-skip: the scan from "02" must have visited exactly the live
            // tail {"03","04"}.  Missing "03" or "04" ⇒ the split migrated the
            // slot and the cursor skipped the new sibling (the CC-1(i) bug).
            let expected: BTreeSet<Vec<u8>> =
                [b"03".to_vec(), b"04".to_vec()].into_iter().collect();
            assert_eq!(
                visited, expected,
                "cursor scan after concurrent split visited {visited:?}, \
                 expected {expected:?} (skipped or duplicated a live key)"
            );
        },
        ITERATIONS,
    );
}

/// position-valid across the lower-half case: a cursor at "01" (index 1 —
/// stays in the OLD BIN after the split) steps forward while "04" is inserted
/// concurrently.  After the race the forward scan from "01" must visit exactly
/// {"02","03","04"} — the split must not drop the migrated entries even though
/// the cursor itself stayed put.  Mirrors
/// `test_cc1_cursor_stays_in_old_bin_after_split`, concurrently.
#[test]
fn stepping_cursor_lower_half_vs_split_complete_scan() {
    shuttle::check_random(
        || {
            let db = build_full_bin_db(202);

            let mut cursor = CursorImpl::new(db.clone(), 2);
            cursor.search(b"01", Some(b"v"), SearchMode::Set).unwrap();
            assert_eq!(cursor.get_current_key(), Some(b"01".as_slice()));

            let inserter = {
                let db = db.clone();
                shuttle::thread::spawn(move || {
                    let mut c = CursorImpl::new(db, 3);
                    c.put(b"04", b"v", PutMode::Overwrite)
                        .expect("concurrent split-insert must succeed");
                })
            };

            let mut visited: BTreeSet<Vec<u8>> = BTreeSet::new();
            if cursor.retrieve_next(GetMode::Next).unwrap()
                == OperationStatus::Success
            {
                let (k, _v) = cursor.get_current().unwrap();
                assert!(visited.insert(k), "no-double-return: repeated key");
            }
            inserter.join().unwrap();
            while cursor.retrieve_next(GetMode::Next).unwrap()
                == OperationStatus::Success
            {
                let (k, _v) = cursor.get_current().unwrap();
                assert!(
                    visited.insert(k),
                    "no-double-return: a key was returned twice"
                );
            }

            let expected: BTreeSet<Vec<u8>> =
                [b"02".to_vec(), b"03".to_vec(), b"04".to_vec()]
                    .into_iter()
                    .collect();
            assert_eq!(
                visited, expected,
                "lower-half cursor scan after concurrent split visited \
                 {visited:?}, expected {expected:?}"
            );
        },
        ITERATIONS,
    );
}

/// no-panic + position-valid under a read-only reader racing the split: a
/// second cursor re-searches for a key ("02") while the split is in flight.
/// The search descent (root→leaf through the seamed node latches) must never
/// panic and must always find the present key — latch coupling means a search
/// cannot fall into the wrong half of a concurrent split.  This is the cursor
/// analogue of `shuttle_bin_split::search_racing_insert_finds_present_key`.
#[test]
fn searching_cursor_vs_split_finds_present_key() {
    shuttle::check_random(
        || {
            let db = build_full_bin_db(203);

            let searcher = {
                let db = db.clone();
                shuttle::thread::spawn(move || {
                    let mut c = CursorImpl::new(db, 4);
                    // "02" is present before and after the split (it moves to
                    // the new sibling, but a fresh search must still find it).
                    let s =
                        c.search(b"02", Some(b"v"), SearchMode::Set).unwrap();
                    assert_eq!(
                        s,
                        OperationStatus::Success,
                        "present key 02 not found during concurrent split"
                    );
                    assert_eq!(c.get_current_key(), Some(b"02".as_slice()));
                })
            };
            let inserter = {
                let db = db.clone();
                shuttle::thread::spawn(move || {
                    let mut c = CursorImpl::new(db, 3);
                    c.put(b"04", b"v", PutMode::Overwrite)
                        .expect("concurrent split-insert must succeed");
                })
            };

            searcher.join().unwrap();
            inserter.join().unwrap();
        },
        ITERATIONS,
    );
}
