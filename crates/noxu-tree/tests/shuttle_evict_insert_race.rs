// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation gate for NEW-10: the DURABLE loss of a
//! committed record when the background evictor DETACHES a BIN concurrently
//! with a foreground INSERT that routes to that BIN's parent slot.
//!
//! # The bug (root cause)
//!
//! Two eviction drivers exist — the background evictor daemon and foreground
//! `env.evict_memory()` — and each can DETACH a BIN: remove the cached child
//! `Arc` from its parent slot, leaving only the slot's key/LSN (the node is
//! re-fetchable from its `last_full_lsn`).  The live insert descent
//! (`Tree::insert_recursive_inner`) selected the target child with
//! `n.get_child(idx).ok_or(TreeError::SplitRequired)` — it did NOT re-fault a
//! non-resident child.  When the evictor had just detached the child, the
//! insert returned `Err(SplitRequired)`, which `CursorImpl::apply_tree_insert`
//! SILENTLY SWALLOWED (`if let Ok(is_new) = tree.insert(..)`).  The LN was
//! already logged to the WAL and the txn committed successfully, but the
//! record never entered the tree and was durably absent on reopen.
//!
//! Confirmed on the real engine with 100%-correlated instrumentation: the key
//! for which `tree.insert` returned `SplitRequired` is exactly the key that
//! `db.get` cannot find after close+reopen.  Control (run_evictor=false, no
//! concurrent detach) is loss-free.
//!
//! # The fix
//!
//! `insert_recursive_inner` now re-faults a detached child via
//! `Tree::child_at_or_fetch` (JE `IN.fetchTarget` faults a non-resident child
//! on a live descent) and re-descends, and `apply_tree_insert` no longer
//! swallows the result.  So a transiently-detached slot never drops a record.
//!
//! # What this gate models
//!
//! An ABSTRACT protocol model (like the repo's other specs) of a single BIN
//! slot shared by two drivers:
//!
//!   * `detach` = the evictor: take the resident BIN out of the parent slot,
//!     publish its keys to the durable store at a fresh LSN, leave the slot
//!     non-resident (child = None) but with a valid re-fetch LSN.
//!   * `insert` = the writer: route a committed key to the slot.  If the slot
//!     is resident, insert into it.  If it is non-resident, the writer must
//!     RE-FAULT the BIN from the store (the fix) before inserting — it must
//!     NOT drop the record.
//!
//! The two run on separate shuttle threads against a shuttle-instrumented
//! `Mutex`, so shuttle explores every detach/insert interleaving.
//!
//! Invariant (NO-LOST-COMMITTED-RECORD): after any interleaving, a key the
//! writer accepted is present — either resident in the slot's BIN or in the
//! durable store image the slot's LSN points at.  For ANY interleaving.
//!
//! # Not vacuous / the regression proof
//!
//! `buggy_insert_drops_detached_slot_record` runs the SAME race with the
//! writer modelling the OLD behaviour (drop the insert when the slot is
//! non-resident) and asserts, deterministically, that a record is lost — so
//! the gate is proven to actually exercise the losing interleaving, and the
//! `resilient_*` gates prove the fix removes it.
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-tree --test shuttle_evict_insert_race
//! ```
#![cfg(noxu_shuttle)]

use shuttle::sync::{Arc, Mutex};

/// Interleavings shuttle explores per randomized test.
const ITERATIONS: usize = 50_000;

/// A single BIN slot in a parent IN, plus the durable store the slot's LSN
/// points at.  Models exactly the state that matters for NEW-10: whether the
/// child is resident, what its resident keys are, and what image the slot LSN
/// would re-fault to.
struct Slot {
    /// The resident BIN's keys, or `None` if the child was detached
    /// (non-resident — only the LSN remains, re-fetchable from `store`).
    resident: Option<Vec<u64>>,
    /// The slot's on-disk LSN → the durable image (keys) at that LSN.
    /// `lsn == 0` means "never logged".
    lsn: u64,
    /// The durable store: lsn → the full set of keys logged at that lsn.
    store: std::collections::HashMap<u64, Vec<u64>>,
    /// Monotonic LSN allocator (models the WAL append point).
    next_lsn: u64,
}

impl Slot {
    fn new(initial_keys: Vec<u64>) -> Self {
        let mut store = std::collections::HashMap::new();
        // The BIN starts resident with a durable image already on disk
        // (last_full_lsn = 1), matching a BIN that has been logged once and
        // is eligible for eviction.
        store.insert(1u64, initial_keys.clone());
        Slot { resident: Some(initial_keys), lsn: 1, store, next_lsn: 2 }
    }

    /// Is key `k` recoverable — resident in the BIN, or in the durable image
    /// the slot LSN points at?  This is the point-get-after-reopen check.
    fn present(&self, k: u64) -> bool {
        if let Some(keys) = &self.resident {
            if keys.contains(&k) {
                return true;
            }
        }
        // Non-resident (or resident-but-absent): what would a refault read?
        if let Some(img) = self.store.get(&self.lsn) {
            return img.contains(&k);
        }
        false
    }
}

/// The evictor detaches the BIN: log its resident keys to a fresh LSN, publish
/// that LSN into the slot, and drop the resident child.  A no-op if the child
/// is already non-resident.  Mirrors `flush_dirty_node_to_log` +
/// `detach_node_by_id`.
fn detach(slot: &Mutex<Slot>) {
    let mut s = slot.lock().unwrap();
    if let Some(keys) = s.resident.take() {
        let lsn = s.next_lsn;
        s.next_lsn += 1;
        s.store.insert(lsn, keys);
        s.lsn = lsn;
        // s.resident is now None (child detached; slot keeps `lsn`).
    }
}

/// The FIXED writer: route committed key `k` into the slot.  If the child is
/// non-resident, RE-FAULT it from the store first (the NEW-10 fix —
/// `child_at_or_fetch` on a live insert descent), then insert.  Never drops
/// the record.
fn insert_resilient(slot: &Mutex<Slot>, k: u64) {
    let mut s = slot.lock().unwrap();
    if s.resident.is_none() {
        // Re-fault the BIN from its slot LSN (child_at_or_fetch), matching the
        // fix: the live insert descent faults a detached child in.
        let lsn = s.lsn;
        let img = s.store.get(&lsn).cloned().unwrap_or_default();
        s.resident = Some(img);
    }
    // Insert into the now-resident BIN.  The BIN is dirty until re-logged; a
    // subsequent detach will capture this key.
    if let Some(keys) = s.resident.as_mut() {
        if !keys.contains(&k) {
            keys.push(k);
        }
    }
}

/// The BUGGY writer (pre-fix behaviour): if the child is non-resident, the
/// insert descent returned `SplitRequired` and `apply_tree_insert` swallowed
/// it — the record is DROPPED.  Used only to prove the gate is not vacuous.
fn insert_buggy(slot: &Mutex<Slot>, k: u64) {
    let mut s = slot.lock().unwrap();
    match s.resident.as_mut() {
        Some(keys) => {
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
        None => {
            // SplitRequired swallowed: record dropped on the floor.
        }
    }
}

/// THE NO-LOST-COMMITTED-RECORD GATE: an insert racing a detach of the same
/// slot.  With the fix (`insert_resilient`), the committed key survives every
/// interleaving.
#[test]
fn resilient_insert_survives_concurrent_detach() {
    shuttle::check_random(
        || {
            // BIN starts resident with keys {10, 20, 30}.
            let slot = Arc::new(Mutex::new(Slot::new(vec![10, 20, 30])));
            // The committed key we insert concurrently with the detach.
            let win: u64 = 25;

            let evictor = {
                let slot = Arc::clone(&slot);
                shuttle::thread::spawn(move || detach(&slot))
            };
            let writer = {
                let slot = Arc::clone(&slot);
                shuttle::thread::spawn(move || insert_resilient(&slot, win))
            };
            evictor.join().unwrap();
            writer.join().unwrap();

            // A second detach may run after both (the daemon keeps sweeping);
            // the key must still be recoverable.  Run one more detach to force
            // the resident BIN (which now holds `win`) to a durable image.
            detach(&slot);

            let s = slot.lock().unwrap();
            assert!(
                s.present(win),
                "NEW-10: committed key {win} lost after detach⇄insert race — \
                 neither resident nor in the durable image at slot lsn {}",
                s.lsn
            );
        },
        ITERATIONS,
    );
}

/// Exhaustive (DFS) variant: shuttle explores EVERY interleaving of the two
/// threads plus the trailing detach, proving the fix has no losing schedule.
#[test]
fn resilient_insert_survives_concurrent_detach_dfs() {
    shuttle::check_dfs(
        || {
            let slot = Arc::new(Mutex::new(Slot::new(vec![10, 20, 30])));
            let win: u64 = 25;

            let evictor = {
                let slot = Arc::clone(&slot);
                shuttle::thread::spawn(move || detach(&slot))
            };
            let writer = {
                let slot = Arc::clone(&slot);
                shuttle::thread::spawn(move || insert_resilient(&slot, win))
            };
            evictor.join().unwrap();
            writer.join().unwrap();
            detach(&slot);

            let s = slot.lock().unwrap();
            assert!(
                s.present(win),
                "NEW-10 (DFS): committed key {win} lost after detach⇄insert \
                 race — slot lsn {}",
                s.lsn
            );
        },
        None,
    );
}

/// NOT-VACUOUS PROOF: the SAME race with the buggy writer (drops the insert
/// when the slot is non-resident) MUST be able to lose the record.  We assert
/// that at least one interleaving loses it, so the gate above is proven to
/// exercise the real losing schedule.
#[test]
fn buggy_insert_drops_detached_slot_record() {
    let lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lost_probe = Arc::clone(&lost);
    shuttle::check_random(
        move || {
            let slot = Arc::new(Mutex::new(Slot::new(vec![10, 20, 30])));
            let win: u64 = 25;

            let evictor = {
                let slot = Arc::clone(&slot);
                shuttle::thread::spawn(move || detach(&slot))
            };
            let writer = {
                let slot = Arc::clone(&slot);
                shuttle::thread::spawn(move || insert_buggy(&slot, win))
            };
            evictor.join().unwrap();
            writer.join().unwrap();
            detach(&slot);

            let s = slot.lock().unwrap();
            if !s.present(win) {
                lost_probe.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        },
        ITERATIONS,
    );
    assert!(
        lost.load(std::sync::atomic::Ordering::SeqCst),
        "expected the buggy writer to lose the record in at least one \
         interleaving — the gate would otherwise be vacuous"
    );
}
