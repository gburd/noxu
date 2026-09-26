// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! NEW-9 (leak fix) — the cross-BIN cursor advance must NOT leak the
//! descent pin (`cursor_count`) on the `lock_ln` error path.
//!
//! # The defect (regression introduced by the NEW-9 pin fix)
//!
//! `CursorImpl::retrieve_next`'s non-dup cross-BIN branch pins the next BIN
//! (`get_next_bin_pinned` → `cursor_count += 1`) BEFORE calling
//! `self.lock_ln(lsn)?`.  `lock_ln` can return `Err` (a lock-timeout /
//! deadlock / `RangeRestart` when the boundary record is write-locked by
//! another transaction).  Pre-fix, that `?` dropped the local `pinned_arc`
//! as a plain `Arc` WITHOUT decrementing `cursor_count`: the crossed-into
//! BIN was left permanently pinned (`cursor_count > 0`), so the evictor's
//! `detach_node_by_id` / `strip_lns_from_node` / `evict_root` guards would
//! refuse to ever evict it — a durable evictor wedge.
//!
//! The fix moves the descent pin into an RAII `PinnedBinGuard` that unpins on
//! drop on every early-return / `?` between the pin and the
//! `update_bin_pin_prepinned` install, and disarms only on the success path.
//!
//! # Why this test is deterministic (not a timing flake)
//!
//! It rides the record write lock, not a sleep:
//!
//!  1. Writer txn `W` updates the BOUNDARY record (the first live key of the
//!     SECOND BIN) in place and does NOT commit — it holds the WRITE lock on
//!     that record for the whole test.
//!  2. Reader txn `R` (short `lock_timeout`) positions its cursor on the LAST
//!     key of the FIRST BIN, then `retrieve_next(Next)` crosses into the
//!     second BIN: it pins the second BIN, then `lock_ln` on the boundary
//!     record BLOCKS on `W`'s write lock and TIMES OUT → `retrieve_next`
//!     returns `Err`.
//!  3. After that error, the second BIN's `cursor_count` MUST be back to 0
//!     (the descent pin was released) so the evictor is not wedged off it.
//!
//! On the base branch tip (`7b717f0e`, pre-leak-fix) step 3 FAILS
//! (`cursor_count == 1`, leaked).  With the RAII guard it PASSES
//! (`cursor_count == 0`).

#![cfg(not(noxu_shuttle))]

use std::sync::{Arc, Mutex};

use noxu_dbi::{
    CursorImpl, DatabaseConfig, EnvironmentImpl, GetMode, OperationStatus,
    PutMode, SearchMode,
};
use tempfile::TempDir;

/// Small node fan-out so a handful of ascending keys span >= 2 BINs.
const NODE_MAX: i32 = 4;
const N_KEYS: u32 = 16;

fn ikey(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

#[test]
fn cross_bin_advance_lock_ln_error_does_not_leak_pin() {
    let dir = TempDir::new().unwrap();
    let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();

    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);
    cfg.set_node_max_entries(NODE_MAX);
    let db = env.open_database("new9_leak", &cfg).unwrap();

    // Seed ascending keys (auto-commit) so the tree splits into >= 2 BINs.
    {
        let mut c = CursorImpl::new(Arc::clone(&db), 1);
        for i in 0..N_KEYS {
            let k = ikey(i);
            c.put(&k, &k, PutMode::Overwrite).unwrap();
        }
    }

    // Find a BIN boundary: the first adjacent seeded-key pair (in ascending
    // order) that lands in two DIFFERENT BINs.  `bin1_last` is the last key of
    // the first BIN; `boundary` is the first key of the second BIN.
    let (bin1_last, boundary) = {
        let guard = db.read();
        let tree = guard.get_real_tree().expect("real tree");
        let keys: Vec<Vec<u8>> = (0..N_KEYS).map(ikey).collect();
        let mut found = None;
        for pair in keys.windows(2) {
            let a = &pair[0];
            let b = &pair[1];
            let ida = tree.bin_node_id_for_key(a).expect("a in a BIN");
            let idb = tree.bin_node_id_for_key(b).expect("b in a BIN");
            if ida != idb {
                found = Some((a.clone(), b.clone()));
                break;
            }
        }
        found.expect(
            "test setup: keys must span >= 2 BINs (increase N_KEYS / lower              NODE_MAX)",
        )
    };

    // Writer txn W: update the boundary record in place and hold the write
    // lock (do NOT commit / do NOT drop) for the rest of the test.
    let w_txn = env.begin_txn().unwrap();
    let mut w_cursor = CursorImpl::new(Arc::clone(&db), w_txn.id_as_locker())
        .with_txn(Arc::new(Mutex::new(w_txn)));
    assert_eq!(
        w_cursor.put(&boundary, b"LOCKED", PutMode::Overwrite).unwrap(),
        OperationStatus::Success,
        "writer must lock the boundary record"
    );

    // Reader txn R: short lock timeout so the blocking lock_ln on the
    // write-locked boundary record returns an error instead of hanging.
    let mut r_txn = env.begin_txn().unwrap();
    r_txn.set_lock_timeout(200);
    let mut r_cursor = CursorImpl::new(Arc::clone(&db), r_txn.id_as_locker())
        .with_txn(Arc::new(Mutex::new(r_txn)));

    // Position R on the LAST key of the FIRST BIN.
    assert_eq!(
        r_cursor.search(&bin1_last, None, SearchMode::Set).unwrap(),
        OperationStatus::Success,
        "reader must position on the first BIN's last key"
    );
    assert_eq!(r_cursor.get_current_key(), Some(&bin1_last[..]));

    eprintln!(
        "DIAG bin1_last={:?} boundary={:?}",
        String::from_utf8_lossy(&bin1_last),
        String::from_utf8_lossy(&boundary)
    );
    {
        let guard = db.read();
        let tree = guard.get_real_tree().expect("real tree");
        eprintln!(
            "DIAG boundary_bin_cursor_count(before)={:?} bin1_id={:?} boundary_id={:?}",
            tree.bin_cursor_count_for_key(&boundary),
            tree.bin_node_id_for_key(&bin1_last),
            tree.bin_node_id_for_key(&boundary),
        );
    }
    {
        let lsn = {
            let guard = db.read();
            let tree = guard.get_real_tree().expect("real tree");
            tree.search_with_data(&boundary).expect("boundary slot").lsn
        };
        let lm = env.get_lock_manager();
        let (owners, waiters) = lm.get_lock_info(lsn);
        eprintln!(
            "DIAG boundary current slot lsn={lsn} owners={owners} waiters={waiters}"
        );
    }
    // Independent reader with short timeout must BLOCK/ERROR on the boundary
    // record if W truly holds the write lock: prove the lock is held.
    {
        let mut probe_txn = env.begin_txn().unwrap();
        probe_txn.set_lock_timeout(200);
        let mut probe = CursorImpl::new(Arc::clone(&db), probe_txn.id_as_locker())
            .with_txn(Arc::new(Mutex::new(probe_txn)));
        let pr = probe.search(&boundary, None, SearchMode::Set);
        eprintln!("DIAG direct search of boundary under W's write lock => {pr:?}");
    }
    // Cross into the SECOND BIN: pins it, then lock_ln on the write-locked
    // boundary record times out => Err.  The pin MUST be released on that
    // error path.
    let res = r_cursor.retrieve_next(GetMode::Next);
    assert!(
        res.is_err(),
        "the cross-BIN lock_ln on the write-locked boundary record must \
         error (lock timeout); got {res:?}"
    );

    // The crux: the second BIN's descent pin must be released.  On the
    // pre-fix branch tip this is 1 (leaked); with the RAII guard it is 0.
    let leaked = {
        let guard = db.read();
        let tree = guard.get_real_tree().expect("real tree");
        tree.bin_cursor_count_for_key(&boundary)
            .expect("boundary must resolve to a BIN")
    };
    assert_eq!(
        leaked, 0,
        "NEW-9 leak: the cross-BIN descent pin on the second BIN was NOT \
         released on the lock_ln error path (cursor_count={leaked}); the \
         evictor would be wedged off this BIN forever"
    );

    // Keep the writer alive until here so its write lock persisted across the
    // reader's failed advance.
    drop(w_cursor);
}
