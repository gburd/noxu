// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! C1 / V1 — READ_COMMITTED post-lock slot revalidation on the UNGUARDED
//! cursor read paths (`get_first` / `retrieve_next` scans).
//!
//! # The defect
//!
//! Every `CursorImpl` read prefetches the target BIN slot's `(key, data, lsn)`
//! under one BIN read latch, drops the latch, then acquires the record lock
//! (`lock_ln`).  A writer that mutates the slot inside that prefetch→lock
//! window leaves the prefetched bytes stale.  The post-lock LSN re-check that
//! closes the window (`slot_lsn_changed` / `revalidate_locked_slot`) was
//! originally applied to only the two `search()` arms (Set/Both, SetRange).
//! `get_first`/`get_last`/`search_dup`/`retrieve_next`/the dup-filter accept
//! sites were left unguarded, so a READ_COMMITTED table scan could still return
//! a torn/dirty read — including an *aborting* writer's uncommitted value.
//!
//! # JE parity
//!
//! JE holds the BIN latch across `CursorImpl.lockLN`, which unlatches to block
//! and re-latches to re-read `bin.getLsn(index)`, reverting+retrying the lock
//! if the LSN changed while unlatched (CursorImpl.java:3641-3680).  The
//! subsequent `getCurrent`/`fetchLN(index)` (CursorImpl.java:2230/2294) then
//! derives the returned data from the *current* latched slot, never from a
//! value captured before the lock.  Noxu's equivalent is the post-lock
//! `revalidate_locked_slot` re-check on every data-returning read site.
//!
//! # Why this test is deterministic (not a timing flake)
//!
//! The reproduction rides the record write-lock, not a sleep:
//!
//!  1. Writer txn W updates key K "AAAA"→"BBBB" in place: the slot now holds
//!     BBBB at a NEW lsn, and W holds the WRITE lock on that new lsn.  This
//!     call returns fully before the reader is released (a `Barrier`).
//!  2. Reader txn R (READ_COMMITTED) runs `get(Get::First)`.  Because W's put
//!     already completed (barrier), R's prefetch reads the uncommitted BBBB at
//!     the new lsn.  R then tries to `lock_ln(new_lsn)` — which R either
//!     acquires immediately (W already aborted) or blocks on (W still holds
//!     it); both orderings converge to the same final tree state.
//!  3. Writer aborts: undo restores AAAA at the OLD lsn in the slot and
//!     releases the write lock.
//!
//! After the abort the slot authoritatively holds AAAA at old_lsn.  A correct
//! READ_COMMITTED read must NEVER surface the aborted BBBB.  On the base
//! (unguarded `get_first`) the cursor installs its stale prefetch → BBBB (the
//! bug).  With the post-lock revalidation the cursor observes the slot lsn
//! moved and re-derives AAAA.
//!
//! There is no assertion that depends on which thread wins the race; the
//! invariant ("never return the aborted value") holds under every interleaving
//! the `Barrier` permits.

use std::sync::{Arc, Barrier};
use std::thread;

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
    TransactionConfig,
};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Arc<noxu_db::Environment> {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    Arc::new(noxu_db::Environment::open(cfg).unwrap())
}

/// A READ_COMMITTED `get_first` scan must never surface an aborting writer's
/// uncommitted in-place update (the C1/V1 unguarded-path dirty read).
///
/// Runs many rounds so that across rounds both interleavings (reader blocks on
/// the write lock; reader acquires after the abort) are exercised.  A single
/// round is already deterministic in its *invariant*; the repetition just
/// widens interleaving coverage.
#[test]
fn get_first_read_committed_never_returns_aborted_inplace_update() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = Arc::new(env.open_database(None, "rc_getfirst", &db_cfg).unwrap());

    // Seed the single committed record K="AAAA".
    {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(
            &txn,
            &DatabaseEntry::from_bytes(b"K"),
            &DatabaseEntry::from_bytes(b"AAAA"),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    const ROUNDS: usize = 200;
    for round in 0..ROUNDS {
        let barrier = Arc::new(Barrier::new(2));

        // Writer thread: in-place update to the uncommitted BBBB, then abort.
        let w_env = Arc::clone(&env);
        let w_db = Arc::clone(&db);
        let w_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let txn = w_env.begin_transaction(None).unwrap();
            // In-place overwrite: installs BBBB at a NEW lsn in the slot and
            // holds the WRITE lock on that new lsn.
            w_db.put_in(
                &txn,
                &DatabaseEntry::from_bytes(b"K"),
                &DatabaseEntry::from_bytes(b"BBBB"),
            )
            .unwrap();
            // Release the reader only AFTER the uncommitted BBBB is in the
            // slot, so the reader's prefetch is guaranteed to capture it.
            w_barrier.wait();
            // Abort: undo restores AAAA at old_lsn and releases the write lock.
            txn.abort().unwrap();
        });

        // Reader thread: READ_COMMITTED get_first.
        let r_env = Arc::clone(&env);
        let r_db = Arc::clone(&db);
        let r_barrier = Arc::clone(&barrier);
        let reader = thread::spawn(move || -> Option<Vec<u8>> {
            // Wait until the writer has installed the uncommitted BBBB.
            r_barrier.wait();
            let rc = TransactionConfig::new().with_read_committed(true);
            let txn = r_env.begin_transaction(Some(&rc)).unwrap();
            let mut cursor = r_db.open_cursor_in(&txn, None).unwrap();
            let mut key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            let status =
                cursor.get(&mut key, &mut data, Get::First, None).unwrap();
            let observed = if status == OperationStatus::Success {
                data.data_opt().map(|d| d.to_vec())
            } else {
                None
            };
            drop(cursor);
            let _ = txn.commit();
            observed
        });

        writer.join().unwrap();
        let observed = reader.join().unwrap();

        if let Some(bytes) = observed {
            assert_ne!(
                bytes.as_slice(),
                b"BBBB",
                "C1/V1 round {round}: READ_COMMITTED get_first returned the \
                 aborting writer's uncommitted in-place value BBBB (dirty \
                 read on the unguarded scan path). Expected the committed \
                 AAAA (or NotFound if the abort's undo raced the read)."
            );
        }
    }

    // Final sanity: the committed state is AAAA.
    let final_val = db.get(b"K").unwrap();
    assert_eq!(
        final_val.as_deref(),
        Some(&b"AAAA"[..]),
        "after all rounds the committed value must be the seeded AAAA"
    );
}

/// Same dirty-read invariant, but on the `retrieve_next` scan path (Get::Next
/// from an uninitialized cursor positions at the first record via get_first,
/// so we seed TWO keys and step onto the SECOND with an explicit Get::Next to
/// drive the `retrieve_next` within-BIN accept site rather than get_first).
///
/// The concurrent writer aborts an in-place update to the second key; a
/// READ_COMMITTED forward scan must never surface the aborted value.
#[test]
fn retrieve_next_read_committed_never_returns_aborted_inplace_update() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = Arc::new(env.open_database(None, "rc_next", &db_cfg).unwrap());

    // Seed two committed records: K1="aaaa", K2="AAAA".  The scan steps K1 -> K2.
    {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(
            &txn,
            &DatabaseEntry::from_bytes(b"K1"),
            &DatabaseEntry::from_bytes(b"aaaa"),
        )
        .unwrap();
        db.put_in(
            &txn,
            &DatabaseEntry::from_bytes(b"K2"),
            &DatabaseEntry::from_bytes(b"AAAA"),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    const ROUNDS: usize = 200;
    for round in 0..ROUNDS {
        let barrier = Arc::new(Barrier::new(2));

        // Writer: in-place update K2 -> uncommitted BBBB, then abort.
        let w_env = Arc::clone(&env);
        let w_db = Arc::clone(&db);
        let w_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let txn = w_env.begin_transaction(None).unwrap();
            w_db.put_in(
                &txn,
                &DatabaseEntry::from_bytes(b"K2"),
                &DatabaseEntry::from_bytes(b"BBBB"),
            )
            .unwrap();
            w_barrier.wait();
            txn.abort().unwrap();
        });

        // Reader: READ_COMMITTED forward scan K1 -> K2 via Get::Next.
        let r_env = Arc::clone(&env);
        let r_db = Arc::clone(&db);
        let r_barrier = Arc::clone(&barrier);
        let reader = thread::spawn(move || -> Option<Vec<u8>> {
            r_barrier.wait();
            let rc = TransactionConfig::new().with_read_committed(true);
            let txn = r_env.begin_transaction(Some(&rc)).unwrap();
            let mut cursor = r_db.open_cursor_in(&txn, None).unwrap();
            let mut key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            // First Next positions at K1 (get_first), second Next steps to K2
            // via retrieve_next's within-BIN accept site (the unguarded path).
            cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
            let observed =
                if cursor.get(&mut key, &mut data, Get::Next, None).unwrap()
                    == OperationStatus::Success
                    && key.data_opt() == Some(&b"K2"[..])
                {
                    data.data_opt().map(|d| d.to_vec())
                } else {
                    None
                };
            drop(cursor);
            let _ = txn.commit();
            observed
        });

        writer.join().unwrap();
        let observed = reader.join().unwrap();

        if let Some(bytes) = observed {
            assert_ne!(
                bytes.as_slice(),
                b"BBBB",
                "C1/V1 round {round}: READ_COMMITTED retrieve_next returned the \
                 aborting writer's uncommitted in-place value BBBB (dirty \
                 read on the unguarded scan path). Expected committed AAAA."
            );
        }
    }

    assert_eq!(
        db.get(b"K2").unwrap().as_deref(),
        Some(&b"AAAA"[..]),
        "after all rounds K2's committed value must be the seeded AAAA"
    );
}
