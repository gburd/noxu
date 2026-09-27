// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation gate for NEW-DEL-RACE-1: N transactions
//! concurrently deleting the SAME single record must serialize so that
//! EXACTLY ONE wins.
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg:
//!
//!   * the cursor's `db_impl` RwLock resolves (through
//!     `noxu_util::dst_sync_pl`, routed in `noxu-dbi/src/cursor_impl.rs`) to a
//!     shuttle-instrumented lock, and
//!   * the `LockManager`'s shard-table `Mutex` and per-waiter grant `Condvar`
//!     resolve through the same seam (as `shuttle_lock_manager.rs` exercises),
//!
//! so shuttle's scheduler explores the acquire / wait / grant interleavings of
//! the *real* `CursorImpl::delete` + `Txn` + `LockManager` write-lock path —
//! not a re-implementation.
//!
//! # The bug this closes (NEW-DEL-RACE-1)
//!
//! `CursorImpl::delete` used to check the deleted-state (D3) BEFORE acquiring
//! the write lock, then `lock_write_before_log` BLOCKED until a concurrent
//! deleter committed, then UNCONDITIONALLY logged a second DeleteLN + re-applied
//! the tree delete and reported success.  With N racing deleters, 6-7 of 8 each
//! observed the record LIVE and committed a delete.  The fix (JE-faithful
//! `CursorImpl.deleteCurrentRecord`'s post-`lockLN` `!lockStanding.recordExists()`
//! revert) re-reads the current committed slot AFTER the write lock is acquired;
//! if the record is gone (`NULL_LSN`) or its LSN changed (a concurrent commit
//! removed it), it returns `KeyEmpty` without a second log/apply.
//!
//! # Invariants (JE record-level-locking atomicity; mapped to `noxu-spec`
//! `lock_manager_deadlock` WriteLocksExclusive)
//!
//!   * **exactly-one-winner** — across every interleaving, EXACTLY ONE deleter
//!     returns `Success`.  This is the record-level-locking atomicity guarantee
//!     that NEW-DEL-RACE-1 violated.
//!   * **no-double-delete / no-lost-delete** — every other deleter returns
//!     `KeyEmpty` (read-committed: it waited for the winner, re-read the slot,
//!     found it gone) or loses a lock conflict (`Err`, serializable
//!     read→write upgrade cycle victim).  Never `Success`.  After the race the
//!     record is physically gone from the tree exactly once.
//!   * **no lost wakeup** — every deleter thread completes (the join gate would
//!     hang shuttle otherwise); a waiter blocked on the winner's write lock is
//!     always granted once the winner commits.
//!
//! # Not vacuous
//!
//! [`concurrent_deleters_exactly_one_wins_read_committed`] asserts the winner
//! count is EXACTLY 1.  If the revalidate-after-lock in `CursorImpl::delete`
//! were removed (revert the NEW-DEL-RACE-1 fix), shuttle finds a schedule where
//! a waiter acquires the write lock after the winner committed, re-applies the
//! delete, and returns `Success` — the winner count becomes > 1 and the assert
//! fires.  To prove non-vacuous locally, in `cursor_impl.rs::delete` delete the
//! `current_lsn == NULL_LSN || current_lsn != old_lsn => KeyEmpty` early return
//! and re-run under `--cfg noxu_shuttle`: the exactly-one assert fails.
//! (Documented, not run automatically — the same convention as
//! `shuttle_cursor.rs`.)
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-dbi --test shuttle_delete_race
//! ```
#![cfg(noxu_shuttle)]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DatabaseId, DatabaseImpl, DbType,
    OperationStatus, SearchMode,
};
use std::time::Duration;

use noxu_txn::{LockManager, Locker, TxnManager};
use noxu_util::dst_sync_pl::RwLock;
use noxu_util::dst_sync_pl::{advance_and_fire, install_sim_clock};
use noxu_util::{Lsn, SimClock};
use shuttle::sync::Arc;
use shuttle::sync::atomic::{AtomicUsize, Ordering};

/// Number of interleavings shuttle explores per test.  The search + write-lock
/// wait + revalidate + commit is a deep schedule, so a moderate count.
const ITERATIONS: usize = 1_000;

/// Number of concurrent deleters.  Kept small (shuttle cost grows fast with
/// thread count); 3 is enough to exercise winner + ≥2 waiters that each must
/// re-read the slot after the winner commits.
const DELETERS: usize = 3;

/// The single record every thread races to delete.
const KEY: &[u8] = b"target";

/// Build a one-record database seeded with a REAL (non-NULL) slot LSN so the
/// delete path takes the LSN-keyed write-lock branch (`lock_write_before_log`
/// on the real `old_lsn`), the exact path NEW-DEL-RACE-1 lived in.  A NULL slot
/// LSN would instead route through the synthetic-key coordination path and not
/// exercise the revalidate-after-lock branch.  Single-threaded uncontended
/// setup, fine on shuttle's cooperative executor.
fn build_single_record_db(id: i64) -> Arc<RwLock<DatabaseImpl>> {
    let db_id = DatabaseId::new(id);
    let config = DatabaseConfig::default();
    let db_impl = DatabaseImpl::new(
        db_id,
        format!("delrace_{id}"),
        DbType::User,
        &config,
    );
    let db = Arc::new(RwLock::new(db_impl));
    {
        let dbi = db.read();
        let tree = dbi.get_real_tree().expect("real tree");
        // A concrete non-NULL LSN; the value is arbitrary but must be stable so
        // every searcher observes the same `old_lsn` pre-lock (the pre-fix race
        // window: all N find the same live LSN).
        tree.insert(KEY.to_vec(), b"v".to_vec(), Lsn::new(1, 100))
            .expect("seed insert");
        dbi.increment_entry_count();
    }
    db
}

/// Runs `DELETERS` threads, each in its own txn, all racing to delete `KEY`,
/// then asserts exactly-one-winner.  `serializable` selects the isolation
/// level of each deleter txn.
fn run_delete_race(env_id: i64, serializable: bool) {
    // A SimClock is required for the lock manager's `wait_for` deadline source
    // (`install_sim_clock() must be called before wait_for`).  timeout_ms = 0
    // (wait forever): a loser waits on the winner's write lock and is woken by
    // the winner's commit-release notify — no lock-timeout preempts it (which
    // would otherwise turn a clean KeyEmpty loser into a spurious timeout).
    let sim = Arc::new(SimClock::new(0));
    install_sim_clock(Arc::clone(&sim));

    let db = build_single_record_db(env_id);
    let lm = Arc::new(LockManager::with_config_clock(
        0, // wait forever
        1, // single shard: dense interleaving on the one LSN
        Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
    ));
    let mgr = Arc::new(TxnManager::new(lm.clone()));

    let success = Arc::new(AtomicUsize::new(0));
    let key_empty = Arc::new(AtomicUsize::new(0));
    let conflict = Arc::new(AtomicUsize::new(0));
    // Threads still running: the clock driver advances the SimClock until all
    // deleters finish, as a lost-wakeup safety net for any interleaving that
    // parks a waiter on the 50 ms deadlock re-detection slice before the
    // winner's release notify lands.
    let running = Arc::new(AtomicUsize::new(DELETERS));

    let mut handles = Vec::with_capacity(DELETERS);
    for _ in 0..DELETERS {
        let db = Arc::clone(&db);
        let lm = Arc::clone(&lm);
        let mgr = Arc::clone(&mgr);
        let success = Arc::clone(&success);
        let key_empty = Arc::clone(&key_empty);
        let conflict = Arc::clone(&conflict);
        let running = Arc::clone(&running);
        handles.push(shuttle::thread::spawn(move || {
            let mut txn = mgr.begin_txn();
            // Wait forever on lock contention (no timeout preemption).
            txn.set_lock_timeout(0);
            if serializable {
                txn.set_serializable_isolation(true);
            } else {
                // Read-committed: read locks release immediately so the
                // subsequent write-lock upgrade in `delete` does not contend
                // on the search read lock — the loser cleanly waits on the
                // winner's write lock, re-reads, and gets KeyEmpty.
                txn.set_read_committed_isolation(true);
            }
            let txn_id = txn.id();
            // `with_txn` takes a std::sync::Mutex<Txn> (per the field doc: the
            // txn_ref mutex is intentionally NOT on the shuttle seam because it
            // is per-cursor uncontended; the cross-thread coordination is in
            // the seamed LockManager).
            let txn_arc = std::sync::Arc::new(std::sync::Mutex::new(txn));

            let mut cursor = CursorImpl::new(db.clone(), txn_id)
                .with_lock_manager(lm.clone())
                .with_txn(txn_arc.clone())
                .with_txn_manager(mgr.clone());

            // Position on the live record (read lock via lock_ln).
            let s = cursor
                .search(KEY, None, SearchMode::Set)
                .expect("search must not error");
            if s != OperationStatus::Success {
                // A prior deleter physically removed the slot before this
                // thread searched — it never had the record, so it is a
                // non-winner.  Count as key-empty (the loser outcome).
                key_empty.fetch_add(1, Ordering::SeqCst);
                mgr.commit_txn(txn_id);
                let _ = txn_arc.lock().unwrap().commit();
                running.fetch_sub(1, Ordering::SeqCst);
                return;
            }

            match cursor.delete() {
                Ok(OperationStatus::Success) => {
                    success.fetch_add(1, Ordering::SeqCst);
                    // Winner commits, releasing the write lock so a waiter
                    // unblocks, re-reads, and observes the record gone.
                    let _ = txn_arc.lock().unwrap().commit();
                    mgr.commit_txn(txn_id);
                }
                Ok(_) => {
                    // KeyEmpty: revalidate-after-lock caught a concurrent
                    // committed delete — the loser outcome we want.
                    key_empty.fetch_add(1, Ordering::SeqCst);
                    let _ = txn_arc.lock().unwrap().commit();
                    mgr.commit_txn(txn_id);
                }
                Err(_) => {
                    // Lock conflict (serializable read→write upgrade cycle
                    // victim).  Also a non-winner; abort releases its locks.
                    conflict.fetch_add(1, Ordering::SeqCst);
                    let _ = txn_arc.lock().unwrap().abort();
                    mgr.abort_txn(txn_id);
                }
            }
            running.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    // Clock driver: advance the SimClock while any deleter is still running so
    // a waiter parked on the 50 ms re-detection slice is never orphaned (no
    // lost wakeup).  Bounded step count guards against a genuine hang.
    let driver = {
        let sim = Arc::clone(&sim);
        let running = Arc::clone(&running);
        shuttle::thread::spawn(move || {
            let mut steps = 0;
            while running.load(Ordering::SeqCst) > 0 {
                advance_and_fire(&sim, Duration::from_millis(60));
                shuttle::thread::yield_now();
                steps += 1;
                assert!(steps < 5000, "deleters never finished (lost wakeup)");
            }
        })
    };

    for h in handles {
        // no lost wakeup: every thread must complete.
        h.join().expect("deleter thread must not panic");
    }
    driver.join().expect("clock driver must not panic");

    let s = success.load(Ordering::SeqCst);
    let ke = key_empty.load(Ordering::SeqCst);
    let cf = conflict.load(Ordering::SeqCst);

    // exactly-one-winner: the record-level-locking atomicity guarantee.
    assert_eq!(
        s, 1,
        "NEW-DEL-RACE-1: exactly one deleter must win; got success={s} \
         key_empty={ke} conflict={cf} (serializable={serializable})"
    );
    // no-double-delete / no-lost-delete: every other deleter is a non-winner.
    assert_eq!(
        s + ke + cf,
        DELETERS,
        "every deleter must be accounted for exactly once (success={s} \
         key_empty={ke} conflict={cf})"
    );

    // The record is physically gone from the tree exactly once.
    let dbi = db.read();
    let tree = dbi.get_real_tree().expect("real tree");
    assert!(
        tree.search(KEY).map(|r| !r.exact_parent_found).unwrap_or(true),
        "the raced record must be gone after exactly one delete"
    );
}

/// exactly-one-winner under read-committed (default-equivalent) isolation.
/// Losers cleanly wait on the winner's write lock, re-read the slot, and get
/// `KeyEmpty` — the direct expression of the revalidate-after-lock fix.
#[test]
fn concurrent_deleters_exactly_one_wins_read_committed() {
    shuttle::check_random(|| run_delete_race(301, false), ITERATIONS);
}

/// exactly-one-winner under SERIALIZABLE isolation.  Read locks are held to
/// commit, so a read→write upgrade may form an upgrade cycle whose victim
/// aborts (`Err`) — still a non-winner.  Exactly one deleter wins regardless.
#[test]
fn concurrent_deleters_exactly_one_wins_serializable() {
    shuttle::check_random(|| run_delete_race(302, true), ITERATIONS);
}
