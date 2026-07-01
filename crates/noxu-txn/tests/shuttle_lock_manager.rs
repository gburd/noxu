// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation tests for the `LockManager` deadlock
//! detection / grant path (DST wave 2).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg, the lock_manager's shard-table / waiter-graph `Mutex` and
//! the per-waiter grant `Condvar` resolve (through the parking_lot-over-shuttle
//! wrapper `noxu_util::dst_sync_pl`) to shuttle-instrumented primitives, so
//! shuttle's scheduler explores the acquire / wait / deadlock-detect / grant
//! interleavings of the *real* lock manager.
//!
//! # The 50 ms re-detection slice, driven by the SimClock
//!
//! The wait loop (`lock_with_timeout`) re-runs deadlock detection at most every
//! 50 ms via a `Condvar::wait_for(.., 50ms)` slice, and computes elapsed /
//! timeout from the injectable `Clock` (`LockManager::with_config_clock`, DST
//! M1.1).  shuttle's `wait_for` never self-expires; a driver thread advances a
//! shared `SimClock` with `advance_and_fire` so the slice fires
//! deterministically, letting the re-detection reach the cycle even when the
//! initial single-pass snapshot missed it (both waiters entering the wait path
//! at once).  The FsyncManager's clock and the wrapper's deadline clock are the
//! SAME `SimClock` instance so the elapsed math and the fired deadline agree.
//!
//! # Invariants (mapped to `noxu-spec` lock_manager_deadlock)
//!
//!   * **no-deadlock-undetected** — when two lockers form a wait-for cycle,
//!     the deadlock detector fires and exactly ONE of them is chosen as the
//!     victim (`TxnError::Deadlock`); the other proceeds.  Neither hangs.
//!   * **victim-consistency** — never do BOTH cycle members proceed (that
//!     would mean a granted lock that violates the compatibility matrix), and
//!     never do BOTH abort as victims (over-aborting).  Exactly one victim.
//!   * **WriteLocksExclusive / compatibility** — a granted write lock is never
//!     co-held with any other owner (checked via `is_owned_write_lock` after
//!     the survivor is granted).
//!   * **no lost wakeup on grant** — a waiter blocked on a held lock is always
//!     granted (and returns) once the holder releases; no waiter is orphaned.
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-txn --test shuttle_lock_manager
//! ```
#![cfg(noxu_shuttle)]

use std::sync::atomic::Ordering;
use std::time::Duration;

use noxu_txn::{LockGrantType, LockManager, LockType, TxnError};
use noxu_util::SimClock;
use noxu_util::dst_sync_pl::{advance_and_fire, install_sim_clock};
use shuttle::sync::Arc;
use shuttle::sync::atomic::{AtomicBool, AtomicUsize};

/// Number of interleavings shuttle explores per test.
const ITERATIONS: usize = 2_000;

/// no-deadlock-undetected + victim-consistency: two lockers form a classic
/// two-lock cycle; the detector must abort EXACTLY ONE and let the other
/// proceed, under every interleaving.  Neither may hang.
///
/// A: holds X, then waits for Y.  B: holds Y, then waits for X.  With the 50 ms
/// re-detection slice driven by the SimClock, the cycle is always detected even
/// when both threads enter the wait path in the same interleaving.
#[test]
fn deadlock_cycle_aborts_exactly_one_victim() {
    shuttle::check_random(
        || {
            const LSN_X: u64 = 0x1111;
            const LSN_Y: u64 = 0x2222;

            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));
            // timeout_ms = 0 (wait forever): no lock-timeout can preempt the
            // deadlock detector, so a detected cycle MUST be broken by the
            // detector choosing a victim (not by a plain timeout).  Single
            // shard so both LSNs share a table and the interleaving is dense.
            let lm = Arc::new(LockManager::with_config_clock(
                0,
                1,
                Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
            ));

            // Pre-seed: A(=1) holds X, B(=2) holds Y (no contention yet).
            lm.lock(LSN_X, 1, LockType::Write, false, false).unwrap();
            lm.lock(LSN_Y, 2, LockType::Write, false, false).unwrap();

            let deadlocks = Arc::new(AtomicUsize::new(0));
            let grants = Arc::new(AtomicUsize::new(0));
            let done = Arc::new(AtomicUsize::new(0));

            // A waits for Y (held by B).
            let a = {
                let lm = Arc::clone(&lm);
                let deadlocks = Arc::clone(&deadlocks);
                let grants = Arc::clone(&grants);
                let done = Arc::clone(&done);
                shuttle::thread::spawn(move || {
                    match lm.lock_with_timeout(
                        LSN_Y,
                        1,
                        LockType::Write,
                        false,
                        false,
                        0,
                    ) {
                        Ok(_) => {
                            grants.fetch_add(1, Ordering::SeqCst);
                            // Survivor: release everything it now holds.
                            lm.release(LSN_Y, 1).ok();
                            lm.release(LSN_X, 1).ok();
                        }
                        Err(TxnError::Deadlock(_)) => {
                            deadlocks.fetch_add(1, Ordering::SeqCst);
                            // Victim: roll back — release its held lock so the
                            // survivor waiting on it can proceed (a real txn
                            // abort releases all locks).
                            lm.release(LSN_X, 1).ok();
                        }
                        Err(e) => panic!("A: unexpected error {e:?}"),
                    }
                    done.fetch_add(1, Ordering::SeqCst);
                })
            };

            // B waits for X (held by A) — closes the cycle.
            let b = {
                let lm = Arc::clone(&lm);
                let deadlocks = Arc::clone(&deadlocks);
                let grants = Arc::clone(&grants);
                let done = Arc::clone(&done);
                shuttle::thread::spawn(move || {
                    match lm.lock_with_timeout(
                        LSN_X,
                        2,
                        LockType::Write,
                        false,
                        false,
                        0,
                    ) {
                        Ok(_) => {
                            grants.fetch_add(1, Ordering::SeqCst);
                            lm.release(LSN_X, 2).ok();
                            lm.release(LSN_Y, 2).ok();
                        }
                        Err(TxnError::Deadlock(_)) => {
                            deadlocks.fetch_add(1, Ordering::SeqCst);
                            // Victim: roll back — release its held Y lock.
                            lm.release(LSN_Y, 2).ok();
                        }
                        Err(e) => panic!("B: unexpected error {e:?}"),
                    }
                    done.fetch_add(1, Ordering::SeqCst);
                })
            };

            // Driver: advance the SimClock so the 50 ms re-detection slice
            // fires; the victim aborts and releases, unblocking the survivor.
            // Bounded so a genuine hang still fails the test.
            let mut steps = 0;
            while done.load(Ordering::SeqCst) < 2 {
                advance_and_fire(&sim, Duration::from_millis(60));
                shuttle::thread::yield_now();
                steps += 1;
                assert!(steps < 3000, "deadlock never resolved (hang)");
            }

            a.join().unwrap();
            b.join().unwrap();

            // victim-consistency: EXACTLY one victim, one survivor.  Never
            // both-proceed (compatibility violation) and never both-abort
            // (over-abort).
            let d = deadlocks.load(Ordering::SeqCst);
            let g = grants.load(Ordering::SeqCst);
            assert_eq!(
                d, 1,
                "expected exactly one deadlock victim, got {d} (grants={g})"
            );
            assert_eq!(
                g, 1,
                "expected exactly one survivor granted, got {g} (victims={d})"
            );
            // no-deadlock-undetected: the survivor + victim together account
            // for both lockers (neither hung; d + g == 2 is implied by the two
            // asserts above and the `done == 2` join gate).
        },
        ITERATIONS,
    );
}

/// no lost wakeup on grant + WriteLocksExclusive: one holder, one waiter; when
/// the holder releases, the waiter is always granted and returns.  No cycle
/// here (no deadlock), so no clock advance is needed — the grant is a pure
/// notify — but a SimClock is installed for the wait loop's deadline source.
#[test]
fn blocked_waiter_granted_on_release_no_lost_wakeup() {
    shuttle::check_random(
        || {
            const LSN: u64 = 0xBEEF;

            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));
            let lm = Arc::new(LockManager::with_config_clock(
                0, // wait forever (0 = no timeout): the release must wake us
                1,
                Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
            ));

            // Holder (=1) takes the write lock.
            lm.lock(LSN, 1, LockType::Write, false, false).unwrap();

            let granted = Arc::new(AtomicBool::new(false));
            let waiter_done = Arc::new(AtomicBool::new(false));

            // Waiter (=2) blocks for the write lock.
            let w = {
                let lm = Arc::clone(&lm);
                let granted = Arc::clone(&granted);
                let waiter_done = Arc::clone(&waiter_done);
                shuttle::thread::spawn(move || {
                    let r = lm.lock_with_timeout(
                        LSN,
                        2,
                        LockType::Write,
                        false,
                        false,
                        0,
                    );
                    if matches!(
                        r,
                        Ok(LockGrantType::New | LockGrantType::Existing)
                    ) {
                        granted.store(true, Ordering::SeqCst);
                    }
                    waiter_done.store(true, Ordering::SeqCst);
                })
            };

            // Holder releases, which must wake the waiter and grant it.
            let r = {
                let lm = Arc::clone(&lm);
                shuttle::thread::spawn(move || {
                    lm.release(LSN, 1).unwrap();
                })
            };

            r.join().unwrap();
            // The waiter should be woken by the release notify.  Drive the
            // SimClock in case an interleaving parked it on the 50 ms slice
            // before the release notify landed (the pre-release wait path).
            let mut steps = 0;
            while !waiter_done.load(Ordering::SeqCst) {
                advance_and_fire(&sim, Duration::from_millis(60));
                shuttle::thread::yield_now();
                steps += 1;
                assert!(steps < 3000, "waiter never granted (lost wakeup)");
            }
            w.join().unwrap();

            assert!(
                granted.load(Ordering::SeqCst),
                "waiter was not granted after the holder released \
                 (lost wakeup on grant)"
            );
            // WriteLocksExclusive: exactly the waiter now owns it.
            assert!(lm.is_owned_write_lock(LSN, 2));
            assert!(!lm.is_owned_write_lock(LSN, 1));
        },
        ITERATIONS,
    );
}
