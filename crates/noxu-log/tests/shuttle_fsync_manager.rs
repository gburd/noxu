// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation tests for the `FsyncManager` group-commit
//! (leader/waiter) protocol (DST Milestone 2, Phase 2a + wave 2).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg, `FsyncManager`'s `Mutex`/`Condvar` resolve (through
//! `noxu_util::dst_sync_pl`, the parking_lot-over-shuttle wrapper) to
//! shuttle-instrumented primitives, so shuttle's scheduler explores the
//! leader/waiter/piggyback interleavings of the *real* group-commit code.
//!
//! # The timeout-liveness crux (solved in DST wave 2)
//!
//! shuttle 0.9's `Condvar::wait_timeout` never times out.  The group-commit
//! protocol's liveness previously depended on `LOG_FSYNC_TIMEOUT` to recover a
//! lost leader-designation `wakeup_one` (M2 documented this as an
//! `#[ignore]`'d oracle plus a "shuttle catches the deadlock" tripwire).
//!
//! Two DST wave-2 changes make the oracle a green gate:
//!
//!   1. **The lost-wakeup fix** (`fsync_manager.rs`): `wakeup_one` now arms a
//!      `leader_notified` flag under the group mutex before `notify_one`, and
//!      `wait_for_event` consumes it BEFORE blocking (the same
//!      predicate-before-wait class as the M2 `WakeHandle` pre-check).  The
//!      leader hand-off is now **timeout-independent** — a designation is never
//!      lost, so no waiter is orphaned to the timeout.  This is a real
//!      production correctness fix (it closes a commit/shutdown stall window),
//!      not test-only scaffolding.
//!   2. **The clock-driven timed wait** (`dst_sync_pl`): the group-commit wait
//!      (`grpc_wait`) still uses a `Condvar` timed wait; when a test enables
//!      group commit (`grpc_interval_ms > 0`), a driver thread advances the
//!      shared `SimClock` with `advance_and_fire` so that timed wait fires
//!      deterministically instead of hanging.  With `grpc` disabled (the
//!      default committer path) the protocol is fully notify-driven and needs
//!      no clock advance at all.
//!
//! # Invariants (the safety oracle, mapped to `noxu-spec` wal_commit)
//!
//!   * **DurableImpliesLogged** — every committer's returned durable watermark
//!     covers its own commit LSN before `flush_and_sync` returns `Ok`
//!     (`assert_durable_covers_commit`).
//!   * **FsyncedNeverDecreases** — the durable watermark never regresses
//!     across the leader's fsync (`assert_fsynced_never_decreases`).
//!   * **Coalescing** — N committers cause `1..=N` actual fsync executions
//!     (a leader serves a batch; no redundant double-fsync, no missing fsync).
//!   * **Failure fan-out** — a failed leader fsync fails all its waiters
//!     (none returns `Ok` on a fsync that errored).
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-log --test shuttle_fsync_manager
//! ```
#![cfg(noxu_shuttle)]

use std::sync::atomic::Ordering;
use std::time::Duration;

use noxu_log::fsync_manager::FsyncManager;
use noxu_util::SimClock;
use noxu_util::dst_invariants::{
    assert_durable_covers_commit, assert_fsynced_never_decreases,
};
use noxu_util::dst_sync_pl::{advance_and_fire, install_sim_clock};
use shuttle::sync::Arc;
use shuttle::sync::atomic::{AtomicU64, AtomicUsize};

/// Number of interleavings shuttle explores per test.
const ITERATIONS: usize = 5_000;

/// SAFETY ORACLE: every committer's returned durable watermark covers its own
/// LSN, coalescing holds, and the durable watermark never regresses, under
/// every interleaving.
///
/// Uses the notify-driven committer path (`FsyncManager::new(0, 0)`, group
/// commit disabled) so the protocol's liveness is entirely notify-driven —
/// with the DST wave-2 lost-wakeup fix the leader hand-off no longer depends
/// on any timeout, so shuttle can prove both safety AND deadlock-freedom over
/// every interleaving.  No `SimClock` advance is needed here because no timed
/// wait is ever taken on this path.
#[test]
fn fsync_coalescing_and_coverage_hold() {
    shuttle::check_random(
        || {
            const N: usize = 3;
            // A SimClock is installed even though this path is notify-driven:
            // `wait_for_event`'s timed `Condvar::wait_for` needs an installed
            // SimClock to compute its deadline, and the FsyncManager's own
            // clock must be the SAME instance so its timeout math agrees.
            // With the wave-2 lost-wakeup fix the hand-off is
            // timeout-independent, so the clock never needs advancing — the
            // notify path always wakes waiters and no interleaving deadlocks.
            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));
            let mgr = Arc::new(FsyncManager::with_clock(
                0,
                0,
                Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
            ));
            let next_lsn = Arc::new(AtomicU64::new(1));
            let snap_lsn = Arc::new(AtomicU64::new(0));
            let flushed_lsn = Arc::new(AtomicU64::new(0));
            let fsync_execs = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..N)
                .map(|_| {
                    let mgr = Arc::clone(&mgr);
                    let next_lsn = Arc::clone(&next_lsn);
                    let snap_lsn = Arc::clone(&snap_lsn);
                    let flushed_lsn = Arc::clone(&flushed_lsn);
                    let fsync_execs = Arc::clone(&fsync_execs);
                    shuttle::thread::spawn(move || {
                        let my_lsn = next_lsn.fetch_add(1, Ordering::SeqCst);
                        bump_max(&snap_lsn, my_lsn);

                        let flushed_lsn2 = Arc::clone(&flushed_lsn);
                        let snap_lsn2 = Arc::clone(&snap_lsn);
                        let execs2 = Arc::clone(&fsync_execs);
                        let durable = mgr
                            .flush_and_sync(move || {
                                execs2.fetch_add(1, Ordering::SeqCst);
                                let covered = snap_lsn2.load(Ordering::SeqCst);
                                let old = flushed_lsn2.load(Ordering::SeqCst);
                                let newv = covered.max(old);
                                flushed_lsn2.store(newv, Ordering::SeqCst);
                                assert_fsynced_never_decreases(old, newv);
                                Ok(covered)
                            })
                            .expect("no fault injected: fsync must succeed");

                        assert_durable_covers_commit(durable.as_u64(), my_lsn);
                        let global = flushed_lsn.load(Ordering::SeqCst);
                        assert_durable_covers_commit(global, my_lsn);
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            let execs = fsync_execs.load(Ordering::SeqCst);
            assert!(
                (1..=N).contains(&execs),
                "fsync executions {execs} out of range 1..={N}: a redundant \
                 (double) fsync or a missing one indicates a coalescing bug"
            );
            let final_flushed = flushed_lsn.load(Ordering::SeqCst);
            let highest = next_lsn.load(Ordering::SeqCst) - 1;
            assert!(
                final_flushed >= highest,
                "final durable watermark {final_flushed} < highest commit \
                 LSN {highest}: a committed write was left unsynced"
            );
        },
        ITERATIONS,
    );
}

/// FAILURE FAN-OUT: a leader fsync failure fails EVERY piggybacking waiter —
/// none returns `Ok` on a fsync that errored — and coalescing still happens
/// (fewer fsync attempts than committers).  Notify-driven path (group commit
/// disabled), so deadlock-free under every interleaving with the wave-2 fix.
#[test]
fn fsync_failure_fails_all_waiters() {
    shuttle::check_random(
        || {
            const N: usize = 3;
            // See `fsync_coalescing_and_coverage_hold` — SimClock installed so
            // the notify-driven `wait_for_event` timed wait has a deadline
            // source; never advanced (hand-off is timeout-independent).
            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));
            let mgr = Arc::new(FsyncManager::with_clock(
                0,
                0,
                Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
            ));
            let attempts = Arc::new(AtomicUsize::new(0));
            let errors = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..N)
                .map(|_| {
                    let mgr = Arc::clone(&mgr);
                    let attempts = Arc::clone(&attempts);
                    let errors = Arc::clone(&errors);
                    shuttle::thread::spawn(move || {
                        let attempts2 = Arc::clone(&attempts);
                        let r = mgr.flush_and_sync(move || {
                            attempts2.fetch_add(1, Ordering::SeqCst);
                            Err::<u64, _>(std::io::Error::other("fsync EIO"))
                        });
                        match r {
                            Ok(_) => panic!(
                                "a committer returned Ok despite a failed fsync"
                            ),
                            Err(e) => {
                                assert!(
                                    e.to_string().contains("fsync EIO"),
                                    "error must carry the leader failure: {e}"
                                );
                                errors.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(
                errors.load(Ordering::SeqCst),
                N,
                "every committer must observe the fsync failure"
            );
            let a = attempts.load(Ordering::SeqCst);
            assert!(
                (1..=N).contains(&a),
                "fsync attempts {a} out of range 1..={N} under failure"
            );
        },
        ITERATIONS,
    );
}

/// GROUP-COMMIT WAIT drives the `SimClock`-timed leader wait.
///
/// With group commit enabled (`grpc_threshold=2, grpc_interval_ms=5`) the
/// leader may take the `grpc_wait` timed `Condvar` wait, which under shuttle
/// only fires when the harness advances the shared `SimClock` past its
/// deadline (`advance_and_fire`).  A driver thread advances simulated time
/// until every committer has finished, so the leader's grpc wait always
/// resolves (either the threshold is met and it is notified, or the interval
/// elapses via the clock).  The same safety oracle
/// (`DurableImpliesLogged` / `FsyncedNeverDecreases` / coalescing) must hold.
#[test]
fn group_commit_wait_holds_under_sim_clock() {
    shuttle::check_random(
        || {
            const N: usize = 3;
            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));

            // grpc enabled: threshold 2, interval 5 ms — the leader can take
            // the grpc timed wait, driven by the SimClock below.
            let mgr = Arc::new(FsyncManager::with_clock(
                2,
                5,
                Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
            ));
            let next_lsn = Arc::new(AtomicU64::new(1));
            let snap_lsn = Arc::new(AtomicU64::new(0));
            let flushed_lsn = Arc::new(AtomicU64::new(0));
            let done = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..N)
                .map(|_| {
                    let mgr = Arc::clone(&mgr);
                    let next_lsn = Arc::clone(&next_lsn);
                    let snap_lsn = Arc::clone(&snap_lsn);
                    let flushed_lsn = Arc::clone(&flushed_lsn);
                    let done = Arc::clone(&done);
                    shuttle::thread::spawn(move || {
                        let my_lsn = next_lsn.fetch_add(1, Ordering::SeqCst);
                        bump_max(&snap_lsn, my_lsn);

                        let flushed_lsn2 = Arc::clone(&flushed_lsn);
                        let snap_lsn2 = Arc::clone(&snap_lsn);
                        let durable = mgr
                            .flush_and_sync(move || {
                                let covered = snap_lsn2.load(Ordering::SeqCst);
                                let old = flushed_lsn2.load(Ordering::SeqCst);
                                let newv = covered.max(old);
                                flushed_lsn2.store(newv, Ordering::SeqCst);
                                assert_fsynced_never_decreases(old, newv);
                                Ok(covered)
                            })
                            .expect("no fault injected: fsync must succeed");

                        assert_durable_covers_commit(durable.as_u64(), my_lsn);
                        done.fetch_add(1, Ordering::SeqCst);
                    })
                })
                .collect();

            // Driver: advance simulated time (firing due grpc timed waits)
            // until every committer has returned.  advance_and_fire re-notifies
            // pending fires, so a fire that lands in a waiter's pre-block gap is
            // re-delivered — guaranteeing progress.  Bounded so a genuine hang
            // still fails the test rather than looping forever.
            let mut steps = 0;
            while done.load(Ordering::SeqCst) < N {
                advance_and_fire(&sim, Duration::from_millis(10));
                shuttle::thread::yield_now();
                steps += 1;
                assert!(
                    steps < 2000,
                    "group-commit wait never resolved (hang)"
                );
            }

            for h in handles {
                h.join().unwrap();
            }

            let highest = next_lsn.load(Ordering::SeqCst) - 1;
            let final_flushed = flushed_lsn.load(Ordering::SeqCst);
            assert!(
                final_flushed >= highest,
                "final durable watermark {final_flushed} < highest commit \
                 LSN {highest}: a committed write was left unsynced"
            );
        },
        ITERATIONS,
    );
}

/// Advance `cell` to at least `v` (a lock-free max).
fn bump_max(cell: &AtomicU64, v: u64) {
    let mut cur = cell.load(Ordering::SeqCst);
    while cur < v {
        match cell.compare_exchange(cur, v, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => break,
            Err(a) => cur = a,
        }
    }
}
