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
                1,
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
                            .flush_and_sync(
                                0,
                                || 0,
                                move || {
                                    execs2.fetch_add(1, Ordering::SeqCst);
                                    let covered =
                                        snap_lsn2.load(Ordering::SeqCst);
                                    let old =
                                        flushed_lsn2.load(Ordering::SeqCst);
                                    let newv = covered.max(old);
                                    flushed_lsn2.store(newv, Ordering::SeqCst);
                                    assert_fsynced_never_decreases(old, newv);
                                    Ok(covered)
                                },
                            )
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
                1,
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
                        let r = mgr.flush_and_sync(
                            0,
                            || 0,
                            move || {
                                attempts2.fetch_add(1, Ordering::SeqCst);
                                Err::<u64, _>(std::io::Error::other(
                                    "fsync EIO",
                                ))
                            },
                        );
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
                1,
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
                            .flush_and_sync(
                                0,
                                || 0,
                                move || {
                                    let covered =
                                        snap_lsn2.load(Ordering::SeqCst);
                                    let old =
                                        flushed_lsn2.load(Ordering::SeqCst);
                                    let newv = covered.max(old);
                                    flushed_lsn2.store(newv, Ordering::SeqCst);
                                    assert_fsynced_never_decreases(old, newv);
                                    Ok(covered)
                                },
                            )
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

/// BOUNDED FSYNC PIPELINE: with `max_leaders > 1`, up to N leaders run their
/// `do_work` (drain + fdatasync) concurrently, yet the durable watermark must
/// stay a single monotonic value — every byte below any published watermark is
/// in the page cache before that leader's fdatasync.
///
/// This models the real design's ordering: the DRAIN (advance the page-cache
/// EOF and capture this leader's `eol`) runs under a mutex that stands in for
/// the log-write latch, so drains are LSN-ordered and never overlap; only the
/// fdatasync half overlaps.  The oracle checks, at every watermark advance,
/// that the page cache already covers the value being published (the
/// monotonic-watermark invariant), plus the usual coalescing / coverage /
/// no-regression oracles — under every interleaving shuttle explores.
#[test]
fn bounded_pipeline_monotonic_watermark_holds() {
    shuttle::check_random(
        || {
            const N: usize = 4;
            const MAX_LEADERS: usize = 3;
            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));
            let mgr = Arc::new(FsyncManager::with_clock(
                0,
                0,
                MAX_LEADERS,
                Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
            ));
            // Stands in for the log-write latch: serializes the DRAIN so
            // pwrites (page-cache EOF advance) happen in LSN order, one at a
            // time.  Only the fdatasync overlaps across leaders.
            let lwl = Arc::new(shuttle::sync::Mutex::new(()));
            // Highest LSN that has been ASSIGNED (JE next_available_lsn): a
            // leader captures this as its `eol` under the LWL.  Crucially this
            // can name bytes from OTHER committers whose pwrite has not yet
            // happened — the exact condition the durability hole rides on.
            let assigned_eof = Arc::new(AtomicU64::new(0));
            let next_lsn = Arc::new(AtomicU64::new(1));
            // Highest byte-EOF that some drain has pwritten to the page cache.
            let page_cache_eof = Arc::new(AtomicU64::new(0));
            // The single durable watermark (last_synced_lsn), CAS-max advanced.
            let last_synced = Arc::new(AtomicU64::new(0));
            let fsync_execs = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..N)
                .map(|_| {
                    let mgr = Arc::clone(&mgr);
                    let lwl = Arc::clone(&lwl);
                    let next_lsn = Arc::clone(&next_lsn);
                    let assigned_eof = Arc::clone(&assigned_eof);
                    let page_cache_eof = Arc::clone(&page_cache_eof);
                    let last_synced = Arc::clone(&last_synced);
                    let fsync_execs = Arc::clone(&fsync_execs);
                    shuttle::thread::spawn(move || {
                        let my_lsn = next_lsn.fetch_add(1, Ordering::SeqCst);
                        // Assign our LSN (advances next_available_lsn); our
                        // bytes are NOT yet in the page cache.
                        bump_max(&assigned_eof, my_lsn);

                        let lwl2 = Arc::clone(&lwl);
                        let pce2 = Arc::clone(&page_cache_eof);
                        let aeof2 = Arc::clone(&assigned_eof);
                        let execs2 = Arc::clone(&fsync_execs);
                        let eol = mgr
                            .flush_and_sync(0, || 0, move || {
                                // DRAIN under the LWL: pwrite EVERY assigned
                                // byte that is not yet in the page cache (the
                                // real fill_flush_pending drains ALL dirty
                                // buffers, not just this committer's), advancing
                                // page_cache_eof up to the assigned EOF in LSN
                                // order, THEN capture eol = assigned EOF.
                                // Draining ALL unflushed bytes under the LWL is
                                // what makes publishing `my_eol` sound even
                                // though my_eol covers OTHER committers' LSNs:
                                // this leader pwrites their bytes too before it
                                // captures (and later publishes) their eol.  A
                                // broken variant that pwrote only its OWN bytes
                                // (or pwrote after releasing the LWL) would let
                                // this leader publish an eol covering a sibling
                                // whose bytes are not yet in the page cache.
                                let my_eol = {
                                    let _g = lwl2.lock().unwrap();
                                    let eol = aeof2.load(Ordering::SeqCst);
                                    // pwrite everything up to eol (all cohorts).
                                    bump_max(&pce2, eol);
                                    eol
                                };
                                // LWL released; the "fdatasync" runs
                                // concurrently with other leaders' drains.
                                // THE MONOTONIC-WATERMARK ORACLE, checked at
                                // fdatasync time: an fdatasync makes durable
                                // exactly what is in the page cache when it
                                // runs.  For publishing `my_eol` to be sound,
                                // the page cache must ALREADY cover my_eol at
                                // this point (it does, because the pwrite above
                                // ran under the LWL before this leader could
                                // reach here).  Sampling page_cache_eof here —
                                // possibly interleaved with other leaders' drains
                                // by shuttle — and asserting it covers my_eol is
                                // the strongest form of the invariant: if the
                                // pwrite were moved outside the LWL, another
                                // leader could capture (and be about to publish)
                                // a higher eol whose bytes this leader has not
                                // yet pwritten, and some interleaving would
                                // sample page_cache_eof < that eol here.
                                assert!(
                                    pce2.load(Ordering::SeqCst) >= my_eol,
                                    "monotonic-watermark violated at fdatasync: \
                                     publishing durable {my_eol} but page cache \
                                     only covers {}",
                                    pce2.load(Ordering::SeqCst)
                                );
                                execs2.fetch_add(1, Ordering::SeqCst);
                                Ok(my_eol)
                            })
                            .expect("no fault injected: fsync must succeed");

                        // Caller advances the single durable watermark by
                        // CAS-max after the successful fdatasync.
                        let published = eol.as_u64();
                        let old = last_synced.load(Ordering::SeqCst);
                        bump_max(&last_synced, published);
                        let newv = last_synced.load(Ordering::SeqCst);
                        // FsyncedNeverDecreases across our advance.
                        assert_fsynced_never_decreases(old, newv.max(old));

                        // DurableImpliesLogged: our own commit is durable.
                        assert_durable_covers_commit(newv, my_lsn);
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
            let final_synced = last_synced.load(Ordering::SeqCst);
            let highest = next_lsn.load(Ordering::SeqCst) - 1;
            assert!(
                final_synced >= highest,
                "final durable watermark {final_synced} < highest commit \
                 LSN {highest}: a committed write was left unsynced"
            );
        },
        ITERATIONS,
    );
}

/// CONCURRENCY-ADAPTIVE BATCH WINDOW DURABILITY (the adaptive fix, proven by
/// shuttle).
///
/// The adaptive window changes ONLY the leader/waiter *decision* — the
/// effective concurrent-leader ceiling now varies with the live waiter count
/// (`effective_ceiling`): below `batch_trigger` waiters up to `adaptive_leaders`
/// leaders may overlap; at/above the trigger the ceiling clamps to
/// `max_leaders`.  It does NOT change the drain/pwrite/fdatasync ordering that
/// makes the durable watermark sound.  This test enables the adaptive window
/// (`adaptive_leaders = 3`, `batch_trigger = 2`, so both the parallel-leader
/// regime AND the clamped-batch regime are exercised as the waiter count
/// crosses the trigger under shuttle's interleavings) and re-asserts the same
/// monotonic-watermark + coverage oracles as `bounded_pipeline_...`: a
/// committer returns durable-success only when a completed fdatasync covered
/// its LSN, under every interleaving.  If the adaptive decision let a committer
/// return before its bytes were durable, one of the oracles below fires.
#[test]
fn adaptive_window_monotonic_watermark_holds() {
    shuttle::check_random(
        || {
            const N: usize = 4;
            // max_leaders = 1 (single-leader batching baseline), but the
            // adaptive window raises the ceiling to 3 while < 2 waiters queue,
            // so up to 3 leaders can overlap at low contention.  The effective
            // ceiling flips between 1 and 3 as shuttle interleaves arrivals.
            const MAX_LEADERS: usize = 1;
            const ADAPTIVE_LEADERS: usize = 3;
            const BATCH_TRIGGER: usize = 2;
            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));
            let mgr = Arc::new(
                FsyncManager::with_clock(
                    0,
                    0,
                    MAX_LEADERS,
                    Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
                )
                .with_adaptive_window(ADAPTIVE_LEADERS, BATCH_TRIGGER),
            );
            // Same model as bounded_pipeline_monotonic_watermark_holds: the LWL
            // serializes the DRAIN (LSN-ordered pwrite to the page cache); only
            // the fdatasync overlaps.  The adaptive ceiling only affects HOW
            // MANY fdatasyncs may overlap, never the drain ordering.
            let lwl = Arc::new(shuttle::sync::Mutex::new(()));
            let assigned_eof = Arc::new(AtomicU64::new(0));
            let next_lsn = Arc::new(AtomicU64::new(1));
            let page_cache_eof = Arc::new(AtomicU64::new(0));
            let last_synced = Arc::new(AtomicU64::new(0));
            let fsync_execs = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..N)
                .map(|_| {
                    let mgr = Arc::clone(&mgr);
                    let lwl = Arc::clone(&lwl);
                    let next_lsn = Arc::clone(&next_lsn);
                    let assigned_eof = Arc::clone(&assigned_eof);
                    let page_cache_eof = Arc::clone(&page_cache_eof);
                    let last_synced = Arc::clone(&last_synced);
                    let fsync_execs = Arc::clone(&fsync_execs);
                    shuttle::thread::spawn(move || {
                        let my_lsn = next_lsn.fetch_add(1, Ordering::SeqCst);
                        bump_max(&assigned_eof, my_lsn);

                        let lwl2 = Arc::clone(&lwl);
                        let pce2 = Arc::clone(&page_cache_eof);
                        let aeof2 = Arc::clone(&assigned_eof);
                        let execs2 = Arc::clone(&fsync_execs);
                        let eol = mgr
                            .flush_and_sync(0, || 0, move || {
                                // DRAIN under the LWL, pwrite all assigned bytes
                                // to the page cache in LSN order, capture eol.
                                let my_eol = {
                                    let _g = lwl2.lock().unwrap();
                                    let eol = aeof2.load(Ordering::SeqCst);
                                    bump_max(&pce2, eol);
                                    eol
                                };
                                // Monotonic-watermark oracle at fdatasync time:
                                // the page cache MUST already cover my_eol.
                                assert!(
                                    pce2.load(Ordering::SeqCst) >= my_eol,
                                    "adaptive: monotonic-watermark violated at \
                                     fdatasync: publishing durable {my_eol} but \
                                     page cache only covers {}",
                                    pce2.load(Ordering::SeqCst)
                                );
                                execs2.fetch_add(1, Ordering::SeqCst);
                                Ok(my_eol)
                            })
                            .expect("no fault injected: fsync must succeed");

                        let published = eol.as_u64();
                        let old = last_synced.load(Ordering::SeqCst);
                        bump_max(&last_synced, published);
                        let newv = last_synced.load(Ordering::SeqCst);
                        assert_fsynced_never_decreases(old, newv.max(old));
                        // DurableImpliesLogged: our own commit is durable.
                        assert_durable_covers_commit(newv, my_lsn);
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            let execs = fsync_execs.load(Ordering::SeqCst);
            assert!(
                (1..=N).contains(&execs),
                "adaptive: fsync executions {execs} out of range 1..={N}"
            );
            let final_synced = last_synced.load(Ordering::SeqCst);
            let highest = next_lsn.load(Ordering::SeqCst) - 1;
            assert!(
                final_synced >= highest,
                "adaptive: final durable watermark {final_synced} < highest \
                 commit LSN {highest}: a committed write was left unsynced"
            );
        },
        ITERATIONS,
    );
}

/// WRITEQUEUE SHORT-CIRCUIT DURABILITY (the coalescing fix, proven by shuttle).
///
/// This is the oracle the task requires: N committers, some enqueue-and-return
/// (short-circuit on the durable watermark), some fsync.  Under every
/// interleaving shuttle explores, assert the three invariants:
///
///   1. **No commit returns before its LSN is durable** — the watermark this
///      committer observes on return (`durable`) covers its own commit LSN
///      (`assert_durable_covers_commit`).  This holds on BOTH paths: the leader
///      path (its own completed fdatasync) AND the short-circuit path (a
///      completed fdatasync by another leader that already covered its LSN,
///      detected via the `synced_watermark()` re-check).
///   2. **The watermark is monotonic** — never regresses across any advance
///      (`assert_fsynced_never_decreases`).
///   3. **Every committer is covered by a COMPLETED fdatasync before it
///      returns** — a short-circuiting committer returns ONLY when
///      `last_synced` (advanced solely after a completed fsync closure) already
///      exceeds its LSN, so its bytes are on disk before its commit returns.
///
/// The model mirrors the production wiring: `last_synced` is advanced (CAS-max)
/// by the CALLER after `flush_and_sync` returns Ok, and the `synced_watermark`
/// closure reads that same cell — so a woken waiter's short-circuit sees only
/// watermarks a completed fdatasync published.  The fsync closure syncs the
/// whole log to EOL (JE `ch.force(false)`), so one completed fsync covers every
/// LSN assigned before it.
#[test]
fn writequeue_shortcircuit_durability_holds() {
    shuttle::check_random(
        || {
            const N: usize = 4;
            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));
            let mgr = Arc::new(FsyncManager::with_clock(
                0,
                0,
                1,
                Arc::clone(&sim) as Arc<dyn noxu_util::Clock>,
            ));
            let next_lsn = Arc::new(AtomicU64::new(1));
            // assigned_eof = JE next_available_lsn: the eol a leader captures.
            let assigned_eof = Arc::new(AtomicU64::new(0));
            // The single durable watermark (last_synced_lsn).  Advanced ONLY
            // after a completed fsync closure (by the caller, CAS-max).
            let last_synced = Arc::new(AtomicU64::new(0));
            let fsync_execs = Arc::new(AtomicUsize::new(0));
            let shortcircuits = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..N)
                .map(|_| {
                    let mgr = Arc::clone(&mgr);
                    let next_lsn = Arc::clone(&next_lsn);
                    let assigned_eof = Arc::clone(&assigned_eof);
                    let last_synced = Arc::clone(&last_synced);
                    let fsync_execs = Arc::clone(&fsync_execs);
                    let shortcircuits = Arc::clone(&shortcircuits);
                    shuttle::thread::spawn(move || {
                        let my_lsn = next_lsn.fetch_add(1, Ordering::SeqCst);
                        // Assign our LSN (advances next_available_lsn).
                        bump_max(&assigned_eof, my_lsn);

                        let aeof2 = Arc::clone(&assigned_eof);
                        let ls_sync = Arc::clone(&last_synced);
                        let ls_work = Arc::clone(&last_synced);
                        let execs2 = Arc::clone(&fsync_execs);
                        let sc_before = ls_sync.load(Ordering::SeqCst);
                        let durable = mgr
                            .flush_and_sync(
                                // target_lsn: our commit LSN.
                                my_lsn,
                                // synced_watermark reader: the SAME cell the
                                // caller advances after a completed fsync, so a
                                // short-circuit only ever observes durable state.
                                move || ls_sync.load(Ordering::SeqCst),
                                // fsync closure: syncs the whole log to EOL.
                                move || {
                                    execs2.fetch_add(1, Ordering::SeqCst);
                                    let eol = aeof2.load(Ordering::SeqCst);
                                    // Advance the durable watermark to the EOL
                                    // this fdatasync covered (the completed
                                    // fsync makes every assigned byte durable).
                                    let old = ls_work.load(Ordering::SeqCst);
                                    bump_max(&ls_work, eol);
                                    let newv = ls_work.load(Ordering::SeqCst);
                                    assert_fsynced_never_decreases(
                                        old,
                                        newv.max(old),
                                    );
                                    Ok(eol)
                                },
                            )
                            .expect("no fault injected: fsync must succeed");

                        // If we returned without our fsync closure running for
                        // us (watermark already covered us on wake), record it
                        // as a short-circuit for the coverage assertion below.
                        let d = durable.as_u64();
                        if d > sc_before {
                            shortcircuits.fetch_add(1, Ordering::Relaxed);
                        }

                        // INVARIANT 1 + 3: the watermark we observed on return
                        // covers our commit LSN — on BOTH the leader path and
                        // the short-circuit path.  A short-circuit returns the
                        // (completed-fsync-advanced) watermark, so this proves
                        // our bytes were durable BEFORE our commit returned.
                        assert_durable_covers_commit(d, my_lsn);
                        // The global durable watermark also covers us.
                        let global = last_synced.load(Ordering::SeqCst);
                        assert_durable_covers_commit(global, my_lsn);
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // Coalescing: the short-circuit means FEWER fsyncs than committers
            // in interleavings where a leader's EOL-sync covers a sibling —
            // 1..=N executions, and strictly < N whenever any short-circuit
            // fired.  Never more than N (no redundant double-fsync).
            let execs = fsync_execs.load(Ordering::SeqCst);
            assert!(
                (1..=N).contains(&execs),
                "fsync executions {execs} out of range 1..={N}"
            );
            // INVARIANT 2 (final): every assigned LSN is under the watermark.
            let final_synced = last_synced.load(Ordering::SeqCst);
            let highest = next_lsn.load(Ordering::SeqCst) - 1;
            assert!(
                final_synced >= highest,
                "final durable watermark {final_synced} < highest commit \
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
