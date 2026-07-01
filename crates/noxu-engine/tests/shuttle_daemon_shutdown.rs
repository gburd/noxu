// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation tests for the `DaemonManager` shutdown /
//! wakeup coordination (DST Milestone 2, Phase 2a).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg, the daemons' `WakeHandle` (`Mutex<bool>` + `Condvar`)
//! resolves — through `noxu_util::dst_sync` — to shuttle-instrumented
//! primitives, so shuttle's scheduler explores every ordering of the
//! daemon-loop sleep vs. the shutdown notify.
//!
//! # Why this protocol is shuttle-clean (unlike `FsyncManager`)
//!
//! The daemon shutdown wakeup is **explicit**: `shutdown()` sets the flag and
//! calls `WakeHandle::notify()` (a `Condvar::notify_all`), so a sleeping daemon
//! is always woken by a notify — its liveness does *not* rely on the sleep
//! timing out.  shuttle can therefore prove deadlock-freedom of the shutdown
//! path.  (By contrast, `FsyncManager`'s leader hand-off relies on the
//! `fsync_timeout` to recover a lost `wakeup_one`, which shuttle's non-timing
//! `wait_timeout` cannot model — see `shuttle_fsync_manager.rs`.)
//!
//! # Invariants
//!
//!   1. **No lost shutdown wakeup** — every daemon that slept on its
//!      `WakeHandle` observes the shutdown notify and exits its loop; a lost
//!      notify would leave a daemon blocked forever and shuttle reports it as a
//!      deadlock.
//!   2. **Shutdown-then-join ordering** — the daemons are joined only after the
//!      notify, in the fixed cleaner -> checkpointer -> evictor order, with no
//!      hang under any interleaving.
//!   3. **No use-after-shutdown** — a daemon never runs its work body once the
//!      shutdown flag is observed.
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-engine --test shuttle_daemon_shutdown
//! ```
#![cfg(noxu_shuttle)]

use std::sync::atomic::Ordering;
use std::time::Duration;

use noxu_engine::daemon_manager::dst_hooks::WakeHandle;
use shuttle::sync::Arc;
use shuttle::sync::atomic::{AtomicBool, AtomicUsize};

const ITERATIONS: usize = 5_000;

/// Model the three daemon loops (`loop { wait_timeout; if notified||shutdown
/// break; work }`) plus the shutdown coordinator, using the REAL `WakeHandle`
/// and the REAL join ordering, and require that shutdown always completes with
/// every daemon having exited (no lost wakeup, no hang) under every schedule.
#[test]
fn shutdown_wakes_all_daemons_no_lost_wakeup() {
    shuttle::check_random(
        || {
            let shutdown = Arc::new(AtomicBool::new(false));
            let cleaner_wake = WakeHandle::new_for_shuttle();
            let checkpointer_wake = WakeHandle::new_for_shuttle();
            let evictor_wake = WakeHandle::new_for_shuttle();
            // Counts daemons that observed shutdown and exited cleanly.
            let exited = Arc::new(AtomicUsize::new(0));
            // Set true if any daemon runs its work body AFTER shutdown was
            // signalled (a use-after-shutdown violation).
            let work_after_shutdown = Arc::new(AtomicBool::new(false));

            let spawn_daemon = |wake: WakeHandle| {
                let shutdown = Arc::clone(&shutdown);
                let exited = Arc::clone(&exited);
                let waf = Arc::clone(&work_after_shutdown);
                shuttle::thread::spawn(move || {
                    // Mirrors the real daemon loop body in daemon_manager.rs.
                    loop {
                        let notified =
                            wake.wait_timeout(Duration::from_millis(5000));
                        if notified || shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        // "Perform work" — must never run once shutdown is set.
                        if shutdown.load(Ordering::Relaxed) {
                            waf.store(true, Ordering::Relaxed);
                        }
                    }
                    exited.fetch_add(1, Ordering::SeqCst);
                })
            };

            let cleaner = spawn_daemon(cleaner_wake.clone());
            let checkpointer = spawn_daemon(checkpointer_wake.clone());
            let evictor = spawn_daemon(evictor_wake.clone());

            // Shutdown coordinator — mirrors DaemonManager::shutdown ordering.
            let shutdown_thread = {
                let shutdown = Arc::clone(&shutdown);
                shuttle::thread::spawn(move || {
                    shutdown.store(true, Ordering::Relaxed);
                    // Wake all daemons (order matches production).
                    cleaner_wake.notify();
                    checkpointer_wake.notify();
                    evictor_wake.notify();
                })
            };

            // Join in the production order: cleaner -> checkpointer -> evictor.
            // A lost wakeup on any of them would block this join forever and
            // shuttle would report a deadlock.
            shutdown_thread.join().unwrap();
            cleaner.join().unwrap();
            checkpointer.join().unwrap();
            evictor.join().unwrap();

            // Invariant 1: all three daemons observed shutdown and exited.
            assert_eq!(
                exited.load(Ordering::SeqCst),
                3,
                "a daemon failed to exit on shutdown (lost wakeup)"
            );
            // Invariant 3: no daemon ran work after shutdown was signalled.
            assert!(
                !work_after_shutdown.load(Ordering::Relaxed),
                "a daemon ran its work body after shutdown (use-after-shutdown)"
            );
        },
        ITERATIONS,
    );
}

/// A daemon that is *already awake and looping* (not yet sleeping) when
/// shutdown fires must still observe the flag and exit — the notify may race
/// ahead of the daemon reaching `wait_timeout`.  shuttle explores both the
/// "notify before wait" and "notify after wait" orderings.
#[test]
fn shutdown_before_daemon_sleeps_still_exits() {
    shuttle::check_random(
        || {
            let shutdown = Arc::new(AtomicBool::new(false));
            let wake = WakeHandle::new_for_shuttle();
            let exited = Arc::new(AtomicBool::new(false));

            let d = {
                let shutdown = Arc::clone(&shutdown);
                let exited = Arc::clone(&exited);
                let wake = wake.clone();
                shuttle::thread::spawn(move || {
                    loop {
                        // Check the flag first (the daemon loop re-checks
                        // shutdown both before and after the wait).
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        let notified =
                            wake.wait_timeout(Duration::from_millis(5000));
                        if notified || shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    exited.store(true, Ordering::SeqCst);
                })
            };

            // Coordinator may run entirely before the daemon reaches its wait.
            shutdown.store(true, Ordering::Relaxed);
            wake.notify();

            d.join().unwrap();
            assert!(
                exited.load(Ordering::SeqCst),
                "daemon did not exit when shutdown raced ahead of its sleep"
            );
        },
        ITERATIONS,
    );
}
