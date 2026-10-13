// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Deterministic liveness witness for **BUG-VERIFY-TRACKER-LATCH-ABBA**.
//!
//! `Environment::verify` (and the `VerifyDaemon`'s `run_once`) used to lock the
//! `UtilizationTracker` Mutex and hold it ACROSS the per-database loop, inside
//! which it read-latches tree nodes (`verify_node`, `gather_tree_lsns`).  The
//! tracker is a LEAF lock: the log-write observer takes it while a BIN latch is
//! held (`LogManager::notify_observer` ->
//! `UtilizationTrackerObserver::count_new_entry`), so the only legal direction
//! is tree-latch -> tracker.  Holding the tracker and then latching the tree is
//! the reverse — an ABBA inversion:
//!
//! * verify:      tracker(A) held  ->  wants tree-latch(B)
//! * checkpoint:  BIN-latch(B) held -> wants tracker(A) (via the observer)
//!
//! Captured in production as a 56-minute hang of
//! `crash_recovery_test::open_txn_spanning_checkpoint_recovers_correctly`
//! (verify in `reopen_db` deadlocked against the reopen checkpoint).
//!
//! JE never holds utilization state across the tree walk: `VerifyUtils.checkLsns`
//! (VerifyUtils.java:70-160) gathers the live tree LSNs FIRST with a
//! `SortedLSNTreeWalker`, THEN reads the utilization profile's obsolete offsets.
//!
//! These witnesses force the exact interleaving with a prewalk test hook (no
//! timing luck): verify is paused at the per-DB race window while a second
//! thread reproduces the checkpointer's legal order (tree write latch ->
//! tracker lock).  Pre-fix the tracker is held at the race window, so the second
//! thread blocks on it forever and verify (waiting for that thread) wedges.
//! Post-fix only an `Arc` clone is held, so the second thread completes and
//! verify proceeds.  Each test asserts completion within a 10 s bound and
//! FAILS (panics) rather than hanging the suite on the deadlock.

#![cfg(not(noxu_shuttle))]

use noxu_db::{
    Database, DatabaseConfig, Environment, EnvironmentConfig, VerifyConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const BOUND: Duration = Duration::from_secs(10);

/// Open a transactional env + one populated database so the tree has a resident
/// root node (for the witness thread's write latch) and some live LSNs.
fn open_populated(dir: &TempDir) -> (Environment, Database) {
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            // Keep the background daemons quiet so only OUR interleaving runs;
            // the daemon path is exercised by its own test below.
            .with_run_cleaner(false)
            .with_run_checkpointer(false)
            .with_run_verifier(false),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "abba",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    for i in 0..64u32 {
        let k = format!("k{i:04}");
        db.put(k.as_bytes(), k.as_bytes()).unwrap();
    }
    // Flush so the tree root is logged / resident and the tracker has entries.
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();
    (env, db)
}

/// Spawn the "checkpointer-order" witness thread: hold a real tree write latch
/// (the BIN-latch leg), then take the tracker lock (the leaf), then release.
/// Signals `b_done` on success.  Returns its JoinHandle (never joined on the
/// deadlock path — the test fails on the bound instead of hanging).
fn spawn_latch_then_tracker(
    env: &Environment,
    barrier: Arc<Barrier>,
    b_done: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let tracker =
        env.utilization_tracker_for_test().expect("rw env has a tracker");
    let root = env
        .root_tree_node_for_test("abba")
        .expect("populated db has a resident root");
    thread::spawn(move || {
        // Rendezvous with verify at the race window.
        barrier.wait();
        // Checkpointer's LEGAL order: tree latch (B) first, THEN tracker (A).
        let _node_guard = root.write();
        {
            let _tg = tracker.lock();
            // (The real observer would count a log entry here.)
        }
        drop(_node_guard);
        b_done.store(true, Ordering::Release);
    })
}

/// Wait up to `BOUND` for `done`; panic (FAIL) on timeout so the ABBA deadlock
/// surfaces as a hard failure instead of hanging the whole test binary.
fn await_or_fail(done: &AtomicBool, what: &str) {
    let deadline = Instant::now() + BOUND;
    while Instant::now() < deadline {
        if done.load(Ordering::Acquire) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "BUG-VERIFY-TRACKER-LATCH-ABBA: {what} did not complete within {:?} \
         (verify held the UtilizationTracker across the tree walk, deadlocking \
         the checkpointer-order thread)",
        BOUND
    );
}

/// `Environment::verify` path.
#[test]
fn verify_does_not_hold_tracker_across_tree_walk() {
    let dir = TempDir::new().unwrap();
    let (env, _db) = open_populated(&dir);

    let barrier = Arc::new(Barrier::new(2));
    let b_done = Arc::new(AtomicBool::new(false));
    let verify_done = Arc::new(AtomicBool::new(false));

    let bh = spawn_latch_then_tracker(
        &env,
        Arc::clone(&barrier),
        Arc::clone(&b_done),
    );

    // Run verify on its own thread so a deadlock does not block the test body.
    let verify_done2 = Arc::clone(&verify_done);
    let b_done_hook = Arc::clone(&b_done);
    let barrier_hook = Arc::clone(&barrier);
    let env = Arc::new(env);
    let env_v = Arc::clone(&env);
    let vh = thread::spawn(move || {
        let cfg = VerifyConfig::new();
        let hook = move || {
            // Race window: pre-fix the tracker is held here.  Release the
            // witness thread, then wait for it to acquire+release the tracker.
            // Pre-fix it never can (verify holds it) -> we block here forever
            // -> the test's bound trips.  Post-fix it completes immediately.
            barrier_hook.wait();
            await_or_fail(
                &b_done_hook,
                "checkpointer-order thread (verify path)",
            );
        };
        let r = env_v.verify_with_prewalk_hook_for_test(&cfg, &hook);
        assert!(r.is_ok(), "verify failed: {r:?}");
        verify_done2.store(true, Ordering::Release);
    });

    await_or_fail(&verify_done, "Environment::verify");
    // Only reached on the fixed path: both completed, so joins are safe.
    bh.join().unwrap();
    vh.join().unwrap();
    assert!(b_done.load(Ordering::Acquire));
}

/// `VerifyDaemon::run_once` path (ENV_RUN_VERIFIER default-on: a nightly verify
/// under checkpoint/write load can deadlock exactly the same way).
#[test]
fn verify_daemon_run_once_does_not_hold_tracker_across_tree_walk() {
    let dir = TempDir::new().unwrap();
    let (env, _db) = open_populated(&dir);

    let barrier = Arc::new(Barrier::new(2));
    let b_done = Arc::new(AtomicBool::new(false));
    let run_done = Arc::new(AtomicBool::new(false));

    let bh = spawn_latch_then_tracker(
        &env,
        Arc::clone(&barrier),
        Arc::clone(&b_done),
    );

    let env_impl = env.env_impl_arc_for_test();
    let run_done2 = Arc::clone(&run_done);
    let b_done_hook = Arc::clone(&b_done);
    let barrier_hook = Arc::clone(&barrier);
    let rh = thread::spawn(move || {
        let cfg = VerifyConfig::new();
        let hook = move || {
            barrier_hook.wait();
            await_or_fail(
                &b_done_hook,
                "checkpointer-order thread (daemon path)",
            );
        };
        noxu_db::verify_daemon::run_once_with_prewalk_hook_for_test(
            &env_impl, &cfg, &hook,
        );
        run_done2.store(true, Ordering::Release);
    });

    await_or_fail(&run_done, "VerifyDaemon::run_once");
    bh.join().unwrap();
    rh.join().unwrap();
    assert!(b_done.load(Ordering::Acquire));
}
