// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Integration tests for the wired JE latch knobs (`ENV_LATCH_TIMEOUT`,
//! `ENV_FORCED_YIELD`) exposed via [`noxu_latch::config`].
//!
//! These tests mutate the process-global latch config, so a single serializing
//! mutex keeps them from racing each other and from racing the latch
//! constructor.  They restore the historical defaults on exit.

use noxu_latch::{ExclusiveLatch, LatchContext, SharedLatch};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

static SERIAL: Mutex<()> = Mutex::new(());

/// Restore the "unset" defaults (JE `ENV_LATCH_TIMEOUT = 300_000`,
/// `ENV_FORCED_YIELD = false`) by installing the historical 5 s latch timeout
/// and clearing forced-yield.  We install a 5 s timeout explicitly because
/// `configure` cannot reproduce the pre-configure `UNSET` sentinel from
/// outside the crate; 5 s is `DEFAULT_LATCH_TIMEOUT`, so behaviour matches.
fn restore_defaults() {
    noxu_latch::configure(5_000, false);
}

/// ENV_LATCH_TIMEOUT: a deliberately-held exclusive latch causes a contending
/// acquire to FAIL (return `LatchError::Timeout`) within the configured
/// timeout, rather than blocking forever.
#[test]
fn env_latch_timeout_fails_held_latch_within_timeout() {
    let _s = SERIAL.lock().unwrap();
    // Configure a short 100 ms timeout (operator opted in to a non-default).
    noxu_latch::configure(100, false);

    // A latch constructed AFTER configure picks up the 100 ms default.
    let latch = Arc::new(ExclusiveLatch::new(LatchContext::new("timeout-me")));

    let barrier = Arc::new(Barrier::new(2));
    let holder = {
        let latch = latch.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let _g = latch.acquire().expect("holder acquires");
            barrier.wait(); // signal: held
            std::thread::sleep(Duration::from_millis(500)); // hold well past timeout
        })
    };

    barrier.wait(); // wait until the latch is held by the other thread
    let start = Instant::now();
    let result = latch.acquire();
    let elapsed = start.elapsed();

    assert!(result.is_err(), "expected LatchError::Timeout, got Ok");
    // It must have failed roughly at the timeout, NOT waited the full 500 ms
    // hold, and certainly not hung forever.
    assert!(
        elapsed < Duration::from_millis(400),
        "acquire should fail near the 100 ms timeout, took {:?}",
        elapsed
    );

    holder.join().unwrap();
    restore_defaults();
}

/// ENV_LATCH_TIMEOUT also applies to the shared latch's exclusive path.
#[test]
fn env_latch_timeout_fails_shared_latch_within_timeout() {
    let _s = SERIAL.lock().unwrap();
    noxu_latch::configure(100, false);
    let latch =
        Arc::new(SharedLatch::new(LatchContext::new("s-timeout"), false));

    let barrier = Arc::new(Barrier::new(2));
    let holder = {
        let latch = latch.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let _g = latch.acquire_exclusive().expect("holder acquires");
            barrier.wait();
            std::thread::sleep(Duration::from_millis(500));
        })
    };

    barrier.wait();
    let start = Instant::now();
    let result = latch.acquire_exclusive();
    assert!(result.is_err(), "expected timeout error");
    assert!(start.elapsed() < Duration::from_millis(400));

    holder.join().unwrap();
    restore_defaults();
}

/// ENV_FORCED_YIELD: when enabled, the injection point is exercised — a latch
/// acquire + release completes without hanging and the flag is observably set.
/// We can't deterministically observe the scheduler yield, but we CAN prove the
/// injection path is reached: with forced-yield on, a tight acquire/release
/// loop across threads still makes progress (every acquire hits `maybe_yield`),
/// and `noxu_latch::forced_yield()` reflects the configured state.
#[test]
fn env_forced_yield_injects_at_acquire_release() {
    let _s = SERIAL.lock().unwrap();
    noxu_latch::configure(5_000, true);
    assert!(
        noxu_latch::forced_yield(),
        "forced_yield flag must reflect configure(_, true)"
    );

    let latch = Arc::new(ExclusiveLatch::new(LatchContext::new("yield-me")));
    let count = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let latch = latch.clone();
            let count = count.clone();
            std::thread::spawn(move || {
                for _ in 0..50 {
                    // Each acquire hits maybe_yield() (post-grant) and each
                    // guard drop hits maybe_yield() (release) with the knob on.
                    let _g =
                        latch.acquire().expect("acquire under forced-yield");
                    count.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    // Progress was made under forced-yield — no deadlock, all iterations ran.
    assert_eq!(count.load(Ordering::SeqCst), 200);

    restore_defaults();
    assert!(!noxu_latch::forced_yield());
}

/// Non-breaking guard: with forced-yield OFF (the default), `forced_yield()` is
/// false and a plain acquire/release still works exactly as before.
#[test]
fn default_config_has_no_forced_yield() {
    let _s = SERIAL.lock().unwrap();
    restore_defaults();
    assert!(!noxu_latch::forced_yield());
    let latch = ExclusiveLatch::new(LatchContext::new("plain"));
    let g = latch.acquire().expect("plain acquire");
    drop(g);
}
