// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Process-global latch configuration (JE `EnvironmentParams` latch knobs).
//!
//! JE configures latch behaviour with a small set of `EnvironmentParams` that
//! the `Latch` implementation reads directly (rather than threading a config
//! object through every `new Latch(...)` call site):
//!
//! - `ENV_LATCH_TIMEOUT` (`EnvironmentParams.ENV_LATCH_TIMEOUT`) — the maximum
//!   time a latch acquisition blocks before failing with a deadlock diagnostic
//!   instead of hanging forever.
//! - `ENV_FORCED_YIELD` (`EnvironmentParams.ENV_FORCED_YIELD`) — a **test-only**
//!   fairness-stress knob that injects `Thread.yield()` at latch
//!   acquire/release points to shake out latch-ordering races.
//!
//! Noxu mirrors JE's approach: rather than plumb a `LatchConfig` through the
//! dozens of ad-hoc `LatchContext::new(...)` sites in `noxu-tree` / `noxu-log`
//! (which would be a large cross-crate churn), the two tractable knobs live
//! here as process-global defaults installed once by `Environment::open` via
//! [`configure`].  [`LatchContext::new`](crate::LatchContext::new) reads
//! [`default_timeout`] at construction time, and the acquire/release paths
//! consult [`forced_yield`].
//!
//! **Default behaviour is byte-identical to before this module existed**: the
//! timeout defaults to [`DEFAULT_LATCH_TIMEOUT`](crate::DEFAULT_LATCH_TIMEOUT)
//! (5 s) and forced-yield defaults to `false` (no yields).  An `Environment`
//! that never calls [`configure`] — every unit test, every embedded use that
//! constructs latches directly — sees exactly the old constants.
//!
//! ## Fair (FIFO) latches — deliberately NOT here
//!
//! JE's `setFairLatches` (`ENV_FAIR_LATCHES`) selects a FIFO-ordered latch.
//! Noxu's latches are backed by `noxu-sync`'s futex primitives, which are
//! fundamentally **non-fair** (a new arrival can barge ahead of a queued
//! waiter) with no FIFO queue to toggle.  A faithful fair-latch mode is a
//! dedicated latch rewrite (a ticket/FIFO wait queue in `noxu-sync`), not a
//! flag flip, so it is intentionally left unimplemented rather than faked.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Sentinel meaning "use the compile-time [`DEFAULT_LATCH_TIMEOUT`] constant".
///
/// Stored in [`TIMEOUT_MS`] as its initial value so that, before any
/// [`configure`] call, [`default_timeout`] returns the historical 5 s constant
/// unchanged.
const UNSET_TIMEOUT: u64 = u64::MAX;

/// Global latch-acquisition timeout in milliseconds, or [`UNSET_TIMEOUT`].
///
/// A value of `0` means **no timeout** (block until acquired) — matching the
/// JE `ENV_LATCH_TIMEOUT = 0` semantics.
static TIMEOUT_MS: AtomicU64 = AtomicU64::new(UNSET_TIMEOUT);

/// Global forced-yield flag (JE `ENV_FORCED_YIELD`).  Default `false`.
static FORCED_YIELD: AtomicBool = AtomicBool::new(false);

/// A very large timeout used to approximate "block forever" while still going
/// through the timed-acquire path.  ~292 million years; effectively infinite
/// for a process lifetime, but a real `Duration` the futex primitives accept.
const EFFECTIVELY_FOREVER: Duration = Duration::from_secs(u64::MAX / 1000);

/// Installs the process-global latch configuration.
///
/// Called once by `Environment::open` from the translated `env_latch_timeout_ms`
/// / `env_forced_yield` config values.  Idempotent and last-writer-wins: a
/// second `Environment` in the same process overwrites the globals (JE's latch
/// params are likewise process-static).
///
/// * `timeout_ms` — `0` means "no timeout" (block, using an effectively-infinite
///   duration); any other value is the per-acquire timeout in milliseconds.
/// * `forced_yield` — when `true`, inject `std::thread::yield_now()` at latch
///   acquire/release points (test-only fairness stress).
pub fn configure(timeout_ms: u64, forced_yield: bool) {
    TIMEOUT_MS.store(timeout_ms, Ordering::Relaxed);
    FORCED_YIELD.store(forced_yield, Ordering::Relaxed);
}

/// Returns the currently-configured default latch timeout.
///
/// Before any [`configure`] call this is [`DEFAULT_LATCH_TIMEOUT`]. After
/// `configure(0, _)` it is [`EFFECTIVELY_FOREVER`] (no timeout). Otherwise it
/// is the configured millisecond value.
///
/// [`DEFAULT_LATCH_TIMEOUT`]: crate::DEFAULT_LATCH_TIMEOUT
pub fn default_timeout() -> Duration {
    match TIMEOUT_MS.load(Ordering::Relaxed) {
        UNSET_TIMEOUT => crate::DEFAULT_LATCH_TIMEOUT,
        0 => EFFECTIVELY_FOREVER,
        ms => Duration::from_millis(ms),
    }
}

/// Returns whether forced-yield injection is enabled (JE `ENV_FORCED_YIELD`).
#[inline]
pub fn forced_yield() -> bool {
    FORCED_YIELD.load(Ordering::Relaxed)
}

/// Injection point: yield the current thread iff forced-yield is enabled.
///
/// Called at latch acquire (post-grant) and release points.  A single relaxed
/// atomic load when disabled — the default — so it is effectively free in
/// production; only a test that sets `env_forced_yield(true)` pays the yield.
#[inline]
pub fn maybe_yield() {
    if forced_yield() {
        std::thread::yield_now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // These tests mutate process-global state; a serializing mutex keeps them
    // from racing each other (they can run on parallel test threads).
    static GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn reset() {
        TIMEOUT_MS.store(UNSET_TIMEOUT, Ordering::Relaxed);
        FORCED_YIELD.store(false, Ordering::Relaxed);
    }

    #[test]
    fn default_before_configure_is_historical_constant() {
        let _g = GUARD.lock().unwrap();
        reset();
        assert_eq!(default_timeout(), crate::DEFAULT_LATCH_TIMEOUT);
        assert!(!forced_yield());
        reset();
    }

    #[test]
    fn configure_zero_means_no_timeout() {
        let _g = GUARD.lock().unwrap();
        reset();
        configure(0, false);
        assert_eq!(default_timeout(), EFFECTIVELY_FOREVER);
        reset();
    }

    #[test]
    fn configure_nonzero_sets_millis() {
        let _g = GUARD.lock().unwrap();
        reset();
        configure(1234, false);
        assert_eq!(default_timeout(), Duration::from_millis(1234));
        reset();
    }

    #[test]
    fn configure_forced_yield_toggles_flag() {
        let _g = GUARD.lock().unwrap();
        reset();
        assert!(!forced_yield());
        configure(300_000, true);
        assert!(forced_yield());
        // maybe_yield must not panic when enabled.
        maybe_yield();
        reset();
    }
}
