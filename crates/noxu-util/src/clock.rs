// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Injectable clock for deterministic simulation testing (DST).
//!
//! The engine reads time through a [`Clock`] trait object rather than calling
//! `std::time` directly at control-flow time sites (fsync timeout, lock
//! timeout, daemon wakeups, TTL expiry).  The default is [`RealClock`], which
//! delegates straight to the standard library — so **production behavior is
//! unchanged and DST is strictly opt-in**.  Under DST the harness installs a
//! [`SimClock`] whose time only advances when the harness calls
//! [`SimClock::advance`], making every timeout/expiry decision a pure function
//! of the simulated timeline.
//!
//! The monotonic source is a `u64` nanosecond tick rather than
//! [`std::time::Instant`] precisely so it can be controlled — `Instant` has no
//! public constructor and cannot be faked.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A source of time, abstracted so DST can substitute a simulated timeline.
///
/// All methods take `&self`; a `Clock` is shared as `Arc<dyn Clock>`.
pub trait Clock: Send + Sync {
    /// Wall-clock time in milliseconds since the Unix epoch.
    ///
    /// Used for timestamps and TTL expiry.  Under [`SimClock`] this is the
    /// simulated wall clock, which advances only via
    /// [`SimClock::advance`] / [`SimClock::set_unix_ms`].
    fn now_unix_ms(&self) -> u64;

    /// A monotonic nanosecond tick.
    ///
    /// Never decreases.  Used to measure elapsed time for timeouts.  Returns
    /// a bare `u64` (not [`std::time::Instant`]) so it is controllable under
    /// simulation.
    fn now_nanos(&self) -> u64;

    /// Sleep for `dur`.
    ///
    /// [`RealClock`] blocks the thread; [`SimClock`] advances the simulated
    /// monotonic and wall clocks instead of blocking (so daemon-loop sleeps
    /// become time-advance no-ops under DST).
    fn sleep(&self, dur: Duration);
}

/// The production clock: delegates directly to `std::time` / `std::thread`.
///
/// Zero overhead beyond the trait-object indirection; selecting this is
/// behaviorally identical to calling the standard library directly.
#[derive(Debug, Default, Clone, Copy)]
pub struct RealClock;

impl RealClock {
    /// Construct a `RealClock` as a shared trait object.
    pub fn arc() -> Arc<dyn Clock> {
        Arc::new(RealClock)
    }
}

impl Clock for RealClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn now_nanos(&self) -> u64 {
        // A process-lifetime monotonic anchor.  Instant has no public
        // constructor so we cannot expose it; nanos-since-anchor is enough for
        // elapsed-time math and never decreases.
        use std::sync::OnceLock;
        use std::time::Instant;
        static ANCHOR: OnceLock<Instant> = OnceLock::new();
        let anchor = *ANCHOR.get_or_init(Instant::now);
        anchor.elapsed().as_nanos() as u64
    }

    fn sleep(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}

/// A simulated clock whose time advances only when the harness tells it to.
///
/// Both the monotonic tick and the wall clock are backed by atomics so the
/// clock can be shared (`Arc<dyn Clock>`) across the engine's threads while
/// the harness drives time from the outside.  Under DST a thread that
/// `sleep`s simply advances the simulated clocks and returns immediately
/// rather than blocking — so daemon-loop timeouts and TTL expiry become a pure
/// function of the harness's `advance` calls.
#[derive(Debug)]
pub struct SimClock {
    /// Monotonic nanosecond tick.
    nanos: AtomicU64,
    /// Simulated wall clock in milliseconds since the Unix epoch.
    unix_ms: AtomicU64,
}

impl SimClock {
    /// Create a `SimClock` starting at the given wall-clock time (ms since
    /// epoch) with the monotonic tick at zero.
    pub fn new(start_unix_ms: u64) -> Self {
        SimClock {
            nanos: AtomicU64::new(0),
            unix_ms: AtomicU64::new(start_unix_ms),
        }
    }

    /// Create a `SimClock` as a shared trait object.
    pub fn arc(start_unix_ms: u64) -> Arc<dyn Clock> {
        Arc::new(SimClock::new(start_unix_ms))
    }

    /// Advance both the monotonic tick and the wall clock by `dur`.
    pub fn advance(&self, dur: Duration) {
        self.nanos.fetch_add(dur.as_nanos() as u64, Ordering::SeqCst);
        self.unix_ms.fetch_add(dur.as_millis() as u64, Ordering::SeqCst);
    }

    /// Set the simulated wall clock directly (does not move the monotonic
    /// tick).
    pub fn set_unix_ms(&self, unix_ms: u64) {
        self.unix_ms.store(unix_ms, Ordering::SeqCst);
    }
}

impl Clock for SimClock {
    fn now_unix_ms(&self) -> u64 {
        self.unix_ms.load(Ordering::SeqCst)
    }

    fn now_nanos(&self) -> u64 {
        self.nanos.load(Ordering::SeqCst)
    }

    fn sleep(&self, dur: Duration) {
        // Sleeping under simulation advances time instead of blocking.
        self.advance(dur);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_clock_is_monotonic() {
        let c = RealClock;
        let a = c.now_nanos();
        let b = c.now_nanos();
        assert!(b >= a);
        assert!(c.now_unix_ms() > 0);
    }

    #[test]
    fn sim_clock_only_moves_on_advance() {
        let c = SimClock::new(1_000);
        assert_eq!(c.now_nanos(), 0);
        assert_eq!(c.now_unix_ms(), 1_000);

        c.advance(Duration::from_millis(500));
        assert_eq!(c.now_unix_ms(), 1_500);
        assert_eq!(c.now_nanos(), 500_000_000);

        // Reading again without advancing does not move time.
        assert_eq!(c.now_unix_ms(), 1_500);
    }

    #[test]
    fn sim_sleep_advances_rather_than_blocks() {
        let c = SimClock::new(0);
        c.sleep(Duration::from_secs(3600));
        // Would block for an hour on RealClock; instant under sim.
        assert_eq!(c.now_unix_ms(), 3_600_000);
    }

    #[test]
    fn trait_object_swap() {
        // The same code runs against either clock — proves the seam is a
        // trait object, not a rewrite.
        fn elapsed_after_sleep(c: &dyn Clock) -> u64 {
            let start = c.now_nanos();
            c.sleep(Duration::from_millis(10));
            c.now_nanos().saturating_sub(start)
        }
        let sim = SimClock::new(0);
        assert_eq!(elapsed_after_sleep(&sim), 10_000_000);
        // RealClock just needs to not panic and be non-negative.
        let _ = elapsed_after_sleep(&RealClock);
    }
}
