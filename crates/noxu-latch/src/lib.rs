// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "7"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! Latching primitives for Noxu DB.
//!
//! Latching primitives — exclusive and shared/exclusive latches used for
//! B-tree node concurrency control.
//!
//! Latches are expected to be held for short, defined periods of time. No
//! deadlock detection is provided; it is the caller's responsibility to
//! sequence latch acquisition in an ordered fashion to avoid deadlocks.
//!
//! Key properties:
//! - Uses `noxu_sync` for the underlying lock primitives
//! - Reentrancy prevention is enforced (panics on reentrant acquire)
//! - Thread ownership tracking is always available via noxu_sync

mod exclusive;
mod shared;

pub use exclusive::{ExclusiveLatch, ExclusiveLatchGuard};
pub use shared::{SharedLatch, SharedLatchReadGuard, SharedLatchWriteGuard};

pub mod latch_order;

use std::fmt;
use std::time::Duration;

/// Default latch timeout.
pub const DEFAULT_LATCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Context information about a latch, used for debugging and diagnostics.
///
/// Stores the latch name, acquisition timeout, and an optional ordering
/// **rank** used by the debug-build latch-ordering assertion (see
/// [`latch_order`]).
#[derive(Debug, Clone)]
pub struct LatchContext {
    /// Name of this latch for debugging.
    pub name: String,
    /// Timeout for acquiring this latch.
    pub timeout: Duration,
    /// Ordering rank for the debug-build latch-ordering assertion.
    ///
    /// Latches must be acquired in **strictly increasing** rank order on any
    /// one thread.  A rank of `0` (the default) opts out of the ordering check
    /// (it neither asserts against the current top nor blocks a higher-ranked
    /// acquire), so existing unranked latches are unaffected.  This is a
    /// faithful analogue of JE's debug-only `Latch` level/rank
    /// (`LatchSupport`/`LatchTable` latch-ordering enforcement).
    pub rank: u32,
}

impl LatchContext {
    /// Creates a new latch context with the given name and default timeout.
    pub fn new(name: impl Into<String>) -> Self {
        LatchContext {
            name: name.into(),
            timeout: DEFAULT_LATCH_TIMEOUT,
            rank: 0,
        }
    }

    /// Creates a new latch context with the given name and timeout.
    pub fn with_timeout(name: impl Into<String>, timeout: Duration) -> Self {
        LatchContext { name: name.into(), timeout, rank: 0 }
    }

    /// Builder-style setter for the ordering [`rank`](Self::rank).
    pub fn with_rank(mut self, rank: u32) -> Self {
        self.rank = rank;
        self
    }
}

impl fmt::Display for LatchContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Errors that can occur during latch operations.
#[derive(Debug)]
pub enum LatchError {
    /// The latch is already held by the current thread (reentrancy detected).
    AlreadyHeld(String),
    /// The latch is not held by the current thread on release.
    NotHeld(String),
    /// The latch acquisition timed out.
    Timeout(String),
}

impl fmt::Display for LatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LatchError::AlreadyHeld(msg) => {
                write!(f, "Latch already held: {}", msg)
            }
            LatchError::NotHeld(msg) => write!(f, "Latch not held: {}", msg),
            LatchError::Timeout(msg) => write!(f, "Latch timeout: {}", msg),
        }
    }
}

impl std::error::Error for LatchError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latch_context_default_timeout() {
        let ctx = LatchContext::new("my-latch");
        assert_eq!(ctx.name, "my-latch");
        assert_eq!(ctx.timeout, DEFAULT_LATCH_TIMEOUT);
    }

    #[test]
    fn test_latch_context_with_timeout() {
        use std::time::Duration;
        let ctx =
            LatchContext::with_timeout("custom", Duration::from_millis(100));
        assert_eq!(ctx.name, "custom");
        assert_eq!(ctx.timeout, Duration::from_millis(100));
    }

    #[test]
    fn test_latch_context_display() {
        let ctx = LatchContext::new("test-display");
        assert_eq!(format!("{}", ctx), "test-display");
    }

    #[test]
    fn test_latch_error_display() {
        let e1 = LatchError::AlreadyHeld("foo".to_string());
        assert!(
            format!("{}", e1).contains("already held")
                || format!("{}", e1).contains("Latch already held")
        );

        let e2 = LatchError::NotHeld("bar".to_string());
        assert!(
            format!("{}", e2).contains("not held")
                || format!("{}", e2).contains("Latch not held")
        );

        let e3 = LatchError::Timeout("baz".to_string());
        assert!(
            format!("{}", e3).contains("timeout")
                || format!("{}", e3).contains("Latch timeout")
        );
    }

    #[test]
    fn test_latch_error_is_error() {
        use std::error::Error;
        let e = LatchError::Timeout("x".to_string());
        let _: &dyn Error = &e;
    }
}
