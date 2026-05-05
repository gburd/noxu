#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Latching primitives for Noxu DB.
//!
//! Port of `com.sleepycat.je.latch` - provides exclusive and shared/exclusive
//! latches used for B-tree node concurrency control.
//!
//! Latches are expected to be held for short, defined periods of time. No
//! deadlock detection is provided; it is the caller's responsibility to
//! sequence latch acquisition in an ordered fashion to avoid deadlocks.
//!
//! Key differences from JE's Java implementation:
//! - Uses `noxu_sync` for the underlying lock primitives (faster than std)
//! - Reentrancy prevention is enforced (matching JE behavior)
//! - Thread ownership tracking is always available via noxu_sync

mod exclusive;
mod shared;

pub use exclusive::{ExclusiveLatch, ExclusiveLatchGuard};
pub use shared::{SharedLatch, SharedLatchReadGuard, SharedLatchWriteGuard};

use std::fmt;
use std::time::Duration;

/// Default latch timeout.
pub const DEFAULT_LATCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Context information about a latch, used for debugging and diagnostics.
///
/// Port of `com.sleepycat.je.latch.LatchContext`. In JE this is an interface
/// implemented by IN to reduce per-latch memory overhead. In Rust we store
/// the name directly since the overhead is minimal.
#[derive(Debug, Clone)]
pub struct LatchContext {
    /// Name of this latch for debugging.
    pub name: String,
    /// Timeout for acquiring this latch.
    pub timeout: Duration,
}

impl LatchContext {
    /// Creates a new latch context with the given name and default timeout.
    pub fn new(name: impl Into<String>) -> Self {
        LatchContext { name: name.into(), timeout: DEFAULT_LATCH_TIMEOUT }
    }

    /// Creates a new latch context with the given name and timeout.
    pub fn with_timeout(name: impl Into<String>, timeout: Duration) -> Self {
        LatchContext { name: name.into(), timeout }
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
        let ctx = LatchContext::with_timeout("custom", Duration::from_millis(100));
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
        assert!(format!("{}", e1).contains("already held") || format!("{}", e1).contains("Latch already held"));

        let e2 = LatchError::NotHeld("bar".to_string());
        assert!(format!("{}", e2).contains("not held") || format!("{}", e2).contains("Latch not held"));

        let e3 = LatchError::Timeout("baz".to_string());
        assert!(format!("{}", e3).contains("timeout") || format!("{}", e3).contains("Latch timeout"));
    }

    #[test]
    fn test_latch_error_is_error() {
        use std::error::Error;
        let e = LatchError::Timeout("x".to_string());
        let _: &dyn Error = &e;
    }
}
