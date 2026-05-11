//! Configuration for manual checkpoint operations.
//!
//! Mirrors JE's `CheckpointConfig`.

/// Specifies the attributes of a checkpoint operation invoked via
/// [`Environment::checkpoint`][crate::environment::Environment::checkpoint].
///
/// # Defaults
///
/// All thresholds default to 0 (disabled) and `force = false`.  If all
/// thresholds are 0 and `force = false`, calling `checkpoint()` still runs
/// a checkpoint subject to normal dirty-node conditions.
#[derive(Clone, Debug, Default)]
pub struct CheckpointConfig {
    /// If `true`, force a checkpoint regardless of whether thresholds have
    /// been exceeded.  Equivalent to JE `CheckpointConfig.setForce(true)`.
    pub force: bool,
    /// Run a checkpoint if more than this many kibibytes of log data have
    /// been written since the last checkpoint.  `0` means disabled.
    pub k_bytes: u32,
    /// Run a checkpoint if more than this many minutes have elapsed since
    /// the last checkpoint.  `0` means disabled.
    pub minutes: u32,
    /// If `true`, perform a full checkpoint that minimises future recovery
    /// time (writes all dirty nodes, not just the minimum required).
    pub minimize_recovery_time: bool,
}

impl CheckpointConfig {
    /// Creates a `CheckpointConfig` with all defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder: set `force`.
    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    /// Builder: set `k_bytes` threshold.
    pub fn with_k_bytes(mut self, k_bytes: u32) -> Self {
        self.k_bytes = k_bytes;
        self
    }

    /// Builder: set `minutes` threshold.
    pub fn with_minutes(mut self, minutes: u32) -> Self {
        self.minutes = minutes;
        self
    }

    /// Builder: set `minimize_recovery_time`.
    pub fn with_minimize_recovery_time(mut self, minimize: bool) -> Self {
        self.minimize_recovery_time = minimize;
        self
    }
}
