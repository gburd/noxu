//! Runtime-mutable environment configuration.
//!
//! Implements `EnvironmentMutableConfig`.

use crate::db::durability::Durability;

/// The subset of environment properties that can be changed after the
/// environment has been opened.
///
/// Obtain via [`Environment::get_mutable_config`][crate::db::environment::Environment::get_mutable_config]
/// and apply via [`Environment::set_mutable_config`][crate::db::environment::Environment::set_mutable_config].
///
/// Implements `EnvironmentMutableConfig`.
///
/// # Example
/// ```ignore
/// let mut cfg = env.get_mutable_config()?;
/// cfg.cache_size = Some(256 * 1024 * 1024); // 256 MiB
/// env.set_mutable_config(cfg)?;
/// ```
#[derive(Clone, Debug, Default)]
pub struct EnvironmentMutableConfig {
    /// Override the B-tree cache size in bytes.  `None` means unchanged.
    ///
    /// Implements `EnvironmentMutableConfig.setCacheSize()`.
    pub cache_size: Option<usize>,

    /// Override the default transaction durability for this environment.
    /// `None` means unchanged.
    ///
    /// Implements `EnvironmentMutableConfig.setDurability()`.
    pub durability: Option<Durability>,

    /// If `true`, committed transactions do not flush to disk (no-sync).
    ///
    /// **Deprecated since 2.4.1** — use [`durability`][Self::durability]
    /// with `Durability::commit_no_sync()` instead.
    pub txn_no_sync: bool,

    /// If `true`, committed transactions flush to the OS buffer but do not
    /// call `fdatasync` (write-no-sync).
    ///
    /// **Deprecated since 2.4.1** — use [`durability`][Self::durability]
    /// with `Durability::commit_write_no_sync()` instead.
    pub txn_write_no_sync: bool,

    /// Enable or disable the cleaner daemon.  `None` means unchanged.
    pub run_cleaner: Option<bool>,

    /// Enable or disable the checkpointer daemon.  `None` means unchanged.
    pub run_checkpointer: Option<bool>,

    /// Enable or disable the evictor daemon.  `None` means unchanged.
    pub run_evictor: Option<bool>,

    /// Lock timeout in milliseconds.  `None` means unchanged.
    ///
    /// To explicitly clear a previously-configured timeout, set
    /// `Some(0)` (which JE interprets as "no timeout").  v1.5.0 used a
    /// `u64` with `0` as the unchanged sentinel which made it
    /// impossible to clear a timeout; see
    /// (Transaction-Env F19/F20).
    pub lock_timeout_ms: Option<u64>,

    /// Transaction timeout in milliseconds.  `None` means unchanged.
    ///
    /// `Some(0)` clears any previously-configured timeout.
    pub txn_timeout_ms: Option<u64>,
}

impl EnvironmentMutableConfig {
    /// Creates a new `EnvironmentMutableConfig` with no changes pending.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the cache size override.
    pub fn with_cache_size(mut self, bytes: usize) -> Self {
        self.cache_size = Some(bytes);
        self
    }

    /// Sets the durability override.
    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = Some(durability);
        self
    }

    /// Sets the `txn_no_sync` flag.
    ///
    /// **Deprecated** — use
    /// [`with_durability`][Self::with_durability] with
    /// `Durability::commit_no_sync()` instead.
    #[deprecated(
        since = "2.4.1",
        note = "use with_durability(Durability::commit_no_sync()) instead"
    )]
    pub fn with_txn_no_sync(mut self, no_sync: bool) -> Self {
        self.txn_no_sync = no_sync;
        self
    }

    /// Sets the `txn_write_no_sync` flag.
    ///
    /// **Deprecated** — use
    /// [`with_durability`][Self::with_durability] with
    /// `Durability::commit_write_no_sync()` instead.
    #[deprecated(
        since = "2.4.1",
        note = "use with_durability(Durability::commit_write_no_sync()) instead"
    )]
    pub fn with_txn_write_no_sync(mut self, write_no_sync: bool) -> Self {
        self.txn_write_no_sync = write_no_sync;
        self
    }

    /// Enables/disables the cleaner daemon.
    pub fn with_run_cleaner(mut self, run: bool) -> Self {
        self.run_cleaner = Some(run);
        self
    }

    /// Enables/disables the checkpointer daemon.
    pub fn with_run_checkpointer(mut self, run: bool) -> Self {
        self.run_checkpointer = Some(run);
        self
    }

    /// Enables/disables the evictor daemon.
    pub fn with_run_evictor(mut self, run: bool) -> Self {
        self.run_evictor = Some(run);
        self
    }

    /// Sets the lock timeout (milliseconds).
    ///
    /// Pass `Some(0)` to clear a previously-configured timeout, or
    /// `None` to leave it unchanged.
    pub fn with_lock_timeout_ms(mut self, ms: Option<u64>) -> Self {
        self.lock_timeout_ms = ms;
        self
    }

    /// Sets the transaction timeout (milliseconds).
    ///
    /// Pass `Some(0)` to clear a previously-configured timeout, or
    /// `None` to leave it unchanged.
    pub fn with_txn_timeout_ms(mut self, ms: Option<u64>) -> Self {
        self.txn_timeout_ms = ms;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_leaves_timeouts_unchanged() {
        let cfg = EnvironmentMutableConfig::new();
        assert_eq!(cfg.lock_timeout_ms, None);
        assert_eq!(cfg.txn_timeout_ms, None);
    }

    #[test]
    fn with_lock_timeout_some_zero_means_clear() {
        // Wave 1C audit cleanup (Transaction-Env F19/F20): the
        // previous `u64` shape used 0 as the unchanged sentinel and
        // could not distinguish "clear the timeout" from "unchanged".
        let cfg = EnvironmentMutableConfig::new().with_lock_timeout_ms(Some(0));
        assert_eq!(cfg.lock_timeout_ms, Some(0));
    }

    #[test]
    fn with_txn_timeout_none_means_unchanged() {
        let cfg = EnvironmentMutableConfig::new()
            .with_txn_timeout_ms(Some(1_000))
            .with_txn_timeout_ms(None);
        assert_eq!(cfg.txn_timeout_ms, None);
    }
}
