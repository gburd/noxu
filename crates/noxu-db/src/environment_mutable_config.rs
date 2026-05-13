//! Runtime-mutable environment configuration.
//!
//! Implements `EnvironmentMutableConfig`.

use crate::durability::Durability;

/// The subset of environment properties that can be changed after the
/// environment has been opened.
///
/// Obtain via [`Environment::get_mutable_config`][crate::environment::Environment::get_mutable_config]
/// and apply via [`Environment::set_mutable_config`][crate::environment::Environment::set_mutable_config].
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
    /// Deprecated in favour of `durability`; retained for compatibility.
    pub txn_no_sync: bool,

    /// If `true`, committed transactions flush to the OS buffer but do not
    /// call `fdatasync` (write-no-sync).
    ///
    /// Deprecated in favour of `durability`; retained for compatibility.
    pub txn_write_no_sync: bool,

    /// Enable or disable the cleaner daemon.  `None` means unchanged.
    pub run_cleaner: Option<bool>,

    /// Enable or disable the checkpointer daemon.  `None` means unchanged.
    pub run_checkpointer: Option<bool>,

    /// Enable or disable the evictor daemon.  `None` means unchanged.
    pub run_evictor: Option<bool>,

    /// Lock timeout in milliseconds.  `0` means unchanged.
    pub lock_timeout_ms: u64,

    /// Transaction timeout in milliseconds.  `0` means unchanged.
    pub txn_timeout_ms: u64,
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
    pub fn with_txn_no_sync(mut self, no_sync: bool) -> Self {
        self.txn_no_sync = no_sync;
        self
    }

    /// Sets the `txn_write_no_sync` flag.
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
    pub fn with_lock_timeout_ms(mut self, ms: u64) -> Self {
        self.lock_timeout_ms = ms;
        self
    }

    /// Sets the transaction timeout (milliseconds).
    pub fn with_txn_timeout_ms(mut self, ms: u64) -> Self {
        self.txn_timeout_ms = ms;
        self
    }
}
