//! Runtime-mutable environment configuration.
//!
//! Implements `EnvironmentMutableConfig`.

use crate::durability::Durability;

/// The subset of environment properties that can be changed after the
/// environment has been opened.
///
/// Obtain via [`Environment::mutable_config`][crate::environment::Environment::mutable_config]
/// and apply via [`Environment::set_mutable_config`][crate::environment::Environment::set_mutable_config].
///
/// Implements `EnvironmentMutableConfig`.
///
/// # Example
/// ```ignore
/// let mut cfg = env.mutable_config()?;
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

    /// Cleaner minimum-utilization threshold (0-100%).  `None` means
    /// unchanged.
    ///
    /// Mutable at runtime (`noxu.cleaner.minUtilization` has
    /// `mutable = true`); a change is pushed to the running cleaner. Mirrors
    /// JE `EnvironmentConfig.CLEANER_MIN_UTILIZATION` re-read via
    /// `EnvConfigObserver`.
    pub cleaner_min_utilization: Option<u32>,
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

    /// Sets the cleaner minimum-utilization threshold (0-100%).
    pub fn with_cleaner_min_utilization(mut self, pct: u32) -> Self {
        self.cleaner_min_utilization = Some(pct);
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

#[cfg(test)]
mod mutable_config_roundtrip_tests {
    use super::*;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use tempfile::TempDir;

    fn open_env() -> (TempDir, Environment) {
        let dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        (dir, env)
    }

    /// Every `Option` builder must record `Some`, so that a caller who wants
    /// to change one knob does not accidentally leave it at the
    /// "unchanged" sentinel.
    #[test]
    fn option_builders_record_some_and_leave_siblings_unchanged() {
        let c = EnvironmentMutableConfig::new()
            .with_cache_size(1 << 20)
            .with_run_cleaner(false)
            .with_run_checkpointer(false)
            .with_run_evictor(false)
            .with_cleaner_min_utilization(37)
            .with_durability(crate::durability::Durability::COMMIT_NO_SYNC);
        assert_eq!(c.cache_size, Some(1 << 20));
        assert_eq!(c.run_cleaner, Some(false));
        assert_eq!(c.run_checkpointer, Some(false));
        assert_eq!(c.run_evictor, Some(false));
        assert_eq!(c.cleaner_min_utilization, Some(37));
        assert_eq!(
            c.durability,
            Some(crate::durability::Durability::COMMIT_NO_SYNC)
        );
        // The two timeouts were never touched, so they must still read as
        // "unchanged" rather than as an accidental Some(0).
        assert_eq!(c.lock_timeout_ms, None);
        assert_eq!(c.txn_timeout_ms, None);
    }

    /// `mutable_config()` -> mutate -> `set_mutable_config()` must actually
    /// change the environment's view of the knob. This is the round-trip the
    /// type exists for, and none of it was covered.
    #[test]
    fn mutable_config_roundtrip_applies_changes() {
        let (_d, mut env) = open_env();

        let before = env.mutable_config().unwrap();
        assert_eq!(
            before.lock_timeout_ms,
            Some(env.config().lock_timeout_ms),
            "mutable_config must report the live lock timeout"
        );

        let changed = EnvironmentMutableConfig::new()
            .with_lock_timeout_ms(Some(4_321))
            .with_txn_timeout_ms(Some(8_765))
            .with_cleaner_min_utilization(41)
            .with_run_cleaner(false);
        env.set_mutable_config(changed).unwrap();

        assert_eq!(env.config().lock_timeout_ms, 4_321);
        assert_eq!(env.config().txn_timeout_ms, 8_765);
        assert_eq!(env.config().cleaner_min_utilization, 41);
        assert!(!env.config().run_cleaner);

        // And the change must be observable through a fresh read.
        let after = env.mutable_config().unwrap();
        assert_eq!(after.lock_timeout_ms, Some(4_321));
        assert_eq!(after.txn_timeout_ms, Some(8_765));
        assert_eq!(after.cleaner_min_utilization, Some(41));
        assert_eq!(after.run_cleaner, Some(false));
    }

    /// `None` means "leave it alone" — the whole reason these fields are
    /// `Option`. Applying an all-default config must change nothing.
    #[test]
    fn applying_an_all_none_config_changes_nothing() {
        let (_d, mut env) = open_env();
        let snapshot = format!("{:?}", env.mutable_config().unwrap());

        env.set_mutable_config(EnvironmentMutableConfig::new()).unwrap();

        assert_eq!(
            format!("{:?}", env.mutable_config().unwrap()),
            snapshot,
            "an all-None mutable config must be a no-op"
        );
    }

    /// `Some(0)` must CLEAR a timeout, not read as "unchanged". This is the
    /// exact distinction the `Option` shape was introduced for (the older
    /// bare-`u64` shape used 0 as the unchanged sentinel and so could not
    /// express "no timeout").
    #[test]
    fn some_zero_clears_a_timeout_rather_than_meaning_unchanged() {
        let (_d, mut env) = open_env();

        env.set_mutable_config(
            EnvironmentMutableConfig::new()
                .with_lock_timeout_ms(Some(9_999))
                .with_txn_timeout_ms(Some(9_999)),
        )
        .unwrap();
        assert_eq!(env.config().lock_timeout_ms, 9_999);
        assert_eq!(env.config().txn_timeout_ms, 9_999);

        env.set_mutable_config(
            EnvironmentMutableConfig::new()
                .with_lock_timeout_ms(Some(0))
                .with_txn_timeout_ms(Some(0)),
        )
        .unwrap();
        assert_eq!(
            env.config().lock_timeout_ms,
            0,
            "Some(0) must clear the lock timeout"
        );
        assert_eq!(
            env.config().txn_timeout_ms,
            0,
            "Some(0) must clear the txn timeout"
        );
    }

    /// `cleaner_min_utilization` is a percentage; a caller handing in a value
    /// above 100 must be clamped rather than truncated into a nonsense `u8`.
    #[test]
    fn cleaner_min_utilization_is_clamped_to_a_percentage() {
        let (_d, mut env) = open_env();
        env.set_mutable_config(
            EnvironmentMutableConfig::new().with_cleaner_min_utilization(4_000),
        )
        .unwrap();
        assert_eq!(
            env.config().cleaner_min_utilization,
            100,
            "an out-of-range percentage must clamp to 100, not wrap"
        );
    }

    /// A closed environment must reject both halves of the round-trip rather
    /// than silently recording changes into a dead handle.
    #[test]
    fn mutable_config_is_rejected_on_a_closed_environment() {
        let (_d, mut env) = open_env();
        env.close().unwrap();
        assert!(env.mutable_config().is_err());
        assert!(
            env.set_mutable_config(EnvironmentMutableConfig::new()).is_err()
        );
    }

    /// `mutable_config()` deliberately reports `durability: None` even when the
    /// environment has a durability configured: the environment stores
    /// durability plus two legacy booleans, and there is no single value to
    /// report without picking one representation over the other. Pinning it so
    /// that whoever unifies them has to update this test on purpose rather
    /// than discovering the asymmetry in production.
    #[test]
    fn mutable_config_does_not_report_durability() {
        let (_d, env) = open_env();
        assert_eq!(
            env.mutable_config().unwrap().durability,
            None,
            "known asymmetry: durability is settable but not readable here"
        );
    }
}
