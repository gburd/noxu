//! Configuration for manual checkpoint operations.
//!
//! Implements `CheckpointConfig`.

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
    /// been exceeded.  Equivalent to`CheckpointConfig.setForce(true)`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_config::DatabaseConfig;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use tempfile::TempDir;

    #[test]
    fn defaults_disable_every_threshold() {
        let c = CheckpointConfig::new();
        assert!(!c.force);
        assert_eq!(c.k_bytes, 0, "0 means the byte threshold is disabled");
        assert_eq!(c.minutes, 0, "0 means the time threshold is disabled");
        assert!(!c.minimize_recovery_time);
        assert_eq!(
            format!("{c:?}"),
            format!("{:?}", CheckpointConfig::default())
        );
    }

    #[test]
    fn builders_are_independent() {
        let c = CheckpointConfig::new()
            .with_force(true)
            .with_k_bytes(64)
            .with_minutes(5)
            .with_minimize_recovery_time(true);
        assert!(c.force);
        assert_eq!(c.k_bytes, 64);
        assert_eq!(c.minutes, 5);
        assert!(c.minimize_recovery_time);

        // Each builder in isolation must leave the others at their default.
        let d = CheckpointConfig::default();
        let only_k = CheckpointConfig::new().with_k_bytes(64);
        assert_eq!(only_k.k_bytes, 64);
        assert_eq!(only_k.force, d.force);
        assert_eq!(only_k.minutes, d.minutes);
        assert_eq!(only_k.minimize_recovery_time, d.minimize_recovery_time);
    }

    fn env_with_records() -> (TempDir, Environment) {
        let dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true)
                // Keep the checkpointer daemon out of the way so the only
                // checkpoints counted are the ones these tests request.
                .with_run_checkpointer(false),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "ckpt",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        for i in 0u16..32 {
            db.put(i.to_be_bytes(), b"v").unwrap();
        }
        db.close().unwrap();
        (dir, env)
    }

    fn checkpoints(env: &Environment) -> u64 {
        env.stats().unwrap().checkpoint.checkpoints
    }

    /// `k_bytes` is a *gate*, not a hint: with a threshold far larger than the
    /// log has grown since the last checkpoint, `checkpoint()` must decline to
    /// run. The whole config was previously a no-op, so a regression here
    /// would silently reintroduce unconditional checkpointing.
    #[test]
    fn k_bytes_threshold_suppresses_a_checkpoint_that_has_not_earned_one() {
        let (_d, env) = env_with_records();

        // Establish a baseline checkpoint so the "bytes since last" window is
        // small.
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        let after_force = checkpoints(&env);
        assert!(after_force > 0, "a forced checkpoint must actually run");

        // 1 GiB of log growth has certainly not happened since then.
        env.checkpoint(Some(&CheckpointConfig::new().with_k_bytes(1 << 20)))
            .unwrap();
        assert_eq!(
            checkpoints(&env),
            after_force,
            "an unmet k_bytes threshold must skip the checkpoint"
        );
    }

    /// `minutes` gates the same way, and `force` must override BOTH gates —
    /// that is the documented meaning of `force`.
    #[test]
    fn minutes_threshold_gates_and_force_overrides_it() {
        let (_d, env) = env_with_records();

        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        let baseline = checkpoints(&env);

        // An hour has not elapsed since the checkpoint a moment ago.
        env.checkpoint(Some(&CheckpointConfig::new().with_minutes(60)))
            .unwrap();
        assert_eq!(
            checkpoints(&env),
            baseline,
            "an unmet minutes threshold must skip the checkpoint"
        );

        // Same unmet thresholds, but forced: must run anyway.
        env.checkpoint(Some(
            &CheckpointConfig::new()
                .with_force(true)
                .with_minutes(60)
                .with_k_bytes(1 << 20),
        ))
        .unwrap();
        assert!(
            checkpoints(&env) > baseline,
            "force must override both the minutes and k_bytes gates"
        );
    }

    /// A threshold of 0 means "disabled", not "threshold of zero bytes that is
    /// trivially unmet". An all-default config must therefore still checkpoint.
    #[test]
    fn zero_thresholds_do_not_gate() {
        let (_d, env) = env_with_records();
        let before = checkpoints(&env);
        env.checkpoint(Some(&CheckpointConfig::new())).unwrap();
        assert!(
            checkpoints(&env) > before,
            "an all-default config must not gate the checkpoint"
        );

        // `None` must behave the same as an all-default config.
        let before = checkpoints(&env);
        env.checkpoint(None).unwrap();
        assert!(checkpoints(&env) > before);
    }

    /// `minimize_recovery_time` is documented as advisory: it selects a
    /// different invoker label but must not suppress the checkpoint.
    #[test]
    fn minimize_recovery_time_does_not_suppress_the_checkpoint() {
        let (_d, env) = env_with_records();
        let before = checkpoints(&env);
        env.checkpoint(Some(
            &CheckpointConfig::new().with_minimize_recovery_time(true),
        ))
        .unwrap();
        assert!(checkpoints(&env) > before);
    }

    #[test]
    fn checkpoint_is_rejected_on_a_closed_environment() {
        let (_d, env) = env_with_records();
        env.close().unwrap();
        assert!(env.checkpoint(Some(&CheckpointConfig::new())).is_err());
    }
}
