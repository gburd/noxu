//! Transaction execution helper with deadlock-aware retry and
//! exponential backoff.
//!
//! Wave 2B redesign (v1.6).  In v1.5 the `&Transaction` supplied to
//! the closure could not be threaded into any `Stored*` method
//! (every Stored* method ignored its txn argument because there was
//! no txn argument).  In v1.6 every Stored* method takes
//! `txn: Option<&Transaction>`, so the runner-managed txn is now
//! fully usable inside the closure:
//!
//! ```ignore
//! let runner = TransactionRunner::new(&env);
//! runner.run(|txn| {
//!     map.put(Some(txn), &k, &v)?;
//!     map.remove(Some(txn), &other_k)?;
//!     Ok(())
//! })?;
//! ```
//!
//! The runner retries on retryable errors (`LockConflict`,
//! `DeadlockDetected`, `LockTimeout`, …) with jittered exponential
//! backoff.  Default retry budget is 10 attempts; default backoff is
//! 10 ms base, 1 s ceiling, ±25% jitter.  All three are configurable.

use std::time::{Duration, Instant};

use noxu_db::{Environment, Transaction};

use crate::error::{CollectionError, Result};

/// Configuration for [`TransactionRunner`]'s retry / backoff loop.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Maximum number of retry attempts after the initial try.
    /// `0` means "no retries" — the closure is invoked at most once.
    pub max_retries: u32,
    /// Base backoff for retry 0 (the first retry).  Each subsequent
    /// retry doubles this up to `max_backoff`.
    pub base_backoff: Duration,
    /// Upper bound on per-retry sleep.
    pub max_backoff: Duration,
    /// Jitter as a fraction of the computed backoff.  `0.0` =
    /// deterministic; `0.25` = ±25%.
    pub jitter: f64,
}

impl RetryConfig {
    /// The Wave 2B default: 10 retries, 10 ms base, 1 s ceiling,
    /// ±25% jitter.
    pub const DEFAULT: RetryConfig = RetryConfig {
        max_retries: 10,
        base_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_secs(1),
        jitter: 0.25,
    };

    /// Computes the backoff for retry attempt `attempt` (zero-based).
    ///
    /// Pure function for testability.  `nanos` is a salt used to
    /// derive a small pseudo-random jitter — it does not need to be
    /// strongly random; it just decorrelates concurrent runners.
    pub fn backoff_for(&self, attempt: u32, nanos: u64) -> Duration {
        // Exponential growth, capped at max_backoff.
        let raw = self.base_backoff.saturating_mul(1u32 << attempt.min(20));
        let capped =
            if raw > self.max_backoff { self.max_backoff } else { raw };

        if self.jitter <= 0.0 {
            return capped;
        }

        // Map the salt onto the range [-jitter, +jitter] of the
        // capped backoff.  We do this in integer arithmetic to avoid
        // pulling in `f64` non-determinism: take the salt's low bits
        // mod 2N+1, subtract N, scale to capped*jitter.
        let nanos_capped = capped.as_nanos();
        let jitter_window = (nanos_capped as f64 * self.jitter).round() as i128;
        if jitter_window <= 0 {
            return capped;
        }
        let span = 2 * jitter_window + 1;
        let salted = (nanos as i128).rem_euclid(span);
        let offset = salted - jitter_window;
        let result_nanos =
            (nanos_capped as i128).saturating_add(offset).max(0) as u128;
        Duration::from_nanos(u64::try_from(result_nanos).unwrap_or(u64::MAX))
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Runs a closure within a transaction with automatic commit/abort
/// and deadlock-aware retry.
pub struct TransactionRunner<'env> {
    env: &'env Environment,
    retry: RetryConfig,
}

impl<'env> TransactionRunner<'env> {
    /// Creates a new runner with the [`RetryConfig::DEFAULT`] policy.
    pub fn new(env: &'env Environment) -> Self {
        TransactionRunner { env, retry: RetryConfig::DEFAULT }
    }

    /// Returns a reference to the environment.
    pub fn environment(&self) -> &'env Environment {
        self.env
    }

    /// Sets the maximum number of retries on a retryable error.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.retry.max_retries = max_retries;
        self
    }

    /// Sets the base backoff (the sleep before the first retry).
    pub fn with_base_backoff(mut self, base: Duration) -> Self {
        self.retry.base_backoff = base;
        self
    }

    /// Sets the upper bound on per-retry sleep.
    pub fn with_max_backoff(mut self, max: Duration) -> Self {
        self.retry.max_backoff = max;
        self
    }

    /// Sets the jitter fraction (0.0 .. 1.0).  Capped at 1.0.
    pub fn with_jitter(mut self, jitter: f64) -> Self {
        self.retry.jitter = jitter.clamp(0.0, 1.0);
        self
    }

    /// Replaces the entire retry config.
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry = config;
        self
    }

    /// Returns the runner's current retry budget.
    pub fn get_max_retries(&self) -> u32 {
        self.retry.max_retries
    }

    /// Returns the active retry config.
    pub fn retry_config(&self) -> RetryConfig {
        self.retry
    }

    /// Runs the closure within a transaction, retrying on retryable
    /// errors with exponential backoff.
    ///
    /// The closure receives a reference to a freshly-begun
    /// [`Transaction`].  In v1.6, this `&Transaction` can be passed
    /// straight into any `Stored*` method as
    /// `Some(txn)` — the operations participate in the runner's
    /// txn, with commit / abort / retry handled automatically.
    ///
    /// On success, the txn is committed and the closure's value is
    /// returned.  On error, the txn is aborted; if the error is
    /// retryable (`is_retryable()`), the runner sleeps for the
    /// configured backoff and tries again until the budget is
    /// exhausted, at which point the last error is returned.
    pub fn run<F, R>(&self, mut f: F) -> Result<R>
    where
        F: FnMut(&Transaction) -> Result<R>,
    {
        self.run_with_sleep(&mut f, std::thread::sleep)
    }

    /// Variant of [`Self::run`] that lets tests inject a fake sleep
    /// to avoid stalling CI.
    pub fn run_with_sleep<F, R, S>(&self, f: &mut F, mut sleep: S) -> Result<R>
    where
        F: FnMut(&Transaction) -> Result<R>,
        S: FnMut(Duration),
    {
        let mut attempt: u32 = 0;
        loop {
            let txn = self.env.begin_transaction(None)?;
            match f(&txn) {
                Ok(value) => {
                    txn.commit().map_err(CollectionError::DatabaseError)?;
                    return Ok(value);
                }
                Err(err) => {
                    let _ = txn.abort();
                    if attempt >= self.retry.max_retries || !is_retryable(&err)
                    {
                        return Err(err);
                    }
                    let nanos = jitter_salt();
                    let sleep_dur = self.retry.backoff_for(attempt, nanos);
                    sleep(sleep_dur);
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }

    /// Runs the closure without a transaction (auto-commit mode).
    ///
    /// The closure receives no txn handle; useful when the
    /// environment is non-transactional or when only auto-commit
    /// `Stored*` calls are used.
    pub fn run_without_txn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> Result<R>,
    {
        f()
    }
}

/// Returns whether the error should trigger a retry.
fn is_retryable(err: &CollectionError) -> bool {
    match err {
        CollectionError::DatabaseError(db_err) => db_err.is_retryable(),
        _ => false,
    }
}

/// Cheap, non-cryptographic salt for jitter calculation.  We avoid
/// pulling `rand` into `noxu-collections`'s dependency closure by
/// reading the wall-clock subsecond nanos.
fn jitter_salt() -> u64 {
    Instant::now().elapsed().as_nanos() as u64
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use noxu_db::{DatabaseConfig, EnvironmentConfig, NoxuError};
    use tempfile::TempDir;

    fn setup_env() -> (TempDir, Environment) {
        let td = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(td.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        (td, env)
    }

    #[test]
    fn defaults_match_wave_2b_spec() {
        let cfg = RetryConfig::DEFAULT;
        assert_eq!(cfg.max_retries, 10);
        assert_eq!(cfg.base_backoff, Duration::from_millis(10));
        assert_eq!(cfg.max_backoff, Duration::from_secs(1));
        assert!((cfg.jitter - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn backoff_grows_then_caps() {
        let cfg = RetryConfig {
            max_retries: 100,
            base_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(80),
            jitter: 0.0,
        };
        assert_eq!(cfg.backoff_for(0, 0), Duration::from_millis(10));
        assert_eq!(cfg.backoff_for(1, 0), Duration::from_millis(20));
        assert_eq!(cfg.backoff_for(2, 0), Duration::from_millis(40));
        assert_eq!(cfg.backoff_for(3, 0), Duration::from_millis(80));
        // Past the ceiling, stay at the ceiling.
        assert_eq!(cfg.backoff_for(10, 0), Duration::from_millis(80));
        assert_eq!(cfg.backoff_for(50, 0), Duration::from_millis(80));
    }

    #[test]
    fn backoff_jitter_within_bounds() {
        let cfg = RetryConfig {
            max_retries: 5,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            jitter: 0.25,
        };
        // For attempt 0 the capped backoff is 100ms.  ±25% means
        // the result is in [75ms, 125ms].
        for nanos in 0u64..1000 {
            let d = cfg.backoff_for(0, nanos);
            assert!(d >= Duration::from_millis(75), "got {:?}", d);
            assert!(d <= Duration::from_millis(125), "got {:?}", d);
        }
    }

    #[test]
    fn run_success_commits() {
        let (_td, env) = setup_env();
        let runner = TransactionRunner::new(&env);
        let result = runner.run(|_txn| Ok(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn run_aborts_on_error() {
        let (_td, env) = setup_env();
        let runner = TransactionRunner::new(&env);
        let result: Result<()> = runner
            .run(|_txn| Err(CollectionError::IllegalState("nope".to_string())));
        assert!(matches!(result, Err(CollectionError::IllegalState(_))));
    }

    #[test]
    fn run_retries_on_deadlock_with_synthetic_sleep() {
        let (_td, env) = setup_env();
        let runner = TransactionRunner::new(&env).with_max_retries(3);

        let calls = Arc::new(AtomicU32::new(0));
        let sleeps = Arc::new(AtomicU32::new(0));
        let calls_clone = Arc::clone(&calls);
        let sleeps_clone = Arc::clone(&sleeps);

        let mut closure = move |_t: &Transaction| -> Result<&'static str> {
            let n = calls_clone.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(CollectionError::DatabaseError(NoxuError::DeadlockDetected))
            } else {
                Ok("ok")
            }
        };

        let result = runner.run_with_sleep(&mut closure, |_d| {
            sleeps_clone.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(result.unwrap(), "ok");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(sleeps.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn run_exhausts_retry_budget() {
        let (_td, env) = setup_env();
        let runner = TransactionRunner::new(&env).with_max_retries(2);

        let calls = Arc::new(AtomicU32::new(0));
        let calls_clone = Arc::clone(&calls);

        let mut closure = move |_t: &Transaction| -> Result<()> {
            calls_clone.fetch_add(1, Ordering::SeqCst);
            Err(CollectionError::DatabaseError(NoxuError::DeadlockDetected))
        };

        let result = runner.run_with_sleep(&mut closure, |_d| {});
        assert!(matches!(
            result,
            Err(CollectionError::DatabaseError(NoxuError::DeadlockDetected)),
        ));
        // Initial attempt + 2 retries = 3 invocations.
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn run_does_not_retry_non_retryable_errors() {
        let (_td, env) = setup_env();
        let runner = TransactionRunner::new(&env).with_max_retries(5);

        let calls = Arc::new(AtomicU32::new(0));
        let calls_clone = Arc::clone(&calls);

        let mut closure = move |_t: &Transaction| -> Result<()> {
            calls_clone.fetch_add(1, Ordering::SeqCst);
            Err(CollectionError::IllegalState("non-retryable".to_string()))
        };

        let result = runner.run_with_sleep(&mut closure, |_d| {});
        assert!(matches!(result, Err(CollectionError::IllegalState(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn run_writes_through_typed_storedmap() {
        use noxu_bind::{IntBinding, StringBinding};

        use crate::stored_map::StoredMap;

        let (_td, env) = setup_env();
        let db = env
            .open_database(
                None,
                "runner_map",
                &DatabaseConfig::new().with_allow_create(true),
            )
            .unwrap();

        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);
        let runner = TransactionRunner::new(&env);

        runner
            .run(|txn| {
                map.put(Some(txn), &1, &"alpha".to_string())?;
                map.put(Some(txn), &2, &"beta".to_string())?;
                Ok(())
            })
            .unwrap();

        assert_eq!(map.get(None, &1).unwrap(), Some("alpha".to_string()));
        assert_eq!(map.get(None, &2).unwrap(), Some("beta".to_string()));
    }

    #[test]
    fn run_aborts_storedmap_writes_on_error() {
        use noxu_bind::{IntBinding, StringBinding};

        use crate::stored_map::StoredMap;

        let (_td, env) = setup_env();
        let db = env
            .open_database(
                None,
                "runner_abort",
                &DatabaseConfig::new().with_allow_create(true),
            )
            .unwrap();

        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);
        let runner = TransactionRunner::new(&env);

        let result: Result<()> = runner.run(|txn| {
            map.put(Some(txn), &1, &"set".to_string())?;
            // Closure decides to roll back.
            Err(CollectionError::IllegalState("rollback".to_string()))
        });
        assert!(result.is_err());
        // Aborted: nothing in the map.
        assert_eq!(map.get(None, &1).unwrap(), None);
    }

    #[test]
    fn run_without_txn_passes_through() {
        let (_td, env) = setup_env();
        let runner = TransactionRunner::new(&env);
        let r = runner.run_without_txn(|| Ok::<i32, CollectionError>(7));
        assert_eq!(r.unwrap(), 7);
    }

    #[test]
    fn with_jitter_clamps_to_unit_range() {
        let (_td, env) = setup_env();
        let runner =
            TransactionRunner::new(&env).with_jitter(2.0).with_jitter(-1.0);
        // Last setter wins; clamped to 0.0.
        assert!((runner.retry_config().jitter - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn classification_helper_matches_db_retryable() {
        assert!(is_retryable(&CollectionError::DatabaseError(
            NoxuError::DeadlockDetected
        )));
        assert!(is_retryable(&CollectionError::DatabaseError(
            NoxuError::LockConflict("x".to_string())
        )));
        assert!(!is_retryable(&CollectionError::IllegalState("x".to_string())));
        assert!(!is_retryable(&CollectionError::ReadOnly));
    }
}

// All tests live in `mod tests` above; nothing else needed at module scope.
