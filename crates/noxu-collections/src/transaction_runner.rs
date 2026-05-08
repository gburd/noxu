//! Transaction execution helper.
//!

use crate::error::{CollectionError, Result};
use noxu_db::{Environment, Transaction};

/// Runs a closure within a transaction with automatic commit/abort.
///
/// 
///
/// The `TransactionRunner` provides a convenient way to execute database
/// operations within a transaction. It handles:
/// - Creating the transaction
/// - Committing on success
/// - Aborting on error
/// - Retrying on deadlock (up to `max_retries` times)
///
/// # Example
/// ```ignore
/// use noxu_collections::TransactionRunner;
///
/// let runner = TransactionRunner::new(&env).with_max_retries(5);
/// let result = runner.run(|txn| {
///     // ... database operations using txn ...
///     Ok(42)
/// });
/// ```
pub struct TransactionRunner<'env> {
    /// Reference to the environment for creating transactions.
    env: &'env Environment,
    /// Maximum number of retries on deadlock.
    max_retries: u32,
}

impl<'env> TransactionRunner<'env> {
    /// Creates a new transaction runner.
    ///
    /// # Arguments
    /// * `env` - The environment to create transactions in
    ///
    /// The default maximum retry count is 10.
    pub fn new(env: &'env Environment) -> Self {
        TransactionRunner { env, max_retries: 10 }
    }

    /// Sets the maximum number of retries on deadlock.
    ///
    /// # Arguments
    /// * `max_retries` - Maximum retry count (0 means no retries)
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Returns the maximum number of retries.
    pub fn get_max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Runs the given closure within a transaction.
    ///
    /// The closure receives a reference to the transaction. On success,
    /// the transaction is committed and the closure's return value is
    /// returned. On error, the transaction is aborted.
    ///
    /// If the error is a deadlock (`NoxuError::DeadlockDetected`), the
    /// operation is retried up to `max_retries` times.
    ///
    /// # Arguments
    /// * `f` - The closure to execute within a transaction
    ///
    /// # Type Parameters
    /// * `F` - Closure type that takes a `&Transaction` and returns `Result<R>`
    /// * `R` - Return type of the closure
    pub fn run<F, R>(&self, f: F) -> Result<R>
    where
        F: Fn(&Transaction) -> Result<R>,
    {
        let mut retries = 0;

        loop {
            let txn = self.env.begin_transaction(None, None)?;

            match f(&txn) {
                Ok(result) => {
                    txn.commit().map_err(CollectionError::DatabaseError)?;
                    return Ok(result);
                }
                Err(e) => {
                    // Always abort on error
                    let _ = txn.abort();

                    // Check if this is a deadlock and we should retry
                    if is_deadlock(&e) && retries < self.max_retries {
                        retries += 1;
                        continue;
                    }

                    return Err(e);
                }
            }
        }
    }

    /// Runs the given closure without a transaction (auto-commit mode).
    ///
    /// This is useful when the environment is not transactional.
    /// The closure receives `None` as the transaction reference.
    ///
    /// # Arguments
    /// * `f` - The closure to execute
    pub fn run_without_txn<F, R>(&self, f: F) -> Result<R>
    where
        F: Fn() -> Result<R>,
    {
        f()
    }

    /// Returns a reference to the environment.
    pub fn environment(&self) -> &'env Environment {
        self.env
    }
}

/// Checks if a CollectionError wraps a deadlock.
fn is_deadlock(err: &CollectionError) -> bool {
    match err {
        CollectionError::DatabaseError(db_err) => {
            matches!(db_err, noxu_db::NoxuError::DeadlockDetected)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
    use tempfile::TempDir;

    fn setup_transactional_env() -> (TempDir, Environment) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();
        (temp_dir, env)
    }

    #[test]
    fn test_new_runner() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env);
        assert_eq!(runner.get_max_retries(), 10);
    }

    #[test]
    fn test_with_max_retries() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env).with_max_retries(5);
        assert_eq!(runner.get_max_retries(), 5);
    }

    #[test]
    fn test_run_success() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env);

        let result = runner.run(|_txn| Ok(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_run_with_db_operations() {
        let (_td, env) = setup_transactional_env();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();

        let runner = TransactionRunner::new(&env);

        let result = runner.run(|_txn| {
            let key = DatabaseEntry::from_bytes(b"key1");
            let val = DatabaseEntry::from_bytes(b"value1");
            db.put(None, &key, &val)?;
            Ok(())
        });
        assert!(result.is_ok());

        // Verify the data was stored
        let key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();
        let status = db.get(None, &key, &mut data).unwrap();
        assert_eq!(status, noxu_db::OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_run_error_aborts() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env);

        let result: Result<()> = runner.run(|_txn| {
            Err(CollectionError::IllegalState("test error".to_string()))
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_run_deadlock_retries() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env).with_max_retries(3);

        let call_count = std::sync::atomic::AtomicU32::new(0);

        let result = runner.run(|_txn| {
            let count =
                call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count < 2 {
                Err(CollectionError::DatabaseError(
                    noxu_db::NoxuError::DeadlockDetected,
                ))
            } else {
                Ok("success")
            }
        });

        assert_eq!(result.unwrap(), "success");
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[test]
    fn test_run_deadlock_exhausts_retries() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env).with_max_retries(2);

        let result: Result<()> = runner.run(|_txn| {
            Err(CollectionError::DatabaseError(
                noxu_db::NoxuError::DeadlockDetected,
            ))
        });

        assert!(result.is_err());
    }

    #[test]
    fn test_run_without_txn() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env);

        let result = runner.run_without_txn(|| Ok(99));
        assert_eq!(result.unwrap(), 99);
    }

    #[test]
    fn test_run_returns_value() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env);

        let result = runner.run(|_txn| Ok(vec![1, 2, 3]));
        assert_eq!(result.unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_environment_accessor() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env);
        assert!(runner.environment().is_transactional());
    }

    #[test]
    fn test_zero_retries() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env).with_max_retries(0);

        let call_count = std::sync::atomic::AtomicU32::new(0);

        let result: Result<()> = runner.run(|_txn| {
            call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(CollectionError::DatabaseError(
                noxu_db::NoxuError::DeadlockDetected,
            ))
        });

        assert!(result.is_err());
        // Should be called exactly once (no retries)
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn test_non_deadlock_error_no_retry() {
        let (_td, env) = setup_transactional_env();
        let runner = TransactionRunner::new(&env).with_max_retries(5);

        let call_count = std::sync::atomic::AtomicU32::new(0);

        let result: Result<()> = runner.run(|_txn| {
            call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(CollectionError::DatabaseError(noxu_db::NoxuError::Timeout))
        });

        assert!(result.is_err());
        // Non-deadlock errors should not be retried
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
