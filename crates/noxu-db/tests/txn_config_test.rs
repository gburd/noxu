//! Tests that TransactionConfig fields are correctly propagated to the inner
//! Txn and Transaction handles.
//!
//! These tests verify the wiring between the public TransactionConfig API and
//! the internal lock/isolation machinery.

use std::sync::Arc;
use std::time::{Duration, Instant};

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    TransactionConfig,
};
use tempfile::TempDir;

fn open_env_and_db(dir: &TempDir) -> (Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config).unwrap();
    let db_config =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "test", &db_config).unwrap();
    (env, db)
}

/// Verify that `lock_timeout_ms` from TransactionConfig is reflected in the
/// Transaction handle's `get_lock_timeout()`.
#[test]
fn test_lock_timeout_propagated_from_config() {
    let dir = TempDir::new().unwrap();
    let (env, _db) = open_env_and_db(&dir);

    let config = TransactionConfig::new().with_lock_timeout_ms(2500);
    let txn = env.begin_transaction(Some(&config)).unwrap();
    assert_eq!(txn.get_lock_timeout(), 2500);
    txn.abort().unwrap();
}

/// Verify that `txn_timeout_ms` from TransactionConfig is reflected in the
/// Transaction handle's `get_txn_timeout()`.
#[test]
fn test_txn_timeout_propagated_from_config() {
    let dir = TempDir::new().unwrap();
    let (env, _db) = open_env_and_db(&dir);

    let config = TransactionConfig::new().with_txn_timeout_ms(8000);
    let txn = env.begin_transaction(Some(&config)).unwrap();
    assert_eq!(txn.get_txn_timeout(), 8000);
    txn.abort().unwrap();
}

/// Verify that `no_wait` config causes immediate lock failure rather than
/// blocking when contending with another writer.
#[test]
fn test_no_wait_causes_immediate_lock_failure() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir);
    let env = Arc::new(env);
    let db = Arc::new(db);

    let key = DatabaseEntry::from_bytes(b"contested");
    let val = DatabaseEntry::from_bytes(b"value1");

    // Writer 1: hold a write lock
    let txn1 = env.begin_transaction(None).unwrap();
    db.put_in(&txn1, &key, &val).unwrap();

    // Writer 2: try to write same key with no_wait — should fail immediately
    let config = TransactionConfig::new().with_no_wait(true);
    let txn2 = env.begin_transaction(Some(&config)).unwrap();

    let start = Instant::now();
    let result =
        db.put_in(&txn2, &key, &DatabaseEntry::from_bytes(b"value2"));
    let elapsed = start.elapsed();

    // Should have failed quickly (< 100ms), not after the default 500ms timeout
    assert!(
        elapsed < Duration::from_millis(100),
        "no_wait took {:?}, expected < 100ms",
        elapsed
    );
    assert!(result.is_err(), "expected lock error with no_wait");

    txn2.abort().unwrap();
    txn1.abort().unwrap();
}

/// Verify that a custom lock_timeout_ms bounds the wait time appropriately.
#[test]
fn test_lock_timeout_bounds_wait_time() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir);
    let env = Arc::new(env);
    let db = Arc::new(db);

    let key = DatabaseEntry::from_bytes(b"timeout_key");
    let val = DatabaseEntry::from_bytes(b"v");

    // Writer 1 holds the lock
    let txn1 = env.begin_transaction(None).unwrap();
    db.put_in(&txn1, &key, &val).unwrap();

    // Writer 2 with 50ms lock timeout
    let config = TransactionConfig::new().with_lock_timeout_ms(50);
    let txn2 = env.begin_transaction(Some(&config)).unwrap();

    let start = Instant::now();
    let result = db.put_in(&txn2, &key, &DatabaseEntry::from_bytes(b"v2"));
    let elapsed = start.elapsed();

    // Should timeout within ~50ms (+/- some jitter)
    assert!(
        elapsed < Duration::from_millis(200),
        "lock_timeout took {:?}, expected ~50ms",
        elapsed
    );
    assert!(result.is_err());

    txn2.abort().unwrap();
    txn1.abort().unwrap();
}

/// Verify that builder methods produce correct field values.
#[test]
fn test_config_builder_new_fields() {
    let config = TransactionConfig::new()
        .with_lock_timeout_ms(100)
        .with_txn_timeout_ms(5000)
        .with_serializable_isolation(true)
        .with_importunate(true)
        .with_local_write(true);

    assert_eq!(config.lock_timeout_ms, 100);
    assert_eq!(config.txn_timeout_ms, 5000);
    assert!(config.serializable_isolation);
    assert!(config.importunate);
    assert!(config.local_write);
}

/// Verify that mutator methods produce correct field values.
#[test]
fn test_config_mutator_new_fields() {
    let mut config = TransactionConfig::new();
    config.set_lock_timeout_ms(200);
    config.set_txn_timeout_ms(3000);
    config.set_serializable_isolation(true);
    config.set_importunate(true);
    config.set_local_write(true);

    assert_eq!(config.lock_timeout_ms, 200);
    assert_eq!(config.txn_timeout_ms, 3000);
    assert!(config.serializable_isolation);
    assert!(config.importunate);
    assert!(config.local_write);
}

/// Verify that default config has zeroed timeouts and false flags.
#[test]
fn test_default_config_new_fields() {
    let config = TransactionConfig::default();
    assert_eq!(config.lock_timeout_ms, 0);
    assert_eq!(config.txn_timeout_ms, 0);
    assert!(!config.serializable_isolation);
    assert!(!config.importunate);
    assert!(!config.local_write);
}
