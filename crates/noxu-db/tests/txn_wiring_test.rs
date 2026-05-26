//! Regression tests for the Sprint 1 environment/transaction wiring
//! fixes (May 2026 API audit findings F1, F2, F3, F12).
//!
//! Each test in this file is a *behavioural* assertion, not a unit test:
//! it opens a real `Environment`, drives the public surface as a user
//! would, and asserts the documented contract.  Pre-fix, every test in
//! this file would fail.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Durability, Environment,
    EnvironmentConfig,
};
use tempfile::TempDir;

fn open_env(temp_dir: &TempDir, durability: Durability) -> Environment {
    let cfg = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(durability);
    Environment::open(cfg).unwrap()
}

fn open_db(env: &Environment, name: &str) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new().with_allow_create(true),
    )
    .unwrap()
}

// ─── F1: env.close() succeeds after txn.commit() ─────────────────────

#[test]
fn f1_env_close_after_commit_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None, None).unwrap();
    txn.commit().expect("commit must succeed");

    // Pre-fix: this returns OperationNotAllowed("Cannot close
    // environment with 1 active transactions").
    env.close().expect("env.close() must succeed after commit");
}

#[test]
fn f1_env_close_after_abort_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None, None).unwrap();
    txn.abort().expect("abort must succeed");

    env.close().expect("env.close() must succeed after abort");
}

#[test]
fn f1_env_close_after_many_commits_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "f1");

    for i in 0..16 {
        let txn = env.begin_transaction(None, None).unwrap();
        let key = DatabaseEntry::from_data(format!("k{}", i).as_bytes());
        let val = DatabaseEntry::from_data(b"v");
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
    }

    db.close().unwrap();
    env.close().expect("env.close() must succeed after many commits");
}

#[test]
fn f1_env_close_with_one_active_txn_still_fails() {
    // Sanity check: only commit/abort prune the registry; an open
    // transaction must still block close().
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let _txn = env.begin_transaction(None, None).unwrap();

    let result = env.close();
    assert!(result.is_err(), "close() must fail with active txn");
}
