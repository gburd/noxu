//! Regression tests for the Sprint 1 environment/transaction wiring
//! fixes (May 2026 API audit findings F1, F2, F3, F12).
//!
//! Each test in this file is a *behavioural* assertion, not a unit test:
//! it opens a real `Environment`, drives the public surface as a user
//! would, and asserts the documented contract.  Pre-fix, every test in
//! this file would fail.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Durability, Environment,
    EnvironmentConfig, OperationStatus, TransactionConfig,
};
use std::sync::Arc;
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

// ─── F2: read_uncommitted on TransactionConfig is honoured ───────────

#[test]
fn f2_read_uncommitted_sees_uncommitted_writes() {
    // A txn with `with_read_uncommitted(true)` must observe writes from
    // a concurrent uncommitted transaction (dirty read).  Pre-fix the
    // flag was silently dropped by `Environment::begin_transaction`, so
    // the reader took a normal read lock and either blocked on the
    // writer or timed out.
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "f2"));

    // Seed the key so there is a "before" value to read.
    let key = DatabaseEntry::from_data(b"k");
    let val_before = DatabaseEntry::from_data(b"before");
    db.put(None, &key, &val_before).unwrap();

    // Writer txn: writes a new value but does NOT commit yet.
    let writer_txn = env.begin_transaction(None, None).unwrap();
    let val_after = DatabaseEntry::from_data(b"after");
    db.put(Some(&writer_txn), &key, &val_after).unwrap();

    // Reader txn: read-uncommitted, should see the dirty write.
    let read_cfg = TransactionConfig::new().with_read_uncommitted(true);
    let reader_txn = env.begin_transaction(None, Some(&read_cfg)).unwrap();

    let mut data = DatabaseEntry::new();
    let key_lookup = DatabaseEntry::from_data(b"k");
    let status = db
        .get(Some(&reader_txn), &key_lookup, &mut data)
        .expect("dirty read must not block / error");
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(
        data.get_data(),
        Some(b"after".as_slice()),
        "read-uncommitted txn must see the writer's dirty value"
    );

    reader_txn.commit().unwrap();
    writer_txn.abort().unwrap();
    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}
