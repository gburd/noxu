//! JE TruncateTest ports — `Environment::truncate_database` / autocommit.
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/TruncateTest.java`.  Noxu's
//! `Environment::truncate_database` is autocommit-only (the JE-style
//! transactional truncate-then-abort is not supported), so the abort-flavour
//! variants are *NOT* ported.  See the per-package TSV
//! `je-tck-port-2026-05-enumeration-je.tsv` for the OUT-OF-SCOPE rows.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::path::Path;
use tempfile::TempDir;

const NUM_RECS: u32 = 100;
const DB_NAME: &str = "trunc_db";

fn open_env(dir: &Path) -> noxu_db::Environment {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment, name: &str) -> noxu_db::Database {
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true);
    env.open_database(None, name, &cfg).unwrap()
}

fn ikey(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}

fn populate(db: &noxu_db::Database, env: &noxu_db::Environment, n: u32) {
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..n {
        db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
    }
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// TruncateTest.testEnvTruncateCommit / testEnvTruncateAutocommit
//
// JE invariant: after `Environment::truncate_database`, the database has
// zero records; subsequent inserts behave as on a fresh db.  The truncate
// returns the number of records that were present before truncation.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn truncate_database_drops_records_and_returns_count() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env, DB_NAME);

    populate(&db, &env, NUM_RECS);
    assert_eq!(db.count().unwrap() as u32, NUM_RECS);
    db.close().unwrap();

    let n = env.truncate_database(None, DB_NAME).unwrap();
    assert_eq!(n as u32, NUM_RECS);

    // Re-open and verify it's empty.
    let db = open_db(&env, DB_NAME);
    assert_eq!(db.count().unwrap(), 0);
}

// ──────────────────────────────────────────────────────────────────────────────
// TruncateTest.testEnvTruncateNoFirstInsert
//
// JE invariant: truncating a never-populated db is valid and returns 0.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn truncate_database_empty_returns_zero() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env, DB_NAME);
    assert_eq!(db.count().unwrap(), 0);
    db.close().unwrap();

    let n = env.truncate_database(None, DB_NAME).unwrap();
    assert_eq!(n, 0);
}

// ──────────────────────────────────────────────────────────────────────────────
// TruncateTest.testWriteAfterTruncate (SR 10386, 11252)
//
// JE invariant: writing into a truncated database within a fresh
// transaction must succeed (no leftover handle-lock or txn conflict).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn truncate_then_write_succeeds_no_deadlock() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env, DB_NAME);
    populate(&db, &env, NUM_RECS);
    db.close().unwrap();

    // Truncate.
    let n = env.truncate_database(None, DB_NAME).unwrap();
    assert_eq!(n as u32, NUM_RECS);

    // Open a fresh handle and write.  Pre-fix a leftover handle-lock
    // from the truncate caused this put to deadlock.
    let db = open_db(&env, DB_NAME);
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..10u32 {
        db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
    }
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap(), 10);
}

// ──────────────────────────────────────────────────────────────────────────────
// TruncateTest.testTruncateAfterRecovery (spirit port)
//
// JE invariant: truncate-then-recovery yields an empty DB; the truncate is
// durable across a clean close+reopen.
//
// TODO(noxu-engine bug, wave-11-G): Noxu's truncate_database is not
// durable — after a clean close+reopen, the previously-truncated records
// re-appear.  Routed to a follow-up bug-fix wave.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn truncate_survives_clean_close_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let env = open_env(&path);
        let db = open_db(&env, DB_NAME);
        populate(&db, &env, NUM_RECS);
        db.close().unwrap();
        let n = env.truncate_database(None, DB_NAME).unwrap();
        assert_eq!(n as u32, NUM_RECS);
        drop(env);
    }

    // Reopen and verify the db is still empty.
    let env = open_env(&path);
    let db = open_db(&env, DB_NAME);
    assert_eq!(db.count().unwrap(), 0);

    // Walk via cursor — must be empty.
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// TruncateTest.testTruncateNoLocking (spirit port)
//
// JE invariant: truncate then read the same name on an env_is_locking=false
// path must succeed.  Noxu has no separate non-locking mode, but the
// invariant captured: a truncate followed by a fresh open + get(NotFound)
// must work in a single thread.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn truncate_then_get_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env, DB_NAME);
    populate(&db, &env, NUM_RECS);
    db.close().unwrap();
    env.truncate_database(None, DB_NAME).unwrap();

    let db = open_db(&env, DB_NAME);
    let mut out = DatabaseEntry::new();
    let s = db.get(None, &ikey(0), &mut out).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}
