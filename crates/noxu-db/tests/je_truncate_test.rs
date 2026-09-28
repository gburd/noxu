//! JE TruncateTest ports — `Environment::truncate_database` (autocommit AND
//! transactional).
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/TruncateTest.java`.  `Environment::truncate_database`
//! now supports a transactional form: when a `Transaction` is passed, the
//! record count is returned synchronously but the physical tree replacement is
//! deferred to commit and rolled back on abort (see `environment.rs`).  The
//! transactional commit/abort variants (`testTruncateCommit`,
//! `testTruncateAbort`, and the `doTruncateAndAdd` env-truncate matrix) are
//! ported here and in `ddl_txn_abort_test.rs`.

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
    let cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    env.open_database(None, name, &cfg).unwrap()
}

fn ikey(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}

fn populate(db: &noxu_db::Database, env: &noxu_db::Environment, n: u32) {
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..n {
        db.put_in(&txn, ikey(i), ikey(i)).unwrap();
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
        db.put_in(&txn, ikey(i), ikey(i)).unwrap();
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
// Regression guard: was a bug (truncate_database was not durable — after a
// clean close+reopen, previously-truncated records reappeared). Fixed in
// commit b947b34; retained as a regression guard.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
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
    let mut c = db.open_cursor_in(&txn, None).unwrap();
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
    let s = db.get_into(None, ikey(0), &mut out).unwrap();
    assert!(!s);
}

// ──────────────────────────────────────────────────────────────────────────────
// TruncateTest.doTruncateAndAdd matrix (testEnvTruncateCommit,
// testEnvTruncateAbort, testEnvTruncateAutocommit, testEnvTruncateNoFirstInsert,
// testNoTxnEnvTruncateCommit)
//
// JE's `doTruncateAndAdd(transactional, step1, autoCommit, step3, abort, step5)`:
//   1. populate `step1` records (under `txn` if transactional)
//   2. optionally commit that txn (autoCommit)
//   3. env.truncateDatabase(txn, name) — asserts the returned count == step1
//   4. reopen the db (under `txn`), add `step3` records
//   5. abort or commit
//   6. assert the final record count == `step5`, both immediately and after a
//      clean close+reopen (recovery)
//
// Noxu adaptation: the `doTruncateAndAdd` helper drives all five configured
// invocations.  When `transactional` is false, `txn` is `None` throughout
// (auto-commit).  When `abort` is true the whole txn (populate + truncate +
// adds) is rolled back, which is why JE expects step5 == 0 for the abort case.
// ──────────────────────────────────────────────────────────────────────────────

fn do_truncate_and_add(
    transactional: bool,
    step1: u32,
    auto_commit: bool,
    step3: u32,
    abort: bool,
    step5: u32,
) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let name = "ttadd";

    let cfg = |create: bool| {
        EnvironmentConfig::new(path.clone())
            .with_allow_create(create)
            .with_transactional(transactional)
            // JE forces a split with NODE_MAX=6; keep it small so the truncate
            // spans real internal nodes.
            .with_node_max_entries(6)
    };
    let dbcfg = || {
        DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(transactional)
    };

    {
        let env = noxu_db::Environment::open(cfg(true)).unwrap();
        let db = env.open_database(None, name, &dbcfg()).unwrap();

        // Step 1: populate (under txn iff transactional).
        let mut txn = if transactional {
            Some(env.begin_transaction(None).unwrap())
        } else {
            None
        };
        for i in 0..step1 {
            match &txn {
                Some(t) => db.put_in(t, ikey(i), ikey(i)).unwrap(),
                None => db.put(ikey(i), ikey(i)).unwrap(),
            };
        }
        db.close().unwrap();

        // Step 2: possibly auto-commit the populate txn before truncating.
        if auto_commit && transactional {
            txn.take().unwrap().commit().unwrap();
        }

        // Step 3: truncate; the returned count must equal step1.
        let truncate_count =
            env.truncate_database(txn.as_ref(), name).unwrap();
        assert_eq!(
            truncate_count as u32, step1,
            "truncate must report the pre-truncate record count"
        );

        // Step 4: reopen and add step3 records.
        let db = env.open_database(txn.as_ref(), name, &dbcfg()).unwrap();
        for i in 0..step3 {
            match &txn {
                Some(t) => db.put_in(t, ikey(i), ikey(i)).unwrap(),
                None => db.put(ikey(i), ikey(i)).unwrap(),
            };
        }
        db.close().unwrap();

        // Step 5: abort or commit.
        if let Some(t) = txn {
            if abort {
                t.abort().unwrap();
            } else {
                t.commit().unwrap();
            }
        }

        // Verify the record count == step5 immediately.
        let db = env.open_database(None, name, &dbcfg()).unwrap();
        assert_eq!(
            db.count().unwrap() as u32,
            step5,
            "record count after truncate+add must equal step5"
        );
        db.close().unwrap();
    }

    // Verify step5 survives a clean close+reopen (recovery).
    let env = noxu_db::Environment::open(cfg(false)).unwrap();
    let db = env.open_database(None, name, &dbcfg()).unwrap();
    assert_eq!(
        db.count().unwrap() as u32,
        step5,
        "record count must survive a clean close+reopen (recovery)"
    );
}

// JE: TruncateTest.testEnvTruncateCommit
//
// ENGINE-BUG CANDIDATE (NEW-TRUNCATE-1): a transactional truncate followed by
// inserts in the SAME transaction, then commit, LOSES the inserts.  Noxu
// defers the physical tree replacement to a commit callback
// (`Environment::truncate_database` -> `register_commit_callback` ->
// `truncate_database_if_id`), and `Transaction::commit` runs that callback
// AFTER the transaction's own data-log writes are committed
// (`transaction.rs` commit path: commit data, THEN run commit callbacks).
// So the 150 inserts are committed into the pre-truncate tree and then wiped
// by the deferred truncate.  Expected (JE): the truncate installs a fresh
// empty tree immediately and the 150 inserts land in it -> final count 150.
// Control: `env_truncate_autocommit` (same records, but the truncate is
// auto-committed BEFORE the inserts) passes with 150, isolating the fault to
// the same-txn truncate-then-insert ordering.  Kept #[ignore]d — a faithful
// port that must not be weakened.
#[test]
#[ignore = "NEW-TRUNCATE-1: same-txn truncate-then-insert loses inserts on commit (deferred truncate ordered after inserts)"]
fn env_truncate_commit() {
    do_truncate_and_add(true, 256, false, 150, false, 150);
}

// JE: TruncateTest.testEnvTruncateAbort — aborting the txn rolls back the
// populate, the truncate, AND the adds, leaving zero records.
#[test]
fn env_truncate_abort() {
    do_truncate_and_add(true, 256, false, 150, true, 0);
}

// JE: TruncateTest.testEnvTruncateAutocommit — the populate is committed, then
// a fresh auto-commit truncate + 150 adds commit, leaving 150 records.
#[test]
fn env_truncate_autocommit() {
    do_truncate_and_add(true, 256, true, 150, false, 150);
}

// JE: TruncateTest.testEnvTruncateNoFirstInsert — truncating a never-populated
// db returns 0; the subsequent 150 adds commit, leaving 150.
//
// ENGINE-BUG CANDIDATE (NEW-TRUNCATE-1): same root cause as
// `env_truncate_commit` — the 150 same-txn inserts after the (0-count)
// truncate are wiped by the deferred truncate callback on commit.  Kept
// #[ignore]d.
#[test]
#[ignore = "NEW-TRUNCATE-1: same-txn truncate-then-insert loses inserts on commit (deferred truncate ordered after inserts)"]
fn env_truncate_no_first_insert() {
    do_truncate_and_add(true, 0, false, 150, false, 150);
}

// JE: TruncateTest.testNoTxnEnvTruncateCommit — the whole flow on a
// non-transactional env (auto-commit throughout) leaves 150 records.
#[test]
fn no_txn_env_truncate_commit() {
    do_truncate_and_add(false, 256, false, 150, false, 150);
}

// ──────────────────────────────────────────────────────────────────────────────
// TruncateTest.testTruncateAbort / testTruncateCommit / testTruncateCommitAutoTxn
//
// JE's `doTruncate(abort, useAutoTxn)`: populate NUM_RECS, truncate, then
// abort/commit; assert the final record count.  useAutoTxn=false + abort=true
// -> records survive (NUM_RECS); useAutoTxn=false + abort=false -> 0;
// useAutoTxn=true -> 0.
//
// The transactional commit/abort forms are also covered in
// `ddl_txn_abort_test.rs` (`truncate_database_under_txn_is_rolled_back_on_abort`
// / `truncate_database_under_txn_commits`).  This is the DB-populate flavour
// with a larger record set to force a tree with real internal nodes.
// ──────────────────────────────────────────────────────────────────────────────

// JE: TruncateTest.testTruncateAbort — transactional truncate then abort;
// the records must survive (the deferred tree replacement is rolled back).
#[test]
fn do_truncate_transactional_abort_preserves_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env, DB_NAME);
    populate(&db, &env, NUM_RECS);
    db.close().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let n = env.truncate_database(Some(&txn), DB_NAME).unwrap();
    assert_eq!(n as u32, NUM_RECS, "count is reported up front");
    txn.abort().unwrap();

    let db = open_db(&env, DB_NAME);
    assert_eq!(
        db.count().unwrap() as u32,
        NUM_RECS,
        "aborted truncate must leave all records intact"
    );
}

// JE: TruncateTest.testTruncateCommit — transactional truncate then commit; 0.
#[test]
fn do_truncate_transactional_commit_clears_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env, DB_NAME);
    populate(&db, &env, NUM_RECS);
    db.close().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let n = env.truncate_database(Some(&txn), DB_NAME).unwrap();
    assert_eq!(n as u32, NUM_RECS);
    txn.commit().unwrap();

    let db = open_db(&env, DB_NAME);
    assert_eq!(db.count().unwrap(), 0, "committed truncate clears all records");
}

// JE: TruncateTest.testTruncateCommitAutoTxn — auto-commit truncate; 0.
#[test]
fn do_truncate_autocommit_clears_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env, DB_NAME);
    populate(&db, &env, NUM_RECS);
    db.close().unwrap();

    let n = env.truncate_database(None, DB_NAME).unwrap();
    assert_eq!(n as u32, NUM_RECS);

    let db = open_db(&env, DB_NAME);
    assert_eq!(db.count().unwrap(), 0);
}
