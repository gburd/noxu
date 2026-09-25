//! B7 / V3 regression: `remove_database` / `rename_database` /
//! `truncate_database` must honour their `txn` argument so that aborting the
//! transaction rolls back the DDL.
//!
//! JE parity: `Environment.removeDatabase/renameDatabase/truncateDatabase`
//! run under a `Locker` derived from the passed `Transaction`
//! (`Environment.java:988/1047/1125` -> `DbNameOperation.runOnce` ->
//! `LockerFactory.getWritableLocker(env, txn, ...)`). The NameLN mutation is
//! transactional and the physical `MapLN`/tree deletion is scheduled for
//! commit via `Txn.markDeleteAtTxnEnd(dbImpl, /*deleteAtCommit=*/true)`
//! (`DbTree.java:1214/1266`, `Txn.java:1481`). An abort therefore never
//! deletes the tree and never removes the NameLN -> the database survives.
//!
//! Before the fix these three operations silently ignored `_txn`, performed
//! the DDL immediately, and had no abort-undo, so aborting lost the database.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

fn open_env(dir: &TempDir, allow_create: bool) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(allow_create)
            .with_transactional(true),
    )
    .unwrap()
}

fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Create a database `name`, put `key`/`val`, and close the handle so DDL is
/// allowed (JE and Noxu both refuse remove/rename/truncate on open handles).
fn seed_db(env: &Environment, name: &str, key: &[u8], val: &[u8]) {
    let db = env.open_database(None, name, &db_cfg()).unwrap();
    db.put(key, val).unwrap();
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// remove_database
// ---------------------------------------------------------------------------

#[test]
fn remove_database_under_txn_is_rolled_back_on_abort() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "victim", b"k", b"v");

    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "victim").unwrap();
    txn.abort().unwrap();

    // Abort must roll back the removal: the database still exists.
    let names = env.database_names().unwrap();
    assert!(
        names.contains(&"victim".to_string()),
        "remove_database aborted: database must still exist, got {names:?}"
    );
    // ...and its data must be intact.
    let db = env
        .open_database(
            None,
            "victim",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"v".as_ref()));
}

#[test]
fn remove_database_auto_commit_still_works() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "gone", b"k", b"v");

    env.remove_database(None, "gone").unwrap();
    assert!(!env.database_names().unwrap().contains(&"gone".to_string()));
}

#[test]
fn remove_database_under_txn_commits() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "doomed", b"k", b"v");

    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "doomed").unwrap();
    txn.commit().unwrap();

    assert!(!env.database_names().unwrap().contains(&"doomed".to_string()));
}

// ---------------------------------------------------------------------------
// rename_database
// ---------------------------------------------------------------------------

#[test]
fn rename_database_under_txn_is_rolled_back_on_abort() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "old", b"k", b"v");

    let txn = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn), "old", "new").unwrap();
    txn.abort().unwrap();

    let names = env.database_names().unwrap();
    assert!(
        names.contains(&"old".to_string()),
        "rename aborted: original name must survive, got {names:?}"
    );
    assert!(
        !names.contains(&"new".to_string()),
        "rename aborted: new name must not exist, got {names:?}"
    );
    let db = env
        .open_database(
            None,
            "old",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(b"v".as_ref()));
}

#[test]
fn rename_database_auto_commit_still_works() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "before", b"k", b"v");

    env.rename_database(None, "before", "after").unwrap();
    let names = env.database_names().unwrap();
    assert!(!names.contains(&"before".to_string()));
    assert!(names.contains(&"after".to_string()));
}

#[test]
fn rename_database_under_txn_commits() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "src", b"k", b"v");

    let txn = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn), "src", "dst").unwrap();
    txn.commit().unwrap();

    let names = env.database_names().unwrap();
    assert!(!names.contains(&"src".to_string()));
    assert!(names.contains(&"dst".to_string()));
}

// ---------------------------------------------------------------------------
// truncate_database
// ---------------------------------------------------------------------------

#[test]
fn truncate_database_under_txn_is_rolled_back_on_abort() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "trunc", b"k", b"v");

    let txn = env.begin_transaction(None).unwrap();
    let removed = env.truncate_database(Some(&txn), "trunc").unwrap();
    assert_eq!(removed, 1, "truncate must report the pre-truncate count");
    txn.abort().unwrap();

    // Abort must roll back: the records are still there.
    let db = env
        .open_database(
            None,
            "trunc",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(
        db.get(b"k").unwrap().as_deref(),
        Some(b"v".as_ref()),
        "truncate aborted: original records must survive"
    );
    assert_eq!(db.count().unwrap(), 1);
}

#[test]
fn truncate_database_auto_commit_still_works() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "clear", b"k", b"v");

    let removed = env.truncate_database(None, "clear").unwrap();
    assert_eq!(removed, 1);

    let db = env
        .open_database(
            None,
            "clear",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(db.count().unwrap(), 0);
}

#[test]
fn truncate_database_under_txn_commits() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "wipe", b"k", b"v");

    let txn = env.begin_transaction(None).unwrap();
    let removed = env.truncate_database(Some(&txn), "wipe").unwrap();
    assert_eq!(removed, 1);
    txn.commit().unwrap();

    let db = env
        .open_database(
            None,
            "wipe",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(db.count().unwrap(), 0);
}
