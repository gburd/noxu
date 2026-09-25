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

// ---------------------------------------------------------------------------
// F-DDL-1 regression: deferred DDL must be identity-guarded, not name-guarded
//
// B7 deferred remove/rename/truncate to a commit callback that re-resolves the
// target BY NAME at commit time. If a concurrent txn removes and RECREATES a
// database under the same name (a brand-new DB with a new DatabaseId) and
// commits first, the deferred callback then destroys the *recreated* DB —
// silent destruction of a committed database.
//
// JE avoids this by binding `markDeleteAtTxnEnd` to the specific
// `DatabaseImpl` (the identity), not the name (`DbTree.java:1214`,
// `Txn.java:1481`). The fix captures the validated `DatabaseId` up front and
// no-ops the callback when the current DB under that name has a different id.
// ---------------------------------------------------------------------------

/// A deferred `remove` that commits AFTER a concurrent same-name recreate must
/// NOT destroy the brand-new committed database.
#[test]
fn deferred_remove_does_not_eat_a_concurrently_recreated_db() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "x", b"old", b"old-val");

    // txn1: defer remove of the ORIGINAL "x".
    let txn1 = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn1), "x").unwrap();

    // Concurrently: remove the original "x" and recreate a BRAND-NEW "x" with
    // fresh data, then commit that. This new "x" has a different DatabaseId.
    env.remove_database(None, "x").unwrap();
    {
        let db = env.open_database(None, "x", &db_cfg()).unwrap();
        db.put(b"new", b"new-val").unwrap();
        db.close().unwrap();
    }

    // Now txn1 commits its deferred remove — which targets the ORIGINAL "x"
    // (already gone), so it must NO-OP and leave the recreated "x" intact.
    txn1.commit().unwrap();

    let names = env.database_names().unwrap();
    assert!(
        names.contains(&"x".to_string()),
        "recreated database must still exist after deferred remove commits, \
         got {names:?}"
    );
    let db = env
        .open_database(
            None,
            "x",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(
        db.get(b"new").unwrap().as_deref(),
        Some(b"new-val".as_ref()),
        "recreated database's data must be intact (deferred remove must not \
         destroy a DB it never validated)"
    );
}

/// A deferred `rename` whose destination name collides with a concurrently
/// recreated DB, and whose source was recreated, must NOT destroy the newer DB
/// under the source name.
#[test]
fn deferred_rename_does_not_eat_a_concurrently_recreated_source() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "src", b"old", b"old-val");

    // txn1: defer rename src -> dst.
    let txn1 = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn1), "src", "dst").unwrap();

    // Concurrently: remove the original "src" and recreate a brand-new "src"
    // with fresh data (new DatabaseId), commit immediately.
    env.remove_database(None, "src").unwrap();
    {
        let db = env.open_database(None, "src", &db_cfg()).unwrap();
        db.put(b"new", b"new-val").unwrap();
        db.close().unwrap();
    }

    // txn1 commits its deferred rename — it targets the ORIGINAL "src" (gone),
    // so it must NO-OP: the recreated "src" survives, no "dst" is created.
    txn1.commit().unwrap();

    let names = env.database_names().unwrap();
    assert!(
        names.contains(&"src".to_string()),
        "recreated source database must still exist after deferred rename \
         commits, got {names:?}"
    );
    let db = env
        .open_database(
            None,
            "src",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(
        db.get(b"new").unwrap().as_deref(),
        Some(b"new-val".as_ref()),
        "recreated source's data must be intact (deferred rename must not \
         relocate a DB it never validated)"
    );
}

/// A deferred `truncate` that commits AFTER a concurrent same-name recreate
/// must NOT wipe the brand-new committed database.
#[test]
fn deferred_truncate_does_not_wipe_a_concurrently_recreated_db() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "t", b"old", b"old-val");

    // txn1: defer truncate of the ORIGINAL "t" (returns count up front).
    let txn1 = env.begin_transaction(None).unwrap();
    let removed = env.truncate_database(Some(&txn1), "t").unwrap();
    assert_eq!(removed, 1, "up-front count is the original DB's count");

    // Concurrently: remove the original "t" and recreate a brand-new "t" with
    // fresh data (new DatabaseId), commit immediately.
    env.remove_database(None, "t").unwrap();
    {
        let db = env.open_database(None, "t", &db_cfg()).unwrap();
        db.put(b"new", b"new-val").unwrap();
        db.close().unwrap();
    }

    // txn1 commits its deferred truncate — targets the ORIGINAL "t" (gone), so
    // it must NO-OP: the recreated "t" keeps its data.
    txn1.commit().unwrap();

    let db = env
        .open_database(
            None,
            "t",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(
        db.get(b"new").unwrap().as_deref(),
        Some(b"new-val".as_ref()),
        "recreated database's data must survive (deferred truncate must not \
         wipe a DB it never validated)"
    );
    assert_eq!(db.count().unwrap(), 1);
}
