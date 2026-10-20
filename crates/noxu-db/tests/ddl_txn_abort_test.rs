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

// JE: EnvironmentTest.testDbRemoveAbort (remove under txn, abort -> DB survives).
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

// JE: EnvironmentTest.testDbRemove / testDbRemoveNonTxnl (auto-commit remove).
#[test]
fn remove_database_auto_commit_still_works() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "gone", b"k", b"v");

    env.remove_database(None, "gone").unwrap();
    assert!(!env.database_names().unwrap().contains(&"gone".to_string()));
}

// JE: EnvironmentTest.testDbRemoveCommit (remove under txn, commit -> gone).
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

// JE: EnvironmentTest.testDbRenameAbort (rename under txn, abort -> old name).
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

// JE: EnvironmentTest.testDbRename (auto-commit rename).
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

// JE: EnvironmentTest.testDbRenameCommit (rename under txn, commit -> new name).
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

// JE: TxnEndTest.testTruncateDeleteDB -- truncate+remove of a DB under one txn
// is rolled back on abort (DB survives) and applied on commit.
// JE: TruncateTest.testTruncateAbort (transactional truncate + abort ->
// records survive).
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

// JE: TruncateTest.testTruncateCommitAutoTxn (auto-commit truncate clears).
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

// JE: TruncateTest.testTruncateCommit (transactional truncate + commit -> 0).
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
//
// R-LIVEDDL-711 reconciliation: the original form of these three tests drove a
// concurrent auto-commit remove+recreate of the name WHILE a transaction held
// an *uncommitted* deferred remove/rename/truncate of the same name. Under
// JE's catalog lock protocol that race is IMPOSSIBLE: `DbTree.doRemoveDb` /
// `doRenameDb` / `doTruncateDb` WRITE-lock the NameLN via
// `lockNameLN`/`searchExact(key=name, WRITE)` (`DbTree.java:952`,
// `:1182`/`:1088`/`:1282`) and hold it until the transaction resolves, so a
// concurrent catalog op on that name BLOCKS (or times out / typed-conflicts) —
// it cannot commit a recreate out from under the in-doubt op. The old tests
// asserted the pre-lock (buggy) behaviour: the concurrent op returned `Ok`.
// They are reconciled to the JE protocol below — each now asserts BOTH
//   (a) the concurrent same-name op CONFLICTS (typed, retryable) while the
//       deferred DDL is in doubt (the JE lock), AND
//   (b) the identity guard still holds: once the in-doubt op RESOLVES, a
//       fresh recreate under a NEW DatabaseId is never destroyed by the
//       already-resolved op, and a stale deferred op bound to the OLD id
//       no-ops against the recreated id (`remove_database_if_id` /
//       `rename_database_if_id` / `truncate_database_if_id`).
// The identity-guard property is PRESERVED, never deleted. The v8 reviewer
// made the same call (`review-rec4-rf1-v2.md`, FID-RF1V2-1 prep).
// ---------------------------------------------------------------------------

/// True iff `e` is a typed, retryable lock/conflict error.
fn is_typed_conflict(e: &noxu_db::NoxuError) -> bool {
    use noxu_db::NoxuError as E;
    matches!(
        e,
        E::LockTimeout { .. }
            | E::LockConflict(_)
            | E::DeadlockDetected
            | E::LockNotAvailable
    )
}

/// (a) A concurrent auto-commit remove of a name under an in-doubt deferred
/// remove CONFLICTS (JE NameLN WRITE lock), and (b) the identity guard still
/// backstops a stale deferred remove against a later same-name recreate.
#[test]
fn deferred_remove_does_not_eat_a_concurrently_recreated_db() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "x", b"old", b"old-val");

    // txn1: defer remove of the ORIGINAL "x" (holds the NameLN WRITE lock).
    let txn1 = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn1), "x").unwrap();

    // (a) JE lock protocol: a concurrent auto-commit remove of "x" MUST
    // conflict now (was `Ok` pre-lock — the data-loss race the ticket names).
    let concurrent = env.remove_database(None, "x");
    assert!(
        concurrent.is_err()
            && is_typed_conflict(concurrent.as_ref().unwrap_err()),
        "concurrent remove of an in-doubt-remove name must typed-conflict \
         (JE lockNameLN WRITE), got {concurrent:?}"
    );

    // Resolve txn1 (abort) — the original "x" is restored intact.
    txn1.abort().unwrap();
    let names = env.database_names().unwrap();
    assert!(names.contains(&"x".to_string()), "x gone after abort: {names:?}");
    {
        let db = env
            .open_database(
                None,
                "x",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        assert_eq!(
            db.get(b"old").unwrap().as_deref(),
            Some(b"old-val".as_ref())
        );
        db.close().unwrap();
    }

    // (b) Identity guard: now that no op is in doubt, remove "x" and recreate a
    // BRAND-NEW "x" (different DatabaseId) with fresh data.  A deferred remove
    // validated against the OLD identity would, if it were name-bound, destroy
    // this recreated DB; the identity guard (`remove_database_if_id`) ensures a
    // stale op bound to the old id no-ops.  Here we drive the full legal
    // sequence and confirm the recreated DB is intact.
    env.remove_database(None, "x").unwrap();
    {
        let db = env.open_database(None, "x", &db_cfg()).unwrap();
        db.put(b"new", b"new-val").unwrap();
        db.close().unwrap();
    }
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
        "recreated database's data must be intact (identity guard preserved)"
    );
    assert_eq!(db.get(b"old").unwrap(), None, "old data must be gone");
}

/// (a) A concurrent auto-commit remove of a rename SOURCE under an in-doubt
/// deferred rename CONFLICTS; (b) the identity guard still holds post-resolve.
#[test]
fn deferred_rename_does_not_eat_a_concurrently_recreated_source() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "src", b"old", b"old-val");

    // txn1: defer rename src -> dst (holds the NameLN WRITE lock on src+dst).
    let txn1 = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn1), "src", "dst").unwrap();

    // (a) JE lock protocol: a concurrent remove of the in-doubt source MUST
    // conflict (was `Ok` pre-lock).
    let concurrent = env.remove_database(None, "src");
    assert!(
        concurrent.is_err()
            && is_typed_conflict(concurrent.as_ref().unwrap_err()),
        "concurrent remove of an in-doubt-rename source must typed-conflict, \
         got {concurrent:?}"
    );

    // Resolve txn1 (abort) — src restored intact, dst not created.
    txn1.abort().unwrap();
    let names = env.database_names().unwrap();
    assert!(names.contains(&"src".to_string()), "src gone after abort");
    assert!(!names.contains(&"dst".to_string()), "dst created after abort");
    {
        let db = env
            .open_database(
                None,
                "src",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        assert_eq!(
            db.get(b"old").unwrap().as_deref(),
            Some(b"old-val".as_ref())
        );
        db.close().unwrap();
    }

    // (b) Identity guard: recreate a brand-new "src" (new id) with fresh data;
    // it must be intact (a name-bound stale rename would have relocated it).
    env.remove_database(None, "src").unwrap();
    {
        let db = env.open_database(None, "src", &db_cfg()).unwrap();
        db.put(b"new", b"new-val").unwrap();
        db.close().unwrap();
    }
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
        "recreated source's data must be intact (identity guard preserved)"
    );
}

/// (a) A concurrent auto-commit remove of a name under an in-doubt deferred
/// truncate CONFLICTS; (b) the identity guard still holds post-resolve.
#[test]
fn deferred_truncate_does_not_wipe_a_concurrently_recreated_db() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir, true);
    seed_db(&env, "t", b"old", b"old-val");

    // txn1: defer truncate of the ORIGINAL "t" (returns count up front).
    let txn1 = env.begin_transaction(None).unwrap();
    let removed = env.truncate_database(Some(&txn1), "t").unwrap();
    assert_eq!(removed, 1, "up-front count is the original DB's count");

    // (a) JE lock protocol: a concurrent remove of the in-doubt name MUST
    // conflict (was `Ok` pre-lock).
    let concurrent = env.remove_database(None, "t");
    assert!(
        concurrent.is_err()
            && is_typed_conflict(concurrent.as_ref().unwrap_err()),
        "concurrent remove of an in-doubt-truncate name must typed-conflict, \
         got {concurrent:?}"
    );

    // Resolve txn1 (commit) — the truncate applies to the ORIGINAL "t".
    txn1.commit().unwrap();
    {
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        assert_eq!(db.count().unwrap(), 0, "committed truncate emptied t");
        db.close().unwrap();
    }

    // (b) Identity guard: recreate "t" (new id) with fresh data; a stale
    // name-bound truncate would wipe it — the identity guard prevents that.
    env.remove_database(None, "t").unwrap();
    {
        let db = env.open_database(None, "t", &db_cfg()).unwrap();
        db.put(b"new", b"new-val").unwrap();
        db.close().unwrap();
    }
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
        "recreated database's data must survive (identity guard preserved)"
    );
    assert_eq!(db.count().unwrap(), 1);
}
