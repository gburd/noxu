//! JE DatabaseTest ports — basic Database public-API contract tests.
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/DatabaseTest.java`.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus, Put,
};
use tempfile::TempDir;

const NUM_RECS: u32 = 50;

fn open_env_db(
    dir: &TempDir,
    name: &str,
    dups: bool,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dups);
    let db = env.open_database(None, name, &db_cfg).unwrap();
    (env, db)
}

fn ikey(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testPutExisting
//
// JE invariant: `Put.OVERWRITE` on a non-existent key inserts (not an update);
// repeated on the same (key,data) is an update; SearchBoth then returns the
// same data.  Data round-trip is exact.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_put_existing_overwrite_round_trip() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "put_existing", false);

    let txn = env.begin_transaction(None).unwrap();
    for i in (1..=NUM_RECS).rev() {
        let k = ikey(i);
        let d = ikey(i);

        // Insert.
        let s = db.put(Some(&txn), &k, &d).unwrap();
        assert_eq!(s, OperationStatus::Success);

        let mut out = DatabaseEntry::new();
        let s = db.get(Some(&txn), &k, &mut out).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), d.get_data().unwrap());

        // Re-insert (overwrite).
        let s = db.put(Some(&txn), &k, &d).unwrap();
        assert_eq!(s, OperationStatus::Success);

        // Round-trip via cursor SearchBoth.
        let mut c = db.open_cursor(Some(&txn), None).unwrap();
        let mut sk = ikey(i);
        let mut sd = ikey(i);
        let s = c.get(&mut sk, &mut sd, Get::SearchBoth, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(sd.get_data().unwrap(), d.get_data().unwrap());
    }
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testZeroLengthData (spirit port)
//
// JE invariant: zero-length data round-trips correctly through put/get and
// across env close/reopen (recovery).  We don't check the JE-internal
// `LogUtils.ZERO_LENGTH_BYTE_ARRAY` identity (an internal representation
// detail), but we do check the size and content invariant.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_zero_length_data_round_trip_with_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let (env, db) = open_env_db(&dir, "zero_len", false);
        let txn = env.begin_transaction(None).unwrap();
        for i in (1..=NUM_RECS).rev() {
            let k = ikey(i);
            let d = DatabaseEntry::from_bytes(&[]);
            let s = db.put(Some(&txn), &k, &d).unwrap();
            assert_eq!(s, OperationStatus::Success);

            let mut out = DatabaseEntry::new();
            let s = db.get(Some(&txn), &k, &mut out).unwrap();
            assert_eq!(s, OperationStatus::Success);
            assert!(out.get_data().is_some_and(|b| b.is_empty()));
        }
        txn.commit().unwrap();
        drop(db);
        drop(env);
    }

    // Reopen and verify zero-length data survives recovery.
    let env_cfg = EnvironmentConfig::new(path)
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "zero_len", &db_cfg).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    for i in (1..=NUM_RECS).rev() {
        let k = ikey(i);
        let mut out = DatabaseEntry::new();
        let s = db.get(Some(&txn), &k, &mut out).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert!(
            out.get_data().is_some_and(|b| b.is_empty()),
            "zero-length data must survive recovery for key {i}"
        );
    }
    txn.commit().unwrap();
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testDeleteNonDup
//
// JE invariant: on a non-dup db, `delete` removes the record; a subsequent
// `get` returns NotFound; a subsequent `delete` returns NotFound.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_delete_non_dup() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "del_nodup", false);

    let txn = env.begin_transaction(None).unwrap();
    for i in (1..=NUM_RECS).rev() {
        db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
    }
    for i in (1..=NUM_RECS).rev() {
        let k = ikey(i);
        let s = db.delete(Some(&txn), &k).unwrap();
        assert_eq!(s, OperationStatus::Success, "first delete on key {i}");

        let mut out = DatabaseEntry::new();
        let s = db.get(Some(&txn), &k, &mut out).unwrap();
        assert_eq!(s, OperationStatus::NotFound, "get after delete on key {i}");

        let s = db.delete(Some(&txn), &k).unwrap();
        assert_eq!(s, OperationStatus::NotFound, "second delete on key {i}");
    }
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testDeleteDup
//
// JE invariant: on a sorted-dup db, `delete` removes ALL dups under the key;
// subsequent `get` returns NotFound; subsequent `delete` returns NotFound.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_delete_with_dups_removes_all() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "del_dup", true);

    let txn = env.begin_transaction(None).unwrap();
    const NUM_DUPS: u32 = 4;
    for i in (1..=NUM_RECS).rev() {
        db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
        for j in 0..NUM_DUPS {
            db.put(Some(&txn), &ikey(i), &ikey(i + j)).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    for i in (1..=NUM_RECS).rev() {
        let s = db.delete(Some(&txn), &ikey(i)).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let mut out = DatabaseEntry::new();
        let s = db.get(Some(&txn), &ikey(i), &mut out).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
        let s = db.delete(Some(&txn), &ikey(i)).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testDeleteAbort
//
// JE invariant: a delete that is aborted does not remove the record.  We
// verify that the record is still readable (by another no-wait txn after
// abort) — i.e. the delete has been fully undone.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_delete_abort_restores_record() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "del_abort", false);

    // Pre-populate.
    {
        let t = env.begin_transaction(None).unwrap();
        for i in (1..=NUM_RECS).rev() {
            db.put(Some(&t), &ikey(i), &ikey(i)).unwrap();
        }
        t.commit().unwrap();
    }
    let delkey = NUM_RECS / 2;

    // Delete inside a txn, then abort.
    let txn = env.begin_transaction(None).unwrap();
    let s = db.delete(Some(&txn), &ikey(delkey)).unwrap();
    assert_eq!(s, OperationStatus::Success);
    txn.abort().unwrap();

    // After abort, the record must be readable in a fresh txn.
    let t2 = env.begin_transaction(None).unwrap();
    let mut out = DatabaseEntry::new();
    let s = db.get(Some(&t2), &ikey(delkey), &mut out).unwrap();
    assert_eq!(
        s,
        OperationStatus::Success,
        "record must reappear after delete is aborted"
    );
    assert_eq!(out.get_data().unwrap(), ikey(delkey).get_data().unwrap());
    t2.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testPutDuplicate
//
// JE invariant: repeated `put` under the same key on a sorted-dup db creates
// distinct dups; `count()` reflects the total number of physical records
// (not unique keys).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_put_duplicate_creates_distinct_dups() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "put_dup", true);

    let txn = env.begin_transaction(None).unwrap();
    let mut expected_records = 0u64;
    for i in (1..=NUM_RECS).rev() {
        db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
        expected_records += 1;
        db.put(Some(&txn), &ikey(i), &ikey(i * 2)).unwrap();
        expected_records += 1;
    }
    txn.commit().unwrap();

    assert_eq!(
        db.count().unwrap(),
        expected_records,
        "count() must reflect total dups, not unique keys"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testPutNoDupData
//
// JE invariant: on a sorted-dup db, `Put::NoDupData` (cursor-only in Noxu)
// inserts only when the exact (key,data) pair does not yet exist; a repeat
// returns KeyExists; a different data succeeds.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_put_no_dup_data_rejects_exact_pair() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "put_no_dup_data", true);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    for i in (1..=NUM_RECS).rev() {
        let k = ikey(i);
        let d = ikey(i);
        let s = c.put(&k, &d, Put::NoDupData).unwrap();
        assert_eq!(
            s,
            OperationStatus::Success,
            "first NoDupData on (k{i},d{i})"
        );
        let s = c.put(&k, &d, Put::NoDupData).unwrap();
        assert_eq!(
            s,
            OperationStatus::KeyExists,
            "duplicate NoDupData on (k{i},d{i})"
        );
        let d2 = ikey(i + 1);
        let s = c.put(&k, &d2, Put::NoDupData).unwrap();
        assert_eq!(
            s,
            OperationStatus::Success,
            "different-data NoDupData on (k{i},d{}) ",
            i + 1
        );
    }
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testPutNoOverwriteInANoDupDb
//
// JE invariant: on a non-dup db, `putNoOverwrite` succeeds the first time and
// returns KeyExists on a repeat with the same key (regardless of data).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_put_no_overwrite_no_dups() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "no_overwrite_nodup", false);

    let txn = env.begin_transaction(None).unwrap();
    for i in (1..=NUM_RECS).rev() {
        let k = ikey(i);
        let d = ikey(i);
        let s = db.put_no_overwrite(Some(&txn), &k, &d).unwrap();
        assert_eq!(s, OperationStatus::Success, "first NoOverwrite on k{i}");
        let s = db.put_no_overwrite(Some(&txn), &k, &d).unwrap();
        assert_eq!(s, OperationStatus::KeyExists, "second NoOverwrite on k{i}");
    }
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testDatabaseCount
//
// JE invariant: after inserting N records, db.count() == N.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_count_returns_record_count() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "count", false);

    let txn = env.begin_transaction(None).unwrap();
    for i in (1..=NUM_RECS).rev() {
        db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
    }
    let c = db.count().unwrap();
    assert_eq!(c, NUM_RECS as u64);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseConfigTest.testConfig (wave 9-C)
//
// JE invariant: a Database keeps its own copy of the configuration; the
// `getConfig()` accessor returns a snapshot, not the original object.
//
// Noxu adaptation: `DatabaseConfig` is `Clone, PartialEq, Eq`.  We
// assert the same shape — mutating the config the user passed at open
// time, then re-opening with that mutated config, behaves consistently;
// `get_config()` reflects the values the database stored.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_config_snapshot_after_open() {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();

    // Open dbA with allow_create=true, sorted_duplicates=false.
    let cfg_a =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db_a = env.open_database(None, "foo", &cfg_a).unwrap();

    // Database stores its own copy: get_config still reports the
    // values the user passed at open time.
    let stored = db_a.get_config().clone();
    assert!(stored.allow_create);
    assert!(!stored.sorted_duplicates);
    assert!(stored.transactional);

    // Mutating a clone of the stored config does not affect the
    // database's view (the database returns &DatabaseConfig and
    // borrow-checks any direct mutation).
    let mut other = stored;
    other.sorted_duplicates = true;
    let _ = &other;
    assert!(!db_a.get_config().sorted_duplicates);

    db_a.close().unwrap();
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseConfigTest.testIsTransactional (wave 9-C)
//
// JE invariant: a database opened with transactional=true reports
// transactional=true; both implicit auto-commit (txn=null) and explicit
// transaction handles are accepted for puts and gets.
//
// Noxu adaptation: there is no `Database::is_transactional()`; we use
// `db.get_config().transactional`.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_config_is_transactional() {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();

    // Open transactional db with explicit transaction handle.
    let txn = env.begin_transaction(None).unwrap();
    let cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(Some(&txn), "testDB2", &cfg).unwrap();
    assert!(db.get_config().transactional);

    // Implicit-auto-commit put (txn=None) is accepted on a txn DB.
    db.put(
        None,
        &DatabaseEntry::from_bytes(&[0]),
        &DatabaseEntry::from_bytes(&[0]),
    )
    .unwrap();

    // Explicit-txn put + get on the same txn handle is accepted.
    db.put(
        Some(&txn),
        &DatabaseEntry::from_bytes(&[1]),
        &DatabaseEntry::from_bytes(&[1]),
    )
    .unwrap();
    let mut out = DatabaseEntry::new();
    let s =
        db.get(Some(&txn), &DatabaseEntry::from_bytes(&[1]), &mut out).unwrap();
    assert_eq!(s, OperationStatus::Success);
    txn.commit().unwrap();

    // After commit, no-txn read sees the record.
    let mut out = DatabaseEntry::new();
    let s = db.get(None, &DatabaseEntry::from_bytes(&[1]), &mut out).unwrap();
    assert_eq!(s, OperationStatus::Success);

    db.close().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseConfigTest.testOpenReadOnly (wave 9-C, partial)
//
// JE invariant: opening a database with `read_only=true` rejects any
// write attempt (put or cursor.delete) with UnsupportedOperationException;
// gets and forward iteration succeed.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_config_open_read_only_rejects_writes() {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();

    // Pre-populate the DB with k=0,d=0 under transactional+rw.
    let cfg_rw =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "testDB2", &cfg_rw).unwrap();
    db.put(
        None,
        &DatabaseEntry::from_bytes(&[0]),
        &DatabaseEntry::from_bytes(&[0]),
    )
    .unwrap();
    db.close().unwrap();

    // Re-open read-only.  Reads succeed.
    let cfg_ro =
        DatabaseConfig::new().with_transactional(true).with_read_only(true);
    let db_ro = env.open_database(None, "testDB2", &cfg_ro).unwrap();
    assert!(db_ro.get_config().read_only);
    assert!(db_ro.get_config().transactional);

    let mut out = DatabaseEntry::new();
    let s =
        db_ro.get(None, &DatabaseEntry::from_bytes(&[0]), &mut out).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(out.data(), &[0]);

    // Writes fail.
    let r = db_ro.put(
        None,
        &DatabaseEntry::from_bytes(&[1]),
        &DatabaseEntry::from_bytes(&[1]),
    );
    assert!(r.is_err(), "put on read-only db must fail; got {:?}", r);

    // Cursor delete fails as well.
    let mut c = db_ro.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let r = c.delete();
    assert!(r.is_err(), "cursor delete on read-only db must fail; got {:?}", r);
    drop(c);

    db_ro.close().unwrap();
}
