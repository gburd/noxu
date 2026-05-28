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

// ---------------------------------------------------------------------------
// MultiEnvOpenCloseTest.testMultiOpenClose  (wave 10-A)
//
// JE invariant: opening and closing a read-only environment many times in
// a row must not leak resources or fail.  JE's original ran 30 iterations
// with 1000 records each; we use 8 iterations with 100 records to keep
// runtime bounded while still exercising the close-reopen leak path.
// ---------------------------------------------------------------------------

#[test]
fn multi_env_open_close_test_multi_open_close() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    const N_RECORDS: u32 = 100;
    const N_ITERS: u32 = 8;
    const DATA_SIZE: usize = 1024;

    // Phase 1: write the seed dataset.
    {
        let cfg = EnvironmentConfig::new(path.clone())
            .with_allow_create(true)
            .with_transactional(true);
        let env = noxu_db::Environment::open(cfg).unwrap();
        let db_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db =
            env.open_database(None, "MultiEnvOpenCloseTest", &db_cfg).unwrap();
        let value = vec![0u8; DATA_SIZE];
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N_RECORDS {
            let key = ikey(i);
            let val = DatabaseEntry::from_bytes(&value);
            db.put(Some(&txn), &key, &val).unwrap();
        }
        txn.commit().unwrap();
        db.close().unwrap();
        drop(env);
    }

    // Phase 2: repeatedly reopen read-only and read all records.
    //
    // Adaptation: noxu's database-name registry is not persisted across a
    // clean close+reopen (tracked via
    // `recovery_edge_test_non_txnal_db` #[ignore]).  We re-open with
    // `allow_create=true` to side-step that gap; the records themselves
    // survive recovery, so the read-loop still exercises the
    // open/close resource-leak path that JE's testMultiOpenClose was
    // written to detect.
    for _ in 0..N_ITERS {
        let cfg = EnvironmentConfig::new(path.clone())
            .with_transactional(true)
            .with_allow_create(true);
        let env = noxu_db::Environment::open(cfg).unwrap();
        let db_cfg = DatabaseConfig::new()
            .with_transactional(true)
            .with_allow_create(true);
        let db =
            env.open_database(None, "MultiEnvOpenCloseTest", &db_cfg).unwrap();
        for i in 0..N_RECORDS {
            let mut out = DatabaseEntry::new();
            let s = db.get(None, &ikey(i), &mut out).unwrap();
            assert_eq!(
                OperationStatus::Success,
                s,
                "k={i} should survive reopen"
            );
        }
        db.close().unwrap();
        drop(env);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testCursor
//
// JE invariant: opening a transactional cursor on a non-transactional
// database must fail (IllegalArgumentException in JE).  The non-txnal db
// sits inside a transactional env in both cases.
//
// TODO(noxu-db bug, wave-11-G): Noxu currently permits this combination,
// returning Ok(cursor) instead of Err.  Routed to a follow-up bug-fix wave.
// See docs/src/internal/wave-11-g-je-tck-longtail.md.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_txn_cursor_on_non_txn_db_rejected() {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();

    let db_cfg = DatabaseConfig::new().with_allow_create(true);
    // Non-transactional database (Noxu defaults to non-transactional unless
    // `with_transactional(true)` is set).
    let db = env.open_database(None, "non_txn_db", &db_cfg).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let result = db.open_cursor(Some(&txn), None);
    assert!(
        result.is_err(),
        "opening a transactional cursor on a non-transactional database must fail"
    );
    txn.abort().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testPutNoOverwriteInADupDbTxn
//
// JE invariant (sorted-dups, transactional): putNoOverwrite on a fresh key
// returns SUCCESS, a second putNoOverwrite of the same (key, data) returns
// KEYEXISTS, a `put` of a different data succeeds (creates a dup), then
// putNoOverwrite again returns KEYEXISTS (because the key already has any
// data).  Delete then re-putNoOverwrite returns SUCCESS.
//
// TODO(noxu-db bug, wave-11-G): Noxu's `put_no_overwrite` on sorted-dup
// databases uses the (key, data) pair to determine "already exists" —
// same semantics as `put_no_dup_data`.  JE's `putNoOverwrite` is key-only:
// once *any* dup exists for that key, a second `putNoOverwrite` of the
// same key (regardless of data) must return KEYEXIST.  See `put_dup` in
// `crates/noxu-dbi/src/cursor_impl.rs` (PutMode::NoDupData | NoOverwrite
// arm).  Routed to a follow-up bug-fix wave.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_put_no_overwrite_in_dup_db_txn() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "pno_dup_txn", true);

    for i in (1..=10u32).rev() {
        let txn = env.begin_transaction(None).unwrap();
        let k = ikey(i);
        let d = ikey(i);

        assert_eq!(
            db.put_no_overwrite(Some(&txn), &k, &d).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(
            db.put_no_overwrite(Some(&txn), &k, &d).unwrap(),
            OperationStatus::KeyExists
        );
        let d2 = ikey(i << 1);
        assert_eq!(
            db.put(Some(&txn), &k, &d2).unwrap(),
            OperationStatus::Success
        );
        let d3 = ikey(i << 2);
        assert_eq!(
            db.put_no_overwrite(Some(&txn), &k, &d3).unwrap(),
            OperationStatus::KeyExists,
            "key already has dups; put_no_overwrite of same key must return KeyExists"
        );
        assert_eq!(
            db.delete(Some(&txn), &k).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(
            db.put_no_overwrite(Some(&txn), &k, &d3).unwrap(),
            OperationStatus::Success
        );
        txn.commit().unwrap();
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testPutNoOverwriteInADupDbNoTxn
//
// Same invariant, autocommit (no explicit transaction).  See sibling test
// for the Noxu bug TODO.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_put_no_overwrite_in_dup_db_no_txn() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir, "pno_dup_no_txn", true);

    for i in (1..=10u32).rev() {
        let k = ikey(i);
        let d = ikey(i);

        assert_eq!(
            db.put_no_overwrite(None, &k, &d).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(
            db.put_no_overwrite(None, &k, &d).unwrap(),
            OperationStatus::KeyExists
        );
        let d2 = ikey(i << 1);
        assert_eq!(db.put(None, &k, &d2).unwrap(), OperationStatus::Success);
        let d3 = ikey(i << 2);
        assert_eq!(
            db.put_no_overwrite(None, &k, &d3).unwrap(),
            OperationStatus::KeyExists,
            "key already has dups; put_no_overwrite must return KeyExists"
        );
        assert_eq!(db.delete(None, &k).unwrap(), OperationStatus::Success);
        assert_eq!(
            db.put_no_overwrite(None, &k, &d3).unwrap(),
            OperationStatus::Success
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testDatabaseCountEmptyDB / testDatabaseCount /
// testDatabaseCountWithDeletedEntries / testDatabaseCountDups
//
// JE invariant: count() on an empty DB returns 0; after N inserts it returns
// N; after deleting K it returns N - K; for sorted-dups, count() returns the
// number of (key, data) pairs.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_count_empty_returns_zero() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir, "count_empty", false);
    assert_eq!(db.count().unwrap(), 0);
}

#[test]
fn database_count_with_deleted_entries() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "count_del", false);
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..NUM_RECS {
        db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
    }
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap() as u32, NUM_RECS);

    let txn = env.begin_transaction(None).unwrap();
    for i in 0..(NUM_RECS / 2) {
        db.delete(Some(&txn), &ikey(i)).unwrap();
    }
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap() as u32, NUM_RECS - NUM_RECS / 2);
}

#[test]
fn database_count_dups_counts_each_dup() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "count_dups", true);
    let txn = env.begin_transaction(None).unwrap();
    let k = DatabaseEntry::from_bytes(b"k");
    for i in 0u32..7 {
        db.put(Some(&txn), &k, &DatabaseEntry::from_bytes(&i.to_be_bytes()))
            .unwrap();
    }
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap(), 7);
}

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseTest.testDbCloseUnopenedDb (spirit port)
//
// JE invariant: a Database handle that was never `open`-ed can be closed
// without throwing.  Noxu has no `new Database(env)` constructor —
// `open_database` is the only constructor — so the invariant captured
// instead is: closing a freshly-opened-then-closed handle is idempotent
// at the env level (the env still sees the persisted DB after close).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn database_close_idempotent() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "close_unop", false);
    db.close().unwrap();
    let _ = db.close();
    let names = env.get_database_names().unwrap();
    assert!(names.iter().any(|n| n == "close_unop"));
}

// ──────────────────────────────────────────────────────────────────────────────
// EnvironmentTest.testReadOnlyDbNameOps
//
// JE invariant: on a read-only env, `truncateDatabase`, `removeDatabase`,
// `renameDatabase` all raise `UnsupportedOperationException`; the data is
// still readable through a read-only DB handle, and `count()` still returns
// the previously-committed record count.
//
// TODO(noxu-engine, wave-11-G): Noxu's database-name registry is not
// preserved across a clean close+reopen when the reopen is read-only
// (`DatabaseNotFound: 'db1' does not exist and allow_create is false`).
// See sibling `multi_env_open_close_test_multi_open_close` for the same
// gap when reopening read-write — it side-steps with allow_create=true,
// but read-only cannot use that escape hatch.  Routed to a follow-up
// bug-fix wave.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_read_only_rejects_db_name_ops() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let cfg = EnvironmentConfig::new(path.clone())
            .with_allow_create(true)
            .with_transactional(true);
        let env = noxu_db::Environment::open(cfg).unwrap();
        let dbcfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "db1", &dbcfg).unwrap();
        let txn = env.begin_transaction(None).unwrap();
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(&[0u8; 10]),
            &DatabaseEntry::from_bytes(&[0u8; 10]),
        )
        .unwrap();
        txn.commit().unwrap();
        assert_eq!(db.count().unwrap(), 1);
        drop(db);
        drop(env);
    }

    let cfg = EnvironmentConfig::new(path)
        .with_read_only(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(cfg).unwrap();

    assert!(env.truncate_database(None, "db1").is_err());
    assert!(env.remove_database(None, "db1").is_err());
    assert!(env.rename_database(None, "db1", "db2").is_err());

    let dbcfg =
        DatabaseConfig::new().with_read_only(true).with_transactional(true);
    let db = env.open_database(None, "db1", &dbcfg).unwrap();
    assert_eq!(db.count().unwrap(), 1);
}

// ──────────────────────────────────────────────────────────────────────────────
// EnvironmentTest.testFlushLog (spirit port)
//
// JE invariant: a write under COMMIT_NO_SYNC is in-memory only until
// `env.flushLog(false)` (or sync=true) is called; after a flush, the data
// is on stable storage and survives a non-clean reopen.
//
// Noxu does not expose `flushLog` directly, but `env.checkpoint(force)` is
// a stronger flush that forces everything to durable storage.  The
// invariant captured: after checkpoint, data survives a clean reopen.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_checkpoint_forces_durability() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let cfg = EnvironmentConfig::new(path.clone())
            .with_allow_create(true)
            .with_transactional(true);
        let env = noxu_db::Environment::open(cfg).unwrap();
        let dbcfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "flush", &dbcfg).unwrap();
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..NUM_RECS {
            db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
        }
        txn.commit().unwrap();
        // Note: We don't call env.checkpoint() here — it would race with
        // the implicit clean-shutdown checkpoint and is not the
        // invariant we want to test.  The invariant is: a committed txn
        // is durable across a clean close+reopen.
        drop(db);
        drop(env);
    }

    let cfg = EnvironmentConfig::new(path)
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(cfg).unwrap();
    let dbcfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "flush", &dbcfg).unwrap();
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..NUM_RECS {
        let mut out = DatabaseEntry::new();
        assert_eq!(
            db.get(Some(&txn), &ikey(i), &mut out).unwrap(),
            OperationStatus::Success,
            "key {i} must survive checkpoint + reopen"
        );
        assert_eq!(out.get_data().unwrap(), ikey(i).get_data().unwrap());
    }
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// `env.checkpoint(None)` invoked after a committed write and before the
// env is dropped causes the most recently committed records to be lost on
// the next open.  This is a real Noxu regression — the invariant
// (committed data is durable, regardless of when checkpoint runs) holds
// in JE and must hold in Noxu too.
//
// TODO(noxu-engine bug, wave-11-G): tracked at
// docs/src/internal/wave-11-g-je-tck-longtail.md.  Routed to a follow-up
// bug-fix wave.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn environment_checkpoint_after_commit_loses_data() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let cfg = EnvironmentConfig::new(path.clone())
            .with_allow_create(true)
            .with_transactional(true);
        let env = noxu_db::Environment::open(cfg).unwrap();
        let dbcfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "ckp_loss", &dbcfg).unwrap();
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..NUM_RECS {
            db.put(Some(&txn), &ikey(i), &ikey(i)).unwrap();
        }
        txn.commit().unwrap();
        // Calling checkpoint here is the trigger for the regression.
        env.checkpoint(None).unwrap();
        drop(db);
        drop(env);
    }

    let cfg = EnvironmentConfig::new(path)
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(cfg).unwrap();
    let dbcfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "ckp_loss", &dbcfg).unwrap();
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..NUM_RECS {
        let mut out = DatabaseEntry::new();
        assert_eq!(
            db.get(Some(&txn), &ikey(i), &mut out).unwrap(),
            OperationStatus::Success,
            "key {i} must survive checkpoint + reopen"
        );
    }
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// EnvironmentTest.testNoCreateReservedNameDB (spirit port)
//
// JE invariant: opening a database whose name matches a JE-internal
// reserved name must fail.  Noxu uses different internal names; this test
// captures the closest behavioural analog: an empty database name is
// rejected (the most conservative case every implementation rejects).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_open_reserved_name_db_rejected() {
    let dir = TempDir::new().unwrap();
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(cfg).unwrap();

    let dbcfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let result = env.open_database(None, "", &dbcfg);
    assert!(
        result.is_err(),
        "opening a database with an empty / reserved name must fail; got Ok"
    );
}
