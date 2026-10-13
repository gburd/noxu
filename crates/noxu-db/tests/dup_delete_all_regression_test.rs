//! BUG-DUP-DELETE-ALL-KD live (non-crash) regression.
//!
//! KD-slot retention (A5 P0: a transactional delete keeps the BIN slot
//! known-deleted until commit + compressor) made `Database::delete_in` on a
//! sorted-dup key return `Ok(false)` during NORMAL operation: the dup
//! delete-all loop re-searches the key (`SearchMode::Set`) after each delete,
//! and without a KD-skipping dup search it re-finds the dup it just marked
//! known-deleted, whose `cursor.delete()` then reports the slot already
//! defunct — so `deleted_any` stays false and dups past the first are never
//! removed.
//!
//! This is a FUNCTIONAL regression (no crash needed), so this witness is a
//! no-regression guard: GREEN on v7.11.0, RED on A5-P0-without-lineage, GREEN
//! after the sorted-dup search KD-skip lineage (bbb0ccc0 -> 6ea0a5e2 ->
//! cc5facc6 -> f5e310a1).  JE skips/locks a defunct slot on every search and
//! step (lockLNAndCheckDefunct, CursorImpl.java:2063-2085).
#![cfg(not(noxu_shuttle))]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Durability, Environment,
    EnvironmentConfig, Mutex, OperationStatus, SecondaryConfig,
    SecondaryDatabase, SecondaryKeyCreator,
};
use std::path::Path;
use std::sync::Arc;

fn env_cfg(p: &Path) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_durability(Durability::COMMIT_SYNC)
        .with_node_max_entries(8);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(false);
    c.set_run_in_compressor(false);
    c
}
fn db_cfg(dups: bool) -> DatabaseConfig {
    let c =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    if dups { c.with_sorted_duplicates(true) } else { c }
}

/// Count the dups under key `k` in a sorted-dup DB via a fresh cursor.
fn dup_count(db: &Database, k: &[u8]) -> u32 {
    let mut cur = db.open_cursor(None).unwrap();
    let mut key = DatabaseEntry::from_bytes(k);
    let mut data = DatabaseEntry::new();
    let mut st =
        cur.get(&mut key, &mut data, noxu_db::Get::Search, None).unwrap();
    let mut cnt = 0u32;
    while st == OperationStatus::Success {
        cnt += 1;
        st = cur.get(&mut key, &mut data, noxu_db::Get::NextDup, None).unwrap();
    }
    cnt
}

#[test]
fn dup_delete_all_in_txn_commit_removes_every_dup() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let db = env.open_database(None, "dd", &db_cfg(true)).unwrap();
    for v in [&b"d1"[..], b"d2", b"d3"] {
        db.put(b"k", v).unwrap();
    }
    assert_eq!(dup_count(&db, b"k"), 3, "precondition: 3 dups");

    let txn = env.begin_transaction(None).unwrap();
    assert!(
        db.delete_in(&txn, b"k").unwrap(),
        "delete_in on a dup key must report Ok(true)"
    );
    txn.commit().unwrap();

    assert_eq!(dup_count(&db, b"k"), 0, "every dup must be gone after commit");
    assert!(db.get(b"k").unwrap().is_none());

    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn dup_delete_all_in_txn_abort_restores_every_dup() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let db = env.open_database(None, "dd", &db_cfg(true)).unwrap();
    for v in [&b"d1"[..], b"d2", b"d3"] {
        db.put(b"k", v).unwrap();
    }

    let txn = env.begin_transaction(None).unwrap();
    assert!(db.delete_in(&txn, b"k").unwrap());
    assert_eq!(db.get_in(&txn, b"k").unwrap(), None, "deleted within txn");
    txn.abort().unwrap();

    assert_eq!(dup_count(&db, b"k"), 3, "all 3 dups restored on abort");

    db.close().unwrap();
    env.close().unwrap();
}

struct DataKey;
impl SecondaryKeyCreator for DataKey {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        match data.data_opt() {
            Some(d) => {
                result.set_data(d);
                true
            }
            None => false,
        }
    }
}

#[test]
fn secondary_primary_delete_in_txn_commit_and_abort() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let pri = Arc::new(Mutex::new(
        env.open_database(None, "pri", &db_cfg(false)).unwrap(),
    ));
    let inner = env.open_database(None, "sec", &db_cfg(true)).unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&pri),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(DataKey)),
    )
    .unwrap();

    // 3 primary records sharing one secondary key "s" (3 dups in the secondary).
    {
        let p = pri.lock();
        p.put(b"p1", b"s").unwrap();
        p.put(b"p2", b"s").unwrap();
        p.put(b"p3", b"s").unwrap();
    }
    let sec_count = |sec: &SecondaryDatabase| -> u32 {
        let mut c = sec.open_cursor(None).unwrap();
        let (mut sk, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        let mut cnt = 0u32;
        let mut st = c.get_first(&mut sk, &mut pk, &mut d).unwrap();
        while st == OperationStatus::Success {
            cnt += 1;
            st = c.get_next(&mut sk, &mut pk, &mut d).unwrap();
        }
        cnt
    };
    assert_eq!(sec_count(&sec), 3, "precondition: 3 secondary dups");

    // Commit path: delete one primary -> its secondary dup is gone.
    let txn = env.begin_transaction(None).unwrap();
    assert!(pri.lock().delete_in(&txn, b"p1").unwrap());
    txn.commit().unwrap();
    assert_eq!(sec_count(&sec), 2, "secondary dup removed on primary delete");

    // Abort path: delete another primary, abort -> secondary dup restored.
    let txn = env.begin_transaction(None).unwrap();
    assert!(pri.lock().delete_in(&txn, b"p2").unwrap());
    txn.abort().unwrap();
    assert_eq!(sec_count(&sec), 2, "aborted primary delete restores secondary");
    assert_eq!(
        pri.lock().get(b"p2").unwrap().as_deref(),
        Some(&b"s"[..]),
        "aborted primary delete restores the primary record"
    );

    drop(sec);
    pri.lock().close().unwrap();
    env.close().unwrap();
}

/// Mirrors the integrate `dup_db_txn_delete_whole_keys` shape that first
/// exposed the regression: several keys, 2 dups each, delete a subset in one
/// txn.  On A5-P0-without-lineage `delete_in` returns Ok(false) on the first
/// such key (the re-search re-finds the dup it just marked KD).
#[test]
fn dup_delete_all_multikey_two_dups_each() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let db = env.open_database(None, "dd", &db_cfg(true)).unwrap();
    let key = |i: u32| format!("k{i:05}").into_bytes();
    for i in 0..20u32 {
        db.put(key(i), b"d1").unwrap();
        db.put(key(i), b"d2").unwrap();
    }
    let txn = env.begin_transaction(None).unwrap();
    for i in (0..20u32).step_by(2) {
        assert!(
            db.delete_in(&txn, key(i)).unwrap(),
            "delete_in must report Ok(true) for key {i} (2 dups)"
        );
    }
    txn.commit().unwrap();
    for i in (0..20u32).step_by(2) {
        assert_eq!(dup_count(&db, &key(i)), 0, "key {i} dups gone");
    }
    for i in (1..20u32).step_by(2) {
        assert_eq!(dup_count(&db, &key(i)), 2, "key {i} untouched");
    }
    db.close().unwrap();
    env.close().unwrap();
}
