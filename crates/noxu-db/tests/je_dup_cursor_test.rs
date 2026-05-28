//! JE DbCursorDuplicateTest / DbCursorDuplicateDeleteTest ports.
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/dbi/DbCursorDuplicate*Test.java`.  These exercise
//! correct dup-chain behaviour through the public Cursor API.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::collections::BTreeMap;
use tempfile::TempDir;

fn open_env_db(
    dir: &TempDir,
    name: &str,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    let db = env.open_database(None, name, &db_cfg).unwrap();
    (env, db)
}

fn build_random_dup_data(seed: u64) -> BTreeMap<Vec<u8>, Vec<Vec<u8>>> {
    // Pseudo-random distinct dup payload.  Generates ~5 keys × ~5 dups each.
    let mut state = seed;
    let mut next = || {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        state
    };

    let mut out: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    for _ in 0..30 {
        let k = (next() % 5) as u8;
        let d = (next() % 100) as u8;
        let key = vec![k];
        let data = vec![d];
        out.entry(key).or_default().push(data);
    }
    for v in out.values_mut() {
        v.sort();
        v.dedup();
    }
    out
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDuplicateTest.testDuplicateCreationForward
//
// JE invariant: random (key, dup) data inserted into a sorted-dups db is
// retrieved in (key asc, data asc) order via cursor.getNext.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_cursor_creation_forward_walks_in_sorted_order() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "dup_fwd");

    let data = build_random_dup_data(42);
    let txn = env.begin_transaction(None).unwrap();
    for (k, dups) in &data {
        for d in dups {
            db.put(
                Some(&txn),
                &DatabaseEntry::from_bytes(k),
                &DatabaseEntry::from_bytes(d),
            )
            .unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut prev: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut count = 0usize;
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let cur =
            (k.get_data().unwrap().to_vec(), d.get_data().unwrap().to_vec());
        if let Some(p) = &prev {
            assert!(
                p.0 < cur.0 || (p.0 == cur.0 && p.1 < cur.1),
                "dup walk must be (k asc, d asc): prev={p:?} cur={cur:?}"
            );
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    let expected: usize = data.values().map(|v| v.len()).sum();
    assert_eq!(count, expected);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDuplicateTest.testDuplicateCreationBackwards
//
// JE invariant: same data set, walked backward via Get::Last + Get::Prev,
// must come out in (key desc, data desc) order.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_cursor_creation_backwards_walks_in_reverse_order() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "dup_bwd");

    let data = build_random_dup_data(123);
    let txn = env.begin_transaction(None).unwrap();
    for (k, dups) in &data {
        for d in dups {
            db.put(
                Some(&txn),
                &DatabaseEntry::from_bytes(k),
                &DatabaseEntry::from_bytes(d),
            )
            .unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut prev: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut count = 0usize;
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::Last, None).unwrap();
    while s == OperationStatus::Success {
        let cur =
            (k.get_data().unwrap().to_vec(), d.get_data().unwrap().to_vec());
        if let Some(p) = &prev {
            assert!(
                p.0 > cur.0 || (p.0 == cur.0 && p.1 > cur.1),
                "reverse dup walk: prev={p:?} cur={cur:?}"
            );
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Prev, None).unwrap();
    }
    let expected: usize = data.values().map(|v| v.len()).sum();
    assert_eq!(count, expected);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDuplicateDeleteTest.testSimpleSingleElementDupTree
//
// JE invariant: insert two dups, delete one via positioned cursor; the
// surviving dup is still retrievable.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_cursor_delete_one_dup_leaves_the_other() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "single_elem");

    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"k1");
    db.put(Some(&txn), &key, &DatabaseEntry::from_bytes(b"d1")).unwrap();
    db.put(Some(&txn), &key, &DatabaseEntry::from_bytes(b"d2")).unwrap();

    // Position on the first dup and delete it.
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(d.get_data().unwrap(), b"d1");
    c.delete().unwrap();

    // Now d2 should be the only remaining dup.
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(d.get_data().unwrap(), b"d2");
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);

    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDuplicateDeleteTest.testEmptyNodes
//
// JE invariant: insert N dups under one key, delete them all, the database
// must be empty (count() == 0, get returns NotFound).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_cursor_delete_all_dups_leaves_empty() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "empty_nodes");
    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"k");
    for i in 0u8..20 {
        db.put(Some(&txn), &key, &DatabaseEntry::from_bytes(&[i])).unwrap();
    }
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap(), 20);

    let txn = env.begin_transaction(None).unwrap();
    let s = db.delete(Some(&txn), &key).unwrap();
    assert_eq!(s, OperationStatus::Success);
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap(), 0);
    let mut out = DatabaseEntry::new();
    let s = db.get(None, &key, &mut out).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDuplicateDeleteTest.testDuplicateDeleteFirst
//
// JE invariant: positioning at FirstDup of a key, deleting, then walking
// next-dup retrieves only the remaining dups in order.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_cursor_delete_first_dup_via_positioned_cursor() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "del_first");

    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"k");
    for i in 0u8..5 {
        db.put(Some(&txn), &key, &DatabaseEntry::from_bytes(&[i])).unwrap();
    }

    // Position via SearchKey + delete (deletes only the first dup).
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = DatabaseEntry::from_bytes(b"k");
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    c.delete().unwrap();

    // Walk remaining dups: must be 1, 2, 3, 4 (not 0).
    let mut found: Vec<u8> = Vec::new();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        found.push(d.get_data().unwrap()[0]);
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(found, vec![1, 2, 3, 4]);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDuplicateTest.testPutNoDupData2
//
// JE invariant: Cursor::put with NoDupData under one key inserts each of N
// distinct dup-data values successfully (no collision since each (k, d) is
// unique).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_cursor_put_no_dup_data_inserts_unique_pairs() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "no_dup2");

    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"oneKey");
    for d in [b"one".as_slice(), b"two", b"three", b"four", b"five",
              b"six", b"seven", b"eight", b"nine"] {
        let s = db
            .put_no_overwrite(Some(&txn), &key, &DatabaseEntry::from_bytes(d))
            .unwrap();
        assert_eq!(s, OperationStatus::Success, "data {d:?} must insert as new dup");
    }
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap(), 9);
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDuplicateTest.testAbortDuplicateTreeCreation
//
// JE invariant: txn1 puts (k, d1) and commits; txn2 puts (k, d2) and aborts.
// Post-abort, only (k, d1) is visible; cursor.count() == 1; getNext after
// the first record returns NotFound.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_cursor_abort_after_dup_creation_keeps_committed_only() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "abort_dup");

    let txn1 = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"oneKey");
    db.put(Some(&txn1), &key, &DatabaseEntry::from_bytes(b"firstData"))
        .unwrap();
    txn1.commit().unwrap();

    let txn2 = env.begin_transaction(None).unwrap();
    db.put(Some(&txn2), &key, &DatabaseEntry::from_bytes(b"secondData"))
        .unwrap();
    txn2.abort().unwrap();

    let txn3 = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn3), None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(d.get_data().unwrap(), b"firstData");
    assert_eq!(c.count().unwrap(), 1, "only one dup must remain after abort");
    let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c);
    txn3.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDeleteTest.testLargeDeleteFirst
//
// JE invariant: insert N keys (no dups), walk forward, delete the first one
// via cursor.delete; the remaining N-1 keys are still walkable in order.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_delete_first_via_walk_keeps_rest() {
    const N: u32 = 100;
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true);
    let db = env.open_database(None, "del_first", &db_cfg).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(&i.to_be_bytes()),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    c.delete().unwrap();
    drop(c);
    txn.commit().unwrap();

    assert_eq!(db.count().unwrap() as u32, N - 1);
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    for i in 1..N {
        let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let mut a = [0u8; 4];
        a.copy_from_slice(k.get_data().unwrap());
        assert_eq!(u32::from_be_bytes(a), i);
    }
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDeleteTest.testLargeDeleteLast
//
// Same as above but delete the last one via Get::Last.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_delete_last_via_walk_keeps_rest() {
    const N: u32 = 100;
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true);
    let db = env.open_database(None, "del_last", &db_cfg).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(&i.to_be_bytes()),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::Last, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    c.delete().unwrap();
    drop(c);
    txn.commit().unwrap();

    assert_eq!(db.count().unwrap() as u32, N - 1);
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::Last, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let mut a = [0u8; 4];
    a.copy_from_slice(k.get_data().unwrap());
    assert_eq!(u32::from_be_bytes(a), N - 2);
    drop(c);
    txn.commit().unwrap();
}
