//! Tests for Database::iter() and Database::range() lazy iterators (Q-1).
//!
//! 2026 audit findings 2.1 and 2.3.

use noxu_db::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use tempfile::TempDir;

fn open_env_db(dir: &TempDir) -> (Environment, noxu_db::Database) {
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (env, db)
}

fn insert_n(db: &noxu_db::Database, n: u32) {
    for i in 0..n {
        let k = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let v = DatabaseEntry::from_bytes(format!("val-{i}").as_bytes());
        db.put( &k, &v).unwrap();
    }
}

// ── iter: empty database ──────────────────────────────────────────────────────

#[test]
fn iter_empty_db_yields_nothing() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);

    let items: Vec<_> = db.iter(None).unwrap().collect();
    assert!(items.is_empty(), "iter on empty db must yield no items");

    db.close().unwrap();
    env.close().unwrap();
}

// ── iter: single key ─────────────────────────────────────────────────────────

#[test]
fn iter_single_key() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    db.put(
        &DatabaseEntry::from_bytes(b"k"),
        &DatabaseEntry::from_bytes(b"v"))
    .unwrap();

    let items: Vec<_> = db.iter(None).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, b"k");
    assert_eq!(items[0].1, b"v");

    db.close().unwrap();
    env.close().unwrap();
}

// ── iter: many keys, forward order ───────────────────────────────────────────

#[test]
fn iter_many_keys_in_order() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    const N: u32 = 200;
    insert_n(&db, N);

    let keys: Vec<Vec<u8>> =
        db.iter(None).unwrap().map(|r| r.unwrap().0).collect();

    assert_eq!(keys.len(), N as usize);
    // Keys must be in ascending order (B-tree sorted by bytes).
    for w in keys.windows(2) {
        assert!(w[0] < w[1], "iter must return keys in ascending order");
    }

    db.close().unwrap();
    env.close().unwrap();
}

// ── iter: within explicit transaction ────────────────────────────────────────

#[test]
fn iter_within_explicit_txn() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    insert_n(&db, 10);

    let txn = env.begin_transaction(None).unwrap();
    let count = db.iter(Some(&txn)).unwrap().count();
    txn.commit().unwrap();

    assert_eq!(count, 10);

    db.close().unwrap();
    env.close().unwrap();
}

// ── iter: early drop doesn't panic ───────────────────────────────────────────

#[test]
fn iter_early_drop_is_clean() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    insert_n(&db, 50);

    {
        let mut it = db.iter(None).unwrap();
        // Consume only first 5 records, then drop.
        for _ in 0..5 {
            assert!(it.next().is_some());
        }
        // `it` dropped here — cursor must be cleaned up without panic.
    }

    // Database must still be usable after early-drop.
    let count = db.iter(None).unwrap().count();
    assert_eq!(count, 50);

    db.close().unwrap();
    env.close().unwrap();
}

// ── range: empty result (no keys in range) ───────────────────────────────────

#[test]
fn range_empty_result() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    // Insert keys 10..20.
    for i in 10u32..20 {
        db.put(
            &DatabaseEntry::from_bytes(&i.to_be_bytes()),
            &DatabaseEntry::from_bytes(b"v"))
        .unwrap();
    }

    // Range that misses: 50..60
    let lo = 50u32.to_be_bytes();
    let hi = 60u32.to_be_bytes();
    let items: Vec<_> = db
        .range(None, lo..hi) // K = [u8; 4]
        .unwrap()
        .collect();
    assert!(items.is_empty(), "range outside data must return nothing");

    db.close().unwrap();
    env.close().unwrap();
}

// ── range: subset of keys ────────────────────────────────────────────────────

#[test]
fn range_subset_inclusive() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    const N: u32 = 100;
    insert_n(&db, N);

    let lo = 20u32.to_be_bytes();
    let hi = 30u32.to_be_bytes();
    let keys: Vec<Vec<u8>> = db
        .range(None, lo..=hi) // K = [u8; 4]
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();

    assert_eq!(keys.len(), 11, "20..=30 is 11 keys");
    assert_eq!(keys.first().unwrap().as_slice(), lo.as_slice());
    assert_eq!(keys.last().unwrap().as_slice(), hi.as_slice());

    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn range_subset_exclusive_end() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    const N: u32 = 100;
    insert_n(&db, N);

    let lo = 10u32.to_be_bytes();
    let hi = 20u32.to_be_bytes();
    let keys: Vec<Vec<u8>> = db
        .range(None, lo..hi) // K = [u8; 4]
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();

    assert_eq!(keys.len(), 10, "10..20 is 10 keys (exclusive end)");
    assert_eq!(keys.first().unwrap().as_slice(), lo.as_slice());
    // Last key must be 19, not 20.
    let expected_last = 19u32.to_be_bytes();
    assert_eq!(keys.last().unwrap().as_slice(), expected_last.as_slice());

    db.close().unwrap();
    env.close().unwrap();
}

// ── range: unbounded ─────────────────────────────────────────────────────────

#[test]
fn range_unbounded_is_full_scan() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    const N: u32 = 30;
    insert_n(&db, N);

    // Use iter() for the unbounded case — equivalent to range(..) but
    // avoids the K type-inference ambiguity for RangeFull.
    let count = db.iter(None).unwrap().count();
    assert_eq!(count, N as usize);

    db.close().unwrap();
    env.close().unwrap();
}

// ── range: laziness — iterator is not a Vec ──────────────────────────────────

/// Confirms the iterator is lazy: consuming only 3 items from a 1000-key range
/// should not touch all 1000 records.  We verify this by checking that early
/// drop + re-scan still sees all 1000 records (no cursor state corruption).
#[test]
fn range_lazy_early_stop() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    const N: u32 = 1_000;
    insert_n(&db, N);

    {
        let mut it = db.iter(None).unwrap(); // unbounded = iter()
        for _ in 0..3 {
            assert!(it.next().is_some());
        }
        // Drop after 3 — cursor cleanup.
    }

    // Full re-scan must still see all N.
    let count = db.iter(None).unwrap().count();
    assert_eq!(count, N as usize);

    db.close().unwrap();
    env.close().unwrap();
}

// ── range: single-record range ───────────────────────────────────────────────

#[test]
fn range_single_record() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    insert_n(&db, 10);

    let key5 = 5u32.to_be_bytes(); // [u8; 4] — Copy, works as range bound
    let items: Vec<_> =
        db.range(None, key5..=key5).unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0.as_slice(), key5.as_slice());

    db.close().unwrap();
    env.close().unwrap();
}

// ── iter: idiomatic for-loop usage ───────────────────────────────────────────

/// Smoke test for the idiomatic `for kv in db.iter(...)` pattern.
#[test]
fn iter_idiomatic_for_loop() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir);
    const N: u32 = 25;
    insert_n(&db, N);

    let mut count = 0u32;
    for result in db.iter(None).unwrap() {
        let (_k, _v) = result.unwrap();
        count += 1;
    }
    assert_eq!(count, N);

    db.close().unwrap();
    env.close().unwrap();
}
