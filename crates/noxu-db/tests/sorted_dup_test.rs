//! Sorted-duplicate database tests.
//!
//! Verifies the full two-part key model for sorted-dup databases:
//! put/get, ordering, cursor navigation (NextDup, PrevDup, NextNoDup,
//! PrevNoDup), delete of specific dup values, count(), transaction
//! isolation, and round-trip recovery.

use noxu_db::Environment;
use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus, Put,
};
use tempfile::TempDir;

/// Open a transactional environment in a temp dir.
fn open_env(dir: &TempDir) -> Environment {
    let config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    Environment::open(config).expect("env open")
}

// ---------------------------------------------------------------------------
// P2-7 tests
// ---------------------------------------------------------------------------

/// Basic single-value put/get in a sorted-dup DB.
#[test]
fn test_put_get_single_dup() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"key1");
    let data = DatabaseEntry::from_bytes(b"val1");
    db.put(&key, &data).unwrap();

    let mut out = DatabaseEntry::new();
    let status = db.get_into(None, &key, &mut out).unwrap();
    assert!(status);
    assert_eq!(out.get_data().unwrap(), b"val1");

    let _ = env.close();
}

/// Insert multiple dups for the same key and verify they are all stored.
#[test]
fn test_put_multiple_dups_same_key() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"k");
    for i in 0u8..5 {
        let d = DatabaseEntry::from_bytes(&[i]);
        db.put(&key, &d).unwrap();
    }

    // count() should report 5 dups.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"k");
    let mut dout = DatabaseEntry::new();
    let s = cursor.get(&mut kout, &mut dout, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(cursor.count().unwrap(), 5);

    cursor.close().unwrap();
    let _ = env.close();
}

/// Insert duplicates out of order; verify get_next_dup returns them sorted.
#[test]
fn test_dup_sorted_order() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"alpha");
    for v in [b"c".as_ref(), b"a", b"b"] {
        db.put(&key, DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"alpha");
    let mut dout = DatabaseEntry::new();

    // Position at first dup for "alpha".
    let s = cursor.get(&mut kout, &mut dout, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(dout.get_data().unwrap(), b"a", "first dup should be 'a'");

    // Advance through dups in order.
    cursor.get(&mut kout, &mut dout, Get::NextDup, None).unwrap();
    assert_eq!(dout.get_data().unwrap(), b"b");

    cursor.get(&mut kout, &mut dout, Get::NextDup, None).unwrap();
    assert_eq!(dout.get_data().unwrap(), b"c");

    // No more dups.
    let s = cursor.get(&mut kout, &mut dout, Get::NextDup, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);

    cursor.close().unwrap();
    let _ = env.close();
}

/// get_next_dup stops at the key boundary.
#[test]
fn test_get_next_dup_stops_at_key_boundary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    db.put(
        DatabaseEntry::from_bytes(b"key1"),
        DatabaseEntry::from_bytes(b"v1"),
    )
    .unwrap();
    db.put(
        DatabaseEntry::from_bytes(b"key1"),
        DatabaseEntry::from_bytes(b"v2"),
    )
    .unwrap();
    db.put(
        DatabaseEntry::from_bytes(b"key2"),
        DatabaseEntry::from_bytes(b"v3"),
    )
    .unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"key1");
    let mut dout = DatabaseEntry::new();

    // Position on key1/v1.
    cursor.get(&mut kout, &mut dout, Get::Search, None).unwrap();
    assert_eq!(dout.get_data().unwrap(), b"v1");

    // Advance to second dup of key1.
    let s = cursor.get(&mut kout, &mut dout, Get::NextDup, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(dout.get_data().unwrap(), b"v2");

    // NextDup from key1/v2 should NOT return key2/v3.
    let s = cursor.get(&mut kout, &mut dout, Get::NextDup, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound, "NextDup crossed key boundary");

    cursor.close().unwrap();
    let _ = env.close();
}

/// get_next_no_dup advances to the first entry of the next primary key.
#[test]
fn test_get_next_no_dup_advances_to_next_key() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    db.put(DatabaseEntry::from_bytes(b"aa"), DatabaseEntry::from_bytes(b"d1"))
        .unwrap();
    db.put(DatabaseEntry::from_bytes(b"aa"), DatabaseEntry::from_bytes(b"d2"))
        .unwrap();
    db.put(DatabaseEntry::from_bytes(b"aa"), DatabaseEntry::from_bytes(b"d3"))
        .unwrap();
    db.put(DatabaseEntry::from_bytes(b"bb"), DatabaseEntry::from_bytes(b"e1"))
        .unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::new();
    let mut dout = DatabaseEntry::new();

    // Position on first entry of "aa".
    cursor.get(&mut kout, &mut dout, Get::First, None).unwrap();
    assert_eq!(kout.get_data().unwrap(), b"aa");
    assert_eq!(dout.get_data().unwrap(), b"d1");

    // NextNoDup should jump directly to "bb"/"e1".
    let s = cursor.get(&mut kout, &mut dout, Get::NextNoDup, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(kout.get_data().unwrap(), b"bb");
    assert_eq!(dout.get_data().unwrap(), b"e1");

    cursor.close().unwrap();
    let _ = env.close();
}

/// Delete one specific dup value; others remain.
#[test]
fn test_dup_delete_specific_value() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"k");
    db.put(&key, DatabaseEntry::from_bytes(b"v1")).unwrap();
    db.put(&key, DatabaseEntry::from_bytes(b"v2")).unwrap();
    db.put(&key, DatabaseEntry::from_bytes(b"v3")).unwrap();

    // Position on v2 using SearchBoth and delete it.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"k");
    let mut dout = DatabaseEntry::from_bytes(b"v2");
    let s = cursor.get(&mut kout, &mut dout, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::Success, "should find (k, v2)");
    cursor.delete().unwrap();
    cursor.close().unwrap();

    // count() should now be 2.
    let mut cursor2 = db.open_cursor(None).unwrap();
    let mut kout2 = DatabaseEntry::from_bytes(b"k");
    let mut dout2 = DatabaseEntry::new();
    cursor2.get(&mut kout2, &mut dout2, Get::Search, None).unwrap();
    assert_eq!(
        cursor2.count().unwrap(),
        2,
        "should have 2 dups after deleting v2"
    );

    // The remaining values should be v1 and v3.
    assert_eq!(dout2.get_data().unwrap(), b"v1");
    cursor2.get(&mut kout2, &mut dout2, Get::NextDup, None).unwrap();
    assert_eq!(dout2.get_data().unwrap(), b"v3");

    cursor2.close().unwrap();
    let _ = env.close();
}

/// count() returns the correct number of dups for the current key.
#[test]
fn test_dup_count() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"mykey");
    for i in 0u8..7 {
        db.put(&key, DatabaseEntry::from_bytes(&[i])).unwrap();
    }

    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"mykey");
    let mut dout = DatabaseEntry::new();
    cursor.get(&mut kout, &mut dout, Get::Search, None).unwrap();
    assert_eq!(cursor.count().unwrap(), 7);

    cursor.close().unwrap();
    let _ = env.close();
}

/// After a commit, the written dup is visible to the next read.
#[test]
fn test_dup_cursor_txn_isolation() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    // Write under a transaction and commit.
    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"iso_key");
    db.put_in(&txn, &key, DatabaseEntry::from_bytes(b"v1")).unwrap();
    txn.commit().unwrap();

    // After commit, data should be visible via a fresh auto-commit read.
    let mut out = DatabaseEntry::new();
    let s = db.get_into(None, &key, &mut out).unwrap();
    assert!(s, "committed dup not visible");
    assert_eq!(out.get_data().unwrap(), b"v1");

    let _ = env.close();
}

/// Insert dups, drop (implicit close+flush), reopen — all dups survive recovery.
#[test]
fn test_dup_database_recovery() {
    let dir = TempDir::new().unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);

    // Phase 1: write dups and close (drop flushes the WAL).
    {
        let env = open_env(&dir);
        let db = env.open_database(None, "recov", &db_cfg).unwrap();

        let key = DatabaseEntry::from_bytes(b"rk");
        for v in [b"a".as_ref(), b"b", b"c", b"d"] {
            db.put(&key, DatabaseEntry::from_bytes(v)).unwrap();
        }
        // Drop db first, then env — env Drop does final checkpoint + flush_sync.
        drop(db);
        drop(env);
    }

    // Phase 2: reopen and verify.
    {
        let env = open_env(&dir);
        let db = env.open_database(None, "recov", &db_cfg).unwrap();

        let mut cursor = db.open_cursor(None).unwrap();
        let mut kout = DatabaseEntry::new();
        let mut dout = DatabaseEntry::new();

        let s = cursor.get(&mut kout, &mut dout, Get::First, None).unwrap();
        assert_eq!(s, OperationStatus::Success, "first entry after recovery");
        assert_eq!(kout.get_data().unwrap(), b"rk");
        assert_eq!(dout.get_data().unwrap(), b"a");

        assert_eq!(cursor.count().unwrap(), 4, "all 4 dups survive recovery");

        cursor.close().unwrap();
        drop(db);
        drop(env);
    }
}

/// PrevDup navigates backward through duplicates.
#[test]
fn test_get_prev_dup() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"pk");
    db.put(&key, DatabaseEntry::from_bytes(b"x")).unwrap();
    db.put(&key, DatabaseEntry::from_bytes(b"y")).unwrap();
    db.put(&key, DatabaseEntry::from_bytes(b"z")).unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::new();
    let mut dout = DatabaseEntry::new();

    // Position on last dup "z".
    cursor.get(&mut kout, &mut dout, Get::Last, None).unwrap();
    assert_eq!(dout.get_data().unwrap(), b"z");

    // PrevDup should give "y" then "x".
    cursor.get(&mut kout, &mut dout, Get::PrevDup, None).unwrap();
    assert_eq!(dout.get_data().unwrap(), b"y");

    cursor.get(&mut kout, &mut dout, Get::PrevDup, None).unwrap();
    assert_eq!(dout.get_data().unwrap(), b"x");

    // No more dups backward.
    let s = cursor.get(&mut kout, &mut dout, Get::PrevDup, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);

    cursor.close().unwrap();
    let _ = env.close();
}

/// NoDupData put mode rejects exact duplicate (key, data) pairs.
#[test]
fn test_put_no_dup_data_rejects_exact_duplicate() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"k");
    let data = DatabaseEntry::from_bytes(b"v");
    db.put(&key, &data).unwrap();

    // Second insert with the same (key, data) pair using NoDupData.
    let mut cursor = db.open_cursor(None).unwrap();
    let s = cursor.put(&key, &data, Put::NoDupData).unwrap();
    assert_eq!(s, OperationStatus::KeyExists);

    // A different data value is allowed.
    let data2 = DatabaseEntry::from_bytes(b"w");
    let s2 = cursor.put(&key, &data2, Put::NoDupData).unwrap();
    assert_eq!(s2, OperationStatus::Success);

    cursor.close().unwrap();
    let _ = env.close();
}

// ---------------------------------------------------------------------------
// v1.5 Sprint 1 — sorted-dup count + delete regressions
// ---------------------------------------------------------------------------

/// `Database::count()` reports the total number of (key, data) pairs in a
/// sorted-duplicate database, including every duplicate.
///
/// Regression for v1.5 Sprint 1 finding (Group B, item 1): `put_dup`
/// previously bypassed the per-database entry counter, so `db.count()`
/// returned 0 even after inserting many duplicates.
#[test]
fn test_database_count_includes_all_dups() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    // Empty: count is 0.
    assert_eq!(db.count().unwrap(), 0);

    // 5 dups for the same key.
    let key = DatabaseEntry::from_bytes(b"k");
    for i in 0u8..5 {
        db.put(&key, DatabaseEntry::from_bytes(&[i])).unwrap();
    }
    // Plus 3 more (key, data) pairs under a different key.
    let key2 = DatabaseEntry::from_bytes(b"k2");
    for i in 0u8..3 {
        db.put(&key2, DatabaseEntry::from_bytes(&[i])).unwrap();
    }

    // Per BDB-JE Database.count() contract: total = 5 + 3 = 8.
    assert_eq!(
        db.count().unwrap(),
        8,
        "db.count() must include every duplicate pair"
    );

    // Re-inserting an existing exact (key, data) pair must not double-count.
    db.put(&key, DatabaseEntry::from_bytes(&[0])).unwrap();
    assert_eq!(db.count().unwrap(), 8);

    let _ = env.close();
}

/// `Database::delete(key)` on a sorted-duplicate database removes every
/// (key, data) pair sharing that key, matching BDB-JE's contract that
/// `Database.delete(key)` removes EVERY record with the supplied key.
///
/// Regression for v1.5 Sprint 1 finding (Group B, item 2): the previous
/// implementation positioned a cursor on the first dup and deleted only
/// that one, leaving the rest of the duplicate set intact.
#[test]
fn test_database_delete_removes_all_dups() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_transactional(true);
    let db = env.open_database(None, "test", &db_cfg).unwrap();

    // 4 dups under "target" + 2 dups under "keep" (to ensure delete is
    // scoped to the requested key).
    let target = DatabaseEntry::from_bytes(b"target");
    for v in [b"a".as_ref(), b"b", b"c", b"d"] {
        db.put(&target, DatabaseEntry::from_bytes(v)).unwrap();
    }
    let keep = DatabaseEntry::from_bytes(b"keep");
    for v in [b"x".as_ref(), b"y"] {
        db.put(&keep, DatabaseEntry::from_bytes(v)).unwrap();
    }
    assert_eq!(db.count().unwrap(), 6);

    // Delete the whole "target" duplicate set in one call.
    let s = db.delete(&target).unwrap();
    assert!(s);

    // No (target, *) record may remain.
    let mut out = DatabaseEntry::new();
    let s = db.get_into(None, &target, &mut out).unwrap();
    assert!(!s, "db.delete(target) must remove every dup");

    // The unrelated key's dups must remain.
    let mut out = DatabaseEntry::new();
    let s = db.get_into(None, &keep, &mut out).unwrap();
    assert!(s);

    // count() should now reflect only the surviving "keep" pairs.
    assert_eq!(db.count().unwrap(), 2);

    // A second delete of an absent key must report NotFound.
    let s = db.delete(&target).unwrap();
    assert!(!s);

    let _ = env.close();
}

// ─── Sprint 6 / Property 3 — SearchGte / Search / SearchBoth oracle ──────────
//
// Property: against a sorted-duplicate database populated with random
// (key, data) pairs (with intentional key collisions),
//
//   * `Get::Search(key)` must succeed iff the oracle has at least one dup
//     for `key`, and on success the returned data must be one of the
//     recorded dups.  (BDB-JE allows any dup; we only assert membership.)
//   * `Get::SearchBoth(key, data)` must succeed iff the (key, data) pair
//     exists in the oracle, and must return NotFound otherwise.
//   * `Get::SearchGte(seed)` must return the same key as the brute-force
//     `oracle.range(seed..).next()` answer.
//
// This is the sorted-dup cousin of the v1.4.3
// `cursor_search_gte_oracle_brute_force_small_random` test and targets
// the bug class fixed in Sprint 1B (Get::Search/SearchBoth/SearchGte
// disagreement with stored dups).

mod prop_sorted_dup_oracle {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 64,
            .. ProptestConfig::default()
        })]

        #[test]
        fn sorted_dup_search_oracle_brute_force_small_random(
            // 1..=64 (key, data) inserts with 1..=8-byte keys (high
            // collision rate -> exercises the dup tree) and 1..=16-byte
            // values.
            pairs in prop::collection::vec(
                (
                    prop::collection::vec(any::<u8>(), 1..=8),
                    prop::collection::vec(any::<u8>(), 1..=16),
                ),
                1..=64,
            ),
            // 1..=32 random probes for Search / SearchGte coverage of
            // both hits and misses.  Probes are independently random so
            // most miss; the few that collide with inserted keys exercise
            // the hit path.
            probes in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 1..=8),
                1..=32,
            ),
            // Random (key, data) probes for SearchBoth misses.
            both_probes in prop::collection::vec(
                (
                    prop::collection::vec(any::<u8>(), 1..=8),
                    prop::collection::vec(any::<u8>(), 1..=16),
                ),
                1..=16,
            ),
        ) {
            let dir = TempDir::new().unwrap();
            let env = open_env(&dir);
            let db_cfg = DatabaseConfig::new()
                .with_allow_create(true).with_transactional(true)
                .with_sorted_duplicates(true)
        .with_transactional(true);
            let db = env
                .open_database(None, "prop_sorted_dup", &db_cfg)
                .unwrap();

            // Oracle: key -> sorted set of dup data values.
            let mut oracle: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>> =
                BTreeMap::new();
            for (k, v) in &pairs {
                db.put(
                    DatabaseEntry::from_bytes(k),
                    DatabaseEntry::from_bytes(v),
                )
                .unwrap();
                // BDB-JE: a duplicate (key, data) re-insert is a no-op on a
                // sorted-dup DB.  The reshaped `put` returns `()` rather than
                // the old KeyExist/Success status, so the per-put status
                // assertion is gone; the resulting dup-set state is verified
                // against this oracle by the cursor-scan properties below.
                let oracle_entry = oracle.entry(k.clone()).or_default();
                oracle_entry.insert(v.clone());
            }

            let mut cursor = db.open_cursor(None).unwrap();

            // Property 3a: Get::Search agrees with oracle.contains_key.
            for probe in &probes {
                let mut k_e = DatabaseEntry::from_bytes(probe);
                let mut d_e = DatabaseEntry::new();
                let s = cursor
                    .get(&mut k_e, &mut d_e, Get::Search, None)
                    .unwrap();
                match (s, oracle.get(probe)) {
                    (OperationStatus::Success, Some(dups)) => {
                        prop_assert_eq!(
                            k_e.data(), probe.as_slice(),
                            "Search({:?}) returned wrong key", probe,
                        );
                        prop_assert!(
                            dups.contains(d_e.data()),
                            "Search({:?}) returned data {:?} not in oracle dup-set {:?}",
                            probe, d_e.data(), dups,
                        );
                    }
                    (OperationStatus::NotFound, None) => { /* agree */ }
                    (got, want) => prop_assert!(
                        false,
                        "Search({:?}) disagreement: got={:?}, oracle={:?}",
                        probe, got, want,
                    ),
                }
            }

            // Property 3b: Get::SearchGte agrees with oracle.range(probe..).
            for probe in &probes {
                let mut k_e = DatabaseEntry::from_bytes(probe);
                let mut d_e = DatabaseEntry::new();
                let s = cursor
                    .get(&mut k_e, &mut d_e, Get::SearchGte, None)
                    .unwrap();
                let want = oracle
                    .range::<Vec<u8>, _>(probe.clone()..)
                    .next();
                match (s, want) {
                    (OperationStatus::Success, Some((wk, wd))) => {
                        prop_assert_eq!(
                            k_e.data(), wk.as_slice(),
                            "SearchGte({:?}) returned wrong key (want {:?})",
                            probe, wk,
                        );
                        prop_assert!(
                            wd.contains(d_e.data()),
                            "SearchGte({:?}) returned data {:?} not in oracle dup-set {:?}",
                            probe, d_e.data(), wd,
                        );
                    }
                    (OperationStatus::NotFound, None) => { /* agree */ }
                    (got, want) => prop_assert!(
                        false,
                        "SearchGte({:?}) disagreement: got={:?}, oracle={:?}",
                        probe, got, want,
                    ),
                }
            }

            // Property 3c: Get::SearchBoth must succeed for every (key, data)
            // that the oracle says is present.
            for (k, dups) in &oracle {
                for d in dups {
                    let mut k_e = DatabaseEntry::from_bytes(k);
                    let mut d_e = DatabaseEntry::from_bytes(d);
                    let s = cursor
                        .get(&mut k_e, &mut d_e, Get::SearchBoth, None)
                        .unwrap();
                    prop_assert_eq!(
                        s, OperationStatus::Success,
                        "SearchBoth({:?},{:?}) returned NotFound for an inserted pair",
                        k, d,
                    );
                    prop_assert_eq!(k_e.data(), k.as_slice());
                    prop_assert_eq!(d_e.data(), d.as_slice());
                }
            }

            // Property 3d: Get::SearchBoth on a (key, data) NOT in the
            // oracle must return NotFound.
            for (k, d) in &both_probes {
                let oracle_has = oracle
                    .get(k)
                    .is_some_and(|dups| dups.contains(d));
                let mut k_e = DatabaseEntry::from_bytes(k);
                let mut d_e = DatabaseEntry::from_bytes(d);
                let s = cursor
                    .get(&mut k_e, &mut d_e, Get::SearchBoth, None)
                    .unwrap();
                let expected = if oracle_has {
                    OperationStatus::Success
                } else {
                    OperationStatus::NotFound
                };
                prop_assert_eq!(
                    s, expected,
                    "SearchBoth({:?},{:?}) status mismatch (oracle_has={})",
                    k, d, oracle_has,
                );
            }

            cursor.close().unwrap();
            let _ = env.close();
        }
    }
}
