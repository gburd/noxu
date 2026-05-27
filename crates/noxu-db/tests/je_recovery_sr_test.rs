//! JE recovery SR-numbered tests ported to Noxu.
//!
//! These verify that `Transaction::abort` (followed by env close + recovery)
//! restores the pre-transaction state.  Each test corresponds to a real JE
//! shipped bug.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

// ─── helpers ──────────────────────────────────────────────────────────────────

fn open_env_db(
    dir: &Path,
    db_name: &str,
    sorted_dups: bool,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(sorted_dups)
        .with_transactional(true);
    let db = env.open_database(None, db_name, &db_config).unwrap();
    (env, db)
}

fn collect_all_kv(db: &noxu_db::Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut c = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        out.push((
            k.get_data().unwrap_or(&[]).to_vec(),
            d.get_data().unwrap_or(&[]).to_vec(),
        ));
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    out
}

fn record_count(db: &noxu_db::Database) -> u64 {
    db.count().unwrap()
}

// ──────────────────────────────────────────────────────────────────────────────
// SR9465 Part 1 — RecoveryAbortTest.testSR9465Part1
//
// JE invariant (no-dups): After committing N records, then in a second txn
// deleting all of them and re-inserting them (with new data) and aborting,
// the database must still contain *exactly* the originally-committed records.
// After closing and recovering, the same invariant must still hold.
//
// JE comment: the abort sequence needs to undo the delete; the bug was that a
// re-insertion making the entry disappear from the BIN could make the entry
// vanish at recovery time (#9465 — "compressor lock taken on knownDeleted LN").
// ──────────────────────────────────────────────────────────────────────────────

const NUM_RECS: u32 = 50;

fn assert_data_matches(
    db: &noxu_db::Database,
    expected: &[(Vec<u8>, Vec<u8>)],
) {
    let actual = collect_all_kv(db);
    assert_eq!(
        actual.len(),
        expected.len(),
        "record count mismatch: actual={:?} expected={:?}",
        actual,
        expected
    );
    let mut e = expected.to_vec();
    let mut a = actual;
    e.sort();
    a.sort();
    assert_eq!(a, e);
}

#[test]
fn sr9465_part1_delete_reinsert_abort_restores_no_dups() {
    let dir = TempDir::new().unwrap();
    let path: PathBuf = dir.path().to_path_buf();

    let expected = {
        // Phase 1: create env, insert N records, commit.
        let (env, db) = open_env_db(&path, "sr9465p1", false);
        let txn1 = env.begin_transaction(None).unwrap();
        let mut expected = Vec::with_capacity(NUM_RECS as usize);
        for i in 0..NUM_RECS {
            let k = i.to_be_bytes().to_vec();
            let v = format!("orig-{i}").into_bytes();
            let key = DatabaseEntry::from_bytes(&k);
            let val = DatabaseEntry::from_bytes(&v);
            db.put(Some(&txn1), &key, &val).unwrap();
            expected.push((k, v));
        }
        txn1.commit().unwrap();

        // Phase 2: delete all + re-insert with different values, then abort.
        let txn2 = env.begin_transaction(None).unwrap();
        for (k, _) in &expected {
            let key = DatabaseEntry::from_bytes(k);
            let s = db.delete(Some(&txn2), &key).unwrap();
            assert_eq!(s, OperationStatus::Success);
        }
        for (k, _) in &expected {
            let key = DatabaseEntry::from_bytes(k);
            let v2 = format!("aborted-{}", k.len()).into_bytes();
            let val = DatabaseEntry::from_bytes(&v2);
            db.put(Some(&txn2), &key, &val).unwrap();
        }
        txn2.abort().unwrap();

        // Verify: the database must still contain the originally-committed data.
        assert_data_matches(&db, &expected);
        assert_eq!(record_count(&db), NUM_RECS as u64);
        // Phase 1+2 handles drop here so the FileManager lock is released
        // before the recovery reopen below.  See `sr9752_part1` for the
        // matching pattern.
        expected
    };

    // Reopen (forces recovery), verify again.
    let (env2, db2) = open_env_db(&path, "sr9465p1", false);
    assert_data_matches(&db2, &expected);
    assert_eq!(record_count(&db2), NUM_RECS as u64);
    drop(db2);
    drop(env2);
}

// ──────────────────────────────────────────────────────────────────────────────
// SR9465 Part 2 — RecoveryAbortTest.testSR9465Part2
//
// Like Part 1, but the aborting txn does delete → insert → delete → abort.
// The double-delete-during-an-aborted-txn was a related #9465 bug.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr9465_part2_delete_reinsert_redelete_abort_restores_no_dups() {
    let dir = TempDir::new().unwrap();
    let path: PathBuf = dir.path().to_path_buf();

    let expected = {
        let (env, db) = open_env_db(&path, "sr9465p2", false);
        let txn1 = env.begin_transaction(None).unwrap();
        let expected = {
            let mut v = Vec::with_capacity(NUM_RECS as usize);
            for i in 0..NUM_RECS {
                let k = i.to_be_bytes().to_vec();
                let val_bytes = format!("orig-{i}").into_bytes();
                db.put(
                    Some(&txn1),
                    &DatabaseEntry::from_bytes(&k),
                    &DatabaseEntry::from_bytes(&val_bytes),
                )
                .unwrap();
                v.push((k, val_bytes));
            }
            v
        };
        txn1.commit().unwrap();

        let txn2 = env.begin_transaction(None).unwrap();
        for (k, _) in &expected {
            db.delete(Some(&txn2), &DatabaseEntry::from_bytes(k)).unwrap();
        }
        for (k, _) in &expected {
            db.put(
                Some(&txn2),
                &DatabaseEntry::from_bytes(k),
                &DatabaseEntry::from_bytes(b"new"),
            )
            .unwrap();
        }
        for (k, _) in &expected {
            db.delete(Some(&txn2), &DatabaseEntry::from_bytes(k)).unwrap();
        }
        txn2.abort().unwrap();

        assert_data_matches(&db, &expected);
        // Phase 1+2 handles drop here; see `sr9465_part1` for rationale.
        expected
    };

    let (env2, db2) = open_env_db(&path, "sr9465p2", false);
    assert_data_matches(&db2, &expected);
    drop(db2);
    drop(env2);
}

// ──────────────────────────────────────────────────────────────────────────────
// SR9752 Part 1 — RecoveryAbortTest.testSR9752Part1 (no-dups)
//
// JE invariant: an aborting txn that mutates a key written by an earlier
// committed txn must fully roll back to the earlier value, including across
// recovery.  The bug was that recovery could resurrect the aborted value.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr9752_part1_abort_after_committed_write_reverts_no_dups() {
    let dir = TempDir::new().unwrap();
    let path: PathBuf = dir.path().to_path_buf();

    {
        let (env, db) = open_env_db(&path, "sr9752p1", false);
        // Commit an initial value.
        let txn1 = env.begin_transaction(None).unwrap();
        db.put(
            Some(&txn1),
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"committed"),
        )
        .unwrap();
        txn1.commit().unwrap();

        // Abort an overwrite.
        let txn2 = env.begin_transaction(None).unwrap();
        db.put(
            Some(&txn2),
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"aborted"),
        )
        .unwrap();
        txn2.abort().unwrap();

        // Pre-recovery: original value must still be visible.
        let mut out = DatabaseEntry::new();
        let s =
            db.get(None, &DatabaseEntry::from_bytes(b"k"), &mut out).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), b"committed");

        drop(db);
        drop(env);
    }

    // Recovery: reopen and verify the same.
    let (env2, db2) = open_env_db(&path, "sr9752p1", false);
    let mut out2 = DatabaseEntry::new();
    let s = db2.get(None, &DatabaseEntry::from_bytes(b"k"), &mut out2).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(
        out2.get_data().unwrap(),
        b"committed",
        "abort + recovery must restore the previously-committed value, not the aborted overwrite"
    );
    drop(db2);
    drop(env2);
}

// ──────────────────────────────────────────────────────────────────────────────
// SR9752 Part 2 — RecoveryAbortTest.testSR9752Part2 (sorted-dups)
//
// Same invariant as Part 1, but for a sorted-dups database: aborted dup
// inserts must NOT show up post-recovery.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr9752_part2_abort_after_committed_dups_reverts_with_dups() {
    let dir = TempDir::new().unwrap();
    let path: PathBuf = dir.path().to_path_buf();

    {
        let (env, db) = open_env_db(&path, "sr9752p2", true);
        // Commit some dups under one key.
        let txn1 = env.begin_transaction(None).unwrap();
        let key = DatabaseEntry::from_bytes(b"k");
        for d in [b"a".as_slice(), b"b", b"c"] {
            db.put(Some(&txn1), &key, &DatabaseEntry::from_bytes(d)).unwrap();
        }
        txn1.commit().unwrap();

        // Abort additional dups.
        let txn2 = env.begin_transaction(None).unwrap();
        for d in [b"x".as_slice(), b"y", b"z"] {
            db.put(Some(&txn2), &key, &DatabaseEntry::from_bytes(d)).unwrap();
        }
        txn2.abort().unwrap();

        // Pre-recovery: only original 3 dups visible.
        let actual = collect_all_kv(&db);
        let actual_data: Vec<&[u8]> =
            actual.iter().map(|(_, d)| d.as_slice()).collect();
        assert_eq!(
            actual_data,
            vec![b"a".as_slice(), b"b", b"c"],
            "after abort, only originally-committed dups must be visible"
        );
        // Inner block drop releases env / db / txn handles before recovery.
    }

    let (env2, db2) = open_env_db(&path, "sr9752p2", true);
    let actual = collect_all_kv(&db2);
    let actual_data: Vec<&[u8]> =
        actual.iter().map(|(_, d)| d.as_slice()).collect();
    assert_eq!(
        actual_data,
        vec![b"a".as_slice(), b"b", b"c"],
        "post-recovery: aborted dups must NOT appear"
    );
    drop(db2);
    drop(env2);
}
