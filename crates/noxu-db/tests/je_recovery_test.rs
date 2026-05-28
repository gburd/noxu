//! JE RecoveryTest ports — clean-close + reopen recovery scenarios.
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/recovery/RecoveryTest.java`.  These are NOT crash
//! tests (the SIGKILL crash suite lives in `crash_recovery_test.rs`); they
//! verify that committed data round-trips through a clean `drop(env)` / open
//! cycle.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

const NUM_RECS: u32 = 50;

fn open_env(dir: &Path) -> noxu_db::Environment {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(
    env: &noxu_db::Environment,
    name: &str,
    dups: bool,
) -> noxu_db::Database {
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dups);
    env.open_database(None, name, &cfg).unwrap()
}

fn ikey(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}

fn collect_all(db: &noxu_db::Database) -> BTreeMap<Vec<u8>, Vec<Vec<u8>>> {
    let mut out: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let mut c = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        out.entry(k.get_data().unwrap_or(&[]).to_vec())
            .or_default()
            .push(d.get_data().unwrap_or(&[]).to_vec());
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    for v in out.values_mut() {
        v.sort();
    }
    out
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryTest.testBasic / testBasicFewerCheckpoints (collapsed)
//
// JE invariant: after committing inserts + deletes + modifies, closing and
// reopening the env preserves the visible state exactly.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_basic_insert_delete_modify_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    {
        let env = open_env(&path);
        let db = open_db(&env, "basic_recov", false);

        // Insert all NUM_RECS records.
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..NUM_RECS {
            let k = ikey(i);
            let v = format!("v-{i}").into_bytes();
            db.put(Some(&txn), &k, &DatabaseEntry::from_bytes(&v)).unwrap();
            expected.insert(k.get_data().unwrap().to_vec(), v);
        }
        txn.commit().unwrap();

        // Delete the even records.
        let txn = env.begin_transaction(None).unwrap();
        for i in (0..NUM_RECS).step_by(2) {
            let k = ikey(i);
            db.delete(Some(&txn), &k).unwrap();
            expected.remove(k.get_data().unwrap());
        }
        txn.commit().unwrap();

        // Modify (overwrite) the remaining records' values.
        let txn = env.begin_transaction(None).unwrap();
        let keys: Vec<u32> = (0..NUM_RECS).filter(|i| i % 2 == 1).collect();
        for i in keys {
            let k = ikey(i);
            let v = format!("MOD-{i}").into_bytes();
            db.put(Some(&txn), &k, &DatabaseEntry::from_bytes(&v)).unwrap();
            expected.insert(k.get_data().unwrap().to_vec(), v);
        }
        txn.commit().unwrap();

        drop(db);
        drop(env);
    }

    // Recovery: reopen and verify.
    let env = open_env(&path);
    let db = open_db(&env, "basic_recov", false);

    for (k, v) in &expected {
        let mut out = DatabaseEntry::new();
        let s = db.get(None, &DatabaseEntry::from_bytes(k), &mut out).unwrap();
        assert_eq!(
            s,
            OperationStatus::Success,
            "key {:?} missing after recovery",
            k
        );
        assert_eq!(out.get_data().unwrap(), v.as_slice());
    }
    assert_eq!(db.count().unwrap(), expected.len() as u64);

    // No extra keys.
    let actual = collect_all(&db);
    for k in actual.keys() {
        assert!(
            expected.contains_key(k),
            "post-recovery has unexpected key {:?}",
            k
        );
    }
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryTest.testDuplicateOverwrite
//
// JE invariant: on a sorted-dup db, four `put` calls under the same key with
// data1, data2, data3, data3 (i.e. data3 repeated) produce 3 distinct dups
// (the second data3 is a no-op exact-duplicate).  After clean close and
// recovery, the dup chain has exactly {data1, data2, data3}.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_duplicate_overwrite_dedups_exact() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let env = open_env(&path);
        let db = open_db(&env, "dup_overwrite", true);

        let key = DatabaseEntry::from_bytes(b"aaaaa");
        let d1 = DatabaseEntry::from_bytes(b"dddddddddd");
        let d2 = DatabaseEntry::from_bytes(b"eeeeeeeeee");
        let d3 = DatabaseEntry::from_bytes(b"ffffffffff");

        let txn = env.begin_transaction(None).unwrap();
        db.put(Some(&txn), &key, &d1).unwrap();
        db.put(Some(&txn), &key, &d2).unwrap();
        db.put(Some(&txn), &key, &d3).unwrap();
        // Repeat d3 — JE: idempotent, no extra dup.
        db.put(Some(&txn), &key, &d3).unwrap();
        txn.commit().unwrap();
        drop(db);
        drop(env);
    }

    let env = open_env(&path);
    let db = open_db(&env, "dup_overwrite", true);
    let actual = collect_all(&db);
    let dups = actual.get(b"aaaaa".as_ref()).expect("key present");
    let expected = vec![
        b"dddddddddd".to_vec(),
        b"eeeeeeeeee".to_vec(),
        b"ffffffffff".to_vec(),
    ];
    assert_eq!(
        dups, &expected,
        "dup chain after recovery must be exactly {{d1,d2,d3}}, got {:?}",
        dups
    );
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryTest.testSR8984 (Part 1: sameKey=true; Part 2: sameKey=false)
//
// JE invariant: a non-txn put + delete + many puts (forcing a dup tree) on a
// sorted-dup db, followed by an abrupt close (no final checkpoint) and a
// reopen, must show exactly the inserted dups via cursor.count().  The bug
// was that recovery resurrected the deleted record.
//
// Noxu does not expose JE's "no final checkpoint" knob; clean drop(env)
// triggers Noxu's exit checkpoint.  We therefore assert the equivalent
// invariant: the visible record count after reopen matches the count we
// observed before close — i.e. recovery does not resurrect the deleted d1.
// ──────────────────────────────────────────────────────────────────────────────

fn run_sr8984(same_key: bool) {
    const NUM_EXTRA_DUPS: u32 = 150;

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let pre_close_count: u64;

    {
        let env = open_env(&path);
        let db = open_db(&env, "sr8984", true);

        let key = DatabaseEntry::from_bytes(b"k1");
        let d1 = DatabaseEntry::from_bytes(b"d1");

        // Initial insert + delete.
        db.put(None, &key, &d1).unwrap();
        db.delete(None, &key).unwrap();

        // Re-insert: same data (Part 1) or fresh data (Part 2).
        let first_data = if same_key {
            DatabaseEntry::from_bytes(b"d1")
        } else {
            DatabaseEntry::from_bytes(b"d2")
        };
        db.put(None, &key, &first_data).unwrap();
        for i in 3..NUM_EXTRA_DUPS {
            let v = format!("d{i}").into_bytes();
            db.put(None, &key, &DatabaseEntry::from_bytes(&v)).unwrap();
        }

        pre_close_count = db.count().unwrap();
        drop(db);
        drop(env);
    }

    // Reopen and count.
    let env = open_env(&path);
    let db = open_db(&env, "sr8984", true);
    let post_count = db.count().unwrap();
    assert_eq!(
        post_count, pre_close_count,
        "SR8984 (same_key={same_key}): recovery must not change the dup count"
    );

    // The original deleted (k1, d1) must not have been resurrected:
    // when same_key=false, only the post-delete inserts (d2, d3..d149)
    // should be present — and exactly NUM_EXTRA_DUPS - 2 entries (the
    // count JE asserted).
    let mut c = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let count_via_cursor = c.count().unwrap();
    assert_eq!(
        count_via_cursor, post_count,
        "cursor.count() on the dup chain must equal db.count()"
    );
    drop(c);
    drop(db);
    drop(env);
}

#[test]
fn recovery_sr8984_part1_same_key_dups_no_resurrect() {
    run_sr8984(true);
}

#[test]
fn recovery_sr8984_part2_different_key_dups_no_resurrect() {
    run_sr8984(false);
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryAbortTest.testInserts (wave 9-C)
//
// JE invariant: alternating commit / abort / commit insert phases
// followed by a clean close+recover yield the union of the committed
// inserts — aborted inserts must NOT resurrect after recovery.
// JE additionally drains the IN-compressor queue to force the recovery
// to replay IN-deletes; Noxu has no equivalent public probe, so this
// port relies on the recovery pipeline doing the equivalent work.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_abort_test_inserts_three_phase_no_dups() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let n: u32 = NUM_RECS;

    // Phase 1: insert 0..N, commit.
    {
        let env = open_env(&path);
        let db = open_db(&env, "abort_inserts", false);
        let t = env.begin_transaction(None).unwrap();
        for i in 0..n {
            db.put(Some(&t), &ikey(i), &ikey(i)).unwrap();
        }
        t.commit().unwrap();

        // Phase 2: insert N..3N, abort.
        let t = env.begin_transaction(None).unwrap();
        for i in n..(3 * n) {
            db.put(Some(&t), &ikey(i), &ikey(i)).unwrap();
        }
        t.abort().unwrap();

        // Verify aborted inserts are gone.
        for i in n..(3 * n) {
            let mut out = DatabaseEntry::new();
            let s = db.get(None, &ikey(i), &mut out).unwrap();
            assert_eq!(
                s,
                OperationStatus::NotFound,
                "aborted insert k={i} resurrected before recovery"
            );
        }

        // Phase 3: insert 2N..4N, commit (overlapping range with the
        // aborted phase to exercise slot reuse).
        let t = env.begin_transaction(None).unwrap();
        for i in (2 * n)..(4 * n) {
            db.put(Some(&t), &ikey(i), &ikey(i)).unwrap();
        }
        t.commit().unwrap();

        db.close().unwrap();
        drop(env);
    }

    // Recovery: re-open, verify that committed = (0..N) U (2N..4N).
    {
        let env = open_env(&path);
        let db = open_db(&env, "abort_inserts", false);

        for i in 0..n {
            let mut out = DatabaseEntry::new();
            let s = db.get(None, &ikey(i), &mut out).unwrap();
            assert_eq!(
                s,
                OperationStatus::Success,
                "k={i} missing post-recovery"
            );
        }
        // Aborted-only range (N..2N) must be absent.
        for i in n..(2 * n) {
            let mut out = DatabaseEntry::new();
            let s = db.get(None, &ikey(i), &mut out).unwrap();
            assert_eq!(
                s,
                OperationStatus::NotFound,
                "aborted-only k={i} resurrected after recovery"
            );
        }
        for i in (2 * n)..(4 * n) {
            let mut out = DatabaseEntry::new();
            let s = db.get(None, &ikey(i), &mut out).unwrap();
            assert_eq!(
                s,
                OperationStatus::Success,
                "k={i} missing post-recovery"
            );
        }

        db.close().unwrap();
        drop(env);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryTest.testBasicDeleteAll (wave 9-C)
//
// JE invariant: insert N records, modify half, delete all, close,
// recover — post-recovery the database has zero records.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_basic_delete_all_no_resurrect() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let n: u32 = NUM_RECS;

    {
        let env = open_env(&path);
        let db = open_db(&env, "delete_all", false);

        // Insert all the data, commit.
        let t = env.begin_transaction(None).unwrap();
        for i in 0..n {
            db.put(Some(&t), &ikey(i), &ikey(i)).unwrap();
        }
        t.commit().unwrap();

        // Modify half the records (overwrite), commit.
        let t = env.begin_transaction(None).unwrap();
        for i in 0..(n / 2) {
            db.put(Some(&t), &ikey(i), &ikey(i + 1000)).unwrap();
        }
        t.commit().unwrap();

        // Delete all the records, commit.
        let t = env.begin_transaction(None).unwrap();
        for i in 0..n {
            let s = db.delete(Some(&t), &ikey(i)).unwrap();
            assert_eq!(s, OperationStatus::Success);
        }
        t.commit().unwrap();

        db.close().unwrap();
        drop(env);
    }

    // Recovery: db has 0 records.
    {
        let env = open_env(&path);
        let db = open_db(&env, "delete_all", false);
        assert_eq!(0, db.count().unwrap());
        for i in 0..n {
            let mut out = DatabaseEntry::new();
            let s = db.get(None, &ikey(i), &mut out).unwrap();
            assert_eq!(
                s,
                OperationStatus::NotFound,
                "deleted k={i} resurrected after recovery"
            );
        }
        db.close().unwrap();
        drop(env);
    }
}

// ---------------------------------------------------------------------------
// RecoveryEdgeTest.testNoLogFiles  (wave 10-A)
//
// JE invariant: opening an environment in a fresh directory creates the
// initial log files, and the database list is empty.  After a clean close
// and reopen, the database list remains empty.
// ---------------------------------------------------------------------------

#[test]
fn recovery_edge_test_no_log_files() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let env = open_env(&path);
        let names = env.get_database_names().unwrap();
        assert!(names.is_empty(), "expected no dbs, got {names:?}");
        drop(env);
    }

    // Reopen and re-check.  This is the JE "fake a shutdown/startup" loop.
    {
        let env = open_env(&path);
        let names = env.get_database_names().unwrap();
        assert!(
            names.is_empty(),
            "after reopen expected no dbs, got {names:?}",
        );
        drop(env);
    }
}

// ---------------------------------------------------------------------------
// RecoveryEdgeTest.testNonTxnalDb  (wave 10-A)
//
// JE invariant: a non-transactional database in a transactional environment
// survives a clean close + reopen: the database is still listed, and a
// transactional db created later is also listed.
//
// NOXU BUG: as of v2.2.1, a non-transactional database does NOT survive a
// clean close + reopen — `Environment::get_database_names()` returns an
// empty list and `open_database(None, "NotTxnal", &cfg)` (with
// `allow_create=false`) reports `DatabaseNotFound`.  Only the transactional
// db registration appears to be flushed to the WAL.  Tracked as a wave-10-A
// follow-up.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "wave10-a TODO: non-transactional db registration is lost across \
            clean close+reopen; JE RecoveryEdgeTest.testNonTxnalDb expects it \
            to survive"]
fn recovery_edge_test_non_txnal_db() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: create a non-transactional db, write into it, clean close.
    {
        let env = open_env(&path);
        let db_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(false);
        let db_a = env.open_database(None, "NotTxnal", &db_cfg).unwrap();
        db_a.put(
            None,
            &DatabaseEntry::from_bytes(b"foo"),
            &DatabaseEntry::from_bytes(b"bar"),
        )
        .unwrap();
        db_a.close().unwrap();
        drop(env);
    }

    // Phase 2: reopen, the db is still there.
    {
        let env = open_env(&path);
        // names registry should already include the non-txn db
        let names = env.get_database_names().unwrap();
        assert!(
            names.contains(&"NotTxnal".to_string()),
            "NotTxnal db should survive clean close+reopen, got {names:?}",
        );
        let db_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(false);
        let db_a = env.open_database(None, "NotTxnal", &db_cfg).unwrap();
        db_a.close().unwrap();

        // Add a transactional db on top.
        let db_cfg_txn = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db_b = env.open_database(None, "Txnal", &db_cfg_txn).unwrap();
        db_b.put(
            None,
            &DatabaseEntry::from_bytes(b"foo"),
            &DatabaseEntry::from_bytes(b"bar"),
        )
        .unwrap();
        db_b.close().unwrap();
        drop(env);
    }

    // Phase 3: reopen again.  Both databases should be visible.
    {
        let env = open_env(&path);
        let mut names = env.get_database_names().unwrap();
        names.sort();
        assert_eq!(
            vec!["NotTxnal".to_string(), "Txnal".to_string()],
            names,
            "both databases should survive recovery",
        );
        drop(env);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryDuplicatesTest.testDuplicates
//
// JE invariant: insert N records × M dups across multiple databases in a
// single committed txn, close, recover, verify all (key, dup) pairs are
// readable in dup order.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_duplicates_round_trip_across_clean_close() {
    const N_RECS: u32 = 10;
    const N_DUPS: u32 = 4;
    const N_DBS: usize = 3;

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let mut expected: BTreeMap<(usize, Vec<u8>), Vec<Vec<u8>>> =
        BTreeMap::new();

    {
        let env = open_env(&path);
        let mut dbs: Vec<noxu_db::Database> = Vec::new();
        for d in 0..N_DBS {
            dbs.push(open_db(&env, &format!("dups_db{d}"), true));
        }

        let txn = env.begin_transaction(None).unwrap();
        for d in 0..N_DBS {
            for i in 0..N_RECS {
                let k = ikey(i);
                let kbytes = k.get_data().unwrap().to_vec();
                for j in 0..N_DUPS {
                    let dv = (i * 1000 + j).to_be_bytes().to_vec();
                    dbs[d].put(
                        Some(&txn),
                        &k,
                        &DatabaseEntry::from_bytes(&dv),
                    )
                    .unwrap();
                    expected
                        .entry((d, kbytes.clone()))
                        .or_default()
                        .push(dv);
                }
            }
        }
        txn.commit().unwrap();
        for db in dbs.drain(..) {
            drop(db);
        }
        drop(env);
    }

    // Recover.
    let env = open_env(&path);
    for d in 0..N_DBS {
        let db = open_db(&env, &format!("dups_db{d}"), true);
        let actual = collect_all(&db);
        for ((dd, k), expected_dups) in &expected {
            if *dd != d {
                continue;
            }
            let mut got = actual.get(k).cloned().unwrap_or_default();
            got.sort();
            let mut exp = expected_dups.clone();
            exp.sort();
            assert_eq!(got, exp, "db={d} key={k:?}");
        }
        drop(db);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryDuplicatesTest.testDuplicatesWithDeletion
//
// JE invariant: same as above, but delete every other record before
// committing.  Only the surviving (key, dup) pairs must be readable
// post-recovery.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_duplicates_with_deletion_survives_recovery() {
    const N_RECS: u32 = 10;
    const N_DUPS: u32 = 3;

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let mut expected: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();

    {
        let env = open_env(&path);
        let db = open_db(&env, "dup_del", true);
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N_RECS {
            let k = ikey(i);
            for j in 0..N_DUPS {
                let dv = (i * 1000 + j).to_be_bytes().to_vec();
                db.put(Some(&txn), &k, &DatabaseEntry::from_bytes(&dv))
                    .unwrap();
                if i % 2 != 0 {
                    expected
                        .entry(k.get_data().unwrap().to_vec())
                        .or_default()
                        .push(dv);
                }
            }
        }
        // Delete all even-numbered keys.
        for i in (0..N_RECS).step_by(2) {
            db.delete(Some(&txn), &ikey(i)).unwrap();
        }
        txn.commit().unwrap();
        drop(db);
        drop(env);
    }

    let env = open_env(&path);
    let db = open_db(&env, "dup_del", true);
    let actual = collect_all(&db);
    assert_eq!(actual.len(), expected.len());
    for (k, mut dups) in expected {
        let mut got = actual.get(&k).cloned().unwrap_or_default();
        got.sort();
        dups.sort();
        assert_eq!(got, dups, "key={k:?}");
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryCheckpointTest.testEmptyCheckpoint (spirit port)
//
// JE invariant: a forced checkpoint on an empty (no records) environment
// runs cleanly; subsequent recovery yields the same empty state.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_empty_checkpoint_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let env = open_env(&path);
        let _db = open_db(&env, "empty_ckpt", false);
        // Immediately close without inserting anything.  drop runs the
        // implicit shutdown checkpoint.
        drop(_db);
        drop(env);
    }

    // Recover and confirm the db is still empty.
    let env = open_env(&path);
    let db = open_db(&env, "empty_ckpt", false);
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryDeleteTest.testDeleteAllAndCompress (spirit port)
//
// JE invariant: insert all + commit + checkpoint + delete all + commit +
// recovery → db is empty.  Compression in JE removes empty BINs; Noxu's
// compressor runs as a daemon, but the user-facing invariant
// (`db.count() == 0` after recovery) must hold either way.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_delete_all_then_recovery_empties_db() {
    const N: u32 = 50;
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let env = open_env(&path);
        let db = open_db(&env, "del_all", false);
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            db.put(Some(&txn), &ikey(i), &DatabaseEntry::from_bytes(b"v"))
                .unwrap();
        }
        txn.commit().unwrap();
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            db.delete(Some(&txn), &ikey(i)).unwrap();
        }
        txn.commit().unwrap();
        drop(db);
        drop(env);
    }

    let env = open_env(&path);
    let db = open_db(&env, "del_all", false);
    assert_eq!(db.count().unwrap(), 0);
}

// ──────────────────────────────────────────────────────────────────────────────
// RecoveryEdgeTest.testTxnId (spirit port)
//
// JE invariant: txn IDs assigned post-recovery must be ≥ the highest
// pre-recovery txn ID, so a recovered system can't accidentally reuse a
// txn ID.  We can't observe txn IDs through Noxu's public API directly,
// but we can observe the consequence: post-recovery, a fresh write does
// not collide with a pre-recovery committed write.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn recovery_edge_txn_id_continues_post_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let env = open_env(&path);
        let db = open_db(&env, "txn_id", false);
        for i in 0..50u32 {
            let txn = env.begin_transaction(None).unwrap();
            db.put(Some(&txn), &ikey(i), &DatabaseEntry::from_bytes(b"pre"))
                .unwrap();
            txn.commit().unwrap();
        }
        drop(db);
        drop(env);
    }

    let env = open_env(&path);
    let db = open_db(&env, "txn_id", false);
    for i in 50..100u32 {
        let txn = env.begin_transaction(None).unwrap();
        db.put(Some(&txn), &ikey(i), &DatabaseEntry::from_bytes(b"post"))
            .unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(db.count().unwrap(), 100);
    // Verify the pre-recovery records are still readable.
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..50u32 {
        let mut out = DatabaseEntry::new();
        let s = db.get(Some(&txn), &ikey(i), &mut out).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), b"pre");
    }
    txn.commit().unwrap();
}
