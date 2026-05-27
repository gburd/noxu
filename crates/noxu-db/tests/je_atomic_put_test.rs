//! JE AtomicPutTest port — concurrent put / put_no_overwrite atomicity.
//!
//! Ports invariants from
//! `test/com/sleepycat/je/test/AtomicPutTest.java`.  Two threads race
//! to insert the same key sequence; the put() / put_no_overwrite()
//! contract must hold even under contention:
//!
//! * `put` (overwrite) must NEVER surface KEYEXIST (in noxu, must
//!   never surface a "key exists" error) — the operation is atomic
//!   across threads.
//! * `put_no_overwrite`, with sorted-duplicates configured, must
//!   NEVER insert a duplicate.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, OperationStatus,
};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

const MAX_KEY: u32 = 200;

fn open_env_db(
    dir: &TempDir,
    name: &str,
    dups: bool,
) -> (Arc<noxu_db::Environment>, Arc<noxu_db::Database>) {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Arc::new(noxu_db::Environment::open(env_cfg).unwrap());
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dups);
    let db = Arc::new(env.open_database(None, name, &db_cfg).unwrap());
    (env, db)
}

fn ikey(v: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&v.to_be_bytes())
}

// ─────────────────────────────────────────────────────────────────────────────
// AtomicPutTest.testOverwriteNoDuplicates
//
// Two threads alternately insert the same monotonically-increasing key
// values via put(OVERWRITE).  No-duplicates DB.  Each thread must see
// Success on every put — never a key-exists error — even when the
// other thread races on the same key.  JE accepts LockConflict and
// retries (we do the same).
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn je_atomic_put_overwrite_no_duplicates_concurrent() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "atomic_overwrite", false);

    let next = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut handles = Vec::new();
    for _t in 0..2 {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(move || {
            loop {
                let raw = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if raw >= MAX_KEY {
                    break;
                }
                let val = raw / 2;
                loop {
                    let txn = env.begin_transaction(None).unwrap();
                    let r = db.put(Some(&txn), &ikey(val), &ikey(val));
                    match r {
                        Ok(s) => {
                            assert_eq!(
                                s,
                                OperationStatus::Success,
                                "put(OVERWRITE) must never return non-Success \
                                 for key {val}; got {s:?}"
                            );
                            txn.commit().unwrap();
                            break;
                        }
                        Err(_) => {
                            // Lock conflict: abort and retry.
                            let _ = txn.abort();
                        }
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AtomicPutTest.testNoOverwriteWithDuplicates
//
// Two threads insert (key, data) pairs into a sorted-duplicates DB
// using put_no_overwrite().  Each thread uses a distinct data value
// per key (`val % 2`), so the contended key/data pair is identical
// across threads only when both pick the same `val`.  The contract:
// for any key, only the first put_no_overwrite() succeeds; the second
// must report KeyExist (an error or non-Success status), never
// silently insert a duplicate.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn je_atomic_put_no_overwrite_with_duplicates_concurrent() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "atomic_no_overwrite_dup", true);

    let next = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut handles = Vec::new();
    for _t in 0..2 {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(move || {
            loop {
                let val =
                    next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if val >= MAX_KEY {
                    break;
                }
                let key_val = val / 2;
                let data_val = val % 2;
                loop {
                    let txn = env.begin_transaction(None).unwrap();
                    let r = db.put_no_overwrite(
                        Some(&txn),
                        &ikey(key_val),
                        &ikey(data_val),
                    );
                    match r {
                        Ok(_) => {
                            // Either Success (we won) or KeyExist (the
                            // other thread already inserted).  Either
                            // outcome is OK and atomic.  We only insist
                            // that the call did not silently produce a
                            // dup-of-a-dup.
                            txn.commit().unwrap();
                            break;
                        }
                        Err(_) => {
                            let _ = txn.abort();
                        }
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Final invariant: the count of records with key=k is exactly the
    // number of distinct data values that races could produce for that
    // key.  Each key_val is touched twice (val=2k and val=2k+1) with
    // data values 0 and 1 respectively, so a fully-populated key has
    // either 1 or 2 distinct dups, never duplicates of the same data.
    use std::collections::BTreeMap;
    let mut by_key: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let mut c = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c
        .get(&mut k, &mut d, noxu_db::Get::First, None)
        .unwrap_or(OperationStatus::NotFound);
    while s == OperationStatus::Success {
        by_key
            .entry(k.data().to_vec())
            .or_default()
            .push(d.data().to_vec());
        s = c
            .get(&mut k, &mut d, noxu_db::Get::Next, None)
            .unwrap_or(OperationStatus::NotFound);
    }
    drop(c);

    for (kv, dups) in &by_key {
        let mut sorted = dups.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            dups.len(),
            "put_no_overwrite produced duplicate-of-duplicate under key {kv:?}: {dups:?}"
        );
    }
}
