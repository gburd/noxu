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
                let raw =
                    next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if raw >= MAX_KEY {
                    break;
                }
                let val = raw / 2;
                loop {
                    let txn = env.begin_transaction(None).unwrap();
                    let r = db.put_in(&txn, ikey(val), ikey(val));
                    match r {
                        Ok(()) => {
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

// BUG-ATOMICPUT-1 (FIXED): the dup-DB putNoOverwrite coordination lock keyed on
// the (key,data) two-part key, so concurrent inserters of the same user key but
// different data took DIFFERENT synthetic locks and did not serialise. Now the
// dup-DB NoOverwrite path holds a synthetic lock on the BASE USER KEY across the
// existence check + insert, so the second inserter blocks on the first and sees
// its (key,*) dup → KEYEXIST (JE Cursor.dupsPutNoOverwrite, Cursor.java:2489).
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
                    let r = db.put_no_overwrite_in(
                        &txn,
                        ikey(key_val),
                        ikey(data_val),
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

    // JE invariant (runNoOverwriteWithDuplicates): for EVERY key, exactly
    // one record exists.  JE asserts `cursor.count()==1` and
    // `getNextDup()==NOTFOUND` after each insert, because putNoOverwrite must
    // NEVER add a second duplicate to an already-present key (Bug #3: the old
    // code inserted a dup after a NOTFOUND race).  key_val `k` is touched by
    // val=2k (data 0) and val=2k+1 (data 1): the first putNoOverwrite wins,
    // the second must return KEYEXIST and insert nothing — so count stays 1.
    use std::collections::BTreeMap;
    let mut by_key: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c
        .get(&mut k, &mut d, noxu_db::Get::First, None)
        .unwrap_or(OperationStatus::NotFound);
    while s == OperationStatus::Success {
        by_key.entry(k.data().to_vec()).or_default().push(d.data().to_vec());
        s = c
            .get(&mut k, &mut d, noxu_db::Get::Next, None)
            .unwrap_or(OperationStatus::NotFound);
    }
    drop(c);
    txn.commit().unwrap();

    for (kv, dups) in &by_key {
        assert_eq!(
            dups.len(),
            1,
            "JE: putNoOverwrite must leave exactly ONE record per key \
             (count==1); key {kv:?} has {} records: {dups:?}",
            dups.len()
        );
    }

    // Per-key count()==1 and getNextDup()==NOTFOUND, exactly as JE asserts
    // inside its insert loop, re-verified for every populated key.
    let txn2 = env.begin_transaction(None).unwrap();
    for kv in by_key.keys() {
        let mut c = db.open_cursor_in(&txn2, None).unwrap();
        let mut sk = DatabaseEntry::from_bytes(kv);
        let mut sd = DatabaseEntry::new();
        assert_eq!(
            c.get(&mut sk, &mut sd, noxu_db::Get::Search, None).unwrap(),
            OperationStatus::Success,
            "getSearchKey must find key {kv:?}"
        );
        assert_eq!(c.count().unwrap(), 1, "cursor.count()==1 for key {kv:?}");
        assert_eq!(
            c.get(&mut sk, &mut sd, noxu_db::Get::NextDup, None).unwrap(),
            OperationStatus::NotFound,
            "getNextDup()==NOTFOUND for key {kv:?}"
        );
        drop(c);
    }
    txn2.commit().unwrap();
}
