//! Collections txn-scope witnesses (FID-DPL-28 / 15 / 18 / 17).
//!
//! These pin the "ONE RMW cursor in ONE auto-commit transaction" fidelity of
//! the `StoredMap` / `StoredList` auto-commit paths against JE
//! `StoredContainer` / RECNO `StoredList` semantics.

use noxu_bind::IntBinding;
use noxu_collections::StoredMap;
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::sync::Arc;
use tempfile::TempDir;

fn env_db(name: &str) -> (TempDir, Arc<Environment>, Arc<noxu_db::Database>) {
    let td = TempDir::new().unwrap();
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(td.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    let db = Arc::new(
        env.open_database(
            None,
            name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    (td, env, db)
}

/// FID-DPL-28 (S2): `StoredMap::put(None, ..)` must be ONE read-modify-write
/// cursor under ONE auto-commit transaction (JE
/// `StoredContainer.putKeyValue`, StoredContainer.java:322), so the returned
/// old value is exactly the value overwritten and no concurrent writer can
/// interleave between the read and the write.
///
/// Two threads each overwrite the same key many times with disjoint value
/// ranges, recording the "old" value each `put` returns.  Under an atomic RMW
/// the old-values form a single serial chain: every value is observed as an
/// "old" by at most one later writer.  If a value is returned as "old" by TWO
/// different puts, two writers read the same old value and one update was lost
/// -- a TOCTOU violation.
///
/// RED on 81b2f048 (get-old in one auto-txn, then a SEPARATE write auto-txn):
/// reliably >1000 duplicate old-values over 2x2000 overwrites.
/// GREEN after FID-DPL-28: exactly 0.
#[test]
fn fid_dpl_28_put_rmw_is_atomic_no_lost_update() {
    let (_td, _env, db) = env_db("dpl28_put");
    {
        let map: StoredMap<'_, i32, i32, _, _> =
            StoredMap::new(&db, IntBinding, IntBinding);
        map.put(None, &0, &-1).unwrap();
    }

    let n = 2000usize;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut handles = vec![];
    for t in 0i32..2 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let base = t * 1_000_000;
        handles.push(std::thread::spawn(move || {
            let map: StoredMap<'_, i32, i32, _, _> =
                StoredMap::new(&db, IntBinding, IntBinding);
            barrier.wait();
            let mut olds = vec![];
            for i in 0..n {
                let newv = base + i as i32;
                loop {
                    match map.put(None, &0, &newv) {
                        Ok(o) => {
                            olds.push(o.unwrap_or(-999));
                            break;
                        }
                        // A lock conflict / deadlock-victim abort is retried;
                        // it never produces a wrong old value.
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }
            olds
        }));
    }
    let mut all_olds = vec![];
    for h in handles {
        all_olds.extend(h.join().unwrap());
    }
    all_olds.sort_unstable();
    let mut dups = 0usize;
    for w in all_olds.windows(2) {
        if w[0] == w[1] && w[0] != -999 {
            dups += 1;
        }
    }
    assert_eq!(
        dups, 0,
        "FID-DPL-28: {dups} old-values were observed by two concurrent \
         put(None) calls -- the get-old and the write were NOT one atomic \
         RMW (TOCTOU / lost update). JE StoredContainer.putKeyValue is a \
         single RMW cursor in one auto-commit txn."
    );
}

/// FID-DPL-28 (S2) for `remove`: `StoredMap::remove(None, ..)` must be ONE
/// RMW cursor under ONE auto-commit transaction (JE
/// `StoredContainer.removeKey`, StoredContainer.java:339), so the value it
/// reports removed is exactly the value it deleted -- no concurrent writer
/// can interleave between the get-old and the delete.
///
/// Two threads concurrently drive put/remove on the same key.  Each
/// `remove` that returns `Some(old)` recorded the value present at the
/// atomic delete; under an atomic RMW no two removes can report the SAME
/// old value (each stored value is deleted at most once).  Duplicate
/// removed-values mean two removes observed the same value and one update
/// was lost between a get and a delete -- a TOCTOU violation.
///
/// RED on 81b2f048 (get-old then a SEPARATE delete): reliably produces
/// duplicate removed-values.  GREEN after FID-DPL-28: exactly 0.
#[test]
fn fid_dpl_28_remove_rmw_is_atomic_no_lost_update() {
    let (_td, _env, db) = env_db("dpl28_remove");
    let n = 2000usize;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut handles = vec![];
    for t in 0i32..2 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let base = t * 1_000_000 + 1;
        handles.push(std::thread::spawn(move || {
            let map: StoredMap<'_, i32, i32, _, _> =
                StoredMap::new(&db, IntBinding, IntBinding);
            barrier.wait();
            let mut removed = vec![];
            for i in 0..n {
                let marker = base + i as i32;
                // Insert our unique marker, then remove the key; record
                // whatever value the remove reports it deleted.
                loop {
                    match map.put(None, &0, &marker) {
                        Ok(_) => break,
                        Err(_) => std::thread::yield_now(),
                    }
                }
                loop {
                    match map.remove(None, &0) {
                        Ok(Some(old)) => {
                            removed.push(old);
                            break;
                        }
                        Ok(None) => break,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }
            removed
        }));
    }
    let mut all: Vec<i32> = vec![];
    for h in handles {
        all.extend(h.join().unwrap());
    }
    all.sort_unstable();
    let mut dups = 0usize;
    for w in all.windows(2) {
        if w[0] == w[1] {
            dups += 1;
        }
    }
    assert_eq!(
        dups, 0,
        "FID-DPL-28: {dups} values were reported removed by two concurrent \
         remove(None) calls -- the get-old and the delete were NOT one \
         atomic RMW (TOCTOU / lost update). JE StoredContainer.removeKey is \
         a single RMW cursor in one auto-commit txn."
    );
}
