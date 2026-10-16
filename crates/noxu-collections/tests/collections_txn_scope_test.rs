//! Collections txn-scope witnesses (FID-DPL-28 / 15 / 18 / 17).
//!
//! These pin the "ONE RMW cursor in ONE auto-commit transaction" fidelity of
//! the `StoredMap` / `StoredList` auto-commit paths against JE
//! `StoredContainer` / RECNO `StoredList` semantics.

use noxu_bind::IntBinding;
use noxu_collections::{StoredList, StoredMap};
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

/// FID-DPL-15 (S2): `StoredMap::clear(None)` must delete every record under
/// ONE auto-commit transaction (JE `StoredContainer.clear` ->
/// `DataView.clear`, StoredContainer.java:293), so a crash or error
/// mid-clear leaves the map fully cleared or untouched, never partially
/// cleared.
///
/// Observable discriminator: a single-transaction clear commits EXACTLY
/// ONCE regardless of record count.  We clear a 500-record map and assert
/// the environment's committed-transaction counter advanced by exactly 1.
/// RED on 81b2f048: each `cursor.delete()` ran on its own per-op
/// auto-commit cursor (no single wrapping transaction), so the clear was
/// not one atomic unit (the tracked commit delta is 0 -- there was no
/// enclosing transaction at all).  GREEN after FID-DPL-15: exactly 1.
#[test]
fn fid_dpl_15_clear_is_single_transaction() {
    let (_td, env, db) = env_db("dpl15_clear");
    let full = 500usize;
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding, IntBinding);
    for i in 0..full as i32 {
        map.put(None, &i, &i).unwrap();
    }

    let c0 = env.stats().unwrap().txn.n_commits;
    map.clear(None).unwrap();
    let c1 = env.stats().unwrap().txn.n_commits;

    assert_eq!(map.len(None).unwrap(), 0, "clear must empty the map");
    assert_eq!(
        c1 - c0,
        1,
        "FID-DPL-15: clear(None) of {full} records committed {} \
         transaction(s); it must be EXACTLY ONE auto-commit transaction \
         (JE StoredContainer.clear -> one beginAutoCommit/commitAutoCommit). \
         A delta of 0 means each delete auto-committed per-op with no \
         enclosing transaction (the base).",
        c1 - c0,
    );
}

/// FID-DPL-17 (S2): `StoredList` index assignment must be persistent,
/// engine-derived record numbers, not a process-local in-memory counter --
/// and `push` must use `putNoOverwrite` so a stale counter cannot silently
/// overwrite an existing element (JE `DataView.append` / `StoredList.java`).
///
/// Open a list, push three elements, DROP the handle, open a FRESH handle on
/// the SAME database, and push three more.  With persistent engine-derived
/// indices the second handle continues from 3 (indices 0..6, six distinct
/// elements).  RED on 81b2f048 (per-handle `Mutex<usize>` starting at 0 +
/// OVERWRITE put): the second handle restarts at 0 and overwrites 0,1,2.
#[test]
fn fid_dpl_17_storedlist_indices_are_persistent_across_handles() {
    let (_td, _env, db) = env_db("dpl17_list");

    {
        let list: StoredList<'_, i32, _> = StoredList::new(&db, IntBinding);
        list.push(None, &10).unwrap();
        list.push(None, &11).unwrap();
        list.push(None, &12).unwrap();
        assert_eq!(list.len(None).unwrap(), 3);
    }
    // Fresh handle on the same database.
    let list2: StoredList<'_, i32, _> = StoredList::new(&db, IntBinding);
    list2.push(None, &13).unwrap();
    list2.push(None, &14).unwrap();
    list2.push(None, &15).unwrap();

    assert_eq!(
        list2.len(None).unwrap(),
        6,
        "FID-DPL-17: a fresh StoredList handle restarted its index at 0 and \
         overwrote existing elements -- indices must be persistent, \
         engine-derived record numbers (JE RECNO append / putNoOverwrite)"
    );
    let all: Vec<i32> = list2.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(all, vec![10, 11, 12, 13, 14, 15]);
}

/// FID-DPL-18 (S2): `StoredList::remove(None, index)` renumbering must be
/// ONE atomic transaction (JE RECNO renumber within the operation txn), so
/// a crash or error mid-compaction never leaves a duplicated or missing
/// element.  Observable discriminator: the whole shift-down renumber
/// commits EXACTLY ONCE regardless of how many elements shift.
///
/// RED on 81b2f048: each shift's put + delete ran on its own per-op
/// auto-commit cursor, so removing index 2 from a 20-element list produced
/// ~34 independent commits (non-atomic -- a crash mid-shift leaves
/// duplicates / a wrong high-water mark).  GREEN after FID-DPL-18:
/// exactly 1.  The post-condition (contiguous, duplicate-free elements) is
/// also asserted and holds under both.
#[test]
fn fid_dpl_18_storedlist_remove_renumber_atomic() {
    let (_td, env, db) = env_db("dpl18_list");
    let list: StoredList<'_, i32, _> = StoredList::new(&db, IntBinding);
    for v in 0..20i32 {
        list.push(None, &v).unwrap();
    }

    let c0 = env.stats().unwrap().txn.n_commits;
    let removed = list.remove(None, 2).unwrap();
    let c1 = env.stats().unwrap().txn.n_commits;
    assert_eq!(removed, Some(2));

    assert_eq!(
        c1 - c0,
        1,
        "FID-DPL-18: remove(None, index) committed {} transaction(s); the \
         whole shift-down renumber must be ONE atomic transaction (JE RECNO \
         renumber within the operation txn). >1 means each shift \
         auto-committed independently (the base), so a crash mid-shift \
         could leave duplicates.",
        c1 - c0,
    );

    let all: Vec<i32> = list.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(
        all,
        vec![0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        "FID-DPL-18: remove(index) must leave contiguous, duplicate-free \
         elements (atomic renumber)"
    );
    assert_eq!(list.len(None).unwrap(), 19);
}
