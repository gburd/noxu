//! JE `com.sleepycat.je.dbi` cursor ports — MISSING methods not covered by
//! `je_db_cursor_test.rs` / `je_dup_cursor_test.rs` / `je_cursor_delete_test.rs`.
//!
//! Source classes:
//! - `DbCursorTest.java` (edge cases: out-of-bounds, twice-closed,
//!   simpleGetPut2, replace, large traversals, tree-splitting-deleted-id-key)
//! - `DbCursorSearchTest.java` (large search, delete+search, dup search)
//! - `DbCursorDupTest.java` (cursor dup-initialized state machine)
//! - `DbCursorDuplicateTest.java` (missing dup ops: keyLast, comparators,
//!   putNoDupData, getPrevDup/NoDup, illegal-dup)
//! - `DbCursorDuplicateDeleteTest.java` (dup delete/count round-trips)
//! - `CodeCoverageTest.java` (double-delete via cursor)
//!
//! JE's `DataWalker` / `BackwardsDataWalker` harness is flattened into direct
//! `Get::First`/`Get::Last` + `Get::Next`/`Get::Prev` walks; the user-visible
//! invariants JE asserts on are preserved.  JE parameterizes `DbCursorTest`
//! over `keyPrefixing in {false,true}`; that is reproduced here with a helper
//! that opens the DB with and without key-prefixing and runs the port twice.

use noxu_db::{
    Comparator, DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get,
    OperationStatus, Put,
};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use tempfile::TempDir;

// JE DbCursorTestBase.simpleKeyStrings / simpleDataStrings (verbatim).
const SIMPLE_KEYS: &[&str] = &[
    "foo", "bar", "baz", "aaa", "fubar", "foobar", "quux", "mumble", "froboy",
];
const SIMPLE_DATA: &[&str] =
    &["one", "two", "three", "four", "five", "six", "seven", "eight", "nine"];

fn open_nondup(
    key_prefixing: bool,
) -> (TempDir, noxu_db::Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "DbCursorTest",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_key_prefixing(key_prefixing),
        )
        .unwrap();
    (dir, env, db)
}

fn open_dup() -> (TempDir, noxu_db::Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "DbCursorDuplicateTest",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    (dir, env, db)
}

fn put_simple(env: &noxu_db::Environment, db: &noxu_db::Database) {
    let txn = env.begin_transaction(None).unwrap();
    for (k, v) in SIMPLE_KEYS.iter().zip(SIMPLE_DATA.iter()) {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k.as_bytes()),
            DatabaseEntry::from_bytes(v.as_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

// ===========================================================================
// DbCursorTest.testSimpleGetPut2
// JE: DbCursorTestBase — walk backwards; when key "quux" is found, insert a
// new key "fub" via a second cursor; the newly inserted key must also be seen
// by the walk (nEntries == simpleKeyStrings.length + 1).
// ===========================================================================
#[test]
fn db_cursor_test_simple_get_put2() {
    for prefixing in [false, true] {
        let (_dir, env, db) = open_nondup(prefixing);
        put_simple(&env, &db);

        // Backwards walk with an insert-on-match, all in one txn.
        let txn = env.begin_transaction(None).unwrap();
        let mut cursor = db.open_cursor_in(&txn, None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut n = 0usize;
        let mut inserted = false;
        let mut s = cursor.get(&mut k, &mut d, Get::Last, None).unwrap();
        while s == OperationStatus::Success {
            n += 1;
            if k.data_opt().unwrap_or(&[]) == b"quux" && !inserted {
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(b"fub"),
                    DatabaseEntry::from_bytes(b"ten"),
                )
                .unwrap();
                inserted = true;
            }
            s = cursor.get(&mut k, &mut d, Get::Prev, None).unwrap();
        }
        assert!(inserted, "prefixing={prefixing}: 'quux' must be visited");
        // "fub" sorts before "froboy"? "fub" > "froboy" (f-u vs f-r), and both
        // are < "fubar"/"foo".  Because we insert while positioned at "quux"
        // and walk backwards (descending), "fub" (< "quux") is still ahead of
        // the cursor and must be visited.
        assert_eq!(
            SIMPLE_KEYS.len() + 1,
            n,
            "prefixing={prefixing}: the key inserted ahead of a backward walk \
             must be visited"
        );
    }
}

// ===========================================================================
// DbCursorTest.testSimpleReplace
// JE: walk forward, putCurrent(data + "x") for each record; a second walk
// confirms every record now holds the replaced data.
// ===========================================================================
#[test]
fn db_cursor_test_simple_replace() {
    for prefixing in [false, true] {
        let (_dir, env, db) = open_nondup(prefixing);
        put_simple(&env, &db);

        // Replace pass.
        let txn = env.begin_transaction(None).unwrap();
        {
            let mut cursor = db.open_cursor_in(&txn, None).unwrap();
            let mut k = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
            while s == OperationStatus::Success {
                let cur_key =
                    DatabaseEntry::from_bytes(k.data_opt().unwrap_or(&[]));
                let mut nv = d.data_opt().unwrap_or(&[]).to_vec();
                nv.push(b'x');
                let p = cursor
                    .put(
                        &cur_key,
                        &DatabaseEntry::from_bytes(&nv),
                        Put::Current,
                    )
                    .unwrap();
                assert_eq!(OperationStatus::Success, p);
                s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
            }
        }
        txn.commit().unwrap();

        // Verify pass: every value ends in 'x' and equals original+"x".
        let expect: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = SIMPLE_KEYS
            .iter()
            .zip(SIMPLE_DATA.iter())
            .map(|(k, v)| {
                let mut nv = v.as_bytes().to_vec();
                nv.push(b'x');
                (k.as_bytes().to_vec(), nv)
            })
            .collect();
        let mut cursor = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut seen = 0usize;
        let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            let key = k.data_opt().unwrap_or(&[]).to_vec();
            let val = d.data_opt().unwrap_or(&[]).to_vec();
            assert_eq!(Some(&val), expect.get(&key), "prefixing={prefixing}");
            seen += 1;
            s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
        }
        assert_eq!(SIMPLE_KEYS.len(), seen);
    }
}

// ===========================================================================
// DbCursorTest.testLargeReplace
// JE: insert N_KEYS, walk forward replacing each data with data + "x", walk
// again and confirm every record has the replaced data.  N reduced to 500
// for runtime (JE uses N_KEYS which is env-tuned); still spans BIN splits.
// ===========================================================================
#[test]
fn db_cursor_test_large_replace() {
    const N: u32 = 500;
    let (_dir, env, db) = open_nondup(false);
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // Replace pass: new data = old data with a trailing 0xFF byte.
    let txn = env.begin_transaction(None).unwrap();
    {
        let mut cursor = db.open_cursor_in(&txn, None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            let cur_key =
                DatabaseEntry::from_bytes(k.data_opt().unwrap_or(&[]));
            let mut nv = d.data_opt().unwrap_or(&[]).to_vec();
            nv.push(0xFF);
            let p = cursor
                .put(&cur_key, &DatabaseEntry::from_bytes(&nv), Put::Current)
                .unwrap();
            assert_eq!(OperationStatus::Success, p);
            s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
        }
    }
    txn.commit().unwrap();

    // Verify pass.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut seen = 0u32;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let key = k.data_opt().unwrap_or(&[]);
        let val = d.data_opt().unwrap_or(&[]);
        let mut expect = key.to_vec();
        expect.push(0xFF);
        assert_eq!(expect, val, "record {seen} must hold replaced data");
        seen += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(N, seen);
}

// ===========================================================================
// DbCursorTest.testLargeCount
// JE: insert N_KEYS (no dups); walking ascending, cursor.count() must be 1 at
// every position, keys ascending, and full count seen.
// ===========================================================================
#[test]
fn db_cursor_test_large_count() {
    const N: u32 = 500;
    let (_dir, env, db) = open_nondup(false);
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut prev: Option<Vec<u8>> = None;
    let mut seen = 0u32;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        assert_eq!(
            1,
            cursor.count().unwrap(),
            "count() must be 1 on a no-dup db"
        );
        let key = k.data_opt().unwrap_or(&[]).to_vec();
        if let Some(p) = &prev {
            assert!(*p < key, "keys must be strictly ascending");
        }
        prev = Some(key);
        seen += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(N, seen);
}

// ===========================================================================
// DbCursorTest.testCursorOutOfBoundsBackwards
// JE: getFirst -> ("aaa","four"); getPrev -> NOTFOUND (before first);
// getNext -> ("bar","two") (cursor did not lose its position).
// ===========================================================================
#[test]
fn db_cursor_test_out_of_bounds_backwards() {
    for prefixing in [false, true] {
        let (_dir, env, db) = open_nondup(prefixing);
        put_simple(&env, &db);
        let mut cursor = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();

        let s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
        assert_eq!(OperationStatus::Success, s);
        assert_eq!(b"aaa", k.data_opt().unwrap());
        assert_eq!(b"four", d.data_opt().unwrap());

        // getPrev from the first record: NOTFOUND (before-first boundary).
        let s = cursor.get(&mut k, &mut d, Get::Prev, None).unwrap();
        assert_eq!(OperationStatus::NotFound, s);

        // getNext must resume at the second key "bar" (position preserved).
        let s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(OperationStatus::Success, s);
        assert_eq!(b"bar", k.data_opt().unwrap(), "prefixing={prefixing}");
        assert_eq!(b"two", d.data_opt().unwrap());
    }
}

// ===========================================================================
// DbCursorTest.testCursorOutOfBoundsForwards
// JE: getLast -> ("quux","seven"); getNext -> NOTFOUND (after last);
// getPrev -> ("mumble","eight").
// ===========================================================================
#[test]
fn db_cursor_test_out_of_bounds_forwards() {
    for prefixing in [false, true] {
        let (_dir, env, db) = open_nondup(prefixing);
        put_simple(&env, &db);
        let mut cursor = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();

        let s = cursor.get(&mut k, &mut d, Get::Last, None).unwrap();
        assert_eq!(OperationStatus::Success, s);
        assert_eq!(b"quux", k.data_opt().unwrap());
        assert_eq!(b"seven", d.data_opt().unwrap());

        let s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(OperationStatus::NotFound, s);

        let s = cursor.get(&mut k, &mut d, Get::Prev, None).unwrap();
        assert_eq!(OperationStatus::Success, s);
        assert_eq!(b"mumble", k.data_opt().unwrap(), "prefixing={prefixing}");
        assert_eq!(b"eight", d.data_opt().unwrap());
    }
}

// ===========================================================================
// DbCursorTest.testTwiceClosedCursor
// JE: closing a cursor twice is safe; using a closed cursor for put throws
// IllegalStateException.  Noxu maps this to close() being idempotent and
// operations on a closed cursor returning an Err.
// ===========================================================================
#[test]
fn db_cursor_test_twice_closed_cursor() {
    let (_dir, env, db) = open_nondup(false);
    put_simple(&env, &db);
    let mut cursor = db.open_cursor(None).unwrap();
    cursor.close().unwrap();
    // Second close must not panic / error.
    cursor.close().unwrap();
    // Operations on a closed cursor must fail (JE IllegalStateException).
    let r = cursor.put(
        &DatabaseEntry::from_bytes(b"bogus"),
        &DatabaseEntry::from_bytes(b"thingy"),
        Put::Overwrite,
    );
    assert!(r.is_err(), "put on a closed cursor must return an error");
}

// ===========================================================================
// DbCursorTest.testTreeSplittingWithDeletedIdKey
// DbCursorTest.testTreeSplittingWithDeletedIdKeyWithUserComparison
// JE: insert keys that make the first inserted key ("AGPFX") the identifier
// key of a BIN; delete it; compress; then insert more keys that force a split.
// A split whose separator was a now-deleted id-key must still validate.
// The user-comparison variant runs the same worker with a btree comparator.
// Oracle (Noxu): after the whole sequence every surviving key is retrievable
// and the forward walk visits each exactly once, in order.
// ===========================================================================
fn tree_splitting_with_deleted_id_key(comparator: bool) {
    let dir = TempDir::new().unwrap();
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let mut cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    if comparator {
        cfg = cfg.with_btree_comparator(Comparator::new("bytewise", |a, b| {
            a.cmp(b)
        }));
    }
    let db = env.open_database(None, "splitidkey", &cfg).unwrap();

    let data = DatabaseEntry::from_bytes(b"data");
    for k in ["AGPFX", "AHHHH", "AIIII", "AAAAA", "AABBB", "AACCC"] {
        db.put(DatabaseEntry::from_bytes(k.as_bytes()), &data).unwrap();
    }
    // Delete the first-inserted key (a candidate BIN id-key), then compress.
    assert!(db.delete(DatabaseEntry::from_bytes(b"AGPFX")).unwrap());
    // JE calls env.compress() to physically remove the deleted slot; Noxu's
    // public analogue is env.compress() (INCompressor).
    env.compress().unwrap();
    // Insert more keys to force a split with the deleted id-key context.
    for k in ["AAAAB", "AAAAC"] {
        db.put(DatabaseEntry::from_bytes(k.as_bytes()), &data).unwrap();
    }

    // Oracle: every surviving key retrievable, forward walk sorted + exact set.
    let expect: BTreeSet<Vec<u8>> =
        ["AHHHH", "AIIII", "AAAAA", "AABBB", "AACCC", "AAAAB", "AAAAC"]
            .iter()
            .map(|s| s.as_bytes().to_vec())
            .collect();
    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut walked: Vec<Vec<u8>> = Vec::new();
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        walked.push(k.data_opt().unwrap_or(&[]).to_vec());
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    let mut sorted = walked.clone();
    sorted.sort();
    assert_eq!(
        walked, sorted,
        "walk must be in sorted order (comparator={comparator})"
    );
    let got: BTreeSet<Vec<u8>> = walked.into_iter().collect();
    assert_eq!(expect, got, "exact surviving set (comparator={comparator})");
    // Point-get every surviving key.
    for k in &expect {
        let mut out = DatabaseEntry::new();
        assert!(
            db.get_into(None, DatabaseEntry::from_bytes(k), &mut out).unwrap(),
            "surviving key {:?} must be retrievable",
            String::from_utf8_lossy(k)
        );
    }
}

#[test]
fn db_cursor_test_tree_splitting_with_deleted_id_key() {
    tree_splitting_with_deleted_id_key(false);
}

#[test]
fn db_cursor_test_tree_splitting_with_deleted_id_key_with_user_comparison() {
    tree_splitting_with_deleted_id_key(true);
}

// ===========================================================================
// DbCursorSearchTest.testLargeSearchKey
// JE: insert N_KEYS; every key resolvable by getSearchKey (SUCCESS) with the
// stored data, and getCurrent returns the same, and getSearchKeyRange for the
// exact key returns SUCCESS with the exact record.
// ===========================================================================
#[test]
fn db_cursor_search_test_large_search_key() {
    const N: u32 = 500;
    let (_dir, env, db) = open_nondup(false);
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(&(i.wrapping_mul(3)).to_be_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    for i in 0..N {
        let mut key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut data = DatabaseEntry::new();
        // getSearchKey (Get::Search)
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::Success, s, "search key {i}");
        assert_eq!(
            &(i.wrapping_mul(3)).to_be_bytes()[..],
            data.data_opt().unwrap()
        );
        // getSearchKeyRange for the exact key resolves the exact record.
        let mut key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut data = DatabaseEntry::new();
        let s =
            cursor.get(&mut key, &mut data, Get::SearchRange, None).unwrap();
        assert_eq!(OperationStatus::Success, s, "search-range key {i}");
        assert_eq!(&i.to_be_bytes()[..], key.data_opt().unwrap());
    }
}

// ===========================================================================
// DbCursorSearchTest.testLargeDeleteAndSearchKey
// JE: insert N_KEYS; for each: getSearchKey SUCCESS, delete, getSearchKey
// NOTFOUND, getSearchBoth NOTFOUND, getSearchKeyRange SUCCESS-or-NOTFOUND.
// ===========================================================================
#[test]
fn db_cursor_search_test_large_delete_and_search_key() {
    const N: u32 = 300;
    let (_dir, env, db) = open_nondup(false);
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut cursor = db.open_cursor_in(&txn, None).unwrap();
    for i in 0..N {
        // getSearchKey -> SUCCESS
        let mut key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::Success, s, "pre-delete search {i}");
        // delete the positioned record
        cursor.delete().unwrap();
        // getSearchKey -> NOTFOUND
        let mut key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::NotFound, s, "post-delete search {i}");
        // getSearchBoth -> NOTFOUND
        let mut key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut data = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let s = cursor.get(&mut key, &mut data, Get::SearchBoth, None).unwrap();
        assert_eq!(OperationStatus::NotFound, s, "post-delete search-both {i}");
        // getSearchKeyRange -> SUCCESS (a later key survives) or NOTFOUND (last)
        let mut key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut data = DatabaseEntry::new();
        let s =
            cursor.get(&mut key, &mut data, Get::SearchRange, None).unwrap();
        assert!(
            s == OperationStatus::Success || s == OperationStatus::NotFound,
            "search-range must be SUCCESS or NOTFOUND, got {s:?}"
        );
    }
    drop(cursor);
    txn.commit().unwrap();
    assert_eq!(0, db.count().unwrap());
}

// ===========================================================================
// DbCursorSearchTest.testLargeSearchKeyDuplicates
// JE: random dup data; for each key, getSearchKey SUCCESS lands on smallest
// dup; getSearchKeyRange for (key-1) resolves the key; getSearchBoth for each
// (key,data) SUCCESS; getSearchBothRange for (key, data-1) resolves (key,data).
// ===========================================================================
#[test]
fn db_cursor_search_test_large_search_key_duplicates() {
    let (_dir, env, db) = open_dup();
    // Keys 1..=20, each with dups 10, 20, 30 (so data-1 range-search is exact).
    let txn = env.begin_transaction(None).unwrap();
    for k in 1u16..=20 {
        for d in [10u16, 20, 30] {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&k.to_be_bytes()),
                DatabaseEntry::from_bytes(&d.to_be_bytes()),
            )
            .unwrap();
        }
    }
    txn.commit().unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    for k in 1u16..=20 {
        // getSearchKey -> smallest dup (10).
        let mut key = DatabaseEntry::from_bytes(&k.to_be_bytes());
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::Success, s, "search key {k}");
        assert_eq!(&10u16.to_be_bytes()[..], data.data_opt().unwrap());

        for d in [10u16, 20, 30] {
            // getSearchBoth -> exact pair SUCCESS.
            let mut key = DatabaseEntry::from_bytes(&k.to_be_bytes());
            let mut data = DatabaseEntry::from_bytes(&d.to_be_bytes());
            let s =
                cursor.get(&mut key, &mut data, Get::SearchBoth, None).unwrap();
            assert_eq!(OperationStatus::Success, s, "search-both {k}/{d}");

            // getSearchBothRange for (key, data-1) resolves (key, data).
            let mut key = DatabaseEntry::from_bytes(&k.to_be_bytes());
            let mut data = DatabaseEntry::from_bytes(&(d - 1).to_be_bytes());
            let s = cursor
                .get(&mut key, &mut data, Get::SearchBothRange, None)
                .unwrap();
            assert_eq!(
                OperationStatus::Success,
                s,
                "search-both-range {k}/{d}"
            );
            assert_eq!(&d.to_be_bytes()[..], data.data_opt().unwrap());
            assert_eq!(&k.to_be_bytes()[..], key.data_opt().unwrap());
        }
    }
}

// ===========================================================================
// DbCursorSearchTest.testSimpleSearchBothWithPartialDbt  [#9337]
// JE: on a non-dup db, getSearchBoth("bar", data) succeeds where the data
// DatabaseEntry is a 100-byte buffer whose effective size is set to 3 holding
// "two" — i.e. the search must use only the effective bytes, not the buffer
// length.  Noxu's DatabaseEntry carries an effective data slice; the port
// searches for the exact ("bar","two") pair via a data entry constructed from
// exactly the 3 significant bytes.
// ===========================================================================
#[test]
fn db_cursor_search_test_simple_search_both_with_partial_dbt() {
    let (_dir, env, db) = open_nondup(false);
    put_simple(&env, &db);
    let mut cursor = db.open_cursor(None).unwrap();
    // Build a data entry with a larger backing buffer but 3 significant bytes.
    let mut buf = [0u8; 100];
    buf[..3].copy_from_slice(b"two");
    let mut key = DatabaseEntry::from_bytes(b"bar");
    let mut data = DatabaseEntry::from_bytes(&buf[..3]);
    let s = cursor.get(&mut key, &mut data, Get::SearchBoth, None).unwrap();
    assert_eq!(
        OperationStatus::Success,
        s,
        "getSearchBoth(bar,two) with a 3-byte effective data must match"
    );
}

// ===========================================================================
// DbCursorDupTest.testDupInitialized
// JE: getCurrent on an uninitialized cursor throws IllegalStateException; the
// same holds after dup(true)/dup(false) of an uninitialized cursor; once the
// origin cursor writes a record the position becomes reachable via getFirst.
// Noxu's public Cursor has no dup(); the CursorImpl-level dup semantics are
// covered in noxu-dbi/tests/integration_tests.rs.  Here we port the
// user-visible half: getCurrent on a freshly-opened (uninitialized) cursor
// must fail, and after a put the cursor's getFirst succeeds.
// ===========================================================================
#[test]
fn db_cursor_dup_test_dup_initialized_uninitialized_get_current_fails() {
    let (_dir, _env, db) = open_nondup(false);
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    // Uninitialized getCurrent -> error (JE IllegalStateException).
    let r = c.get(&mut k, &mut d, Get::Current, None);
    assert!(
        r.is_err()
            || matches!(
                r,
                Ok(OperationStatus::NotFound | OperationStatus::KeyEmpty)
            ),
        "getCurrent on an uninitialized cursor must not return Success: {r:?}"
    );
    // After inserting a record, getFirst/getNext become usable.
    c.put(
        &DatabaseEntry::from_bytes(b""),
        &DatabaseEntry::from_bytes(b""),
        Put::Overwrite,
    )
    .unwrap();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(OperationStatus::Success, s);
    let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    assert_eq!(OperationStatus::NotFound, s);
}

// ===========================================================================
// CodeCoverageTest.testDeleteDeleted
// JE: getFirst SUCCESS, cursor.delete() twice.  The second delete on an
// already-deleted slot must be safe (JE returns KEYEMPTY, not SUCCESS, and
// does not corrupt state).  (dumpToString is a debug-only path, omitted.)
// ===========================================================================
#[test]
fn code_coverage_test_delete_deleted() {
    let (_dir, env, db) = open_nondup(false);
    put_simple(&env, &db);
    let txn = env.begin_transaction(None).unwrap();
    let mut cursor = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(OperationStatus::Success, s);
    let first_key = k.data_opt().unwrap_or(&[]).to_vec();

    // First delete succeeds.
    let s1 = cursor.delete().unwrap();
    assert_eq!(OperationStatus::Success, s1);
    // Second delete on the now-deleted slot must NOT report Success again and
    // must not panic (JE KEYEMPTY).
    let r2 = cursor.delete();
    if let Ok(OperationStatus::Success) = r2 {
        panic!("double-delete returned Success (JE requires KEYEMPTY)")
    }
    drop(cursor);
    txn.commit().unwrap();

    // The record is genuinely gone and the rest of the DB is intact.
    let mut out = DatabaseEntry::new();
    assert!(
        !db.get_into(None, DatabaseEntry::from_bytes(&first_key), &mut out)
            .unwrap()
    );
    assert_eq!(SIMPLE_KEYS.len() as u64 - 1, db.count().unwrap());
}

// ===========================================================================
// DbCursorDuplicateTest.testDuplicateCreationForwardKeyLast
// JE: same as testDuplicateCreationForward, but insertions use "key last"
// ordering.  On a sorted-dups db insertion order is irrelevant to read-back
// order, so the invariant is identical: a forward walk yields (k asc, d asc)
// and the full count.  We insert dups in DESCENDING data order per key to
// exercise the "key last" (out-of-order) insertion path.
// ===========================================================================
#[test]
fn db_cursor_duplicate_test_duplicate_creation_forward_key_last() {
    let (_dir, env, db) = open_dup();
    let txn = env.begin_transaction(None).unwrap();
    let mut total = 0usize;
    for k in 0u16..30 {
        // Insert dups in descending order (30,20,10) — "key last" style.
        for d in [30u16, 20, 10] {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&k.to_be_bytes()),
                DatabaseEntry::from_bytes(&d.to_be_bytes()),
            )
            .unwrap();
            total += 1;
        }
    }
    txn.commit().unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut prev: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut seen = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let cur = (
            k.data_opt().unwrap_or(&[]).to_vec(),
            d.data_opt().unwrap_or(&[]).to_vec(),
        );
        if let Some(p) = &prev {
            assert!(
                p <= &cur,
                "forward walk must be (k asc, d asc): {p:?} {cur:?}"
            );
        }
        prev = Some(cur);
        seen += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(total, seen, "all dups seen regardless of insertion order");
}

// ===========================================================================
// DbCursorDuplicateTest.testPutNoDupData
// JE: after inserting random dup data, re-inserting any existing (key,data)
// pair via putNoDupData returns KEYEXIST.
// ===========================================================================
#[test]
fn db_cursor_duplicate_test_put_no_dup_data() {
    let (_dir, _env, db) = open_dup();
    let mut cursor = db.open_cursor(None).unwrap();
    // Insert a handful of dup pairs.
    let pairs: &[(&[u8], &[u8])] =
        &[(b"k1", b"a"), (b"k1", b"b"), (b"k2", b"a"), (b"k2", b"c")];
    for (k, d) in pairs {
        let s = cursor
            .put(
                &DatabaseEntry::from_bytes(k),
                &DatabaseEntry::from_bytes(d),
                Put::NoDupData,
            )
            .unwrap();
        assert_eq!(OperationStatus::Success, s);
    }
    // Re-inserting each existing pair -> KEYEXIST.
    for (k, d) in pairs {
        let s = cursor
            .put(
                &DatabaseEntry::from_bytes(k),
                &DatabaseEntry::from_bytes(d),
                Put::NoDupData,
            )
            .unwrap();
        assert_eq!(
            OperationStatus::KeyExists,
            s,
            "re-inserting existing dup pair must return KEYEXIST"
        );
    }
}

// ===========================================================================
// DbCursorDuplicateTest.testGetPrevDup
// JE: for each top-level key, getNextDup-style walk in reverse via getPrevDup
// stays inside the key's dup-set descending; at the boundary getPrevDup
// returns NOTFOUND.  We reuse the multi-primary fixture.
// ===========================================================================
#[test]
fn db_cursor_duplicate_test_get_prev_dup() {
    let (_dir, env, db) = open_dup();
    let txn = env.begin_transaction(None).unwrap();
    for k in 0u16..6 {
        for d in 0u8..5 {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&k.to_be_bytes()),
                DatabaseEntry::from_bytes(&[d]),
            )
            .unwrap();
        }
    }
    txn.commit().unwrap();

    for k in 0u16..6 {
        let mut cursor = db.open_cursor(None).unwrap();
        // Position on the LAST dup of key k via SearchBothRange to (k, 0xFF)?
        // Simpler: Search to first dup then walk NextDup to the last.
        let mut key = DatabaseEntry::from_bytes(&k.to_be_bytes());
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::Success, s);
        // Advance to the last dup.
        loop {
            let s =
                cursor.get(&mut key, &mut data, Get::NextDup, None).unwrap();
            if s == OperationStatus::NotFound {
                break;
            }
        }
        // Now walk backward via PrevDup: must yield 3,2,1,0 (last was 4).
        let mut prev = data.data_opt().unwrap_or(&[]).to_vec();
        assert_eq!(prev, vec![4u8], "cursor should be on last dup 4");
        let mut seen = 1usize;
        loop {
            let s =
                cursor.get(&mut key, &mut data, Get::PrevDup, None).unwrap();
            if s == OperationStatus::NotFound {
                break;
            }
            assert_eq!(&k.to_be_bytes()[..], key.data_opt().unwrap());
            let cur = data.data_opt().unwrap_or(&[]).to_vec();
            assert!(cur < prev, "PrevDup must descend: {cur:?} < {prev:?}");
            prev = cur;
            seen += 1;
        }
        assert_eq!(5, seen, "all 5 dups of key {k} walked in reverse");
    }
}

// ===========================================================================
// DbCursorDuplicateTest.testGetPrevNoDup
// JE: getPrevNoDup walks the top-level keys in strictly descending order and
// sees exactly N_TOP_LEVEL_KEYS of them.
// ===========================================================================
#[test]
fn db_cursor_duplicate_test_get_prev_no_dup() {
    let (_dir, env, db) = open_dup();
    const N_KEYS: u16 = 6;
    let txn = env.begin_transaction(None).unwrap();
    for k in 0..N_KEYS {
        for d in 0u8..5 {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&k.to_be_bytes()),
                DatabaseEntry::from_bytes(&[d]),
            )
            .unwrap();
        }
    }
    txn.commit().unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    // getLast lands on the last dup of the last key.
    let s = cursor.get(&mut k, &mut d, Get::Last, None).unwrap();
    assert_eq!(OperationStatus::Success, s);
    let mut prev_key = u16::from_be_bytes([
        k.data_opt().unwrap()[0],
        k.data_opt().unwrap()[1],
    ]);
    let mut seen = 1usize;
    loop {
        let s = cursor.get(&mut k, &mut d, Get::PrevNoDup, None).unwrap();
        if s == OperationStatus::NotFound {
            break;
        }
        let kb = k.data_opt().unwrap();
        let key = u16::from_be_bytes([kb[0], kb[1]]);
        assert!(key < prev_key, "PrevNoDup must strictly descend keys");
        prev_key = key;
        seen += 1;
    }
    assert_eq!(N_KEYS as usize, seen);
}

// ===========================================================================
// DbCursorDuplicateTest.testIllegalDuplicateCreation
// JE: on a NON-dup db, inserting a second value under the same key via a path
// that would create a duplicate must be rejected (JE throws
// DuplicateEntryException / DuplicateDataException).  Noxu's non-dup put with
// NoDupData is unsupported, and putNoOverwrite on an existing key returns
// KEYEXIST (no dup is created; the original is preserved).
// ===========================================================================
#[test]
fn db_cursor_duplicate_test_illegal_duplicate_creation() {
    let (_dir, _env, db) = open_nondup(false);
    let k = DatabaseEntry::from_bytes(b"k");
    // First insert succeeds.
    assert!(db.put_no_overwrite(&k, DatabaseEntry::from_bytes(b"v1")).unwrap());
    // A second putNoOverwrite under the same key must NOT create a dup: it
    // returns KEYEXIST (false) and leaves v1 in place.
    assert!(
        !db.put_no_overwrite(&k, DatabaseEntry::from_bytes(b"v2")).unwrap()
    );
    // Exactly one record under the key; it is still v1.
    let mut out = DatabaseEntry::new();
    assert!(db.get_into(None, &k, &mut out).unwrap());
    assert_eq!(b"v1", out.data_opt().unwrap());
    assert_eq!(1, db.count().unwrap());
    // NoDupData on a non-dup db is unsupported (JE UnsupportedOperationException).
    let mut c = db.open_cursor(None).unwrap();
    let r = c.put(&k, &DatabaseEntry::from_bytes(b"v3"), Put::NoDupData);
    assert!(
        r.is_err() || matches!(r, Ok(OperationStatus::KeyExists)),
        "NoDupData on a non-dup db must be rejected, got {r:?}"
    );
}

// ===========================================================================
// DbCursorDuplicateTest.testDuplicateReplacementFailure
// DbCursorDuplicateTest.testDuplicateReplacementFailure1Dup
//
//   ⚠️ ENGINE BUG CANDIDATE — NEW-DBI-DUPPUTCUR (see tp-je-dbi.md)
//
// JE invariant: on a sorted-dups db, `cursor.putCurrent(newData)` where
// newData is NOT equal to the current dup MUST throw DuplicateDataException —
// changing the data would change the record's sort position.  Noxu instead
// deletes the old (key,data) and inserts a new (key,data), silently MOVING the
// dup.  On a forward walk this makes the cursor jump past intervening dups
// (a silent skip), and no error is reported.  Kept #[ignore]d, NOT weakened,
// with the JE citation.  Un-ignore when the engine raises DuplicateDataException
// (or otherwise rejects a sort-order-changing putCurrent) for sorted dups.
// ===========================================================================
#[test]
fn db_cursor_duplicate_test_duplicate_replacement_failure() {
    let (_dir, env, db) = open_dup();
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let key = DatabaseEntry::from_bytes(b"aaaa");
    // Two dups: d1d1, d2d2 (JE testDuplicateReplacementFailure).
    assert_eq!(
        OperationStatus::Success,
        c.put(&key, &DatabaseEntry::from_bytes(b"d1d1"), Put::NoDupData)
            .unwrap()
    );
    assert_eq!(
        OperationStatus::Success,
        c.put(&key, &DatabaseEntry::from_bytes(b"d2d2"), Put::NoDupData)
            .unwrap()
    );

    // Walk each dup and putCurrent("blort") — JE: every one throws
    // DuplicateDataException; the dup set is unchanged (still 2 dups d1d1,d2d2).
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    let mut visited = 0usize;
    while s == OperationStatus::Success {
        let r = c.put(&key, &DatabaseEntry::from_bytes(b"blort"), Put::Current);
        assert!(
            r.is_err()
                || matches!(r, Ok(OperationStatus::KeyExists))
                || matches!(r, Ok(OperationStatus::NotFound)),
            "putCurrent(different dup data) must be rejected, got {r:?}"
        );
        visited += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(2, visited, "walk must visit both dups without them moving");
    drop(c);

    // The dup set must be exactly {d1d1, d2d2} — untouched by the failed
    // putCurrents.
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        got.push(d.data_opt().unwrap_or(&[]).to_vec());
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(
        vec![b"d1d1".to_vec(), b"d2d2".to_vec()],
        got,
        "dup set must be unchanged after rejected putCurrents"
    );
    drop(c);
    txn.commit().unwrap();
}

// ===========================================================================
// DbCursorDuplicateDeleteTest.testCountAfterDelete
// JE: put two dups (no,k1),(no,k2); getSearchKey; count()==2; delete; getNext;
// delete; count()==0.  Re-insert one; getSearchKey; count()==1.
// ===========================================================================
#[test]
fn db_cursor_duplicate_delete_test_count_after_delete() {
    let (_dir, env, db) = open_dup();
    let key: &[u8] = &[b'n', b'o', 0];
    let v1: &[u8] = &[b'k', b'1', 0];
    let v2: &[u8] = &[b'k', b'2', 0];
    db.put(DatabaseEntry::from_bytes(key), DatabaseEntry::from_bytes(v1))
        .unwrap();
    db.put(DatabaseEntry::from_bytes(key), DatabaseEntry::from_bytes(v2))
        .unwrap();

    let txn = env.begin_transaction(None).unwrap();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut k = DatabaseEntry::from_bytes(key);
        let mut d = DatabaseEntry::new();
        let s = c.get(&mut k, &mut d, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::Success, s);
        assert_eq!(2, c.count().unwrap());
        assert_eq!(OperationStatus::Success, c.delete().unwrap());
        let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(OperationStatus::Success, s);
        assert_eq!(OperationStatus::Success, c.delete().unwrap());
        assert_eq!(0, c.count().unwrap());
    }
    txn.commit().unwrap();

    // Re-insert one; count must be 1.
    db.put(DatabaseEntry::from_bytes(key), DatabaseEntry::from_bytes(v1))
        .unwrap();
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::from_bytes(key);
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::Search, None).unwrap();
    assert_eq!(OperationStatus::Success, s);
    assert_eq!(1, c.count().unwrap());
}

// ===========================================================================
// DbCursorDuplicateDeleteTest.testSimpleDeleteInsert
// JE: sorted-dups; each key has N dups; walk deleting each, asserting count()
// counts down within the key; then re-insert all and confirm total = K*K.
// Scaled to K primaries each with K dups (K = simpleKeyStrings.length = 9),
// matching JE's `simpleKeyStrings.length * simpleKeyStrings.length` assertion.
// ===========================================================================
#[test]
fn db_cursor_duplicate_delete_test_simple_delete_insert() {
    let (_dir, env, db) = open_dup();
    let k = SIMPLE_KEYS.len();

    let put_all = |env: &noxu_db::Environment, db: &noxu_db::Database| {
        let txn = env.begin_transaction(None).unwrap();
        for pk in SIMPLE_KEYS.iter().take(k) {
            for dv in SIMPLE_DATA.iter().take(k) {
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(pk.as_bytes()),
                    DatabaseEntry::from_bytes(dv.as_bytes()),
                )
                .unwrap();
            }
        }
        txn.commit().unwrap();
    };

    put_all(&env, &db);
    assert_eq!((k * k) as u64, db.count().unwrap());

    // Delete every record via a walk.  (JE also asserts count() counts down
    // within the dup set after each delete; that count()-after-delete
    // assertion is exercised by
    // db_cursor_duplicate_delete_test_count_after_delete_returns_remaining
    // below — NEW-DBI-COUNT-AFTER-DELETE, now fixed.)
    let txn = env.begin_transaction(None).unwrap();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut cur = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut s = c.get(&mut cur, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            assert_eq!(OperationStatus::Success, c.delete().unwrap());
            s = c.get(&mut cur, &mut d, Get::Next, None).unwrap();
        }
    }
    txn.commit().unwrap();
    assert_eq!(0, db.count().unwrap());

    // Re-insert and confirm the full K*K set is back.
    put_all(&env, &db);
    assert_eq!((k * k) as u64, db.count().unwrap());
}

// ===========================================================================
// DbCursorDuplicateDeleteTest.testDuplicateDeletionAll
// JE: random dup data (10 keys x 1000 dups); walk deleting every record,
// asserting count()==ht.size() (remaining dups in the key) after each delete;
// then a second walk finds nothing.  Scaled to 10 keys x 100 dups for runtime.
// ===========================================================================
#[test]
#[allow(unused_assignments)]
fn db_cursor_duplicate_delete_test_duplicate_deletion_all() {
    let (_dir, env, db) = open_dup();
    const N_KEYS: u16 = 10;
    const N_DUPS: u16 = 100;
    let txn = env.begin_transaction(None).unwrap();
    for kk in 0..N_KEYS {
        for dd in 0..N_DUPS {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&kk.to_be_bytes()),
                DatabaseEntry::from_bytes(&dd.to_be_bytes()),
            )
            .unwrap();
        }
    }
    txn.commit().unwrap();
    assert_eq!((N_KEYS as u64) * (N_DUPS as u64), db.count().unwrap());

    // Walk deleting every record, asserting dups within each key ascend.
    // (JE also asserts count()==remaining after each delete; that assertion
    // is exercised by
    // db_cursor_duplicate_delete_test_count_after_delete_returns_remaining
    // below — NEW-DBI-COUNT-AFTER-DELETE, now fixed.)
    let txn = env.begin_transaction(None).unwrap();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut cur = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut prev_key: Option<Vec<u8>> = None;
        let mut prev_data: Option<Vec<u8>> = None;
        let mut s = c.get(&mut cur, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            let key = cur.data_opt().unwrap_or(&[]).to_vec();
            let data = d.data_opt().unwrap_or(&[]).to_vec();
            if prev_key.as_ref() != Some(&key) {
                prev_data = None;
            } else if let Some(pd) = &prev_data {
                assert!(*pd < data, "dups within a key must ascend");
            }
            prev_key = Some(key);
            prev_data = Some(data);
            assert_eq!(OperationStatus::Success, c.delete().unwrap());
            s = c.get(&mut cur, &mut d, Get::Next, None).unwrap();
        }
    }
    txn.commit().unwrap();
    assert_eq!(0, db.count().unwrap());
    // Second walk: empty.
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert_eq!(
        OperationStatus::NotFound,
        c.get(&mut k, &mut d, Get::First, None).unwrap()
    );
}

// ===========================================================================
// DbCursorDuplicateDeleteTest.testDuplicateDeletionAssorted
// JE: random dup data; delete ~80% of records at random during a walk, then
// verify the survivors exactly match the un-deleted set.  Uses a deterministic
// PRNG for reproducibility.
// ===========================================================================
#[test]
fn db_cursor_duplicate_delete_test_duplicate_deletion_assorted() {
    let (_dir, env, db) = open_dup();
    const N_KEYS: u16 = 10;
    const N_DUPS: u16 = 100;
    let txn = env.begin_transaction(None).unwrap();
    for kk in 0..N_KEYS {
        for dd in 0..N_DUPS {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&kk.to_be_bytes()),
                DatabaseEntry::from_bytes(&dd.to_be_bytes()),
            )
            .unwrap();
        }
    }
    txn.commit().unwrap();

    // Deterministic PRNG (LCG) for the ~80% delete decision.
    let mut state = 0x9E3779B97F4A7C15u64;
    let mut next = || {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        (state >> 33) as u32
    };

    let mut deleted: BTreeSet<(u16, u16)> = BTreeSet::new();
    let txn = env.begin_transaction(None).unwrap();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut cur = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut s = c.get(&mut cur, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            let kb = cur.data_opt().unwrap();
            let db_ = d.data_opt().unwrap();
            let key = u16::from_be_bytes([kb[0], kb[1]]);
            let dat = u16::from_be_bytes([db_[0], db_[1]]);
            if next() % 10 < 8 {
                assert_eq!(OperationStatus::Success, c.delete().unwrap());
                deleted.insert((key, dat));
            }
            s = c.get(&mut cur, &mut d, Get::Next, None).unwrap();
        }
    }
    txn.commit().unwrap();

    // Survivors must be exactly the complement of `deleted`.
    let mut survivors: BTreeSet<(u16, u16)> = BTreeSet::new();
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let kb = k.data_opt().unwrap();
        let db_ = d.data_opt().unwrap();
        survivors.insert((
            u16::from_be_bytes([kb[0], kb[1]]),
            u16::from_be_bytes([db_[0], db_[1]]),
        ));
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    // No deleted pair may survive.
    for pair in &deleted {
        assert!(!survivors.contains(pair), "deleted {pair:?} resurrected");
    }
    // Every non-deleted pair must survive.
    for kk in 0..N_KEYS {
        for dd in 0..N_DUPS {
            let pair = (kk, dd);
            if !deleted.contains(&pair) {
                assert!(survivors.contains(&pair), "lost non-deleted {pair:?}");
            }
        }
    }
    assert_eq!(
        (N_KEYS as usize * N_DUPS as usize) - deleted.len(),
        survivors.len()
    );
}

// ===========================================================================
// DbCursorDuplicateDeleteTest.testDuplicateDeletionAssortedSR15375  [#15375]
// JE: like Assorted, but for each deleted dup add back a NEW dup (foundData +
// "x") during the same walk.  The regression: adding a dup back while deleting
// during a cursor walk must not corrupt the dup set.  Oracle: after the walk,
// survivors = (original minus deleted) plus the added-back dups, and no deleted
// pair resurfaces.
// ===========================================================================
#[test]
fn db_cursor_duplicate_delete_test_duplicate_deletion_assorted_sr15375() {
    let (_dir, env, db) = open_dup();
    const N_KEYS: u16 = 8;
    const N_DUPS: u16 = 40;
    // Data values are even numbers so "+x" (represented as +1, an odd number)
    // never collides with an existing dup.
    let txn = env.begin_transaction(None).unwrap();
    for kk in 0..N_KEYS {
        for dd in 0..N_DUPS {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&kk.to_be_bytes()),
                DatabaseEntry::from_bytes(&(dd * 2).to_be_bytes()),
            )
            .unwrap();
        }
    }
    txn.commit().unwrap();

    let mut state = 0x243F6A8885A308D3u64;
    let mut next = || {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        (state >> 33) as u32
    };

    let mut expected: BTreeSet<(u16, u16)> = BTreeSet::new();
    for kk in 0..N_KEYS {
        for dd in 0..N_DUPS {
            expected.insert((kk, dd * 2));
        }
    }

    let txn = env.begin_transaction(None).unwrap();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut cur = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut s = c.get(&mut cur, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            let kb = cur.data_opt().unwrap();
            let db_ = d.data_opt().unwrap();
            let key = u16::from_be_bytes([kb[0], kb[1]]);
            let dat = u16::from_be_bytes([db_[0], db_[1]]);
            if next() % 10 < 8 {
                assert_eq!(OperationStatus::Success, c.delete().unwrap());
                expected.remove(&(key, dat));
                // Add back a new dup (dat + 1) via the same cursor.
                let added = dat + 1;
                let s2 = c
                    .put(
                        &DatabaseEntry::from_bytes(&key.to_be_bytes()),
                        &DatabaseEntry::from_bytes(&added.to_be_bytes()),
                        Put::NoDupData,
                    )
                    .unwrap();
                assert_eq!(
                    OperationStatus::Success,
                    s2,
                    "add-back must succeed"
                );
                expected.insert((key, added));
            }
            s = c.get(&mut cur, &mut d, Get::Next, None).unwrap();
        }
    }
    txn.commit().unwrap();

    // Survivors must equal `expected` exactly.
    let mut survivors: BTreeSet<(u16, u16)> = BTreeSet::new();
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let kb = k.data_opt().unwrap();
        let db_ = d.data_opt().unwrap();
        survivors.insert((
            u16::from_be_bytes([kb[0], kb[1]]),
            u16::from_be_bytes([db_[0], db_[1]]),
        ));
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(
        expected, survivors,
        "SR15375: delete+add-back corrupted dup set"
    );
}

// ===========================================================================
// DbCursorDuplicateDeleteTest.testDuplicateDeletionAll / testSimpleDeleteInsert
// (the count()-after-delete assertion)
//
// NEW-DBI-COUNT-AFTER-DELETE (fixed): immediately after `cursor.delete()` on a
// sorted-dups db (with the cursor still parked on the just-deleted slot, NOT
// repositioned), `cursor.count()` returns the number of dups REMAINING in the
// current key.  JE Cursor.countHandleDups re-anchors by the current KEY
// (getCurrentKey survives a delete) and counts the live dups; Noxu's
// `CursorImpl::count()` now does the same — it resolves the primary key from
// the retained anchor (`current_key`, or `last_deleted_key` after a delete),
// searches the tree fresh for the first LIVE dup, and counts forward, so a
// defunct current slot no longer breaks the walk and a fully-emptied key
// reports 0.  Ref: Cursor.java countHandleDups / tp-je-dbi.md.
// ===========================================================================
#[test]
fn db_cursor_duplicate_delete_test_count_after_delete_returns_remaining() {
    let (_dir, env, db) = open_dup();
    let key = DatabaseEntry::from_bytes(b"k");
    for d in 0u8..5 {
        db.put(&key, DatabaseEntry::from_bytes(&[d])).unwrap();
    }
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    // Walk deleting each dup; count() immediately after delete must equal the
    // remaining dup count (4, 3, 2, 1, 0).
    let mut remaining = 5u64;
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        assert_eq!(OperationStatus::Success, c.delete().unwrap());
        remaining -= 1;
        assert_eq!(
            remaining,
            c.count().unwrap(),
            "count() immediately after delete must equal remaining dups"
        );
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    drop(c);
    txn.commit().unwrap();
}

// POSITIVE GUARDS (must pass on BOTH base and fix): count() on a NON-deleted
// position must be unaffected by the NEW-DBI-COUNT-AFTER-DELETE fix.
//
//   * a live dup position reports the FULL dup count of the current key,
//     from any offset within the dup set;
//   * a non-dup DB reports 1 when positioned.
#[test]
fn count_on_live_dup_position_reports_full_dup_count() {
    let (_dir, env, db) = open_dup();
    let key = DatabaseEntry::from_bytes(b"k");
    for d in 0u8..5 {
        db.put(&key, DatabaseEntry::from_bytes(&[d])).unwrap();
    }
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    // From every offset within the (undeleted) dup set count() must be 5.
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    let mut offset = 0;
    while s == OperationStatus::Success {
        assert_eq!(
            5u64,
            c.count().unwrap(),
            "count() on a live dup position (offset {offset}) must be the \
             full dup count"
        );
        offset += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(5, offset, "walk must visit all five dups");
    drop(c);
    txn.commit().unwrap();
}

#[test]
fn count_on_nondup_db_reports_one() {
    let (_dir, env, db) = open_nondup(false);
    db.put(DatabaseEntry::from_bytes(b"k"), DatabaseEntry::from_bytes(b"v"))
        .unwrap();
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::from_bytes(b"k");
    let mut d = DatabaseEntry::new();
    assert_eq!(
        OperationStatus::Success,
        c.get(&mut k, &mut d, Get::Search, None).unwrap()
    );
    assert_eq!(1, c.count().unwrap(), "non-dup count() when positioned is 1");
    drop(c);
    txn.commit().unwrap();
}
// Keep an unused-import guard: Ordering is used by the comparator ctor.
const _: fn(&[u8], &[u8]) -> Ordering = |a, b| a.cmp(b);
