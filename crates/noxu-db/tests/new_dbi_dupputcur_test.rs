//! NEW-DBI-DUPPUTCUR regression: `Cursor::put(.., Put::Current)` on a
//! sorted-duplicate database must NOT move a duplicate to a new sort
//! position.
//!
//! JE contract (`Cursor.putCurrent` / `CursorImpl.putCurrent`,
//! Cursor.java:2624, CursorImpl.java:1618): for a sorted-dups DB the
//! putCurrent 2-part key must COMPARE EQUAL to the current slot's key
//! under the DB's (composite) comparator; otherwise
//! `DuplicateDataException` is thrown.  You may only update the current
//! duplicate to a value that sorts EQUAL — you may never MOVE it.
//!
//! Before the fix, `put_dup` PutMode::Current did an unconditional
//! delete+reinsert with no compare-equal guard.  Writing DIFFERENT data
//! re-sorted the entry to a new position (a silent MOVE, returning
//! Ok(Success)), after which a forward walk SKIPPED intervening dups —
//! a silent wrong result.

use noxu_db::Environment;
use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, NoxuError,
    OperationStatus, Put,
};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Environment {
    let config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    Environment::open(config).expect("env open")
}

fn dup_db(env: &Environment, name: &str) -> noxu_db::Database {
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    env.open_database(None, name, &db_cfg).unwrap()
}

/// PRIMARY repro: putCurrent with DIFFERENT dup data on a sorted-dups DB
/// must return `DuplicateDataException` (NOT Success), the dup set must be
/// UNCHANGED, and a forward walk must still visit every original dup in
/// order.
///
/// FAILS on base 69917353 (put_dup silently moves the dup and returns
/// Success, so the walk skips dups); PASSES on the compare-equal fix.
#[test]
fn put_current_different_dup_data_raises_and_leaves_set_unchanged() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = dup_db(&env, "dupputcur");

    // Dup set for key "k": a, b, c, d (byte-order sorted).
    let key = DatabaseEntry::from_bytes(b"k");
    for v in [b"a".as_ref(), b"b", b"c", b"d"] {
        db.put(&key, DatabaseEntry::from_bytes(v)).unwrap();
    }

    // Position the cursor on the FIRST dup ("a").
    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"k");
    let mut dout = DatabaseEntry::new();
    let s = cursor.get(&mut kout, &mut dout, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(dout.data_opt().unwrap(), b"a");

    // putCurrent with DIFFERENT data ("z", which would re-sort to the end).
    // JE: DuplicateDataException — you cannot MOVE a dup via putCurrent.
    let new_data = DatabaseEntry::from_bytes(b"z");
    let res = cursor.put(&key, &new_data, Put::Current);
    match res {
        Err(NoxuError::DuplicateDataException) => { /* correct */ }
        other => panic!(
            "putCurrent(different data) must raise DuplicateDataException, got {:?}",
            other
        ),
    }
    cursor.close().unwrap();

    // The dup set must be UNCHANGED: a forward walk must still visit
    // a, b, c, d in order (no skip, no move, no loss).
    let mut cur2 = db.open_cursor(None).unwrap();
    let mut k2 = DatabaseEntry::from_bytes(b"k");
    let mut d2 = DatabaseEntry::new();
    let mut seen = Vec::new();
    let mut s = cur2.get(&mut k2, &mut d2, Get::Search, None).unwrap();
    while s == OperationStatus::Success {
        seen.push(d2.data_opt().unwrap().to_vec());
        s = cur2.get(&mut k2, &mut d2, Get::NextDup, None).unwrap();
    }
    cur2.close().unwrap();

    assert_eq!(
        seen,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
        "dup set must be unchanged and fully visited after rejected putCurrent"
    );

    // count() must still be 4.
    let mut cur3 = db.open_cursor(None).unwrap();
    let mut k3 = DatabaseEntry::from_bytes(b"k");
    let mut d3 = DatabaseEntry::new();
    cur3.get(&mut k3, &mut d3, Get::Search, None).unwrap();
    assert_eq!(cur3.count().unwrap(), 4, "dup count must be unchanged");
    cur3.close().unwrap();

    let _ = env.close();
}

/// Repro on a MIDDLE dup: putCurrent moving "b" to "cc" (between c and d)
/// on base silently reorders and a subsequent walk skips a dup.
#[test]
fn put_current_different_data_on_middle_dup_rejected() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = dup_db(&env, "dupputcur_mid");

    let key = DatabaseEntry::from_bytes(b"k");
    for v in [b"a".as_ref(), b"b", b"c", b"d"] {
        db.put(&key, DatabaseEntry::from_bytes(v)).unwrap();
    }

    // Position on "b" via SearchBoth.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"k");
    let mut dout = DatabaseEntry::from_bytes(b"b");
    let s = cursor.get(&mut kout, &mut dout, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::Success);

    // Move "b" -> "cc" (a NEW sort position between c and d): must be rejected.
    let res = cursor.put(&key, &DatabaseEntry::from_bytes(b"cc"), Put::Current);
    assert!(
        matches!(res, Err(NoxuError::DuplicateDataException)),
        "putCurrent moving a middle dup must raise DuplicateDataException, got {:?}",
        res
    );
    cursor.close().unwrap();

    // Full set intact.
    let mut cur2 = db.open_cursor(None).unwrap();
    let mut k2 = DatabaseEntry::from_bytes(b"k");
    let mut d2 = DatabaseEntry::new();
    let mut seen = Vec::new();
    let mut s = cur2.get(&mut k2, &mut d2, Get::Search, None).unwrap();
    while s == OperationStatus::Success {
        seen.push(d2.data_opt().unwrap().to_vec());
        s = cur2.get(&mut k2, &mut d2, Get::NextDup, None).unwrap();
    }
    cur2.close().unwrap();
    assert_eq!(
        seen,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
    );

    let _ = env.close();
}

/// putCurrent with EQUAL data (byte-identical) on a sorted-dups DB is a
/// JE-allowed in-place update and must return Success, leaving the set
/// unchanged.
#[test]
fn put_current_equal_dup_data_succeeds_in_place() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = dup_db(&env, "dupputcur_equal");

    let key = DatabaseEntry::from_bytes(b"k");
    for v in [b"a".as_ref(), b"b", b"c"] {
        db.put(&key, DatabaseEntry::from_bytes(v)).unwrap();
    }

    // Position on "b".
    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"k");
    let mut dout = DatabaseEntry::from_bytes(b"b");
    let s = cursor.get(&mut kout, &mut dout, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::Success);

    // putCurrent with the SAME data ("b"): in-place update, allowed.
    let s = cursor
        .put(&key, &DatabaseEntry::from_bytes(b"b"), Put::Current)
        .unwrap();
    assert_eq!(s, OperationStatus::Success);
    cursor.close().unwrap();

    // Set unchanged: a, b, c.
    let mut cur2 = db.open_cursor(None).unwrap();
    let mut k2 = DatabaseEntry::from_bytes(b"k");
    let mut d2 = DatabaseEntry::new();
    let mut seen = Vec::new();
    let mut s = cur2.get(&mut k2, &mut d2, Get::Search, None).unwrap();
    while s == OperationStatus::Success {
        seen.push(d2.data_opt().unwrap().to_vec());
        s = cur2.get(&mut k2, &mut d2, Get::NextDup, None).unwrap();
    }
    cur2.close().unwrap();
    assert_eq!(seen, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

    let _ = env.close();
}

/// REGRESSION: putCurrent on a NON-dup database replaces the data
/// normally (this path must be unaffected by the dup guard).
#[test]
fn put_current_on_non_dup_db_replaces_data() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    // No sorted_duplicates: plain DB.
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "plain", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"k");
    db.put(&key, DatabaseEntry::from_bytes(b"v1")).unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"k");
    let mut dout = DatabaseEntry::new();
    cursor.get(&mut kout, &mut dout, Get::Search, None).unwrap();
    assert_eq!(dout.data_opt().unwrap(), b"v1");

    // putCurrent with different data: plain replace, allowed.
    let s = cursor
        .put(&key, &DatabaseEntry::from_bytes(b"v2"), Put::Current)
        .unwrap();
    assert_eq!(s, OperationStatus::Success);
    cursor.close().unwrap();

    let mut out = DatabaseEntry::new();
    assert!(db.get_into(None, &key, &mut out).unwrap());
    assert_eq!(
        out.data_opt().unwrap(),
        b"v2",
        "non-dup putCurrent must replace"
    );

    let _ = env.close();
}

/// REGRESSION: a normal dup INSERT (Put::Overwrite / db.put, not
/// putCurrent) still adds dups.
#[test]
fn normal_dup_insert_still_adds_dups() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = dup_db(&env, "dupinsert");

    let key = DatabaseEntry::from_bytes(b"k");
    for v in [b"a".as_ref(), b"b", b"c"] {
        db.put(&key, DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"k");
    let mut dout = DatabaseEntry::new();
    cursor.get(&mut kout, &mut dout, Get::Search, None).unwrap();
    assert_eq!(cursor.count().unwrap(), 3);
    cursor.close().unwrap();

    let _ = env.close();
}

/// EQUAL-under-a-CUSTOM-dup-comparator: JE allows putCurrent to replace the
/// current dup with a byte-DIFFERENT value that still sorts EQUAL under the
/// duplicate comparator (CursorImpl.java:1618 note: "the 2 keys may not be
/// identical if custom comparators are used").
///
/// Here a case-insensitive dup comparator treats "a" and "A" as equal, so
/// putCurrent("a" -> "A") is an allowed in-place update (Success), while
/// putCurrent("a" -> "b") (which sorts DIFFERENTLY) is rejected.
#[test]
fn put_current_sort_equal_under_custom_dup_comparator_succeeds() {
    use noxu_db::Comparator;
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);

    let ci = Comparator::new("ascii_ci", |a: &[u8], b: &[u8]| {
        let la: Vec<u8> = a.iter().map(|c| c.to_ascii_lowercase()).collect();
        let lb: Vec<u8> = b.iter().map(|c| c.to_ascii_lowercase()).collect();
        la.cmp(&lb)
    });
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_duplicate_comparator(ci);
    let db = env.open_database(None, "dupputcur_ci", &db_cfg).unwrap();

    let key = DatabaseEntry::from_bytes(b"k");
    // Under case-insensitive dup ordering these are distinct dups.
    db.put(&key, DatabaseEntry::from_bytes(b"a")).unwrap();
    db.put(&key, DatabaseEntry::from_bytes(b"m")).unwrap();

    // Position on "a".
    let mut cursor = db.open_cursor(None).unwrap();
    let mut kout = DatabaseEntry::from_bytes(b"k");
    let mut dout = DatabaseEntry::from_bytes(b"a");
    let s = cursor.get(&mut kout, &mut dout, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::Success);

    // "A" sorts EQUAL to "a" under the CI comparator: allowed in-place update.
    let s = cursor
        .put(&key, &DatabaseEntry::from_bytes(b"A"), Put::Current)
        .unwrap();
    assert_eq!(
        s,
        OperationStatus::Success,
        "sort-equal replace under a custom dup comparator must be allowed"
    );

    // The cursor is now on the updated "A" entry.  "b" sorts DIFFERENTLY
    // (it would move between "a"/"A" and "m") -> rejected.
    let res = cursor.put(&key, &DatabaseEntry::from_bytes(b"b"), Put::Current);
    assert!(
        matches!(res, Err(NoxuError::DuplicateDataException)),
        "sort-DIFFERENT replace under a custom dup comparator must be rejected, got {:?}",
        res
    );
    cursor.close().unwrap();

    let _ = env.close();
}
