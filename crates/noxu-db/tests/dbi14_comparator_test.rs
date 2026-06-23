//! DBI-14 / DBI-15 — user-supplied Btree + duplicate comparators.
//!
//! Headline tests:
//!  1. A DB opened with a custom Btree comparator (reverse order, and
//!     big-endian-integer order) sorts/seeks/range-scans in THAT order.
//!     Fail-pre: byte order.  Pass-post: comparator order.
//!  2. Reopening a DB whose comparator-identity was persisted WITHOUT
//!     supplying a matching comparator FAILS (mismatch semantics) — no
//!     silent sort corruption.
//!  3. A duplicate comparator orders dup data.

use noxu_db::{
    Comparator, DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get,
    OperationStatus,
};
use tempfile::TempDir;

fn env(dir: &TempDir) -> noxu_db::Environment {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    noxu_db::Environment::open(cfg).unwrap()
}

fn put(db: &noxu_db::Database, k: &[u8], v: &[u8]) {
    db.put(None, &DatabaseEntry::from_bytes(k), &DatabaseEntry::from_bytes(v))
        .unwrap();
}

/// Walk the whole DB in cursor (First → Next) order, returning the keys.
fn cursor_keys(db: &noxu_db::Database) -> Vec<Vec<u8>> {
    let mut cur = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut out = Vec::new();
    let mut s = cur.get(&mut key, &mut data, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        out.push(key.data().to_vec());
        s = cur.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    out
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE TEST 1 — custom Btree comparator drives sort/seek/scan order.
// ───────────────────────────────────────────────────────────────────────────

/// Reverse (descending byte) order.  Cursor walk must yield keys in DESCENDING
/// order, the exact opposite of the default unsigned-byte ascending walk.
#[test]
fn headline1_reverse_btree_comparator_orders_cursor_walk() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let cmp = Comparator::new("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_btree_comparator(cmp);
    let db = e.open_database(None, "rev", &cfg).unwrap();

    for k in [b"a".as_ref(), b"b", b"c", b"d", b"e"] {
        put(&db, k, b"v");
    }

    let keys = cursor_keys(&db);
    // Pass-post: comparator (descending) order.  Fail-pre (no comparator)
    // would be ascending [a,b,c,d,e].
    assert_eq!(
        keys,
        vec![
            b"e".to_vec(),
            b"d".to_vec(),
            b"c".to_vec(),
            b"b".to_vec(),
            b"a".to_vec()
        ],
        "cursor walk must follow the reverse comparator, not byte order"
    );
}

/// Big-endian integer order over 4-byte keys.  We insert keys whose raw byte
/// order DIFFERS from their integer order is not possible for fixed-width BE
/// (BE byte order == integer order), so instead use a comparator that parses
/// keys as little-endian u32 — there the byte order and the integer order
/// genuinely diverge, proving the comparator (not byte order) decides.
#[test]
fn headline1_le_integer_comparator_diverges_from_byte_order() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let cmp = Comparator::new("le_u32", |a: &[u8], b: &[u8]| {
        let pa = u32::from_le_bytes(a.try_into().unwrap());
        let pb = u32::from_le_bytes(b.try_into().unwrap());
        pa.cmp(&pb)
    });
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_btree_comparator(cmp);
    let db = e.open_database(None, "le", &cfg).unwrap();

    // Integer values 1, 256, 65536 — as LE bytes their lexicographic byte
    // order is the REVERSE of their integer order.
    let vals: [u32; 3] = [1, 256, 65536];
    for v in vals {
        put(&db, &v.to_le_bytes(), b"v");
    }

    let keys = cursor_keys(&db);
    let got: Vec<u32> = keys
        .iter()
        .map(|k| u32::from_le_bytes(k[..].try_into().unwrap()))
        .collect();
    // Pass-post: integer ascending [1,256,65536].
    // Fail-pre (byte order) would be [65536,256,1] (LE bytes lexicographic).
    assert_eq!(got, vec![1u32, 256, 65536]);

    // Seek must also honour the comparator: SearchGte 200 → 256.
    let mut cur = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(&200u32.to_le_bytes());
    let mut data = DatabaseEntry::new();
    let s = cur.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(u32::from_le_bytes(key.data().try_into().unwrap()), 256);
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE TEST 2 — persisted comparator-identity mismatch on reopen FAILS.
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn headline2_reopen_without_matching_comparator_fails() {
    let dir = TempDir::new().unwrap();
    {
        let e = env(&dir);
        let cmp = Comparator::new("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_btree_comparator(cmp);
        let db = e.open_database(None, "rev", &cfg).unwrap();
        put(&db, b"a", b"1");
        put(&db, b"b", b"2");
        drop(db);
        e.close().unwrap();
    }

    // Reopen WITHOUT supplying any comparator — must FAIL (mismatch), not
    // silently fall back to byte order (which would corrupt the sort).
    let e = env(&dir);
    let cfg =
        DatabaseConfig::new().with_allow_create(false).with_transactional(true);
    let res = e.open_database(None, "rev", &cfg);
    assert!(
        res.is_err(),
        "reopen without matching comparator must fail, not silently \
         reinterpret a comparator-ordered tree as byte-ordered"
    );
}

#[test]
fn headline2_reopen_with_matching_identity_succeeds() {
    let dir = TempDir::new().unwrap();
    {
        let e = env(&dir);
        let cmp = Comparator::new("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_btree_comparator(cmp);
        let db = e.open_database(None, "rev", &cfg).unwrap();
        put(&db, b"a", b"1");
        put(&db, b"b", b"2");
        put(&db, b"c", b"3");
        drop(db);
        e.close().unwrap();
    }

    let e = env(&dir);
    let cmp = Comparator::new("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
    let cfg = DatabaseConfig::new()
        .with_allow_create(false)
        .with_transactional(true)
        .with_btree_comparator(cmp);
    let db = e.open_database(None, "rev", &cfg).unwrap();
    let keys = cursor_keys(&db);
    assert_eq!(
        keys,
        vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()],
        "reopened tree must keep its comparator order"
    );
}

#[test]
fn headline2_reopen_with_wrong_identity_fails() {
    let dir = TempDir::new().unwrap();
    {
        let e = env(&dir);
        let cmp = Comparator::new("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_btree_comparator(cmp);
        let db = e.open_database(None, "rev", &cfg).unwrap();
        put(&db, b"a", b"1");
        drop(db);
        e.close().unwrap();
    }

    // Supply a comparator with a DIFFERENT identity — mismatch, must fail.
    let e = env(&dir);
    let cmp = Comparator::new("forward", |a: &[u8], b: &[u8]| a.cmp(b));
    let cfg = DatabaseConfig::new()
        .with_allow_create(false)
        .with_transactional(true)
        .with_btree_comparator(cmp);
    let res = e.open_database(None, "rev", &cfg);
    assert!(res.is_err(), "mismatched comparator identity must fail open");
}

#[test]
fn headline2_override_allows_replacing_persisted_comparator() {
    let dir = TempDir::new().unwrap();
    {
        let e = env(&dir);
        let cmp = Comparator::new("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_btree_comparator(cmp);
        let db = e.open_database(None, "rev", &cfg).unwrap();
        put(&db, b"a", b"1");
        drop(db);
        e.close().unwrap();
    }

    // With override set, a different comparator is accepted (JE
    // setOverrideBtreeComparator).
    let e = env(&dir);
    let cmp = Comparator::new("forward", |a: &[u8], b: &[u8]| a.cmp(b));
    let mut cfg = DatabaseConfig::new()
        .with_allow_create(false)
        .with_transactional(true)
        .with_btree_comparator(cmp);
    cfg.set_override_btree_comparator(true);
    let res = e.open_database(None, "rev", &cfg);
    assert!(res.is_ok(), "override must permit replacing the comparator");
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE TEST 3 — duplicate comparator orders dup data.
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn headline3_duplicate_comparator_orders_dup_data() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    // Reverse the data order within a key.
    let dup_cmp = Comparator::new("rev_dup", |a: &[u8], b: &[u8]| b.cmp(a));
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_duplicate_comparator(dup_cmp);
    let db = e.open_database(None, "dup", &cfg).unwrap();

    // Single key, several data values inserted out of order.
    for d in [b"a".as_ref(), b"c", b"b", b"e", b"d"] {
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(d),
        )
        .unwrap();
    }

    // Walk all duplicates of "k": data must come back in DESCENDING order.
    let mut cur = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut datas = Vec::new();
    let mut s = cur.get(&mut key, &mut data, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        datas.push(data.data().to_vec());
        s = cur.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    assert_eq!(
        datas,
        vec![
            b"e".to_vec(),
            b"d".to_vec(),
            b"c".to_vec(),
            b"b".to_vec(),
            b"a".to_vec()
        ],
        "duplicate data must follow the dup comparator (descending)"
    );
}

/// Default duplicate ordering (no dup comparator) must stay ascending byte
/// order — the regression guard for the faithful default.
#[test]
fn default_duplicate_order_is_ascending_byte_order() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    let db = e.open_database(None, "dup_def", &cfg).unwrap();
    for d in [b"c".as_ref(), b"a", b"b"] {
        db.put(
            None,
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(d),
        )
        .unwrap();
    }
    let mut cur = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut datas = Vec::new();
    let mut s = cur.get(&mut key, &mut data, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        datas.push(data.data().to_vec());
        s = cur.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    assert_eq!(datas, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

// Sanity: identity-only equality of the public Comparator type.
#[test]
fn comparator_equality_is_by_identity() {
    let a = Comparator::new("x", |p: &[u8], q: &[u8]| p.cmp(q));
    let b = Comparator::new("x", |p: &[u8], q: &[u8]| q.cmp(p));
    let c = Comparator::new("y", |p: &[u8], q: &[u8]| p.cmp(q));
    assert_eq!(a, b); // same identity
    assert_ne!(a, c); // different identity
}
