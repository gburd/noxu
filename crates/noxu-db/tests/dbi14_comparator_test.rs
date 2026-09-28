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
    db.put(DatabaseEntry::from_bytes(k), DatabaseEntry::from_bytes(v)).unwrap();
}

/// Walk the whole DB in cursor (First → Next) order, returning the keys.
fn cursor_keys(db: &noxu_db::Database) -> Vec<Vec<u8>> {
    let mut cur = db.open_cursor(None).unwrap();
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
// JE: DatabaseComparatorsTest.testSR12517 / testSR16816ReverseComparator —
// a custom (reverse) Btree comparator drives the stored sort order and the
// cursor walk order.
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
    let mut cur = db.open_cursor(None).unwrap();
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

// JE parity: RecoveryTest.testBasicRecoveryWithBtreeComparator — a DB
// opened with a custom Btree comparator keeps its comparator order across
// a close+reopen (recovery); the persisted comparator identity is honoured.
// JE: DatabaseConfigTest.testPersistentAndMutableConfigs (Btree-comparator
// persistence branch) — a persisted comparator identity is honoured across a
// close+reopen when a matching comparator is re-supplied.
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

// JE: DatabaseConfigTest.testConfigOverrideUpdateSR15743 —
// setOverrideBtreeComparator(true) lets a subsequent open replace the
// persisted comparator instead of failing the mismatch check.
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

// JE: DatabaseComparatorsTest.testSR16816ReverseComparator (dup-comparator
// branch) / DatabaseConfigTest.testPersistentAndMutableConfigs
// (Duplicate-comparator persistence branch) — a custom duplicate comparator
// orders dup data.
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
        db.put(DatabaseEntry::from_bytes(b"k"), DatabaseEntry::from_bytes(d))
            .unwrap();
    }

    // Walk all duplicates of "k": data must come back in DESCENDING order.
    let mut cur = db.open_cursor(None).unwrap();
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
        db.put(DatabaseEntry::from_bytes(b"k"), DatabaseEntry::from_bytes(d))
            .unwrap();
    }
    let mut cur = db.open_cursor(None).unwrap();
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

// Reopen of a sorted-dup DB with a custom dup comparator must preserve the
// dup order (the recovery resort path, DBI-14).
#[test]
fn reopen_sorted_dup_with_dup_comparator_preserves_order() {
    let dir = TempDir::new().unwrap();
    let dup_id = "rev_dup";
    {
        let e = env(&dir);
        let dup_cmp = Comparator::new(dup_id, |a: &[u8], b: &[u8]| b.cmp(a));
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(true)
            .with_duplicate_comparator(dup_cmp);
        let db = e.open_database(None, "dup", &cfg).unwrap();
        for d in [b"a".as_ref(), b"c", b"b", b"e", b"d"] {
            db.put(
                DatabaseEntry::from_bytes(b"k"),
                DatabaseEntry::from_bytes(d),
            )
            .unwrap();
        }
        drop(db);
        e.close().unwrap();
    }

    let e = env(&dir);
    let dup_cmp = Comparator::new(dup_id, |a: &[u8], b: &[u8]| b.cmp(a));
    let cfg = DatabaseConfig::new()
        .with_allow_create(false)
        .with_transactional(true)
        .with_sorted_duplicates(true)
        .with_duplicate_comparator(dup_cmp);
    let db = e.open_database(None, "dup", &cfg).unwrap();
    let mut cur = db.open_cursor(None).unwrap();
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
        "reopened sorted-dup DB must keep dup-comparator order"
    );
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

// ──────────────────────────────────────────────────────────────────────────────
// DatabaseComparatorsTest.testReuseSlotAbortPartialKey /
// testReuseSlotRecoverPartialKey  [JE #15704]
//
// A "partial" btree comparator compares only the FIRST 4 bytes of an 8-byte
// key, so key={0,0} and key={0,1} are EQUAL under the comparator.  Sequence:
//   * auto-commit insert key={0,0}/data={0}
//   * txn: delete key={0,1} (== {0,0} to the comparator), insert
//     key={0,1}/data={1} — this REUSES the slot for {0,0} because the keys
//     compare equal — then ABORT.
//   * after abort (optionally after recovery) the record must roll back to
//     key={0,0}/data={0}.
//
// This exercises slot-reuse-then-abort with a partial comparator: the abort
// must restore both the original key bytes and the original data.
// ──────────────────────────────────────────────────────────────────────────────

fn first4_comparator() -> noxu_db::Comparator {
    // Compare only the first 4 bytes (big-endian int) of the key.
    noxu_db::Comparator::new("partial_first4", |a: &[u8], b: &[u8]| {
        a.get(..4).unwrap_or(a).cmp(b.get(..4).unwrap_or(b))
    })
}

fn e2(p1: u32, p2: u32) -> noxu_db::DatabaseEntry {
    let mut v = Vec::with_capacity(8);
    v.extend_from_slice(&p1.to_be_bytes());
    v.extend_from_slice(&p2.to_be_bytes());
    noxu_db::DatabaseEntry::from_bytes(&v)
}
fn e1(p1: u32) -> noxu_db::DatabaseEntry {
    noxu_db::DatabaseEntry::from_bytes(&p1.to_be_bytes())
}

fn do_test_reuse_slot_partial_key(run_recovery: bool) {
    use noxu_db::{DatabaseConfig, EnvironmentConfig, Get, OperationStatus};
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let open = |create: bool| {
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(path.clone())
                .with_allow_create(create)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "reuseKey",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true)
                    .with_btree_comparator(first4_comparator()),
            )
            .unwrap();
        (env, db)
    };

    let (env, db) = open(true);

    // Insert key={0,0}/data={0} auto-commit; getSearchBoth by {0,1} (== {0,0}).
    db.put(e2(0, 0), e1(0)).unwrap();
    {
        let mut c = db.open_cursor(None).unwrap();
        let mut k = e2(0, 1);
        let mut d = e1(0);
        assert_eq!(
            c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap(),
            OperationStatus::Success
        );
        // The stored key is the ORIGINAL {0,0}, not the search key {0,1}.
        assert_eq!(k.data_opt().unwrap(), e2(0, 0).data_opt().unwrap());
        assert_eq!(d.data_opt().unwrap(), e1(0).data_opt().unwrap());
    }

    // txn: delete {0,1}, insert {0,1}/data={1} (reuses the {0,0} slot), abort.
    let txn = env.begin_transaction(None).unwrap();
    assert!(db.delete_in(&txn, e2(0, 1)).unwrap());
    let mut out = noxu_db::DatabaseEntry::new();
    assert!(
        !db.get_into(Some(&txn), e2(0, 0), &mut out).unwrap(),
        "after delete the key must be gone within the txn"
    );
    db.put_in(&txn, e2(0, 1), e1(1)).unwrap();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut k = e2(0, 0);
        let mut d = e1(1);
        assert_eq!(
            c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(k.data_opt().unwrap(), e2(0, 1).data_opt().unwrap());
    }
    txn.abort().unwrap();

    let (env, db) = if run_recovery {
        db.close().unwrap();
        env.close().unwrap();
        // Drop by scope: reopen fresh.
        drop(env);
        open(false)
    } else {
        (env, db)
    };

    // After abort (+optional recovery) the record must roll back to the
    // original committed {0,0}/data={0} — it must still EXIST (count == 1).
    assert_eq!(
        db.count().unwrap(),
        1,
        "aborted slot reuse must not lose the original committed record"
    );
    let mut c = db.open_cursor(None).unwrap();
    let mut k = e2(0, 9);
    let mut d = e1(9);
    assert_eq!(
        c.get(&mut k, &mut d, Get::First, None).unwrap(),
        OperationStatus::Success,
        "abort must roll back to the original committed record"
    );
    assert_eq!(
        k.data_opt().unwrap(),
        e2(0, 0).data_opt().unwrap(),
        "aborted slot reuse must restore the original key bytes {{0,0}}"
    );
    assert_eq!(
        d.data_opt().unwrap(),
        e1(0).data_opt().unwrap(),
        "aborted slot reuse must restore the original data {{0}}"
    );
    drop(c);
    db.close().unwrap();
}

// JE: DatabaseComparatorsTest.testReuseSlotAbortPartialKey
//
// ENGINE-BUG CANDIDATE (NEW-REUSESLOT-1) — DATA LOSS: aborting a
// partial-comparator slot reuse LOSES the original committed record.
//
// Runtime-probed with a control (2026-05): the sequence
//   put {0,0}/{0} (commit) ; txn{ delete {0,1} (== {0,0} under the partial
//   comparator, so it targets the existing slot) ; insert {0,1}/{1} (reuses
//   that slot) } ; ABORT
// leaves the database EMPTY (count == 0, getFirst == NotFound) — the original
// {0,0}/{0} is gone.  DURABLE: a point-get of the original committed key {0,0}
// returns NotFound after the abort AND still after a close+reopen (recovery) —
// the committed record is permanently lost, not merely mis-counted.  Expected
// (JE): abort rolls the slot back to {0,0}/{0} (count == 1).  Control: the SAME
// sequence with the DEFAULT byte comparator
// (where {0,0} and {0,1} are DISTINCT keys, so no slot reuse occurs) correctly
// leaves count == 1 after abort — isolating the fault to the slot-reuse path
// under a partial (compares-equal) comparator, not the abort machinery itself.
// JE reference: DatabaseComparatorsTest / [#15704] (slot reuse must restore the
// pre-reuse LSN on abort).  Kept #[ignore]d — faithful repro, must not be
// weakened; escalated for a dedicated fix worker.  Severity: DATA LOSS but
// NARROW (requires a partial/compares-equal btree comparator + abort).
#[test]
#[ignore = "NEW-REUSESLOT-1: abort after partial-comparator slot reuse LOSES the original record (count 0); control with distinct keys keeps count 1"]
fn reuse_slot_abort_partial_key() {
    do_test_reuse_slot_partial_key(false);
}

// JE: DatabaseComparatorsTest.testReuseSlotRecoverPartialKey
// Same DATA-LOSS finding as reuse_slot_abort_partial_key, after recovery.
#[test]
#[ignore = "NEW-REUSESLOT-1: see reuse_slot_abort_partial_key (data loss on abort of partial-comparator slot reuse)"]
fn reuse_slot_recover_partial_key() {
    do_test_reuse_slot_partial_key(true);
}
