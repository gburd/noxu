//! NEW-TREE-RLE — durable data loss when key-prefixing is combined with a
//! custom btree comparator whose order diverges from unsigned-byte order.
//!
//! Faithful port of JE `KeyPrefixTest.testRLEComparator`
//! (`test/com/sleepycat/je/tree/KeyPrefixTest.java`).
//!
//! Root cause (crates/noxu-tree/src/tree.rs): JE keeps prefix compression ON
//! even with a custom comparator (`IN.computeKeyPrefix` runs regardless,
//! `byteOrdered=false` forces an all-keys scan precisely because a comparator
//! can reorder keys — IN.java:1623) and every slot search reconstructs the
//! FULL key (prefix+suffix) and applies the comparator
//! (`INKeyRep.compareKeys` → `Key.compareKeys`, INKeyRep.java:216/225).  A
//! stored slot is ALWAYS a prefix-stripped SUFFIX (`IN.setKey` →
//! `computeKeySuffix`, IN.java:1533).  Noxu`s `BinStub::insert_cmp` violated
//! this: on a new insert into a BIN that already had a non-empty prefix (set
//! by a prior split`s `recompute_key_prefix`), it stored the FULL key into a
//! suffix-compressed slot array, mixing full keys with suffixes → the
//! comparator binary search desyncs → keys become unreachable, DURABLY.
//!
//! JE: KeyPrefixTest.testRLEComparator

use noxu_db::{Comparator, DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use tempfile::TempDir;

// ── RLE codec (JE KeyPrefixTest.strToRLEbytes / rleBytesToStr) ──────────────
// 4-byte big-endian run length + 1-byte char, per run.  The encoded byte order
// differs from the decoded-string order, so a comparator that decodes-then-
// compares diverges from unsigned-byte order over the encoded keys.
fn str_to_rle(s: &str) -> Vec<u8> {
    let bytes: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let mut len = 1u32;
        while i + 1 < bytes.len() && bytes[i] == bytes[i + 1] {
            len += 1;
            i += 1;
        }
        out.extend_from_slice(&len.to_be_bytes());
        out.push(bytes[i] as u8);
        i += 1;
    }
    out
}

fn rle_to_str(b: &[u8]) -> String {
    let mut out = String::new();
    let mut i = 0;
    while i + 5 <= b.len() {
        let len = u32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
        let c = b[i + 4] as char;
        for _ in 0..len {
            out.push(c);
        }
        i += 5;
    }
    out
}

fn env(dir: &TempDir) -> noxu_db::Environment {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    noxu_db::Environment::open(cfg).unwrap()
}

// Deterministic pseudo-random longs (JE uses Random(0)); xorshift here.
fn make_keys(key_count: usize) -> Vec<Vec<u8>> {
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        let n = next() as i64;
        keys.push(str_to_rle(&n.to_string()));
    }
    keys
}

fn get_present(db: &noxu_db::Database, key: &[u8]) -> bool {
    let mut out = DatabaseEntry::new();
    db.get_into(None, DatabaseEntry::from_bytes(key), &mut out).unwrap()
}

use noxu_db::{Get, OperationStatus};

// Walk the whole DB in cursor (First → Next) order, returning decoded keys.
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

// Small fanout so ~300 keys force many BIN splits (first split at ~fanout).
const FANOUT: u32 = 16;
const KEY_COUNT: usize = 300;

// ── PRIMARY REPRO ───────────────────────────────────────────────────────────
// Prefixing ON + RLE (non-byte-order) comparator + > fanout keys (many splits).
// Every key must be retrievable by point-get, the cursor scan must be in
// comparator order and contain every key, env.verify() must report zero
// errors, and all of this must SURVIVE a close+reopen (durable).
//
// FAILS on base 65ea5165 (~251/300 missing after the first split); PASSES on
// the fix.  JE: KeyPrefixTest.testRLEComparator.
#[test]
fn rle_comparator_prefixing_no_loss_and_durable() {
    let dir = TempDir::new().unwrap();
    let keys = make_keys(KEY_COUNT);

    {
        let e = env(&dir);
        let cmp = Comparator::new("rle", |a: &[u8], b: &[u8]| {
            rle_to_str(a).cmp(&rle_to_str(b))
        });
        let cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_key_prefixing(true)
            .with_node_max_entries(FANOUT)
            .with_btree_comparator(cmp);
        let db = e.open_database(None, "rle", &cfg).unwrap();

        // put + immediate get (as JE does), then a full sweep.
        for k in &keys {
            db.put(DatabaseEntry::from_bytes(k), DatabaseEntry::from_bytes(k))
                .unwrap();
            assert!(
                get_present(&db, k),
                "RLE key must be retrievable immediately after insert"
            );
        }

        // (1) point-get sweep: every seeded key present.
        let mut missing = 0usize;
        for k in &keys {
            if !get_present(&db, k) {
                missing += 1;
            }
        }
        assert_eq!(
            missing,
            0,
            "point-get: {missing}/{} keys unretrievable under prefixing + \
             non-byte-order comparator",
            keys.len()
        );

        // (2) cursor scan cross-check (NEW-4 lesson: do not diagnose via cursor
        // scan alone).  Must contain every unique key AND be in comparator
        // (decoded-string) order.
        let mut unique: std::collections::HashSet<Vec<u8>> =
            keys.iter().cloned().collect();
        let scan = cursor_keys(&db);
        for k in &scan {
            unique.remove(k);
        }
        assert!(
            unique.is_empty(),
            "cursor scan missing {} unique keys",
            unique.len()
        );
        let mut decoded: Vec<String> =
            scan.iter().map(|k| rle_to_str(k)).collect();
        let sorted = {
            let mut s = decoded.clone();
            s.sort();
            s
        };
        assert_eq!(
            decoded, sorted,
            "cursor scan must be in comparator (decoded-string) order"
        );
        decoded.dedup();
        assert_eq!(
            decoded.len(),
            scan.len(),
            "cursor scan must not contain duplicate keys"
        );

        // (3) structural verify: zero errors.
        let vr = db.verify(&noxu_db::VerifyConfig::new()).unwrap();
        assert_eq!(
            vr.error_count(),
            0,
            "verify found {} structural error(s): {:?}",
            vr.error_count(),
            vr.errors
        );

        drop(db);
        e.close().unwrap();
    }

    // (4) DURABILITY: reopen and re-check every key + verify.
    let e = env(&dir);
    let cmp = Comparator::new("rle", |a: &[u8], b: &[u8]| {
        rle_to_str(a).cmp(&rle_to_str(b))
    });
    let cfg = DatabaseConfig::new()
        .with_allow_create(false)
        .with_transactional(true)
        .with_key_prefixing(true)
        .with_node_max_entries(FANOUT)
        .with_btree_comparator(cmp);
    let db = e.open_database(None, "rle", &cfg).unwrap();

    let mut missing = 0usize;
    for k in &keys {
        if !get_present(&db, k) {
            missing += 1;
        }
    }
    assert_eq!(
        missing,
        0,
        "AFTER REOPEN: {missing}/{} keys unretrievable (durable loss)",
        keys.len()
    );
    let vr = db.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(
        vr.error_count(),
        0,
        "AFTER REOPEN: verify found {} structural error(s): {:?}",
        vr.error_count(),
        vr.errors
    );
}

// ── CONTROL (a): prefixing OFF + same RLE comparator loses nothing. ─────────
// Guards that the fault is the interaction with prefixing (must pass on base
// AND fix).
#[test]
fn control_a_rle_comparator_prefixing_off_no_loss() {
    let dir = TempDir::new().unwrap();
    let keys = make_keys(KEY_COUNT);
    let e = env(&dir);
    let cmp = Comparator::new("rle", |a: &[u8], b: &[u8]| {
        rle_to_str(a).cmp(&rle_to_str(b))
    });
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_key_prefixing(false)
        .with_node_max_entries(FANOUT)
        .with_btree_comparator(cmp);
    let db = e.open_database(None, "rle_noprefix", &cfg).unwrap();
    for k in &keys {
        db.put(DatabaseEntry::from_bytes(k), DatabaseEntry::from_bytes(k))
            .unwrap();
    }
    let mut missing = 0usize;
    for k in &keys {
        if !get_present(&db, k) {
            missing += 1;
        }
    }
    assert_eq!(missing, 0, "control(a): prefixing OFF lost {missing} keys");
}

// ── CONTROL (b): prefixing ON + a byte-order-CONSISTENT comparator (reverse)
// loses nothing.  Guards that the fix does not regress the byte-order path
// (must pass on base AND fix).
#[test]
fn control_b_byte_consistent_comparator_prefixing_on_no_loss() {
    let dir = TempDir::new().unwrap();
    // Reverse comparator: b.cmp(a) — a TOTAL order that is byte-order-derived
    // (descending), i.e. suffix-byte order and slot order stay consistent.
    let mut raw: Vec<Vec<u8>> = (0..KEY_COUNT as u32)
        .map(|i| format!("record:{i:08}").into_bytes())
        .collect();
    raw.dedup();
    let e = env(&dir);
    let cmp = Comparator::new("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_key_prefixing(true)
        .with_node_max_entries(FANOUT)
        .with_btree_comparator(cmp);
    let db = e.open_database(None, "rev_prefix", &cfg).unwrap();
    for k in &raw {
        db.put(DatabaseEntry::from_bytes(k), DatabaseEntry::from_bytes(k))
            .unwrap();
    }
    let mut missing = 0usize;
    for k in &raw {
        if !get_present(&db, k) {
            missing += 1;
        }
    }
    assert_eq!(
        missing, 0,
        "control(b): byte-consistent comparator + prefixing lost {missing} keys"
    );
    // Cursor scan must be in reverse (descending) order and complete.
    let scan = cursor_keys(&db);
    assert_eq!(scan.len(), raw.len(), "control(b): cursor scan incomplete");
    let mut expect = scan.clone();
    expect.sort_by(|a, b| b.cmp(a));
    assert_eq!(scan, expect, "control(b): scan must be descending");
    let vr = db.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(
        vr.error_count(),
        0,
        "control(b): verify errors {:?}",
        vr.errors
    );
}
