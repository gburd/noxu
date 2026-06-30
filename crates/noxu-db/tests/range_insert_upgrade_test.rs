//! Regression test for the illegal `RangeInsert -> Write` lock-upgrade panic
//! (dynomite/dyniak bug report, 2026-06).
//!
//! When a single transaction inserts/writes keys that are adjacent in key
//! order, an insert of key A takes a `RangeInsert` next-key lock on A's
//! successor B's LSN (phantom prevention).  When the same transaction then
//! writes B (an existing key, locked by its real LSN), it would request `Write`
//! on the LSN it already holds as `RangeInsert` -> an illegal upgrade that
//! formerly `panic!`ed (and then poison-aborted the process in Drop).
//!
//! Fixed by releasing the txn's own `RangeInsert` on that LSN before the Write
//! (Txn::release_range_insert_for_write) so the Write is a fresh, legal grant.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use tempfile::TempDir;

fn setup() -> (TempDir, noxu_db::Environment, noxu_db::Database) {
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
            "adj",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (dir, env, db)
}

/// The minimal reproducer: one serializable txn inserts adjacent keys, then
/// overwrites them, so an insert's successor-key RangeInsert lock lands on a
/// key the SAME txn later writes.  Must NOT panic.
#[test]
fn adjacent_key_writes_in_one_txn_do_not_panic() {
    let (_dir, env, db) = setup();

    // Pre-populate a couple of committed keys so inserts have real successors
    // (the RangeInsert lands on an existing key's real LSN, not the EOF sentinel).
    {
        let txn = env.begin_transaction(None).unwrap();
        for i in 0u8..6 {
            let k = DatabaseEntry::from_vec(vec![b'0' + i]);
            let v = DatabaseEntry::from_bytes(b"seed");
            db.put_in(&txn, &k, &v).unwrap();
        }
        txn.commit().unwrap();
    }

    // The trigger: within ONE serializable txn, insert a NEW key whose
    // successor is an existing key, then overwrite that successor.  Inserting
    // the new key range-locks the successor's real LSN; overwriting the
    // successor then write-locks that same LSN (the (RangeInsert, Write)
    // collision).  Repeat across rounds, deleting the inserted keys each round
    // so the next round re-inserts them as new.
    for round in 0..50 {
        let txn = env.begin_transaction(None).unwrap(); // serializable by default
        // Insert new keys interleaved with the seeded ones: "0a" sorts between
        // "0" and "1", so its successor is the existing "1"; then overwrite "1".
        for i in 0u8..5 {
            let new_key = DatabaseEntry::from_vec(vec![b'0' + i, b'a']);
            db.put_in(
                &txn,
                &new_key,
                DatabaseEntry::from_vec(format!("n{round}").into_bytes()),
            )
            .expect("insert of interleaved new key must not panic");
            // Overwrite the successor (existing key i+1) — formerly the panic.
            let succ = DatabaseEntry::from_vec(vec![b'0' + i + 1]);
            db.put_in(
                &txn,
                &succ,
                DatabaseEntry::from_vec(format!("r{round}").into_bytes()),
            )
            .expect(
                "overwrite of the range-insert-locked successor must not panic",
            );
            // Remove the interleaved key so next round re-inserts it new.
            db.delete_in(&txn, &new_key).ok();
        }
        txn.commit().expect("commit must succeed");
    }

    // Sanity: the final values are present and readable.
    let txn = env.begin_transaction(None).unwrap();
    let mut out = DatabaseEntry::new();
    let k = DatabaseEntry::from_vec(vec![b'3']);
    let status = db.get_into(Some(&txn), &k, &mut out).unwrap();
    assert!(status);
    assert_eq!(out.data(), b"r49");
    txn.commit().unwrap();
}

/// Explicit form from the report: insert a NEW key A whose immediate successor
/// is an EXISTING key B, then OVERWRITE B in the same txn.  Inserting A
/// range-locks B's *real* LSN; overwriting B then write-locks that same real
/// LSN -> the (RangeInsert, Write) collision.  Must not panic.
#[test]
fn insert_then_insert_successor_same_txn() {
    let (_dir, env, db) = setup();

    // Seed B (and a tail key) as committed keys with real LSNs.
    {
        let txn = env.begin_transaction(None).unwrap();
        for k in [b"B".as_slice(), b"D".as_slice()] {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k),
                DatabaseEntry::from_bytes(b"seed"),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }

    let txn = env.begin_transaction(None).unwrap();
    // Insert A (new): its successor is the existing key B -> range-locks B's
    // real LSN.
    db.put_in(
        &txn,
        DatabaseEntry::from_bytes(b"A"),
        DatabaseEntry::from_bytes(b"va"),
    )
    .expect("insert A must not panic");
    // Overwrite B (existing): write-locks B's real LSN — the SAME LSN the txn
    // holds as RangeInsert.  Formerly an illegal (RangeInsert, Write) upgrade
    // that panicked + poison-aborted the process; now a fresh legal Write.
    db.put_in(
        &txn,
        DatabaseEntry::from_bytes(b"B"),
        DatabaseEntry::from_bytes(b"vb_new"),
    )
    .expect("overwrite of the range-insert-locked successor must not panic");
    txn.commit().expect("commit must succeed");

    // Verify B's new value committed.
    let txn = env.begin_transaction(None).unwrap();
    let mut out = DatabaseEntry::new();
    db.get_into(Some(&txn), DatabaseEntry::from_bytes(b"B"), &mut out).unwrap();
    assert_eq!(out.data(), b"vb_new");
    txn.commit().unwrap();
}

/// S2 sibling: insert NEW key A (range-locks existing successor B's real LSN),
/// then GET B in the SAME txn -> (RangeInsert, Read/RangeRead) illegal upgrade.
#[test]
fn s2_insert_then_get_successor_same_txn() {
    let (_dir, env, db) = setup();
    {
        let txn = env.begin_transaction(None).unwrap();
        for k in [b"B".as_slice(), b"D".as_slice()] {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k),
                DatabaseEntry::from_bytes(b"seed"),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
    let txn = env.begin_transaction(None).unwrap();
    db.put_in(
        &txn,
        DatabaseEntry::from_bytes(b"A"),
        DatabaseEntry::from_bytes(b"va"),
    )
    .expect("insert A");
    // GET B (the range-insert-locked successor) in the same txn:
    let mut out = DatabaseEntry::new();
    db.get_into(Some(&txn), DatabaseEntry::from_bytes(b"B"), &mut out)
        .expect("get of range-insert-locked successor must not panic/error");
    txn.commit().expect("commit");
}
