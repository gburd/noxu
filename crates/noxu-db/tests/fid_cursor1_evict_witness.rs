// FID-CURSOR-1 eviction/re-fault witness: a BIN logged (by the evictor or
// checkpointer) while a slot is PENDING_DELETED, then dropped from memory and
// re-faulted from the log, MUST restore the slot as PENDING_DELETED — not
// KNOWN_DELETED.  Otherwise a concurrent scan would skip the re-faulted slot
// WITHOUT a lock, resurfacing the FID-CURSOR-1 dirty read via eviction.
//
// JE persists both deleted bits: `IN.writeToLog` writes
// `entryStates[i] & CLEAR_TRANSIENT_BITS` and `TRANSIENT_BITS` is the OFFHEAP
// bits only (EntryStates.java:36), so KNOWN_DELETED (0x01) and
// PENDING_DELETED (0x08) are both durable; `materialize` (IN.java) restores
// them.  Noxu's v3 slot "deleted" byte now encodes 0x01 (KD) / 0x08 (PD)
// (BinEntry::deleted_state_byte / decode_deleted_state_byte).
//
// Two cases:
//   * evict_refault: txn A deletes F (uncommitted, lock held); force a
//     checkpoint + evict_memory so F's BIN is logged and dropped; txn B then
//     scans from C with getNext — it MUST BLOCK on F's write lock (because the
//     re-faulted slot is still PD), and because A ABORTS, B returns F.
//   * crash_recovery: txn A deletes F (uncommitted) and the env is dropped
//     (crash) with A still open; after reopen+recovery the uncommitted delete
//     is undone, so F is present again.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Arc<Environment> {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_node_max_entries(6); // two BINs for 7 keys
    Arc::new(Environment::open(cfg).unwrap())
}

fn seed(env: &Environment) -> Arc<noxu_db::Database> {
    let dbcfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(false);
    let db = Arc::new(env.open_database(None, "fidDB", &dbcfg).unwrap());
    let txn = env.begin_transaction(None).unwrap();
    for k in [&b"A"[..], b"B", b"C", b"F", b"G", b"H", b"I"] {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k),
            DatabaseEntry::from_bytes(&[0u8; 10]),
        )
        .unwrap();
    }
    txn.commit().unwrap();
    db
}

/// A BIN whose slot is PENDING-deleted (uncommitted), logged and evicted, then
/// re-faulted, must keep the slot PENDING-deleted: a concurrent scan BLOCKS on
/// the deleter and sees the restored record on abort — it does NOT skip.
#[test]
fn fid_cursor1_evict_refault_keeps_pending_deleted() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = seed(&env);

    let seq = Arc::new(AtomicU32::new(0));

    // t1: delete F (uncommitted), close the cursor (so the BIN is unpinned and
    // evictable) but KEEP the txn open (write lock on F's DeleteLN held), then
    // ABORT after t2 has reached getNext.
    let t1 = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            let s =
                db.delete_in(&txn, DatabaseEntry::from_bytes(b"F")).unwrap();
            assert!(s, "t1 delete F");
            seq.store(1, Ordering::SeqCst); // delete done, lock held
            while seq.load(Ordering::SeqCst) < 2 {
                thread::yield_now();
            }
            thread::sleep(Duration::from_millis(400));
            txn.abort().unwrap(); // restore F
            seq.store(3, Ordering::SeqCst);
        })
    };

    // Main thread drives the eviction while t1 holds the uncommitted delete.
    while seq.load(Ordering::SeqCst) < 1 {
        thread::yield_now();
    }
    // Force the dirty BIN (carrying the PENDING-deleted F slot) to the log,
    // then drop it from the in-memory cache.  On re-fault the slot must come
    // back PENDING-deleted, not KNOWN-deleted.
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();
    let _ = env.evict_memory().unwrap();

    // t2: position on C, then getNext under a long lock timeout.  The slot F
    // is now a re-faulted PENDING-deleted slot; a correct engine LOCKS it
    // (blocking on t1) rather than skipping it.
    let t2 = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            txn.set_lock_timeout(30_000);
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            let mut k = DatabaseEntry::from_bytes(b"C");
            let mut d = DatabaseEntry::new();
            let s = c.get(&mut k, &mut d, Get::Search, None).unwrap();
            assert_eq!(s, OperationStatus::Success, "t2 position on C");
            seq.store(2, Ordering::SeqCst);
            let start = Instant::now();
            let mut nk = DatabaseEntry::new();
            let mut nd = DatabaseEntry::new();
            let s = c.get(&mut nk, &mut nd, Get::Next, None).unwrap();
            let waited = start.elapsed();
            assert_eq!(
                s,
                OperationStatus::Success,
                "getNext must find a record"
            );
            let got = nk.data_opt().unwrap().to_vec();
            assert_eq!(
                got,
                b"F",
                "FID-cursor-1 (evict/re-fault) RED: getNext returned {:?} \
                 (expected F).  The re-faulted PENDING-deleted slot F was \
                 skipped without blocking — PD was lost to KD on re-fault, \
                 resurfacing the dirty read via eviction.  waited={:?}",
                String::from_utf8_lossy(&got),
                waited
            );
            assert!(
                waited >= Duration::from_millis(300),
                "FID-cursor-1 (evict/re-fault) RED: getNext did not block on \
                 F's write lock (waited only {:?}); the re-faulted slot was \
                 treated as KNOWN-deleted and skipped.",
                waited
            );
            drop(c);
            txn.commit().unwrap();
        })
    };

    t1.join().unwrap();
    t2.join().unwrap();
}

/// Crash variant: an uncommitted delete that is durable in the log (checkpoint)
/// at the moment of a crash must be UNDONE by recovery — the record is present
/// again after reopen.  (The on-disk slot read back as PENDING/KNOWN-deleted is
/// reverted by the undo chain because the deleting txn never committed.)
///
/// A crash is simulated by copying the DB files (minus the lock file) WHILE the
/// delete is uncommitted and a checkpoint has flushed the dirty BIN, then
/// recovering from that copy — the same technique the dup_txn_undo crash tests
/// use.
#[test]
fn fid_cursor1_crash_recovery_undoes_uncommitted_delete() {
    let dir = TempDir::new().unwrap();
    let img = {
        let env = open_env(&dir);
        let db = seed(&env);
        // Uncommitted delete of F, made durable by a forced checkpoint.
        let txn = env.begin_transaction(None).unwrap();
        let s = db.delete_in(&txn, DatabaseEntry::from_bytes(b"F")).unwrap();
        assert!(s, "delete F");
        env.checkpoint(Some(
            &noxu_db::CheckpointConfig::new().with_force(true),
        ))
        .unwrap();
        // Snapshot the crash state (uncommitted delete durable in the log)
        // BEFORE resolving the txn.
        let img = snapshot_crash_image(dir.path());
        // Clean up the original cleanly (does not affect the snapshot).
        txn.abort().unwrap();
        db.close().unwrap();
        env.close().unwrap();
        img
    };

    // Reopen + recover from the crash image.
    let cfg = EnvironmentConfig::new(img.path().to_path_buf())
        .with_allow_create(false)
        .with_transactional(true)
        .with_node_max_entries(6);
    let env = Arc::new(Environment::open(cfg).unwrap());
    let dbcfg = DatabaseConfig::new()
        .with_transactional(true)
        .with_sorted_duplicates(false);
    let db = env.open_database(None, "fidDB", &dbcfg).unwrap();
    let txn = env.begin_transaction(None).unwrap();
    let mut k = DatabaseEntry::from_bytes(b"F");
    let mut d = DatabaseEntry::new();
    let s = {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        c.get(&mut k, &mut d, Get::Search, None).unwrap()
    };
    assert_eq!(
        s,
        OperationStatus::Success,
        "FID-cursor-1 (crash) RED: F must be present after recovery undoes the \
         uncommitted delete that was durable in the log at crash time"
    );
    txn.commit().unwrap();
}

/// Copy the DB files (excluding the lock file) to a fresh dir, capturing the
/// current on-disk state as a "crash" image.
fn snapshot_crash_image(dir: &std::path::Path) -> TempDir {
    let img = TempDir::new().unwrap();
    for f in std::fs::read_dir(dir).unwrap() {
        let f = f.unwrap();
        if f.file_type().unwrap().is_file() && f.file_name() != "noxu.lck" {
            std::fs::copy(f.path(), img.path().join(f.file_name())).unwrap();
        }
    }
    img
}
