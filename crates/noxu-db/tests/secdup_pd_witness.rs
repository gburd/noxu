// R-SECREAD-NOTFOUND (Layer B) — deterministic witness for the sorted-dup
// (secondary-index) PENDING-deleted slot.
//
// JE model Noxu must preserve for the via-secondary read path:
//   * A transactional delete of a sorted-dup (sec_key, pri_key) slot marks it
//     PENDING-deleted (PD) under the deleter's WRITE lock; the slot is NOT
//     physically removed (IN.deleteRecord → setPendingDeleted, IN.java:3797).
//   * BIN.compress removes a deleted (KD or PD) slot ONLY when its LSN lock is
//     uncontended, i.e. the deleter has committed/aborted and released the
//     lock (BIN.java:1143-1172: "We have to be able to lock the LN before we
//     can compress the entry… locks protect the aborts").
//   * A reader seeking the sec_key therefore LANDS ON the PD slot and LOCKS it
//     (CursorImpl.lockAndGetCurrent skips only KNOWN-deleted,
//     CursorImpl.java:2060-2064), BLOCKING on the in-flight deleter instead of
//     momentarily seeing NOTFOUND (the via-secondary residual).  On commit it
//     sees the delete; on abort the slot is restored live.
//
// Before the Layer B fix, noxu-tree's dup-Set/BOTH seek
// (`first_entry_at_or_after_with_index`) skipped the PD slot with
// `slot_is_live` (PD-excluded), so the reader returned NOTFOUND without ever
// locking the deleter — the via-secondary hole.  After the fix the seek skips
// only `slot_scan_skippable` (KNOWN-deleted / expired), so the PD slot is
// landed on and routed through the lock-and-revalidate path.
//
// OBSERVABLE PROOF (public API): the deleter holds the (S,P) slot's WRITE lock
// uncommitted.  A concurrent LOCKING reader's SearchBoth for (S,P) must BLOCK
// on that lock (it finds the PD slot — it does NOT return NOTFOUND and does NOT
// return instantly).  That blocking is only possible if the slot is physically
// present and lockable, i.e. the compressor did NOT remove it while the lock
// was contended.  On commit the blocked reader then observes the delete
// (NOTFOUND); on abort it observes the restored record (SUCCESS).  A reader
// under a SHORT lock timeout gets a LockTimeout error (never NOTFOUND) — the
// same "blocked on the deleter" proof in a bounded form.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    NoxuError, OperationStatus,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn open_dup_env(
    dir: &TempDir,
    lock_timeout_ms: u64,
) -> (Arc<Environment>, Arc<noxu_db::Database>) {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_run_in_compressor(false) // compress only on explicit call
        .with_lock_timeout(lock_timeout_ms)
        .with_txn_timeout(60_000);
    let env = Arc::new(Environment::open(cfg).unwrap());
    let dbcfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    let db = Arc::new(env.open_database(None, "secDup", &dbcfg).unwrap());
    (env, db)
}

const SEC: &[u8] = b"S";
const PRI: &[u8] = b"P";

fn is_lock_timeout(e: &NoxuError) -> bool {
    let s = e.to_string().to_lowercase();
    s.contains("locktimeout") || (s.contains("lock") && s.contains("timeout"))
}

/// (a) A short-timeout LOCKING reader hits the write-locked PD slot and gets a
/// LockTimeout (NOT NOTFOUND) while the delete is in flight — proving the PD
/// slot is present, lockable, and NOT compressed while the lock is contended.
/// After COMMIT a fresh reader sees NOTFOUND and the compressor reclaims the
/// slot; the unrelated sibling dup survives.
#[test]
fn secdup_uncommitted_delete_is_pd_locked_and_not_compressed_until_commit() {
    let dir = TempDir::new().unwrap();
    // Short lock timeout so the contended reader fails fast with LockTimeout.
    let (env, db) = open_dup_env(&dir, 400);

    {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(&txn, SEC, PRI).unwrap();
        db.put_in(&txn, b"S", b"Q").unwrap(); // sibling dup, must survive
        txn.commit().unwrap();
    }

    // txn1 deletes (S,P) and HOLDS the write lock open.
    let txn1 = env.begin_transaction(None).unwrap();
    {
        let mut c = db.open_cursor_in(&txn1, None).unwrap();
        let mut k = DatabaseEntry::from_bytes(SEC);
        let mut d = DatabaseEntry::from_bytes(PRI);
        let st = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
        assert_eq!(st, OperationStatus::Success, "deleter must find (S,P)");
        c.delete().unwrap();
        c.close().unwrap();
    }

    // Run the compressor while the PD slot is write-locked: it must NOT remove
    // the slot (JE isLockUncontended).  We prove non-removal via the contended
    // read below.
    let _ = env.compress().unwrap(); // returns BINs visited, not slots removed

    // A concurrent LOCKING reader must BLOCK on the PD slot (not NOTFOUND); with
    // the short timeout it surfaces as a LockTimeout error.  A NOTFOUND or an
    // instant Success here would mean the reader skipped the PD slot without
    // locking (the R-SECREAD hole) or the compressor wrongly removed it.
    let t0 = Instant::now();
    let probe = {
        let txn = env.begin_transaction(None).unwrap();
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut k = DatabaseEntry::from_bytes(SEC);
        let mut d = DatabaseEntry::from_bytes(PRI);
        let r = c.get(&mut k, &mut d, Get::SearchBoth, None);
        let _ = c.close();
        let _ = txn.abort();
        r
    };
    let waited = t0.elapsed();
    match probe {
        Err(ref e) if is_lock_timeout(e) => { /* blocked on the deleter */ }
        other => panic!(
            "contended reader must BLOCK on the write-locked PD slot and \
             surface a LockTimeout, got {other:?} after {waited:?} — a \
             NOTFOUND means the PD slot was skipped without locking (the \
             via-secondary R-SECREAD hole) or wrongly compressed away"
        ),
    }
    assert!(
        waited >= Duration::from_millis(300),
        "the reader should have waited ~the lock timeout before failing, \
         waited {waited:?}"
    );

    // Commit the delete: the slot's lock becomes uncontended.
    txn1.commit().unwrap();

    // Now a fresh reader sees the committed delete.
    {
        let txn = env.begin_transaction(None).unwrap();
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut k = DatabaseEntry::from_bytes(SEC);
        let mut d = DatabaseEntry::from_bytes(PRI);
        let st = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
        c.close().unwrap();
        txn.commit().unwrap();
        assert_eq!(
            st,
            OperationStatus::NotFound,
            "after commit the (S,P) dup is gone → NOTFOUND"
        );
    }
    // The compressor can now reclaim the committed-deleted slot (uncontended);
    // the sibling (S,Q) survives.
    let _ = env.compress().unwrap();
    {
        let txn = env.begin_transaction(None).unwrap();
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut k = DatabaseEntry::from_bytes(b"S");
        let mut d = DatabaseEntry::from_bytes(b"Q");
        let st = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
        c.close().unwrap();
        txn.commit().unwrap();
        assert_eq!(st, OperationStatus::Success, "(S,Q) sibling must survive");
    }
}

/// (b) ABORT restores the dup slot: a reader that blocks on the in-flight
/// delete sees the (S,P) record LIVE again once the deleter ABORTS (a generous
/// lock timeout so the reader waits through the abort rather than timing out).
#[test]
fn secdup_aborted_delete_restores_the_slot_for_a_blocked_reader() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_dup_env(&dir, 30_000);
    {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(&txn, SEC, PRI).unwrap();
        txn.commit().unwrap();
    }

    let seq = Arc::new(AtomicU32::new(0));

    let deleter = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            let mut k = DatabaseEntry::from_bytes(SEC);
            let mut d = DatabaseEntry::from_bytes(PRI);
            c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
            c.delete().unwrap();
            c.close().unwrap();
            seq.store(1, Ordering::SeqCst);
            let start = Instant::now();
            while seq.load(Ordering::SeqCst) < 2 {
                if start.elapsed() > Duration::from_secs(10) {
                    panic!("reader did not reach the blocked state");
                }
                thread::sleep(Duration::from_millis(5));
            }
            thread::sleep(Duration::from_millis(300));
            txn.abort().unwrap(); // restore the slot
        })
    };

    let reader = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        thread::spawn(move || {
            while seq.load(Ordering::SeqCst) < 1 {
                thread::sleep(Duration::from_millis(2));
            }
            let txn = env.begin_transaction(None).unwrap();
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            let mut k = DatabaseEntry::from_bytes(SEC);
            let mut d = DatabaseEntry::from_bytes(PRI);
            seq.store(2, Ordering::SeqCst);
            let t0 = Instant::now();
            let st = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
            let waited = t0.elapsed();
            c.close().unwrap();
            txn.commit().unwrap();
            assert!(
                waited >= Duration::from_millis(150),
                "reader must BLOCK on the PD slot's write lock, waited {:?}",
                waited
            );
            assert_eq!(
                st,
                OperationStatus::Success,
                "after the deleter ABORTS, (S,P) is restored LIVE → the \
                 blocked reader must see it (not NOTFOUND)"
            );
        })
    };

    deleter.join().unwrap();
    reader.join().unwrap();
}
