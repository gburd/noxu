// FID-cursor-1 witness: a REPEATABLE_READ / default-isolation cursor scan must
// NOT dirty-read a concurrent *uncommitted* delete by silently skipping the
// deleted slot.
//
// JE model (the invariant Noxu must preserve):
//   * IN.deleteRecord sets PENDING_DELETED (NOT known-deleted) for an in-flight
//     txn delete (IN.java:3765-3798); known-deleted means "committed deletion"
//     (IN.isEntryKnownDeleted javadoc).
//   * CursorImpl.lockAndGetCurrent only skips WITHOUT locking when
//     isEntryKnownDeleted(index) (CursorImpl.java:2060-2064). A pending-deleted
//     (but not known-deleted) slot is NOT skipped: it falls through to
//     lockLNAndCheckDefunct, which locks the LSN and BLOCKS on the uncommitted
//     deleter until it commits/aborts.
//
// Noxu bug (A5 / mark_deleted): a txn delete sets the single `known_deleted`
// bit at OPERATION time (pre-commit); the scan's slot_is_live() skip then treats
// the uncommitted delete as committed and SKIPS it with no lock, no block.
//
// Expectation on a correct engine, run under a 30s lock timeout:
//   t1 deletes F (uncommitted), holds F's write lock.
//   t2 (own txn) positions on C, then getNext.  getNext must BLOCK on F's
//   write lock until t1 resolves.  t1 ABORTS -> F is restored -> t2's getNext
//   must return F.
//
// On the RC this is RED: t2's getNext returns G in ~0ms (skipped F, never
// blocked), so when t1 aborts t2 has already returned the wrong record.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn fid_cursor1_scan_blocks_on_uncommitted_delete_then_sees_it_on_abort() {
    let dir = TempDir::new().unwrap();
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_node_max_entries(6); // two BINs for 7 keys
    let env = Arc::new(Environment::open(cfg).unwrap());
    let dbcfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(false);
    let db = Arc::new(env.open_database(None, "fidDB", &dbcfg).unwrap());

    // Seed A,B,C,F,G,H,I.
    {
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
    }

    let seq = Arc::new(AtomicU32::new(0));

    // t1: delete F (uncommitted), hold the write lock, then ABORT.
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
            // Wait until t2 has reached getNext.
            while seq.load(Ordering::SeqCst) < 2 {
                thread::yield_now();
            }
            // Hold the lock long enough that any non-blocking skip is exposed.
            thread::sleep(Duration::from_millis(400));
            txn.abort().unwrap(); // restore F
            seq.store(3, Ordering::SeqCst);
        })
    };

    // t2: position on C, then getNext under a long lock timeout.
    let t2 = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            txn.set_lock_timeout(30_000);
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            // Wait for t1's uncommitted delete + held lock.
            while seq.load(Ordering::SeqCst) < 1 {
                thread::yield_now();
            }
            let mut k = DatabaseEntry::from_bytes(b"C");
            let mut d = DatabaseEntry::new();
            let s = c.get(&mut k, &mut d, Get::Search, None).unwrap();
            assert_eq!(s, OperationStatus::Success, "t2 position on C");
            seq.store(2, Ordering::SeqCst); // positioned; release t1's sleep
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

            // A correct (JE) engine BLOCKS on F's uncommitted-delete write lock
            // (so `waited` is >= ~400ms, t1's hold) and, because t1 ABORTED,
            // returns F.  The RC skips F without blocking (waited ~0ms) and
            // returns G -- a dirty read of the uncommitted delete.
            assert_eq!(
                got,
                b"F",
                "FID-cursor-1 RED: getNext returned {:?} (expected F). The scan \
                 skipped the uncommitted-deleted slot F without blocking and \
                 dirty-read t1's uncommitted delete; on abort F is restored and \
                 must be visible.  waited={:?}",
                String::from_utf8_lossy(&got),
                waited
            );
            // Secondary evidence: a correct engine must have blocked.
            assert!(
                waited >= Duration::from_millis(300),
                "FID-cursor-1 RED: getNext did not block on F's write lock \
                 (waited only {:?}); it skipped the uncommitted-deleted slot.",
                waited
            );
            drop(c);
            txn.commit().unwrap();
        })
    };

    t1.join().unwrap();
    t2.join().unwrap();
}
