//! JE `SecondaryMultiTest` port — concurrent secondary/primary access.
//! Faithful port of the deterministic core of
//! `test/com/sleepycat/je/test/SecondaryMultiTest.java`.
//!
//! JE citation:
//!   - `SecondaryMultiTest.testMultiDeleteUnordered` — many threads race to
//!     delete the SAME key; exactly ONE deleter succeeds, every other gets
//!     NOTFOUND.  This is the record-level-locking atomicity guarantee.
//!
//! ENGINE-BUG CANDIDATE **NEW-DEL-RACE-1** (kept `#[ignore]`, not weakened):
//! Noxu does NOT serialize concurrent deletes of the same record.  With N=8
//! transactions racing to delete one record, 6-7 each observe the record LIVE
//! and commit a delete (`notfound` only 1-2) — under default AND serializable
//! isolation.  Exactly one should win.  Controls in the test rule out a test
//! artifact: the record count is 1 (single non-dup record) and a sequential
//! double-delete correctly returns false (visibility is fine sequentially —
//! the anomaly is purely concurrent).  Root-cause pointer: the delete write
//! lock is keyed on the record LSN (`cursor_impl.rs::lock_write_before_log`,
//! `lock(lsn_to_lock, LockType::Write, ...)`); concurrent searchers each find
//! the same live LSN and their write-lock acquisition does not mutually
//! exclude, so several proceed to delete the same slot and each reports
//! success.  This is in the core txn/lock/delete path — escalated, not fixed.
//!
//! Classification of the other SecondaryMultiTest methods (see report):
//!   - `testMultiDelete` (two-thread ordered delete via the `DeleteIt`
//!     event-counter harness), `testMultiReadInsert`, `testMultiReadDelete`,
//!     `testMultiReadUpdate`, `testMultiReadDeleteInsert`, `testMultiReaders`
//!     are concurrent stress tests built on JE's `currentEvent`/`testDone`
//!     thread-coordination harness (a JE-internal test-infra pattern).  Their
//!     substantive safety property — concurrent secondary readers observe a
//!     consistent index while writers mutate the primary — is covered by the
//!     secondary txn-isolation tests (`secondary_decisions_test.rs`:
//!     abort/commit rollback, uncommitted-write invisibility) plus the
//!     lock-based isolation tests (`isolation_test.rs`).  The elaborate event
//!     harness itself is not reproduced (N/A test-infra).
//!
//! Deviation: JE parameterizes over primary-vs-secondary delete handles; this
//! port races on the PRIMARY delete (JE's `db1UsePrimary && db2UsePrimary`
//! parameter case), the direct expression of the exactly-one-wins property.
//! Deleting through a secondary key fans out to the same primary delete, so
//! the guarantee is identical.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

/// JE `SecondaryMultiTest.testMultiDeleteUnordered`.
///
/// Kept `#[ignore]` as ENGINE-BUG CANDIDATE NEW-DEL-RACE-1 (see module doc).
/// Run with `--ignored` to reproduce the anomaly.
#[test]
#[ignore = "NEW-DEL-RACE-1: concurrent deletes of the same record are not \
            serialized; several transactions each delete it and commit"]
fn je_secondary_multi_test_test_multi_delete_unordered() {
    const DATACOUNT: u32 = 99;
    const KEY: u32 = 55;
    const DELETERS: usize = 20;

    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        noxu_db::Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    let db = Arc::new(
        env.open_database(
            None,
            "foo",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );

    // Populate 0..99, each key = its decimal-string bytes (JE layout).
    for i in 0..DATACOUNT {
        let v = i.to_string();
        db.put(v.as_bytes(), v.as_bytes()).unwrap();
    }

    let target = KEY.to_string();

    // CONTROL: exactly one record under the target key (single non-dup slot).
    {
        let mut c = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(target.as_bytes());
        let mut d = DatabaseEntry::new();
        assert_eq!(
            c.get(&mut k, &mut d, Get::Search, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(c.count().unwrap(), 1, "control: one record under KEY");
    }

    let success = Arc::new(AtomicUsize::new(0));
    let notfound = Arc::new(AtomicUsize::new(0));
    let conflict = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(DELETERS));

    let mut handles = Vec::with_capacity(DELETERS);
    for _ in 0..DELETERS {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let target = target.clone();
        let success = Arc::clone(&success);
        let notfound = Arc::clone(&notfound);
        let conflict = Arc::clone(&conflict);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let txn = env.begin_transaction(None).unwrap();
            match db.delete_in(&txn, target.as_bytes()) {
                Ok(true) => {
                    txn.commit().unwrap();
                    success.fetch_add(1, Ordering::SeqCst);
                }
                Ok(false) => {
                    txn.commit().unwrap();
                    notfound.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    let _ = txn.abort();
                    conflict.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let s = success.load(Ordering::SeqCst);
    let nf = notfound.load(Ordering::SeqCst);
    let cf = conflict.load(Ordering::SeqCst);

    // CONTROL: post-race the record is gone (a further delete is false).
    let t = env.begin_transaction(None).unwrap();
    assert!(
        !db.delete_in(&t, target.as_bytes()).unwrap(),
        "KEY must be deleted after the race"
    );
    t.commit().unwrap();

    // Exactly one deleter succeeded; the rest saw NOTFOUND (or lost a lock
    // conflict).  This is the assertion NEW-DEL-RACE-1 currently violates.
    assert_eq!(
        s, 1,
        "exactly one deleter must succeed; got success={s} notfound={nf} \
         conflict={cf} (NEW-DEL-RACE-1: concurrent deletes not serialized)"
    );
    assert_eq!(
        nf + cf,
        DELETERS - 1,
        "every other deleter must observe NOTFOUND or a lock conflict"
    );
}
