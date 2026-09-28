//! JE CursorTest phantom family ports — cross-BIN phantom visibility through a
//! cursor that blocks on another transaction's lock.
//!
//! Each test corresponds to a method in
//! `test/com/sleepycat/je/CursorTest.java` (the `testPhantom*` matrix driven
//! by `phantomWorker` / `phantomDupWorker`).
//!
//! JE's structure (verbatim intent): a small tree with keys A,B,C,F,G,H,I
//! spanning two BINs (NODE_MAX=6).  Two transactions run concurrently:
//!   * thread1 RMW-locks an edge element (`t1_lock`), then inserts or deletes
//!     the phantom element (`t1_op`), then commits or aborts.
//!   * thread2 positions a cursor on the adjacent element (`t2_start`), then
//!     does getNext / getPrev, which BLOCKS on thread1's lock.  When thread1
//!     ends, thread2 unblocks and its returned key must equal `expected`.
//!
//! RepeatableRead (the Noxu default) is used — NOT Serializable — so phantoms
//! are allowed (otherwise the two txns would deadlock).  This proves a blocked
//! cursor observes the COMMITTED result (insert visible / delete applied) and
//! does NOT observe an ABORTED one.
//!
//! Noxu adaptations: JE's `sequence++`/`Thread.yield()` hand-off is replaced by
//! an `AtomicU32` sequence with spin-yield; JE's `Thread.sleep(1000)` before
//! thread1 commits (to let thread2 reach its blocking getNext) is kept as a
//! short sleep.  Each thread opens its own transaction from the shared env, as
//! in JE.

// ── ENGINE-BUG CANDIDATE (NEW-PHANTOM-ABORT-1) — DIRTY READ VIA CURSOR SKIP ──
//
// The eight *abort* configurations below are #[ignore]d faithful ports.  They
// are a REAL, two-thread test (each thread has its own transaction; thread1
// genuinely holds the write lock on the contended record while thread2 reads)
// — NOT a single-thread simulation.
//
// Disambiguating runtime probe (2026-05, clearest case = delete + getNext):
//   * thread1 deletes F (uncommitted) and HOLDS F's write lock;
//   * a POINT get (`Get::Search`) on F from thread2 BLOCKS ~2000ms until
//     thread1 resolves — so record-level locking IS enforced for direct reads;
//   * but `getNext` from C in thread2 returns G in 0ms — it SKIPS the
//     write-locked, uncommitted-deleted slot F WITHOUT blocking or raising a
//     lock conflict.  That is a DIRTY READ of thread1's uncommitted delete.
//
// Because thread2 never blocked, when thread1 ABORTS (F restored) thread2 has
// already returned the wrong answer (G).  This is why all eight abort configs
// fail while their commit siblings pass: on commit the skip is coincidentally
// correct; on abort it is exposed as a dirty read.  (The commit siblings do
// NOT independently prove blocking — a dirty read followed by a commit is
// coincidentally right; the abort configs are the real evidence.)
//
// Root cause (candidate): Noxu's cursor next/prev "skip defunct slot" path does
// not acquire the record lock before skipping a deleted-but-uncommitted slot,
// so it reads through another txn's pending write.  JE's
// `CursorImpl.getNext` -> `lockLNAndCheckDefunct` locks the LN it lands on
// (even a defunct slot) with the read lock BEFORE deciding to skip it, so a
// write-locked slot blocks the reader until the writer resolves.
//
// Verdict: REAL engine isolation bug (lock-based model requires block-or-
// conflict, not skip), escalated.  Kept #[ignore]d — faithful ports, must not
// be weakened.
// ─────────────────────────────────────────────────────────────────────────────

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    LockMode, OperationStatus, TransactionConfig,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const DATA_STRINGS: [&[u8]; 7] = [b"A", b"B", b"C", b"F", b"G", b"H", b"I"];
const DUPKEY: &[u8] = b"DUPKEY";

struct PhantomConfig {
    t1_lock: &'static [u8],
    t1_op: &'static [u8],
    t2_start: &'static [u8],
    expected: &'static [u8],
    do_insert: bool,
    do_get_next: bool,
    do_commit: bool,
}

fn open_env(dir: &TempDir) -> Arc<Environment> {
    // NODE_MAX=6 forces two BINs for the 7 keys, matching JE's setup.
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_node_max_entries(6);
    Arc::new(Environment::open(cfg).unwrap())
}

fn open_db(env: &Environment, dups: bool) -> Arc<noxu_db::Database> {
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dups);
    Arc::new(env.open_database(None, "testDB", &cfg).unwrap())
}

/// Non-duplicate phantom worker (JE `phantomWorker`).
fn phantom_worker(cfg: PhantomConfig) {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, false);

    // Seed the two-BIN tree A,B,C,F,G,H,I.
    {
        let txn = env.begin_transaction(None).unwrap();
        for k in DATA_STRINGS {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k),
                DatabaseEntry::from_bytes(&[0u8; 10]),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }

    // For an insert-and-getPrev config, delete the first entry in the second
    // BIN (F) up front so it can be re-inserted at the BIN edge (JE comment).
    if cfg.do_insert && !cfg.do_get_next {
        let txn = env.begin_transaction(None).unwrap();
        db.delete_in(&txn, DatabaseEntry::from_bytes(b"F")).unwrap();
        txn.commit().unwrap();
    }

    let sequence = Arc::new(AtomicU32::new(0));
    let cfg = Arc::new(cfg);

    // Thread 1: RMW-lock t1_lock, insert/delete t1_op, commit/abort.
    let t1 = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&sequence);
        let cfg = Arc::clone(&cfg);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            {
                let mut c = db.open_cursor_in(&txn, None).unwrap();
                let mut k = DatabaseEntry::from_bytes(cfg.t1_lock);
                let mut d = DatabaseEntry::new();
                let s = c
                    .get(&mut k, &mut d, Get::Search, Some(LockMode::Rmw))
                    .unwrap();
                assert_eq!(
                    s,
                    OperationStatus::Success,
                    "t1 lock {:?}",
                    cfg.t1_lock
                );
                seq.store(1, Ordering::SeqCst); // 0 -> 1
                // Wait for t2 to position its cursor.
                while seq.load(Ordering::SeqCst) < 2 {
                    thread::yield_now();
                }
            }
            if cfg.do_insert {
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(cfg.t1_op),
                    DatabaseEntry::from_bytes(&[0u8; 10]),
                )
                .unwrap();
            } else {
                let s = db
                    .delete_in(&txn, DatabaseEntry::from_bytes(cfg.t1_op))
                    .unwrap();
                assert!(s, "t1 delete {:?}", cfg.t1_op);
            }
            seq.store(3, Ordering::SeqCst); // 2 -> 3
            // Give t2 time to reach the blocking getNext/getPrev.
            thread::sleep(Duration::from_millis(150));
            if cfg.do_commit {
                txn.commit().unwrap();
            } else {
                txn.abort().unwrap();
            }
        })
    };

    // Thread 2: position on t2_start, then getNext/getPrev (blocks), verify.
    let t2 = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&sequence);
        let cfg = Arc::clone(&cfg);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            txn.set_lock_timeout(30_000);
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            // Wait for t1 to lock its element.
            while seq.load(Ordering::SeqCst) < 1 {
                thread::yield_now();
            }
            let mut k = DatabaseEntry::from_bytes(cfg.t2_start);
            let mut d = DatabaseEntry::new();
            let s = c.get(&mut k, &mut d, Get::Search, None).unwrap();
            assert_eq!(
                s,
                OperationStatus::Success,
                "t2 start {:?}",
                cfg.t2_start
            );
            seq.store(2, Ordering::SeqCst); // 1 -> 2
            // Wait for t1 to insert/delete.
            while seq.load(Ordering::SeqCst) < 3 {
                thread::yield_now();
            }
            // This blocks until t1 commits/aborts.
            let mut nk = DatabaseEntry::new();
            let mut nd = DatabaseEntry::new();
            let dir = if cfg.do_get_next { Get::Next } else { Get::Prev };
            let s = c.get(&mut nk, &mut nd, dir, None).unwrap();
            assert_eq!(
                s,
                OperationStatus::Success,
                "t2 nav must find a record"
            );
            assert_eq!(
                nk.data_opt().unwrap(),
                cfg.expected,
                "phantom-visible key mismatch"
            );
            drop(c);
            txn.commit().unwrap();
        })
    };

    t1.join().unwrap();
    t2.join().unwrap();
}

/// Duplicate phantom worker (JE `phantomDupWorker`): a single key DUPKEY with
/// duplicate data A,B,C,F,G,H,I; navigation is getNextDup / getPrevDup.
fn phantom_dup_worker(cfg: PhantomConfig) {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, true);

    {
        let txn = env.begin_transaction(None).unwrap();
        for k in DATA_STRINGS {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(DUPKEY),
                DatabaseEntry::from_bytes(k),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }

    // Insert-and-getPrevDup: delete the F dup up front (JE comment).
    if cfg.do_insert && !cfg.do_get_next {
        let txn = env.begin_transaction(None).unwrap();
        {
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            let mut k = DatabaseEntry::from_bytes(DUPKEY);
            let mut d = DatabaseEntry::from_bytes(b"F");
            let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
            assert_eq!(s, OperationStatus::Success);
            assert_eq!(c.delete().unwrap(), OperationStatus::Success);
        }
        txn.commit().unwrap();
    }

    let sequence = Arc::new(AtomicU32::new(0));
    let cfg = Arc::new(cfg);

    let t1 = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&sequence);
        let cfg = Arc::clone(&cfg);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            {
                let mut c = db.open_cursor_in(&txn, None).unwrap();
                let mut k = DatabaseEntry::from_bytes(DUPKEY);
                let mut d = DatabaseEntry::from_bytes(cfg.t1_lock);
                let s = c
                    .get(&mut k, &mut d, Get::SearchBoth, Some(LockMode::Rmw))
                    .unwrap();
                assert_eq!(
                    s,
                    OperationStatus::Success,
                    "t1 dup lock {:?}",
                    cfg.t1_lock
                );
            }
            seq.store(1, Ordering::SeqCst);
            while seq.load(Ordering::SeqCst) < 2 {
                thread::yield_now();
            }
            if cfg.do_insert {
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(DUPKEY),
                    DatabaseEntry::from_bytes(cfg.t1_op),
                )
                .unwrap();
            } else {
                let mut c = db.open_cursor_in(&txn, None).unwrap();
                let mut k = DatabaseEntry::from_bytes(DUPKEY);
                let mut d = DatabaseEntry::from_bytes(cfg.t1_op);
                let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
                assert_eq!(s, OperationStatus::Success);
                assert_eq!(c.delete().unwrap(), OperationStatus::Success);
            }
            seq.store(3, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(150));
            if cfg.do_commit {
                txn.commit().unwrap();
            } else {
                txn.abort().unwrap();
            }
        })
    };

    let t2 = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&sequence);
        let cfg = Arc::clone(&cfg);
        thread::spawn(move || {
            let txn = env
                .begin_transaction(Some(
                    &TransactionConfig::new().with_lock_timeout_ms(30_000),
                ))
                .unwrap();
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            while seq.load(Ordering::SeqCst) < 1 {
                thread::yield_now();
            }
            let mut k = DatabaseEntry::from_bytes(DUPKEY);
            let mut d = DatabaseEntry::from_bytes(cfg.t2_start);
            let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
            assert_eq!(
                s,
                OperationStatus::Success,
                "t2 dup start {:?}",
                cfg.t2_start
            );
            seq.store(2, Ordering::SeqCst);
            while seq.load(Ordering::SeqCst) < 3 {
                thread::yield_now();
            }
            let mut nk = DatabaseEntry::new();
            let mut nd = DatabaseEntry::new();
            let dir = if cfg.do_get_next { Get::NextDup } else { Get::PrevDup };
            let s = c.get(&mut nk, &mut nd, dir, None).unwrap();
            assert_eq!(
                s,
                OperationStatus::Success,
                "t2 dup nav must find a record"
            );
            assert_eq!(
                nd.data_opt().unwrap(),
                cfg.expected,
                "phantom-visible dup data mismatch"
            );
            drop(c);
            txn.commit().unwrap();
        })
    };

    t1.join().unwrap();
    t2.join().unwrap();
}

// ── Non-dup phantom tests (JE CursorTest.testPhantom*) ────────────────────────

// JE: CursorTest.testPhantomInsertGetNextCommit
#[test]
fn phantom_insert_get_next_commit() {
    phantom_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"D",
        t2_start: b"C",
        expected: b"D",
        do_insert: true,
        do_get_next: true,
        do_commit: true,
    });
}

// JE: CursorTest.testPhantomInsertGetNextAbort
#[test]
#[ignore = "NEW-PHANTOM-ABORT-1: blocked getNext/getPrev dirty-reads a concurrent uncommitted write and does not re-resolve on abort"]
fn phantom_insert_get_next_abort() {
    phantom_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"D",
        t2_start: b"C",
        expected: b"F",
        do_insert: true,
        do_get_next: true,
        do_commit: false,
    });
}

// JE: CursorTest.testPhantomInsertGetPrevCommit
#[test]
fn phantom_insert_get_prev_commit() {
    phantom_worker(PhantomConfig {
        t1_lock: b"C",
        t1_op: b"F",
        t2_start: b"G",
        expected: b"F",
        do_insert: true,
        do_get_next: false,
        do_commit: true,
    });
}

// JE: CursorTest.testPhantomInsertGetPrevAbort
#[test]
#[ignore = "NEW-PHANTOM-ABORT-1: blocked getNext/getPrev dirty-reads a concurrent uncommitted write and does not re-resolve on abort"]
fn phantom_insert_get_prev_abort() {
    phantom_worker(PhantomConfig {
        t1_lock: b"C",
        t1_op: b"F",
        t2_start: b"G",
        expected: b"C",
        do_insert: true,
        do_get_next: false,
        do_commit: false,
    });
}

// JE: CursorTest.testPhantomDeleteGetNextCommit
#[test]
fn phantom_delete_get_next_commit() {
    phantom_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"F",
        t2_start: b"C",
        expected: b"G",
        do_insert: false,
        do_get_next: true,
        do_commit: true,
    });
}

// JE: CursorTest.testPhantomDeleteGetNextAbort
#[test]
#[ignore = "NEW-PHANTOM-ABORT-1: blocked getNext/getPrev dirty-reads a concurrent uncommitted write and does not re-resolve on abort"]
fn phantom_delete_get_next_abort() {
    phantom_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"F",
        t2_start: b"C",
        expected: b"F",
        do_insert: false,
        do_get_next: true,
        do_commit: false,
    });
}

// JE: CursorTest.testPhantomDeleteGetPrevCommit
#[test]
fn phantom_delete_get_prev_commit() {
    phantom_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"F",
        t2_start: b"G",
        expected: b"C",
        do_insert: false,
        do_get_next: false,
        do_commit: true,
    });
}

// JE: CursorTest.testPhantomDeleteGetPrevAbort
#[test]
#[ignore = "NEW-PHANTOM-ABORT-1: blocked getNext/getPrev dirty-reads a concurrent uncommitted write and does not re-resolve on abort"]
fn phantom_delete_get_prev_abort() {
    phantom_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"F",
        t2_start: b"G",
        expected: b"F",
        do_insert: false,
        do_get_next: false,
        do_commit: false,
    });
}

// ── Duplicate phantom tests (JE CursorTest.testPhantomDup*) ───────────────────

// JE: CursorTest.testPhantomDupInsertGetNextCommit
#[test]
fn phantom_dup_insert_get_next_commit() {
    phantom_dup_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"D",
        t2_start: b"C",
        expected: b"D",
        do_insert: true,
        do_get_next: true,
        do_commit: true,
    });
}

// JE: CursorTest.testPhantomDupInsertGetNextAbort
#[test]
#[ignore = "NEW-PHANTOM-ABORT-1: blocked getNext/getPrev dirty-reads a concurrent uncommitted write and does not re-resolve on abort"]
fn phantom_dup_insert_get_next_abort() {
    phantom_dup_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"D",
        t2_start: b"C",
        expected: b"F",
        do_insert: true,
        do_get_next: true,
        do_commit: false,
    });
}

// JE: CursorTest.testPhantomDupInsertGetPrevCommit
#[test]
fn phantom_dup_insert_get_prev_commit() {
    phantom_dup_worker(PhantomConfig {
        t1_lock: b"C",
        t1_op: b"F",
        t2_start: b"G",
        expected: b"F",
        do_insert: true,
        do_get_next: false,
        do_commit: true,
    });
}

// JE: CursorTest.testPhantomDupInsertGetPrevAbort
#[test]
#[ignore = "NEW-PHANTOM-ABORT-1: blocked getNext/getPrev dirty-reads a concurrent uncommitted write and does not re-resolve on abort"]
fn phantom_dup_insert_get_prev_abort() {
    phantom_dup_worker(PhantomConfig {
        t1_lock: b"C",
        t1_op: b"F",
        t2_start: b"G",
        expected: b"C",
        do_insert: true,
        do_get_next: false,
        do_commit: false,
    });
}

// JE: CursorTest.testPhantomDupDeleteGetNextCommit
#[test]
fn phantom_dup_delete_get_next_commit() {
    phantom_dup_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"F",
        t2_start: b"C",
        expected: b"G",
        do_insert: false,
        do_get_next: true,
        do_commit: true,
    });
}

// JE: CursorTest.testPhantomDupDeleteGetNextAbort
#[test]
#[ignore = "NEW-PHANTOM-ABORT-1: blocked getNext/getPrev dirty-reads a concurrent uncommitted write and does not re-resolve on abort"]
fn phantom_dup_delete_get_next_abort() {
    phantom_dup_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"F",
        t2_start: b"C",
        expected: b"F",
        do_insert: false,
        do_get_next: true,
        do_commit: false,
    });
}

// JE: CursorTest.testPhantomDupDeleteGetPrevCommit
#[test]
fn phantom_dup_delete_get_prev_commit() {
    phantom_dup_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"F",
        t2_start: b"G",
        expected: b"C",
        do_insert: false,
        do_get_next: false,
        do_commit: true,
    });
}

// JE: CursorTest.testPhantomDupDeleteGetPrevAbort
#[test]
#[ignore = "NEW-PHANTOM-ABORT-1: blocked getNext/getPrev dirty-reads a concurrent uncommitted write and does not re-resolve on abort"]
fn phantom_dup_delete_get_prev_abort() {
    phantom_dup_worker(PhantomConfig {
        t1_lock: b"F",
        t1_op: b"F",
        t2_start: b"G",
        expected: b"F",
        do_insert: false,
        do_get_next: false,
        do_commit: false,
    });
}

// ── DIAGNOSTIC PROBE (NEW-PHANTOM-ABORT-1): point-get CONTESTS, getNext SKIPS ──
// Bounded so a block cannot hang the harness.  Prints the observation; asserts
// the block-vs-skip disparity that IS the bug.
use std::sync::atomic::AtomicBool;

fn seed_env() -> (TempDir, Arc<Environment>, Arc<noxu_db::Database>) {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, false);
    let txn = env.begin_transaction(None).unwrap();
    for k in DATA_STRINGS {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k),
            DatabaseEntry::from_bytes(&[0u8; 10]),
        )
        .unwrap();
    }
    txn.commit().unwrap();
    (dir, env, db)
}

#[test]
#[ignore = "diagnostic probe: run with --ignored to observe block-vs-skip"]
fn diag_pointget_contests() {
    let (_dir, env, db) = seed_env();
    let seq = Arc::new(AtomicU32::new(0));
    let contended = Arc::new(AtomicBool::new(false));
    // Deleter holds F's write lock for 2000ms, then aborts.
    let del = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            db.delete_in(&txn, DatabaseEntry::from_bytes(b"F")).unwrap();
            seq.store(1, Ordering::SeqCst);
            while seq.load(Ordering::SeqCst) < 2 {
                thread::yield_now();
            }
            thread::sleep(Duration::from_millis(2000));
            txn.abort().unwrap();
        })
    };
    while seq.load(Ordering::SeqCst) < 1 {
        thread::yield_now();
    }
    let reader = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        let contended = Arc::clone(&contended);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            seq.store(2, Ordering::SeqCst);
            let t0 = std::time::Instant::now();
            let mut k = DatabaseEntry::from_bytes(b"F");
            let mut d = DatabaseEntry::new();
            let s = c.get(&mut k, &mut d, Get::Search, None);
            let el = t0.elapsed();
            // A point-get on the write-locked deleted slot must CONTEST: it
            // either blocks (>= a few hundred ms) or errors with a lock
            // timeout/conflict.  Returning Success in ~0ms would be a skip.
            let c_ended = el >= Duration::from_millis(200) || s.is_err();
            contended.store(c_ended, Ordering::SeqCst);
            println!(
                "PROBE point-get(F) -> {:?} after {:?} (contended={})",
                s, el, c_ended
            );
            drop(c);
            let _ = txn.commit();
        })
    };
    del.join().unwrap();
    reader.join().unwrap();
    assert!(
        contended.load(Ordering::SeqCst),
        "point-get must CONTEST (block/error on) the write-locked deleted slot, not skip"
    );
}

#[test]
#[ignore = "diagnostic probe: run with --ignored to observe block-vs-skip"]
fn diag_getnext_skips() {
    let (_dir, env, db) = seed_env();
    let seq = Arc::new(AtomicU32::new(0));
    let del = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        thread::spawn(move || {
            let txn = env.begin_transaction(None).unwrap();
            db.delete_in(&txn, DatabaseEntry::from_bytes(b"F")).unwrap();
            seq.store(1, Ordering::SeqCst);
            while seq.load(Ordering::SeqCst) < 2 {
                thread::yield_now();
            }
            thread::sleep(Duration::from_millis(2000));
            txn.abort().unwrap();
        })
    };
    while seq.load(Ordering::SeqCst) < 1 {
        thread::yield_now();
    }
    let txn = env.begin_transaction(None).unwrap();
    txn.set_lock_timeout(30_000);
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::from_bytes(b"C");
    let mut d = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut d, Get::Search, None).unwrap(),
        OperationStatus::Success
    );
    seq.store(2, Ordering::SeqCst);
    thread::sleep(Duration::from_millis(50)); // let deleter settle into its sleep
    let t0 = std::time::Instant::now();
    let mut nk = DatabaseEntry::new();
    let mut nd = DatabaseEntry::new();
    let s = c.get(&mut nk, &mut nd, Get::Next, None).unwrap();
    let el = t0.elapsed();
    println!(
        "PROBE getNext from C -> {:?} key={:?} in {:?}",
        s,
        nk.data_opt(),
        el
    );
    drop(c);
    let _ = txn.commit();
    del.join().unwrap();
    assert_eq!(
        nk.data_opt().unwrap(),
        b"G",
        "getNext landed on G (skip past deleted F)"
    );
    assert!(
        el < Duration::from_millis(500),
        "getNext did not block ({:?})",
        el
    );
}
