//! JE `com.sleepycat.je.dbi` miscellaneous behavioural ports (public API).
//!
//! Source classes:
//!   - `UncontendedLockTest.java` (lock-request / wait accounting)
//!   - `SR12641.java`             (concurrent BIN-splits vs. backward scans)
//!   - `DbTreeTest.java`          (named-database lookup / reopen)
//!   - `MemoryBudgetTest.java`    (explicit cache-size override half)
//!
//! Deviations recorded in tp-je-dbi.md:
//!   - JE's `UncontendedLockTest` reads per-locker `LOCK_READ_LOCKS` /
//!     `LOCK_WRITE_LOCKS` via `DbInternal.getLocker(txn).collectStats()`.
//!     Noxu exposes lock accounting at the ENV level (`stats().lock`): the
//!     portable invariants are `n_waits == 0` when uncontended and `n_waits
//!     > 0` under real contention, with `n_requests` counting the requests.
//!   - JE's `MemoryBudgetTest.testCacheSizing` derives the default cache size
//!     from `Runtime.maxMemory() * percent / 100` (a JVM-heap concept with no
//!     Noxu analogue); only the explicit `setCacheSize` override half is
//!     portable and is ported here.  `testDefaults` is ported in
//!     `crates/noxu-dbi/tests/je_memory_budget_test.rs`.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_checkpointer(false);
    cfg.set_run_cleaner(false);
    cfg.set_run_evictor(false);
    Environment::open(cfg).unwrap()
}

// ===========================================================================
// UncontendedLockTest.testUncontended / testUncontendedDups
// JE: a single txn doing N inserts (dups or not) issues N lock requests with
// ZERO waits (nothing else contends).  Noxu: env-level `n_waits` stays 0 and
// `n_requests` counts the requests.
// ===========================================================================
fn uncontended(dups: bool) {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "uncontended",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(dups),
        )
        .unwrap();

    let before = env.stats().unwrap().lock;
    let txn = env.begin_transaction(None).unwrap();
    if dups {
        // N dups under one key.
        let k = DatabaseEntry::from_bytes(b"k");
        for i in 0u8..10 {
            db.put_in(&txn, &k, DatabaseEntry::from_bytes(&[i])).unwrap();
        }
    } else {
        for i in 0u32..10 {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&i.to_be_bytes()),
                DatabaseEntry::from_bytes(b"v"),
            )
            .unwrap();
        }
    }
    let during = env.stats().unwrap().lock;
    txn.commit().unwrap();

    assert_eq!(
        0, during.n_waits,
        "uncontended {dups}-dup writes must produce zero lock waits"
    );
    assert!(
        during.n_requests > before.n_requests,
        "the writes must have issued lock requests (JE getNRequests)"
    );
}

#[test]
fn uncontended_lock_test_uncontended() {
    uncontended(false);
}

#[test]
fn uncontended_lock_test_uncontended_dups() {
    uncontended(true);
}

// ===========================================================================
// UncontendedLockTest.testContended / testContendedDups
// JE: with two lockers contending on the same record, the second waits.
// Noxu: env-level `n_waits` becomes > 0 while T2 blocks on T1's write lock.
// ===========================================================================
fn contended(dups: bool) {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(open_env(&dir));
    let db = Arc::new(
        env.open_database(
            None,
            "contended",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(dups),
        )
        .unwrap(),
    );
    // Seed one record.  For the dups case this establishes the (K, v0) pair
    // that both txns will contend on.
    db.put(DatabaseEntry::from_bytes(b"k"), DatabaseEntry::from_bytes(b"v0"))
        .unwrap();

    // T1 write-locks the EXISTING record and holds it.  For the dups case,
    // re-putting the seeded (k, v0) pair via Put::Overwrite write-locks that
    // exact pair; T2 will contend on the SAME pair.
    let t1 = env.begin_transaction(None).unwrap();
    if dups {
        let mut c = db.open_cursor_in(&t1, None).unwrap();
        c.put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v0"),
            noxu_db::Put::Overwrite,
        )
        .unwrap();
        drop(c);
    } else {
        db.put_in(
            &t1,
            DatabaseEntry::from_bytes(b"k"),
            DatabaseEntry::from_bytes(b"v1"),
        )
        .unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));
    let env2 = Arc::clone(&env);
    let db2 = Arc::clone(&db);
    let b2 = Arc::clone(&barrier);
    let started = Arc::new(AtomicBool::new(false));
    let started2 = Arc::clone(&started);
    let handle = std::thread::spawn(move || {
        b2.wait();
        let t2 = env2.begin_transaction(None).unwrap();
        started2.store(true, Ordering::SeqCst);
        // Attempt to touch the SAME record T1 holds: blocks on T1's lock.
        if dups {
            // Overwrite the SAME (k, v0) pair T1 write-locked: must block.
            let mut c = db2.open_cursor_in(&t2, None).unwrap();
            let _ = c.put(
                &DatabaseEntry::from_bytes(b"k"),
                &DatabaseEntry::from_bytes(b"v0"),
                noxu_db::Put::Overwrite,
            );
            drop(c);
        } else {
            let _ = db2.put_in(
                &t2,
                DatabaseEntry::from_bytes(b"k"),
                DatabaseEntry::from_bytes(b"v2"),
            );
        }
        let _ = t2.abort();
    });

    barrier.wait();
    // Give T2 time to reach the blocking lock request.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_wait = false;
    while Instant::now() < deadline {
        if started.load(Ordering::SeqCst) {
            let s = env.stats().unwrap().lock;
            if s.n_waits > 0 || s.n_waiters > 0 {
                saw_wait = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    // Release T1 so T2 can finish.
    t1.commit().unwrap();
    handle.join().unwrap();

    assert!(
        saw_wait,
        "contended {dups}-dup write must produce a lock wait (JE getNWaits > 0)"
    );
}

#[test]
fn uncontended_lock_test_contended() {
    contended(false);
}

#[test]
fn uncontended_lock_test_contended_dups() {
    contended(true);
}

// ===========================================================================
// SR12641.testSplitsWithScans / testSplitsWithScansDups
// JE: a writer thread inserts up to 100_000 records (dups or not) forcing many
// BIN splits in the last BIN, while a read-uncommitted reader repeatedly does
// getLast then getPrev NODE_MAX+1 times (moving from the last BIN into the
// prior BIN).  The bug (#12641 / #9543): the backward scan racing the splitter
// deadlocked/hung.  JE ships this only as a debug repro (not in the suite).
//
// This is a BOUNDED port: fewer records, a time-boxed loop.  The invariant is
// that a read-uncommitted backward scan racing a BIN-splitting writer neither
// panics nor deadlocks, and the DB stays consistent (final count matches the
// writer's tally; a full forward walk sees each key once, in order).
// ===========================================================================
fn splits_with_scans(dups: bool) {
    const N: u32 = 5_000; // bounded; forces many splits at fanout 256
    let dir = TempDir::new().unwrap();
    let env = Arc::new(open_env(&dir));
    let db = Arc::new(
        env.open_database(
            None,
            "sr12641",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(dups),
        )
        .unwrap(),
    );

    let writer_done = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));

    // Writer: ascending inserts to split the last BIN repeatedly.
    let env_w = Arc::clone(&env);
    let db_w = Arc::clone(&db);
    let wd = Arc::clone(&writer_done);
    let wcnt = Arc::clone(&written);
    let writer = std::thread::spawn(move || {
        for i in 0..N {
            // Small batched txns keep the tree growing steadily.
            let txn = env_w.begin_transaction(None).unwrap();
            if dups {
                // Single key, ascending dup data -> dup-chain / BIN growth.
                db_w.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(b"K"),
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                )
                .unwrap();
            } else {
                db_w.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(b"v"),
                )
                .unwrap();
            }
            txn.commit().unwrap();
            wcnt.fetch_add(1, Ordering::Relaxed);
        }
        wd.store(true, Ordering::SeqCst);
    });

    // Reader: read-uncommitted backward hops from the last BIN into the prior.
    // We approximate NODE_MAX+1 hops with a fixed count.
    let db_r = Arc::clone(&db);
    let wd_r = Arc::clone(&writer_done);
    let reader = std::thread::spawn(move || {
        while !wd_r.load(Ordering::SeqCst) {
            let mut c = db_r.open_cursor(None).unwrap();
            let mut k = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            // getLast, then a burst of getPrev (may hit before-first).
            let mut s = c.get(&mut k, &mut d, Get::Last, None).unwrap();
            for _ in 0..130 {
                if s != OperationStatus::Success {
                    break;
                }
                s = c.get(&mut k, &mut d, Get::Prev, None).unwrap();
            }
            drop(c);
        }
    });

    writer.join().unwrap();
    reader.join().unwrap();

    // Consistency: final count matches the writer's tally.
    let n = written.load(Ordering::Relaxed);
    assert_eq!(N as u64, n, "writer must have committed all records");
    assert_eq!(
        N as u64,
        db.count().unwrap(),
        "db.count() must match the committed record count"
    );

    // Full forward walk sees each key/record once, in ascending order.
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut prev: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut steps = 0u64;
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let cur = (
            k.data_opt().unwrap_or(&[]).to_vec(),
            d.data_opt().unwrap_or(&[]).to_vec(),
        );
        if let Some(p) = &prev {
            assert!(p < &cur, "forward walk must be strictly ascending");
        }
        prev = Some(cur);
        steps += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(N as u64, steps, "forward walk must visit each record once");
}

#[test]
fn sr12641_splits_with_scans() {
    splits_with_scans(false);
}

#[test]
fn sr12641_splits_with_scans_dups() {
    splits_with_scans(true);
}

// ===========================================================================
// DbTreeTest.testDbLookup
// JE: create two named DBs ("abcd", "xyz"); reopen each by name with
// allowCreate=false; close all.  The DbTree name→id lookup must round-trip.
// Deviation: Noxu's public Environment permits only ONE open handle per DB
// name at a time (returns DatabaseAlreadyExists otherwise; environment.rs:784),
// whereas JE allows several handles.  The lookup INTENT is preserved by
// closing the first handle before reopening by name.  (Also reconciled against
// integration_test::test_multiple_databases_isolated / ::test_get_database_names.)
// ===========================================================================
#[test]
fn db_tree_test_db_lookup() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let create =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db_abcd = env.open_database(None, "abcd", &create).unwrap();
    let db_xyz = env.open_database(None, "xyz", &create).unwrap();
    // Put one record in each so the reopen resolves a non-empty tree.
    db_abcd
        .put(DatabaseEntry::from_bytes(b"a"), DatabaseEntry::from_bytes(b"1"))
        .unwrap();
    db_xyz
        .put(DatabaseEntry::from_bytes(b"x"), DatabaseEntry::from_bytes(b"2"))
        .unwrap();
    // Close the first handles (single-handle-per-name policy) before reopening.
    db_abcd.close().unwrap();
    db_xyz.close().unwrap();

    // Reopen each by name with allowCreate=false — the lookup must succeed.
    let noreate = DatabaseConfig::new().with_transactional(true);
    let re_abcd = env.open_database(None, "abcd", &noreate).unwrap();
    let re_xyz = env.open_database(None, "xyz", &noreate).unwrap();

    // The reopened handles see the same records.
    let mut out = DatabaseEntry::new();
    assert!(
        re_abcd
            .get_into(None, DatabaseEntry::from_bytes(b"a"), &mut out)
            .unwrap()
    );
    assert_eq!(b"1", out.data_opt().unwrap());
    let mut out = DatabaseEntry::new();
    assert!(
        re_xyz
            .get_into(None, DatabaseEntry::from_bytes(b"x"), &mut out)
            .unwrap()
    );
    assert_eq!(b"2", out.data_opt().unwrap());

    // Opening a NON-existent name with allowCreate=false must fail.
    let missing = env.open_database(None, "nope", &noreate);
    assert!(missing.is_err(), "lookup of a missing db must fail");
}

// ===========================================================================
// MemoryBudgetTest.testCacheSizing  (explicit-override half)
// JE: setCacheSize(X) => env.getConfig().getCacheSize() == X and
// MemoryBudget.getMaxMemory() == X.  Noxu: with_cache_size(X) =>
// env.stats().cache_size == X.  (The JVM-heap-percentage default is N/A —
// no JVM heap; see module docs.)
// ===========================================================================
#[test]
fn memory_budget_test_cache_sizing_explicit_override() {
    let dir = TempDir::new().unwrap();
    let target: u64 = 8 * 1024 * 1024; // 8 MiB explicit cache
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_cache_percent(0);
    cfg.set_cache_size(target);
    let env = Environment::open(cfg).unwrap();
    assert_eq!(
        target,
        env.stats().unwrap().cache_size,
        "explicit cache size must be honoured as the memory budget"
    );
}
