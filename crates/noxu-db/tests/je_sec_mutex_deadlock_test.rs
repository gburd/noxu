// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Liveness guard for **BUG-SEC-MUTEX-DEADLOCK**.
//!
//! `SecondaryDatabase::get_into` / `SecondaryCursor` read + maintenance paths
//! used to hold the `Mutex<Database>` primary-handle guard ACROSS the blocking
//! primary record-lock acquisition.  A secondary reader (holding a sec-index
//! read lock) would block on that `Mutex` while a concurrent writer held the
//! `Mutex` and blocked on the sec-index write lock -> a `Mutex` <-> record-lock
//! cycle the txn deadlock detector cannot see (the `Mutex` is not a txn lock),
//! wedging both parties for the full lock timeout (and then forever, as each
//! retries into the same cycle).
//!
//! Repro (both fail-before, pre-fix, hang 5s-forever): 1 secondary reader +
//! 1 deleter (via secondary OR via primary — the primary delete fans out to the
//! sec-index via the auto-hook, so it also contends on the sec-index lock while
//! the reader holds the `Mutex`).  The equivalent primary-only reader+deleter
//! completes in ~0.2s, proving the hang is specific to the secondary path.
//!
//! Fix (JE-faithful): the secondary read / maintenance path reads and modifies
//! the primary record through a lock-free primary handle
//! (`Database::handle_clone`), never holding the handle `Mutex` across the
//! record-lock wait — mirroring JE reading the primary through the `Database`
//! reference returned by `SecondaryDatabase.getPrimary` with no monitor held
//! (`SecondaryCursor.readPrimaryAfterGet`, SecondaryCursor.java:1717-1748;
//! `Cursor.readPrimaryAfterGet`, Cursor.java:4805-4960).
//!
//! This guard asserts the workload makes progress and COMPLETES within a bound.
//! A watchdog aborts the process on a stall so the undetectable-hang regression
//! shows up as a hard failure, not a silently-timing-out test.  The remaining
//! sec->primary ABBA deadlock (both parties via txn locks, which the detector
//! DOES see and aborts) is a SEPARATE engine gap owned by the secondary
//! read-locking workstream (JE sec-dirty-read + lockPrimaryOnly deadlock
//! avoidance); here a `DeadlockDetected` / `SecondaryIntegrityException` is a
//! retriable transient, NOT a hang.

#![cfg(not(noxu_shuttle))]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Identity key creator (sec key = pri key).
struct IdentityKeyCreator;
impl SecondaryKeyCreator for IdentityKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        key: &DatabaseEntry,
        _data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        result.set_data(key.data_opt().unwrap_or(&[]));
        true
    }
}

fn open_sec(
    dir: &TempDir,
) -> (Arc<Environment>, Arc<Mutex<Database>>, Arc<SecondaryDatabase>) {
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true)
                // Short timeouts: if the Mutex-deadlock regression returns,
                // a timeout fires fast and the retry re-enters the cycle, so
                // the watchdog trips quickly rather than after a long wait.
                .with_txn_timeout(2_000)
                .with_lock_timeout(2_000),
        )
        .unwrap(),
    );
    let primary = Arc::new(Mutex::new(
        env.open_database(
            None,
            "foo",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    ));
    let inner = env
        .open_database(
            None,
            "fooSec",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    let sec = Arc::new(
        SecondaryDatabase::open(
            Arc::clone(&primary),
            inner,
            SecondaryConfig::new()
                .with_allow_create(true)
                .with_allow_populate(true)
                .with_sorted_duplicates(true)
                .with_key_creator(Box::new(IdentityKeyCreator)),
        )
        .unwrap(),
    );
    (env, primary, sec)
}

fn is_retriable(e: &noxu_db::NoxuError) -> bool {
    // Txn-level lock conflicts / timeouts AND the sec->primary integrity race
    // (owned by the secondary read-locking workstream) are retriable
    // transients.  The Mutex-deadlock this guard targets produced NONE of
    // these — it produced an undetectable hang the watchdog catches.
    //
    // v7.11.1 backport deviation: at the tag, `Database::get`/`get_into`/
    // `delete*` string-wrap the typed txn error as
    // `OperationNotAllowed("transaction error: …")` (the typed
    // `map_err(NoxuError::from)` mapping lands with the later A11/A14
    // error-handling work, out of this batch).  So the acceptable
    // sec->primary txn-visible DEADLOCK / LOCK TIMEOUT surfaces as a string,
    // not a typed `DeadlockDetected`/`LockTimeout`, and the typed predicates
    // below miss it.  Recognise the string form too.  This does NOT weaken
    // the hang oracle: the Mutex<->record hang produces NO error at all (both
    // parties block forever), so the watchdog SIGABRT catches it regardless of
    // this classifier.
    fn is_wrapped_txn_transient(e: &noxu_db::NoxuError) -> bool {
        if let noxu_db::NoxuError::OperationNotAllowed(m) = e {
            let m = m.to_ascii_lowercase();
            m.contains("deadlock")
                || m.contains("lock timeout")
                || m.contains("transaction timeout")
                || m.contains("lock conflict")
        } else {
            false
        }
    }
    e.is_lock_conflict()
        || e.is_lock_timeout()
        || matches!(e, noxu_db::NoxuError::SecondaryIntegrityException(_))
        || is_wrapped_txn_transient(e)
}

/// Spawn a watchdog that aborts the process if `done` is not set within
/// `secs`.  This turns the pre-fix undetectable hang into a hard failure.
fn watchdog(
    done: Arc<AtomicBool>,
    secs: u64,
    msg: &'static str,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            if done.load(Ordering::Acquire) {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        eprintln!("STALL (BUG-SEC-MUTEX-DEADLOCK regression?): {msg}");
        std::process::abort();
    })
}

/// Secondary reader (loop until the primary key is deleted) + a deleter via the
/// SECONDARY handle.  Pre-fix: Mutex<->sec-index deadlock, hangs forever.
#[test]
fn sec_read_vs_sec_delete_makes_progress() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) = open_sec(&dir);
    // Populate 0..40 (identity secondary).
    for i in 0..40u32 {
        let v = i.to_string();
        primary.lock().put(v.as_bytes(), v.as_bytes()).unwrap();
    }
    let key = b"20".to_vec();
    let done = Arc::new(AtomicBool::new(false));
    let wd = watchdog(Arc::clone(&done), 30, "sec read vs sec delete wedged");
    let barrier = Arc::new(Barrier::new(2));

    let deleter = {
        let env = Arc::clone(&env);
        let sec = Arc::clone(&sec);
        let barrier = Arc::clone(&barrier);
        let key = key.clone();
        thread::spawn(move || {
            barrier.wait();
            loop {
                let txn = env.begin_transaction(None).unwrap();
                match sec.delete_in(&txn, &key) {
                    Ok(d) => {
                        txn.commit().unwrap();
                        return d;
                    }
                    Err(ref e) if is_retriable(e) => {
                        let _ = txn.abort();
                    }
                    Err(e) => panic!("delete: {e:?}"),
                }
            }
        })
    };
    let reader = {
        let env = Arc::clone(&env);
        let sec = Arc::clone(&sec);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            loop {
                let txn = env.begin_transaction(None).unwrap();
                let mut pk = DatabaseEntry::new();
                let mut d = DatabaseEntry::new();
                match sec.get_into(Some(&txn), &key, &mut pk, &mut d) {
                    Ok(true) => {
                        txn.commit().unwrap();
                    }
                    Ok(false) => {
                        txn.commit().unwrap();
                        return;
                    }
                    Err(ref e) if is_retriable(e) => {
                        let _ = txn.abort();
                    }
                    Err(e) => panic!("read: {e:?}"),
                }
            }
        })
    };

    assert!(deleter.join().unwrap(), "deleter must delete the key");
    reader.join().unwrap();
    done.store(true, Ordering::Release);
    wd.join().unwrap();
}

/// Secondary reader + a deleter via the PRIMARY handle (the primary delete fans
/// out to the sec-index via the auto-hook, so it still contends on the
/// sec-index lock while the reader holds the handle Mutex).  Pre-fix: hangs
/// forever on the same Mutex<->record cycle.
#[test]
fn sec_read_vs_primary_delete_makes_progress() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) = open_sec(&dir);
    for i in 0..40u32 {
        let v = i.to_string();
        primary.lock().put(v.as_bytes(), v.as_bytes()).unwrap();
    }
    let key = b"20".to_vec();
    let done = Arc::new(AtomicBool::new(false));
    let wd =
        watchdog(Arc::clone(&done), 30, "sec read vs primary delete wedged");
    let barrier = Arc::new(Barrier::new(2));

    let deleter = {
        let env = Arc::clone(&env);
        let primary = Arc::clone(&primary);
        let barrier = Arc::clone(&barrier);
        let key = key.clone();
        thread::spawn(move || {
            barrier.wait();
            loop {
                let txn = env.begin_transaction(None).unwrap();
                let r = primary.lock().delete_in(&txn, &key);
                match r {
                    Ok(d) => {
                        txn.commit().unwrap();
                        return d;
                    }
                    Err(ref e) if is_retriable(e) => {
                        let _ = txn.abort();
                    }
                    Err(e) => panic!("delete: {e:?}"),
                }
            }
        })
    };
    let reader = {
        let env = Arc::clone(&env);
        let sec = Arc::clone(&sec);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            loop {
                let txn = env.begin_transaction(None).unwrap();
                let mut pk = DatabaseEntry::new();
                let mut d = DatabaseEntry::new();
                match sec.get_into(Some(&txn), &key, &mut pk, &mut d) {
                    Ok(true) => {
                        txn.commit().unwrap();
                    }
                    Ok(false) => {
                        txn.commit().unwrap();
                        return;
                    }
                    Err(ref e) if is_retriable(e) => {
                        let _ = txn.abort();
                    }
                    Err(e) => panic!("read: {e:?}"),
                }
            }
        })
    };

    assert!(deleter.join().unwrap(), "deleter must delete the key");
    reader.join().unwrap();
    done.store(true, Ordering::Release);
    wd.join().unwrap();
}
