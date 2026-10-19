// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! R-LATCHLOCK independent-review EXTRA witnesses (reviewer-authored).
//!
//! The author's `dirty_read_711_witness` covers a READ_COMMITTED *point get*.
//! These extend the coverage demanded by the review brief: a committed-
//! isolation reader must NEVER observe an UNCOMMITTED insert via
//!   (1) a READ_COMMITTED **cursor scan** (get_first / get_next), not a point get;
//!   (2) an **RMW read** (LockMode::Rmw, which takes a WRITE lock — must block
//!       the inserter's txn, never return its uncommitted bytes);
//!   (3) a **secondary-index read** (the secondary->primary inner read path).
//!
//! Each races an INSERT+ABORT churner of a key whose value is b"UNCOMMIT" and
//! which is therefore NEVER committed-present, so any observed b"UNCOMMIT" is a
//! dirty read of uncommitted data (isolation S2).  RED on the base
//! (lock-outside-the-latch TOCTOU) under 2-core load, GREEN after the
//! lock-while-latched fix.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use noxu_db::transaction_config::TransactionConfig;
use noxu_db::{
    CursorConfig, Database, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, Get, LockMode, ReadOptions, SecondaryConfig,
    SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use tempfile::TempDir;

const UNCOMMIT: &[u8] = b"UNCOMMIT";

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
}

fn open_primary(env: &Environment, name: &str) -> Arc<Mutex<Database>> {
    Arc::new(Mutex::new(
        env.open_database(
            None,
            name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    ))
}

/// (1) READ_COMMITTED **cursor scan** vs an INSERT+ABORT churner.
#[test]
fn read_committed_cursor_scan_never_sees_uncommitted_insert() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(open_env(&dir));
    let db = open_primary(&env, "pri");

    let dirty = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let env_w = Arc::clone(&env);
    let db_w = Arc::clone(&db);
    let done_w = Arc::clone(&done);
    let writer = std::thread::spawn(move || {
        while !done_w.load(Ordering::Acquire) {
            let txn = env_w.begin_transaction(None).unwrap();
            {
                let g = db_w.lock();
                let _ = g.put_in(&txn, b"hot", UNCOMMIT);
            }
            txn.abort().unwrap();
        }
    });

    let mut readers = Vec::new();
    for _ in 0..6 {
        let env_r = Arc::clone(&env);
        let db_r = Arc::clone(&db);
        let dirty_r = Arc::clone(&dirty);
        let reads_r = Arc::clone(&reads);
        let done_r = Arc::clone(&done);
        readers.push(std::thread::spawn(move || {
            for _ in 0..3000 {
                if done_r.load(Ordering::Acquire) {
                    break;
                }
                let mut cfg = TransactionConfig::new();
                cfg.set_read_committed(true);
                let txn = env_r.begin_transaction(Some(&cfg)).unwrap();
                {
                    let g = db_r.lock();
                    let cfg_cursor = CursorConfig::new();
                    if let Ok(mut cursor) =
                        g.open_cursor_in(&txn, Some(&cfg_cursor))
                    {
                        let mut key = DatabaseEntry::new();
                        let mut data = DatabaseEntry::new();
                        let mut status = cursor.get(
                            &mut key,
                            &mut data,
                            Get::First,
                            Some(LockMode::ReadCommitted),
                        );
                        while let Ok(noxu_db::OperationStatus::Success) = status
                        {
                            reads_r.fetch_add(1, Ordering::Relaxed);
                            if data.data_opt() == Some(UNCOMMIT) {
                                dirty_r.fetch_add(1, Ordering::Relaxed);
                            }
                            status = cursor.get(
                                &mut key,
                                &mut data,
                                Get::Next,
                                Some(LockMode::ReadCommitted),
                            );
                        }
                    }
                }
                let _ = txn.commit();
            }
        }));
    }

    for r in readers {
        r.join().unwrap();
    }
    done.store(true, Ordering::Release);
    writer.join().unwrap();

    assert_eq!(
        dirty.load(Ordering::Relaxed),
        0,
        "RC cursor scan observed an UNCOMMITTED insert value ({} of {} reads)",
        dirty.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
    );
}

/// (2) **RMW read** (LockMode::Rmw) vs an INSERT+ABORT churner.
#[test]
fn rmw_read_never_sees_uncommitted_insert() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(open_env(&dir));
    let db = open_primary(&env, "pri");

    let dirty = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let env_w = Arc::clone(&env);
    let db_w = Arc::clone(&db);
    let done_w = Arc::clone(&done);
    let writer = std::thread::spawn(move || {
        while !done_w.load(Ordering::Acquire) {
            let txn = env_w.begin_transaction(None).unwrap();
            {
                let g = db_w.lock();
                let _ = g.put_in(&txn, b"hot", UNCOMMIT);
            }
            txn.abort().unwrap();
        }
    });

    let mut readers = Vec::new();
    for _ in 0..6 {
        let env_r = Arc::clone(&env);
        let db_r = Arc::clone(&db);
        let dirty_r = Arc::clone(&dirty);
        let reads_r = Arc::clone(&reads);
        let done_r = Arc::clone(&done);
        readers.push(std::thread::spawn(move || {
            let opts = ReadOptions::new().with_lock_mode(LockMode::Rmw);
            for _ in 0..3000 {
                if done_r.load(Ordering::Acquire) {
                    break;
                }
                let txn = env_r.begin_transaction(None).unwrap();
                {
                    let g = db_r.lock();
                    match g.get_with_options(Some(&txn), b"hot", &opts) {
                        Ok(Some(v)) => {
                            reads_r.fetch_add(1, Ordering::Relaxed);
                            if v.as_ref() == UNCOMMIT {
                                dirty_r.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Ok(None) => {
                            reads_r.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {}
                    }
                }
                let _ = txn.commit();
            }
        }));
    }

    for r in readers {
        r.join().unwrap();
    }
    done.store(true, Ordering::Release);
    writer.join().unwrap();

    assert_eq!(
        dirty.load(Ordering::Relaxed),
        0,
        "RMW read observed an UNCOMMITTED insert value ({} of {} reads)",
        dirty.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
    );
}

/// First-byte secondary key creator: the secondary key is byte 0 of the data.
struct FirstByteCreator;
impl SecondaryKeyCreator for FirstByteCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _pri_key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(d) = data.data_opt()
            && !d.is_empty()
        {
            result.set_data(&d[..1]);
            return true;
        }
        false
    }
}

/// (3) **Secondary-index read** vs an INSERT+ABORT churner on the primary.
#[test]
fn secondary_read_never_sees_uncommitted_insert() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(open_env(&dir));
    let pri = open_primary(&env, "pri");
    let inner = env
        .open_database(
            None,
            "sec",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    let sec = Arc::new(Mutex::new(
        SecondaryDatabase::open(
            Arc::clone(&pri),
            inner,
            SecondaryConfig::new()
                .with_allow_create(true)
                .with_key_creator(Box::new(FirstByteCreator)),
        )
        .unwrap(),
    ));

    let dirty = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let env_w = Arc::clone(&env);
    let pri_w = Arc::clone(&pri);
    let done_w = Arc::clone(&done);
    let writer = std::thread::spawn(move || {
        while !done_w.load(Ordering::Acquire) {
            let txn = env_w.begin_transaction(None).unwrap();
            {
                let g = pri_w.lock();
                let _ = g.put_in(&txn, b"hot", UNCOMMIT);
            }
            txn.abort().unwrap();
        }
    });

    let mut readers = Vec::new();
    for _ in 0..6 {
        let env_r = Arc::clone(&env);
        let sec_r = Arc::clone(&sec);
        let dirty_r = Arc::clone(&dirty);
        let reads_r = Arc::clone(&reads);
        let done_r = Arc::clone(&done);
        readers.push(std::thread::spawn(move || {
            let opts =
                ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
            for _ in 0..3000 {
                if done_r.load(Ordering::Acquire) {
                    break;
                }
                let mut cfg = TransactionConfig::new();
                cfg.set_read_committed(true);
                let txn = env_r.begin_transaction(Some(&cfg)).unwrap();
                {
                    let g = sec_r.lock();
                    let mut p_key = DatabaseEntry::new();
                    let mut data = DatabaseEntry::new();
                    // Secondary key b'U' maps to the uncommitted primary.
                    match g.get_into_with_options(
                        Some(&txn),
                        b"U",
                        &mut p_key,
                        &mut data,
                        &opts,
                    ) {
                        Ok(true) => {
                            reads_r.fetch_add(1, Ordering::Relaxed);
                            if data.data_opt() == Some(UNCOMMIT) {
                                dirty_r.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Ok(false) => {
                            reads_r.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {}
                    }
                }
                let _ = txn.commit();
            }
        }));
    }

    for r in readers {
        r.join().unwrap();
    }
    done.store(true, Ordering::Release);
    writer.join().unwrap();

    assert_eq!(
        dirty.load(Ordering::Relaxed),
        0,
        "secondary read observed an UNCOMMITTED insert value ({} of {} reads)",
        dirty.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
    );
}
