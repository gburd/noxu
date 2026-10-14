// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! BUG-RC-SCAN-TORN-READ (tracked item **A11-scan**, batch D).
//!
//! A READ_COMMITTED cursor scan (`Get::First`/`Next`/`Last`/`Prev`) and the
//! edge positioners (`get_first` / `get_last`) can observe a concurrent
//! writer's UNCOMMITTED in-place slot bytes.  Exposed at v7.11.0 via
//! `CursorImpl::revalidate_locked_slot` (cursor_impl.rs): when a slot's LSN
//! moves in the prefetch->lock window, that helper re-reads the slot's CURRENT
//! bytes WITHOUT re-locking the new LSN, so an aborting writer's value leaks
//! into an RC reader.
//!
//! This is the SAME root cause as the point-read torn read fixed for
//! `search(Set)`/`search(SetRange)` in batch B (A14-min,
//! `reread_data_locking_current_lsn`), but on the broader `revalidate_locked_slot`
//! surface (8 call sites across get_first/get_last/scan/dup/edge), which the
//! full A11 port (`restart_after_slot_change` + `run_with_defunct_restart`,
//! integrate f5e310a1 / wave4) fixes.
//!
//! These are RED witnesses for batch D: they FAIL on v7.11.0 (and still fail
//! after batch B, since A14-min deliberately does not touch
//! `revalidate_locked_slot`).  Batch D (A11 full) must un-`#[ignore]` them and
//! show them GREEN.  Severity: isolation (dirty read under READ_COMMITTED
//! during a scan / edge positioning).
#![cfg(not(noxu_shuttle))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus, TransactionConfig,
};
use tempfile::TempDir;

fn open() -> (TempDir, Arc<Environment>, Arc<noxu_db::Database>) {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    let db = Arc::new(
        env.open_database(
            None,
            "rcscan",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    (dir, env, db)
}

/// Drive an RC reader using `getter` (First+Next scan, or First, or Last)
/// against an abort-cycling writer that moves every key's LSN with an
/// uncommitted "Bvalue".  Returns true iff a torn read was observed.
fn torn_under(
    getter: impl Fn(&mut noxu_db::Cursor) -> bool + Send + Sync + 'static,
) -> (bool, u64) {
    let (_dir, env, db) = open();
    for i in 0..10u32 {
        db.put(format!("K{i:02}").as_bytes(), b"Avalue").unwrap();
    }
    let torn = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let iters = Arc::new(AtomicU64::new(0));
    let getter = Arc::new(getter);

    let r_db = Arc::clone(&db);
    let r_env = Arc::clone(&env);
    let r_torn = Arc::clone(&torn);
    let r_done = Arc::clone(&done);
    let r_it = Arc::clone(&iters);
    let r_get = Arc::clone(&getter);
    let reader = thread::spawn(move || {
        let rc = TransactionConfig::new().with_read_committed(true);
        while !r_done.load(Ordering::Relaxed) {
            let Ok(txn) = r_env.begin_transaction(Some(&rc)) else { continue };
            if let Ok(mut cur) = r_db.open_cursor_in(&txn, None)
                && r_get(&mut cur)
            {
                r_torn.store(true, Ordering::Relaxed);
            }
            let _ = txn.commit();
            r_it.fetch_add(1, Ordering::Relaxed);
        }
    });
    for _ in 0..4000 {
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..10u32 {
            let _ = db.put_in(&txn, format!("K{i:02}").as_bytes(), b"Bvalue");
        }
        txn.abort().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    (torn.load(Ordering::Relaxed), iters.load(Ordering::Relaxed))
}

fn saw_bvalue(d: &DatabaseEntry) -> bool {
    d.data_opt().is_some_and(|v| v != b"Avalue")
}

/// RC forward scan (Get::First + Get::Next) must never see uncommitted data.
#[test]
#[ignore = "BUG-RC-SCAN-TORN-READ (A11-scan): fixed in batch D (A11 full); \
            RED on v7.11.0 + batch B"]
fn rc_scan_next_never_sees_uncommitted() {
    let (torn, iters) = torn_under(|cur| {
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut st = cur.get(&mut k, &mut d, Get::First, None);
        while matches!(st, Ok(OperationStatus::Success)) {
            if saw_bvalue(&d) {
                return true;
            }
            st = cur.get(&mut k, &mut d, Get::Next, None);
        }
        false
    });
    assert!(!torn, "RC forward scan observed uncommitted value. iters={iters}");
}

/// RC `get_first` must never return uncommitted data.
#[test]
#[ignore = "BUG-RC-SCAN-TORN-READ (A11-scan): fixed in batch D (A11 full); \
            RED on v7.11.0 + batch B"]
fn rc_get_first_never_sees_uncommitted() {
    let (torn, iters) = torn_under(|cur| {
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        matches!(
            cur.get(&mut k, &mut d, Get::First, None),
            Ok(OperationStatus::Success)
        ) && saw_bvalue(&d)
    });
    assert!(!torn, "RC get_first observed uncommitted value. iters={iters}");
}

/// RC `get_last` must never return uncommitted data.
#[test]
#[ignore = "BUG-RC-SCAN-TORN-READ (A11-scan): fixed in batch D (A11 full); \
            RED on v7.11.0 + batch B"]
fn rc_get_last_never_sees_uncommitted() {
    let (torn, iters) = torn_under(|cur| {
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        matches!(
            cur.get(&mut k, &mut d, Get::Last, None),
            Ok(OperationStatus::Success)
        ) && saw_bvalue(&d)
    });
    assert!(!torn, "RC get_last observed uncommitted value. iters={iters}");
}
