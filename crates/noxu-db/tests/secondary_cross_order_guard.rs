// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Regression guard for the secondary->primary cross-order interaction
//! (FEAT-SEC-DEADLOCK-AVOID is EXCLUDED from v7.11.1 — see below).
//!
//! The full `lockPrimaryOnly` deadlock-AVOIDANCE (parity/integrate fd7895dbc5)
//! is a feature-adjacent change (adds a new `READ_UNCOMMITTED_ALL` lock mode,
//! 536 lines) whose failure mode is a DETECTOR-VISIBLE, retriable deadlock
//! (abort + retry), not a hang or data loss.  It is EXCLUDED from the v7.11.1
//! patch and stays on integrate for v8.0.0.
//!
//! This guard pins the behaviour the patch DOES guarantee at the tip (after
//! A7b's lock-free secondary primary read): a transactional secondary reader
//! vs a primary writer that keeps rewriting the same primary (alternating the
//! secondary key to contend the secondary entry) makes BOUNDED progress — both
//! parties complete many iterations within the window, and any deadlock that
//! does surface is the retriable, detector-visible kind, not a wedge.  If a
//! future change reintroduced a *hang* (an undetectable Mutex<->record cycle,
//! the A7/A8 class), this guard's progress bound would trip.
#![cfg(not(noxu_shuttle))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use tempfile::TempDir;

struct FirstByte;
impl SecondaryKeyCreator for FirstByte {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _k: &DatabaseEntry,
        d: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        match d.data_opt().and_then(|v| v.first().copied()) {
            Some(b) => {
                result.set_data(&[b]);
                true
            }
            None => false,
        }
    }
}

#[test]
fn secondary_primary_cross_order_makes_bounded_progress() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true)
                .with_lock_timeout(2000),
        )
        .unwrap(),
    );
    let pri = env
        .open_database(
            None,
            "pri",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let pri = Arc::new(Mutex::new(pri));
    let sec_inner = env
        .open_database(
            None,
            "sec",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    let sec = Arc::new(
        SecondaryDatabase::open(
            Arc::clone(&pri),
            sec_inner,
            SecondaryConfig::new()
                .with_allow_create(true)
                .with_key_creator(Box::new(FirstByte)),
        )
        .unwrap(),
    );
    pri.lock().put(b"K", b"Aval").unwrap();

    let reads = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let hard_err = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));

    // Transactional secondary reader: sec->primary order.
    let r_sec = Arc::clone(&sec);
    let r_env = Arc::clone(&env);
    let r_reads = Arc::clone(&reads);
    let r_hard = Arc::clone(&hard_err);
    let r_done = Arc::clone(&done);
    let reader = thread::spawn(move || {
        while !r_done.load(Ordering::Relaxed) {
            let txn = r_env.begin_transaction(None).unwrap();
            if let Ok(mut cur) = r_sec.open_cursor_in(&txn, None) {
                let sk = DatabaseEntry::from_bytes(b"A");
                let mut pk = DatabaseEntry::new();
                let mut d = DatabaseEntry::new();
                match cur.get_search_key(&sk, &mut pk, &mut d) {
                    Ok(_) => {}
                    Err(e) => {
                        let m = format!("{e:?}").to_ascii_lowercase();
                        // Detector-visible deadlock / lock timeout / integrity
                        // (concurrent sec-key change) are the EXPECTED,
                        // retriable transients.  Anything else is a hard error.
                        if !(m.contains("deadlock")
                            || m.contains("lock timeout")
                            || m.contains("lock conflict")
                            || m.contains("integrity"))
                        {
                            r_hard.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
            let _ = txn.commit();
            r_reads.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Primary writer: primary->sec order (fan-out), alternating the sec key.
    let w_pri = Arc::clone(&pri);
    let w_env = Arc::clone(&env);
    let w_writes = Arc::clone(&writes);
    let w_done = Arc::clone(&done);
    let writer = thread::spawn(move || {
        let mut n = 0u8;
        while !w_done.load(Ordering::Relaxed) {
            let txn = w_env.begin_transaction(None).unwrap();
            let first = if n.is_multiple_of(2) { b'A' } else { b'B' };
            let val = [first, n];
            match w_pri.lock().put_in(&txn, b"K", val) {
                Ok(_) => {
                    let _ = txn.commit();
                }
                Err(_) => {
                    let _ = txn.abort();
                }
            }
            n = n.wrapping_add(1);
            w_writes.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Progress watchdog: a HANG (undetectable Mutex<->record cycle) stalls both
    // counters; a retriable deadlock does not.  Trip on a reader stall.
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut last_reads = 0u64;
    let mut stalls = 0;
    loop {
        thread::sleep(Duration::from_millis(500));
        let r = reads.load(Ordering::Relaxed);
        if r == last_reads {
            stalls += 1;
        } else {
            stalls = 0;
        }
        last_reads = r;
        assert!(
            stalls < 4,
            "secondary reader stalled (reads={r}) — a cross-order interaction \
             wedged instead of aborting+retrying (hang regression)"
        );
        if Instant::now() >= deadline {
            break;
        }
    }
    done.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    writer.join().unwrap();

    assert!(
        !hard_err.load(Ordering::Relaxed),
        "reader hit a non-retriable error"
    );
    assert!(
        reads.load(Ordering::Relaxed) > 10,
        "reader made too little progress"
    );
    assert!(
        writes.load(Ordering::Relaxed) > 10,
        "writer made too little progress"
    );
}
