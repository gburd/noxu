// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Worker process for crash recovery tests.
//!
//! Reads NOXU_CRASH_DIR and NOXU_CRASH_MODE from the environment, then runs
//! the requested write scenario. The parent test kills this process at a
//! deterministic point and verifies recovery semantics by reopening the env.
//!
//! # Modes
//!
//! ## `committed_then_uncommitted`
//! - Phase 1: write keys 0..50 each in their own committed transaction,
//!   then write flag file `phase1_done`.
//! - Phase 2: open one transaction, write keys 1000..1050 without committing,
//!   write flag file `phase2_started`, then loop until killed.
//!
//! ## `uncommitted_only`
//! - Write a sentinel key `b"sentinel"` = `b"ok"` in a committed transaction.
//! - Write flag file `sentinel_committed`.
//! - Open one transaction, write keys 0..50 without committing, write flag
//!   file `uncommitted_started`, then loop until killed.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

fn main() {
    let dir: PathBuf =
        env::var("NOXU_CRASH_DIR").expect("NOXU_CRASH_DIR must be set").into();
    let mode =
        env::var("NOXU_CRASH_MODE").expect("NOXU_CRASH_MODE must be set");

    let env_config = EnvironmentConfig::new(dir.clone())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).expect("open env");
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "test", &db_config).expect("open db");

    match mode.as_str() {
        "committed_then_uncommitted" => {
            // Phase 1: one committed transaction per key.
            for i in 0u32..50 {
                let txn = env.begin_transaction(None).expect("begin txn");
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"committed");
                db.put(Some(&txn), &key, &val).expect("put");
                txn.commit().expect("commit");
            }
            flag(&dir, "phase1_done");

            // Phase 2: one uncommitted transaction — parent kills us here.
            let txn = env.begin_transaction(None).expect("begin txn");
            for i in 1000u32..1050 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"uncommitted");
                db.put(Some(&txn), &key, &val).expect("put");
            }
            flag(&dir, "phase2_started");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "uncommitted_only" => {
            // Committed sentinel so the parent can confirm the db was open.
            let txn = env.begin_transaction(None).expect("begin txn");
            let sentinel_key = DatabaseEntry::from_bytes(b"sentinel");
            let sentinel_val = DatabaseEntry::from_bytes(b"ok");
            db.put(Some(&txn), &sentinel_key, &sentinel_val)
                .expect("put sentinel");
            txn.commit().expect("commit sentinel");
            flag(&dir, "sentinel_committed");

            // Uncommitted batch — parent kills us here.
            let txn = env.begin_transaction(None).expect("begin txn");
            for i in 0u32..50 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"uncommitted");
                db.put(Some(&txn), &key, &val).expect("put");
            }
            flag(&dir, "uncommitted_started");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ----------------------------------------------------------------
        // ordered_commits: T1 commits 25 keys (0..25); T2 writes 25 keys
        // (100..125) but never commits — the parent kills after t2_started.
        //
        // After recovery:
        //   - T1 keys 0..25 must be present (committed before kill).
        //   - T2 keys 100..125 must be absent (never committed).
        //
        // Probes commit ordering: the committed T1 state survives even though
        // an uncommitted T2 was in-flight at crash time.
        "ordered_commits" => {
            // T1: committed.
            let txn = env.begin_transaction(None).expect("begin T1");
            for i in 0u32..25 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"t1");
                db.put(Some(&txn), &key, &val).expect("put T1");
            }
            txn.commit().expect("commit T1");
            flag(&dir, "t1_done");

            // T2: uncommitted — parent kills us here.
            let txn = env.begin_transaction(None).expect("begin T2");
            for i in 100u32..125 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"t2");
                db.put(Some(&txn), &key, &val).expect("put T2");
            }
            flag(&dir, "t2_started");
            drop(txn); // suppress "unused" lint — never committed
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ----------------------------------------------------------------
        // clean_then_dirty: write 25 committed records, signal readiness,
        // then wait — used to test clean-close vs SIGKILL parity.
        // The parent can either let this exit cleanly (graceful) or SIGKILL it.
        "clean_then_dirty" => {
            for i in 0u32..25 {
                let txn = env.begin_transaction(None).expect("begin txn");
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"parity");
                db.put(Some(&txn), &key, &val).expect("put");
                txn.commit().expect("commit");
            }
            flag(&dir, "writes_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ----------------------------------------------------------------
        // open_txn_spanning_checkpoint:
        //
        // Phase 1: write 20 committed keys (key=b"committed_NNN"), signal
        //          "phase1_done".
        // Phase 2: begin a transaction, write 10 keys (key=b"open_NNN"),
        //          FORCE a checkpoint (so CkptStart > the txn's first LN),
        //          signal "open_txn_ready" (txn still open, not committed),
        //          loop until killed.
        //
        // After SIGKILL, recovery must:
        //   - See all 20 committed keys.
        //   - NOT see any of the 10 open-txn keys.
        //
        // This is the exact P-2 correctness blocker: without the
        // first_active_lsn fix, recovery scans from CkptStart, which is
        // after the open txn's LNs, and the undo pass never sees them.
        "open_txn_spanning_checkpoint" => {
            use noxu_db::CheckpointConfig;

            // Phase 1: committed writes.
            for i in 0u32..20 {
                let k = format!("committed_{i:03}");
                let txn = env.begin_transaction(None).expect("begin txn");
                let key = DatabaseEntry::from_bytes(k.as_bytes());
                let val = DatabaseEntry::from_bytes(b"ok");
                db.put(Some(&txn), &key, &val).expect("put");
                txn.commit().expect("commit");
            }
            flag(&dir, "phase1_done");

            // Phase 2: open transaction (keys written BEFORE the checkpoint).
            let txn = env.begin_transaction(None).expect("begin txn");
            for i in 0u32..10 {
                let k = format!("open_{i:03}");
                let key = DatabaseEntry::from_bytes(k.as_bytes());
                let val = DatabaseEntry::from_bytes(b"should_not_survive");
                db.put(Some(&txn), &key, &val).expect("put open key");
            }

            // Force a checkpoint AFTER the open txn has written its LNs.
            // CkptStart will be > the txn's firstLoggedLsn.  Without the
            // first_active_lsn fix, recovery would start from CkptStart and
            // miss the txn's LNs — failing to undo the uncommitted writes.
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("forced checkpoint");

            // Signal that the txn is open and the checkpoint is done.
            // The parent will now SIGKILL us.
            flag(&dir, "open_txn_ready");

            // txn intentionally NOT committed or aborted — crash here.
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        other => panic!("Unknown NOXU_CRASH_MODE: {other}"),
    }
}

fn flag(dir: &Path, name: &str) {
    fs::write(dir.join(name), b"ok")
        .unwrap_or_else(|e| panic!("write flag {name}: {e}"));
}
