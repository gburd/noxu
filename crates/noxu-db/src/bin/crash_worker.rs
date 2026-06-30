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

    // DST: if a seed is provided, install the storage-fault layer over posio
    // before opening the env.  This drops not-yet-synced bytes at a
    // seed-chosen write (torn write / dropped fsync) by exiting the process,
    // or injects ENOSPC / corruption — a byte-precise, reproducible power
    // loss the SIGKILL sweep cannot do.  Absent NOXU_DST_SEED the fault layer
    // stays inactive and the worker behaves exactly as before.
    if let Ok(seed_str) = env::var("NOXU_DST_SEED") {
        let seed: u64 = seed_str
            .parse()
            .unwrap_or_else(|_| panic!("NOXU_DST_SEED must be a u64"));
        noxu_log::faultdisk::install_seed(seed);
    }

    let env_config = EnvironmentConfig::new(dir.clone())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).expect("open env");
    let db_config =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
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

        // open_txn_spanning_checkpoint:
        //
        // Phase 1: write 20 committed keys (b"committed_NNN"), signal
        //          "phase1_done".
        // Phase 2: begin a transaction, write 10 keys (b"open_NNN") BEFORE a
        //          forced checkpoint (so CkptStart > the txn's first LN),
        //          signal "open_txn_ready", loop until killed (txn never
        //          committed or aborted).
        //
        // After SIGKILL, recovery must see all 20 committed keys and NONE of
        // the 10 open-txn keys.  Current recovery scans the whole log from the
        // start, so it sees the open txn's LNs and the undo pass reverts them.
        // This test locks in that invariant: any future recovery scan-range
        // optimization (P-2) that started from CkptStart without accounting
        // for the open txn's earlier first-LSN would surface the uncommitted
        // keys and fail this test.
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

            // Force a checkpoint AFTER the open txn has written its LNs, so
            // CkptStart > the txn's firstLoggedLsn.
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("forced checkpoint");

            // Signal open + checkpoint done; the parent will SIGKILL us.
            flag(&dir, "open_txn_ready");

            // txn intentionally NOT committed or aborted — crash here.
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // aborted_then_committed_same_key:
        //
        // T1 inserts key "K" = "v1" and ABORTS (clean abort record written).
        // T3 then inserts the SAME key "K" = "v3" and COMMITS.
        // T2 writes an unrelated key and stays OPEN (active at crash) so the
        // recovery undo pass actually runs (it is skipped entirely when no txn
        // is active at crash time).
        // Signal ready; the parent SIGKILLs us.
        //
        // After recovery, K MUST equal "v3": T3's committed write must not be
        // clobbered when the undo pass reverts T1's aborted write of the same
        // key. This is the recovery currency-check (JE BIN.recoverRecord)
        // scenario.
        "aborted_then_committed_same_key" => {
            let k = DatabaseEntry::from_bytes(b"K");

            let t1 = env.begin_transaction(None).expect("begin t1");
            db.put(Some(&t1), &k, &DatabaseEntry::from_bytes(b"v1"))
                .expect("t1 put");
            t1.abort().expect("t1 abort");

            let t3 = env.begin_transaction(None).expect("begin t3");
            db.put(Some(&t3), &k, &DatabaseEntry::from_bytes(b"v3"))
                .expect("t3 put");
            t3.commit().expect("t3 commit");

            // T2 stays open (active at crash) so the undo pass is not skipped.
            let t2 = env.begin_transaction(None).expect("begin t2");
            db.put(
                Some(&t2),
                &DatabaseEntry::from_bytes(b"other"),
                &DatabaseEntry::from_bytes(b"x"),
            )
            .expect("t2 put");
            // Leak the handle so no abort/commit record is written on drop.
            std::mem::forget(t2);

            flag(&dir, "abort_commit_ready");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // in_redo_bin_delta_reconstituted:
        //
        // Write keys to fill a BIN, then force a FULL checkpoint (BINs logged
        // as full entries).  Then modify a few keys (making the BIN dirty) and
        // force another checkpoint (which may write BIN-deltas).  SIGKILL.
        //
        // After recovery all keys must be present.  The Stage 3 BIN-delta
        // reconstitution path (DRIFT-10) must handle the delta correctly.
        "in_redo_bin_delta_reconstituted" => {
            use noxu_db::CheckpointConfig;

            // Phase 1: write 20 keys and force first full checkpoint.
            for i in 0u32..20 {
                let k = i.to_be_bytes();
                let key = DatabaseEntry::from_bytes(&k);
                let val = DatabaseEntry::from_bytes(b"v1");
                db.put(None, &key, &val).expect("put v1");
            }
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("full checkpoint");

            // Phase 2: modify a subset of keys (dirty some BIN slots)
            // and force a second checkpoint (may produce BIN-deltas).
            for i in 0u32..5 {
                let k = i.to_be_bytes();
                let key = DatabaseEntry::from_bytes(&k);
                let val = DatabaseEntry::from_bytes(b"v2");
                db.put(None, &key, &val).expect("put v2");
            }
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("delta checkpoint");

            flag(&dir, "phase1_done");

            // Hang until killed.
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        //
        // After recovery:
        //   - All 50 pre-checkpoint keys MUST be present (via IN-redo OR LN-redo).
        //   - The 1 post-checkpoint key MUST also be present (via LN-redo).
        //   - A marker file records how many INs were replayed (checked by test).
        "in_redo_bin_flushed_by_checkpoint" => {
            use noxu_db::CheckpointConfig;

            // Phase 1: write 50 committed keys.
            for i in 0u32..50 {
                let k = i.to_be_bytes();
                let key = DatabaseEntry::from_bytes(&k);
                let val = DatabaseEntry::from_bytes(b"before_ckpt");
                db.put(None, &key, &val).expect("put");
            }

            // Force a checkpoint so the BINs are flushed to the WAL.
            // At this point, the 50 keys are represented in logged BIN entries.
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("forced checkpoint");

            // Write 1 post-checkpoint key.
            let post_key = DatabaseEntry::from_bytes(b"post_ckpt");
            let post_val = DatabaseEntry::from_bytes(b"after_ckpt");
            db.put(None, &post_key, &post_val).expect("put post");

            flag(&dir, "phase1_done");

            // Hang until killed.
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }
        // Part-3 acceptance test (DRIFT-3/7 fix): exercise the file-flip path.
        //
        // Writes enough data to force a file flip (tiny max_file_size), commits
        // all records, signals readiness, then loops.  The parent SIGKILLs right
        // after the flip, and recovery must find all committed records in BOTH
        // the old and new files.
        "file_flip" => {
            // Re-open with a tiny max-file-size so a flip is forced after a
            // small number of writes.
            drop(db);
            drop(env);
            let env_config = EnvironmentConfig::new(dir.clone())
                .with_allow_create(true)
                .with_transactional(true)
                .with_log_file_max_bytes(65_536); // 64 KiB — forces flip quickly
            let env =
                noxu_db::Environment::open(env_config).expect("open env flip");
            let db_config = DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true);
            let db = env
                .open_database(None, "test", &db_config)
                .expect("open db flip");

            // Write enough records to guarantee at least one file flip.
            for i in 0u32..200 {
                let txn = env.begin_transaction(None).expect("begin txn");
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(&[0u8; 256]); // 256-byte values
                db.put(Some(&txn), &key, &val).expect("put");
                txn.commit().expect("commit");
            }

            flag(&dir, "flip_committed");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ----------------------------------------------------------------
        // concurrent_commit_sync: N threads each CommitSync-commit a disjoint
        // range of keys, barrier-synchronised so their fsync requests race and
        // exercise the group-commit coalescing path (the leader/waiter fix).
        // After every committed transaction returns (CommitSync => durable),
        // raise `concurrent_committed`; the parent SIGKILLs us here.  Recovery
        // must find EVERY committed key (no committed txn lost despite the
        // coalesced single-fsync-serves-many ordering).
        // ----------------------------------------------------------------
        "concurrent_commit_sync" => {
            use std::sync::{Arc, Barrier};
            const THREADS: u32 = 8;
            const KEYS_PER_THREAD: u32 = 50;
            let env = Arc::new(env);
            let db = Arc::new(db);
            let barrier = Arc::new(Barrier::new(THREADS as usize));
            let handles: Vec<_> = (0..THREADS)
                .map(|tid| {
                    let env = Arc::clone(&env);
                    let db = Arc::clone(&db);
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        barrier.wait();
                        for k in 0..KEYS_PER_THREAD {
                            // Disjoint key space per thread: tid * 1000 + k.
                            let id = tid * 1000 + k;
                            let txn =
                                env.begin_transaction(None).expect("begin txn");
                            let key =
                                DatabaseEntry::from_bytes(&id.to_be_bytes());
                            let val = DatabaseEntry::from_bytes(b"committed");
                            db.put(Some(&txn), &key, &val).expect("put");
                            // Default durability is COMMIT_SYNC => real fsync.
                            txn.commit().expect("commit");
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("join worker thread");
            }
            // All committed transactions have returned => all durable.
            flag(&dir, "concurrent_committed");
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
