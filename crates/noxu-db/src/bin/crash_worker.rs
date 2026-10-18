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
    let dst_seed = env::var("NOXU_DST_SEED").ok().map(|s| {
        s.parse::<u64>()
            .unwrap_or_else(|_| panic!("NOXU_DST_SEED must be a u64"))
    });
    if let Some(seed) = dst_seed {
        noxu_log::faultdisk::install_seed(seed);
    }

    // Build the env config.  Under DST (a seed is set) the fault layer fires
    // at a *write index* (`FaultController::target_write`) counted over EVERY
    // posio write from EVERY thread (`faultdisk::WRITE_COUNT` is one global
    // atomic).  A background daemon (in practice the checkpointer) can issue a
    // log write CONCURRENTLY with this worker's foreground commits, so with
    // daemons running the write that lands at `target_write` -- and how many
    // committed transactions it batches -- can depend on thread scheduling,
    // not on the seed alone.  That makes the torn / dropped-fsync point (and
    // therefore the recovered state) nondeterministic for a fixed seed on a
    // small fraction of runs (empirically ~0.08% of crash points land in a
    // checkpointer write; cleaner/evictor/compressor/verifier never write at
    // this workload size), which is enough to break the DST headline property
    // "same seed => same recovered state"
    // (dst_crash_sweep::dst_same_seed_reproduces_exactly).
    //
    // Only the DETERMINISM test needs a byte-stable input: it runs the same
    // seed twice and compares the two recovered states.  The *sweep* only
    // asserts the recovery INVARIANTS (committed-prefix, no-leak, total
    // recovery), which hold regardless of where the crash lands -- so the
    // sweep keeps daemons ON (realistic concurrency, and it retains the small
    // amount of mid-checkpoint crash coverage the fault layer provides).  We
    // therefore disable the uncontrolled daemons ONLY when the caller opts in
    // via `NOXU_DST_QUIET=1` (set by `dst_same_seed_reproduces_exactly`), and
    // keep them default-on for the sweep and for non-DST crash tests.  Tests
    // that specifically need a checkpoint still force one explicitly via
    // `env.checkpoint(..with_force(true))`.
    let dst_quiet =
        env::var("NOXU_DST_QUIET").map(|v| v == "1").unwrap_or(false);
    let mut env_config = EnvironmentConfig::new(dir.clone())
        .with_allow_create(true)
        .with_transactional(true);
    if dst_quiet {
        env_config = env_config
            .with_run_checkpointer(false)
            .with_run_cleaner(false)
            .with_run_evictor(false)
            .with_run_in_compressor(false)
            .with_run_verifier(false);
    }
    // FID-REC-3 witness: force deterministic eviction of a dirty
    // deferred-write BIN with NO checkpoint running.  A tiny cache plus
    // an explicit `evict_memory()` call (below) makes the evictor log the
    // unsynced DW BIN; the checkpointer is OFF so
    // `get_eviction_provisional` sees no checkpoint in progress -- exactly
    // the case the DW branch (JE DirtyINMap case 1) must still make
    // provisional.
    if mode == "deferred_write_nosync_evict_unsynced" {
        env_config = env_config
            .with_cache_size(1024 * 1024)
            .with_run_checkpointer(false);
    }
    // R-FIRSTLSN witness: a tiny log file so a handful of writes roll many
    // files, and all daemons OFF so the mode drives cleaning / checkpointing
    // explicitly WHILE the victim txn is open.  minAge=1 and a high
    // minUtilization keep the forced-clean path simple and deterministic.
    if mode.starts_with("firstlsn_") {
        env_config = env_config
            .with_log_file_max_bytes(1_000_000)
            .with_run_cleaner(false)
            .with_run_checkpointer(false)
            .with_run_evictor(false)
            .with_run_in_compressor(false)
            .with_cleaner_min_age(1)
            .with_cleaner_min_utilization(90);
    }
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
                db.put_in(&txn, &key, &val).expect("put");
                txn.commit().expect("commit");
            }
            flag(&dir, "phase1_done");

            // Phase 2: one uncommitted transaction — parent kills us here.
            let txn = env.begin_transaction(None).expect("begin txn");
            for i in 1000u32..1050 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"uncommitted");
                db.put_in(&txn, &key, &val).expect("put");
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
            db.put_in(&txn, &sentinel_key, &sentinel_val)
                .expect("put sentinel");
            txn.commit().expect("commit sentinel");
            flag(&dir, "sentinel_committed");

            // Uncommitted batch — parent kills us here.
            let txn = env.begin_transaction(None).expect("begin txn");
            for i in 0u32..50 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"uncommitted");
                db.put_in(&txn, &key, &val).expect("put");
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
                db.put_in(&txn, &key, &val).expect("put T1");
            }
            txn.commit().expect("commit T1");
            flag(&dir, "t1_done");

            // T2: uncommitted — parent kills us here.
            let txn = env.begin_transaction(None).expect("begin T2");
            for i in 100u32..125 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"t2");
                db.put_in(&txn, &key, &val).expect("put T2");
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
                db.put_in(&txn, &key, &val).expect("put");
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
                db.put_in(&txn, &key, &val).expect("put");
                txn.commit().expect("commit");
            }
            flag(&dir, "phase1_done");

            // Phase 2: open transaction (keys written BEFORE the checkpoint).
            let txn = env.begin_transaction(None).expect("begin txn");
            for i in 0u32..10 {
                let k = format!("open_{i:03}");
                let key = DatabaseEntry::from_bytes(k.as_bytes());
                let val = DatabaseEntry::from_bytes(b"should_not_survive");
                db.put_in(&txn, &key, &val).expect("put open key");
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
            db.put_in(&t1, &k, DatabaseEntry::from_bytes(b"v1"))
                .expect("t1 put");
            t1.abort().expect("t1 abort");

            let t3 = env.begin_transaction(None).expect("begin t3");
            db.put_in(&t3, &k, DatabaseEntry::from_bytes(b"v3"))
                .expect("t3 put");
            t3.commit().expect("t3 commit");

            // T2 stays open (active at crash) so the undo pass is not skipped.
            let t2 = env.begin_transaction(None).expect("begin t2");
            db.put_in(
                &t2,
                DatabaseEntry::from_bytes(b"other"),
                DatabaseEntry::from_bytes(b"x"),
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
                db.put(&key, &val).expect("put v1");
            }
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("full checkpoint");

            // Phase 2: modify a subset of keys (dirty some BIN slots)
            // and force a second checkpoint (may produce BIN-deltas).
            for i in 0u32..5 {
                let k = i.to_be_bytes();
                let key = DatabaseEntry::from_bytes(&k);
                let val = DatabaseEntry::from_bytes(b"v2");
                db.put(&key, &val).expect("put v2");
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
                db.put(&key, &val).expect("put");
            }

            // Force a checkpoint so the BINs are flushed to the WAL.
            // At this point, the 50 keys are represented in logged BIN entries.
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("forced checkpoint");

            // Write 1 post-checkpoint key.
            let post_key = DatabaseEntry::from_bytes(b"post_ckpt");
            let post_val = DatabaseEntry::from_bytes(b"after_ckpt");
            db.put(&post_key, &post_val).expect("put post");

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
                db.put_in(&txn, &key, &val).expect("put");
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
                            db.put_in(&txn, &key, &val).expect("put");
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

        // ----------------------------------------------------------------
        // deferred_write_nosync: JE DeferredWriteTest.testRecoverNoSync /
        // testCloseOpenNoSync.  A deferred-write DB's records are DURABLE only
        // after an explicit sync(); records written after the last sync are
        // LOST on an abnormal (crash) close.
        //
        // Phase 1 (synced): write keys 0..50, then db.sync() -> durable.
        // Phase 2 (unsynced): write keys 1000..1050, NO sync -> volatile.
        // Parent SIGKILLs after `dw_unsynced_started`.
        //
        // After recovery: keys 0..50 present ("synced"); keys 1000..1050 gone.
        //
        // `_evict` variant: before phase 2, write a large filler batch and
        // force eviction so the synced BINs are flushed from cache; the
        // recovery path must still restore exactly the synced set.
        // root_split_sync: build a MULTI-LEVEL deferred-write tree (a root
        // split) with distinct keys, db.sync() to make it durable, then write
        // an unsynced batch and loop until SIGKILL.  Validates that a split
        // root (upper IN) logged by syncDatabase recovers WITHOUT a CkptEnd.
        "root_split_sync" => {
            let dw_cfg = DatabaseConfig::new()
                .with_allow_create(true)
                .with_deferred_write(true);
            let dw =
                env.open_database(None, "dw", &dw_cfg).expect("open dw db");
            // Enough keys with sizeable values to force several BINs under an
            // upper-IN root.
            let val = vec![7u8; 512];
            for i in 0u32..300 {
                dw.put(
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(&val),
                )
                .expect("put synced");
            }
            dw.sync().expect("sync split tree");
            flag(&dir, "dw_phase1_synced");
            for i in 1000u32..1050 {
                dw.put(
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(b"unsynced"),
                )
                .expect("put unsynced");
            }
            flag(&dir, "dw_unsynced_started");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "deferred_write_nosync_evict_unsynced" => {
            // FID-REC-3 (S2): a deferred-write DB only guarantees what was
            // made durable by an explicit sync().  We make a tree durable
            // (root + upper INs + BINs on disk), then OVERWRITE every key
            // without sync() and without a checkpoint, and force eviction
            // of those now-dirty BINs.  Because the tree path is already
            // durable, recovery can REACH the evicted BINs via their
            // parent slots.  On 81b2f048 the evicted BINs were logged
            // Provisional::No, so recovery redid the newer (unsynced)
            // image and the overwrite survived (RED).  With the DW branch
            // (JE DirtyINMap case 1) they are logged Provisional::Yes and
            // recovery skips them, so every key keeps its last SYNCED
            // value.
            let dw_cfg = DatabaseConfig::new()
                .with_allow_create(true)
                .with_deferred_write(true);
            let dw =
                env.open_database(None, "dw", &dw_cfg).expect("open dw db");

            // Phase 1: write keys 0..2000 = "synced", then sync() so the
            // whole tree (multi-level: root + upper INs + many BINs) is
            // durable on disk.
            for i in 0u32..2000 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"synced");
                dw.put(&key, &val).expect("put synced");
            }
            dw.sync().expect("sync phase 1 (make tree durable)");
            flag(&dir, "dw_phase1_synced");

            // Phase 2 (UNSYNCED): OVERWRITE every key with "unsynced".
            // No sync(), no checkpoint.
            for i in 0u32..2000 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"unsynced");
                dw.put(&key, &val).expect("put unsynced overwrite");
            }
            // Force eviction so the dirty (overwritten) BINs are logged to
            // the WAL (JE: the evictor logs each dirty BIN; for a DW DB with
            // the FID-REC-3 fix each is logged Provisional::Yes).
            for _ in 0..8 {
                let _ = env.evict_memory();
            }
            flag(&dir, "dw_unsynced_started");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "deferred_write_nosync" | "deferred_write_nosync_evict" => {
            let evict = mode == "deferred_write_nosync_evict";
            let dw_cfg = DatabaseConfig::new()
                .with_allow_create(true)
                .with_deferred_write(true);
            let dw =
                env.open_database(None, "dw", &dw_cfg).expect("open dw db");

            // Phase 1: synced batch.
            for i in 0u32..50 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"synced");
                dw.put(&key, &val).expect("put synced");
            }
            dw.sync().expect("sync phase 1");
            if evict {
                // Force cache pressure: a filler batch + a checkpoint-free
                // eviction nudge.  (The synced phase-1 data is already on disk;
                // the filler is itself synced so recovery has a clean base.)
                for i in 100_000u32..101_000 {
                    let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                    let val = DatabaseEntry::from_bytes(b"filler");
                    dw.put(&key, &val).expect("put filler");
                }
                dw.sync().expect("sync filler");
            }
            flag(&dir, "dw_phase1_synced");

            // Phase 2: unsynced batch — parent kills us here.
            for i in 1000u32..1050 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"unsynced");
                dw.put(&key, &val).expect("put unsynced");
            }
            flag(&dir, "dw_unsynced_started");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "dw_sync_then_remove" => {
            // Task-3 recovery-correctness witness: a database created and made
            // durable with db.sync(), then REMOVED, then crashed, must stay
            // REMOVED after recovery — the "instantiate a tree for every db in
            // dirty_ins" rule (BUG-DW-SYNC-1) must NOT resurrect a removed DB.
            // JE skips a removed DB's stale logged INs because
            // `dbMapTree.getDb(dbId)` returns null (RecoveryManager.java:1091).
            let dw_cfg = DatabaseConfig::new()
                .with_allow_create(true)
                .with_deferred_write(true);
            let dw =
                env.open_database(None, "dw", &dw_cfg).expect("open dw db");
            for i in 0u32..50 {
                dw.put(
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(b"synced"),
                )
                .expect("put");
            }
            // sync() logs the DW tree's BINs/INs -> db_id lands in dirty_ins.
            dw.sync().expect("sync dw");
            dw.close().expect("close dw");
            // Remove the DB inside a transaction and COMMIT (the commit flushes
            // the NameLN-delete durably, so the removal survives the crash).
            let txn = env.begin_transaction(None).expect("begin txn");
            env.remove_database(Some(&txn), "dw").expect("remove dw");
            txn.commit().expect("commit remove");
            flag(&dir, "dw_phase1_synced");
            flag(&dir, "dw_unsynced_started");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ----------------------------------------------------------------
        // BUG-DDL-DURABILITY-1 txnal DDL durability witnesses.
        //
        // Each mode performs a TRANSACTIONAL remove/rename/truncate, then
        // either COMMITs or ABORTs, then signals `ddl_done` and loops.  NO
        // db.sync() and NO checkpoint: the ONLY path to durability is the
        // commit's own SYNC fsync (JE Txn.logCommitEntry, Txn.java:983-986).
        // The parent SIGKILLs after `ddl_done`.  The committed DDL op must be
        // durable across the crash; the aborted one must leave the db intact.
        //
        // JE: the NameLN delete/rename is logged as an ordinary transactional
        // LN inside the user txn (DbTree.doRemoveDb, DbTree.java:1182-1206);
        // the physical MapLN cleanup is deferred to Txn.deletedDatabases
        // (Txn.java:1485-1489) and runs post-commit only.
        // ----------------------------------------------------------------
        "txnal_remove_commit" | "txnal_remove_abort" => {
            let commit = mode == "txnal_remove_commit";
            // Seed one committed record so the db really exists on disk.
            db.put(
                DatabaseEntry::from_bytes(&[0u8; 10]),
                DatabaseEntry::from_bytes(b"rec"),
            )
            .expect("seed put");
            db.close().expect("close before remove");
            let txn = env.begin_transaction(None).expect("begin");
            env.remove_database(Some(&txn), "test").expect("remove under txn");
            if commit {
                txn.commit().expect("commit remove");
            } else {
                txn.abort().expect("abort remove");
            }
            flag(&dir, "ddl_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "txnal_rename_commit" | "txnal_rename_abort" => {
            let commit = mode == "txnal_rename_commit";
            db.put(
                DatabaseEntry::from_bytes(&[0u8; 10]),
                DatabaseEntry::from_bytes(b"rec"),
            )
            .expect("seed put");
            db.close().expect("close before rename");
            let txn = env.begin_transaction(None).expect("begin");
            env.rename_database(Some(&txn), "test", "renamed")
                .expect("rename under txn");
            if commit {
                txn.commit().expect("commit rename");
            } else {
                txn.abort().expect("abort rename");
            }
            flag(&dir, "ddl_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "txnal_truncate_commit" | "txnal_truncate_abort" => {
            let commit = mode == "txnal_truncate_commit";
            // Seed several committed records so a surviving truncate is
            // observably empty and a surviving abort keeps all records.
            for i in 0u32..20 {
                db.put(
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(b"rec"),
                )
                .expect("seed put");
            }
            db.close().expect("close before truncate");
            let txn = env.begin_transaction(None).expect("begin");
            let n = env
                .truncate_database(Some(&txn), "test")
                .expect("truncate under txn");
            assert_eq!(n, 20, "truncate returnCount");
            if commit {
                txn.commit().expect("commit truncate");
            } else {
                txn.abort().expect("abort truncate");
            }
            flag(&dir, "ddl_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ----------------------------------------------------------------
        // R-FIRSTLSN witnesses (candidate S1 data loss).
        //
        // TxnManager::get_first_active_lsn() is fed ONLY by CursorImpl's LN
        // write path (update_first_lsn).  A txn whose FIRST log entry is a
        // transactional NameLN (create/rename/remove/truncate DB inside a
        // txn) never reports that LSN to the TxnManager, so the cleaner's
        // first-active-txn-file clamp (cleaner.rs:949) under-reports and the
        // log file holding the OPEN txn's provisional NameLNTxn is
        // selectable+deletable.  relog_live_catalog only re-logs COMMITTED
        // (name_map) catalog entries, not an open txn's pending_names entry,
        // so nothing else protects the file.  We delete that file WHILE the
        // txn is open, COMMIT, then the parent SIGKILLs: recovery cannot find
        // the NameLN and the committed DB (or its data) is lost.
        //
        // JE: every transactional LN logs txn.addLogInfo(lsn) in
        // LNLogEntry.postLogWork (LNLogEntry.java:~890-898) -> Txn.addLogInfo
        // sets firstLoggedLsn (Txn.java:1443-1456) -> TxnManager.getFirstActiveLsn
        // (:355-366) sees it, so the clamp protects the whole active-txn
        // window including a NameLN.
        //
        // Shared helper flow for every firstlsn_* mode is inlined per-arm so
        // each can vary the DDL op (create/rename/remove/truncate) and the
        // commit/abort decision.  `db` ("test") is the filler used to roll
        // files and generate obsolete bytes.
        // ----------------------------------------------------------------
        "firstlsn_create_commit" | "firstlsn_create_abort" => {
            let commit = mode == "firstlsn_create_commit";
            firstlsn_roll_files(&db, &env, 40);
            // Open txn T; create the victim DB inside it (provisional
            // NameLNTxn).  Keep T open; do NOT write a data LN through T so
            // the TxnManager never learns T's first LSN.
            let txn = env.begin_transaction(None).expect("begin victim txn");
            let victim = env
                .open_database(Some(&txn), "victim", &db_config)
                .expect("create victim under txn");
            // Roll files PAST the NameLNTxn file and delete it via the
            // two-checkpoint barrier WHILE T is open.
            firstlsn_roll_files(&db, &env, 60);
            firstlsn_force_delete_barrier(&env);
            victim.close().expect("close victim handle before commit");
            if commit {
                txn.commit().expect("commit victim create");
            } else {
                txn.abort().expect("abort victim create");
            }
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "firstlsn_remove_commit" | "firstlsn_remove_abort" => {
            // Victim DB is COMMITTED first and has a durable NameLN; then an
            // open txn T logs the NameLN-delete tombstone as its first entry.
            // T's first LSN is unreported, so the file holding the tombstone
            // (and, if cleaned, surrounding data) is unprotected.
            let commit = mode == "firstlsn_remove_commit";
            // Create + commit the victim with one record.
            {
                let vtxn =
                    env.begin_transaction(None).expect("begin victim create");
                let v = env
                    .open_database(Some(&vtxn), "victim", &db_config)
                    .expect("create victim");
                v.put_in(
                    &vtxn,
                    DatabaseEntry::from_bytes(b"k"),
                    DatabaseEntry::from_bytes(b"v"),
                )
                .expect("seed victim rec");
                v.close().expect("close victim");
                vtxn.commit().expect("commit victim create");
            }
            // Force a checkpoint so the committed victim NameLN is in name_map
            // (relog_live_catalog will protect THAT copy; the point of the
            // bug is the open-txn TOMBSTONE below, whose file is unprotected).
            {
                use noxu_db::CheckpointConfig;
                env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                    .expect("ckpt after victim create");
            }
            firstlsn_roll_files(&db, &env, 40);
            let txn = env.begin_transaction(None).expect("begin remove txn");
            env.remove_database(Some(&txn), "victim")
                .expect("remove victim under txn");
            firstlsn_roll_files(&db, &env, 60);
            firstlsn_force_delete_barrier(&env);
            if commit {
                txn.commit().expect("commit remove");
            } else {
                txn.abort().expect("abort remove");
            }
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "firstlsn_rename_commit" | "firstlsn_rename_abort" => {
            let commit = mode == "firstlsn_rename_commit";
            {
                let vtxn =
                    env.begin_transaction(None).expect("begin victim create");
                let v = env
                    .open_database(Some(&vtxn), "victim", &db_config)
                    .expect("create victim");
                v.put_in(
                    &vtxn,
                    DatabaseEntry::from_bytes(b"k"),
                    DatabaseEntry::from_bytes(b"v"),
                )
                .expect("seed victim rec");
                v.close().expect("close victim");
                vtxn.commit().expect("commit victim create");
            }
            {
                use noxu_db::CheckpointConfig;
                env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                    .expect("ckpt after victim create");
            }
            firstlsn_roll_files(&db, &env, 40);
            let txn = env.begin_transaction(None).expect("begin rename txn");
            env.rename_database(Some(&txn), "victim", "victim2")
                .expect("rename victim under txn");
            firstlsn_roll_files(&db, &env, 60);
            firstlsn_force_delete_barrier(&env);
            if commit {
                txn.commit().expect("commit rename");
            } else {
                txn.abort().expect("abort rename");
            }
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "firstlsn_truncate_commit" | "firstlsn_truncate_abort" => {
            let commit = mode == "firstlsn_truncate_commit";
            {
                let vtxn =
                    env.begin_transaction(None).expect("begin victim create");
                let v = env
                    .open_database(Some(&vtxn), "victim", &db_config)
                    .expect("create victim");
                for i in 0u32..20 {
                    v.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(b"rec"),
                    )
                    .expect("seed victim rec");
                }
                v.close().expect("close victim");
                vtxn.commit().expect("commit victim create");
            }
            {
                use noxu_db::CheckpointConfig;
                env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                    .expect("ckpt after victim create");
            }
            firstlsn_roll_files(&db, &env, 40);
            let txn = env.begin_transaction(None).expect("begin truncate txn");
            let n = env
                .truncate_database(Some(&txn), "victim")
                .expect("truncate victim under txn");
            assert_eq!(n, 20, "truncate returnCount");
            firstlsn_roll_files(&db, &env, 60);
            firstlsn_force_delete_barrier(&env);
            if commit {
                txn.commit().expect("commit truncate");
            } else {
                txn.abort().expect("abort truncate");
            }
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ----------------------------------------------------------------
        // R-RELOG-PENDING-DDL witnesses (candidate S1 data loss).
        //
        // While a remove/rename txn T is OPEN, the victim's COMMITTED NameLN is
        // still in `name_map` (the physical removal is deferred to T's commit
        // callback).  relog_live_catalog runs at the START OF EVERY checkpoint
        // and would re-log a fresh NON-TRANSACTIONAL live NameLN for the victim
        // AFTER T's delete tombstone.  Recovery's latest-NameLN-per-name rule
        // then clears the pending delete and RESURRECTS the removed / renamed-
        // away name after commit + crash.  Distinct from R-FIRSTLSN: the clamp
        // fix does not touch relog ordering.
        //
        // JE never re-logs a NameLN for relocation; its cleaner migrates a live
        // NameLN under a non-blocking READ lock on the record
        // (FileProcessor.processFoundLN, FileProcessor.java:1354), and a NameLN
        // write-locked by the pending DDL txn is DENIED (:1358) and deferred to
        // the pending-LN queue (file retained), so the relocation respects the
        // DDL txn.  Noxu's fix skips a pending-DDL name in relog and keeps a
        // pre-tombstone live copy alive via the first-active clamp.
        //
        // The rename mode writes DATA into the victim before the rename and
        // asserts (post-crash) the NEW name carries that data.
        // ----------------------------------------------------------------
        "relog_remove_commit" => {
            {
                let vtxn =
                    env.begin_transaction(None).expect("begin victim create");
                let v = env
                    .open_database(Some(&vtxn), "victim", &db_config)
                    .expect("create victim");
                v.put_in(
                    &vtxn,
                    DatabaseEntry::from_bytes(b"k"),
                    DatabaseEntry::from_bytes(b"v"),
                )
                .expect("seed victim rec");
                v.close().expect("close victim");
                vtxn.commit().expect("commit victim create");
            }
            {
                use noxu_db::CheckpointConfig;
                env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                    .expect("ckpt after victim create");
            }
            firstlsn_roll_files(&db, &env, 40);
            let txn = env.begin_transaction(None).expect("begin remove txn");
            env.remove_database(Some(&txn), "victim")
                .expect("remove victim under txn");
            firstlsn_roll_files(&db, &env, 60);
            firstlsn_force_delete_barrier(&env);
            txn.commit().expect("commit remove");
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "relog_rename_commit" => {
            {
                let vtxn =
                    env.begin_transaction(None).expect("begin victim create");
                let v = env
                    .open_database(Some(&vtxn), "victim", &db_config)
                    .expect("create victim");
                for i in 0u32..8 {
                    v.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(b"payload"),
                    )
                    .expect("seed victim rec");
                }
                v.close().expect("close victim");
                vtxn.commit().expect("commit victim create");
            }
            {
                use noxu_db::CheckpointConfig;
                env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                    .expect("ckpt after victim create");
            }
            firstlsn_roll_files(&db, &env, 40);
            let txn = env.begin_transaction(None).expect("begin rename txn");
            env.rename_database(Some(&txn), "victim", "victim2")
                .expect("rename victim under txn");
            firstlsn_roll_files(&db, &env, 60);
            firstlsn_force_delete_barrier(&env);
            txn.commit().expect("commit rename");
            flag(&dir, "firstlsn_done");
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

/// R-FIRSTLSN helper: roll several small log files with committed data,
/// overwriting a fixed key range so earlier files accumulate obsolete bytes.
fn firstlsn_roll_files(
    db: &noxu_db::Database,
    env: &noxu_db::Environment,
    gens: u32,
) {
    let value = vec![0x5Au8; 4096];
    for g in 0..gens {
        let txn = env.begin_transaction(None).expect("begin filler txn");
        for k in 0..40u32 {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(
                    &(g.wrapping_mul(40) + k).to_be_bytes(),
                ),
                DatabaseEntry::from_bytes(&value),
            )
            .expect("filler put");
        }
        txn.commit().expect("commit filler txn");
    }
}

/// R-FIRSTLSN helper: drive the two-checkpoint deletion barrier with forced
/// cleaning passes.  Runs several (clean, checkpoint) rounds so BeingCleaned ->
/// Cleaned -> Checkpointed -> safe_to_delete -> deleted.  Called WHILE the
/// victim txn is open, so relog_live_catalog (name_map only) does not re-log
/// the open txn's provisional NameLNTxn.
fn firstlsn_force_delete_barrier(env: &noxu_db::Environment) {
    use noxu_db::CheckpointConfig;
    let force = CheckpointConfig::new().with_force(true);
    for _ in 0..6 {
        let _ = env.clean_log_forced().expect("forced clean");
        env.checkpoint(Some(&force)).expect("barrier checkpoint");
        let _ = env.clean_log_forced().expect("forced clean");
        env.checkpoint(Some(&force)).expect("barrier checkpoint");
    }
}
