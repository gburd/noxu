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
    if mode.starts_with("firstlsn_")
        || mode.starts_with("xarec_")
        || mode.starts_with("xaddl_")
    {
        env_config = env_config
            .with_log_file_max_bytes(1_000_000)
            .with_run_cleaner(false)
            .with_run_checkpointer(false)
            .with_run_evictor(false)
            .with_run_in_compressor(false)
            .with_cleaner_min_age(1)
            .with_cleaner_min_utilization(90);
    }
    // Witness-faithful read-only probe: open EXACTLY like the witness final
    // reopen (allow_create=false, default daemons incl. checkpointer ON), list
    // names, then exit.  Does NOT open "test".  This isolates whether the
    // post-recovery checkpoint relog (pending_ddl_names empty after recovery)
    // resurrects a committed-removed/renamed name on a read-only consumer.
    // Simulates a real XA transaction manager's recovery scan: a CLEAN
    // open (allow_create=false, checkpointer ON) that surfaces the in-doubt
    // branch (xa_recover), waits for a post-recovery checkpoint, then closes
    // cleanly.  This is the standard "reopen; xa_recover(); resolve later"
    // flow.  Exits without crash.
    if mode == "xaddl_recover_open" {
        let ro = noxu_db::Environment::open(
            noxu_db::EnvironmentConfig::new(dir.clone())
                .with_allow_create(false)
                .with_transactional(true),
        )
        .expect("recover-open");
        assert_eq!(
            ro.recovered_prepared_txns().len(),
            1,
            "xa_recover() must surface the in-doubt branch"
        );
        // Force a checkpoint explicitly (a real long-lived env would checkpoint
        // between recover and resolve; the default checkpointer would too).
        use noxu_db::CheckpointConfig;
        ro.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .expect("post-recover checkpoint");
        ro.close().ok();
        flag(&dir, "firstlsn_done");
        return;
    }
    // R-XADDL-711 fuzz recover: like xaddl_recover_open but for a MULTI-branch
    // plan -- surface >=1 in-doubt branch (no fixed count), checkpoint, close.
    if mode == "xaddl_fuzz_recover" {
        let ro = noxu_db::Environment::open(
            noxu_db::EnvironmentConfig::new(dir.clone())
                .with_allow_create(false)
                .with_transactional(true),
        )
        .expect("fuzz recover-open");
        assert!(
            !ro.recovered_prepared_txns().is_empty(),
            "xa_recover() must surface at least one in-doubt branch"
        );
        use noxu_db::CheckpointConfig;
        ro.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .expect("post-recover checkpoint");
        ro.close().ok();
        flag(&dir, "firstlsn_done");
        return;
    }
    if mode == "xaddl_witness_probe" {
        let ro = noxu_db::Environment::open(
            noxu_db::EnvironmentConfig::new(dir.clone())
                .with_allow_create(false)
                .with_transactional(true),
        )
        .expect("witness probe open");
        // Give any post-recovery checkpoint a moment to run (as it would in
        // the witness between open and the names read).
        let settle_ms: u64 = std::env::var("XADDL_SETTLE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        thread::sleep(Duration::from_millis(settle_ms));
        let names = ro.database_names().expect("database_names");
        eprintln!("XADDL_WITNESS_PROBE names={names:?}");
        ro.close().ok();
        flag(&dir, "firstlsn_done");
        return;
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

        // ----------------------------------------------------------------
        // XA-RECOVERED (candidate S1): a recovered prepared XA txn is resolved
        // via write_txn_commit_for_recovered / apply_recovered_prepared_lns,
        // which are NOT routed through the TxnManager (not in all_txns), so the
        // cleaner's first-active-txn clamp does NOT protect the prepared LNs'
        // file, and the prepared LNs were never redone into a tree slot
        // (slot-liveness does not protect them either).  If a cleaner pass
        // reclaims that file between env-open and xa_commit, the slot installed
        // by apply_recovered_prepared_lns points at ln.original_lsn in a
        // CLEANED file; a SECOND crash before a checkpoint flushes the BIN
        // leaves recovery unable to rebuild the slot -> data loss.
        //
        // Two-phase subprocess harness (engine-level, mirrors noxu-xa xa_commit
        // without the Xid layer, which noxu-db cannot depend on):
        //   xarec_prepare : seed+commit, begin txn, put keys, prepare(), roll
        //                   files so the prepared LNs sit in an OLD file;
        //                   signal, spin -> parent SIGKILLs (first crash).
        //   xarec_commit  : reopen (recovery surfaces the prepared txn), run the
        //                   two-checkpoint deletion barrier (reclaim the prepared
        //                   LNs' file), then resolve exactly as xa_commit does
        //                   (take+apply recovered LNs, write_txn_commit_for_
        //                   recovered); signal, spin -> parent SIGKILLs BEFORE
        //                   any checkpoint (second crash).
        // ----------------------------------------------------------------
        "xarec_prepare" => {
            // Seed + commit so the DB and some filler files exist.
            {
                let t = env.begin_transaction(None).expect("begin seed");
                db.put_in(
                    &t,
                    DatabaseEntry::from_bytes(b"seed"),
                    DatabaseEntry::from_bytes(b"seed"),
                )
                .expect("seed put");
                t.commit().expect("commit seed");
            }
            firstlsn_roll_files(&db, &env, 20);
            // The prepared txn: put XA keys, then prepare (durable TxnPrepare).
            let txn = env.begin_transaction(None).expect("begin xa txn");
            // XA keys live in a DISTINCT range (100_000..) from the filler
            // keys (0..1600 written by firstlsn_roll_files), so the filler
            // never lock-conflicts with the prepared txn's WRITE locks and
            // the final read-back reads the SAME range it wrote.
            for i in 0u32..8 {
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&(100_000u32 + i).to_be_bytes()),
                    DatabaseEntry::from_bytes(&xarec_payload(i)),
                )
                .expect("xa put");
            }
            txn.prepare(1, b"xarec_gtrid", b"br1").expect("prepare xa txn");
            // Roll MORE files AFTER prepare so the prepared LNs' file becomes an
            // older, cleanable file once the txn is in-doubt across restart.
            firstlsn_roll_files(&db, &env, 40);
            // Leak the prepared txn (never drop): we spin until SIGKILL so the
            // in-doubt TxnPrepare frame is the on-disk tail for this txn.
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "xarec_commit" => {
            // Recovery (at env open above) surfaced the prepared txn.
            let prepared = env.recovered_prepared_txns();
            assert_eq!(
                prepared.len(),
                1,
                "xarec_commit: expected exactly one recovered prepared txn"
            );
            let txn_id = prepared[0].txn_id;
            // Reclaim the prepared LNs' file: drive the two-checkpoint deletion
            // barrier WHILE the prepared txn is in-doubt (unprotected by the
            // clamp, LNs not redone into a slot).
            firstlsn_force_delete_barrier(&env);
            // Resolve exactly as noxu-xa::xa_commit does for a recovered branch.
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.apply_recovered_prepared_lns(&lns)
                .expect("apply recovered prepared lns");
            env.write_txn_commit_for_recovered(txn_id)
                .expect("write txn commit for recovered");
            env.forget_recovered_prepared_txn(txn_id);
            // Signal and spin: parent SIGKILLs BEFORE any checkpoint flushes the
            // installed slot, so recovery must rebuild it from the log.
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "xarec_commit_noclean" => {
            // CONTROL: identical to xarec_commit but WITHOUT the cleaner
            // barrier.  If keys SURVIVE here but are LOST in xarec_commit, the
            // loss is caused by the cleaner reclaiming the prepared LNs' file
            // (the dangling-LSN bug), not by the resolve path itself.
            let prepared = env.recovered_prepared_txns();
            assert_eq!(
                prepared.len(),
                1,
                "xarec_commit_noclean: one prepared txn"
            );
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.apply_recovered_prepared_lns(&lns)
                .expect("apply recovered lns");
            env.write_txn_commit_for_recovered(txn_id).expect("write commit");
            env.forget_recovered_prepared_txn(txn_id);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "xarec_rollback" => {
            // R-XAREC FINDING F1 witness (durable recovered rollback): recovery
            // surfaced the prepared txn and redid+relocked its LNs.  Resolve by
            // ROLLBACK exactly as noxu-xa::xa_rollback does for a recovered
            // branch (undo + write_txn_abort_for_recovered), then spin ->
            // parent SIGKILLs BEFORE any checkpoint.  On reopen the branch MUST
            // be resolved (not resurfaced) and the record absent — i.e. the
            // resolving TxnAbort must be durable (JE Txn.abort(xid) force-flush).
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "xarec_rollback: one prepared txn");
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.undo_recovered_prepared_lns(&lns)
                .expect("undo recovered prepared lns");
            env.write_txn_abort_for_recovered(txn_id)
                .expect("write txn abort for recovered");
            env.forget_recovered_prepared_txn(txn_id);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "xarec_recover_only" => {
            // R-XAREC witness #6a: THIRD-crash coverage — crash AFTER recovery
            // redid+relocked the prepared LNs but BEFORE resolution.  Recover,
            // drive the deletion barrier (several checkpoints) WHILE the branch
            // is in-doubt, then spin -> SIGKILL.  The redone slots must have
            // been checkpointed and protected by the clamp so a subsequent
            // reopen still surfaces the prepared txn and its data.
            let prepared = env.recovered_prepared_txns();
            assert_eq!(
                prepared.len(),
                1,
                "xarec_recover_only: one prepared txn"
            );
            firstlsn_force_delete_barrier(&env);
            // Do NOT resolve: leave the branch in-doubt and crash.
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ----------------------------------------------------------------
        // XA-DDL probe (prepared DDL / NameLN across crash).
        //
        // Mirrors the real noxu-xa recovered-branch resolution exactly:
        //   commit  : take_recovered_prepared_lns -> apply_recovered_prepared_lns
        //             -> write_txn_commit_for_recovered -> forget_recovered_prepared_txn
        //   rollback: take_recovered_prepared_lns -> undo_recovered_prepared_lns
        //             -> write_txn_abort_for_recovered -> forget_recovered_prepared_txn
        //
        // A DDL op logs a NameLN(Txn), which recovery deserialises as
        // LogEntry::NameLn (NOT LogEntry::Ln), so collect_prepared_txn_lns
        // never captures it: take_recovered_prepared_lns is empty and the
        // apply/undo no-ops over the DDL.  The DDL's fate is decided entirely
        // by run_mapping_tree_undo_pass (which undoes any NameLN whose creating
        // txn is not in committed_txns) plus the bare TxnCommit/TxnAbort frame.
        //
        // Env uses the firstlsn_/xarec_ tiny-file, daemons-off profile (set
        // at open above via the `mode.starts_with("xarec_")` branch -- these
        // modes start with "xaddl_" so add that prefix to the open profile).
        //
        // ---- CREATE "x" (+ records) under a prepared XA txn ----
        "xaddl_create_prepare" => {
            // Open the victim DB under a txn, write 8 records, prepare, leak.
            let txn = env.begin_transaction(None).expect("begin xa ddl txn");
            let victim = env
                .open_database(Some(&txn), "x", &db_config)
                .expect("create x under txn");
            for i in 0u32..8 {
                victim
                    .put_in(
                        &txn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("put into x");
            }
            victim.close().expect("close victim handle before prepare");
            txn.prepare(1, b"xaddl_gtrid", b"create").expect("prepare xa ddl");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- REMOVE "x" (pre-existing committed, with records) ----
        "xaddl_remove_prepare" => {
            // Create + commit the victim with 8 records.
            {
                let vtxn =
                    env.begin_transaction(None).expect("begin victim create");
                let v = env
                    .open_database(Some(&vtxn), "x", &db_config)
                    .expect("create x");
                for i in 0u32..8 {
                    v.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed x rec");
                }
                v.close().expect("close x");
                vtxn.commit().expect("commit x create");
            }
            // Remove "x" under a txn, prepare, leak.
            let txn = env.begin_transaction(None).expect("begin remove txn");
            env.remove_database(Some(&txn), "x").expect("remove x under txn");
            txn.prepare(1, b"xaddl_gtrid", b"remove").expect("prepare remove");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- RENAME "x" -> "y" (x pre-existing committed, with records) ----
        "xaddl_rename_prepare" => {
            {
                let vtxn =
                    env.begin_transaction(None).expect("begin victim create");
                let v = env
                    .open_database(Some(&vtxn), "x", &db_config)
                    .expect("create x");
                for i in 0u32..8 {
                    v.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed x rec");
                }
                v.close().expect("close x");
                vtxn.commit().expect("commit x create");
            }
            let txn = env.begin_transaction(None).expect("begin rename txn");
            env.rename_database(Some(&txn), "x", "y")
                .expect("rename x->y under txn");
            txn.prepare(1, b"xaddl_gtrid", b"rename").expect("prepare rename");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- DDL + DATA in the SAME prepared branch (R-XADDL-711): a
        // prepared txn that CREATES "x" (+8 records) AND writes 8 records
        // into a pre-existing committed "d".  Confirms the recovered branch
        // resolves BOTH the DDL (create x) and the DATA LNs (to d, via the
        // R-XARECOVERED redo+relock path) together.  commit -> both x and d's
        // new records present; rollback -> x absent and d unchanged.
        "xaddl_create_data_prepare" => {
            // Seed pre-existing committed "d" with a BASE record set so
            // rollback can be verified (d keeps exactly its base).
            {
                let vtxn = env.begin_transaction(None).expect("begin d create");
                let d = env
                    .open_database(Some(&vtxn), "d", &db_config)
                    .expect("create d");
                for i in 0u32..4 {
                    d.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&(100 + i).to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(100 + i)),
                    )
                    .expect("seed d base rec");
                }
                d.close().expect("close d");
                vtxn.commit().expect("commit d base");
            }
            // One prepared branch: create x (+records) AND write into d.
            let txn = env.begin_transaction(None).expect("begin combined txn");
            let x = env
                .open_database(Some(&txn), "x", &db_config)
                .expect("create x under txn");
            for i in 0u32..8 {
                x.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(&xarec_payload(i)),
                )
                .expect("put into x");
            }
            x.close().expect("close x before prepare");
            let d = env
                .open_database(Some(&txn), "d", &db_config)
                .expect("reopen d under txn");
            for i in 0u32..8 {
                d.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(&xarec_payload(i)),
                )
                .expect("put into d");
            }
            d.close().expect("close d before prepare");
            txn.prepare(1, b"xaddl_gtrid", b"create_data")
                .expect("prepare combined");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- REMOVE + DATA in the SAME prepared branch: remove "x"
        // (pre-existing) AND write 8 records into pre-existing "d".
        "xaddl_remove_data_prepare" => {
            // Seed committed "x" (8 recs) and "d" (4 base recs).
            {
                let vtxn = env.begin_transaction(None).expect("begin seed");
                let xdb = env
                    .open_database(Some(&vtxn), "x", &db_config)
                    .expect("create x");
                for i in 0u32..8 {
                    xdb.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed x rec");
                }
                xdb.close().expect("close x");
                let d = env
                    .open_database(Some(&vtxn), "d", &db_config)
                    .expect("create d");
                for i in 0u32..4 {
                    d.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&(100 + i).to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(100 + i)),
                    )
                    .expect("seed d base rec");
                }
                d.close().expect("close d");
                vtxn.commit().expect("commit seed");
            }
            let txn = env.begin_transaction(None).expect("begin combined txn");
            env.remove_database(Some(&txn), "x").expect("remove x under txn");
            let d = env
                .open_database(Some(&txn), "d", &db_config)
                .expect("reopen d under txn");
            for i in 0u32..8 {
                d.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(&xarec_payload(i)),
                )
                .expect("put into d");
            }
            d.close().expect("close d before prepare");
            txn.prepare(1, b"xaddl_gtrid", b"remove_data")
                .expect("prepare combined");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- Resolve a recovered DDL branch exactly as noxu-xa does ----
        "xaddl_commit" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(
                prepared.len(),
                1,
                "xaddl_commit: expected exactly one recovered prepared txn"
            );
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.apply_recovered_prepared_lns(&lns)
                .expect("apply recovered prepared lns");
            env.write_txn_commit_for_recovered(txn_id)
                .expect("write txn commit for recovered");
            env.forget_recovered_prepared_txn(txn_id);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "xaddl_rollback" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(
                prepared.len(),
                1,
                "xaddl_rollback: expected exactly one recovered prepared txn"
            );
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.undo_recovered_prepared_lns(&lns)
                .expect("undo recovered prepared lns");
            env.write_txn_abort_for_recovered(txn_id)
                .expect("write txn abort for recovered");
            env.forget_recovered_prepared_txn(txn_id);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- RV-ADV: resolve (commit) + post-resolution checkpoint + CLEAN
        // close (no crash).  Exercises the resolve-call + relog-suppression
        // path the author witness never hits (it always SIGKILLs right after
        // commit).  WITH resolution: remove/rename-away is physically applied
        // and `pending_ddl_names` cleared, so the forced checkpoint cannot
        // resurrect the victim and a clean reopen sees it GONE.  This is the
        // RED-when-resolve-is-disabled witness (TEETH-3 teeth).
        "xaddl_commit_ckpt_clean" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(
                prepared.len(),
                1,
                "xaddl_commit_ckpt_clean: expected exactly one recovered prepared txn"
            );
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.apply_recovered_prepared_lns(&lns)
                .expect("apply recovered prepared lns");
            env.write_txn_commit_for_recovered(txn_id)
                .expect("write txn commit for recovered");
            env.forget_recovered_prepared_txn(txn_id);
            // Force a post-resolution checkpoint: this runs relog_live_catalog,
            // which must NOT resurrect the just-removed/renamed-away victim.
            use noxu_db::CheckpointConfig;
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("post-resolution checkpoint");
            env.close().ok();
            flag(&dir, "firstlsn_done");
        }

        // ---- RV-ADV2: resolve(commit) then, in the SAME live env, assert the
        // catalog reflects the committed REMOVE immediately -- database_names()
        // must NOT list "x" and a concurrent remove("x") must now fail NotFound
        // (the in-doubt conflict must be GONE once resolved).  WITHOUT the
        // resolve call the in-memory name_map still carries the seeded-live "x"
        // and the recovered-ddl conflict entry is never removed, so the live
        // env sees "x" and the conflict persists.  Exits clean (prints result).
        "xaddl_commit_live_check" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "commit_live_check: one prepared");
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.apply_recovered_prepared_lns(&lns).expect("apply lns");
            env.write_txn_commit_for_recovered(txn_id)
                .expect("commit recovered");
            env.forget_recovered_prepared_txn(txn_id);
            // LIVE post-commit assertions in the SAME env:
            let names = env.database_names().expect("database_names");
            let x_listed = names.iter().any(|n| n == "x");
            // After a committed remove, a fresh remove("x") must be NotFound
            // (not a conflict, not a success): the name is gone and unlocked.
            let t = env.begin_transaction(None).expect("rt");
            let remove_res = env.remove_database(Some(&t), "x");
            t.commit().ok();
            let outcome = match remove_res {
                Ok(_) => "removed_again",
                Err(e) => {
                    let s = format!("{e:?}");
                    if s.contains("NotFound") {
                        "notfound"
                    } else if s.contains("InUse") || s.contains("DatabaseInUse")
                    {
                        "conflict"
                    } else {
                        "othererr"
                    }
                }
            };
            eprintln!("RV_ADV2 x_listed={x_listed} remove_outcome={outcome}");
            flag(&dir, &format!("adv2_xlisted_{x_listed}"));
            flag(&dir, &format!("adv2_outcome_{outcome}"));
            env.close().ok();
            flag(&dir, "firstlsn_done");
        }

        // ---- RV-ADV3: MULTIPLE DDL ops in ONE prepared branch (task 1e):
        // create "a" (+recs) + remove pre-existing "b" + rename pre-existing
        // "c"->"d", all in a single prepared XA txn.  JE resurrects + relocks
        // EACH NameLN and Txn.commit runs deletedDatabases/undoDatabases over
        // ALL of them.  The Noxu classifier keys on (deletes.len, creates.len)
        // per-txn, so this branch is (2 del: b, c-old) + (2 crt: a, d-new) =
        // (2,2) -> the `_ => skip` arm -> DDL reconstruction DROPPED entirely.
        // Expected after commit: a present(+recs), b gone, c gone, d present.
        "xaddl_multi_prepare" => {
            // Seed committed b and c.
            {
                let vtxn = env.begin_transaction(None).expect("seed txn");
                let b = env
                    .open_database(Some(&vtxn), "b", &db_config)
                    .expect("create b");
                for i in 0u32..8 {
                    b.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed b");
                }
                b.close().ok();
                let c = env
                    .open_database(Some(&vtxn), "c", &db_config)
                    .expect("create c");
                for i in 0u32..8 {
                    c.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed c");
                }
                c.close().ok();
                vtxn.commit().expect("commit seed");
            }
            let txn = env.begin_transaction(None).expect("begin multi txn");
            let a = env
                .open_database(Some(&txn), "a", &db_config)
                .expect("create a");
            for i in 0u32..8 {
                a.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(&xarec_payload(i)),
                )
                .expect("put a");
            }
            a.close().ok();
            env.remove_database(Some(&txn), "b").expect("remove b");
            env.rename_database(Some(&txn), "c", "d").expect("rename c->d");
            txn.prepare(1, b"xaddl_gtrid", b"multi").expect("prepare multi");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- RV-ADV5: TWO DDL ops in one prepared branch: create "a"(+recs)
        // + remove pre-existing "b".  (1 del b, 1 crt a) -> the classifier
        // MISREADS this as RENAME b->a.  Ground-truth on commit: a present(8),
        // b GONE.  Misclassification would keep b's old name live (as a rename
        // source) and/or map a to b's db_id.
        "xaddl_create_plus_remove_prepare" => {
            {
                let vtxn = env.begin_transaction(None).expect("seed txn");
                let b = env
                    .open_database(Some(&vtxn), "b", &db_config)
                    .expect("create b");
                for i in 0u32..8 {
                    b.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed b");
                }
                b.close().ok();
                vtxn.commit().expect("commit seed");
            }
            let txn = env.begin_transaction(None).expect("begin ca txn");
            let a = env
                .open_database(Some(&txn), "a", &db_config)
                .expect("create a");
            for i in 0u32..8 {
                a.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(&xarec_payload(i)),
                )
                .expect("put a");
            }
            a.close().ok();
            env.remove_database(Some(&txn), "b").expect("remove b");
            txn.prepare(1, b"xaddl_gtrid", b"ca").expect("prepare ca");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- RV-ADV6 (task 1a): crash BETWEEN the durable TxnCommit and the
        // in-memory resolution.  Writes the fsync'd TxnCommit for the recovered
        // branch, then HANGS (parent SIGKILLs) BEFORE resolve_recovered_prepared_ddl
        // runs.  On the next recovery the branch is committed (tombstone wins),
        // so the committed REMOVE must still take effect -- durability must not
        // depend on the in-memory resolution step surviving.
        "xaddl_commit_crash_before_resolve" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "one prepared");
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.apply_recovered_prepared_lns(&lns).expect("apply lns");
            // Durable TxnCommit (fsync) -- then CRASH before any in-memory
            // resolution or forget.
            env.write_txn_commit_for_recovered(txn_id)
                .expect("commit recovered");
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- RV-ADV8 (task 1c): in-doubt REMOVE "x" -- probe the JE lock-type
        // equivalents while the branch is unresolved.  JE: getDb/open READ-locks
        // the NameLN (DENIED -> preempt), truncate WRITE-locks (conflict),
        // getDbNames LockType.NONE (no block, sees committed image).  Record
        // Noxu's observed behavior for open / truncate / names.
        "xaddl_indoubt_probe_ops" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "indoubt_probe_ops: one prepared");
            // (1) open_database("x") read handle (allow_create=false)
            let open_res = {
                let cfg = DatabaseConfig::new()
                    .with_allow_create(false)
                    .with_transactional(true);
                match env.open_database(None, "x", &cfg) {
                    Ok(d) => {
                        let _ = d.close();
                        "opened"
                    }
                    Err(e) => {
                        let s = format!("{e:?}");
                        if s.contains("InUse") || s.contains("DatabaseInUse") {
                            "conflict"
                        } else if s.contains("NotFound") {
                            "notfound"
                        } else {
                            "othererr"
                        }
                    }
                }
            };
            // (2) truncate_database("x")
            let trunc_res = match env.truncate_database(None, "x") {
                Ok(n) => {
                    let _ = n;
                    "truncated"
                }
                Err(e) => {
                    let s = format!("{e:?}");
                    if s.contains("InUse") || s.contains("DatabaseInUse") {
                        "conflict"
                    } else if s.contains("NotFound") {
                        "notfound"
                    } else {
                        "othererr"
                    }
                }
            };
            // (3) get_database_names()
            let names = env.database_names().expect("database_names");
            let x_listed = names.iter().any(|n| n == "x");
            eprintln!(
                "RV_ADV8 open={open_res} truncate={trunc_res} x_listed={x_listed}"
            );
            flag(&dir, &format!("adv8_open_{open_res}"));
            flag(&dir, &format!("adv8_truncate_{trunc_res}"));
            flag(&dir, &format!("adv8_xlisted_{x_listed}"));
            env.close().ok();
            flag(&dir, "firstlsn_done");
        }

        // ---- RV-ADV9 (task 1c, data-loss): in-doubt REMOVE "x"(8 recs) -- a
        // concurrent truncate("x") succeeds (NOT blocked), emptying the db.
        // This mode just performs the concurrent truncate while in-doubt and
        // exits clean; the harness then ROLLS BACK the branch and checks "x"
        // should still have its 8 records (JE WRITE-relock would have blocked
        // the truncate).  Data loss if "x" comes back empty.
        "xaddl_indoubt_truncate_then_leave" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "one prepared");
            // Open "x" first so its recovered tree is materialized in db_map
            // (mirrors a real reader), THEN truncate -- this is the path that
            // deterministically succeeds against the in-doubt-remove victim.
            {
                let cfg = DatabaseConfig::new()
                    .with_allow_create(false)
                    .with_transactional(true);
                if let Ok(d) = env.open_database(None, "x", &cfg) {
                    let _ = d.close();
                }
            }
            let n = env.truncate_database(None, "x");
            eprintln!("RV_ADV9 truncate_result={:?}", n);
            env.close().ok();
            flag(&dir, "firstlsn_done");
        }

        // ---- RV-ADV10 (task 1f): in-doubt prepared CREATE "x" -- run the cleaner
        // (forced) + a checkpoint WHILE the branch is in-doubt, then commit.  The
        // cleaner must NOT reclaim the log file holding the in-doubt NameLN/data
        // LNs (R-FIRSTLSN file protection + recovered-prepared pinning).  After
        // commit "x" must exist with its 8 records.
        "xaddl_indoubt_clean_then_commit" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "one prepared");
            // Force cleaner + checkpoint while in-doubt.
            let _ = env.clean_log_forced();
            use noxu_db::CheckpointConfig;
            let _ =
                env.checkpoint(Some(&CheckpointConfig::new().with_force(true)));
            let _ = env.clean_log_forced();
            // Now resolve (commit) via the standard recovered path.
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.apply_recovered_prepared_lns(&lns).expect("apply lns");
            env.write_txn_commit_for_recovered(txn_id)
                .expect("commit recovered");
            env.forget_recovered_prepared_txn(txn_id);
            env.close().ok();
            flag(&dir, "firstlsn_done");
        }

        // ---- RV-ADV11 (task 1b): crash BETWEEN durable TxnAbort and in-memory
        // resolution for a prepared REMOVE.  On recovery the branch is aborted
        // (tombstone not applied), so "x" must survive with its 8 records.
        "xaddl_abort_crash_before_resolve" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "one prepared");
            let txn_id = prepared[0].txn_id;
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.undo_recovered_prepared_lns(&lns).expect("undo lns");
            env.write_txn_abort_for_recovered(txn_id).expect("abort recovered");
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- RV-ADV12: TWO REMOVEs in one prepared branch (remove b + remove c),
        // both pre-existing (2 del, 0 crt) -> classifier _ => skip -> NO marks ->
        // both resurrected on commit.  Ground-truth: b gone, c gone.
        "xaddl_two_removes_prepare" => {
            {
                let vtxn = env.begin_transaction(None).expect("seed txn");
                for nm in ["b", "c"] {
                    let d = env
                        .open_database(Some(&vtxn), nm, &db_config)
                        .expect("create");
                    for i in 0u32..8 {
                        d.put_in(
                            &vtxn,
                            DatabaseEntry::from_bytes(&i.to_be_bytes()),
                            DatabaseEntry::from_bytes(&xarec_payload(i)),
                        )
                        .expect("seed");
                    }
                    d.close().ok();
                }
                vtxn.commit().expect("commit seed");
            }
            let txn = env.begin_transaction(None).expect("begin tr txn");
            env.remove_database(Some(&txn), "b").expect("remove b");
            env.remove_database(Some(&txn), "c").expect("remove c");
            txn.prepare(1, b"xaddl_gtrid", b"two_rm").expect("prepare two_rm");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- In-doubt isolation probe ----
        // Recover (branch is surfaced, STILL in-doubt / unresolved), then a
        // SECOND txn attempts the conflicting catalog op.  It must block or
        // conflict -- it must NEVER see the in-doubt DDL as committed.
        // We record the observed outcome in flag files the parent inspects;
        // we use a bounded begin+op so a true block shows up as a timeout
        // (recorded as "blocked"), never a hang that stalls the harness.
        "xaddl_indoubt_probe_create" => {
            // CREATE in-doubt: a concurrent open_database("x", allow_create)
            // must NOT succeed-as-committed; it should conflict/block because
            // the prepared branch holds the name.  Record the outcome.
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "indoubt_probe_create: one prepared");
            // Probe in a bounded worker thread so a legitimate BLOCK (lock
            // wait) is observed as a timeout, not a harness hang.
            let res = probe_bounded(&dir, || {
                let t = env.begin_transaction(None)?;
                let d = env.open_database(Some(&t), "x", &db_config)?;
                d.close()?;
                t.commit()?;
                Ok(())
            });
            flag(&dir, &format!("probe_result_{res}"));
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "xaddl_indoubt_probe_remove" => {
            // REMOVE in-doubt: "x" pre-existed committed; a prepared remove is
            // in-doubt.  A concurrent remove("x") must conflict/block, never
            // "succeed" as if the in-doubt remove were committed (and never
            // "fail NotFound" as if it were committed-removed).
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "indoubt_probe_remove: one prepared");
            let res = probe_bounded(&dir, || {
                let t = env.begin_transaction(None)?;
                env.remove_database(Some(&t), "x")?;
                t.commit()?;
                Ok(())
            });
            flag(&dir, &format!("probe_result_{res}"));
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        "xaddl_indoubt_probe_rename" => {
            // RENAME in-doubt: prepared rename x->y in-doubt.  A concurrent
            // open_database("y") must not see y as committed; a concurrent
            // open of "x" must not see x as already-gone.  We probe "y"
            // (the post-rename name) with allow_create=false: it must NOT be
            // found-as-committed (expected NotFound/err or block).
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "indoubt_probe_rename: one prepared");
            let res = probe_bounded(&dir, || {
                let t = env.begin_transaction(None)?;
                // rename y->z would succeed only if y already visible-committed
                env.rename_database(Some(&t), "y", "z")?;
                t.commit()?;
                Ok(())
            });
            flag(&dir, &format!("probe_result_{res}"));
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // Diagnostic: open read-only, print catalog + per-db record counts,
        // then exit (NO hang).  Used to inspect post-resolution state under
        // RUST_LOG=debug without the parent SIGKILL.
        // Diagnostic (read-only, allow_create=false path like the witness):
        // this mode is reached via the normal setup which opens "test" with
        // allow_create=true -- so to mirror the witness's pure read-only
        // reopen we must NOT rely on that.  Instead we re-open a SECOND env
        // handle is impossible (single lock), so we simply report what the
        // already-open env sees in database_names() BEFORE any write.  The
        // common setup above DID open "test" (a write) -- so for a faithful
        // read-only probe use the witness/test harness, not this.  This mode
        // instead prints names immediately then exits.
        // ============================================================
        // R-XADDL-711 v2 re-review ADVERSARIAL modes (independent, no reuse).
        // ============================================================

        // RVX-1: a prepared branch whose ops touch the SAME name more than
        // once: create "a"(8 recs) -> rename a->b -> create "a" again(3 recs)
        // -> remove pre-existing "e".  Commit ground-truth: b present(8 - a's
        // first-create records, renamed keeps db_id), a present(3 - the SECOND
        // create), e gone.  Exercises create/rename-source/create on one name
        // in one branch (db_id pairing MUST pair the first a-delete with the
        // b-create, NOT with the second a-create).
        "rvx_same_name_twice_prepare" => {
            {
                let vtxn = env.begin_transaction(None).expect("seed txn");
                let e = env
                    .open_database(Some(&vtxn), "e", &db_config)
                    .expect("create e");
                for i in 0u32..8 {
                    e.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed e");
                }
                e.close().ok();
                vtxn.commit().expect("commit seed");
            }
            // Seed committed "a"(8) alongside e so the rename a->b has a
            // committed source (a create-then-rename of an uncommitted name in
            // one branch is a separate pre-existing deferred-DDL limitation, not
            // under test here).  The second create of "a" reuses the freed name.
            {
                let vtxn2 = env.begin_transaction(None).expect("seed a txn");
                let a0 = env
                    .open_database(Some(&vtxn2), "a", &db_config)
                    .expect("create a0");
                for i in 0u32..8 {
                    a0.put_in(
                        &vtxn2,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed a0");
                }
                a0.close().ok();
                vtxn2.commit().expect("commit seed a");
            }
            let txn = env.begin_transaction(None).expect("begin tr");
            env.rename_database(Some(&txn), "a", "b").expect("rename a->b");
            let a2 = env
                .open_database(Some(&txn), "a", &db_config)
                .expect("create a2");
            for i in 0u32..3 {
                a2.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(&xarec_payload(i)),
                )
                .expect("put a2");
            }
            a2.close().ok();
            env.remove_database(Some(&txn), "e").expect("remove e");
            txn.prepare(1, b"rvx_gtrid", b"same_twice")
                .expect("prepare same_twice");
            std::mem::forget(txn);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // RVX-2: TWO in-doubt branches touching DIFFERENT names, resolved in
        // OPPOSITE order.  Branch "p" removes pre-existing "p"(8); branch "q"
        // removes pre-existing "q"(8).  After recover, COMMIT q FIRST then
        // COMMIT p.  Both removals must take effect regardless of order.
        "rvx_two_branches_prepare" => {
            {
                let vtxn = env.begin_transaction(None).expect("seed txn");
                for nm in ["p", "q"] {
                    let d = env
                        .open_database(Some(&vtxn), nm, &db_config)
                        .expect("create");
                    for i in 0u32..8 {
                        d.put_in(
                            &vtxn,
                            DatabaseEntry::from_bytes(&i.to_be_bytes()),
                            DatabaseEntry::from_bytes(&xarec_payload(i)),
                        )
                        .expect("seed");
                    }
                    d.close().ok();
                }
                vtxn.commit().expect("commit seed");
            }
            let tp = env.begin_transaction(None).expect("begin p");
            env.remove_database(Some(&tp), "p").expect("remove p");
            tp.prepare(1, b"rvx_gtrid", b"br_p").expect("prepare p");
            std::mem::forget(tp);
            let tq = env.begin_transaction(None).expect("begin q");
            env.remove_database(Some(&tq), "q").expect("remove q");
            tq.prepare(1, b"rvx_gtrid", b"br_q").expect("prepare q");
            std::mem::forget(tq);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // RVX-2 resolve: commit the branch that removed "q" FIRST, then the
        // branch that removed "p".  (bqual "br_q"/"br_p" identifies them.)
        "rvx_two_branches_resolve_qp" => {
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 2, "rvx_two_branches: two prepared");
            // Identify each branch by its recovered bqual.
            let mut txn_q: Option<u64> = None;
            let mut txn_p: Option<u64> = None;
            for pt in &prepared {
                let bq = String::from_utf8_lossy(&pt.xid_bqual).to_string();
                if bq == "br_q" {
                    txn_q = Some(pt.txn_id);
                } else if bq == "br_p" {
                    txn_p = Some(pt.txn_id);
                }
            }
            let txn_q = txn_q.expect("found br_q");
            let txn_p = txn_p.expect("found br_p");
            // Commit q FIRST.
            let lq = env.take_recovered_prepared_lns(txn_q);
            env.apply_recovered_prepared_lns(&lq).expect("apply q");
            env.write_txn_commit_for_recovered(txn_q).expect("commit q");
            env.forget_recovered_prepared_txn(txn_q);
            // Then commit p.
            let lp = env.take_recovered_prepared_lns(txn_p);
            env.apply_recovered_prepared_lns(&lp).expect("apply p");
            env.write_txn_commit_for_recovered(txn_p).expect("commit p");
            env.forget_recovered_prepared_txn(txn_p);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // RVX-3: in-doubt branch, then a CLEAN env.close(), then REOPEN, then
        // resolve.  A prepared REMOVE "x"(8) survives a clean close+reopen
        // while still in doubt, is re-surfaced by xa_recover, and commits.
        // (The close() runs a final checkpoint; the branch must still resolve.)
        "rvx_close_reopen_resolve" => {
            // Phase keyed by a flag so the SAME mode can drive both opens.
            if !dir.join("rvx3_first_done").exists() {
                let prepared = env.recovered_prepared_txns();
                assert_eq!(
                    prepared.len(),
                    1,
                    "rvx3: one prepared on first open"
                );
                // Clean close WHILE in doubt (final checkpoint runs).
                env.close().ok();
                flag(&dir, "rvx3_first_done");
                flag(&dir, "firstlsn_done");
            } else {
                // Second open: branch must still be in doubt; commit it.
                let prepared = env.recovered_prepared_txns();
                assert_eq!(
                    prepared.len(),
                    1,
                    "rvx3: branch must survive clean close+reopen in doubt"
                );
                let txn_id = prepared[0].txn_id;
                let lns = env.take_recovered_prepared_lns(txn_id);
                env.apply_recovered_prepared_lns(&lns).expect("apply");
                env.write_txn_commit_for_recovered(txn_id).expect("commit");
                env.forget_recovered_prepared_txn(txn_id);
                flag(&dir, "firstlsn_done");
                loop {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }

        // RVX-4: resolve a committed in-doubt REMOVE "x" WHILE another thread
        // repeatedly opens "x".  Before resolution every open must conflict
        // (DatabaseInUse); after resolution opens of the removed name must
        // fail cleanly (NotFound) -- never panic, deadlock, or succeed as a
        // live handle on a removed db.  The opener loops for a bounded time.
        "rvx_resolve_vs_concurrent_open" => {
            use std::sync::Arc;
            use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
            let prepared = env.recovered_prepared_txns();
            assert_eq!(prepared.len(), 1, "rvx4: one prepared");
            let txn_id = prepared[0].txn_id;
            let env = Arc::new(env);
            let stop = Arc::new(AtomicBool::new(false));
            let inuse = Arc::new(AtomicU64::new(0));
            let notfound = Arc::new(AtomicU64::new(0));
            let other = Arc::new(AtomicU64::new(0));
            let opened_ok = Arc::new(AtomicBool::new(false));
            let mut handles = Vec::new();
            for _ in 0..4 {
                let env = Arc::clone(&env);
                let stop = Arc::clone(&stop);
                let inuse = Arc::clone(&inuse);
                let notfound = Arc::clone(&notfound);
                let other = Arc::clone(&other);
                let opened_ok = Arc::clone(&opened_ok);
                handles.push(thread::spawn(move || {
                    let cfg = DatabaseConfig::new()
                        .with_allow_create(false)
                        .with_transactional(true);
                    while !stop.load(Ordering::SeqCst) {
                        match env.open_database(None, "x", &cfg) {
                            Ok(d) => {
                                opened_ok.store(true, Ordering::SeqCst);
                                let _ = d.close();
                            }
                            Err(e) => {
                                let s = format!("{e:?}");
                                // R-LIVEDDL-711 (ONE MECHANISM): an open of an
                                // in-doubt name is now refused by the real
                                // NameLN READ lock -> `LockConflict` (the JE
                                // `DatabasePreemptedException` analogue), the
                                // former lock-free path returned `DatabaseInUse`.
                                // Count either as the in-doubt refusal.
                                if s.contains("InUse")
                                    || s.contains("in use")
                                    || s.contains("LockConflict")
                                    || s.contains("Conflict")
                                    || s.contains("LockTimeout")
                                {
                                    inuse.fetch_add(1, Ordering::SeqCst);
                                } else if s.contains("NotFound")
                                    || s.contains("does not exist")
                                {
                                    notfound.fetch_add(1, Ordering::SeqCst);
                                } else {
                                    other.fetch_add(1, Ordering::SeqCst);
                                }
                            }
                        }
                        thread::sleep(Duration::from_millis(2));
                    }
                }));
            }
            // Let the openers run against the in-doubt name.
            thread::sleep(Duration::from_millis(200));
            let before_inuse = inuse.load(Ordering::SeqCst);
            // Resolve (commit the removal) while they hammer.
            let lns = env.take_recovered_prepared_lns(txn_id);
            env.apply_recovered_prepared_lns(&lns).expect("apply");
            env.write_txn_commit_for_recovered(txn_id).expect("commit");
            env.forget_recovered_prepared_txn(txn_id);
            // Let them observe the post-resolution state.
            thread::sleep(Duration::from_millis(200));
            stop.store(true, Ordering::SeqCst);
            for h in handles {
                let _ = h.join();
            }
            flag(
                &dir,
                &format!(
                    "rvx4_before_inuse_{}",
                    if before_inuse > 0 { 1 } else { 0 }
                ),
            );
            flag(
                &dir,
                &format!(
                    "rvx4_opened_ok_{}",
                    if opened_ok.load(Ordering::SeqCst) { 1 } else { 0 }
                ),
            );
            flag(
                &dir,
                &format!("rvx4_notfound_{}", notfound.load(Ordering::SeqCst)),
            );
            flag(&dir, &format!("rvx4_other_{}", other.load(Ordering::SeqCst)));
            flag(&dir, "firstlsn_done");
        }

        // RVX-5: LIVE (no crash, no XA) analogue of FID-RF1V2-1.  Txn T does
        // remove("x") (x pre-existing, 8 recs) but does NOT commit; a
        // concurrent op (truncate / remove / open) hits "x"; then T ABORTS.
        // On 7.11.x "x" has no name-record lock (D-ADDITIVE-SAFETY #18) and the
        // LIVE deferred remove is deliberately NOT blocked.  Records the
        // concurrent op's outcome + x's final record count so the test can
        // judge whether the live case loses data.  op selected by $RVX5_OP.
        "rvx_live_remove_concurrent_then_abort" => {
            use std::sync::Arc;
            let op = env::var("RVX5_OP").unwrap_or_else(|_| "truncate".into());
            {
                let vtxn = env.begin_transaction(None).expect("seed txn");
                let x = env
                    .open_database(Some(&vtxn), "x", &db_config)
                    .expect("create x");
                for i in 0u32..8 {
                    x.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed x");
                }
                x.close().ok();
                vtxn.commit().expect("commit seed");
            }
            let env = Arc::new(env);
            // T: uncommitted remove("x").
            let t = env.begin_transaction(None).expect("begin T");
            env.remove_database(Some(&t), "x").expect("remove x under T");
            // Concurrent op from another thread (bounded).
            let env2 = Arc::clone(&env);
            #[allow(clippy::redundant_clone)]
            let op2 = op.clone();
            let h = thread::spawn(move || -> String {
                let cfg = DatabaseConfig::new()
                    .with_allow_create(false)
                    .with_transactional(true);
                match op2.as_str() {
                    "truncate" => match env2.truncate_database(None, "x") {
                        Ok(n) => format!("truncate_ok_{n}"),
                        Err(e) => format!("truncate_err_{e:?}"),
                    },
                    "remove" => match env2.remove_database(None, "x") {
                        Ok(()) => "remove_ok".into(),
                        Err(e) => format!("remove_err_{e:?}"),
                    },
                    _ => match env2.open_database(None, "x", &cfg) {
                        Ok(d) => {
                            let _ = d.close();
                            "open_ok".into()
                        }
                        Err(e) => format!("open_err_{e:?}"),
                    },
                }
            });
            let concurrent = h.join().unwrap_or_else(|_| "panic".into());
            // Now ABORT T -> the remove is rolled back; x must survive intact.
            t.abort().expect("abort T");
            // Observe x's live record count (same probe as the test helper:
            // count the 8 seeded keys whose value matches xarec_payload).
            let (x_ex, xr) = {
                let cfg = DatabaseConfig::new()
                    .with_allow_create(false)
                    .with_transactional(true);
                match env.open_database(None, "x", &cfg) {
                    Ok(d) => {
                        let t = env.begin_transaction(None).expect("rtxn");
                        let mut c = 0u32;
                        for i in 0u32..8 {
                            let want = xarec_payload(i);
                            if let Ok(Some(v)) = d.get_in(&t, i.to_be_bytes())
                                && v[..] == want[..]
                            {
                                c += 1;
                            }
                        }
                        t.commit().ok();
                        let _ = d.close();
                        (true, c)
                    }
                    Err(_) => (false, 0),
                }
            };
            flag(&dir, &format!("rvx5_concurrent_{concurrent}"));
            flag(&dir, &format!("rvx5_x_ex_{}", if x_ex { 1 } else { 0 }));
            flag(&dir, &format!("rvx5_xr_{xr}"));
            flag(&dir, "firstlsn_done");
        }

        "xaddl_dump_ro" => {
            let names = env.database_names().expect("database_names");
            eprintln!("XADDL_DUMP_RO names={names:?}");
            flag(&dir, "firstlsn_done");
        }
        "xaddl_dump" => {
            let names = env.database_names().expect("database_names");
            eprintln!("XADDL_DUMP names={names:?}");
            for n in &names {
                if n == "test" {
                    continue;
                }
                if let Ok(d) = env.open_database(
                    None,
                    n,
                    &DatabaseConfig::new().with_transactional(true),
                ) {
                    let t = env.begin_transaction(None).expect("rt");
                    let mut cnt = 0u32;
                    for i in 0u32..8 {
                        if d.get_in(&t, i.to_be_bytes()).expect("get").is_some()
                        {
                            cnt += 1;
                        }
                    }
                    t.commit().ok();
                    d.close().ok();
                    eprintln!("XADDL_DUMP db={n} records={cnt}");
                }
            }
            flag(&dir, "firstlsn_done");
        }

        // ---- R-XADDL-711 fuzz: PREPARE phase.  Seed-driven, deterministic.
        // Builds 1-3 XA branches, each doing 1-4 DDL ops (create/remove/rename
        // over per-branch names) + data writes to a per-branch data db, then
        // prepares each branch under a distinct bqual (= branch index) and
        // leaks it.  Writes the executed plan to `fuzz_plan.txt` so the test
        // can fold a ground-truth model over it (the plan is the spec; the
        // recovered DB is the implementation under test).  Parent SIGKILLs.
        "xaddl_fuzz_prepare" => {
            let seed: u64 = env::var("XADDL_FUZZ_SEED")
                .ok()
                .and_then(|v| v.parse().ok())
                .expect("XADDL_FUZZ_SEED must be set");
            let plan = fuzz_build_plan(seed);
            // Pre-seed every committed target db the plan removes/renames, plus
            // every per-branch data db, in ONE auto-committed setup txn.
            {
                let stxn = env.begin_transaction(None).expect("seed txn");
                for name in plan.preseed_names() {
                    let d = env
                        .open_database(Some(&stxn), &name, &db_config)
                        .expect("preseed create");
                    for i in 0u32..8 {
                        d.put_in(
                            &stxn,
                            DatabaseEntry::from_bytes(&i.to_be_bytes()),
                            DatabaseEntry::from_bytes(&xarec_payload(i)),
                        )
                        .expect("preseed put");
                    }
                    d.close().ok();
                }
                stxn.commit().expect("commit preseed");
            }
            // One prepared branch per plan branch.
            for b in &plan.branches {
                let txn = env.begin_transaction(None).expect("begin branch");
                for op in &b.ops {
                    match op {
                        FuzzOp::Create { name } => {
                            let d = env
                                .open_database(Some(&txn), name, &db_config)
                                .expect("fuzz create");
                            for i in 0u32..8 {
                                d.put_in(
                                    &txn,
                                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                                    DatabaseEntry::from_bytes(&xarec_payload(
                                        i,
                                    )),
                                )
                                .expect("fuzz create put");
                            }
                            d.close().ok();
                        }
                        FuzzOp::Remove { name } => {
                            env.remove_database(Some(&txn), name)
                                .expect("fuzz remove");
                        }
                        FuzzOp::Rename { old, new } => {
                            env.rename_database(Some(&txn), old, new)
                                .expect("fuzz rename");
                        }
                        FuzzOp::Data { db, key } => {
                            // Write one extra record (key 100+key) to a
                            // per-branch data db that survives the branch.
                            let d = env
                                .open_database(Some(&txn), db, &db_config)
                                .expect("fuzz data open");
                            d.put_in(
                                &txn,
                                DatabaseEntry::from_bytes(&key.to_be_bytes()),
                                DatabaseEntry::from_bytes(&xarec_payload(*key)),
                            )
                            .expect("fuzz data put");
                            d.close().ok();
                        }
                    }
                }
                txn.prepare(1, b"xaddl_fuzz_gtrid", &b.bqual)
                    .expect("prepare branch");
                std::mem::forget(txn);
            }
            fuzz_write_plan(&dir, &plan);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }

        // ---- R-XADDL-711 fuzz: RESOLVE phase.  Recovers (xa_recover surfaces
        // every prepared branch), then for each branch applies the seed-chosen
        // decision: commit, rollback, or LEAVE IN DOUBT (at least one branch is
        // always left in doubt).  The decision is keyed by the branch's bqual
        // (= branch index), so it is stable across the crash.  Writes the
        // per-branch decisions to `fuzz_decisions.txt`, then SIGKILLs so the
        // in-doubt branches are re-surfaced by the FINAL recovery exactly as
        // left.
        "xaddl_fuzz_resolve" => {
            let seed: u64 = env::var("XADDL_FUZZ_SEED")
                .ok()
                .and_then(|v| v.parse().ok())
                .expect("XADDL_FUZZ_SEED must be set");
            let plan = fuzz_build_plan(seed);
            let prepared = env.recovered_prepared_txns();
            assert_eq!(
                prepared.len(),
                plan.branches.len(),
                "xaddl_fuzz_resolve: recovered {} prepared branches, plan has {}",
                prepared.len(),
                plan.branches.len()
            );
            let mut decisions: Vec<(u8, &str)> = Vec::new();
            for info in &prepared {
                // bqual byte identifies the branch index.
                let bidx = *info.xid_bqual.first().expect("bqual byte");
                let decision = plan
                    .branches
                    .iter()
                    .find(|b| b.bqual[0] == bidx)
                    .map(|b| b.decision)
                    .expect("branch for bqual");
                let txn_id = info.txn_id;
                match decision {
                    FuzzDecision::Commit => {
                        let lns = env.take_recovered_prepared_lns(txn_id);
                        env.apply_recovered_prepared_lns(&lns)
                            .expect("apply lns");
                        env.write_txn_commit_for_recovered(txn_id)
                            .expect("commit recovered");
                        env.forget_recovered_prepared_txn(txn_id);
                        decisions.push((bidx, "commit"));
                    }
                    FuzzDecision::Rollback => {
                        let lns = env.take_recovered_prepared_lns(txn_id);
                        env.undo_recovered_prepared_lns(&lns)
                            .expect("undo lns");
                        env.write_txn_abort_for_recovered(txn_id)
                            .expect("abort recovered");
                        env.forget_recovered_prepared_txn(txn_id);
                        decisions.push((bidx, "rollback"));
                    }
                    FuzzDecision::InDoubt => {
                        // Leave the branch untouched: do NOT take its LNs, do
                        // NOT forget it -- the FINAL recovery must re-surface
                        // it exactly as prepared (still in doubt).
                        decisions.push((bidx, "indoubt"));
                    }
                }
            }
            fuzz_write_decisions(&dir, &decisions);
            flag(&dir, "firstlsn_done");
            loop {
                thread::sleep(Duration::from_millis(50));
            }
        }
        "rvx_same_name_twice_live" => {
            // LIVE control for RVX-1: same op sequence (rename a->b, recreate a,
            // remove e) in ONE txn, committed DIRECTLY (no prepare, no crash).
            // Isolates whether the recreate-after-rename-away loss is specific
            // to the recovered in-doubt resolution or a pre-existing live limit.
            {
                let vtxn = env.begin_transaction(None).expect("seed");
                for nm in ["a", "e"] {
                    let d = env
                        .open_database(Some(&vtxn), nm, &db_config)
                        .expect("seed open");
                    for i in 0u32..8 {
                        d.put_in(
                            &vtxn,
                            DatabaseEntry::from_bytes(&i.to_be_bytes()),
                            DatabaseEntry::from_bytes(&xarec_payload(i)),
                        )
                        .expect("seed put");
                    }
                    d.close().ok();
                }
                vtxn.commit().expect("commit seed");
            }
            let txn = env.begin_transaction(None).expect("begin");
            env.rename_database(Some(&txn), "a", "b").expect("rename a->b");
            let a2 = env
                .open_database(Some(&txn), "a", &db_config)
                .expect("create a2");
            for i in 0u32..3 {
                a2.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&i.to_be_bytes()),
                    DatabaseEntry::from_bytes(&xarec_payload(i)),
                )
                .expect("put a2");
            }
            a2.close().ok();
            env.remove_database(Some(&txn), "e").expect("remove e");
            txn.commit().expect("commit txn");
            let names = env.database_names().expect("names");
            flag(&dir, &format!("rvxlive_names_{}", names.join("-")));
            env.close().ok();
            flag(&dir, "firstlsn_done");
        }
        // RVX-LIVE: LIVE (no crash, no XA) FID-RF1V2-1 matrix.  Txn T does
        // a DDL op on pre-existing "x"(8 recs) but does NOT resolve yet; a
        // concurrent AUTO-COMMIT op (remove / truncate / rename / open) hits
        // "x"; then T is committed OR aborted per $RVXL_T.  Records the
        // concurrent op's outcome and x's (or the rename target's) final
        // record count.  $RVXL_OP in {remove,truncate,rename,open};
        // $RVXL_T in {abort,commit}.  7.11.x takes no name-record lock on the
        // LIVE deferred DDL (D-ADDITIVE-SAFETY #18), so this probes whether the
        // live case loses data.
        "rvxl_matrix" => {
            use std::sync::Arc;
            let op = env::var("RVXL_OP").unwrap_or_else(|_| "remove".into());
            let decision =
                env::var("RVXL_T").unwrap_or_else(|_| "abort".into());
            {
                let vtxn = env.begin_transaction(None).expect("seed txn");
                let x = env
                    .open_database(Some(&vtxn), "x", &db_config)
                    .expect("create x");
                for i in 0u32..8 {
                    x.put_in(
                        &vtxn,
                        DatabaseEntry::from_bytes(&i.to_be_bytes()),
                        DatabaseEntry::from_bytes(&xarec_payload(i)),
                    )
                    .expect("seed x");
                }
                x.close().ok();
                vtxn.commit().expect("commit seed");
            }
            let env = Arc::new(env);
            // T: an UNCOMMITTED DDL op on "x" (for rename, rename x->y).
            let t = env.begin_transaction(None).expect("begin T");
            match op.as_str() {
                "remove" => {
                    env.remove_database(Some(&t), "x").expect("T remove x");
                }
                "truncate" => {
                    // A deferred truncate under T (records deleted under T,
                    // restored on abort).
                    env.truncate_database(Some(&t), "x").expect("T truncate x");
                }
                "rename" => {
                    env.rename_database(Some(&t), "x", "y")
                        .expect("T rename x->y");
                }
                _ => {
                    // T holds an open handle on x (a reader) uncommitted.
                    let d = env
                        .open_database(Some(&t), "x", &db_config)
                        .expect("T open x");
                    d.close().ok();
                }
            }
            // Concurrent AUTO-COMMIT op from another thread.
            let env2 = Arc::clone(&env);
            #[allow(clippy::redundant_clone)]
            let op2 = op.clone();
            let h = thread::spawn(move || -> String {
                let cfg = DatabaseConfig::new()
                    .with_allow_create(false)
                    .with_transactional(true);
                // The concurrent op mirrors the T op's hazard: a remove races a
                // remove, a truncate races a truncate, a rename races a remove.
                match op2.as_str() {
                    "truncate" => match env2.truncate_database(None, "x") {
                        Ok(n) => format!("truncate_ok_{n}"),
                        Err(e) => format!("truncate_err_{e:?}"),
                    },
                    "open" => match env2.open_database(None, "x", &cfg) {
                        Ok(d) => {
                            let _ = d.close();
                            "open_ok".into()
                        }
                        Err(e) => format!("open_err_{e:?}"),
                    },
                    // remove and rename both race a concurrent remove("x").
                    _ => match env2.remove_database(None, "x") {
                        Ok(()) => "remove_ok".into(),
                        Err(e) => format!("remove_err_{e:?}"),
                    },
                }
            });
            let concurrent = h.join().unwrap_or_else(|_| "panic".into());
            // Resolve T.
            if decision == "commit" {
                t.commit().expect("commit T");
            } else {
                t.abort().expect("abort T");
            }
            // Count x's (and y's) records.
            let count = |env: &noxu_db::Environment, nm: &str| -> (u8, u32) {
                let cfg = DatabaseConfig::new()
                    .with_allow_create(false)
                    .with_transactional(true);
                match env.open_database(None, nm, &cfg) {
                    Ok(d) => {
                        let rt = env.begin_transaction(None).expect("rt");
                        let mut c = 0u32;
                        for i in 0u32..8 {
                            let want = xarec_payload(i);
                            if let Ok(Some(v)) = d.get_in(&rt, i.to_be_bytes())
                                && v[..] == want[..]
                            {
                                c += 1;
                            }
                        }
                        rt.commit().ok();
                        let _ = d.close();
                        (1, c)
                    }
                    Err(_) => (0, 0),
                }
            };
            let (x_ex, xr) = count(&env, "x");
            let (y_ex, yr) = count(&env, "y");
            flag(&dir, &format!("rvxl_concurrent_{concurrent}"));
            flag(&dir, &format!("rvxl_x_ex_{x_ex}"));
            flag(&dir, &format!("rvxl_xr_{xr}"));
            flag(&dir, &format!("rvxl_y_ex_{y_ex}"));
            flag(&dir, &format!("rvxl_yr_{yr}"));
            flag(&dir, "firstlsn_done");
        }

        // FID-NONTXN-SYNC-FSYNC witness: a NON-transactional db write has no
        // per-write fsync (JE `LN.logInternal` logs flushRequired=false,
        // fsyncRequired=false).  Durability comes from `Environment::sync()`
        // (JE `Environment.sync()` = a forced checkpoint).  So: records written
        // before `env.sync()` SURVIVE a crash; records written after `sync`
        // (unsynced) are LOST -- exactly the deferred-write property, but for a
        // plain non-transactional db.  The parent SIGKILLs mid-unsynced-batch.
        "nontxn_sync_survives" => {
            let nt_cfg = DatabaseConfig::new().with_allow_create(true);
            let nt = env
                .open_database(None, "nontxn", &nt_cfg)
                .expect("open non-txn db");
            for i in 0u32..50 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"synced");
                nt.put(&key, &val).expect("put synced non-txn");
            }
            env.sync().expect("env.sync() phase 1");
            flag(&dir, "nontxn_phase1_synced");
            for i in 1000u32..1050 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let val = DatabaseEntry::from_bytes(b"unsynced");
                nt.put(&key, &val).expect("put unsynced non-txn");
            }
            flag(&dir, "nontxn_unsynced_started");
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

// ====================================================================
// R-XADDL-711 fuzz: deterministic multi-op multi-branch plan generator.
// Shared between the prepare and resolve phases (seed-driven, so both
// phases reconstruct the SAME plan without persisting ops).  The test
// reads the plan + decisions files to fold a ground-truth model.
// ====================================================================

/// A single DDL / data operation inside a fuzz branch.
#[derive(Debug, Clone)]
enum FuzzOp {
    Create { name: String },
    Remove { name: String },
    Rename { old: String, new: String },
    Data { db: String, key: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FuzzDecision {
    Commit,
    Rollback,
    InDoubt,
}

#[derive(Debug, Clone)]
struct FuzzBranch {
    bqual: Vec<u8>,
    ops: Vec<FuzzOp>,
    decision: FuzzDecision,
}

#[derive(Debug, Clone)]
struct FuzzPlan {
    branches: Vec<FuzzBranch>,
}

impl FuzzPlan {
    /// Every committed db that must exist BEFORE the branches run: each
    /// branch's own data db plus every remove/rename-source target.
    fn preseed_names(&self) -> Vec<String> {
        let mut v: Vec<String> = Vec::new();
        for b in &self.branches {
            for op in &b.ops {
                match op {
                    FuzzOp::Remove { name } => v.push(name.clone()),
                    FuzzOp::Rename { old, .. } => v.push(old.clone()),
                    FuzzOp::Data { db, .. } => v.push(db.clone()),
                    FuzzOp::Create { .. } => {}
                }
            }
        }
        v.sort();
        v.dedup();
        v
    }
}

/// Tiny deterministic SplitMix64 PRNG (no external dependency): identical
/// output for a given seed on every platform, so the prepare and resolve
/// phases rebuild the same plan.
struct SplitMix64(u64);
impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// Build the deterministic plan for `seed`: 1-3 branches, each 1-4 ops over
/// PER-BRANCH names (prefix `b{idx}_`) so branches never collide, each with
/// its own committed data db.  Decisions are seed-chosen with at least one
/// branch forced InDoubt (the point of the fuzz is leaving some unresolved).
fn fuzz_build_plan(seed: u64) -> FuzzPlan {
    let mut rng = SplitMix64(seed ^ 0xD1B5_4A32_D192_ED03);
    let n_branches = 1 + rng.below(3) as usize; // 1..=3
    let mut branches = Vec::new();
    for idx in 0..n_branches {
        let pfx = format!("b{idx}_");
        let n_ops = 1 + rng.below(4) as usize; // 1..=4
        let mut ops = Vec::new();
        // Each branch gets exactly one data write to its own data db, plus
        // n_ops DDL ops.  Fresh-create names are numbered to stay unique.
        let mut next_new = 0u32;
        // Pre-plan the source targets this branch will remove/rename: a small
        // pool `b{idx}_t{0..3}` that the preseed step creates.
        let n_targets = 4u32;
        let mut avail_targets: Vec<u32> = (0..n_targets).collect();
        ops.push(FuzzOp::Data { db: format!("{pfx}data"), key: 100 });
        for _ in 0..n_ops {
            // Choose an op kind; remove/rename only if a target is available.
            let kind = rng.below(3);
            match kind {
                0 => {
                    let name = format!("{pfx}new{next_new}");
                    next_new += 1;
                    ops.push(FuzzOp::Create { name });
                }
                1 if !avail_targets.is_empty() => {
                    let ti = rng.below(avail_targets.len() as u64) as usize;
                    let t = avail_targets.remove(ti);
                    ops.push(FuzzOp::Remove { name: format!("{pfx}t{t}") });
                }
                _ if !avail_targets.is_empty() => {
                    let ti = rng.below(avail_targets.len() as u64) as usize;
                    let t = avail_targets.remove(ti);
                    let new = format!("{pfx}new{next_new}");
                    next_new += 1;
                    ops.push(FuzzOp::Rename { old: format!("{pfx}t{t}"), new });
                }
                _ => {
                    // No target left: fall back to a create.
                    let name = format!("{pfx}new{next_new}");
                    next_new += 1;
                    ops.push(FuzzOp::Create { name });
                }
            }
        }
        let decision = match rng.below(3) {
            0 => FuzzDecision::Commit,
            1 => FuzzDecision::Rollback,
            _ => FuzzDecision::InDoubt,
        };
        branches.push(FuzzBranch { bqual: vec![idx as u8], ops, decision });
    }
    // Guarantee at least one branch is left in doubt (the whole point).
    if !branches.iter().any(|b| b.decision == FuzzDecision::InDoubt) {
        let last = branches.len() - 1;
        branches[last].decision = FuzzDecision::InDoubt;
    }
    FuzzPlan { branches }
}

/// Write the executed plan (ops per branch) so the test can fold a model.
/// Format: one line per op: `<bidx> <kind> <args...>`.
fn fuzz_write_plan(dir: &Path, plan: &FuzzPlan) {
    use std::fmt::Write as _;
    let mut s = String::new();
    for b in &plan.branches {
        let bidx = b.bqual[0];
        for op in &b.ops {
            match op {
                FuzzOp::Create { name } => {
                    let _ = writeln!(s, "{bidx} create {name}");
                }
                FuzzOp::Remove { name } => {
                    let _ = writeln!(s, "{bidx} remove {name}");
                }
                FuzzOp::Rename { old, new } => {
                    let _ = writeln!(s, "{bidx} rename {old} {new}");
                }
                FuzzOp::Data { db, key } => {
                    let _ = writeln!(s, "{bidx} data {db} {key}");
                }
            }
        }
    }
    fs::write(dir.join("fuzz_plan.txt"), s).expect("write fuzz_plan.txt");
}

/// Write the per-branch decisions chosen during the resolve phase.
fn fuzz_write_decisions(dir: &Path, decisions: &[(u8, &str)]) {
    use std::fmt::Write as _;
    let mut s = String::new();
    for (bidx, d) in decisions {
        let _ = writeln!(s, "{bidx} {d}");
    }
    fs::write(dir.join("fuzz_decisions.txt"), s)
        .expect("write fuzz_decisions.txt");
}

/// XA-DDL in-doubt isolation probe: run a catalog op on THIS thread while a
/// watchdog thread (holding only the scratch dir) writes a `blocked` result
/// and the done-flag if the op has not finished by the deadline.  Classifies:
///   "committed" — the op SUCCEEDED (BAD: the in-doubt DDL was treated as
///                 committed, so a conflicting op completed).
///   "conflict"  — the op returned an error (DatabaseExists / NotFound / lock
///                 conflict): acceptable.
///   "blocked"   — the op did not return by the deadline (a true lock wait on
///                 the prepared branch held locks): acceptable.
/// The op runs inline so it may borrow `env`; the watchdog only touches `dir`.
fn probe_bounded<F>(dir: &Path, op: F) -> &'static str
where
    F: FnOnce() -> noxu_db::Result<()>,
{
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let done = Arc::new(AtomicBool::new(false));
    let watchdog_dir = dir.to_path_buf();
    let watchdog_done = Arc::clone(&done);
    let watchdog = thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if watchdog_done.load(Ordering::SeqCst) {
                return; // op finished first
            }
            thread::sleep(Duration::from_millis(50));
        }
        // Op still running after the deadline => it is blocked on a lock wait.
        flag(&watchdog_dir, "probe_result_blocked");
        flag(&watchdog_dir, "firstlsn_done");
    });
    let ok = op().is_ok();
    done.store(true, Ordering::SeqCst);
    let _ = watchdog.join();
    // If the watchdog already declared "blocked", honour that (the op may have
    // returned right at the edge of the deadline).
    if dir.join("probe_result_blocked").exists() {
        "blocked"
    } else if ok {
        "committed"
    } else {
        "conflict"
    }
}

/// R-XAREC: deterministic per-key payload for the prepared XA keys so a
/// witness can validate not just presence but the CORRECT committed value
/// (and detect a wrong-value / stale before-image).
fn xarec_payload(i: u32) -> Vec<u8> {
    format!("xa_payload_{i}").into_bytes()
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
