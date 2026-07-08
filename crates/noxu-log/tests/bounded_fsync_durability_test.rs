// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Durability oracle for the bounded fsync pipeline (`LOG_FSYNC_MAX_LEADERS`).
//!
//! # The invariant under test (the monotonic durable watermark)
//!
//! `last_synced_lsn` \u2014 the watermark a SYNC committer's fast-path trusts \u2014 must
//! mean: *every byte with offset < last_synced_lsn is in the OS page cache AND
//! covered by a COMPLETED fdatasync.*  Equivalently, the moment a SYNC
//! `log(..., fsync_required=true)` returns LSN `L`, some completed fdatasync
//! must have made every byte below `L` durable.
//!
//! With `max_leaders > 1`, up to N `fdatasync`s run concurrently on the log
//! file.  The design keeps this sound by pwriting each drained range to the
//! page cache UNDER the log-write latch, in LSN order, BEFORE capturing the
//! `eol` that this leader's fdatasync will publish \u2014 so a completed fdatasync
//! (which flushes ALL of the fd's dirty pages) makes every byte below its
//! captured `eol` durable regardless of which cohort wrote it, and the
//! watermark advances by CAS-max so an out-of-order (lower-eol) completion
//! never regresses it.  If that ordering were broken (e.g. pwrite moved back
//! outside the latch, letting a higher-eol leader publish past a lower-eol
//! leader's not-yet-pwritten bytes), this test fails.
//!
//! # How this test is deterministic (no fast device needed)
//!
//! It arms [`fsync_probe`], which (a) sleeps a fixed delay at the START of each
//! fdatasync \u2014 deliberately WIDENING the window in which several pipeline
//! leaders overlap \u2014 and (b) records, via a monotonic max, the highest on-disk
//! EOF any COMPLETED fdatasync has made durable (`SYNCED_EOF`).  The oracle
//! then asserts, for every SYNC commit, that `SYNCED_EOF >= the returned commit
//! LSN` at the instant the commit returns.

use noxu_log::file_handle::fsync_probe;
use noxu_log::{FileManager, LogEntryType, LogManager, provisional::Provisional};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;

/// Serialises this test file so only one test arms `fsync_probe` at a time
/// (the probe uses process-global atomics).
static PROBE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Run N concurrent SYNC committers at the given pipeline depth and assert the
/// monotonic-watermark durability invariant: no commit returns durable before
/// a completed fdatasync covered its bytes, and the durable watermark (as seen
/// via completed-fdatasync EOF) never lags a returned commit LSN.
fn run_depth(max_leaders: usize) {
    let _serial = PROBE_LOCK.lock().unwrap();

    const N_THREADS: usize = 8;
    const OPS_PER_THREAD: usize = 40;

    let dir = TempDir::new().unwrap();
    // Single large file so all LSNs share file_num 0 (SYNCED_EOF vs commit-LSN
    // is a plain u64 comparison within one file).  10 MiB >> the few hundred
    // small entries this test writes.
    let fm =
        Arc::new(FileManager::new(dir.path(), false, 10_000_000, 100).unwrap());

    let mut lm = LogManager::new(Arc::clone(&fm), 3, 1_048_576, 4096);
    // Group commit off (shipped default); bounded fsync pipeline under test.
    lm.set_group_commit_pipelined(0, 0, max_leaders);
    let lm = Arc::new(lm);

    // Arm the probe: 300us sleep at each fdatasync start widens the
    // concurrent-leader overlap window deterministically on ANY device.
    fsync_probe::arm(300);

    let failed = Arc::new(AtomicBool::new(false));
    let failure_msg = Arc::new(std::sync::Mutex::new(String::new()));
    let barrier = Arc::new(std::sync::Barrier::new(N_THREADS));

    let handles: Vec<_> = (0..N_THREADS)
        .map(|_| {
            let lm = Arc::clone(&lm);
            let b = Arc::clone(&barrier);
            let failed = Arc::clone(&failed);
            let failure_msg = Arc::clone(&failure_msg);
            std::thread::spawn(move || {
                b.wait();
                for i in 0..OPS_PER_THREAD {
                    let payload =
                        format!("commit-sync-{i:04}-durability-oracle-payload");
                    // SYNC commit: fsync_required = true.  On return the caller
                    // treats `lsn` as durable.
                    let lsn = lm
                        .log_with_old_lsn(
                            LogEntryType::TxnCommit,
                            payload.as_bytes(),
                            Provisional::No,
                            true, // flush_required
                            true, // fsync_required (COMMIT_SYNC)
                            None,
                        )
                        .expect("log commit-sync");

                    // THE ORACLE: a completed fdatasync must have covered this
                    // commit's bytes by the time the commit returns durable.
                    let synced_eof =
                        fsync_probe::SYNCED_EOF.load(Ordering::SeqCst);
                    if synced_eof < lsn.as_u64() {
                        failed.store(true, Ordering::SeqCst);
                        let mut m = failure_msg.lock().unwrap();
                        if m.is_empty() {
                            *m = format!(
                                "DURABILITY HOLE: commit returned durable at \
                                 LSN {} (offset {}) but the highest \
                                 completed-fdatasync EOF was only {} (offset \
                                 {}). Bytes were acked before any fdatasync \
                                 covered them.",
                                lsn.as_u64(),
                                lsn.file_offset(),
                                synced_eof,
                                noxu_util::Lsn::from_u64(synced_eof)
                                    .file_offset(),
                            );
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let completed = fsync_probe::COMPLETED.load(Ordering::SeqCst);
    fsync_probe::disarm();

    // Physical-plausibility check: with N concurrent SYNC committers the number
    // of real fdatasyncs must be plentiful, NOT ~1 covering hundreds.  A single
    // fdatasync covering the whole workload would mean commits were acked
    // without their own sync.  The LOWER bound is the meaningful guard \u2014 a
    // collapse to a tiny count is the smell of a coalescing-past-durability bug.
    assert!(
        completed >= (N_THREADS as u64),
        "max_leaders={max_leaders}: only {completed} fdatasyncs completed for \
         {} SYNC commits — implausibly few; commits cannot all be durable",
        N_THREADS * OPS_PER_THREAD
    );

    assert!(
        !failed.load(Ordering::SeqCst),
        "max_leaders={max_leaders}: {}",
        failure_msg.lock().unwrap()
    );
}

/// max_leaders=1 (single-leader): every batch fsyncs under one leader.
#[test]
fn bounded_fsync_durability_leaders_1() {
    run_depth(1);
}

/// max_leaders=2.
#[test]
fn bounded_fsync_durability_leaders_2() {
    run_depth(2);
}

/// max_leaders=4.
#[test]
fn bounded_fsync_durability_leaders_4() {
    run_depth(4);
}

/// max_leaders=8.
#[test]
fn bounded_fsync_durability_leaders_8() {
    run_depth(8);
}
