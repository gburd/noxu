//! Durability oracle for the bounded fsync pipeline (`LOG_FSYNC_PIPELINE_DEPTH`).
//!
//! # Why this test exists
//!
//! The crash-recovery and shuttle suites did NOT catch the pipeline durability
//! hole because they run at too low a commit rate to open the concurrent-leader
//! window on a fast device (or the model abstracts the write queue away).  On
//! real NVMe an 8-thread COMMIT_SYNC workload measured 1.56M commits/s with
//! 3636 commits per `fdatasync` — physically impossible (the device caps at
//! ~10K `fdatasync`/s), proving commits were being acked before their bytes
//! were durable.
//!
//! # The invariant under test
//!
//! `last_synced_lsn` (the watermark a SYNC committer's fast-path trusts) must
//! mean: *every byte with offset < last_synced_lsn is in the OS page cache AND
//! covered by a COMPLETED fdatasync.*  Equivalently, the moment
//! `log(..., fsync_required=true)` returns LSN `L`, some completed fdatasync
//! must have made every byte below `L` durable.
//!
//! # How this test is deterministic (no fast device needed)
//!
//! It arms [`fsync_probe`], which (a) sleeps a fixed delay at the START of each
//! fdatasync — deliberately WIDENING the window in which several pipeline
//! leaders overlap, the exact window the hole lives in — and (b) records, via a
//! monotonic max, the highest on-disk EOF that any COMPLETED fdatasync has made
//! durable (`SYNCED_EOF`).  The oracle then asserts, for every SYNC commit, that
//! `SYNCED_EOF >= the returned commit LSN` at the instant the commit returns.
//!
//! On the buggy code (drain pwrites OUTSIDE the LWL and MAY enqueue), a
//! higher-eol leader publishes `last_synced_lsn` past bytes that a lower-eol
//! leader has not yet pwritten (they sit in the write queue / are not yet in the
//! page cache), so `SYNCED_EOF` lags the returned LSN and this test FAILS.  On
//! the fixed code (drain force-pwrites UNDER the LWL before capturing eol),
//! every byte below any published watermark is in the page cache before that
//! leader's fdatasync, so `SYNCED_EOF >= returned LSN` always holds.

use noxu_log::file_handle::fsync_probe;
use noxu_log::{
    FileManager, LogEntryType, LogManager, provisional::Provisional,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;

/// Serialises the whole-test-file so only one test arms `fsync_probe` at a
/// time (the probe uses process-global atomics).
static PROBE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Run N concurrent SYNC committers at the given pipeline depth and assert the
/// durability invariant: no commit returns durable before a completed fdatasync
/// covered its bytes.
fn run_depth(depth: usize) {
    let _serial = PROBE_LOCK.lock().unwrap();

    const N_THREADS: usize = 8;
    const OPS_PER_THREAD: usize = 40;

    let dir = TempDir::new().unwrap();
    // Single large file so all LSNs share file_num 0 (SYNCED_EOF vs commit-LSN
    // comparison is a plain u64 comparison within one file).  10 MiB >> the
    // few hundred small entries this test writes.
    let fm =
        Arc::new(FileManager::new(dir.path(), false, 10_000_000, 100).unwrap());
    // Enable the Write Queue (the mechanism the hole rode on) so this exercises
    // the real production configuration, not a queue-disabled shortcut.
    fm.configure_write_queue(true, 1 << 20);

    let mut lm = LogManager::new(Arc::clone(&fm), 3, 1_048_576, 4096);
    // Group commit off (shipped default); pipeline depth under test.
    lm.set_group_commit_pipelined(0, 0, depth);
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
                                 completed-fdatasync EOF was only {} \
                                 (offset {}). Bytes were acked before any \
                                 fdatasync covered them.",
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
    // of real fdatasyncs must be O(threads * ops), NOT ~1 covering hundreds.
    // A single fdatasync covering the whole workload would mean commits were
    // acked without their own sync.  At least one fdatasync per "cohort" — with
    // 8 threads x 40 ops and depth-bounded coalescing we expect well more than
    // a handful.  (The exact number is scheduling-dependent; the LOWER bound is
    // the meaningful guard — a collapse to a tiny count is the smell.)
    assert!(
        completed >= (N_THREADS as u64),
        "depth={depth}: only {completed} fdatasyncs completed for \
         {} SYNC commits — implausibly few; commits cannot all be durable",
        N_THREADS * OPS_PER_THREAD
    );

    assert!(
        !failed.load(Ordering::SeqCst),
        "depth={depth}: {}",
        failure_msg.lock().unwrap()
    );
}

/// depth=1 (historical single-leader): every commit fsyncs; trivially safe.
#[test]
fn pipeline_durability_depth_1() {
    run_depth(1);
}

/// depth=2.
#[test]
fn pipeline_durability_depth_2() {
    run_depth(2);
}

/// depth=4 (production default `LOG_FSYNC_PIPELINE_DEPTH`).
#[test]
fn pipeline_durability_depth_4() {
    run_depth(4);
}

/// depth=8.
#[test]
fn pipeline_durability_depth_8() {
    run_depth(8);
}
