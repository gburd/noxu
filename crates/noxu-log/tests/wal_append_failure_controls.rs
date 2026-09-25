//! Investigation controls for failed/unfinished append handling. NOT MERGE-READY.
//! No production hooks: faultdisk, the real file latch, and (Linux only) RLIMIT_FSIZE.
#![cfg(not(noxu_shuttle))]

use noxu_log::faultdisk::{self, FaultController, FaultKind};
use noxu_log::{
    FileManager, LogEntryType, LogFileReader, LogManager, Provisional,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct FaultReset;
impl Drop for FaultReset {
    fn drop(&mut self) {
        faultdisk::uninstall();
    }
}

fn manager(dir: &tempfile::TempDir) -> Arc<LogManager> {
    let fm =
        Arc::new(FileManager::new(dir.path(), false, 1_000_000, 100).unwrap());
    Arc::new(LogManager::new(fm, 3, 256, 4096))
}

// Run this executable with --test-threads=1: faultdisk is process-global.
#[test]
fn disk_full_retry_must_not_leave_a_gap_before_acknowledged_entries() {
    let _reset = FaultReset;
    faultdisk::uninstall();
    let dir = tempfile::tempdir().unwrap();
    let lm = manager(&dir);
    let old = lm
        .log(LogEntryType::Trace, b"old", Provisional::No, true, true)
        .unwrap();
    let failed_lsn = lm.file_manager().get_next_available_lsn();
    faultdisk::install(FaultController::for_test(FaultKind::DiskFull, 0));
    assert!(
        lm.log(LogEntryType::Trace, &[2; 4096], Provisional::No, false, false)
            .is_err()
    );
    assert_eq!(faultdisk::write_count(), 1);
    faultdisk::uninstall();
    assert_eq!(lm.read_entry(old).unwrap().1, b"old");
    if lm.is_io_invalid() {
        assert!(
            lm.log(
                LogEntryType::Trace,
                &[3; 4096],
                Provisional::No,
                true,
                true
            )
            .is_err()
        );
        return; // Explicit fail-stop is an acceptable alternative to retry.
    }
    let retry = lm
        .log(LogEntryType::Trace, &[3; 4096], Provisional::No, true, true)
        .unwrap();
    assert_eq!(lm.read_entry(retry).unwrap().1, vec![3; 4096]);
    let mut reader =
        LogFileReader::open(Arc::clone(lm.file_manager()), old.file_number())
            .unwrap();
    assert_eq!(reader.read_next_strict().unwrap().unwrap().0, old);
    let next = reader.read_next_strict();
    eprintln!(
        "failed={failed_lsn:?}, acknowledged retry={retry:?}, sequential next={next:?}"
    );
    assert_eq!(
        next.unwrap().unwrap().0,
        retry,
        "acknowledged retry must be reachable by sequential WAL scan"
    );
}

#[test]
fn sync_must_not_cover_an_unwritten_oversized_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let lm = manager(&dir);
    lm.log(LogEntryType::Trace, b"old", Provisional::No, true, true).unwrap();
    let pending = lm.file_manager().get_next_available_lsn();
    let handle =
        lm.file_manager().get_file_handle(pending.file_number()).unwrap();
    // Real write latch blocks the oversized pwrite, but not fdatasync.
    let held = handle.acquire().unwrap();
    let writer_lm = Arc::clone(&lm);
    let writer = std::thread::spawn(move || {
        writer_lm.log(
            LogEntryType::Trace,
            &[7; 4096],
            Provisional::No,
            false,
            false,
        )
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while lm.file_manager().get_next_available_lsn() == pending
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    let assigned = lm.file_manager().get_next_available_lsn() > pending;
    // Run sync on another thread so a corrected serial append may block it.
    let (tx, rx) = std::sync::mpsc::channel();
    let sync_lm = Arc::clone(&lm);
    let sync =
        std::thread::spawn(move || tx.send(sync_lm.flush_sync()).unwrap());
    let early = rx.recv_timeout(Duration::from_millis(250)).ok();
    let eof = std::fs::metadata(
        dir.path().join(format!("{:08x}.ndb", pending.file_number())),
    )
    .unwrap()
    .len();
    drop(held);
    let written = writer.join().unwrap().unwrap();
    sync.join().unwrap();
    assert_eq!(written, pending);
    assert!(
        assigned,
        "control must observe reservation while pwrite is blocked"
    );
    assert_eq!(
        eof,
        pending.file_offset() as u64,
        "pending bytes must not yet exist"
    );
    eprintln!("pending={pending:?}, physical EOF={eof}, early sync={early:?}");
    if let Some(Ok(covered)) = early {
        assert!(
            covered <= pending,
            "durable boundary {covered:?} covers unwritten reservation {pending:?}"
        );
    }
}

// Blocker 1 (data-loss, false durable watermark): a `flush_no_sync` that
// marks a range flushed under the LWL but pwrites it OFF the LWL lets a
// concurrent `flush_sync` seal a durable boundary over bytes not yet on disk.
//
// Repro (all under production locks):
//  1. A buffered NO_SYNC record is appended (bytes in the buffer, not on disk).
//  2. `flush_no_sync` snapshots + `mark_flushed` under the LWL, then blocks in
//     its pwrite (we hold the file write latch to freeze it there).
//  3. `flush_sync` runs: its drain sees the range already flushed (empty
//     unflushed data), skips it, captures eol PAST that range, fdatasyncs, and
//     publishes a durable watermark covering the un-pwritten range.
//  4. The `flush_sync` return value (the durable LSN) then names an offset
//     beyond the file's physical length -> power-loss loss.
//
// The fix keeps the no-sync snapshot + pwrite + watermark publication under
// the LWL, so `flush_sync` cannot even acquire the LWL until the no-sync
// pwrite has landed; the durable watermark can then never exceed on-disk EOF.
// Because the fix serializes both under the LWL, holding the file latch would
// deadlock the covering `flush_sync` on the LWL; we release the latch from an
// independent control, join both threads, and assert the FINAL durable
// watermark never covers un-pwritten bytes.
//
// Run with --test-threads=1 (faultdisk-free, but shares the process file latch).
#[test]
fn no_sync_drain_must_not_let_sync_publish_a_false_durable_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let lm = manager(&dir);
    // First, a flushed record so the log FILE physically exists on disk (its
    // bytes are pwritten). This lets us hold that file's write latch.
    let base = lm
        .log(LogEntryType::Trace, b"base", Provisional::No, true, false)
        .unwrap();
    let file_num = base.file_number();
    let path = dir.path().join(format!("{file_num:08x}.ndb"));
    let eof_base = std::fs::metadata(&path).unwrap().len();
    // A small buffered record: flush=false, fsync=false -> lives in the write
    // buffer, its bytes are NOT on disk yet.
    lm.log(LogEntryType::Trace, b"buffered-nosync", Provisional::No, false, false)
        .unwrap();
    // The next LSN marks the exclusive end of the buffered range.
    let eol_before = lm.file_manager().get_next_available_lsn();
    assert_eq!(eol_before.file_number(), file_num);

    // Freeze the no-sync Phase-2 pwrite by holding the file's write latch.
    let handle = lm.file_manager().get_file_handle(file_num).unwrap();
    let held = handle.acquire().unwrap();

    // Thread N: flush_no_sync. On the buggy code it marks the range flushed
    // under the LWL, releases the LWL, then blocks on the file latch in its
    // off-LWL pwrite. On the fixed code it blocks on the file latch WHILE
    // holding the LWL.
    let nosync_lm = Arc::clone(&lm);
    let nosync = std::thread::spawn(move || nosync_lm.flush_no_sync());

    // Give thread N time to reach (and block at) the pwrite.
    std::thread::sleep(Duration::from_millis(200));

    // Thread S: covering flush_sync. On the buggy code it acquires the LWL
    // (thread N already released it), skips the already-marked range, captures
    // eol past it, fdatasyncs and returns a durable LSN covering un-pwritten
    // bytes. On the fixed code it BLOCKS on the LWL held by thread N.
    let (tx, rx) = std::sync::mpsc::channel();
    let sync_lm = Arc::clone(&lm);
    let sync = std::thread::spawn(move || tx.send(sync_lm.flush_sync()).unwrap());

    // Observe an EARLY sync completion (the bug) before releasing the latch.
    let early = rx.recv_timeout(Duration::from_millis(400)).ok();
    let eof_at_early = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    eprintln!(
        "eol_before={eol_before:?} eof_base={eof_base} early sync={early:?} EOF@early={eof_at_early}"
    );
    if let Some(Ok(durable)) = early {
        assert!(
            (durable.file_number() < file_num)
                || (durable.file_number() == file_num
                    && (durable.file_offset() as u64) <= eof_at_early),
            "durable watermark {durable:?} covers bytes past physical EOF {eof_at_early} \
             (false durable boundary over an un-pwritten no-sync range)"
        );
    }

    // Release the latch so the frozen pwrite (and the blocked sync, if any)
    // can complete, then join both threads.
    drop(held);
    let nosync_res = nosync.join().unwrap();
    sync.join().unwrap();
    nosync_res.unwrap();
    let durable = rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    let eof_final = std::fs::metadata(&path).unwrap().len();
    eprintln!("final durable={durable:?} EOF_final={eof_final}");
    assert!(
        (durable.file_number() < file_num)
            || (durable.file_number() == file_num
                && (durable.file_offset() as u64) <= eof_final),
        "final durable watermark {durable:?} must not exceed on-disk EOF {eof_final}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn partial_write_error_requires_fail_stop_or_complete_rollback() {
    // Isolate the process's soft file-size limit and ignored SIGXFSZ. No unsafe
    // code or production injection hooks; the kernel performs the short write.
    if std::env::var_os("NOXU_PARTIAL_APPEND_CHILD").is_none() {
        let output = std::process::Command::new("bash")
            .args(["-c", "trap '' XFSZ; ulimit -Sf 2; exec \"$1\" --exact partial_write_error_requires_fail_stop_or_complete_rollback --nocapture", "bash"])
            .arg(std::env::current_exe().unwrap())
            .env("NOXU_PARTIAL_APPEND_CHILD", "1")
            .output().unwrap();
        eprintln!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "partial-write child failed: {}",
            output.status
        );
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let lm = manager(&dir);
    let old = lm
        .log(LogEntryType::Trace, b"old", Provisional::No, true, true)
        .unwrap();
    let start = lm.file_manager().get_next_available_lsn();
    let result =
        lm.log(LogEntryType::Trace, &[9; 4096], Provisional::No, false, false);
    assert!(result.is_err());
    let eof = std::fs::metadata(
        dir.path().join(format!("{:08x}.ndb", start.file_number())),
    )
    .unwrap()
    .len();
    assert!(
        eof > start.file_offset() as u64
            && eof < start.file_offset() as u64 + 4096,
        "kernel must leave a nonempty incomplete prefix, EOF={eof}"
    );
    assert_eq!(lm.read_entry(old).unwrap().1, b"old");
    eprintln!(
        "partial write start={start:?}, EOF={eof}, result={result:?}, invalid={}",
        lm.is_io_invalid()
    );
    assert!(
        lm.is_io_invalid(),
        "partial append remains on disk and LSN is advanced: continuing is unsafe without rollback"
    );
}
