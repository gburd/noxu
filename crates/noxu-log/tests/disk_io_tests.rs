//! Integration tests for the log disk I/O paths.
//!
//! These tests exercise the write path (LogManager::log -> FileManager::write_buffer),
//! the read path (LogManager::read_entry, LogFileReader), CRC validation, file
//! flipping, and fsync semantics.
//!
//! All tests use `tempfile::TempDir` for isolation.

use noxu_log::{
    FileManager, LogFileReader, LogManager, entry_type::LogEntryType,
    provisional::Provisional,
};
use std::sync::Arc;
use tempfile::TempDir;

// ---- helpers ----------------------------------------------------------------

/// Create a FileManager + LogManager pair in a fresh temp directory.
fn make_managers(dir: &TempDir) -> (Arc<FileManager>, LogManager) {
    let fm =
        Arc::new(FileManager::new(dir.path(), false, 10_000_000, 100).unwrap());
    let lm = LogManager::new(Arc::clone(&fm), 3, 1_048_576, 4096);
    (fm, lm)
}

/// Create managers with a very small max file size to trigger flips quickly.
fn make_managers_small_file(
    dir: &TempDir,
    max_file_size: u64,
) -> (Arc<FileManager>, LogManager) {
    let fm = Arc::new(
        FileManager::new(dir.path(), false, max_file_size, 100).unwrap(),
    );
    // Use a buffer pool large enough (2 MB) so entries always fit in a buffer.
    let lm = LogManager::new(Arc::clone(&fm), 3, 2_097_152, 4096);
    (fm, lm)
}

// ---- tests ------------------------------------------------------------------

/// Write one entry, read it back by LSN, verify the payload matches.
///
/// equivalent: LogManager.logItem() -> LogManager.getLogEntry().
#[test]
fn test_write_and_read_entry() {
    let dir = TempDir::new().unwrap();
    let (fm, lm) = make_managers(&dir);

    let payload = b"noxu write-and-read test";

    // Write the entry and force a flush so bytes reach the file.
    let lsn = lm
        .log(LogEntryType::Trace, payload, Provisional::No, true, false)
        .unwrap();

    // Read back by LSN.  The entry must be on disk because flush was requested.
    let (entry_type, read_payload) = lm.read_entry(lsn).unwrap();

    assert_eq!(entry_type, LogEntryType::Trace);
    assert_eq!(read_payload.as_slice(), payload);

    drop(fm); // silence unused warning
}

/// Write entries until the file flips, verify a second .ndb file is created.
///
/// equivalent: FileManager.shouldFlipFile() -> flipFile().
#[test]
fn test_file_flip() {
    let dir = TempDir::new().unwrap();

    // Each payload is 4 KB; with a 10 KB max file size we need 3+ writes
    // before a flip occurs.  The file header (20 B) plus each entry header
    // (14 B) are counted, so a 4096-byte payload entry is ~4110 bytes total.
    // Two such entries (8220 + 20 header) exceed the 10 KB max.
    let max_size: u64 = 10_240;
    let (fm, lm) = make_managers_small_file(&dir, max_size);

    // Large enough payload to trigger a flip in a handful of writes.
    let payload = vec![0xABu8; 4096];

    for _ in 0..4 {
        lm.log(LogEntryType::Trace, &payload, Provisional::No, true, false)
            .unwrap();
    }

    // At least two log files should exist.
    let files = fm.list_file_numbers().unwrap();
    assert!(
        files.len() >= 2,
        "expected at least 2 log files, found {:?}",
        files
    );
}

/// Corrupt a byte in the log file and verify that `LogFileReader::read_next_strict`
/// returns a checksum error.
///
/// equivalent: ChecksumValidator mismatch -> ChecksumException.
///
/// NOTE: We use `LogFileReader::read_next_strict` (which always reads from
/// disk) rather than `LogManager::read_entry` (which checks the write-buffer
/// pool first).  This avoids the hot-path short-circuit that would return the
/// uncorrupted in-memory copy.
#[test]
fn test_crc_validation_on_read() {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    let dir = TempDir::new().unwrap();
    let (fm, lm) = make_managers(&dir);

    let payload = b"data to corrupt";
    let lsn = lm
        .log(LogEntryType::Trace, payload, Provisional::No, true, false)
        .unwrap();

    // Flush all buffers to disk so the entry is definitely on disk.
    lm.flush_sync().unwrap();
    // Clear the file-handle cache so the next read opens the file fresh.
    fm.clear_cache();

    // Corrupt a payload byte inside the entry.
    // The entry starts at lsn.file_offset(); the payload begins after the
    // 14-byte fixed header.
    let payload_byte_offset = lsn.file_offset() as u64
        + noxu_log::entry_header::MIN_HEADER_SIZE as u64;

    let file_path = dir.path().join(format!("{:08x}.ndb", lsn.file_number()));

    {
        let mut f = OpenOptions::new().write(true).open(&file_path).unwrap();
        f.seek(SeekFrom::Start(payload_byte_offset)).unwrap();
        // Flip the first payload byte - this invalidates the CRC.
        let original = payload[0];
        f.write_all(&[original ^ 0xFF]).unwrap();
        f.flush().unwrap();
    }

    // Now read via LogFileReader which always goes to disk.
    fm.clear_cache();
    let mut reader =
        LogFileReader::open(Arc::clone(&fm), lsn.file_number()).unwrap();

    let result = reader.read_next_strict();
    assert!(
        result.is_err(),
        "Expected a checksum error but got {:?}",
        result.ok()
    );
    match result.unwrap_err() {
        noxu_log::NoxuLogError::Checksum { .. } => {} // expected
        other => panic!("Expected Checksum error, got {:?}", other),
    }
}

/// Write 10 entries, then use LogFileReader to read all 10 in forward order.
///
/// equivalent: FileReader.readNextEntry() loop.
#[test]
fn test_sequential_read() {
    let dir = TempDir::new().unwrap();
    let (fm, lm) = make_managers(&dir);

    let n_entries = 10usize;
    let mut lsns = Vec::with_capacity(n_entries);

    for i in 0..n_entries {
        let payload = format!("entry-{}", i);
        let lsn = lm
            .log(
                LogEntryType::Trace,
                payload.as_bytes(),
                Provisional::No,
                false,
                false,
            )
            .unwrap();
        lsns.push(lsn);
    }

    // Flush all buffered data to disk.
    lm.flush_no_sync().unwrap();

    // All entries land in file 0 (payload is small).
    let file_num = lsns[0].file_number();
    let mut reader = LogFileReader::open(Arc::clone(&fm), file_num).unwrap();

    let mut count = 0usize;
    while let Some((_lsn, entry_type, _payload)) = reader.read_next() {
        assert_eq!(entry_type, LogEntryType::Trace);
        count += 1;
    }

    assert_eq!(
        count, n_entries,
        "expected {} entries, LogFileReader found {}",
        n_entries, count
    );
}

/// After sync(), data must be readable even if the process were to restart
/// (simulated by clearing the file cache and reading from disk directly).
///
/// equivalent: FileManager.syncLogEnd() guarantees durability.
#[test]
fn test_sync_flushes_to_disk() {
    let dir = TempDir::new().unwrap();
    let (fm, lm) = make_managers(&dir);

    let payload = b"durable data";
    let lsn = lm
        .log(LogEntryType::Trace, payload, Provisional::No, false, false)
        .unwrap();

    // Issue an fsync - data must now be on stable storage.
    lm.flush_sync().unwrap();

    // Simulate "process restart" by clearing every in-memory cache and
    // re-reading directly from the file.
    fm.clear_cache();
    drop(lm); // destroy the write-buffer pool

    // Open a fresh read-only FileManager pointing at the same directory.
    // The environment lock has been released because `fm` will be dropped
    // at end of scope, so we just use `fm` itself for the read test since
    // the lock is still held.  Instead, verify via LogFileReader directly.
    let mut reader =
        LogFileReader::open(Arc::clone(&fm), lsn.file_number()).unwrap();

    let result = reader.read_next();
    assert!(result.is_some(), "entry must be on disk after sync");
    let (_read_lsn, entry_type, read_payload) = result.unwrap();
    assert_eq!(entry_type, LogEntryType::Trace);
    assert_eq!(read_payload.as_slice(), payload);
}
