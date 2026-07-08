// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! End-to-end integration test for `EnvironmentConfig::with_halt_on_commit_
//! after_checksum_exception`.
//!
//! By default, if end-of-log discovery hits a checksum error near the end
//! of the log, the reader assumes an ordinary torn write from an
//! interrupted crash and quietly truncates the log there. When this flag
//! is enabled, a checksum error instead triggers a forward scan looking
//! for a committed transaction beyond the corruption point; if one is
//! found, that is a sign of real corruption (not just a torn tail — data
//! written and committed AFTER the corrupted region would otherwise be
//! silently discarded), so the environment refuses to open rather than
//! truncate away possibly-committed data.
//!
//! The scanner-level logic (`FileManagerLogScanner::find_end_of_log`) is
//! exhaustively unit-tested in `noxu-dbi/src/file_manager_scanner.rs`
//! (L-14). This test proves the public-API wiring end to end: a real
//! `Environment`, opened with the flag set, refuses to mount a log whose
//! mid-file corruption is followed by a committed transaction.
//!
//! Uses EXPLICIT transactions: an auto-commit put uses a lightweight
//! locker that writes no separate `TxnCommit` WAL record for a single
//! completed operation (there is nothing to roll back), so a detectable
//! "committed txn after corruption" only exists with explicit commits.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::io::{Seek, SeekFrom, Write};

fn scratch(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "noxu-halt-checksum-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn write_explicit_commits(dir: &std::path::Path, n: u32) {
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_transactional(true)
            .with_allow_create(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    for i in 0..n {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(&txn, format!("k{i:04}").as_bytes(), b"v").unwrap();
        txn.commit().unwrap();
    }
    db.close().unwrap();
    env.close().unwrap();
}

/// Locate every `TxnCommit` (type 30) record by walking the log file's
/// entries via header-declared sizes (mirrors `parse_entry_from_bytes`
/// sizing) and corrupt the payload of a commit that is NOT the last one, so
/// at least one further committed txn remains on disk after the
/// corruption point.
fn corrupt_before_last_commit(path: &std::path::Path) {
    const MIN_HEADER_SIZE: usize = 14;
    const FILE_HEADER_SIZE: usize = 36; // noxu_log::file_header::FILE_HEADER_SIZE (v3)
    const TXN_COMMIT: u8 = 30;

    let bytes = std::fs::read(path).unwrap();
    let mut commit_offsets = Vec::new();
    let mut offset = FILE_HEADER_SIZE;
    while offset + MIN_HEADER_SIZE <= bytes.len() {
        let hdr = &bytes[offset..offset + MIN_HEADER_SIZE];
        if hdr[4] == 0 {
            break;
        }
        let entry_type = hdr[4];
        let flags = hdr[5];
        let item_size =
            u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]]) as usize;
        let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
        let header_size =
            if vlsn_present { MIN_HEADER_SIZE + 8 } else { MIN_HEADER_SIZE };
        let entry_size = header_size + item_size;
        if offset + entry_size > bytes.len() {
            break;
        }
        if entry_type == TXN_COMMIT {
            commit_offsets.push(offset);
        }
        offset += entry_size;
    }
    assert!(
        commit_offsets.len() >= 2,
        "need >=2 TxnCommit records to corrupt one that isn't the last; \
         found {}",
        commit_offsets.len()
    );
    // Corrupt the first commit's payload: every later commit remains valid
    // and readable after it.
    let target = commit_offsets[0];
    let corrupt_at = target as u64 + MIN_HEADER_SIZE as u64 + 1;
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(corrupt_at)).unwrap();
    f.write_all(&[0xDE]).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn halt_on_commit_after_checksum_exception_refuses_to_mount() {
    // Case A: default (flag disabled) must tolerate the corruption via the
    // common-case truncate-and-continue (no hard failure on open).
    {
        let dir = scratch("default");
        write_explicit_commits(&dir, 10);
        corrupt_before_last_commit(&dir.join("00000000.ndb"));

        let env = Environment::open(
            EnvironmentConfig::new(dir.clone()).with_transactional(true),
        );
        assert!(
            env.is_ok(),
            "default (halt disabled) must tolerate the corruption via \
             truncate-and-continue (the normal recovery-from-a-crash path); \
             got {:?}",
            env.err()
        );
        env.unwrap().close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Case B: flag enabled must REFUSE to mount — this guards exactly the
    // case (corrupted entry followed by a later committed txn) that
    // indicates real corruption, by refusing to open instead of silently
    // discarding the committed data via truncation.
    {
        let dir = scratch("refuse");
        write_explicit_commits(&dir, 10);
        corrupt_before_last_commit(&dir.join("00000000.ndb"));

        let env = Environment::open(
            EnvironmentConfig::new(dir.clone())
                .with_transactional(true)
                .with_halt_on_commit_after_checksum_exception(true),
        );
        assert!(
            env.is_err(),
            "halt_on_commit_after_checksum_exception=true must refuse to \
             mount a log with a committed txn after mid-file corruption \
             instead of silently truncating committed data"
        );
        let msg = format!("{}", env.err().unwrap());
        assert!(
            msg.contains("FoundCommittedTxn") || msg.contains("committed"),
            "error should surface the FoundCommittedTxn reason; got: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
