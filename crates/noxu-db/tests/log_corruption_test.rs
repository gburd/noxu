//! C6 — log-file corruption detection.
//!
//! Faithful in spirit to JE `com.sleepycat.je.util.LogFileCorruptionTest`
//! (`testDataCorruptWithVerifier`): a committed workload is written, the
//! environment is closed, a byte inside a committed log file is FLIPPED
//! (JE seeks to `fileLength / 2` and rewrites one byte), and the corruption
//! must be DETECTED — JE raises `EnvironmentFailureException`.
//!
//! Noxu CRC32s every log entry on read (`LogFileReader` /
//! `LogEntryHeader::read_from_log` validate the per-entry checksum, and the
//! v3 file header carries its own CRC). A flipped byte in a committed entry
//! must therefore be CAUGHT — either surfaced as a recovery error, or treated
//! as a torn/end-of-log boundary so the corrupt bytes are NEVER silently
//! returned as valid data. This test proves a flipped committed entry does not
//! silently corrupt the recovered data set.
//!
//! Two scenarios (both faithful to the JE corruption / torn-write model):
//!   1. Flip a byte inside a committed entry in a NON-final log file (the
//!      classic media-corruption case — JE's `fileLength/2` flip).
//!   2. Truncate the last log file mid-entry (torn write) and confirm the
//!      torn tail is not returned as data.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, OperationStatus,
};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn open_env(dir: &Path) -> noxu_db::Environment {
    // Small log files so the workload spans several files and we can corrupt
    // a non-final (already-committed, fsync'd) file.
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_log_file_max_bytes(4096);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "corruptdb",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    )
    .unwrap()
}

fn list_log_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "ndb").unwrap_or(false))
        .collect();
    files.sort();
    files
}

/// Write `n` committed keys, one per single-key transaction, then close.
fn write_committed_workload(dir: &Path, n: u32) {
    let env = open_env(dir);
    let db = open_db(&env);
    for i in 0..n {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(format!("k_{i:05}").as_bytes()),
            DatabaseEntry::from_bytes(format!("v_{i:05}").as_bytes()),
        )
        .unwrap();
        txn.commit().unwrap();
    }
    db.close().unwrap();
    env.close().unwrap();
}

/// Try to open + scan; return the recovered key set, or an Err string if
/// recovery / scan signalled the corruption.
fn try_recover_and_scan(
    dir: &Path,
) -> Result<std::collections::BTreeMap<Vec<u8>, Vec<u8>>, String> {
    // Recovery happens during open; a detected corruption surfaces here.
    let result = std::panic::catch_unwind(|| {
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(dir.to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .map_err(|e| format!("open/recovery error: {e}"))?;
        let db = env
            .open_database(
                None,
                "corruptdb",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .map_err(|e| format!("open_database error: {e}"))?;

        let mut cursor = db
            .open_cursor(None)
            .map_err(|e| format!("open_cursor error: {e}"))?;
        let mut map = std::collections::BTreeMap::new();
        let mut key = DatabaseEntry::new();
        let mut val = DatabaseEntry::new();
        loop {
            match cursor.get(&mut key, &mut val, noxu_db::Get::Next, None) {
                Ok(OperationStatus::Success) => {
                    map.insert(
                        key.data_opt().unwrap_or(&[]).to_vec(),
                        val.data_opt().unwrap_or(&[]).to_vec(),
                    );
                }
                Ok(_) => break,
                Err(e) => return Err(format!("scan error: {e}")),
            }
        }
        let _ = cursor.close();
        Ok(map)
    });
    match result {
        Ok(inner) => inner,
        Err(_) => Err("panic during recovery/scan".to_string()),
    }
}

// ---------------------------------------------------------------------------
// C6.1 — byte flip inside a committed entry must be detected
// ---------------------------------------------------------------------------

/// JE `LogFileCorruptionTest.testDataCorruptWithVerifier`: flip a byte at
/// `fileLength / 2` of a committed log file; the corruption must be detected.
///
/// Noxu invariant: a flipped byte inside a committed entry must NOT be
/// silently returned as valid data. Either recovery errors, or the entry's
/// CRC mismatch makes it (and everything after it in that file) treated as a
/// torn-write boundary. In all cases the recovered data set must NOT contain a
/// silently-corrupted value, and the corruption must be observable as either
/// an error or a truncated prefix of the committed set.
#[test]
fn byte_flip_in_committed_entry_is_detected() {
    let dir = TempDir::new().unwrap();
    let n = 200u32;
    write_committed_workload(dir.path(), n);

    // The full, uncorrupted expected set.
    let mut full_expected = std::collections::BTreeMap::new();
    for i in 0..n {
        full_expected.insert(
            format!("k_{i:05}").into_bytes(),
            format!("v_{i:05}").into_bytes(),
        );
    }

    // Sanity: clean reopen sees everything (proves the workload + recovery are
    // correct before we corrupt anything).
    {
        let clean = try_recover_and_scan(dir.path())
            .expect("clean reopen before corruption must succeed");
        assert_eq!(
            clean, full_expected,
            "pre-corruption clean recovery must see all {n} committed keys"
        );
    }

    // Pick a NON-final log file (already committed + fsync'd) to corrupt, like
    // JE's media-corruption case. Fall back to the only file if there is one.
    let files = list_log_files(dir.path());
    assert!(!files.is_empty(), "expected at least one .ndb file");
    let target = if files.len() >= 2 {
        files[files.len() / 2].clone()
    } else {
        files[0].clone()
    };

    // JE: seek to fileLength/2 and flip one byte (b = (b == 0) ? 1 : 0 style).
    {
        let mut bytes = std::fs::read(&target).unwrap();
        assert!(bytes.len() > 64, "log file too small to corrupt meaningfully");
        let pos = bytes.len() / 2;
        bytes[pos] ^= 0xFF; // flip every bit of one byte
        std::fs::write(&target, &bytes).unwrap();
    }

    // Reopen + scan. The corruption MUST be detected: either an error, or the
    // recovered set is a STRICT prefix of the committed set (the corrupt entry
    // and everything after it in that file is dropped at the torn boundary).
    // It must NEVER silently equal the full set with a wrong value, nor return
    // a corrupted value.
    match try_recover_and_scan(dir.path()) {
        Err(_e) => {
            // Detected via error — acceptable (JE EnvironmentFailureException).
        }
        Ok(recovered) => {
            // No silently-corrupted value: every recovered value must be a
            // correct "v_NNNNN" matching its key (i.e. no garbage was returned
            // as data).
            for (k, v) in &recovered {
                let expected = full_expected.get(k);
                assert_eq!(
                    Some(v),
                    expected,
                    "corruption returned a wrong/garbage value for key {:?}: \
                     got {:?}",
                    std::str::from_utf8(k),
                    std::str::from_utf8(v),
                );
            }
            // The corruption must have had an observable effect: the recovered
            // set is a strict subset of the full set (some committed keys in
            // and after the corrupt entry were dropped at the torn boundary).
            // If it silently equaled the full set, the corruption was masked.
            assert!(
                recovered.len() < full_expected.len(),
                "byte flip in a committed entry was SILENTLY MASKED: recovered \
                 set equals the full committed set ({} keys) despite \
                 corruption — corruption was not detected",
                recovered.len()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// C6.2 — mid-entry truncation (torn write) must not return the torn tail
// ---------------------------------------------------------------------------

/// Truncate the last log file mid-entry (torn write). Recovery must treat the
/// torn tail as end-of-log (CRC / short-read boundary) and never return the
/// torn bytes as data. The recovered set must be a valid prefix of the
/// committed set with no garbage values.
#[test]
fn mid_entry_truncation_torn_tail_not_returned() {
    let dir = TempDir::new().unwrap();
    let n = 200u32;
    write_committed_workload(dir.path(), n);

    let mut full_expected = std::collections::BTreeMap::new();
    for i in 0..n {
        full_expected.insert(
            format!("k_{i:05}").into_bytes(),
            format!("v_{i:05}").into_bytes(),
        );
    }

    let files = list_log_files(dir.path());
    let last = files.last().unwrap().clone();

    // Truncate the last file by a few bytes so its final entry is torn.
    {
        let len = std::fs::metadata(&last).unwrap().len();
        assert!(len > 32, "last file too small");
        // Cut 7 bytes — lands inside the final entry's payload/header.
        let new_len = len - 7;
        let f = std::fs::OpenOptions::new().write(true).open(&last).unwrap();
        f.set_len(new_len).unwrap();
    }

    match try_recover_and_scan(dir.path()) {
        Err(_e) => {
            // Detected via error — acceptable.
        }
        Ok(recovered) => {
            // No garbage values.
            for (k, v) in &recovered {
                assert_eq!(
                    full_expected.get(k),
                    Some(v),
                    "torn-tail recovery returned a wrong value for key {:?}",
                    std::str::from_utf8(k),
                );
            }
            // The recovered set is a subset of the committed set (the torn
            // final entry, if it was a committed record, may be dropped).
            assert!(
                recovered.len() <= full_expected.len(),
                "torn-tail recovery produced MORE keys than were committed"
            );
            for k in recovered.keys() {
                assert!(
                    full_expected.contains_key(k),
                    "torn-tail recovery surfaced a key that was never \
                     committed: {:?}",
                    std::str::from_utf8(k)
                );
            }
        }
    }
}
