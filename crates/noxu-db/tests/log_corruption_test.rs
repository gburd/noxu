//! C6 — log-file corruption detection.
//!
//! Faithful in spirit to JE `com.sleepycat.je.util.LogFileCorruptionTest`
//! (`testDataCorruptWithVerifier` / `testDataCorruptWithoutVerifier`): a
//! committed workload is written, a byte inside a committed log file is
//! FLIPPED (JE seeks to `fileLength / 2` and rewrites one byte), and the
//! behaviour of the reader is checked.
//!
//! Noxu CRC32s every log entry ON READ.  The production recovery scanner
//! (`noxu-dbi/src/file_manager_scanner.rs::parse_entry_from_bytes`) and the
//! non-recovery reader (`file_reader.rs`) both validate the per-entry
//! checksum, and the v3 file header carries its own CRC.  So a flipped byte in
//! an entry that recovery **actually reads and replays** is CAUGHT: recovery
//! either surfaces a recovery error, or treats the mismatch as a torn/end-of-
//! log boundary and drops the corrupt entry (and everything after it in that
//! file) — the corrupt bytes are NEVER silently returned as valid data.
//!
//! IMPORTANT MODELLING NOTE (NEW-REC-3): Noxu's per-entry CRC only runs on
//! entries recovery reads.  A byte flip inside a log entry that a *later
//! checkpoint has made obsolete* — e.g. a small LN whose value was embedded
//! into a checkpointed BIN (`TREE_MAX_EMBEDDED_LN`, default 16 bytes), so
//! recovery rebuilds the tree from the BIN and never re-reads the LN — is
//! NOT observed by crash recovery, because that region is legitimately never
//! read.  JE only detects THAT case via its background `DataVerifier` /
//! `DbVerifyLog` log-file scrubber (proactive CRC over ALL log files); JE's
//! own `testDataCorruptWithoutVerifier` asserts the flip is NOT caught
//! without that scrubber.  Noxu's `VerifyDaemon` implements only the
//! `BtreeVerifier` (structural, in-memory) half — there is no `DbVerifyLog`
//! log-file scrubber — so proactive detection of obsolete-region corruption
//! is a tracked, not-yet-implemented gap (see the `#[ignore]`d
//! `obsolete_region_flip_requires_log_scrubber_new_rec3` below).
//!
//! The two live tests here therefore assert the corruption detection Noxu
//! genuinely HAS (per-entry CRC on the replay path), both faithful to the JE
//! corruption / torn-write model:
//!   1. Flip a byte inside a committed entry that recovery REPLAYS (the entry
//!      is still live at recovery: a large, non-embedded value, and no clean
//!      final checkpoint superseded it because the writer crashed) — recovery
//!      detects the CRC mismatch and drops at the corrupt boundary.
//!   2. Truncate the last log file mid-entry (torn write) and confirm the
//!      torn tail is not returned as data.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn open_env(dir: &Path) -> noxu_db::Environment {
    // Small log files so the workload spans several files and we can corrupt
    // a committed, fsync'd file.  Daemons off so recovery, not a background
    // pass, is what we exercise.
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_log_file_max_bytes(4096);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_verifier(false);
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

/// A large (64-byte) value string keyed by `i`.  64 > `TREE_MAX_EMBEDDED_LN`
/// (default 16) so the LN is NEVER embedded into its BIN slot: recovery must
/// read the LN log entry itself (and thus CRC-validate it) to reconstruct the
/// record.  This is what makes the corrupted entry *live at recovery*, unlike
/// a small embedded value whose LN a checkpoint would make obsolete.
fn big_value(i: u32) -> Vec<u8> {
    let mut v = format!("v_{i:05}").into_bytes();
    v.resize(64, b'X');
    v
}

/// Scan the whole database into a BTreeMap, or return an Err string if
/// recovery / scan signalled the corruption (either via an open/recovery
/// error or a panic during recovery).
fn recover_and_scan(
    dir: &Path,
) -> Result<std::collections::BTreeMap<Vec<u8>, Vec<u8>>, String> {
    let dir = dir.to_path_buf();
    let result = std::panic::catch_unwind(move || {
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(dir)
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
            match cursor.get(&mut key, &mut val, Get::Next, None) {
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

/// Copy every `.ndb` file from `src` into a fresh temp dir and return it, so a
/// corruption / recovery run operates on a pristine snapshot without a prior
/// recovery having truncated or checkpointed the originals.
fn snapshot_log(src: &Path) -> TempDir {
    let dst = TempDir::new().unwrap();
    for f in list_log_files(src) {
        std::fs::copy(&f, dst.path().join(f.file_name().unwrap())).unwrap();
    }
    dst
}

// ---------------------------------------------------------------------------
// C6.1 — byte flip inside a REPLAYED committed entry must be detected
// ---------------------------------------------------------------------------

/// JE `LogFileCorruptionTest.testDataCorruptWithVerifier`: flip a byte at
/// `fileLength / 2` of a committed log file; the corruption must be detected.
///
/// Noxu invariant asserted here (the real, working property): a flipped byte
/// inside a committed entry that recovery REPLAYS must NOT be silently
/// returned as valid data.  The entry's CRC mismatch makes recovery either
/// error, or treat it (and everything after it in that file) as a torn-write
/// boundary and drop it.  In all cases the recovered set contains NO
/// silently-corrupted value, and the corruption is observable as an error or
/// a strict-subset prefix of the committed set.
///
/// To keep the corrupted entry LIVE AT RECOVERY (so recovery actually reads +
/// CRC-validates it, rather than skipping it as obsolete):
///   * the value is 64 bytes — larger than `TREE_MAX_EMBEDDED_LN` (16) — so
///     the LN is not embedded into a BIN slot, and
///   * the writer CRASHES (child process exits without a clean close), so no
///     final checkpoint flushes a superseding BIN that would let recovery
///     skip the LN.
///
/// JE parity: RecoveryEdgeTest.testBadChecksum — a corrupt/checksum-failing
/// region in the log must never be returned as valid data (JE recovers the
/// committed prefix / raises EnvironmentFailureException).
#[test]
fn byte_flip_in_committed_entry_is_detected() {
    const CHILD_MODE: &str = "NOXU_LOG_CORRUPT_CHILD";
    const CHILD_HOME: &str = "NOXU_LOG_CORRUPT_HOME";
    let n = 200u32;

    // Child process: write the committed workload, then CRASH (no clean
    // close, no final checkpoint) so the LNs stay live at recovery.
    if std::env::var(CHILD_MODE).is_ok() {
        let home = std::env::var_os(CHILD_HOME).unwrap();
        let env = open_env(Path::new(&home));
        let db = open_db(&env);
        for i in 0..n {
            let txn = env.begin_transaction(None).unwrap();
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(format!("k_{i:05}").as_bytes()),
                DatabaseEntry::from_bytes(&big_value(i)),
            )
            .unwrap();
            txn.commit().unwrap();
        }
        // Crash: no close, no final checkpoint.
        std::process::exit(73);
    }

    let dir = TempDir::new().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "byte_flip_in_committed_entry_is_detected",
            "--nocapture",
        ])
        .env(CHILD_MODE, "1")
        .env(CHILD_HOME, dir.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73), "child did not reach the crash point");

    // The full, uncorrupted expected set.
    let mut full_expected = std::collections::BTreeMap::new();
    for i in 0..n {
        full_expected.insert(format!("k_{i:05}").into_bytes(), big_value(i));
    }

    // Control: clean crash-recovery on a pristine snapshot sees everything
    // (proves the workload + recovery are correct before we corrupt anything).
    {
        let clean_dir = snapshot_log(dir.path());
        let clean = recover_and_scan(clean_dir.path())
            .expect("clean crash-recovery before corruption must succeed");
        assert_eq!(
            clean, full_expected,
            "pre-corruption clean crash-recovery must see all {n} committed keys"
        );
    }

    // Corrupt a committed entry that recovery replays: seek to fileLength/2 of
    // the last log file (which holds live, un-checkpointed committed LNs) on a
    // PRISTINE snapshot and flip one byte, like JE's media-corruption case.
    let corrupt_dir = snapshot_log(dir.path());
    let files = list_log_files(corrupt_dir.path());
    assert!(!files.is_empty(), "expected at least one .ndb file");
    let target = files.last().unwrap().clone();
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
    // It must NEVER silently equal the full set, nor return a corrupted value.
    match recover_and_scan(corrupt_dir.path()) {
        Err(_e) => {
            // Detected via error — acceptable (JE EnvironmentFailureException).
        }
        Ok(recovered) => {
            // No silently-corrupted value: every recovered value must be the
            // correct value for its key (no garbage was returned as data).
            for (k, v) in &recovered {
                let expected = full_expected.get(k);
                assert_eq!(
                    Some(v),
                    expected,
                    "corruption returned a wrong/garbage value for key {:?}",
                    std::str::from_utf8(k),
                );
            }
            // The corruption must have had an observable effect: the recovered
            // set is a strict subset of the full set (the corrupt entry and
            // those after it in that file were dropped at the torn boundary).
            // If it silently equaled the full set, the corruption was masked.
            assert!(
                recovered.len() < full_expected.len(),
                "byte flip in a REPLAYED committed entry was SILENTLY MASKED: \
                 recovered set equals the full committed set ({} keys) despite \
                 corruption — the per-entry CRC did not detect it",
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
// JE parity: RecoveryEdgeTest.testNoCheckpointEnd / testBadChecksumReadOnly-
// ReadPastLastFile — a torn tail must be treated as end-of-log and never
// surfaced as data.
#[test]
fn mid_entry_truncation_torn_tail_not_returned() {
    let dir = TempDir::new().unwrap();
    let n = 200u32;
    {
        let env = open_env(dir.path());
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

    match recover_and_scan(dir.path()) {
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

// ---------------------------------------------------------------------------
// NEW-REC-3 — proactive log-file scrubbing (DbVerifyLog) is not implemented
// ---------------------------------------------------------------------------

/// TRACKING REPRO for NEW-REC-3 (currently `#[ignore]`d — a not-yet-
/// implemented feature, not a live red test).
///
/// A byte flip inside a log entry that a later checkpoint has made OBSOLETE —
/// specifically a small LN whose value was embedded into a checkpointed BIN
/// (`TREE_MAX_EMBEDDED_LN`, default 16 bytes), so recovery rebuilds the tree
/// from the BIN and never re-reads the LN — is NOT detected by crash recovery,
/// because recovery legitimately never reads that region.  Recovery's
/// per-entry CRC therefore cannot see it.
///
/// JE catches this case ONLY via its background `DataVerifier` /
/// `DbVerifyLog` log-file scrubber, which proactively re-CRCs EVERY log file
/// (including old/obsolete ones) on a schedule (`VERIFY_SCHEDULE`).  JE's own
/// `LogFileCorruptionTest.testDataCorruptWithoutVerifier` asserts that WITHOUT
/// that scrubber the same byte flip is NOT caught.  Noxu's `VerifyDaemon`
/// implements only the `BtreeVerifier` (structural, in-memory B-tree) half of
/// JE's `DataVerifier`; it has NO `DbVerifyLog` log-file CRC scrubber, so
/// proactive detection of obsolete-region corruption is unimplemented.
///
/// This test writes small (embedded) values with a clean close (so a final
/// checkpoint embeds them into BINs and obsoletes the LNs), flips a byte in a
/// non-final file's obsolete LN, and asserts the flip IS detected.  It FAILS
/// today (the flip is invisible) and will PASS once a `DbVerifyLog`-style
/// proactive log-file scrubber is added.
///
/// JE ref: `com.sleepycat.je.util.LogFileCorruptionTest`
/// (`testDataCorruptWithVerifier` / `testDataCorruptWithoutVerifier`),
/// `com.sleepycat.je.util.DbVerifyLog`, `com.sleepycat.je.dbi.DataVerifier`.
#[test]
#[ignore = "NEW-REC-3: requires a DbVerifyLog background log-file CRC scrubber \
            (proactive verification of old/obsolete log files); Noxu's \
            VerifyDaemon implements only the structural BtreeVerifier. JE's \
            own testDataCorruptWithoutVerifier confirms this flip is NOT \
            caught without the scrubber."]
fn obsolete_region_flip_requires_log_scrubber_new_rec3() {
    let dir = TempDir::new().unwrap();
    let n = 200u32;
    // Small values (<= TREE_MAX_EMBEDDED_LN) + a clean close: the final
    // checkpoint embeds each value into its BIN slot and the LN becomes
    // obsolete, so recovery never re-reads the LN log entries.
    {
        let env = open_env(dir.path());
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

    let mut full_expected = std::collections::BTreeMap::new();
    for i in 0..n {
        full_expected.insert(
            format!("k_{i:05}").into_bytes(),
            format!("v_{i:05}").into_bytes(),
        );
    }

    // Flip a byte at fileLength/2 of a NON-final (already checkpoint-obsoleted)
    // committed file — JE's classic media-corruption case.
    let files = list_log_files(dir.path());
    assert!(files.len() >= 2, "expected several .ndb files");
    let target = files[files.len() / 2].clone();
    {
        let mut bytes = std::fs::read(&target).unwrap();
        assert!(bytes.len() > 64, "log file too small to corrupt meaningfully");
        let pos = bytes.len() / 2;
        bytes[pos] ^= 0xFF;
        std::fs::write(&target, &bytes).unwrap();
    }

    // A proactive log-file scrubber would detect this. Recovery alone cannot,
    // because the corrupted LN is obsolete-by-checkpoint (embedded into a BIN)
    // and is never re-read. This assertion FAILS until the scrubber exists.
    match recover_and_scan(dir.path()) {
        Err(_e) => { /* detected via error — the desired end state */ }
        Ok(recovered) => {
            assert!(
                recovered.len() < full_expected.len(),
                "NEW-REC-3: byte flip in an obsolete (checkpoint-superseded, \
                 embedded-LN) region was not detected — requires a DbVerifyLog \
                 log-file scrubber (recovered {} of {} keys)",
                recovered.len(),
                full_expected.len(),
            );
        }
    }
}
