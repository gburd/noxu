//! C2 — deterministic stepwise per-log-entry truncation sweep.
//!
//! Faithful port of JE `com.sleepycat.je.recovery.CheckBase.stepwiseLoop`
//! driven by `CheckSplitsTest.testBasicInsert` (and the `recovery/stepwise`
//! support classes `EntryTrackerReader` / `LogEntryInfo` / `TestData`).
//!
//! JE's stepwise loop truncates the log after EACH successive log entry and
//! asserts the recovered data set equals the EXACT surviving subset — a
//! deterministic exhaustive torn-write boundary sweep (not a random sample
//! like `power_loss_sweep.rs`).
//!
//! ## How JE does it
//!
//! 1. `setStepwiseStart()` records the LSN where the workload begins.
//! 2. `generateData()` runs a known workload (21 non-txnal autocommit puts
//!    with `NODE_MAX = 4` so the inserts force BIN splits).
//! 3. `makeLogDescription()` walks every log entry from the start point with
//!    an `EntryTrackerReader`, producing one `LogEntryInfo` per entry.
//! 4. `stepwiseLoop()` then, for `i` in `0..logDescription.size()`:
//!      - copies the saved log files back,
//!      - truncates the log at `logDescription[i].getLsn()`,
//!      - recovers + scans (`tryRecovery` → `recoverAndLoadData`),
//!      - asserts the recovered set == the per-boundary expected set,
//!      - then advances the expected set via `info.updateExpectedSet(...)`.
//!
//!    For a NON-transactional LN (`NonTxnalEntry`), `updateExpectedSet` adds
//!    the key/data immediately (durable as soon as the LN entry is written).
//!
//! ## Faithful Noxu adaptation
//!
//! Noxu reads the same `.ndb` log with the production header parser
//! (`noxu_log::LogEntryHeader`) and LN payload parser
//! (`LnLogEntry::parse_from_slice`) — exactly analogous to JE's
//! `EntryTrackerReader`. For each entry boundary in the log we:
//!   - truncate a fresh copy of the env directory at that byte offset,
//!   - recover and collect the data,
//!   - independently REPLAY the surviving log prefix to compute the exact
//!     expected set (apply non-txnal Insert/Update LNs, remove Delete LNs),
//!   - assert recovered == expected (same exact-set assertion strength as
//!     JE `CheckBase.validate`).
//!
//! `recover_and_collect` (shared with `recovery_correctness_test.rs`) already
//! runs `env.verify()` after every recovery — this is JE's
//! `recoverAndLoadData` calling `env.verify()` + `VerifyUtils.checkLsns`.
//!
//! Deviation note: JE's `CheckSplitsTest.testBasicInsert` uses
//! `IntegerBinding` 4-byte keys; Noxu uses ASCII `key_NNNN` keys. The
//! scenario (ascending unique inserts forcing splits at NODE_MAX=4) and the
//! assertion strength (exact recovered-set equality at every entry boundary)
//! are preserved.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use noxu_log::{FileHeader, LogEntryHeader, LogEntryType};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers (mirroring recovery_correctness_test.rs conventions)
// ---------------------------------------------------------------------------

fn open_env(dir: &Path) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    // JE CheckBase.turnOffEnvDaemons: cleaner/checkpointer/evictor/
    // incompressor off so the log description is deterministic.
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    // JE CheckSplitsTest: NODE_MAX = 4 so the inserts force BIN splits.
    cfg.set_node_max_entries(4);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "simpleDB",
        &DatabaseConfig::new().with_allow_create(true),
    )
    .unwrap()
}

fn collect_all(db: &noxu_db::Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut map = BTreeMap::new();
    let mut key = DatabaseEntry::new();
    let mut val = DatabaseEntry::new();
    let mut status = cursor.get(&mut key, &mut val, Get::First, None).unwrap();
    while status == OperationStatus::Success {
        map.insert(
            key.get_data().unwrap_or(&[]).to_vec(),
            val.get_data().unwrap_or(&[]).to_vec(),
        );
        status = cursor.get(&mut key, &mut val, Get::Next, None).unwrap();
    }
    cursor.close().unwrap();
    map
}

/// JE `CheckBase.recoverAndLoadData`: open (which recovers), run
/// `env.verify()`, then full-scan. Asserts zero structural errors.
fn recover_and_collect(dir: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let env = open_env(dir);
    let db = open_db(&env);
    let vresult = env
        .verify(&noxu_db::VerifyConfig::new())
        .expect("verify after recovery");
    assert_eq!(
        vresult.error_count(),
        0,
        "post-recovery structural verification found {} error(s): {:?}",
        vresult.error_count(),
        vresult.errors,
    );
    let result = collect_all(&db);
    drop(db);
    drop(env);
    result
}

// ---------------------------------------------------------------------------
// Log-entry walking (mirrors JE EntryTrackerReader)
// ---------------------------------------------------------------------------

/// One entry boundary discovered while walking a single log file.
struct EntryBoundary {
    /// Byte offset just past the end of this entry (the truncation point that
    /// would keep this entry and drop everything after it).
    end: u64,
    entry_type: LogEntryType,
    /// Entry payload bytes (excluding the header). `None` for entries we did
    /// not need to materialise.
    payload: Vec<u8>,
}

/// List all `*.ndb` files under `dir`, sorted ascending by file number.
fn list_log_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "ndb").unwrap_or(false))
        .collect();
    files.sort();
    files
}

/// Walk every log entry in `file_bytes`, using the production header parser.
///
/// This is the Noxu analogue of JE's `EntryTrackerReader.readNextEntry()`
/// loop. Stops at the first malformed/short entry (i.e. it reads the valid
/// prefix), which matches recovery's own "stop at torn entry" behaviour.
fn walk_entries(file_bytes: &[u8], file_num: u32) -> Vec<EntryBoundary> {
    let mut out = Vec::new();

    // First-entry offset depends on the file header version (v2=32, v3=36).
    // Parse the header to learn the version, then resolve via on_disk_size.
    let mut hdr_reader = file_bytes;
    let version = match FileHeader::read_from(&mut hdr_reader) {
        Ok(h) => h.log_version,
        Err(_) => return out, // unreadable header → no entries
    };
    let mut offset = FileHeader::on_disk_size(version) as u64;

    loop {
        let off = offset as usize;
        if off + noxu_log::entry_header::MIN_HEADER_SIZE > file_bytes.len() {
            break;
        }
        let lsn = noxu_util::Lsn::new(file_num, offset as u32);
        let header =
            match LogEntryHeader::read_from_log(&file_bytes[off..], lsn) {
                Ok(h) => h,
                Err(_) => break, // torn / malformed header → stop
            };
        let header_size = header.size();
        let item_size = header.item_size() as usize;
        let total = header_size + item_size;
        if off + total > file_bytes.len() {
            break; // entry body is truncated on disk → stop
        }
        let payload = file_bytes[off + header_size..off + total].to_vec();
        out.push(EntryBoundary {
            end: offset + total as u64,
            entry_type: header.entry_type(),
            payload,
        });
        offset += total as u64;
    }
    out
}

/// Replay the surviving log entries to compute the EXACT expected data set,
/// the way JE's `updateExpectedSet` builds it incrementally.
///
/// We only track keys in our known namespace (`key_*`) so internal MapLN /
/// NameLN / FileSummaryLN entries are ignored. Non-transactional LNs become
/// visible the moment their entry survives (JE `NonTxnalEntry`).
fn replay_expected(
    all_files: &[(u32, Vec<EntryBoundary>)],
    truncate_file: u32,
    truncate_offset: u64,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (fnum, entries) in all_files {
        for e in entries {
            // Stop once we pass the truncation boundary in the last file.
            if *fnum == truncate_file && e.end > truncate_offset {
                break;
            }
            apply_entry(&mut expected, e);
        }
        if *fnum == truncate_file {
            break;
        }
    }
    expected
}

fn apply_entry(expected: &mut BTreeMap<Vec<u8>, Vec<u8>>, e: &EntryBoundary) {
    use noxu_log::entry::LnLogEntry;
    let is_txn = e.entry_type.is_transactional();
    let (insert, delete) = match e.entry_type {
        LogEntryType::InsertLN | LogEntryType::UpdateLN => (true, false),
        LogEntryType::DeleteLN => (false, true),
        // Transactional variants: our C2 workload is non-transactional, so
        // we don't expect these. (A txnal workload would require buffering
        // until the matching TxnCommit — JE TxnalEntry/CommitEntry.)
        _ => return,
    };
    let parsed = match LnLogEntry::parse_from_slice(&e.payload, is_txn) {
        Ok(p) => p,
        Err(_) => return,
    };
    // Only track our known key namespace.
    if !parsed.key.starts_with(b"key_") {
        return;
    }
    if insert {
        expected.insert(
            parsed.key.to_vec(),
            parsed.data.map(|d| d.to_vec()).unwrap_or_default(),
        );
    } else if delete {
        expected.remove(parsed.key);
    }
}

/// Copy the whole env directory `src` into a fresh temp dir and return it.
fn copy_env_dir(src: &Path) -> TempDir {
    let dst = TempDir::new().unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        if from.is_file() {
            let to = dst.path().join(entry.file_name());
            std::fs::copy(&from, &to).unwrap();
        }
    }
    dst
}

// ---------------------------------------------------------------------------
// C2 — the stepwise sweep
// ---------------------------------------------------------------------------

/// JE `CheckSplitsTest.testBasicInsert` + `CheckBase.stepwiseLoop`.
///
/// Write 21 ascending non-transactional autocommit inserts with NODE_MAX=4
/// (forcing BIN splits), then for EVERY log-entry boundary in EVERY log file,
/// truncate there, recover, and assert the recovered set equals the exact
/// surviving subset computed by an independent replay of the log prefix.
#[test]
fn stepwise_truncation_basic_insert() {
    let src = TempDir::new().unwrap();

    // --- generateData (JE setupBasicInsertData): 21 ascending autocommit
    //     puts, NODE_MAX=4 forces splits. db.put(null, ...) == autocommit. ---
    {
        let env = open_env(src.path());
        let db = open_db(&env);
        for i in 0u32..21 {
            let k = format!("key_{i:04}");
            let v = format!("val_{i:04}");
            db.put(
                None,
                &DatabaseEntry::from_bytes(k.as_bytes()),
                &DatabaseEntry::from_bytes(v.as_bytes()),
            )
            .unwrap();
        }
        // Clean close → final flush. (No checkpoint: checkpointer is off and
        // we rely on autocommit fsync per put.)
        db.close().unwrap();
        env.close().unwrap();
    }

    // --- makeLogDescription: walk every entry in every log file. ---
    let files = list_log_files(src.path());
    assert!(!files.is_empty(), "expected at least one .ndb log file");

    let mut all_files: Vec<(u32, Vec<EntryBoundary>)> = Vec::new();
    for f in &files {
        let fname = f.file_name().unwrap().to_str().unwrap();
        let file_num = u32::from_str_radix(fname.trim_end_matches(".ndb"), 16)
            .expect("parse .ndb file number");
        let bytes = std::fs::read(f).unwrap();
        let entries = walk_entries(&bytes, file_num);
        all_files.push((file_num, entries));
    }

    // Total number of LN inserts we should see across the whole log == 21.
    let total_inserts: usize = all_files
        .iter()
        .flat_map(|(_, es)| es.iter())
        .filter(|e| {
            matches!(
                e.entry_type,
                LogEntryType::InsertLN | LogEntryType::UpdateLN
            ) && {
                use noxu_log::entry::LnLogEntry;
                LnLogEntry::parse_from_slice(&e.payload, false)
                    .map(|p| p.key.starts_with(b"key_"))
                    .unwrap_or(false)
            }
        })
        .count();
    assert_eq!(
        total_inserts, 21,
        "expected 21 user-key LN inserts in the log, found {total_inserts}"
    );

    // --- stepwiseLoop: truncate after each entry boundary, recover,
    //     assert exact recovered-set equality. ---
    let mut boundaries_checked = 0usize;
    for (file_num, entries) in &all_files {
        for e in entries {
            // Truncation point: keep this entry, drop everything after it.
            let truncate_offset = e.end;

            // Copy the env, truncate the chosen file at `truncate_offset`.
            let work = copy_env_dir(src.path());
            let target = work.path().join(format!("{file_num:08x}.ndb"));
            let f =
                std::fs::OpenOptions::new().write(true).open(&target).unwrap();
            f.set_len(truncate_offset).unwrap();
            drop(f);

            // Recover + collect (runs env.verify()).
            let recovered = recover_and_collect(work.path());

            // Independent oracle: replay the surviving prefix.
            let expected =
                replay_expected(&all_files, *file_num, truncate_offset);

            assert_eq!(
                recovered, expected,
                "stepwise boundary (file {file_num:08x} truncate@{truncate_offset}, \
                 entry_type {:?}): recovered set != exact surviving subset",
                e.entry_type,
            );
            boundaries_checked += 1;
        }
    }

    // Sanity: we actually swept a non-trivial number of boundaries, and the
    // final (no-truncation) state has all 21 keys.
    assert!(
        boundaries_checked >= 21,
        "expected to sweep >= 21 boundaries, swept {boundaries_checked}"
    );
    let full = recover_and_collect(src.path());
    assert_eq!(full.len(), 21, "full recovery must see all 21 keys");
}
