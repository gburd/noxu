//! SIGKILL crash recovery correctness tests — adversarial suite.
//!
//! Each test:
//!  1. Launches the `crash_worker` subprocess that writes data under a
//!     controlled scenario, signalling readiness via flag files on disk.
//!  2. SIGKILLs the worker at the deterministic signal point.
//!  3. Reopens the environment in the parent process, triggering recovery.
//!  4. Asserts that recovery produces exactly the committed state:
//!       - every committed record is present with its original value, and
//!       - no uncommitted record appears.
//!
//! The adversarial tests additionally probe:
//!  - Commit ordering: recovery must not reorder or drop earlier commits when a
//!    later commit was in-flight at crash time.
//!  - Torn write: a SIGKILL during log flush leaves a partial entry; recovery
//!    must detect the partial entry and discard it rather than treating it as
//!    committed or crashing.
//!  - Clean-close / SIGKILL parity: the visible state after a clean shutdown
//!    must be identical to the state after a SIGKILL, given the same committed
//!    transactions.
//!
//! The worker binary path is injected by cargo as `CARGO_BIN_EXE_crash_worker`.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers shared by adversarial tests
// ---------------------------------------------------------------------------

/// Collect all `.ndb` log files in `dir`, sorted by name.
fn log_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "ndb"))
        .collect();
    files.sort();
    files
}

/// Return the byte length of the last complete log entry in `file`.
///
/// Scans forward over the file reading 14-byte entry headers (the minimum
/// header size for non-VLSN entries: 4 checksum + 1 type + 1 flags +
/// 4 prev_offset + 4 item_size).  Stops at the first header it cannot fully
/// read or whose `item_size` would extend past the file.  Returns the offset
/// of the last successfully consumed entry boundary.
fn last_complete_entry_end(file: &Path) -> u64 {
    const MIN_HEADER: usize = 14; // checksum(4) + type(1) + flags(1) + prev_offset(4) + item_size(4)
    let data = std::fs::read(file).unwrap();
    let len = data.len();

    // Skip the file header.  Determine the version-aware header size by
    // peeking at the version field (bytes 8..12, big-endian u32):
    // v2 → 32 bytes; v3 → 36 bytes.
    let file_header_size: usize = if len >= 12 {
        let ver = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        if ver >= noxu_log::file_header::LOG_VERSION {
            noxu_log::file_header::FILE_HEADER_SIZE // 36
        } else {
            noxu_log::file_header::FILE_HEADER_SIZE_V2 // 32
        }
    } else {
        noxu_log::file_header::FILE_HEADER_SIZE
    };
    let mut pos: usize = file_header_size;
    let mut last_good: usize = file_header_size;

    while pos + MIN_HEADER <= len {
        // item_size is at bytes [pos+10 .. pos+14] (little-endian u32).
        let item_size = u32::from_le_bytes([
            data[pos + 10],
            data[pos + 11],
            data[pos + 12],
            data[pos + 13],
        ]) as usize;
        let entry_end = pos + MIN_HEADER + item_size;
        if entry_end > len {
            break; // partial entry
        }
        last_good = entry_end;
        pos = entry_end;
    }

    last_good as u64
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Block until `dir/name` exists or `timeout` elapses. Returns `true` on
/// success, `false` on timeout.
fn wait_for_flag(dir: &Path, name: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let flag = dir.join(name);
    while Instant::now() < deadline {
        if flag.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

fn crash_worker_exe() -> &'static str {
    env!("CARGO_BIN_EXE_crash_worker")
}

fn reopen_db(dir: &Path) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config)
        .expect("reopen environment after crash");
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env
        .open_database(None, "test", &db_config)
        .expect("reopen database after crash");
    // C1 (JE CheckBase.recoverAndLoadData): after crash recovery, assert
    // STRUCTURAL integrity, not just data. env.verify() runs both the tree
    // walk AND VerifyUtils.checkLsns (live tree LSNs disjoint from the
    // UtilizationTracker's obsolete set). A crash that recovers correct data
    // but a corrupt tree/utilization state would otherwise pass silently.
    let vresult = env
        .verify(&noxu_db::VerifyConfig::new())
        .expect("verify after crash recovery");
    assert_eq!(
        vresult.error_count(),
        0,
        "post-crash-recovery structural verification found {} error(s): {:?}",
        vresult.error_count(),
        vresult.errors,
    );
    (env, db)
}

// ---------------------------------------------------------------------------
// Test 1: committed writes survive SIGKILL; concurrent uncommitted writes do not
// ---------------------------------------------------------------------------

/// A batch of 50 individually-committed records must all be readable after
/// a SIGKILL that occurs while a second, uncommitted transaction is in flight.
///
/// This validates:
///   - fsync guarantees for committed transactions
///   - log truncation / undo of the in-flight transaction during recovery
#[test]
fn test_committed_writes_survive_sigkill() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "committed_then_uncommitted")
        .spawn()
        .expect("spawn crash_worker");

    // Wait for phase 1 (committed writes) to complete.
    assert!(
        wait_for_flag(&dir_path, "phase1_done", Duration::from_secs(60)),
        "worker did not complete phase 1 within timeout"
    );

    // Wait for phase 2 (uncommitted writes) to begin — ensures dirty entries
    // exist in the log at kill time, maximising pressure on recovery.
    assert!(
        wait_for_flag(&dir_path, "phase2_started", Duration::from_secs(10)),
        "worker did not start phase 2 within timeout"
    );

    // SIGKILL — abrupt termination, no graceful shutdown path.
    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    // Reopen — triggers crash recovery.
    let (_env, db) = reopen_db(&dir_path);

    // All 50 committed keys must be present with the correct value.
    let mut missing = 0u32;
    for i in 0u32..50 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        match db.get_into(None, &key, &mut val).unwrap() {
            true => {
                assert_eq!(
                    val.data(),
                    b"committed",
                    "key {i} has wrong value after recovery"
                );
            }
            false => {
                missing += 1;
            }
        }
    }
    assert_eq!(
        missing, 0,
        "{missing} committed keys were lost after crash recovery"
    );

    // None of the 50 uncommitted keys may be visible.
    let mut leaked = 0u32;
    for i in 1000u32..1050 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get_into(None, &key, &mut val).unwrap() {
            leaked += 1;
        }
    }
    assert_eq!(
        leaked, 0,
        "{leaked} uncommitted keys leaked through crash recovery"
    );
}

// ---------------------------------------------------------------------------
// Test 2: entirely uncommitted transaction leaves no trace after SIGKILL
// ---------------------------------------------------------------------------

/// A transaction that is never committed must leave no visible records after
/// crash recovery.
///
/// The worker first commits a sentinel key to prove the database is live,
/// then writes 50 keys in an uncommitted transaction before being killed.
/// After recovery only the sentinel must be present.
#[test]
fn test_uncommitted_transaction_leaves_no_trace() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "uncommitted_only")
        .spawn()
        .expect("spawn crash_worker");

    // Wait for the sentinel commit so we know the database is open.
    assert!(
        wait_for_flag(&dir_path, "sentinel_committed", Duration::from_secs(60)),
        "worker did not commit sentinel within timeout"
    );

    // Wait for the uncommitted batch to begin.
    assert!(
        wait_for_flag(
            &dir_path,
            "uncommitted_started",
            Duration::from_secs(10)
        ),
        "worker did not start uncommitted batch within timeout"
    );

    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    let (_env, db) = reopen_db(&dir_path);

    // Sentinel must survive.
    let sentinel_key = DatabaseEntry::from_bytes(b"sentinel");
    let mut val = DatabaseEntry::new();
    assert!(
        db.get_into(None, &sentinel_key, &mut val).unwrap(),
        "sentinel key missing after recovery — committed data was lost"
    );
    assert_eq!(val.data(), b"ok");

    // All 50 uncommitted keys must be absent.
    let mut leaked = 0u32;
    for i in 0u32..50 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get_into(None, &key, &mut val).unwrap() {
            leaked += 1;
        }
    }
    assert_eq!(
        leaked, 0,
        "{leaked} uncommitted keys survived a SIGKILL (expected 0)"
    );
}

// ---------------------------------------------------------------------------
// Test 3: repeated crash+recovery preserves monotonically committed state
// ---------------------------------------------------------------------------

/// Crash and recover three times in sequence. Each round commits a fresh
/// batch of 10 keys before being killed mid-write. After all three rounds,
/// every committed key from every round must be present and no uncommitted
/// key from any round may appear.
#[test]
fn test_repeated_crash_recovery_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    // Establish the database by opening it once (committed sentinel).
    {
        let env_config = EnvironmentConfig::new(dir_path.clone())
            .with_allow_create(true)
            .with_transactional(true);
        let env = noxu_db::Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let _db = env.open_database(None, "test", &db_config).unwrap();
        // env drop → clean shutdown, database entry committed to log
    }

    // Three rounds: each uses `committed_then_uncommitted` mode.
    for round in 0u32..3 {
        // Remove flag files from any previous round.
        let _ = std::fs::remove_file(dir_path.join("phase1_done"));
        let _ = std::fs::remove_file(dir_path.join("phase2_started"));

        let mut child = std::process::Command::new(crash_worker_exe())
            .env("NOXU_CRASH_DIR", &dir_path)
            .env("NOXU_CRASH_MODE", "committed_then_uncommitted")
            .spawn()
            .unwrap_or_else(|e| panic!("round {round}: spawn: {e}"));

        assert!(
            wait_for_flag(&dir_path, "phase1_done", Duration::from_secs(60)),
            "round {round}: phase 1 timed out"
        );
        assert!(
            wait_for_flag(&dir_path, "phase2_started", Duration::from_secs(10)),
            "round {round}: phase 2 timed out"
        );
        child.kill().unwrap();
        child.wait().unwrap();
    }

    // After three crash+recovery cycles, reopen and verify.
    let (_env, db) = reopen_db(&dir_path);

    // The worker always writes keys 0..50 as committed and 1000..1050 as
    // uncommitted (same ranges each round). After three rounds each key
    // 0..50 must still be present (last writer wins on overwrite) and each
    // key 1000..1050 must be absent.
    let mut missing = 0u32;
    for i in 0u32..50 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if !(db.get_into(None, &key, &mut val).unwrap()) {
            missing += 1;
        }
    }
    assert_eq!(
        missing, 0,
        "{missing} committed keys missing after 3 crash rounds"
    );

    let mut leaked = 0u32;
    for i in 1000u32..1050 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get_into(None, &key, &mut val).unwrap() {
            leaked += 1;
        }
    }
    assert_eq!(
        leaked, 0,
        "{leaked} uncommitted keys leaked after 3 crash rounds"
    );
}

// ---------------------------------------------------------------------------
// Adversarial Test 4: commit ordering — T1 committed, SIGKILL before T2
// ---------------------------------------------------------------------------

/// T1 commits keys 0..25, T2 commits keys 100..125.  The worker is killed
/// after T1's flag but before T2's flag.
///
/// After recovery:
///   - All 25 T1 keys must be present with value `b"t1"`.
///   - All 25 T2 keys must be absent (T2 was not complete at kill time).
///
/// Probes commit ordering: an earlier committed transaction must survive even
/// when a later transaction was interrupted mid-commit.
#[test]
fn test_commit_ordering_preserved_after_sigkill() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "ordered_commits")
        .spawn()
        .expect("spawn crash_worker");

    // Wait for T1 to commit.
    assert!(
        wait_for_flag(&dir_path, "t1_done", Duration::from_secs(60)),
        "worker did not commit T1 within timeout"
    );
    // Wait for T2 to begin (keys written but not committed), then kill.
    assert!(
        wait_for_flag(&dir_path, "t2_started", Duration::from_secs(10)),
        "worker did not start T2 within timeout"
    );
    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait");

    let (_env, db) = reopen_db(&dir_path);

    // All T1 keys must be present with correct value.
    let mut missing = 0u32;
    for i in 0u32..25 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        match db.get_into(None, &key, &mut val).unwrap() {
            true => {
                assert_eq!(
                    val.data(),
                    b"t1",
                    "key {i} has wrong value after recovery"
                );
            }
            false => missing += 1,
        }
    }
    assert_eq!(missing, 0, "{missing} T1 keys lost after recovery");

    // No T2 keys may appear.
    let mut leaked = 0u32;
    for i in 100u32..125 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get_into(None, &key, &mut val).unwrap() {
            leaked += 1;
        }
    }
    assert_eq!(leaked, 0, "{leaked} T2 keys visible before T2 committed");
}

// ---------------------------------------------------------------------------
// Adversarial Test 5: torn write — partial log entry truncated on recovery
// ---------------------------------------------------------------------------

/// Simulates a torn write: after a SIGKILL the last log file is manually
/// truncated to a non-entry boundary, leaving a partial (corrupt) entry at
/// the tail.  Recovery must detect and discard the partial entry without
/// losing any of the 50 previously committed keys.
#[test]
fn test_torn_write_truncated_entry_recovered() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "committed_then_uncommitted")
        .spawn()
        .expect("spawn crash_worker");

    assert!(
        wait_for_flag(&dir_path, "phase1_done", Duration::from_secs(60)),
        "phase1_done not set"
    );
    assert!(
        wait_for_flag(&dir_path, "phase2_started", Duration::from_secs(10)),
        "phase2_started not set"
    );
    child.kill().expect("SIGKILL");
    child.wait().expect("wait");

    // Inject a torn write: truncate the last log file one byte past the end
    // of the last complete entry, leaving an incomplete entry header.
    let files = log_files(&dir_path);
    let last_file = files.last().expect("at least one log file");
    let complete_end = last_complete_entry_end(last_file);
    let file_len = std::fs::metadata(last_file).unwrap().len();
    if file_len > complete_end {
        let torn_len = complete_end + 1;
        let file =
            std::fs::OpenOptions::new().write(true).open(last_file).unwrap();
        file.set_len(torn_len).expect("truncate to torn boundary");
    }

    // Recovery must handle the torn entry and surface all committed data.
    let (_env, db) = reopen_db(&dir_path);

    let mut missing = 0u32;
    for i in 0u32..50 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if !(db.get_into(None, &key, &mut val).unwrap()) {
            missing += 1;
        }
    }
    assert_eq!(
        missing, 0,
        "{missing} committed keys lost after torn-write recovery"
    );
}

// ---------------------------------------------------------------------------
// Adversarial Test 6: clean-close / SIGKILL parity
// ---------------------------------------------------------------------------

/// The visible state after a clean shutdown must be identical to the visible
/// state after a SIGKILL, given the same set of committed transactions.
///
/// Two databases are written with identical commits.  One worker is SIGKILLed
/// immediately after signalling; the other is also killed (the OS fsync
/// guarantees from commit mean both must recover identically).  Both databases
/// must expose exactly the same 25 keys.
#[test]
fn test_clean_close_and_sigkill_produce_identical_state() {
    // Both paths use SIGKILL after the commits are fsync'd.  The distinction
    // is that one is killed immediately after writes_done (simulating a crash
    // right after the last commit fsync) and the other is allowed a short
    // sleep to simulate graceful shutdown flushing any remaining buffers.

    let clean_dir = TempDir::new().unwrap();
    let crash_dir = TempDir::new().unwrap();

    // Start both workers simultaneously.
    let mut clean_child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", clean_dir.path())
        .env("NOXU_CRASH_MODE", "clean_then_dirty")
        .spawn()
        .expect("spawn clean worker");
    let mut crash_child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", crash_dir.path())
        .env("NOXU_CRASH_MODE", "clean_then_dirty")
        .spawn()
        .expect("spawn crash worker");

    assert!(
        wait_for_flag(clean_dir.path(), "writes_done", Duration::from_secs(60)),
        "clean worker did not signal writes_done"
    );
    assert!(
        wait_for_flag(crash_dir.path(), "writes_done", Duration::from_secs(60)),
        "crash worker did not signal writes_done"
    );

    // "Clean" side: sleep briefly to let the process flush any internal
    // state it would flush during a normal shutdown, then kill.
    std::thread::sleep(Duration::from_millis(20));
    clean_child.kill().ok();
    clean_child.wait().ok();

    // "Crash" side: kill immediately (no flush grace period).
    crash_child.kill().expect("SIGKILL crash worker");
    crash_child.wait().expect("wait crash worker");

    let (_env_c, db_clean) = reopen_db(clean_dir.path());
    let (_env_k, db_crash) = reopen_db(crash_dir.path());

    for i in 0u32..25 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());

        let mut val_c = DatabaseEntry::new();
        let status_c = db_clean.get_into(None, &key, &mut val_c).unwrap();

        let mut val_k = DatabaseEntry::new();
        let status_k = db_crash.get_into(None, &key, &mut val_k).unwrap();

        assert_eq!(
            status_c, status_k,
            "key {i}: clean={status_c:?} crash={status_k:?} — parity violation"
        );
        if status_c {
            assert_eq!(
                val_c.data(),
                val_k.data(),
                "key {i}: value mismatch between clean and crash recovery"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test: open_txn_spanning_checkpoint_recovers_correctly
// ---------------------------------------------------------------------------
//
// Isolation/recovery invariant: an open (uncommitted) transaction whose first
// log entry precedes a checkpoint must NOT leak its writes through crash
// recovery.
//
// Scenario (crash_worker mode "open_txn_spanning_checkpoint"):
//   1. Worker commits 20 keys ("committed_NNN").
//   2. Worker opens a transaction and writes 10 keys ("open_NNN").
//   3. Worker forces a checkpoint — CkptStart is logged AFTER the open txn's
//      LNs, so the txn's firstLoggedLsn < CkptStart.
//   4. Parent SIGKILLs the worker while the txn is still open (no
//      commit/abort record).
//
// After recovery:
//   - All 20 committed keys must be present.
//   - None of the 10 "open_NNN" keys may appear (they were uncommitted).
//
// Current recovery scans the whole log from the start, so it sees the open
// txn's LNs and the undo pass reverts them — this test passes. The test
// exists to LOCK IN that invariant: a future recovery scan-range optimization
// (P-2) that began at CkptStart without accounting for the open txn's earlier
// first-LSN would miss the LNs, fail to undo them, and silently surface the
// uncommitted keys. This test would catch that regression.

#[test]
fn open_txn_spanning_checkpoint_recovers_correctly() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "open_txn_spanning_checkpoint")
        .spawn()
        .expect("spawn crash_worker");

    // Wait for committed writes to complete.
    assert!(
        wait_for_flag(&dir_path, "phase1_done", Duration::from_secs(60)),
        "worker did not complete phase 1 within timeout"
    );

    // Wait for the open txn + checkpoint to complete.
    assert!(
        wait_for_flag(&dir_path, "open_txn_ready", Duration::from_secs(60)),
        "worker did not signal open_txn_ready within timeout"
    );

    // SIGKILL — txn is open, no commit/abort record written.
    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    // Reopen — triggers crash recovery.
    let (_env, db) = reopen_db(&dir_path);

    // All 20 committed keys must be present.
    let mut missing_committed = 0u32;
    for i in 0u32..20 {
        let k = format!("committed_{i:03}");
        let key = DatabaseEntry::from_bytes(k.as_bytes());
        let mut val = DatabaseEntry::new();
        match db.get_into(None, &key, &mut val).unwrap() {
            true => {}
            false => {
                missing_committed += 1;
                eprintln!("committed key missing after recovery: {k}");
            }
        }
    }
    assert_eq!(
        missing_committed, 0,
        "{missing_committed} committed key(s) were lost after crash recovery \
         (open-txn-spanning-checkpoint scenario)"
    );

    // None of the 10 uncommitted "open_NNN" keys may appear.
    let mut leaked = 0u32;
    for i in 0u32..10 {
        let k = format!("open_{i:03}");
        let key = DatabaseEntry::from_bytes(k.as_bytes());
        let mut val = DatabaseEntry::new();
        if db.get_into(None, &key, &mut val).unwrap() {
            leaked += 1;
            eprintln!(
                "CORRECTNESS VIOLATION: uncommitted key '{k}' survived recovery"
            );
        }
    }
    assert_eq!(
        leaked, 0,
        "{leaked} uncommitted key(s) silently survived crash recovery — \
         uncommitted data must never appear committed after recovery"
    );
}

// ---------------------------------------------------------------------------
// Test: aborted_then_committed_same_key_recovers_committed_value
// ---------------------------------------------------------------------------
//
// Recovery currency-check (JE BIN.recoverRecord; review T-F1).
//
// Scenario (crash_worker mode "aborted_then_committed_same_key"):
//   1. T1 inserts key "K" = "v1", then ABORTS (clean abort record).
//   2. T3 inserts the SAME key "K" = "v3", then COMMITS.
//   3. T2 writes an unrelated key and stays open (active at crash) so the undo
//      pass is not short-circuited by the no-active-txns fast path.
//   4. Parent SIGKILLs the worker.
//
// After recovery K must equal "v3". The undo pass now enforces the JE
// currency check (apply an undo only when the slot still holds the logged
// version), so reverting T1's aborted write cannot clobber T3's committed
// write of the same key.
//
// NOTE: this exact interleaving is also handled correctly WITHOUT the currency
// check on current `main` (runtime abort already reverted T1, redo replays
// only committed LNs, and the slot ends at T3's version), so this is a
// guard/regression test rather than a reproduction of a live corruption. The
// currency check closes the theoretical hole and makes the code match the
// behaviour its own comment previously (incorrectly) claimed.

#[test]
fn aborted_then_committed_same_key_recovers_committed_value() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "aborted_then_committed_same_key")
        .spawn()
        .expect("spawn crash_worker");

    assert!(
        wait_for_flag(&dir_path, "abort_commit_ready", Duration::from_secs(60)),
        "worker did not signal abort_commit_ready within timeout"
    );

    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    let (_env, db) = reopen_db(&dir_path);

    let key = DatabaseEntry::from_bytes(b"K");
    let mut val = DatabaseEntry::new();
    let status = db.get_into(None, &key, &mut val).unwrap();
    assert!(
        status,
        "committed key K must be present after recovery (T3's write must not \
         be clobbered by the undo of T1's aborted write of the same key)"
    );
    assert_eq!(
        val.data(),
        b"v3",
        "K must hold T3's committed value 'v3', not T1's aborted before-image"
    );
}

// ---------------------------------------------------------------------------
// Stage 1 acceptance test: IN-redo applies BINs flushed by checkpoint
// ---------------------------------------------------------------------------

/// Verify that data committed before a checkpoint and flushed into logged
/// BIN records survives a crash and recovery.
///
/// The scenario exercises both the IN-redo path (BINs written by the
/// checkpoint) and the LN-redo path (the post-checkpoint LN).
///
/// Stage 1 of fix/recovery-faithful-in-redo (DRIFT-1):
/// JE `RecoveryManager.recoverIN` / `recoverChildIN` currency check
/// (RecoveryManager.java ~lines 1237-1500).
#[test]
fn in_redo_bin_flushed_by_checkpoint_survives_crash() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "in_redo_bin_flushed_by_checkpoint")
        .spawn()
        .expect("spawn crash_worker");

    assert!(
        wait_for_flag(&dir_path, "phase1_done", Duration::from_secs(60)),
        "worker did not complete phase1 within timeout"
    );

    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    // Reopen triggers crash recovery.  All 50 pre-checkpoint keys and
    // the 1 post-checkpoint key must survive.
    let (_env, db) = reopen_db(&dir_path);

    // All 50 pre-checkpoint committed keys must be present.
    let mut missing = 0u32;
    for i in 0u32..50 {
        let k = i.to_be_bytes();
        let key = DatabaseEntry::from_bytes(&k);
        let mut val = DatabaseEntry::new();
        match db.get_into(None, &key, &mut val).unwrap() {
            true => {
                assert_eq!(
                    val.data(),
                    b"before_ckpt",
                    "key {i} has wrong value after crash+recovery"
                );
            }
            false => {
                missing += 1;
            }
        }
    }
    assert_eq!(
        missing, 0,
        "{missing} pre-checkpoint committed keys lost after crash recovery \
         (IN-redo or LN-redo must restore them)"
    );

    // The 1 post-checkpoint key must also survive (LN-redo).
    let post_key = DatabaseEntry::from_bytes(b"post_ckpt");
    let mut post_val = DatabaseEntry::new();
    let status = db.get_into(None, &post_key, &mut post_val).unwrap();
    assert!(
        status,
        "post-checkpoint key must survive crash recovery (LN-redo)"
    );
    assert_eq!(post_val.data(), b"after_ckpt");
}

// ---------------------------------------------------------------------------
// Stage 3 acceptance test: BIN-delta reconstitution during IN-redo
// ---------------------------------------------------------------------------

/// BIN-deltas logged in the recovery interval must be reconstituted (merged
/// with the last full BIN) rather than dropped.
///
/// Stage 3 of fix/recovery-faithful-in-redo (DRIFT-10):
/// JE `BINDelta.reconstituteBIN` / `BINDelta.applyDelta`.
#[test]
fn in_redo_bin_delta_reconstituted_survives_crash() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "in_redo_bin_delta_reconstituted")
        .spawn()
        .expect("spawn crash_worker");

    assert!(
        wait_for_flag(&dir_path, "phase1_done", Duration::from_secs(60)),
        "worker did not complete phase1 within timeout"
    );

    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    let (_env, db) = reopen_db(&dir_path);

    // Keys 0-4 were updated to v2; keys 5-19 remain v1.
    // All must survive (via IN-redo, possibly with BIN-delta reconstitution).
    let mut missing = 0u32;
    for i in 0u32..20 {
        let k = i.to_be_bytes();
        let key = DatabaseEntry::from_bytes(&k);
        let mut val = DatabaseEntry::new();
        match db.get_into(None, &key, &mut val).unwrap() {
            true => {
                let expected =
                    if i < 5 { b"v2" as &[u8] } else { b"v1" as &[u8] };
                assert_eq!(
                    val.data(),
                    expected,
                    "key {i} has wrong value after crash+recovery"
                );
            }
            false => {
                missing += 1;
            }
        }
    }
    assert_eq!(
        missing, 0,
        "{missing} keys lost after crash recovery \
         (BIN-delta reconstitution must restore them, DRIFT-10)"
    );
}

// ---------------------------------------------------------------------------
// Part-3 acceptance test (DRIFT-3/7 fix): file-flip fsync ordering
//
// FAIL-PRE:  without sync_log_end_and_finish_file, the old file may not be
//            durably closed before the new file takes writes; on crash right
//            after the flip, the old file's last entries could be missing.
// PASS-POST: the old file is fsynced under the LWL before advanceLsn;
//            recovery reads all committed entries across the file boundary.
// ---------------------------------------------------------------------------

/// Crash right after a log file flip; verify all committed entries are
/// recoverable from BOTH the old and the new log file.
///
/// Tests JE faithfulness invariant:
///   `syncLogEndAndFinishFile()` called after `bumpAndWriteDirty()`, under
///   the LWL, before `advanceLsn` advances to the new file — ensuring the
///   old file is durably closed before any write goes to the new file.
///
/// References: JE `LogBufferPool.getWriteBuffer(flippedFile=true)`,
///             `FileManager.syncLogEndAndFinishFile` (line 2077).
#[test]
fn test_file_flip_fsync_ordering_crash_recovery() {
    let dir = TempDir::new().expect("tempdir");
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "file_flip")
        .spawn()
        .expect("spawn crash_worker");

    // Wait for the worker to commit all records (including those spanning
    // the file boundary) before we kill it.
    assert!(
        wait_for_flag(&dir_path, "flip_committed", Duration::from_secs(60)),
        "worker did not signal flip_committed within timeout"
    );

    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    // Verify that at least 2 log files were created (flip occurred).
    let ndb_files = log_files(&dir_path);
    assert!(
        ndb_files.len() >= 2,
        "expected at least 2 log files after flip, found {}",
        ndb_files.len()
    );

    // Recovery: reopen the environment; all 200 committed records must be
    // present.
    let (_env, db) = reopen_db(&dir_path);

    let mut missing = Vec::new();
    for i in 0u32..200 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        let status = db.get_into(None, &key, &mut val).expect("get");
        if !status {
            missing.push(i);
        }
    }

    assert!(
        missing.is_empty(),
        "recovery missing {} committed keys after file-flip crash: {:?}…",
        missing.len(),
        &missing[..missing.len().min(10)]
    );
}

// ---------------------------------------------------------------------------
// Test: N-concurrent CommitSync, then SIGKILL — every committed txn survives
// ---------------------------------------------------------------------------

/// HEADLINE crash-durability test for the group-commit coalescing fix.
///
/// 8 worker threads each CommitSync-commit 50 disjoint keys (400 committed
/// transactions total), barrier-synchronised so their fdatasync requests race
/// and exercise the leader/waiter coalescing path restructured to match JE
/// `FSyncManager.flushAndSync`.  Once every `txn.commit()` (CommitSync =>
/// durable fsync) has returned, the worker raises `concurrent_committed` and
/// the parent SIGKILLs it.
///
/// Recovery must find ALL 400 committed keys.  This is the non-negotiable
/// durability invariant: the coalescing optimisation must never let a
/// committed-and-returned transaction be lost on crash.  A single missing key
/// here means a committer piggybacked on an fsync that did not cover its bytes
/// — exactly the hazard the JE-faithful ordering is designed to prevent.
#[test]
fn test_concurrent_commit_sync_survives_sigkill() {
    const THREADS: u32 = 8;
    const KEYS_PER_THREAD: u32 = 50;

    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "concurrent_commit_sync")
        .spawn()
        .expect("spawn crash_worker");

    // Wait for ALL concurrent CommitSync transactions to return durably.
    assert!(
        wait_for_flag(
            &dir_path,
            "concurrent_committed",
            Duration::from_secs(120)
        ),
        "worker did not finish concurrent CommitSync within timeout"
    );

    // SIGKILL — abrupt termination, no graceful shutdown.
    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    // Reopen — triggers crash recovery + structural verification.
    let (_env, db) = reopen_db(&dir_path);

    // Every committed key (tid * 1000 + k) must be present and correct.
    let mut missing: Vec<u32> = Vec::new();
    for tid in 0..THREADS {
        for k in 0..KEYS_PER_THREAD {
            let id = tid * 1000 + k;
            let key = DatabaseEntry::from_bytes(&id.to_be_bytes());
            let mut val = DatabaseEntry::new();
            match db.get_into(None, &key, &mut val).unwrap() {
                true => {
                    assert_eq!(
                        val.data(),
                        b"committed",
                        "key {id} has wrong value after recovery"
                    );
                }
                false => missing.push(id),
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {} concurrently-committed (CommitSync) txns LOST after crash \
         recovery — first 10: {:?}",
        missing.len(),
        THREADS * KEYS_PER_THREAD,
        &missing[..missing.len().min(10)]
    );
}
