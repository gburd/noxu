//! SIGKILL crash recovery correctness tests.
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
//! The worker binary path is injected by cargo as `CARGO_BIN_EXE_crash_worker`.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig, OperationStatus};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

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

fn reopen_db(
    dir: &Path,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config)
        .expect("reopen environment after crash");
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env
        .open_database(None, "test", &db_config)
        .expect("reopen database after crash");
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
        match db.get(None, &key, &mut val).unwrap() {
            OperationStatus::Success => {
                assert_eq!(
                    val.data(),
                    b"committed",
                    "key {i} has wrong value after recovery"
                );
            }
            OperationStatus::NotFound => {
                missing += 1;
            }
            other => panic!("unexpected status {other:?} for committed key {i}"),
        }
    }
    assert_eq!(missing, 0, "{missing} committed keys were lost after crash recovery");

    // None of the 50 uncommitted keys may be visible.
    let mut leaked = 0u32;
    for i in 1000u32..1050 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get(None, &key, &mut val).unwrap() == OperationStatus::Success {
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
        wait_for_flag(&dir_path, "uncommitted_started", Duration::from_secs(10)),
        "worker did not start uncommitted batch within timeout"
    );

    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    let (_env, db) = reopen_db(&dir_path);

    // Sentinel must survive.
    let sentinel_key = DatabaseEntry::from_bytes(b"sentinel");
    let mut val = DatabaseEntry::new();
    assert_eq!(
        db.get(None, &sentinel_key, &mut val).unwrap(),
        OperationStatus::Success,
        "sentinel key missing after recovery — committed data was lost"
    );
    assert_eq!(val.data(), b"ok");

    // All 50 uncommitted keys must be absent.
    let mut leaked = 0u32;
    for i in 0u32..50 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get(None, &key, &mut val).unwrap() == OperationStatus::Success {
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
        if db.get(None, &key, &mut val).unwrap() == OperationStatus::NotFound {
            missing += 1;
        }
    }
    assert_eq!(missing, 0, "{missing} committed keys missing after 3 crash rounds");

    let mut leaked = 0u32;
    for i in 1000u32..1050 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get(None, &key, &mut val).unwrap() == OperationStatus::Success {
            leaked += 1;
        }
    }
    assert_eq!(
        leaked, 0,
        "{leaked} uncommitted keys leaked after 3 crash rounds"
    );
}
