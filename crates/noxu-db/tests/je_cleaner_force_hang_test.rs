//! BUG-CLEANER-FORCE-HANG-1 — a forced cleaning pass must TERMINATE.
//!
//! `clean_log_forced()` calls `run_cleaner(u32::MAX, force=true)`.  JE's
//! `Cleaner.doClean` bounds a pass to the file count captured at the start and
//! stops when `getBestFile` returns no file; a forced pass must likewise
//! terminate even when migration keeps producing new output files and when the
//! selector can pick no safe file.  This witness drives repeated forced passes
//! with heavy migration churn under a hard timeout — if the forced loop fails
//! to terminate, the spawned thread never signals and the test fails instead of
//! hanging CI for hours.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, EnvironmentConfig,
};
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

fn force() -> CheckpointConfig {
    CheckpointConfig::new().with_force(true)
}

/// Run `body` on a worker thread; fail if it does not finish within `secs`.
/// This converts a would-be infinite hang into a deterministic test failure.
fn with_deadline(secs: u64, body: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    let h = std::thread::spawn(move || {
        body();
        let _ = tx.send(());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(()) => {
            h.join().unwrap();
        }
        Err(_) => panic!(
            "BUG-CLEANER-FORCE-HANG-1: forced cleaning did not terminate \
             within {secs}s (the forced do_clean pass must be bounded like \
             JE Cleaner.doClean)"
        ),
    }
}

#[test]
// Un-ignored for v7.11.1 RC: BUG-CLEANER-CHURN-LOSS is fixed by C2 9a090720
// (BUG-CLEANER-LIVE-FILE-DELETE: checked_sub(min_age) + cleaner_min_age.max(1))
// plus the R-CHURN defense-in-depth guard; verified green 20x (DEBUG+RELEASE)
// on this stack.
fn forced_clean_terminates_under_migration_churn() {
    with_deadline(120, || {
        let dir = TempDir::new().unwrap();
        // Small files so even a modest working set spans many log files; the
        // behaviour under test is that a FORCED pass bounds itself even as
        // migration keeps appending fresh output files.
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "churn",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();

        let value = vec![0x5Au8; 512];
        for i in 0..200u32 {
            db.put(DatabaseEntry::from_bytes(&i.to_be_bytes()), &value)
                .unwrap();
        }

        for _ in 0..30 {
            // Rewrite the whole working set to create obsolete bytes.
            for i in 0..200u32 {
                db.put(DatabaseEntry::from_bytes(&i.to_be_bytes()), &value)
                    .unwrap();
            }
            env.checkpoint(Some(&force())).unwrap();
            // The reported trigger: forced cleaning (force=true, u32::MAX).
            let _ = env.clean_log_forced().unwrap();
            env.checkpoint(Some(&force())).unwrap();
        }

        // Data must survive the churn.
        for i in 0..200u32 {
            let mut v = DatabaseEntry::new();
            assert!(
                db.get_into(None, i.to_be_bytes(), &mut v).unwrap(),
                "key {i} lost after repeated forced cleaning"
            );
        }
        db.close().unwrap();
        env.close().unwrap();
    });
}

/// A forced pass with NO eligible file (nothing to clean) must return promptly,
/// not spin.  JE: getBestFile returns no file → doClean stops.
#[test]
fn forced_clean_with_nothing_to_clean_terminates() {
    with_deadline(60, || {
        let dir = TempDir::new().unwrap();
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "fresh",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        // A tiny, fully-live DB: no obsolete bytes, nothing worth cleaning.
        for i in 0..4u32 {
            db.put(DatabaseEntry::from_bytes(&i.to_be_bytes()), b"v").unwrap();
        }
        env.checkpoint(Some(&force())).unwrap();
        // Several forced passes in a row must each return promptly.
        for _ in 0..5 {
            let _ = env.clean_log_forced().unwrap();
        }
        db.close().unwrap();
        env.close().unwrap();
    });
}
