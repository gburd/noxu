//! BUG-CLEANER-LIVE-FILE-DELETE — the cleaner must never delete the current,
//! still-being-written log file.  On a single-file environment (every small or
//! freshly created database) the file that holds the live write position also
//! holds the NameDB database-mapping record, so deleting it does not merely
//! drop a few keys — it destroys the database: on reopen `open_database` fails
//! with "Database '…' does not exist".
//!
//! Root cause: `FileSelector` computed `lastFileToClean = firstActiveFile -
//! minAge` with an unsigned `saturating_sub`, so a single file 0 with the
//! default `minAge = 2` yielded `0` and `file_num(0) <= 0` made the live file
//! age-eligible.  JE computes this signed (`UtilizationCalculator.java:224`),
//! where `0 - 2 = -2` and the gate `fileNum > lastFileToClean`
//! (`UtilizationCalculator.java:339`) excludes it; JE also enforces
//! `CLEANER_MIN_AGE >= 1` (`EnvironmentParams.java:1366`).  Fixed by 9a090720
//! (`checked_sub(min_age)?` + `cleaner_min_age.max(1)`).
//!
//! This witness is deterministic: `clean_log()` alone (no checkpoint, no
//! daemon) deletes the live file on the buggy build, so the reopen below fails
//! every run on v7.11.0 / stack-v1 and passes every run with the fix.

use noxu_db::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use std::path::Path;

fn env_config(dir: &Path) -> EnvironmentConfig {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    // Isolate the defect: no background daemons, only the explicit clean_log()
    // below.  minAge stays at its default (2).
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg
}

fn db_config() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Overwrite the same 20 keys many times so the single log file is almost all
/// obsolete bytes (low utilization) — the one precondition the selector needs
/// to pick a file for cleaning.
fn churn(db: &noxu_db::Database) {
    let value = vec![0x5Au8; 512];
    for _gen in 0..50u32 {
        for k in 0..20u32 {
            db.put(DatabaseEntry::from_bytes(&k.to_be_bytes()), &value)
                .unwrap();
        }
    }
}

#[test]
fn single_file_db_survives_clean_log_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: create the DB, write a known value to all 20 keys, churn to
    // drive utilization down, then run an explicit forced clean.
    let final_value = vec![0xA5u8; 512];
    let deletions;
    {
        let env = Environment::open(env_config(&path)).unwrap();
        let db = env.open_database(None, "live", &db_config()).unwrap();
        churn(&db);
        // Last write wins: every key ends at `final_value`.
        for k in 0..20u32 {
            db.put(DatabaseEntry::from_bytes(&k.to_be_bytes()), &final_value)
                .unwrap();
        }
        // The reported trigger sequence (mirrors the C2 cleaner_live_file
        // witness): clean, force a checkpoint, clean again.  On the buggy
        // build the first clean selects and the barrier then deletes the live
        // file 0; with the fix every clean is a no-op (the only file is the
        // active one).
        let force = noxu_db::CheckpointConfig::new().with_force(true);
        env.clean_log().unwrap();
        env.checkpoint(Some(&force)).unwrap();
        env.clean_log().unwrap();
        deletions = env.stats().unwrap().cleaner.deletions;
        db.close().unwrap();
        env.close().unwrap();
    }

    // A single-file environment must never have deleted a log file: there is
    // only the one active file and it is never old enough to clean.
    assert_eq!(
        deletions, 0,
        "the cleaner deleted the live log file (deletions={deletions}); \
         on a single-file env this destroys the database"
    );

    // Phase 2: reopen and prove the database and all its records survived.
    // On the buggy build this is where the whole-DB loss surfaces:
    // open_database fails "Database 'live' does not exist".
    let env = Environment::open(
        EnvironmentConfig::new(path)
            .with_allow_create(false)
            .with_transactional(true),
    )
    .expect("environment must reopen after clean_log()");
    let db = env
        .open_database(None, "live", &DatabaseConfig::new().with_transactional(true))
        .expect(
            "database 'live' must still exist after clean_log()+reopen — if it \
             is gone, the cleaner deleted the live file holding the NameDB entry",
        );
    for k in 0..20u32 {
        let mut v = DatabaseEntry::new();
        let found = db
            .get_into(None, k.to_be_bytes(), &mut v)
            .expect("get must not hit a dangling slot into a deleted file");
        assert!(found, "key {k} lost after clean_log()+reopen");
        assert_eq!(v.data(), &final_value[..], "key {k} value corrupted");
    }
    db.close().unwrap();
    env.close().unwrap();
}
