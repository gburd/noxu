//! The cleaner must never clean (and so never delete) the newest log file,
//! which holds the current write position.  JE
//! `UtilizationCalculator.getBestFile` (UtilizationCalculator.java:207-224,
//! :339): `lastFileToClean = firstActiveFile - minAge` is signed, so with a
//! single file 0 and the default minAge 2 it is -2 and no file qualifies.
//! A saturating (unsigned) subtraction made it 0, so the live file 0 was
//! cleaned, passed the checkpoint barrier and was deleted: data lost.
use noxu_db::{
    CheckpointConfig, DatabaseConfig, Environment, EnvironmentConfig,
};
use std::path::Path;

fn config(dir: &Path, min_age: u32) -> EnvironmentConfig {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_cleaner_min_age(min_age);
    // Default minUtilization (50).
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg
}

fn db_config() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Overwrites leave the only file (0) mostly obsolete: low utilization.
fn churn(db: &noxu_db::Database) {
    for g in 0..50u8 {
        for k in 0..20u32 {
            db.put(k.to_be_bytes(), vec![g; 200]).unwrap();
        }
    }
}

fn assert_present(dir: &Path, min_age: u32) {
    let env =
        Environment::open(config(dir, min_age)).expect("reopen after cleaning");
    let db = env.open_database(None, "live", &db_config()).unwrap();
    for k in 0..20u32 {
        let v = db.get(k.to_be_bytes()).unwrap();
        assert_eq!(v.as_deref(), Some(&[49u8; 200][..]), "key {k} lost");
    }
    db.close().unwrap();
    env.close().unwrap();
    drop(db);
    drop(env);
}

// Handles are dropped before reopen: on this lineage env close keeps the
// env lock while a Database value is alive.
fn survives_clean_log_and_checkpoint(min_age: u32) {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(config(dir.path(), min_age)).unwrap();
    let db = env.open_database(None, "live", &db_config()).unwrap();
    churn(&db);
    let force = CheckpointConfig::new().with_force(true);
    env.clean_log().unwrap();
    env.checkpoint(Some(&force)).unwrap();
    env.clean_log().unwrap();
    let deletions = env.stats().unwrap().cleaner.deletions;
    db.close().unwrap();
    env.close().unwrap();
    drop(db);
    drop(env);
    assert_present(dir.path(), min_age);
    assert_eq!(deletions, 0, "the live file was deleted");
}

#[test]
fn single_file_env_survives_clean_log_and_checkpoint() {
    // Default minAge 2: JE lastFileToClean = 0 - 2 < 0.
    survives_clean_log_and_checkpoint(2);
}

#[test]
fn min_age_zero_does_not_clean_the_live_file() {
    // JE rejects minAge 0 (CLEANER_MIN_AGE min 1, EnvironmentParams.java:1366).
    survives_clean_log_and_checkpoint(0);
}
