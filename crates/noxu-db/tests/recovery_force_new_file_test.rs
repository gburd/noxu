//! NEW-UTIL-1: ENV_RECOVERY_FORCE_NEW_FILE must force the next log write onto
//! a NEW log file after recovery, so a restored backup's last log file is not
//! appended to (protecting it from being written past its backed-up length).
//!
//! JE reference: `RecoveryManager.recover` (~line 329) calls
//! `fileManager.forceNewLogFile()` under `ENV_RECOVERY_FORCE_NEW_FILE`
//! [#22834]; `FileManager.shouldFlipFile` then flips on the first write.
//!
//! Fail-before / pass-after: before wiring, the param was a silent no-op —
//! the first post-recovery write appended to the last recovered file (highest
//! file number unchanged; last file grew). After wiring, the first write lands
//! in a fresh file (highest file number bumped; the previously-last file is
//! byte-for-byte unchanged).

use noxu_db::{DatabaseConfig, EnvironmentConfig, VerifyConfig};
use std::path::Path;
use tempfile::TempDir;

/// Highest `.ndb` log file number in `dir` (files are named `{:08x}.ndb`).
fn max_log_file_num(dir: &Path) -> u32 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            let stem = name.strip_suffix(".ndb")?;
            u32::from_str_radix(stem, 16).ok()
        })
        .max()
        .expect("at least one .ndb file")
}

/// Byte length of log file `num` in `dir`.
fn log_file_len(dir: &Path, num: u32) -> u64 {
    let path = dir.join(format!("{num:08x}.ndb"));
    std::fs::metadata(&path)
        .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
        .len()
}

/// Populate an env with committed data, then clean-close (drop).  Returns the
/// highest log file number and that file's byte length at close.
fn populate_and_close(dir: &Path) -> (u32, u64) {
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "nu1_db",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    for i in 0u32..64 {
        db.put(
            format!("nu1_{i:04}").into_bytes(),
            format!("val_{i:04}").into_bytes(),
        )
        .unwrap();
    }
    drop(db);
    drop(env);

    let last = max_log_file_num(dir);
    let len = log_file_len(dir, last);
    (last, len)
}

/// Reopen with the given force-new-file setting, write one record, verify the
/// prior data is intact, then clean-close.  Returns the highest log file
/// number after the write.
fn reopen_write_one(dir: &Path, force_new_file: bool) -> u32 {
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_env_recovery_force_new_file(force_new_file),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "nu1_db",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    // Prior committed data must have survived recovery either way.
    assert_eq!(
        db.get(b"nu1_0000").unwrap().as_deref(),
        Some(b"val_0000".as_slice()),
        "recovered key nu1_0000 must be present after recovery"
    );

    // The write that must go to a fresh file when the flag is set.
    db.put(b"post_recovery".to_vec(), b"post_value".to_vec())
        .unwrap();

    let n = max_log_file_num(dir);
    drop(db);
    drop(env);
    n
}

/// With the param SET, the first post-recovery write starts a NEW log file:
/// the highest file number is bumped and the previously-last recovered file
/// is byte-for-byte unchanged (not appended past its recovered length).
#[test]
fn force_new_file_starts_fresh_file_after_recovery() {
    let dir = TempDir::new().unwrap();
    let (last_file, last_len) = populate_and_close(dir.path());

    let after = reopen_write_one(dir.path(), /* force_new_file = */ true);

    assert!(
        after > last_file,
        "PASS-AFTER: with ENV_RECOVERY_FORCE_NEW_FILE set, the first \
         post-recovery write must start a new file: last recovered file was \
         {last_file:#010x}, but highest file after the write is still \
         {after:#010x}"
    );
    assert_eq!(
        log_file_len(dir.path(), last_file),
        last_len,
        "the previously-last recovered log file ({last_file:#010x}) must NOT \
         be appended to (its backed-up length must be preserved)"
    );

    // env.verify()==0: reopen and confirm a clean, consistent environment.
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let vresult = env.verify(&VerifyConfig::new()).unwrap();
    assert_eq!(
        vresult.error_count(),
        0,
        "env.verify() must report 0 errors, got {}: {:?}",
        vresult.error_count(),
        vresult.errors,
    );
}

/// NON-VACUOUS neuter: with the param CLEAR (default), the same first
/// post-recovery write appends to the last recovered file — the highest file
/// number is UNCHANGED.  This is exactly the pre-fix (silent no-op) behavior;
/// it proves the test above measures the wired effect and is not vacuous.
#[test]
fn without_force_new_file_appends_to_last_file() {
    let dir = TempDir::new().unwrap();
    let (last_file, _last_len) = populate_and_close(dir.path());

    let after = reopen_write_one(dir.path(), /* force_new_file = */ false);

    assert_eq!(
        after, last_file,
        "NEUTER: without the flag, the first post-recovery write appends to \
         the last recovered file {last_file:#010x} (no new file); got \
         highest file {after:#010x}"
    );
}
