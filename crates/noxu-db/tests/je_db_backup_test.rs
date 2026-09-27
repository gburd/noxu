//! Faithful ports of JE `com.sleepycat.je.util.DbBackupTest` (DbBackup.java).
//!
//! These complement `backup_test.rs`
//! (`backup_pins_files_and_restore_recovers_same_data`, which ports
//! `DbBackupTest.testBackupVsCleaning`) by covering the remaining
//! DbBackupTest cases that map onto Noxu's `Environment::start_backup` /
//! `Backup` API.
//!
//! Adaptation notes (language/API, per the test-parity contract):
//!   - JE's `DbBackup` is a mutable handle whose `startBackup` /
//!     `endBackup` / `getLastFileInBackupSet` / `getLogFilesInBackupSet`
//!     throw `IllegalStateException` when called out of order.  Noxu models
//!     the same lifecycle with an owned `Backup` value: `start_backup()`
//!     returns the handle, `end_backup(self)` consumes it, and the
//!     enumerators (`log_files_in_backup_set` / `last_file_in_backup_set`)
//!     take `&self`.  The out-of-order calls JE guards at runtime are
//!     therefore prevented at *compile time* in Noxu (you cannot call an
//!     enumerator after `end_backup` moved the handle, and you cannot start
//!     twice on one handle).  The runtime-observable guard that remains — a
//!     backup cannot be started on a read-only / cleaner-less env — is ported
//!     as `read_only_env_cannot_start_backup`.

use std::path::Path;

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig,
};

const NUM_RECS: u32 = 200;

fn open_env(dir: &Path, read_only: bool, force_new_file: bool) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(!read_only)
        .with_transactional(true)
        .with_read_only(read_only)
        // Small files (JE uses LOG_FILE_MAX=10000) so growFiles rolls files.
        .with_log_file_max_bytes(10_000);
    if force_new_file {
        cfg = cfg.with_env_recovery_force_new_file(true);
    }
    Environment::open(cfg).expect("open env")
}

/// JE `DbBackupTest.growFiles`: write NUM_RECS records twice, forcing several
/// log files to roll under the small `log_file_max`.
fn grow_files(env: &Environment, db_name: &str) {
    let db = env
        .open_database(
            None,
            db_name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");
    let value = vec![0u8; 1024];
    for pass in 0..2 {
        let txn = env.begin_transaction(None).expect("begin");
        for i in 0..NUM_RECS {
            let key = format!("k{i:06}-{pass}").into_bytes();
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key),
                DatabaseEntry::from_bytes(&value),
            )
            .expect("put");
        }
        txn.commit().expect("commit");
    }
    drop(db);
}

/// Count the `.ndb` log files on disk (JE `getAllFileNumbers().length`).
fn count_log_files(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().extension().and_then(|s| s.to_str()) == Some("ndb")
        })
        .count()
}

/// JE: `DbBackupTest.testReadOnly` (DbBackup.java) — constructing a `DbBackup`
/// on a read-only environment fails.  In JE `new DbBackup(env)` throws
/// `IllegalArgumentException` for a read-only env; Noxu returns an `Err` from
/// `start_backup()` because a hot backup needs the running cleaner's file
/// protector to pin the set (a read-only env has no cleaner).
///
/// This is the runtime-observable half of JE `testBadUsage` / `testReadOnly`:
/// the other out-of-order-call guards JE checks at runtime are compile-time
/// impossibilities in Noxu's owned-handle API (see module docs).
#[test]
fn read_only_env_cannot_start_backup() {
    let dir = tempfile::tempdir().unwrap();

    // Create a populated read-write env first, then close it.
    {
        let env = open_env(dir.path(), false, false);
        grow_files(&env, "db1");
        env.close().expect("close rw");
    }

    // Reopen read-only: start_backup must fail (no cleaner / file protector).
    let ro = open_env(dir.path(), true, false);
    let result = ro.start_backup();
    assert!(
        result.is_err(),
        "start_backup on a read-only env must fail (JE testReadOnly: \
         DbBackup rejects a read-only environment)"
    );
    ro.close().expect("close ro");
}

/// JE: `DbBackupTest.testBackupVsCleaning` (partial) — the backup set enumerated
/// by `getLogFilesInBackupSet` reports the last file number consistently with
/// the set contents (`getLastFileInBackupSet` == highest file in the set), and
/// the set is exactly the files present at `start_backup`.
///
/// This ports the `saveFiles` membership invariant of JE's test:
/// `assertEquals(lastFile + 1, backupSet.length)` — i.e. the backup set is the
/// contiguous range `0..=lastFile`.
#[test]
fn backup_set_membership_matches_last_file() {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(dir.path(), false, false);
    grow_files(&env, "db1");
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
        .expect("checkpoint");

    let backup = env.start_backup().expect("start_backup");
    let files = backup.log_files_in_backup_set();
    let last = backup.last_file_in_backup_set();

    assert!(!files.is_empty(), "backup set must be non-empty");
    assert!(files.len() >= 2, "growFiles should roll multiple files");

    // JE: getLogFilesInBackupSet()[last] == getLastFileInBackupSet(), and the
    // set is the contiguous range 0..=lastFile, so length == lastFile + 1.
    let file_nums: Vec<u32> = files
        .iter()
        .map(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| u32::from_str_radix(s, 16).ok())
                .expect("parse .ndb file number")
        })
        .collect();
    let max = *file_nums.iter().max().unwrap();
    assert_eq!(
        max, last,
        "last_file_in_backup_set must equal the highest file in the set"
    );
    assert_eq!(
        files.len() as u32,
        last + 1,
        "JE saveFiles invariant: backup set is the contiguous range 0..=lastFile"
    );

    backup.end_backup().expect("end_backup");
    env.close().expect("close");
}

/// JE: `DbBackupTest.testForceNewFile` (DbBackup.java) — with
/// `ENV_RECOVERY_FORCE_NEW_FILE` (`env_recovery_force_new_file`) set, recovery
/// flips to a new log file so the previous last file becomes immutable; with
/// the flag unset, recovery reuses the existing last file.
///
/// JE asserts `getLastFile()` transitions 0 -> 1 when the flag is enabled, and
/// that file 0's size is unchanged (the old last file is not appended to).
/// Noxu has no public last-file accessor, so we observe the flip via the
/// `.ndb` file count on disk: forcing a new file at recovery must create one
/// more log file than a plain reopen would.
///
/// ENGINE-BUG CANDIDATE (NEW-UTIL-1): this faithful port FAILS. Noxu exposes
/// `ENV_RECOVERY_FORCE_NEW_FILE` as a config parameter
/// (`noxu-config/src/params.rs`), plumbs it through
/// `EnvironmentConfig::env_recovery_force_new_file`,
/// `DbiConfig::env_recovery_force_new_file`
/// (`crates/noxu-dbi/src/dbi_config.rs:22`) and copies it in
/// `environment.rs:380` — but the recovery manager NEVER consumes it: there is
/// no reference to `env_recovery_force_new_file` anywhere in `noxu-recovery`,
/// and no equivalent of JE's `RecoveryManager.java:329` calling
/// `fileManager.forceNewLogFile()`. The flag is therefore a silent no-op.
///
/// CONTROL: the plain-reopen half of this test (asserting file count does NOT
/// change without the flag) PASSES, proving the observable (`.ndb` file count)
/// is sound and that plain recovery correctly does not flip. Only the
/// force-new-file half fails, isolating the defect to the unconsumed flag.
///
/// IMPACT: JE forces the flip so the last file of a restored backup is
/// immutable (`RecoveryManager.java:326`, JE `[#22834]`); without it a
/// post-restore write appends to the last backup file, which can invalidate a
/// prior incremental-backup snapshot that assumed that file was frozen. Noxu's
/// non-incremental `Backup` API does not currently rely on the flip, so this is
/// latent rather than actively corrupting today — but the config parameter
/// claims a behavior it does not deliver.
///
/// ROOT CAUSE POINTER: wire `DbiConfig::env_recovery_force_new_file` into the
/// recovery finish path (equivalent of `RecoveryManager.forceNewLogFile()` +
/// force-checkpoint) — see JE `RecoveryManager.java:323-336`.
///
/// Kept as a faithful failing port (not weakened): remove `#[ignore]` once the
/// flag is consumed by recovery.
#[test]
#[ignore = "NEW-UTIL-1: ENV_RECOVERY_FORCE_NEW_FILE is plumbed through config             but never consumed by recovery (no forceNewLogFile equivalent);             JE RecoveryManager.java:329. See doc comment for control + root cause."]
fn recovery_force_new_file_flips_last_file() {
    let dir = tempfile::tempdir().unwrap();

    // Create a small env with a single (or few) log files.
    {
        let env = open_env(dir.path(), false, false);
        let db = env
            .open_database(
                None,
                "db1",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .expect("open db");
        let txn = env.begin_transaction(None).expect("begin");
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(b"k"),
            DatabaseEntry::from_bytes(b"v"),
        )
        .expect("put");
        txn.commit().expect("commit");
        drop(db);
        env.close().expect("close");
    }

    // Plain reopen: file count must NOT increase (flag default false).
    let before = count_log_files(dir.path());
    {
        let env = open_env(dir.path(), false, false);
        let after_plain = count_log_files(dir.path());
        assert_eq!(
            after_plain, before,
            "plain recovery must not flip to a new file \
             (JE testForceNewFile: file does not flip by default)"
        );
        env.close().expect("close");
    }

    // Reopen with force-new-file: a new log file must appear.
    let base = count_log_files(dir.path());
    {
        let env = open_env(dir.path(), false, true);
        let after_forced = count_log_files(dir.path());
        assert_eq!(
            after_forced,
            base + 1,
            "ENV_RECOVERY_FORCE_NEW_FILE must flip recovery to a new log file \
             (JE testForceNewFile: getLastFile() goes 0 -> 1)"
        );
        env.close().expect("close");
    }
}
