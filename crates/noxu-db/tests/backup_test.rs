//! Integration test for the JE `DbBackup`-equivalent hot-backup API.
//!
//! Ported contract (JE `util/DbBackup.java`):
//!   - `start_backup()` pins the active log-file set via the cleaner's
//!     `FileProtector`, so the cleaner cannot delete any file in the backup
//!     set while the backup is open (`DbBackup.startBackup`, DbBackup.java:480).
//!   - `log_files_in_backup_set()` returns a stable, non-empty set of the
//!     `.ndb` files current as of `start_backup` (`getLogFilesInBackupSet`,
//!     DbBackup.java:648).
//!   - Copying that set to a fresh directory yields an environment that
//!     recovers to the same data.
//!   - `end_backup()` (or drop) releases the protection so the cleaner can
//!     reclaim (`DbBackup.endBackup`, DbBackup.java:588).
//!
//! This test is written BEFORE the implementation and is expected to fail to
//! compile against the pre-fix API (there is no `start_backup`).

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use noxu_db::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};

fn sample_records() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0u32..500)
        .map(|i| {
            let k = format!("key-{i:06}").into_bytes();
            // Values big enough to roll several log files under a small
            // log_file_max so the backup set is more than one file.
            let v = vec![(i % 251) as u8; 400];
            (k, v)
        })
        .collect()
}

fn open_rw(dir: &Path) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            // Small files so 500 * ~400B rolls many log files.
            .with_log_file_max_bytes(64 * 1024),
    )
    .expect("open env")
}

fn populate(env: &Environment) {
    let db = env
        .open_database(
            None,
            "data",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");
    let txn = env.begin_transaction(None).expect("begin");
    for (k, v) in sample_records() {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k),
            DatabaseEntry::from_bytes(&v),
        )
        .expect("put");
    }
    txn.commit().expect("commit");
    drop(db);
}

fn read_all(dir: &Path) -> BTreeSet<(Vec<u8>, Vec<u8>)> {
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf()).with_read_only(true),
    )
    .expect("reopen env");
    let db = env
        .open_database(
            None,
            "data",
            &DatabaseConfig::new().with_read_only(true),
        )
        .expect("reopen db");
    let mut out = BTreeSet::new();
    for r in db.iter(None).expect("iter") {
        let (k, v) = r.expect("read");
        out.insert((k, v));
    }
    drop(db);
    env.close().expect("close");
    out
}

/// HEADLINE: the full JE DbBackup workflow — pin, enumerate, copy, recover,
/// release.
#[test]
fn backup_pins_files_and_restore_recovers_same_data() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    let env = open_rw(src.path());
    populate(&env);
    // Reduce recovery time after restore, as JE recommends before startBackup.
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .expect("checkpoint");

    let expected = {
        // Snapshot the live data via a throwaway read of the src (through the
        // same open env would need a read txn; simplest is to compute from the
        // known input).
        sample_records().into_iter().collect::<BTreeSet<_>>()
    };

    // --- start_backup: pin the active file set. ---
    let backup = env.start_backup().expect("start_backup");

    let files = backup.log_files_in_backup_set();
    assert!(
        !files.is_empty(),
        "backup set must be non-empty for a populated env"
    );
    assert!(
        files.len() >= 2,
        "expected multiple log files with a 64 KiB log_file_max, got {}",
        files.len()
    );

    // --- cleaner must NOT be able to delete a pinned file. ---
    // Run cleaning + checkpoint while the backup is open; the pinned files
    // must all still exist on disk afterward.
    let _ = env.clean_log();
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .expect("checkpoint during backup");
    let _ = env.clean_log();

    for f in &files {
        assert!(
            f.exists(),
            "pinned backup file {} was deleted while backup was open",
            f.display()
        );
    }

    // --- copy the backup set to a fresh directory. ---
    for f in &files {
        let name = f.file_name().expect("file name");
        fs::copy(f, dst.path().join(name)).expect("copy backup file");
    }

    // --- end_backup releases protection. ---
    backup.end_backup().expect("end_backup");

    env.close().expect("close src env");

    // --- the copied set opens and recovers to the same data. ---
    let restored = read_all(dst.path());
    assert_eq!(
        restored, expected,
        "restored env data must match the source data exactly"
    );
}
