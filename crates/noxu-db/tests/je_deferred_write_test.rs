//! JE `DeferredWriteTest` port — deferred-write database durability.
//! Faithful port of the cleanly-observable core of
//! `test/com/sleepycat/je/test/DeferredWriteTest.java`.
//!
//! JE citation:
//!   - `DeferredWriteTest.testRecoverSync` / `testCloseOpen` (doSync=true):
//!     a deferred-write DB's records are NOT written to the WAL on put (they
//!     are flushed only at eviction / checkpoint / explicit `sync()`), yet
//!     after `db.sync()` + a clean close they survive reopen.
//!
//! Classification of the other DeferredWriteTest methods (see report):
//!   - `testRecoverNoSync` / `testRecoverNoSyncEvict` / `testCloseOpenNoSync`:
//!     the DISTINGUISHING deferred-write property — unsynced records are LOST
//!     after an ABNORMAL close (no checkpoint).  Noxu's `Environment` runs a
//!     FINAL CHECKPOINT on clean close/drop (F12, JE parity) and exposes no
//!     "close without checkpoint" knob (documented in
//!     `je_recovery_test.rs`), so an unsynced-loss test requires a
//!     SIGKILL-crash subprocess harness (`crash_worker`).  N/A here
//!     (test-infra: needs the crash harness), not an engine deviation.
//!   - `testTempIsDeferredWriteMode`, `testTransientLsn`, `testCheckpoint*`,
//!     `testPruneBINs`, `testCompressAfterSlotReuse`,
//!     `testTempEvictionAndObsoleteCounting`, `testCleaning5000`,
//!     `testCleanAfterDelete`, `testEmptyDatabaseSR14744`,
//!     `testRemoveNonPersistentDbSR15317`, `testLockDuringLogging`,
//!     `testPreloadSync`/`testPreloadNoSync`: JE-internal probes
//!     (`DbInternal.getFileManager().getNextLsn()`, transient-LSN internals,
//!     per-file obsolete accounting, BIN-pruning representation) or
//!     temp-DB-removal / cleaner internals.  N/A (JE-internal).
//!   - `testBadConfigurations`: JE rejects a txnal deferred-write DB and a
//!     mismatched reopen; Noxu does not surface those specific config
//!     rejections (deferred-write + transactional is not validated).  N/A
//!     (config-validation gap, noted).
//!
//! Deviation: JE `testTempIsDeferredWriteMode` proves "no WAL write on insert"
//! via `getNextLsn()`.  Noxu exposes no public next-LSN probe; we instead
//! assert the observable durability contract (sync makes deferred writes
//! survive reopen), which exercises the same deferred-flush code path.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use std::collections::BTreeSet;
use std::path::Path;
use tempfile::TempDir;

const DBNAME: &str = "foo";
const NUM_RECORDS: u32 = 30;

fn open_env(dir: &Path) -> noxu_db::Environment {
    // Non-transactional env (deferred-write DBs are non-transactional in JE).
    noxu_db::Environment::open(
        EnvironmentConfig::new(dir.to_path_buf()).with_allow_create(true),
    )
    .unwrap()
}

fn open_deferred_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        DBNAME,
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_deferred_write(true),
    )
    .unwrap()
}

fn ikey(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

fn collect_keys(db: &noxu_db::Database) -> BTreeSet<u32> {
    let mut c = db.open_cursor(None).unwrap();
    let mut out = BTreeSet::new();
    while let Some((k, _)) = c.next().unwrap() {
        out.insert(u32::from_be_bytes([k[0], k[1], k[2], k[3]]));
    }
    out
}

/// JE `DeferredWriteTest.testRecoverSync` (doSync=true): a deferred-write DB's
/// records survive a `sync()` + clean close/reopen.
#[test]
fn je_deferred_write_test_sync_then_survives_reopen() {
    let dir = TempDir::new().unwrap();

    let expected: BTreeSet<u32> = (1..=NUM_RECORDS).collect();
    {
        let env = open_env(dir.path());
        let db = open_deferred_db(&env);

        // Insert into the deferred-write DB (these skip the WAL).
        for i in 1..=NUM_RECORDS {
            db.put(ikey(i), ikey(i)).unwrap();
        }

        // In-session the records are visible.
        assert_eq!(
            collect_keys(&db),
            expected,
            "deferred-write records must be readable in-session"
        );

        // sync() flushes the deferred writes to the log (JE db.sync()).
        db.sync().unwrap();
        db.close().unwrap();
        drop(env);
    }

    // Reopen: the synced deferred-write records must all survive.
    let env = open_env(dir.path());
    let db = open_deferred_db(&env);
    assert_eq!(
        collect_keys(&db),
        expected,
        "synced deferred-write records must survive close + reopen"
    );
    // Point-verify a couple of records (not just the scan).
    let mut out = DatabaseEntry::new();
    for i in [1u32, NUM_RECORDS] {
        assert!(
            db.get_into(None, ikey(i), &mut out).unwrap(),
            "record {i} must survive"
        );
        assert_eq!(out.data_opt().unwrap(), &ikey(i));
    }
    db.close().unwrap();
}
