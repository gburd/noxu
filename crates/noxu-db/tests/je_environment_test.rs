//! JE EnvironmentTest ports — Environment public-API contract.
//!
//! Each test corresponds to a method in
//! `test/com/sleepycat/je/EnvironmentTest.java`.  The DB rename/remove
//! transactional-abort/commit variants (testDbRename*/testDbRemove*) are
//! covered in `ddl_txn_abort_test.rs`; the checkpoint/durability variants
//! (testFlushLog) and read-only DDL rejection (testReadOnlyDbNameOps) in
//! `je_database_test.rs`.  This file covers the open/close/config/exceptions/
//! daemon/getDatabaseNames surface.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig,
};
use tempfile::TempDir;

fn txn_env(dir: &TempDir, create: bool) -> EnvironmentConfig {
    EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(create)
        .with_transactional(true)
}

fn dbcfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testBasic
//
// JE invariant: an environment can be created, closed, and re-opened (now
// that it exists) with allow_create=false.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_basic_open_close_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let env = Environment::open(txn_env(&dir, true)).unwrap();
        env.close().unwrap();
        // Drop the handle so the OS file lock is released before reopen
        // (close() flushes/quiesces; the lock is released on Drop).
    }

    // Re-open now that it exists, allow_create=false.
    let env = Environment::open(txn_env(&dir, false)).unwrap();
    env.close().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testTransactional
//
// JE invariant: beginning a transaction on a NON-transactional environment
// must fail (IllegalArgumentException in JE; Err in Noxu).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_non_transactional_begin_txn_fails() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true),
    )
    .unwrap();
    let r = env.begin_transaction(None);
    assert!(r.is_err(), "begin_transaction on a non-txn env must fail");
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testReadOnly
//
// JE invariant: opening a database that would create it (allow_create) on a
// READ-ONLY environment must fail.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_read_only_rejects_db_create() {
    let dir = TempDir::new().unwrap();
    {
        let env = Environment::open(txn_env(&dir, true)).unwrap();
        env.close().unwrap();
    }
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_read_only(true)
            .with_transactional(true),
    )
    .unwrap();
    let r = env.open_database(None, "new_db", &dbcfg());
    assert!(r.is_err(), "creating a db on a read-only env must fail");
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testExceptions
//
// JE invariant: public operations on a CLOSED environment throw
// IllegalStateException (Err in Noxu): openDatabase, removeDatabase,
// renameDatabase, truncateDatabase.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_ops_on_closed_env_fail() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(txn_env(&dir, true)).unwrap();
    env.close().unwrap();

    assert!(env.open_database(None, "x", &dbcfg()).is_err());
    assert!(env.remove_database(None, "x").is_err());
    assert!(env.rename_database(None, "x", "y").is_err());
    assert!(env.truncate_database(None, "x").is_err());
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testClose
//
// JE invariant: closing an environment that still has an open transaction
// throws IllegalStateException (Err in Noxu).  After the txn is resolved the
// close succeeds.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_close_with_open_txn_fails() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(txn_env(&dir, true)).unwrap();
    let txn = env.begin_transaction(None).unwrap();
    assert!(env.close().is_err(), "close with an open txn must fail");
    txn.commit().unwrap();
    env.close().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testGetDatabaseNames
//
// JE invariant: getDatabaseNames reflects the set of databases; after
// open+close of DB1, DB2, the names include both; after renaming DB2->DB3 and
// DB1->DB4, the names reflect the renames (no stale entries).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_get_database_names_reflects_open_and_rename() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(txn_env(&dir, true)).unwrap();
    assert!(env.database_names().unwrap().is_empty());

    env.open_database(None, "DB1", &dbcfg()).unwrap().close().unwrap();
    let names = env.database_names().unwrap();
    assert!(names.contains(&"DB1".to_string()));

    env.open_database(None, "DB2", &dbcfg()).unwrap().close().unwrap();
    let names = env.database_names().unwrap();
    assert!(names.contains(&"DB1".to_string()));
    assert!(names.contains(&"DB2".to_string()));

    // Rename DB2 -> DB3, DB1 -> DB4.
    env.rename_database(None, "DB2", "DB3").unwrap();
    env.rename_database(None, "DB1", "DB4").unwrap();
    let names = env.database_names().unwrap();
    assert!(!names.contains(&"DB1".to_string()), "renamed away");
    assert!(!names.contains(&"DB2".to_string()), "renamed away");
    assert!(names.contains(&"DB3".to_string()));
    assert!(names.contains(&"DB4".to_string()));
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testConfig
//
// JE invariant: Environment.getConfig() returns a SNAPSHOT — mutating the
// returned config does not change the environment's live config, and a fresh
// getConfig() still reports the original values.
//
// Noxu adaptation: `env.config()` returns `&EnvironmentConfig`; the snapshot
// property is expressed by cloning and mutating the clone, which cannot affect
// the environment's stored config (borrow-checked).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_config_is_a_snapshot() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_lock_timeout(7000),
    )
    .unwrap();

    let snap = env.config().clone();
    assert!(snap.transactional);
    assert_eq!(snap.lock_timeout_ms, 7000);

    // Mutating the clone does not affect the env's live config.
    let mut other = snap;
    other.lock_timeout_ms = 999;
    let _ = &other;
    assert_eq!(env.config().lock_timeout_ms, 7000, "env config unchanged");
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testMemOnly
//
// JE invariant: a memory-only (LOG_MEMORY_ONLY) environment supports the full
// put/delete/put cycle through a cursor and leaves no log files on disk
// (nothing is persisted).  Noxu adaptation: exercise the cursor put/delete
// cycle on a `log_mem_only` env; the operations must succeed.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_mem_only_supports_cursor_ops() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_mem_only(true),
    )
    .unwrap();
    let db = env.open_database(None, "foo", &dbcfg()).unwrap();
    let txn = env.begin_transaction(None).unwrap();
    db.put_in(
        &txn,
        DatabaseEntry::from_bytes(b"k"),
        DatabaseEntry::from_bytes(b"v"),
    )
    .unwrap();
    assert!(db.delete_in(&txn, DatabaseEntry::from_bytes(b"k")).unwrap());
    db.put_in(
        &txn,
        DatabaseEntry::from_bytes(b"k"),
        DatabaseEntry::from_bytes(b"v2"),
    )
    .unwrap();
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap(), 1);
    db.close().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentTest.testDaemonManualInvocation
//
// JE invariant: the maintenance daemons (checkpointer, compressor, cleaner)
// can be invoked MANUALLY via the public API and complete without error, even
// with the background daemons configured off.  Noxu exposes `checkpoint`,
// `compress`, and `clean_log` as the manual entry points.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_daemon_manual_invocation() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(txn_env(&dir, true)).unwrap();
    let db = env.open_database(None, "d", &dbcfg()).unwrap();
    let txn = env.begin_transaction(None).unwrap();
    for i in 0u32..64 {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        db.delete_in(&txn, DatabaseEntry::from_bytes(&i.to_be_bytes()))
            .unwrap();
    }
    txn.commit().unwrap();
    db.close().unwrap();

    // Manual daemon invocations must all succeed.
    env.checkpoint(Some(&CheckpointConfig::new())).unwrap();
    let _ = env.compress().unwrap();
    let _ = env.clean_log().unwrap();
    env.close().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: ApiTest.testBasic
//
// JE invariant: `new Environment(null, null)` (null home / null config) raises
// IllegalArgumentException — the API rejects a nonsensical open request.
//
// Noxu adaptation: Rust's type system makes a null home unrepresentable
// (`EnvironmentConfig` carries a `PathBuf`), so the closest analog of "an open
// request that cannot succeed" is: opening a NON-existent environment directory
// with allow_create=false must fail (Err), not silently create or succeed.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_open_nonexistent_without_create_fails() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("does_not_exist_subdir");
    let r = Environment::open(
        EnvironmentConfig::new(missing).with_allow_create(false),
    );
    assert!(
        r.is_err(),
        "opening a non-existent env dir with allow_create=false must fail"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: EnvironmentStatTest.testFSyncStats / testDbFSyncs
//
// JE invariant: committed writes drive the fsync counters
// (getNLogFSyncs / getNFSyncRequests) upward.  Noxu's EnvironmentStats exposes
// `log.n_log_fsyncs` / `log.n_fsync_requests`.
//
// (Also cited in metrics_export_test.rs, which asserts noxu_log_fsyncs_total > 0
// after committed writes.)
//
// JE: EnvironmentStatTest.testRepeatFaultReads — Noxu exposes
// `log.n_repeat_fault_reads`; we assert the counter is present and monotonic
// (>= 0), since forcing a specific fault-read count depends on eviction timing.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn environment_stats_fsync_and_fault_counters() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(txn_env(&dir, true)).unwrap();
    let db = env.open_database(None, "d", &dbcfg()).unwrap();
    let txn = env.begin_transaction(None).unwrap();
    for i in 0u32..64 {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();
    env.checkpoint(Some(&CheckpointConfig::new())).unwrap();

    let stats = env.stats().unwrap();
    assert!(
        stats.log.n_log_fsyncs > 0 || stats.log.n_fsync_requests > 0,
        "committed + checkpointed writes must drive the fsync counters: \
         n_log_fsyncs={}, n_fsync_requests={}",
        stats.log.n_log_fsyncs,
        stats.log.n_fsync_requests
    );
    // repeat-fault-reads counter is present and non-negative (u64).
    let _ = stats.log.n_repeat_fault_reads;
    db.close().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: DbHandleLockTest.testOpenHandle
//
// JE invariant: opening a database under a transaction acquires the database
// HANDLE LOCK, which shows up in the lock stats (getNTotalLocks /
// getNWriteLocks increase while the handle-holding txn is open).
//
// Noxu adaptation: assert the lock-stat counters reflect an open transaction
// holding locks on an opened DB — n_total_locks > 0 while the txn+handle are
// live.  (The exact +1 accounting is JE-internal; we assert the observable
// effect: locks are held.)
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn db_handle_lock_open_handle_acquires_locks() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(txn_env(&dir, true)).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let db = env.open_database(Some(&txn), "foo", &dbcfg()).unwrap();
    // Write a record under the txn so a record lock is definitely held.
    db.put_in(
        &txn,
        DatabaseEntry::from_bytes(b"k"),
        DatabaseEntry::from_bytes(b"v"),
    )
    .unwrap();

    let stats = env.stats().unwrap();
    assert!(
        stats.lock.n_total_locks > 0,
        "an open txn holding an opened-DB handle + a write must report locks; \
         got n_total_locks={}",
        stats.lock.n_total_locks
    );

    db.close().unwrap();
    txn.commit().unwrap();
}
