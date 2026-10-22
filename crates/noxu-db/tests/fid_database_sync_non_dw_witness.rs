// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-DATABASE-SYNC-NONDW (F2) — `Database::sync()` on a NON-deferred-write
//! database must be refused, exactly as JE.
//!
//! JE `Database.sync()` calls `DatabaseImpl.sync(true)`, which throws
//! `UnsupportedOperationException("Database.sync() is only supported for
//! deferred-write databases")` when the database is not durable-deferred-write
//! (DatabaseImpl.java:1389-1395).  Before this fix Noxu silently routed a
//! non-DW `Database::sync()` through `LogManager::flush_sync()` and returned
//! `Ok(())` — more behaviour than JE offers there.  A JE user makes
//! non-transactional / non-DW writes durable with `Environment::sync()` or
//! `Environment::flush_log(true)` instead.
//!
//!  * RED before the fix: `db.sync()` on a non-DW db returned `Ok(())`.
//!  * GREEN after: it returns `OperationNotAllowed` with the JE message.
//!  * A deferred-write db's `sync()` still succeeds (unchanged).
//!  * `Environment::flush_log(true)` is the supported non-DW durability path.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, NoxuError,
};
use tempfile::TempDir;

#[test]
fn database_sync_on_non_deferred_write_db_is_refused() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "p",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    db.put(DatabaseEntry::from_bytes(b"k"), DatabaseEntry::from_bytes(b"v"))
        .unwrap();

    match db.sync() {
        Err(NoxuError::OperationNotAllowed(m)) => assert!(
            m.contains(
                "Database.sync() is only supported for deferred-write databases"
            ),
            "unexpected message: {m}"
        ),
        other => panic!(
            "Database::sync() on a non-deferred-write db must be refused, got \
             {other:?}"
        ),
    }
    // The supported non-DW durability path still works.
    env.flush_log(true).unwrap();
}

#[test]
fn database_sync_on_non_dw_non_transactional_db_is_refused() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "p",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    db.put(DatabaseEntry::from_bytes(b"k"), DatabaseEntry::from_bytes(b"v"))
        .unwrap();

    assert!(
        matches!(db.sync(), Err(NoxuError::OperationNotAllowed(_))),
        "a non-deferred-write db must refuse Database::sync()"
    );
}

#[test]
fn database_sync_on_deferred_write_db_still_succeeds() {
    let dir = TempDir::new().unwrap();
    // Deferred-write DBs are non-transactional in JE.
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "dw",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_deferred_write(true),
        )
        .unwrap();
    db.put(DatabaseEntry::from_bytes(b"k"), DatabaseEntry::from_bytes(b"v"))
        .unwrap();

    // A deferred-write sync is the one case JE supports.
    db.sync().unwrap();
}
