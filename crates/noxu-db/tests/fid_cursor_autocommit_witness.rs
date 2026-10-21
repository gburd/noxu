// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-CURSOR-AUTOCOMMIT — a cursor opened WITHOUT a transaction on a
//! TRANSACTIONAL database must refuse every update (JE
//! `Cursor.updateOperationsProhibited`).
//!
//! JE: `Cursor.init` sets `updateOperationsProhibited = ... ||
//! (databaseImpl.isTransactional() && !locker.isTransactional()) || ...`
//! (Cursor.java:333-338), and `checkUpdatesAllowed` throws
//! `UnsupportedOperationException` "Write operation is not allowed because a
//! Transaction was not supplied to openCursor and the Database is
//! transactional." (Cursor.java:5340-5367).  JE has NO auto-commit cursor
//! writes on a transactional db.
//!
//! Noxu maps that throw to `NoxuError::OperationNotAllowed` (the variant v8
//! uses).  Reads stay allowed.  Internal engine cursors (secondary
//! maintenance, `noxu-collections`, `noxu-persist`, XA, cleaner/recovery) run
//! under a transactional auto-commit locker, so their writes are allowed —
//! they use `open_cursor_internal`, not the user-facing `open_cursor`.
//!
//! Witnesses:
//!   * txnal db, `open_cursor(None)`: `put` / `delete` / `put_current` all
//!     return `OperationNotAllowed` (RED before the fix: they succeeded).
//!   * txnal db, `open_cursor(None)`: reads (`get`, scan) are allowed.
//!   * non-txn db, `open_cursor(None)`: writes are allowed (JE ThreadLocker).
//!   * txnal db, `open_cursor_in(&txn)`: writes are allowed.
//!   * secondary-index maintenance via `Database::put`/`delete` (internal
//!     transactional locker) still maintains secondaries.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    NoxuError, OperationStatus, Put,
};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("open env")
}

fn is_prohibited(e: &NoxuError) -> bool {
    matches!(e, NoxuError::OperationNotAllowed(msg)
        if msg.contains("a Transaction was not supplied to openCursor")
            && msg.contains("Database is transactional"))
}

#[test]
fn txnal_db_cursor_put_without_txn_is_refused() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "txnal",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let mut c = db.open_cursor(None).expect("open read cursor on txnal db");
    let err = c
        .put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
            Put::Overwrite,
        )
        .expect_err("cursor put without txn on a txnal db must be refused");
    assert!(is_prohibited(&err), "wrong error: {err:?}");
    // No write landed.
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn txnal_db_cursor_delete_without_txn_is_refused() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "txnal",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    // Seed a record via a legal auto-commit put.
    db.put(b"k", b"v").unwrap();

    let mut c = db.open_cursor(None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    // Reads are allowed — position the cursor.
    assert_eq!(
        c.get(&mut key, &mut data, Get::First, None).unwrap(),
        OperationStatus::Success,
    );
    let err =
        c.delete().expect_err("cursor delete without txn must be refused");
    assert!(is_prohibited(&err), "wrong error: {err:?}");
    // The record is still there.
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn txnal_db_cursor_put_current_without_txn_is_refused() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "txnal",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    db.put(b"k", b"v").unwrap();

    let mut c = db.open_cursor(None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut key, &mut data, Get::First, None).unwrap(),
        OperationStatus::Success,
    );
    let err = c
        .put(
            &DatabaseEntry::new(),
            &DatabaseEntry::from_bytes(b"v2"),
            Put::Current,
        )
        .expect_err("cursor put_current without txn must be refused");
    assert!(is_prohibited(&err), "wrong error: {err:?}");
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn txnal_db_cursor_reads_without_txn_are_allowed() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "txnal",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    for i in 0u32..8 {
        db.put(i.to_be_bytes(), b"v").unwrap();
    }

    let mut c = db.open_cursor(None).expect("open read cursor");
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut seen = 0u32;
    let mut st = c.get(&mut key, &mut data, Get::First, None).unwrap();
    while st == OperationStatus::Success {
        seen += 1;
        st = c.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    assert_eq!(seen, 8, "all 8 records must be readable via open_cursor(None)");
}

#[test]
fn nontxn_db_cursor_writes_without_txn_are_allowed() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    // Non-transactional db (JE ThreadLocker) — writes are allowed.
    let db = env
        .open_database(
            None,
            "nontxn",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    let mut c = db.open_cursor(None).unwrap();
    assert_eq!(
        c.put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
            Put::Overwrite,
        )
        .unwrap(),
        OperationStatus::Success,
    );
    c.close().unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn txnal_db_cursor_writes_with_txn_are_allowed() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "txnal",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let txn = env.begin_transaction(None).unwrap();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        assert_eq!(
            c.put(
                &DatabaseEntry::from_bytes(b"k"),
                &DatabaseEntry::from_bytes(b"v"),
                Put::Overwrite,
            )
            .unwrap(),
            OperationStatus::Success,
        );
        c.close().unwrap();
    }
    txn.commit().unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}
