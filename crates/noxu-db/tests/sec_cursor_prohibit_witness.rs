// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-CURSOR-AUTOCOMMIT completeness (F1) — a SECONDARY cursor opened with no
//! transaction on a *transactional* database must refuse writes, exactly as
//! the primary `Cursor` does.
//!
//! JE: `Cursor.init` sets `updateOperationsProhibited = ... ||
//! (databaseImpl.isTransactional() && !locker.isTransactional()) || ...`
//! (Cursor.java:333-338); `SecondaryCursor.delete` goes through the same
//! `checkUpdatesAllowed` throw (Cursor.java:5340-5367).  A `SecondaryCursor`
//! opened with `txn == null` on a transactional db has a non-transactional
//! locker, so JE throws `UnsupportedOperationException` before cascading the
//! delete to the primary.
//!
//!  * RED before the fix: a no-txn `SecondaryCursor::delete` on a
//!    transactional secondary SUCCEEDED (the primary record was deleted).
//!  * GREEN after: it returns `OperationNotAllowed` with the JE message.
//!  * A non-transactional secondary still ALLOWS the delete.
//!  * An explicit-txn secondary cursor still ALLOWS the delete.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    NoxuError, OperationStatus, Put, SecondaryConfig, SecondaryDatabase,
    SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

struct OffsetKeyCreator;
impl SecondaryKeyCreator for OffsetKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _k: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(d) = data.data_opt()
            && !d.is_empty()
        {
            result.set_data(&[d[0].wrapping_add(100)]);
            return true;
        }
        false
    }
}

fn setup(
    txnal: bool,
) -> (TempDir, Environment, Arc<Mutex<Database>>, SecondaryDatabase) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let primary = Arc::new(Mutex::new(
        env.open_database(
            None,
            "p",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(txnal),
        )
        .unwrap(),
    ));
    let inner = env
        .open_database(
            None,
            "s",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(txnal)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap();
    (dir, env, primary, sec)
}

fn insert(env: &Environment, primary: &Arc<Mutex<Database>>, txnal: bool) {
    if txnal {
        let t = env.begin_transaction(None).unwrap();
        {
            let pri = primary.lock();
            let mut c = pri.open_cursor_in(&t, None).unwrap();
            c.put(
                &DatabaseEntry::from_bytes(b"k"),
                &DatabaseEntry::from_bytes(&[5]),
                Put::Overwrite,
            )
            .unwrap();
            c.close().unwrap();
        }
        t.commit().unwrap();
    } else {
        let pri = primary.lock();
        pri.put(b"k", [5]).unwrap();
    }
}

#[test]
fn no_txn_secondary_cursor_delete_on_txnal_db_is_refused() {
    let (_dir, _env, primary, sec) = setup(true);
    insert(&_env, &primary, true);

    let mut cur = sec.open_cursor(None).unwrap();
    let mut sk = DatabaseEntry::new();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert_eq!(
        cur.get_first(&mut sk, &mut pk, &mut d).unwrap(),
        OperationStatus::Success
    );
    match cur.delete() {
        Err(NoxuError::OperationNotAllowed(m)) => assert!(
            m.contains("a Transaction was not supplied to openCursor"),
            "unexpected message: {m}"
        ),
        other => panic!("expected refusal, got {other:?}"),
    }
    // The primary record must still be present (the delete was refused).
    drop(cur);
    assert_eq!(primary.lock().get(b"k").unwrap().as_deref(), Some(&[5][..]));
}

#[test]
fn no_txn_secondary_cursor_delete_on_nontxn_db_is_allowed() {
    let (_dir, _env, primary, sec) = setup(false);
    insert(&_env, &primary, false);

    let mut cur = sec.open_cursor(None).unwrap();
    let mut sk = DatabaseEntry::new();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert_eq!(
        cur.get_first(&mut sk, &mut pk, &mut d).unwrap(),
        OperationStatus::Success
    );
    assert_eq!(cur.delete().unwrap(), OperationStatus::Success);
    drop(cur);
    assert!(primary.lock().get(b"k").unwrap().is_none());
}

#[test]
fn explicit_txn_secondary_cursor_delete_on_txnal_db_is_allowed() {
    let (_dir, env, primary, sec) = setup(true);
    insert(&env, &primary, true);

    let t = env.begin_transaction(None).unwrap();
    {
        let mut cur = sec.open_cursor_in(&t, None).unwrap();
        let mut sk = DatabaseEntry::new();
        let mut pk = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        assert_eq!(
            cur.get_first(&mut sk, &mut pk, &mut d).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(cur.delete().unwrap(), OperationStatus::Success);
        cur.close().unwrap();
    }
    t.commit().unwrap();
    assert!(primary.lock().get(b"k").unwrap().is_none());
}
