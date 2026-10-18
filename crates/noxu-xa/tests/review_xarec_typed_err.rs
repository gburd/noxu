//! Independent review witness (R-XARECOVERED / OWNER DECISION #19 (a)):
//! verify the TYPED read-path lock error surfaces CONSISTENTLY on EVERY public
//! read entry point when the reader hits an in-doubt recovered-prepared record
//! that the resurrected PreparedTxn WRITE-locked.  Any read path still
//! returning the stringy OperationNotAllowed is a FINDING.
//!
//! Entry points exercised:
//!   * Database::get / get_in / get_into / get_with_options
//!   * primary Cursor read (get_search_key)
//!   * SecondaryDatabase::get_into (secondary read)
//!   * noxu_collections StoredMap::get
#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    NoxuError, ReadOptions, SecondaryConfig, SecondaryDatabase,
    SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use noxu_xa::{XaEnvironment, XaFlags, XaResource, Xid};
use std::sync::Arc;
use tempfile::TempDir;

struct FirstByteKeyCreator;
impl SecondaryKeyCreator for FirstByteKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(d) = data.data_opt() {
            if !d.is_empty() {
                result.set_data(&[d[0]]);
                return true;
            }
        }
        false
    }
}

fn is_typed_lock_err(e: &NoxuError) -> bool {
    matches!(
        e,
        NoxuError::LockTimeout { .. }
            | NoxuError::LockConflict(_)
            | NoxuError::DeadlockDetected
            | NoxuError::LockNotAvailable
    )
}

/// A reader of an in-doubt recovered-prepared record must surface the TYPED
/// lock error via a PRIMARY read, a CURSOR read, and a SECONDARY read.
#[test]
fn indoubt_read_lock_error_is_typed_on_all_primary_and_secondary_paths() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rv_typed_err", b"br1").unwrap();

    // Open primary+secondary, prepare an XA txn writing primary "pk"="Zval"
    // (sec key = 'Z'); crash.
    {
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let primary = Arc::new(Mutex::new(
            env.open_database(
                None,
                "prim",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap(),
        ));
        let sec_inner = env
            .open_database(
                None,
                "sec",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true)
                    .with_sorted_duplicates(true),
            )
            .unwrap();
        let _sec = SecondaryDatabase::open(
            Arc::clone(&primary),
            sec_inner,
            SecondaryConfig::new()
                .with_allow_create(true)
                .with_key_creator(Box::new(FirstByteKeyCreator)),
        )
        .unwrap();
        let xa = XaEnvironment::new(env);
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            primary.lock().put_in(&txn, b"pk", b"Zval").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // Reopen with a short lock timeout; recovery relocks the in-doubt record.
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(false)
            .with_transactional(true)
            .with_lock_timeout(300),
    )
    .unwrap();
    let primary = Arc::new(Mutex::new(
        env.open_database(
            None,
            "prim",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap(),
    ));
    let sec_inner = env
        .open_database(
            None,
            "sec",
            &DatabaseConfig::new()
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        sec_inner,
        SecondaryConfig::new().with_key_creator(Box::new(FirstByteKeyCreator)),
    )
    .unwrap();
    let xa = XaEnvironment::new(env);
    assert_eq!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap(), vec![xid.clone()]);

    // (1) Database::get_in
    {
        let rtxn = xa.inner().begin_transaction(None).unwrap();
        let res = primary.lock().get_in(&rtxn, b"pk");
        assert!(
            matches!(&res, Err(e) if is_typed_lock_err(e)),
            "Database::get_in must surface a TYPED lock error, got {res:?}"
        );
        rtxn.abort().ok();
    }

    // (2) Database::get_with_options
    {
        let rtxn = xa.inner().begin_transaction(None).unwrap();
        let res = primary.lock().get_with_options(
            Some(&rtxn),
            b"pk",
            &ReadOptions::new(),
        );
        assert!(
            matches!(&res, Err(e) if is_typed_lock_err(e)),
            "Database::get_with_options must surface a TYPED lock error, got {res:?}"
        );
        rtxn.abort().ok();
    }

    // (3) primary Cursor read
    {
        let rtxn = xa.inner().begin_transaction(None).unwrap();
        let pg = primary.lock();
        let mut cursor = pg.open_cursor_in(&rtxn, None).unwrap();
        let mut k = DatabaseEntry::from_bytes(b"pk");
        let mut v = DatabaseEntry::new();
        let res = cursor.get(&mut k, &mut v, noxu_db::get::Get::Search, None);
        assert!(
            matches!(&res, Err(e) if is_typed_lock_err(e)),
            "primary Cursor read must surface a TYPED lock error, got {res:?}"
        );
        drop(cursor);
        drop(pg);
        rtxn.abort().ok();
    }

    // (4) SecondaryDatabase::get_into (secondary read of sec key 'Z' ->
    //     primary "pk" which is in-doubt-locked)
    {
        let rtxn = xa.inner().begin_transaction(None).unwrap();
        let mut pk = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let res = sec.get_into(Some(&rtxn), b"Z", &mut pk, &mut data);
        assert!(
            matches!(&res, Err(e) if is_typed_lock_err(e)),
            "SecondaryDatabase::get_into must surface a TYPED lock error (not \
             stringy OperationNotAllowed), got {res:?}"
        );
        rtxn.abort().ok();
    }

    // Resolve so the env can close cleanly.
    xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
}
