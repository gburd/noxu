//! TXN-6: a transactional handle must be rejected on a non-transactional
//! database for EVERY operation (get/put/delete), not just cursor-open.
//! JE LockerFactory.getWritableLocker/getReadableLocker throw
//! IllegalArgumentException on every op.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};

#[test]
fn txn6_get_put_delete_reject_txn_on_non_txnal_db() {
    let dir = tempfile::tempdir().unwrap();
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();

    // Open a NON-transactional database in a transactional env.
    let db_config =
        DatabaseConfig::new().with_allow_create(true).with_transactional(false);
    let db = env.open_database(None, "nontxn", &db_config).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"k");
    let val = DatabaseEntry::from_bytes(b"v");
    let mut out = DatabaseEntry::new();

    // get/put/delete with Some(txn) must all be rejected (IllegalArgument),
    // matching JE's per-operation guard (not just cursor-open).
    assert!(
        db.get_into(Some(&txn), &key, &mut out).is_err(),
        "TXN-6: db.get(Some(txn)) on a non-txnal DB must be rejected"
    );
    assert!(
        db.put_in(&txn, &key, &val).is_err(),
        "TXN-6: db.put(Some(txn)) on a non-txnal DB must be rejected"
    );
    assert!(
        db.delete_in(&txn, &key).is_err(),
        "TXN-6: db.delete(Some(txn)) on a non-txnal DB must be rejected"
    );

    // None (auto-commit) must still work on the non-txnal DB.
    assert!(db.put(&key, &val).is_ok());
    assert!(db.get_into(None, &key, &mut out).unwrap());
    let _ = txn.abort();
}
