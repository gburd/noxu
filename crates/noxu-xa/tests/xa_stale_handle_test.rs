//! Regression test for audit finding C4 / F15 / V13 (MEDIUM):
//! `xa_end(TMFAIL)` must mark the inner `Txn` abort-only so a stale
//! `Transaction` handle obtained *before* `xa_end` can no longer write.
//!
//! JE parity: `XAEnvironment.end(Xid, int flags)` (XAEnvironment.java:113-159)
//! on `TMFAIL` constructs `new XAFailureException(txn)`
//! (XAEnvironment.java:150) purely for its side effect —
//! `XAFailureException`'s constructor calls
//! `super(locker, true /*abortOnly*/, ...)` (XAFailureException.java:37-41),
//! which sets the **Locker's own `abortOnly` flag** (the
//! `Locker.setOnlyAbortable()` semantics). After `TMFAIL` the underlying
//! `Txn`/`Locker` itself refuses any further put/get regardless of whether
//! the caller goes through the XA wrapper or holds a raw handle obtained
//! earlier. Noxu mirrors this by calling `Txn::set_only_abortable()`
//! (noxu-txn/src/txn.rs:864) on the inner transaction.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use noxu_xa::{XaEnvironment, XaFlags, XaResource, Xid};
use tempfile::TempDir;

struct Cluster {
    xa: XaEnvironment,
    db: Database,
    _dir: TempDir,
}

impl Cluster {
    fn new(name: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, name, &db_config).unwrap();
        let xa = XaEnvironment::new(env);
        Self { xa, db, _dir: dir }
    }
}

fn xid() -> Xid {
    Xid::new(1, b"gtrid_tmfail", b"branch_00").unwrap()
}

/// The core regression: a caller who took the `Transaction` handle BEFORE
/// `xa_end(TMFAIL)` (the normal usage pattern) must NOT be able to write
/// through it afterward. On base 72f60240 this write wrongly SUCCEEDS.
#[test]
fn tmfail_marks_inner_txn_abort_only_stale_handle_rejected() {
    let c = Cluster::new("db");
    let x = xid();

    c.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();

    // Obtain the handle BEFORE xa_end — the common pattern.
    let txn = c.xa.get_transaction(&x).unwrap();

    let k1 = DatabaseEntry::from_vec(b"k1".to_vec());
    let v1 = DatabaseEntry::from_vec(b"v1".to_vec());
    c.db.put_in(&txn, &k1, &v1).unwrap();

    // Branch fails.
    c.xa.xa_end(&x, XaFlags::TMFAIL).unwrap();

    // A protocol-violating write through the STALE handle must be rejected.
    let k2 = DatabaseEntry::from_vec(b"k2".to_vec());
    let v2 = DatabaseEntry::from_vec(b"v2".to_vec());
    let res = c.db.put_in(&txn, &k2, &v2);
    assert!(
        res.is_err(),
        "C4/F15: write through stale handle after xa_end(TMFAIL) must be \
         rejected (inner Txn abort-only), but it succeeded: {res:?}"
    );

    // Re-fetching through the XA API is (and was) already rejected.
    assert!(
        c.xa.get_transaction(&x).is_err(),
        "get_transaction after TMFAIL must still reject (branch not Active)"
    );

    // The branch is rollback-only: xa_rollback must still succeed (the
    // abort-only flag must NOT block the legitimate rollback path).
    c.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// The normal, well-behaved XA flow must be completely unaffected:
/// xa_end(TMSUCCESS) -> xa_prepare -> xa_commit, and data is durable.
#[test]
fn tmsuccess_flow_unaffected() {
    let c = Cluster::new("db");
    let x = Xid::new(1, b"gtrid_ok", b"branch_00").unwrap();

    c.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    {
        let txn = c.xa.get_transaction(&x).unwrap();
        let k = DatabaseEntry::from_vec(b"key".to_vec());
        let v = DatabaseEntry::from_vec(b"val".to_vec());
        c.db.put_in(&txn, &k, &v).unwrap();
    }
    c.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    assert!(matches!(
        c.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap(),
        noxu_xa::PrepareResult::Ok
    ));
    c.xa.xa_commit(&x, XaFlags::NOFLAGS).unwrap();

    let k = DatabaseEntry::from_vec(b"key".to_vec());
    let mut val = DatabaseEntry::new();
    assert!(c.db.get_into(None, &k, &mut val).unwrap());
    assert_eq!(val.data_opt(), Some(b"val".as_ref()));
}
