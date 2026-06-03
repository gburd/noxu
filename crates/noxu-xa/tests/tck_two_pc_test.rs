//! JE TCK port of `com.sleepycat.je.txn.TwoPCTest`.
//!
//! TwoPCTest exercises the basic XA two-phase-commit cycle through JE's
//! `XAEnvironment` API.  Noxu's analogous surface is `XaEnvironment`
//! plus the `XaResource` trait (`xa_start` / `xa_end` / `xa_prepare` /
//! `xa_commit` / `xa_rollback`).  The state-machine semantics are
//! identical: a transaction transitions Nonexistent → Active → Idle →
//! Prepared → committed / rolled-back.
//!
//! Adaptations
//!
//! - JE's `env.beginTransaction(); env.setXATransaction(xid, txn)` is
//!   replaced by `xa_start(xid, NOFLAGS)`, which atomically creates the
//!   underlying transaction and binds it to the XID.
//! - JE's `env.prepare(xid)` returns `XAResource.XA_RDONLY` for a
//!   read-only transaction; noxu's `xa_prepare` returns
//!   `PrepareResult::ReadOnly`.
//! - JE's `TransactionStats` (nBegins / nXAPrepares / nXACommits) are not
//!   modelled here; the round-trip and error paths are the invariants we
//!   port.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use noxu_xa::{PrepareResult, XaEnvironment, XaFlags, XaResource, Xid};
use tempfile::TempDir;

struct Harness {
    xa: XaEnvironment,
    db: Database,
    _dir: TempDir,
}

impl Harness {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "foo", &db_config).unwrap();
        let xa = XaEnvironment::new(env);
        Self { xa, db, _dir: dir }
    }

    fn put(&self, xid: &Xid, key: &[u8], val: &[u8]) {
        let txn = self.xa.get_transaction(xid).unwrap();
        let k = DatabaseEntry::from_bytes(key);
        let v = DatabaseEntry::from_bytes(val);
        self.db.put(Some(&*txn), &k, &v).unwrap();
        self.xa.mark_write(xid).unwrap();
    }

    fn exists(&self, key: &[u8]) -> bool {
        let k = DatabaseEntry::from_bytes(key);
        let mut v = DatabaseEntry::new();
        matches!(
            self.db.get(None, &k, &mut v).unwrap(),
            OperationStatus::Success,
        )
    }
}

fn xid(label: &str) -> Xid {
    Xid::new(1, label.as_bytes(), b"b1").unwrap()
}

// ---------------------------------------------------------------------------
// testBasic2PC
// ---------------------------------------------------------------------------

/// Port of `TwoPCTest.testBasic2PC`.
///
/// JE drives: beginTransaction → setXATransaction → put → prepare → commit
/// and asserts no errors.  Noxu's equivalent: xa_start → put → xa_end →
/// xa_prepare → xa_commit.  We additionally assert the value is durable
/// after commit.
#[test]
fn tck_two_pc_test_basic_two_pc() {
    let env = Harness::new();
    let x = xid("TwoPCTest1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"key", b"data");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();

    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    assert_eq!(PrepareResult::Ok, result);

    env.xa.xa_commit(&x, XaFlags::NOFLAGS).unwrap();
    assert!(env.exists(b"key"));
}

// ---------------------------------------------------------------------------
// testROPrepare
// ---------------------------------------------------------------------------

/// Port of `TwoPCTest.testROPrepare`: a transaction with no writes
/// returns `XA_RDONLY` from prepare.  Noxu encodes this as
/// `PrepareResult::ReadOnly`.
#[test]
fn tck_two_pc_test_ro_prepare() {
    let env = Harness::new();
    let x = xid("TwoPCTest1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    // No writes, no mark_write.
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    assert_eq!(PrepareResult::ReadOnly, result);
}

// ---------------------------------------------------------------------------
// testTwicePreparedTransaction
// ---------------------------------------------------------------------------

/// Port of `TwoPCTest.testTwicePreparedTransaction`: calling `prepare`
/// on an already-prepared transaction must be rejected.  After the error,
/// the original prepare survives and the branch can be committed.
#[test]
fn tck_two_pc_test_twice_prepared_transaction() {
    let env = Harness::new();
    let x = xid("TwoPCTest2");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"key", b"data");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();

    env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    // Second prepare must fail.
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS);
    assert!(
        result.is_err(),
        "second xa_prepare on Prepared branch must error, got {result:?}",
    );

    // After the (rejected) second prepare, the original prepare is still
    // valid and we can commit.
    env.xa.xa_commit(&x, XaFlags::NOFLAGS).unwrap();
    assert!(env.exists(b"key"));
}

// ---------------------------------------------------------------------------
// testRollbackNonExistent / testCommitNonExistent
// ---------------------------------------------------------------------------
//
// JE's testRollbackNonExistent and testCommitNonExistent are already
// covered by `xa_protocol_test::test_rollback_nonexistent_xid` and
// `xa_protocol_test::test_commit_nonexistent_xid`; the TSV is updated
// to point at those tests.  No new tests are added here.
