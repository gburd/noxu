//! XA Protocol Corner-Case Test Suite
//!
//! An adversarial Transaction Manager that systematically drives every state
//! transition, error path, and edge case in the XA implementation. Unlike the
//! chaos test (random exploration), this suite deterministically verifies each
//! protocol rule from the X/Open XA spec.
//!
//! ## Protocol Rules Tested
//!
//! 1. State machine transitions (valid and invalid)
//! 2. Flag combinations and interactions
//! 3. Multi-branch same-gtrid coordination
//! 4. Concurrent branch interleaving
//! 5. Recovery semantics
//! 6. Error propagation and cleanup

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use noxu_xa::{
    PrepareResult, XaEnvironment, XaError, XaFlags, XaResource, Xid, XidError,
};
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Test infrastructure
// ─────────────────────────────────────────────────────────────────────────────

struct TestEnv {
    xa: XaEnvironment,
    db: Database,
    _dir: TempDir,
}

impl TestEnv {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "protocol_test", &db_config).unwrap();
        let xa = XaEnvironment::new(env);
        Self { xa, db, _dir: dir }
    }

    fn put(&self, xid: &Xid, key: &[u8], val: &[u8]) {
        let txn = self.xa.get_transaction(xid).unwrap();
        let k = DatabaseEntry::from_bytes(key);
        let v = DatabaseEntry::from_bytes(val);
        self.db.put_in(&txn, &k, &v).unwrap();
        self.xa.mark_write(xid).unwrap();
    }

    fn exists(&self, key: &[u8]) -> bool {
        let k = DatabaseEntry::from_bytes(key);
        let mut v = DatabaseEntry::new();
        self.db.get_into(None, &k, &mut v).unwrap()
    }
}

fn xid(gtrid: &str, bqual: &str) -> Xid {
    Xid::new(1, gtrid.as_bytes(), bqual.as_bytes()).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. State Machine — Valid Transitions
// ─────────────────────────────────────────────────────────────────────────────

/// Nonexistent → Active (xa_start with NOFLAGS)
#[test]
fn test_state_nonexistent_to_active() {
    let env = TestEnv::new();
    let x = xid("g1", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    // Verify we can get the transaction (proves Active state)
    let _txn = env.xa.get_transaction(&x).unwrap();
    // Cleanup
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Active → Idle (xa_end with TMSUCCESS)
#[test]
fn test_state_active_to_idle() {
    let env = TestEnv::new();
    let x = xid("g2", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    // Now in Idle — prepare should work
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    assert_eq!(result, PrepareResult::ReadOnly); // no writes
}

/// Active → Suspended (xa_end with TMSUSPEND)
#[test]
fn test_state_active_to_suspended() {
    let env = TestEnv::new();
    let x = xid("g3", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUSPEND).unwrap();
    // get_transaction should fail (not Active)
    assert!(env.xa.get_transaction(&x).is_err());
    // Cleanup
    env.xa.xa_start(&x, XaFlags::RESUME).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Active → RollbackOnly (xa_end with TMFAIL)
#[test]
fn test_state_active_to_rollback_only() {
    let env = TestEnv::new();
    let x = xid("g4", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"rbo_key", b"rbo_val");
    env.xa.xa_end(&x, XaFlags::TMFAIL).unwrap();
    // Must rollback — prepare is forbidden
    assert!(env.xa.xa_prepare(&x, XaFlags::NOFLAGS).is_err());
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
    assert!(!env.exists(b"rbo_key"));
}

/// Suspended → Active (xa_start with RESUME)
#[test]
fn test_state_suspended_to_active() {
    let env = TestEnv::new();
    let x = xid("g5", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUSPEND).unwrap();
    env.xa.xa_start(&x, XaFlags::RESUME).unwrap();
    // Should be Active again
    let _txn = env.xa.get_transaction(&x).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Idle → Prepared (xa_prepare with writes)
#[test]
fn test_state_idle_to_prepared() {
    let env = TestEnv::new();
    let x = xid("g6", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"prep_key", b"prep_val");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    assert_eq!(result, PrepareResult::Ok);
    // Recovery should show this
    let recovered = env.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert_eq!(recovered.len(), 1);
    env.xa.xa_commit(&x, XaFlags::NOFLAGS).unwrap();
}

/// Prepared → committed (xa_commit)
#[test]
fn test_state_prepared_to_committed() {
    let env = TestEnv::new();
    let x = xid("g7", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"commit_key", b"commit_val");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_commit(&x, XaFlags::NOFLAGS).unwrap();
    assert!(env.exists(b"commit_key"));
    // Branch removed — recover should be empty
    let recovered = env.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty());
}

/// Prepared → rolled back (xa_rollback)
#[test]
fn test_state_prepared_to_rolled_back() {
    let env = TestEnv::new();
    let x = xid("g8", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"rb_key", b"rb_val");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
    assert!(!env.exists(b"rb_key"));
}

/// Idle → committed via one-phase (xa_commit ONEPHASE)
#[test]
fn test_state_idle_to_committed_onephase() {
    let env = TestEnv::new();
    let x = xid("g9", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"1pc_key", b"1pc_val");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_commit(&x, XaFlags::ONEPHASE).unwrap();
    assert!(env.exists(b"1pc_key"));
}

/// Idle → rolled back (xa_rollback from Idle, no prepare needed)
#[test]
fn test_state_idle_to_rolled_back() {
    let env = TestEnv::new();
    let x = xid("g10", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"idle_rb_key", b"val");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
    assert!(!env.exists(b"idle_rb_key"));
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. State Machine — Invalid Transitions (must return Protocol error)
// ─────────────────────────────────────────────────────────────────────────────

/// Cannot xa_end on non-active branch
#[test]
fn test_invalid_end_on_idle() {
    let env = TestEnv::new();
    let x = xid("inv1", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    // Now Idle — second xa_end should fail
    let result = env.xa.xa_end(&x, XaFlags::TMSUCCESS);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot xa_prepare on Active branch (must xa_end first)
#[test]
fn test_invalid_prepare_on_active() {
    let env = TestEnv::new();
    let x = xid("inv2", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot xa_prepare on Suspended branch
#[test]
fn test_invalid_prepare_on_suspended() {
    let env = TestEnv::new();
    let x = xid("inv3", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUSPEND).unwrap();
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    // Cleanup
    env.xa.xa_start(&x, XaFlags::RESUME).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot xa_prepare on RollbackOnly
#[test]
fn test_invalid_prepare_on_rollback_only() {
    let env = TestEnv::new();
    let x = xid("inv4", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMFAIL).unwrap();
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot xa_commit (non-ONEPHASE) on Idle branch
#[test]
fn test_invalid_commit_on_idle() {
    let env = TestEnv::new();
    let x = xid("inv5", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"inv5_key", b"val");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    // Attempt 2PC commit without prepare
    let result = env.xa.xa_commit(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot xa_commit ONEPHASE on Prepared branch
#[test]
fn test_invalid_onephase_on_prepared() {
    let env = TestEnv::new();
    let x = xid("inv6", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"inv6_key", b"val");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    // ONEPHASE expects Idle, not Prepared
    let result = env.xa.xa_commit(&x, XaFlags::ONEPHASE);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot xa_rollback on Active branch
#[test]
fn test_invalid_rollback_on_active() {
    let env = TestEnv::new();
    let x = xid("inv7", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    let result = env.xa.xa_rollback(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot xa_rollback on Suspended branch
#[test]
fn test_invalid_rollback_on_suspended() {
    let env = TestEnv::new();
    let x = xid("inv8", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUSPEND).unwrap();
    let result = env.xa.xa_rollback(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_start(&x, XaFlags::RESUME).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot RESUME a non-suspended branch
#[test]
fn test_invalid_resume_on_active() {
    let env = TestEnv::new();
    let x = xid("inv9", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    // Branch is Active, not Suspended — RESUME should fail
    let result = env.xa.xa_start(&x, XaFlags::RESUME);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// Cannot JOIN a non-active branch
#[test]
fn test_invalid_join_on_idle() {
    let env = TestEnv::new();
    let x = xid("inv10", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    // Branch is Idle — JOIN should fail
    let result = env.xa.xa_start(&x, XaFlags::JOIN);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Operations on Nonexistent XID
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_end_nonexistent_xid() {
    let env = TestEnv::new();
    let x = xid("ghost", "b1");
    let result = env.xa.xa_end(&x, XaFlags::TMSUCCESS);
    assert!(matches!(result, Err(XaError::NotFound)));
}

#[test]
fn test_prepare_nonexistent_xid() {
    let env = TestEnv::new();
    let x = xid("ghost", "b2");
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::NotFound)));
}

#[test]
fn test_commit_nonexistent_xid() {
    let env = TestEnv::new();
    let x = xid("ghost", "b3");
    let result = env.xa.xa_commit(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::NotFound)));
}

#[test]
fn test_rollback_nonexistent_xid() {
    let env = TestEnv::new();
    let x = xid("ghost", "b4");
    let result = env.xa.xa_rollback(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::NotFound)));
}

#[test]
fn test_forget_nonexistent_xid() {
    let env = TestEnv::new();
    let x = xid("ghost", "b5");
    let result = env.xa.xa_forget(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::NotFound)));
}

#[test]
fn test_get_transaction_nonexistent_xid() {
    let env = TestEnv::new();
    let x = xid("ghost", "b6");
    let result = env.xa.get_transaction(&x);
    assert!(matches!(result, Err(XaError::NotFound)));
}

#[test]
fn test_mark_write_nonexistent_xid() {
    let env = TestEnv::new();
    let x = xid("ghost", "b7");
    let result = env.xa.mark_write(&x);
    assert!(matches!(result, Err(XaError::NotFound)));
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Duplicate XID
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_duplicate_xid_rejected() {
    let env = TestEnv::new();
    let x = xid("dup", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    let result = env.xa.xa_start(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::DuplicateXid)));
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// After commit, same XID can be reused
#[test]
fn test_xid_reuse_after_commit() {
    let env = TestEnv::new();
    let x = xid("reuse", "b1");

    // First use
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"reuse_k1", b"v1");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_commit(&x, XaFlags::ONEPHASE).unwrap();

    // Reuse same XID
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"reuse_k2", b"v2");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_commit(&x, XaFlags::ONEPHASE).unwrap();

    assert!(env.exists(b"reuse_k1"));
    assert!(env.exists(b"reuse_k2"));
}

/// After rollback, same XID can be reused
#[test]
fn test_xid_reuse_after_rollback() {
    let env = TestEnv::new();
    let x = xid("reuse_rb", "b1");

    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"rb_reuse_k", b"v1");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
    assert!(!env.exists(b"rb_reuse_k"));

    // Reuse
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"rb_reuse_k", b"v2");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_commit(&x, XaFlags::ONEPHASE).unwrap();
    assert!(env.exists(b"rb_reuse_k"));
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Multi-Branch Same Global Transaction
// ─────────────────────────────────────────────────────────────────────────────

/// Two branches (same gtrid, different bqual) are independent
#[test]
fn test_multi_branch_independent() {
    let env = TestEnv::new();
    let b1 = xid("global_1", "branch_A");
    let b2 = xid("global_1", "branch_B");

    env.xa.xa_start(&b1, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_start(&b2, XaFlags::NOFLAGS).unwrap();

    env.put(&b1, b"mb_k1", b"from_A");
    env.put(&b2, b"mb_k2", b"from_B");

    env.xa.xa_end(&b1, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_end(&b2, XaFlags::TMSUCCESS).unwrap();

    // Commit b1, rollback b2
    env.xa.xa_prepare(&b1, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_commit(&b1, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_rollback(&b2, XaFlags::NOFLAGS).unwrap();

    assert!(env.exists(b"mb_k1"));
    assert!(!env.exists(b"mb_k2"));
}

/// Commit both branches of same global transaction
#[test]
fn test_multi_branch_both_commit() {
    let env = TestEnv::new();
    let b1 = xid("global_2", "branch_X");
    let b2 = xid("global_2", "branch_Y");

    env.xa.xa_start(&b1, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_start(&b2, XaFlags::NOFLAGS).unwrap();

    env.put(&b1, b"mb2_k1", b"X_val");
    env.put(&b2, b"mb2_k2", b"Y_val");

    env.xa.xa_end(&b1, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_end(&b2, XaFlags::TMSUCCESS).unwrap();

    // Both prepare then commit
    assert_eq!(
        env.xa.xa_prepare(&b1, XaFlags::NOFLAGS).unwrap(),
        PrepareResult::Ok
    );
    assert_eq!(
        env.xa.xa_prepare(&b2, XaFlags::NOFLAGS).unwrap(),
        PrepareResult::Ok
    );
    env.xa.xa_commit(&b1, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_commit(&b2, XaFlags::NOFLAGS).unwrap();

    assert!(env.exists(b"mb2_k1"));
    assert!(env.exists(b"mb2_k2"));
}

/// Recover returns all prepared branches of a global transaction
#[test]
fn test_multi_branch_recover() {
    let env = TestEnv::new();
    let b1 = xid("global_3", "branch_1");
    let b2 = xid("global_3", "branch_2");
    let b3 = xid("global_3", "branch_3");

    for (i, b) in [&b1, &b2, &b3].iter().enumerate() {
        env.xa.xa_start(b, XaFlags::NOFLAGS).unwrap();
        env.put(b, format!("mb3_{i}").as_bytes(), b"v");
        env.xa.xa_end(b, XaFlags::TMSUCCESS).unwrap();
        env.xa.xa_prepare(b, XaFlags::NOFLAGS).unwrap();
    }

    let recovered = env.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert_eq!(recovered.len(), 3);

    // Commit all
    for b in [&b1, &b2, &b3] {
        env.xa.xa_commit(b, XaFlags::NOFLAGS).unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Suspend/Resume Edge Cases
// ─────────────────────────────────────────────────────────────────────────────

/// Multiple suspend/resume cycles preserve transaction state
#[test]
fn test_multiple_suspend_resume_cycles() {
    let env = TestEnv::new();
    let x = xid("cycles", "b1");

    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();

    for i in 0..5 {
        env.put(&x, format!("cycle_k{i}").as_bytes(), b"val");
        env.xa.xa_end(&x, XaFlags::TMSUSPEND).unwrap();
        // Branch is suspended — cannot access transaction
        assert!(env.xa.get_transaction(&x).is_err());
        env.xa.xa_start(&x, XaFlags::RESUME).unwrap();
    }

    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_commit(&x, XaFlags::ONEPHASE).unwrap();

    for i in 0..5 {
        assert!(env.exists(format!("cycle_k{i}").as_bytes()));
    }
}

/// Cannot RESUME from Idle state
#[test]
fn test_cannot_resume_from_idle() {
    let env = TestEnv::new();
    let x = xid("no_resume", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    // In Idle, not Suspended — RESUME should fail
    let result = env.xa.xa_start(&x, XaFlags::RESUME);
    assert!(matches!(result, Err(XaError::Protocol(_))));
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
}

/// TMSUSPEND then TMFAIL (end with fail after suspend+resume)
#[test]
fn test_suspend_then_fail() {
    let env = TestEnv::new();
    let x = xid("susp_fail", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"sf_key", b"sf_val");
    env.xa.xa_end(&x, XaFlags::TMSUSPEND).unwrap();
    env.xa.xa_start(&x, XaFlags::RESUME).unwrap();
    env.xa.xa_end(&x, XaFlags::TMFAIL).unwrap();
    // Now RollbackOnly
    assert!(env.xa.xa_prepare(&x, XaFlags::NOFLAGS).is_err());
    env.xa.xa_rollback(&x, XaFlags::NOFLAGS).unwrap();
    assert!(!env.exists(b"sf_key"));
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. JOIN flag
// ─────────────────────────────────────────────────────────────────────────────

/// JOIN allows another caller to participate in an Active branch
#[test]
fn test_join_active_branch() {
    let env = TestEnv::new();
    let x = xid("join_test", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    // JOIN on active branch succeeds
    env.xa.xa_start(&x, XaFlags::JOIN).unwrap();
    // Can still use transaction
    env.put(&x, b"join_key", b"join_val");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_commit(&x, XaFlags::ONEPHASE).unwrap();
    assert!(env.exists(b"join_key"));
}

/// JOIN on nonexistent XID fails with NotFound
#[test]
fn test_join_nonexistent() {
    let env = TestEnv::new();
    let x = xid("no_join", "b1");
    let result = env.xa.xa_start(&x, XaFlags::JOIN);
    assert!(matches!(result, Err(XaError::NotFound)));
}

/// RESUME on nonexistent XID fails with NotFound
#[test]
fn test_resume_nonexistent() {
    let env = TestEnv::new();
    let x = xid("no_resume2", "b1");
    let result = env.xa.xa_start(&x, XaFlags::RESUME);
    assert!(matches!(result, Err(XaError::NotFound)));
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Read-Only Optimization
// ─────────────────────────────────────────────────────────────────────────────

/// Read-only branch returns ReadOnly and is auto-removed
#[test]
fn test_readonly_branch_auto_removed() {
    let env = TestEnv::new();
    let x = xid("readonly", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    // No writes
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    assert_eq!(result, PrepareResult::ReadOnly);
    // Branch is gone — commit should fail
    let result = env.xa.xa_commit(&x, XaFlags::NOFLAGS);
    assert!(matches!(result, Err(XaError::NotFound)));
}

/// Read-only branch with get (read) but no put still counts as read-only
#[test]
fn test_readonly_with_reads() {
    let env = TestEnv::new();

    // Pre-populate a key
    let setup_xid = xid("setup", "b1");
    env.xa.xa_start(&setup_xid, XaFlags::NOFLAGS).unwrap();
    env.put(&setup_xid, b"preexist", b"value");
    env.xa.xa_end(&setup_xid, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_commit(&setup_xid, XaFlags::ONEPHASE).unwrap();

    // Read-only branch: get but no put
    let x = xid("ro_read", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    {
        let txn = env.xa.get_transaction(&x).unwrap();
        let key = DatabaseEntry::from_bytes(b"preexist");
        let mut val = DatabaseEntry::new();
        let _status = env.db.get_into(Some(&txn), &key, &mut val).unwrap();
    }
    // Don't call mark_write
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    let result = env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    assert_eq!(result, PrepareResult::ReadOnly);
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. xa_forget
// ─────────────────────────────────────────────────────────────────────────────

/// xa_forget removes a prepared branch without commit or rollback
#[test]
fn test_forget_prepared_branch() {
    let env = TestEnv::new();
    let x = xid("forget_me", "b1");
    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"forget_k", b"forget_v");
    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();

    // Forget it (simulating heuristic resolution by admin)
    env.xa.xa_forget(&x, XaFlags::NOFLAGS).unwrap();

    // No longer in recover list
    let recovered = env.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty());

    // Cannot commit or rollback
    assert!(matches!(
        env.xa.xa_commit(&x, XaFlags::NOFLAGS),
        Err(XaError::NotFound)
    ));
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. Xid Validation
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_xid_max_gtrid_length() {
    // 64 bytes is OK
    let gtrid = vec![b'x'; 64];
    let result = Xid::new(1, &gtrid, b"bq");
    assert!(result.is_ok());
}

#[test]
fn test_xid_exceeds_max_gtrid_length() {
    let gtrid = vec![b'x'; 65];
    let result = Xid::new(1, &gtrid, b"bq");
    assert!(matches!(result, Err(XidError::GtridTooLong(_))));
}

#[test]
fn test_xid_max_bqual_length() {
    let bqual = vec![b'y'; 64];
    let result = Xid::new(1, b"gt", &bqual);
    assert!(result.is_ok());
}

#[test]
fn test_xid_exceeds_max_bqual_length() {
    let bqual = vec![b'y'; 65];
    let result = Xid::new(1, b"gt", &bqual);
    assert!(matches!(result, Err(XidError::BqualTooLong(_))));
}

#[test]
fn test_xid_empty_components_valid() {
    // Empty gtrid/bqual are valid per XA spec (though unusual)
    let result = Xid::new(1, b"", b"");
    assert!(result.is_ok());
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. Concurrent Access — Thread Safety
// ─────────────────────────────────────────────────────────────────────────────

/// Multiple threads operating on different XIDs simultaneously
///
/// This test was previously `#[ignore]`d because of a real concurrent-
/// commit lost-write bug in `noxu-tree::Tree::insert`: the first-key
/// path checked `self.root.read().is_none()` then promoted to a write
/// lock, but the read-then-write pattern was a TOCTOU race — N threads
/// could all observe an empty tree, each build a fresh single-entry
/// root, and the last `*self.root.write() = Some(...)` silently
/// discarded the others. With 8 concurrent first-time inserts on an
/// empty tree, ~30% of runs lost data. Fixed by holding the write
/// lock across the is_none check and the root replacement; verified
/// 200/200 stable runs.
#[test]
fn test_concurrent_independent_xids() {
    let env = std::sync::Arc::new(TestEnv::new());
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));

    let handles: Vec<_> = (0..8)
        .map(|tid| {
            let env = std::sync::Arc::clone(&env);
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                let x = Xid::new(1, format!("conc_g{tid}").as_bytes(), b"b1")
                    .unwrap();
                barrier.wait();

                env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
                {
                    let txn = env.xa.get_transaction(&x).unwrap();
                    let key = DatabaseEntry::from_vec(
                        format!("conc_k{tid}").into_bytes(),
                    );
                    let val = DatabaseEntry::from_bytes(b"conc_val");
                    env.db.put_in(&txn, &key, &val).unwrap();
                    env.xa.mark_write(&x).unwrap();
                }
                env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
                env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
                env.xa.xa_commit(&x, XaFlags::NOFLAGS).unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all committed
    for tid in 0..8 {
        assert!(env.exists(format!("conc_k{tid}").as_bytes()));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. Recovery — xa_recover correctness
// ─────────────────────────────────────────────────────────────────────────────

/// Recover returns only Prepared branches, not Active/Idle/RollbackOnly
#[test]
fn test_recover_only_prepared() {
    let env = TestEnv::new();

    // Active branch
    let active = xid("rec_active", "b1");
    env.xa.xa_start(&active, XaFlags::NOFLAGS).unwrap();

    // Idle branch
    let idle = xid("rec_idle", "b1");
    env.xa.xa_start(&idle, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&idle, XaFlags::TMSUCCESS).unwrap();

    // RollbackOnly branch
    let rbo = xid("rec_rbo", "b1");
    env.xa.xa_start(&rbo, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_end(&rbo, XaFlags::TMFAIL).unwrap();

    // Prepared branch
    let prepared = xid("rec_prepared", "b1");
    env.xa.xa_start(&prepared, XaFlags::NOFLAGS).unwrap();
    env.put(&prepared, b"rec_k", b"rec_v");
    env.xa.xa_end(&prepared, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_prepare(&prepared, XaFlags::NOFLAGS).unwrap();

    let recovered = env.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0], prepared);

    // Cleanup
    env.xa.xa_end(&active, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_rollback(&active, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_rollback(&idle, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_rollback(&rbo, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_commit(&prepared, XaFlags::NOFLAGS).unwrap();
}

/// Empty recover on fresh environment
#[test]
fn test_recover_empty() {
    let env = TestEnv::new();
    let recovered = env.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// 13. Data Isolation — Uncommitted data not visible
// ─────────────────────────────────────────────────────────────────────────────

/// Data written in XA branch is isolated from other readers until commit.
///
/// In Noxu's lock-based isolation model (like JE), writes go to the BIN
/// immediately but readers block on the write lock. With no_wait or timeout,
/// the reader gets a lock error — proving the data is isolated.
#[test]
fn test_uncommitted_data_isolated_until_commit() {
    let env = TestEnv::new();
    let x = xid("isolation", "b1");

    env.xa.xa_start(&x, XaFlags::NOFLAGS).unwrap();
    env.put(&x, b"iso_key", b"iso_val");

    // Outside the transaction, reading the key should fail with a lock error
    // (the write lock blocks the reader — data is isolated)
    let k = DatabaseEntry::from_bytes(b"iso_key");
    let mut v = DatabaseEntry::new();
    let result = env.db.get_into(None, &k, &mut v);
    assert!(result.is_err(), "expected lock error, got: {result:?}");

    env.xa.xa_end(&x, XaFlags::TMSUCCESS).unwrap();
    env.xa.xa_prepare(&x, XaFlags::NOFLAGS).unwrap();
    env.xa.xa_commit(&x, XaFlags::NOFLAGS).unwrap();

    // NOW visible (no lock conflict)
    assert!(env.exists(b"iso_key"));
}
