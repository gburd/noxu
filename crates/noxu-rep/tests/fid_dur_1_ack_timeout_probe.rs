//! fid_dur_1_ack_timeout_probe — FID-DUR-1 committed-at-master ack-timeout
//! coverage, kept alive under R-REPUSER composition.
//!
//! WHY THIS FILE EXISTS. R-REPUSER (BUG-REP-USER-WRITES-NOT-REPLICATED) makes a
//! user write on a data env wired to the REAL `noxu_rep::ReplicatedEnvironment`
//! coordinator fail closed AT THE WRITE OP (before any LN), because that
//! coordinator cannot replicate user writes in 7.11.x. That refusal fires
//! BEFORE the commit, so the FID-DUR-1 commit-time ack-timeout path
//! (`commit_with_durability` logs+fsyncs the TxnCommit, THEN waits for acks,
//! and on timeout surfaces a *committed-at-master* error with locks released
//! and `is_retryable() == false`) is UNREACHABLE through the real coordinator.
//!
//! That shipped code path still exists and must keep a witness. This test
//! reaches it with a TEST-DOUBLE `ReplicaAckCoordinator` that does NOT refuse
//! user writes (default `refuse_user_write_commit() == None`) but whose
//! `await_replica_acks` TIMES OUT. So the write commits locally (durable), the
//! ack wait fails, and the commit returns the committed-at-master ack-timeout
//! error — the exact FID-DUR-1 behaviour, independent of R-REPUSER.
//!
//! COMPOSITION NOTE (optb-coll-v2 base vs. +replay-v2):
//!
//! - On this branch's base (optb-coll-v2) the committed-at-master ack-timeout
//!   error is `NoxuError::InsufficientReplicas` (the ack wait runs after the
//!   local TxnCommit fsync; `is_retryable() == false`).
//! - replay-v2 (FID-DUR-1) renames that post-commit case to
//!   `NoxuError::InsufficientAcks` and adds `is_committed_at_master() == true`.
//!
//! The assertion below is written for THIS branch's base; the RC assembler,
//! when composing with replay-v2, swaps the error arm to `InsufficientAcks`
//! and adds `e.is_committed_at_master()` (see `repuser-failclosed.md`, the
//! composition table). Either way the invariants held are identical: the txn
//! is committed+durable at the master, locks are released, and the error is
//! NOT auto-retryable.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use noxu_db::durability::{Durability, ReplicaAckPolicy, SyncPolicy};
use noxu_db::error::NoxuError;
use noxu_db::{
    AckWaitError, AckWaitErrorKind, DatabaseConfig, Environment,
    EnvironmentConfig, ReplicaAckCoordinator, ReplicaAckPolicyKind,
};
use tempfile::TempDir;

/// A test-double coordinator that ALWAYS times out the ack wait, and does NOT
/// refuse user writes (default `refuse_user_write_commit() == None`). This is
/// the stand-in for "a master whose replicas never ack" — the FID-DUR-1 case
/// — without the R-REPUSER refusal (which only the real `ReplicatedEnvironment`
/// reports). Records the number of ack waits so the test proves the commit
/// path actually reached the ack wait (i.e. the local commit happened first).
struct TimingOutCoord {
    waits: AtomicU32,
}

impl ReplicaAckCoordinator for TimingOutCoord {
    fn await_replica_acks(
        &self,
        _policy: ReplicaAckPolicyKind,
        _timeout: Duration,
    ) -> std::result::Result<u32, AckWaitError> {
        self.waits.fetch_add(1, Ordering::SeqCst);
        // The policy needs an ack; none arrives → timeout. `received: 0`.
        Err(AckWaitError {
            kind: AckWaitErrorKind::Timeout,
            needed: 1,
            received: 0,
        })
    }
    // refuse_user_write_commit() uses the default (None): NOT an R-REPUSER
    // refusing coordinator, so the write is allowed to commit locally and the
    // ack wait runs — exactly the FID-DUR-1 path.
}

/// FID-DUR-1: a committed-at-master ack timeout. The write COMMITS locally
/// (durable), the ack wait times out, and the commit returns the
/// committed-at-master timeout error with locks released and
/// `is_retryable() == false`. This path is NOT reachable through the real
/// R-REPUSER-refusing `ReplicatedEnvironment`, so it needs its own witness.
#[test]
fn fid_dur_1_ack_timeout_is_committed_at_master_not_retryable() {
    let tmp = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let coord = Arc::new(TimingOutCoord { waits: AtomicU32::new(0) });
    env.set_replica_coordinator(coord.clone());
    env.set_replica_ack_timeout(Duration::from_millis(100));

    let txn = env.begin_transaction(None).unwrap();
    // A real data-logging write. The test-double coordinator does NOT refuse
    // (default None), so the write succeeds and is logged.
    db.put_in(&txn, b"k", b"v")
        .expect("a non-refusing coordinator must allow the write");
    let durability = Durability::new(
        SyncPolicy::Sync,
        SyncPolicy::Sync,
        ReplicaAckPolicy::SimpleMajority,
    );
    let started = Instant::now();
    let res = txn.commit_with_durability(durability);
    let elapsed = started.elapsed();

    // The commit reached the ack wait (local commit happened first). The
    // test-double returns the timeout error immediately — we assert the ack
    // wait was consulted, not its internal timing (that is the real
    // coordinator's concern, covered by `replica_ack_policy_test`).
    assert_eq!(
        coord.waits.load(Ordering::SeqCst),
        1,
        "commit_with_durability must consult the coordinator exactly once          (the local commit happened first, then the ack wait)"
    );
    let _ = elapsed;

    // ── committed-at-master ack-timeout error (FID-DUR-1) ──
    // On this branch's base (optb-coll-v2) that is `InsufficientReplicas`.
    // Under +replay-v2 the RC assembler swaps this arm to `InsufficientAcks`
    // and adds `assert!(e.is_committed_at_master())` (see repuser-failclosed.md).
    match &res {
        // COMPOSED-TREE ARM (+replay-v2 / FID-DUR-1): InsufficientAcks +
        // is_committed_at_master(). (RC-assembler swap of the base
        // InsufficientReplicas arm.)
        Err(e @ NoxuError::InsufficientAcks { required, available }) => {
            assert!(*required >= 1, "required acks must be > 0");
            assert_eq!(*available, 0, "no replica acked");
            assert!(
                e.is_committed_at_master(),
                "InsufficientAcks must report the txn committed at the master"
            );
            assert!(
                !e.is_retryable(),
                "a committed-at-master ack timeout must NOT be auto-retryable"
            );
        }
        other => panic!(
            "expected InsufficientAcks (committed at master; FID-DUR-1), \
             got {other:?} after {elapsed:?}"
        ),
    }

    // The txn is COMMITTED+DURABLE at the master despite the ack failure: the
    // value is visible locally now, and survives a close + reopen.
    assert_eq!(
        db.get(b"k").unwrap().as_deref(),
        Some(&b"v"[..]),
        "the write must be committed+visible at the master after the ack \
         timeout (committed-at-master)"
    );

    let _ = db.close();
    let _ = env.close();

    let reopen = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_transactional(true);
    let env2 = Environment::open(reopen).unwrap();
    let db2 = env2
        .open_database(
            None,
            "d",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(
        db2.get(b"k").unwrap().as_deref(),
        Some(&b"v"[..]),
        "the committed-at-master write must be durable after reopen"
    );
    let _ = db2.close();
    let _ = env2.close();
}

/// Control: locks are released after the committed-at-master ack timeout — the
/// txn did not leak a write lock (a second txn can read/overwrite the key).
/// `Environment::close` would also fail with a leaked lock, so a clean reopen
/// in the test above already proves release; this makes it explicit.
#[test]
fn fid_dur_1_ack_timeout_releases_locks() {
    let tmp = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let coord = Arc::new(TimingOutCoord { waits: AtomicU32::new(0) });
    env.set_replica_coordinator(coord);
    env.set_replica_ack_timeout(Duration::from_millis(50));

    let txn = env.begin_transaction(None).unwrap();
    db.put_in(&txn, b"k", b"v1").unwrap();
    let dur = Durability::new(
        SyncPolicy::Sync,
        SyncPolicy::Sync,
        ReplicaAckPolicy::SimpleMajority,
    );
    let _ = txn.commit_with_durability(dur); // committed-at-master, ack timeout

    // A fresh txn must be able to lock + overwrite the key — proving the first
    // txn's write lock was released on the committed-at-master path.
    let txn2 = env.begin_transaction(None).unwrap();
    db.put_in(&txn2, b"k", b"v2")
        .expect("the key's write lock must have been released");
    // Second txn has no coordinator-refusal and the double times out again, but
    // the write itself (lock acquisition) is what we are proving here.
    let _ = txn2.abort();

    let _ = db.close();
    let _ = env.close();
}
