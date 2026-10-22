//! ENG-DISKFULL-ACK-1 witness (7.11.x port): a transactional `commit()` on an
//! ALREADY-INVALIDATED environment MUST return `Err` — it may NEVER return Ok
//! (an acknowledged-but-not-durable commit).
//!
//! Root cause (traced on v8, identical code on 7.11.x): once a log write /
//! fdatasync error invalidates the environment (`LogManager::io_invalid`), a
//! transaction whose own write finds the log already invalid logs NOTHING, so
//! `Transaction::commit_with_durability`'s empty-txn fast path (gated on
//! `has_logged_data()`) skipped `write_txn_end` — the only env-validity check
//! on the path — and returned Ok.
//!
//! JE: `Transaction.commit()` calls `checkEnv()` -> `checkIfInvalid()` FIRST
//! (Transaction.java:331/889, EnvironmentImpl.java:1728), before the empty-txn
//! fast path (`if (updateLoggedForTxn())`, Txn.java:730/765), so a commit on an
//! invalidated env always throws.  The fix adds the same check to Noxu.
//!
//! Deterministic: FaultController::for_test fires DiskFull at a chosen write
//! index; no rv_adv oracle (v8-only) needed.
//!
//! RED on the unfixed engine (txn2 commit returns Ok); GREEN after the fix.
use noxu_db::{DatabaseConfig, Durability, Environment, EnvironmentConfig};
use noxu_log::faultdisk::{self, FaultController, FaultKind};
use std::sync::Mutex;

// faultdisk is process-global: serialise.
static GUARD: Mutex<()> = Mutex::new(());

fn cfg(p: &std::path::Path) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
}
fn dbcfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

#[test]
fn diskfull_commit_on_invalidated_env_returns_err() {
    let _serial = GUARD.lock().unwrap_or_else(|p| p.into_inner());
    faultdisk::uninstall();
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(cfg(dir.path())).unwrap();
    let db = env.open_database(None, "a", &dbcfg()).unwrap();
    db.put(b"seed", b"v").unwrap();
    // Flush + quiesce the global write counter so the fault lands on our own
    // txn1 commit path.
    env.flush_log(true).unwrap();

    // Install DiskFull at the very next posio write — txn1's commit drain
    // (its TxnCommit/LN flush) will hit ENOSPC and invalidate the env.
    let next = faultdisk::write_count();
    faultdisk::install(FaultController::for_test(FaultKind::DiskFull, next));

    // FIELD ORDERING: txn2 is BEGUN before any fault fires (as in the worker
    // loop), so `begin_transaction` succeeds; the env is invalidated by a
    // DIFFERENT (concurrent, here sequential) write AFTER txn2 began, and only
    // then does txn2 do its put + commit.  This is the exact window the field
    // trace showed: the lost op's txn pre-existed the invalidation.
    let t2 = env.begin_transaction(None).unwrap();

    // txn1: a SYNC put whose commit's fsync drain hits ENOSPC -> Err, env
    // invalidated.  (A careless app swallows the error.)
    let t1 = env.begin_transaction(None).unwrap();
    let _ = db.put_in(
        &t1,
        noxu_db::DatabaseEntry::from_bytes(b"k1"),
        noxu_db::DatabaseEntry::from_bytes(b"w3-5"),
    );
    let c1 = t1.commit_with_durability(Durability::COMMIT_SYNC);
    // txn1's commit must fail (its own write/fsync hit ENOSPC). The env MUST
    // now be invalid for the key assertion below.
    let env_invalid = !env.is_valid();

    // txn2: the LOST op in the field. Its put finds the log already invalid and
    // logs NOTHING; without the fix, commit() takes the empty-txn fast path and
    // returns Ok -> an acknowledged commit that never reached disk.
    let _ = db.put_in(
        &t2,
        noxu_db::DatabaseEntry::from_bytes(b"k2"),
        noxu_db::DatabaseEntry::from_bytes(b"w3-6"),
    );
    let c2 = t2.commit_with_durability(Durability::COMMIT_SYNC);

    faultdisk::uninstall();

    assert!(
        env_invalid,
        "witness precondition: env should be invalidated after txn1's ENOSPC \
         commit (c1={c1:?})"
    );
    assert!(
        c2.is_err(),
        "ENG-DISKFULL-ACK-1: commit() on an invalidated env returned Ok \
         (acknowledged-but-not-durable commit) — JE Transaction.checkEnv \
         throws here. c1={c1:?} c2={c2:?}"
    );
    let _ = env.close();
}
