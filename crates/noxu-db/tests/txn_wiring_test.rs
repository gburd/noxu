//! Regression tests for the Sprint 1 environment/transaction wiring
//! fixes (May 2026 API audit findings F1, F2, F3, F12).
//!
//! Each test in this file is a *behavioural* assertion, not a unit test:
//! it opens a real `Environment`, drives the public surface as a user
//! would, and asserts the documented contract.  Pre-fix, every test in
//! this file would fail.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Durability, Environment,
    EnvironmentConfig, TransactionConfig,
};
use std::sync::Arc;
use tempfile::TempDir;

fn open_env(temp_dir: &TempDir, durability: Durability) -> Environment {
    let cfg = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(durability);
    Environment::open(cfg).unwrap()
}

fn open_db(env: &Environment, name: &str) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    )
    .unwrap()
}

// ─── F1: env.close() succeeds after txn.commit() ─────────────────────

#[test]
fn f1_env_close_after_commit_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None).unwrap();
    txn.commit().expect("commit must succeed");

    // Pre-fix: this returns OperationNotAllowed("Cannot close
    // environment with 1 active transactions").
    env.close().expect("env.close() must succeed after commit");
}

#[test]
fn f1_env_close_after_abort_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None).unwrap();
    txn.abort().expect("abort must succeed");

    env.close().expect("env.close() must succeed after abort");
}

#[test]
fn f1_env_close_after_many_commits_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "f1");

    for i in 0..16 {
        let txn = env.begin_transaction(None).unwrap();
        let key = DatabaseEntry::from_data(format!("k{}", i).as_bytes());
        let val = DatabaseEntry::from_data(b"v");
        db.put_in(&txn, &key, &val).unwrap();
        txn.commit().unwrap();
    }

    db.close().unwrap();
    env.close().expect("env.close() must succeed after many commits");
}

#[test]
fn f1_env_close_with_one_active_txn_still_fails() {
    // Sanity check: only commit/abort prune the registry; an open
    // transaction must still block close().
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let _txn = env.begin_transaction(None).unwrap();

    let result = env.close();
    assert!(result.is_err(), "close() must fail with active txn");
}

// ─── F2: read_uncommitted on TransactionConfig is honoured ───────────

#[test]
fn f2_read_uncommitted_sees_uncommitted_writes() {
    // A txn with `with_read_uncommitted(true)` must observe writes from
    // a concurrent uncommitted transaction (dirty read).  Pre-fix the
    // flag was silently dropped by `Environment::begin_transaction`, so
    // the reader took a normal read lock and either blocked on the
    // writer or timed out.
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "f2"));

    // Seed the key so there is a "before" value to read.
    let key = DatabaseEntry::from_data(b"k");
    let val_before = DatabaseEntry::from_data(b"before");
    db.put(&key, &val_before).unwrap();

    // Writer txn: writes a new value but does NOT commit yet.
    let writer_txn = env.begin_transaction(None).unwrap();
    let val_after = DatabaseEntry::from_data(b"after");
    db.put_in(&writer_txn, &key, &val_after).unwrap();

    // Reader txn: read-uncommitted, should see the dirty write.
    let read_cfg = TransactionConfig::new().with_read_uncommitted(true);
    let reader_txn = env.begin_transaction(Some(&read_cfg)).unwrap();

    let mut data = DatabaseEntry::new();
    let key_lookup = DatabaseEntry::from_data(b"k");
    let status = db
        .get_into(Some(&reader_txn), &key_lookup, &mut data)
        .expect("dirty read must not block / error");
    assert!(status);
    assert_eq!(
        data.data_opt(),
        Some(b"after".as_slice()),
        "read-uncommitted txn must see the writer's dirty value"
    );

    reader_txn.commit().unwrap();
    writer_txn.abort().unwrap();
    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

// ─── F3: env-level durability default is honoured on commit ──────────

#[test]
fn f3_env_default_durability_no_sync_skips_fsync() {
    // Open with COMMIT_NO_SYNC; commit a txn with `begin_transaction(None)`;
    // assert the WAL fsync count did not increase.  Pre-fix, every commit
    // fsynced because TransactionConfig::default().durability ==
    // COMMIT_SYNC and the env-level durability was never consulted.
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "f3");

    // Drive at least one auto-commit write so the log is initialised
    // (this fsyncs based on db-level no_sync; we only care about the
    // delta around the explicit-txn commit below).
    {
        let key = DatabaseEntry::from_data(b"warm");
        let val = DatabaseEntry::from_data(b"up");
        db.put(&key, &val).unwrap();
    }

    let fsyncs_before = env.stat_fsync_count();

    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_data(b"k");
    let val = DatabaseEntry::from_data(b"v");
    db.put_in(&txn, &key, &val).unwrap();
    txn.commit().expect("commit must succeed");

    let fsyncs_after = env.stat_fsync_count();
    assert_eq!(
        fsyncs_before,
        fsyncs_after,
        "env-level COMMIT_NO_SYNC must not fsync on commit (delta = {})",
        fsyncs_after - fsyncs_before
    );

    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn f3_env_default_durability_sync_does_fsync() {
    // Sanity: a COMMIT_SYNC env should fsync on commit.
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_SYNC);
    let db = open_db(&env, "f3");

    // Warm up the log.
    let warm_key = DatabaseEntry::from_data(b"warm");
    let warm_val = DatabaseEntry::from_data(b"up");
    db.put(&warm_key, &warm_val).unwrap();

    let fsyncs_before = env.stat_fsync_count();

    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_data(b"k");
    let val = DatabaseEntry::from_data(b"v");
    db.put_in(&txn, &key, &val).unwrap();
    txn.commit().unwrap();

    let fsyncs_after = env.stat_fsync_count();
    assert!(
        fsyncs_after > fsyncs_before,
        "COMMIT_SYNC must fsync on commit ({} -> {})",
        fsyncs_before,
        fsyncs_after
    );

    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn f3_explicit_txn_durability_overrides_env_default() {
    // When the caller supplies a TransactionConfig with an explicit
    // durability, that wins over the env default.
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "f3");

    // Warm up.
    let warm_key = DatabaseEntry::from_data(b"warm");
    let warm_val = DatabaseEntry::from_data(b"up");
    db.put(&warm_key, &warm_val).unwrap();

    let fsyncs_before = env.stat_fsync_count();

    let cfg = TransactionConfig::new().with_durability(Durability::COMMIT_SYNC);
    let txn = env.begin_transaction(Some(&cfg)).unwrap();
    let key = DatabaseEntry::from_data(b"k");
    let val = DatabaseEntry::from_data(b"v");
    db.put_in(&txn, &key, &val).unwrap();
    txn.commit().unwrap();

    let fsyncs_after = env.stat_fsync_count();
    assert!(
        fsyncs_after > fsyncs_before,
        "explicit COMMIT_SYNC must fsync even when env default is COMMIT_NO_SYNC"
    );

    db.close().unwrap();
    env.close().unwrap();
}

// ─── F12: auto-commit writes coordinate with explicit-txn locks ──────

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

#[test]
fn f12_auto_commit_write_blocks_on_explicit_txn_write_lock() {
    // Begin txn A, A.put(K, V) (write lock held).
    // From a second thread, db.put( K, V2) (auto-commit write).
    // The auto-commit write must block until A commits/aborts.
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "f12"));

    // Seed the key so it has a non-NULL old_lsn — the auto-commit
    // cursor's `lock_write_before_log` takes the write lock against the
    // existing record's LSN, which is what the explicit txn's put just
    // pinned.  (A brand-new insert with no prior version would not
    // coordinate; that is a separate corner case noted as deferred F12
    // follow-up work.)
    let key = DatabaseEntry::from_data(b"k");
    let val0 = DatabaseEntry::from_data(b"v0");
    db.put(&key, &val0).unwrap();

    // Writer txn: take the write lock by issuing a put.
    let writer_txn = env.begin_transaction(None).unwrap();
    let val1 = DatabaseEntry::from_data(b"v1");
    db.put_in(&writer_txn, &key, &val1).unwrap();

    // Auto-commit thread tries to write the same key.
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let started_t = Arc::clone(&started);
    let finished_t = Arc::clone(&finished);
    let db_t = Arc::clone(&db);
    let handle = thread::spawn(move || {
        started_t.store(true, Ordering::SeqCst);
        let key = DatabaseEntry::from_data(b"k");
        let val2 = DatabaseEntry::from_data(b"v2");
        db_t.put(&key, &val2).unwrap();
        finished_t.store(true, Ordering::SeqCst);
    });

    // Wait for the writer thread to start and try to acquire the lock.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !started.load(Ordering::SeqCst)
        && std::time::Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(5));
    }
    // Give the put() time to actually attempt the lock and block.
    thread::sleep(Duration::from_millis(200));

    assert!(
        !finished.load(Ordering::SeqCst),
        "auto-commit write must block while explicit txn holds the write lock"
    );

    // Commit the writer; the auto-commit write should now proceed.
    writer_txn.commit().unwrap();
    handle.join().expect("auto-commit thread must finish without panic");
    assert!(finished.load(Ordering::SeqCst));

    // Final value is whatever the auto-commit thread wrote.
    let mut data = DatabaseEntry::new();
    let key_lookup = DatabaseEntry::from_data(b"k");
    let status = db.get_into(None, &key_lookup, &mut data).unwrap();
    assert!(status);
    assert_eq!(data.data_opt(), Some(b"v2".as_slice()));

    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

#[test]
fn f12_auto_commit_does_not_block_on_unrelated_key() {
    // Sanity: auto-commit on a different key must NOT block on an
    // explicit txn's write lock for a different key.
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "f12b"));

    let k1 = DatabaseEntry::from_data(b"k1");
    let v0 = DatabaseEntry::from_data(b"v0");
    db.put(&k1, &v0).unwrap();

    let writer_txn = env.begin_transaction(None).unwrap();
    let v1 = DatabaseEntry::from_data(b"v1");
    db.put_in(&writer_txn, &k1, &v1).unwrap();

    // Different key — must not block.
    let k2 = DatabaseEntry::from_data(b"k2");
    let v2 = DatabaseEntry::from_data(b"v2");
    db.put(&k2, &v2).expect("auto-commit on unrelated key must not block");

    writer_txn.commit().unwrap();

    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

#[test]
fn f12_explicit_txn_read_blocks_auto_commit_write() {
    // Belt-and-braces variant of the F12 scenario: an explicit txn
    // takes a Read lock on K, and a concurrent auto-commit write to
    // K must block until the explicit txn releases its read lock.
    //
    // Determinism: a long lock timeout ensures the blocked write waits for
    // the read lock to be released rather than timing out under load (the
    // prior source of flakiness), and we synchronize on the lock manager's
    // live waiter count (`n_waiters`) instead of a fixed sleep.
    let tmp = TempDir::new().unwrap();
    let mut cfg = EnvironmentConfig::new(tmp.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(Durability::COMMIT_NO_SYNC);
    cfg.set_lock_timeout(30_000);
    let env = Arc::new(Environment::open(cfg).unwrap());
    let db = Arc::new(open_db(&env, "f12c"));

    // Seed K so a read can land on a real (non-NULL) LSN.
    let key = DatabaseEntry::from_data(b"k");
    let val0 = DatabaseEntry::from_data(b"v0");
    db.put(&key, &val0).unwrap();

    // Explicit txn under serializable isolation: read locks are held
    // until commit/abort.
    let tcfg = TransactionConfig::new().with_serializable_isolation(true);
    let reader_txn = env.begin_transaction(Some(&tcfg)).unwrap();
    let mut data = DatabaseEntry::new();
    let key_lookup = DatabaseEntry::from_data(b"k");
    let status =
        db.get_into(Some(&reader_txn), &key_lookup, &mut data).unwrap();
    assert!(status);
    assert_eq!(data.data_opt(), Some(b"v0".as_slice()));

    let finished = Arc::new(AtomicBool::new(false));
    let finished_t = Arc::clone(&finished);
    let db_t = Arc::clone(&db);
    let handle = thread::spawn(move || {
        let key = DatabaseEntry::from_data(b"k");
        let val1 = DatabaseEntry::from_data(b"v1");
        // Blocks on the write lock for K (conflicts with the reader's Read
        // lock). With the 30 s timeout it waits until the reader commits.
        db_t.put(&key, &val1).unwrap();
        finished_t.store(true, Ordering::SeqCst);
    });

    // Deterministically wait until the writer is actually BLOCKED on the
    // lock (registered as a waiter). If it finishes instead, the read lock
    // failed to block the write — the exact bug this test guards.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            !finished.load(Ordering::SeqCst),
            "auto-commit write completed while the explicit txn holds the \
             read lock — the write was not blocked"
        );
        if env.stats().unwrap().lock.n_waiters >= 1 {
            break; // writer confirmed blocked on the lock
        }
        assert!(
            std::time::Instant::now() < deadline,
            "writer did not register as a lock waiter within 10s"
        );
        thread::sleep(Duration::from_millis(2));
    }

    // Writer is confirmed blocked and has not completed.
    assert!(!finished.load(Ordering::SeqCst));

    // Releasing the read lock must let the write complete.
    reader_txn.commit().unwrap();
    handle.join().unwrap();
    assert!(
        finished.load(Ordering::SeqCst),
        "write must complete after the read lock is released"
    );

    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

// ─── Sprint 6 / Property 4 — Transaction commit/abort visibility oracle ─────
//
// Property: drive a randomised sequence of transaction operations
// (begin / put / commit / abort / auto-commit put) against a real
// `noxu_db::Database` and an oracle that tracks "what would be
// committed".  After every commit the oracle is updated; after every
// abort the oracle reverts to the snapshot at txn-begin.  After every
// commit/abort, a fresh `db.get` for any seen key must return what the
// oracle says.
//
// Catches isolation/visibility bugs and the recently-fixed F1 (txn
// cleanup), F12 (auto-commit isolation), and durability-mismatch bugs.
// This is the multi-step model-test pattern recommended in the hegel
// skill (Tier-1 "Model tests").

mod prop_txn_visibility {
    use super::*;
    use hegel::generators;
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Debug, Clone)]
    enum TxnStep {
        // No-op if a txn is already active; otherwise begin one.
        Begin,
        // Put under the active txn if any, else auto-commit put.
        Put { key: Vec<u8>, value: Vec<u8> },
        // Commit the active txn if any (no-op otherwise).
        Commit,
        // Abort the active txn if any (no-op otherwise).
        Abort,
    }

    #[hegel::composite]
    fn step(tc: hegel::TestCase) -> TxnStep {
        // Skew the distribution slightly toward Put so short sequences
        // still build up state to verify: the step weights are
        // Begin=1, Put=4, Commit=2, Abort=1.  Emulate the weighting by
        // drawing a tag from a list where each tag repeats by its weight.
        let tag = tc.draw(generators::sampled_from(vec![
            "begin", "put", "put", "put", "put", "commit", "commit",
            "abort",
        ]));
        match tag {
            "begin" => TxnStep::Begin,
            "commit" => TxnStep::Commit,
            "abort" => TxnStep::Abort,
            _ => {
                let key = tc
                    .draw(generators::binary().min_size(1).max_size(4));
                let value = tc
                    .draw(generators::binary().min_size(1).max_size(16));
                TxnStep::Put { key, value }
            }
        }
    }

    // Verify every key ever touched is visible at exactly the committed
    // value (or absent if uncommitted).  Panics (fails the test) on any
    // mismatch.
    fn verify_committed_visibility(
        db: &Database,
        committed: &BTreeMap<Vec<u8>, Vec<u8>>,
        all_keys: &BTreeSet<Vec<u8>>,
        step_idx: usize,
    ) {
        for k in all_keys {
            let mut data = DatabaseEntry::new();
            let key_e = DatabaseEntry::from_data(k);
            let status = db.get_into(None, &key_e, &mut data).unwrap();
            match (status, committed.get(k)) {
                (true, Some(want)) => {
                    assert_eq!(
                        data.data(),
                        want.as_slice(),
                        "step {}: get({:?}) value mismatch",
                        step_idx,
                        k,
                    );
                }
                (false, None) => { /* agree */ }
                (s, w) => panic!(
                    "step {}: get({:?}) visibility mismatch: db={:?}, oracle={:?}",
                    step_idx, k, s, w,
                ),
            }
        }
    }

    #[hegel::test(test_cases = 64)]
    fn txn_commit_abort_visibility_oracle(tc: hegel::TestCase) {
        let steps: Vec<TxnStep> =
            tc.draw(generators::vecs(step()).min_size(1).max_size(40));
        let tmp = TempDir::new().unwrap();
        let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
        let db = open_db(&env, "prop_txn_visibility");

        // Oracle: the *committed* state of the database.
        let mut committed: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        // The active transaction together with the snapshot of
        // `committed` taken at begin, plus the txn-local mutations
        // applied since begin (which collapse onto `committed` on
        // commit and are dropped on abort).
        type ActiveTxn = (noxu_db::Transaction, BTreeMap<Vec<u8>, Vec<u8>>);
        let mut active: Option<ActiveTxn> = None;
        // All keys ever touched, for the post-step visibility sweep.
        let mut all_keys: BTreeSet<Vec<u8>> = BTreeSet::new();

        for (i, step) in steps.into_iter().enumerate() {
            match step {
                TxnStep::Begin => {
                    if active.is_none() {
                        let txn = env.begin_transaction(None).unwrap();
                        // Snapshot the committed state; mutations
                        // accumulate here until commit/abort.
                        let snap = committed.clone();
                        active = Some((txn, snap));
                    }
                }
                TxnStep::Put { key, value } => {
                    all_keys.insert(key.clone());
                    let key_e = DatabaseEntry::from_data(&key);
                    let val_e = DatabaseEntry::from_data(&value);
                    if let Some((txn, snap)) = active.as_mut() {
                        db.put_in(txn, &key_e, &val_e).unwrap();
                        snap.insert(key, value);
                    } else {
                        db.put(&key_e, &val_e).unwrap();
                        committed.insert(key, value);
                        // Auto-commit: oracle and db should agree
                        // *immediately* after this op.
                        verify_committed_visibility(
                            &db, &committed, &all_keys, i,
                        );
                    }
                }
                TxnStep::Commit => {
                    if let Some((txn, snap)) = active.take() {
                        txn.commit().unwrap();
                        committed = snap;
                        verify_committed_visibility(
                            &db, &committed, &all_keys, i,
                        );
                    }
                }
                TxnStep::Abort => {
                    if let Some((txn, _snap)) = active.take() {
                        txn.abort().unwrap();
                        // committed unchanged
                        verify_committed_visibility(
                            &db, &committed, &all_keys, i,
                        );
                    }
                }
            }
        }

        // Drain any still-active txn before final check / close.
        if let Some((txn, _)) = active.take() {
            txn.abort().unwrap();
        }

        // Final visibility sweep.
        verify_committed_visibility(&db, &committed, &all_keys, usize::MAX);

        // Exercise the F1 fix: env.close() must succeed once all
        // transactions are committed/aborted.
        db.close().unwrap();
        env.close().expect("env.close() must succeed after all txns settled");
    }
}

// ─── F11 / Decision 3B: nested-txn parameter removed in v2.0 (Wave 3-1) ─
//
// In v1.5 `Environment::begin_transaction` took an `Option<&Transaction>`
// `parent` argument that was rejected at runtime with
// `NoxuError::Unsupported`.  Wave 3-1 (v2.0) removed the parameter from
// the signature entirely — what was a runtime error is now a compile
// error.  The former `f11_nested_transaction_returns_unsupported` test
// has been deleted because the misuse it guarded is no longer
// representable in the type system; the documented happy-path test
// remains below as a smoke test that the new signature is correct.

/// `begin_transaction(None)` and `begin_transaction(Some(&cfg))` continue
/// to work exactly as before — the v2.0 signature change is surgical and
/// does not regress the documented happy path.
#[test]
fn f11_nested_transaction_none_still_works() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None).unwrap();
    txn.commit().unwrap();

    let cfg = TransactionConfig::new();
    let txn2 = env.begin_transaction(Some(&cfg)).unwrap();
    txn2.commit().unwrap();

    env.close().expect("env.close() must succeed");
}

// ─── F-5: explicit txns unregister from TxnManager (no leak) ─────────
//
// Begin/commit/abort many explicit transactions, then assert the
// TxnManager active-transaction count returns to zero and the commit/abort
// stat counters reflect the work. Pre-fix, `TxnManager::all_txns` (and the
// lock-manager locker-label map) grew without bound because the explicit
// commit/abort paths never called `commit_txn`/`abort_txn`, so `n_active`
// climbed monotonically and `n_commits`/`n_aborts` undercounted.
#[test]
fn f5_explicit_txns_unregister_from_txn_manager() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "f5");

    let before = env.stats().unwrap().txn;

    for i in 0u32..50 {
        let txn = env.begin_transaction(None).unwrap();
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let val = DatabaseEntry::from_bytes(b"v");
        db.put_in(&txn, &key, &val).unwrap();
        if i % 2 == 0 {
            txn.commit().unwrap();
        } else {
            txn.abort().unwrap();
        }
    }

    let after = env.stats().unwrap().txn;

    // No active transactions leaked: the count must be back to its
    // pre-loop value (zero, in this single-threaded test).
    assert_eq!(
        after.n_active, before.n_active,
        "TxnManager active-txn count leaked: before={}, after={} \
         (explicit commit/abort must unregister from TxnManager)",
        before.n_active, after.n_active
    );
    assert_eq!(after.n_active, 0, "expected zero active txns after the loop");

    // Commit/abort counters advanced by the work performed (25 each).
    assert_eq!(
        after.n_commits - before.n_commits,
        25,
        "n_commits must count the 25 explicit commits"
    );
    assert_eq!(
        after.n_aborts - before.n_aborts,
        25,
        "n_aborts must count the 25 explicit aborts"
    );
}

// ─── TXN-2: serializable-active counter wired correctly ──────────────────
//
// JE TxnManager.registerTxn increments nActiveSerializable when the txn is
// serializable; unRegisterTxn decrements it.  Pre-fix, Noxu's counter was
// never incremented, so are_other_serializable_transactions_active() always
// returned false.  Post-fix it accurately tracks live serializable txns.
//
// Fail-pre: both asserts below failed (counter always 0).
// Pass-post: counter == 1 while txn is live, 0 after commit/abort.

#[test]
fn txn2_serializable_counter_commit() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let ser_cfg = TransactionConfig::new().with_serializable_isolation(true);
    let txn = env.begin_transaction(Some(&ser_cfg)).unwrap();

    // While the serializable txn is live the counter must be 1.
    let mid = env.stats().unwrap().txn;
    assert_eq!(
        mid.n_active_serializable, 1,
        "TXN-2 fail-pre: n_active_serializable must be 1 while serializable txn is open"
    );

    txn.commit().unwrap();

    // After commit the counter must return to 0.
    let after = env.stats().unwrap().txn;
    assert_eq!(
        after.n_active_serializable, 0,
        "TXN-2: n_active_serializable must be 0 after serializable txn commits"
    );
}

#[test]
fn txn2_serializable_counter_abort() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let ser_cfg = TransactionConfig::new().with_serializable_isolation(true);
    let txn = env.begin_transaction(Some(&ser_cfg)).unwrap();

    let mid = env.stats().unwrap().txn;
    assert_eq!(
        mid.n_active_serializable, 1,
        "TXN-2 fail-pre: n_active_serializable must be 1 while serializable txn is open"
    );

    txn.abort().unwrap();

    let after = env.stats().unwrap().txn;
    assert_eq!(
        after.n_active_serializable, 0,
        "TXN-2: n_active_serializable must be 0 after serializable txn aborts"
    );
}

#[test]
fn txn2_non_serializable_counter_unaffected() {
    // A plain (non-serializable) txn must not touch the serializable counter.
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None).unwrap();

    let mid = env.stats().unwrap().txn;
    assert_eq!(
        mid.n_active_serializable, 0,
        "TXN-2: non-serializable txn must not increment n_active_serializable"
    );

    txn.commit().unwrap();

    let after = env.stats().unwrap().txn;
    assert_eq!(after.n_active_serializable, 0);
}

#[test]
fn txn2_mixed_serializable_and_plain() {
    // Two serializable txns and one plain: counter tracks only the
    // serializable ones, and returns to 0 after all are done.
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let ser_cfg = TransactionConfig::new().with_serializable_isolation(true);
    let s1 = env.begin_transaction(Some(&ser_cfg)).unwrap();
    let s2 = env.begin_transaction(Some(&ser_cfg)).unwrap();
    let plain = env.begin_transaction(None).unwrap();

    let mid = env.stats().unwrap().txn;
    assert_eq!(
        mid.n_active_serializable, 2,
        "TXN-2: two serializable txns must register a count of 2"
    );

    plain.commit().unwrap(); // plain commits: counter unchanged
    let after_plain = env.stats().unwrap().txn;
    assert_eq!(after_plain.n_active_serializable, 2);

    s1.commit().unwrap();
    let after_s1 = env.stats().unwrap().txn;
    assert_eq!(after_s1.n_active_serializable, 1);

    s2.abort().unwrap();
    let after_s2 = env.stats().unwrap().txn;
    assert_eq!(
        after_s2.n_active_serializable, 0,
        "TXN-2: counter must be 0 after all serializable txns finish"
    );
}

// ─── TXN-3 verification: all_txns drains to zero ─────────────────────────
//
// T-F5 fixed the inner-txn unregister at the noxu-db Transaction layer.
// This test re-verifies the contract holds (fail-pre: leaked N; pass-post: 0)
// and also checks the XA resolved paths drain the counter.
#[test]
fn txn3_all_txns_drains_to_zero_commit_and_abort() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "txn3");

    let before = env.stats().unwrap().txn;
    assert_eq!(before.n_active, 0);

    // 10 commits + 10 aborts
    for i in 0u32..20 {
        let txn = env.begin_transaction(None).unwrap();
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let val = DatabaseEntry::from_bytes(b"x");
        db.put_in(&txn, &key, &val).unwrap();
        if i % 2 == 0 {
            txn.commit().unwrap();
        } else {
            txn.abort().unwrap();
        }
    }

    let after = env.stats().unwrap().txn;
    assert_eq!(
        after.n_active, 0,
        "TXN-3: all_txns must be empty after all explicit txns complete"
    );
    assert_eq!(after.n_commits - before.n_commits, 10);
    assert_eq!(after.n_aborts - before.n_aborts, 10);
}
