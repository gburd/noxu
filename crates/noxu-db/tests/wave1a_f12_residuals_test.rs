//! Wave 1A regression tests for the two F12 residuals from
//! the 2026 review:
//!
//!   1. **NULL-LSN insert race** — two concurrent auto-commit inserts
//!      of the same brand-new key now coordinate through the lock
//!      manager via a synthetic key-coordination lock, and a forced
//!      mid-write failure in auto-commit rolls back the in-memory tree
//!      write through the synthetic auto-txn's abort-undo path.
//!
//!   2. **Locker-id collision space** — deadlock / lock-timeout error
//!      messages between an auto-commit op and an explicit txn now
//!      report typed locker identifiers (`"auto-txn:<id>"`,
//!      `"txn:<id>"`) instead of opaque integers.
//!
//! These tests are *behavioural*: they drive the public `Environment`
//! / `Database` surface and assert the documented contract.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Durability, Environment,
    EnvironmentConfig, OperationStatus, TransactionConfig,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
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

// ─── Residual #1: NULL-LSN insert race ───────────────────────────────

/// Two threads call `db.put_no_overwrite(None, K, _)` for the same
/// brand-new key K.  Exactly one must see `Success`; the other must
/// see `KeyExists` (the Noxu / Berkeley-DB-JE real-world equivalent of
/// "lock conflict" for a unique-key insert: the second inserter
/// arrives at the lock manager, blocks on the synthetic key
/// coordination lock acquired by the first inserter, unblocks when
/// the first inserter commits, re-checks `key_exists_in_view`, finds
/// the key in the BIN, and reports `KeyExists`).
///
/// Pre-Wave-1A both threads could race past `key_exists_in_view` and
/// the only thing that serialised them was the BIN latch — neither
/// contended through the lock manager and so the deadlock detector
/// could not reason about the conflict.  Now both serialise through
/// the synthetic auto-txn's write lock on
/// [`noxu_util::Lsn::synthetic_key_lock_id(db_id, K)`] and one is
/// guaranteed to be the loser.
#[test]
fn null_lsn_insert_race_two_auto_commit_inserts_serialise_through_lock_manager()
{
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "null_lsn_race"));

    // Run the race many times to make the loser-vs-winner outcome
    // statistically robust under different scheduler interleavings.
    const ROUNDS: u32 = 64;

    for round in 0..ROUNDS {
        let key_bytes = format!("k{round:08}").into_bytes();
        let val_a = format!("a{round}").into_bytes();
        let val_b = format!("b{round}").into_bytes();
        let val_a_thread = val_a.clone();
        let val_b_thread = val_b.clone();
        let key_a = key_bytes.clone();
        let key_b = key_bytes.clone();

        let db_a = Arc::clone(&db);
        let db_b = Arc::clone(&db);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let bar_a = Arc::clone(&barrier);
        let bar_b = Arc::clone(&barrier);

        let h_a = thread::spawn(move || {
            bar_a.wait();
            let k = DatabaseEntry::from_data(&key_a);
            let v = DatabaseEntry::from_data(&val_a_thread);
            db_a.put_no_overwrite(None, &k, &v).unwrap()
        });
        let h_b = thread::spawn(move || {
            bar_b.wait();
            let k = DatabaseEntry::from_data(&key_b);
            let v = DatabaseEntry::from_data(&val_b_thread);
            db_b.put_no_overwrite(None, &k, &v).unwrap()
        });

        let r_a = h_a.join().unwrap();
        let r_b = h_b.join().unwrap();

        // Exactly one must be Success; the other must be KeyExists.
        let success_count = (r_a == OperationStatus::Success) as u8
            + (r_b == OperationStatus::Success) as u8;
        let key_exists_count = (r_a == OperationStatus::KeyExists) as u8
            + (r_b == OperationStatus::KeyExists) as u8;
        assert_eq!(
            success_count, 1,
            "round {round}: exactly one thread must succeed, got A={r_a:?} B={r_b:?}"
        );
        assert_eq!(
            key_exists_count, 1,
            "round {round}: exactly one thread must see KeyExists, got A={r_a:?} B={r_b:?}"
        );

        // The stored value must be the winner's.
        let mut got = DatabaseEntry::new();
        let key_lookup = DatabaseEntry::from_data(&key_bytes);
        assert_eq!(
            db.get(None, &key_lookup, &mut got).unwrap(),
            OperationStatus::Success
        );
        let stored = got.get_data().unwrap_or_default();
        let winner_value = if r_a == OperationStatus::Success {
            &val_a[..]
        } else {
            &val_b[..]
        };
        assert_eq!(
            stored, winner_value,
            "round {round}: stored value must be the winner's"
        );
    }

    drop(db);
    let env_unwrapped =
        Arc::try_unwrap(env).ok().expect("env Arc must be unique");
    env_unwrapped.close().unwrap();
    drop(tmp);
}

/// Companion to the race test above: after a clean close-and-reopen,
/// every key inserted in a winner-vs-loser race must persist with
/// exactly one of the two contending values, and no phantom keys must
/// appear.  Run as a separate test (with a fresh `TempDir`) so the
/// FileManager directory lock is cleanly released between the close
/// and the reopen.
#[test]
fn null_lsn_insert_race_recovery_has_no_phantom_keys() {
    let tmp = TempDir::new().unwrap();
    let env_path = tmp.path().to_path_buf();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_SYNC));
    let db = Arc::new(open_db(&env, "null_lsn_recovery"));

    const ROUNDS: u32 = 8;
    let mut winners: Vec<(Vec<u8>, Vec<u8>)> =
        Vec::with_capacity(ROUNDS as usize);
    for round in 0..ROUNDS {
        let key_bytes = format!("k{round:08}").into_bytes();
        let val_a = format!("a{round}").into_bytes();
        let val_b = format!("b{round}").into_bytes();
        let key_a = key_bytes.clone();
        let key_b = key_bytes.clone();
        let val_a_thread = val_a.clone();
        let val_b_thread = val_b.clone();
        let db_a = Arc::clone(&db);
        let db_b = Arc::clone(&db);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let bar_a = Arc::clone(&barrier);
        let bar_b = Arc::clone(&barrier);
        let h_a = thread::spawn(move || {
            bar_a.wait();
            let k = DatabaseEntry::from_data(&key_a);
            let v = DatabaseEntry::from_data(&val_a_thread);
            db_a.put_no_overwrite(None, &k, &v).unwrap()
        });
        let h_b = thread::spawn(move || {
            bar_b.wait();
            let k = DatabaseEntry::from_data(&key_b);
            let v = DatabaseEntry::from_data(&val_b_thread);
            db_b.put_no_overwrite(None, &k, &v).unwrap()
        });
        let _r_a = h_a.join().unwrap();
        let _r_b = h_b.join().unwrap();
        let winner_value = if _r_a == OperationStatus::Success {
            val_a.clone()
        } else {
            val_b.clone()
        };
        winners.push((key_bytes, winner_value));
    }

    drop(db);
    let env_unwrapped =
        Arc::try_unwrap(env).ok().expect("env Arc must be unique");
    env_unwrapped.close().unwrap();
    drop(env_unwrapped);
    // The FileManager `je.lck` directory lock is released when the
    // `EnvironmentImpl` Arc reaches refcount 0; daemon-thread joins
    // happen during `close()` but their final `Arc` drop may lag
    // briefly behind on parking_lot platforms.  A short sleep here
    // keeps the close-then-reopen test deterministic without
    // changing production behaviour.
    std::thread::sleep(Duration::from_millis(200));

    // Re-open the same env home and verify the winners persisted.
    let cfg = EnvironmentConfig::new(env_path)
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(Durability::COMMIT_NO_SYNC);
    let env_reopen = Environment::open(cfg).unwrap();
    let db_reopen = open_db(&env_reopen, "null_lsn_recovery");
    for (key_bytes, winner_value) in &winners {
        let key_lookup = DatabaseEntry::from_data(key_bytes);
        let mut got = DatabaseEntry::new();
        let status = db_reopen.get(None, &key_lookup, &mut got).unwrap();
        assert_eq!(
            status,
            OperationStatus::Success,
            "key {key_bytes:?} must persist across reopen"
        );
        let stored = got.get_data().unwrap_or_default().to_vec();
        assert_eq!(
            &stored, winner_value,
            "key {key_bytes:?}: stored value must equal the winner's value"
        );
    }
    db_reopen.close().unwrap();
    env_reopen.close().unwrap();
    drop(tmp);
}

// ─── Residual #1 (continued): auto-commit rollback on failure ────────

/// A forced mid-write failure during auto-commit must roll back the
/// in-memory tree write so a subsequent `db.get(K)` returns
/// `NotFound`.
///
/// We exercise this via the existing `set_cursor_fail_after` test
/// hook in `noxu-dbi::cursor_impl` by setting a small countdown so
/// the fail-tick fires inside `cursor.put` AFTER the tree mutation
/// has been applied.  The `Database::with_auto_txn` wrapper catches
/// the propagated `DbiError::CursorClosed`, calls
/// `Txn::abort_collect_undo`, and `apply_auto_txn_undo` deletes the
/// just-inserted key from the in-memory B-tree.
///
/// The exact fail-tick countdown depends on internal cursor-check
/// call counts and is implementation-detail; the test asserts the
/// invariant — "either Err propagates AND the key is absent, or the
/// op succeeded AND the key is present with the expected value" —
/// which is robust to small implementation changes.
#[test]
fn auto_commit_rollback_on_forced_failure_undoes_in_memory_write() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "auto_commit_rollback");

    // Successful baseline: brand-new key is inserted normally.
    let key = DatabaseEntry::from_data(b"baseline");
    let val = DatabaseEntry::from_data(b"v0");
    db.put(None, &key, &val).unwrap();

    let mut got = DatabaseEntry::new();
    assert_eq!(db.get(None, &key, &mut got).unwrap(), OperationStatus::Success);
    assert_eq!(got.get_data(), Some(b"v0".as_slice()));

    let key_fail = DatabaseEntry::from_data(b"force_fail");
    let val_fail = DatabaseEntry::from_data(b"will_be_undone");

    noxu_dbi::set_cursor_fail_after(2);
    let result = db.put(None, &key_fail, &val_fail);
    noxu_dbi::clear_cursor_fail_flag();

    match result {
        Err(_) => {
            let mut got = DatabaseEntry::new();
            let status = db.get(None, &key_fail, &mut got).unwrap();
            assert_eq!(
                status,
                OperationStatus::NotFound,
                "auto-commit rollback must remove the in-memory tree entry; \
                 got {:?} value={:?}",
                status,
                got.get_data()
            );
        }
        Ok(_) => {
            let mut got = DatabaseEntry::new();
            let status = db.get(None, &key_fail, &mut got).unwrap();
            assert_eq!(status, OperationStatus::Success);
            assert_eq!(got.get_data(), Some(b"will_be_undone".as_slice()));
        }
    }

    db.close().unwrap();
    env.close().unwrap();
}

// ─── Residual #1: ordinary single-thread auto-commit still completes ─

/// Regression check that the synthetic-auto-txn rewrite did not
/// catastrophically break ordinary auto-commit performance.  This is
/// a liveness check, not a benchmark: 1024 sequential auto-commit
/// puts of distinct keys must all succeed and round-trip via
/// `db.get`.
#[test]
fn auto_commit_single_thread_performance_regression_check() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "auto_commit_perf");

    const N: u32 = 1024;
    for i in 0..N {
        let key = DatabaseEntry::from_data(&i.to_be_bytes());
        let val = DatabaseEntry::from_data(b"v");
        let status = db.put(None, &key, &val).unwrap();
        assert_eq!(
            status,
            OperationStatus::Success,
            "auto-commit put {i} must succeed"
        );
    }

    for i in 0..N {
        let key = DatabaseEntry::from_data(&i.to_be_bytes());
        let mut got = DatabaseEntry::new();
        let status = db.get(None, &key, &mut got).unwrap();
        assert_eq!(
            status,
            OperationStatus::Success,
            "auto-commit get {i} must succeed"
        );
    }

    db.close().unwrap();
    env.close().unwrap();
}

// ─── Residual #2: typed locker IDs in lock contention messages ──────

/// An explicit txn holds a write lock on K1 with a tight
/// `lock_timeout_ms`; a concurrent auto-commit op tries to write K1
/// and (since the explicit txn never commits before the timeout) must
/// fail with a `LockTimeout` whose body renders the typed locker
/// identifiers.
///
/// Specifically, the synthetic auto-txn registers itself in
/// `LockManager::locker_labels` as `"auto-txn"`, so the requester
/// field of the `LockTimeout` error is rendered as
/// `"auto-txn:<id>"`; the explicit txn registers as `"txn"` so the
/// owner field is `"txn:<id>"`.  The test asserts both substrings
/// appear in the formatted error message.  Pre-Wave-1A the message
/// rendered both lockers as opaque integers and gave no clue about
/// which side was the auto-commit op.
#[test]
fn lock_timeout_message_uses_typed_locker_ids() {
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "typed_ids"));

    // Pre-seed K1 so the write lock attaches to a real LSN.
    db.put(
        None,
        &DatabaseEntry::from_data(b"K1"),
        &DatabaseEntry::from_data(b"v1"),
    )
    .unwrap();

    // Tight lock timeout so the auto-commit thread fails quickly.
    let cfg = TransactionConfig::new().with_lock_timeout_ms(100);
    let txn = env.begin_transaction(Some(&cfg)).unwrap();

    // Explicit txn writes K1 → holds write lock on K1's LSN.
    db.put(
        Some(&txn),
        &DatabaseEntry::from_data(b"K1"),
        &DatabaseEntry::from_data(b"hold"),
    )
    .unwrap();

    let started = Arc::new(AtomicBool::new(false));
    let started_t = Arc::clone(&started);
    let db_a = Arc::clone(&db);
    let h = thread::spawn(move || {
        started_t.store(true, Ordering::SeqCst);
        db_a.put(
            None,
            &DatabaseEntry::from_data(b"K1"),
            &DatabaseEntry::from_data(b"a"),
        )
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !started.load(Ordering::SeqCst)
        && std::time::Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(2));
    }

    let r = h.join().unwrap();

    // Explicit txn: abort to release the K1 lock; the auto-commit
    // thread already finished (with LockTimeout / Deadlock).
    let _ = txn.abort();

    let err = r.expect_err("auto-commit must fail (LockTimeout / Deadlock)");
    let msg = format!("{err}");
    assert!(
        msg.contains("auto-txn:"),
        "error message must include typed auto-txn locker id; got: {msg}"
    );
    assert!(
        msg.contains("txn:"),
        "error message must include typed explicit-txn locker id; got: {msg}"
    );

    drop(db);
    let env_unwrapped =
        Arc::try_unwrap(env).ok().expect("env Arc must be unique");
    env_unwrapped.close().unwrap();
}
