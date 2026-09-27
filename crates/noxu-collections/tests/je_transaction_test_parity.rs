// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
//! Faithful ports of the behavioural gaps in JE's
//! `com.sleepycat.collections.test.TransactionTest`, plus the
//! `CurrentTransaction`-lifecycle intent of `TestSR15721`, at the
//! `noxu-collections` layer.
//!
//! The wave4-c / wave9-c ports in `tck_collection_semantics.rs` and
//! `collection_tests.rs` already cover the *runner commit / abort /
//! visibility* halves of `TransactionTest`; the ledger row for those JE
//! methods is cited there.  This file closes the remaining rows the ledger
//! recorded as `NOT-PORTED` or mis-cited (`testGetters` → evictor arbiter,
//! `testTransactional` → log entry_type were bogus name-match heuristics):
//!
//! - `TransactionTest.testGetters` -> `getter_txn_lifecycle` (txn half).
//! - `TransactionTest.testTransactional` -> `transactional_vs_non_transactional_map`.
//! - `TransactionTest.testExceptions` -> `commit_or_abort_with_no_active_txn_errors`.
//! - `TransactionTest.testRetry` -> `runner_retries_then_succeeds_when_lock_released`
//!   and `runner_exhausts_retries_then_surfaces_conflict`.
//!
//! JE source: `_/je/test/com/sleepycat/collections/test/TransactionTest.java`.
//!
//! Intentional deviations (recorded, not silently weakened):
//!   - `StoredCollections.configuredMap(...)` / `getCursorConfig()` view-level
//!     read-committed / read-uncommitted inheritance (the second half of
//!     `testGetters`, `testReadCommittedCollection`,
//!     `testReadUncommittedCollection`) has no `noxu-collections` analogue:
//!     Noxu threads isolation through `Option<&Transaction>` /
//!     `TransactionConfig`, not through per-view cursor-config objects.  The
//!     underlying read-committed lock-release / dirty-read-rejection engine
//!     behaviour is COVERED-CITED in `crates/noxu-db/tests/isolation_test.rs`
//!     (`test_read_committed_releases_lock_allowing_concurrent_writer`,
//!     `test_dirty_read_prevented_under_all_isolation_levels`).
//!   - `testNested` — nested transactions were deliberately removed
//!     (AGENTS.md; sprint3-1); N/A.
//!   - `testExceptionHandler` — a pluggable `handleException` override is not
//!     exposed by `TransactionRunner`; the retry-budget-then-surface half is
//!     covered by `runner_exhausts_retries_then_surfaces_conflict`.
//!   - `testCurrentTransactionGC` — relies on Java `WeakHashMap` GC
//!     reachability; Rust uses `Drop`.  N/A.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use noxu_bind::IntBinding;
use noxu_collections::{CollectionError, StoredMap, TransactionRunner};
use noxu_db::{
    Database, DatabaseConfig, Environment, EnvironmentConfig, NoxuError,
};
use tempfile::TempDir;

fn open_txn_env() -> (TempDir, Environment) {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    (td, env)
}

fn open_db(env: &Environment, name: &str, transactional: bool) -> Database {
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(transactional);
    env.open_database(None, name, &cfg).unwrap()
}

// JE constants ONE / TWO / THREE are int keys 1 / 2 / 3.
const ONE: i32 = 1;
const TWO: i32 = 2;

// ---------------------------------------------------------------------------
// TransactionTest.testGetters — txn-lifecycle half.
// ---------------------------------------------------------------------------

/// JE: `TransactionTest.testGetters` (the `CurrentTransaction`
/// begin/commit/abort → getTransaction() half).
///
/// JE asserts: before any begin, `getTransaction()` is null; after
/// `beginTransaction` it is non-null; after `commitTransaction` it is null
/// again; after a begin + `abortTransaction` it is null again.  Noxu has no
/// thread-bound `CurrentTransaction`; the analogue is that
/// `Environment::begin_transaction` yields a live handle, and commit/abort
/// consume it (the handle cannot be reused).  The view-level
/// read-committed/uncommitted cursor-config inheritance half is N/A (see
/// module docs).
#[test]
fn getter_txn_lifecycle() {
    let (_td, env) = open_txn_env();
    let db = open_db(&env, "getters", true);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding, IntBinding);

    // begin → a live txn drives a write that is visible within the txn.
    let txn = env.begin_transaction(None).unwrap();
    assert!(map.get(Some(&txn), &ONE).unwrap().is_none());
    map.put(Some(&txn), &ONE, &ONE).unwrap();
    assert_eq!(map.get(Some(&txn), &ONE).unwrap(), Some(ONE));
    // commit → the write is durable and visible under auto-commit.
    txn.commit().unwrap();
    assert_eq!(map.get(None, &ONE).unwrap(), Some(ONE));

    // begin + abort → the write rolls back and is invisible afterwards.
    let txn = env.begin_transaction(None).unwrap();
    map.put(Some(&txn), &TWO, &TWO).unwrap();
    assert_eq!(map.get(Some(&txn), &TWO).unwrap(), Some(TWO));
    txn.abort().unwrap();
    assert_eq!(map.get(None, &TWO).unwrap(), None);
}

// ---------------------------------------------------------------------------
// TransactionTest.testTransactional
// ---------------------------------------------------------------------------

/// JE: `TransactionTest.testTransactional`.
///
/// JE asserts a map over a non-transactional DB is `!isTransactional()` and
/// accepts an auto-commit `put`/`get`, while a map over a transactional DB is
/// `isTransactional()` and accepts a put under an explicit transaction.  Noxu
/// exposes transactionality via the DB config rather than a `isTransactional()`
/// method on the collection view, so the assertion is expressed as: a
/// non-transactional map round-trips a value under auto-commit, and a
/// transactional map round-trips a value threaded through an explicit
/// `Transaction`.
#[test]
fn transactional_vs_non_transactional_map() {
    let (_td, env) = open_txn_env();

    // Non-transactional DB: auto-commit put/get round-trips.
    let non_txn_db = open_db(&env, "non_txn", false);
    let non_txn_map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&non_txn_db, IntBinding, IntBinding);
    assert!(non_txn_map.put(None, &ONE, &ONE).unwrap().is_none());
    assert_eq!(non_txn_map.get(None, &ONE).unwrap(), Some(ONE));

    // Transactional DB: an explicit txn drives the put and it commits.
    let txn_db = open_db(&env, "txn", true);
    let txn_map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&txn_db, IntBinding, IntBinding);
    let txn = env.begin_transaction(None).unwrap();
    txn_map.put(Some(&txn), &ONE, &ONE).unwrap();
    assert_eq!(txn_map.get(Some(&txn), &ONE).unwrap(), Some(ONE));
    txn.commit().unwrap();
    assert_eq!(txn_map.get(None, &ONE).unwrap(), Some(ONE));
}

// ---------------------------------------------------------------------------
// TransactionTest.testExceptions
// ---------------------------------------------------------------------------

/// JE: `TransactionTest.testExceptions`.
///
/// JE asserts that calling `commitTransaction()` or `abortTransaction()` with
/// no active `CurrentTransaction` throws `IllegalStateException`.  Noxu has no
/// thread-bound current transaction; the equivalent misuse is committing or
/// aborting a `Transaction` handle twice — the second call must error rather
/// than silently succeed or panic.
#[test]
fn commit_or_abort_with_no_active_txn_errors() {
    let (_td, env) = open_txn_env();

    // Double-commit: the second commit must be rejected.
    let txn = env.begin_transaction(None).unwrap();
    txn.commit().unwrap();
    assert!(
        txn.commit().is_err(),
        "committing an already-committed transaction must error"
    );

    // Double-abort: the second abort must be rejected.
    let txn = env.begin_transaction(None).unwrap();
    txn.abort().unwrap();
    assert!(
        txn.abort().is_err(),
        "aborting an already-aborted transaction must error"
    );

    // Commit-after-abort must also be rejected.
    let txn = env.begin_transaction(None).unwrap();
    txn.abort().unwrap();
    assert!(
        txn.commit().is_err(),
        "committing an already-aborted transaction must error"
    );
}

// ---------------------------------------------------------------------------
// TransactionTest.testRetry
// ---------------------------------------------------------------------------

/// JE: `TransactionTest.testRetry` (the "release the lock after N tries and
/// the next try succeeds" half).
///
/// JE inserts a record, locks it in `txn1`, then runs a worker in the runner
/// that keeps hitting a lock conflict; on the K-th try the worker frees the
/// lock, so the (K)-th run succeeds with no conflict and exactly K tries are
/// observed.  Noxu's `TransactionRunner` retries a retryable error up to
/// `max_retries` times; this test drives a worker that returns a retryable
/// `DeadlockDetected` for the first K-1 tries and `Ok` on the K-th, and
/// asserts the runner performs exactly K attempts and then succeeds.
#[test]
fn runner_retries_then_succeeds_when_lock_released() {
    let (_td, env) = open_txn_env();
    // Plenty of budget so the success on try 3 is reached.
    let runner = TransactionRunner::new(&env).with_max_retries(10);

    let tries = Arc::new(AtomicU32::new(0));
    let release_after: u32 = 3;
    let t2 = Arc::clone(&tries);

    let result: noxu_collections::Result<&str> = runner.run(move |_txn| {
        let n = t2.fetch_add(1, Ordering::SeqCst) + 1; // 1-based try count
        if n < release_after {
            // Lock still held: retryable conflict.
            Err(CollectionError::DatabaseError(NoxuError::DeadlockDetected))
        } else {
            // Lock released on the release_after-th try: succeed.
            Ok("done")
        }
    });

    assert_eq!(result.unwrap(), "done");
    assert_eq!(
        tries.load(Ordering::SeqCst),
        release_after,
        "runner must perform exactly {release_after} tries before success"
    );
}

/// JE: `TransactionTest.testRetry` (the "exhaust the default retries then get
/// a LockConflictException" half).
///
/// JE asserts that when the lock is never released the worker is tried
/// `DEFAULT_MAX_RETRIES + 1` times and the final outcome is a
/// `LockConflictException`.  Noxu's runner performs `max_retries + 1` total
/// attempts for a persistently-retryable error and then surfaces that error.
/// This asserts both the exact try count and that the surfaced error is the
/// retryable conflict (not swallowed).
#[test]
fn runner_exhausts_retries_then_surfaces_conflict() {
    let (_td, env) = open_txn_env();
    let max_retries: u32 = 4;
    let runner = TransactionRunner::new(&env).with_max_retries(max_retries);

    let tries = Arc::new(AtomicU32::new(0));
    let t2 = Arc::clone(&tries);

    let result: noxu_collections::Result<()> = runner.run(move |_txn| {
        t2.fetch_add(1, Ordering::SeqCst);
        Err(CollectionError::DatabaseError(NoxuError::DeadlockDetected))
    });

    // The surfaced error must be the retryable conflict, not swallowed.
    match result {
        Err(CollectionError::DatabaseError(NoxuError::DeadlockDetected)) => {}
        other => panic!("expected surfaced DeadlockDetected, got {other:?}"),
    }
    // max_retries retries means max_retries + 1 total attempts.
    assert_eq!(
        tries.load(Ordering::SeqCst),
        max_retries + 1,
        "runner must try exactly max_retries + 1 times before giving up"
    );
}
