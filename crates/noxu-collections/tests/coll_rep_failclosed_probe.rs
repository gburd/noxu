//! coll_rep_failclosed_probe — R-REPUSER fail-closed across the collections API.
//!
//! Closes the coordinator-review gap: `StoredMap` / `StoredList` /
//! `StoredSortedMap` writes use their own txn plumbing (`run_in_auto_commit`
//! for auto-commit, an explicit `Transaction` / `TransactionRunner` otherwise)
//! and the RMW path goes through an auto-commit CURSOR, not `Database::put`.
//! This witnesses that every collections write — auto-commit AND explicit-txn —
//! is refused on a data env wired to a coordinator that cannot replicate user
//! writes, because the shared env-level gate sits at `Database::check_writable`
//! AND the `CursorImpl` write backstop that `run_in_auto_commit` funnels through.
//!
//! On the base (coordinator not refusing) these all return Ok and silently drop
//! the write (never shipped) — the S1 defect this guards against.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use noxu_bind::{IntBinding, StringBinding};
use noxu_collections::{
    CollectionError, StoredList, StoredMap, StoredSortedMap, TransactionRunner,
};
use noxu_db::error::NoxuError;
use noxu_db::{
    AckWaitError, AckWaitErrorKind, Database, DatabaseConfig, Environment,
    EnvironmentConfig, ReplicaAckCoordinator, ReplicaAckPolicyKind,
};
use tempfile::TempDir;

/// A coordinator that reports it CANNOT replicate user writes (what the real
/// `noxu_rep::ReplicatedEnvironment` reports in 7.11.x). Its `await_replica_acks`
/// is never reached (the write is refused at the op), so it returns Ok.
struct RefusingCoord;

impl ReplicaAckCoordinator for RefusingCoord {
    fn await_replica_acks(
        &self,
        _policy: ReplicaAckPolicyKind,
        _timeout: Duration,
    ) -> std::result::Result<u32, AckWaitError> {
        Err(AckWaitError {
            kind: AckWaitErrorKind::Timeout,
            needed: 1,
            received: 0,
        })
    }
    fn refuse_user_write_commit(&self) -> Option<&'static str> {
        Some("R-REPUSER: user writes not replicated in 7.11.x (test)")
    }
}

type IntStrMap<'db> = StoredMap<'db, i32, String, IntBinding, StringBinding>;
type IntStrSortedMap<'db> =
    StoredSortedMap<'db, i32, String, IntBinding, StringBinding>;
type StrList<'db> = StoredList<'db, String, StringBinding>;

/// Open env + DB (BEFORE wiring, as a real app sets up its containers), then
/// install the refusing coordinator.
fn env_db(tmp: &TempDir) -> (Environment, Database) {
    let env = Environment::open(
        EnvironmentConfig::new(tmp.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "c",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    env.set_replica_coordinator(Arc::new(RefusingCoord));
    (env, db)
}

fn assert_refused<T: std::fmt::Debug>(
    res: Result<T, CollectionError>,
    path: &str,
) {
    match res {
        Err(CollectionError::DatabaseError(
            NoxuError::ReplicationNotSupported { reason },
        )) => assert!(!reason.is_empty(), "{path}: refusal reason empty"),
        Ok(v) => panic!(
            "{path}: S1 REGRESSION — returned Ok({v:?}); the write was silently \
             accepted and will never be streamed."
        ),
        Err(other) => {
            panic!("{path}: expected ReplicationNotSupported, got {other:?}")
        }
    }
}

// ── StoredMap auto-commit put / remove (run_in_auto_commit -> cursor RMW) ──
#[test]
fn stored_map_auto_commit_put_remove_refused() {
    let tmp = TempDir::new().unwrap();
    // Pre-seed one entry BEFORE wiring so `remove` has a real record to delete
    // (removing a MISSING key is a no-op read that writes nothing and is
    // correctly not refused).
    let env = Environment::open(
        EnvironmentConfig::new(tmp.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "c",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    {
        let seed: IntStrMap = StoredMap::new(&db, IntBinding, StringBinding);
        seed.put(None, &1, &"seed".to_string()).unwrap();
    }
    env.set_replica_coordinator(Arc::new(RefusingCoord));

    let map: IntStrMap = StoredMap::new(&db, IntBinding, StringBinding);
    assert_refused(map.put(None, &2, &"a".to_string()), "StoredMap put (auto)");
    // remove of the EXISTING seeded key writes a delete LN -> refused.
    assert_refused(map.remove(None, &1), "StoredMap remove (auto)");
    let _ = db.close();
    let _ = env.close();
}

// ── StoredMap explicit-txn put ──
#[test]
fn stored_map_explicit_txn_put_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db) = env_db(&tmp);
    let map: IntStrMap = StoredMap::new(&db, IntBinding, StringBinding);
    let txn = env.begin_transaction(None).unwrap();
    assert_refused(
        map.put(Some(&txn), &2, &"b".to_string()),
        "StoredMap put (explicit txn)",
    );
    let _ = txn.abort();
    let _ = db.close();
    let _ = env.close();
}

// ── StoredList push (auto-commit, run_in_auto_commit) ──
#[test]
fn stored_list_push_refused() {
    let tmp = TempDir::new().unwrap();
    // StoredList needs its own 8-byte-key DB; open it BEFORE wiring.
    let env = Environment::open(
        EnvironmentConfig::new(tmp.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "lst",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    env.set_replica_coordinator(Arc::new(RefusingCoord));
    let list: StrList = StoredList::new(&db, StringBinding);
    assert_refused(list.push(None, &"x".to_string()), "StoredList push (auto)");
    let _ = db.close();
    let _ = env.close();
}

// ── StoredSortedMap put (auto-commit AND explicit txn) ──
#[test]
fn stored_sorted_map_put_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db) = env_db(&tmp);
    let map: IntStrSortedMap =
        StoredSortedMap::new(&db, IntBinding, StringBinding);
    assert_refused(
        map.put(None, &3, &"c".to_string()),
        "StoredSortedMap put (auto)",
    );
    let txn = env.begin_transaction(None).unwrap();
    assert_refused(
        map.put(Some(&txn), &4, &"d".to_string()),
        "StoredSortedMap put (explicit txn)",
    );
    let _ = txn.abort();
    let _ = db.close();
    let _ = env.close();
}

// ── TransactionRunner (its own txn plumbing) ──
#[test]
fn transaction_runner_put_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db) = env_db(&tmp);
    let map: IntStrMap = StoredMap::new(&db, IntBinding, StringBinding);
    let runner = TransactionRunner::new(&env).with_max_retries(0);
    let res: Result<(), CollectionError> =
        runner.run(|txn| map.put(Some(txn), &5, &"e".to_string()).map(|_| ()));
    assert_refused(res, "TransactionRunner put");
    let _ = db.close();
    let _ = env.close();
}

// ── control: standalone env (no coordinator) — collections all work ──
#[test]
fn standalone_collections_unaffected() {
    let tmp = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(tmp.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "c",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    // No set_replica_coordinator.
    let map: IntStrMap = StoredMap::new(&db, IntBinding, StringBinding);
    map.put(None, &1, &"a".to_string())
        .expect("standalone StoredMap put must work");
    assert_eq!(map.get(None, &1).unwrap(), Some("a".to_string()));
    // StoredList uses 8-byte record-number keys, so it needs its OWN database.
    let listdb = env
        .open_database(
            None,
            "lst",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let list: StrList = StoredList::new(&listdb, StringBinding);
    list.push(None, &"x".to_string())
        .expect("standalone StoredList push must work");
    let _ = listdb.close();
    let _ = db.close();
    let _ = env.close();
}
