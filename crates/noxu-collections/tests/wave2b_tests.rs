//! Wave 2B regression tests: typed-API + txn threading for
//! `noxu-collections`.
//!
//! These tests replace the v1.5 Sprint 3C scope-down tests.  They
//! pin down the audit findings *closed* (no longer just deferred)
//! by Wave 2B, in a way that future refactors can't silently
//! regress.  See the 2026 review
//! for the full scope/narrative; per-test docstrings name the
//! audit finding they cover.
//!
//! Findings closed here:
//!   - #1 -- Stored* ops accept `Option<&Transaction>` and thread it through
//!   - #3 / #4 -- TransactionRunner's `&Transaction` drives Stored* methods
//!   - #5 -- StoredList::remove now compacts (shift-down)
//!   - #6 -- StoredList::open recovers next_index across reopen
//!   - #11 / #12 -- typed Stored{Map,Set,List} surface

use std::path::{Path, PathBuf};

use noxu_bind::{ByteArrayBinding, IntBinding, StringBinding};
use noxu_collections::{
    CollectionError, StoredList, StoredMap, TransactionRunner,
};
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fresh_env_dir() -> (TempDir, PathBuf) {
    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();
    (td, path)
}

fn open_env(path: &Path, transactional: bool) -> Environment {
    let mut cfg =
        EnvironmentConfig::new(path.to_path_buf()).with_allow_create(true);
    if transactional {
        cfg = cfg.with_transactional(true);
    }
    Environment::open(cfg).unwrap()
}

fn open_db(env: &Environment, name: &str) -> Database {
    let db_config =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    env.open_database(None, name, &db_config).unwrap()
}

// ---------------------------------------------------------------------------
// Audit finding #1 -- Stored* ops accept Option<&Transaction>.
// ---------------------------------------------------------------------------

/// Every Stored* op accepts `txn: Option<&Transaction>` as the leading
/// argument.  This test locks in the v1.6 surface so that any
/// regression that drops the txn parameter fails to compile.
#[test]
fn stored_map_methods_take_optional_txn() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, true);
    let db = open_db(&env, "ws_map");
    let map: StoredMap<'_, i32, String, _, _> =
        StoredMap::new(&db, IntBinding, StringBinding);

    // Auto-commit form (txn = None).
    map.put(None, &1, &"alpha".to_string()).unwrap();
    assert_eq!(map.get(None, &1).unwrap(), Some("alpha".to_string()));
    assert!(map.contains_key(None, &1).unwrap());
    assert_eq!(map.len(None).unwrap(), 1);
    assert!(!map.is_empty(None).unwrap());

    // Explicit-txn form (txn = Some(&t)).
    let txn = env.begin_transaction(None).unwrap();
    map.put(Some(&txn), &2, &"beta".to_string()).unwrap();
    assert_eq!(map.get(Some(&txn), &2).unwrap(), Some("beta".to_string()),);
    assert!(map.contains_key(Some(&txn), &2).unwrap());
    txn.commit().unwrap();

    // Iteration variants take txn too.  They are now lazy (review P1-7),
    // so drop the iterators (closing their cursors) before committing.
    let txn = env.begin_transaction(None).unwrap();
    {
        let _items = map.iter(Some(&txn)).unwrap();
        let _keys = map.keys(Some(&txn)).unwrap();
        let _values = map.values(Some(&txn)).unwrap();
    }
    txn.commit().unwrap();
}

/// User-txn writes that abort do not leak into the database.
#[test]
fn stored_map_writes_abort_with_user_txn() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, true);
    let db = open_db(&env, "abort_map");
    let map: StoredMap<'_, i32, String, _, _> =
        StoredMap::new(&db, IntBinding, StringBinding);

    // Pre-populate.
    map.put(None, &1, &"original".to_string()).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    map.put(Some(&txn), &1, &"modified".to_string()).unwrap();
    map.put(Some(&txn), &2, &"new".to_string()).unwrap();
    txn.abort().unwrap();

    assert_eq!(map.get(None, &1).unwrap(), Some("original".to_string()));
    assert_eq!(map.get(None, &2).unwrap(), None);
}

// ---------------------------------------------------------------------------
// Audit findings #3 / #4 -- TransactionRunner drives Stored* methods.
// ---------------------------------------------------------------------------

/// The runner-managed `&Transaction` is now usable inside Stored*
/// methods.  This is the central Wave 2B contract.
#[test]
fn runner_txn_drives_storedmap_writes() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, true);
    let db = open_db(&env, "runner_map");
    let map: StoredMap<'_, i32, String, _, _> =
        StoredMap::new(&db, IntBinding, StringBinding);

    let runner = TransactionRunner::new(&env);
    runner
        .run(|txn| {
            map.put(Some(txn), &1, &"a".to_string())?;
            map.put(Some(txn), &2, &"b".to_string())?;
            map.remove(Some(txn), &2)?;
            Ok(())
        })
        .unwrap();

    assert_eq!(map.get(None, &1).unwrap(), Some("a".to_string()));
    assert_eq!(map.get(None, &2).unwrap(), None);
}

/// Runner aborts every Stored* write on closure error.
#[test]
fn runner_aborts_storedmap_writes_on_closure_error() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, true);
    let db = open_db(&env, "runner_abort");
    let map: StoredMap<'_, i32, String, _, _> =
        StoredMap::new(&db, IntBinding, StringBinding);

    let runner = TransactionRunner::new(&env);
    let result: noxu_collections::Result<()> = runner.run(|txn| {
        map.put(Some(txn), &1, &"set".to_string())?;
        Err(CollectionError::IllegalState("rollback".into()))
    });
    assert!(result.is_err());
    assert_eq!(map.get(None, &1).unwrap(), None);
}

/// Runner retries on a competing-thread-induced lock conflict.
///
/// We force a deadlock by issuing it directly from the closure
/// (the simplest deterministic shape — the runner's retry loop
/// sees the retryable error, sleeps with backoff, and tries
/// again).  Wave 2B's contract is that `LockConflict`,
/// `DeadlockDetected`, and `LockTimeout` all trigger retry.
#[test]
fn runner_retries_lock_conflict_with_jittered_backoff() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, true);
    let _db = open_db(&env, "runner_retry");

    let runner = TransactionRunner::new(&env)
        .with_max_retries(5)
        .with_base_backoff(std::time::Duration::from_micros(10))
        .with_max_backoff(std::time::Duration::from_micros(100));

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let calls_clone = calls.clone();
    let result = runner.run(move |_txn| {
        let n = calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n < 3 {
            Err(CollectionError::DatabaseError(
                noxu_db::NoxuError::LockConflict("simulated".into()),
            ))
        } else {
            Ok(n)
        }
    });
    assert_eq!(result.unwrap(), 3);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
}

// ---------------------------------------------------------------------------
// Audit finding #5 -- StoredList::remove now compacts.
// ---------------------------------------------------------------------------

/// Wave 2B closes finding #5: `remove(idx)` shifts every higher
/// element down by one slot and decrements `next_index`.  After the
/// call there is no gap at `idx`.
#[test]
fn stored_list_remove_compacts_no_gaps() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, false);
    let db = open_db(&env, "compact_list");
    let list: StoredList<'_, String, _> = StoredList::new(&db, StringBinding);

    for i in 0..5 {
        list.push(None, &format!("v{i}")).unwrap();
    }
    assert_eq!(list.next_index(), 5);

    let removed = list.remove(None, 1).unwrap();
    assert_eq!(removed, Some("v1".to_string()));

    // After remove(1): list = [v0, v2, v3, v4]
    assert_eq!(list.next_index(), 4);
    assert_eq!(list.len(None).unwrap(), 4);
    assert_eq!(list.get(None, 0).unwrap(), Some("v0".to_string()));
    assert_eq!(list.get(None, 1).unwrap(), Some("v2".to_string()));
    assert_eq!(list.get(None, 2).unwrap(), Some("v3".to_string()));
    assert_eq!(list.get(None, 3).unwrap(), Some("v4".to_string()));
    assert_eq!(list.get(None, 4).unwrap(), None);

    // iter() yields the dense compacted view.
    let collected: Vec<String> =
        list.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(
        collected,
        vec![
            "v0".to_string(),
            "v2".to_string(),
            "v3".to_string(),
            "v4".to_string(),
        ],
    );
}

// ---------------------------------------------------------------------------
// Audit finding #6 -- StoredList::next_index reopen recovery.
// ---------------------------------------------------------------------------

#[test]
fn stored_list_open_recovers_next_index_after_reopen() {
    let (_td, path) = fresh_env_dir();

    {
        let env = open_env(&path, false);
        let db = open_db(&env, "list_reopen");
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);
        list.push(None, &"alpha".to_string()).unwrap();
        list.push(None, &"beta".to_string()).unwrap();
        list.push(None, &"gamma".to_string()).unwrap();
        let _ = db.close();
    }
    {
        let env = open_env(&path, false);
        let db = open_db(&env, "list_reopen");
        let list: StoredList<'_, String, _> =
            StoredList::open(&db, StringBinding).unwrap();
        assert_eq!(list.next_index(), 3);
        assert_eq!(list.get(None, 0).unwrap(), Some("alpha".to_string()));
        assert_eq!(list.get(None, 1).unwrap(), Some("beta".to_string()));
        assert_eq!(list.get(None, 2).unwrap(), Some("gamma".to_string()));
        let idx = list.push(None, &"delta".to_string()).unwrap();
        assert_eq!(idx, 3);
    }
}

#[test]
fn stored_list_new_does_not_recover_and_overwrites_on_reopen() {
    let (_td, path) = fresh_env_dir();

    {
        let env = open_env(&path, false);
        let db = open_db(&env, "list_new_reopen");
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);
        list.push(None, &"first".to_string()).unwrap();
        list.push(None, &"second".to_string()).unwrap();
        let _ = db.close();
    }
    {
        let env = open_env(&path, false);
        let db = open_db(&env, "list_new_reopen");
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);
        assert_eq!(list.next_index(), 0);
        let idx = list.push(None, &"clobber".to_string()).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(list.get(None, 0).unwrap(), Some("clobber".to_string()));
    }
}

#[test]
fn stored_list_open_on_empty_database_starts_at_zero() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, false);
    let db = open_db(&env, "list_empty");
    let list: StoredList<'_, String, _> =
        StoredList::open(&db, StringBinding).unwrap();
    assert_eq!(list.next_index(), 0);
    assert_eq!(list.push(None, &"x".to_string()).unwrap(), 0);
    assert_eq!(list.push(None, &"y".to_string()).unwrap(), 1);
}

#[test]
fn stored_list_open_rejects_mixed_use_database() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, false);
    let db = open_db(&env, "list_mixed");

    let key = DatabaseEntry::from_bytes(b"not-an-index");
    let val = DatabaseEntry::from_bytes(b"v");
    db.put(key.data(), val.data()).unwrap();

    let err = StoredList::<String, _>::open(&db, StringBinding)
        .err()
        .expect("open must fail");
    match err {
        CollectionError::IllegalState(msg) => {
            assert!(msg.contains("StoredList::open"));
        }
        other => panic!("expected IllegalState, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Audit findings #11 / #12 -- typed StoredMap<K, V> / StoredList<V> surface.
// ---------------------------------------------------------------------------

/// The typed surface compiles: a `StoredMap<i32, String, ...>` and a
/// `StoredList<String, ...>` round-trip Rust types via the configured
/// bindings, with no `&[u8]` shenanigans on the user-facing API.
#[test]
fn typed_storedmap_round_trip_by_value() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, true);
    let db = open_db(&env, "typed_round_trip");
    let map: StoredMap<'_, i32, String, _, _> =
        StoredMap::new(&db, IntBinding, StringBinding);

    map.put(None, &42, &"the answer".to_string()).unwrap();
    let value: Option<String> = map.get(None, &42).unwrap();
    assert_eq!(value, Some("the answer".to_string()));
}

#[test]
fn typed_storedlist_round_trip_by_value() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, true);
    let db = open_db(&env, "typed_list_round_trip");
    let list: StoredList<'_, String, _> = StoredList::new(&db, StringBinding);

    list.push(None, &"hello".to_string()).unwrap();
    list.push(None, &"world".to_string()).unwrap();
    let v: Option<String> = list.get(None, 0).unwrap();
    assert_eq!(v, Some("hello".to_string()));
}

/// `ByteArrayBinding` lets users emulate the v1.5 byte-keyed surface
/// when they really want raw bytes — the rest of the typed API
/// works unchanged.
#[test]
fn typed_storedmap_byte_array_binding_emulates_legacy() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, false);
    let db = open_db(&env, "byte_legacy");
    let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
        StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);

    map.put(None, &b"hello".to_vec(), &b"world".to_vec()).unwrap();
    assert_eq!(
        map.get(None, &b"hello".to_vec()).unwrap(),
        Some(b"world".to_vec()),
    );
}
