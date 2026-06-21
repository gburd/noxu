//! JE TCK port: collections semantics.
//!
//! Ports invariants from JE
//! `com.sleepycat.collections.test.CollectionTest`,
//! `NullValueTest`, `IterRepositionTest`, and
//! `KeyRangeTest` onto noxu's typed `StoredMap` /
//! `StoredSortedMap` / `StoredKeySet` / `StoredValueSet`.
//!
//! Focus is on invariants not already covered by `collection_tests.rs`
//! and `wave2b_tests.rs` in this crate:
//!
//! - `java.util.Map.put` contract: returns `Some(old)` on overwrite,
//!   `None` on insert.
//! - `Map.remove(absent)` returns `None`; `Map.remove(present)`
//!   returns `Some(old)`.
//! - Iterator-after-mutation: a snapshot taken before the mutation
//!   reflects the pre-mutation state (noxu's Stored* iterators are
//!   block-snapshots, not live cursors, so this is the documented
//!   contract — see `StoredIterator` docs).
//! - Submap-style range scans: `iter_from(k)` starts at first key
//!   `>= k` and skips earlier ones (KeyRangeTest invariant).
//! - "Null value" support via `Option<T>` value bindings (NullValueTest).
//! - Transaction abort hides writes from a reader that opens after
//!   the abort; this is the JE collections-level analogue of
//!   `TransactionTest`.

use noxu_bind::{IntBinding, SerdeBinding};
use noxu_collections::{StoredMap, StoredSortedMap, TransactionRunner};
use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn open_env(transactional: bool) -> (TempDir, Environment, Database) {
    let td = TempDir::new().unwrap();
    let mut cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    if transactional {
        cfg = cfg.with_transactional(true);
    }
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(
            None,
            "tckdb",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (td, env, db)
}

// ---------------------------------------------------------------------------
// JE Map.put contract — port of CollectionTest.testCreation/testUnindexed
// ---------------------------------------------------------------------------
//
// `java.util.Map.put(k, v)` returns the previous value associated with k,
// or null if there was none.  The JE StoredMap honours this contract.

#[test]
fn tck_collection_map_put_returns_previous_value_on_overwrite() {
    let (_td, _env, db) = open_env(false);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());

    // First insert: no previous value.
    assert_eq!(None, map.put(None, &1, &100).unwrap());
    // Overwrite: returns the old value.
    assert_eq!(Some(100), map.put(None, &1, &200).unwrap());
    // Final state.
    assert_eq!(Some(200), map.get(None, &1).unwrap());
}

#[test]
fn tck_collection_map_remove_returns_previous_value() {
    let (_td, _env, db) = open_env(false);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());

    map.put(None, &7, &42).unwrap();

    // Removing an absent key returns None.
    assert_eq!(None, map.remove(None, &999).unwrap());
    // Removing a present key returns the old value.
    assert_eq!(Some(42), map.remove(None, &7).unwrap());
    // Subsequent lookup is None.
    assert_eq!(None, map.get(None, &7).unwrap());
}

// ---------------------------------------------------------------------------
// Iteration order — port of CollectionTest "iter sorted by key"
// ---------------------------------------------------------------------------

#[test]
fn tck_collection_iteration_yields_keys_in_sorted_order() {
    let (_td, _env, db) = open_env(false);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());

    // Insert in scrambled order to defeat insertion-order iteration.
    for &k in &[5, 1, 9, 3, 7, 2, 8, 4, 6] {
        map.put(None, &k, &(k * 10)).unwrap();
    }

    let pairs: Vec<(i32, i32)> =
        map.iter(None).unwrap().map(Result::unwrap).collect();
    let expected: Vec<(i32, i32)> = (1..=9).map(|k| (k, k * 10)).collect();
    assert_eq!(expected, pairs);
}

// ---------------------------------------------------------------------------
// Iterator snapshot semantics — JE BlockIterator-style behaviour
// ---------------------------------------------------------------------------
//
// Noxu's StoredIterator is a snapshot taken at construction time.
// The JE BlockIterator (default Stored* iterator) caches blocks of
// records.  In both implementations, a mutation made *after* the
// iterator is constructed is invisible to that iterator.

#[test]
fn tck_collection_iterator_is_a_snapshot_of_construction_time() {
    let (_td, _env, db) = open_env(false);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());
    for k in 1..=5 {
        map.put(None, &k, &(k * 10)).unwrap();
    }

    let snapshot = map.iter(None).unwrap();

    // Mutate after constructing the iterator.
    map.put(None, &6, &60).unwrap();
    map.remove(None, &3).unwrap();

    // The snapshot reflects the pre-mutation state.
    let pairs: Vec<(i32, i32)> = snapshot.map(Result::unwrap).collect();
    assert_eq!(vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)], pairs,);
}

// ---------------------------------------------------------------------------
// Submap / iter_from — port of KeyRangeTest.testSubRanges
// ---------------------------------------------------------------------------

#[test]
fn tck_collection_iter_from_starts_at_or_after_key() {
    let (_td, _env, db) = open_env(false);
    let map: StoredSortedMap<'_, i32, i32, _, _> =
        StoredSortedMap::new(&db, IntBinding::new(), IntBinding::new());

    for k in (1..=10).step_by(2) {
        // 1, 3, 5, 7, 9
        map.put(None, &k, &(k * 100)).unwrap();
    }

    // iter_from(4) should start at the first key >= 4, i.e. 5.
    let from_4: Vec<(i32, i32)> =
        map.iter_from(None, &4).unwrap().map(Result::unwrap).collect();
    assert_eq!(
        vec![(5, 500), (7, 700), (9, 900)],
        from_4,
        "iter_from must skip keys < 4",
    );

    // iter_from(5) starts exactly at 5 (inclusive).
    let from_5: Vec<(i32, i32)> =
        map.iter_from(None, &5).unwrap().map(Result::unwrap).collect();
    assert_eq!(vec![(5, 500), (7, 700), (9, 900)], from_5);

    // iter_from(beyond all) is empty.
    let from_99: Vec<(i32, i32)> =
        map.iter_from(None, &99).unwrap().map(Result::unwrap).collect();
    assert!(from_99.is_empty());
}

#[test]
fn tck_collection_iter_reverse_yields_descending_order() {
    let (_td, _env, db) = open_env(false);
    let map: StoredSortedMap<'_, i32, i32, _, _> =
        StoredSortedMap::new(&db, IntBinding::new(), IntBinding::new());

    for k in 1..=5 {
        map.put(None, &k, &(k * 10)).unwrap();
    }

    let rev: Vec<(i32, i32)> =
        map.iter_reverse(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(vec![(5, 50), (4, 40), (3, 30), (2, 20), (1, 10)], rev);
}

// ---------------------------------------------------------------------------
// Null-value support — port of NullValueTest
// ---------------------------------------------------------------------------
//
// JE: `SerialBinding(catalog, null /*baseClass*/)` allows storing null
// values; `SerialBinding(catalog, String.class)` does not.
// Noxu equivalent: a `SerdeBinding<Option<T>>` allows both `Some(v)`
// and `None` as values; `SerdeBinding<T>` (no Option) cannot represent
// "null" because the type system rejects it at the call site.

#[test]
fn tck_collection_null_values_round_trip_via_option() {
    let (_td, _env, db) = open_env(false);
    let map: StoredMap<'_, i32, Option<String>, _, _> = StoredMap::new(
        &db,
        IntBinding::new(),
        SerdeBinding::<Option<String>>::new(),
    );

    map.put(None, &1, &None).unwrap();
    assert_eq!(Some(None), map.get(None, &1).unwrap());

    map.put(None, &2, &Some("hello".to_string())).unwrap();
    assert_eq!(Some(Some("hello".to_string())), map.get(None, &2).unwrap(),);

    // values() iterator yields None for the null entry.
    let vals: Vec<Option<String>> =
        map.values(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(vec![None, Some("hello".to_string())], vals);
}

// ---------------------------------------------------------------------------
// Transaction abort visibility — port of CollectionTest TXN/abort flows
// ---------------------------------------------------------------------------

#[test]
fn tck_collection_aborted_writes_are_invisible_after_abort() {
    let (_td, env, db) = open_env(true);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());

    // Pre-load one record under auto-commit so we can verify it
    // survives the abort.
    map.put(None, &1, &10).unwrap();

    // Manual user-controlled txn: write then abort.
    let txn = env.begin_transaction(None).unwrap();
    map.put(Some(&txn), &2, &20).unwrap();
    map.put(Some(&txn), &1, &999).unwrap(); // overwrite
    txn.abort().unwrap();

    // Auto-commit reader sees only the pre-abort state.
    assert_eq!(Some(10), map.get(None, &1).unwrap()); // overwrite rolled back
    assert_eq!(None, map.get(None, &2).unwrap()); // insert rolled back
}

#[test]
fn tck_collection_committed_writes_are_visible_after_commit() {
    let (_td, env, db) = open_env(true);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());

    let txn = env.begin_transaction(None).unwrap();
    map.put(Some(&txn), &42, &4242).unwrap();
    txn.commit().unwrap();

    assert_eq!(Some(4242), map.get(None, &42).unwrap());
}

// ---------------------------------------------------------------------------
// TransactionRunner — port of CollectionTest's TransactionWorker pattern
// ---------------------------------------------------------------------------

#[test]
fn tck_collection_transaction_runner_commits_on_ok() {
    let (_td, env, db) = open_env(true);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());

    let runner = TransactionRunner::new(&env);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls_inner = std::sync::Arc::clone(&calls);

    runner
        .run(|txn| {
            calls_inner.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            map.put(Some(txn), &1, &11)?;
            map.put(Some(txn), &2, &22)?;
            Ok(())
        })
        .unwrap();

    // One successful invocation, no retry.
    assert_eq!(1, calls.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(Some(11), map.get(None, &1).unwrap());
    assert_eq!(Some(22), map.get(None, &2).unwrap());
}

#[test]
fn tck_collection_transaction_runner_rolls_back_on_err() {
    let (_td, env, db) = open_env(true);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());

    let runner = TransactionRunner::new(&env);
    let result: Result<(), noxu_collections::CollectionError> =
        runner.run(|txn| {
            map.put(Some(txn), &1, &11)?;
            Err(noxu_collections::CollectionError::ReadOnly) // arbitrary error
        });
    assert!(result.is_err());

    // Closure error => txn rolled back => map is empty.
    assert_eq!(None, map.get(None, &1).unwrap());
    assert_eq!(0, map.len(None).unwrap());
}

// ---------------------------------------------------------------------------
// Read-after-write visibility (single-thread, two cursors) — port of
// the JE "writer commit, reader observes" invariant from CollectionTest
// without the cross-thread complication.  noxu's `Database` is not
// `Sync`, so the JE "two threads sharing an env" pattern requires
// re-opening the database in each thread; the noxu engine intentionally
// rejects that today (`DatabaseAlreadyExists`).  The narrower
// invariant — auto-commit writes are immediately visible to a fresh
// auto-commit reader on the same handle — is what we port here.
// ---------------------------------------------------------------------------

#[test]
fn tck_collection_auto_commit_writes_are_immediately_visible() {
    let (_td, _env, db) = open_env(true);
    let map: StoredMap<'_, i32, i32, _, _> =
        StoredMap::new(&db, IntBinding::new(), IntBinding::new());

    map.put(None, &7, &777).unwrap();
    // Fresh auto-commit reader sees the value with no other action.
    assert_eq!(Some(777), map.get(None, &7).unwrap());

    map.put(None, &7, &888).unwrap();
    assert_eq!(Some(888), map.get(None, &7).unwrap());
}
