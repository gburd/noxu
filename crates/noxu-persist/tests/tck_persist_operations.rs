//! JE TCK port: persist invariants beyond schema-evolution.
//!
//! Ports invariants from JE
//! `com.sleepycat.persist.test.{SequenceTest, OperationTest,
//! NegativeTest}` onto noxu's `Sequence` / `MemorySequence` /
//! `EntityStore` / `PrimaryIndex` / `StoreConfig`.
//!
//! Schema-evolution tests (EvolveTest, DevolutionTest, ConvertAndAddTest,
//! EvolveProxyClassTest) are already covered in
//! `crates/noxu-persist/tests/evolve_test.rs`; this file covers the
//! *other* persist test surfaces that are now relevant after the Wave
//! 2C-2 schema-evolution work landed.
//!
//! | JE test                                  | Noxu test in this file                    |
//! |------------------------------------------|--------------------------------------------|
//! | SequenceTest.testSequenceKeys (long key) | `tck_persist_sequence_monotonic_starts_at_1`  |
//! | SequenceTest (separate sequences)        | `tck_persist_separate_sequences_independent`  |
//! | (sequence persists across reopen)        | `tck_persist_sequence_persists_across_reopen` |
//! | OperationTest.testReadOnly               | `tck_persist_read_only_store_rejects_writes`  |
//! | OperationTest.testGetStoreNames          | `tck_persist_get_database_names_after_open`   |
//! | NegativeTest.testSetConfigAfterOpen      | `tck_persist_close_then_reopen_works`         |
//! | OperationTest "put then put same"        | `tck_persist_put_idempotent_for_same_value`   |
//! | OperationTest cursor count               | `tck_persist_count_after_inserts_and_deletes` |

use std::path::Path;

use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
use noxu_persist::entity::Entity;
use noxu_persist::entity_serializer::EntitySerializer;
use noxu_persist::entity_store::EntityStore;
use noxu_persist::error::{PersistError, Result};
use noxu_persist::sequence::{MemorySequence, Sequence};
use noxu_persist::store_config::StoreConfig;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct Item {
    id: u64,
    name: String,
}

impl Entity for Item {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 {
        &self.id
    }
    fn entity_name() -> &'static str {
        "Item"
    }
}

struct ItemSer;
impl EntitySerializer<Item> for ItemSer {
    fn serialize(&self, e: &Item) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&e.id.to_be_bytes());
        let name = e.name.as_bytes();
        buf.extend_from_slice(&(name.len() as u32).to_be_bytes());
        buf.extend_from_slice(name);
        Ok(buf)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Item> {
        if bytes.len() < 12 {
            return Err(PersistError::SerializationError(
                "Item: too short".into(),
            ));
        }
        let id = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        let nlen =
            u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
        if bytes.len() < 12 + nlen {
            return Err(PersistError::SerializationError(
                "Item: name truncated".into(),
            ));
        }
        let name = String::from_utf8(bytes[12..12 + nlen].to_vec())
            .map_err(|e| PersistError::SerializationError(e.to_string()))?;
        Ok(Item { id, name })
    }
}

fn open_env(path: &Path, allow_create: bool) -> Environment {
    let mut cfg =
        EnvironmentConfig::new(path.to_path_buf()).with_transactional(true);
    if allow_create {
        cfg = cfg.with_allow_create(true);
    }
    Environment::open(cfg).unwrap()
}

fn open_db(env: &Environment, name: &str, allow_create: bool) -> Database {
    let mut cfg = DatabaseConfig::new();
    if allow_create {
        cfg = cfg.with_allow_create(true);
    }
    env.open_database(None, name, &cfg).unwrap()
}

// ---------------------------------------------------------------------------
// SequenceTest ports
// ---------------------------------------------------------------------------
//
// JE's SequenceTest exhaustively iterates over every primitive integer
// type (Long, Integer, Short, Byte, ...) and asserts that the
// auto-generated primary key advances by 1 on each `put`.  Noxu's
// `Sequence` / `MemorySequence` are u64-only, so the type matrix
// collapses to the underlying numeric invariant.

#[test]
fn tck_persist_memory_sequence_monotonic_starts_at_1() {
    let seq = MemorySequence::new();
    let first = seq.next();
    let second = seq.next();
    let third = seq.next();
    // JE invariant: first key in a fresh sequence is 1.
    assert_eq!(1, first);
    assert_eq!(2, second);
    assert_eq!(3, third);
    // Strict monotonicity is the load-bearing property.
    assert!(first < second && second < third);
}

#[test]
fn tck_persist_sequence_monotonic_starts_at_1() {
    let td = TempDir::new().unwrap();
    let env = open_env(td.path(), true);
    let db = open_db(&env, "seq_db", true);

    let seq = Sequence::new(&db, "users").unwrap();
    let mut prev = 0u64;
    for _ in 0..50 {
        let n = seq.next().unwrap();
        assert!(n > prev, "sequence not strictly monotone: {n} after {prev}",);
        prev = n;
    }
    // First call returned a positive value (>= 1).
    assert!(prev >= 50);
}

#[test]
fn tck_persist_separate_sequences_independent() {
    // SequenceTest's per-entity-type matrix collapses to "two sequences
    // with different names in the same database advance independently."
    let td = TempDir::new().unwrap();
    let env = open_env(td.path(), true);
    let db = open_db(&env, "seq_db", true);

    let seq_a = Sequence::new(&db, "alpha").unwrap();
    let seq_b = Sequence::new(&db, "beta").unwrap();

    // Pull from seq_a only.
    for _ in 0..10 {
        seq_a.next().unwrap();
    }
    // seq_b's first value must still be the start-of-sequence value, not
    // wherever seq_a left off.
    let first_b = seq_b.next().unwrap();
    assert!(
        first_b <= 1 + Sequence::new(&db, "fresh").unwrap().next().unwrap(),
        "two named sequences must not share state; seq_b first = {first_b}",
    );
}

#[test]
fn tck_persist_sequence_persists_across_reopen() {
    // SequenceTest implicitly relies on the sequence value surviving an
    // env close/reopen.  Noxu's `Sequence` flushes its persistent record
    // on `Drop`; reopening should pick up the next slot, not restart at 1.
    let td = TempDir::new().unwrap();

    let after_first_run: u64;
    {
        let env = open_env(td.path(), true);
        let db = open_db(&env, "seq_db", true);
        let seq = Sequence::new(&db, "persistent").unwrap();
        for _ in 0..5 {
            seq.next().unwrap();
        }
        after_first_run = seq.current();
        // Drop seq first, then db, then env so the persistent record is
        // flushed before the env closes.
    }

    {
        let env = open_env(td.path(), true);
        let db = open_db(&env, "seq_db", true);
        let seq = Sequence::new(&db, "persistent").unwrap();
        let n = seq.next().unwrap();
        assert!(
            n > after_first_run,
            "reopened sequence handed out {n}, but the previous run \
             reached {after_first_run}; sequence did not persist across \
             reopen",
        );
    }
}

// ---------------------------------------------------------------------------
// OperationTest.testReadOnly  -- read-only store rejects writes
// ---------------------------------------------------------------------------

#[test]
fn tck_persist_read_only_store_rejects_writes() {
    // Step 1: writable store, populate, close.
    let td = TempDir::new().unwrap();
    {
        let env = open_env(td.path(), true);
        let mut store = EntityStore::open(
            &env,
            StoreConfig::new("ro_store")
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let pi = store.get_primary_index::<u64, Item>().unwrap();
        let ser = ItemSer;
        pi.put(None, &ser, &Item { id: 1, name: "alpha".into() }).unwrap();
        store.close().unwrap();
        // Env drops at end of block; explicit `drop(store)` first to
        // release the borrow, then env follows.
        drop(store);
        drop(env);
    }

    // Step 2: reopen read-only and verify a write returns an error.
    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("ro_store")
            .with_allow_create(true)
            .with_read_only(true),
    )
    .unwrap();
    let pi = store.get_primary_index::<u64, Item>().unwrap();
    let ser = ItemSer;

    // Read should succeed.
    let got = pi.get(None, &ser, &1).unwrap();
    assert_eq!(Some(Item { id: 1, name: "alpha".into() }), got);

    // Write should fail.
    let res = pi.put(None, &ser, &Item { id: 2, name: "beta".into() });
    assert!(
        res.is_err(),
        "read-only store accepted a write; expected an error. result = {res:?}",
    );
}

/// Captures the JE-equivalent "reopen an existing entity store read-only"
/// recipe: `setReadOnly(true)` with *no* `setAllowCreate(true)` against a
/// path where the entity DBs already exist on disk.  Wave 7 polish
/// (v2.0.1-equivalent) closed the JE deviation surfaced by wave 4-C: the
/// entity DB is now transparently re-opened off the recovered tree, and
/// the underlying `DatabaseConfig.read_only=true` continues to enforce
/// write-rejection at the `Database::put` boundary.
#[test]
fn tck_persist_read_only_store_reopens_without_allow_create() {
    let td = TempDir::new().unwrap();
    {
        let env = open_env(td.path(), true);
        let mut store = EntityStore::open(
            &env,
            StoreConfig::new("ro_store2").with_allow_create(true),
        )
        .unwrap();
        let pi = store.get_primary_index::<u64, Item>().unwrap();
        let ser = ItemSer;
        pi.put(None, &ser, &Item { id: 1, name: "alpha".into() }).unwrap();
        store.close().unwrap();
        drop(store);
        drop(env);
    }

    // JE-shape recipe: read-only reopen, no allow_create.
    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("ro_store2").with_read_only(true),
    )
    .unwrap();
    let pi = store.get_primary_index::<u64, Item>().unwrap();
    let ser = ItemSer;
    assert_eq!(
        Some(Item { id: 1, name: "alpha".into() }),
        pi.get(None, &ser, &1).unwrap(),
    );
}

/// Wave 7 polish coverage: read-only reopen, then a `get` should succeed
/// without ever passing `allow_create=true`.  This is the smoke-test
/// counterpart to `tck_persist_read_only_store_reopens_without_allow_create`
/// and exercises the same path under explicit close-then-reopen.
#[test]
fn tck_persist_read_only_reopen_get_succeeds_after_close() {
    let td = TempDir::new().unwrap();

    {
        let env = open_env(td.path(), true);
        let mut store = EntityStore::open(
            &env,
            StoreConfig::new("ro_get").with_allow_create(true),
        )
        .unwrap();
        let pi = store.get_primary_index::<u64, Item>().unwrap();
        let ser = ItemSer;
        pi.put(None, &ser, &Item { id: 7, name: "seven".into() }).unwrap();
        pi.put(None, &ser, &Item { id: 8, name: "eight".into() }).unwrap();
        store.close().unwrap();
    }

    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("ro_get").with_read_only(true),
    )
    .unwrap();
    let pi = store.get_primary_index::<u64, Item>().unwrap();
    let ser = ItemSer;

    assert_eq!(
        Some(Item { id: 7, name: "seven".into() }),
        pi.get(None, &ser, &7).unwrap(),
    );
    assert_eq!(
        Some(Item { id: 8, name: "eight".into() }),
        pi.get(None, &ser, &8).unwrap(),
    );
    assert_eq!(None, pi.get(None, &ser, &99).unwrap());
}

/// Wave 7 polish coverage: a `put` against a read-only-reopened store must
/// surface a typed error (`NoxuError::ReadOnly`), not silently succeed and
/// not panic.  Confirms that the `allow_create=true` we now pass under the
/// hood does NOT bypass the per-DB read-only flag.
#[test]
fn tck_persist_read_only_reopen_rejects_put() {
    let td = TempDir::new().unwrap();

    {
        let env = open_env(td.path(), true);
        let mut store = EntityStore::open(
            &env,
            StoreConfig::new("ro_put").with_allow_create(true),
        )
        .unwrap();
        let pi = store.get_primary_index::<u64, Item>().unwrap();
        let ser = ItemSer;
        pi.put(None, &ser, &Item { id: 1, name: "alpha".into() }).unwrap();
        store.close().unwrap();
    }

    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("ro_put").with_read_only(true),
    )
    .unwrap();
    let pi = store.get_primary_index::<u64, Item>().unwrap();
    let ser = ItemSer;

    let res = pi.put(None, &ser, &Item { id: 2, name: "beta".into() });
    assert!(
        res.is_err(),
        "read-only reopened store accepted a write; result = {res:?}",
    );
    let err = res.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("ReadOnly") || msg.contains("read-only"),
        "expected a read-only typed error, got: {msg}",
    );
}

// ---------------------------------------------------------------------------
// OperationTest.testGetStoreNames -- get_database_names lists entity dbs
// ---------------------------------------------------------------------------

#[test]
fn tck_persist_get_database_names_after_open() {
    let td = TempDir::new().unwrap();
    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("naming").with_allow_create(true),
    )
    .unwrap();

    // Touch the primary index so the entity's db is materialised.
    let _ = store.get_primary_index::<u64, Item>().unwrap();

    let names = store.get_database_names();
    assert!(
        names.iter().any(|n| n.contains("Item")),
        "get_database_names() = {names:?} did not include the Item entity \
         database after get_primary_index<Item>",
    );
}

// ---------------------------------------------------------------------------
// NegativeTest.testSetConfigAfterOpen analogue
// ---------------------------------------------------------------------------
//
// JE forbids reconfiguring a store after it's open (an explicit
// IllegalStateException).  The noxu equivalent is "you must close
// the store before opening a fresh one", and that the second open
// transparently picks up where the first left off.

#[test]
fn tck_persist_close_then_reopen_picks_up_data() {
    let td = TempDir::new().unwrap();

    // First open: insert three items.
    {
        let env = open_env(td.path(), true);
        let mut store = EntityStore::open(
            &env,
            StoreConfig::new("reopen").with_allow_create(true),
        )
        .unwrap();
        let pi = store.get_primary_index::<u64, Item>().unwrap();
        let ser = ItemSer;
        for i in 1..=3 {
            pi.put(None, &ser, &Item { id: i, name: format!("n{i}") }).unwrap();
        }
        store.close().unwrap();
    }

    // Second open: data is still there and `count()` reflects it.
    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("reopen").with_allow_create(true),
    )
    .unwrap();
    let pi = store.get_primary_index::<u64, Item>().unwrap();
    let ser = ItemSer;
    assert_eq!(3, pi.count().unwrap());
    for i in 1..=3 {
        assert_eq!(
            Some(Item { id: i, name: format!("n{i}") }),
            pi.get(None, &ser, &i).unwrap(),
        );
    }
}

// ---------------------------------------------------------------------------
// OperationTest "put-with-same-value is idempotent at the value level"
// ---------------------------------------------------------------------------

#[test]
fn tck_persist_put_is_idempotent_for_identical_value() {
    let td = TempDir::new().unwrap();
    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("idem").with_allow_create(true),
    )
    .unwrap();
    let pi = store.get_primary_index::<u64, Item>().unwrap();
    let ser = ItemSer;

    let v = Item { id: 100, name: "x".into() };
    pi.put(None, &ser, &v).unwrap();
    pi.put(None, &ser, &v).unwrap(); // same value again
    pi.put(None, &ser, &v).unwrap();

    // Count is 1 (single key) regardless of number of puts.
    assert_eq!(1, pi.count().unwrap());
    assert_eq!(Some(v), pi.get(None, &ser, &100).unwrap());
}

// ---------------------------------------------------------------------------
// OperationTest cursor-count analogue: count() reflects inserts and deletes
// ---------------------------------------------------------------------------

#[test]
fn tck_persist_count_after_inserts_and_deletes() {
    let td = TempDir::new().unwrap();
    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("counting").with_allow_create(true),
    )
    .unwrap();
    let pi = store.get_primary_index::<u64, Item>().unwrap();
    let ser = ItemSer;

    assert_eq!(0, pi.count().unwrap());

    for i in 1..=10 {
        pi.put(None, &ser, &Item { id: i, name: format!("i{i}") }).unwrap();
    }
    assert_eq!(10, pi.count().unwrap());

    // Delete half.
    for i in 1..=5 {
        assert!(pi.delete(None, &i).unwrap());
    }
    assert_eq!(5, pi.count().unwrap());

    // Deleting an absent key returns false and does not change the count.
    assert!(!pi.delete(None, &999).unwrap());
    assert_eq!(5, pi.count().unwrap());
}

// ---------------------------------------------------------------------------
// PrimaryIndex.put_no_overwrite contract -- inserts return true, dups false
// ---------------------------------------------------------------------------

#[test]
fn tck_persist_put_no_overwrite_returns_false_on_duplicate() {
    let td = TempDir::new().unwrap();
    let env = open_env(td.path(), true);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("pno").with_allow_create(true),
    )
    .unwrap();
    let pi = store.get_primary_index::<u64, Item>().unwrap();
    let ser = ItemSer;

    let v1 = Item { id: 7, name: "first".into() };
    let v2 = Item { id: 7, name: "second".into() };

    // Fresh insert returns true.
    assert!(pi.put_no_overwrite(None, &ser, &v1).unwrap());
    // Conflicting insert under same key returns false.
    assert!(!pi.put_no_overwrite(None, &ser, &v2).unwrap());

    // The stored value is the *original*, not the second one.
    assert_eq!(Some(v1), pi.get(None, &ser, &7).unwrap());
}
