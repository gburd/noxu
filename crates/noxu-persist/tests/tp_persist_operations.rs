//! Additional faithful DPL ports from BDB-JE's
//! `persist/test/OperationTest.java` that are not already covered by
//! `noxu_persist_tests.rs`.
//!
//! ## Mapping to JE
//!
//! | JE                                     | Noxu                              |
//! |----------------------------------------|-----------------------------------|
//! | `OperationTest.testDeleteFromSubIndex` | `operation_test_delete_from_sub_index` |
//! | `OperationTest.testSecondaryBulkLoad*` (populate on open) | `operation_test_secondary_populated_on_open` |

use std::sync::Arc;

use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::{
    Entity, EntitySerializer, EntityStore, PrimaryIndex, Result,
    SecondaryIndex, StoreConfig,
};
use tempfile::TempDir;

// MyEntity: int primary key + one MANY_TO_ONE Integer secondary key,
// mirroring OperationTest.MyEntity.
#[derive(Clone, Debug, PartialEq)]
struct MyEntity {
    pri_key: i32,
    sec_key: i32,
}

impl Entity for MyEntity {
    type PrimaryKey = i32;
    fn primary_key(&self) -> &i32 {
        &self.pri_key
    }
    fn entity_name() -> &'static str {
        "MyEntity"
    }
}

struct MyEntitySer;
impl EntitySerializer<MyEntity> for MyEntitySer {
    fn serialize(&self, e: &MyEntity) -> Result<Vec<u8>> {
        let mut b = e.pri_key.to_be_bytes().to_vec();
        b.extend_from_slice(&e.sec_key.to_be_bytes());
        Ok(b)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<MyEntity> {
        let pri_key = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let sec_key = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
        Ok(MyEntity { pri_key, sec_key })
    }
}

fn open() -> (TempDir, Environment) {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true),
    )
    .unwrap();
    (td, env)
}

/// Faithful port of `OperationTest.testDeleteFromSubIndex`.
///
/// JE inserts four entities {1,2,3,4} all sharing secKey==1, obtains
/// `secIndex.subIndex(1)`, verifies each primary key is reachable through
/// the sub-index, deletes primary key 1 through the sub-index, and iterates
/// the sub-index deleting priKey 3 mid-cursor.  The invariant asserted at
/// the end: priKeys {1,3} are gone, {2,4} remain.
///
/// noxu-persist's DPL exposes the sub-index as `SecondaryIndex::sub_index`
/// (the duplicate run of primary keys for one secondary value) rather than a
/// typed `EntityIndex` view; deletion of an individual member is done through
/// the primary index (the DPL-idiomatic way — sub-index membership follows
/// the primary).  The asserted invariant is identical to JE's.
///
/// JE: `com.sleepycat.persist.test.OperationTest.testDeleteFromSubIndex`
#[test]
fn operation_test_delete_from_sub_index() {
    let (_td, env) = open();
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("test").with_allow_create(true),
    )
    .unwrap();
    let ser = Arc::new(MyEntitySer);
    let mut pri: PrimaryIndex<i32, MyEntity> =
        store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> = store
        .open_secondary_index(&mut pri, "secKey", Arc::clone(&ser), {
            |e: &MyEntity| Some(e.sec_key)
        })
        .unwrap();

    // Four entities, all with secKey == 1.
    for pk in 1..=4 {
        pri.put(None, ser.as_ref(), &MyEntity { pri_key: pk, sec_key: 1 })
            .unwrap();
    }

    // subIndex(1) reaches every primary key {1,2,3,4}; priKey 5 is absent.
    let mut sub = sec.sub_index(&1);
    sub.sort_unstable();
    assert_eq!(sub, vec![1, 2, 3, 4]);
    assert!(pri.get(None, ser.as_ref(), &1).unwrap().is_some());
    assert!(pri.get(None, ser.as_ref(), &2).unwrap().is_some());
    assert!(pri.get(None, ser.as_ref(), &3).unwrap().is_some());
    assert!(pri.get(None, ser.as_ref(), &5).unwrap().is_none());

    // Delete primary key 1 (JE: subIndex.delete(txn, 1)).
    assert!(pri.delete(None, &1).unwrap());
    assert!(pri.get(None, ser.as_ref(), &1).unwrap().is_none());
    assert!(pri.get(None, ser.as_ref(), &2).unwrap().is_some());

    // Iterate the sub-index deleting priKey 3 (JE: cursor.delete() on 3),
    // and confirm priKey 4 is still seen during the scan.
    let mut saw4 = false;
    for pk in sec.sub_index(&1) {
        if pk == 3 {
            assert!(pri.delete(None, &pk).unwrap());
        }
        if pk == 4 {
            saw4 = true;
        }
    }
    assert!(saw4, "priKey 4 must be visited in the sub-index scan");

    // Final invariant: {1,3} gone, {2,4} remain (JE's closing asserts).
    assert!(pri.get(None, ser.as_ref(), &1).unwrap().is_none());
    assert!(pri.get(None, ser.as_ref(), &3).unwrap().is_none());
    assert!(pri.get(None, ser.as_ref(), &2).unwrap().is_some());
    assert!(pri.get(None, ser.as_ref(), &4).unwrap().is_some());
    let mut remaining = sec.sub_index(&1);
    remaining.sort_unstable();
    assert_eq!(remaining, vec![2, 4]);
}

/// Faithful port of the portable core of
/// `OperationTest.testSecondaryBulkLoad1/2`: a secondary index opened
/// **after** the primary is already populated is back-filled (populated)
/// from the existing primary records when it is first opened.
///
/// JE's bulk-load test is largely about `StoreConfig.setSecondaryBulkLoad`
/// deferring secondary creation + the foreign-key constraint interplay
/// (constraint enforcement is a separate, not-yet-wired DPL feature — see
/// the report's N/A notes on `on_related_entity_delete`).  The portable,
/// DPL-observable behavior — "opening a secondary over an already-populated
/// primary populates the secondary" — is asserted here.
///
/// JE: `com.sleepycat.persist.test.OperationTest.testSecondaryBulkLoad1`
///     (secondary population half)
#[test]
fn operation_test_secondary_populated_on_open() {
    let (_td, env) = open();
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("bulk").with_allow_create(true),
    )
    .unwrap();
    let ser = Arc::new(MyEntitySer);

    // Populate the primary FIRST, with no secondary open.
    {
        let pri: PrimaryIndex<i32, MyEntity> =
            store.get_primary_index().unwrap();
        for pk in 1..=5 {
            pri.put(
                None,
                ser.as_ref(),
                &MyEntity { pri_key: pk, sec_key: pk % 2 },
            )
            .unwrap();
        }
    }

    // Now open the secondary — it must be back-filled from the 5 existing
    // primary records (JE's allow_populate on first open).
    let mut pri: PrimaryIndex<i32, MyEntity> =
        store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> = store
        .open_secondary_index(&mut pri, "secKey", Arc::clone(&ser), {
            |e: &MyEntity| Some(e.sec_key)
        })
        .unwrap();

    // secKey 0 -> {2,4}; secKey 1 -> {1,3,5}
    let mut zero = sec.sub_index(&0);
    zero.sort_unstable();
    assert_eq!(zero, vec![2, 4], "secondary must be populated on open");
    let mut one = sec.sub_index(&1);
    one.sort_unstable();
    assert_eq!(one, vec![1, 3, 5]);
}
