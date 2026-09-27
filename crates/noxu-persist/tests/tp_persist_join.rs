//! Faithful DPL port of BDB-JE's `persist/test/JoinTest.java`.
//!
//! JE's `JoinTest.testJoin` builds an `EntityJoin` over several
//! `SecondaryIndex`es of the same `EntityStore` and asserts that the join
//! returns exactly the primary keys (and entities) that satisfy **all** of
//! the equality conditions — an equi-join / set-intersection over the
//! secondary indexes.
//!
//! noxu-persist does not expose a typed `EntityJoin` wrapper.  The
//! DPL-idiomatic way to express the same equi-join is to intersect the
//! primary-key sets of each condition's `SecondaryIndex::sub_index(sk)`
//! (`sub_index` is the DPL analogue of JE `SecondaryIndex.subIndex(secKey)`
//! — the duplicate run of primary keys for one secondary value).  This port
//! asserts the SAME OUTCOME JE's `testJoin` asserts: for each
//! `(k1, k2, k3)` condition tuple, the join yields exactly the expected set
//! of primary keys.
//!
//! (A raw `noxu_db::Database::join` `JoinCursor` also exists, but the DPL
//! primary is a `Arc<Mutex<Database>>` and its secondary cursors read
//! *through* to the primary, so driving the low-level join while holding the
//! primary mutex deadlocks — the DPL has no lock-free join entry point.
//! The `sub_index`-intersection form below is the faithful, deadlock-free
//! expression of the same join semantics through the DPL's public API.)
//!
//! ## Mapping to JE
//!
//! | JE                          | Noxu                                       |
//! |-----------------------------|--------------------------------------------|
//! | `JoinTest.testJoin`         | `join_test_test_join_via_put`              |
//! | `JoinTest.testJoin` (faithful putNoOverwrite) | `join_test_test_join` (ignored, NEW-PNO-SEC-1) |
//! | `EntityJoin.addCondition(sec, k)` | `sec.sub_index(&k)` as a candidate set |
//! | `join.keys(...)` / `entities(...)` | intersection of the candidate sets   |
//! | condition absent (`-1`)     | omit that secondary from the intersection  |
//!
//! ## Engine-bug candidate found by this port — NEW-PNO-SEC-1
//!
//! JE's `JoinTest.testJoin` inserts its records with `putNoOverwrite`.
//! A faithful port using `PrimaryIndex::put_no_overwrite` reveals that the
//! DPL / `noxu_db` **`put_no_overwrite` write path does NOT maintain
//! registered secondary indexes** — `noxu_db::Database::put_no_overwrite_bytes`
//! fires triggers but omits the `secondaries.maintain(...)` fan-out that the
//! plain `put` path performs (`database.rs`: compare `put_bytes`'s
//! "associate()-style hook" block against `put_no_overwrite_bytes`).  Every
//! secondary index is therefore left empty after a `put_no_overwrite`, so
//! the join returns nothing.  In JE, `putNoOverwrite` maintains secondaries
//! exactly like `put`.
//!
//! The faithful test `join_test_test_join` is kept `#[ignore]`d with this
//! citation so the finding is not lost; `join_test_test_join_via_put`
//! (identical join, records inserted with `put`) passes and proves the join
//! semantics themselves are correct.

use std::collections::BTreeSet;
use std::sync::Arc;

use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::{
    Entity, EntitySerializer, EntityStore, PrimaryIndex, Result,
    SecondaryIndex, StoreConfig,
};
use tempfile::TempDir;

const N_RECORDS: i32 = 5;

// ---------------------------------------------------------------------------
// Entity mirroring JoinTest.MyEntity: an int primary key and three
// MANY_TO_ONE int secondary keys (k1, k2, k3).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct MyEntity {
    id: i32,
    k1: i32,
    k2: i32,
    k3: i32,
}

impl Entity for MyEntity {
    type PrimaryKey = i32;
    fn primary_key(&self) -> &i32 {
        &self.id
    }
    fn entity_name() -> &'static str {
        "MyEntity"
    }
}

struct MyEntitySerializer;

impl EntitySerializer<MyEntity> for MyEntitySerializer {
    fn serialize(&self, e: &MyEntity) -> Result<Vec<u8>> {
        let mut b = e.id.to_be_bytes().to_vec();
        b.extend_from_slice(&e.k1.to_be_bytes());
        b.extend_from_slice(&e.k2.to_be_bytes());
        b.extend_from_slice(&e.k3.to_be_bytes());
        Ok(b)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<MyEntity> {
        let id = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let k1 = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
        let k2 = i32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let k3 = i32::from_be_bytes(bytes[12..16].try_into().unwrap());
        Ok(MyEntity { id, k1, k2, k3 })
    }
}

/// Drives one `doJoin(k1, k2, k3, expectKeys)` case from JE's `JoinTest`:
/// the intersection of the present conditions' `sub_index` candidate sets.
///
/// A negative `k` means "don't include this condition" (JE's `-1` sentinel).
fn do_join(
    sec1: &SecondaryIndex<i32, i32, MyEntity>,
    sec2: &SecondaryIndex<i32, i32, MyEntity>,
    sec3: &SecondaryIndex<i32, i32, MyEntity>,
    k1: i32,
    k2: i32,
    k3: i32,
) -> Vec<i32> {
    let conditions: [(&SecondaryIndex<i32, i32, MyEntity>, i32); 3] =
        [(sec1, k1), (sec2, k2), (sec3, k3)];

    let mut acc: Option<BTreeSet<i32>> = None;
    for (sec, k) in conditions {
        if k < 0 {
            continue; // condition omitted
        }
        let candidates: BTreeSet<i32> = sec.sub_index(&k).into_iter().collect();
        acc = Some(match acc {
            None => candidates,
            Some(prev) => prev.intersection(&candidates).copied().collect(),
        });
    }
    // JE requires at least one condition; testJoin always supplies one.
    acc.unwrap_or_default().into_iter().collect()
}

/// The (k1, k2, k3, expected primary keys) matrix — exactly JE
/// `JoinTest.testJoin`'s `doJoin(...)` cases.  `-1` == condition omitted.
///
/// Data layout (from JE):
/// Primary keys: {   0,   1,   2,   3,   4 }
/// Secondary k1: { 0:0, 0:1, 0:2, 0:3, 0:4 }
/// Secondary k2: { 0:0, 1:1, 0:2, 1:3, 0:4 }
/// Secondary k3: { 0:0, 1:1, 2:2, 0:3, 1:4 }
const JOIN_CASES: &[(i32, i32, i32, &[i32])] = &[
    (0, 0, 0, &[0]),
    (0, 0, 1, &[4]),
    (0, 0, -1, &[0, 2, 4]),
    (-1, 1, 1, &[1]),
    (-1, 2, 2, &[]),
    (-1, -1, 2, &[2]),
];

fn assert_all_cases(
    sec1: &SecondaryIndex<i32, i32, MyEntity>,
    sec2: &SecondaryIndex<i32, i32, MyEntity>,
    sec3: &SecondaryIndex<i32, i32, MyEntity>,
) {
    for &(k1, k2, k3, expect) in JOIN_CASES {
        let found = do_join(sec1, sec2, sec3, k1, k2, k3);
        let want = expect.to_vec();
        assert_eq!(found, want, "join({}, {}, {}) mismatch", k1, k2, k3);
    }
}

/// FAITHFUL port of `JoinTest.testJoin`: records inserted with
/// `putNoOverwrite`, exactly as JE does.
///
/// IGNORED — currently fails because `put_no_overwrite` does not maintain
/// secondary indexes (see NEW-PNO-SEC-1 in the module docs / report).  The
/// join itself is correct; the secondaries are simply never populated on the
/// no-overwrite path, so every condition resolves to an empty set.  Keep
/// this test as the escalation anchor; un-ignore it once the engine
/// maintains secondaries on `put_no_overwrite`.
///
/// JE: `com.sleepycat.persist.test.JoinTest.testJoin`
#[test]
fn join_test_test_join() {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true),
    )
    .unwrap();
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("test").with_allow_create(true),
    )
    .unwrap();
    let ser = Arc::new(MyEntitySerializer);
    let mut primary: PrimaryIndex<i32, MyEntity> =
        store.get_primary_index().unwrap();
    let sec1 = store
        .open_secondary_index(&mut primary, "k1", Arc::clone(&ser), {
            |e: &MyEntity| Some(e.k1)
        })
        .unwrap();
    let sec2 = store
        .open_secondary_index(&mut primary, "k2", Arc::clone(&ser), {
            |e: &MyEntity| Some(e.k2)
        })
        .unwrap();
    let sec3 = store
        .open_secondary_index(&mut primary, "k3", Arc::clone(&ser), {
            |e: &MyEntity| Some(e.k3)
        })
        .unwrap();

    for i in 0..N_RECORDS {
        let e = MyEntity { id: i, k1: 0, k2: i % 2, k3: i % 3 };
        // JE: primary.putNoOverwrite(txn, e) == true
        assert!(primary.put_no_overwrite(None, ser.as_ref(), &e).unwrap());
    }

    assert_all_cases(&sec1, &sec2, &sec3);
}

/// Control / de-vacuum guard for `join_test_test_join`: identical join
/// matrix, but records inserted with `put` (which DOES maintain
/// secondaries).  Proves the DPL equi-join returns exactly the JE-expected
/// intersection for every case, including the `(-1, -1, 2) -> [2]` case.
///
/// JE: `com.sleepycat.persist.test.JoinTest.testJoin`
#[test]
fn join_test_test_join_via_put() {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true),
    )
    .unwrap();
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("test").with_allow_create(true),
    )
    .unwrap();
    let ser = Arc::new(MyEntitySerializer);
    let mut primary: PrimaryIndex<i32, MyEntity> =
        store.get_primary_index().unwrap();
    let sec1 = store
        .open_secondary_index(&mut primary, "k1", Arc::clone(&ser), {
            |e: &MyEntity| Some(e.k1)
        })
        .unwrap();
    let sec2 = store
        .open_secondary_index(&mut primary, "k2", Arc::clone(&ser), {
            |e: &MyEntity| Some(e.k2)
        })
        .unwrap();
    let sec3 = store
        .open_secondary_index(&mut primary, "k3", Arc::clone(&ser), {
            |e: &MyEntity| Some(e.k3)
        })
        .unwrap();

    for i in 0..N_RECORDS {
        let e = MyEntity { id: i, k1: 0, k2: i % 2, k3: i % 3 };
        primary.put(None, ser.as_ref(), &e).unwrap();
    }

    // Sanity (guards against a vacuous join): the secondaries are populated.
    assert_eq!(sec1.sub_index(&0).len(), 5, "k1=0 must hold all 5 records");
    assert_eq!(sec2.sub_index(&0).len(), 3, "k2=0 must hold ids 0,2,4");
    assert_eq!(sec3.sub_index(&2).len(), 1, "k3=2 must hold id 2 only");

    assert_all_cases(&sec1, &sec2, &sec3);
}
