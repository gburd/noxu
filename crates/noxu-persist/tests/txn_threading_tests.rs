//! Sprint 3B (2026 audit: C6 / #10 / #11 / #18) regression
//! tests for `txn`-threading through `PrimaryIndex` and `SecondaryIndex`.
//!
//! What this file proves:
//!
//! 1. `PrimaryIndex::put(Some(&txn), …)` participates in the user
//!    transaction: a commit makes the write visible, an abort rolls it
//!    back.  Pre-fix this was structurally impossible because the API
//!    accepted no `txn` parameter and always called `db.put(None, …)`.
//!    Closes audit C6.
//!
//! 2. `PrimaryIndex::put(None, …)` keeps working as auto-commit — the
//!    historical pre-v1.5 path that all in-tree callers still use.  This
//!    is a regression guard.
//!
//! 3. **DPL secondary indexes are now transactional** (closes audit
//!    #10 / #11).  DPL secondaries are real, persistent
//!    `noxu_db::SecondaryDatabase`s maintained inside the active
//!    transaction by the primary `put` / `delete` fan-out.  When a primary
//!    `put` happens inside a txn that is later aborted, the secondary
//!    index update rolls back **with** the primary.  This file pins that
//!    correctness invariant (the headline test
//!    `secondary_index_rolls_back_with_aborted_txn`).

use std::sync::Arc;

use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::{
    Entity, EntitySerializer, EntityStore, PrimaryIndex, Result,
    SecondaryIndex, StoreConfig,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test entity & serializer
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct Widget {
    id: u64,
    name: String,
    color: String,
}

impl Entity for Widget {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 {
        &self.id
    }
    fn entity_name() -> &'static str {
        "Widget"
    }
}

struct WidgetSer;

impl EntitySerializer<Widget> for WidgetSer {
    fn serialize(&self, w: &Widget) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&w.id.to_be_bytes());
        let nb = w.name.as_bytes();
        buf.extend_from_slice(&(nb.len() as u32).to_be_bytes());
        buf.extend_from_slice(nb);
        let cb = w.color.as_bytes();
        buf.extend_from_slice(&(cb.len() as u32).to_be_bytes());
        buf.extend_from_slice(cb);
        Ok(buf)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Widget> {
        let id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let nl = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let name = String::from_utf8(bytes[12..12 + nl].to_vec()).unwrap();
        let p = 12 + nl;
        let cl =
            u32::from_be_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
        let color =
            String::from_utf8(bytes[p + 4..p + 4 + cl].to_vec()).unwrap();
        Ok(Widget { id, name, color })
    }
}

fn widget(id: u64, name: &str, color: &str) -> Widget {
    Widget { id, name: name.into(), color: color.into() }
}

// Helpers to set up a transactional environment + store.
fn make_txn_env() -> (TempDir, Environment) {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    (td, env)
}

fn make_txn_store(env: &Environment) -> EntityStore<'_> {
    EntityStore::open(
        env,
        StoreConfig::new("widgetstore")
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
}

// ===========================================================================
// 1. Closes audit C6: PrimaryIndex::put with Some(&txn) participates in
//    commit/abort.  Pre-fix this test could not be written: there was no
//    `txn` parameter.
// ===========================================================================

/// `put(Some(&txn), …)` followed by `txn.commit()` makes the entity
/// visible to subsequent auto-commit reads.  Closes audit C6.
#[test]
fn put_with_txn_commit_makes_entity_visible() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    let txn = env.begin_transaction(None).unwrap();
    index.put(Some(&txn), &ser, &widget(1, "alpha", "red")).unwrap();

    // Inside the txn: visible.
    let inside = index.get(Some(&txn), &ser, &1u64).unwrap();
    assert_eq!(inside.as_ref().map(|w| w.name.as_str()), Some("alpha"));

    txn.commit().unwrap();

    // After commit: visible to a fresh auto-commit reader.
    let after = index.get(None, &ser, &1u64).unwrap();
    assert_eq!(after.as_ref().map(|w| w.color.as_str()), Some("red"));
}

/// `put(Some(&txn), …)` followed by `txn.abort()` rolls the entity back
/// — pre-fix this was structurally impossible (the call always used
/// `db.put(None, …)` so the abort never saw the write).  Closes audit C6.
#[test]
fn put_with_txn_abort_rolls_back_entity() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    let txn = env.begin_transaction(None).unwrap();
    index.put(Some(&txn), &ser, &widget(2, "beta", "blue")).unwrap();
    txn.abort().unwrap();

    // After abort: must NOT be visible.
    let after = index.get(None, &ser, &2u64).unwrap();
    assert!(
        after.is_none(),
        "PrimaryIndex::put inside an aborted txn must roll back; got {:?}",
        after
    );
}

/// `delete_with_entity(Some(&txn), …)` participates in the txn — abort
/// undoes the delete.
#[test]
fn delete_with_entity_with_txn_abort_restores_entity() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    // Seed via auto-commit.
    index.put(None, &ser, &widget(3, "gamma", "green")).unwrap();
    assert!(index.contains(None, &3u64).unwrap());

    let txn = env.begin_transaction(None).unwrap();
    let deleted = index.delete_with_entity(Some(&txn), &ser, &3u64).unwrap();
    assert!(deleted);
    // Inside txn: gone.
    assert!(!index.contains(Some(&txn), &3u64).unwrap());
    txn.abort().unwrap();

    // After abort: still there.
    assert!(
        index.contains(None, &3u64).unwrap(),
        "delete_with_entity inside an aborted txn must restore the entity"
    );
}

/// `put_no_overwrite(Some(&txn), …)` participates in the txn.
#[test]
fn put_no_overwrite_with_txn_abort_rolls_back() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    let txn = env.begin_transaction(None).unwrap();
    let inserted = index
        .put_no_overwrite(Some(&txn), &ser, &widget(4, "delta", "yellow"))
        .unwrap();
    assert!(inserted);
    txn.abort().unwrap();

    assert!(
        !index.contains(None, &4u64).unwrap(),
        "put_no_overwrite inside an aborted txn must roll back"
    );
}

// ===========================================================================
// 2. Regression: PrimaryIndex::put(None, …) auto-commit still works.
// ===========================================================================

/// `put(None, …)` keeps the historical auto-commit semantics: the entity
/// is visible immediately, no enclosing transaction required.
#[test]
fn put_with_none_auto_commits() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    index.put(None, &ser, &widget(10, "epsilon", "orange")).unwrap();

    // Visible immediately under auto-commit, with no surrounding txn.
    let got = index.get(None, &ser, &10u64).unwrap().unwrap();
    assert_eq!(got, widget(10, "epsilon", "orange"));
    assert!(index.contains(None, &10u64).unwrap());
    assert_eq!(index.count().unwrap(), 1);
}

/// `delete(None, …)` and `delete_with_entity(None, …)` keep the
/// historical auto-commit semantics.
#[test]
fn delete_with_none_auto_commits() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    index.put(None, &ser, &widget(11, "zeta", "violet")).unwrap();
    assert!(index.delete(None, &11u64).unwrap());
    assert!(index.get(None, &ser, &11u64).unwrap().is_none());
}

/// `entities(None, …)` and `keys(None)` iterate under auto-commit.
#[test]
fn iterators_with_none_auto_commit() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    for i in 1u64..=3 {
        index.put(None, &ser, &widget(i, &format!("w{i}"), "x")).unwrap();
    }
    let entities: Vec<Widget> = index
        .entities(None, &ser)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(entities.len(), 3);

    let mut keys = index.keys(None).unwrap();
    assert!(keys.next().is_some());
}

// ===========================================================================
// 3. DPL secondary indexes are TRANSACTIONAL (closes audit #10 / #11).
//    Secondaries are real persistent `noxu_db::SecondaryDatabase`s
//    maintained inside the active txn by the primary put/delete fan-out.
// ===========================================================================

/// HEADLINE correctness proof.
///
/// Register a DPL entity with a secondary index; inside a transaction,
/// put an entity (updating the secondary), then **abort**.  The secondary
/// index must NOT contain the rolled-back entry, and neither must the
/// primary — the secondary update is atomic with the primary write.
///
/// Pre-fix (in-memory side `HashMap`): the secondary kept the stale entry
/// after abort (the bug).  Post-fix (transactional `SecondaryDatabase`):
/// the secondary is consistent with the aborted primary.
#[test]
fn secondary_index_rolls_back_with_aborted_txn() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let mut index: PrimaryIndex<u64, Widget> =
        store.get_primary_index().unwrap();
    let ser = Arc::new(WidgetSer);

    // Open a real, transactional secondary index keyed by colour.
    let by_color: SecondaryIndex<String, u64, Widget> = store
        .open_secondary_index(
            &mut index,
            "by_color",
            Arc::clone(&ser),
            |w: &Widget| Some(w.color.clone()),
        )
        .unwrap();

    // Inside a txn: write a Widget with colour "rare".
    let txn = env.begin_transaction(None).unwrap();
    index
        .put(Some(&txn), ser.as_ref(), &widget(100, "secret", "rare"))
        .unwrap();
    // Inside the txn the secondary already shows the new entry (the
    // maintenance ran under `txn`).
    assert!(
        by_color.contains_txn(Some(&txn), &"rare".to_string()).unwrap(),
        "inside the txn the secondary should reflect the uncommitted write"
    );
    txn.abort().unwrap();

    // Primary record was rolled back.
    assert!(
        index.get(None, ser.as_ref(), &100u64).unwrap().is_none(),
        "primary write inside an aborted txn must NOT be visible"
    );

    // THE FIX: the secondary index is rolled back together with the
    // primary — it must NOT contain the stale "rare" entry.
    assert!(
        !by_color.contains(&"rare".to_string()),
        "secondary index must roll back with the aborted primary write \
         (DPL secondaries are now transactional)"
    );
    assert!(
        by_color
            .get(None, ser.as_ref(), &index, &"rare".to_string())
            .unwrap()
            .is_none(),
        "secondary join must return None after abort"
    );
}

/// A committed txn's secondary update IS visible after commit.
#[test]
fn secondary_index_visible_after_committed_txn() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let mut index: PrimaryIndex<u64, Widget> =
        store.get_primary_index().unwrap();
    let ser = Arc::new(WidgetSer);

    let by_color: SecondaryIndex<String, u64, Widget> = store
        .open_secondary_index(
            &mut index,
            "by_color",
            Arc::clone(&ser),
            |w: &Widget| Some(w.color.clone()),
        )
        .unwrap();

    let txn = env.begin_transaction(None).unwrap();
    index.put(Some(&txn), ser.as_ref(), &widget(7, "lamp", "teal")).unwrap();
    txn.commit().unwrap();

    // After commit the secondary is visible to a fresh auto-commit read.
    assert!(by_color.contains(&"teal".to_string()));
    let found =
        by_color.get(None, ser.as_ref(), &index, &"teal".to_string()).unwrap();
    assert_eq!(found.map(|w| w.id), Some(7));
}

/// A secondary query after store reopen works: the secondary is
/// persistent (survives — it is not rebuilt from scratch).
#[test]
fn secondary_index_persists_across_reopen() {
    let (_td, env) = make_txn_env();
    // Phase 1: write + index, then close the store.
    {
        let mut store = make_txn_store(&env);
        let mut index: PrimaryIndex<u64, Widget> =
            store.get_primary_index().unwrap();
        let ser = Arc::new(WidgetSer);
        let _by_color: SecondaryIndex<String, u64, Widget> = store
            .open_secondary_index(
                &mut index,
                "by_color",
                Arc::clone(&ser),
                |w: &Widget| Some(w.color.clone()),
            )
            .unwrap();
        index.put(None, ser.as_ref(), &widget(42, "chair", "olive")).unwrap();
        store.close().unwrap();
    }

    // Phase 2: reopen the store + secondary; the entry must be found via
    // the secondary key without re-inserting it.
    {
        let mut store = EntityStore::open(
            &env,
            StoreConfig::new("widgetstore")
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let mut index: PrimaryIndex<u64, Widget> =
            store.get_primary_index().unwrap();
        let ser = Arc::new(WidgetSer);
        let by_color: SecondaryIndex<String, u64, Widget> = store
            .open_secondary_index(
                &mut index,
                "by_color",
                Arc::clone(&ser),
                |w: &Widget| Some(w.color.clone()),
            )
            .unwrap();
        let found = by_color
            .get(None, ser.as_ref(), &index, &"olive".to_string())
            .unwrap();
        assert_eq!(
            found.map(|w| w.id),
            Some(42),
            "secondary index must survive store reopen (persistent)"
        );
    }
}

/// `delete_with_entity(Some(&txn), …)` cascades to the secondary inside
/// the txn; an abort restores both primary and secondary.
#[test]
fn secondary_index_delete_rolls_back_with_aborted_txn() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let mut index: PrimaryIndex<u64, Widget> =
        store.get_primary_index().unwrap();
    let ser = Arc::new(WidgetSer);

    let by_color: SecondaryIndex<String, u64, Widget> = store
        .open_secondary_index(
            &mut index,
            "by_color",
            Arc::clone(&ser),
            |w: &Widget| Some(w.color.clone()),
        )
        .unwrap();

    // Seed (auto-commit) one Widget.
    index.put(None, ser.as_ref(), &widget(9, "box", "cyan")).unwrap();
    assert!(by_color.contains(&"cyan".to_string()));

    // Delete inside a txn, then abort.
    let txn = env.begin_transaction(None).unwrap();
    index.delete_with_entity(Some(&txn), ser.as_ref(), &9u64).unwrap();
    txn.abort().unwrap();

    // Both primary and secondary are restored.
    assert!(index.get(None, ser.as_ref(), &9u64).unwrap().is_some());
    assert!(
        by_color.contains(&"cyan".to_string()),
        "secondary entry must be restored when the delete txn aborts"
    );
}

/// Without secondaries registered, `put(Some(&txn), …)` succeeds.
#[test]
fn put_with_txn_without_secondaries_does_not_warn() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    let txn = env.begin_transaction(None).unwrap();
    index.put(Some(&txn), &ser, &widget(50, "phi", "magenta")).unwrap();
    txn.commit().unwrap();

    assert!(index.contains(None, &50u64).unwrap());
}
