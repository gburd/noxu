//! Sprint 3B (api-audit-2026-05-persist-xa, C6 / #10 / #11 / #18) regression
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
//! 3. The v1.5 in-memory secondary-index limitation (audit #10 / #11) is
//!    documented behaviour: when a primary `put` happens inside an
//!    explicit txn that is later aborted, the secondary map stays
//!    updated (the limitation), and the `PersistError::SecondariesNotTransactional`
//!    typed error message can be observed.  v1.6 will back secondaries
//!    with a real `Database` to make them atomic with the txn.

use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::{
    Entity, EntitySerializer, EntityStore, PersistError, PrimaryIndex, Result,
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
// 3. Documents v1.5 limitation (audit #10 / #11): in-memory secondaries
//    are NOT atomic with the user transaction.
// ===========================================================================

/// In v1.5 a secondary-keyed entity has its in-memory secondary map
/// updated immediately on `put`, regardless of the surrounding txn.
/// On `txn.abort()` the **primary** record is rolled back, but the
/// **secondary map stays updated** — this test pins the documented
/// behaviour so a future change either (a) fixes it (closing audit
/// #10 / #11 in v1.6) and updates this test to assert the fixed
/// behaviour, or (b) explicitly preserves the limitation knowing the
/// test exists.
///
/// The new `PersistError::SecondariesNotTransactional` is emitted as a
/// `log::warn!` once per `PrimaryIndex` when this path is taken; the
/// test additionally suppresses the matching `debug_assert!` via the
/// documented opt-in env var so the suite remains green in debug
/// builds.
#[test]
fn secondary_index_update_is_not_atomic_with_txn_v1_5() {
    // SAFETY (single-threaded test): set the documented opt-in before any
    // PrimaryIndex method is invoked and unset it on the way out so
    // neighbour tests are unaffected.  cargo runs tests in parallel by
    // default but `set_var` is process-global; using `--test-threads=1`
    // for this file would be heavy-handed, so we rely on the env var
    // being a no-op for tests that don't use Some(&txn) + secondaries.
    // SAFETY: `std::env::set_var` is `unsafe` on edition 2024 because
    // setting environment variables is process-global and racy with
    // threads that read env vars; we accept that this is best-effort
    // and confined to debug-assert silencing.
    unsafe {
        std::env::set_var("NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES", "1");
    }

    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let mut index: PrimaryIndex<u64, Widget> =
        store.get_primary_index().unwrap();
    let ser = WidgetSer;

    // Register a secondary index keyed by colour.
    let by_color: SecondaryIndex<String, u64, Widget> =
        index.open_secondary_index(|w: &Widget| Some(w.color.clone()));

    // Inside an aborted txn: write a Widget with colour "rare".
    let txn = env.begin_transaction(None).unwrap();
    index.put(Some(&txn), &ser, &widget(100, "secret", "rare")).unwrap();
    // Inside the txn the secondary already shows the new entry.
    assert!(by_color.contains(&"rare".to_string()));
    txn.abort().unwrap();

    // Primary record was rolled back.
    assert!(
        index.get(None, &ser, &100u64).unwrap().is_none(),
        "primary write inside an aborted txn must NOT be visible"
    );

    // Secondary map is the documented v1.5 limitation: it is NOT rolled
    // back.  The pre-existing entry stays in the in-memory BTreeMap.
    // This test assertion intentionally pins the limitation; v1.6 will
    // back secondaries with a real Database and this assertion will
    // flip to `assert!(!by_color.contains(...))`.
    assert!(
        by_color.contains(&"rare".to_string()),
        "v1.5 limitation: secondary index is in-memory and is NOT rolled \
         back on txn.abort(); see PersistError::SecondariesNotTransactional"
    );

    // SAFETY: edition-2024 requires unsafe for std::env::remove_var;
    // this is test-only code that resets the env var set earlier in this test.
    unsafe {
        std::env::remove_var("NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES");
    }
}

/// The typed error variant carries the documented message and prints
/// usefully.
#[test]
fn secondaries_not_transactional_error_message() {
    let err = PersistError::SecondariesNotTransactional;
    let msg = err.to_string();
    assert!(
        msg.contains("in-memory") && msg.contains("transaction"),
        "error message should explain the limitation; got {msg:?}"
    );
}

/// Without secondaries registered, the warning path is never taken —
/// `put(Some(&txn), …)` must succeed silently regardless of the
/// `NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES` env var.
#[test]
fn put_with_txn_without_secondaries_does_not_warn() {
    let (_td, env) = make_txn_env();
    let mut store = make_txn_store(&env);
    let index: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSer;

    let txn = env.begin_transaction(None).unwrap();
    // No secondaries registered, so the warning + debug_assert path is
    // bypassed. This must succeed even without the env-var opt-in.
    index.put(Some(&txn), &ser, &widget(50, "phi", "magenta")).unwrap();
    txn.commit().unwrap();

    assert!(index.contains(None, &50u64).unwrap());
}
