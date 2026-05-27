//! Wave 2A regression tests — v1.6 secondary unification.
//!
//! These tests cover three architectural decisions that were
//! previously enforced as v1.5 limitations and are now FULLY
//! IMPLEMENTED in v1.6:
//!
//! * **Decision 1B (audit C4)** — sorted-dup secondaries: many
//!   primaries may share a secondary key.  `Database::put` drives the
//!   shared-key insert as a duplicate value; reads via
//!   [`SecondaryCursor`] enumerate them.
//! * **Decision 2C (audit C2)** — foreign-key constraints: `Abort`,
//!   `Cascade`, and `Nullify` are honoured by `Database::delete` on
//!   the FK-target primary.
//! * **audit C3** — `associate()`-style automatic maintenance:
//!   `Database::put` / `Database::delete` drive every registered
//!   secondary inside the same txn without a manual `update_secondary`
//!   call.
//!
//! See `docs/src/internal/wave-2a-secondary-unification.md`.

use noxu_db::secondary_config::{
    ForeignKeyDeleteAction, ForeignKeyNullifier, ForeignMultiKeyNullifier,
    SecondaryMultiKeyCreator,
};
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    NoxuError, OperationStatus, SecondaryConfig, SecondaryDatabase,
    SecondaryKeyCreator, Transaction,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

// ─── Helpers ──────────────────────────────────────────────────────────

struct FirstByteCreator;
impl SecondaryKeyCreator for FirstByteCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(d) = data.get_data()
            && !d.is_empty()
        {
            result.set_data(&d[..1]);
            return true;
        }
        false
    }
}

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
}

fn open_pri(env: &Environment, name: &str) -> Arc<Mutex<Database>> {
    let db = env
        .open_database(
            None,
            name,
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    Arc::new(Mutex::new(db))
}

fn open_inner_sec_db(env: &Environment, name: &str) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true),
    )
    .unwrap()
}

fn open_basic_secondary(
    env: &Environment,
    primary: Arc<Mutex<Database>>,
    name: &str,
) -> SecondaryDatabase {
    let inner = open_inner_sec_db(env, name);
    SecondaryDatabase::open(
        primary,
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap()
}

// ─── Decision 1B — sorted-dup secondaries ─────────────────────────────

/// v1.6: two distinct primary keys that map to the same secondary key
/// COEXIST as duplicates of `sec_key`.  Pre-v1.6 the second insert
/// returned `NoxuError::Unsupported` (Decision 1B / audit C4).
#[test]
fn d1b_secondary_collision_now_succeeds_v16() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let sec = open_basic_secondary(&env, Arc::clone(&primary), "secondary");

    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let v1 = DatabaseEntry::from_bytes(b"Apple");
    primary.lock().put(None, &pk1, &v1).unwrap();

    let pk2 = DatabaseEntry::from_bytes(b"pk2");
    let v2 = DatabaseEntry::from_bytes(b"Apricot");
    primary.lock().put(None, &pk2, &v2).unwrap();

    // Both primaries are now indexed under sec_key 'A'.  Read them
    // via the cursor.
    let mut cursor = sec.open_cursor(None, None).unwrap();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = cursor
        .get_search_key(
            &DatabaseEntry::from_bytes(b"A"),
            &mut p_key,
            &mut data,
        )
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    let mut found: Vec<Vec<u8>> = vec![p_key.get_data().unwrap().to_vec()];
    loop {
        let mut sk = DatabaseEntry::new();
        let mut pk = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        match cursor.get_next_dup(&mut sk, &mut pk, &mut d).unwrap() {
            OperationStatus::Success => {
                found.push(pk.get_data().unwrap().to_vec());
            }
            _ => break,
        }
    }
    found.sort();
    assert_eq!(found, vec![b"pk1".to_vec(), b"pk2".to_vec()]);
}

/// v1.6 happy path: distinct first bytes still index cleanly.
#[test]
fn d1b_one_to_one_happy_path() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let sec = open_basic_secondary(&env, Arc::clone(&primary), "secondary");

    let entries: &[(&[u8], &[u8])] =
        &[(b"pk1", b"Apple"), (b"pk2", b"Banana"), (b"pk3", b"Cherry")];
    for &(pk, val) in entries {
        primary
            .lock()
            .put(
                None,
                &DatabaseEntry::from_bytes(pk),
                &DatabaseEntry::from_bytes(val),
            )
            .unwrap();
    }

    let mappings: &[(u8, &[u8])] =
        &[(b'A', b"pk1"), (b'B', b"pk2"), (b'C', b"pk3")];
    for &(sec_byte, expected_pk) in mappings {
        let key = DatabaseEntry::from_bytes(&[sec_byte]);
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let st = sec.get(None, &key, &mut p_key, &mut data).unwrap();
        assert_eq!(st, OperationStatus::Success);
        assert_eq!(p_key.get_data().unwrap(), expected_pk);
    }
}

/// Idempotent re-insert of the same `(sec_key, pri_key)` pair through
/// the manual `update_secondary` API is still a clean no-op.
#[test]
fn d1b_same_primary_idempotent_reinsert_ok() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let sec = open_basic_secondary(&env, Arc::clone(&primary), "secondary");

    let pk = DatabaseEntry::from_bytes(b"pk1");
    let v = DatabaseEntry::from_bytes(b"Apple");
    primary.lock().put(None, &pk, &v).unwrap();

    // The auto-maintenance hook already inserted the (sec_key, pri_key)
    // pair.  Calling update_secondary manually is a no-op (the inner
    // sorted-dup put is `Put::NoOverwrite` which silently treats an
    // existing pair as success).
    sec.update_secondary(None, &pk, None, Some(&v)).unwrap();
    sec.update_secondary(None, &pk, None, Some(&v)).unwrap();

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
}

/// v1.6 many-to-one + cursor enumeration: 5 primaries with the same
/// first byte all coexist.
#[test]
fn d1b_many_to_one_five_primaries() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let sec = open_basic_secondary(&env, Arc::clone(&primary), "secondary");

    let entries: &[(&[u8], &[u8])] = &[
        (b"pk1", b"Apple"),
        (b"pk2", b"Apricot"),
        (b"pk3", b"Avocado"),
        (b"pk4", b"Almond"),
        (b"pk5", b"Anchovy"),
    ];
    for &(pk, val) in entries {
        primary
            .lock()
            .put(
                None,
                &DatabaseEntry::from_bytes(pk),
                &DatabaseEntry::from_bytes(val),
            )
            .unwrap();
    }

    let mut cursor = sec.open_cursor(None, None).unwrap();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = cursor
        .get_search_key(
            &DatabaseEntry::from_bytes(b"A"),
            &mut p_key,
            &mut data,
        )
        .unwrap();
    assert_eq!(st, OperationStatus::Success);

    let mut found = vec![p_key.get_data().unwrap().to_vec()];
    loop {
        let mut sk = DatabaseEntry::new();
        let mut pk = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        match cursor.get_next_dup(&mut sk, &mut pk, &mut d).unwrap() {
            OperationStatus::Success => {
                found.push(pk.get_data().unwrap().to_vec());
            }
            _ => break,
        }
    }
    found.sort();
    let mut expected: Vec<Vec<u8>> =
        entries.iter().map(|(pk, _)| pk.to_vec()).collect();
    expected.sort();
    assert_eq!(found, expected);
}

/// v1.6: deleting one primary leaves the rest of the duplicate set
/// intact in the secondary.
#[test]
fn d1b_delete_one_primary_leaves_others() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let sec = open_basic_secondary(&env, Arc::clone(&primary), "secondary");

    for &(pk, val) in &[
        (&b"pk1"[..], &b"Apple"[..]),
        (&b"pk2"[..], &b"Apricot"[..]),
        (&b"pk3"[..], &b"Avocado"[..]),
    ] {
        primary
            .lock()
            .put(
                None,
                &DatabaseEntry::from_bytes(pk),
                &DatabaseEntry::from_bytes(val),
            )
            .unwrap();
    }

    primary
        .lock()
        .delete(None, &DatabaseEntry::from_bytes(b"pk2"))
        .unwrap();

    let mut cursor = sec.open_cursor(None, None).unwrap();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = cursor
        .get_search_key(
            &DatabaseEntry::from_bytes(b"A"),
            &mut p_key,
            &mut data,
        )
        .unwrap();
    assert_eq!(st, OperationStatus::Success);

    let mut found = vec![p_key.get_data().unwrap().to_vec()];
    loop {
        let mut sk = DatabaseEntry::new();
        let mut pk = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        match cursor.get_next_dup(&mut sk, &mut pk, &mut d).unwrap() {
            OperationStatus::Success => {
                found.push(pk.get_data().unwrap().to_vec());
            }
            _ => break,
        }
    }
    found.sort();
    assert_eq!(found, vec![b"pk1".to_vec(), b"pk3".to_vec()]);
}

// ─── Decision 2C — FK constraints ─────────────────────────────────────

/// `SecondaryDatabase::open` accepts FK config when
/// `open_with_foreign_key` is used and the FK target Database handle
/// is supplied.  `SecondaryDatabase::open()` (no FK) refuses the
/// config.
#[test]
fn d2c_fk_config_requires_open_with_foreign_key() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let _foreign = env
        .open_database(
            None,
            "foreign",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    let inner = open_inner_sec_db(&env, "secondary");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database("foreign");

    // Plain SecondaryDatabase::open with FK fields set is rejected.
    let result = SecondaryDatabase::open(Arc::clone(&primary), inner, cfg);
    assert!(matches!(result, Err(NoxuError::IllegalArgument(_))));
}

/// `ForeignKeyDeleteAction::Abort` blocks the parent delete with a
/// typed [`NoxuError::ForeignConstraintViolation`] when the parent has
/// at least one referrer; the parent record stays intact.
#[test]
fn d2c_abort_blocks_parent_delete() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let parent = open_pri(&env, "parent");
    let child = open_pri(&env, "child");

    // Child secondary indexed by first byte of child's data; FK target
    // = parent.
    let inner = open_inner_sec_db(&env, "child_fk_index");
    let fk_cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database("parent")
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Abort);
    let _child_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&child),
        inner,
        fk_cfg,
        Arc::clone(&parent),
    )
    .unwrap();

    // Insert parent record P_A (FK key 'A'), then a child whose value
    // first-byte is 'A' (so it references P_A).
    parent
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"A"),
            &DatabaseEntry::from_bytes(b"parent-A"),
        )
        .unwrap();
    child
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"c1"),
            &DatabaseEntry::from_bytes(b"A-child-data"),
        )
        .unwrap();

    // Attempt to delete parent A: must fail with FK violation.
    let result =
        parent.lock().delete(None, &DatabaseEntry::from_bytes(b"A"));
    match result {
        Err(NoxuError::ForeignConstraintViolation(_)) => {}
        other => panic!("expected ForeignConstraintViolation, got {other:?}"),
    }

    // Parent is still present, child untouched.
    let mut buf = DatabaseEntry::new();
    assert_eq!(
        parent
            .lock()
            .get(None, &DatabaseEntry::from_bytes(b"A"), &mut buf)
            .unwrap(),
        OperationStatus::Success
    );
    let mut cbuf = DatabaseEntry::new();
    assert_eq!(
        child
            .lock()
            .get(None, &DatabaseEntry::from_bytes(b"c1"), &mut cbuf)
            .unwrap(),
        OperationStatus::Success
    );
}

/// `ForeignKeyDeleteAction::Cascade` deletes referring children when
/// the referenced parent is deleted.
#[test]
fn d2c_cascade_deletes_children() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let parent = open_pri(&env, "parent");
    let child = open_pri(&env, "child");

    let inner = open_inner_sec_db(&env, "child_fk_index");
    let fk_cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database("parent")
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade);
    let _child_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&child),
        inner,
        fk_cfg,
        Arc::clone(&parent),
    )
    .unwrap();

    parent
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"A"),
            &DatabaseEntry::from_bytes(b"parent-A"),
        )
        .unwrap();
    for i in 1..=3 {
        let pk = format!("c{i}").into_bytes();
        let val = format!("A-c{i}").into_bytes();
        child
            .lock()
            .put(
                None,
                &DatabaseEntry::from_bytes(&pk),
                &DatabaseEntry::from_bytes(&val),
            )
            .unwrap();
    }

    parent
        .lock()
        .delete(None, &DatabaseEntry::from_bytes(b"A"))
        .unwrap();

    // All three children are gone.
    for i in 1..=3 {
        let pk = format!("c{i}").into_bytes();
        let mut buf = DatabaseEntry::new();
        let st = child
            .lock()
            .get(None, &DatabaseEntry::from_bytes(&pk), &mut buf)
            .unwrap();
        assert_eq!(
            st,
            OperationStatus::NotFound,
            "child {} should have been cascade-deleted",
            String::from_utf8_lossy(&pk)
        );
    }
}

/// Cascade is transitive: parent → child → grandchild.  Deleting
/// parent fires both the parent→child cascade and (recursively) the
/// child→grandchild cascade.
#[test]
fn d2c_cascade_transitive_three_levels() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let parent = open_pri(&env, "parent");
    let child = open_pri(&env, "child");
    let grand = open_pri(&env, "grand");

    let child_inner = open_inner_sec_db(&env, "child_idx");
    let _child_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&child),
        child_inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteCreator))
            .with_foreign_key_database("parent")
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade),
        Arc::clone(&parent),
    )
    .unwrap();

    let grand_inner = open_inner_sec_db(&env, "grand_idx");
    let _grand_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&grand),
        grand_inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteCreator))
            .with_foreign_key_database("child")
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade),
        Arc::clone(&child),
    )
    .unwrap();

    // parent['A'], child['c1'] -> 'A...', grand['g1'] -> 'c1...'  but
    // we're using FirstByteCreator so the FK keys are SINGLE BYTES.
    // Use single-byte primary keys throughout.
    parent
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"A"),
            &DatabaseEntry::from_bytes(b"parent-A"),
        )
        .unwrap();
    // child 'C', value first-byte 'A' (refs parent A)
    child
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"C"),
            &DatabaseEntry::from_bytes(b"A-child"),
        )
        .unwrap();
    // grandchild 'G', value first-byte 'C' (refs child C)
    grand
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"G"),
            &DatabaseEntry::from_bytes(b"C-grand"),
        )
        .unwrap();

    // Delete parent A.
    parent
        .lock()
        .delete(None, &DatabaseEntry::from_bytes(b"A"))
        .unwrap();

    // child C is gone (parent→child cascade).
    let mut buf = DatabaseEntry::new();
    assert_eq!(
        child
            .lock()
            .get(None, &DatabaseEntry::from_bytes(b"C"), &mut buf)
            .unwrap(),
        OperationStatus::NotFound
    );
    // grandchild G is gone (child→grand cascade chained).
    let mut gbuf = DatabaseEntry::new();
    assert_eq!(
        grand
            .lock()
            .get(None, &DatabaseEntry::from_bytes(b"G"), &mut gbuf)
            .unwrap(),
        OperationStatus::NotFound
    );
}

/// Cascade cycle detection: A → B → A.  Delete on A must surface a
/// typed `ForeignConstraintViolation` rather than infinite-loop.
#[test]
fn d2c_cascade_cycle_detection() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let a_db = open_pri(&env, "a");
    let b_db = open_pri(&env, "b");

    // a_secondary on a_db references b_db (so b deletes cascade into a)
    let a_inner = open_inner_sec_db(&env, "a_idx");
    let _a_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&a_db),
        a_inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteCreator))
            .with_foreign_key_database("b")
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade),
        Arc::clone(&b_db),
    )
    .unwrap();
    // b_secondary on b_db references a_db
    let b_inner = open_inner_sec_db(&env, "b_idx");
    let _b_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&b_db),
        b_inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteCreator))
            .with_foreign_key_database("a")
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade),
        Arc::clone(&a_db),
    )
    .unwrap();

    // Set up a circular reference: a['X'] -> "Y..." (refs b Y),
    //                              b['Y'] -> "X..." (refs a X).
    a_db.lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"X"),
            &DatabaseEntry::from_bytes(b"Y-data"),
        )
        .unwrap();
    b_db.lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"Y"),
            &DatabaseEntry::from_bytes(b"X-data"),
        )
        .unwrap();

    // Delete a['X'] — the cascade chain is a[X] → b[Y] → a[X] (cycle).
    let result = a_db.lock().delete(None, &DatabaseEntry::from_bytes(b"X"));
    match result {
        Err(NoxuError::ForeignConstraintViolation(msg)) => {
            assert!(
                msg.contains("cycle") || msg.contains("in flight"),
                "expected cycle detection wording, got: {msg}"
            );
        }
        other => panic!("expected ForeignConstraintViolation, got {other:?}"),
    }
}

/// `Nullify` (single-key): zeroes out the FK field via the user's
/// nullifier.
#[test]
fn d2c_nullify_single_key() {
    struct ZeroNullifier;
    impl ForeignKeyNullifier for ZeroNullifier {
        fn nullify_foreign_key(
            &self,
            _db: &Database,
            data: &mut DatabaseEntry,
        ) -> bool {
            // Zero out the first byte (the FK reference) and keep the rest.
            if let Some(d) = data.get_data()
                && !d.is_empty()
            {
                let mut new_data = d.to_vec();
                new_data[0] = 0;
                data.set_data(&new_data);
                return true;
            }
            false
        }
    }

    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let parent = open_pri(&env, "parent");
    let child = open_pri(&env, "child");

    let inner = open_inner_sec_db(&env, "child_idx");
    let _child_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&child),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteCreator))
            .with_foreign_key_database("parent")
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify)
            .with_foreign_key_nullifier(Box::new(ZeroNullifier)),
        Arc::clone(&parent),
    )
    .unwrap();

    parent
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"A"),
            &DatabaseEntry::from_bytes(b"parent-A"),
        )
        .unwrap();
    child
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"c1"),
            &DatabaseEntry::from_bytes(b"A-data"),
        )
        .unwrap();

    parent
        .lock()
        .delete(None, &DatabaseEntry::from_bytes(b"A"))
        .unwrap();

    // child still exists with a zeroed FK byte.
    let mut data = DatabaseEntry::new();
    let st = child
        .lock()
        .get(None, &DatabaseEntry::from_bytes(b"c1"), &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    let bytes = data.get_data().unwrap();
    assert_eq!(bytes[0], 0);
    assert_eq!(&bytes[1..], b"-data");
}

/// `Nullify` (multi-key): each nullified key is removed from the
/// child's data.
#[test]
fn d2c_nullify_multi_key() {
    struct CommaSplitMulti;
    impl SecondaryMultiKeyCreator for CommaSplitMulti {
        fn create_secondary_keys(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            results: &mut Vec<DatabaseEntry>,
        ) {
            // Split data at commas; each part is a secondary key.
            if let Some(d) = data.get_data() {
                for part in d.split(|b| *b == b',') {
                    if !part.is_empty() {
                        results.push(DatabaseEntry::from_bytes(part));
                    }
                }
            }
        }
    }
    struct CommaRemover;
    impl ForeignMultiKeyNullifier for CommaRemover {
        fn nullify_foreign_key(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &mut DatabaseEntry,
            secondary_key: &DatabaseEntry,
        ) -> bool {
            let target = match secondary_key.get_data() {
                Some(t) => t.to_vec(),
                None => return false,
            };
            let bytes = match data.get_data() {
                Some(b) => b.to_vec(),
                None => return false,
            };
            let parts: Vec<&[u8]> = bytes
                .split(|b| *b == b',')
                .filter(|p| *p != target.as_slice())
                .collect();
            let mut new_data: Vec<u8> = Vec::new();
            for (i, part) in parts.iter().enumerate() {
                if i > 0 {
                    new_data.push(b',');
                }
                new_data.extend_from_slice(part);
            }
            data.set_data(&new_data);
            true
        }
    }

    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let parent = open_pri(&env, "parent");
    let child = open_pri(&env, "child");

    let inner = open_inner_sec_db(&env, "child_idx");
    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true)
        .with_multi_key_creator(Box::new(CommaSplitMulti))
        .with_foreign_key_database("parent")
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify)
        .with_foreign_multi_key_nullifier(Box::new(CommaRemover));
    let _child_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&child),
        inner,
        cfg,
        Arc::clone(&parent),
    )
    .unwrap();

    parent
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"alpha"),
            &DatabaseEntry::from_bytes(b"parent-alpha"),
        )
        .unwrap();
    parent
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"beta"),
            &DatabaseEntry::from_bytes(b"parent-beta"),
        )
        .unwrap();
    child
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"c1"),
            &DatabaseEntry::from_bytes(b"alpha,beta,gamma"),
        )
        .unwrap();

    // Delete parent alpha — child c1 should now be "beta,gamma".
    parent
        .lock()
        .delete(None, &DatabaseEntry::from_bytes(b"alpha"))
        .unwrap();

    let mut data = DatabaseEntry::new();
    let st = child
        .lock()
        .get(None, &DatabaseEntry::from_bytes(b"c1"), &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(data.get_data().unwrap(), b"beta,gamma");
}

/// FK Abort under an explicit txn: aborting the txn leaves no side
/// effects.
#[test]
fn d2c_abort_under_txn_rolls_back() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let parent = open_pri(&env, "parent");
    let child = open_pri(&env, "child");

    let inner = open_inner_sec_db(&env, "child_idx");
    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database("parent")
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade);
    let _child_sec = SecondaryDatabase::open_with_foreign_key(
        Arc::clone(&child),
        inner,
        cfg,
        Arc::clone(&parent),
    )
    .unwrap();

    parent
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"A"),
            &DatabaseEntry::from_bytes(b"parent-A"),
        )
        .unwrap();
    child
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"c1"),
            &DatabaseEntry::from_bytes(b"A-c1"),
        )
        .unwrap();

    // Begin txn, delete parent (cascade deletes child), abort txn.
    let txn = env.begin_transaction(None, None).unwrap();
    parent
        .lock()
        .delete(Some(&txn), &DatabaseEntry::from_bytes(b"A"))
        .unwrap();
    txn.abort().unwrap();

    // Both records are restored.
    let mut buf = DatabaseEntry::new();
    assert_eq!(
        parent
            .lock()
            .get(None, &DatabaseEntry::from_bytes(b"A"), &mut buf)
            .unwrap(),
        OperationStatus::Success
    );
    let mut cbuf = DatabaseEntry::new();
    assert_eq!(
        child
            .lock()
            .get(None, &DatabaseEntry::from_bytes(b"c1"), &mut cbuf)
            .unwrap(),
        OperationStatus::Success
    );
}

// ─── Audit C3 — automatic associate()-style maintenance ───────────────

fn open_pri_sec_for_txn(
    dir: &TempDir,
    primary_name: &str,
    secondary_name: &str,
) -> (Environment, Arc<Mutex<Database>>, SecondaryDatabase) {
    let env = open_env(dir);
    let primary = open_pri(&env, primary_name);
    let sec = open_basic_secondary(&env, Arc::clone(&primary), secondary_name);
    (env, primary, sec)
}

/// `db.put(Some(&txn), ...)` automatically updates registered
/// secondaries inside `txn` — no manual `update_secondary` call.
#[test]
fn associate_put_under_txn_drives_secondary() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    let txn = env.begin_transaction(None, None).unwrap();
    primary
        .lock()
        .put(
            Some(&txn),
            &DatabaseEntry::from_bytes(b"pk1"),
            &DatabaseEntry::from_bytes(b"Apple"),
        )
        .unwrap();
    txn.commit().unwrap();

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
}

/// Aborting the user's txn rolls back BOTH the primary write AND the
/// auto-driven secondary update.
#[test]
fn associate_abort_rolls_back_primary_and_secondary() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    let txn = env.begin_transaction(None, None).unwrap();
    primary
        .lock()
        .put(
            Some(&txn),
            &DatabaseEntry::from_bytes(b"pk1"),
            &DatabaseEntry::from_bytes(b"Apple"),
        )
        .unwrap();
    // The same txn sees its own write through the secondary.
    {
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let st = sec
            .get(
                Some(&txn),
                &DatabaseEntry::from_bytes(b"A"),
                &mut p_key,
                &mut data,
            )
            .unwrap();
        assert_eq!(st, OperationStatus::Success);
    }
    txn.abort().unwrap();

    // Primary is gone.
    let mut data = DatabaseEntry::new();
    assert_eq!(
        primary
            .lock()
            .get(None, &DatabaseEntry::from_bytes(b"pk1"), &mut data)
            .unwrap(),
        OperationStatus::NotFound
    );
    // Secondary is gone.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    assert_eq!(
        sec.get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
            .unwrap(),
        OperationStatus::NotFound
    );
}

/// Two registered secondaries on the same primary: a single
/// `db.put(Some(&txn), ...)` updates BOTH inside `txn`.
#[test]
fn associate_two_secondaries_under_one_txn() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");

    // Two secondaries: one on first byte, one on last byte.
    let s1 = open_basic_secondary(&env, Arc::clone(&primary), "first_byte");

    struct LastByteCreator;
    impl SecondaryKeyCreator for LastByteCreator {
        fn create_secondary_key(
            &self,
            _db: &Database,
            _k: &DatabaseEntry,
            data: &DatabaseEntry,
            result: &mut DatabaseEntry,
        ) -> bool {
            if let Some(d) = data.get_data()
                && !d.is_empty()
            {
                result.set_data(&d[d.len() - 1..]);
                return true;
            }
            false
        }
    }
    let inner2 = open_inner_sec_db(&env, "last_byte");
    let s2 = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner2,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(LastByteCreator)),
    )
    .unwrap();

    let txn = env.begin_transaction(None, None).unwrap();
    primary
        .lock()
        .put(
            Some(&txn),
            &DatabaseEntry::from_bytes(b"pk1"),
            &DatabaseEntry::from_bytes(b"Apple"),
        )
        .unwrap();
    txn.commit().unwrap();

    let mut p1 = DatabaseEntry::new();
    let mut d1 = DatabaseEntry::new();
    assert_eq!(
        s1.get(None, &DatabaseEntry::from_bytes(b"A"), &mut p1, &mut d1)
            .unwrap(),
        OperationStatus::Success
    );

    let mut p2 = DatabaseEntry::new();
    let mut d2 = DatabaseEntry::new();
    assert_eq!(
        s2.get(None, &DatabaseEntry::from_bytes(b"e"), &mut p2, &mut d2)
            .unwrap(),
        OperationStatus::Success
    );
}

/// V1 → V2 update: secondary is updated to the new sec_key under the
/// same txn (delete old sec_key entry, insert new).
#[test]
fn associate_update_v1_v2_replaces_secondary_key() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    let txn = env.begin_transaction(None, None).unwrap();
    primary
        .lock()
        .put(
            Some(&txn),
            &DatabaseEntry::from_bytes(b"pk1"),
            &DatabaseEntry::from_bytes(b"Mango"),
        )
        .unwrap();
    primary
        .lock()
        .put(
            Some(&txn),
            &DatabaseEntry::from_bytes(b"pk1"),
            &DatabaseEntry::from_bytes(b"Pineapple"),
        )
        .unwrap();
    txn.commit().unwrap();

    // Old sec_key 'M' — gone.
    let mut p = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert_eq!(
        sec.get(None, &DatabaseEntry::from_bytes(b"M"), &mut p, &mut d)
            .unwrap(),
        OperationStatus::NotFound
    );
    // New sec_key 'P' — present.
    let mut p = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let st = sec
        .get(None, &DatabaseEntry::from_bytes(b"P"), &mut p, &mut d)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(p.get_data().unwrap(), b"pk1");
}

/// Multi-key creator: a single put produces multiple secondary keys;
/// a single delete removes all of them.
#[test]
fn associate_multi_key_creator_inserts_and_deletes_all() {
    struct CommaSplit;
    impl SecondaryMultiKeyCreator for CommaSplit {
        fn create_secondary_keys(
            &self,
            _db: &Database,
            _k: &DatabaseEntry,
            data: &DatabaseEntry,
            results: &mut Vec<DatabaseEntry>,
        ) {
            if let Some(d) = data.get_data() {
                for part in d.split(|b| *b == b',') {
                    if !part.is_empty() {
                        results.push(DatabaseEntry::from_bytes(part));
                    }
                }
            }
        }
    }

    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "tags");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_multi_key_creator(Box::new(CommaSplit)),
    )
    .unwrap();

    primary
        .lock()
        .put(
            None,
            &DatabaseEntry::from_bytes(b"pk1"),
            &DatabaseEntry::from_bytes(b"red,green,blue"),
        )
        .unwrap();

    for tag in &[&b"red"[..], &b"green"[..], &b"blue"[..]] {
        let mut p = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let st = sec
            .get(None, &DatabaseEntry::from_bytes(tag), &mut p, &mut d)
            .unwrap();
        assert_eq!(
            st,
            OperationStatus::Success,
            "tag {} missing",
            String::from_utf8_lossy(tag)
        );
        assert_eq!(p.get_data().unwrap(), b"pk1");
    }

    // Delete primary.
    primary
        .lock()
        .delete(None, &DatabaseEntry::from_bytes(b"pk1"))
        .unwrap();

    for tag in &[&b"red"[..], &b"green"[..], &b"blue"[..]] {
        let mut p = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let st = sec
            .get(None, &DatabaseEntry::from_bytes(tag), &mut p, &mut d)
            .unwrap();
        assert_eq!(
            st,
            OperationStatus::NotFound,
            "tag {} should have been removed",
            String::from_utf8_lossy(tag)
        );
    }
}

// ─── Sprint 4½ regression — explicit-txn manual update path still works
// (escape hatch for callers that pre-date the auto-maintenance hook). ───

fn put_under_txn_manual(
    primary: &Arc<Mutex<Database>>,
    sec: &SecondaryDatabase,
    txn: &Transaction,
    pk: &[u8],
    val: &[u8],
) {
    let pk_e = DatabaseEntry::from_bytes(pk);
    let v_e = DatabaseEntry::from_bytes(val);
    primary.lock().put(Some(txn), &pk_e, &v_e).unwrap();
    // The auto-maintenance hook already inserted the (sec_key, pri_key)
    // pair; calling update_secondary again is idempotent.
    sec.update_secondary(Some(txn), &pk_e, None, Some(&v_e)).unwrap();
}

#[test]
fn s4h_manual_update_still_idempotent_under_txn() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    let txn = env.begin_transaction(None, None).unwrap();
    put_under_txn_manual(&primary, &sec, &txn, b"pk1", b"Apple");
    txn.commit().unwrap();

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
}
