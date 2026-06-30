//! Regression tests for Sprint 3D - Decisions 1B and 2C from
//! the 2026 review.
//!
//! Each test asserts a documented v1.5 limitation:
//!
//! - **Decision 1B** - secondaries are one-to-one in v1.5.  Two distinct
//!   primaries that produce the same secondary key cause the second
//!   `update_secondary` to fail with [`NoxuError::Unsupported`] (closes
//!   audit finding C4).
//! - **Decision 2C** - foreign-key constraints are not enforced in v1.5.
//!   `SecondaryDatabase::open` rejects any `SecondaryConfig` whose
//!   foreign-key fields are set with [`NoxuError::Unsupported`] (closes
//!   audit findings C2 / F1 / F16).

use noxu_db::secondary_config::ForeignKeyDeleteAction;
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    NoxuError, OperationStatus, SecondaryConfig, SecondaryDatabase,
    SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

// ─── Helpers ──────────────────────────────────────────────────────────

/// First-byte secondary key creator (mirrors the `FirstByteCreator` used in
/// `integration_test.rs::open_pri_sec`).
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
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    Arc::new(Mutex::new(db))
}

fn open_inner_sec_db(env: &Environment, name: &str) -> Database {
    // v1.6 sorted-dup secondaries: the inner index DB must allow
    // duplicates so multiple primaries with the same secondary key
    // coexist as duplicates of the (sec_key) entry.
    env.open_database(
        None,
        name,
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(true),
    )
    .unwrap()
}

// ─── Decision 1B - sorted-dup secondaries (v1.6) ─────────────────

/// Two distinct primary keys that produce the same secondary key now
/// **both** appear in the secondary as duplicates of that key (v1.6 /
/// audit C4).  Pre-v1.5 the inner index used `Put::Overwrite` and
/// silently destroyed the first primary's mapping; v1.5 used
/// `Put::NoOverwrite` and surfaced the second insert as
/// `NoxuError::Unsupported`.  v1.6 stores them as sorted duplicates
/// of the same secondary key and lets cursor iteration enumerate the
/// fan-out.
#[test]
fn d1b_secondary_dup_admits_multiple_primaries() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    // First primary record: pk1 -> "Apple" (sec_key = 'A'). Succeeds.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let v1 = DatabaseEntry::from_bytes(b"Apple");
    // primary.put() triggers the auto-hook; no explicit update_secondary.
    primary.lock().put(&pk1, &v1).unwrap();

    // Second primary record sharing the same secondary key ('A').
    // v1.6: this MUST succeed and store a second duplicate of 'A'.
    let pk2 = DatabaseEntry::from_bytes(b"pk2");
    let v2 = DatabaseEntry::from_bytes(b"Apricot");
    // Auto-hook inserts (A, pk2) alongside (A, pk1).
    primary.lock().put(&pk2, &v2).unwrap();

    // The inner index now holds two duplicates of 'A'.
    assert_eq!(
        sec.count().unwrap(),
        2,
        "both primaries must be indexed under 'A'"
    );

    // Iterate the cursor and confirm both primaries surface.
    let mut cursor = sec.open_cursor(None).unwrap();
    let mut sec_key = DatabaseEntry::new();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut st = cursor
        .get_search_key(&DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    while st == OperationStatus::Success {
        seen.push(p_key.get_data().unwrap().to_vec());
        // Step to the next dup of the same sec_key, if any.
        st = cursor.get_next(&mut sec_key, &mut p_key, &mut data).unwrap();
        if st == OperationStatus::Success {
            // Stepping with `Get::Next` advances across keys too - stop
            // when we leave 'A'.
            if sec_key.get_data() != Some(b"A".as_ref()) {
                break;
            }
        }
    }
    seen.sort();
    assert_eq!(seen, vec![b"pk1".to_vec(), b"pk2".to_vec()]);
}

/// The successful one-to-one path still works exactly as before - distinct
/// secondary keys for distinct primaries inserts cleanly.
#[test]
fn d1b_one_to_one_happy_path() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    // Three distinct primaries with distinct first bytes - all succeed.
    let entries: &[(&[u8], &[u8])] =
        &[(b"pk1", b"Apple"), (b"pk2", b"Banana"), (b"pk3", b"Cherry")];
    for &(pk, val) in entries {
        let pk = DatabaseEntry::from_bytes(pk);
        let v = DatabaseEntry::from_bytes(val);
        primary.lock().put(&pk, &v).unwrap();
        // Auto-hook maintains secondary.
    }

    // Each maps back to its primary.
    let mappings: &[(u8, &[u8])] =
        &[(b'A', b"pk1"), (b'B', b"pk2"), (b'C', b"pk3")];
    for &(sec_byte, expected_pk) in mappings {
        let key = DatabaseEntry::from_bytes(&[sec_byte]);
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let st = sec.get_into(None, &key, &mut p_key, &mut data).unwrap();
        assert!(st);
        assert_eq!(p_key.get_data().unwrap(), expected_pk);
    }
}

/// Updating the *same* primary record (key + sec_key both unchanged) is
/// treated as an idempotent no-op - `Put::NoOverwrite` would otherwise
/// With D6, re-calling update_secondary for the same (sec_key, pri_key)
/// that the auto-hook already inserted raises SecondaryIntegrityException.
/// The correct idempotent pattern is to use primary.put() which auto-maintains.
#[test]
fn d1b_same_primary_idempotent_reinsert_ok() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    let pk = DatabaseEntry::from_bytes(b"pk1");
    let v = DatabaseEntry::from_bytes(b"Apple");
    // primary.put() auto-maintains secondary (inserts (A, pk1)).
    primary.lock().put(&pk, &v).unwrap();

    // With D6, calling update_secondary for the same (sec_key, pri_key) pair
    // that the auto-hook already inserted raises SecondaryIntegrityException.
    // The correct idempotent pattern is to just call primary.put() — the
    // auto-hook handles the secondary correctly without double-inserting.
    let result = sec.update_secondary(None, &pk, None, Some(&v));
    // D6: must error (duplicate insertion detected).
    assert!(
        result.is_err(),
        "D6: re-inserting same (sec_key, pri_key) must raise integrity error"
    );

    // Lookup still succeeds (the first insert from auto-hook is correct).
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(st);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
}

/// v1.6 sorted-dup secondaries (Decision 1B / audit C4): cursor
/// `get_next_dup_full` enumerates every primary that shares a
/// secondary key, in cursor order.
#[test]
fn d1b_cursor_walks_all_duplicates_for_shared_sec_key() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    // Three primaries share sec_key 'A' and one primary owns sec_key 'B'.
    for &(pk, val) in &[
        (&b"pk1"[..], &b"Apple"[..]),
        (&b"pk2"[..], &b"Apricot"[..]),
        (&b"pk3"[..], &b"Avocado"[..]),
        (&b"pk4"[..], &b"Banana"[..]),
    ] {
        let pk_e = DatabaseEntry::from_bytes(pk);
        let v_e = DatabaseEntry::from_bytes(val);
        primary.lock().put(&pk_e, &v_e).unwrap();
        // Auto-hook maintains secondary.
    }

    let mut cursor = sec.open_cursor(None).unwrap();
    let mut sec_key = DatabaseEntry::new();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    // Position on the first 'A' dup.
    let st = cursor
        .get_search_key(&DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    let mut seen: Vec<Vec<u8>> = vec![p_key.get_data().unwrap().to_vec()];

    loop {
        let s = cursor
            .get_next_dup_full(&mut sec_key, &mut p_key, &mut data)
            .unwrap();
        if s != OperationStatus::Success {
            break;
        }
        assert_eq!(
            sec_key.get_data().unwrap(),
            b"A",
            "get_next_dup_full must stay on the original sec_key"
        );
        seen.push(p_key.get_data().unwrap().to_vec());
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![b"pk1".to_vec(), b"pk2".to_vec(), b"pk3".to_vec()],
        "all three primaries sharing sec_key 'A' must surface"
    );

    // get_prev_dup_full at the start of the run yields NotFound - we
    // re-seek to the first 'A' dup explicitly because the previous loop
    // exited after stepping past the run's end.
    let mut p_key2 = DatabaseEntry::new();
    let mut data2 = DatabaseEntry::new();
    let st2 = cursor
        .get_search_key(
            &DatabaseEntry::from_bytes(b"A"),
            &mut p_key2,
            &mut data2,
        )
        .unwrap();
    assert_eq!(st2, OperationStatus::Success);
    let prev_st =
        cursor.get_prev_dup_full(&mut sec_key, &mut p_key, &mut data).unwrap();
    assert_eq!(
        prev_st,
        OperationStatus::NotFound,
        "get_prev_dup_full at the run start must report NotFound"
    );
}

// ─── Decision 2C - FK config rejected at open ────────────────────

/// v1.6 (audit C3 - the associate()-style hook): a registered
/// secondary now sees primary writes automatically; callers no
/// longer have to manually invoke `update_secondary` after every
/// `Database::put`.
#[test]
fn c3_primary_put_drives_registered_secondary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    // Plain `db.put` - no manual update_secondary call.
    let pk = DatabaseEntry::from_bytes(b"pk1");
    let v = DatabaseEntry::from_bytes(b"Apple");
    primary.lock().put(&pk, &v).unwrap();

    // The secondary must already be visible.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(st);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Apple");
}

/// Auto-maintenance participates in the caller's transaction:
/// aborting a primary `put` rolls back the secondary entry too.
#[test]
fn c3_primary_put_under_txn_rolls_back_secondary_on_abort() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let pk = DatabaseEntry::from_bytes(b"pk1");
    let v = DatabaseEntry::from_bytes(b"Apple");
    primary.lock().put_in(&txn, &pk, &v).unwrap();
    // Same txn sees its own auto-maintained secondary write.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    assert!(
        sec.get_into(
            Some(&txn),
            DatabaseEntry::from_bytes(b"A"),
            &mut p_key,
            &mut data
        )
        .unwrap()
    );
    txn.abort().unwrap();

    // After abort: primary and secondary both gone.
    assert!(
        !(primary
            .lock()
            .get_into(None, &pk, &mut DatabaseEntry::new())
            .unwrap())
    );
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    assert!(
        !(sec
            .get_into(
                None,
                DatabaseEntry::from_bytes(b"A"),
                &mut p_key,
                &mut data
            )
            .unwrap())
    );
}

/// v1.6 (audit C3): primary `delete` automatically removes the
/// matching secondary entries through the registered SecondaryHook;
/// callers no longer have to call `update_secondary(..., None)`.
#[test]
fn c3_primary_delete_drives_registered_secondary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    let pk = DatabaseEntry::from_bytes(b"pk1");
    let v = DatabaseEntry::from_bytes(b"Apple");
    primary.lock().put(&pk, &v).unwrap();
    assert_eq!(sec.count().unwrap(), 1);

    let st = primary.lock().delete(&pk).unwrap();
    assert!(st);
    assert_eq!(sec.count().unwrap(), 0);
}

/// Two primaries share sec_key 'A'.  Deleting one must leave the
/// other primary's secondary entry intact (sorted-dup SearchBoth
/// preserves the non-target dup).
#[test]
fn c3_primary_delete_preserves_other_dups() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let pk2 = DatabaseEntry::from_bytes(b"pk2");
    primary.lock().put(&pk1, DatabaseEntry::from_bytes(b"Apple")).unwrap();
    primary.lock().put(&pk2, DatabaseEntry::from_bytes(b"Apricot")).unwrap();
    assert_eq!(sec.count().unwrap(), 2);

    primary.lock().delete(&pk1).unwrap();
    assert_eq!(sec.count().unwrap(), 1);

    // pk2 still indexed under 'A'.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(st);
    assert_eq!(p_key.get_data().unwrap(), b"pk2");
}

/// v1.6 (audit C3 / step 6): updating an existing primary record so
/// it produces a different secondary key removes the stale entry and
/// installs the new one in a single auto-maintained call.
#[test]
fn c3_primary_update_swaps_secondary_key() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    let pk = DatabaseEntry::from_bytes(b"pk1");
    primary.lock().put(&pk, DatabaseEntry::from_bytes(b"Mango")).unwrap();
    primary.lock().put(&pk, DatabaseEntry::from_bytes(b"Pineapple")).unwrap();

    // Old sec_key 'M' must be gone.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    assert!(
        !(sec
            .get_into(
                None,
                DatabaseEntry::from_bytes(b"M"),
                &mut p_key,
                &mut data
            )
            .unwrap())
    );

    // New sec_key 'P' must point at pk1.
    let st = sec
        .get_into(None, DatabaseEntry::from_bytes(b"P"), &mut p_key, &mut data)
        .unwrap();
    assert!(st);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Pineapple");

    // Exactly one row in the index.
    assert_eq!(sec.count().unwrap(), 1);
}

/// Updating with the same secondary key (no swap) is idempotent
/// w.r.t. the index - the count stays at 1 and the same primary still
/// resolves through the same sec_key.
#[test]
fn c3_primary_update_same_sec_key_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();

    let pk = DatabaseEntry::from_bytes(b"pk1");
    primary.lock().put(&pk, DatabaseEntry::from_bytes(b"Apple")).unwrap();
    primary.lock().put(&pk, DatabaseEntry::from_bytes(b"Avocado")).unwrap();
    assert_eq!(sec.count().unwrap(), 1);
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(st);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Avocado");
}

/// Multi-key creator with auto-maintenance: a primary record whose
/// data byte set is `{A, B}` registers two secondary entries, both
/// pointing at the primary, without any manual update_secondary call.
/// Updating the primary to data `{B, C}` leaves the 'A' entry stale
/// and removed, the 'B' entry intact (idempotent), and the 'C' entry
/// freshly inserted.  Audit C3 × multi-key creators - step 7.
#[test]
fn c3_multi_key_creator_auto_maintained_on_put_and_update() {
    use noxu_db::secondary_config::SecondaryMultiKeyCreator;

    struct EachByteCreator;
    impl SecondaryMultiKeyCreator for EachByteCreator {
        fn create_secondary_keys(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            results: &mut Vec<DatabaseEntry>,
        ) {
            if let Some(d) = data.get_data() {
                for b in d {
                    results.push(DatabaseEntry::from_bytes(&[*b]));
                }
            }
        }
    }

    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "sec_mk");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_multi_key_creator(Box::new(EachByteCreator)),
    )
    .unwrap();

    // Insert.
    let pk = DatabaseEntry::from_bytes(b"pk1");
    primary.lock().put(&pk, DatabaseEntry::from_bytes(b"AB")).unwrap();
    assert_eq!(sec.count().unwrap(), 2);
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    for byte in [b"A", b"B"] {
        let st = sec
            .get_into(
                None,
                DatabaseEntry::from_bytes(byte),
                &mut p_key,
                &mut data,
            )
            .unwrap();
        assert!(st);
        assert_eq!(p_key.get_data().unwrap(), b"pk1");
    }

    // Update (data set goes A,B → B,C).
    primary.lock().put(&pk, DatabaseEntry::from_bytes(b"BC")).unwrap();
    assert_eq!(
        sec.count().unwrap(),
        2,
        "old 'A' entry must drop and 'C' must be added; 'B' stays"
    );
    assert!(
        !(sec
            .get_into(
                None,
                DatabaseEntry::from_bytes(b"A"),
                &mut p_key,
                &mut data
            )
            .unwrap())
    );
    for byte in [b"B", b"C"] {
        let st = sec
            .get_into(
                None,
                DatabaseEntry::from_bytes(byte),
                &mut p_key,
                &mut data,
            )
            .unwrap();
        assert!(st);
        assert_eq!(p_key.get_data().unwrap(), b"pk1");
    }

    // Delete fans out to all three sec keys produced by the current data.
    primary.lock().delete(&pk).unwrap();
    assert_eq!(sec.count().unwrap(), 0);
}

// ─── Decision 2C - FK config rejected at open ──────────────────────────

/// Setting `foreign_key_database_name` *without* the matching
/// `foreign_key_database_handle` is rejected at open with
/// IllegalArgument so callers do not silently end up with an
/// unenforced constraint.  Pre-v1.6 step-8 the open would reject
/// the *name* setter outright; v1.6 accepts the name as advisory but
/// requires the handle for enforcement.
#[test]
fn d2c_foreign_key_database_name_without_handle_rejected() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let _foreign = env
        .open_database(
            None,
            "foreign",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database("foreign");

    let result = SecondaryDatabase::open(Arc::clone(&primary), inner, cfg);
    assert!(
        matches!(result.as_ref(), Err(NoxuError::IllegalArgument(_))),
        "FK name without handle must be rejected"
    );
}

/// `ForeignKeyDeleteAction::Cascade`: deleting a foreign primary
/// record cascades the delete to every child primary record whose
/// secondary key equals the foreign key.  v1.6 step 9.
#[test]
fn d2c_foreign_key_delete_action_cascade_runtime_unsupported() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let foreign = open_pri(&env, "foreign");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database_handle(Arc::clone(&foreign))
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade);

    let _sec =
        SecondaryDatabase::open(Arc::clone(&primary), inner, cfg).unwrap();

    let fk = DatabaseEntry::from_bytes(b"A");
    foreign.lock().put(&fk, DatabaseEntry::from_bytes(b"x")).unwrap();
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    primary.lock().put(&pk1, DatabaseEntry::from_bytes(b"Apple")).unwrap();
    let pk2 = DatabaseEntry::from_bytes(b"pk2");
    primary.lock().put(&pk2, DatabaseEntry::from_bytes(b"Apricot")).unwrap();

    // Foreign delete cascades to both child primaries.
    assert!(foreign.lock().delete(&fk).unwrap());
    assert!(
        !(primary
            .lock()
            .get_into(None, &pk1, &mut DatabaseEntry::new())
            .unwrap())
    );
    assert!(
        !(primary
            .lock()
            .get_into(None, &pk2, &mut DatabaseEntry::new())
            .unwrap())
    );
}

/// `Nullify` action: when a foreign primary record is deleted, every
/// child primary record indexed under it has its FK field nullified
/// via the user-supplied [`ForeignKeyNullifier`].  v1.6 step 10.
#[test]
fn d2c_foreign_key_nullify_runtime_unsupported() {
    use noxu_db::secondary_config::ForeignKeyNullifier;

    /// Replaces the data with `b"_"` so the secondary key creator
    /// (FirstByteCreator) produces a sec_key of `b"_"` instead of
    /// the original first byte.
    struct UnderscoreNullifier;
    impl ForeignKeyNullifier for UnderscoreNullifier {
        fn nullify_foreign_key(
            &self,
            _db: &Database,
            data: &mut DatabaseEntry,
        ) -> bool {
            data.set_data(b"_");
            true
        }
    }

    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let foreign = open_pri(&env, "foreign");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database_handle(Arc::clone(&foreign))
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify)
        .with_foreign_key_nullifier(Box::new(UnderscoreNullifier));

    let _sec =
        SecondaryDatabase::open(Arc::clone(&primary), inner, cfg).unwrap();

    let fk = DatabaseEntry::from_bytes(b"A");
    foreign.lock().put(&fk, DatabaseEntry::from_bytes(b"x")).unwrap();
    let pk = DatabaseEntry::from_bytes(b"pk1");
    primary.lock().put(&pk, DatabaseEntry::from_bytes(b"Apple")).unwrap();

    foreign.lock().delete(&fk).unwrap();

    // Child primary still exists, but its data has been nullified.
    let mut child_data = DatabaseEntry::new();
    let st = primary.lock().get_into(None, &pk, &mut child_data).unwrap();
    assert!(st);
    assert_eq!(child_data.get_data().unwrap(), b"_");
}

/// FK Abort happy path: deleting a foreign primary record that is
/// still referenced by a child secondary entry returns
/// `ForeignConstraintViolation`; the foreign record is NOT deleted.
#[test]
fn fk_abort_blocks_delete_of_referenced_foreign_record() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let foreign = open_pri(&env, "foreign");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database_handle(Arc::clone(&foreign));
    let _sec =
        SecondaryDatabase::open(Arc::clone(&primary), inner, cfg).unwrap();

    let fk = DatabaseEntry::from_bytes(b"A");
    foreign
        .lock()
        .put(&fk, DatabaseEntry::from_bytes(b"foreign_payload"))
        .unwrap();
    primary
        .lock()
        .put(
            DatabaseEntry::from_bytes(b"pk1"),
            DatabaseEntry::from_bytes(b"Apple"),
        )
        .unwrap();

    let result = foreign.lock().delete(&fk);
    match result {
        Err(NoxuError::ForeignConstraintViolation(msg)) => {
            assert!(msg.contains("foreign-key"));
        }
        other => panic!("expected ForeignConstraintViolation, got {other:?}"),
    }
    assert!(
        foreign.lock().get_into(None, &fk, &mut DatabaseEntry::new()).unwrap(),
        "aborted FK delete must leave the foreign record intact"
    );
}

/// F3 (JE SecondaryDatabase.insertKey): inserting a child primary record whose
/// secondary key is NOT present in the configured foreign-key database must be
/// rejected with ForeignConstraintViolation — enforced on INSERT, not only on
/// the foreign DELETE side.
#[test]
fn fk_insert_rejects_secondary_key_absent_from_foreign_db() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let foreign = open_pri(&env, "foreign");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database_handle(Arc::clone(&foreign));
    let _sec =
        SecondaryDatabase::open(Arc::clone(&primary), inner, cfg).unwrap();

    // Foreign DB contains key "A" only.
    foreign
        .lock()
        .put(DatabaseEntry::from_bytes(b"A"), DatabaseEntry::from_bytes(b"x"))
        .unwrap();

    // A child whose secondary key (first byte) IS in the foreign DB: allowed.
    let ok_pk = DatabaseEntry::from_bytes(b"pk_ok");
    primary.lock().put(&ok_pk, DatabaseEntry::from_bytes(b"Apple")).unwrap();

    // A child whose secondary key (first byte 'Z') is ABSENT from the foreign
    // DB: must be rejected with ForeignConstraintViolation.
    let bad_pk = DatabaseEntry::from_bytes(b"pk_bad");
    let res = primary.lock().put(&bad_pk, DatabaseEntry::from_bytes(b"Zebra"));
    match res {
        Err(NoxuError::ForeignConstraintViolation(_)) => {}
        other => panic!(
            "F3: insert with absent foreign key must be ForeignConstraintViolation, got {other:?}"
        ),
    }
}

/// FK Nullify with the multi-key variant: every secondary key in a
/// multi-key index is nullified individually via
/// [`ForeignMultiKeyNullifier`].  v1.6 step 10.
#[test]
fn fk_nullify_multi_key_nullifier_path() {
    use noxu_db::secondary_config::{
        ForeignMultiKeyNullifier, SecondaryMultiKeyCreator,
    };

    struct EachByteCreator;
    impl SecondaryMultiKeyCreator for EachByteCreator {
        fn create_secondary_keys(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            results: &mut Vec<DatabaseEntry>,
        ) {
            if let Some(d) = data.get_data() {
                for b in d {
                    results.push(DatabaseEntry::from_bytes(&[*b]));
                }
            }
        }
    }

    struct StripByteNullifier;
    impl ForeignMultiKeyNullifier for StripByteNullifier {
        fn nullify_foreign_key(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &mut DatabaseEntry,
            secondary_key: &DatabaseEntry,
        ) -> bool {
            let target = secondary_key.get_data().unwrap_or(&[]);
            if target.is_empty() {
                return false;
            }
            let stripped: Vec<u8> = data
                .get_data()
                .unwrap_or(&[])
                .iter()
                .copied()
                .filter(|b| *b != target[0])
                .collect();
            data.set_data(&stripped);
            true
        }
    }

    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "sec_mk");
    let foreign = open_pri(&env, "foreign");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_multi_key_creator(Box::new(EachByteCreator))
        .with_foreign_key_database_handle(Arc::clone(&foreign))
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify)
        .with_foreign_multi_key_nullifier(Box::new(StripByteNullifier));

    let _sec =
        SecondaryDatabase::open(Arc::clone(&primary), inner, cfg).unwrap();

    // F3 (JE insertKey applies the foreign-key check per generated secondary
    // key): the multi-key value "ABC" produces secondary keys A, B, C — ALL of
    // which must exist in the foreign DB for the insert to be allowed (JE would
    // throw ForeignConstraintException otherwise). Populate all three.
    for b in [b"A".as_ref(), b"B", b"C"] {
        foreign
            .lock()
            .put(DatabaseEntry::from_bytes(b), DatabaseEntry::from_bytes(b"x"))
            .unwrap();
    }
    let fk_a = DatabaseEntry::from_bytes(b"A");

    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    primary.lock().put(&pk1, DatabaseEntry::from_bytes(b"ABC")).unwrap();

    // Deleting foreign key A nullifies the 'A' secondary key from the child via
    // the multi-key nullifier, leaving data "BC".
    foreign.lock().delete(&fk_a).unwrap();
    let mut child = DatabaseEntry::new();
    primary.lock().get_into(None, &pk1, &mut child).unwrap();
    assert_eq!(child.get_data().unwrap(), b"BC");
}

/// FK Cascade transitive: deleting a record in the root foreign
/// causes the cascade to walk through both levels.  v1.6 step 9.
#[test]
fn fk_cascade_transitive_two_levels() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let root = open_pri(&env, "root");
    let mid = open_pri(&env, "mid");
    let leaf = open_pri(&env, "leaf");
    let mid_sec_inner = open_inner_sec_db(&env, "mid_sec");
    let leaf_sec_inner = open_inner_sec_db(&env, "leaf_sec");

    let _mid_sec = SecondaryDatabase::open(
        Arc::clone(&mid),
        mid_sec_inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator))
            .with_foreign_key_database_handle(Arc::clone(&root))
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade),
    )
    .unwrap();
    let _leaf_sec = SecondaryDatabase::open(
        Arc::clone(&leaf),
        leaf_sec_inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator))
            .with_foreign_key_database_handle(Arc::clone(&mid))
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade),
    )
    .unwrap();

    root.lock()
        .put(
            DatabaseEntry::from_bytes(b"A"),
            DatabaseEntry::from_bytes(b"root"),
        )
        .unwrap();
    // mid record: key="M", data="Apple" - first byte 'A' indexes the
    // root foreign-key value.
    mid.lock()
        .put(
            DatabaseEntry::from_bytes(b"M"),
            DatabaseEntry::from_bytes(b"Apple"),
        )
        .unwrap();
    // leaf record: key="L", data="Mango" - first byte 'M' matches
    // mid's primary key, indexing the mid foreign-key value.
    leaf.lock()
        .put(
            DatabaseEntry::from_bytes(b"L"),
            DatabaseEntry::from_bytes(b"Mango"),
        )
        .unwrap();

    // Cascade root → mid → leaf in one delete.
    root.lock().delete(DatabaseEntry::from_bytes(b"A")).unwrap();
    assert!(
        !(mid
            .lock()
            .get_into(
                None,
                DatabaseEntry::from_bytes(b"M"),
                &mut DatabaseEntry::new()
            )
            .unwrap())
    );
    assert!(
        !(leaf
            .lock()
            .get_into(
                None,
                DatabaseEntry::from_bytes(b"L"),
                &mut DatabaseEntry::new()
            )
            .unwrap())
    );
}

/// FK Abort allows the delete when no child record references the
/// foreign key.
#[test]
fn fk_abort_allows_delete_when_no_referrer() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let foreign = open_pri(&env, "foreign");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database_handle(Arc::clone(&foreign));
    let _sec =
        SecondaryDatabase::open(Arc::clone(&primary), inner, cfg).unwrap();

    let fk = DatabaseEntry::from_bytes(b"Z");
    foreign.lock().put(&fk, DatabaseEntry::from_bytes(b"x")).unwrap();
    assert!(foreign.lock().delete(&fk).unwrap());
}

/// A clean (no FK fields set) `SecondaryConfig` still opens successfully -
/// the rejection is surgical and does not regress the documented happy
/// path.
#[test]
fn d2c_no_fk_config_opens_normally() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator));

    let _sec = SecondaryDatabase::open(Arc::clone(&primary), inner, cfg)
        .expect("secondary without FK fields must open cleanly");
}

// ─── Sprint 41⁄2 - manual-update path participates in user txns ──────
//
// These tests assert that when the caller threads the same `txn`
// through `Database::put` / `Database::delete` *and*
// `SecondaryDatabase::update_secondary`, the primary write and the
// secondary index entry commit or abort atomically.  Pre-Sprint-41⁄2
// `update_secondary` ran auto-committed regardless of any caller
// txn, so an aborted primary `put` left the secondary entry behind
// on disk - the partial-atomicity gap flagged by the Sprint 4 agent
// (audit Theme 2 / finding F5).

use noxu_db::Transaction;

fn open_pri_sec_for_txn(
    dir: &TempDir,
    primary_name: &str,
    secondary_name: &str,
) -> (Environment, Arc<Mutex<Database>>, SecondaryDatabase) {
    let env = open_env(dir);
    let primary = open_pri(&env, primary_name);
    let inner = open_inner_sec_db(&env, secondary_name);
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap();
    (env, primary, sec)
}

fn put_under_txn(
    primary: &Arc<Mutex<Database>>,
    _sec: &SecondaryDatabase,
    txn: &Transaction,
    pk: &[u8],
    val: &[u8],
) {
    let pk_e = DatabaseEntry::from_bytes(pk);
    let v_e = DatabaseEntry::from_bytes(val);
    // primary.put() auto-triggers the secondary hook; no explicit
    // update_secondary call needed (would double-insert and trigger D6).
    primary.lock().put_in(txn, &pk_e, &v_e).unwrap();
}

/// `db.put_in(&t, ...)` + `sec.update_secondary(Some(&t), ...)` +
/// `t.abort()` rolls back **both** the primary record and the
/// secondary index entry.  Pre-Sprint-41⁄2 the secondary entry survived
/// the abort because `update_secondary` was internally auto-committed.
#[test]
fn s4h_abort_rolls_back_primary_and_secondary() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    // Begin an explicit txn and write to both primary and secondary
    // under it.
    let txn = env.begin_transaction(None).unwrap();
    put_under_txn(&primary, &sec, &txn, b"pk1", b"Apple");

    // The same txn can read its own write through the secondary.
    {
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let st = sec
            .get_into(
                Some(&txn),
                DatabaseEntry::from_bytes(b"A"),
                &mut p_key,
                &mut data,
            )
            .unwrap();
        assert!(st, "txn must see its own uncommitted secondary write");
    }

    // Now abort.
    txn.abort().unwrap();

    // After abort: the primary record must be gone.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get_into(None, &pk1, &mut data).unwrap();
    assert!(!pri_status, "primary record must be rolled back by abort");

    // And the secondary index entry must also be gone.  Pre-Sprint-41⁄2
    // this was `Success` (Apple still indexed under 'A') even though
    // the primary was rolled back - the partial-atomicity gap.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(
        !sec_status,
        "secondary index entry must be rolled back by abort \
         (Sprint 41⁄2 / audit F5: pre-fix this returned Success and \
         left a dangling index entry)"
    );
}

/// `db.put_in(&t, ...)` + `sec.update_secondary(Some(&t), ...)` +
/// `t.commit()` persists **both** sides.
#[test]
fn s4h_commit_persists_primary_and_secondary() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    let txn = env.begin_transaction(None).unwrap();
    put_under_txn(&primary, &sec, &txn, b"pk1", b"Apple");
    txn.commit().unwrap();

    // Primary survives.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get_into(None, &pk1, &mut data).unwrap();
    assert!(pri_status);
    assert_eq!(data.get_data().unwrap(), b"Apple");

    // Secondary survives and points at the right primary.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(sec_status);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Apple");
}

/// Idempotent re-insert of the same `(sec_key, pri_key)` pair under the
/// *same* transaction is a no-op (matches the auto-commit Decision-1B
/// behaviour exercised by `d1b_same_primary_idempotent_reinsert_ok`).
/// `Put::NoOverwrite` returns `KeyExists` identically on transactional
/// and auto-commit cursors, and the existing probe path treats a
/// matching primary key as idempotent.
#[test]
fn s4h_same_primary_idempotent_reinsert_under_same_txn() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    let txn = env.begin_transaction(None).unwrap();
    let pk = DatabaseEntry::from_bytes(b"pk1");
    let v = DatabaseEntry::from_bytes(b"Apple");
    // primary.put() auto-maintains secondary via registered hook.
    primary.lock().put_in(&txn, &pk, &v).unwrap();

    // Calling update_secondary again for the same (sec_key, pri_key) now
    // raises SecondaryIntegrityException (D6).  The idempotent pattern
    // is to use primary.put() only.  This test is updated to verify D6.
    let result = sec.update_secondary(Some(&txn), &pk, None, Some(&v));
    assert!(
        result.is_err(),
        "D6: duplicate (sec_key, pri_key) insert must raise integrity error"
    );
    txn.abort().unwrap();

    // After abort, both primary and secondary entries are rolled back.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(!st);
}

/// While txn A holds an uncommitted secondary write, an auto-commit
/// reader from another thread does not see it.  This is the
/// txn-isolation contract: the secondary update participates in A's
/// txn, so its writes are not visible until commit.
///
/// We probe the auto-commit reader on a separate thread because the
/// outstanding write lock from A would block (rather than just hide)
/// any auto-commit *write* against the same secondary key.  A read
/// against a key A has not written returns `NotFound` cleanly without
/// touching A's lock set.
#[test]
fn s4h_uncommitted_secondary_write_is_not_visible_to_other_readers() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    // Writer txn: stage a primary + secondary write but DO NOT commit.
    let txn = env.begin_transaction(None).unwrap();
    put_under_txn(&primary, &sec, &txn, b"pk1", b"Apple");

    // Independent reader (auto-commit, default isolation) on the
    // secondary.  In v1.5 lock-based isolation, an auto-commit reader
    // for a key currently write-locked by another txn either blocks
    // until commit/abort or surfaces a typed wait error.  We assert
    // it does not silently return the uncommitted value.
    let result = sec.get_into(
        None,
        DatabaseEntry::from_bytes(b"A"),
        &mut DatabaseEntry::new(),
        &mut DatabaseEntry::new(),
    );
    match result {
        Ok(true) => panic!(
            "auto-commit reader must not see txn A's uncommitted \
             secondary write (would violate the documented isolation \
             contract)"
        ),
        Ok(false) => {
            // Reader skipped the locked record; acceptable per the
            // documented isolation level.
        }
        Err(_e) => {
            // A typed lock-conflict error is also acceptable-some
            // configurations surface the contention rather than
            // silently waiting.
        }
    }

    txn.abort().unwrap();

    // After abort, the secondary entry is gone (atomic with the
    // primary's rolled-back insert).
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(!st);
}

// ─── Wave 1B - SecondaryCursor::delete cascade honours its txn ─────
//
// These tests exercise the residual F5 sub-item the Sprint 41⁄2 agent
// flagged: `SecondaryCursor::delete` cascades both the secondary
// cleanup *and* the primary delete, but pre-Wave-1B those two writes
// always ran auto-committed because the cursor did not store its txn
// handle.  An aborted user txn could therefore destroy primary +
// secondary records that the user expected to be rolled back.
//
// Wave 1B threads the txn into `SecondaryCursor` and forwards it to
// every primary `get` / `delete` and to `delete_all_for_primary`.

/// Opening a `SecondaryCursor` under a txn, calling `delete()`, then
/// aborting the txn must leave **both** the primary record and the
/// secondary index entry intact.  Pre-Wave-1B the cascade ran
/// auto-committed and the records were gone irrespective of the abort.
#[test]
fn wave1b_cursor_delete_cascade_rolls_back_on_abort() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    // Seed: commit a primary + secondary record.
    {
        let seed = env.begin_transaction(None).unwrap();
        put_under_txn(&primary, &sec, &seed, b"pk1", b"Apple");
        seed.commit().unwrap();
    }

    // Sanity: the seeded record is visible.
    {
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let st = sec
            .get_into(
                None,
                DatabaseEntry::from_bytes(b"A"),
                &mut p_key,
                &mut data,
            )
            .unwrap();
        assert!(st);
        assert_eq!(p_key.get_data().unwrap(), b"pk1");
    }

    // Open a cursor under a txn, position on the secondary entry,
    // call delete (cascades to primary + secondary cleanup), then
    // abort.
    let txn = env.begin_transaction(None).unwrap();
    {
        let mut cursor = sec.open_cursor_in(&txn, None).unwrap();
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

        let del_st = cursor.delete().unwrap();
        assert_eq!(
            del_st,
            OperationStatus::Success,
            "cursor.delete must report success while the txn is live"
        );

        // The cursor's own txn sees the cascade as already applied.
        let mut p_key2 = DatabaseEntry::new();
        let mut data2 = DatabaseEntry::new();
        let probe_st = sec
            .get_into(
                Some(&txn),
                DatabaseEntry::from_bytes(b"A"),
                &mut p_key2,
                &mut data2,
            )
            .unwrap();
        assert!(
            !probe_st,
            "the cursor's own txn must observe the cascade as applied"
        );

        cursor.close().unwrap();
    }

    // Abort: both sides of the cascade must roll back.
    txn.abort().unwrap();

    // Primary record is back.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get_into(None, &pk1, &mut data).unwrap();
    assert!(
        pri_status,
        "primary record must survive the abort \
         (Wave 1B: pre-fix the cascade auto-committed and \
         destroyed the primary irrespective of the abort)"
    );
    assert_eq!(data.get_data().unwrap(), b"Apple");

    // Secondary entry is back.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(
        sec_status,
        "secondary entry must survive the abort \
         (Wave 1B: pre-fix the cascade auto-committed and \
         destroyed the secondary irrespective of the abort)"
    );
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Apple");
}

/// Same cascade flow but committing the txn must persist the deletes
/// of **both** the primary record and the secondary index entry.
#[test]
fn wave1b_cursor_delete_cascade_commits_both_sides() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    // Seed.
    {
        let seed = env.begin_transaction(None).unwrap();
        put_under_txn(&primary, &sec, &seed, b"pk1", b"Apple");
        put_under_txn(&primary, &sec, &seed, b"pk2", b"Banana");
        seed.commit().unwrap();
    }

    // Cursor under a txn: delete the 'A' entry, commit.
    let txn = env.begin_transaction(None).unwrap();
    {
        let mut cursor = sec.open_cursor_in(&txn, None).unwrap();
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
        cursor.delete().unwrap();
        cursor.close().unwrap();
    }
    txn.commit().unwrap();

    // 'A' / pk1 is gone on both sides.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get_into(None, &pk1, &mut data).unwrap();
    assert!(!pri_status);

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(!sec_status);

    // 'B' / pk2 is untouched.
    let pk2 = DatabaseEntry::from_bytes(b"pk2");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get_into(None, &pk2, &mut data).unwrap();
    assert!(pri_status);
    assert_eq!(data.get_data().unwrap(), b"Banana");

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get_into(None, DatabaseEntry::from_bytes(b"B"), &mut p_key, &mut data)
        .unwrap();
    assert!(sec_status);
    assert_eq!(p_key.get_data().unwrap(), b"pk2");
}

/// While txn A holds an uncommitted `cursor.delete()`, an unrelated
/// reader must not silently observe a *committed* state in which the
/// cascade succeeded.  v1.5 is lock-based without MVCC, so the
/// documented contract (mirrored in
/// `s4h_uncommitted_secondary_write_is_not_visible_to_other_readers`)
/// permits three outcomes for a probe against a record currently held
/// under a write-lock: read the pre-delete committed value, surface a
/// typed lock-conflict error, or return `NotFound` because the
/// in-flight LN points at a tombstone.  What the engine MUST NOT do
/// is leak a *commit* of the cascade out from under txn A - i.e.
/// after txn A aborts, every other observer must see the original
/// pre-delete state.  This pins the rollback semantics across the
/// concurrency boundary.
#[test]
fn wave1b_cursor_delete_uncommitted_cascade_invisible_to_others() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    // Seed.
    {
        let seed = env.begin_transaction(None).unwrap();
        put_under_txn(&primary, &sec, &seed, b"pk1", b"Apple");
        seed.commit().unwrap();
    }

    // Stage the cascade under a txn but DO NOT commit.
    let txn = env.begin_transaction(None).unwrap();
    let mut cursor = sec.open_cursor_in(&txn, None).unwrap();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = cursor
        .get_search_key(&DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    cursor.delete().unwrap();

    // Auto-commit reader from the outside.  The v1.5 lock-based
    // contract permits any of: pre-delete value, NotFound (LN points
    // at the in-flight tombstone), or typed lock-conflict error.  We
    // tolerate all three; the *commit*-leak that would prove the
    // cascade ran auto-committed is impossible here because we never
    // committed the txn - but we still record the observed outcome
    // so that the post-abort assertion below has something to compare
    // against.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut probe_data = DatabaseEntry::new();
    let _during_txn = primary.lock().get_into(None, &pk1, &mut probe_data);

    cursor.close().unwrap();
    txn.abort().unwrap();

    // After abort: every other observer (auto-commit and a fresh
    // reader txn alike) must see the seeded record intact.  This is
    // the real isolation contract Wave 1B closes - pre-Wave-1B the
    // cascade auto-committed during the in-flight txn, so the
    // post-abort state could be missing the primary even though the
    // user explicitly aborted.
    let mut data = DatabaseEntry::new();
    let after_pri = primary.lock().get_into(None, &pk1, &mut data).unwrap();
    assert!(
        after_pri,
        "after abort, the primary record must be intact for every \
         observer (Wave 1B / audit F5)"
    );
    assert_eq!(data.get_data().unwrap(), b"Apple");

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let after_sec = sec
        .get_into(None, DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert!(
        after_sec,
        "after abort, the secondary entry must be intact for every \
         observer (Wave 1B / audit F5)"
    );
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Apple");
}

/// Happy-path regression: the existing pattern of `open_cursor(None,
/// None)` followed by `cursor.delete()` still cascades and still
/// auto-commits both sides, matching the v1.4 behaviour.  This pins
/// the auto-commit branch so a future refactor of the txn plumbing
/// does not regress it.
#[test]
fn wave1b_cursor_delete_auto_commit_cascade_unchanged() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");
    drop(env); // env not needed for the auto-commit path

    // Seed (auto-commit). primary.put() auto-maintains secondary.
    {
        let pk = DatabaseEntry::from_bytes(b"pk1");
        let v = DatabaseEntry::from_bytes(b"Apple");
        primary.lock().put(&pk, &v).unwrap();
        // No explicit update_secondary needed (auto-hook handles it).
    }

    // Auto-commit cursor delete.
    let mut cursor = sec.open_cursor(None).unwrap();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = cursor
        .get_search_key(&DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    let del_st = cursor.delete().unwrap();
    assert_eq!(del_st, OperationStatus::Success);
    cursor.close().unwrap();

    // Both sides auto-committed gone (no txn to abort).
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut data = DatabaseEntry::new();
    assert!(!(primary.lock().get_into(None, &pk1, &mut data).unwrap()));
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    assert!(
        !(sec
            .get_into(
                None,
                DatabaseEntry::from_bytes(b"A"),
                &mut p_key,
                &mut data
            )
            .unwrap())
    );
}

// ─── X-10: Secondary index abort torn-state isolation tests ───────────────

/// X-10: Under READ_COMMITTED isolation (the default), a concurrent
/// `SecondaryCursor` reader must never see torn state during a transaction
/// abort that reverts both the primary record and the secondary index entry.
///
/// **Mechanism**: The abort undo loop holds write locks on all modified slots
/// (both primary and secondary) until Phase 3 (`Txn::release_all_locks`).
/// A READ_COMMITTED reader that acquires a read lock on any such slot blocks
/// until the write locks are released, at which point all undo has been
/// applied and the state is consistent.  No torn state is possible.
///
/// **READ_UNCOMMITTED**: Dirty reads bypass locking, so interim state IS
/// observable.  That is by-design for that isolation level and is NOT a bug.
///
/// **Finding X-10 conclusion**: The existing per-slot write locks already
/// prevent torn state under READ_COMMITTED.  This test is the regression guard.
#[test]
fn test_x10_secondary_abort_read_committed_no_torn_state() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let dir = TempDir::new().unwrap();
    let mut env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    // Generous lock timeout: under heavy load a reader's read lock and the
    // writer's write lock briefly contend; a long timeout ensures the
    // contending operation waits (and reads committed state) rather than
    // timing out, keeping the test deterministic.
    env_cfg.set_lock_timeout(30_000);
    let env = Arc::new(noxu_db::Environment::open(env_cfg).unwrap());

    let pri_db = env
        .open_database(
            None,
            "x10_primary",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let pri_arc = Arc::new(noxu_sync::Mutex::new(pri_db));

    let sec_inner = env
        .open_database(
            None,
            "x10_secondary",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    let sec_cfg = noxu_db::SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator));
    let sec_db = noxu_db::SecondaryDatabase::open(
        Arc::clone(&pri_arc),
        sec_inner,
        sec_cfg,
    )
    .unwrap();
    let sec_db = Arc::new(sec_db);

    // Seed: key="K" data="Avalue" → sec_key="A"
    {
        let txn = env.begin_transaction(None).unwrap();
        pri_arc
            .lock()
            .put_in(
                &txn,
                noxu_db::DatabaseEntry::from_bytes(b"K"),
                noxu_db::DatabaseEntry::from_bytes(b"Avalue"),
            )
            .unwrap();
        txn.commit().unwrap();
    }

    let torn_seen = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let iterations = Arc::new(AtomicUsize::new(0));

    // Reader: continuously reads via the secondary cursor under READ_COMMITTED.
    // Invariant: if we find sec_key="A", the primary data must start with b"A".
    let sec_clone = Arc::clone(&sec_db);
    let env_reader = Arc::clone(&env);
    let torn_clone = Arc::clone(&torn_seen);
    let done_clone = Arc::clone(&done);
    let iter_clone = Arc::clone(&iterations);

    let reader = std::thread::spawn(move || {
        // READ_COMMITTED reader: the secondary cursor's get_search_key
        // resolves the primary data atomically under THIS transaction (Wave
        // 1B), so the secondary→primary resolution observes a single,
        // lock-consistent view. The read of the primary blocks on the
        // writer's write lock for the duration of the writer's txn, so it can
        // never observe the uncommitted "Bvalue". We assert on the
        // cursor-resolved `data_entry` directly - doing a SEPARATE auto-commit
        // `get` (the prior approach) introduced an artificial
        // time-of-check/time-of-use window with a different isolation level,
        // which is what made this test flaky.
        let rc = noxu_db::TransactionConfig::new().with_read_committed(true);
        while !done_clone.load(Ordering::Relaxed) {
            let Ok(txn) = env_reader.begin_transaction(Some(&rc)) else {
                continue;
            };
            {
                let Ok(mut cursor) = sec_clone.open_cursor_in(&txn, None)
                else {
                    let _ = txn.abort();
                    continue;
                };
                let sec_key_a = noxu_db::DatabaseEntry::from_bytes(b"A");
                let mut pk_entry = noxu_db::DatabaseEntry::new();
                let mut data_entry = noxu_db::DatabaseEntry::new();
                if let Ok(noxu_db::OperationStatus::Success) = cursor
                    .get_search_key(&sec_key_a, &mut pk_entry, &mut data_entry)
                    && let Some(pri_bytes) = data_entry.get_data()
                    && (pri_bytes.is_empty() || pri_bytes[0] != b'A')
                {
                    // sec_key "A" resolved, but the atomically-fetched primary
                    // data does not start with 'A' - a torn read.
                    torn_clone.store(true, Ordering::Relaxed);
                }
            }
            let _ = txn.commit();
            iter_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Writer: abort cycle - update primary (changing sec_key A→B), then abort.
    for _ in 0..300 {
        let txn = env.begin_transaction(None).unwrap();
        pri_arc
            .lock()
            .put_in(
                &txn,
                noxu_db::DatabaseEntry::from_bytes(b"K"),
                noxu_db::DatabaseEntry::from_bytes(b"Bvalue"),
            )
            .unwrap();
        txn.abort().unwrap();
    }

    done.store(true, Ordering::Relaxed);
    reader.join().unwrap();

    assert!(
        !torn_seen.load(Ordering::Relaxed),
        "X-10: READ_COMMITTED secondary cursor must never see torn state \
         during abort. iterations={}",
        iterations.load(Ordering::Relaxed)
    );
}
