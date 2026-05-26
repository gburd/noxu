//! Regression tests for Sprint 3D — Decisions 1B and 2C from
//! `docs/src/internal/v1.5-decisions-2026-05.md`.
//!
//! Each test asserts a documented v1.5 limitation:
//!
//! - **Decision 1B** — secondaries are one-to-one in v1.5.  Two distinct
//!   primaries that produce the same secondary key cause the second
//!   `update_secondary` to fail with [`NoxuError::Unsupported`] (closes
//!   audit finding C4).
//! - **Decision 2C** — foreign-key constraints are not enforced in v1.5.
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
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    Arc::new(Mutex::new(db))
}

fn open_inner_sec_db(env: &Environment, name: &str) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new().with_allow_create(true),
    )
    .unwrap()
}

// ─── Decision 1B — one-to-one secondaries ────────────────────

/// Two distinct primary keys that map to the same secondary key cause the
/// second insert to fail with [`NoxuError::Unsupported`].  Pre-Sprint-3 the
/// inner index used `Put::Overwrite` and silently destroyed the first
/// primary's mapping (audit finding C4).
#[test]
fn d1b_secondary_collision_returns_unsupported() {
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
    primary.lock().put(None, &pk1, &v1).unwrap();
    sec.update_secondary(&pk1, None, Some(&v1)).unwrap();

    // Sanity: lookup by 'A' returns pk1.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");

    // Second primary record: pk2 -> "Apricot" (sec_key = 'A'). MUST fail.
    let pk2 = DatabaseEntry::from_bytes(b"pk2");
    let v2 = DatabaseEntry::from_bytes(b"Apricot");
    primary.lock().put(None, &pk2, &v2).unwrap();
    let result = sec.update_secondary(&pk2, None, Some(&v2));

    match result {
        Err(NoxuError::Unsupported(msg)) => {
            assert!(
                msg.contains("one-to-one"),
                "expected one-to-one wording: {msg}"
            );
            assert!(
                msg.contains("v1.5") && msg.contains("v1.6"),
                "expected v1.5 and v1.6 references: {msg}"
            );
        }
        Ok(()) => panic!(
            "second update_secondary must fail with NoxuError::Unsupported, \
             got Ok"
        ),
        Err(other) => panic!(
            "second update_secondary must fail with NoxuError::Unsupported, \
             got: {other:?}"
        ),
    }

    // Decision 1B (honesty): the first primary's mapping is preserved
    // because we used Put::NoOverwrite.  The user sees a loud failure
    // *and* their existing index is intact.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Apple");
}

/// The successful one-to-one path still works exactly as before — distinct
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

    // Three distinct primaries with distinct first bytes — all succeed.
    let entries: &[(&[u8], &[u8])] =
        &[(b"pk1", b"Apple"), (b"pk2", b"Banana"), (b"pk3", b"Cherry")];
    for &(pk, val) in entries {
        let pk = DatabaseEntry::from_bytes(pk);
        let v = DatabaseEntry::from_bytes(val);
        primary.lock().put(None, &pk, &v).unwrap();
        sec.update_secondary(&pk, None, Some(&v)).unwrap();
    }

    // Each maps back to its primary.
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

/// Updating the *same* primary record (key + sec_key both unchanged) is
/// treated as an idempotent no-op — `Put::NoOverwrite` would otherwise
/// reject the second call.  Without this, the documented manual
/// `update_secondary(pk, None, Some(data))` call pattern would falsely
/// report a collision when the user simply re-runs an init step.
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
    primary.lock().put(None, &pk, &v).unwrap();

    // First insert into secondary.
    sec.update_secondary(&pk, None, Some(&v)).unwrap();

    // Same (pk, sec_key) again — must be a no-op, not a collision error.
    sec.update_secondary(&pk, None, Some(&v))
        .expect("re-inserting the same primary key for the same sec key must be idempotent");

    // Lookup still succeeds.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
}

// ─── Decision 2C — FK config rejected at open ──────────────────────────

/// Setting `foreign_key_database` on a `SecondaryConfig` causes
/// `SecondaryDatabase::open` to return `NoxuError::Unsupported`.
#[test]
fn d2c_foreign_key_database_rejected_at_open() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");
    let foreign = env
        .open_database(
            None,
            "foreign",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database(&foreign);

    let result = SecondaryDatabase::open(Arc::clone(&primary), inner, cfg);
    match result {
        Err(NoxuError::Unsupported(msg)) => {
            assert!(
                msg.contains("foreign-key"),
                "expected foreign-key wording: {msg}"
            );
            assert!(
                msg.contains("v1.5") && msg.contains("v1.6"),
                "expected v1.5 and v1.6 references: {msg}"
            );
        }
        Ok(_) => panic!(
            "SecondaryDatabase::open with foreign_key_database must fail \
             with NoxuError::Unsupported"
        ),
        Err(other) => panic!(
            "SecondaryDatabase::open with foreign_key_database must fail \
             with NoxuError::Unsupported, got: {other:?}"
        ),
    }
}

/// Setting `foreign_key_delete_action = Cascade` (any non-`Abort` value) is
/// also rejected at open.
#[test]
fn d2c_foreign_key_delete_action_cascade_rejected_at_open() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade);

    let result = SecondaryDatabase::open(Arc::clone(&primary), inner, cfg);
    assert!(
        matches!(result, Err(NoxuError::Unsupported(_))),
        "non-Abort delete action must be rejected with Unsupported"
    );
}

/// A `ForeignKeyNullifier` set on the config is also rejected at open.
#[test]
fn d2c_foreign_key_nullifier_rejected_at_open() {
    use noxu_db::secondary_config::ForeignKeyNullifier;

    struct NullNullifier;
    impl ForeignKeyNullifier for NullNullifier {
        fn nullify_foreign_key(
            &self,
            _db: &Database,
            _data: &mut DatabaseEntry,
        ) -> bool {
            false
        }
    }

    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_pri(&env, "primary");
    let inner = open_inner_sec_db(&env, "secondary");

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify)
        .with_foreign_key_nullifier(Box::new(NullNullifier));

    let result = SecondaryDatabase::open(Arc::clone(&primary), inner, cfg);
    assert!(
        matches!(result, Err(NoxuError::Unsupported(_))),
        "foreign_key_nullifier must be rejected with Unsupported"
    );
}

/// A clean (no FK fields set) `SecondaryConfig` still opens successfully —
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
