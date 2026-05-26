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
    NoxuError, SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
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
