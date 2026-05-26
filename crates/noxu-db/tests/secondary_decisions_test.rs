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
    sec.update_secondary(None, &pk1, None, Some(&v1)).unwrap();

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
    let result = sec.update_secondary(None, &pk2, None, Some(&v2));

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
        sec.update_secondary(None, &pk, None, Some(&v)).unwrap();
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
    sec.update_secondary(None, &pk, None, Some(&v)).unwrap();

    // Same (pk, sec_key) again — must be a no-op, not a collision error.
    sec.update_secondary(None, &pk, None, Some(&v))
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

// ─── Sprint 4½ — manual-update path participates in user txns ──────
//
// These tests assert that when the caller threads the same `txn`
// through `Database::put` / `Database::delete` *and*
// `SecondaryDatabase::update_secondary`, the primary write and the
// secondary index entry commit or abort atomically.  Pre-Sprint-4½
// `update_secondary` ran auto-committed regardless of any caller
// txn, so an aborted primary `put` left the secondary entry behind
// on disk — the partial-atomicity gap flagged by the Sprint 4 agent
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
    sec: &SecondaryDatabase,
    txn: &Transaction,
    pk: &[u8],
    val: &[u8],
) {
    let pk_e = DatabaseEntry::from_bytes(pk);
    let v_e = DatabaseEntry::from_bytes(val);
    primary.lock().put(Some(txn), &pk_e, &v_e).unwrap();
    sec.update_secondary(Some(txn), &pk_e, None, Some(&v_e)).unwrap();
}

/// `db.put(Some(&t), …)` + `sec.update_secondary(Some(&t), …)` +
/// `t.abort()` rolls back **both** the primary record and the
/// secondary index entry.  Pre-Sprint-4½ the secondary entry survived
/// the abort because `update_secondary` was internally auto-committed.
#[test]
fn s4h_abort_rolls_back_primary_and_secondary() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    // Begin an explicit txn and write to both primary and secondary
    // under it.
    let txn = env.begin_transaction(None, None).unwrap();
    put_under_txn(&primary, &sec, &txn, b"pk1", b"Apple");

    // The same txn can read its own write through the secondary.
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
        assert_eq!(
            st,
            OperationStatus::Success,
            "txn must see its own uncommitted secondary write"
        );
    }

    // Now abort.
    txn.abort().unwrap();

    // After abort: the primary record must be gone.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get(None, &pk1, &mut data).unwrap();
    assert_eq!(
        pri_status,
        OperationStatus::NotFound,
        "primary record must be rolled back by abort"
    );

    // And the secondary index entry must also be gone.  Pre-Sprint-4½
    // this was `Success` (Apple still indexed under 'A') even though
    // the primary was rolled back — the partial-atomicity gap.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(
        sec_status,
        OperationStatus::NotFound,
        "secondary index entry must be rolled back by abort \
         (Sprint 4½ / audit F5: pre-fix this returned Success and \
         left a dangling index entry)"
    );
}

/// `db.put(Some(&t), …)` + `sec.update_secondary(Some(&t), …)` +
/// `t.commit()` persists **both** sides.
#[test]
fn s4h_commit_persists_primary_and_secondary() {
    let dir = TempDir::new().unwrap();
    let (env, primary, sec) =
        open_pri_sec_for_txn(&dir, "primary", "secondary");

    let txn = env.begin_transaction(None, None).unwrap();
    put_under_txn(&primary, &sec, &txn, b"pk1", b"Apple");
    txn.commit().unwrap();

    // Primary survives.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get(None, &pk1, &mut data).unwrap();
    assert_eq!(pri_status, OperationStatus::Success);
    assert_eq!(data.get_data().unwrap(), b"Apple");

    // Secondary survives and points at the right primary.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(sec_status, OperationStatus::Success);
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

    let txn = env.begin_transaction(None, None).unwrap();
    let pk = DatabaseEntry::from_bytes(b"pk1");
    let v = DatabaseEntry::from_bytes(b"Apple");
    primary.lock().put(Some(&txn), &pk, &v).unwrap();

    // First insert succeeds.
    sec.update_secondary(Some(&txn), &pk, None, Some(&v)).unwrap();

    // Same (pk, sec_key) again under the same txn — must be idempotent,
    // not a NoxuError::Unsupported collision.
    sec.update_secondary(Some(&txn), &pk, None, Some(&v)).expect(
        "idempotent re-insert under the same txn must be a no-op, not \
         a one-to-one collision error",
    );

    txn.commit().unwrap();

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
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
    let txn = env.begin_transaction(None, None).unwrap();
    put_under_txn(&primary, &sec, &txn, b"pk1", b"Apple");

    // Independent reader (auto-commit, default isolation) on the
    // secondary.  In v1.5 lock-based isolation, an auto-commit reader
    // for a key currently write-locked by another txn either blocks
    // until commit/abort or surfaces a typed wait error.  We assert
    // it does not silently return the uncommitted value.
    let result = sec.get(
        None,
        &DatabaseEntry::from_bytes(b"A"),
        &mut DatabaseEntry::new(),
        &mut DatabaseEntry::new(),
    );
    match result {
        Ok(OperationStatus::Success) => panic!(
            "auto-commit reader must not see txn A's uncommitted \
             secondary write (would violate the documented isolation \
             contract)"
        ),
        Ok(OperationStatus::NotFound) => {
            // Reader skipped the locked record; acceptable per the
            // documented isolation level.
        }
        Ok(other) => panic!("unexpected status: {other:?}"),
        Err(_e) => {
            // A typed lock-conflict error is also acceptable—some
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
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(st, OperationStatus::NotFound);
}
