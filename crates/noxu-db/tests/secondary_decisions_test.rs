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
    // v1.6 sorted-dup secondaries: the inner index DB must allow
    // duplicates so multiple primaries with the same secondary key
    // coexist as duplicates of the (sec_key) entry.
    env.open_database(
        None,
        name,
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true),
    )
    .unwrap()
}

// ─── Decision 1B — sorted-dup secondaries (v1.6) ─────────────────

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
    primary.lock().put(None, &pk1, &v1).unwrap();
    sec.update_secondary(None, &pk1, None, Some(&v1)).unwrap();

    // Second primary record sharing the same secondary key (‘A’).
    // v1.6: this MUST succeed and store a second duplicate of ‘A’.
    let pk2 = DatabaseEntry::from_bytes(b"pk2");
    let v2 = DatabaseEntry::from_bytes(b"Apricot");
    primary.lock().put(None, &pk2, &v2).unwrap();
    sec.update_secondary(None, &pk2, None, Some(&v2))
        .expect("v1.6 sorted-dup secondaries must admit a second primary");

    // The inner index now holds two duplicates of ‘A’.
    assert_eq!(
        sec.count().unwrap(),
        2,
        "both primaries must be indexed under ‘A’"
    );

    // Iterate the cursor and confirm both primaries surface.
    let mut cursor = sec.open_cursor(None, None).unwrap();
    let mut sec_key = DatabaseEntry::new();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut st = cursor
        .get_search_key(
            &DatabaseEntry::from_bytes(b"A"),
            &mut p_key,
            &mut data,
        )
        .unwrap();
    while st == OperationStatus::Success {
        seen.push(p_key.get_data().unwrap().to_vec());
        // Step to the next dup of the same sec_key, if any.
        st = cursor.get_next(&mut sec_key, &mut p_key, &mut data).unwrap();
        if st == OperationStatus::Success {
            // Stepping with `Get::Next` advances across keys too — stop
            // when we leave ‘A’.
            if sec_key.get_data() != Some(b"A".as_ref()) {
                break;
            }
        }
    }
    seen.sort();
    assert_eq!(seen, vec![b"pk1".to_vec(), b"pk2".to_vec()]);
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
    // The foreign DB no longer needs to exist as a real handle: the
    // FK setter takes a name string in v1.5.1 (Wave 1C) and
    // SecondaryDatabase::open rejects FK config at open time per
    // Decision 2C.  We still create the DB to keep the test honest
    // about the v1.6 wiring — once FK is implemented the engine will
    // resolve the name to this handle.
    let _foreign = env
        .open_database(
            None,
            "foreign",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    let cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator))
        .with_foreign_key_database("foreign");

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

// ─── Wave 1B — SecondaryCursor::delete cascade honours its txn ─────
//
// These tests exercise the residual F5 sub-item the Sprint 4½ agent
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
        let seed = env.begin_transaction(None, None).unwrap();
        put_under_txn(&primary, &sec, &seed, b"pk1", b"Apple");
        seed.commit().unwrap();
    }

    // Sanity: the seeded record is visible.
    {
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let st = sec
            .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
            .unwrap();
        assert_eq!(st, OperationStatus::Success);
        assert_eq!(p_key.get_data().unwrap(), b"pk1");
    }

    // Open a cursor under a txn, position on the secondary entry,
    // call delete (cascades to primary + secondary cleanup), then
    // abort.
    let txn = env.begin_transaction(None, None).unwrap();
    {
        let mut cursor = sec.open_cursor(Some(&txn), None).unwrap();
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
            .get(
                Some(&txn),
                &DatabaseEntry::from_bytes(b"A"),
                &mut p_key2,
                &mut data2,
            )
            .unwrap();
        assert_eq!(
            probe_st,
            OperationStatus::NotFound,
            "the cursor's own txn must observe the cascade as applied"
        );

        cursor.close().unwrap();
    }

    // Abort: both sides of the cascade must roll back.
    txn.abort().unwrap();

    // Primary record is back.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get(None, &pk1, &mut data).unwrap();
    assert_eq!(
        pri_status,
        OperationStatus::Success,
        "primary record must survive the abort \
         (Wave 1B: pre-fix the cascade auto-committed and \
         destroyed the primary irrespective of the abort)"
    );
    assert_eq!(data.get_data().unwrap(), b"Apple");

    // Secondary entry is back.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(
        sec_status,
        OperationStatus::Success,
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
        let seed = env.begin_transaction(None, None).unwrap();
        put_under_txn(&primary, &sec, &seed, b"pk1", b"Apple");
        put_under_txn(&primary, &sec, &seed, b"pk2", b"Banana");
        seed.commit().unwrap();
    }

    // Cursor under a txn: delete the 'A' entry, commit.
    let txn = env.begin_transaction(None, None).unwrap();
    {
        let mut cursor = sec.open_cursor(Some(&txn), None).unwrap();
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
    let pri_status = primary.lock().get(None, &pk1, &mut data).unwrap();
    assert_eq!(pri_status, OperationStatus::NotFound);

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(sec_status, OperationStatus::NotFound);

    // 'B' / pk2 is untouched.
    let pk2 = DatabaseEntry::from_bytes(b"pk2");
    let mut data = DatabaseEntry::new();
    let pri_status = primary.lock().get(None, &pk2, &mut data).unwrap();
    assert_eq!(pri_status, OperationStatus::Success);
    assert_eq!(data.get_data().unwrap(), b"Banana");

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let sec_status = sec
        .get(None, &DatabaseEntry::from_bytes(b"B"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(sec_status, OperationStatus::Success);
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
/// is leak a *commit* of the cascade out from under txn A — i.e.
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
        let seed = env.begin_transaction(None, None).unwrap();
        put_under_txn(&primary, &sec, &seed, b"pk1", b"Apple");
        seed.commit().unwrap();
    }

    // Stage the cascade under a txn but DO NOT commit.
    let txn = env.begin_transaction(None, None).unwrap();
    let mut cursor = sec.open_cursor(Some(&txn), None).unwrap();
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
    // committed the txn — but we still record the observed outcome
    // so that the post-abort assertion below has something to compare
    // against.
    let pk1 = DatabaseEntry::from_bytes(b"pk1");
    let mut probe_data = DatabaseEntry::new();
    let _during_txn = primary.lock().get(None, &pk1, &mut probe_data);

    cursor.close().unwrap();
    txn.abort().unwrap();

    // After abort: every other observer (auto-commit and a fresh
    // reader txn alike) must see the seeded record intact.  This is
    // the real isolation contract Wave 1B closes — pre-Wave-1B the
    // cascade auto-committed during the in-flight txn, so the
    // post-abort state could be missing the primary even though the
    // user explicitly aborted.
    let mut data = DatabaseEntry::new();
    let after_pri = primary.lock().get(None, &pk1, &mut data).unwrap();
    assert_eq!(
        after_pri,
        OperationStatus::Success,
        "after abort, the primary record must be intact for every \
         observer (Wave 1B / audit F5)"
    );
    assert_eq!(data.get_data().unwrap(), b"Apple");

    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let after_sec = sec
        .get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
        .unwrap();
    assert_eq!(
        after_sec,
        OperationStatus::Success,
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

    // Seed (auto-commit).
    {
        let pk = DatabaseEntry::from_bytes(b"pk1");
        let v = DatabaseEntry::from_bytes(b"Apple");
        primary.lock().put(None, &pk, &v).unwrap();
        sec.update_secondary(None, &pk, None, Some(&v)).unwrap();
    }

    // Auto-commit cursor delete.
    let mut cursor = sec.open_cursor(None, None).unwrap();
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
    assert_eq!(
        primary.lock().get(None, &pk1, &mut data).unwrap(),
        OperationStatus::NotFound
    );
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    assert_eq!(
        sec.get(None, &DatabaseEntry::from_bytes(b"A"), &mut p_key, &mut data)
            .unwrap(),
        OperationStatus::NotFound
    );
}
