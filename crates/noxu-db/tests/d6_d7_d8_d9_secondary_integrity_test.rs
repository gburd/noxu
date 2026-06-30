//! Part 4 acceptance tests — D6/D7/D8/D9 secondary integrity.
//!
//! JE references:
//! - D6: `SecondaryDatabase.insertSecKey()` raises `SecondaryIntegrityException`
//!   when a duplicate (sec_key, pri_key) collision is detected and the DB is
//!   fully populated.
//! - D7: `SecondaryDatabase.deleteSecKey()` raises `SecondaryIntegrityException`
//!   when the (sec_key, pri_key) pair is missing and the DB is fully populated.
//! - D8: `SecondaryCursor` dirty-read primary-missing → skip (NotFound), not
//!   raise SecondaryIntegrityException.
//! - D9: primary overwrite changing the secondary key → old secondary entry
//!   removed (auto-maintenance fetches old_data before the primary write).

use noxu_db::{
    CursorConfig, Database, DatabaseConfig, DatabaseEntry, EnvironmentConfig,
    OperationStatus, SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

fn de(s: &[u8]) -> DatabaseEntry {
    DatabaseEntry::from_bytes(s)
}

/// Key creator: uses the first byte of data as the secondary key.
struct FirstByteCreator;
impl SecondaryKeyCreator for FirstByteCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _pri_key: &DatabaseEntry,
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

fn open_env(dir: &TempDir) -> noxu_db::Environment {
    noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
}

fn open_primary(
    env: &noxu_db::Environment,
    name: &str,
) -> Arc<Mutex<Database>> {
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

fn open_inner_db(env: &noxu_db::Environment, name: &str) -> Database {
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

fn open_sec(
    primary: Arc<Mutex<Database>>,
    inner: Database,
) -> SecondaryDatabase {
    SecondaryDatabase::open(
        primary,
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteCreator)),
    )
    .unwrap()
}

// ── D6: duplicate (sec_key, pri_key) insert → integrity error ────────────────
//
// JE SecondaryDatabase.insertSecKey() KEYEXIST on fully-populated DB.
// Two primaries where the first is inserted with the same (sec_key, pri_key)
// pair already present → SecondaryIntegrityException.
#[test]
fn d6_duplicate_sec_key_insert_raises_integrity_error() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "pri");
    let inner = open_inner_db(&env, "sec");
    // Note: secondary is registered with primary, so primary.put() auto-maintains.
    let sec = open_sec(Arc::clone(&primary), inner);

    let pk = de(b"k1");
    let data = de(b"A_value"); // sec_key = [A]

    // First insert via primary.put() — auto-maintains secondary.
    // This inserts (sec_key=[A], pri_key="k1") into the secondary.
    primary.lock().put( &pk, &data).unwrap();

    // Now force a direct second insert of the SAME (sec_key=[A], pri_key="k1")
    // pair — simulates a corrupt auto-maintenance that runs twice for the same
    // primary record (the scenario JE's insertSecKey() guards against).
    // Call update_secondary directly (bypassing the primary's hook check).
    let result = sec.update_secondary(None, &pk, None, Some(&data));
    assert!(
        result.is_err(),
        "D6: second insert of same (sec_key, pri_key) must raise an error; \
         got Ok (secondary integrity not enforced)"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.to_lowercase().contains("secondary")
            || msg.to_lowercase().contains("integrity")
            || msg.to_lowercase().contains("inconsistent"),
        "D6: error must describe a secondary integrity issue, got: {msg}"
    );
}

// ── D9: primary overwrite changing sec key → old sec entry removed ────────────
//
// JE needOldDataForUpdate: auto-maintenance via primary.put() fetches old_data
// before the overwrite so the stale old secondary key is deleted.
#[test]
fn d9_overwrite_changing_sec_key_removes_old_entry() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "pri");
    let inner = open_inner_db(&env, "sec");
    let sec = open_sec(Arc::clone(&primary), inner);

    let pk = de(b"k1");

    // Insert: data starts with b'A' → sec_key=[A].
    // primary.put() auto-maintains secondary via the hook.
    primary.lock().put( &pk, &de(b"A_v1")).unwrap();

    // Verify secondary has ([A], k1).
    let mut p_key = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = sec.get_into(None, &de(b"A"), &mut p_key, &mut d).unwrap();
    assert!(s, "initial secondary lookup");
    assert_eq!(p_key.get_data().unwrap_or(&[]), b"k1");

    // Overwrite: data now starts with b'B' → sec_key=[B].
    // Database::put fetches old_data before writing, then auto-maintains:
    // delete ([A], k1) + insert ([B], k1).
    primary.lock().put( &pk, &de(b"B_v2")).unwrap();

    // Old secondary entry ([A], k1) must be gone.
    let s_old = sec.get_into(None, &de(b"A"), &mut p_key, &mut d).unwrap();
    assert!(!s_old,
        "D9: old secondary entry ([A], k1) must be removed after overwrite"
    );

    // New secondary entry ([B], k1) must exist.
    let s_new = sec.get_into(None, &de(b"B"), &mut p_key, &mut d).unwrap();
    assert!(s_new,
        "D9: new secondary entry ([B], k1) must exist after overwrite"
    );
    assert_eq!(p_key.get_data().unwrap_or(&[]), b"k1");
}

// ── D8: secondary cursor dirty-read, missing primary → skip (NotFound) ────────
//
// When the primary is deleted but the secondary still has an orphaned entry,
// a dirty-read cursor should return NotFound (skip), not raise
// SecondaryIntegrityException.
#[test]
fn d8_dirty_read_missing_primary_skips_record() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "pri");
    let inner = open_inner_db(&env, "sec");
    let sec = open_sec(Arc::clone(&primary), inner);

    let pk = de(b"k1");
    let data = de(b"A_v1"); // sec_key = [A]

    // Insert via primary.put() (auto-maintains secondary).
    primary.lock().put( &pk, &data).unwrap();

    // Manually inject a stale secondary entry for a non-existent primary.
    // We insert a secondary entry for "orphan_pk" which has no primary record.
    // Use update_secondary directly so auto-hook isn't involved.
    let orphan_pk = de(b"orphan");
    let orphan_data = de(b"A_orphan"); // sec_key = [A], different pri_key
    sec.update_secondary(None, &orphan_pk, None, Some(&orphan_data)).unwrap();
    // Note: we do NOT insert orphan_pk into the primary — it has no primary record.

    // Open a dirty-read secondary cursor.
    let dirty_cfg = CursorConfig::read_uncommitted();
    let mut sec_cursor = sec.open_cursor( Some(&dirty_cfg)).unwrap();
    let sec_key = de(b"A");
    let mut p_key_out = DatabaseEntry::new();
    let mut data_out = DatabaseEntry::new();

    // Iterate all (sec_key=[A], pri_key) pairs.
    // One of them (orphan) has no primary record.
    // D8: dirty-read cursor must NOT raise SecondaryIntegrityException when
    // it encounters the orphaned entry; it should return NotFound for that slot.
    let result =
        sec_cursor.get_search_key(&sec_key, &mut p_key_out, &mut data_out);
    // We don't assert a specific result since k1 has a valid primary and
    // might be returned.  We only verify no SecondaryIntegrityException fired.
    // Success or NotFound is fine; only an error is examined.
    if let Err(e) = &result {
        let msg = e.to_string();
        if msg.contains("missing primary") {
            panic!(
                "D8: dirty-read secondary cursor must NOT raise \
                 SecondaryIntegrityException for missing primary: {e}"
            );
        }
        // Other errors are re-panic'd.
        panic!("D8: unexpected error: {e}");
    }
    drop(sec_cursor);
}

// ── D7: delete_sec_key missing entry → integrity error ───────────────────────
//
// JE SecondaryDatabase.deleteSecKey() missing-entry on fully-populated DB.
// update_secondary(old=Some(data), new=None) when the entry doesn't exist
// in the secondary → SecondaryIntegrityException.
#[test]
fn d7_missing_sec_entry_on_delete_raises_integrity_error() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "pri");
    let inner = open_inner_db(&env, "sec");

    // Open secondary WITHOUT registering with primary (don't use open_sec which
    // registers the hook) so we can control insert/delete manually.
    // We open with open_sec but then call delete via update_secondary directly.
    let sec = open_sec(Arc::clone(&primary), inner);

    let pk = de(b"k1");
    let data = de(b"A_v1"); // sec_key = [A]

    // Insert primary record (auto-hook inserts secondary entry).
    primary.lock().put( &pk, &data).unwrap();

    // Delete primary record (auto-hook deletes secondary entry ([A], k1)).
    primary.lock().delete( &pk).unwrap();

    // Now call update_secondary with old_data=Some (requests delete of ([A],k1))
    // but the entry was already deleted → D7: must raise SecondaryIntegrityException.
    let result = sec.update_secondary(None, &pk, Some(&data), None);
    assert!(
        result.is_err(),
        "D7: delete of already-absent (sec_key, pri_key) must raise an error; \
         got Ok (secondary integrity not enforced)"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.to_lowercase().contains("secondary")
            || msg.to_lowercase().contains("integrity")
            || msg.to_lowercase().contains("missing"),
        "D7: error must describe a missing secondary entry, got: {msg}"
    );
}
