//! NEW-EVOLVE-ABORT-ROLLBACK regression (shared noxu-dbi layer).
//!
//! `CursorImpl::get_slot_before_image` captures the abort-undo before-image
//! for every write path. When the target record was made NON-RESIDENT by the
//! cache evictor (its in-memory LN data was stripped / its BIN detached, so
//! `get_data_from_tree` surfaces `Some(empty_vec)` even though the slot still
//! carries a valid LSN), the before-image path used to return that empty vec.
//! The undo record then stored an EMPTY `abort_data`, and on abort the undo
//! loop did `tree.insert(key, [], lsn)` -- DURABLY OVERWRITING the caller,s
//! original record with 0 bytes. This is a general abort-atomicity DATA-LOSS
//! bug: ANY abort of an update to an evicted record could zero it out. The
//! DPL evolution failure (`evolve_aborts_on_listener_failure`) was one
//! surface; this test proves the corruption at the noxu-db level directly.
//!
//! Fix (`get_slot_before_image`): when the resident data is empty but the LSN
//! is valid, re-fetch the LN from the log (`fetch_ln_data_from_log`), exactly
//! as the read path,s `rehydrate_current_data` already does
//! (JE `IN.fetchTarget` / `CursorImpl.LockStanding.prepareForUpdate`). Guarded
//! by `!fetched.is_empty()` so a GENUINELY-empty stored value is left empty.

use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            // Small cache so ordinary insert pressure evicts real records.
            .with_cache_size(1024 * 1024),
    )
    .unwrap()
}

fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Fill well past the ~1 MiB cache budget with distinct 1 KiB records, then
/// checkpoint (so slots carry valid logged LSNs -- the strip precondition)
/// and drive eviction. This forces the target record NON-RESIDENT through the
/// production evictor path (no synthetic strip, no internal-API poking).
fn fill_and_evict(env: &Environment, db: &Database) {
    for i in 0..4000u32 {
        db.put(i.to_be_bytes(), vec![0x76u8; 1000]).unwrap();
    }
    env.checkpoint(None).unwrap();
    for _ in 0..300 {
        let _ = env.evict_memory().unwrap();
    }
}

/// The load-bearing proof: an update to an EVICTED non-empty record, then an
/// abort, must restore the ORIGINAL value -- not an empty (0-byte) record.
///
/// Without the fix this fails with `get(...) == Some([])` (durable 0-byte
/// corruption).
#[test]
fn update_of_evicted_record_then_abort_restores_original() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env.open_database(None, "d", &db_cfg()).unwrap();

    db.put(b"the-key", b"ORIGINAL_VALUE").unwrap();
    fill_and_evict(&env, &db);

    let txn = env.begin_transaction(None).unwrap();
    db.put_in(&txn, b"the-key", b"NEW_VALUE").unwrap();
    txn.abort().unwrap();

    let got = db.get(b"the-key").unwrap();
    assert_eq!(
        got.as_deref(),
        Some(b"ORIGINAL_VALUE".as_ref()),
        "abort of an update to an evicted record must restore the original \
         value, not a 0-byte record; got {got:?}"
    );
}

/// The disambiguation control: a GENUINELY-empty (0-byte) stored value must
/// survive an update+abort AS EMPTY. `get_data_from_tree` returns
/// `Some(empty_vec)` for BOTH a stripped LN and a real 0-byte value; the fix
/// keys off `!fetched.is_empty()`, so the re-fetch of a genuinely-empty record
/// (log data is empty) does NOT fire and the empty image is kept. This test
/// fails if the fix ever mis-restores a genuine empty value to some non-empty
/// log bytes.
#[test]
fn update_of_evicted_genuinely_empty_record_then_abort_stays_empty() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env.open_database(None, "d", &db_cfg()).unwrap();

    db.put(b"the-key", b"").unwrap(); // genuine 0-byte value
    fill_and_evict(&env, &db);

    let txn = env.begin_transaction(None).unwrap();
    db.put_in(&txn, b"the-key", b"NEW_VALUE").unwrap();
    txn.abort().unwrap();

    let got = db.get(b"the-key").unwrap();
    assert_eq!(
        got.as_deref(),
        Some(b"".as_ref()),
        "a genuinely-empty stored value must survive abort as empty, not be \
         mis-restored from the log; got {got:?}"
    );
}
