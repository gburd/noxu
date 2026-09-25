//! NEW-3 — cross-BIN cursor delete-all traversal.
//!
//! Regression for a bug unmasked by the NEW-2 NODE_MAX-recovery fix: a cursor
//! `Get::Next` delete-all loop over a multi-BIN database (NODE_MAX=4, 12 keys,
//! ~4 BINs) deletes only the entries in the FIRST BIN and then stops. After a
//! `delete()` the cursor clears `current_key`, so when the current BIN is
//! exhausted the cross-BIN advance in `retrieve_next` has no anchor key and
//! returns NotFound — the traversal never crosses into the next BIN.
//!
//! JE contract: `Cursor.getNext` after `Cursor.delete` keeps the cursor
//! positioned at the deleted slot (the "gap") and `CursorImpl.getNext`
//! advances to the next live record, crossing BIN boundaries via
//! `Tree.getNextBin` (CursorImpl.java getNext / getNextNoDup; the deleted
//! slot is skipped and the boundary crossed on the BIN's next-key). The
//! deleted-but-not-yet-compressed slot still carries its key, so JE always
//! has an anchor for the cross-BIN step.
//!
//! We assert:
//!   1. A plain `Get::Next` scan (no deletes) visits ALL 12 keys — isolates
//!      whether the traversal itself is broken vs. the delete interaction.
//!   2. A `Get::Next` + `delete()` loop deletes ALL 12 keys and leaves the DB
//!      empty.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
    StatsConfig,
};
use std::collections::BTreeSet;
use std::path::Path;
use tempfile::TempDir;

const NODE_MAX: u32 = 4;
const N_KEYS: u32 = 12;

fn open_env(dir: &Path) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_node_max_entries(NODE_MAX);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "simpleDB",
        &DatabaseConfig::new().with_allow_create(true),
    )
    .unwrap()
}

/// Fixed-width ascending key so the tree splits deterministically.
fn ikey(i: u32) -> String {
    format!("{i:08}")
}

fn populate(db: &noxu_db::Database) {
    for i in 0..N_KEYS {
        let k = ikey(i);
        db.put(
            DatabaseEntry::from_bytes(k.as_bytes()),
            DatabaseEntry::from_bytes(k.as_bytes()),
        )
        .unwrap();
    }
}

fn bin_count(db: &noxu_db::Database) -> u64 {
    db.stats(Some(&StatsConfig::new().with_fast(false)))
        .unwrap()
        .btree
        .bottom_internal_node_count
}

/// Control: a plain forward scan must visit every key across every BIN.
#[test]
fn plain_get_next_scan_visits_all_keys_across_bins() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env);
    populate(&db);
    assert!(
        bin_count(&db) >= 3,
        "test setup: NODE_MAX={NODE_MAX} with {N_KEYS} keys should span \
         >=3 BINs, got {}",
        bin_count(&db)
    );

    let mut c = db.open_cursor(None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut val = DatabaseEntry::new();
    let mut seen = BTreeSet::new();
    while c.get(&mut key, &mut val, Get::Next, None).unwrap()
        == OperationStatus::Success
    {
        seen.insert(key.data_opt().unwrap_or(&[]).to_vec());
    }
    c.close().unwrap();

    let expected: BTreeSet<Vec<u8>> =
        (0..N_KEYS).map(|i| ikey(i).into_bytes()).collect();
    assert_eq!(
        seen,
        expected,
        "plain Get::Next scan must visit all {N_KEYS} keys across all BINs; \
         visited {} of {N_KEYS}",
        seen.len()
    );

    db.close().unwrap();
    env.close().unwrap();
}

/// NEW-3 regression: delete-all via Get::Next + delete() must remove every key
/// across every BIN and leave the DB empty.
#[test]
fn get_next_delete_loop_removes_all_keys_across_bins() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env);
    populate(&db);
    let pre_bins = bin_count(&db);
    assert!(
        pre_bins >= 3,
        "test setup: NODE_MAX={NODE_MAX} with {N_KEYS} keys should span \
         >=3 BINs, got {pre_bins}"
    );

    let mut deleted = 0u32;
    {
        let mut c = db.open_cursor(None).unwrap();
        let mut key = DatabaseEntry::new();
        let mut val = DatabaseEntry::new();
        while c.get(&mut key, &mut val, Get::Next, None).unwrap()
            == OperationStatus::Success
        {
            assert_eq!(c.delete().unwrap(), OperationStatus::Success);
            deleted += 1;
        }
        c.close().unwrap();
    }
    assert_eq!(
        deleted, N_KEYS,
        "Get::Next + delete() loop must delete all {N_KEYS} keys \
         (pre-delete bins={pre_bins}); deleted only {deleted}"
    );

    // The database must be empty.
    let mut c = db.open_cursor(None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut val = DatabaseEntry::new();
    let remaining = c.get(&mut key, &mut val, Get::First, None).unwrap();
    c.close().unwrap();
    assert_eq!(
        remaining,
        OperationStatus::NotFound,
        "after delete-all the DB must be empty"
    );

    db.close().unwrap();
    env.close().unwrap();
}
