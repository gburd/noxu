//! BUG-DUP-DELETEALL-SPAN regression: `Database::delete` / `delete_in` on a
//! sorted-dup key whose duplicates SPAN more than one BIN must remove every
//! duplicate, not just the first BIN's worth.
//!
//! Pre-existing at v7.11.0 (reproduces with non-transactional `delete`, which
//! never touches KD-slot retention): the dup delete-all loop re-searches
//! `SearchMode::Set` after each delete, and `first_entry_at_or_after*` stopped
//! at the exhausted floor BIN instead of advancing to the next BIN, so the
//! loop reported `Ok(true)` while leaving most dups behind — silent partial
//! deletion of a key reported as success.  Fixed by the range-seek advance
//! (integrate fa8ec26d; `first_live_slot_after_bin`).  JE
//! `Cursor.searchRange` -> `searchRangeAdvanceAndCheckKey` ->
//! `CursorImpl.getNext` crosses BINs (Cursor.java:4288-4293,:4523-4550).
#![cfg(not(noxu_shuttle))]

use noxu_db::{
    Database, DatabaseConfig, Durability, Environment, EnvironmentConfig,
};
use std::path::Path;

fn env_cfg(p: &Path) -> EnvironmentConfig {
    // DEFAULT node_max_entries on purpose: the bug reproduces at the default
    // BIN size as soon as one key's dups span a BIN.
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_durability(Durability::COMMIT_SYNC);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(false);
    c.set_run_in_compressor(false);
    c
}
fn dupdb(env: &Environment) -> Database {
    env.open_database(
        None,
        "dd",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(true),
    )
    .unwrap()
}

#[test]
fn delete_removes_every_dup_when_dups_span_bins() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let db = dupdb(&env);
    for i in 0..300u32 {
        db.put(b"k", format!("v{i:05}").as_bytes()).unwrap();
    }
    assert_eq!(db.count().unwrap(), 300);
    assert!(db.delete(b"k").unwrap(), "delete reports success");
    assert_eq!(db.count().unwrap(), 0, "every spanning dup must be removed");
    assert!(db.get(b"k").unwrap().is_none());
    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn delete_in_txn_removes_every_dup_across_many_keys() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let db = dupdb(&env);
    let key = |i: u32| format!("k{i:03}").into_bytes();
    for i in 0..10u32 {
        for j in 0..200u32 {
            db.put(key(i), format!("v{j:05}").as_bytes()).unwrap();
        }
    }
    assert_eq!(db.count().unwrap(), 2000);
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..10u32 {
        assert!(db.delete_in(&txn, key(i)).unwrap(), "delete_in key {i}");
    }
    txn.commit().unwrap();
    assert_eq!(db.count().unwrap(), 0, "all 2000 spanning dups removed");
    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn nontxn_delete_removes_every_dup_across_many_keys() {
    // Same as above but non-transactional — proves the bug (and fix) is
    // unrelated to KD-slot retention (non-txn delete physically removes).
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let db = dupdb(&env);
    let key = |i: u32| format!("k{i:03}").into_bytes();
    for i in 0..10u32 {
        for j in 0..200u32 {
            db.put(key(i), format!("v{j:05}").as_bytes()).unwrap();
        }
    }
    assert_eq!(db.count().unwrap(), 2000);
    for i in 0..10u32 {
        assert!(db.delete(key(i)).unwrap(), "delete key {i}");
    }
    assert_eq!(db.count().unwrap(), 0, "all 2000 spanning dups removed");
    db.close().unwrap();
    env.close().unwrap();
}
