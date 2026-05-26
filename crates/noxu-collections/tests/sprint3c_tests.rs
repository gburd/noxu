//! Sprint 3C regression tests: scope-down of `noxu-collections` for v1.5.
//!
//! These tests pin down the audit findings closed (or explicitly
//! deferred) by sprint 3C in a way that future refactors can't
//! silently regress.  See
//! `docs/src/internal/sprint-3-collections-restriction.md` for the
//! full scope/narrative; per-test docstrings name the audit finding.
//!
//! Findings touched here:
//!   - #1 / #3 / #4 -- Stored* operations are auto-commit only
//!   - #5 -- StoredList::remove does not compact (rustdoc fix)
//!   - #6 -- StoredList::next_index recovery on reopen via
//!     `StoredList::open`

use std::path::Path;
use std::path::PathBuf;

use noxu_collections::{CollectionError, StoredList, StoredMap};
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fresh_env_dir() -> (TempDir, PathBuf) {
    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();
    (td, path)
}

fn open_env(path: &Path, transactional: bool) -> Environment {
    let mut cfg =
        EnvironmentConfig::new(path.to_path_buf()).with_allow_create(true);
    if transactional {
        cfg = cfg.with_transactional(true);
    }
    Environment::open(cfg).unwrap()
}

fn open_db(env: &Environment, name: &str) -> Database {
    let db_config = DatabaseConfig::new().with_allow_create(true);
    env.open_database(None, name, &db_config).unwrap()
}

// ---------------------------------------------------------------------------
// Audit findings #1, #3, #4 -- Stored* ops are auto-commit only.
// ---------------------------------------------------------------------------

/// Documents the auto-commit-only contract: every Stored* op succeeds
/// with no transaction argument.  This test locks in the v1.5 API
/// shape so the v1.6 transition (Option<&Transaction> threading)
/// has to be a deliberate, visible API change.
#[test]
fn stored_map_ops_succeed_without_txn_argument() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, /* transactional = */ true);
    let db = open_db(&env, "auto_commit_map");
    let map = StoredMap::new(&db, false);

    // No method below takes an Option<&Transaction>; every call here
    // is the v1.5 surface.  A v1.6 redesign that makes any of these
    // signatures fail to compile is the signal we want.
    assert!(map.put(b"k", b"v").unwrap().is_none());
    assert_eq!(map.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert!(map.contains_key(b"k").unwrap());
    assert_eq!(map.len().unwrap(), 1);
    assert_eq!(map.remove(b"k").unwrap(), Some(b"v".to_vec()));
    assert!(map.is_empty().unwrap());
}

/// Same intent for `StoredList`.
#[test]
fn stored_list_ops_succeed_without_txn_argument() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, /* transactional = */ true);
    let db = open_db(&env, "auto_commit_list");
    let list = StoredList::new(&db);

    assert_eq!(list.push(b"a").unwrap(), 0);
    assert_eq!(list.push(b"b").unwrap(), 1);
    assert_eq!(list.get(0).unwrap(), Some(b"a".to_vec()));
    assert_eq!(list.pop().unwrap(), Some(b"b".to_vec()));
    assert_eq!(list.remove(0).unwrap(), Some(b"a".to_vec()));
    assert!(list.is_empty().unwrap());
}

// ---------------------------------------------------------------------------
// Audit finding #6 -- StoredList::next_index reopen path.
// ---------------------------------------------------------------------------

/// Regression for finding #6.
///
/// Pre-3C behaviour: `StoredList::new(&db)` always set `next_index`
/// to 0, so a session that wrote N records, closed, and reopened
/// the list would overwrite record 0 on the next `push`.  The 3C fix
/// is `StoredList::open(&db) -> Result<Self>`, which scans
/// `Get::Last` and recovers `next_index` from the largest key.
#[test]
fn stored_list_open_recovers_next_index_after_reopen() {
    let (_td, path) = fresh_env_dir();

    // First session: push 3 entries.
    {
        let env = open_env(&path, false);
        let db = open_db(&env, "list_reopen");
        let list = StoredList::new(&db);
        assert_eq!(list.push(b"alpha").unwrap(), 0);
        assert_eq!(list.push(b"beta").unwrap(), 1);
        assert_eq!(list.push(b"gamma").unwrap(), 2);
        assert_eq!(list.next_index(), 3);
        let _ = db.close();
    }

    // Second session: reopen with `open` and confirm next_index recovery.
    {
        let env = open_env(&path, false);
        let db = open_db(&env, "list_reopen");
        let list = StoredList::open(&db).expect("open must succeed");

        assert_eq!(
            list.next_index(),
            3,
            "open() must recover next_index from the largest existing key",
        );

        // Existing entries are still there.
        assert_eq!(list.get(0).unwrap(), Some(b"alpha".to_vec()));
        assert_eq!(list.get(1).unwrap(), Some(b"beta".to_vec()));
        assert_eq!(list.get(2).unwrap(), Some(b"gamma".to_vec()));

        // Push must land at index 3, not overwrite index 0.
        let idx = list.push(b"delta").unwrap();
        assert_eq!(
            idx, 3,
            "post-reopen push must continue from recovered next_index",
        );
        assert_eq!(list.get(0).unwrap(), Some(b"alpha".to_vec()));
        assert_eq!(list.get(3).unwrap(), Some(b"delta".to_vec()));
    }
}

/// Documents the *bug* that motivated finding #6: `StoredList::new`
/// after a reopen *does* still overwrite (it is the fast path for
/// known-empty databases).  This test pins down the warned-about
/// behaviour so future code can't silently change `new`'s contract.
#[test]
fn stored_list_new_does_not_recover_and_overwrites_on_reopen() {
    let (_td, path) = fresh_env_dir();

    // First session: push 2 entries.
    {
        let env = open_env(&path, false);
        let db = open_db(&env, "list_new_reopen");
        let list = StoredList::new(&db);
        list.push(b"first").unwrap();
        list.push(b"second").unwrap();
        let _ = db.close();
    }

    // Second session: use `new` (the documented unsafe path) and
    // confirm that next_index resets to 0 and the next push
    // overwrites index 0.  This is the audit-#6 hazard the
    // rustdoc now warns about.
    {
        let env = open_env(&path, false);
        let db = open_db(&env, "list_new_reopen");
        let list = StoredList::new(&db);

        assert_eq!(
            list.next_index(),
            0,
            "new() does not recover next_index (see audit finding #6)",
        );

        let idx = list.push(b"clobber").unwrap();
        assert_eq!(idx, 0, "push after `new` reopen lands at index 0");
        assert_eq!(
            list.get(0).unwrap(),
            Some(b"clobber".to_vec()),
            "the previous value at index 0 was overwritten",
        );
    }
}

/// `open` on a brand-new (empty) database initialises next_index to 0,
/// matching `new`.  Empty-database behaviour must not regress.
#[test]
fn stored_list_open_on_empty_database_starts_at_zero() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, false);
    let db = open_db(&env, "list_empty");
    let list = StoredList::open(&db).expect("open on empty db must succeed");
    assert_eq!(list.next_index(), 0);
    assert_eq!(list.push(b"x").unwrap(), 0);
    assert_eq!(list.push(b"y").unwrap(), 1);
}

/// `open` rejects databases whose largest key is not an 8-byte
/// big-endian index, so users can't accidentally clobber an index
/// that was populated by some other writer.
#[test]
fn stored_list_open_rejects_mixed_use_database() {
    let (_td, path) = fresh_env_dir();
    let env = open_env(&path, false);
    let db = open_db(&env, "list_mixed");

    // Write a non-8-byte key directly via the Database API.
    let key = DatabaseEntry::from_bytes(b"not-an-index");
    let val = DatabaseEntry::from_bytes(b"v");
    db.put(None, &key, &val).unwrap();

    let err = StoredList::open(&db).err().expect("open must fail");
    match err {
        CollectionError::IllegalState(msg) => {
            assert!(
                msg.contains("StoredList::open"),
                "error must name the constructor: {msg}",
            );
        }
        other => panic!("expected IllegalState, got {:?}", other),
    }
}
