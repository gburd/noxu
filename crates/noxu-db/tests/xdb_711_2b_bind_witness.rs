//! XDB-711 #2b — EVERY name-binding DDL path must take the catalog NameLN lock
//! FIRST, then re-check existence, then mutate (S1, silent cross-database
//! corruption).
//!
//! Root cause (`xdb-711-fix.md` §8b): the TRANSACTIONAL create path
//! (`Environment::open_database`, the `is_transactional_create` branch) took
//! **NO** name-record lock.  So a transactional create of a name could bind it
//! concurrently with a rename-to (or another create) of the SAME name: TWO
//! committed NameLN bindings for one name, written at different LSNs, and
//! recovery's latest-NameLN-per-name-wins full replay cross-binds them — one
//! database's committed records surface under another database's name and the
//! correct database's records are lost, SILENTLY (recovery returns Ok; verify
//! passes).  JE `DbTree.doCreateDb` takes `lockNameLN(WRITE)` under the
//! creating locker before inserting the NameLN (DbTree.java:586-609); whoever
//! takes the name's NameLN lock second finds the key live and fails / returns
//! the existing db (`Environment.setupDatabase`: a non-exclusive create of an
//! existing name opens it; a rename onto an existing name → DatabaseAlreadyExists).
//!
//! The fix locks EVERY name-binding path (transactional create, auto-commit
//! create, rename-to, recreate) FIRST, then re-validates under the held lock.
//! These witnesses are DETERMINISTIC in a single process: the in-doubt txn T
//! holds its NameLN lock across the whole op, and the concurrent conflicting op
//! runs with a short per-lock timeout so it fails FAST with a typed lock error
//! (never races through).  A race-through (the pre-#2b base) binds a second
//! copy of the name — caught here directly (the concurrent op SUCCEEDS where it
//! must conflict), and after T commits + a crash-free reopen the name must bind
//! to exactly ONE id with no cross-db records.
//!
//! RED on `b9ef3aaf` (transactional create takes no name lock): the concurrent
//! bind of the same name SUCCEEDS.  GREEN with the #2b lock: it fails typed.

#![allow(clippy::unwrap_used)]

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig, NoxuError};
use std::path::Path;
use tempfile::TempDir;

// Short per-lock timeout: a conflicting concurrent op fails fast with a typed
// LockTimeout instead of blocking the test thread forever, making every case
// deterministic in a single process (mirror of liveddl_711.rs).
const LOCK_MS: u64 = 400;

fn open_env(dir: &Path) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_lock_timeout(LOCK_MS)
            // Daemons off so no background relog/cleaner perturbs the catalog
            // between the concurrent op and the reopen oracle.
            .with_run_checkpointer(false)
            .with_run_cleaner(false)
            .with_run_evictor(false)
            .with_run_in_compressor(false)
            .with_run_verifier(false),
    )
    .unwrap()
}

fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Create `name` with a few committed records, each value tagged with the
/// owning name so a cross-db bind is self-detecting, then close the handle.
fn seed(env: &Environment, name: &str) {
    let db = env.open_database(None, name, &db_cfg()).unwrap();
    for i in 0..4u32 {
        let t = env.begin_transaction(None).unwrap();
        db.put_in(&t, i.to_be_bytes(), format!("{name}-{i}").as_bytes())
            .unwrap();
        t.commit().unwrap();
    }
    db.close().unwrap();
}

fn exists(env: &Environment, name: &str) -> bool {
    env.database_names().unwrap().contains(&name.to_string())
}

/// All record VALUES under `name` (auto-commit read), or None if absent.
fn values(env: &Environment, name: &str) -> Option<Vec<String>> {
    let db = env
        .open_database(
            None,
            name,
            &DatabaseConfig::new().with_transactional(true),
        )
        .ok()?;
    let mut out = Vec::new();
    {
        let mut cur = db.open_cursor(None).unwrap();
        while let Some((_k, v)) = cur.next().unwrap() {
            out.push(String::from_utf8_lossy(v.as_ref()).into_owned());
        }
    }
    db.close().ok();
    out.sort();
    Some(out)
}

fn is_typed_conflict(e: &NoxuError) -> bool {
    matches!(
        e,
        NoxuError::LockTimeout { .. }
            | NoxuError::LockConflict(_)
            | NoxuError::DeadlockDetected
            | NoxuError::LockNotAvailable
            | NoxuError::DatabaseAlreadyExists(_)
    )
}

/// Reopen the env (full-scan recovery) and assert `name` binds to exactly its
/// own records — never a cross-db set — proving no second committed binding
/// survived to the recovered catalog.
fn assert_single_bind_after_reopen(dir: &Path, name: &str, own_prefix: &str) {
    let env = open_env(dir);
    let vals = values(&env, name)
        .unwrap_or_else(|| panic!("{name} absent after reopen"));
    for v in &vals {
        assert!(
            v.starts_with(own_prefix),
            "XDB-711 #2b: {name} recovered a CROSS-DB record {v:?} \
             (expected only {own_prefix}-*): a second committed binding \
             survived — {vals:?}"
        );
    }
    env.close().ok();
}

// ---------------------------------------------------------------------------
// 1. create-vs-rename-to: exactly one binds; the other errors (JE).
// ---------------------------------------------------------------------------

/// T: transactional CREATE "delta" (holds delta's NameLN WRITE lock, in doubt);
/// concurrent auto-commit rename "bravo" -> "delta".  JE: the rename-to's
/// create of the "delta" NameLN WRITE-locks it and blocks behind T; with the
/// short lock timeout it fails typed.  Exactly ONE binding (T's create) ends
/// up committed for "delta".  Pre-#2b: T's create takes no lock, so the rename
/// slips through and BOTH bind "delta" -> cross-bind on recovery.
#[test]
fn create_vs_rename_to_exactly_one_binds() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    seed(&env, "bravo");

    let txn = env.begin_transaction(None).unwrap();
    // T creates "delta" transactionally and writes a tagged record.
    let d = env.open_database(Some(&txn), "delta", &db_cfg()).unwrap();
    d.put_in(&txn, 0u32.to_be_bytes(), b"delta-0").unwrap();
    d.close().unwrap();

    // Concurrent auto-commit rename bravo -> delta: must conflict on delta's
    // in-doubt NameLN WRITE lock (held by T), not bind a second "delta".
    let concurrent = env.rename_database(None, "bravo", "delta");
    assert!(
        concurrent.is_err()
            && is_typed_conflict(concurrent.as_ref().unwrap_err()),
        "XDB-711 #2b: rename-to the in-doubt create target did NOT conflict \
         (got {concurrent:?}) — a second binding of 'delta' was committed"
    );

    txn.commit().unwrap();
    // delta is T's db; bravo must still exist (its rename was rejected).
    assert!(exists(&env, "delta"), "delta absent after committed create");
    assert!(exists(&env, "bravo"), "bravo lost to a rejected rename");
    assert_eq!(values(&env, "delta"), Some(vec!["delta-0".to_string()]));
    env.close().ok();

    assert_single_bind_after_reopen(dir.path(), "delta", "delta");
}

// ---------------------------------------------------------------------------
// 2. create-vs-create: exactly one binds.
// ---------------------------------------------------------------------------

/// T: transactional CREATE "gamma" (in doubt); concurrent auto-commit create of
/// "gamma".  The concurrent create must conflict on T's in-doubt NameLN WRITE
/// lock (or observe it pending) — never a second independent binding.
#[test]
fn create_vs_create_exactly_one_binds() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());

    let txn = env.begin_transaction(None).unwrap();
    let g = env.open_database(Some(&txn), "gamma", &db_cfg()).unwrap();
    g.put_in(&txn, 0u32.to_be_bytes(), b"gamma-0").unwrap();
    g.close().unwrap();

    // Concurrent auto-commit create of the same name: must conflict / observe
    // the in-doubt create, never create a second "gamma".
    let concurrent = env.open_database(None, "gamma", &db_cfg());
    let conflicted = match &concurrent {
        Ok(db) => {
            db.close().ok();
            false
        }
        Err(e) => is_typed_conflict(e),
    };
    assert!(
        conflicted,
        "XDB-711 #2b: concurrent create of the in-doubt name did NOT conflict \
         (ok={}, err={:?})",
        concurrent.is_ok(),
        concurrent.as_ref().err()
    );

    txn.commit().unwrap();
    assert!(exists(&env, "gamma"), "gamma absent after committed create");
    assert_eq!(values(&env, "gamma"), Some(vec!["gamma-0".to_string()]));
    env.close().ok();

    assert_single_bind_after_reopen(dir.path(), "gamma", "gamma");
}

// ---------------------------------------------------------------------------
// 3. recreate-vs-rename: recreate after remove races a rename-to the same name.
// ---------------------------------------------------------------------------

/// Seed "echo", remove it (committed), then T transactionally RECREATES "echo"
/// (in doubt, holds echo's NameLN WRITE lock).  Concurrent auto-commit rename
/// "bravo" -> "echo" must conflict — exactly one binding for "echo" (T's fresh
/// recreate).  Pre-#2b: the transactional recreate took no lock, so a rename-to
/// could bind a second "echo".
#[test]
fn recreate_vs_rename_to_exactly_one_binds() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    seed(&env, "bravo");
    seed(&env, "echo");
    // Remove echo (committed) so the recreate below is a fresh binding.
    env.remove_database(None, "echo").unwrap();
    assert!(!exists(&env, "echo"));

    let txn = env.begin_transaction(None).unwrap();
    let e = env.open_database(Some(&txn), "echo", &db_cfg()).unwrap();
    e.put_in(&txn, 0u32.to_be_bytes(), b"echo-new-0").unwrap();
    e.close().unwrap();

    // Concurrent rename bravo -> echo: conflicts on echo's in-doubt WRITE lock.
    let concurrent = env.rename_database(None, "bravo", "echo");
    assert!(
        concurrent.is_err()
            && is_typed_conflict(concurrent.as_ref().unwrap_err()),
        "XDB-711 #2b: rename-to the in-doubt recreate target did NOT conflict \
         (got {concurrent:?}) — a second binding of 'echo' was committed"
    );

    txn.commit().unwrap();
    assert!(exists(&env, "echo"), "echo absent after committed recreate");
    assert!(exists(&env, "bravo"), "bravo lost to a rejected rename");
    assert_eq!(values(&env, "echo"), Some(vec!["echo-new-0".to_string()]));
    env.close().ok();

    // After reopen, echo must hold ONLY its freshly-recreated record, never
    // bravo's records (the cross-bind the pre-#2b base produced).
    assert_single_bind_after_reopen(dir.path(), "echo", "echo-new");
}
