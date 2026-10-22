//! ENG-AUTODDL-1 / -3 witness: an AUTO-COMMIT (txn = None) catalog DDL must be
//! atomic AND durable at return, exactly as a transactional DDL is, matching
//! JE's `DbNameOperation` → `LockerFactory.getWritableLocker` →
//! `Txn.createAutoTxn` (committed at the env default durability, COMMIT_SYNC;
//! Environment.java:1176-1243, LockerFactory.java:64-120, Txn.java:324-356,625).
//!
//! Generalises `fixes/coord_autocommit_ddl_probe.rs`: the child does an
//! auto-commit rename / remove / truncate / create then `process::exit(0)`
//! (no destructors, no later user-space log flush = SIGKILL-equivalent power
//! loss). The parent recovers and asserts the catalog + records are EXACTLY
//! what a durable, atomic op would leave.
//!
//! RED on rc-base (2b582eed): auto-commit rename/create log their new-name
//! NameLN lazily (flush=false,fsync=false) and are NOT durable at return, so
//! after the crash `rename` recovers `[]` (the whole db is lost under an
//! orphaned id) and `create` recovers only `[a]`. GREEN once the auto-commit
//! path runs through an internal auto-txn SYNC commit.
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::process::Command;

fn cfg(p: &std::path::Path) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
}

fn dbcfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Child: build the pre-state (db "a" with a SYNC-durable record), do the
/// auto-commit op named by `AUTODDL_MODE`, then power-cut via `process::exit`.
#[test]
fn autoddl_crash_child() {
    let Ok(dir) = std::env::var("AUTODDL_DIR") else { return };
    let mode = std::env::var("AUTODDL_MODE").unwrap();
    let env = Environment::open(cfg(dir.as_ref())).unwrap();
    let db = env.open_database(None, "a", &dbcfg()).unwrap();
    db.put(b"k", b"v").unwrap(); // auto-commit SYNC put: "a" + k durable
    db.close().unwrap();
    match mode.as_str() {
        "rename" => env.rename_database(None, "a", "b").unwrap(),
        "remove" => env.remove_database(None, "a").unwrap(),
        "truncate" => {
            env.truncate_database(None, "a").unwrap();
        }
        "create" => {
            env.open_database(None, "c", &dbcfg()).unwrap().close().unwrap()
        }
        other => panic!("unknown AUTODDL_MODE {other}"),
    }
    // No flush_log / checkpoint / clean close: a durable auto-commit op must
    // already be on disk here. Power-cut.
    std::process::exit(0);
}

/// Recover `dir` and return the sorted `name -> get("k")` view.
fn recover_view(dir: &std::path::Path) -> Vec<String> {
    let env = Environment::open(cfg(dir)).unwrap();
    let mut names = env.database_names().unwrap();
    names.sort();
    let rdcfg = DatabaseConfig::new().with_transactional(true);
    let view: Vec<String> = names
        .iter()
        .map(|n| {
            let d = env.open_database(None, n, &rdcfg).unwrap();
            let v = d
                .get(b"k")
                .ok()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).to_string());
            d.close().ok();
            format!("{n}:k={v:?}")
        })
        .collect();
    env.close().ok();
    view
}

fn run_child(dir: &std::path::Path, mode: &str) {
    let st = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "autoddl_crash_child",
            "--test-threads=1",
            "--nocapture",
        ])
        .env("AUTODDL_DIR", dir)
        .env("AUTODDL_MODE", mode)
        .status()
        .unwrap();
    assert!(st.success(), "child mode={mode} exited {st:?}");
}

#[test]
fn autoddl_crash_parent() {
    if std::env::var("AUTODDL_DIR").is_ok() {
        return; // we are the child invocation
    }

    // rename: durable+atomic => after crash the db lives under "b".
    {
        let dir = tempfile::tempdir().unwrap();
        run_child(dir.path(), "rename");
        let v = recover_view(dir.path());
        assert_eq!(
            v,
            vec!["b:k=Some(\"v\")".to_string()],
            "ENG-AUTODDL-1: auto rename lost/leaked the db after crash: {v:?}"
        );
    }

    // remove: durable => after crash the db is gone (tombstone already fsync'd
    // on base, so this is the control that must stay green).
    {
        let dir = tempfile::tempdir().unwrap();
        run_child(dir.path(), "remove");
        let v = recover_view(dir.path());
        assert!(
            v.is_empty(),
            "auto remove: db should be gone after crash: {v:?}"
        );
    }

    // truncate: durable => after crash "a" exists but k is gone.
    {
        let dir = tempfile::tempdir().unwrap();
        run_child(dir.path(), "truncate");
        let v = recover_view(dir.path());
        assert_eq!(
            v,
            vec!["a:k=None".to_string()],
            "ENG-AUTODDL: auto truncate not durable after crash: {v:?}"
        );
    }

    // create: durable => after crash BOTH "a" (with k) and "c" exist.
    {
        let dir = tempfile::tempdir().unwrap();
        run_child(dir.path(), "create");
        let v = recover_view(dir.path());
        assert_eq!(
            v,
            vec!["a:k=Some(\"v\")".to_string(), "c:k=None".to_string()],
            "ENG-AUTODDL-3: auto create not durable after crash: {v:?}"
        );
    }
}
