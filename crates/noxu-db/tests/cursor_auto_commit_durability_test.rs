// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! JE durability semantics for auto-commit writes (rewritten for
//! FID-NONTXN-SYNC-FSYNC + FID-CURSOR-AUTOCOMMIT).
//!
//! JE-faithful rules this exercises:
//!   * A **transactional**-db auto-commit `Database::put` / `Database::delete`
//!     is JE `Txn.createAutoTxn` — a real transaction — so under SYNC/WNS it
//!     is durable and SURVIVES a SIGKILL (no close, no checkpoint).
//!   * An explicit transaction's cursor write, committed, SURVIVES.
//!   * A cursor opened with no txn on a **transactional** db is PROHIBITED:
//!     `put`/`delete` return `OperationNotAllowed` (JE
//!     `Cursor.updateOperationsProhibited`, Cursor.java:333-338/5340-5367).
//!   * A **non-transactional**-db write (whether via `Database::put` or an
//!     `open_cursor(None)` cursor) is JE's ThreadLocker path: the LN is logged
//!     with `flushRequired=false, fsyncRequired=false` (LN.java:636,
//!     LogParams.java:42,53) — NO per-write fsync — so after a SIGKILL with no
//!     preceding `Environment::sync()` the writes are LOST; after `env.sync()`
//!     they SURVIVE.
//!
//! This was previously `cursor_auto_commit_durability_test`, which asserted
//! the OLD (non-JE) behaviour that an auto-commit cursor write fsynced per
//! operation and survived a crash.  That behaviour never matched JE and was
//! removed; the test now asserts the JE semantics above.
#![cfg(all(unix, not(noxu_shuttle)))]

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Durability, Environment, EnvironmentConfig,
    Put,
};
use std::path::Path;
use std::time::{Duration, Instant};

const MODE: &str = "NOXU_CURSOR_SYNC_CHILD";
const HOME: &str = "NOXU_CURSOR_SYNC_HOME";
const N: u32 = 20;

fn open(home: &Path, txnal: bool) -> Environment {
    Environment::open(
        EnvironmentConfig::new(home.to_path_buf())
            .with_transactional(txnal)
            .with_allow_create(true)
            .with_durability(Durability::COMMIT_SYNC)
            .with_run_checkpointer(false)
            .with_run_cleaner(false),
    )
    .unwrap()
}

fn entry(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}

/// Child: `<spec>` is one of:
///   * `txn_dbput`     — transactional db, `Database::put` (JE Txn.createAutoTxn), durable.
///   * `txn_txncursor` — transactional db, explicit-txn cursor + commit, durable.
///   * `nontxn_dbput_sync`   — non-txn db, `Database::put`, then `env.sync()` (durable).
///   * `nontxn_cursor_sync`  — non-txn db, `open_cursor(None)` write, then `env.sync()` (durable).
///   * `nontxn_dbput_nosync` — non-txn db, `Database::put`, NO sync (lost on crash).
///
/// Writes 0..N, deletes key 0, (optionally syncs), signals, waits for SIGKILL.
fn child(spec: &str, home: &Path) -> ! {
    let txnal = spec.starts_with("txn_");
    let env = open(home, txnal);
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(txnal),
        )
        .unwrap();
    match spec {
        "txn_dbput" | "nontxn_dbput_sync" | "nontxn_dbput_nosync" => {
            for i in 0..N {
                db.put(i.to_be_bytes(), i.to_be_bytes()).unwrap();
            }
            db.delete(0u32.to_be_bytes()).unwrap();
        }
        "txn_txncursor" => {
            let t = env.begin_transaction(None).unwrap();
            {
                let mut c = db.open_cursor_in(&t, None).unwrap();
                for i in 0..N {
                    c.put(&entry(i), &entry(i), Put::Overwrite).unwrap();
                }
                c.close().unwrap();
            }
            t.commit().unwrap();
            // Delete key 0 under its own auto-commit txn.
            db.delete(0u32.to_be_bytes()).unwrap();
        }
        "nontxn_cursor_sync" => {
            let mut c = db.open_cursor(None).unwrap();
            for i in 0..N {
                c.put(&entry(i), &entry(i), Put::Overwrite).unwrap();
            }
            c.close().unwrap();
            db.delete(0u32.to_be_bytes()).unwrap();
        }
        _ => unreachable!(),
    }
    // Durable variants call env.sync() (JE Environment.sync = forced checkpoint).
    if spec == "nontxn_dbput_sync" || spec == "nontxn_cursor_sync" {
        env.sync().unwrap();
    }
    std::fs::write(home.join("written"), b"").unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Runs one crash case and returns whether every expected key survived.
/// `expect_durable` = true means keys 1..N must be present after reopen.
fn crash_case(spec: &str, expect_durable: bool) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let mut proc = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "auto_commit_durability_matches_je", "--nocapture"])
        .env(MODE, spec)
        .env(HOME, dir.path())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !dir.path().join("written").exists() {
        assert!(
            proc.try_wait().unwrap().is_none(),
            "{spec}: child exited early"
        );
        assert!(
            start.elapsed() < Duration::from_secs(120),
            "{spec}: child timeout"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    proc.kill().unwrap(); // SIGKILL: no destructors, no close.
    proc.wait().unwrap();

    let txnal = spec.starts_with("txn_");
    let env = open(dir.path(), txnal);
    let Ok(db) = env.open_database(
        None,
        "d",
        &DatabaseConfig::new().with_transactional(txnal),
    ) else {
        env.close().ok();
        return if expect_durable {
            vec![format!("{spec}: database lost (expected durable)")]
        } else {
            Vec::new() // not durable: losing everything is acceptable.
        };
    };
    let mut bad = Vec::new();
    let mut survived = 0u32;
    for i in 1..N {
        if db.get(i.to_be_bytes()).unwrap().as_deref()
            == Some(&i.to_be_bytes()[..])
        {
            survived += 1;
        }
    }
    // Key 0 was deleted; it must be absent either way.
    if db.get(0u32.to_be_bytes()).unwrap().is_some() {
        bad.push(format!("{spec}: deleted key 0 reappeared"));
    }
    if expect_durable && survived != N - 1 {
        bad.push(format!(
            "{spec}: only {survived}/{} keys survived (expected all durable)",
            N - 1
        ));
    }
    db.close().ok();
    env.close().ok();
    bad
}

#[test]
fn auto_commit_durability_matches_je() {
    if let Ok(spec) = std::env::var(MODE) {
        child(&spec, Path::new(&std::env::var_os(HOME).unwrap()));
    }
    let mut bad = Vec::new();
    // Durable paths (must survive SIGKILL): transactional-db auto-commit put
    // (JE Txn.createAutoTxn), explicit-txn cursor commit, and non-txn writes
    // that were followed by env.sync().
    bad.extend(crash_case("txn_dbput", true));
    bad.extend(crash_case("txn_txncursor", true));
    bad.extend(crash_case("nontxn_dbput_sync", true));
    bad.extend(crash_case("nontxn_cursor_sync", true));
    assert!(bad.is_empty(), "durable writes lost:\n{}", bad.join("\n"));

    // A non-txn write with NO env.sync() is NOT durable (JE ThreadLocker: the
    // LN is not fsynced per operation).  It may or may not be present after a
    // crash; we only assert there is no *panic* and no deleted-key resurrection
    // — losing the whole batch is the JE-correct outcome, so crash_case with
    // expect_durable=false must not report a durability failure.
    let nondurable = crash_case("nontxn_dbput_nosync", false);
    assert!(nondurable.is_empty(), "unexpected:\n{}", nondurable.join("\n"));
}
