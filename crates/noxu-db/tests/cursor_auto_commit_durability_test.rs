// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! Regression: an auto-commit `Cursor::put` / `Cursor::delete` must honour the
//! environment durability exactly like `Database::put` (the `open_cursor` doc:
//! "each cursor write is its own transaction and is fsynced before
//! returning").  The child process writes through an `open_cursor(None)`
//! cursor, signals, and is SIGKILLed while the env is still open (no close,
//! no checkpoint), so only bytes the write path itself pushed out survive.
//! Before the fix nothing flushed the log buffer on the cursor path and every
//! key was lost.
#![cfg(all(unix, not(noxu_shuttle)))]

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Durability, Environment, EnvironmentConfig,
    Get, Put,
};
use noxu_log::{FileManager, LogEntryType, LogFileReader};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const MODE: &str = "NOXU_CURSOR_SYNC_CHILD";
const HOME: &str = "NOXU_CURSOR_SYNC_HOME";
const N: u32 = 20;

fn durability(name: &str) -> Durability {
    match name {
        "sync" => Durability::COMMIT_SYNC,
        "wns" => Durability::COMMIT_WRITE_NO_SYNC,
        _ => unreachable!(),
    }
}

fn open(home: &Path, txnal: bool, dur: &str) -> Environment {
    Environment::open(
        EnvironmentConfig::new(home.to_path_buf())
            .with_transactional(txnal)
            .with_allow_create(true)
            .with_durability(durability(dur))
            .with_run_checkpointer(false)
            .with_run_cleaner(false),
    )
    .unwrap()
}

fn entry(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}

/// Child: `<txnal>:<dur>:<path>` where path is `cursor` (auto-commit cursor),
/// `txncursor` (cursor in an explicit txn, control) or `dbput` (control).
/// Writes 0..N, deletes key 0, then waits to be SIGKILLed.
fn child(spec: &str, home: &Path) -> ! {
    let parts: Vec<&str> = spec.split(':').collect();
    let txnal = parts[0] == "txn";
    let env = open(home, txnal, parts[1]);
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(txnal),
        )
        .unwrap();
    match parts[2] {
        "cursor" => {
            let mut c = db.open_cursor(None).unwrap();
            for i in 0..N {
                c.put(&entry(i), &entry(i), Put::Overwrite).unwrap();
            }
            let (mut k, mut v) = (entry(0), DatabaseEntry::new());
            c.get(&mut k, &mut v, Get::Search, None).unwrap();
            c.delete().unwrap();
            c.close().unwrap();
        }
        "txncursor" => {
            let t = env.begin_transaction(None).unwrap();
            {
                let mut c = db.open_cursor_in(&t, None).unwrap();
                for i in 0..N {
                    c.put(&entry(i), &entry(i), Put::Overwrite).unwrap();
                }
                let (mut k, mut v) = (entry(0), DatabaseEntry::new());
                c.get(&mut k, &mut v, Get::Search, None).unwrap();
                c.delete().unwrap();
                c.close().unwrap();
            }
            t.commit().unwrap();
        }
        "dbput" => {
            for i in 0..N {
                db.put(i.to_be_bytes(), i.to_be_bytes()).unwrap();
            }
            db.delete(0u32.to_be_bytes()).unwrap();
        }
        _ => unreachable!(),
    }
    std::fs::write(home.join("written"), b"").unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Counts LN and commit frames in the crash image (WAL witness).
fn wal_counts(home: &Path) -> (usize, usize, usize) {
    let fm = Arc::new(FileManager::new(home, true, 1 << 30, 100).unwrap());
    let (mut ins, mut del, mut commit) = (0, 0, 0);
    for f in fm.list_file_numbers().unwrap() {
        let mut r = LogFileReader::open(Arc::clone(&fm), f).unwrap();
        while let Some((_, t, _)) = r.read_next() {
            match t {
                LogEntryType::InsertLN | LogEntryType::InsertLNTxn => ins += 1,
                LogEntryType::DeleteLN | LogEntryType::DeleteLNTxn => del += 1,
                LogEntryType::TxnCommit => commit += 1,
                _ => {}
            }
        }
    }
    (ins, del, commit)
}

/// Runs one crash case and returns the recovered key set's mismatches.
fn crash_case(spec: &str) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let mut proc = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cursor_auto_commit_writes_survive_sigkill",
            "--nocapture",
        ])
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
    proc.kill().unwrap(); // SIGKILL: no destructors, no close, no checkpoint
    proc.wait().unwrap();
    let (ins, del, commit) = wal_counts(dir.path());
    let parts: Vec<&str> = spec.split(':').collect();
    let env = open(dir.path(), parts[0] == "txn", parts[1]);
    eprintln!("{spec}: WAL ins={ins} del={del} commit={commit}");
    let Ok(db) = env.open_database(
        None,
        "d",
        &DatabaseConfig::new().with_transactional(parts[0] == "txn"),
    ) else {
        // Even the database's creation record never reached disk.
        env.close().unwrap();
        return vec![format!("{spec}: database lost (all {N} keys)")];
    };
    let mut bad = Vec::new();
    for i in 0..N {
        let got = db.get(i.to_be_bytes()).unwrap();
        let want = (i != 0).then(|| i.to_be_bytes().to_vec());
        if got.as_deref().map(|b| b.to_vec()) != want {
            bad.push(format!("{spec}: key {i} = {got:?}, want {want:?}"));
        }
    }
    eprintln!("{spec}: bad={}", bad.len());
    db.close().unwrap();
    env.close().unwrap();
    bad
}

#[test]
fn cursor_auto_commit_writes_survive_sigkill() {
    if let Ok(spec) = std::env::var(MODE) {
        child(&spec, Path::new(&std::env::var_os(HOME).unwrap()));
    }
    let mut bad = Vec::new();
    for txnal in ["txn", "notxn"] {
        for dur in ["sync", "wns"] {
            for path in ["cursor", "dbput"] {
                bad.extend(crash_case(&format!("{txnal}:{dur}:{path}")));
            }
        }
    }
    // Control: cursor in an explicit txn, then commit (transactional only).
    bad.extend(crash_case("txn:sync:txncursor"));
    assert!(bad.is_empty(), "lost acknowledged writes:\n{}", bad.join("\n"));
}
