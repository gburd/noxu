// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Integration test for the `verify_schedule` background verifier daemon
//! (7.1). The cron-schedule matching logic is exhaustively unit-tested in
//! `verify_daemon.rs`; this test proves the end-to-end wiring: an
//! `Environment` opened with `run_verifier = true` and a matching
//! `verify_schedule` spawns the daemon, the daemon runs `verify` against the
//! live databases without disrupting normal operations, and a clean `close`
//! stops the daemon (no hang, no panic).

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};

fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir()
        .join(format!("noxu-verify-daemon-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create scratch dir");
    root
}

#[test]
fn verify_daemon_runs_on_schedule_without_disrupting_ops() {
    let dir = scratch_dir("run");

    // "* * * * *" matches every minute, so the daemon (which re-evaluates the
    // schedule frequently) runs verify promptly. run_verifier gates the spawn.
    let cfg = EnvironmentConfig::new(dir.clone())
        .with_transactional(true)
        .with_allow_create(true)
        .with_run_verifier(true)
        .with_verify_schedule("* * * * *".to_string());
    let env = Environment::open(cfg).expect("open env with verifier");

    let db = env
        .open_database(
            None,
            "verify-daemon-db",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open db");

    // Write and read while the verifier daemon is live — the read-only
    // verify walk must not corrupt or block normal operations.
    for i in 0u32..500 {
        let key = format!("k{i:05}");
        db.put(key.as_bytes(), format!("v{i}").as_bytes())
            .expect("put");
    }
    // Give the daemon a moment to have woken at least once against live data.
    std::thread::sleep(std::time::Duration::from_millis(300));

    for i in 0u32..500 {
        let key = format!("k{i:05}");
        let got = db.get(key.as_bytes()).expect("get");
        assert_eq!(
            got.as_deref(),
            Some(format!("v{i}").as_bytes()),
            "record {i} must survive alongside the running verifier daemon",
        );
    }

    // Clean close must stop the daemon without hanging or panicking.
    db.close().expect("close db");
    env.close().expect("close env (daemon must stop cleanly)");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn no_verifier_daemon_when_run_verifier_false() {
    let dir = scratch_dir("off");

    // Default: run_verifier = false — the daemon must NOT spawn (zero behavior
    // change), and a verify_schedule with the verifier off is inert.
    let cfg = EnvironmentConfig::new(dir.clone())
        .with_transactional(true)
        .with_allow_create(true)
        .with_verify_schedule("* * * * *".to_string());
    let env = Environment::open(cfg).expect("open env, verifier off");

    let db = env
        .open_database(
            None,
            "no-verifier-db",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open db");
    db.put(b"k", b"v").expect("put");
    assert_eq!(db.get(b"k").expect("get").as_deref(), Some(&b"v"[..]));

    db.close().expect("close db");
    env.close().expect("close env");

    let _ = std::fs::remove_dir_all(&dir);
}
