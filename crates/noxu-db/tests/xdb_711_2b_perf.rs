//! XDB-711 #2b — perf guard: distinct-name transactional creates and reopens
//! of existing databases must NOT be slowed by the #2b NameLN-lock change.
//!
//! JE takes one record lock per DDL; the #2b fix does the same (WRITE for a
//! fresh create, a shared READ for a reopen of an existing db).  The ~2x
//! slowdown seen on the adversarial `rv_adv_xdb_711` probe is same-name
//! contention (inherent and JE-correct — a transactional create now correctly
//! blocks behind an in-doubt rename/remove of the same name).  A NORMAL
//! workload touches DISTINCT names (no contention) or REOPENS existing dbs
//! (shared READ, no mutual blocking), so it must run at the pre-fix speed.
//!
//! This is a reported MEASUREMENT (printed with --nocapture), not a hard
//! threshold assert (wall-clock thresholds flake under shared CI load); run on
//! base and fix and compare the printed us/op.

#![allow(clippy::unwrap_used)]

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::time::Instant;
use tempfile::TempDir;

fn env(dir: &std::path::Path) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_run_checkpointer(false)
            .with_run_cleaner(false),
    )
    .unwrap()
}

fn cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

const N: usize = 2000;

#[test]
fn perf_distinct_name_transactional_creates() {
    let dir = TempDir::new().unwrap();
    let e = env(dir.path());
    let t0 = Instant::now();
    for i in 0..N {
        let txn = e.begin_transaction(None).unwrap();
        let db =
            e.open_database(Some(&txn), &format!("db-{i}"), &cfg()).unwrap();
        db.close().unwrap();
        txn.commit().unwrap();
    }
    let us = t0.elapsed().as_micros() as f64 / N as f64;
    eprintln!("PERF distinct-name txn-create: {us:.1} us/op ({N} ops)");
    e.close().ok();
}

#[test]
fn perf_reopen_existing_transactional() {
    let dir = TempDir::new().unwrap();
    let e = env(dir.path());
    // Create 100 distinct names once.
    let names: Vec<String> = (0..100).map(|i| format!("r-{i}")).collect();
    for n in &names {
        let db = e.open_database(None, n, &cfg()).unwrap();
        db.close().unwrap();
    }
    // Now time transactional REOPENS (allow_create set but names exist -> the
    // fix takes a shared READ, not a WRITE).
    let t0 = Instant::now();
    for i in 0..N {
        let n = &names[i % names.len()];
        let txn = e.begin_transaction(None).unwrap();
        let db = e.open_database(Some(&txn), n, &cfg()).unwrap();
        db.close().unwrap();
        txn.commit().unwrap();
    }
    let us = t0.elapsed().as_micros() as f64 / N as f64;
    eprintln!("PERF reopen-existing txn: {us:.1} us/op ({N} ops)");
    e.close().ok();
}
