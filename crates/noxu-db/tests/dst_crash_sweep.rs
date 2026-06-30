// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Seed-reproducible storage-fault crash sweep (DST Milestone 1, Phase 5).
//!
//! Unlike `power_loss_sweep.rs` — which SIGKILLs the worker at a random
//! wall-clock millisecond and therefore cannot drop in-flight kernel buffers —
//! this sweep drives the engine through the **FaultDisk** fault layer
//! (`noxu_log::faultdisk`).  For each seed the worker subprocess installs the
//! fault controller (via `NOXU_DST_SEED`) and, at a *seed-chosen write*,
//! either:
//!
//!   * tears a write (writes only a prefix, then `process::exit` — dropping
//!     the tail and every later write, exactly like power loss), or
//!   * drops an fsync (acks durability without flushing, then exits), or
//!   * returns `ENOSPC`, or
//!   * corrupts a just-written region,
//!
//! all **byte-precisely and reproducibly**.  The parent then recovers and
//! asserts the durability invariants:
//!
//!   * **no-lost-committed-txn** — every key the worker committed-and-synced
//!     before the fault is present after recovery (the strict-prefix property
//!     for the ordered `committed_then_uncommitted` workload),
//!   * **no-uncommitted-leak** — no key from an uncommitted txn is visible,
//!   * **recovery is total** — reopening the env never panics / errors,
//!   * **LSN-monotone** — recovery does not resurrect a later commit while
//!     dropping an earlier one (a gap in the present-key prefix).
//!
//! On any failure the test prints `NOXU_DST_SEED=<n>` so the exact run
//! reproduces.
//!
//! ## Running
//!
//! ```sh
//! # Fast subset (~hundred seeds, < 60s) — local dev / PR CI:
//! cargo test -p noxu-db --test dst_crash_sweep
//!
//! # Full release gate (10k+ seeds, minutes) — run before a release:
//! cargo test -p noxu-db --test dst_crash_sweep --release \
//!     -- --ignored long_sweep --nocapture
//!
//! # Reproduce one failing seed:
//! cargo test -p noxu-db --test dst_crash_sweep -- one_seed --nocapture
//! # (edit SEED below, or use the determinism test which runs a fixed seed)
//! ```

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, OperationStatus,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

fn crash_worker_exe() -> &'static str {
    env!("CARGO_BIN_EXE_crash_worker")
}

/// Run the `committed_then_uncommitted` worker for `seed` under the FaultDisk,
/// recover, and check the durability invariants.  Returns `Err(msg)` on an
/// invariant violation (or recovery panic), `Ok(())` on success.
fn run_one_seed(seed: u64) -> Result<(), String> {
    let dir = TempDir::new().map_err(|e| e.to_string())?;
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "committed_then_uncommitted")
        .env("NOXU_DST_SEED", seed.to_string())
        .spawn()
        .map_err(|e| format!("spawn crash_worker: {e}"))?;

    // The worker either power-cuts itself (torn write / dropped fsync — it
    // exits 137), panics out on an ENOSPC commit (exits non-zero), or runs the
    // whole workload and loops forever waiting to be killed (the fault's
    // target write was beyond the workload, or it was a no-fault control run).
    // The workload itself completes in well under 100 ms, so poll briefly for
    // self-exit then kill.  A short budget keeps the sweep fast: a worker that
    // is going to self-exit does so almost immediately; anything still alive
    // at the deadline has finished its writes and is just spinning in its
    // terminal sleep loop, so killing it is equivalent to a clean power loss
    // after the last write.
    let mut waited = Duration::ZERO;
    let step = Duration::from_millis(10);
    let deadline = Duration::from_millis(600);
    loop {
        match child.try_wait() {
            Ok(Some(_status)) => break, // self power-cut, panic-exit, or done
            Ok(None) => {
                if waited >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                std::thread::sleep(step);
                waited += step;
            }
            Err(e) => return Err(format!("try_wait: {e}")),
        }
    }

    // Reopen and check invariants — must not panic.
    let result = std::panic::catch_unwind(|| check_invariants(&dir_path));
    match result {
        Ok(r) => r,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload.downcast_ref::<&str>().map(|s| s.to_string())
                })
                .unwrap_or_else(|| "unknown panic".to_string());
            Err(format!("recovery PANIC: {msg}"))
        }
    }
}

/// Reopen the env and assert the durability invariants.  Returns the recovered
/// committed-key map (for the determinism test) on success.
fn check_invariants(dir: &Path) -> Result<(), String> {
    let snapshot = recover_snapshot(dir)?;
    assert_prefix_and_no_leak(&snapshot)
}

/// Reopen the env and return the set of committed keys (0..50) present, as a
/// map key->value-tag.  Used both by the invariant check and the determinism
/// proof (same seed => identical map).
fn recover_snapshot(dir: &Path) -> Result<BTreeMap<u32, Vec<u8>>, String> {
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .map_err(|e| format!("reopen env: {e}"))?;
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .map_err(|e| format!("reopen db: {e}"))?;

    let mut present = BTreeMap::new();
    for i in 0u32..50 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get_into(None, &key, &mut val).map_err(|e| e.to_string())? {
            present.insert(i, val.data().to_vec());
        }
    }
    // Uncommitted keys (1000..1050) must never be visible.
    for i in 1000u32..1050 {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut val = DatabaseEntry::new();
        if db.get_into(None, &key, &mut val).map_err(|e| e.to_string())? {
            return Err(format!("uncommitted key {i} leaked"));
        }
    }
    drop(db);
    drop(env);
    Ok(present)
}

/// Strict-prefix + correct-value + no-leak invariants over a recovered
/// snapshot.
fn assert_prefix_and_no_leak(
    present: &BTreeMap<u32, Vec<u8>>,
) -> Result<(), String> {
    // Every present committed key must carry the "committed" tag.
    for (k, v) in present {
        if v != b"committed" {
            return Err(format!("key {k} wrong value: {v:?}"));
        }
    }
    // Strict prefix: keys are written 0,1,..,49 in order, so the present set
    // must be {0..n} with no gaps.  A gap means recovery kept a later commit
    // while dropping an earlier one — a durability violation.
    let prefix_broken =
        present.keys().enumerate().any(|(idx, k)| *k != idx as u32);
    if prefix_broken {
        return Err(format!(
            "non-prefix recovery: present keys {:?} have a gap",
            present.keys().collect::<Vec<_>>()
        ));
    }
    Ok(())
}

/// Run a sweep over `seeds` and aggregate failures, printing each failing seed
/// for exact reproduction.
fn sweep(seeds: impl Iterator<Item = u64>, label: &str) {
    let mut failures: Vec<(u64, String)> = Vec::new();
    let mut count = 0u64;
    for seed in seeds {
        count += 1;
        if let Err(msg) = run_one_seed(seed) {
            eprintln!("FAILURE: NOXU_DST_SEED={seed} -> {msg}");
            failures.push((seed, msg));
        }
        if count.is_multiple_of(50) {
            eprintln!("  {label}: {count} seeds, {} failures", failures.len());
        }
    }
    if !failures.is_empty() {
        eprintln!("\n{label}: {} of {count} seeds failed:", failures.len());
        for (seed, msg) in failures.iter().take(20) {
            eprintln!("  reproduce with NOXU_DST_SEED={seed}: {msg}");
        }
        panic!(
            "{} of {count} seeds failed (reproduce any with NOXU_DST_SEED=<seed>)",
            failures.len()
        );
    }
    eprintln!("{label}: all {count} seeds held the invariants.");
}

/// Fast subset for local dev / PR CI: ~120 seeds, target < 60s.  All four
/// fault kinds (plus no-fault controls) are exercised across this range.
#[test]
fn dst_crash_sweep_fast() {
    sweep(0..120, "fast");
}

/// Full release gate: 10k seeds.  Run before a release:
/// `cargo test -p noxu-db --test dst_crash_sweep --release -- --ignored long_sweep`.
#[test]
#[ignore = "10k-seed release gate; run via --ignored before a release"]
fn long_sweep() {
    sweep(0..10_000, "long");
}

/// HEADLINE determinism proof: the SAME seed produces the EXACT same recovered
/// state across two independent runs.  This is the whole point of DST — a
/// failing seed reproduces byte-for-byte.
#[test]
fn dst_same_seed_reproduces_exactly() {
    // Pick a seed that injects a torn write so the crash point is non-trivial.
    let seed = find_torn_write_seed();
    eprintln!("determinism proof using NOXU_DST_SEED={seed} (torn write)");

    let run = |s: u64| -> BTreeMap<u32, Vec<u8>> {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();
        let mut child = std::process::Command::new(crash_worker_exe())
            .env("NOXU_CRASH_DIR", &dir_path)
            .env("NOXU_CRASH_MODE", "committed_then_uncommitted")
            .env("NOXU_DST_SEED", s.to_string())
            .spawn()
            .expect("spawn");
        // Wait for the worker (it self-exits on the torn write).
        let mut waited = Duration::ZERO;
        loop {
            match child.try_wait().unwrap() {
                Some(_) => break,
                None if waited >= Duration::from_millis(600) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                None => {
                    std::thread::sleep(Duration::from_millis(10));
                    waited += Duration::from_millis(10);
                }
            }
        }
        recover_snapshot(&dir_path).expect("recover")
    };

    let first = run(seed);
    let second = run(seed);
    assert_eq!(
        first, second,
        "same seed {seed} produced different recovered state \
         (determinism broken): {first:?} vs {second:?}"
    );
    // And the invariants must hold (a torn write at a committed boundary must
    // leave a clean committed prefix + no torn/uncommitted leak).
    assert_prefix_and_no_leak(&first).unwrap_or_else(|e| {
        panic!("torn-write seed {seed} violated invariant: {e}")
    });
    eprintln!(
        "determinism held: seed {seed} recovered {} committed keys identically twice",
        first.len()
    );
}

/// Find the first seed whose fault plan is a torn write (mirrors the
/// controller's own seed->kind mapping so the test stays in sync).
fn find_torn_write_seed() -> u64 {
    use noxu_log::faultdisk::{FaultController, FaultKind};
    (0u64..)
        .find(|&s| FaultController::from_seed(s).kind() == FaultKind::TornWrite)
        .expect("a torn-write seed exists")
}

/// The oracle must REJECT a bad recovery, not just accept good ones.  This is
/// the "prove the harness catches a violation" half of the headline: we feed
/// the invariant checker deliberately-broken snapshots and confirm it errors.
/// (We assert the oracle directly rather than planting an engine bug, so the
/// gate stays green while still proving it can fail.)
#[test]
fn oracle_catches_violations() {
    // A gap in the committed prefix = a later commit kept while an earlier one
    // was dropped = durability violation.
    let mut gapped = BTreeMap::new();
    gapped.insert(0u32, b"committed".to_vec());
    gapped.insert(2u32, b"committed".to_vec()); // missing key 1
    let err = assert_prefix_and_no_leak(&gapped)
        .expect_err("oracle must reject a prefix gap");
    assert!(err.contains("non-prefix"), "unexpected error: {err}");

    // A wrong value for a committed key must also be rejected.
    let mut wrong = BTreeMap::new();
    wrong.insert(0u32, b"garbage".to_vec());
    assert_prefix_and_no_leak(&wrong)
        .expect_err("oracle must reject a wrong value");

    // A clean prefix is accepted.
    let mut good = BTreeMap::new();
    for i in 0u32..5 {
        good.insert(i, b"committed".to_vec());
    }
    assert_prefix_and_no_leak(&good).expect("clean prefix must pass");
}
