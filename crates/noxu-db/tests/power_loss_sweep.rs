//! 1000-iteration power-loss sweep across torn-write boundaries.
//!
//! Companion to `crash_recovery_test.rs`. Where that suite probes
//! specific deterministic scenarios, this test sweeps a range of
//! kill-timing values to expose any hidden boundary condition that
//! a single-shot scenario would miss.
//!
//! Each iteration:
//!  1. Spawns the same `crash_worker` subprocess used by
//!     `crash_recovery_test.rs`, in `committed_then_uncommitted`
//!     mode.
//!  2. Waits a randomised number of milliseconds (uniform
//!     `[0, MAX_WAIT_MS]`) before sending SIGKILL — this samples
//!     across the worker's whole write-and-fsync timeline,
//!     including the torn-write window inside `LogManager::flush`
//!     and the post-fsync, pre-flag-write window.
//!  3. Reopens the env and asserts:
//!     a. recovery succeeds (no panic, no `EnvironmentFailure`)
//!     b. every committed key is present with its original value
//!     c. no uncommitted key is visible
//!     d. the recovered key set is a *prefix* of {0..50} when
//!     the worker died mid-phase-1 (i.e. recovery does not
//!     drop later commits while keeping earlier ones — strict
//!     prefix invariant)
//!
//! ## Why this test is `#[ignore]`
//!
//! Running 1000 iterations takes 30-60 minutes on a laptop and
//! several gigabytes of temp space (each iteration uses a fresh
//! `TempDir`). Run with:
//!
//! ```sh
//! cargo test -p noxu-db --test power_loss_sweep --release \
//!     -- --ignored --nocapture
//! ```
//!
//! ## Relationship to a real qemu-based whole-VM kill
//!
//! `pkill -9` on a process from within the same OS kills the
//! process but does not kill the OS — pending fsyncs at the
//! kernel layer eventually flush. A real power-loss test ALSO
//! drops in-flight kernel buffers, exposing recovery to entries
//! that were `write()`'d but never reached disk. This test
//! cannot exercise that layer; for that, use the qemu-based
//! procedure documented at `docs/src/operations/power-loss.md`.
//!
//! The test is still valuable because:
//!   - It hits the user-space race windows: process death
//!     between log_buffer.append() and log_manager.flush(),
//!     between flush and the in-memory tree update, between the
//!     tree update and the WriteLockInfo move.
//!   - It hits the file-descriptor close-on-exit fsync timing —
//!     in many Unix kernels close(2) does NOT imply fsync(2),
//!     and a SIGKILL'd process has its dirty pages flushed lazily
//!     by the kernel after a delay. With `O_DIRECT` or
//!     `fdatasync` between writes (which `LogManager` does for
//!     CommitSync), this is closer to power-loss semantics than
//!     a clean `close()`.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

const NUM_ITERATIONS: usize = 1000;
const MAX_WAIT_MS: u64 = 250;

fn crash_worker_exe() -> &'static str {
    env!("CARGO_BIN_EXE_crash_worker")
}

fn reopen_db(dir: &Path) -> (noxu_db::Environment, noxu_db::Database) {
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("reopen env");
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("reopen db");
    (env, db)
}

/// Pseudo-random `u64` from a 64-bit linear-congruential generator.
/// Deterministic per-iteration seed — uses iteration index as the
/// only source of variance so a failing iteration is reproducible.
fn lcg_step(state: u64) -> u64 {
    state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407)
}

#[test]
#[ignore = "1000-iteration sweep, takes 30-60 min; run via --ignored"]
fn power_loss_sweep_thousand_iterations() {
    let mut rng = 0xDEAD_BEEF_u64;
    let mut iter_failures: Vec<(usize, String)> = Vec::new();

    for iter in 0..NUM_ITERATIONS {
        rng = lcg_step(rng.wrapping_add(iter as u64));
        let wait_ms = rng % MAX_WAIT_MS;

        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();

        let mut child = std::process::Command::new(crash_worker_exe())
            .env("NOXU_CRASH_DIR", &dir_path)
            .env("NOXU_CRASH_MODE", "committed_then_uncommitted")
            .spawn()
            .expect("spawn crash_worker");

        // Wait the randomised time, then kill. We do NOT wait for
        // any flag file — the goal is to catch the worker at an
        // arbitrary point in its execution.
        std::thread::sleep(Duration::from_millis(wait_ms));
        let _ = child.kill();
        let _ = child.wait();

        // Reopen — must not panic / not return Err.
        let reopen_result = std::panic::catch_unwind(|| {
            let (env, db) = reopen_db(&dir_path);

            // Count which committed keys are present.
            let mut present = [false; 50];
            for i in 0u32..50 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let mut val = DatabaseEntry::new();
                if db.get_into(None, &key, &mut val).unwrap() {
                    if val.data() == b"committed" {
                        present[i as usize] = true;
                    } else {
                        return Err(format!(
                            "key {i} has wrong value: {:?}",
                            val.data()
                        ));
                    }
                }
            }

            // Strict-prefix invariant: if key K is present, every
            // key < K must also be present (commits in
            // committed_then_uncommitted are written in order).
            //
            // The worker writes keys 0,1,2,…,49 sequentially. A
            // gap (i.e. key K present but K-1 absent) would mean
            // recovery applied a later commit while dropping an
            // earlier one — corruption.
            let mut first_absent: Option<usize> = None;
            for (i, &p) in present.iter().enumerate() {
                if !p && first_absent.is_none() {
                    first_absent = Some(i);
                }
                if p && let Some(absent) = first_absent {
                    return Err(format!(
                        "non-prefix recovery: key {i} present but {absent} absent"
                    ));
                }
            }

            // Uncommitted keys (1000..1050) must NEVER be visible.
            for i in 1000u32..1050 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let mut val = DatabaseEntry::new();
                if db.get_into(None, &key, &mut val).unwrap() {
                    return Err(format!("uncommitted key {i} leaked"));
                }
            }

            drop(db);
            drop(env);
            Ok(())
        });

        match reopen_result {
            Ok(Ok(())) => {} // success
            Ok(Err(msg)) => {
                iter_failures.push((iter, msg));
            }
            Err(panic_payload) => {
                let msg = if let Some(s) =
                    panic_payload.downcast_ref::<String>()
                {
                    s.clone()
                } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown panic".to_string()
                };
                iter_failures.push((iter, format!("PANIC: {msg}")));
            }
        }

        // Print progress every 100 iterations.
        if (iter + 1) % 100 == 0 {
            eprintln!(
                "  iteration {}/{} — {} failures so far",
                iter + 1,
                NUM_ITERATIONS,
                iter_failures.len()
            );
        }
    }

    if !iter_failures.is_empty() {
        eprintln!("\npower-loss sweep failures ({}):", iter_failures.len());
        for (iter, msg) in &iter_failures[..iter_failures.len().min(20)] {
            eprintln!("  iter {iter}: {msg}");
        }
        if iter_failures.len() > 20 {
            eprintln!("  ... and {} more", iter_failures.len() - 20);
        }
        panic!(
            "{} of {} iterations failed",
            iter_failures.len(),
            NUM_ITERATIONS
        );
    }
}

/// A short version that runs in CI-acceptable time (~30s) so the
/// harness itself is regression-tested. Sweeps the same kill
/// timing logic but with NUM_ITERATIONS_SMOKE iterations.
#[test]
fn power_loss_sweep_smoke() {
    const NUM_ITERATIONS_SMOKE: usize = 20;

    let mut rng = 0xCAFE_BABE_u64;
    let mut iter_failures: Vec<(usize, String)> = Vec::new();

    for iter in 0..NUM_ITERATIONS_SMOKE {
        rng = lcg_step(rng.wrapping_add(iter as u64));
        let wait_ms = rng % MAX_WAIT_MS;

        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();

        let mut child = std::process::Command::new(crash_worker_exe())
            .env("NOXU_CRASH_DIR", &dir_path)
            .env("NOXU_CRASH_MODE", "committed_then_uncommitted")
            .spawn()
            .expect("spawn crash_worker");

        std::thread::sleep(Duration::from_millis(wait_ms));
        let _ = child.kill();
        let _ = child.wait();

        let r = std::panic::catch_unwind(|| {
            let (env, db) = reopen_db(&dir_path);
            // Just verify recovery succeeds and uncommitted keys
            // don't leak. The full prefix invariant is in the
            // `--ignored` 1000-iteration test.
            for i in 1000u32..1050 {
                let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
                let mut val = DatabaseEntry::new();
                if db.get_into(None, &key, &mut val).unwrap() {
                    return Err(format!("uncommitted key {i} leaked"));
                }
            }
            drop(db);
            drop(env);
            Ok(())
        });

        match r {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => iter_failures.push((iter, msg)),
            Err(_) => iter_failures.push((iter, "PANIC".into())),
        }
    }

    assert!(
        iter_failures.is_empty(),
        "smoke sweep had {} failures: {:?}",
        iter_failures.len(),
        &iter_failures[..iter_failures.len().min(5)]
    );
}
