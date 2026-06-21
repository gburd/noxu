//! Stress regression for the noxu-db concurrent-commit lost-write race
//! that was originally tracked under
//! `xa_protocol_test::test_concurrent_independent_xids`.
//!
//! The first-key TOCTOU in `noxu-tree::Tree::insert` was fixed in commit
//! a3d40cc, and the deeper descender-vs-splitter family in commit
//! 155fa01. This test exercises a much heavier workload to surface any
//! analogous lost-write races deeper in the engine — multi-thread
//! concurrent commits that each insert disjoint key ranges through
//! independent transactions, then verify every committed key is
//! readable through a non-transactional `db.get(None, ...)`.
//!
//! The test deliberately:
//!   * uses far more threads than the cores on a typical dev box, to
//!     maximize racing between Tree::insert paths;
//!   * commits many keys per thread (so the first-key path is exited
//!     quickly and the descent path is exercised heavily);
//!   * repeats the whole thing many times against fresh `Database`s so
//!     each iteration replays the empty-tree → fully-populated cycle.
//!
//! The test is `#[ignore]`d because it takes 70-130 s on a development
//! laptop — well past the default `cargo nextest` timeout (60 s slow,
//! 120 s terminate). Run it with:
//!
//! ```text
//! cargo nextest run -p noxu-db --test concurrent_commits_stress \
//!     --profile slow --run-ignored all
//! ```
//!
//! or directly:
//!
//! ```text
//! cargo test -p noxu-db --test concurrent_commits_stress -- --ignored
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

const THREADS: usize = 32;
const KEYS_PER_THREAD: usize = 100;
const ITERATIONS: usize = 8;

fn open_env() -> (TempDir, Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "stress",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (dir, env, db)
}

#[test]
#[ignore = "stress test — takes ~70-130s; run with --ignored or `--profile slow --run-ignored all`"]
fn concurrent_commits_no_lost_writes() {
    for iter in 0..ITERATIONS {
        let (_dir, env, db) = open_env();
        let env = Arc::new(env);
        let db = Arc::new(db);
        let barrier = Arc::new(Barrier::new(THREADS));

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let env = Arc::clone(&env);
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for k in 0..KEYS_PER_THREAD {
                        let txn = env.begin_transaction(None).unwrap();
                        let key = DatabaseEntry::from_vec(
                            format!("t{tid:02}_k{k:04}").into_bytes(),
                        );
                        let val = DatabaseEntry::from_vec(
                            format!("v{tid:02}_{k:04}").into_bytes(),
                        );
                        db.put(Some(&txn), &key, &val).unwrap();
                        txn.commit().unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify every (tid, k) pair is visible via a non-transactional read.
        let mut missing: Vec<(usize, usize)> = Vec::new();
        for tid in 0..THREADS {
            for k in 0..KEYS_PER_THREAD {
                let key = DatabaseEntry::from_vec(
                    format!("t{tid:02}_k{k:04}").into_bytes(),
                );
                let mut out = DatabaseEntry::new();
                let status = db.get(None, &key, &mut out).unwrap();
                if status != OperationStatus::Success {
                    missing.push((tid, k));
                }
            }
        }
        assert!(
            missing.is_empty(),
            "iter={iter}: lost {} of {} writes — first 8: {:?}",
            missing.len(),
            THREADS * KEYS_PER_THREAD,
            &missing[..missing.len().min(8)]
        );
    }
}

/// Faster smoke variant of the same workload — runs in default CI to
/// catch regressions of the descender-vs-splitter family early. Sized
/// to complete well under the default 60 s nextest slow threshold on
/// development hardware (8 threads × 25 keys × 2 iters ≈ 5-15 s).
#[test]
fn concurrent_commits_no_lost_writes_smoke() {
    const SMOKE_THREADS: usize = 8;
    const SMOKE_KEYS_PER_THREAD: usize = 25;
    const SMOKE_ITERATIONS: usize = 2;
    for iter in 0..SMOKE_ITERATIONS {
        let (_dir, env, db) = open_env();
        let env = Arc::new(env);
        let db = Arc::new(db);
        let barrier = Arc::new(Barrier::new(SMOKE_THREADS));
        let handles: Vec<_> = (0..SMOKE_THREADS)
            .map(|tid| {
                let env = Arc::clone(&env);
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for k in 0..SMOKE_KEYS_PER_THREAD {
                        let txn = env.begin_transaction(None).unwrap();
                        let key = DatabaseEntry::from_vec(
                            format!("t{tid:02}_k{k:04}").into_bytes(),
                        );
                        let val = DatabaseEntry::from_vec(
                            format!("v{tid:02}_{k:04}").into_bytes(),
                        );
                        db.put(Some(&txn), &key, &val).unwrap();
                        txn.commit().unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let mut missing: Vec<(usize, usize)> = Vec::new();
        for tid in 0..SMOKE_THREADS {
            for k in 0..SMOKE_KEYS_PER_THREAD {
                let key = DatabaseEntry::from_vec(
                    format!("t{tid:02}_k{k:04}").into_bytes(),
                );
                let mut out = DatabaseEntry::new();
                let status = db.get(None, &key, &mut out).unwrap();
                if status != OperationStatus::Success {
                    missing.push((tid, k));
                }
            }
        }
        assert!(
            missing.is_empty(),
            "iter={iter}: lost {} of {} writes — first 8: {:?}",
            missing.len(),
            SMOKE_THREADS * SMOKE_KEYS_PER_THREAD,
            &missing[..missing.len().min(8)]
        );
    }
}
