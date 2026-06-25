// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Group-commit coalescing benchmark for the JE-faithful `flushAndSync`
//! restructure.
//!
//! Counts `Environment::stat_fsync_count()` against the number of committed
//! (CommitSync) transactions for an N-thread concurrent workload.  With the
//! coalescing fix, the leader/waiter decision happens BEFORE the buffer drain
//! (matching JE `FSyncManager.flushAndSync`), so one fdatasync serves many
//! concurrent committers and `fsyncs << commits`.
//!
//! ## How to run (REAL disk required)
//!
//! Coalescing only manifests on a disk whose fsync is slow enough for
//! concurrent committers to queue.  Run on a real block device, NOT tmpfs:
//!
//! ```sh
//! NOXU_BENCH_DIR=/scratch \
//!   cargo test -p noxu-db --release --test group_commit_coalesce_bench \
//!   -- --ignored --nocapture
//! ```
//!
//! `/scratch` here is btrfs-on-dm-crypt (~2000 fsync/s).  On tmpfs the
//! fsync is a no-op and the workload runs too fast to coalesce, so the test
//! refuses to draw conclusions from a tmpfs path (it still runs, just prints
//! a note).
//!
//! `#[ignore]` because it depends on an external mount point and is a
//! measurement, not a pass/fail gate.  It DOES assert the headline property
//! (fsyncs materially fewer than commits under concurrency) so a regression
//! that re-inverts the ordering fails it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Durability, Environment, EnvironmentConfig,
};
use std::sync::{Arc, Barrier};
use std::time::Instant;

fn bench_dir() -> std::path::PathBuf {
    std::env::var("NOXU_BENCH_DIR")
        .unwrap_or_else(|_| "/scratch".to_string())
        .into()
}

/// Run `threads` concurrent committers, each doing `keys` CommitSync commits
/// of disjoint keys.  Returns (commits, fsyncs, elapsed_ms).
fn run(threads: usize, keys: usize) -> (u64, u64, u128) {
    let root = bench_dir();
    std::fs::create_dir_all(&root).expect("create bench root");
    let dir = tempfile::TempDir::new_in(&root).expect("tempdir on bench disk");

    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_durability(Durability::COMMIT_SYNC),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "bench",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let env = Arc::new(env);
    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(threads));

    let fsyncs_before = env.stat_fsync_count();
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|tid| {
            let env = Arc::clone(&env);
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for k in 0..keys {
                    let txn = env.begin_transaction(None).unwrap();
                    let key = DatabaseEntry::from_vec(
                        format!("t{tid:02}_k{k:05}").into_bytes(),
                    );
                    let val = DatabaseEntry::from_vec(vec![b'v'; 64]);
                    db.put(Some(&txn), &key, &val).unwrap();
                    // Default durability is COMMIT_SYNC => real fdatasync.
                    txn.commit().unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let elapsed_ms = start.elapsed().as_millis();
    let fsyncs = env.stat_fsync_count() - fsyncs_before;
    let commits = (threads * keys) as u64;
    (commits, fsyncs, elapsed_ms)
}

#[test]
#[ignore = "real-disk benchmark; run with NOXU_BENCH_DIR=/scratch --ignored --nocapture"]
fn group_commit_coalesce_fsyncs_per_commit() {
    let on_tmpfs = is_tmpfs(&bench_dir());
    if on_tmpfs {
        eprintln!(
            "NOTE: NOXU_BENCH_DIR={:?} looks like tmpfs; fsync is a no-op so \
             coalescing cannot be observed.  Point NOXU_BENCH_DIR at a real \
             block device (e.g. /scratch) for a meaningful number.",
            bench_dir()
        );
    }

    for &(threads, keys) in &[(8usize, 500usize), (16usize, 500usize)] {
        let (commits, fsyncs, ms) = run(threads, keys);
        let ratio = fsyncs as f64 / commits as f64;
        eprintln!(
            "threads={threads:2} commits={commits} fsyncs={fsyncs} \
             fsyncs/commit={ratio:.3} elapsed={ms}ms"
        );

        // Headline property: under concurrency the coalesced fsync count must
        // be materially below the commit count.  Pre-fix, fsyncs ~= commits
        // (ratio ~1.0); post-fix, ratio should be well under 1.0.  Only assert
        // on a real disk (tmpfs runs too fast to queue committers).
        if !on_tmpfs {
            assert!(
                ratio < 0.9,
                "expected fsync coalescing (fsyncs/commit < 0.9) with \
                 {threads} concurrent committers, got {ratio:.3} \
                 ({fsyncs} fsyncs for {commits} commits)"
            );
        }
    }
}

/// Best-effort tmpfs detection: a tmpfs mount reports a 0-byte device file.
/// Falls back to false (treat as real disk) if it can't tell.
fn is_tmpfs(path: &std::path::Path) -> bool {
    // /proc/mounts lists "tmpfs <mountpoint> tmpfs ...".  Find the longest
    // mountpoint prefix of `path` and check its fstype.
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.into());
    let mounts = match std::fs::read_to_string("/proc/mounts") {
        Ok(m) => m,
        Err(_) => return false,
    };
    let mut best: Option<(usize, bool)> = None;
    for line in mounts.lines() {
        let mut it = line.split_whitespace();
        let _dev = it.next();
        let mp = match it.next() {
            Some(m) => m,
            None => continue,
        };
        let fstype = it.next().unwrap_or("");
        if canon.starts_with(mp) {
            let len = mp.len();
            if best.map(|(b, _)| len > b).unwrap_or(true) {
                best = Some((len, fstype == "tmpfs"));
            }
        }
    }
    best.map(|(_, is)| is).unwrap_or(false)
}
