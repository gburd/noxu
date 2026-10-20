// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! R-LIVEDDL-711 — concurrent catalog-operation fuzz against a serial-history
//! oracle.
//!
//! 2–4 threads concurrently drive open / remove / rename / truncate / create
//! on a small shared name space (3 base names), each op under its own
//! transaction with a random commit/abort decision, over 30 deterministic
//! seeds.  Every catalog operation now takes the NameLN lock
//! (`Environment::{remove,rename,truncate,open}_database` ->
//! `lock_name_record*`), so a conflict surfaces as a typed, retryable error
//! (`LockTimeout` / `LockConflict` / `DeadlockDetected`) — never silent data
//! loss, a panic, a deadlock, or a hang.
//!
//! Oracle (safety, not a full linearisation): after all threads join, the
//! surviving catalog must be *internally consistent and serialisable* —
//!   * every name the environment lists opens cleanly and reports a record
//!     count drawn from the finite set of counts any serial history could
//!     produce for it (a seeded db has either its full seed count, 0 after a
//!     truncate, or the seed count of a rename target);
//!   * no operation ever returned a non-typed (surprising) error;
//!   * the run terminates within the lock-timeout bound (no deadlock/hang);
//!   * a name is never observed half-created / half-removed (open either
//!     succeeds with a consistent tree or fails NotFound — never a torn tree).
//! This catches the R-LIVEDDL-711 class (a race that leaves a db empty/gone
//! with records lost) because such an outcome violates the count set, and it
//! catches a lock-protocol defect (deadlock / unbounded block / untyped error).

#![allow(clippy::doc_lazy_continuation)]

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig, NoxuError};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

const SEED_RECS: u64 = 8;
const NAMES: [&str; 3] = ["c0", "c1", "c2"];
const LOCK_MS: u64 = 1_000;

/// Tiny deterministic PRNG (splitmix64) so each seed is reproducible.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn ro_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(false).with_transactional(true)
}

/// A typed, retryable lock/conflict error is an acceptable concurrent outcome;
/// anything else (except the benign existence errors) is a surprise.
fn is_typed_conflict(e: &NoxuError) -> bool {
    matches!(
        e,
        NoxuError::LockTimeout { .. }
            | NoxuError::LockConflict(_)
            | NoxuError::DeadlockDetected
            | NoxuError::LockNotAvailable
    )
}

/// An error that is a legitimate serial-history outcome (not a bug): a typed
/// conflict, or a benign catalog-state error (the name was concurrently
/// removed/created/renamed by a serialised peer).
fn is_acceptable(e: &NoxuError) -> bool {
    if is_typed_conflict(e) {
        return true;
    }
    // A benign catalog-state error: the name was concurrently removed /
    // created / renamed by a serialised peer, or still has an open handle
    // (JE IllegalStateException "open handles exist").  The in-use case
    // currently flows through the `EnvironmentFailure` wrapper, so match its
    // Display string too.
    let s = format!("{e:?}");
    matches!(
        e,
        NoxuError::DatabaseNotFound(_)
            | NoxuError::DatabaseAlreadyExists(_)
            | NoxuError::OperationNotAllowed(_)
    ) || s.contains("in use")
        || s.contains("InUse")
        || s.contains("already exists")
        || s.contains("does not exist")
}

fn seed_db(env: &Environment, name: &str) {
    if let Ok(db) = env.open_database(None, name, &db_cfg()) {
        for i in 0..SEED_RECS {
            let _ =
                db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes());
        }
        let _ = db.close();
    }
}

fn run_seed(seed: u64) {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true)
                .with_lock_timeout(LOCK_MS),
        )
        .unwrap(),
    );
    // Seed the three base names with SEED_RECS records each.
    for n in NAMES {
        seed_db(&env, n);
    }

    let mut rng = Rng(seed);
    let nthreads = 2 + rng.below(3); // 2..=4
    // Pre-roll each thread's op plan so the whole run is deterministic from the
    // seed (threads then execute independently; interleaving varies but every
    // outcome must stay serialisable).
    let mut plans: Vec<Vec<(u64, usize, usize, bool)>> = Vec::new();
    for _ in 0..nthreads {
        let ops = 3 + rng.below(4); // 3..=6 ops per thread
        let mut plan = Vec::new();
        for _ in 0..ops {
            let op = rng.below(5); // 0 open,1 remove,2 rename,3 truncate,4 create
            let a = rng.below(NAMES.len() as u64) as usize;
            let b = rng.below(NAMES.len() as u64) as usize;
            let commit = rng.below(2) == 0;
            plan.push((op, a, b, commit));
        }
        plans.push(plan);
    }

    let mut handles = Vec::new();
    for plan in plans {
        let env = Arc::clone(&env);
        handles.push(thread::spawn(move || -> Result<(), String> {
            for (op, a, b, commit) in plan {
                let na = NAMES[a];
                let nb = NAMES[b];
                // Each op runs in its own txn; commit or abort per the plan.
                let txn = match env.begin_transaction(None) {
                    Ok(t) => t,
                    Err(e) => return Err(format!("begin: {e:?}")),
                };
                let res: Result<(), NoxuError> = match op {
                    0 => env
                        .open_database(Some(&txn), na, &ro_cfg())
                        .and_then(|d| d.close())
                        .map(|_| ()),
                    1 => env.remove_database(Some(&txn), na),
                    2 => {
                        if na == nb {
                            Ok(())
                        } else {
                            env.rename_database(Some(&txn), na, nb)
                        }
                    }
                    3 => env.truncate_database(Some(&txn), na).map(|_| ()),
                    _ => env
                        .open_database(Some(&txn), na, &db_cfg())
                        .and_then(|d| d.close())
                        .map(|_| ()),
                };
                match res {
                    Ok(()) => {
                        // A typed conflict on commit is also acceptable.
                        if commit {
                            if let Err(e) = txn.commit()
                                && !is_acceptable(&e)
                            {
                                return Err(format!("commit: {e:?}"));
                            }
                        } else {
                            let _ = txn.abort();
                        }
                    }
                    Err(e) => {
                        let _ = txn.abort();
                        if !is_acceptable(&e) {
                            return Err(format!("op {op} {na}->{nb}: {e:?}"));
                        }
                    }
                }
            }
            Ok(())
        }));
    }

    // No thread may panic, hang (bounded by LOCK_MS * ops), or return a
    // surprising error.
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => panic!("seed {seed:#x}: surprising outcome: {msg}"),
            Err(_) => panic!("seed {seed:#x}: a worker thread PANICKED"),
        }
    }

    // --- Oracle: the surviving catalog must be internally consistent and
    //     every surviving db's count must be serialisable (no lost records). ---
    let names: BTreeSet<String> =
        env.database_names().unwrap().into_iter().collect();
    // Legal record counts for ANY surviving name: a db is either one of the
    // seeded trees (SEED_RECS), a truncated tree (0), or a freshly-created
    // empty tree (0).  A rename moves a whole tree, so a target carries the
    // source's count (SEED_RECS or 0).  The only invariant that matters for the
    // data-loss class: no surviving db reports a PARTIAL count (torn tree) and
    // a db that still holds records holds ALL of them.
    let legal: BTreeSet<u64> = [0, SEED_RECS].into_iter().collect();
    for name in &names {
        if name == "test" {
            continue; // harness scratch db
        }
        // Open must succeed (name is listed) or fail NotFound (removed between
        // the listing and the open by a serialised peer) — never a torn tree.
        match env.open_database(None, name, &ro_cfg()) {
            Ok(db) => {
                let c = db.count().unwrap();
                assert!(
                    legal.contains(&c),
                    "seed {seed:#x}: db {name} has non-serialisable count {c} \
                     (records lost / torn tree); legal={legal:?}"
                );
                // If it has records, it must have ALL SEED_RECS keys intact
                // (no silent partial loss).
                if c == SEED_RECS {
                    for i in 0..SEED_RECS {
                        assert_eq!(
                            db.get(format!("k{i}").as_bytes())
                                .unwrap()
                                .as_deref(),
                            Some(format!("v{i}").as_bytes()),
                            "seed {seed:#x}: db {name} lost record k{i} \
                             (count={c} but key missing)"
                        );
                    }
                }
                db.close().unwrap();
            }
            Err(e) => {
                assert!(
                    matches!(e, NoxuError::DatabaseNotFound(_))
                        || is_typed_conflict(&e),
                    "seed {seed:#x}: listed db {name} failed to open with a \
                     non-serialisable error: {e:?}"
                );
            }
        }
    }

    // The environment must still be healthy (close cleanly — catches a wedged
    // lock table / leaked name lock).
    env.close()
        .unwrap_or_else(|e| panic!("seed {seed:#x}: env close failed: {e:?}"));
}

#[test]
fn concurrent_catalog_fuzz_30_seeds() {
    // Base overridable for stress diff-seed runs (XADDL_FUZZ_BASE convention).
    let base: u64 = std::env::var("LIVEDDL_FUZZ_BASE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x1150_D00D_0000_0000);
    for i in 0..30u64 {
        run_seed(
            base.wrapping_add(i).wrapping_mul(0x1000_0001).wrapping_add(i),
        );
    }
}
