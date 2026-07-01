// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation test for the SHARED_CACHE shared-evictor
//! register / deregister / eviction-scan interleavings.
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//!
//! # What this models
//!
//! The production `SharedEvictorHandle` (crates/noxu-evictor/src/shared.rs)
//! shares ONE `Mutex<HashMap<key, Arc<Tree>>>` registry across all sharing
//! environments.  Three concurrent operations touch it:
//!   - **register**: an env inserts its tree `Arc` under a unique key.
//!   - **deregister** (env close): removes the env's keys, then the env's tree
//!     `Arc`s drop.
//!   - **scan** (the shared daemon's `do_evict`): snapshots the tree `Arc`s
//!     under the registry lock, then operates on the snapshot.
//!
//! This test replaces the real `Tree`/`Evictor` with a minimal `TreeStub`
//! that flips an `alive` flag false on drop, and models the three operations
//! against a shuttle-instrumented `Mutex<HashMap>`, requiring under EVERY
//! interleaving:
//!
//!   1. **No use-after-close.** A scanner that pulled a tree out of the
//!      registry under the lock holds a strong `Arc`, so the tree it operates
//!      on is ALWAYS alive — even if the owning env deregisters and drops its
//!      own `Arc` concurrently.  (The scanner asserts `alive` on every tree it
//!      touches.)
//!   2. **No lost / stale registration after close.** Once `deregister`
//!      returns, a scan that starts AFTER it never sees the closed env's tree
//!      (the key is gone from the map).
//!   3. **Deregister-before-drop ordering.** The env removes its key from the
//!      shared map BEFORE it drops its owning `Arc`, so the shared map never
//!      holds the only reference to a tree the env is about to free — the map
//!      is never the last owner of a dropped tree.
//!
//! These are exactly the close-safety properties the feature must hold: a
//! closing env must fully deregister before its trees drop, and the scan must
//! not race the deregister into touching a freed node.
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-evictor --test shuttle_shared_cache
//! ```
#![cfg(noxu_shuttle)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use shuttle::sync::{Arc, Mutex};

const ITERATIONS: usize = 5_000;

/// Minimal stand-in for a `Tree` behind an `Arc`.  `alive` is set false when
/// the LAST `Arc` is dropped, so a scanner touching a freed tree is detected.
struct TreeStub {
    alive: AtomicBool,
}

impl TreeStub {
    fn new() -> Arc<Self> {
        Arc::new(TreeStub { alive: AtomicBool::new(true) })
    }
}

impl Drop for TreeStub {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}

type Registry = Arc<Mutex<HashMap<i64, Arc<TreeStub>>>>;

/// One env registers a tree, then deregisters it (removing the key BEFORE
/// dropping its owning Arc), while a scanner concurrently snapshots + uses the
/// registry.  Under every interleaving the scanner must only ever touch ALIVE
/// trees (no use-after-close).
#[test]
fn register_deregister_vs_scan_no_use_after_close() {
    shuttle::check_random(
        || {
            let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
            // Env A registers first (so a scan can always find at least A).
            let tree_a = TreeStub::new();
            registry.lock().unwrap().insert(1, Arc::clone(&tree_a));

            // Env B: register key 2, then deregister (remove key, drop Arc).
            let env_b = {
                let registry = Arc::clone(&registry);
                shuttle::thread::spawn(move || {
                    let tree_b = TreeStub::new();
                    registry.lock().unwrap().insert(2, Arc::clone(&tree_b));
                    // --- close: deregister BEFORE dropping the owning Arc ---
                    registry.lock().unwrap().remove(&2);
                    // env B's `tree_b` Arc drops here at end of scope; because
                    // the key was already removed, the map is not the last
                    // owner of a tree it still advertises.
                    drop(tree_b);
                })
            };

            // Scanner: snapshot the tree Arcs UNDER THE LOCK (mirrors
            // `Evictor::candidate_trees`), then operate on the snapshot with
            // the lock released.  Every tree in the snapshot must be alive.
            let scanner = {
                let registry = Arc::clone(&registry);
                shuttle::thread::spawn(move || {
                    let snapshot: Vec<Arc<TreeStub>> = {
                        let map = registry.lock().unwrap();
                        map.values().map(Arc::clone).collect()
                    };
                    // Operate on the snapshot: the strong Arcs keep every tree
                    // alive for the whole scan, so none can be freed under us.
                    for t in &snapshot {
                        assert!(
                            t.alive.load(Ordering::SeqCst),
                            "scanner touched a freed tree (use-after-close)"
                        );
                    }
                })
            };

            env_b.join().unwrap();
            scanner.join().unwrap();

            // After env B fully deregistered, the map must not advertise key 2.
            assert!(
                !registry.lock().unwrap().contains_key(&2),
                "closed env B's tree still registered (lost deregister)"
            );
            // Env A is untouched: the survivor keeps working.
            assert!(
                tree_a.alive.load(Ordering::SeqCst),
                "survivor env A's tree must stay alive"
            );
            assert!(
                registry.lock().unwrap().contains_key(&1),
                "survivor env A's tree must stay registered"
            );
        },
        ITERATIONS,
    );
}

/// Two envs register concurrently while a scanner runs; then BOTH deregister.
/// No registration may be lost (both keys visible until their own deregister)
/// and no scan may touch a freed tree.
#[test]
fn concurrent_register_and_deregister_no_lost_registration() {
    shuttle::check_random(
        || {
            let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

            let make_env = |key: i64, reg: Registry| {
                shuttle::thread::spawn(move || {
                    let tree = TreeStub::new();
                    reg.lock().unwrap().insert(key, Arc::clone(&tree));
                    // deregister BEFORE dropping the owning Arc
                    reg.lock().unwrap().remove(&key);
                    drop(tree);
                })
            };

            let e1 = make_env(1, Arc::clone(&registry));
            let e2 = make_env(2, Arc::clone(&registry));
            let scanner = {
                let registry = Arc::clone(&registry);
                shuttle::thread::spawn(move || {
                    let snapshot: Vec<Arc<TreeStub>> = {
                        let map = registry.lock().unwrap();
                        map.values().map(Arc::clone).collect()
                    };
                    for t in &snapshot {
                        assert!(
                            t.alive.load(Ordering::SeqCst),
                            "scanner touched a freed tree (use-after-close)"
                        );
                    }
                })
            };

            e1.join().unwrap();
            e2.join().unwrap();
            scanner.join().unwrap();

            // Both envs closed -> registry empty (no leaked/dangling trees).
            assert!(
                registry.lock().unwrap().is_empty(),
                "all envs deregistered -> registry must be empty"
            );
        },
        ITERATIONS,
    );
}
