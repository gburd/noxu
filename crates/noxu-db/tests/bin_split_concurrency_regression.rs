//! Regression test for the BIN/IN split-path check-then-act race in
//! `noxu-tree`'s `split_child` (found by the 96-thread `noxu-saturation`
//! benchmark; full diagnosis in
//! `.agent/archived-audits/bench/bug-bin-split-concurrency.md`).
//!
//! `insert_recursive_inner` tested `child.get_n_entries() >= max_entries`
//! under a PARENT READ lock, dropped that lock (required — the split needs
//! `parent.write()`), then called `split_child`. Read locks do not exclude,
//! so two descenders could both pass the fullness check on the same child and
//! both call `split_child`. They serialise on `parent.write()`: the first
//! splits the child (leaving only its left half), and the second then built a
//! `SplitEntries` from a no-longer-full child and panicked in
//! `SplitEntries::get_key(split_index)` on an empty entries vec
//! (`tree.rs SplitEntries::get_key`, `index out of bounds: len is 0`).
//!
//! The fix re-validates `child_guard.get_n_entries() >= max_entries` after
//! acquiring the child write lock in `split_child` and returns a benign no-op
//! (`Ok(())`) when the child is no longer full — the caller re-descends and
//! re-checks. This is JE-faithful: `IN.split` re-checks `needsSplitting()`
//! after latching the node it will split.
//!
//! Without the fix this test can panic at high thread counts, but the race is
//! timing-sensitive end-to-end, so the AUTHORITATIVE, deterministic pre-fix
//! reproduction lives in `noxu-tree`'s in-module test
//! `tree::tests::split_child_is_noop_when_child_no_longer_full` (it drives the
//! exact `SplitEntries::get_key` panic directly and reproduces it every run).
//! This end-to-end stress test is a complementary sanity check: with the fix it
//! completes cleanly, every inserted key is readable, and `env.verify()`
//! reports no structural errors.
//!
//! Heavy (many threads, sustained inserts forcing splits) — marked
//! `#[ignore]` so it does not run in the default `nextest` gate, but it is
//! runnable via `cargo nextest run -p noxu-db --run-ignored all
//! bin_split_concurrency` (or `cargo test -- --ignored`).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, VerifyConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// Number of concurrent writer threads. The bug reproduces at high write
/// concurrency; 24 threads reproduces it reliably on CI-class hardware while
/// staying well under the 96 the saturation bench used.
const N_WRITERS: usize = 32;
/// Keys per thread. Enough to drive many BIN splits per thread on the default
/// BIN capacity so the check-then-act window is hit repeatedly.
const KEYS_PER_WRITER: usize = 6_000;
/// How many full insert/delete rounds the writers run. Multiple rounds keep
/// the tree churning (split ↔ merge) so the split path repeatedly races the
/// compressor that clears merged nodes.
const ROUNDS: usize = 4;
/// Number of concurrent compressor threads hammering `env.compress()`. More
/// than one widens the window in which a node has been cleared by a merge but
/// not yet pruned, which is precisely what a racing split must survive.
const N_COMPRESSORS: usize = 3;

/// Build the key for `(thread, i)`. Every thread interleaves two access
/// patterns so BIN splits happen densely and concurrently across threads:
///  - a per-thread SEQUENTIAL band (adjacent keys → repeated splits of the
///    same right-edge BIN, the AllRight hint path), and
///  - a SHARED band that all threads hammer (keys collide on the same BINs
///    across threads → two threads racing the same `split_child`, the exact
///    interleaving that triggered the panic).
fn make_key(thread: usize, i: usize) -> Vec<u8> {
    if i.is_multiple_of(2) {
        // Shared band: all threads target the same key space so their splits
        // (and, when keys are deleted, the compressor's merges) collide on the
        // same parent/child nodes — the exact interleaving that triggered the
        // panic.
        format!("shared:{:08}", i).into_bytes()
    } else {
        // Per-thread sequential band.
        format!("t{:02}:{:08}", thread, i).into_bytes()
    }
}

#[test]
#[ignore = "heavy concurrency stress; run explicitly to reproduce the split race"]
fn concurrent_bin_splits_no_panic_and_verify_clean() {
    let dir = tempfile::TempDir::new().unwrap();
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    let db = Arc::new(
        env.open_database(
            None,
            "split_stress",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );

    let barrier = Arc::new(Barrier::new(N_WRITERS + 1));
    let stop = Arc::new(AtomicBool::new(false));

    // Compressor threads: hammer env.compress() while the writers churn. The
    // INCompressor merges under-full siblings and CLEARS the merged-away left
    // node's entries (tree.rs compress_node: `lb.entries.clear()`), which is
    // the concrete source of an EMPTY child node in an otherwise
    // insert-heavy tree. A concurrent split that latched such a node before
    // it was cleared — or picked it by a now-stale index — is what drove
    // `SplitEntries::get_key(0)` onto a len-0 vec.
    let compressors: Vec<_> = (0..N_COMPRESSORS)
        .map(|_| {
            let env = Arc::clone(&env);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let _ = env.compress();
                    std::thread::yield_now();
                }
            })
        })
        .collect();

    let writers: Vec<_> = (0..N_WRITERS)
        .map(|t| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                // All writers start together to maximise the odds of two
                // threads racing the same split.
                barrier.wait();
                for round in 0..ROUNDS {
                    // Insert this thread's whole key range.
                    for i in 0..KEYS_PER_WRITER {
                        let key = make_key(t, i);
                        // Auto-commit put — the shortest write path, hammering
                        // the tree hot path with no txn bookkeeping between
                        // inserts.
                        let val =
                            format!("v{}:{}:{}", t, round, i).into_bytes();
                        db.put(&key, &val).unwrap();
                    }
                    // On every round but the LAST, delete a fraction of the
                    // per-thread band so the compressor has under-full BINs to
                    // merge (and clear) — producing empty nodes that race the
                    // splits driven by the re-inserts on the next round. The
                    // last round leaves every key present for the readback.
                    if round + 1 < ROUNDS {
                        for i in (1..KEYS_PER_WRITER).step_by(2) {
                            // Only delete the per-thread band (odd i) so the
                            // deletes don't fight across threads.
                            let key = make_key(t, i);
                            let _ = db.delete(&key);
                        }
                    }
                }
            })
        })
        .collect();

    // Release the writers and bound the whole run.
    barrier.wait();
    let deadline = Instant::now() + Duration::from_secs(120);

    for w in writers {
        // A panic in the split path (the bug) propagates out of the writer
        // thread here as a join error → test failure.
        w.join().expect("writer thread panicked (split-path race)");
        assert!(
            Instant::now() < deadline,
            "writers exceeded the 120s deadline"
        );
    }
    stop.store(true, Ordering::Release);
    for c in compressors {
        c.join().expect("compressor thread panicked");
    }

    // Every per-thread key from the final round must be readable. With the
    // pre-fix race, keys could be silently lost when a split left them in the
    // wrong half (data-corruption class), so read them all back.
    let mut missing = 0usize;
    for t in 0..N_WRITERS {
        for i in 0..KEYS_PER_WRITER {
            let key = DatabaseEntry::from_vec(make_key(t, i));
            let mut out = DatabaseEntry::new();
            if !db.get_into(None, &key, &mut out).unwrap() {
                missing += 1;
            }
        }
    }
    assert_eq!(
        missing, 0,
        "{} inserted keys were not readable after the concurrent load",
        missing
    );

    // Structural integrity: the whole B-tree must verify clean.
    let result = env
        .verify(
            &VerifyConfig::new()
                .with_btree_verification(true)
                .with_max_errors(64),
        )
        .unwrap();
    assert!(
        result.passed && result.errors.is_empty(),
        "env.verify() reported {} structural errors after the concurrent \
         split load (first few: {:?})",
        result.error_count(),
        &result.errors[..result.errors.len().min(8)]
    );
}
