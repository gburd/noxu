//! Regression test for the reader-vs-splitter race on the noxu-tree
//! read paths (`Tree::search`, `get_first_node`, `get_last_node`).
//!
//! Before the latch-coupling fix on the read paths, a `db.get(None, key, …)`
//! that ran concurrently with an insert that triggered a `split_child`
//! could return `NotFound` for a key that *was* in the tree: the reader
//! captured the target BIN's `Arc` while holding the parent's read lock,
//! dropped the parent read lock, and only then took the BIN's read lock.
//! In the gap a concurrent `split_child(parent, …)` could move half the
//! BIN's entries into a new sibling that the reader's parent snapshot
//! never saw. The reader's BIN now holds the left half; its target key
//! lives in the new sibling; the parent snapshot hasn't been re-read,
//! so the reader gives up at the BIN and reports `NotFound`.
//!
//! This test does many concurrent `db.get(None, key, …)` calls on a
//! database that another thread is actively populating. Every key the
//! writer reports as "committed" must be visible to a subsequent reader;
//! a race-induced false `NotFound` fails the assertion.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn concurrent_reads_during_inserts_no_false_not_found() {
    // Workload sized to exit the first-key path quickly, fill enough BINs
    // to force splits, and leave the reader threads racing the writer
    // through the descent path. 2,000 keys is enough on the default
    // BIN capacity to drive at least a few splits per run.
    const N_KEYS: usize = 2_000;
    const N_READERS: usize = 8;

    let dir = TempDir::new().unwrap();
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
            "rw",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap(),
    );

    let next_committed = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(N_READERS + 1));

    // Writer: insert keys 0..N_KEYS sequentially through committed
    // transactions, advancing `next_committed` after each commit so
    // readers know what is now visible.
    let writer = {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let next_committed = Arc::clone(&next_committed);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            for i in 0..N_KEYS {
                let txn = env.begin_transaction(None).unwrap();
                let key =
                    DatabaseEntry::from_vec(format!("k{i:06}").into_bytes());
                let val =
                    DatabaseEntry::from_vec(format!("v{i:06}").into_bytes());
                db.put(Some(&txn), &key, &val).unwrap();
                txn.commit().unwrap();
                // Publish the commit. Release ordering pairs with
                // Acquire on the reader side so the reader sees the
                // writes the writer made before this index advance.
                next_committed.store(i + 1, Ordering::Release);
            }
            stop.store(true, Ordering::Release);
        })
    };

    // Readers: race the writer with `db.get(None, key, …)`. Pick a
    // random committed index `j < next_committed` each iteration; if
    // the get returns NotFound, that's a false negative caused by a
    // concurrent split crossing our descent.
    let readers: Vec<_> = (0..N_READERS)
        .map(|_| {
            let db = Arc::clone(&db);
            let next_committed = Arc::clone(&next_committed);
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || -> Vec<usize> {
                barrier.wait();
                let mut misses: Vec<usize> = Vec::new();
                // Per-thread xorshift seed derived from pid + a hash of
                // the thread id (ThreadId::as_u64() is unstable per
                // AGENTS.md).
                use std::hash::{Hash, Hasher};
                let mut hasher =
                    std::collections::hash_map::DefaultHasher::new();
                std::thread::current().id().hash(&mut hasher);
                let mut rng = (std::process::id() as u64)
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    ^ hasher.finish();
                let deadline = Instant::now() + Duration::from_secs(15);
                while !stop.load(Ordering::Acquire) && Instant::now() < deadline
                {
                    let high = next_committed.load(Ordering::Acquire);
                    if high == 0 {
                        std::thread::yield_now();
                        continue;
                    }
                    // xorshift64
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let j = (rng as usize) % high;
                    let key = DatabaseEntry::from_vec(
                        format!("k{j:06}").into_bytes(),
                    );
                    let mut out = DatabaseEntry::new();
                    let status = db.get(None, &key, &mut out).unwrap();
                    if status != OperationStatus::Success {
                        misses.push(j);
                    }
                }
                misses
            })
        })
        .collect();

    writer.join().unwrap();
    let mut total_misses: Vec<(usize, usize)> = Vec::new();
    for (tid, h) in readers.into_iter().enumerate() {
        for j in h.join().unwrap() {
            total_misses.push((tid, j));
        }
    }

    assert!(
        total_misses.is_empty(),
        "{} false NotFound on already-committed keys (first 8: {:?})",
        total_misses.len(),
        &total_misses[..total_misses.len().min(8)]
    );
}
