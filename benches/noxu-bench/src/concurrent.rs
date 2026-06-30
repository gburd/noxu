//! W10 – Concurrent mixed workload.
//!
//! Spawns `reader_threads` reader threads and `writer_threads` writer threads.
//! All threads synchronise at a Barrier before starting, so that wall-clock
//! time measures pure throughput rather than thread-startup latency.

use noxu_db::{Database, DatabaseEntry, Environment};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::{Arc, Barrier};
use std::time::Instant;

/// Result returned by `run_concurrent`.
pub struct ConcurrentResult {
    /// Total number of logical operations completed across all threads.
    pub total_ops: u64,
    /// Wall-clock elapsed time from barrier release to last thread join (ms).
    pub elapsed_ms: f64,
    /// Aggregate throughput: total_ops / elapsed_seconds.
    pub ops_per_sec: f64,
}

/// Run concurrent workload: `reader_threads` reader threads + `writer_threads`
/// writer threads, each performing `ops_per_thread` operations.
///
/// Readers perform random point-gets; writers insert records with keys
/// `writer_id * ops_per_thread + i` so each writer owns a disjoint key range.
///
/// Returns aggregate metrics for the whole run.
pub fn run_concurrent(
    db: Arc<Database>,
    reader_threads: usize,
    writer_threads: usize,
    ops_per_thread: usize,
    value_size: usize,
) -> ConcurrentResult {
    let total_threads = reader_threads + writer_threads;
    let barrier = Arc::new(Barrier::new(total_threads));

    let mut handles = Vec::with_capacity(total_threads);

    // ── Reader threads ────────────────────────────────────────────────────────
    for reader_id in 0..reader_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let n = ops_per_thread;

        let handle = std::thread::spawn(move || -> u64 {
            // Seed is distinct per reader so different threads access different
            // keys, exercising more of the key space.
            let mut rng =
                SmallRng::seed_from_u64(reader_id as u64 * 1_000_003 + 7);

            barrier_clone.wait();

            let mut ops: u64 = 0;
            let mut data = DatabaseEntry::new();

            for _ in 0..n {
                // Read from the pre-populated range 0..n (keys written by populate()).
                let idx: usize = rng.gen_range(0..n);
                let k = DatabaseEntry::from_vec(
                    format!("{:010}", idx).into_bytes(),
                );
                // Ignore NotFound — the key may have been deleted by a concurrent writer.
                let _ = db_clone.get_into(None, &k, &mut data);
                ops += 1;
            }

            ops
        });

        handles.push(handle);
    }

    // ── Writer threads ────────────────────────────────────────────────────────
    for writer_id in 0..writer_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let n = ops_per_thread;

        let handle = std::thread::spawn(move || -> u64 {
            let value = vec![0x58u8; value_size];

            barrier_clone.wait();

            let mut ops: u64 = 0;
            for i in 0..n {
                // Each writer owns keys in a disjoint range to avoid
                // artificially high lock conflicts.
                let key_idx = writer_id * n + i;
                let k = DatabaseEntry::from_vec(
                    format!("{:010}", key_idx).into_bytes(),
                );
                let v = DatabaseEntry::from_bytes(&value);
                let _ = db_clone.put(&k, &v);
                ops += 1;
            }

            ops
        });

        handles.push(handle);
    }

    // ── Measure wall-clock time (barrier released inside each thread) ─────────
    //
    // We start the clock here just before all threads reach the barrier.
    // Because the barrier synchronises them, the real work starts very close
    // to this instant.
    let t0 = Instant::now();

    let mut total_ops: u64 = 0;
    for h in handles {
        total_ops += h.join().unwrap_or(0);
    }

    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let ops_per_sec = if elapsed_ms > 0.0 {
        total_ops as f64 / (elapsed_ms / 1000.0)
    } else {
        0.0
    };

    ConcurrentResult { total_ops, elapsed_ms, ops_per_sec }
}

/// Run a transactional concurrent workload with explicit transactions.
///
/// Each writer wraps its put in a `begin_transaction` / `commit` pair.
/// This exercises the full WAL commit + `flush_sync` + `FsyncManager` path.
/// When `group_commit_threshold > 0` and `group_commit_interval_ms > 0`,
/// concurrent commit threads coalesce fsyncs through the FSyncManager.
pub fn run_concurrent_txn(
    env: &Environment,
    db: &Database,
    writer_threads: usize,
    ops_per_thread: usize,
    value_size: usize,
) -> ConcurrentResult {
    use std::sync::{Arc, Barrier};
    let barrier = Arc::new(Barrier::new(writer_threads));
    let t0 = std::time::Instant::now();

    // scoped threads borrow env and db with the lifetime of this function,
    // avoiding 'static requirements or Arc<Environment>.
    let total_ops: u64 = std::thread::scope(|s| {
        let handles: Vec<_> = (0..writer_threads)
            .map(|writer_id| {
                let barrier = Arc::clone(&barrier);
                s.spawn(move || -> u64 {
                    let value = vec![0x58u8; value_size];
                    barrier.wait();
                    let mut ops: u64 = 0;
                    for i in 0..ops_per_thread {
                        let key_idx = writer_id * ops_per_thread + i;
                        let k = DatabaseEntry::from_vec(
                            format!("{:010}", key_idx).into_bytes(),
                        );
                        let v = DatabaseEntry::from_bytes(&value);
                        let txn = env.begin_transaction(None).unwrap();
                        let _ = db.put_in(&txn, &k, &v);
                        let _ = txn.commit();
                        ops += 1;
                    }
                    ops
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap_or(0)).sum()
    });

    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let ops_per_sec = if elapsed_ms > 0.0 {
        total_ops as f64 / (elapsed_ms / 1000.0)
    } else {
        0.0
    };
    ConcurrentResult { total_ops, elapsed_ms, ops_per_sec }
}
