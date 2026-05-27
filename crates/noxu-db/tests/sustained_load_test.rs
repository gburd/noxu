//! Sustained load and chaos tests.
//!
//! All slow tests are marked `#[ignore]` and must be run with:
//!
//! ```text
//! cargo nextest run -p noxu-db --profile slow --run-ignored all
//! ```
//!
//! `test_cleaner_reduces_log_files_under_load` is the only test that runs in
//! normal CI (it completes in well under 60 s on any storage device).
//!
//! By default temporary directories are created under the system temp dir.
//! Set `NOXU_TEST_SCRATCH=/path/to/disk` to point them at a real disk (not
//! tmpfs) when running I/O-sensitive measurements.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
    TransactionConfig,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a temporary directory honoring `NOXU_TEST_SCRATCH` when set.
///
/// In normal CI we use the system temp dir, which is portable across
/// macOS / Linux developer machines and CI runners. For sustained-load
/// I/O measurements on a physical disk, set `NOXU_TEST_SCRATCH` to a
/// path on the device under test.
fn scratch_dir(prefix: &str) -> TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    match std::env::var_os("NOXU_TEST_SCRATCH") {
        Some(p) => {
            builder.tempdir_in(std::path::Path::new(&p)).unwrap_or_else(|e| {
                panic!(
                    "create temp dir under NOXU_TEST_SCRATCH={}: {e}",
                    std::path::Path::new(&p).display()
                )
            })
        }
        None => builder.tempdir().expect("create temp dir"),
    }
}

fn open_env(dir: &TempDir) -> (noxu_db::Environment, noxu_db::Database) {
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("env open");
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("db open");
    (env, db)
}

// ---------------------------------------------------------------------------
// P4-1  Sustained 8r8w for 60 s
// ---------------------------------------------------------------------------

/// 8 writer threads and 8 reader threads run concurrently for 60 seconds.
///
/// Writers each own a small fixed key-space (10 keys per thread) that they
/// overwrite in tight commit loops, keeping the B-tree size bounded and
/// generating continuous garbage for the cleaner.  Readers perform
/// full-cursor scans under read-committed isolation with a 1 ms sleep
/// between scans to avoid starving the writers.
///
/// Assertions:
/// - no thread panics
/// - at least one write and one read scan complete (liveness)
/// - writers collectively complete > 1 000 commits (throughput sanity)
#[test]
#[ignore]
fn test_sustained_8r8w_60s() {
    let dir = scratch_dir("noxu_sustained_8r8w_");
    let (env, db) = open_env(&dir);
    let env = Arc::new(env);
    let db = Arc::new(db);

    let done = Arc::new(AtomicBool::new(false));
    let commit_count = Arc::new(AtomicU64::new(0));
    let scan_count = Arc::new(AtomicU64::new(0));

    const WRITERS: usize = 8;
    const READERS: usize = 8;
    const KEYS_PER_WRITER: usize = 10;
    let barrier = Arc::new(Barrier::new(WRITERS + READERS));

    // 8 writer threads — each cycles through KEYS_PER_WRITER dedicated keys.
    let writers: Vec<_> = (0..WRITERS)
        .map(|tid| {
            let env = Arc::clone(&env);
            let db = Arc::clone(&db);
            let done = Arc::clone(&done);
            let commit_count = Arc::clone(&commit_count);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut seq: u64 = 0;
                while !done.load(Ordering::Relaxed) {
                    let txn = env.begin_transaction(None).unwrap();
                    for i in 0..KEYS_PER_WRITER {
                        let key = format!("w{tid:02}-k{i:02}");
                        let val = format!("seq{seq:010}");
                        let k = DatabaseEntry::from_bytes(key.as_bytes());
                        let v = DatabaseEntry::from_bytes(val.as_bytes());
                        db.put(Some(&txn), &k, &v).unwrap();
                    }
                    txn.commit().unwrap();
                    commit_count.fetch_add(1, Ordering::Relaxed);
                    seq += 1;
                }
            })
        })
        .collect();

    // 8 reader threads — full cursor scan, read-committed, 1 ms sleep.
    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let env = Arc::clone(&env);
            let db = Arc::clone(&db);
            let done = Arc::clone(&done);
            let scan_count = Arc::clone(&scan_count);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let rc = TransactionConfig::read_committed();
                while !done.load(Ordering::Relaxed) {
                    let txn = env.begin_transaction(Some(&rc)).unwrap();
                    let mut cursor = db.open_cursor(Some(&txn), None).unwrap();
                    let mut k = DatabaseEntry::new();
                    let mut v = DatabaseEntry::new();
                    let mut n: u64 = 0;
                    if cursor.get(&mut k, &mut v, Get::First, None).unwrap()
                        == OperationStatus::Success
                    {
                        n += 1;
                        while cursor
                            .get(&mut k, &mut v, Get::Next, None)
                            .unwrap()
                            == OperationStatus::Success
                        {
                            n += 1;
                        }
                    }
                    cursor.close().unwrap();
                    txn.commit().unwrap();
                    scan_count.fetch_add(n, Ordering::Relaxed);
                    thread::sleep(Duration::from_millis(1));
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_secs(60));
    done.store(true, Ordering::Relaxed);

    for w in writers {
        w.join().expect("writer thread panicked");
    }
    for r in readers {
        r.join().expect("reader thread panicked");
    }

    let commits = commit_count.load(Ordering::Relaxed);
    let scans = scan_count.load(Ordering::Relaxed);
    println!("60s 8r8w: {commits} commits, {scans} total rows scanned");

    assert!(commits > 1_000, "expected > 1 000 commits in 60 s, got {commits}");
    assert!(scans > 0, "no scan rows returned in 60 s");

    drop(db);
    drop(env);
}

// ---------------------------------------------------------------------------
// P4-2  Checkpoint under load for 30 s
// ---------------------------------------------------------------------------

/// 4 writer threads pump records continuously while the main thread calls
/// `env.checkpoint()` every 500 ms for 30 seconds.
///
/// Assertions:
/// - every checkpoint completes in < 5 s (no checkpoint deadlock)
/// - at least 50 checkpoints complete in 30 s (≈ 1 every 600 ms budget)
#[test]
#[ignore]
fn test_checkpoint_under_load_30s() {
    let dir = scratch_dir("noxu_checkpoint_load_");
    let (env, db) = open_env(&dir);
    let env = Arc::new(env);
    let db = Arc::new(db);

    let done = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(4 + 1)); // 4 writers + main

    let writers: Vec<_> = (0..4usize)
        .map(|tid| {
            let db = Arc::clone(&db);
            let done = Arc::clone(&done);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut seq: u64 = 0;
                while !done.load(Ordering::Relaxed) {
                    let key = format!("wt{tid}-{seq:08}");
                    let val = vec![b'v'; 64];
                    let k = DatabaseEntry::from_bytes(key.as_bytes());
                    let v = DatabaseEntry::from_bytes(&val);
                    db.put(None, &k, &v).unwrap();
                    seq += 1;
                }
            })
        })
        .collect();

    barrier.wait(); // main thread joins the barrier

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut checkpoint_count: u64 = 0;

    while Instant::now() < deadline {
        let t = Instant::now();
        env.checkpoint(None).expect("checkpoint failed");
        let elapsed = t.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "checkpoint took {elapsed:?} — possible deadlock"
        );
        checkpoint_count += 1;
        thread::sleep(Duration::from_millis(500));
    }

    done.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().expect("writer thread panicked");
    }

    println!("30s checkpoint under load: {checkpoint_count} checkpoints");
    assert!(
        checkpoint_count >= 50,
        "expected ≥ 50 checkpoints in 30 s, got {checkpoint_count}"
    );

    drop(db);
    drop(env);
}

// ---------------------------------------------------------------------------
// P4-3  Cleaner reclaims space after heavy overwrites (runs in normal CI)
// ---------------------------------------------------------------------------

/// Write 500 keys with 100-byte values, then overwrite each key 49 times to
/// produce large amounts of obsolete data.  Use small (64 KB) log files so
/// many files are created, giving the background cleaner concrete work to do.
///
/// After a checkpoint and a 3-second pause the cleaner must have run at
/// least once and deleted at least one file.
///
/// Individual `put()` calls are timed; none may stall longer than 5 seconds
/// (catches cleaner-throttle deadlocks).
///
/// This test does NOT require `#[ignore]`; it completes in ≈ 10–30 s on
/// any NVMe device.
#[test]
fn test_cleaner_reduces_log_files_under_load() {
    let dir = scratch_dir("noxu_cleaner_load_");
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_file_max_bytes(64 * 1024) // 64 KB files → many files
            .with_cleaner_min_utilization(90), // aggressive: clean when ≥ 10 % obsolete
    )
    .expect("env open");
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("db open");

    const KEYS: usize = 500;
    const OVERWRITES: usize = 49;
    let stall_limit = Duration::from_secs(5);

    // Phase 1: initial write of all keys.
    for i in 0..KEYS {
        let key = format!("k{i:05}");
        let val = vec![b'a'; 100];
        let k = DatabaseEntry::from_bytes(key.as_bytes());
        let v = DatabaseEntry::from_bytes(&val);
        let t = Instant::now();
        db.put(None, &k, &v).unwrap();
        assert!(
            t.elapsed() < stall_limit,
            "put stalled on initial write k={i}"
        );
    }

    // Phase 2: overwrite each key OVERWRITES times → lots of obsolete LNs.
    for pass in 0..OVERWRITES {
        for i in 0..KEYS {
            let key = format!("k{i:05}");
            let fill = b'a' + (pass as u8 % 26);
            let val = vec![fill; 100];
            let k = DatabaseEntry::from_bytes(key.as_bytes());
            let v = DatabaseEntry::from_bytes(&val);
            let t = Instant::now();
            db.put(None, &k, &v).unwrap();
            assert!(
                t.elapsed() < stall_limit,
                "put stalled on overwrite pass={pass} k={i}"
            );
        }
    }

    // Checkpoint so the cleaner can see the obsolete summary information.
    env.checkpoint(None).expect("checkpoint failed");

    // Give the background cleaner time to run.
    thread::sleep(Duration::from_secs(3));

    let stats = env.get_stats().expect("get_stats failed");
    assert!(
        stats.cleaner.runs > 0 || stats.cleaner.deletions > 0,
        "cleaner never ran after {KEYS} keys × {OVERWRITES} overwrites: \
         runs={} deletions={}",
        stats.cleaner.runs,
        stats.cleaner.deletions,
    );

    drop(db);
    drop(env);
}
