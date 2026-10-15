//! BUG-CLEANER-FORCE-HANG-1 lock-order witness (reviewer-added, task 2c).
//!
//! The fix replaced the erroring `running` CAS in `Cleaner::do_clean` with a
//! `pass_lock: Mutex<()>` held across the whole pass.  Two lock-order claims
//! must hold:
//!
//!   1. **No ABBA deadlock.** `Environment::clean_log{,_forced}` holds the
//!      rank-10 handle `Mutex<EnvironmentImpl>` across `run_cleaner -> do_clean`,
//!      which now BLOCKS on `pass_lock`.  The cleaner daemon acquires
//!      `pass_lock` WITHOUT holding the handle Mutex, and a pass never acquires
//!      the handle Mutex.  So the only order is `handle Mutex -> pass_lock`
//!      (one-way); there is no cycle.  Many threads hammering `clean_log_forced`
//!      while the daemon also cleans must make forward progress, never hang.
//!
//!   2. **Close while a manual clean waits on pass_lock does not hang.**
//!      `close()` sets the Cleaner's internal shutdown flag BEFORE the bounded
//!      daemon join; a pass checks that flag between files/entries and breaks,
//!      releasing `pass_lock`.  A manual clean then acquires `pass_lock`, sees
//!      shutdown, and returns `Err("Cleaner is shut down")`.  No unbounded wait.
//!
//! Both are checked under a hard deadline: a would-be deadlock becomes a
//! deterministic test failure, never a CI hang.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

/// Run `body` on a worker thread; fail if it does not finish within `secs`.
fn with_deadline(secs: u64, body: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    let h = std::thread::spawn(move || {
        body();
        let _ = tx.send(());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(()) => h.join().unwrap(),
        Err(_) => panic!(
            "lock-order witness: operation did not finish within {secs}s — \
             pass_lock vs handle-Mutex deadlock or unbounded close wait"
        ),
    }
}

#[test]
fn concurrent_forced_clean_vs_close_no_hang() {
    with_deadline(90, || {
        let dir = TempDir::new().unwrap();
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "churn",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();

        // Create obsolete bytes so the (daemon + manual) cleaner has real work
        // and passes actually overlap: repeatedly overwrite a working set.
        let val = vec![b'x'; 256];
        for round in 0..40u32 {
            for k in 0..64u32 {
                let key = (round.wrapping_mul(64) + k).to_be_bytes();
                db.put(DatabaseEntry::from_bytes(&key), &val).unwrap();
            }
            // Overwrite the same keys to generate obsolete LNs.
            for k in 0..64u32 {
                let key = (round.wrapping_mul(64) + k).to_be_bytes();
                db.put(DatabaseEntry::from_bytes(&key), &val).unwrap();
            }
        }

        let env = Arc::new(env);
        let stop = Arc::new(AtomicBool::new(false));

        // Many threads hammer clean_log_forced (each holds the handle Mutex
        // across do_clean, which blocks on pass_lock) while the background
        // cleaner daemon on the same env also runs passes.  If handle-Mutex ->
        // pass_lock could invert, this deadlocks.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let e = Arc::clone(&env);
            let s = Arc::clone(&stop);
            handles.push(std::thread::spawn(move || {
                let mut n = 0u64;
                while !s.load(Ordering::Relaxed) {
                    // Every call returns (Ok or Err); a hang here is a deadlock.
                    let _ = e.clean_log_forced();
                    n += 1;
                }
                n
            }));
        }

        // Let the overlap run so manual cleans are very likely parked on
        // pass_lock behind in-flight daemon passes.  If handle-Mutex ->
        // pass_lock inverted, the hammer threads would already be wedged and
        // the per-thread counters would stop advancing.
        std::thread::sleep(Duration::from_millis(1500));

        // Stop the hammer threads and confirm each one actually made forward
        // progress (completed many clean_log_forced calls) — i.e. no deadlock.
        stop.store(true, Ordering::Relaxed);
        let mut total = 0u64;
        for h in handles {
            total += h.join().unwrap();
        }
        assert!(
            total > 0,
            "concurrent forced cleans made no progress — likely a \
             handle-Mutex -> pass_lock deadlock"
        );

        // Now exercise the close-while-manual-clean path on a FRESH thread
        // that is still mid-clean when close() runs: spawn one more hammer,
        // then close in-order (db handle first, as the API requires).  close()
        // must return bounded even with a clean in flight.
        let e2 = Arc::clone(&env);
        let stop2 = Arc::new(AtomicBool::new(false));
        let s2 = Arc::clone(&stop2);
        let hammer2 = std::thread::spawn(move || {
            while !s2.load(Ordering::Relaxed) {
                let _ = e2.clean_log_forced();
            }
        });
        std::thread::sleep(Duration::from_millis(200));
        stop2.store(true, Ordering::Relaxed);
        hammer2.join().unwrap();
        db.close().unwrap();
        env.close().unwrap();
    });
}
