//! Regression: grant/timeout race in `LockManager::lock_with_timeout_and_txn`.
//!
//! JE `LockManager.waitForLock` checks `validateOwnership`/`isOwner`
//! (LockManager.java:544/581/1555) on every timeout exit, BEFORE flushing the
//! waiter and throwing `LockTimeoutException`.  The reason: a concurrent
//! `release()` can promote a waiter to OWNER (under the shard mutex) in the
//! same instant the waiter's own `wait()` times out.  The waiter may have read
//! its grant flag as `false` just before `release` set it, so without the
//! ownership re-check it would flush itself (a no-op — it is no longer a
//! waiter) and return a timeout, leaving the lock OWNED by a locker that never
//! records or releases it: a permanently leaked lock that stalls every later
//! waiter until timeout (the `stress_nondup` hot-key stall).
//!
//! Oracle: after a bout of grant/timeout-boundary contention, no lock may be
//! leaked (owner count returns to 0) and a locker that is reported as having
//! timed out must NOT still own the lock.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use noxu_txn::{LockManager, LockType, TxnError};

/// Drive the exact race: a holder releases the hot lock right around the
/// moment a short-timeout waiter wakes, so a grant and a timeout collide.
/// Repeated enough that the collision window is hit many times.
#[test]
fn grant_timeout_race_does_not_leak_lock() {
    let hot: u64 = 85;
    // Short lock timeout so waiters routinely reach the timeout boundary.
    let lm = Arc::new(LockManager::with_lock_timeout(2));
    let stop = Arc::new(AtomicBool::new(false));
    let wn = Arc::new(AtomicU64::new(0));

    // Holder thread: repeatedly acquire+hold-briefly+release the hot lock so
    // its release lands near a waiter's timeout.
    let lm_h = Arc::clone(&lm);
    let stop_h = Arc::clone(&stop);
    let wn_h = Arc::clone(&wn);
    let holder = thread::spawn(move || {
        let mut locker: i64 = 1_000_000;
        let mut n = 0u64;
        while !stop_h.load(Ordering::Relaxed) {
            locker += 1;
            if lm_h.lock(hot, locker, LockType::Write, false, false).is_ok() {
                // Hold ~ the waiter's timeout so release races the timeout.
                thread::sleep(Duration::from_micros(1500));
                let _ = lm_h.release(hot, locker);
            }
            n += 1;
            wn_h.store(n, Ordering::Relaxed);
        }
        n
    });

    // Waiter loop: request the hot lock with the 2ms timeout. Each request
    // either times out or is granted; a granted request must be released, and
    // a timed-out request must NOT leave us owning the lock.
    let mut timed_out = 0u64;
    let mut granted = 0u64;
    let mut leaked_after_timeout = 0u64;
    let mut locker: i64 = 2_000_000;
    for _ in 0..20_000 {
        locker += 1;
        match lm.lock(hot, locker, LockType::Write, false, false) {
            Ok(_) => {
                granted += 1;
                let _ = lm.release(hot, locker);
            }
            Err(TxnError::LockTimeout { .. })
            | Err(TxnError::TransactionTimeout { .. }) => {
                timed_out += 1;
                // The crux: a timed-out request must own nothing. Before the
                // fix a grant-during-timeout leaves us owning `hot`, so this
                // probe fires and the leaked lock stalls the holder forever.
                if lm.is_owned_write_lock(hot, locker) {
                    leaked_after_timeout += 1;
                    // Clean it up so the test can finish and report.
                    let _ = lm.release(hot, locker);
                }
            }
            Err(_) => {}
        }
    }
    stop.store(true, Ordering::Relaxed);
    let hn = holder.join().unwrap();
    eprintln!(
        "grant/timeout race: granted={granted} timed_out={timed_out} \
         leaked_after_timeout={leaked_after_timeout} holder_iters={hn}"
    );

    // Oracle 1: no timed-out waiter may still own the lock (the leak).
    assert_eq!(
        leaked_after_timeout, 0,
        "a waiter reported LockTimeout but still OWNS the lock \
         (grant/timeout race leaked a lock) — JE validateOwnership guard missing"
    );
    // Oracle 2: after everyone is done and the holder stopped, the hot lock
    // must be fully drained (no leaked owner, no stuck waiter).
    // Give the holder's final release a moment to settle.
    thread::sleep(Duration::from_millis(20));
    let (owners, waiters) = lm.get_lock_info(hot);
    assert_eq!(
        (owners, waiters),
        (0, 0),
        "hot lock not drained after test: owners={owners} waiters={waiters} \
         (a leaked grant/timeout lock remains)"
    );
}
