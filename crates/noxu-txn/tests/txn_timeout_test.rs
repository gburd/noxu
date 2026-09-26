//! F22 / C6 — transaction-level timeout (`txn_timeout_ms`) enforcement.
//!
//! Verifies that a transaction configured with a SHORT transaction-level
//! timeout is cut short while blocked on a lock held by another txn, even
//! when the per-lock timeout is LONG (or 0 == wait forever) — mirroring JE
//! `LockManager.lock()` (LockManager.java:298-306) "if the txn time remaining
//! is less than the lock timeout, use the txn time remaining instead" and
//! `waitForLock` (LockManager.java:729-738) precedence "when both timeouts
//! occur, throw TransactionTimeout".
//!
//! The harness mirrors `lock_manager_test.rs::lock_times_out_when_blocked`:
//! one txn holds a write lock, a second txn (on another thread) blocks on it
//! with a bounded timeout, and we assert the *elapsed* wait is bounded by the
//! shorter of the two timeouts, and the error TYPE reflects which deadline
//! fired.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use noxu_txn::{LockManager, LockType, Locker, Txn, TxnError};

/// A LockManager whose per-lock default timeout is "wait forever" (0), so the
/// ONLY thing that can cut a wait short is a txn-level timeout.
fn lm_forever() -> Arc<LockManager> {
    Arc::new(LockManager::with_lock_timeout(0))
}

// ── 1. SHORT txn timeout, LONG/zero lock timeout ──────────────────────────────
//
// txn1 holds a write lock on lsn 1 (never releases). txn2 sets a 100 ms
// txn-level timeout and a 60 s per-lock timeout, then blocks. It MUST fail with
// TransactionTimeout after ~100 ms, NOT wait the 60 s lock timeout.
#[test]
fn short_txn_timeout_cuts_long_lock_wait_short() {
    let lm = lm_forever();

    // txn1 holds the write lock forever.
    let mut holder = Txn::new(1, Arc::clone(&lm));
    holder.lock(1, LockType::Write, false).unwrap();

    // txn2 blocks with a short txn timeout and a long lock timeout.
    let lm2 = Arc::clone(&lm);
    let (elapsed, result) = thread::spawn(move || {
        let mut waiter = Txn::new(2, lm2);
        waiter.set_lock_timeout(3_000); // 3 s per-lock timeout
        waiter.set_txn_timeout(100); // 100 ms txn-level timeout
        let start = Instant::now();
        let r = waiter.lock(1, LockType::Read, false);
        (start.elapsed(), r)
    })
    .join()
    .unwrap();

    assert!(
        matches!(result, Err(TxnError::TransactionTimeout { txn_id: 2, .. })),
        "expected TransactionTimeout for txn 2, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(1_500),
        "wait must be bounded by the 100ms txn timeout, not the 3s lock \
         timeout; elapsed = {elapsed:?}"
    );

    drop(holder);
}

// ── 2. SHORT txn timeout, lock timeout == 0 (wait forever) ───────────────────
//
// Same as above but the waiter passes lock timeout 0 (forever). On base this
// hangs forever; with the fix the txn timeout must still fire.
#[test]
fn short_txn_timeout_fires_when_lock_timeout_is_forever() {
    let lm = lm_forever();

    let mut holder = Txn::new(10, Arc::clone(&lm));
    holder.lock(1, LockType::Write, false).unwrap();

    let lm2 = Arc::clone(&lm);
    let handle = thread::spawn(move || {
        let mut waiter = Txn::new(20, lm2);
        waiter.set_lock_timeout(0); // wait forever at the lock level
        waiter.set_txn_timeout(100); // 100 ms txn-level timeout
        let start = Instant::now();
        let r = waiter.lock(1, LockType::Read, false);
        (start.elapsed(), r)
    });

    // Give it a generous but bounded window; the fix should return in ~100 ms.
    thread::sleep(Duration::from_secs(3));
    assert!(
        handle.is_finished(),
        "waiter with a 100ms txn timeout must not wait forever"
    );
    let (elapsed, result) = handle.join().unwrap();
    assert!(
        matches!(result, Err(TxnError::TransactionTimeout { txn_id: 20, .. })),
        "expected TransactionTimeout for txn 20, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "wait must be bounded by the 100ms txn timeout; elapsed = {elapsed:?}"
    );

    drop(holder);
}

// ── 3. LONG txn timeout, SHORT lock timeout → LockTimeout wins ────────────────
//
// Preserves existing per-lock timeout behaviour: when the lock deadline is the
// one that expires first, the error is LockTimeout (not TransactionTimeout).
#[test]
fn long_txn_timeout_yields_to_short_lock_timeout() {
    let lm = lm_forever();

    let mut holder = Txn::new(100, Arc::clone(&lm));
    holder.lock(1, LockType::Write, false).unwrap();

    let lm2 = Arc::clone(&lm);
    let (elapsed, result) = thread::spawn(move || {
        let mut waiter = Txn::new(200, lm2);
        waiter.set_lock_timeout(100); // 100 ms per-lock timeout
        waiter.set_txn_timeout(60_000); // 60 s txn-level timeout
        let start = Instant::now();
        let r = waiter.lock(1, LockType::Read, false);
        (start.elapsed(), r)
    })
    .join()
    .unwrap();

    assert!(
        matches!(result, Err(TxnError::LockTimeout { .. })),
        "expected LockTimeout (lock deadline fires first), got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "elapsed = {elapsed:?}"
    );

    drop(holder);
}

// ── 4. BOTH expire → TransactionTimeout wins (JE precedence) ─────────────────
//
// txn and lock timeouts are equal, so both deadlines expire together; JE
// throws TransactionTimeout in that case (LockManager.java:729-738) "otherwise
// TransactionTimeout may never be thrown".
#[test]
fn both_timeouts_expire_transaction_timeout_wins() {
    let lm = lm_forever();

    let mut holder = Txn::new(1000, Arc::clone(&lm));
    holder.lock(1, LockType::Write, false).unwrap();

    let lm2 = Arc::clone(&lm);
    let result = thread::spawn(move || {
        let mut waiter = Txn::new(2000, lm2);
        waiter.set_lock_timeout(100);
        waiter.set_txn_timeout(100);
        waiter.lock(1, LockType::Read, false)
    })
    .join()
    .unwrap();

    assert!(
        matches!(result, Err(TxnError::TransactionTimeout { txn_id: 2000, .. })),
        "when both deadlines expire, TransactionTimeout must win, got {result:?}"
    );

    drop(holder);
}

// ── 5. No txn timeout set (== 0) → existing per-lock behaviour unchanged ──────
#[test]
fn no_txn_timeout_preserves_lock_timeout() {
    let lm = lm_forever();

    let mut holder = Txn::new(3, Arc::clone(&lm));
    holder.lock(1, LockType::Write, false).unwrap();

    let lm2 = Arc::clone(&lm);
    let result = thread::spawn(move || {
        let mut waiter = Txn::new(4, lm2);
        waiter.set_lock_timeout(100); // 100 ms per-lock timeout
        // txn_timeout left at default 0 == no txn timeout
        waiter.lock(1, LockType::Read, false)
    })
    .join()
    .unwrap();

    assert!(
        matches!(result, Err(TxnError::LockTimeout { .. })),
        "with no txn timeout, only the per-lock timeout applies, got {result:?}"
    );

    drop(holder);
}
