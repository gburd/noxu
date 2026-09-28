//! In-process ports of `je.rep.txn.LockPreemptionTest`.
//!
//! ## What the JE test proves
//!
//! In a 2-node group, a replica opens a read lock on a record, then the master
//! overwrites that record. The master's write replicates to the replica, whose
//! replayer (an *importunate* locker) STEALS the read lock so the replicated
//! write can be applied without waiting — "the master's write must win over a
//! replica-side reader". The JE test then checks the reader's side: its next
//! operation that TAKES A NEW LOCK throws `LockPreemptedException` (its read
//! snapshot was invalidated), while operations that take no new lock (commit,
//! abort, a read-committed cursor MOVE to a different record) do NOT throw.
//!
//! ## Noxu mapping (documented deviation: no live replica cursor layer here)
//!
//! Noxu's lock manager already implements the load-bearing half — the
//! importunate steal (`LockManager::lock_importunate_with_timeout`, cited to
//! `LockManagerTest.testImportunateTxn1` in `lock_manager.rs`). These ports
//! drive that path directly at the lock-manager level, which is where the JE
//! `LockPreemptedException` machinery lives (JE `LockManager.stealLock` →
//! `Locker` preempted flag → `checkPreempted` on the next lock request).
//!
//! The SEVERE-class invariant — a stale replica reader must NOT be able to
//! block or delay the master's replicated write — is asserted directly and
//! PASSES: the importunate steal succeeds even while the reader holds the lock.
//!
//! The victim-NOTIFICATION half — the reader learning, via
//! `LockPreemptedException`, that its snapshot was invalidated — is NOT
//! implemented in Noxu (the victim is silently removed from the lock's owners;
//! there is no `LockPreempted` error and no per-locker preempted flag). That is
//! recorded as an `#[ignore]`d faithful port + escalated as a fidelity gap in
//! the package report. It is NOT data-loss/corruption (no write is lost, the
//! master always wins); it is a read-stability notification gap.

use noxu_txn::{LockGrantType, LockManager, LockType, TxnError};

const LSN_KEY1: u64 = 0x1001;

// =====================================================================
// LockPreemptionTest.testPreempted  (master-write-wins half — PASSES)
// =====================================================================

/// JE: `LockPreemptionTest.testPreempted`.
///
/// Also covers (COVERED-CITED, master-write-wins is the shared invariant):
///   * `LockPreemptionTest.testPreemptedWithCursor`
///   * `LockPreemptionTest.testPreemptedWithReadCommittedCursor`
///   * `LockPreemptionTest.testPreemptedWithNonTransactionalCursor`
///   * `LockPreemptionTest.testPreemptedWithTwoReadCommittedCursors`
///   * `LockPreemptionTest.testPreemptedWithTwoNonTransactionalCursors`
///   * `LockPreemptionTest.testPreemptedWithReadCommittedCursorThenDbRead`
///   * `LockPreemptionTest.testPreemptedWithDbReadThenReadCommittedCursor`
///   * `LockPreemptionTest.testPreemptedAfterAttemptToMoveReadCommittedCursor`
///   * `LockPreemptionTest.testPreemptedAfterAttemptToMoveNonTransactionalCursor`
///
/// Every one of those JE variants sets up the SAME preemption event — a
/// replica reader holds a lock, the master steals it via the replay's
/// importunate write — and differs only in which reader-side cursor idiom
/// re-touches the lock afterward (a cursor idiom Noxu's lock manager has no
/// analog for). The event itself, and its safety consequence (the master's
/// write is applied without waiting on the reader), is what this port asserts.
///
/// SEVERE-CLASS INVARIANT: the reader (preemptable) must not be able to hold
/// off the master's replicated write. The importunate steal must succeed
/// immediately even though the reader holds a conflicting lock.
#[test]
fn lock_preemption_master_write_wins_over_replica_reader() {
    let lm = LockManager::new();

    // Replica reader (locker 1, preemptable) holds a read lock on KEY1 —
    // JE: `replicaDb.get(replicaTxn, KEY1, ...)`.
    let g = lm.lock(LSN_KEY1, 1, LockType::Read, false, false).unwrap();
    assert_eq!(g, LockGrantType::New, "reader takes its read lock");
    assert!(lm.get_owned_lock_type(LSN_KEY1, 1).is_some());

    // The master overwrites KEY1; the replica's replayer (importunate locker
    // 2) STEALS the lock to apply the replicated write — JE: `masterDb.put`
    // then the stream applies on the replica. With a short timeout, the steal
    // must grant IMMEDIATELY rather than waiting on the reader or timing out.
    let grant = lm
        .lock_importunate_with_timeout(LSN_KEY1, 2, LockType::Write, false, 200)
        .expect("master replay must steal the reader's lock, not block on it");
    assert!(matches!(grant, LockGrantType::New | LockGrantType::Existing));

    // The master's write now owns the record; the reader was preempted.
    assert!(lm.is_owned_write_lock(LSN_KEY1, 2), "master write wins");
    assert!(
        lm.get_owned_lock_type(LSN_KEY1, 1).is_none(),
        "the preempted reader no longer owns the lock"
    );

    // VACUITY GUARD: prove the steal is load-bearing. A NON-importunate
    // conflicting write with the same short timeout must NOT get the lock
    // instantly — it waits and times out, showing the read lock really did
    // conflict and only the importunate path can bypass it.
    let non_importunate =
        lm.lock_with_timeout(LSN_KEY1, 3, LockType::Write, false, false, 50);
    assert!(
        matches!(
            non_importunate,
            Err(TxnError::LockTimeout { .. }
                | TxnError::LockNotAvailable { .. })
        ),
        "a non-importunate write must conflict with the held lock, proving \
         the importunate steal above was doing real work; got {non_importunate:?}"
    );
}

// =====================================================================
// LockPreemptionTest.testNotPreempted*  (commit/abort after steal — PASSES)
// =====================================================================

/// JE: `LockPreemptionTest.testNotPreemptedCommit`.
///
/// Also covers (COVERED-CITED — "no NEW lock is taken after the steal, so no
/// preemption is signalled"):
///   * `LockPreemptionTest.testNotPreemptedCommitWithCursor`
///   * `LockPreemptionTest.testNotPreemptedAbort`
///   * `LockPreemptionTest.testNotPreemptedAbortWithCursor`
///   * `LockPreemptionTest.testNotPreemptedMoveReadCommittedCursor`
///   * `LockPreemptionTest.testNotPreemptedMoveNonTransactionalCursor`
///   * `LockPreemptionTest.testNotPreemptedAfterAttemptToMoveReadCommittedCursor`
///   * `LockPreemptionTest.testNotPreemptedAfterAttemptToMoveNonTransactionalCursor`
///
/// The JE invariant across this family: after its lock is stolen, a reader that
/// does NOT take a new lock (it commits, aborts, or moves a read-committed
/// cursor to a *different*, still-available record) must complete WITHOUT a
/// `LockPreemptedException`. In Noxu, a preempted locker's lock is simply
/// removed from the owners; releasing whatever it still holds (commit/abort
/// path) is unconditionally safe and takes no new lock, so it never errors.
#[test]
fn lock_preemption_not_signalled_when_no_new_lock_taken() {
    let lm = LockManager::new();

    // Reader holds a read lock; master steals it.
    lm.lock(LSN_KEY1, 1, LockType::Read, false, false).unwrap();
    lm.lock_importunate_with_timeout(LSN_KEY1, 2, LockType::Write, false, 200)
        .expect("steal");
    assert!(lm.get_owned_lock_type(LSN_KEY1, 1).is_none());

    // The reader takes NO new lock; it just releases (its commit/abort path).
    // JE: `replicaTxn.commit()` / `replicaTxn.abort()` after the steal — no
    // LockPreemptedException. In Noxu, releasing a lock the locker no longer
    // owns is a harmless no-op, never an error.
    let r = lm.release(LSN_KEY1, 1);
    assert!(
        r.is_ok(),
        "releasing after a steal (commit/abort path, no new lock) must not \
         error; got {r:?}"
    );

    // JE's `testNotPreemptedMoveReadCommittedCursor` only avoids
    // LockPreemptedException because a READ_COMMITTED cursor RELEASES its read
    // lock immediately after reading — so at steal time it holds nothing, the
    // steal never marks it preempted (`setPreempted` is not called), and its
    // subsequent MOVE to another key succeeds. Model that faithfully with a
    // separate read-committed-style locker (locker 3) that released KEY2
    // BEFORE the steal: it was never preempted, so an unrelated new lock still
    // succeeds. (A locker that HELD its lock through the steal — locker 1
    // above — IS preempted and its next new lock gets LockPreempted; that JE
    // contract is asserted in `lock_preemption_victim_is_notified_on_next_lock`.)
    const LSN_KEY2: u64 = 0x1002;
    lm.lock(LSN_KEY2, 3, LockType::Read, false, false).unwrap();
    lm.release(LSN_KEY2, 3).unwrap(); // read-committed: released before steal
    let g2 = lm.lock(LSN_KEY2, 3, LockType::Read, false, false);
    assert!(
        matches!(g2, Ok(LockGrantType::New | LockGrantType::Existing)),
        "an unrelated lock by a never-preempted (read-committed) locker after \
         a steal must still be grantable; got {g2:?}"
    );
}

// =====================================================================
// LockPreemptionTest.testPreempted  (victim NOTIFICATION half — GAP)
// =====================================================================

/// JE: `LockPreemptionTest.testPreempted` (victim-notification half).
///
/// ENGINE-FIDELITY GAP (escalated in tp-je-rep-txn.md, NOT a data-loss bug):
/// after its lock is stolen, the reader's NEXT lock-taking operation must throw
/// `LockPreemptedException` and mark the transaction invalid
/// (`assertFalse(replicaTxn.isValid())`), so a reader whose snapshot was
/// invalidated by a replicated write is forced to abort rather than silently
/// continue with a broken read-stability guarantee.
///
/// Noxu's lock manager silently removes the victim from the lock's owners and
/// has no `LockPreempted` error variant nor a per-locker preempted flag. A
/// victim's next lock request therefore does not distinguish "your lock was
/// stolen" from an ordinary conflict:
///   * a NON-blocking re-read returns `LockNotAvailable` (an ordinary no-wait
///     conflict) — not a preemption signal;
///   * a BLOCKING re-read would WAIT for the stealer to release and then read
///     the NEW value, never learning its original snapshot was invalidated.
///
/// This is a read-stability / notification deviation, not data loss: the
/// master's write is always applied (see
/// `lock_preemption_master_write_wins_over_replica_reader`) and nothing is
/// lost or corrupted. Ignored until Noxu grows a `LockPreemptedException`
/// analog (a per-locker preempted flag set by `steal_lock` + checked on the
/// next lock request). Kept as the faithful assertion so the gap is not lost.
#[test]
fn lock_preemption_victim_is_notified_on_next_lock() {
    let lm = LockManager::new();

    // Reader holds a read lock; master steals it.
    lm.lock(LSN_KEY1, 1, LockType::Read, false, false).unwrap();
    lm.lock_importunate_with_timeout(LSN_KEY1, 2, LockType::Write, false, 200)
        .expect("steal");

    // JE: the reader's next lock-taking op throws LockPreemptedException. Noxu
    // now surfaces the equivalent `TxnError::LockPreempted` (set by the steal
    // via `LockManager::mark_preempted`, checked in the lock funnel), so the
    // victim learns its snapshot was invalidated instead of seeing an ordinary
    // `LockNotAvailable` no-wait conflict.
    let r = lm.lock(LSN_KEY1, 1, LockType::Read, true, false);
    match r {
        Err(TxnError::LockPreempted { lsn }) => {
            assert_eq!(
                lsn, LSN_KEY1,
                "preemption is reported for the stolen LSN"
            );
            let msg = format!("{}", TxnError::LockPreempted { lsn });
            assert!(
                msg.contains("preempt"),
                "the LockPreempted display must signal PREEMPTION (JE \
                 LockPreemptedException); got {msg}"
            );
        }
        Err(e) => panic!(
            "the preempted reader's next lock request must signal PREEMPTION \
             (TxnError::LockPreempted, JE LockPreemptedException), not an \
             ordinary conflict; got {e:?}"
        ),
        Ok(g) => panic!(
            "the preempted reader must NOT silently re-acquire; got {g:?}"
        ),
    }
}
