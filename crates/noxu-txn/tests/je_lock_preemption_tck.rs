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
//! `LockPreemptedException`, that its snapshot was invalidated — is now
//! implemented (NEW-LOCK-PREEMPT-EXN). The importunate steal marks each
//! preempted victim (`LockManager::mark_preempted`, the analog of JE
//! `Locker.setPreempted`), and the victim's next lock-taking request observes
//! the flag and gets `TxnError::LockPreempted` (JE `LockPreemptedException`)
//! rather than a generic `LockNotAvailable`. This was previously an
//! `#[ignore]`d faithful port + escalated fidelity gap; it is now LIVE. It was
//! never a data-loss/corruption issue (no write is lost, the master always
//! wins); it was purely a read-stability notification gap.

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
/// The JE invariant across this family, split into its two distinct causes:
///
///   1. A locker that WAS preempted (held its lock through the steal) but then
///      takes NO new lock — it commits or aborts — completes WITHOUT a
///      `LockPreemptedException`, because commit/abort acquire no lock and JE
///      only calls `checkPreempted` after a successful lock GRANT
///      (Locker.java:509). This is asserted below via `release` after a steal.
///
///   2. `testNotPreemptedMove{ReadCommitted,NonTransactional}Cursor` complete
///      WITHOUT `LockPreemptedException` NOT because moving after preemption is
///      allowed, but because a READ_COMMITTED / non-transactional cursor
///      RELEASES its read lock immediately after reading — so at steal time it
///      holds nothing, the steal never marks it preempted
///      (`setPreempted` is not called), and its subsequent move to another key
///      is an ordinary, never-preempted lock. This is asserted below with a
///      separate locker (locker 3) that released before the steal.
///
/// (A locker that DID hold its lock through the steal is preempted and its next
/// NEW lock on ANY key throws — that is
/// `lock_preemption_victim_is_notified_on_next_lock`.) In Noxu, releasing a
/// lock the locker no longer owns is a harmless no-op and takes no new lock, so
/// the commit/abort path never errors.
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
/// NEW-LOCK-PREEMPT-EXN (fidelity fix, NOT a data-loss bug): after its lock is
/// stolen, the reader's NEXT lock-taking operation must throw
/// `LockPreemptedException` and mark the transaction invalid
/// (`assertFalse(replicaTxn.isValid())`, LockPreemptionTest.java:176), so a
/// reader whose snapshot was invalidated by a replicated write is forced to
/// abort rather than silently continue with a broken read-stability guarantee.
///
/// JE tracks preemption per-`Locker`, not per-lock: `Locker.setPreempted` sets
/// `preemptedCause` (Locker.java:96,319), and `Locker.lock(lsn, ...)` calls
/// `checkPreempted` after EVERY successful grant on ANY lsn (Locker.java:509),
/// which `throwIfPreempted` turns into a `LockPreemptedException` whenever
/// `preemptedCause != null` (Locker.java:359-364) — i.e. a locker preempted on
/// KEY1 gets the exception on its next new lock even for a DIFFERENT key. The
/// no-wait DENIED path (`nonBlockingLock`) does NOT call `checkPreempted`
/// (Locker.java:539-542), so an op that takes no lock never spuriously throws.
///
/// Noxu now mirrors this with a lock-manager-level per-locker preempted flag
/// (`LockManager::mark_preempted`, set by the steal; checked in the single lock
/// funnel `lock_with_timeout_and_txn` behind a `preempted_nonempty` fast-path
/// atomic; cleared at the real txn-end path `Txn::release_all_locks` ->
/// `LockManager::clear_preempted`, on both commit and abort). The steal
/// MECHANISM (master write wins) is
/// unchanged — see `lock_preemption_master_write_wins_over_replica_reader`.
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
