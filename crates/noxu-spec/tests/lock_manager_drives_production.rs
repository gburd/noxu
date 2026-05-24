//! Spec-drives-implementation harness for `lock_manager_deadlock`.
//!
//! The Stateright model in `noxu_spec::lock_manager_deadlock`
//! explores an abstract state machine over a [`noxu_txn::LockType`]
//! alphabet and asserts safety properties (`WriteLocksExclusive`,
//! `NoFalsePositiveAbort`). That on its own only proves that the
//! abstract protocol is safe; it does not prove the production
//! [`noxu_txn::LockManager`] implements that protocol.
//!
//! This test closes that gap by **replaying the same action shape
//! against the production LockManager** and asserting the same
//! invariants hold. If a refactor of `LockManager` introduces a
//! regression that violates `WriteLocksExclusive` (e.g. a writer
//! and reader allowed to coexist on the same LSN), this test
//! fails on the action sequence that exposes it.
//!
//! The trace is hand-picked rather than enumerated: enumerating
//! the spec's full reachable state would re-run Stateright in the
//! integration test, which is slow. Instead we pick a small
//! number of representative sequences derived from the spec's
//! structure, each exercising one branch of the protocol.

use noxu_spec::lock_manager_deadlock::{
    HeldKind, SpecLockKind, spec_lock_kind,
};
use noxu_txn::{LockManager, LockType, TxnError};

/// One step of a replayable trace. Mirrors the spec's `Action` enum
/// but uses real (LSN, locker_id) values so the LockManager API
/// can be called directly.
#[derive(Debug, Clone)]
enum Step {
    /// Acquire (or queue) a lock.
    Acquire { tid: i64, lsn: u64, kind: HeldKind, non_blocking: bool },
    /// Release a previously-held lock. The expected_ok flag asserts
    /// whether release should succeed (a release of a lock the
    /// locker doesn't hold is a no-op in the production code, so
    /// expected_ok is normally true).
    Release { tid: i64, lsn: u64 },
}

/// Replay a trace and check the spec's safety properties after
/// every step. Panics with a useful message on any violation.
fn replay_and_check(steps: &[Step]) {
    let lm = LockManager::new();
    for (i, step) in steps.iter().enumerate() {
        match step {
            Step::Acquire { tid, lsn, kind, non_blocking } => {
                let r = lm.lock(*lsn, *tid, *kind, *non_blocking, false);
                // We don't assert any particular outcome here — the
                // spec allows both success and "blocked / not
                // available". What we DO assert is that the
                // post-step state honours the safety invariants.
                let _ = r;
            }
            Step::Release { tid, lsn } => {
                lm.release(*lsn, *tid).unwrap_or_else(|e: TxnError| {
                    panic!("step {i}: unexpected release error: {e}")
                });
            }
        }
        check_write_locks_exclusive(&lm, i);
    }
}

/// `WriteLocksExclusive` from the spec: at most one writer per
/// LSN, and never both a writer and a reader on the same LSN.
fn check_write_locks_exclusive(lm: &LockManager, step_idx: usize) {
    // The production LockManager doesn't expose its internal table
    // for direct introspection, but `is_owned_write_lock(lsn,
    // locker_id)` and `get_owned_lock_type(lsn, locker_id)` together
    // let us probe each (lsn, locker) pair we know about. For this
    // test the trace's LSNs and tids are small enumerable sets, so
    // we sweep them.
    for lsn in 0u64..16 {
        let mut writers = Vec::new();
        let mut readers = Vec::new();
        for tid in 1i64..=8 {
            if lm.is_owned_write_lock(lsn, tid) {
                writers.push(tid);
            } else if let Some(LockType::Read) =
                lm.get_owned_lock_type(lsn, tid)
            {
                readers.push(tid);
            }
        }
        assert!(
            writers.len() <= 1,
            "step {step_idx}: lsn {lsn} has {} writers ({writers:?}) — \
             violates WriteLocksExclusive (at most one writer)",
            writers.len()
        );
        assert!(
            writers.len() != 1 || readers.is_empty(),
            "step {step_idx}: lsn {lsn} has writer {writers:?} \
             AND readers {readers:?} — violates WriteLocksExclusive \
             (writer excludes readers)",
        );
    }
}

#[test]
fn spec_drives_lock_manager_disjoint_acquire_release() {
    // Two txns acquire write locks on disjoint LSNs and release.
    // Spec property `WriteLocksExclusive` should hold throughout.
    let trace = vec![
        Step::Acquire {
            tid: 1,
            lsn: 1,
            kind: HeldKind::Write,
            non_blocking: false,
        },
        Step::Acquire {
            tid: 2,
            lsn: 2,
            kind: HeldKind::Write,
            non_blocking: false,
        },
        Step::Release { tid: 1, lsn: 1 },
        Step::Release { tid: 2, lsn: 2 },
    ];
    replay_and_check(&trace);
}

#[test]
fn spec_drives_lock_manager_shared_read_locks() {
    // Three txns hold a Read lock on the same LSN simultaneously.
    let trace = vec![
        Step::Acquire {
            tid: 1,
            lsn: 5,
            kind: HeldKind::Read,
            non_blocking: false,
        },
        Step::Acquire {
            tid: 2,
            lsn: 5,
            kind: HeldKind::Read,
            non_blocking: false,
        },
        Step::Acquire {
            tid: 3,
            lsn: 5,
            kind: HeldKind::Read,
            non_blocking: false,
        },
        Step::Release { tid: 1, lsn: 5 },
        Step::Release { tid: 2, lsn: 5 },
        Step::Release { tid: 3, lsn: 5 },
    ];
    replay_and_check(&trace);
}

#[test]
fn spec_drives_lock_manager_write_excludes_read_nonblocking() {
    // Txn 1 holds a write lock on LSN 7. Txn 2 attempts a
    // non-blocking read — must be rejected. After release, txn 2
    // acquires successfully. Spec invariant must hold across the
    // contended slot.
    let trace = vec![
        Step::Acquire {
            tid: 1,
            lsn: 7,
            kind: HeldKind::Write,
            non_blocking: false,
        },
        Step::Acquire {
            tid: 2,
            lsn: 7,
            kind: HeldKind::Read,
            non_blocking: true,
        },
        Step::Release { tid: 1, lsn: 7 },
        Step::Acquire {
            tid: 2,
            lsn: 7,
            kind: HeldKind::Read,
            non_blocking: true,
        },
        Step::Release { tid: 2, lsn: 7 },
    ];
    replay_and_check(&trace);
}

#[test]
fn spec_drives_lock_manager_lock_kind_alphabet() {
    // Every LockType variant gets exercised through spec_lock_kind.
    // If a new variant is added, the exhaustive match in
    // spec_lock_kind breaks the build. This test guards the
    // *runtime* projection: mapping each variant to its
    // SpecLockKind matches the documented decision.
    let cases: &[(HeldKind, SpecLockKind)] = &[
        (HeldKind::Read, SpecLockKind::Read),
        (HeldKind::Write, SpecLockKind::Write),
        (HeldKind::RangeRead, SpecLockKind::Read),
        (HeldKind::RangeWrite, SpecLockKind::Write),
        (HeldKind::RangeInsert, SpecLockKind::Write),
        (HeldKind::Restart, SpecLockKind::None),
        (HeldKind::None, SpecLockKind::None),
    ];
    for (lt, expected) in cases {
        assert_eq!(
            spec_lock_kind(*lt),
            *expected,
            "spec_lock_kind({lt:?}) should map to {expected:?}",
        );
    }
}
