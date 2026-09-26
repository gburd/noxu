//! C3/SC-5/V5: MemoryBudget's lock/txn categories must be fed by production
//! code so the over-budget view sees TOTAL memory, not just tree nodes.
//!
//! JE `dbi/MemoryBudget.java` keeps four live counters (treeMemoryUsage,
//! lockMemoryUsage, txnMemoryUsage, adminMemoryUsage), each updated by its
//! owning subsystem (LockManager on grant/release, Txn on create/close, the
//! tracker for admin), and the eviction arbiter reads their SUM
//! (`getCacheMemoryUsage()`). A lock/txn-heavy (not tree-heavy) workload
//! therefore triggers eviction in JE purely from lock/txn footprint.
//!
//! Before this fix the Noxu lock/txn categories were never incremented by
//! any production path (only the struct's own unit tests wrote them), so a
//! large lock table or many open transactions stayed invisible to the budget.
//! These tests fail on base 97ddac4f (categories stuck at 0) and pass once
//! the LockManager and TxnManager feed their shared counters.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_dbi::EnvironmentImpl;
use noxu_txn::LockType;
use tempfile::TempDir;

/// Holding many record locks in the live LockManager must show up in the
/// MemoryBudget's lock category and hence in total_usage().
#[test]
fn lock_table_footprint_counts_toward_memory_budget() {
    let dir = TempDir::new().unwrap();
    let env = EnvironmentImpl::new(dir.path(), false, true).expect("open env");

    let budget = env.get_memory_budget();
    let lock_before = budget.get_lock_memory_usage();
    let total_before = budget.total_usage();

    // Take a large number of distinct write locks — a "large lock table"
    // workload with essentially no tree growth.
    let lm = env.get_lock_manager();
    let locker_id = 42_i64;
    let n_locks = 2_000_u64;
    for lsn in 1..=n_locks {
        lm.lock(lsn, locker_id, LockType::Write, /*non_blocking=*/ true, false)
            .expect("non-blocking lock on a fresh lsn always granted");
    }

    let lock_after = budget.get_lock_memory_usage();
    let total_after = budget.total_usage();

    assert!(
        lock_after > lock_before,
        "lock-table memory must grow as locks are held: before={lock_before} \
         after={lock_after} (base 97ddac4f: stuck at 0 — no production feeder)"
    );
    assert!(
        total_after > total_before,
        "total budget must reflect lock-table footprint: before={total_before} \
         after={total_after}"
    );

    // Releasing every lock must return the lock category to its start.
    let released = lm.release_all_for_locker(locker_id);
    assert_eq!(released as u64, n_locks, "all locks released");
    assert_eq!(
        budget.get_lock_memory_usage(),
        lock_before,
        "lock memory must return to baseline after all locks released"
    );
}

/// Many open transactions must show up in the MemoryBudget's txn category.
///
/// Txn accounting is tied to TxnManager membership (`begin_*` inserts into
/// `all_txns`; `commit_txn`/`abort_txn` remove), mirroring JE where a `Txn`
/// costs `TXN_OVERHEAD` for its whole lifetime and is credited back when it
/// ends. A bare `Txn` value dropped without commit/abort intentionally stays
/// "active" (matching `n_active_txns`), so we drain via `abort_txn`.
#[test]
fn open_txn_footprint_counts_toward_memory_budget() {
    let dir = TempDir::new().unwrap();
    let env = EnvironmentImpl::new(dir.path(), false, true).expect("open env");

    let budget = env.get_memory_budget();
    let tm = env.get_txn_manager();
    let txn_before = budget.get_txn_memory_usage();

    // Open many transactions and keep them alive (small tree, no puts).
    let mut ids = Vec::new();
    let mut txns = Vec::new();
    for _ in 0..500 {
        let txn = env.begin_txn().expect("begin txn");
        ids.push(txn.id_as_locker());
        txns.push(txn);
    }

    let txn_after = budget.get_txn_memory_usage();
    assert!(
        txn_after > txn_before,
        "open-txn memory must grow as transactions are begun: before={txn_before} \
         after={txn_after} (base 97ddac4f: stuck at 0 — no production feeder)"
    );

    // Draining every txn via the manager must return the txn category to
    // baseline.
    for id in &ids {
        tm.abort_txn(*id);
    }
    drop(txns);
    let txn_final = budget.get_txn_memory_usage();
    assert_eq!(
        txn_final, txn_before,
        "txn memory must return to baseline once all txns end: \
         final={txn_final} baseline={txn_before}"
    );
}
