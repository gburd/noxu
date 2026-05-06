//! Transaction manager.
//!
//! Port of `com.sleepycat.je.txn.TxnManager`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};

use noxu_sync::RwLock;

use crate::group_commit::GroupCommit;
use crate::LockManager;
use crate::txn::Txn;

/// Manages all active transactions.
///
/// Port of `com.sleepycat.je.txn.TxnManager`.
pub struct TxnManager {
    /// All active transactions, keyed by txn ID.
    all_txns: RwLock<HashMap<i64, ()>>, // Just track IDs for now
    /// Next transaction ID generator.
    next_txn_id: AtomicI64,
    /// Lock manager shared by all transactions.
    lock_manager: Arc<LockManager>,
    /// Optional group-commit handler (Master or Replica).
    ///
    /// `None` in non-replicated environments — fsyncs go directly through
    /// `FSyncManager`.  When set, committing transactions call
    /// `group_commit.buffer_commit()` after writing their WAL entry.
    ///
    /// Port of `TxnManager.groupCommit: AtomicReference<GroupCommit>` (NoSQL fork).
    group_commit: StdRwLock<Option<Arc<dyn GroupCommit>>>,
    /// Statistics.
    n_begins: AtomicU64,
    n_commits: AtomicU64,
    n_aborts: AtomicU64,
}

impl TxnManager {
    /// Creates a new TxnManager.
    pub fn new(lock_manager: Arc<LockManager>) -> Self {
        TxnManager {
            all_txns: RwLock::new(HashMap::new()),
            next_txn_id: AtomicI64::new(1),
            lock_manager,
            group_commit: StdRwLock::new(None),
            n_begins: AtomicU64::new(0),
            n_commits: AtomicU64::new(0),
            n_aborts: AtomicU64::new(0),
        }
    }

    /// Begins a new transaction.
    pub fn begin_txn(&self) -> Txn {
        let id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        self.n_begins.fetch_add(1, Ordering::Relaxed);
        self.all_txns.write().insert(id, ());
        Txn::new(id, self.lock_manager.clone())
    }

    /// Records that a transaction has committed.
    pub fn commit_txn(&self, txn_id: i64) {
        self.all_txns.write().remove(&txn_id);
        self.n_commits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that a transaction has aborted.
    pub fn abort_txn(&self, txn_id: i64) {
        self.all_txns.write().remove(&txn_id);
        self.n_aborts.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the number of currently active transactions.
    pub fn n_active_txns(&self) -> usize {
        self.all_txns.read().len()
    }

    /// Returns transaction statistics.
    pub fn get_stats(&self) -> TxnStats {
        TxnStats {
            n_begins: self.n_begins.load(Ordering::Relaxed),
            n_commits: self.n_commits.load(Ordering::Relaxed),
            n_aborts: self.n_aborts.load(Ordering::Relaxed),
            n_active: self.n_active_txns() as u64,
        }
    }

    /// Returns a reference to the lock manager.
    pub fn lock_manager(&self) -> &Arc<LockManager> {
        &self.lock_manager
    }

    // ========================================================================
    // GroupCommit  —  NoSQL JE fork
    // ========================================================================

    /// Returns the current group-commit handler, if any.
    ///
    /// Port of `TxnManager.getGroupCommit()` (NoSQL fork).
    pub fn get_group_commit(&self) -> Option<Arc<dyn GroupCommit>> {
        self.group_commit.read().unwrap().clone()
    }

    /// Installs the group-commit handler for the **Master** role.
    ///
    /// Called when this node transitions to Master in a replicated
    /// environment.  Creates a [`crate::group_commit::GroupCommitMaster`]
    /// with default configuration and stores it.
    ///
    /// Port of `TxnManager.setupGroupCommitMaster()` (NoSQL fork).
    pub fn setup_group_commit_master(&self) {
        use crate::group_commit::GroupCommitMaster;
        let gc = Arc::new(GroupCommitMaster::default());
        *self.group_commit.write().unwrap() = Some(gc);
    }

    /// Installs the group-commit handler for the **Replica** role.
    ///
    /// Called when this node is operating as a Replica.
    ///
    /// Port of `TxnManager.setupGroupCommitReplica(Replay)` (NoSQL fork).
    pub fn setup_group_commit_replica(&self) {
        use crate::group_commit::GroupCommitReplica;
        let gc = Arc::new(GroupCommitReplica::default());
        *self.group_commit.write().unwrap() = Some(gc);
    }

    /// Clears the group-commit handler.
    ///
    /// Called on role transitions or shutdown.
    pub fn clear_group_commit(&self) {
        *self.group_commit.write().unwrap() = None;
    }
}

/// Transaction statistics.
#[derive(Debug, Clone, Default)]
pub struct TxnStats {
    pub n_begins: u64,
    pub n_commits: u64,
    pub n_aborts: u64,
    pub n_active: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Locker;

    fn create_test_manager() -> TxnManager {
        let lock_manager = Arc::new(LockManager::new());
        TxnManager::new(lock_manager)
    }

    #[test]
    fn test_begin_txn_generates_unique_ids() {
        let manager = create_test_manager();

        let txn1 = manager.begin_txn();
        let txn2 = manager.begin_txn();
        let txn3 = manager.begin_txn();

        assert_ne!(txn1.id(), txn2.id());
        assert_ne!(txn2.id(), txn3.id());
        assert_ne!(txn1.id(), txn3.id());
    }

    #[test]
    fn test_commit_txn_removes_from_active() {
        let manager = create_test_manager();

        let mut txn = manager.begin_txn();
        let txn_id = txn.id();
        assert_eq!(manager.n_active_txns(), 1);

        txn.commit().unwrap();
        manager.commit_txn(txn_id);
        assert_eq!(manager.n_active_txns(), 0);
    }

    #[test]
    fn test_abort_txn_removes_from_active() {
        let manager = create_test_manager();

        let mut txn = manager.begin_txn();
        let txn_id = txn.id();
        assert_eq!(manager.n_active_txns(), 1);

        txn.abort().unwrap();
        manager.abort_txn(txn_id);
        assert_eq!(manager.n_active_txns(), 0);
    }

    #[test]
    fn test_statistics_tracking() {
        let manager = create_test_manager();

        let stats = manager.get_stats();
        assert_eq!(stats.n_begins, 0);
        assert_eq!(stats.n_commits, 0);
        assert_eq!(stats.n_aborts, 0);
        assert_eq!(stats.n_active, 0);

        let mut txn1 = manager.begin_txn();
        let mut txn2 = manager.begin_txn();
        let txn1_id = txn1.id();
        let txn2_id = txn2.id();

        let stats = manager.get_stats();
        assert_eq!(stats.n_begins, 2);
        assert_eq!(stats.n_active, 2);

        txn1.commit().unwrap();
        manager.commit_txn(txn1_id);

        let stats = manager.get_stats();
        assert_eq!(stats.n_commits, 1);
        assert_eq!(stats.n_active, 1);

        txn2.abort().unwrap();
        manager.abort_txn(txn2_id);

        let stats = manager.get_stats();
        assert_eq!(stats.n_aborts, 1);
        assert_eq!(stats.n_active, 0);
    }

    #[test]
    fn test_n_active_txns() {
        let manager = create_test_manager();

        assert_eq!(manager.n_active_txns(), 0);

        let txn1 = manager.begin_txn();
        assert_eq!(manager.n_active_txns(), 1);

        let txn2 = manager.begin_txn();
        assert_eq!(manager.n_active_txns(), 2);

        let txn3 = manager.begin_txn();
        assert_eq!(manager.n_active_txns(), 3);

        manager.commit_txn(txn1.id());
        assert_eq!(manager.n_active_txns(), 2);

        manager.abort_txn(txn2.id());
        assert_eq!(manager.n_active_txns(), 1);

        manager.commit_txn(txn3.id());
        assert_eq!(manager.n_active_txns(), 0);
    }

    #[test]
    fn test_lock_manager_reference() {
        let lock_manager = Arc::new(LockManager::new());
        let manager = TxnManager::new(lock_manager.clone());

        let lm_ref = manager.lock_manager();
        assert!(Arc::ptr_eq(lm_ref, &lock_manager));
    }
}
