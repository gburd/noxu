#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Transaction management and locking for Noxu DB.
//!
//! transaction management and
//! record-level locking with deadlock detection.
//!
//! ## Phase 3: Lock Type System Foundation (Implemented)
//!
//! - Lock types, conflict and upgrade matrices
//! - Lock grant types and results
//! - Write lock info for undo operations
//! - Lock statistics
//! - Locker hierarchy (BasicLocker, ThreadLocker, HandleLocker)

// Error types
mod error;
pub use error::{TxnError, TxnResult};

// Lock types and conflict/upgrade matrices
mod lock_type;
pub use lock_type::LockType;

mod lock_conflict;
pub use lock_conflict::LockConflict;

mod lock_upgrade;
pub use lock_upgrade::LockUpgrade;

mod lock_grant_type;
pub use lock_grant_type::LockGrantType;

mod lock_info;
pub use lock_info::LockInfo;

mod write_lock_info;
pub use write_lock_info::WriteLockInfo;

mod lock_attempt_result;
pub use lock_attempt_result::LockAttemptResult;

mod lock_result;
pub use lock_result::LockResult;

mod lock_stat;
pub use lock_stat::LockStats;

// Lock implementations (Agent 2)
mod lock_impl;
pub use lock_impl::LockImpl;

mod thin_lock_impl;
pub use thin_lock_impl::ThinLockImpl;

mod lock;
pub use lock::Lock;

// Lock manager (Agent 3)
mod lock_manager;
pub use lock_manager::LockManager;

mod dummy_lock_manager;
pub use dummy_lock_manager::DummyLockManager;

mod deadlock_detector;
pub use deadlock_detector::DeadlockDetector;

// Locker hierarchy (Agent 3)
pub mod locker;
pub use locker::Locker;

pub mod basic_locker;
pub use basic_locker::BasicLocker;

pub mod thread_locker;
pub use thread_locker::ThreadLocker;

pub mod handle_locker;
pub use handle_locker::HandleLocker;

pub mod locker_factory;
pub use locker_factory::LockerFactory;

// Transaction state and lifecycle
mod txn_state;
pub use txn_state::TxnState;

mod txn_end;
pub use txn_end::TxnEnd;

mod txn_commit;
pub use txn_commit::TxnCommit;

mod txn_abort;
pub use txn_abort::TxnAbort;

mod txn;
pub use txn::{Durability, Txn, UndoRecord};

pub mod group_commit;
pub use group_commit::{GroupCommit, GroupCommitMaster, GroupCommitReplica};

mod txn_manager;
pub use txn_manager::{NULL_TXN_ID, TxnManager, TxnStats};

mod txn_chain;
pub use txn_chain::{CompareSlot, RevertInfo, TxnChain};
