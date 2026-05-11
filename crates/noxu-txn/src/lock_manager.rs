//! Lock manager for Noxu DB.
//!
//!
//! The LockManager is the central authority for all lock operations in the
//! system. It manages N sharded lock tables, each protected by its own mutex,
//! to allow concurrent lock operations on different LSNs.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use noxu_sync::{Condvar, Mutex};

use crate::{DeadlockDetector, Lock, LockGrantType, LockStats, LockType, TxnError};
use crate::lock_info::WaiterNotify;
use std::sync::atomic::{AtomicU64, Ordering};

/// Number of lock table shards.
///
/// Multiple lock tables reduce contention by allowing concurrent operations
/// on locks in different tables.  64 shards provide good distribution across
/// multi-core systems under high concurrency (16+ threads).  The hash
/// function spreads LSNs uniformly so collision probability is low.
const N_LOCK_TABLES: usize = 64;

/// The LockManager manages all locks in the system.
///
/// Locks are sharded across N_LOCK_TABLES tables, each protected by its own
/// mutex.  This allows concurrent lock operations on different LSNs.
///
/// # Architecture
///
/// - Each lock is identified by an LSN (packed u64)
/// - Locks are hashed to one of N lock tables
/// - Each table has its own mutex for fine-grained locking
/// - Lock objects start as Thin locks and mutate to Full locks when needed
///
/// # Blocking / waiting
///
/// When `lock()` cannot grant immediately it:
/// 1. Registers the calling thread as a waiter (inside the shard mutex) and
///    attaches a per-waiter `Arc<(Mutex<bool>, Condvar)>` notify pair.
/// 2. Checks for deadlocks using the `DeadlockDetector` before sleeping.
/// 3. Releases the shard mutex and waits on the condvar for up to
///    `lock_timeout_ms` milliseconds.
/// 4. On wakeup re-acquires the shard mutex and checks ownership.
/// 5. On timeout removes itself from the waiter list and returns
///    `TxnError::LockTimeout`.
///
/// This mirrors the flow in `LockManager.lock()` / `waitForLock()`.
///
/// 
pub struct LockManager {
    /// Sharded lock tables, keyed by LSN.
    lock_tables: Vec<Mutex<HashMap<u64, Lock>>>,

    /// Statistics tracking.
    stats: LockManagerStats,

    /// Default lock-wait timeout in milliseconds.
    ///
    /// 0 means wait forever (`EnvironmentConfig.setLockTimeout(0)`).
    /// Configured at open time from `EnvironmentConfig`; can be overridden
    /// per-call via `lock_with_timeout()`.
    ///
    /// 
    lock_timeout_ms: AtomicU64,

    /// Locker sharing registry: maps locker_id → share_group_id.
    ///
    /// ThreadLockers register their thread_id (as i64) as the group_id.
    /// HandleLockers with a buddy register the buddy's ID as the group_id.
    /// Two lockers are in the same sharing group iff they map to the same
    /// group_id, and thus bypass lock-conflict detection (JE
    /// `Locker.sharesLocksWith(other)`).
    ///
    /// (thread-locker map), extended
    /// to support HandleLocker buddy sharing.
    share_registry: RwLock<HashMap<i64, i64>>,

    /// Incremental waits-for graph for O(1) deadlock detection.
    ///
    /// Maps waiting_locker_id → [owner_locker_ids it is blocked by].
    /// Inserted O(1) when a locker enters the wait path; removed when it
    /// exits (grant, timeout, or deadlock abort).
    ///
    /// `check_deadlock_for_waiter` reads from this small graph instead of
    /// rescanning all N_LOCK_TABLES shards — eliminates the O(64) full scan
    /// that stalls all threads under high contention.
    waiter_graph: Mutex<HashMap<i64, Vec<i64>>>,
}

/// Internal statistics tracking.
struct LockManagerStats {
    /// Total number of lock requests.
    lock_requests: AtomicU64,

    /// Total number of lock waits (blocked requests).
    lock_waits: AtomicU64,

    /// Total number of lock acquisitions that timed out.
    lock_timeouts: AtomicU64,
}

impl LockManager {
    /// Creates a new LockManager with N_LOCK_TABLES shards and the default
    /// lock timeout of 500 ms (matching default).
    pub fn new() -> Self {
        Self::with_lock_timeout(500)
    }

    /// Creates a new LockManager with a specific default lock timeout.
    ///
    /// `timeout_ms == 0` means wait forever (`setLockTimeout(0, MILLISECONDS)`).
    ///
    /// Call this from `EnvironmentImpl` after reading `EnvironmentConfig.lock_timeout_ms`.
    pub fn with_lock_timeout(timeout_ms: u64) -> Self {
        let mut lock_tables = Vec::with_capacity(N_LOCK_TABLES);
        for _ in 0..N_LOCK_TABLES {
            lock_tables.push(Mutex::new(HashMap::new()));
        }

        Self {
            lock_tables,
            stats: LockManagerStats {
                lock_requests: AtomicU64::new(0),
                lock_waits: AtomicU64::new(0),
                lock_timeouts: AtomicU64::new(0),
            },
            lock_timeout_ms: AtomicU64::new(timeout_ms),
            share_registry: RwLock::new(HashMap::new()),
            waiter_graph: Mutex::new(HashMap::new()),
        }
    }

    /// Updates the default lock timeout.
    ///
    /// Thread-safe; takes effect for subsequent `lock()` calls.
    /// 
    pub fn set_lock_timeout(&self, timeout_ms: u64) {
        self.lock_timeout_ms.store(timeout_ms, Ordering::Relaxed);
    }

    /// Returns the current default lock timeout in milliseconds.
    pub fn get_lock_timeout_ms(&self) -> u64 {
        self.lock_timeout_ms.load(Ordering::Relaxed)
    }

    /// Acquires a lock on the given LSN for the given locker, blocking the
    /// calling thread if necessary.
    ///
    /// # Arguments
    ///
    /// * `lsn` - The LSN to lock (packed u64)
    /// * `locker_id` - The ID of the requesting locker
    /// * `lock_type` - The type of lock requested
    /// * `non_blocking` - If true, return `LockNotAvailable` instead of waiting
    /// * `jump_ahead_of_waiters` - If true, skip ahead of existing waiters
    /// * `lock_timeout_ms` - How long to wait; 0 = wait forever
    ///
    /// # Returns
    ///
    /// The `LockGrantType` on success:
    /// - `New` / `Promotion` / `Existing` — lock held immediately
    /// - `NoneNeeded` — `lock_type` was `None`
    ///
    /// # Errors
    ///
    /// - `TxnError::RangeRestart` if `lock_type` is `Restart`
    /// - `TxnError::LockNotAvailable` if `non_blocking` and lock unavailable
    /// - `TxnError::LockTimeout` if the timeout expired while waiting
    /// - `TxnError::Deadlock` if a wait-for cycle is detected before waiting
    ///
    /// 
    pub fn lock(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
    ) -> Result<LockGrantType, TxnError> {
        self.lock_with_timeout(
            lsn,
            locker_id,
            lock_type,
            non_blocking,
            jump_ahead_of_waiters,
            self.lock_timeout_ms.load(Ordering::Relaxed),
        )
    }

    /// Like `lock()` but the caller supplies the timeout in milliseconds.
    /// `timeout_ms == 0` means wait forever.
    ///
    /// 
    pub fn lock_with_timeout(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
        timeout_ms: u64,
    ) -> Result<LockGrantType, TxnError> {
        // No lock needed for dirty-read, return immediately.
        if lock_type == LockType::None {
            return Ok(LockGrantType::NoneNeeded);
        }

        // Special restart lock type throws immediately.
        if lock_type == LockType::Restart {
            return Err(TxnError::RangeRestart);
        }

        // Track statistics.
        self.stats.lock_requests.fetch_add(1, Ordering::Relaxed);

        let table_idx = self.get_table_index(lsn);

        // --- Phase 1: attempt to acquire the lock under the shard mutex. ---
        //
        // "Attempt to lock without any initial wait."
        let (initial_grant, owner_ids, notify_pair) = {
            let mut table = self.lock_tables[table_idx].lock();
            let lock = table.entry(lsn).or_insert_with(Lock::new_thin);

            let result = lock.lock(
                lock_type,
                locker_id,
                non_blocking,
                jump_ahead_of_waiters,
            );

            if result.success {
                // Granted immediately; no waiting needed.
                return Ok(result.lock_grant);
            }

            if result.lock_grant == LockGrantType::Denied {
                // Non-blocking request was denied.
                return Err(TxnError::LockNotAvailable { lsn });
            }

            // We must wait.  Collect owner IDs for deadlock detection and
            // attach a per-waiter notify pair to our waiter entry.
            //
            // "locker.setWaitingFor(lsn, type)" then deadlock detect then
            //     "locker.wait(timeout)".
            self.stats.lock_waits.fetch_add(1, Ordering::Relaxed);

            let owner_ids = lock.get_owner_ids();

            // Build the notify pair and attach it to our waiter entry so the
            // releasing thread can wake us.
            let pair: WaiterNotify =
                Arc::new((Mutex::new(false), Condvar::new()));
            lock.set_waiter_notify(locker_id, pair.clone());

            (result.lock_grant, owner_ids, pair)
        };
        // Shard mutex is released here.

        // Register in the incremental waits-for graph so deadlock detection
        // can find this edge without rescanning all lock-table shards.
        self.record_wait(locker_id, &owner_ids);

        // --- Phase 2: deadlock detection before sleeping. ---
        //
        // Runs DeadlockChecker after setWaitingFor.  If the current
        // locker is selected as the victim, throw DeadlockException.
        //
        // We build a lightweight waits-for snapshot from the current lock
        // table state and check for a cycle.  If this locker is the victim
        // OR if the cycle cannot be broken without aborting this locker
        // (i.e. the victim is not reachable / not waiting), we abort.
        //
        // Note: a single-pass snapshot may be incomplete when both threads
        // are entering the wait path simultaneously.  We therefore also
        // perform a deadlock check after each spurious wakeup inside the
        // wait loop (Phase 3).
        if let Some(deadlock_err) = self.check_deadlock_for_waiter(
            lsn, locker_id, lock_type, &owner_ids,
        ) {
            // We are the chosen victim.  Flush from waiter list and throw.
            self.clear_wait(locker_id);
            let mut table = self.lock_tables[table_idx].lock();
            if let Some(lock) = table.get_mut(&lsn) {
                lock.flush_waiter(locker_id);
                if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                    table.remove(&lsn);
                }
            }
            return Err(deadlock_err);
        }

        // --- Phase 3: wait on the condvar. ---
        //
        // "locker.wait(timeRemaining)" in a loop, checking ownership on
        //     each wakeup.  We also re-run deadlock detection on each
        //     iteration so that cycles formed after we enter the wait path
        //     are caught.
        let start = std::time::Instant::now();
        let (mutex, condvar) = &*notify_pair;
        let mut granted_guard = mutex.lock();

        loop {
            if *granted_guard {
                // We were woken by the releasing thread which set our flag and
                // called notify_all.  Ownership was already transferred to us
                // inside release() -> try_lock().
                break;
            }

            // Compute remaining time.
            let remaining_ms = if timeout_ms == 0 {
                0 // 0 means wait forever
            } else {
                let elapsed = start.elapsed().as_millis() as u64;
                if elapsed >= timeout_ms {
                    // Already timed out before we even slept this iteration.
                    drop(granted_guard);
                    self.clear_wait(locker_id);
                    let mut table = self.lock_tables[table_idx].lock();
                    if let Some(lock) = table.get_mut(&lsn) {
                        lock.flush_waiter(locker_id);
                        if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                            table.remove(&lsn);
                        }
                    }
                    self.stats.lock_timeouts.fetch_add(1, Ordering::Relaxed);
                    return Err(TxnError::LockTimeout {
                        timeout_ms,
                        lsn,
                        owner: format!("owners of LSN {}", lsn),
                        requested_type: lock_type,
                        requester: locker_id.to_string(),
                    });
                }
                timeout_ms - elapsed
            };

            // Use a short slice (up to 50 ms) so we can re-check for
            // deadlocks that may form after we entered the wait path.
            // uses a "deadlock detection delay" for the same purpose.
            let slice_ms = if remaining_ms == 0 {
                50
            } else {
                remaining_ms.min(50)
            };

            let timed_out = condvar
                .wait_for(
                    &mut granted_guard,
                    Duration::from_millis(slice_ms),
                )
                .timed_out();

            if *granted_guard {
                // Granted while we were sleeping.
                break;
            }

            // Re-run deadlock detection after each wakeup / slice expiry.
            // This catches deadlocks that formed after our initial check.
            drop(granted_guard);
            {
                let cur_owner_ids = {
                    let table = self.lock_tables[table_idx].lock();
                    table.get(&lsn).map(|l| l.get_owner_ids()).unwrap_or_default()
                };
                if let Some(deadlock_err) = self.check_deadlock_for_waiter(
                    lsn, locker_id, lock_type, &cur_owner_ids,
                ) {
                    self.clear_wait(locker_id);
                    let mut table = self.lock_tables[table_idx].lock();
                    if let Some(lock) = table.get_mut(&lsn) {
                        lock.flush_waiter(locker_id);
                        if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                            table.remove(&lsn);
                        }
                    }
                    return Err(deadlock_err);
                }
            }
            granted_guard = mutex.lock();

            if *granted_guard {
                break;
            }

            if timed_out {
                // Check if total time is exceeded.
                if timeout_ms > 0
                    && start.elapsed().as_millis() as u64 >= timeout_ms
                {
                    drop(granted_guard);
                    self.clear_wait(locker_id);
                    let mut table = self.lock_tables[table_idx].lock();
                    if let Some(lock) = table.get_mut(&lsn) {
                        lock.flush_waiter(locker_id);
                        if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                            table.remove(&lsn);
                        }
                    }
                    self.stats.lock_timeouts.fetch_add(1, Ordering::Relaxed);
                    return Err(TxnError::LockTimeout {
                        timeout_ms,
                        lsn,
                        owner: format!("owners of LSN {}", lsn),
                        requested_type: lock_type,
                        requester: locker_id.to_string(),
                    });
                }
            }

            // Spurious wakeup or slice expired without timeout; loop.
        }

        drop(granted_guard);
        self.clear_wait(locker_id);

        // Determine which grant type to report.  On wakeup the lock type we
        // actually hold is exactly what we requested (or a promotion of it).
        // Reconstruct the grant type from context.
        let grant = match initial_grant {
            LockGrantType::WaitNew => LockGrantType::New,
            LockGrantType::WaitPromotion => LockGrantType::Promotion,
            LockGrantType::WaitRestart => LockGrantType::New,
            other => other,
        };

        Ok(grant)
    }

    /// Releases a lock on the given LSN for the given locker.
    ///
    /// Promotes compatible waiters to owners, signals their condvars so they
    /// wake up, and removes the lock entry when it becomes empty.
    ///
    /// 
    pub fn release(&self, lsn: u64, locker_id: i64) -> Result<(), TxnError> {
        let table_idx = self.get_table_index(lsn);
        let mut table = self.lock_tables[table_idx].lock();

        if let Some(lock) = table.get_mut(&lsn) {
            // release() moves eligible waiters to owners and signals each
            // granted waiter's condvar inside LockImpl::release().
            let _notify_ids = lock.release(locker_id);

            // If the lock has no owners and no waiters, remove it from the
            // table to free memory.
            if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                table.remove(&lsn);
            }
        }

        Ok(())
    }

    /// Downgrades a write lock to a read lock.
    ///
    /// 
    pub fn demote(&self, lsn: u64, locker_id: i64) -> Result<(), TxnError> {
        let table_idx = self.get_table_index(lsn);
        let mut table = self.lock_tables[table_idx].lock();

        if let Some(lock) = table.get_mut(&lsn) {
            lock.demote(locker_id);
        }

        Ok(())
    }

    /// Steals a lock for the given locker.
    ///
    /// Used by the HA replayer to forcibly acquire locks, removing all other
    /// preemptable owners.
    ///
    /// 
    pub fn steal_lock(&self, lsn: u64, locker_id: i64) -> Result<(), TxnError> {
        let table_idx = self.get_table_index(lsn);
        let mut table = self.lock_tables[table_idx].lock();

        let lock = table.entry(lsn).or_insert_with(Lock::new_thin);
        let _preempted = lock.steal_lock(locker_id);

        Ok(())
    }

    /// Returns true if the given locker owns a write lock on the LSN.
    ///
    /// 
    pub fn is_owned_write_lock(&self, lsn: u64, locker_id: i64) -> bool {
        let table_idx = self.get_table_index(lsn);
        let table = self.lock_tables[table_idx].lock();

        if let Some(lock) = table.get(&lsn) {
            lock.is_owned_write_lock(locker_id)
        } else {
            false
        }
    }

    /// Returns the lock type owned by the locker, or None.
    ///
    /// 
    pub fn get_owned_lock_type(
        &self,
        lsn: u64,
        locker_id: i64,
    ) -> Option<LockType> {
        let table_idx = self.get_table_index(lsn);
        let table = self.lock_tables[table_idx].lock();

        if let Some(lock) = table.get(&lsn) {
            lock.get_owned_lock_type(locker_id)
        } else {
            None
        }
    }

    /// Returns the owner count and waiter count for a given LSN.
    pub fn get_lock_info(&self, lsn: u64) -> (usize, usize) {
        let table_idx = self.get_table_index(lsn);
        let table = self.lock_tables[table_idx].lock();

        if let Some(lock) = table.get(&lsn) {
            (lock.n_owners(), lock.n_waiters())
        } else {
            (0, 0)
        }
    }

    /// Returns current lock statistics.
    ///
    /// 
    pub fn get_stats(&self) -> LockStats {
        LockStats {
            lock_requests: self.stats.lock_requests.load(Ordering::Relaxed),
            lock_waits: self.stats.lock_waits.load(Ordering::Relaxed),
            n_owners: self.n_total_locks() as u64,
            n_waiters: 0,
            n_total_locks: self.n_total_locks() as u64,
            n_read_locks: 0,
            n_write_locks: 0,
            n_lock_timeouts: self.stats.lock_timeouts.load(Ordering::Relaxed),
        }
    }

    /// Returns the number of lock entries across all tables.
    pub fn n_total_locks(&self) -> usize {
        let mut total = 0;
        for table in &self.lock_tables {
            total += table.lock().len();
        }
        total
    }

    // ========================================================================
    // Lock-sharing registry — `LockManager.threadLockers` analogue
    // ========================================================================

    /// Registers a locker in the sharing registry with the given group ID.
    ///
    /// All lockers sharing the same `group_id` bypass conflict detection with
    /// each other (`Locker.sharesLocksWith(other)`).
    ///
    /// Called by `ThreadLocker::new()` (group = thread_id) and by
    /// `HandleLocker::with_buddy()` (group = buddy_locker_id).
    ///
    /// 
    pub fn register_locker_sharing(&self, locker_id: i64, group_id: i64) {
        self.share_registry
            .write()
            .unwrap()
            .insert(locker_id, group_id);
    }

    /// Removes a locker from the sharing registry.
    ///
    /// Called by `ThreadLocker::drop()` and `HandleLocker::drop()`.
    ///
    /// 
    pub fn unregister_locker_sharing(&self, locker_id: i64) {
        self.share_registry.write().unwrap().remove(&locker_id);
    }

    /// Returns true if `a` and `b` are in the same lock-sharing group.
    ///
    /// Used by `ThreadLocker::shares_locks_with()` and
    /// `HandleLocker::shares_locks_with()`.
    pub fn same_share_group(&self, a: i64, b: i64) -> bool {
        let registry = self.share_registry.read().unwrap();
        match (registry.get(&a), registry.get(&b)) {
            (Some(ga), Some(gb)) => ga == gb,
            _ => false,
        }
    }

    /// Like `lock_with_timeout()` but also performs a `sharesLocksWith` check
    /// for every conflict that would otherwise block.
    ///
    /// `LockManager.lock()` / `LockImpl.tryLock()` — skips conflict
    /// detection when both lockers are in the same sharing group.
    ///
    /// This method is used by the locker implementations when they call the
    /// lock manager; the plain `lock()` / `lock_with_timeout()` path uses the
    /// registry automatically.
    pub fn lock_with_sharing(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
    ) -> Result<LockGrantType, TxnError> {
        self.lock_with_sharing_and_timeout(
            lsn,
            locker_id,
            lock_type,
            non_blocking,
            jump_ahead_of_waiters,
            self.lock_timeout_ms.load(Ordering::Relaxed),
        )
    }

    /// Full `lock_with_sharing` with explicit timeout.
    pub fn lock_with_sharing_and_timeout(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
        timeout_ms: u64,
    ) -> Result<LockGrantType, TxnError> {
        if lock_type == LockType::None {
            return Ok(LockGrantType::NoneNeeded);
        }
        if lock_type == LockType::Restart {
            return Err(TxnError::RangeRestart);
        }

        self.stats.lock_requests.fetch_add(1, Ordering::Relaxed);
        let table_idx = self.get_table_index(lsn);

        // Snapshot the sharing registry before entering the lock-table critical
        // section.  This avoids capturing `self` in the closure below while
        // also holding a mutable borrow of `self.lock_tables[table_idx]`.
        let requester_group: Option<i64> = {
            let reg = self.share_registry.read().unwrap();
            reg.get(&locker_id).copied()
        };
        // Only clone the registry when the requester is actually in a sharing
        // group — avoids a HashMap allocation on every lock call for the common
        // path where requester_group is None (BasicLockers, most internal ops).
        let registry_snapshot: Option<std::collections::HashMap<i64, i64>> =
            if requester_group.is_some() {
                Some(self.share_registry.read().unwrap().clone())
            } else {
                None
            };
        let shares = move |owner_id: i64| -> bool {
            if let (Some(req_group), Some(reg)) = (requester_group, &registry_snapshot) {
                reg.get(&owner_id).copied() == Some(req_group)
            } else {
                false
            }
        };

        // Phase 1: attempt under shard mutex.
        let (initial_grant, owner_ids, notify_pair) = {
            let mut table = self.lock_tables[table_idx].lock();
            let lock = table.entry(lsn).or_insert_with(Lock::new_thin);

            let result = lock.lock_with_sharing(
                lock_type,
                locker_id,
                non_blocking,
                jump_ahead_of_waiters,
                &shares,
            );

            if result.success {
                return Ok(result.lock_grant);
            }
            if result.lock_grant == LockGrantType::Denied {
                return Err(TxnError::LockNotAvailable { lsn });
            }

            self.stats.lock_waits.fetch_add(1, Ordering::Relaxed);
            let owner_ids = lock.get_owner_ids();
            let pair: WaiterNotify = Arc::new((Mutex::new(false), Condvar::new()));
            lock.set_waiter_notify(locker_id, pair.clone());
            (result.lock_grant, owner_ids, pair)
        };

        // Register in the incremental waits-for graph (same as lock_with_timeout).
        self.record_wait(locker_id, &owner_ids);

        // Phase 2: deadlock check.
        if let Some(deadlock_err) = self.check_deadlock_for_waiter(
            lsn, locker_id, lock_type, &owner_ids,
        ) {
            self.clear_wait(locker_id);
            let mut table = self.lock_tables[table_idx].lock();
            if let Some(lock) = table.get_mut(&lsn) {
                lock.flush_waiter(locker_id);
                if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                    table.remove(&lsn);
                }
            }
            return Err(deadlock_err);
        }

        // Phase 3: condvar wait (identical to lock_with_timeout).
        let start = std::time::Instant::now();
        let (mutex, condvar) = &*notify_pair;
        let mut granted_guard = mutex.lock();

        loop {
            if *granted_guard {
                break;
            }
            let remaining_ms = if timeout_ms == 0 {
                0
            } else {
                let elapsed = start.elapsed().as_millis() as u64;
                if elapsed >= timeout_ms {
                    drop(granted_guard);
                    self.clear_wait(locker_id);
                    let mut table = self.lock_tables[table_idx].lock();
                    if let Some(lock) = table.get_mut(&lsn) {
                        lock.flush_waiter(locker_id);
                        if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                            table.remove(&lsn);
                        }
                    }
                    return Err(TxnError::LockTimeout {
                        timeout_ms,
                        lsn,
                        owner: format!("owners of LSN {}", lsn),
                        requested_type: lock_type,
                        requester: locker_id.to_string(),
                    });
                }
                timeout_ms - elapsed
            };
            let slice_ms = if remaining_ms == 0 { 50 } else { remaining_ms.min(50) };
            let timed_out = condvar.wait_for(
                &mut granted_guard,
                Duration::from_millis(slice_ms),
            );
            if timed_out.timed_out()
                && let Some(dl_err) = self.check_deadlock_for_waiter(
                    lsn, locker_id, lock_type, &owner_ids,
                ) {
                drop(granted_guard);
                self.clear_wait(locker_id);
                let mut table = self.lock_tables[table_idx].lock();
                if let Some(lock) = table.get_mut(&lsn) {
                    lock.flush_waiter(locker_id);
                    if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                        table.remove(&lsn);
                    }
                }
                return Err(dl_err);
            }
        }

        drop(granted_guard);
        self.clear_wait(locker_id);

        let grant = match initial_grant {
            LockGrantType::WaitNew => LockGrantType::New,
            LockGrantType::WaitPromotion => LockGrantType::Promotion,
            LockGrantType::WaitRestart => LockGrantType::New,
            other => other,
        };
        Ok(grant)
    }

    // ========================================================================

    /// Returns the lock table index for a given LSN.
    ///
    ///
    /// 
    fn get_table_index(&self, lsn: u64) -> usize {
        ((lsn as usize) & 0x7fff_ffff) % N_LOCK_TABLES
    }

    /// Records that `locker_id` is now waiting on `owner_ids` in the
    /// incremental waits-for graph.  Called right after Phase 1 in both wait
    /// paths, before the first deadlock check.
    fn record_wait(&self, locker_id: i64, owner_ids: &[i64]) {
        let mut graph = self.waiter_graph.lock();
        graph.insert(locker_id, owner_ids.to_vec());
    }

    /// Removes `locker_id` from the incremental waits-for graph.  Called at
    /// every exit point after `record_wait`: grant, timeout, and deadlock abort.
    fn clear_wait(&self, locker_id: i64) {
        let mut graph = self.waiter_graph.lock();
        graph.remove(&locker_id);
    }

    /// Checks whether this locker is involved in a deadlock and should be
    /// aborted as the victim.
    ///
    /// Returns `Some(TxnError::Deadlock)` if the cycle is detected and this
    /// locker is the chosen victim, `None` otherwise.
    ///
    /// Reads the incremental `waiter_graph` snapshot — O(n_active_waiters),
    /// no shard re-acquisition.  Victim selection uses "youngest locker"
    /// heuristic (highest locker_id, i.e. most recently started transaction)
    /// since we avoid the O(N_LOCK_TABLES) scan needed for exact lock counts.
    fn check_deadlock_for_waiter(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        owner_ids: &[i64],
    ) -> Option<TxnError> {
        // Build the waits-for snapshot from the incremental graph.  Also
        // ensure the current requester's edge is present (record_wait may
        // not have been called yet on the very first check).
        let waits_for: HashMap<i64, HashSet<i64>> = {
            let graph = self.waiter_graph.lock();
            let mut wf: HashMap<i64, HashSet<i64>> = graph
                .iter()
                .map(|(&wid, owners)| (wid, owners.iter().copied().collect()))
                .collect();
            wf.entry(locker_id)
                .or_insert_with(|| owner_ids.iter().copied().collect());
            wf
        };

        let cycle = DeadlockDetector::detect(locker_id, owner_ids, &waits_for)?;
        // Pass empty lock_counts: select_victim falls back to youngest (highest ID).
        let victim = DeadlockDetector::select_victim(&cycle, &HashMap::new());

        if victim == locker_id {
            Some(TxnError::Deadlock(format!(
                "deadlock cycle detected; locker {} chosen as victim \
                 while waiting for LSN {} ({:?})",
                locker_id, lsn, lock_type
            )))
        } else {
            None
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // -----------------------------------------------------------------------
    // Original single-threaded tests (preserved)
    // -----------------------------------------------------------------------

    #[test]
    fn test_new_lock_manager() {
        let lm = LockManager::new();
        assert_eq!(lm.n_total_locks(), 0);

        let stats = lm.get_stats();
        assert_eq!(stats.lock_requests, 0);
        assert_eq!(stats.lock_waits, 0);
    }

    #[test]
    fn test_lock_type_none() {
        let lm = LockManager::new();
        let result = lm.lock(1000, 1, LockType::None, false, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), LockGrantType::NoneNeeded);

        let stats = lm.get_stats();
        assert_eq!(stats.lock_requests, 0);
    }

    #[test]
    fn test_lock_type_restart() {
        let lm = LockManager::new();
        let result = lm.lock(1000, 1, LockType::Restart, false, false);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TxnError::RangeRestart));
    }

    #[test]
    fn test_basic_lock_release() {
        let lm = LockManager::new();

        let result = lm.lock(1000, 1, LockType::Read, false, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), LockGrantType::New);
        assert_eq!(lm.n_total_locks(), 1);

        let result = lm.release(1000, 1);
        assert!(result.is_ok());
        assert_eq!(lm.n_total_locks(), 0);
    }

    #[test]
    fn test_multiple_readers() {
        let lm = LockManager::new();

        let result = lm.lock(1000, 1, LockType::Read, false, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), LockGrantType::New);

        let result = lm.lock(1000, 2, LockType::Read, false, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), LockGrantType::New);

        assert_eq!(lm.n_total_locks(), 1);
        let (owners, waiters) = lm.get_lock_info(1000);
        assert_eq!(owners, 2);
        assert_eq!(waiters, 0);
    }

    #[test]
    fn test_non_blocking_denied() {
        let lm = LockManager::new();

        let result = lm.lock(1000, 1, LockType::Write, false, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), LockGrantType::New);

        let result = lm.lock(1000, 2, LockType::Write, true, false);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TxnError::LockNotAvailable { .. }
        ));
    }

    #[test]
    fn test_table_sharding() {
        let lm = LockManager::new();

        for i in 0..100u64 {
            let result =
                lm.lock(i * 1000, i as i64, LockType::Write, false, false);
            assert!(result.is_ok());
        }

        assert_eq!(lm.n_total_locks(), 100);

        let idx1 = lm.get_table_index(1000);
        let idx2 = lm.get_table_index(2000);
        assert!(idx1 < N_LOCK_TABLES);
        assert!(idx2 < N_LOCK_TABLES);
    }

    #[test]
    fn test_lock_cleanup() {
        let lm = LockManager::new();

        for i in 0..100 {
            let _ = lm.lock(i, 1, LockType::Write, false, false);
        }
        assert_eq!(lm.n_total_locks(), 100);

        for i in 0..100 {
            let _ = lm.release(i, 1);
        }

        assert_eq!(lm.n_total_locks(), 0);
    }

    #[test]
    fn test_statistics() {
        let lm = LockManager::new();

        let _ = lm.lock(1000, 1, LockType::Read, false, false);
        let _ = lm.lock(1000, 2, LockType::Read, false, false);

        let stats = lm.get_stats();
        assert_eq!(stats.lock_requests, 2);
    }

    #[test]
    fn test_is_owned_write_lock() {
        let lm = LockManager::new();

        let _ = lm.lock(1000, 1, LockType::Write, false, false);

        assert!(lm.is_owned_write_lock(1000, 1));
        assert!(!lm.is_owned_write_lock(1000, 2));
        assert!(!lm.is_owned_write_lock(2000, 1));
    }

    #[test]
    fn test_get_owned_lock_type() {
        let lm = LockManager::new();

        let _ = lm.lock(1000, 1, LockType::Read, false, false);

        assert_eq!(lm.get_owned_lock_type(1000, 1), Some(LockType::Read));
        assert_eq!(lm.get_owned_lock_type(1000, 2), None);
        assert_eq!(lm.get_owned_lock_type(2000, 1), None);
    }

    #[test]
    fn test_demote() {
        let lm = LockManager::new();

        let _ = lm.lock(1000, 1, LockType::Write, false, false);
        assert!(lm.is_owned_write_lock(1000, 1));

        let _ = lm.demote(1000, 1);
        assert!(!lm.is_owned_write_lock(1000, 1));
        assert_eq!(lm.get_owned_lock_type(1000, 1), Some(LockType::Read));
    }

    #[test]
    fn test_steal_lock() {
        let lm = LockManager::new();

        let _ = lm.lock(1000, 1, LockType::Read, false, false);
        assert_eq!(lm.get_owned_lock_type(1000, 1), Some(LockType::Read));

        let _ = lm.steal_lock(1000, 2);
    }

    // -----------------------------------------------------------------------
    // Multi-threaded blocking tests
    // -----------------------------------------------------------------------

    /// Thread A holds a write lock; thread B blocks on it.  When A releases,
    /// B should be granted the lock.
    ///
    /// Waitforlock / notifyall flow.
    #[test]
    fn test_blocking_lock_granted_on_release() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0xDEAD_BEEF;

        // Thread A acquires the write lock.
        lm.lock(LSN, 1, LockType::Write, false, false).unwrap();

        // Sync: wait for B to register as waiter before A releases.
        let ready = Arc::new((Mutex::new(false), Condvar::new()));

        let lm_b = Arc::clone(&lm);
        let ready_b = Arc::clone(&ready);
        let b = thread::spawn(move || {
            // Signal that B is about to block.
            {
                let (m, cv) = &*ready_b;
                let mut g = m.lock();
                *g = true;
                cv.notify_all();
            }
            // Block until A releases (5 s timeout so test doesn't hang).
            
            lm_b.lock_with_timeout(
                LSN, 2, LockType::Write, false, false, 5000,
            )
        });

        // Wait until B has at least started, then give it a moment to block.
        {
            let (m, cv) = &*ready;
            let mut g = m.lock();
            while !*g {
                cv.wait(&mut g);
            }
        }
        // Small sleep so B enters the condvar wait.
        thread::sleep(Duration::from_millis(50));

        // A releases the lock.
        lm.release(LSN, 1).unwrap();

        // B should wake up and get the lock.
        let result = b.join().unwrap();
        assert!(
            result.is_ok(),
            "thread B expected Ok, got {:?}",
            result
        );
        assert_eq!(result.unwrap(), LockGrantType::New);
    }

    /// Thread A holds a write lock.  Thread B waits with a short timeout.
    /// A never releases, so B should receive `LockTimeout`.
    #[test]
    fn test_lock_timeout() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0xCAFE_BABE;

        // Thread A acquires the write lock and holds it for the entire test.
        lm.lock(LSN, 1, LockType::Write, false, false).unwrap();

        let lm_b = Arc::clone(&lm);
        let b = thread::spawn(move || {
            // 100 ms timeout — A will not release.
            lm_b.lock_with_timeout(LSN, 2, LockType::Read, false, false, 100)
        });

        let result = b.join().unwrap();
        assert!(
            matches!(result, Err(TxnError::LockTimeout { .. })),
            "expected LockTimeout, got {:?}",
            result
        );

        // Clean up: A releases.
        lm.release(LSN, 1).unwrap();
    }

    /// A -> holds X, waits Y; B -> holds Y, waits X.
    /// One of them must get `Deadlock` error.
    #[test]
    fn test_deadlock_detected() {
        let lm = Arc::new(LockManager::new());
        const LSN_X: u64 = 0x1111_1111;
        const LSN_Y: u64 = 0x2222_2222;

        // Thread A holds X.
        lm.lock(LSN_X, 1, LockType::Write, false, false).unwrap();

        // Thread B holds Y.
        lm.lock(LSN_Y, 2, LockType::Write, false, false).unwrap();

        let lm_a = Arc::clone(&lm);
        let lm_b = Arc::clone(&lm);

        // A waits for Y (held by B), B waits for X (held by A) — classic
        // deadlock.  Use a generous timeout so the deadlock detector fires
        // rather than the timeout.
        let a = thread::spawn(move || {
            lm_a.lock_with_timeout(LSN_Y, 1, LockType::Write, false, false, 3000)
        });

        // Give A a moment to register as waiter.
        thread::sleep(Duration::from_millis(50));

        let b = thread::spawn(move || {
            lm_b.lock_with_timeout(LSN_X, 2, LockType::Write, false, false, 3000)
        });

        let res_a = a.join().unwrap();
        let res_b = b.join().unwrap();

        // At least one must be a Deadlock error.
        let one_deadlock = matches!(res_a, Err(TxnError::Deadlock(_)))
            || matches!(res_b, Err(TxnError::Deadlock(_)));
        assert!(
            one_deadlock,
            "expected at least one Deadlock, got a={:?} b={:?}",
            res_a, res_b
        );
    }

    /// One write lock released; multiple waiting readers must all be granted.
    ///
    /// grants all compatible waiters at once in LockImpl.release().
    #[test]
    fn test_multiple_readers_unblocked() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0xFEED_FACE;
        const N_READERS: usize = 4;

        // Writer holds the lock.
        lm.lock(LSN, 1, LockType::Write, false, false).unwrap();

        let started = Arc::new((Mutex::new(0usize), Condvar::new()));
        let mut handles = Vec::new();

        for i in 0..N_READERS {
            let lm_r = Arc::clone(&lm);
            let started_r = Arc::clone(&started);
            let h = thread::spawn(move || {
                {
                    let (m, cv) = &*started_r;
                    let mut g = m.lock();
                    *g += 1;
                    cv.notify_all();
                }
                lm_r.lock_with_timeout(
                    LSN,
                    (i + 2) as i64,
                    LockType::Read,
                    false,
                    false,
                    5000,
                )
            });
            handles.push(h);
        }

        // Wait until all readers have signalled.
        {
            let (m, cv) = &*started;
            let mut g = m.lock();
            while *g < N_READERS {
                cv.wait(&mut g);
            }
        }
        // Allow time for all readers to block.
        thread::sleep(Duration::from_millis(80));

        // Release the write lock.
        lm.release(LSN, 1).unwrap();

        // All readers should have been granted.
        for h in handles {
            let result = h.join().unwrap();
            assert!(
                result.is_ok(),
                "reader expected Ok, got {:?}",
                result
            );
            assert_eq!(result.unwrap(), LockGrantType::New);
        }
    }

    // -----------------------------------------------------------------------
    // Ported from LockManagerTest.java — testNegatives
    // -----------------------------------------------------------------------

    /// Query methods return false before
    /// a lock is acquired, and the lock entry is cleaned up after release.
    #[test]
    fn test_je_negatives_query_before_lock() {
        let lm = LockManager::new();
        let lsn: u64 = 1;

        // No lock held yet.
        assert_eq!(lm.get_owned_lock_type(lsn, 1), None);
        assert_eq!(lm.get_owned_lock_type(lsn, 1), None); // write check
        let (owners, _) = lm.get_lock_info(lsn);
        assert_eq!(owners, 0);

        // Acquire READ lock for locker 1.
        let r = lm.lock(lsn, 1, LockType::Read, false, false);
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), LockGrantType::New);

        // A second request for the same lock by the same locker → EXISTING.
        let r2 = lm.lock(lsn, 1, LockType::Read, false, false);
        assert_eq!(r2.unwrap(), LockGrantType::Existing);

        // Locker 2 does not own it.
        assert_eq!(lm.get_owned_lock_type(lsn, 2), None);

        // The lock entry exists.
        let (owners, _) = lm.get_lock_info(lsn);
        assert_eq!(owners, 1);

        // Release a non-existent LSN — should not panic and lock should persist.
        let _ = lm.release(2, 1); // lsn=2 doesn't exist
        let (owners2, _) = lm.get_lock_info(lsn);
        assert_eq!(owners2, 1);

        // Release by a non-owner (locker 2) should not release lsn=1.
        let _ = lm.release(lsn, 2);
        let (owners3, _) = lm.get_lock_info(lsn);
        assert_eq!(owners3, 1);
        assert_eq!(lm.get_owned_lock_type(lsn, 1), Some(LockType::Read));

        // True release by the actual owner.
        lm.release(lsn, 1).unwrap();
        let (owners4, _) = lm.get_lock_info(lsn);
        assert_eq!(owners4, 0);
        assert_eq!(lm.get_owned_lock_type(lsn, 1), None);
    }

    /// Holding write then requesting
    /// READ for the same locker succeeds (WRITE subsumes READ).
    #[test]
    fn test_je_write_then_read_same_locker_ok() {
        let lm = LockManager::new();
        let lsn: u64 = 1;

        lm.lock(lsn, 1, LockType::Write, false, false).unwrap();
        // READ request for same locker — must succeed (EXISTING or better).
        let r = lm.lock(lsn, 1, LockType::Read, false, false);
        assert!(r.is_ok());
        // A third WRITE request should also be EXISTING.
        let r2 = lm.lock(lsn, 1, LockType::Write, false, false);
        assert_eq!(r2.unwrap(), LockGrantType::Existing);
    }

    // -----------------------------------------------------------------------
    // Ported from LockManagerTest.java — testSR15926LargeNodeIds
    // -----------------------------------------------------------------------

    /// Lsn values with the
    /// sign bit set (> 0x80000000) must hash to a non-negative table index.
    #[test]
    fn test_je_large_lsn_no_negative_index() {
        let lm = LockManager::new();
        // 0x80000000 is the value from the original bug report.
        let large_lsn: u64 = 0x80000000u64;
        let result = lm.lock(large_lsn, 1, LockType::Write, false, false);
        assert!(result.is_ok(), "large LSN should not cause a panic or error");
        lm.release(large_lsn, 1).unwrap();
    }

    // -----------------------------------------------------------------------
    // Ported from LockManagerTest — 16 lock table shards
    // -----------------------------------------------------------------------

    /// The LockManager uses N_LOCK_TABLES (16) shards; verify the constant.
    #[test]
    fn test_je_sixteen_lock_tables() {
        // N_LOCK_TABLES is a private const, but we can verify the behaviour
        // by distributing 16 distinct LSNs and checking all are managed.
        let lm = LockManager::new();
        for i in 0..16u64 {
            lm.lock(i, 1, LockType::Write, false, false).unwrap();
        }
        assert_eq!(lm.n_total_locks(), 16);
        for i in 0..16u64 {
            lm.release(i, 1).unwrap();
        }
        assert_eq!(lm.n_total_locks(), 0);
    }

    // -----------------------------------------------------------------------
    // Ported from LockManagerTest.java — testMultipleReaders
    // -----------------------------------------------------------------------

    /// Three concurrent threads
    /// can all hold read locks simultaneously.
    #[test]
    fn test_je_multiple_readers_concurrent() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0xAAAA;
        let ready = Arc::new((Mutex::new(0usize), Condvar::new()));
        let mut handles = Vec::new();

        for locker_id in 1i64..=3 {
            let lm2 = Arc::clone(&lm);
            let ready2 = Arc::clone(&ready);
            let h = thread::spawn(move || {
                lm2.lock(LSN, locker_id, LockType::Read, false, false).unwrap();
                assert_eq!(lm2.get_owned_lock_type(LSN, locker_id), Some(LockType::Read));
                {
                    let (m, cv) = &*ready2;
                    let mut g = m.lock();
                    *g += 1;
                    cv.notify_all();
                }
                // Wait for all three to own
                {
                    let (m, cv) = &*ready2;
                    let mut g = m.lock();
                    while *g < 3 {
                        cv.wait(&mut g);
                    }
                }
                lm2.release(LSN, locker_id).unwrap();
            });
            handles.push(h);
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // Ported from LockManagerTest.java — testNonBlockingLock1 / 2
    // -----------------------------------------------------------------------

    /// A read lock is held;
    /// a non-blocking write request is denied; after release the write succeeds.
    #[test]
    fn test_je_nonblocking_write_denied_then_granted() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0xBBBB;

        // Thread 1 holds a read lock.
        lm.lock(LSN, 1, LockType::Read, false, false).unwrap();

        let lm2 = Arc::clone(&lm);
        let h = thread::spawn(move || {
            // Non-blocking write → must be denied.
            let r = lm2.lock(LSN, 2, LockType::Write, true, false);
            assert!(
                matches!(r, Err(TxnError::LockNotAvailable { .. })),
                "expected LockNotAvailable, got {:?}",
                r
            );
            // Locker 2 is not an owner.
            assert_eq!(lm2.get_owned_lock_type(LSN, 2), None);
            let (_, waiters) = lm2.get_lock_info(LSN);
            assert_eq!(waiters, 0);
            let (owners, _) = lm2.get_lock_info(LSN);
            assert_eq!(owners, 1);
        });
        h.join().unwrap();

        // Now release locker 1; locker 2 can acquire afterwards.
        lm.release(LSN, 1).unwrap();
        let r2 = lm.lock(LSN, 2, LockType::Write, false, false);
        assert_eq!(r2.unwrap(), LockGrantType::New);
        lm.release(LSN, 2).unwrap();
    }

    /// A write lock is held;
    /// a non-blocking read request is denied; after release the read succeeds.
    #[test]
    fn test_je_nonblocking_read_denied_then_granted() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0xCCCC;

        // Locker 1 holds a write lock.
        lm.lock(LSN, 1, LockType::Write, false, false).unwrap();
        assert!(lm.is_owned_write_lock(LSN, 1));

        // Non-blocking read for locker 2 → denied.
        let r = lm.lock(LSN, 2, LockType::Read, true, false);
        assert!(
            matches!(r, Err(TxnError::LockNotAvailable { .. })),
            "expected LockNotAvailable, got {:?}",
            r
        );
        assert_eq!(lm.get_owned_lock_type(LSN, 2), None);

        // Release locker 1, then locker 2 can read.
        lm.release(LSN, 1).unwrap();
        let r2 = lm.lock(LSN, 2, LockType::Read, false, false);
        assert_eq!(r2.unwrap(), LockGrantType::New);
        assert!(!lm.is_owned_write_lock(LSN, 2));
        lm.release(LSN, 2).unwrap();
    }

    // -----------------------------------------------------------------------
    // Ported from LockManagerTest.java — testMultipleReadersSingleWrite1
    // -----------------------------------------------------------------------

    /// Two readers
    /// hold a lock; a writer blocks; when both readers release the writer is
    /// granted.
    #[test]
    fn test_je_two_readers_one_writer_blocks_then_granted() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0xDDDD;
        let writers_waiting = Arc::new((Mutex::new(false), Condvar::new()));

        // Locker 1 and 2 acquire read locks upfront.
        lm.lock(LSN, 1, LockType::Read, false, false).unwrap();
        lm.lock(LSN, 2, LockType::Read, false, false).unwrap();

        let lm3 = Arc::clone(&lm);
        let ww = Arc::clone(&writers_waiting);
        let writer = thread::spawn(move || {
            {
                let (m, cv) = &*ww;
                let mut g = m.lock();
                *g = true;
                cv.notify_all();
            }
            // Block until both readers release.
            lm3.lock_with_timeout(LSN, 3, LockType::Write, false, false, 5000)
        });

        // Wait until writer has registered as waiter.
        {
            let (m, cv) = &*writers_waiting;
            let mut g = m.lock();
            while !*g {
                cv.wait(&mut g);
            }
        }
        thread::sleep(Duration::from_millis(30));

        let (_, waiters) = lm.get_lock_info(LSN);
        assert_eq!(waiters, 1, "writer should be waiting");

        lm.release(LSN, 1).unwrap();
        lm.release(LSN, 2).unwrap();

        let result = writer.join().unwrap();
        assert!(result.is_ok(), "writer should have been granted, got {:?}", result);
        assert!(lm.is_owned_write_lock(LSN, 3));
        lm.release(LSN, 3).unwrap();
    }

    // -----------------------------------------------------------------------
    // Ported from DeadlockTest.java — testDeadlockBetweenTwoLockers
    // -----------------------------------------------------------------------

    /// Classic 2-locker
    /// deadlock.  Locker 1 holds L1 and waits for L2; locker 2 holds L2 and
    /// waits for L1.  At least one must receive a Deadlock error.
    #[test]
    fn test_je_deadlock_two_lockers() {
        let lm = Arc::new(LockManager::new());
        const L1: u64 = 0x1001;
        const L2: u64 = 0x2002;

        lm.lock(L1, 1, LockType::Write, false, false).unwrap();
        lm.lock(L2, 2, LockType::Write, false, false).unwrap();

        let lm_a = Arc::clone(&lm);
        let lm_b = Arc::clone(&lm);

        let a = thread::spawn(move || {
            lm_a.lock_with_timeout(L2, 1, LockType::Write, false, false, 3000)
        });
        thread::sleep(Duration::from_millis(50));
        let b = thread::spawn(move || {
            lm_b.lock_with_timeout(L1, 2, LockType::Write, false, false, 3000)
        });

        let ra = a.join().unwrap();
        let rb = b.join().unwrap();

        let one_dead = matches!(ra, Err(TxnError::Deadlock(_)))
            || matches!(rb, Err(TxnError::Deadlock(_)));
        assert!(
            one_dead,
            "expected at least one Deadlock, got a={:?} b={:?}",
            ra, rb
        );
    }

    // -----------------------------------------------------------------------
    // Ported from DeadlockTest.java — testDeadlockAmongThreeLockers
    // -----------------------------------------------------------------------

    /// 3-locker cycle.
    /// Locker1 → L2, Locker2 → L3, Locker3 → L1.  At least one deadlock.
    #[test]
    fn test_je_deadlock_three_lockers_cycle() {
        let lm = Arc::new(LockManager::new());
        const L1: u64 = 0x3001;
        const L2: u64 = 0x3002;
        const L3: u64 = 0x3003;

        // Each locker acquires its first lock.
        lm.lock(L1, 1, LockType::Write, false, false).unwrap();
        lm.lock(L2, 2, LockType::Write, false, false).unwrap();
        lm.lock(L3, 3, LockType::Write, false, false).unwrap();

        let lm1 = Arc::clone(&lm);
        let lm2 = Arc::clone(&lm);
        let lm3 = Arc::clone(&lm);

        let t1 = thread::spawn(move || {
            lm1.lock_with_timeout(L2, 1, LockType::Write, false, false, 3000)
        });
        thread::sleep(Duration::from_millis(30));
        let t2 = thread::spawn(move || {
            lm2.lock_with_timeout(L3, 2, LockType::Write, false, false, 3000)
        });
        thread::sleep(Duration::from_millis(30));
        let t3 = thread::spawn(move || {
            lm3.lock_with_timeout(L1, 3, LockType::Write, false, false, 3000)
        });

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();
        let r3 = t3.join().unwrap();

        let any_dead = matches!(r1, Err(TxnError::Deadlock(_)))
            || matches!(r2, Err(TxnError::Deadlock(_)))
            || matches!(r3, Err(TxnError::Deadlock(_)));
        assert!(
            any_dead,
            "3-locker cycle: expected at least one Deadlock error"
        );
    }

    // -----------------------------------------------------------------------
    // Ported from DeadlockTest.java — testThrowCorrectException
    // -----------------------------------------------------------------------

    /// A single waiter with
    /// no cycle should time out with LockTimeout (not Deadlock).
    #[test]
    fn test_je_no_cycle_gives_timeout_not_deadlock() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0x4444;

        // Locker 1 holds the lock and never releases.
        lm.lock(LSN, 1, LockType::Write, false, false).unwrap();

        let lm2 = Arc::clone(&lm);
        let h = thread::spawn(move || {
            lm2.lock_with_timeout(LSN, 2, LockType::Write, false, false, 200)
        });

        let r = h.join().unwrap();
        assert!(
            matches!(r, Err(TxnError::LockTimeout { .. })),
            "no cycle → expected LockTimeout, got {:?}",
            r
        );

        lm.release(LSN, 1).unwrap();
    }

    // -----------------------------------------------------------------------
    // Ported from LockManagerTest — lock statistics increment
    // -----------------------------------------------------------------------

    /// Lock statistics (lock_requests, lock_waits) must increment correctly.
    #[test]
    fn test_je_lock_stats_increment() {
        let lm = LockManager::new();

        lm.lock(10, 1, LockType::Read, false, false).unwrap();
        lm.lock(10, 2, LockType::Read, false, false).unwrap();
        lm.lock(20, 3, LockType::Write, false, false).unwrap();

        let stats = lm.get_stats();
        assert_eq!(stats.lock_requests, 3, "3 lock requests should be counted");
        // No waits because all were immediately granted.
        assert_eq!(stats.lock_waits, 0, "no waits expected");
    }

    // -----------------------------------------------------------------------
    // Ported from LockManagerTest.java — testUpgradeLock
    // -----------------------------------------------------------------------

    /// A promotion waiter (locker
    /// that already holds a read lock) is placed ahead of new write waiters
    /// so it gets the write lock before them.
    #[test]
    fn test_je_upgrade_lock_butts_in_front() {
        let lm = Arc::new(LockManager::new());
        const LSN: u64 = 0x5555;

        // Locker 1 and 2 hold read locks.
        lm.lock(LSN, 1, LockType::Read, false, false).unwrap();
        lm.lock(LSN, 2, LockType::Read, false, false).unwrap();

        let lm3 = Arc::clone(&lm);
        let lm2 = Arc::clone(&lm);

        // Locker 3 waits for write (new waiter).
        let t3 = thread::spawn(move || {
            lm3.lock_with_timeout(LSN, 3, LockType::Write, false, false, 5000)
        });
        thread::sleep(Duration::from_millis(30));

        // Locker 2 upgrades read → write (promotion waiter, should jump ahead).
        let t2 = thread::spawn(move || {
            lm2.lock_with_timeout(LSN, 2, LockType::Write, false, false, 5000)
        });
        thread::sleep(Duration::from_millis(20));

        // Release locker 1's read lock; locker 2's promotion should be granted
        // before locker 3.
        lm.release(LSN, 1).unwrap();

        let r2 = t2.join().unwrap();
        assert!(r2.is_ok(), "locker 2 promotion should succeed, got {:?}", r2);
        assert_eq!(r2.unwrap(), LockGrantType::Promotion);

        // Now release locker 2's write; locker 3 gets it.
        lm.release(LSN, 2).unwrap();
        let r3 = t3.join().unwrap();
        assert!(r3.is_ok(), "locker 3 should succeed after locker 2, got {:?}", r3);
        lm.release(LSN, 3).unwrap();
    }
}
