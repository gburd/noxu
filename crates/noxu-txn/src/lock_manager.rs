//! Lock manager for Noxu DB.
//!
//!
//! The LockManager is the central authority for all lock operations in the
//! system. It manages N sharded lock tables, each protected by its own mutex,
//! to allow concurrent lock operations on different LSNs.
//!
//! # Internal lock ordering (H-2, 2026 audit F-6.2)
//!
//! Two internal mutexes must never be held simultaneously, but when code
//! paths need to update BOTH in sequence the canonical order is:
//!
//!   **shard mutex first, then waiter_graph mutex**.
//!
//! Concretely:
//! - Lock the relevant `lock_tables[idx]` shard first.
//! - Release the shard before (or immediately before) acquiring `waiter_graph`.
//! - Never acquire a shard while holding `waiter_graph`.
//!
//! All victim-cleanup paths (flush_waiter + clear_wait) are structured to
//! acquire the shard first, then call `clear_wait()` after the shard guard
//! is dropped. This prevents a lock-ordering inversion that would otherwise
//! create a potential process hang under extreme contention.

use hashbrown::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

// The shard-table / waiter-graph `Mutex` and the per-waiter grant `Condvar`
// route through the parking_lot-over-shuttle seam so the deadlock-detection
// and grant/wakeup interleavings are schedulable by shuttle (DST wave 2).
// The default build re-exports the real `noxu-sync` futex types, so
// production behaviour and the on-heap layout are unchanged.
//
// The read-mostly registry `RwLock`s (`locker_labels`, `non_preemptable`)
// route through the std-shaped `dst_sync` seam (DST txn wave): `begin_txn` /
// `commit_txn` / `abort_txn` call `register_locker_label` /
// `unregister_locker_label`, so the txn-commit shuttle gate DOES contend on
// them across green threads.  An un-instrumented `std::sync::RwLock` held by
// one shuttle green thread while the scheduler switches to another that
// blocks on it would hang shuttle's single-OS-thread executor; routing them
// through `dst_sync` makes the acquire a schedulable point.  Under the
// default cfg `dst_sync::RwLock` is a transparent `std::sync::RwLock`
// re-export — production is byte-identical.
use noxu_util::dst_sync::RwLock;
use noxu_util::dst_sync_pl::{Condvar, Mutex};
use noxu_util::{Clock, RealClock};

use crate::lock_info::WaiterNotify;
use crate::{
    DeadlockDetector, Lock, LockGrantType, LockStats, LockType, TxnError,
};
use std::sync::atomic::{AtomicU64, Ordering};

/// Number of lock table shards.
///
/// Multiple lock tables reduce contention by allowing concurrent operations
/// on locks in different tables.  64 shards provide good distribution across
/// multi-core systems under high concurrency (16+ threads).  The hash
/// function spreads LSNs uniformly so collision probability is low.
/// Default number of lock-table shards when not explicitly configured.
/// Noxu deliberately defaults to 64 (a documented deviation from JE's default
/// of 1) for higher write concurrency; the count is configurable via
/// `LOCK_N_LOCK_TABLES` (`noxu.lock.nLockTables`).
const DEFAULT_N_LOCK_TABLES: usize = 64;

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
    /// Number of shards (== lock_tables.len()); configurable via
    /// `LOCK_N_LOCK_TABLES`. Cached so get_table_index avoids a Vec len read.
    n_lock_tables: usize,

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
    /// group_id, and thus bypass lock-conflict detection (
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

    /// Diagnostic label registry: maps locker_id → static label such as
    /// `"txn"`, `"auto-txn"`, or `"cleaner"`.
    ///
    /// Used by [`LockManager::format_locker`] to render a typed identifier
    /// like `"auto-txn:42"` in deadlock and timeout error messages so a
    /// deadlock involving a synthetic auto-commit txn and an explicit txn is
    /// visibly distinguishable from one involving two explicit txns.
    ///
    /// Closes the second F12 residual (May 2026 audit follow-up).  Lockers
    /// without a registered label are reported as `"locker:<id>"`.
    locker_labels: RwLock<HashMap<i64, &'static str>>,

    /// Set of locker IDs that are NOT preemptable (importunate lockers must
    /// not have their locks stolen).  JE tracks this per-`Locker` via
    /// `getPreemptable()`; the lock manager needs it to decide, during an
    /// importunate steal, whether a remaining owner blocks the steal
    /// (LockManager.java:556 — "Lock holder is non-preemptable, wait again").
    /// A locker absent from this set is preemptable (the default).
    non_preemptable: RwLock<HashSet<i64>>,

    /// Injectable clock for the wait-loop timeout / deadlock re-detection
    /// cadence (DST M1.1).  JE reads `System.currentTimeMillis()` /
    /// `nanoTime()` at these sites; here the elapsed math routes through an
    /// injectable [`Clock`] so a [`noxu_util::SimClock`] makes the lock-timeout
    /// and 50 ms re-detection slice a pure function of the simulated timeline.
    /// Defaults to [`RealClock`] — production behavior is unchanged.
    clock: Arc<dyn Clock>,
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
        Self::with_config(timeout_ms, DEFAULT_N_LOCK_TABLES)
    }

    /// Creates a new LockManager with an explicit shard count (JE
    /// `LOCK_N_LOCK_TABLES` / `noxu.lock.nLockTables`) and lock timeout.
    /// Production (`EnvironmentImpl`) passes the configured value; `0` or
    /// values are clamped to at least 1 shard.
    pub fn with_config(timeout_ms: u64, n_lock_tables: usize) -> Self {
        Self::with_config_clock(timeout_ms, n_lock_tables, RealClock::arc())
    }

    /// Creates a `LockManager` with an injectable [`Clock`] (DST M1.1).
    ///
    /// Production uses [`LockManager::with_config`] (which passes
    /// [`RealClock`]); DST harnesses pass a [`noxu_util::SimClock`] so the
    /// lock-wait timeout and the 50 ms deadlock re-detection slice become a
    /// pure function of the simulated timeline.  Additive: no existing caller
    /// changes.
    pub fn with_config_clock(
        timeout_ms: u64,
        n_lock_tables: usize,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let n_lock_tables = n_lock_tables.max(1);
        let mut lock_tables = Vec::with_capacity(n_lock_tables);
        for _ in 0..n_lock_tables {
            lock_tables.push(Mutex::new(HashMap::new()));
        }

        Self {
            lock_tables,
            n_lock_tables,
            stats: LockManagerStats {
                lock_requests: AtomicU64::new(0),
                lock_waits: AtomicU64::new(0),
                lock_timeouts: AtomicU64::new(0),
            },
            lock_timeout_ms: AtomicU64::new(timeout_ms),
            share_registry: RwLock::new(HashMap::new()),
            waiter_graph: Mutex::new(HashMap::new()),
            locker_labels: RwLock::new(HashMap::new()),
            non_preemptable: RwLock::new(HashSet::new()),
            clock,
        }
    }

    /// Registers a diagnostic label for `locker_id`.
    ///
    /// Stored in `Self::locker_labels` and looked up by
    /// [`Self::format_locker`] when building deadlock / lock-timeout error
    /// messages.  Typical labels are `"txn"` (explicit transaction),
    /// `"auto-txn"` (synthetic auto-commit transaction created by
    /// `TxnManager::begin_auto_txn`), and `"cleaner"` (cleaner-locker IDs).
    ///
    /// Re-registering the same `locker_id` overwrites the previous label.
    /// Lockers without a registered label are reported as `"locker:<id>"`,
    /// which preserves backward compatibility with callers that never
    /// registered.
    pub fn register_locker_label(&self, locker_id: i64, label: &'static str) {
        self.locker_labels.write().unwrap().insert(locker_id, label);
    }

    /// Removes the diagnostic label for `locker_id`.
    ///
    /// Called when a transaction (explicit or synthetic auto-commit)
    /// terminates so the registry does not grow without bound.  Idempotent —
    /// removing an unknown id is a no-op.
    pub fn unregister_locker_label(&self, locker_id: i64) {
        self.locker_labels.write().unwrap().remove(&locker_id);
    }

    /// Returns a typed identifier string for `locker_id`.
    ///
    /// Looks up the label registered via [`Self::register_locker_label`] and
    /// returns `"<label>:<id>"`; if no label is registered, returns
    /// `"locker:<id>"`.
    ///
    /// Used to format the `requester` and `owner` fields of
    /// [`TxnError::LockTimeout`] and the message body of
    /// [`TxnError::Deadlock`] so a mixed auto-commit/explicit-txn deadlock
    /// reports e.g. `"auto-txn:42"` and `"txn:17"` rather than two opaque
    /// integers — closing the second F12 residual.
    pub fn format_locker(&self, locker_id: i64) -> String {
        match self.locker_labels.read().unwrap().get(&locker_id).copied() {
            Some(label) => format!("{label}:{locker_id}"),
            None => format!("locker:{locker_id}"),
        }
    }

    /// Returns a comma-separated typed identifier list for `locker_ids`.
    ///
    /// Convenience wrapper used in deadlock error messages.
    pub fn format_lockers(&self, locker_ids: &[i64]) -> String {
        let mut out = String::new();
        for (i, id) in locker_ids.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(&self.format_locker(*id));
        }
        out
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
    #[inline]
    /// Read fast-path probe (auto-commit / read-committed reads).
    ///
    /// A read-committed or auto-commit read acquires a `Read` lock and
    /// releases it immediately, so the only thing the lock buys is detecting
    /// whether a *writer* currently holds the slot (in which case the caller
    /// must wait + re-read the committed value).  When the slot has no
    /// lock-table entry at all — the overwhelmingly common case under a
    /// read-heavy workload — the acquire+release pair is two shard-mutex
    /// round-trips of pure overhead.
    ///
    /// This probe takes the shard mutex ONCE and returns `true` (uncontended)
    /// iff there is no entry for `lsn`, or an entry exists but has no write
    /// owner other than the requester.  In that case the caller may skip the
    /// formal acquire+release entirely — behaviour-identical to acquiring a
    /// Read lock and releasing it immediately, since (a) read-committed/
    /// auto-commit never holds the read lock past the operation, and (b) the
    /// BIN write-guard a writer must hold to mutate the slot excludes the
    /// reader's snapshot, so "no write owner now" means the snapshot is
    /// committed data.  Returns `false` (must take the slow path) when a write
    /// owner or any waiter is present, so contention detection + the
    /// re-read-after-writer path are preserved exactly.
    ///
    /// Only valid for the immediate-release isolation levels (read-committed,
    /// auto-commit).  Repeatable-read / serializable hold the read lock for
    /// the txn and MUST use the full `lock()` path.
    pub fn probe_read_uncontended(&self, lsn: u64, requester_id: i64) -> bool {
        let table_idx = self.get_table_index(lsn);
        let table = self.lock_tables[table_idx].lock();
        match table.get(&lsn) {
            None => true, // no lock at all — unlocked, uncontended
            Some(lock) => {
                // A waiter means someone is contending — take the slow path
                // so fairness/ordering is preserved.
                if lock.n_waiters() > 0 {
                    return false;
                }
                match lock.get_write_owner_locker_id() {
                    None => true,                 // only readers own it
                    Some(w) => w == requester_id, // we already write-own it
                }
            }
        }
    }

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

        // Snapshot the sharing registry before entering the lock-table
        // critical section so `LockImpl::try_lock_with_sharing` can consult
        // `sharesLocksWith` on every acquisition.  JE `LockImpl.tryLock`
        // checks `!locker.sharesLocksWith(ownerLocker) &&
        // !ownerLocker.sharesLocksWith(locker)` (LockImpl.java:647-648) on
        // EVERY lock request, letting siblings (two ThreadLockers on the
        // same thread, or a HandleLocker + its buddy txn) co-own a lock
        // without conflict.
        let shares = self.build_shares_fn(locker_id);

        // --- Phase 1: attempt to acquire the lock under the shard mutex. ---
        //
        // "Attempt to lock without any initial wait."
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
                // Granted immediately; no waiting needed.
                return Ok(result.lock_grant);
            }

            if result.lock_grant == LockGrantType::IllegalUpgrade {
                // An impossible upgrade transition: surface as an error so the
                // txn aborts and the environment survives (not a process panic).
                return Err(TxnError::IllegalUpgrade {
                    held: lock
                        .get_owned_lock_type(locker_id)
                        .unwrap_or(LockType::None),
                    requested: lock_type,
                });
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
        if let Some(deadlock_err) = self
            .check_deadlock_for_waiter(lsn, locker_id, lock_type, &owner_ids)
        {
            // We are the chosen victim.  Flush from waiter list and throw.
            // H-2: use flush_and_clear_waiter to acquire shard before
            // waiter_graph (canonical lock ordering).
            //
            // TXN-F4 (design point, intentionally NOT changed): JE
            // `LockManager.waitForLock` proactively wakes the victim via
            // `notifyVictim` so it exits its own `wait()` promptly; here the
            // selected victim returns synchronously the moment its own check
            // detects the cycle, so no cross-thread victim notify is needed.
            // This assumes every waiter on a cycle reaches the same
            // victim-consistency conclusion on its next check slice (≤50 ms),
            // which holds because all checks read the same `waiter_graph`
            // snapshot and use the same deterministic victim selection.
            self.flush_and_clear_waiter(table_idx, lsn, locker_id);
            return Err(deadlock_err);
        }

        // --- Phase 3: wait on the condvar. ---
        //
        // "locker.wait(timeRemaining)" in a loop, checking ownership on
        //     each wakeup.  We also re-run deadlock detection on each
        //     iteration so that cycles formed after we enter the wait path
        //     are caught.
        let start_ns = self.clock.now_nanos();
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
                let elapsed =
                    self.clock.now_nanos().saturating_sub(start_ns) / 1_000_000;
                if elapsed >= timeout_ms {
                    // Already timed out before we even slept this iteration.
                    drop(granted_guard);
                    // H-2: shard before waiter_graph.
                    self.flush_and_clear_waiter(table_idx, lsn, locker_id);
                    self.stats.lock_timeouts.fetch_add(1, Ordering::Relaxed);
                    return Err(TxnError::LockTimeout {
                        timeout_ms,
                        lsn,
                        owner: format!(
                            "[{}] on LSN {lsn}",
                            self.format_lockers(&owner_ids)
                        ),
                        requested_type: lock_type,
                        requester: self.format_locker(locker_id),
                    });
                }
                timeout_ms - elapsed
            };

            // Use a short slice (up to 50 ms) so we can re-check for
            // deadlocks that may form after we entered the wait path.
            // uses a "deadlock detection delay" for the same purpose.
            let slice_ms =
                if remaining_ms == 0 { 50 } else { remaining_ms.min(50) };

            let timed_out = condvar
                .wait_for(&mut granted_guard, Duration::from_millis(slice_ms))
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
                    table
                        .get(&lsn)
                        .map(|l| l.get_owner_ids())
                        .unwrap_or_default()
                };
                if let Some(deadlock_err) = self.check_deadlock_for_waiter(
                    lsn,
                    locker_id,
                    lock_type,
                    &cur_owner_ids,
                ) {
                    // H-2: shard before waiter_graph.
                    self.flush_and_clear_waiter(table_idx, lsn, locker_id);
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
                    && self.clock.now_nanos().saturating_sub(start_ns)
                        / 1_000_000
                        >= timeout_ms
                {
                    drop(granted_guard);
                    // H-2: shard before waiter_graph.
                    self.flush_and_clear_waiter(table_idx, lsn, locker_id);
                    self.stats.lock_timeouts.fetch_add(1, Ordering::Relaxed);
                    return Err(TxnError::LockTimeout {
                        timeout_ms,
                        lsn,
                        owner: format!(
                            "[{}] on LSN {lsn}",
                            self.format_lockers(&owner_ids)
                        ),
                        requested_type: lock_type,
                        requester: self.format_locker(locker_id),
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
        //
        // WaitRestart: the waiter's lock_type was changed to Restart in
        // lock_impl::lock(), so the lock was never added to the owner set.
        // Returning RangeRestart tells the caller (lock_ln / put) to abort
        // the current scan and restart — mirroring JE's RangeRestartException.
        let grant = match initial_grant {
            LockGrantType::WaitNew => LockGrantType::New,
            LockGrantType::WaitPromotion => LockGrantType::Promotion,
            LockGrantType::WaitRestart => {
                return Err(TxnError::RangeRestart);
            }
            LockGrantType::IllegalUpgrade => {
                return Err(TxnError::IllegalUpgrade {
                    held: LockType::None,
                    requested: lock_type,
                });
            }
            other => other,
        };

        Ok(grant)
    }

    /// Importunate lock acquisition for HA replay (`ReplayTxn`).
    ///
    /// JE `LockManager.waitForLock`: an importunate locker that would block
    /// steals the lock from preemptable owners instead of waiting
    /// (`if (isImportunate) { result = stealLock(...); if (result.success)
    /// break; continue; }`, LockManager.java:552-557), and deadlock
    /// detection is skipped for it (LockManager.java:472).
    ///
    /// Strategy: attempt non-blocking; if granted, done.  Otherwise steal
    /// from preemptable owners and re-attempt (`stealLockInternal`,
    /// LockManager.java:1599).  If the steal grants the lock, done.  If a
    /// remaining owner is non-preemptable the steal fails, so fall back to a
    /// normal blocking wait (mirroring JE's `continue` — wait for the
    /// non-preemptable holder to release, then retry).
    pub fn lock_importunate_with_timeout(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        non_blocking: bool,
        timeout_ms: u64,
    ) -> Result<LockGrantType, TxnError> {
        if lock_type == LockType::None {
            return Ok(LockGrantType::NoneNeeded);
        }
        if lock_type == LockType::Restart {
            return Err(TxnError::RangeRestart);
        }

        // Try a normal non-blocking attempt first (jumpAheadOfWaiters=false,
        // as JE Locker.lock always passes — Locker.java:503).
        match self.lock(lsn, locker_id, lock_type, true, false) {
            Ok(grant) => return Ok(grant),
            Err(TxnError::LockNotAvailable { .. }) => {
                // Blocked: steal from preemptable owners and re-attempt.
                if self.steal_lock_in_wait(lsn, locker_id, lock_type) {
                    return Ok(LockGrantType::New);
                }
                // A remaining owner is non-preemptable; fall back to a normal
                // blocking wait (JE `continue`).
            }
            Err(e) => return Err(e),
        }

        if non_blocking {
            return Err(TxnError::LockNotAvailable { lsn });
        }
        self.lock_with_timeout(
            lsn, locker_id, lock_type, false, false, timeout_ms,
        )
    }

    /// Releases a lock on the given LSN for the given locker.
    ///
    /// Promotes compatible waiters to owners, signals their condvars so they
    /// wake up, and removes the lock entry when it becomes empty.
    ///
    ///
    pub fn release(&self, lsn: u64, locker_id: i64) -> Result<(), TxnError> {
        let table_idx = self.get_table_index(lsn);
        // Snapshot this locker's share group so the RANGE_INSERT restart-wake
        // check in LockImpl::release_with_sharing can honor sharesLocksWith
        // (JE rangeInsertConflict) without holding the registry across the
        // table borrow.
        let shares_fn = self.build_shares_fn(locker_id);
        let mut table = self.lock_tables[table_idx].lock();

        if let Some(lock) = table.get_mut(&lsn) {
            // release() moves eligible waiters to owners and signals each
            // granted waiter's condvar inside LockImpl::release().
            let _notify_ids = lock.release_with_sharing(locker_id, &shares_fn);

            // If the lock has no owners and no waiters, remove it from the
            // table to free memory.
            if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                table.remove(&lsn);
            }
        }

        Ok(())
    }

    /// Releases every lock currently held by `locker_id`, across all
    /// shards. Returns the number of (lsn, lock) entries the locker
    /// actually owned and released.
    ///
    /// Equivalent to a manual `for lsn in lockers_locks(id): release(lsn, id)`,
    /// but does not require the caller to track which LSNs the locker
    /// has touched. The cleaner uses this in three situations:
    ///
    ///   - **Reaping abandoned cleaner-locker IDs.** `migrate_ln_slot`
    ///     allocates a fresh locker id per migration attempt
    ///     (`next_cleaner_locker_id`), takes a non-blocking read
    ///     lock, and releases. If `release()` fails for any reason
    ///     the entry would otherwise leak, since the locker id is
    ///     never reused. The cleaner can call this method when its
    ///     run terminates to sweep up anything its short-lived
    ///     locker ids left behind.
    ///   - **Catastrophic per-locker abort.** When a deadlock-detector
    ///     victim is too far along to drain its own per-txn write_locks
    ///     map (e.g. it is in the middle of `commit_append_phase`
    ///     and the panic handler needs to clean up), this method
    ///     guarantees the lock-manager view drops the locker even if
    ///     the per-txn view is corrupt.
    ///   - **Test cleanup.** Many integration tests hold a `LockManager`
    ///     across multiple txns and need a quick "drop everything for
    ///     locker N" without re-creating the manager.
    ///
    /// Errors from individual `Lock::release` calls are logged and
    /// the sweep continues; the count returned is the number of
    /// release attempts (each removing the locker from one Lock),
    /// not the number that succeeded — losing one lock release leaks
    /// one entry, but losing the whole sweep would defeat the
    /// purpose.
    pub fn release_all_for_locker(&self, locker_id: i64) -> usize {
        let mut released = 0usize;
        // Snapshot this locker's share group for the RANGE_INSERT restart-wake
        // check (JE rangeInsertConflict / sharesLocksWith).
        let shares_fn = self.build_shares_fn(locker_id);
        for table in &self.lock_tables {
            let mut table = table.lock();
            // Collect target LSNs first to avoid mutating the map
            // while iterating it.
            let target_lsns: Vec<u64> = table
                .iter()
                .filter_map(|(lsn, lock)| {
                    if lock.get_owned_lock_type(locker_id).is_some() {
                        Some(*lsn)
                    } else {
                        None
                    }
                })
                .collect();
            for lsn in target_lsns {
                if let Some(lock) = table.get_mut(&lsn) {
                    let _notify_ids =
                        lock.release_with_sharing(locker_id, &shares_fn);
                    released += 1;
                    if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                        table.remove(&lsn);
                    }
                }
            }
        }
        released
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

    /// Importunate lock steal performed inside the wait loop, mirroring JE
    /// `LockManager.stealLockInternal` (LockManager.java:1599): flush this
    /// locker's waiter entry, remove all preemptable owners, then re-attempt
    /// the lock.  Non-preemptable owners (other importunate lockers) are left
    /// in place, so the re-attempt may still fail — returning `false` tells
    /// the wait loop to keep waiting (LockManager.java:556 `continue`).
    ///
    /// Returns `true` iff the lock is now held by `locker_id`.
    fn steal_lock_in_wait(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
    ) -> bool {
        // Only preempt owners that are not marked non-preemptable.
        let non_preemptable: Option<HashSet<i64>> = {
            let np = self.non_preemptable.read().unwrap();
            if np.is_empty() { None } else { Some(np.clone()) }
        };
        let preemptable_fn = move |owner_id: i64| -> bool {
            match &non_preemptable {
                Some(np) => !np.contains(&owner_id),
                None => true,
            }
        };
        let shares = self.build_shares_fn(locker_id);

        let table_idx = self.get_table_index(lsn);
        let mut table = self.lock_tables[table_idx].lock();
        let lock = table.entry(lsn).or_insert_with(Lock::new_thin);

        // flushWaiter: our waiter entry may still be present.
        lock.flush_waiter(locker_id);
        // stealLock: remove all preemptable owners.
        let _preempted =
            lock.steal_lock_preemptable(locker_id, &preemptable_fn);
        // Re-attempt as a non-blocking, jump-ahead request.
        let result = lock.lock_with_sharing(
            lock_type, locker_id, true,  // non_blocking
            false, // jump_ahead_of_waiters
            &shares,
        );
        result.success
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
        // Single pass over all lock tables to compute live counts. n_waiters
        // and n_owners were previously hardcoded to 0 / lock-count; report the
        // real aggregate so callers (and tests) can observe contention.
        let mut n_total_locks: u64 = 0;
        let mut n_owners: u64 = 0;
        let mut n_waiters: u64 = 0;
        for table in &self.lock_tables {
            let table = table.lock();
            for lock in table.values() {
                n_total_locks += 1;
                n_owners += lock.n_owners() as u64;
                n_waiters += lock.n_waiters() as u64;
            }
        }
        LockStats {
            lock_requests: self.stats.lock_requests.load(Ordering::Relaxed),
            lock_waits: self.stats.lock_waits.load(Ordering::Relaxed),
            n_owners,
            n_waiters,
            n_total_locks,
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

    /// Enumerate locks still held (with at least one owner) at close time.
    ///
    /// Returns `(lsn, owner_locker_ids)` for every outstanding lock, sorted by
    /// LSN.  Used by `env_check_leaks` (JE `EnvironmentImpl` leak checking):
    /// at a clean `Environment::close` all user transactions have committed or
    /// aborted and released their locks, so a non-empty result means an
    /// application leaked a locker (e.g. dropped a `Transaction` without
    /// commit/abort, or held a cursor open).  This is diagnostic only — it
    /// reports, it does not force-release.
    pub fn report_leaked_locks(&self) -> Vec<(u64, Vec<i64>)> {
        let mut leaks: Vec<(u64, Vec<i64>)> = Vec::new();
        for table in &self.lock_tables {
            let table = table.lock();
            for (lsn, lock) in table.iter() {
                if lock.n_owners() > 0 {
                    leaks.push((*lsn, lock.get_owner_ids()));
                }
            }
        }
        leaks.sort_unstable_by_key(|(lsn, _)| *lsn);
        leaks
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
        self.share_registry.write().unwrap().insert(locker_id, group_id);
    }

    /// Removes a locker from the sharing registry.
    ///
    /// Called by `ThreadLocker::drop()` and `HandleLocker::drop()`.
    ///
    ///
    pub fn unregister_locker_sharing(&self, locker_id: i64) {
        self.share_registry.write().unwrap().remove(&locker_id);
    }

    /// Marks `locker_id` as non-preemptable (its locks cannot be stolen).
    ///
    /// Called for importunate lockers (HA `ReplayTxn`).  JE tracks this per
    /// `Locker` via `getPreemptable()`; the lock manager mirrors it so the
    /// importunate steal in the wait loop can honor a non-preemptable owner
    /// (LockManager.java:556).
    pub fn register_non_preemptable(&self, locker_id: i64) {
        self.non_preemptable.write().unwrap().insert(locker_id);
    }

    /// Clears the non-preemptable mark for `locker_id`.
    pub fn unregister_non_preemptable(&self, locker_id: i64) {
        self.non_preemptable.write().unwrap().remove(&locker_id);
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

    /// Builds the `sharesLocksWith` predicate for `locker_id`, used by every
    /// acquisition (`lock_with_timeout`) and release (`release`) so that
    /// `LockImpl` can skip conflict detection between cooperating lockers.
    /// `shares(owner_id)` returns true iff `owner_id` is in `locker_id`'s
    /// sharing group.  JE: `LockImpl.tryLock` / `rangeInsertConflict` consult
    /// `sharesLocksWith` (LockImpl.java:647-648, 719).
    ///
    /// The registry HashMap is cloned only when `locker_id` actually belongs
    /// to a sharing group; the common path (BasicLockers, most internal ops)
    /// shares with no one and returns a closure that allocates nothing.
    fn build_shares_fn(&self, locker_id: i64) -> impl Fn(i64) -> bool + use<> {
        let requester_group: Option<i64> =
            self.share_registry.read().unwrap().get(&locker_id).copied();
        let registry_snapshot: Option<hashbrown::HashMap<i64, i64>> =
            if requester_group.is_some() {
                Some(self.share_registry.read().unwrap().clone())
            } else {
                None
            };
        move |owner_id: i64| -> bool {
            match (requester_group, &registry_snapshot) {
                (Some(req_group), Some(reg)) => {
                    reg.get(&owner_id).copied() == Some(req_group)
                }
                _ => false,
            }
        }
    }

    /// Deprecated alias for `lock_with_timeout()`.
    ///
    /// TXN-F2 fix: the plain `lock()` / `lock_with_timeout()` path now
    /// consults the sharing registry on every acquisition (JE
    /// `LockImpl.tryLock` checks `sharesLocksWith`, LockImpl.java:647-648),
    /// so a separate sharing entry point is redundant.  Kept as a thin
    /// forwarder so existing callers keep compiling; new code should call
    /// `lock` / `lock_with_timeout` directly.
    pub fn lock_with_sharing(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
    ) -> Result<LockGrantType, TxnError> {
        self.lock(
            lsn,
            locker_id,
            lock_type,
            non_blocking,
            jump_ahead_of_waiters,
        )
    }

    /// Deprecated alias for `lock_with_timeout()`; see `lock_with_sharing`.
    pub fn lock_with_sharing_and_timeout(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
        timeout_ms: u64,
    ) -> Result<LockGrantType, TxnError> {
        self.lock_with_timeout(
            lsn,
            locker_id,
            lock_type,
            non_blocking,
            jump_ahead_of_waiters,
            timeout_ms,
        )
    }

    // ========================================================================

    /// Returns the lock table index for a given LSN.
    ///
    ///
    ///
    #[inline]
    /// Returns the configured number of lock-table shards.
    pub fn n_lock_tables(&self) -> usize {
        self.n_lock_tables
    }

    fn get_table_index(&self, lsn: u64) -> usize {
        ((lsn as usize) & 0x7fff_ffff) % self.n_lock_tables
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

    /// Removes `locker_id` from the on-shard waiter list and from the
    /// incremental waiter graph, in canonical lock order (shard first).
    ///
    /// H-2 (2026 audit F-6.2): all victim-cleanup paths must
    /// acquire the shard mutex BEFORE (or without) the waiter_graph mutex.
    /// This helper enforces the ordering: it locks the shard, flushes the
    /// waiter entry, drops the shard guard, then calls `clear_wait()` to
    /// remove from the waiter_graph.  Never call `clear_wait()` before this.
    fn flush_and_clear_waiter(
        &self,
        table_idx: usize,
        lsn: u64,
        locker_id: i64,
    ) {
        // Shard first (canonical ordering).
        {
            let mut table = self.lock_tables[table_idx].lock();
            if let Some(lock) = table.get_mut(&lsn) {
                lock.flush_waiter(locker_id);
                if lock.n_owners() == 0 && lock.n_waiters() == 0 {
                    table.remove(&lsn);
                }
            }
        }
        // Waiter_graph after shard is released.
        self.clear_wait(locker_id);
    }

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
        // Compute per-locker lock counts for the cycle so select_victim can
        // apply its primary criterion (fewest locks held) instead of falling
        // through to the youngest-locker tiebreaker.  This walks every shard,
        // but it only runs when a deadlock cycle has been detected (a rare
        // event), so the scan cost is amortized over the rare deadlock
        // event and is not on the common no-cycle path.
        let lock_counts = self.compute_lock_counts(&cycle);
        let victim = DeadlockDetector::select_victim(&cycle, &lock_counts);

        if victim == locker_id {
            // Format the cycle as typed locker IDs (e.g.
            // `"auto-txn:42 -> txn:17"`) so a mixed auto-commit/explicit-txn
            // deadlock is visibly distinguishable in the error message.
            // Closes the second F12 residual.
            let cycle_fmt = self.format_lockers(&cycle);
            let victim_fmt = self.format_locker(locker_id);
            Some(TxnError::Deadlock(format!(
                "deadlock cycle detected ({cycle_fmt}); {victim_fmt} chosen \
                 as victim while waiting for LSN {lsn} ({lock_type:?})"
            )))
        } else {
            None
        }
    }

    /// Tallies, for every locker_id in `cycle`, the number of locks they
    /// currently hold across all shards.
    ///
    /// Used by deadlock victim selection so the primary criterion (fewest
    /// locks held = least work to roll back) can be applied.  Walks every
    /// shard but is only called after a deadlock cycle has been detected,
    /// so the scan cost is paid only on the rare cycle path, never on the
    /// common no-cycle path.
    fn compute_lock_counts(&self, cycle: &[i64]) -> HashMap<i64, usize> {
        use std::collections::HashSet;
        let cycle_set: HashSet<i64> = cycle.iter().copied().collect();
        let mut counts: HashMap<i64, usize> =
            cycle.iter().copied().map(|id| (id, 0usize)).collect();
        for shard in &self.lock_tables {
            let table = shard.lock();
            for lock in table.values() {
                for owner_id in lock.get_owner_ids() {
                    if cycle_set.contains(&owner_id) {
                        *counts.entry(owner_id).or_insert(0) += 1;
                    }
                }
            }
        }
        counts
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
    fn test_report_leaked_locks() {
        let lm = LockManager::new();
        // No locks held: no leaks.
        assert!(lm.report_leaked_locks().is_empty());

        // Deliberately acquire two write locks for locker 42 and never release
        // one of them.
        lm.lock(1000, 42, LockType::Write, false, false).unwrap();
        lm.lock(2000, 42, LockType::Write, false, false).unwrap();

        let leaks = lm.report_leaked_locks();
        assert_eq!(leaks.len(), 2, "both held locks must be reported");
        // Sorted by LSN; each reports locker 42 as an owner.
        assert_eq!(leaks[0].0, 1000);
        assert_eq!(leaks[1].0, 2000);
        assert!(leaks[0].1.contains(&42));
        assert!(leaks[1].1.contains(&42));

        // Release one: only the still-held lock is reported.
        lm.release(1000, 42).unwrap();
        let leaks = lm.report_leaked_locks();
        assert_eq!(leaks.len(), 1, "released lock must not be reported");
        assert_eq!(leaks[0].0, 2000);

        // Release all for the locker: no leaks remain.
        lm.release_all_for_locker(42);
        assert!(
            lm.report_leaked_locks().is_empty(),
            "no leaks after all locks released"
        );
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
        assert!(idx1 < lm.n_lock_tables);
        assert!(idx2 < lm.n_lock_tables);
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

    /// TXN-F3 regression: an importunate locker steals a held, conflicting
    /// lock from a preemptable owner instead of waiting/timing out.  JE
    /// `LockManager.waitForLock`: `if (isImportunate) { result =
    /// stealLock(...); if (result.success) break; }` (LockManager.java:552).
    #[test]
    fn test_importunate_steals_conflicting_lock() {
        const LSN: u64 = 4242;
        let lm = LockManager::new();

        // Owner 1 (preemptable) holds a Write lock.
        assert_eq!(
            lm.lock(LSN, 1, LockType::Write, false, false).unwrap(),
            LockGrantType::New
        );
        assert!(lm.is_owned_write_lock(LSN, 1));

        // Importunate locker 2 requests a conflicting Write lock.  Use a
        // non_blocking=false call with a SHORT timeout: pre-fix this would
        // either time out (LockTimeout) or block; post-fix the steal grants
        // it immediately.
        let grant = lm
            .lock_importunate_with_timeout(LSN, 2, LockType::Write, false, 200)
            .expect("importunate locker must steal the conflicting lock");
        assert!(matches!(grant, LockGrantType::New | LockGrantType::Existing));

        // The steal preempted owner 1; locker 2 now holds the write lock.
        assert!(lm.is_owned_write_lock(LSN, 2));
        assert!(!lm.is_owned_write_lock(LSN, 1));
    }

    /// A non-preemptable owner blocks the steal: the importunate request
    /// falls back to a normal wait and times out rather than stealing.
    /// JE: "Lock holder is non-preemptable, wait again" (LockManager.java:556).
    #[test]
    fn test_importunate_cannot_steal_non_preemptable() {
        const LSN: u64 = 4343;
        let lm = LockManager::new();

        // Owner 1 is non-preemptable (another importunate locker).
        lm.register_non_preemptable(1);
        assert_eq!(
            lm.lock(LSN, 1, LockType::Write, false, false).unwrap(),
            LockGrantType::New
        );

        // Importunate locker 2 cannot steal; with a short timeout it must
        // time out, and owner 1 keeps the lock.
        let r = lm.lock_importunate_with_timeout(
            LSN,
            2,
            LockType::Write,
            false,
            100,
        );
        assert!(matches!(r, Err(TxnError::LockTimeout { .. })));
        assert!(lm.is_owned_write_lock(LSN, 1));
        assert!(!lm.is_owned_write_lock(LSN, 2));
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

            lm_b.lock_with_timeout(LSN, 2, LockType::Write, false, false, 5000)
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
        assert!(result.is_ok(), "thread B expected Ok, got {:?}", result);
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
            lm_a.lock_with_timeout(
                LSN_Y,
                1,
                LockType::Write,
                false,
                false,
                3000,
            )
        });

        // Give A a moment to register as waiter.
        thread::sleep(Duration::from_millis(50));

        let b = thread::spawn(move || {
            lm_b.lock_with_timeout(
                LSN_X,
                2,
                LockType::Write,
                false,
                false,
                3000,
            )
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
            assert!(result.is_ok(), "reader expected Ok, got {:?}", result);
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
    // Ported from LockManagerTest — distinct-LSN distribution across shards
    // -----------------------------------------------------------------------

    /// Verify shard distribution: distinct LSNs are independently managed
    /// regardless of the configured shard count (default 64).
    #[test]
    fn test_with_config_shard_count_honored() {
        // DRIFT-2 regression: the shard count must come from config, not a
        // hardcoded constant. default new() = 64; with_config respects N.
        assert_eq!(LockManager::new().n_lock_tables(), 64);
        assert_eq!(LockManager::with_config(500, 8).n_lock_tables(), 8);
        // Clamped to at least 1.
        assert_eq!(LockManager::with_config(500, 0).n_lock_tables(), 1);
        // get_table_index stays within the configured shard count.
        let lm = LockManager::with_config(500, 8);
        for lsn in [1u64, 7, 64, 1000, u64::MAX] {
            assert!(lm.get_table_index(lsn) < 8);
        }
    }

    #[test]
    fn test_je_sixteen_lock_tables() {
        // Distribute 16 distinct LSNs and check all are independently managed
        // (shard count is an instance field, default 64).
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
                assert_eq!(
                    lm2.get_owned_lock_type(LSN, locker_id),
                    Some(LockType::Read)
                );
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
        assert!(
            result.is_ok(),
            "writer should have been granted, got {:?}",
            result
        );
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
        assert!(
            r3.is_ok(),
            "locker 3 should succeed after locker 2, got {:?}",
            r3
        );
        lm.release(LSN, 3).unwrap();
    }

    // -----------------------------------------------------------------------
    // release_all_for_locker
    // -----------------------------------------------------------------------

    #[test]
    fn release_all_for_locker_returns_count() {
        let lm = LockManager::new();
        // Locker 7 takes 5 locks, locker 8 takes 2.
        for lsn in [10u64, 20, 30, 40, 50] {
            lm.lock(lsn, 7, LockType::Read, false, false).unwrap();
        }
        for lsn in [100u64, 200] {
            lm.lock(lsn, 8, LockType::Write, false, false).unwrap();
        }
        assert_eq!(lm.n_total_locks(), 7);

        let released = lm.release_all_for_locker(7);
        assert_eq!(released, 5);
        // Only locker 8's 2 locks remain.
        assert_eq!(lm.n_total_locks(), 2);

        let released2 = lm.release_all_for_locker(8);
        assert_eq!(released2, 2);
        assert_eq!(lm.n_total_locks(), 0);
    }

    #[test]
    fn release_all_for_locker_unknown_id_is_zero() {
        let lm = LockManager::new();
        lm.lock(1, 1, LockType::Read, false, false).unwrap();
        let released = lm.release_all_for_locker(999);
        assert_eq!(released, 0);
        assert_eq!(lm.n_total_locks(), 1);
        lm.release(1, 1).unwrap();
    }

    #[test]
    fn release_all_for_locker_idempotent() {
        // Calling twice is safe — second call reaps zero entries.
        let lm = LockManager::new();
        lm.lock(1, 1, LockType::Read, false, false).unwrap();
        lm.lock(2, 1, LockType::Write, false, false).unwrap();
        assert_eq!(lm.release_all_for_locker(1), 2);
        assert_eq!(lm.release_all_for_locker(1), 0);
    }

    #[test]
    fn release_all_for_locker_preserves_other_owners() {
        // Multiple lockers sharing a read lock at the same LSN: releasing
        // one locker leaves the others' entry intact.
        let lm = LockManager::new();
        lm.lock(1, 1, LockType::Read, false, false).unwrap();
        lm.lock(1, 2, LockType::Read, false, false).unwrap();
        lm.lock(1, 3, LockType::Read, false, false).unwrap();

        let released = lm.release_all_for_locker(2);
        assert_eq!(released, 1);
        // Lock entry persists because lockers 1 and 3 still own it.
        assert_eq!(lm.n_total_locks(), 1);

        // Verify locker 2 no longer has it.
        let released_again = lm.release_all_for_locker(2);
        assert_eq!(released_again, 0);

        lm.release(1, 1).unwrap();
        lm.release(1, 3).unwrap();
        assert_eq!(lm.n_total_locks(), 0);
    }

    #[test]
    fn release_all_for_locker_clears_lock_when_last_owner_leaves() {
        let lm = LockManager::new();
        lm.lock(42, 1, LockType::Write, false, false).unwrap();
        assert_eq!(lm.n_total_locks(), 1);
        lm.release_all_for_locker(1);
        // Lock entry was the last owner of LSN 42 — entry removed.
        assert_eq!(lm.n_total_locks(), 0);
    }

    /// H-2 regression: verify that no internal deadlock occurs when the lock
    /// manager processes concurrent waiter registrations and deadlock-victim
    /// cleanups.  Before this fix, different code paths acquired shard and
    /// waiter_graph mutexes in inconsistent order, creating a potential
    /// process hang under extreme contention.
    ///
    /// The test spawns two threads:
    ///   Thread A: holds a write lock on LSN 1, then waits on LSN 2.
    ///   Thread B: holds a write lock on LSN 2, then waits on LSN 1.
    /// This is a classic 2-txn deadlock cycle.  The lock manager must detect
    /// it (aborting one victim) and complete without hanging.  The 2-second
    /// timeout is the safety net.
    #[test]
    fn test_lock_ordering_no_internal_deadlock() {
        use std::sync::{Arc, Barrier};
        use std::thread;
        use std::time::Duration;

        let lm = Arc::new(LockManager::new());
        const LSN_A: u64 = 0xDEAD_0001;
        const LSN_B: u64 = 0xDEAD_0002;
        const LOCKER_A: i64 = 1001;
        const LOCKER_B: i64 = 1002;

        // Both threads acquire their first lock before trying for the second.
        let barrier = Arc::new(Barrier::new(2));

        let lm_a = Arc::clone(&lm);
        let barrier_a = Arc::clone(&barrier);
        let t_a = thread::spawn(move || {
            // Locker A grabs LSN_A, then tries to grab LSN_B (held by B).
            lm_a.lock(LSN_A, LOCKER_A, LockType::Write, false, false).unwrap();
            barrier_a.wait(); // both sides have their first lock
            lm_a.lock(LSN_B, LOCKER_A, LockType::Write, false, false)
        });

        let lm_b = Arc::clone(&lm);
        let barrier_b = Arc::clone(&barrier);
        let t_b = thread::spawn(move || {
            // Locker B grabs LSN_B, then tries to grab LSN_A (held by A).
            lm_b.lock(LSN_B, LOCKER_B, LockType::Write, false, false).unwrap();
            barrier_b.wait(); // both sides have their first lock
            lm_b.lock(LSN_A, LOCKER_B, LockType::Write, false, false)
        });

        // One thread must deadlock; the other must complete.  Neither should hang.
        let res_a = t_a.join();
        let res_b = t_b.join();

        // Exactly one of the two must be a deadlock error.
        let both = [res_a, res_b];
        let n_deadlocks = both
            .iter()
            .filter(|r| matches!(r, Ok(Err(TxnError::Deadlock(_)))))
            .count();
        let n_success = both.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();
        // Allow for timeout as well (one deadlock or one timeout + one success)
        assert!(
            (n_deadlocks == 1 && n_success <= 1) || n_deadlocks == 2,
            "expected at least one deadlock error, got: n_deadlocks={n_deadlocks} n_success={n_success}"
        );
        let _ = Duration::from_secs(0); // suppress unused import warning
    }

    /// H-4 regression: when select_victim has populated lock_counts, the
    /// transaction holding the fewest locks is chosen, regardless of which
    /// is youngest.
    ///
    /// Construct a 2-locker cycle where the *older* (lower-id) locker holds
    /// many additional locks and the *younger* (higher-id) locker holds
    /// only the cycle lock plus a couple more, then verify the younger
    /// locker is selected.  (With the previous bug, lock_counts was always
    /// empty so select_victim fell through to the youngest-tiebreaker; the
    /// younger would be chosen *for the wrong reason*.  This test pins the
    /// counts so the primary criterion drives the choice.)
    #[test]
    fn test_h4_victim_selection_uses_lock_counts() {
        let lm = Arc::new(LockManager::new());
        // L_OLD is held by locker 1 (older, holds 5 unrelated locks).
        const L_OLD: u64 = 0x6001;
        // L_NEW is held by locker 2 (younger, holds 0 unrelated locks).
        const L_NEW: u64 = 0x6002;

        // Locker 1 owns 5 unrelated locks then takes L_OLD.
        for i in 0..5 {
            lm.lock(0x7000 + i, 1, LockType::Write, false, false).unwrap();
        }
        lm.lock(L_OLD, 1, LockType::Write, false, false).unwrap();

        // Locker 2 owns 0 unrelated locks, then takes L_NEW.
        lm.lock(L_NEW, 2, LockType::Write, false, false).unwrap();

        // Compute counts on the cycle [1, 2].
        let counts = lm.compute_lock_counts(&[1, 2]);
        assert_eq!(
            counts.get(&1).copied().unwrap_or(0),
            6,
            "locker 1 holds 6 locks"
        );
        assert_eq!(
            counts.get(&2).copied().unwrap_or(0),
            1,
            "locker 2 holds 1 lock"
        );

        // select_victim with these counts must pick locker 2 (fewest locks).
        let victim = DeadlockDetector::select_victim(&[1, 2], &counts);
        assert_eq!(victim, 2, "victim must be locker 2 (fewest locks held)");
    }

    /// TXN-1 regression test: `lock_with_sharing_and_timeout` must detect a
    /// deadlock formed WHILE waiting (not only after a 50 ms slice).
    ///
    /// Setup: two lockers on the sharing path (HandleLocker-like) each hold
    /// a lock the other wants. The deadlock must be detected and returned as
    /// `DeadlockException` well within the test timeout, NOT after 50 ms.
    ///
    /// This is a structural test rather than a timing test: we form a clear
    /// two-node cycle via `lock_with_sharing_and_timeout` and verify the error
    /// is a deadlock (not a timeout). Prior to the fix, the check only fired
    /// on `timed_out.timed_out()` with stale owner IDs, so a deadlock on the
    /// sharing path could wait a full 50 ms slice before being detected.
    #[test]
    fn test_txn1_sharing_path_deadlock_detected_promptly() {
        use std::sync::Barrier;
        use std::thread;

        let lm = Arc::new(LockManager::new());
        const L1: u64 = 0xA001;
        const L2: u64 = 0xA002;
        const LOCKER_A: i64 = 101;
        const LOCKER_B: i64 = 102;
        const TIMEOUT_MS: u64 = 5_000;

        // A holds L1; B holds L2.
        lm.lock(L1, LOCKER_A, LockType::Write, false, false).unwrap();
        lm.lock(L2, LOCKER_B, LockType::Write, false, false).unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let lm_b = Arc::clone(&lm);
        let barrier_b = Arc::clone(&barrier);

        // Thread B tries to acquire L1 (blocked by A).
        let handle = thread::spawn(move || {
            barrier_b.wait();
            lm_b.lock_with_sharing_and_timeout(
                L1,
                LOCKER_B,
                LockType::Write,
                false,
                false,
                TIMEOUT_MS,
            )
        });

        // Main thread: wait until B is queued, then try L2 (blocked by B).
        barrier.wait();
        // Small yield to let B enter the wait loop before we enqueue ourselves.
        std::thread::sleep(Duration::from_millis(5));
        let result_a = lm.lock_with_sharing_and_timeout(
            L2,
            LOCKER_A,
            LockType::Write,
            false,
            false,
            TIMEOUT_MS,
        );

        let result_b = handle.join().expect("thread B panicked");

        // Exactly one of the two must get a DeadlockException; the other
        // should succeed or also get a deadlock. Both getting deadlock is fine.
        let a_dl = matches!(result_a, Err(TxnError::Deadlock(..)));
        let b_dl = matches!(result_b, Err(TxnError::Deadlock(..)));
        assert!(
            a_dl || b_dl,
            "TXN-1: expected at least one DeadlockException on the sharing path; \
             got A={result_a:?}, B={result_b:?}"
        );
    }
}
