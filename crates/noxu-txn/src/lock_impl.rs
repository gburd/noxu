//! Full lock implementation with support for multiple owners and waiters.
//!

use crate::{
    LockAttemptResult, LockConflict, LockGrantType, LockInfo, LockType,
    LockUpgrade, lock_info::WaiterNotify,
};

/// A Lock embodies the lock state of an LSN.
/// It includes a set of owners and a list of waiters.
///
/// The owners set is always in one of the following states:
/// 1. Empty
/// 2. A single writer
/// 3. One or more readers
/// 4. Multiple writers or a mix of readers and writers, all for
///    txns which share locks (all ThreadLocker instances for the same thread)
///
/// Both ownerSet and waiterList are a collection of LockInfo. Since the
/// common case is that there is only one owner or waiter, we have added an
/// optimization to avoid the cost of collections. FirstOwner and
/// firstWaiter are used for the first owner or waiter of the lock, and the
/// corresponding collection is instantiated and used only if more owners arrive.
///
///
#[derive(Debug)]
pub struct LockImpl {
    /// First owner (optimization for single-owner case).
    first_owner: Option<LockInfo>,
    /// Additional owners (only allocated if > 1 owner).
    owner_set: Option<Vec<LockInfo>>,
    /// First waiter (optimization for single-waiter case).
    first_waiter: Option<LockInfo>,
    /// Additional waiters (only allocated if > 1 waiter).
    waiter_list: Option<Vec<LockInfo>>,
}

impl LockImpl {
    /// Create a new empty Lock.
    pub fn new() -> Self {
        Self {
            first_owner: None,
            owner_set: None,
            first_waiter: None,
            waiter_list: None,
        }
    }

    /// Create a Lock from an existing one (used when releasing lock).
    pub fn from_lock(lock: &LockImpl) -> Self {
        Self {
            first_owner: lock.first_owner.clone(),
            owner_set: lock.owner_set.clone(),
            first_waiter: lock.first_waiter.clone(),
            waiter_list: lock.waiter_list.clone(),
        }
    }

    /// Create a Lock from a single owner (used when mutating from ThinLockImpl).
    pub fn from_first_owner(first_owner: LockInfo) -> Self {
        Self {
            first_owner: Some(first_owner),
            owner_set: None,
            first_waiter: None,
            waiter_list: None,
        }
    }

    /// Attempts to acquire the lock and returns the LockAttemptResult.
    ///
    /// Algorithm (from the):
    /// 1. Check if locker already owns this lock -> check upgrade
    /// 2. If not owner, check for conflicts with all owners
    /// 3. If no conflict and no waiters -> grant NEW
    /// 4. If conflict or waiters exist -> WAIT_NEW or DENIED
    ///
    /// Assumes we hold the lockTableLatch when entering this method.
    #[inline]
    pub fn lock(
        &mut self,
        request_type: LockType,
        locker_id: i64,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
    ) -> LockAttemptResult {
        // No lock-sharing groups by default; delegate to the sharing variant
        // with a predicate that never shares.  This keeps a single canonical
        // implementation (including the restart-conflict waiter scan) and
        // mirrors how `try_lock` delegates to `try_lock_with_sharing`.
        self.lock_with_sharing(
            request_type,
            locker_id,
            non_blocking,
            jump_ahead_of_waiters,
            &|_| false,
        )
    }

    /// Releases a lock held by the given locker.
    ///
    /// Moves the next eligible waiter(s) from the waiter list to the owner set.
    /// For each newly-granted waiter that has a notify pair attached, this method
    /// sets the "granted" flag and signals the condvar so the blocked thread wakes
    /// up.  This mirrors `LockManager.release()` -> `notifyAll()` flow.
    ///
    /// Returns the locker IDs of all waiters that were promoted to owners, or
    /// `None` if the given locker was not an owner.
    pub fn release(&mut self, locker_id: i64) -> Option<Vec<i64>> {
        // No lock-sharing groups by default (preserves prior behavior for
        // callers without a sharing registry).
        self.release_with_sharing(locker_id, &|_| false)
    }

    /// As `release`, but consults `shares_fn` when deciding whether a
    /// RANGE_INSERT owner conflicts with a RESTART waiter being woken
    /// (JE `rangeInsertConflict` uses `sharesLocksWith`). `shares_fn(owner_id)`
    /// returns true when the owner is in the woken waiter's lock-sharing group.
    pub fn release_with_sharing<F: Fn(i64) -> bool>(
        &mut self,
        locker_id: i64,
        shares_fn: &F,
    ) -> Option<Vec<i64>> {
        let removed_lock = self.flush_owner(locker_id);
        removed_lock.as_ref()?;

        if self.n_waiters() == 0 {
            // No more waiters, so no one to notify.
            return Some(Vec::new());
        }

        // Move the next set of waiters to the owners set. Iterate through the
        // firstWaiter field, then the waiterList.
        //
        // (LockImpl.release): "Move the next set of waiters to the owners
        // set.  Iterate through the firstWaiter field, then the waiterList."
        //
        // NOTE: first_waiter may be None even when waiter_list has entries
        // (this can happen when a head-of-list waiter was granted in a prior
        // release pass, leaving first_waiter null but waiter_list intact).
        // We must fall through to waiter_list in that case.
        let mut notify_set = Vec::new();
        let mut waiter_idx = 0;

        loop {
            // Map waiter_idx to a storage slot:
            //   0         -> first_waiter (may be None; if so, skip to waiter_list[0])
            //   1..       -> waiter_list[waiter_idx - 1]
            let waiter = if waiter_idx == 0 {
                if self.first_waiter.is_some() {
                    self.first_waiter.clone()
                } else {
                    // first_waiter slot empty; fall through to waiter_list[0].
                    waiter_idx += 1;
                    self.waiter_list.as_ref().and_then(|l| l.first().cloned())
                }
            } else if let Some(ref list) = self.waiter_list {
                list.get(waiter_idx - 1).cloned()
            } else {
                None
            };

            match waiter {
                Some(w) => {
                    // Make the waiter an owner if the lock can be acquired.
                    let waiter_type = w.lock_type;
                    let waiter_locker = w.locker_id;
                    // Capture the notify pair before the waiter entry is
                    // consumed by try_lock (which calls add_owner, moving the
                    // LockInfo into the owner set and losing the notify field).
                    let notify_pair = w.notify.clone();
                    let grant = if waiter_type == LockType::Restart {
                        // Special case for restarts: see rangeInsertConflict.
                        if self.range_insert_conflict_with_sharing(
                            waiter_locker,
                            shares_fn,
                        ) {
                            LockGrantType::WaitNew
                        } else {
                            LockGrantType::New
                        }
                    } else {
                        // Try locking.
                        self.try_lock(w.clone(), true)
                    };

                    // Check if granted.
                    if grant == LockGrantType::New
                        || grant == LockGrantType::Existing
                        || grant == LockGrantType::Promotion
                    {
                        // Remove it from the waiters list.
                        // waiter_idx==0 means it came from first_waiter; any
                        // higher index means waiter_list[waiter_idx - 1].
                        if waiter_idx == 0 {
                            self.first_waiter = None;
                            // Don't increment; the next waiter (if any) in
                            // waiter_list[0] will be picked up on the next
                            // loop iteration when we skip the empty first_waiter.
                        } else {
                            if let Some(ref mut list) = self.waiter_list {
                                list.remove(waiter_idx - 1);
                                // After removal from list, don't increment —
                                // the next item has shifted into waiter_idx-1.
                            }
                        }
                        notify_set.push(waiter_locker);

                        // Wake the waiting thread.  calls notifyAll() on
                        // the locker object here; we signal the per-waiter
                        // condvar instead.
                        if let Some(pair) = notify_pair {
                            let (mutex, condvar) = &*pair;
                            let mut granted = mutex.lock();
                            *granted = true;
                            condvar.notify_all();
                        }
                    } else {
                        debug_assert!(
                            grant == LockGrantType::WaitNew
                                || grant == LockGrantType::WaitPromotion
                                || grant == LockGrantType::WaitRestart
                        );
                        // Stop on first waiter that cannot be an owner.
                        break;
                    }
                }
                None => break,
            }
        }

        Some(notify_set)
    }

    /// Downgrade a write lock to a read lock.
    pub fn demote(&mut self, locker_id: i64) {
        if let Some(owner) = self.get_owner_lock_info(locker_id) {
            let lock_type = owner.lock_type;
            if lock_type.is_write_lock() {
                let new_type = if lock_type == LockType::RangeWrite {
                    LockType::RangeRead
                } else {
                    LockType::Read
                };
                self.update_owner_type(locker_id, new_type);
            }
        }
    }

    /// Remove all owners except the given one (lock stealing for HA).
    /// Returns the list of locker IDs that were preempted.
    pub fn steal_lock(&mut self, locker_id: i64) -> Vec<i64> {
        self.steal_lock_preemptable(locker_id, &|_| true)
    }

    /// As `steal_lock`, but only removes owners for which `preemptable_fn`
    /// returns true.  JE `LockImpl.stealLock` skips owners whose locker is
    /// non-preemptable (`thisLocker.getPreemptable()`, LockImpl.java:543/557).
    /// Returns the locker IDs that were actually preempted (removed).
    pub fn steal_lock_preemptable<F: Fn(i64) -> bool>(
        &mut self,
        locker_id: i64,
        preemptable_fn: &F,
    ) -> Vec<i64> {
        let mut preempted = Vec::new();

        if let Some(ref owner) = self.first_owner
            && owner.locker_id != locker_id
            && preemptable_fn(owner.locker_id)
        {
            preempted.push(owner.locker_id);
            self.first_owner = None;
        }

        if let Some(ref mut set) = self.owner_set {
            set.retain(|info| {
                if info.locker_id != locker_id && preemptable_fn(info.locker_id)
                {
                    preempted.push(info.locker_id);
                    false
                } else {
                    true
                }
            });
        }

        preempted
    }

    /// Remove a waiter from the waiter list.
    pub fn flush_waiter(&mut self, locker_id: i64) {
        if let Some(ref waiter) = self.first_waiter
            && waiter.locker_id == locker_id
        {
            self.first_waiter = None;
            return;
        }

        if let Some(ref mut list) = self.waiter_list {
            list.retain(|info| info.locker_id != locker_id);
        }
    }

    /// Attach a notify pair to the waiter entry for `locker_id`.
    ///
    /// Called by `LockManager::lock()` after `Lock::lock()` has registered the
    /// waiter entry and before the calling thread begins to wait.  Matching is by
    /// locker_id; the entry must already be in the waiter list.
    pub fn set_waiter_notify(&mut self, locker_id: i64, notify: WaiterNotify) {
        if let Some(ref mut waiter) = self.first_waiter
            && waiter.locker_id == locker_id
        {
            waiter.notify = Some(notify);
            return;
        }
        if let Some(ref mut list) = self.waiter_list {
            for waiter in list.iter_mut() {
                if waiter.locker_id == locker_id {
                    waiter.notify = Some(notify);
                    return;
                }
            }
        }
    }

    /// Return true if locker is an owner of this Lock for lockType.
    pub fn is_owner(&self, locker_id: i64, lock_type: LockType) -> bool {
        self.get_owner_lock_info(locker_id)
            .is_some_and(|info| info.lock_type == lock_type)
    }

    /// Return true if locker is an owner of this Lock and this is a write lock.
    pub fn is_owned_write_lock(&self, locker_id: i64) -> bool {
        self.get_owner_lock_info(locker_id)
            .is_some_and(|info| info.lock_type.is_write_lock())
    }

    /// Return the lock type owned by this locker, or None if not an owner.
    pub fn get_owned_lock_type(&self, locker_id: i64) -> Option<LockType> {
        self.get_owner_lock_info(locker_id).map(|info| info.lock_type)
    }

    /// Return true if locker is a waiter on this Lock.
    pub fn is_waiter(&self, locker_id: i64) -> bool {
        if let Some(ref waiter) = self.first_waiter
            && waiter.locker_id == locker_id
        {
            return true;
        }

        if let Some(ref list) = self.waiter_list {
            return list.iter().any(|info| info.locker_id == locker_id);
        }

        false
    }

    /// Return the number of waiters.
    pub fn n_waiters(&self) -> usize {
        let mut count = 0;
        if self.first_waiter.is_some() {
            count += 1;
        }
        if let Some(ref list) = self.waiter_list {
            count += list.len();
        }
        count
    }

    /// Return the number of owners.
    pub fn n_owners(&self) -> usize {
        let mut count = 0;
        if self.first_owner.is_some() {
            count += 1;
        }
        if let Some(ref set) = self.owner_set {
            count += set.len();
        }
        count
    }

    /// Return the locker ID that has a write ownership on this lock.
    /// If no write owner exists, return None.
    pub fn get_write_owner_locker_id(&self) -> Option<i64> {
        if let Some(ref owner) = self.first_owner
            && owner.lock_type.is_write_lock()
        {
            return Some(owner.locker_id);
        }

        if let Some(ref set) = self.owner_set {
            for owner in set {
                if owner.lock_type.is_write_lock() {
                    return Some(owner.locker_id);
                }
            }
        }

        None
    }

    /// Get a clone of the owners list for debugging.
    pub fn get_owners_clone(&self) -> Vec<LockInfo> {
        let mut owners = Vec::new();
        if let Some(ref owner) = self.first_owner {
            owners.push(owner.clone());
        }
        if let Some(ref set) = self.owner_set {
            owners.extend(set.iter().cloned());
        }
        owners
    }

    /// Get a clone of the waiters list for debugging.
    pub fn get_waiters_clone(&self) -> Vec<LockInfo> {
        let mut waiters = Vec::new();
        if let Some(ref waiter) = self.first_waiter {
            waiters.push(waiter.clone());
        }
        if let Some(ref list) = self.waiter_list {
            waiters.extend(list.iter().cloned());
        }
        waiters
    }

    // Private helper methods

    /// The first waiter goes into the firstWaiter member variable. Once the
    /// waiterList is made, all appended waiters go into waiterList, even after
    /// the firstWaiter goes away and leaves that field null, so as to leave the
    /// list ordered.
    fn add_waiter_to_end_of_list(&mut self, waiter: LockInfo) {
        match self.waiter_list.as_mut() {
            Some(list) => list.push(waiter),
            None => {
                if self.first_waiter.is_none() {
                    self.first_waiter = Some(waiter);
                } else {
                    self.waiter_list = Some(vec![waiter]);
                }
            }
        }
    }

    /// Add this waiter to the front of the list.
    fn add_waiter_to_head_of_list(&mut self, waiter: LockInfo) {
        // Shuffle the current first waiter down a slot.
        if let Some(current_first) = self.first_waiter.take() {
            if self.waiter_list.is_none() {
                self.waiter_list = Some(Vec::new());
            }
            self.waiter_list.as_mut().unwrap().insert(0, current_first);
        }

        self.first_waiter = Some(waiter);
    }

    /// Add an owner to this lock.
    fn add_owner(&mut self, new_lock: LockInfo) {
        if self.first_owner.is_none() {
            self.first_owner = Some(new_lock);
        } else {
            if self.owner_set.is_none() {
                self.owner_set = Some(Vec::new());
            }
            self.owner_set.as_mut().unwrap().push(new_lock);
        }
    }

    /// Remove this locker from the owner set and return the removed LockInfo.
    fn flush_owner(&mut self, locker_id: i64) -> Option<LockInfo> {
        if let Some(ref owner) = self.first_owner
            && owner.locker_id == locker_id
        {
            return self.first_owner.take();
        }

        if let Some(ref mut set) = self.owner_set
            && let Some(pos) = set.iter().position(|o| o.locker_id == locker_id)
        {
            return Some(set.remove(pos));
        }

        None
    }

    /// Returns the owner LockInfo for a locker, or None if locker is not an owner.
    fn get_owner_lock_info(&self, locker_id: i64) -> Option<&LockInfo> {
        if let Some(ref owner) = self.first_owner
            && owner.locker_id == locker_id
        {
            return Some(owner);
        }

        if let Some(ref set) = self.owner_set {
            return set.iter().find(|o| o.locker_id == locker_id);
        }

        None
    }

    /// Update the lock type for an existing owner.
    fn update_owner_type(&mut self, locker_id: i64, new_type: LockType) {
        if let Some(ref mut owner) = self.first_owner
            && owner.locker_id == locker_id
        {
            owner.lock_type = new_type;
            return;
        }

        if let Some(ref mut set) = self.owner_set
            && let Some(owner) =
                set.iter_mut().find(|o| o.locker_id == locker_id)
        {
            owner.lock_type = new_type;
        }
    }

    /// Like `lock()` but uses a sharing predicate to skip conflict detection
    /// between cooperating lockers.
    ///
    /// When `!locker.sharesLocksWith(ownerLocker)`
    /// evaluates to false (they *do* share), the conflict matrix is skipped.
    /// This allows multiple ThreadLockers on the same thread to co-own a lock
    /// without conflicts.
    ///
    /// `shares_fn(owner_id)` should return `true` if the requesting locker
    /// (`locker_id`) shares locks with `owner_id`.
    #[inline]
    pub fn lock_with_sharing<F: Fn(i64) -> bool>(
        &mut self,
        request_type: LockType,
        locker_id: i64,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
        shares_fn: &F,
    ) -> LockAttemptResult {
        let new_lock = LockInfo::new(locker_id, request_type);
        let mut grant = self.try_lock_with_sharing(
            new_lock.clone(),
            jump_ahead_of_waiters || self.n_waiters() == 0,
            shares_fn,
        );

        if grant == LockGrantType::WaitNew
            || grant == LockGrantType::WaitPromotion
            || grant == LockGrantType::WaitRestart
        {
            if request_type.causes_restart()
                && grant != LockGrantType::WaitRestart
            {
                let mut waiter_idx = 0;
                loop {
                    let waiter = if waiter_idx == 0 {
                        self.first_waiter.as_ref()
                    } else if let Some(ref list) = self.waiter_list {
                        list.get(waiter_idx - 1)
                    } else {
                        None
                    };
                    match waiter {
                        Some(w) => {
                            // Ignore LockType::Restart in the waiter list and
                            // skip a waiter the requestor shares locks with.
                            // JE: `waiterType != RESTART && locker !=
                            // waiterLocker && !locker.sharesLocksWith(
                            // waiterLocker)` (LockImpl.java:395).
                            if w.lock_type != LockType::Restart
                                && locker_id != w.locker_id
                                && !shares_fn(w.locker_id)
                            {
                                let conflict =
                                    w.lock_type.get_conflict(request_type);
                                if conflict == LockConflict::Restart {
                                    grant = LockGrantType::WaitRestart;
                                    break;
                                }
                            }
                            waiter_idx += 1;
                        }
                        None => break,
                    }
                }
            }

            if non_blocking {
                grant = LockGrantType::Denied;
            } else {
                if grant == LockGrantType::WaitPromotion {
                    self.add_waiter_to_head_of_list(new_lock);
                } else {
                    let mut waiter = new_lock;
                    if grant == LockGrantType::WaitRestart {
                        waiter.lock_type = LockType::Restart;
                    }
                    self.add_waiter_to_end_of_list(waiter);
                }
            }
        }

        LockAttemptResult::new(grant)
    }

    /// Called from lock() to try locking a new request, and from release() to
    /// try locking a waiting request.
    ///
    /// @param new_lock is the lock that is requested.
    ///
    /// @param first_waiter_in_line determines whether to grant the lock when a
    /// NEW lock can be granted, but other non-conflicting owners exist; for
    /// example, when a new READ lock is requested but READ locks are held by
    /// other owners. This parameter should be true if the requestor is the
    /// first waiter in line (or if there are no waiters), and false otherwise.
    ///
    /// @return LockGrantType::EXISTING, NEW, PROMOTION, WAIT_RESTART, WAIT_NEW
    /// or WAIT_PROMOTION.
    #[inline]
    fn try_lock(
        &mut self,
        new_lock: LockInfo,
        first_waiter_in_line: bool,
    ) -> LockGrantType {
        self.try_lock_with_sharing(new_lock, first_waiter_in_line, &|_| false)
    }

    /// Inner `try_lock` with a sharing predicate.
    ///
    /// `LockImpl.tryLock(LockInfo newLock, boolean firstWaiterInLine)` —
    /// when `locker.sharesLocksWith(ownerLocker)` is true, the conflict matrix
    /// is skipped and the lock is co-granted.  This allows multiple
    /// ThreadLockers on the same thread to share a lock without deadlock.
    ///
    ///
    #[inline]
    fn try_lock_with_sharing<F: Fn(i64) -> bool>(
        &mut self,
        new_lock: LockInfo,
        first_waiter_in_line: bool,
        shares_fn: &F,
    ) -> LockGrantType {
        // If no one owns this right now, just grab it.
        if self.n_owners() == 0 {
            self.add_owner(new_lock);
            return LockGrantType::New;
        }

        let locker_id = new_lock.locker_id;
        let request_type = new_lock.lock_type;
        let mut upgrade: Option<LockUpgrade> = None;
        let mut owner_exists = false;
        let mut owner_conflicts = false;

        // Iterate through the current owners. See if there is a current owner
        // who has to be upgraded from read to write. Also track whether there
        // is a conflict with another owner.
        //
        // The iteration pattern maps index to storage slot:
        //   idx 0        -> first_owner  (may be None even when owner_set has entries,
        //                                  e.g. after flush_owner removed first_owner)
        //   idx 1..      -> owner_set[idx - 1]
        //
        // When first_owner is None at idx==0 we must NOT break; we must fall
        // through to owner_set so we don't miss those owners.
        let mut owner_idx = 0;
        loop {
            let owner = if owner_idx == 0 {
                if self.first_owner.is_some() {
                    self.first_owner.as_ref()
                } else {
                    // first_owner slot is empty; check owner_set[0] next.
                    owner_idx += 1;
                    if let Some(ref set) = self.owner_set {
                        set.first()
                    } else {
                        None
                    }
                }
            } else if let Some(ref set) = self.owner_set {
                set.get(owner_idx - 1)
            } else {
                None
            };

            match owner {
                Some(o) => {
                    let owner_locker = o.locker_id;
                    let owner_type = o.lock_type;

                    if locker_id == owner_locker {
                        // Requestor currently holds this lock: check for upgrades.
                        // If no type change is needed, return EXISTING now to avoid
                        // iterating further; otherwise, we need to check for conflicts
                        // before granting the upgrade.
                        debug_assert!(upgrade.is_none()); // An owner should appear only once
                        let upg = owner_type.get_upgrade(request_type);
                        if upg.is_illegal() {
                            // An impossible transition (e.g. RangeInsert -> Read).
                            // Surface it as an error grant rather than silently
                            // returning Existing (which would no-op the request)
                            // or panicking the process.  The LockManager maps
                            // IllegalUpgrade -> TxnError::IllegalUpgrade so the
                            // txn aborts and the environment survives.
                            log::error!(
                                "illegal lock upgrade from {:?} to {:?} \
                                 (locker {})",
                                owner_type,
                                request_type,
                                locker_id
                            );
                            return LockGrantType::IllegalUpgrade;
                        }
                        upgrade = Some(upg);
                        if upg.get_upgrade_type().is_none() {
                            return LockGrantType::Existing;
                        }
                    } else {
                        // Requestor does not hold this lock.
                        //
                        // Skip conflict detection when the requesting and
                        // owning lockers share locks (e.g. two ThreadLockers on
                        // the same thread).  `shares_fn(owner_locker)` returns
                        // true iff they are in the same sharing group.
                        if shares_fn(owner_locker) {
                            // They share — act as if this owner does not exist
                            // for conflict purposes.
                        } else {
                            let conflict =
                                owner_type.get_conflict(request_type);
                            if conflict == LockConflict::Restart {
                                return LockGrantType::WaitRestart;
                            } else {
                                if conflict == LockConflict::Block {
                                    owner_conflicts = true;
                                }
                                owner_exists = true;
                            }
                        }
                    }

                    owner_idx += 1;
                }
                None => break,
            }
        }

        // Now handle the upgrade or conflict as appropriate.
        if let Some(upg) = upgrade {
            // The requestor holds this lock.
            if upg.is_illegal() {
                // An impossible upgrade transition (e.g. RangeInsert -> Read).
                // This is normally a caller bug, but we surface it as an error
                // rather than panicking the process: the LockManager maps
                // IllegalUpgrade to TxnError::IllegalUpgrade so the txn aborts
                // and the environment survives.  JE treats the equivalent as a
                // catchable EnvironmentFailureException, not a JVM abort.
                log::error!(
                    "illegal lock upgrade from {:?} to {:?} (locker {}) \
                     — returning IllegalUpgrade instead of panicking",
                    new_lock.lock_type,
                    request_type,
                    locker_id
                );
                return LockGrantType::IllegalUpgrade;
            }

            let upgrade_type = match upg.get_upgrade_type() {
                Some(t) => t,
                None => {
                    // A non-illegal upgrade with no concrete upgrade type means
                    // "no type change needed" (JE getUpgrade().getUpgrade() ==
                    // null -> EXISTING).  Treat as already-held.
                    return LockGrantType::Existing;
                }
            };
            if !owner_conflicts {
                // No conflict: grant the upgrade.
                self.update_owner_type(locker_id, upgrade_type);
                if upg.is_promotion() {
                    LockGrantType::Promotion
                } else {
                    LockGrantType::Existing
                }
            } else {
                // Upgrade cannot be granted at this time.
                LockGrantType::WaitPromotion
            }
        } else {
            // The requestor doesn't hold this lock.
            if !owner_conflicts && (!owner_exists || first_waiter_in_line) {
                // No conflict: grant the lock.
                self.add_owner(new_lock);
                LockGrantType::New
            } else {
                // Lock cannot be granted at this time.
                LockGrantType::WaitNew
            }
        }
    }

    /// Called from release() when a RESTART request is waiting to determine if
    /// any RANGE_INSERT owners exist. We can't call try_lock for a RESTART
    /// lock because it must never be granted.
    fn range_insert_conflict(&self, waiter_locker: i64) -> bool {
        // Default: no lock-sharing groups (callers without a sharing registry).
        self.range_insert_conflict_with_sharing(waiter_locker, &|_| false)
    }

    /// As `range_insert_conflict`, but skips an owner that shares locks with
    /// the waiter (JE `rangeInsertConflict`, LockImpl.java:719:
    /// `!ownerLocker.sharesLocksWith(waiterLocker)`). `shares_fn(owner_id)`
    /// returns true when the owner is in the waiter's lock-sharing group.
    fn range_insert_conflict_with_sharing<F: Fn(i64) -> bool>(
        &self,
        waiter_locker: i64,
        shares_fn: &F,
    ) -> bool {
        let mut owner_idx = 0;
        loop {
            let owner = if owner_idx == 0 {
                self.first_owner.as_ref()
            } else if let Some(ref set) = self.owner_set {
                set.get(owner_idx - 1)
            } else {
                None
            };

            match owner {
                Some(o) => {
                    let owner_locker = o.locker_id;
                    if owner_locker != waiter_locker
                        && !shares_fn(owner_locker)
                        && o.lock_type == LockType::RangeInsert
                    {
                        return true;
                    }
                    owner_idx += 1;
                }
                None => break,
            }
        }

        false
    }
}

impl Default for LockImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_insert_conflict_honors_sharing() {
        // DRIFT-1 regression (JE rangeInsertConflict / sharesLocksWith): a
        // RANGE_INSERT owner that shares locks with the waiter must NOT be
        // reported as a conflict.
        let mut lock = LockImpl::new();
        assert_eq!(
            lock.lock(LockType::RangeInsert, 1, false, false).grant_type,
            LockGrantType::New
        );
        // Different, non-sharing locker -> conflict.
        assert!(lock.range_insert_conflict(2));
        // Same locker never conflicts with itself.
        assert!(!lock.range_insert_conflict(1));
        // A waiter that SHARES locks with owner 1 -> no conflict (the JE
        // sharesLocksWith clause DRIFT-1 had dropped).
        assert!(
            !lock.range_insert_conflict_with_sharing(2, &|owner| owner == 1),
            "a sharing-group owner must not conflict (DRIFT-1)"
        );
    }

    #[test]
    fn test_single_owner_lock_release() {
        let mut lock = LockImpl::new();
        let locker_id = 1;

        // Acquire a read lock
        let result = lock.lock(LockType::Read, locker_id, false, false);
        assert_eq!(result.grant_type, LockGrantType::New);
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 0);

        // Release the lock
        let notified = lock.release(locker_id);
        assert!(notified.is_some());
        assert_eq!(notified.unwrap().len(), 0);
        assert_eq!(lock.n_owners(), 0);
    }

    #[test]
    fn test_multiple_readers() {
        let mut lock = LockImpl::new();

        // Two readers can co-own
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::New);

        assert_eq!(lock.n_owners(), 2);
    }

    #[test]
    fn test_write_blocks_read() {
        let mut lock = LockImpl::new();

        // Writer acquires lock
        let result1 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        // Reader tries to acquire (non-blocking)
        let result2 = lock.lock(LockType::Read, 2, true, false);
        assert_eq!(result2.grant_type, LockGrantType::Denied);

        // Reader tries to acquire (blocking - would wait)
        let result3 = lock.lock(LockType::Read, 3, false, false);
        assert_eq!(result3.grant_type, LockGrantType::WaitNew);
        assert_eq!(lock.n_waiters(), 1);
    }

    #[test]
    fn test_write_blocks_write() {
        let mut lock = LockImpl::new();

        // Writer acquires lock
        let result1 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        // Another writer tries to acquire (non-blocking)
        let result2 = lock.lock(LockType::Write, 2, true, false);
        assert_eq!(result2.grant_type, LockGrantType::Denied);
    }

    #[test]
    fn test_illegal_upgrade_returns_grant_not_panic() {
        // Hold a RangeInsert lock, then request Read on the same locker.
        // (RangeInsert -> Read) is an Illegal entry in the upgrade matrix.
        // It must surface as LockGrantType::IllegalUpgrade, NOT panic the
        // process (so the txn can abort and the environment survives).
        let mut lock = LockImpl::new();
        let r1 = lock.lock(LockType::RangeInsert, 1, false, false);
        assert_eq!(r1.grant_type, LockGrantType::New);

        let r2 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(
            r2.grant_type,
            LockGrantType::IllegalUpgrade,
            "illegal upgrade must return IllegalUpgrade, not panic"
        );
        assert!(!r2.success, "illegal upgrade is not a successful grant");
    }

    #[test]
    fn test_lock_upgrade_read_to_write() {
        let mut lock = LockImpl::new();

        // Acquire read lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        // Upgrade to write lock (no other owners)
        let result2 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result2.grant_type, LockGrantType::Promotion);
        assert_eq!(lock.n_owners(), 1);
        assert!(lock.is_owned_write_lock(1));
    }

    #[test]
    fn test_lock_upgrade_with_conflict() {
        let mut lock = LockImpl::new();

        // Two readers
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::New);

        // First reader tries to upgrade to write (conflicts with second reader)
        let result3 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result3.grant_type, LockGrantType::WaitPromotion);
        assert_eq!(lock.n_waiters(), 1);
    }

    #[test]
    fn test_range_insert_no_conflict() {
        let mut lock = LockImpl::new();

        // RANGE_INSERT doesn't conflict with READ
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        let result2 = lock.lock(LockType::RangeInsert, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::New);
        assert_eq!(lock.n_owners(), 2);

        // RANGE_INSERT doesn't conflict with WRITE
        let mut lock2 = LockImpl::new();
        let result3 = lock2.lock(LockType::Write, 1, false, false);
        assert_eq!(result3.grant_type, LockGrantType::New);

        let result4 = lock2.lock(LockType::RangeInsert, 2, false, false);
        assert_eq!(result4.grant_type, LockGrantType::New);
        assert_eq!(lock2.n_owners(), 2);
    }

    #[test]
    fn test_range_insert_conflicts_with_range_read() {
        let mut lock = LockImpl::new();

        // RANGE_INSERT held
        let result1 = lock.lock(LockType::RangeInsert, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        // RANGE_READ request causes restart
        let result2 = lock.lock(LockType::RangeRead, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::WaitRestart);
    }

    #[test]
    fn test_release_promotes_waiters() {
        let mut lock = LockImpl::new();

        // Writer acquires lock
        let result1 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        // Reader waits
        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::WaitNew);

        // Writer releases, reader should be promoted
        let notified = lock.release(1);
        assert!(notified.is_some());
        let notified = notified.unwrap();
        assert_eq!(notified.len(), 1);
        assert_eq!(notified[0], 2);
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 0);
    }

    #[test]
    fn test_demote() {
        let mut lock = LockImpl::new();

        // Acquire write lock
        let result1 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);
        assert!(lock.is_owned_write_lock(1));

        // Demote to read
        lock.demote(1);
        assert!(!lock.is_owned_write_lock(1));
        assert!(lock.is_owner(1, LockType::Read));
    }

    #[test]
    fn test_flush_waiter() {
        let mut lock = LockImpl::new();

        // Writer acquires lock
        let result1 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        // Reader waits
        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::WaitNew);
        assert_eq!(lock.n_waiters(), 1);

        // Flush the waiter
        lock.flush_waiter(2);
        assert_eq!(lock.n_waiters(), 0);
    }

    #[test]
    fn test_steal_lock() {
        let mut lock = LockImpl::new();

        // Multiple owners
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::New);

        let result3 = lock.lock(LockType::Read, 3, false, false);
        assert_eq!(result3.grant_type, LockGrantType::New);

        // Steal lock for locker 2
        let preempted = lock.steal_lock(2);
        assert_eq!(preempted.len(), 2);
        assert!(preempted.contains(&1));
        assert!(preempted.contains(&3));
        assert_eq!(lock.n_owners(), 1);
        assert!(lock.is_owner(2, LockType::Read));
    }

    #[test]
    fn test_query_methods() {
        let mut lock = LockImpl::new();

        // Acquire read lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        assert!(lock.is_owner(1, LockType::Read));
        assert!(!lock.is_owner(1, LockType::Write));
        assert!(!lock.is_owned_write_lock(1));
        assert_eq!(lock.get_owned_lock_type(1), Some(LockType::Read));
        assert_eq!(lock.get_write_owner_locker_id(), None);

        // Upgrade to write
        let result2 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result2.grant_type, LockGrantType::Promotion);

        assert!(lock.is_owned_write_lock(1));
        assert_eq!(lock.get_owned_lock_type(1), Some(LockType::Write));
        assert_eq!(lock.get_write_owner_locker_id(), Some(1));
    }

    #[test]
    fn test_existing_lock() {
        let mut lock = LockImpl::new();

        // Acquire read lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        // Request same lock again
        let result2 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result2.grant_type, LockGrantType::Existing);
        assert_eq!(lock.n_owners(), 1);
    }

    // -----------------------------------------------------------------------
    // Ported from LockTest.java — testLockConflicts
    // -----------------------------------------------------------------------

    /// Read lock granted new the first time,
    /// EXISTING on a second request by the same locker.
    #[test]
    fn test_je_read_new_then_existing() {
        let mut lock = LockImpl::new();
        let r1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(r1.grant_type, LockGrantType::New);
        // Same locker requests READ again — must be EXISTING (idempotent).
        let r2 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(r2.grant_type, LockGrantType::Existing);
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 0);
    }

    /// Two readers co-own, then both try
    /// write upgrades — each gets WAIT_PROMOTION.
    #[test]
    fn test_je_two_readers_then_write_promotion_waits() {
        let mut lock = LockImpl::new();
        // txn1 read
        assert_eq!(
            lock.lock(LockType::Read, 1, false, false).grant_type,
            LockGrantType::New
        );
        // txn2 read
        assert_eq!(
            lock.lock(LockType::Read, 2, false, false).grant_type,
            LockGrantType::New
        );
        // txn1 requests write — conflict with txn2's read → WAIT_PROMOTION
        assert_eq!(
            lock.lock(LockType::Write, 1, false, false).grant_type,
            LockGrantType::WaitPromotion
        );
        // txn2 requests write — conflict with txn1's read → WAIT_PROMOTION
        assert_eq!(
            lock.lock(LockType::Write, 2, false, false).grant_type,
            LockGrantType::WaitPromotion
        );
        assert_eq!(lock.n_owners(), 2);
        assert_eq!(lock.n_waiters(), 2);
    }

    /// Releasing one of two readers with
    /// a pending write-promotion promotes the remaining reader to write owner.
    #[test]
    fn test_je_release_reader_promotes_writer() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::Read, 1, false, false);
        lock.lock(LockType::Read, 2, false, false);
        // txn1 wants write promotion
        lock.lock(LockType::Write, 1, false, false);
        // txn2 wants write promotion
        lock.lock(LockType::Write, 2, false, false);
        // 2 owners, 2 waiters
        assert_eq!(lock.n_owners(), 2);
        assert_eq!(lock.n_waiters(), 2);

        // Release txn1's read lock; txn2 (the other reader) should promote.
        lock.release(1);
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 1);

        // Release txn2's write lock; now txn1's write waiter should be granted.
        lock.release(2);
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 0);

        // Release txn1's write lock.
        lock.release(1);
        assert_eq!(lock.n_owners(), 0);
        assert_eq!(lock.n_waiters(), 0);
    }

    /// Holding write and requesting read
    /// for the same locker returns EXISTING (write subsumes read).
    #[test]
    fn test_je_write_then_read_existing() {
        let mut lock = LockImpl::new();
        assert_eq!(
            lock.lock(LockType::Write, 1, false, false).grant_type,
            LockGrantType::New
        );
        assert_eq!(
            lock.lock(LockType::Read, 1, false, false).grant_type,
            LockGrantType::Existing
        );
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 0);
        lock.release(1);
    }

    /// Read then write by same locker (no
    /// other owners) yields PROMOTION.
    #[test]
    fn test_je_read_then_write_promotion() {
        let mut lock = LockImpl::new();
        assert_eq!(
            lock.lock(LockType::Read, 1, false, false).grant_type,
            LockGrantType::New
        );
        assert_eq!(
            lock.lock(LockType::Write, 1, false, false).grant_type,
            LockGrantType::Promotion
        );
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 0);
        lock.release(1);
    }

    /// Non-blocking write request while a
    /// read lock is held by another locker → DENIED, no waiter added.
    #[test]
    fn test_je_nonblocking_write_denied() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::Read, 1, false, false);
        let r = lock.lock(LockType::Write, 2, true, false);
        assert_eq!(r.grant_type, LockGrantType::Denied);
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 0);
        lock.release(1);
    }

    /// Two write requests from the same
    /// locker → second is EXISTING.
    #[test]
    fn test_je_double_write_existing() {
        let mut lock = LockImpl::new();
        assert_eq!(
            lock.lock(LockType::Write, 1, false, false).grant_type,
            LockGrantType::New
        );
        assert_eq!(
            lock.lock(LockType::Write, 1, false, false).grant_type,
            LockGrantType::Existing
        );
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 0);
        lock.release(1);
    }

    /// A read lock followed by a blocking
    /// write request from another locker → WAIT_NEW; a subsequent read from a
    /// third locker must also wait (WAIT_NEW) because a write waiter exists.
    #[test]
    fn test_je_read_behind_write_waiter_also_waits() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::Read, 1, false, false);
        // txn2 wants write — must wait
        let r2 = lock.lock(LockType::Write, 2, false, false);
        assert_eq!(r2.grant_type, LockGrantType::WaitNew);
        // txn3 wants read — must also wait because a write waiter is ahead
        let r3 = lock.lock(LockType::Read, 3, false, false);
        assert_eq!(r3.grant_type, LockGrantType::WaitNew);
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 2);
        lock.release(1);
        lock.release(2);
        lock.release(3);
    }

    /// Non-blocking write denied but
    /// non-blocking read succeeds when only a read lock is held.
    #[test]
    fn test_je_nonblocking_read_granted_with_reader() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::Read, 1, false, false);
        // Non-blocking write → DENIED
        let rw = lock.lock(LockType::Write, 2, true, false);
        assert_eq!(rw.grant_type, LockGrantType::Denied);
        // Non-blocking read → NEW (compatible)
        let rr = lock.lock(LockType::Read, 3, true, false);
        assert_eq!(rr.grant_type, LockGrantType::New);
        assert_eq!(lock.n_owners(), 2);
        assert_eq!(lock.n_waiters(), 0);
        lock.release(1);
        lock.release(3);
    }

    /// Three concurrent readers all succeed.
    #[test]
    fn test_je_three_concurrent_readers() {
        let mut lock = LockImpl::new();
        assert_eq!(
            lock.lock(LockType::Read, 1, false, false).grant_type,
            LockGrantType::New
        );
        assert_eq!(
            lock.lock(LockType::Read, 2, false, false).grant_type,
            LockGrantType::New
        );
        assert_eq!(
            lock.lock(LockType::Read, 3, false, false).grant_type,
            LockGrantType::New
        );
        assert_eq!(lock.n_owners(), 3);
        assert_eq!(lock.n_waiters(), 0);
        lock.release(1);
        lock.release(2);
        lock.release(3);
    }

    // -----------------------------------------------------------------------
    // Ported from LockTest.java — testOwners
    // -----------------------------------------------------------------------

    /// No write owner until a write lock is held.
    #[test]
    fn test_je_no_write_owner_with_only_reads() {
        let mut lock = LockImpl::new();
        // Fresh lock has no write owner.
        assert_eq!(lock.get_write_owner_locker_id(), None);
        lock.lock(LockType::Read, 1, false, false);
        lock.lock(LockType::Read, 2, false, false);
        lock.lock(LockType::Read, 3, false, false);
        // Still no write owner.
        assert_eq!(lock.get_write_owner_locker_id(), None);
        lock.release(1);
        lock.release(2);
        lock.release(3);
    }

    /// Owner list tracks additions and removals.
    #[test]
    fn test_je_owner_set_add_remove() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::Read, 1, false, false);
        lock.lock(LockType::Read, 2, false, false);
        lock.lock(LockType::Read, 3, false, false);
        assert_eq!(lock.n_owners(), 3);

        lock.release(1);
        assert_eq!(lock.n_owners(), 2);
        assert!(lock.is_owner(2, LockType::Read));
        assert!(lock.is_owner(3, LockType::Read));

        lock.lock(LockType::Read, 4, false, false);
        assert_eq!(lock.n_owners(), 3);

        lock.release(2);
        assert_eq!(lock.n_owners(), 2);
        lock.release(3);
        assert_eq!(lock.n_owners(), 1);
        // Only txn4 left — still no write owner.
        assert_eq!(lock.get_write_owner_locker_id(), None);
        lock.release(4);
        assert_eq!(lock.n_owners(), 0);
    }

    // -----------------------------------------------------------------------
    // Ported from LockTest.java — testPromotion
    // -----------------------------------------------------------------------

    /// Releasing the single writer promotes
    /// ALL waiting readers to owners simultaneously.
    #[test]
    fn test_je_release_writer_promotes_all_readers() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::Write, 1, false, false);
        // Three readers wait
        assert_eq!(
            lock.lock(LockType::Read, 2, false, false).grant_type,
            LockGrantType::WaitNew
        );
        assert_eq!(
            lock.lock(LockType::Read, 3, false, false).grant_type,
            LockGrantType::WaitNew
        );
        assert_eq!(
            lock.lock(LockType::Read, 4, false, false).grant_type,
            LockGrantType::WaitNew
        );
        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 3);

        // Release writer; all 3 readers should be promoted.
        let notified = lock.release(1).unwrap();
        assert_eq!(notified.len(), 3);
        assert_eq!(lock.n_owners(), 3);
        assert_eq!(lock.n_waiters(), 0);
        assert!(lock.is_owner(2, LockType::Read));
        assert!(lock.is_owner(3, LockType::Read));
        assert!(lock.is_owner(4, LockType::Read));

        lock.release(2);
        lock.release(3);
        lock.release(4);
    }

    // -----------------------------------------------------------------------
    // Ported from LockTest.java — testWaiters
    // -----------------------------------------------------------------------

    /// Flush_waiter removes from waiter list
    /// without affecting owners.
    #[test]
    fn test_je_flush_waiter_removes_entry() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::Read, 1, false, false);
        lock.lock(LockType::Read, 2, false, false);
        assert_eq!(
            lock.lock(LockType::Write, 3, false, false).grant_type,
            LockGrantType::WaitNew
        );
        assert_eq!(
            lock.lock(LockType::Write, 4, false, false).grant_type,
            LockGrantType::WaitNew
        );
        assert_eq!(lock.n_waiters(), 2);

        lock.flush_waiter(4);
        assert_eq!(lock.n_waiters(), 1);
        assert!(lock.is_waiter(3));
        assert!(!lock.is_waiter(4));
    }

    /// A wait_promotion waiter is inserted at
    /// the head of the waiter list (promotion takes priority).
    #[test]
    fn test_je_promotion_waiter_at_head() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::Read, 1, false, false);
        lock.lock(LockType::Read, 2, false, false);
        // txn3 (new txn) adds a write waiter
        assert_eq!(
            lock.lock(LockType::Write, 3, false, false).grant_type,
            LockGrantType::WaitNew
        );
        // txn1 (existing reader) upgrades — should be WAIT_PROMOTION at head
        assert_eq!(
            lock.lock(LockType::Write, 1, false, false).grant_type,
            LockGrantType::WaitPromotion
        );

        let waiters = lock.get_waiters_clone();
        // The first waiter in line must be the WAIT_PROMOTION (txn1), not the
        // WAIT_NEW (txn3), because moves promotions to the head.
        assert_eq!(waiters[0].locker_id, 1);
        assert_eq!(waiters[0].lock_type, LockType::Write);
    }

    // -----------------------------------------------------------------------
    // Ported from LockTest.java — testRangeConflicts (spot-checks)
    // -----------------------------------------------------------------------

    /// Range_insert held → range_read
    /// requested → WAIT_RESTART (not NEW or WAIT_NEW).
    #[test]
    fn test_je_range_insert_conflicts_range_read() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::RangeInsert, 1, false, false);
        let r = lock.lock(LockType::RangeRead, 2, false, false);
        assert_eq!(r.grant_type, LockGrantType::WaitRestart);
        lock.release(1);
        lock.release(2);
    }

    /// Range_insert held → range_insert
    /// requested by another locker → NEW (compatible).
    #[test]
    fn test_je_range_insert_compatible_with_range_insert() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::RangeInsert, 1, false, false);
        let r = lock.lock(LockType::RangeInsert, 2, false, false);
        assert_eq!(r.grant_type, LockGrantType::New);
        lock.release(1);
        lock.release(2);
    }

    /// Range_read held → range_write
    /// request → WAIT_NEW (conflict).
    #[test]
    fn test_je_range_read_vs_range_write_conflict() {
        let mut lock = LockImpl::new();
        lock.lock(LockType::RangeRead, 1, false, false);
        let r = lock.lock(LockType::RangeWrite, 2, false, false);
        assert_eq!(r.grant_type, LockGrantType::WaitNew);
        lock.release(1);
        lock.release(2);
    }

    // -----------------------------------------------------------------------
    // Ported from LockTest.java — testRangeInsertWaiterConflict
    // -----------------------------------------------------------------------

    /// When a range_insert
    /// is already waiting, a subsequent RANGE_READ request sees WAIT_RESTART
    /// (the waiter list is examined for restart conflicts).
    #[test]
    fn test_je_range_insert_waiter_causes_restart() {
        let mut lock = LockImpl::new();
        // txn1 holds RANGE_READ
        assert_eq!(
            lock.lock(LockType::RangeRead, 1, false, false).grant_type,
            LockGrantType::New
        );
        // txn2 waits with RANGE_INSERT
        assert_eq!(
            lock.lock(LockType::RangeInsert, 2, false, false).grant_type,
            LockGrantType::WaitNew
        );
        // txn3 requests RANGE_READ — sees txn2's RANGE_INSERT waiter → WAIT_RESTART
        let r = lock.lock(LockType::RangeRead, 3, false, false);
        assert_eq!(r.grant_type, LockGrantType::WaitRestart);

        assert_eq!(lock.n_owners(), 1);
        assert_eq!(lock.n_waiters(), 2);

        let waiters = lock.get_waiters_clone();
        // txn2 waits as RANGE_INSERT; txn3 is stored as RESTART
        assert_eq!(waiters[0].lock_type, LockType::RangeInsert);
        assert_eq!(waiters[1].lock_type, LockType::Restart);

        lock.release(1);
        lock.release(2);
        lock.release(3);
    }

    /// TXN-F1 regression: the restart-conflict waiter scan must skip a waiter
    /// the requestor shares locks with.  JE `LockImpl.lock` checks
    /// `waiterType != RESTART && locker != waiterLocker &&
    /// !locker.sharesLocksWith(waiterLocker)` (LockImpl.java:395) — the third
    /// clause was missing from `lock_with_sharing`'s restart loop, so a
    /// requestor sharing locks with the RANGE_INSERT waiter would spuriously
    /// restart instead of waiting normally.
    ///
    /// Same setup as `test_je_range_insert_waiter_causes_restart`, but txn3
    /// shares locks with txn2 (the RANGE_INSERT waiter).  With sharing
    /// honored in the restart loop, txn3 must get WAIT_NEW, not WAIT_RESTART.
    #[test]
    fn test_txn_f1_restart_loop_skips_shared_waiter() {
        let mut lock = LockImpl::new();
        // txn1 holds RANGE_READ.
        assert_eq!(
            lock.lock(LockType::RangeRead, 1, false, false).grant_type,
            LockGrantType::New
        );
        // txn2 waits with RANGE_INSERT (blocked by txn1's RANGE_READ).
        assert_eq!(
            lock.lock(LockType::RangeInsert, 2, false, false).grant_type,
            LockGrantType::WaitNew
        );
        // txn3 requests RANGE_READ and SHARES LOCKS WITH txn2 only.
        let shares = |owner: i64| owner == 2;
        let r = lock.lock_with_sharing(
            LockType::RangeRead,
            3,
            false,
            false,
            &shares,
        );
        // JE skips the shared RANGE_INSERT waiter in the restart scan, so no
        // spurious restart: txn3 waits as a normal new waiter.
        assert_eq!(r.grant_type, LockGrantType::WaitNew);

        let waiters = lock.get_waiters_clone();
        assert_eq!(waiters[0].lock_type, LockType::RangeInsert);
        // txn3 stored as RANGE_READ (a normal waiter), NOT downgraded to RESTART.
        assert_eq!(waiters[1].lock_type, LockType::RangeRead);

        lock.release(1);
        lock.release(2);
        lock.release(3);
    }
}
