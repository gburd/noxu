//! Globally-unique locker-id allocation for *non-transactional* lockers.
//!
//! # Why this exists (BUG-LOCKER-ID-COLLISION)
//!
//! The [`LockManager`](crate::LockManager) keys every lock owner, waiter,
//! waits-for-graph edge and `compute_lock_counts` tally on a bare `i64`
//! `locker_id`.  JE instead keys `Lock.owners`/`waiters` on the `Locker`
//! **object** (`LockInfo.locker`), and gives every non-transactional
//! `BasicLocker` the shared sentinel id `NULL_TXN_ID` (`-1`) while still
//! distinguishing them by object identity, so two different non-txn lockers
//! are never conflated (`BasicLocker.generateId` -> `TxnManager.NULL_TXN_ID`).
//!
//! Noxu has no object identity in the lock table -- only the `i64` -- so every
//! locker that can hold a record lock concurrently with another MUST have a
//! unique id, AND non-txn locker ids MUST be disjoint from the transaction id
//! space.  Before this fix, auto-commit cursor lockers drew ids from a
//! `NEXT_CURSOR_ID` counter that started at `1` and counted *up*, exactly
//! overlapping the positive transaction ids from `TxnManager` (also from `1`
//! up).  When a cursor id collided with a live txn id the lock manager treated
//! them as ONE owner, which could:
//!   - grant the cursor a read on a record the txn write-locked (a dirty read);
//!   - grant the cursor a write while the txn held a conflicting lock;
//!   - release the TXN's lock when the cursor released "its" lock;
//!   - mis-count `is_locked` / owner counts for the compressor / cleaner gate;
//!   - fabricate a false deadlock cycle (the originally-observed symptom).
//!
//! The cleaner had the same class of bug on the negative side: its locker ids
//! started at `-1` -- i.e. exactly `NULL_TXN_ID` -- and counted down.
//!
//! # The allocator
//!
//! All non-transactional lockers (auto-commit cursors, the cleaner's migration
//! probe locks, and any future internal non-txn locker) now draw from a single
//! monotonic counter that lives in the deeply-negative id band, disjoint from:
//!   - positive transaction ids (`1 ..= i64::MAX`, from `TxnManager`),
//!   - `NULL_TXN_ID` (`-1`), and
//!   - JE's reserved replicated-master band (`-10` and just below).
//!
//! IDs are unique among non-txn lockers (one shared counter) and can never
//! equal a transaction id (opposite sign).  This mirrors JE's invariant
//! ("non-txn lockers are distinct from each other and from txn ids") using the
//! only identity Noxu's lock table has: the integer.
//!
//! Nothing here is persisted: a non-txn locker id never reaches the log (an
//! auto-commit cursor logs its writes with `txn_id = None`; see
//! `CursorImpl::log_ln_write`), so this is purely an in-memory concern with no
//! on-disk-format impact and no recovery seeding interaction.

use noxu_util::dst_sync::atomic::{AtomicI64, Ordering};

/// First id handed out to a non-transactional locker.
///
/// Chosen a full billion below zero so the entire `NULL_TXN_ID` (`-1`) and
/// JE replicated-master band (`-10` downward) stays reserved and un-reachable.
/// The counter moves further negative from here.
///
/// # Disjointness from the whole locker-id map (RC v2)
///
/// All lock-manager locker ids live on the single `i64` line, partitioned into
/// non-overlapping bands:
///
/// | band | range | source |
/// |---|---|---|
/// | transactions | `1 ..= i64::MAX` | `TxnManager` (local / auto / XA / synthetic / Option B auto txn) and `Environment::next_txn_id` fallback — all positive |
/// | reserved | `-10 ..= -1` | `NULL_TXN_ID` (`-1`) + JE replicated-master band |
/// | **non-txn (this allocator)** | `(i64::MIN/4, -1_000_000_000]` | auto-commit cursors + cleaner probes |
/// | replay ephemeral (non-txn LN) | `(i64::MIN/2, i64::MIN/4]` (decrementing) | `replica_replay::ephemeral_locker` (start `i64::MIN/4`) |
/// | replay transactions (importunate `ReplayTxn`) | `~ i64::MIN/2 + master_txn_id` | `replica_replay::txn_locker_id` (base `i64::MIN/2`) |
///
/// This band (`FIRST_NON_TXN_LOCKER_ID = -10^9`, decrementing) sits entirely in
/// the *shallow*-negative quadrant above `i64::MIN/4` (`-2.3e18`); replay-v2's
/// ephemeral and `ReplayTxn` lockers sit in the *deep*-negative half at or
/// below `i64::MIN/4`.  Reaching the replay region from here would require
/// ~`2.3e18` concurrently-live non-txn lockers; the hard floor below guards the
/// pathological case so the two can never meet even in principle.
pub const FIRST_NON_TXN_LOCKER_ID: i64 = -1_000_000_000;

/// Hard floor for the non-txn band: the allocator never returns an id below
/// this.  Set to `i64::MIN / 8` (`-1.15e18`), comfortably above replay-v2's
/// `ephemeral_locker` start of `i64::MIN / 4` (`-2.3e18`) and the `ReplayTxn`
/// base `i64::MIN / 2`, so the non-txn band is *structurally* disjoint from the
/// replay bands, not merely disjoint-in-practice.  Exhausting `~1.15e18` ids is
/// not reachable in any real run; if it ever were, the allocator panics rather
/// than silently wrap into the replay region (which would re-introduce
/// BUG-LOCKER-ID-COLLISION against a replica write locker).
pub const NON_TXN_LOCKER_FLOOR: i64 = i64::MIN / 8;

/// The next non-transactional locker id to hand out.  Decrements (grows more
/// negative) so successive ids never collide with each other and never cross
/// into the positive transaction id space.
static NEXT_NON_TXN_LOCKER_ID: AtomicI64 =
    AtomicI64::new(FIRST_NON_TXN_LOCKER_ID);

/// Allocates a process-globally-unique id for a non-transactional locker
/// (auto-commit cursor, cleaner migration probe, ...).
///
/// The returned id is always in `[NON_TXN_LOCKER_FLOOR, FIRST_NON_TXN_LOCKER_ID]`,
/// i.e. strictly negative, disjoint from every transaction id, from
/// `NULL_TXN_ID`, and from the replay-v2 ephemeral / `ReplayTxn` bands.
/// See the module docs for the full rationale (BUG-LOCKER-ID-COLLISION).
#[inline]
pub fn next_non_txn_locker_id() -> i64 {
    let id = NEXT_NON_TXN_LOCKER_ID.fetch_sub(1, Ordering::Relaxed);
    assert!(
        id >= NON_TXN_LOCKER_FLOOR,
        "non-txn locker id space exhausted (id {id} < floor \
         {NON_TXN_LOCKER_FLOOR}); refusing to wrap into the replay-locker band"
    );
    id
}
