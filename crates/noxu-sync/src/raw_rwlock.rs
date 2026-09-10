//! Futex-based raw reader-writer lock implementing `lock_api::RawRwLock`.
//!
//! Design derived directly from
//! `docs/src/internal/rwlock-state-table-2026-09.md` — read that document
//! first; this module is its implementation, and every non-obvious ordering
//! or branch below cites the table row it comes from.
//!
//! # State encoding (single `AtomicU32`)
//!
//!   bits 0-29: reader count  (`ONE_READER` = 1, max ~1 billion concurrent readers)
//!   bit  30:   `WRITE_LOCKED` — **reservation-or-hold**, disambiguated by the
//!              reader count: `WRITE_LOCKED != 0 && READERS_MASK == 0` means a
//!              writer holds the lock; `WRITE_LOCKED != 0 && READERS_MASK > 0`
//!              means a writer has reserved it and is waiting for the
//!              in-flight readers to drain. Bit 31 is unused (formerly
//!              `WRITE_WAITING`; see "What changed" below).
//!
//! This is the table's four states projected onto the word: `free` (0),
//! `read_held(n)` (`READERS_MASK == n`, `WRITE_LOCKED` clear),
//! `reserved_draining(n)` (`WRITE_LOCKED` set, `READERS_MASK == n > 0`),
//! `write_held` (`WRITE_LOCKED` set, `READERS_MASK == 0`).
//!
//! # What changed from the eventual-fairness design, and why
//!
//! The previous design used an *advisory* `WRITE_WAITING` bit: a writer that
//! had waited past `FAIRNESS_THRESHOLD` set it to refuse new readers, then
//! re-raced the plain `state == 0` acquire CAS once the count reached zero.
//! That re-race is the whole cost this rewrite removes: it cannot promise
//! the writer the lock the instant the last reader leaves, because the CAS
//! can still lose to a reader that sneaks in first, so the writer pays a
//! futex round-trip per drain step. Measured: p50 write latency 1007us at 63
//! readers, vs `parking_lot`'s 0us (`docs/src/internal/parking-lot-removal-2026-09.md`,
//! "The honest remaining gap").
//!
//! **`WRITE_LOCKED` now means what `parking_lot`'s `WRITER_BIT` means**: a
//! writer that cannot acquire outright *reserves* the bit unconditionally
//! (gated only on no OTHER writer already holding it — never on the reader
//! count), which excludes every future reader immediately and makes the
//! eventual hand-off unconditional rather than a re-race. The
//! `WRITE_WAITING` gate and `FAIRNESS_THRESHOLD` are deleted entirely rather
//! than kept alongside reservation: the state table's design-decision
//! section argues (and the accompanying shuttle model — see
//! `crates/noxu-sync/tests/shuttle_rwlock_reservation.rs` — found no
//! contradiction) that they become dead, redundant machinery once
//! reservation exists, and running them concurrently with reservation is the
//! likely cause of a fourth, previously unexplained hang in an earlier
//! attempt at this exact change.
//!
//! The `WRITE_SPIN_ATTEMPTS` barging spin is UNCHANGED and still runs before
//! any reservation is attempted: reserving on every momentary overlap with a
//! departing reader would be as costly as the old immediately-closing gate
//! (measured 2.9x throughput cost against the B-tree root), so a writer
//! still spins for a genuinely free lock first and only reserves once that
//! has failed.
//!
//! Writers are preferred over readers once a reservation exists (no new
//! reader can be admitted past that point), but writers are NOT ordered
//! among themselves — this is writer preference via reservation, not FIFO
//! fairness.
//!
//! # Fields
//!
//!   `read_waiters`  — readers blocked waiting for `WRITE_LOCKED` to clear
//!   `write_waiters` — writers that have NOT YET obtained `WRITE_LOCKED` in
//!                     either form; decremented at the instant a writer's
//!                     reserve CAS succeeds (not at final acquisition — see
//!                     "Bug 4" in the state table's failure-mode mapping),
//!                     or when a writer gives up before ever reserving.
//!   `exclusive_owner` — thread ID hash of the write-lock owner (set only
//!                     once `write_held` is actually reached, never while
//!                     merely reserved-and-draining)
//!
//! # Three disjoint futex words
//!
//! See the state table's "A third futex word is required" section for the
//! full derivation of why a single shared word is unsound here. Summary:
//!
//!   * `state` — readers park here refused admission; woken in FULL
//!     (`ALL`), never targeted, whenever that refusal might no longer hold.
//!   * `write_futex` — writers that have not yet reserved park here while
//!     another writer holds the bit; any one of them is a fungible target
//!     for a targeted wake (unchanged from the prior targeted-wakeup design).
//!   * `drain_futex` — ONLY the current reservation-holder ever parks here.
//!     At most one writer is ever `reserved_draining` at a time (the
//!     reserve-CAS's own exclusivity), so a targeted wake on this word is
//!     unambiguous by construction — this is the actual fix for the
//!     tail-latency gap: the last reader out hands off directly, no CAS,
//!     no re-race.

use crate::futex::{futex_wait, futex_wake};
use lock_api;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// State bit meaning "a writer has reserved or holds this lock" — see the
/// module doc comment's "State encoding" section for how the reader count
/// disambiguates which of the two it is.
pub(crate) const WRITE_LOCKED: u32 = 1 << 30;
/// Spin attempts a writer makes for a genuinely FREE lock before committing
/// to a reservation.
///
/// Tuned by measurement against the B-tree root — the most-traversed lock in
/// the engine, read-latched by every descent. Engine read throughput at 64
/// threads (ycsb_c, idle 64-vCPU box), with `parking_lot` at 664k ops/s for
/// reference:
///
/// | spin | throughput |
/// |---:|---:|
/// | 0 (reserve immediately) | 78k |
/// | 40 | 139k |
/// | **400** | **649k** |
/// | 4000 | 657k |
///
/// 400 is the knee; beyond it the curve is flat, so the extra spinning only
/// delays a genuinely contended writer for nothing. Reserving excludes every
/// future reader immediately (unlike spinning, which excludes nobody), so
/// this must not fire for a momentary overlap with a departing reader —
/// only once spinning has genuinely failed to find a free window.
const WRITE_SPIN_ATTEMPTS: u32 = 400;
/// Spin attempts a RESERVING writer makes while waiting for the last
/// draining reader to reach zero, before parking on `drain_futex`.
//
// Reservation excludes every future reader the instant it is taken, so
// unlike the barging spin above (which trades throughput against
// admission), this spin trades nothing but a park/wake round-trip: the
// readers already in flight are, by construction, the only remaining
// obstacle, and they are typically microseconds from finishing. Skipping
// straight to a blocking futex_wait for every reservation was measured to
// cost 70-100x parking_lot's p50 at 63 readers on a 32-vCPU box -- not a
// protocol cost, a scheduling one: the reserving writer is usually the
// (`readers + 1`)-th runnable thread, so parking sends it to the back of
// the CPU queue instead of letting it observe the drain complete via a few
// cache-coherent loads.
//
// A FLAT busy-spin here is actively harmful under oversubscription (readers
// + 1 writer > vCPUs): it burns a CPU slot competing with the very readers
// it is waiting on, instead of yielding that slot back to the scheduler so a
// reader can actually run and finish. Measured: a flat `spin_loop() x100`
// pushed max latency at 63 readers to 20-70ms -- worse than parking
// immediately. `parking_lot`'s own `wait_for_readers` avoids exactly this
// with `SpinWait`: a handful of short CPU-relax bursts, THEN yields the
// thread to the OS for the remaining attempts, never just busy-looping
// throughout. `spin_then_yield` below is that same two-phase strategy.
const DRAIN_SPIN_RELAX_ATTEMPTS: u32 = 3;
const DRAIN_SPIN_YIELD_ATTEMPTS: u32 = 7;
/// Each reader increments the state by this amount.
const ONE_READER: u32 = 1;
/// Mask for extracting the reader count (bits 0-29).
const READERS_MASK: u32 = WRITE_LOCKED - 1;

/// Futex-based raw reader-writer lock.
///
/// Implements `lock_api::RawRwLock` and `lock_api::RawRwLockTimed`.
pub struct NoxuRawRwLock {
    /// Combined state: reader count (bits 0-29) | `WRITE_LOCKED` (bit 30).
    /// See the module doc comment for the full state encoding.
    pub(crate) state: AtomicU32,
    /// Dedicated futex word for writers that have not yet reserved
    /// `WRITE_LOCKED` (another writer currently holds or has reserved it).
    ///
    /// Writers park here rather than on `state`, so a release can wake exactly
    /// ONE writer. On a shared word that is unsound: the single wakeup may land
    /// on a reader which re-parks and swallows it, stranding the writer -- so a
    /// shared word forces `futex_wake(ALL)` and a thundering herd on every
    /// transition.
    ///
    /// The value is a generation counter bumped on every notification. A writer
    /// samples it BEFORE testing `state` and passes the sample to `futex_wait`,
    /// so a release landing between the test and the park has already moved the
    /// counter and the wait returns immediately instead of sleeping through it.
    write_futex: AtomicU32,
    /// Dedicated futex word for the CURRENT reservation-holder only.
    ///
    /// At most one writer is ever `reserved_draining` at a time (the
    /// reserve CAS's own exclusivity on `WRITE_LOCKED`), so a targeted
    /// wake here is unambiguous by construction — no reader or other
    /// writer is ever asleep on this address to swallow it. Uses the same
    /// generation-counter idiom as `write_futex` (see its doc comment), and
    /// for the identical reason: the last draining reader bumps this
    /// BEFORE waking, and the reserving writer samples it BEFORE checking
    /// `state`, so a hand-off landing in the gap cannot be missed.
    drain_futex: AtomicU32,
    /// Number of reader threads sleeping in futex_wait.
    read_waiters: AtomicUsize,
    /// Number of writer threads sleeping in futex_wait that have NOT YET
    /// reserved `WRITE_LOCKED`. See the field's own note above the struct.
    write_waiters: AtomicUsize,
    /// Thread ID hash of the exclusive owner (0 if not write-locked). Set
    /// only once `write_held` is reached, never while merely reserved.
    pub(crate) exclusive_owner: AtomicU64,
}

// SAFETY: `lock_api::RawRwLock` requires that this type actually provide
// mutual exclusion between exclusive holders, and shared-but-not-exclusive
// access between shared holders, so that `lock_api` may hand out `&mut T` to an
// exclusive holder and `&T` to shared holders.
//
// That holds here: `state` packs a reader count (low bits, `READERS_MASK`) with
// a `WRITE_LOCKED` bit (1 << 30). Every transition that sets `WRITE_LOCKED` is a
// `compare_exchange`/`compare_exchange_weak` conditioned on the bit being clear
// beforehand (never a blind `fetch_or`), so at most one writer ever holds it —
// this excludes every other writer immediately upon reservation, not just upon
// full acquisition, so two writers can never simultaneously believe they hold
// `write_held` (state table, failure mode 1). `lock_shared` only succeeds while
// `WRITE_LOCKED` is clear, so readers never coexist with a writer that has
// reserved OR holds the lock. All transitions are compare-exchange or
// fetch_sub/fetch_and on a single atomic word with Acquire on acquisition and
// Release on release, giving the happens-before edges `lock_api` relies on.
//
// The exclusive-vs-reserved distinction that new bit meaning introduces is
// confined to `is_locked_exclusive`/`is_write_locked` and the internal
// transition logic below; `lock_api`'s contract only cares that a caller
// granted exclusive access (i.e., one who observed `write_held`, never merely
// `reserved_draining`) has genuine exclusion, which the reserve-then-drain
// CAS sequence provides (state table, failure mode 3: reaching
// `READERS_MASK == 0` while reserved is an OBSERVATION of an already-held
// exclusion, never a second acquisition attempt).
//
// Liveness (not a soundness property, but worth stating next to the contract):
// the lock is writer-preferring via reservation once a writer has failed to
// find a free window (`WRITE_SPIN_ATTEMPTS`), so a reader stream cannot starve
// a queued writer — the reserve CAS is guarded ONLY on `WRITE_LOCKED` being
// clear, never on the reader count, so an overlapping reader relay cannot
// prevent it (validated as a shuttle model property,
// `writer_can_reserve_under_an_overlapping_reader_relay`). Writers are not
// ordered among themselves. Neither property affects the exclusion guarantees
// this `unsafe impl` asserts.
unsafe impl lock_api::RawRwLock for NoxuRawRwLock {
    const INIT: Self = NoxuRawRwLock {
        state: AtomicU32::new(0),
        write_futex: AtomicU32::new(0),
        drain_futex: AtomicU32::new(0),
        read_waiters: AtomicUsize::new(0),
        write_waiters: AtomicUsize::new(0),
        exclusive_owner: AtomicU64::new(0),
    };

    type GuardMarker = lock_api::GuardSend;

    // -----------------------------------------------------------------------
    // Shared (read) lock
    // -----------------------------------------------------------------------

    #[inline]
    fn lock_shared(&self) {
        if !self.try_lock_shared_fast() {
            self.lock_shared_slow(None);
        }
    }

    #[inline]
    fn try_lock_shared(&self) -> bool {
        self.try_lock_shared_fast()
    }

    #[inline]
    unsafe fn unlock_shared(&self) {
        let prev = self.state.fetch_sub(ONE_READER, Ordering::Release);
        // `prev` is the atomic snapshot THIS release observed, so branching on
        // it is well-defined even though a writer's reserve-CAS may be racing
        // concurrently — see the state table's note on the read_held(1) ->
        // free race for why no third case can arise.
        if prev & READERS_MASK == ONE_READER {
            if prev & WRITE_LOCKED != 0 {
                // We were the last reader draining an outstanding
                // reservation: reserved_draining(1) -> write_held. The
                // reserving writer needs no CAS and no re-race here -- it
                // already owns WRITE_LOCKED; this wakeup is purely "stop
                // waiting and notice you're done," which is the entire fix
                // for the tail-latency problem this module exists to close.
                self.notify_drain();
            } else if self.write_waiters.load(Ordering::Relaxed) > 0 {
                // read_held(1) -> free, with a writer queued but not yet
                // reserved: hand off to ONE such writer.
                self.notify_one_writer();
            }
        }
        // Otherwise the reader count is still > 0 after this release --
        // read_held(n>1) -> read_held(n-1), or reserved_draining(n>1) ->
        // reserved_draining(n-1) -- nobody needs to be woken either way (the
        // reserving writer, if any, only cares about the count reaching 0).
    }

    // -----------------------------------------------------------------------
    // Exclusive (write) lock
    // -----------------------------------------------------------------------

    #[inline]
    fn lock_exclusive(&self) {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.exclusive_owner
                .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
            return;
        }
        self.lock_exclusive_slow(None);
    }

    #[inline]
    fn try_lock_exclusive(&self) -> bool {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.exclusive_owner
                .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    #[inline]
    unsafe fn unlock_exclusive(&self) {
        self.exclusive_owner.store(0, Ordering::Relaxed);
        // We are the sole possible mutator here: while WRITE_LOCKED is set
        // AND the reader count is zero (write_held), readers refuse
        // admission (gated on WRITE_LOCKED) and other writers' reserve-CAS
        // fails (also gated on WRITE_LOCKED), so nothing else can touch
        // `state` until this clears the bit. A plain fetch_and is therefore
        // sufficient -- no CAS/retry needed, and there are no reader bits to
        // preserve since the count is provably 0 here.
        self.state.fetch_and(!WRITE_LOCKED, Ordering::Release);

        // Wake BOTH populations unconditionally -- state table, "write_held
        // -- writer release" row, and failure mode 4's fix: an `else if`
        // here is exactly the bug that starved readers whenever a writer
        // happened to be queued.
        if self.write_waiters.load(Ordering::Relaxed) > 0 {
            self.notify_one_writer();
        }
        if self.read_waiters.load(Ordering::Relaxed) > 0 {
            // i32::MAX as u32 — kernel nr_wake is signed; u32::MAX wraps to -1.
            futex_wake(&self.state, i32::MAX as u32);
        }
    }

    #[inline]
    fn is_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }

    #[inline]
    fn is_locked_exclusive(&self) -> bool {
        // `WRITE_LOCKED` alone is ambiguous between reserved-and-draining and
        // genuinely held (state table, failure mode 2) -- true exclusive
        // ownership additionally requires the reader count to be zero. A
        // reservation with readers still in flight MUST report `false` here:
        // callers use this predicate to assert "nobody else can be touching
        // the protected data," which is false while readers are still
        // draining.
        let state = self.state.load(Ordering::Relaxed);
        state & WRITE_LOCKED != 0 && state & READERS_MASK == 0
    }
}

unsafe impl lock_api::RawRwLockTimed for NoxuRawRwLock {
    type Duration = Duration;
    type Instant = Instant;

    fn try_lock_shared_for(&self, timeout: Duration) -> bool {
        if self.try_lock_shared_fast() {
            return true;
        }
        self.lock_shared_slow(Some(Instant::now() + timeout))
    }

    fn try_lock_shared_until(&self, deadline: Instant) -> bool {
        if self.try_lock_shared_fast() {
            return true;
        }
        self.lock_shared_slow(Some(deadline))
    }

    fn try_lock_exclusive_for(&self, timeout: Duration) -> bool {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.exclusive_owner
                .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
            return true;
        }
        self.lock_exclusive_slow(Some(Instant::now() + timeout))
    }

    fn try_lock_exclusive_until(&self, deadline: Instant) -> bool {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.exclusive_owner
                .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
            return true;
        }
        self.lock_exclusive_slow(Some(deadline))
    }
}

impl NoxuRawRwLock {
    /// Fast path: try to increment reader count when no writer has reserved
    /// or holds the lock.
    #[inline]
    fn try_lock_shared_fast(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        // Refuse admission whenever a writer has reserved or holds the lock
        // -- state table: every `reserved_draining`/`write_held` row refuses
        // reader admission unconditionally. This is what makes reservation
        // the whole fix: the instant a writer reserves, no MORE readers can
        // ever join, so the count is now monotonically non-increasing and
        // must reach zero.
        if state & WRITE_LOCKED != 0 {
            return false;
        }
        // No overflow check: WRITE_LOCKED bit acts as sentinel.
        self.state
            .compare_exchange(
                state,
                state + ONE_READER,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Slow path for shared lock with optional deadline.
    fn lock_shared_slow(&self, deadline: Option<Instant>) -> bool {
        loop {
            let state = self.state.load(Ordering::Relaxed);

            // Same admission rule as the fast path.
            if state & WRITE_LOCKED == 0 {
                if self
                    .state
                    .compare_exchange_weak(
                        state,
                        state + ONE_READER,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return true;
                }
                // CAS failed — retry.
                continue;
            }

            // A writer has reserved or holds the lock — park until released.
            let timeout = match deadline {
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        return false;
                    }
                    Some(dl - now)
                }
                None => None,
            };

            self.read_waiters.fetch_add(1, Ordering::Relaxed);
            futex_wait(&self.state, state, timeout);
            let did_timeout =
                deadline.map(|dl| Instant::now() >= dl).unwrap_or(false);
            self.read_waiters.fetch_sub(1, Ordering::Relaxed);

            if did_timeout {
                return false;
            }
        }
    }

    /// A writer gives up BEFORE ever reserving `WRITE_LOCKED` (still
    /// spinning, or parked on `write_futex` waiting for another writer's
    /// reservation/hold to clear).
    ///
    /// Never touches `state` -- there is nothing to undo, only the
    /// `write_waiters` bookkeeping. Contrast with `try_give_up_reservation`,
    /// which handles the give-up path AFTER a reservation has already been
    /// made and must therefore also release `WRITE_LOCKED`.
    fn abandon_before_reserving(&self) {
        self.write_waiters.fetch_sub(1, Ordering::Relaxed);
    }

    /// Attempt to give up an OUTSTANDING RESERVATION because the caller's
    /// deadline elapsed. This is the exact path the task brief warned about:
    /// "a reserved writer that times out must release WRITE_LOCKED, or the
    /// lock is permanently dead — readers park on a bit with no owner and no
    /// unlock coming."
    ///
    /// Returns `true` if the give-up won the race against the
    /// drain-completing reader (state is restored to `read_held`, both
    /// populations woken). Returns `false` if the drain had ALREADY
    /// completed by the time this ran -- in that case the CALLER now
    /// legitimately owns `write_held` and MUST NOT treat this as a failed
    /// acquisition; see `wait_for_drain`'s use of this return value.
    ///
    /// Does NOT touch `write_waiters`: it was already decremented at
    /// reservation time (state table, failure mode 4's fix), so this must
    /// not decrement it again.
    fn try_give_up_reservation(&self) -> bool {
        loop {
            let state = self.state.load(Ordering::Relaxed);
            if state & READERS_MASK == 0 {
                // The drain won the race: this thread already owns the
                // lock. Do not clobber `state` -- there is nothing to give
                // up.
                return false;
            }
            if self
                .state
                .compare_exchange_weak(
                    state,
                    state & !WRITE_LOCKED,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // Wake BOTH populations unconditionally -- same Bug-4
                // reasoning as `unlock_exclusive`: a queued writer and
                // queued readers can legitimately coexist, and both deserve
                // a chance to race for the lock this give-up just freed.
                if self.write_waiters.load(Ordering::Relaxed) > 0 {
                    self.notify_one_writer();
                }
                if self.read_waiters.load(Ordering::Relaxed) > 0 {
                    futex_wake(&self.state, i32::MAX as u32);
                }
                return true;
            }
            // CAS failed -- state changed concurrently (most likely a
            // reader released). Retry from the top, which re-checks
            // READERS_MASK == 0 first on the fresh value.
        }
    }

    /// Wait for the drain to complete after this thread has already
    /// reserved `WRITE_LOCKED` (state is `reserved_draining(n)` for some
    /// `n > 0`).
    ///
    /// Returns `true` once this thread legitimately owns `write_held`
    /// (either the drain completed normally, or a give-up attempt on
    /// timeout raced the drain and LOST). Returns `false` only if the
    /// deadline elapsed and the give-up won cleanly.
    fn wait_for_drain(&self, deadline: Option<Instant>) -> bool {
        // Spin briefly before ever parking -- see
        // `DRAIN_SPIN_RELAX_ATTEMPTS`/`DRAIN_SPIN_YIELD_ATTEMPTS`'s doc
        // comment for why this is a pure latency win with no admission
        // cost (unlike the barging spin in `lock_exclusive_slow`), and why
        // it MUST yield rather than stay a flat busy-spin once the short
        // relax phase is exhausted.
        for i in 0..(DRAIN_SPIN_RELAX_ATTEMPTS + DRAIN_SPIN_YIELD_ATTEMPTS) {
            if self.state.load(Ordering::Relaxed) & READERS_MASK == 0 {
                self.exclusive_owner
                    .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
                return true;
            }
            if i < DRAIN_SPIN_RELAX_ATTEMPTS {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }

        loop {
            // Sample the generation BEFORE testing `state`; see
            // `drain_futex`'s doc comment for why (identical reasoning to
            // `write_futex`).
            let seen_gen = self.drain_futex.load(Ordering::Acquire);
            let state = self.state.load(Ordering::Relaxed);

            if state & READERS_MASK == 0 {
                self.exclusive_owner
                    .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
                return true;
            }

            if let Some(dl) = deadline
                && Instant::now() >= dl
            {
                if self.try_give_up_reservation() {
                    return false;
                }
                // Give-up lost the race: the drain completed concurrently
                // and this thread now legitimately owns write_held.
                self.exclusive_owner
                    .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
                return true;
            }

            let timeout =
                deadline.map(|dl| dl.saturating_duration_since(Instant::now()));
            futex_wait(&self.drain_futex, seen_gen, timeout);
        }
    }

    /// Wake exactly ONE blocked writer that has not yet reserved
    /// `WRITE_LOCKED`.
    ///
    /// Bumps the generation counter first (so a writer that sampled the previous
    /// value cannot park on a condition that has already passed), then wakes a
    /// single waiter from the dedicated writer word.
    #[inline]
    fn notify_one_writer(&self) {
        self.write_futex.fetch_add(1, Ordering::Release);
        futex_wake(&self.write_futex, 1);
    }

    /// Wake the reserving writer whose drain just completed.
    ///
    /// Bumps the generation counter first (mirrors `notify_one_writer`), then
    /// wakes the ONE thread parked on `drain_futex`. Unambiguous by
    /// construction: at most one writer is ever `reserved_draining` at a
    /// time (the reserve-CAS's own exclusivity), so there is structurally
    /// nobody else asleep on this address who could swallow the wakeup --
    /// this is the actual fix for the tail-latency gap this module exists
    /// to close (see the module doc comment and
    /// `docs/src/internal/rwlock-state-table-2026-09.md`).
    #[inline]
    fn notify_drain(&self) {
        self.drain_futex.fetch_add(1, Ordering::Release);
        futex_wake(&self.drain_futex, 1);
    }

    /// Slow path for exclusive lock with optional deadline.
    fn lock_exclusive_slow(&self, deadline: Option<Instant>) -> bool {
        self.write_waiters.fetch_add(1, Ordering::Relaxed);

        // Spin briefly for a GENUINELY FREE lock before committing to a
        // reservation.
        //
        // Reserving excludes every future reader immediately (unlike the old
        // advisory gate, which was merely a hint), so reserving on the very
        // first CAS failure would be as costly as an immediately-closing
        // gate: a writer landing on the B-tree root -- read-latched by every
        // descent -- would stall every reader in the engine behind one
        // background split or eviction. Measured at 64 threads read-heavy:
        // 78k ops/s with an immediate reservation vs 631k with no
        // reservation mechanism at all vs parking_lot's 691k.
        //
        // Most contention here is a brief overlap with a reader that is
        // about to release. Spinning through that window keeps the barging
        // fast path for the common case and reserves the reservation
        // mechanism for a writer that has genuinely failed to find a free
        // window — the same trade `parking_lot` makes by setting
        // `WRITER_BIT` only once a writer has actually parked.
        for _ in 0..WRITE_SPIN_ATTEMPTS {
            let state = self.state.load(Ordering::Relaxed);
            if state & (READERS_MASK | WRITE_LOCKED) == 0 {
                if self
                    .state
                    .compare_exchange_weak(
                        state,
                        WRITE_LOCKED,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.write_waiters.fetch_sub(1, Ordering::Relaxed);
                    self.exclusive_owner.store(
                        crate::raw_mutex::thread_id(),
                        Ordering::Relaxed,
                    );
                    return true;
                }
                continue;
            }
            std::hint::spin_loop();
        }

        // Spinning did not find a free window. RESERVE unconditionally: the
        // guard is ONLY `WRITE_LOCKED == 0` -- never the reader count -- so
        // an overlapping reader relay cannot prevent this from succeeding
        // (state table, "read_held(n) -- writer reserve" row; validated as
        // the shuttle model's `writer_can_reserve_under_an_overlapping_reader_relay`
        // property). This is the fix for the whole tail-latency problem: once
        // reserved, the eventual hand-off from the last draining reader is
        // unconditional (see `unlock_shared`'s `notify_drain` branch), not a
        // re-race.
        loop {
            // Sample the writer generation BEFORE testing `state`; see
            // `write_futex`'s doc comment.
            let seen_gen = self.write_futex.load(Ordering::Acquire);
            let state = self.state.load(Ordering::Relaxed);

            if state & WRITE_LOCKED == 0 {
                match self.state.compare_exchange_weak(
                    state,
                    state | WRITE_LOCKED,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // This writer is no longer "waiting to reserve" --
                        // decrement now, not at final acquisition (state
                        // table, failure mode 4's fix: a writer mid-drain
                        // must NOT keep inflating write_waiters).
                        self.write_waiters.fetch_sub(1, Ordering::Relaxed);

                        // The OR only touched bit 30, so the reader bits in
                        // `state` (the CAS's expected/old value) are exactly
                        // the reader count of the new state too -- no reload
                        // needed to determine whether a drain is required.
                        let readers_now = state & READERS_MASK;
                        if readers_now == 0 {
                            // The lock was actually free at the moment of
                            // reservation (state table: this degenerates to
                            // the "free -- writer acquire" row via the same
                            // CAS) -- no drain needed, already write_held.
                            self.exclusive_owner.store(
                                crate::raw_mutex::thread_id(),
                                Ordering::Relaxed,
                            );
                            return true;
                        }
                        return self.wait_for_drain(deadline);
                    }
                    Err(_) => continue,
                }
            }

            // Another writer already reserved or holds WRITE_LOCKED -- park
            // on write_futex until it might have cleared.
            let timeout = match deadline {
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        self.abandon_before_reserving();
                        return false;
                    }
                    Some(dl - now)
                }
                None => None,
            };

            futex_wait(&self.write_futex, seen_gen, timeout);

            if deadline.map(|dl| Instant::now() >= dl).unwrap_or(false) {
                self.abandon_before_reserving();
                return false;
            }
        }
    }

    /// Returns `true` if the write lock is currently HELD (not merely
    /// reserved-and-draining). See `is_locked_exclusive`'s doc comment for
    /// why the reader count must also be checked.
    #[inline]
    pub fn is_write_locked(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        state & WRITE_LOCKED != 0 && state & READERS_MASK == 0
    }

    /// Returns the total number of threads waiting to acquire this lock.
    #[inline]
    pub fn get_n_waiters(&self) -> usize {
        self.read_waiters.load(Ordering::Relaxed)
            + self.write_waiters.load(Ordering::Relaxed)
    }

    /// Returns the number of active readers.
    #[inline]
    pub fn reader_count(&self) -> u32 {
        self.state.load(Ordering::Relaxed) & READERS_MASK
    }

    /// Returns the exclusive owner thread ID hash (0 if not write-locked).
    #[inline]
    pub fn get_exclusive_owner(&self) -> u64 {
        self.exclusive_owner.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lock_api::{RawRwLock as _, RawRwLockTimed as _};
    use std::sync::Arc;

    /// `try_lock_exclusive` on the raw lock (the `lock_api::RawRwLock`
    /// entry point, not the wrapping `noxu_sync::RwLock`) must succeed
    /// when free and report the acquiring thread as the exclusive owner.
    #[test]
    fn raw_try_lock_exclusive_succeeds_when_free_and_sets_owner() {
        let raw = NoxuRawRwLock::INIT;
        assert!(!raw.is_locked());
        assert!(!raw.is_locked_exclusive());
        assert_eq!(raw.get_exclusive_owner(), 0);

        assert!(raw.try_lock_exclusive());
        assert!(raw.is_locked());
        assert!(raw.is_locked_exclusive());
        assert!(raw.is_write_locked());
        assert_ne!(
            raw.get_exclusive_owner(),
            0,
            "owner must be recorded after acquiring the write lock"
        );

        unsafe { raw.unlock_exclusive() };
        assert!(!raw.is_locked());
        assert_eq!(
            raw.get_exclusive_owner(),
            0,
            "owner must be cleared on unlock_exclusive"
        );
    }

    /// `try_lock_exclusive` must fail (not block, not panic) when a
    /// shared reader already holds the lock.
    #[test]
    fn raw_try_lock_exclusive_fails_while_read_locked() {
        let raw = NoxuRawRwLock::INIT;
        raw.lock_shared();
        assert!(raw.is_locked());
        assert!(!raw.is_locked_exclusive());
        assert_eq!(raw.reader_count(), 1);

        assert!(
            !raw.try_lock_exclusive(),
            "exclusive acquire must fail while a reader holds the lock"
        );

        unsafe { raw.unlock_shared() };
        assert_eq!(raw.reader_count(), 0);
        assert!(!raw.is_locked());
    }

    /// `try_lock_exclusive_for` / `try_lock_exclusive_until` (the
    /// `RawRwLockTimed` entry points) must time out rather than block
    /// forever when the lock is held, and must return control to the
    /// caller with `false`.
    #[test]
    fn raw_try_lock_exclusive_for_times_out_when_contended() {
        let raw = Arc::new(NoxuRawRwLock::INIT);
        assert!(raw.try_lock_exclusive());

        let raw2 = Arc::clone(&raw);
        let timed_out = std::thread::spawn(move || {
            !raw2.try_lock_exclusive_for(Duration::from_millis(30))
        })
        .join()
        .unwrap();
        assert!(timed_out);

        unsafe { raw.unlock_exclusive() };
    }

    /// A writer that RESERVES against a reader and then times out because
    /// the reader never releases must give up cleanly: `WRITE_LOCKED` is
    /// released, `is_locked_exclusive()`/`is_write_locked()` both report
    /// `false` again, and a SUBSEQUENT reader can still be admitted -- i.e.
    /// the lock is not left permanently dead. This is the exact failure
    /// mode the task brief called out: "a reserved writer that times out
    /// must release WRITE_LOCKED, or the lock is permanently dead."
    #[test]
    fn raw_reserved_writer_gives_up_cleanly_on_timeout_without_deadlocking_the_lock()
     {
        let raw = Arc::new(NoxuRawRwLock::INIT);
        raw.lock_shared();
        assert_eq!(raw.reader_count(), 1);

        let raw2 = Arc::clone(&raw);
        let timed_out = std::thread::spawn(move || {
            !raw2.try_lock_exclusive_for(Duration::from_millis(50))
        })
        .join()
        .unwrap();
        assert!(timed_out, "writer must time out while the reader holds on");

        // The lock must NOT be dead: is_locked_exclusive/is_write_locked
        // must both report false (the reservation was released, not left
        // dangling), and a fresh reader must still be admittable.
        assert!(
            !raw.is_locked_exclusive(),
            "a timed-out reservation must not leave the lock reporting \
             exclusive ownership"
        );
        assert!(!raw.is_write_locked());
        assert!(
            raw.try_lock_shared(),
            "a fresh reader must still be admitted after a reservation \
             timeout -- if this fails, WRITE_LOCKED was left set with no \
             owner and no unlock coming, i.e. the lock is permanently dead"
        );
        unsafe { raw.unlock_shared() };
        unsafe { raw.unlock_shared() };
        assert!(!raw.is_locked());
    }

    /// `try_lock_shared_for` on a write-locked raw lock must time out and
    /// leave `get_n_waiters()` back at zero once the parked reader gives up
    /// -- proves the waiter counter used by `noxu_sync::RwLock::get_n_waiters`
    /// is not leaked on a timeout path.
    #[test]
    fn raw_try_lock_shared_for_times_out_and_clears_waiter_count() {
        let raw = Arc::new(NoxuRawRwLock::INIT);
        assert!(raw.try_lock_exclusive());

        let raw2 = Arc::clone(&raw);
        let timed_out = std::thread::spawn(move || {
            !raw2.try_lock_shared_for(Duration::from_millis(30))
        })
        .join()
        .unwrap();
        assert!(timed_out);
        assert_eq!(
            raw.get_n_waiters(),
            0,
            "waiter count must be decremented on the timeout path"
        );

        unsafe { raw.unlock_exclusive() };
    }

    /// A writer parked behind a reader must be woken and granted the lock
    /// once the reader releases it -- exercises the real
    /// `lock_exclusive_slow` park/wake path (not just the CAS fast path)
    /// together with `unlock_shared`'s reservation-drain-complete branch.
    ///
    /// Also pins failure mode 2 from the state table: while the reader
    /// still holds the lock (and the writer has therefore RESERVED, not
    /// acquired), `is_locked_exclusive()` must report `false` -- a
    /// reservation is not ownership.
    #[test]
    fn raw_writer_parks_behind_reader_then_acquires_on_release() {
        let raw = Arc::new(NoxuRawRwLock::INIT);
        raw.lock_shared();

        let raw2 = Arc::clone(&raw);
        let writer = std::thread::spawn(move || {
            raw2.lock_exclusive();
            unsafe { raw2.unlock_exclusive() };
        });

        // Give the writer a chance to park behind the reader.
        std::thread::sleep(Duration::from_millis(30));
        assert!(!raw.is_locked_exclusive(), "reader still holds the lock");

        unsafe { raw.unlock_shared() };
        writer.join().unwrap();

        // Lock must be free again after the writer completed.
        assert!(!raw.is_locked());
    }

    /// `try_lock_exclusive_until` (deadline form) must also succeed on the
    /// fast (uncontended) path, exercising the CAS-success branch that
    /// `try_lock_exclusive_for` shares by delegation.
    #[test]
    fn raw_try_lock_exclusive_until_succeeds_fast_path() {
        let raw = NoxuRawRwLock::INIT;
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(raw.try_lock_exclusive_until(deadline));
        assert!(raw.is_locked_exclusive());
        unsafe { raw.unlock_exclusive() };
    }
}
