//! Futex-based raw reader-writer lock implementing `lock_api::RawRwLock`.
//!
//! Design derived directly from
//! `docs/src/internal/rwlock-state-table-2026-09.md` — read that document
//! first; this module is its implementation, and every non-obvious ordering
//! or branch below cites the table row it comes from.
//!
//! # State encoding (single `AtomicU32`)
//!
//!   bits 0-28: reader count  (`ONE_READER` = 1, max ~536 million concurrent readers)
//!   bit  29:   `WRITERS_PARKED` — **writer wake gate**: set by a writer in
//!              `lock_exclusive_slow` before it commits to
//!              `futex_wait(&write_futex, …)`, and read-then-cleared by any
//!              release that could let a parked writer proceed. Same purpose
//!              as `READERS_PARKED` but for the writer population (which parks
//!              on `write_futex`, not `state`): it lets a release decide
//!              whether to issue the targeted `write_futex` wake from the
//!              same `state` snapshot that cleared `WRITE_LOCKED`, so the
//!              uncontended path makes NO syscall. See "The lost-wake fix".
//!   bit  30:   `WRITE_LOCKED` — **reservation-or-hold**, disambiguated by the
//!              reader count: `WRITE_LOCKED != 0 && READERS_MASK == 0` means a
//!              writer holds the lock; `WRITE_LOCKED != 0 && READERS_MASK > 0`
//!              means a writer has reserved it and is waiting for the
//!              in-flight readers to drain.
//!   bit  31:   `READERS_PARKED` — **reader wake gate**: set by a reader in
//!              `lock_shared_slow` immediately before it commits to
//!              `futex_wait(&state, …)`, and cleared by any release that
//!              re-opens reader admission (clears `WRITE_LOCKED`). It lives
//!              in `state` ON PURPOSE: the writer's "should I wake parked
//!              readers?" decision is made from the SAME word the reader
//!              parks on (the value returned by the clearing RMW), so there
//!              is no store→load race between two different atomics. See
//!              "The lost-wake fix" below. (This bit was formerly the
//!              advisory `WRITE_WAITING`; see "What changed".)
//!
//! # The lost-wake fix (2026-10): wake decisions gated on `state`, not on a
//! separate counter
//!
//! The shipped design gated BOTH wake populations on the auxiliary waiter
//! COUNTERS (`read_waiters`/`write_waiters`), loaded `Relaxed`, right after a
//! `Release` store to `state`. That is a Dekker / store-buffer (StoreLoad)
//! pattern across two DIFFERENT atomics:
//!
//!   reader park:    read_waiters.fetch_add(1, …) then load(state) [via futex_wait]
//!   writer release: fetch_and(state, …)          then load(read_waiters)
//!
//! Each side stores one location and then loads the other. Without a
//! StoreLoad barrier on BOTH sides, the writer can load `read_waiters == 0`
//! (the reader's increment not yet visible) while the reader's futex re-check
//! still sees `WRITE_LOCKED` set (the writer's clear not yet visible): the
//! writer skips the wake and the reader sleeps forever on a lock that is
//! actually free. On x86 every `LOCK`-prefixed RMW is a full barrier so the
//! race is unobservable; on aarch64 the LSE `ldclrl`/`ldadd` Release/Relaxed
//! forms are NOT StoreLoad barriers, so it is reachable — reproduced at a ~5%
//! hang rate against the engine's handle map (`db_map`), and shown reachable
//! by a loom model of the exact pattern.
//!
//! The fix follows `raw_mutex`'s proven shape: gate every wake on the SAME
//! atomic word the sleeper waits on. Readers park on `state`; the
//! `READERS_PARKED` bit (bit 31) records "≥1 reader parked" IN `state`, so the
//! releasing writer's clearing RMW returns a snapshot that already tells it
//! whether to wake — no second atomic, no StoreLoad race. Writers park on
//! `write_futex` (with the generation-counter idiom that closes the
//! park-window), but "≥1 writer parked" is likewise recorded IN `state` as
//! `WRITERS_PARKED` (bit 29): a release reads that bit from the same snapshot
//! that cleared `WRITE_LOCKED` and only then issues the targeted `write_futex`
//! wake. This matters for more than correctness: gating the writer wake on
//! the word (rather than the old racy `write_waiters > 0`, OR an
//! unconditional wake) is what keeps the UNCONTENDED last-reader/writer
//! unlock free of any `futex_wake` syscall — critical on this (root) latch,
//! where an unconditional wake measured ~1 syscall per operation. The
//! `read_waiters` / `write_waiters` counters are RETAINED but are now purely
//! advisory (`get_n_waiters`): they no longer gate any wake, so their
//! `Relaxed` ordering can no longer cost a wakeup.
//!
//! This shape is provable under loom (Release/Acquire on one location),
//! unlike a SeqCst-on-the-counter fix: loom 0.7 under-models SeqCst's
//! StoreLoad guarantee (it reports the store-buffer litmus `r1==r2==0` even
//! under SeqCst), so a counter-based fix could not have been model-checked
//! here even if it were correct. Gating on `state` is also robust on every
//! memory model, not only where a correctly-placed SeqCst fence would suffice.
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
//! The `WRITE_SPIN_ATTEMPTS` barging spin still runs before any reservation
//! is attempted, with the same total budget (400 iterations) as the
//! pre-reservation design: reserving on every momentary overlap with a
//! departing reader would be as costly as the old immediately-closing gate
//! (measured 2.9x throughput cost against the B-tree root), so a writer
//! still spins for a genuinely free lock first and only reserves once that
//! has failed. Its internal spin STRATEGY changed (exponential backoff
//! between re-reads of `state` rather than reloading every iteration --
//! see `WRITE_SPIN_ATTEMPTS`'s doc comment); the exit condition and budget
//! did not.
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
/// State bit meaning "at least one reader is parked in `futex_wait(&state)`".
///
/// Set by `lock_shared_slow` immediately before it commits to parking, and
/// cleared by any release that re-opens reader admission (`unlock_exclusive`
/// on `write_held -> free`, and `try_give_up_reservation` on
/// `reserved_draining -> read_held`). It is the rwlock analogue of
/// `raw_mutex`'s `LOCKED_CONTENDED`: a wake-gate that lives in the SAME word
/// the sleeper parks on, so the waker decides whether to wake from the
/// snapshot its own clearing RMW returns — never from a separate counter that
/// could be stale under a relaxed store→load (the lost-wake bug this fixes;
/// see the module doc comment's "The lost-wake fix" section).
///
/// Occupies bit 31, reclaimed from the deleted advisory `WRITE_WAITING`.
const READERS_PARKED: u32 = 1 << 31;
/// State bit meaning "at least one writer is parked on `write_futex`".
///
/// The writer-side analogue of `READERS_PARKED`: set by a writer in
/// `lock_exclusive_slow` before it commits to `futex_wait(&write_futex)`, and
/// read (then cleared) by any release that could let a parked writer proceed
/// (`unlock_exclusive`, `try_give_up_reservation`, and `unlock_shared`'s
/// `read_held(1) -> free`). It makes the writer wake a SAME-WORD gate too:
/// without it, those releases would have to either read the racy
/// `write_waiters` counter (the lost-wake bug) or wake `write_futex`
/// UNCONDITIONALLY — and the latter costs a `futex_wake` SYSCALL on every
/// uncontended last-reader/writer unlock of this (root) latch, measured at
/// ~1 syscall/op. Gating on this bit restores the zero-syscall uncontended
/// path while keeping the wake race-free: the release reads the bit from the
/// same RMW snapshot that cleared `WRITE_LOCKED`, and only then issues the
/// targeted `write_futex` wake.
///
/// Occupies bit 29, shrinking `READERS_MASK` to bits 0-28 (max ~536 million
/// concurrent readers — see `READERS_MASK`'s overflow `debug_assert` in
/// `try_lock_shared_fast`).
const WRITERS_PARKED: u32 = 1 << 29;
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
///
/// This table was measured against a flat per-iteration `state.load()`,
/// which the writer-reservation port's spin loop no longer does (see
/// `lock_exclusive_slow`'s exponential-backoff comment): under heavy read
/// contention a flat reload pays a contended-cache-line cost on every one
/// of these 400 iterations, which was later found to be the dominant term
/// in write-tail latency at high reader counts
/// (`docs/src/internal/parking-lot-removal-2026-09.md`, "Third attempt:
/// shipped"). The iteration BUDGET (400) is unchanged from this table --
/// only how often `state` is actually reloaded while spinning changed. Not
/// independently re-measured against this exact microbenchmark, but the
/// full engine A/B (`noxu-xbench`, ycsb_c) showed +4.2% read-only
/// throughput with the backoff version, so the historical throughput
/// number above has not regressed in practice.
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
// throughout. `wait_for_drain` below is that same two-phase strategy.
const DRAIN_SPIN_RELAX_ATTEMPTS: u32 = 3;
const DRAIN_SPIN_YIELD_ATTEMPTS: u32 = 7;
/// Each reader increments the state by this amount.
const ONE_READER: u32 = 1;
/// Mask for extracting the reader count (bits 0-28).
const READERS_MASK: u32 = WRITERS_PARKED - 1;

/// Futex-based raw reader-writer lock.
///
/// Implements `lock_api::RawRwLock` and `lock_api::RawRwLockTimed`.
pub struct NoxuRawRwLock {
    /// Combined state: reader count (bits 0-28) | `WRITERS_PARKED` (bit 29) |
    /// `WRITE_LOCKED` (bit 30) | `READERS_PARKED` (bit 31).
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
    /// Number of reader threads sleeping in futex_wait. **Advisory only**
    /// (`get_n_waiters`): the decision to wake parked readers is gated on the
    /// `READERS_PARKED` bit in `state`, never on this counter — see the module
    /// doc comment's "The lost-wake fix" section.
    read_waiters: AtomicUsize,
    /// Number of writer threads sleeping in futex_wait that have NOT YET
    /// reserved `WRITE_LOCKED`. **Advisory only** (`get_n_waiters`): writer
    /// wakes are now unconditional (the `write_futex` generation idiom is the
    /// gate), never conditioned on this counter — see the module doc comment's
    /// "The lost-wake fix" section.
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
// That holds here: `state` packs a reader count (low bits, `READERS_MASK` =
// bits 0-28) with a `WRITE_LOCKED` bit (1 << 30) and two wake-gate bits,
// `WRITERS_PARKED` (1 << 29) and `READERS_PARKED` (1 << 31).
// Every transition that sets `WRITE_LOCKED` is a
// `compare_exchange`/`compare_exchange_weak` conditioned on the bit being clear
// beforehand (never a blind `fetch_or`), so at most one writer ever holds it —
// this excludes every other writer immediately upon reservation, not just upon
// full acquisition, so two writers can never simultaneously believe they hold
// `write_held` (state table, failure mode 1). `lock_shared` only succeeds while
// `WRITE_LOCKED` is clear, so readers never coexist with a writer that has
// reserved OR holds the lock. All transitions are compare-exchange or
// fetch_sub/fetch_and on a single atomic word with Acquire on acquisition and
// Release on release, giving the happens-before edges `lock_api` relies on.
// `READERS_PARKED`/`WRITERS_PARKED` affect neither exclusion nor the reader
// count (`READERS_MASK` is bits 0-28, disjoint from bits 29 and 31); they are
// pure wake-gates (module doc, "The lost-wake fix"), set/cleared alongside
// those transitions
// and never read as part of granting access.
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
            } else if prev & WRITERS_PARKED != 0 {
                // read_held(1) -> free with a writer parked on `write_futex`
                // (it set WRITERS_PARKED before parking). Same-word gate: we
                // learned a writer is waiting from our OWN fetch_sub snapshot
                // (`prev`), not from the racy `write_waiters` counter. Clear
                // the hint so a subsequent uncontended release does not
                // re-wake, then hand off to ONE writer. The common
                // uncontended case (`prev & WRITERS_PARKED == 0`) skips both
                // the clear and the `futex_wake` syscall entirely.
                self.state.fetch_and(!WRITERS_PARKED, Ordering::Relaxed);
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
        // We are the sole possible mutator of the WRITE_LOCKED / reader-count
        // bits here: while WRITE_LOCKED is set AND the reader count is zero
        // (write_held), readers refuse admission (gated on WRITE_LOCKED) and
        // other writers' reserve-CAS fails (also gated on WRITE_LOCKED), so
        // nothing else can change those bits until this clears WRITE_LOCKED.
        // The two bits a concurrent thread CAN still flip are the two
        // wake-gate bits: READERS_PARKED (bit 31), set by a reader that
        // arrived while we held the lock, and WRITERS_PARKED (bit 29), set by
        // another writer parked on `write_futex`. We clear WRITE_LOCKED and
        // BOTH gate bits together in a single Release RMW and read the PRIOR
        // value, so each wake decision below is made from the same word the
        // sleeper's gate lives in -- no store->load race against a separate
        // counter (the lost-wake bug; see the module doc comment). The reader
        // count is provably 0, so there are no reader bits to preserve.
        let prev = self.state.fetch_and(
            !(WRITE_LOCKED | READERS_PARKED | WRITERS_PARKED),
            Ordering::Release,
        );

        // Wake ONE writer IFF the snapshot we just cleared had WRITERS_PARKED
        // set -- the same-word gate (state table, "write_held -- writer
        // release" row; failure mode 4's fix requires waking BOTH populations
        // when both are present, which the two independent `if`s below do).
        // Gating on the bit (not an unconditional wake, not the racy
        // `write_waiters` load) is what keeps the UNCONTENDED write unlock
        // free of any `futex_wake` syscall. `notify_one_writer` bumps the
        // `write_futex` generation then wakes one.
        if prev & WRITERS_PARKED != 0 {
            self.notify_one_writer();
        }
        // Wake ALL parked readers IFF the snapshot had READERS_PARKED set.
        // If it was clear, no reader was parked (or the last one is about to
        // re-set it and re-check against the now-cleared WRITE_LOCKED, so it
        // will not park).
        if prev & READERS_PARKED != 0 {
            // i32::MAX as u32 — kernel nr_wake is signed; u32::MAX wraps to -1.
            futex_wake(&self.state, i32::MAX as u32);
        }
    }

    #[inline]
    fn is_locked(&self) -> bool {
        // "Locked" means a reader or writer actually HOLDS/reserves the lock:
        // reader count > 0 or WRITE_LOCKED set. The two wake-gate bits
        // (WRITERS_PARKED/READERS_PARKED) are explicitly masked out so a
        // transient "parked-bit set, nothing held" word (which can appear for
        // an instant in `unlock_shared`'s read_held(1)->free path, between
        // the reader-count decrement and clearing the gate bit) does not
        // report the lock as held.
        self.state.load(Ordering::Relaxed) & (READERS_MASK | WRITE_LOCKED) != 0
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

// SAFETY: `RawRwLockTimed` extends `RawRwLock` with deadline-bounded
// acquisition; it requires the same shared/exclusive exclusion guarantee,
// which the `RawRwLock` impl above establishes and documents. Each
// `try_lock_*_for` / `try_lock_*_until` here acquires only via the same
// fast-path CAS (`try_lock_shared_fast` / `try_lock_exclusive_fast`) or the
// same `*_slow` paths used by `RawRwLock`, and returns `false` without
// acquiring once the deadline passes — so a `true` return always means this
// thread now holds the requested (shared or exclusive) access.
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
        // Reader-count overflow guard. The count is bits 0-28 (`READERS_MASK`,
        // ~536 million); the next bit up is `WRITERS_PARKED`, so an overflow
        // would corrupt the wake-gate rather than hit a `WRITE_LOCKED`
        // sentinel (as it did before bit 29 was taken). ~536M concurrent
        // readers is far beyond any real thread count, so this is a
        // debug_assert, not a runtime branch on the hot path.
        debug_assert!(
            state & READERS_MASK != READERS_MASK,
            "reader count overflow: {state:#x} would spill into WRITERS_PARKED"
        );
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
                // Admit: bump the reader count. Preserve the gate bits
                // (READERS_PARKED, and WRITERS_PARKED if a writer is queued):
                // `state + ONE_READER` only touches the low count bits, and
                // they are cleared by the next writer release. Same overflow
                // guard as the fast path (count is bits 0-28; overflow would
                // spill into WRITERS_PARKED).
                debug_assert!(
                    state & READERS_MASK != READERS_MASK,
                    "reader count overflow: {state:#x} would spill into \
                     WRITERS_PARKED"
                );
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

            // A writer has reserved or holds the lock -- we must park. Record
            // "a reader is parked" IN `state` (bit 31) BEFORE parking, so the
            // releasing writer decides whether to wake us from the snapshot
            // its own clearing RMW returns -- the same-word gate that closes
            // the lost-wake race (module doc, "The lost-wake fix"). If the
            // CAS to set the bit fails, `state` changed underneath us; loop
            // and re-evaluate from the fresh value (it may now be
            // admittable, or already have the bit set).
            if state & READERS_PARKED == 0
                && self
                    .state
                    .compare_exchange_weak(
                        state,
                        state | READERS_PARKED,
                        // Relaxed is sufficient: this CAS publishes nothing
                        // the writer needs ordered against data; the writer
                        // reads this bit via its own Release RMW's return
                        // value, and our subsequent futex_wait re-checks
                        // `state` under the kernel's own barrier.
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_err()
            {
                continue;
            }
            // From here the value we must observe unchanged in order to sleep
            // is `state` WITH READERS_PARKED set.
            let parked_state = state | READERS_PARKED;

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

            // Advisory counter for get_n_waiters only -- NOT a wake gate.
            self.read_waiters.fetch_add(1, Ordering::Relaxed);
            // futex_wait parks ONLY if *state == parked_state; if the writer
            // already cleared WRITE_LOCKED/READERS_PARKED (its Release RMW
            // happens-before this load inside the syscall), the value differs
            // and we return immediately rather than sleeping through the
            // wake.
            futex_wait(&self.state, parked_state, timeout);
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
                    state & !(WRITE_LOCKED | READERS_PARKED | WRITERS_PARKED),
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // Wake ONE writer IFF this give-up's own CAS cleared a set
                // WRITERS_PARKED bit, and ALL readers IFF it cleared a set
                // READERS_PARKED bit -- the same-word gate as
                // `unlock_exclusive`. Both populations can legitimately
                // coexist behind the reservation (Bug-4 reasoning), and both
                // deserve a chance to race for the lock this give-up just
                // freed. Each gate reads `state` (the CAS's own expected
                // value), never a separate counter, so there is no
                // store->load race (module doc, "The lost-wake fix"), and the
                // uncontended give-up (neither bit set) makes no syscall.
                if state & WRITERS_PARKED != 0 {
                    self.notify_one_writer();
                }
                if state & READERS_PARKED != 0 {
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
        // Exponential backoff between re-reads of `state`, not a flat
        // per-iteration reload: under heavy read contention (many readers
        // doing fetch_add/fetch_sub on the same cache line) a flat
        // `state.load()` every iteration pays the contended-cache-line cost
        // on EVERY one of `WRITE_SPIN_ATTEMPTS` reads. Measured directly:
        // this was the dominant remaining term in the writer's p50 at 63
        // readers (~220us), not the drain wait -- confirmed by it being
        // present, unchanged, in the pre-reservation baseline this whole
        // module exists to improve on (same flat-reload loop, same
        // 400-iteration count). Backing off means paying that cache-miss
        // cost O(log iterations) times instead of O(iterations) times,
        // which is exactly `parking_lot_core::SpinWait`'s own strategy
        // (`cpu_relax(1 << counter)`, doubling each attempt) -- ported here
        // rather than re-derived, since it is solving the identical
        // problem.
        let mut backoff: u32 = 1;
        let mut iterations_spent = 0u32;
        while iterations_spent < WRITE_SPIN_ATTEMPTS {
            let state = self.state.load(Ordering::Relaxed);
            if state & (READERS_MASK | WRITE_LOCKED) == 0 {
                // No readers and no writer -- acquire. Use `state |
                // WRITE_LOCKED` rather than a bare `WRITE_LOCKED` so that if a
                // gate bit (WRITERS_PARKED/READERS_PARKED) is somehow set on
                // an otherwise-free word, this acquire PRESERVES it rather
                // than dropping it (which would strand that parked thread).
                // By the gate-bit invariant (a gate bit implies WRITE_LOCKED
                // set) this is normally a no-op difference, but keeping it
                // makes the acquire robust to any transient and matches the
                // reserve CAS below.
                if self
                    .state
                    .compare_exchange_weak(
                        state,
                        state | WRITE_LOCKED,
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
                iterations_spent += 1;
                continue;
            }
            for _ in 0..backoff {
                std::hint::spin_loop();
            }
            iterations_spent += backoff;
            backoff = (backoff * 2).min(WRITE_SPIN_ATTEMPTS);
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
                // Acquire-to-contended: set WRITE_LOCKED *and keep*
                // WRITERS_PARKED. We reach this loop only after the barging
                // spin failed, i.e. this writer is genuinely contending, and
                // other writers may be parked on `write_futex` behind us.
                // `notify_one_writer` wakes exactly ONE, and the release that
                // woke THIS writer cleared the shared WRITERS_PARKED bit
                // (there is only one bit for the whole writer population).
                // If we re-acquired with the bit CLEAR, our own release would
                // see no WRITERS_PARKED and fail to wake the next parked
                // writer -- the wake chain would break and every writer
                // behind us would strand (the v2 bug). Mirroring
                // `raw_mutex`'s LOCKED_CONTENDED discipline, a slow-path
                // acquire conservatively RE-ESTABLISHES the contended marker,
                // so each writer's release hands off to the next. The marker
                // self-heals: once the backlog drains, the lock reaches a
                // genuinely free `0` and the next writer takes the
                // uncontended `lock_exclusive` fast path (CAS `0 ->
                // WRITE_LOCKED`, no marker), after at most one trailing
                // spurious wake.
                match self.state.compare_exchange_weak(
                    state,
                    state | WRITE_LOCKED | WRITERS_PARKED,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // This writer is no longer "waiting to reserve" --
                        // decrement now, not at final acquisition (state
                        // table, failure mode 4's fix: a writer mid-drain
                        // must NOT keep inflating write_waiters).
                        self.write_waiters.fetch_sub(1, Ordering::Relaxed);

                        // We only set bits 30 (WRITE_LOCKED) and 29
                        // (WRITERS_PARKED), so the reader bits in `state` (the
                        // CAS's expected/old value, masked by READERS_MASK)
                        // are exactly the reader count of the new state too --
                        // no reload needed to determine whether a drain is
                        // required.
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
            // on write_futex until it might have cleared. Record "a writer is
            // parked" IN `state` (WRITERS_PARKED) BEFORE parking, so the
            // releasing thread decides whether to issue the targeted
            // write_futex wake from the same snapshot its own clearing RMW
            // returns -- the same-word gate that keeps the uncontended
            // release syscall-free while staying race-free. If the CAS to set
            // the bit fails, `state` changed underneath us (most likely
            // WRITE_LOCKED cleared); loop and re-evaluate from the fresh
            // value, which may now be acquirable.
            if state & WRITERS_PARKED == 0
                && self
                    .state
                    .compare_exchange_weak(
                        state,
                        state | WRITERS_PARKED,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_err()
            {
                continue;
            }

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

            // Park on write_futex expecting `seen_gen` (sampled at the top of
            // this loop iteration, BEFORE the WRITERS_PARKED CAS and the state
            // check). A release that read our just-set WRITERS_PARKED bit
            // calls `notify_one_writer`, which bumps the generation BEFORE
            // waking -- so if it lands between here and the park, the value
            // differs from `seen_gen` and `futex_wait` returns immediately
            // instead of sleeping through the wake.
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

        // SAFETY: this thread acquired the write lock via `try_lock_exclusive`
        // above and holds it, so it may release it.
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

        // SAFETY: this thread acquired a shared lock via `raw.lock_shared()`
        // above and holds it, so it may release one shared hold.
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

        // SAFETY: this thread holds the write lock (`try_lock_exclusive` above;
        // the spawned thread timed out and never acquired it), so it may
        // release it.
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
        // SAFETY: the original `raw.lock_shared()` hold and the `try_lock_shared`
        // hold above are both still held by this thread (the writer timed out
        // and acquired nothing), so it may release both shared holds.
        unsafe { raw.unlock_shared() };
        // SAFETY: as above — the second of this thread's two shared holds.
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

        // SAFETY: this thread holds the write lock (`try_lock_exclusive` above;
        // the spawned reader timed out), so it may release it.
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
            // SAFETY: this closure acquired the write lock via
            // `raw2.lock_exclusive()` on the line above and holds it, so it
            // may release it.
            unsafe { raw2.unlock_exclusive() };
        });

        // Give the writer a chance to park behind the reader.
        std::thread::sleep(Duration::from_millis(30));
        assert!(!raw.is_locked_exclusive(), "reader still holds the lock");

        // SAFETY: this thread holds the shared lock it took via
        // `raw.lock_shared()` above; releasing it lets the parked writer
        // acquire.
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
        // SAFETY: `try_lock_exclusive_until` returned `true`, so this thread
        // holds the write lock and may release it.
        unsafe { raw.unlock_exclusive() };
    }

    // ---------------------------------------------------------------------
    // Wake-gate bit edge cases (READERS_PARKED bit 31, WRITERS_PARKED bit 29)
    // ---------------------------------------------------------------------

    /// `is_locked()` must mask out the wake-gate bits: a word with ONLY a
    /// gate bit set (the transient that appears for an instant in
    /// `unlock_shared`'s read_held(1)->free path, between the count
    /// decrement and clearing the gate bit) is NOT "locked".
    #[test]
    fn is_locked_ignores_lone_gate_bits() {
        let raw = NoxuRawRwLock::INIT;
        // Simulate the transient: READERS_PARKED alone, nothing held.
        raw.state.store(READERS_PARKED, Ordering::Relaxed);
        assert!(!raw.is_locked(), "a lone READERS_PARKED bit is not 'locked'");
        assert!(!raw.is_locked_exclusive());
        assert_eq!(
            raw.reader_count(),
            0,
            "gate bit must not count as a reader"
        );
        // WRITERS_PARKED alone, likewise.
        raw.state.store(WRITERS_PARKED, Ordering::Relaxed);
        assert!(!raw.is_locked(), "a lone WRITERS_PARKED bit is not 'locked'");
        assert_eq!(raw.reader_count(), 0);
        // Clean up so Drop/debug asserts see a sane word.
        raw.state.store(0, Ordering::Relaxed);
    }

    /// A writer must acquire a lock that is logically FREE even if a gate bit
    /// is set in the word (the slow-path spin CAS preserves the bit rather
    /// than dropping it, and never stalls). Exercises the
    /// `state | WRITE_LOCKED` acquire in `lock_exclusive_slow`.
    #[test]
    fn writer_acquires_free_lock_with_a_gate_bit_set() {
        let raw = NoxuRawRwLock::INIT;
        // Free lock, but READERS_PARKED stuck set (e.g. a timed-out reader
        // left it). A writer must still acquire, and the fast-path CAS(0,..)
        // will miss, so this drives the slow path.
        raw.state.store(READERS_PARKED, Ordering::Relaxed);
        assert!(
            !raw.is_locked(),
            "precondition: logically free despite the stuck gate bit"
        );
        // try_lock_exclusive uses CAS(0, WRITE_LOCKED) which (correctly)
        // fails here; the blocking lock_exclusive drives the slow path.
        assert!(!raw.try_lock_exclusive(), "fast CAS(0,..) misses by design");
        raw.lock_exclusive();
        assert!(raw.is_write_locked(), "slow path must acquire the free lock");
        // SAFETY: this thread acquired the write lock via `lock_exclusive`.
        unsafe { raw.unlock_exclusive() };
        assert!(!raw.is_locked());
    }

    /// End-to-end: a reader that parks (setting READERS_PARKED) behind a
    /// writer is woken when the writer releases, and the lock is clean
    /// afterwards (both gate bits cleared). Repeated to exercise the
    /// park/set-bit/wake/clear cycle many times.
    #[test]
    fn reader_parks_sets_bit_and_is_woken_repeatedly() {
        use std::sync::atomic::AtomicBool;
        for _ in 0..50 {
            let raw = Arc::new(NoxuRawRwLock::INIT);
            raw.lock_exclusive();
            let raw2 = Arc::clone(&raw);
            let got = Arc::new(AtomicBool::new(false));
            let got2 = Arc::clone(&got);
            let reader = std::thread::spawn(move || {
                raw2.lock_shared(); // parks behind the writer, sets the bit
                got2.store(true, Ordering::SeqCst);
                // SAFETY: lock_shared above acquired a shared hold.
                unsafe { raw2.unlock_shared() };
            });
            // Let the reader reach the park and publish READERS_PARKED.
            std::thread::sleep(Duration::from_millis(5));
            assert!(!got.load(Ordering::SeqCst), "reader must still be parked");
            // SAFETY: this thread holds the write lock; releasing wakes the
            // parked reader via the READERS_PARKED gate.
            unsafe { raw.unlock_exclusive() };
            reader.join().unwrap();
            assert!(got.load(Ordering::SeqCst), "reader must have been woken");
            // After everyone leaves, the word is fully clean (no stuck bit).
            assert_eq!(
                raw.state.load(Ordering::Relaxed),
                0,
                "both gate bits and WRITE_LOCKED must be clear after release"
            );
        }
    }

    /// A reader that TIMES OUT after setting READERS_PARKED leaves the lock
    /// usable: the bit may linger (a later release does a harmless spurious
    /// wake), but there is no lost wake and a fresh acquirer still works.
    #[test]
    fn reader_timeout_after_setting_parked_bit_leaves_lock_usable() {
        let raw = Arc::new(NoxuRawRwLock::INIT);
        assert!(raw.try_lock_exclusive());
        let raw2 = Arc::clone(&raw);
        // Reader times out while parked behind the writer (sets the bit, then
        // gives up).
        let timed_out = std::thread::spawn(move || {
            !raw2.try_lock_shared_for(Duration::from_millis(30))
        })
        .join()
        .unwrap();
        assert!(timed_out, "reader must time out while the writer holds");
        assert_eq!(
            raw.get_n_waiters(),
            0,
            "advisory waiter count must be back to zero after the timeout"
        );
        // Release the writer. If READERS_PARKED lingered, this does a
        // spurious wake of an empty queue -- harmless.
        // SAFETY: this thread holds the write lock from try_lock_exclusive.
        unsafe { raw.unlock_exclusive() };
        // The lock must be fully usable: a fresh writer and reader both work.
        assert!(raw.try_lock_exclusive());
        // SAFETY: just acquired above.
        unsafe { raw.unlock_exclusive() };
        assert!(raw.try_lock_shared());
        // SAFETY: just acquired a shared hold above.
        unsafe { raw.unlock_shared() };
        assert!(!raw.is_locked());
    }

    /// MANY writers contending on one lock must ALL eventually acquire and
    /// release -- the writer wake-CHAIN. The `WRITERS_PARKED` wake-gate is a
    /// single shared bit; a release that wakes ONE writer clears it, so the
    /// woken writer must RE-ESTABLISH the bit on its slow-path acquire
    /// (acquire-to-contended) or every writer queued behind it strands with
    /// no wake coming. Without that re-establishment this test hangs; the
    /// watchdog turns the hang into a fast, explicit failure instead of a CI
    /// timeout. (Regression test for the v2 chain-break: 19 writers parked
    /// forever in `lock_exclusive_slow` on a free lock.)
    #[test]
    fn many_writers_all_acquire_no_chain_break() {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        let raw = Arc::new(NoxuRawRwLock::INIT);
        let n_writers = 16usize;
        let per = 200u64;
        let completed = Arc::new(AtomicU64::new(0));
        let target = n_writers as u64 * per;

        // Watchdog: if the completed count stops advancing for 10s while the
        // lock is observed free, writers are stranded (chain break).
        let done = Arc::new(AtomicBool::new(false));
        let wd = {
            let completed = Arc::clone(&completed);
            let done = Arc::clone(&done);
            let raw = Arc::clone(&raw);
            std::thread::spawn(move || {
                let mut last = 0u64;
                let mut stalls = 0;
                while !done.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(500));
                    let now = completed.load(Ordering::Relaxed);
                    if now == last && now < target {
                        stalls += 1;
                        if stalls >= 20 {
                            // 10s with no progress.
                            let st = raw.state.load(Ordering::Relaxed);
                            panic!(
                                "WRITER CHAIN BREAK: writers stranded at \
                                 {now}/{target}, state={st:#x} \
                                 (write_locked={}, readers={})",
                                st & WRITE_LOCKED != 0,
                                st & READERS_MASK
                            );
                        }
                    } else {
                        stalls = 0;
                        last = now;
                    }
                }
            })
        };

        let mut hs = Vec::new();
        for _ in 0..n_writers {
            let raw = Arc::clone(&raw);
            let completed = Arc::clone(&completed);
            hs.push(std::thread::spawn(move || {
                for _ in 0..per {
                    raw.lock_exclusive();
                    // Hold briefly so contending writers exhaust their spin
                    // budget and genuinely futex-PARK (the chain-break only
                    // strands futex-parked writers; a pure spin never reaches
                    // the park path). ~a few microseconds is enough.
                    for _ in 0..2000 {
                        std::hint::spin_loop();
                    }
                    // SAFETY: just acquired the write lock above.
                    unsafe { raw.unlock_exclusive() };
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        done.store(true, Ordering::Relaxed);
        wd.join().unwrap();
        assert_eq!(completed.load(Ordering::Relaxed), target);
        assert!(!raw.is_locked(), "lock must be free after all writers leave");
    }

    /// Mixed readers + writers contending: every thread must make progress
    /// (exercises both the READERS_PARKED and WRITERS_PARKED wake chains and
    /// their interaction) with a watchdog guarding against any lost wake.
    #[test]
    fn mixed_readers_writers_all_make_progress() {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        let raw = Arc::new(NoxuRawRwLock::INIT);
        let n = 8usize;
        let per = 300u64;
        let completed = Arc::new(AtomicU64::new(0));
        let target = (2 * n) as u64 * per;
        let done = Arc::new(AtomicBool::new(false));
        let wd = {
            let completed = Arc::clone(&completed);
            let done = Arc::clone(&done);
            let raw = Arc::clone(&raw);
            std::thread::spawn(move || {
                let mut last = 0u64;
                let mut stalls = 0;
                while !done.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(500));
                    let now = completed.load(Ordering::Relaxed);
                    if now == last && now < target {
                        stalls += 1;
                        if stalls >= 20 {
                            let st = raw.state.load(Ordering::Relaxed);
                            panic!(
                                "LOST WAKE: mixed workload stranded at \
                                 {now}/{target}, state={st:#x}"
                            );
                        }
                    } else {
                        stalls = 0;
                        last = now;
                    }
                }
            })
        };
        let mut hs = Vec::new();
        for _ in 0..n {
            let raw_w = Arc::clone(&raw);
            let completed_w = Arc::clone(&completed);
            hs.push(std::thread::spawn(move || {
                for _ in 0..per {
                    raw_w.lock_exclusive();
                    // SAFETY: acquired on the line above.
                    unsafe { raw_w.unlock_exclusive() };
                    completed_w.fetch_add(1, Ordering::Relaxed);
                }
            }));
            let raw_r = Arc::clone(&raw);
            let completed_r = Arc::clone(&completed);
            hs.push(std::thread::spawn(move || {
                for _ in 0..per {
                    raw_r.lock_shared();
                    // SAFETY: acquired a shared hold on the line above.
                    unsafe { raw_r.unlock_shared() };
                    completed_r.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        done.store(true, Ordering::Relaxed);
        wd.join().unwrap();
        assert_eq!(completed.load(Ordering::Relaxed), target);
        assert!(!raw.is_locked());
    }
}
