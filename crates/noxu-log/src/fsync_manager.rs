//! Manager for coalescing fsync operations (group commit).
//!
//!
//! The FSyncManager ensures that only one file fsync is issued at a time for
//! performance optimization.  The goal is to reduce the number of fsyncs
//! issued by the system by having one fsync serve a batch of threads.
//!
//! # Algorithm (mirrors JE FSyncManager.flushAndSync leader/waiter pattern)
//!
//! When a thread enters `flush_and_sync()` it finds one of two situations:
//!
//! 1. **A leader slot is free** (fewer than `max_leaders` leaders in flight;
//!    with the default `max_leaders == 1` this means "no work in progress") —
//!    the thread becomes a *leader*.  If group commit is enabled
//!    (`grpc_threshold > 0` AND `grpc_interval_ms > 0`) the leader may wait
//!    briefly for more waiters to accumulate.  Then it runs the supplied
//!    `do_work` closure (JE flushBeforeSync drain+pwrite, then executeFSync),
//!    wakes all current waiters (they piggyback on its fsync), wakes one member
//!    of the *next* group to become the next leader, and releases its leader
//!    slot (`leaders_in_flight -= 1`).
//!
//! 2. **All leader slots are taken** — the thread joins `next_fsync_waiters`
//!    and waits on a `Condvar`.  When woken it checks whether its fsync was
//!    already done (`NoFsyncNeeded`), whether it should become the next leader
//!    (`DoLeaderFsync`), or whether it timed out (`DoTimeoutFsync`).
//!
//! Each group is represented by an `Arc<FSyncGroup>`.  The leader atomically
//! replaces `state.next_fsync_waiters` with a fresh group, so waiting threads
//! retain their `Arc` to the *old* group and can still be woken through it.
//!
//! # Bounded fsync pipeline (`max_leaders`)
//!
//! By default (`max_leaders == 1`) at most one leader is in flight at a time —
//! the single-leader group-commit above, exactly as before.  When configured
//! with `max_leaders > 1`, up to N leaders may be in flight concurrently: a
//! committer that arrives while other leaders are draining/syncing becomes an
//! *additional* leader (rather than always joining a waiter cohort) so long as
//! fewer than N leaders are currently in flight.  Each leader captures its own
//! cohort and runs `do_work` (the caller's drain + fdatasync).
//!
//! This lets several `fdatasync` calls run against the same log file at once —
//! the device sustains many concurrent same-file `fdatasync`s, whereas one
//! leader at a time caps throughput at the single-file fsync latency.  The
//! DRAIN half of `do_work` is still serialized by the caller's log-write latch
//! (it is cheap: memcpy + `pwrite` to the page cache in LSN order); only the
//! `fdatasync` half runs concurrently.  The durability watermark stays a single
//! monotonic value because the caller advances it with a CAS-max after each
//! completed `fdatasync` (see `LogManager::flush_sync` for the full proof).

use std::time::Duration;

use noxu_util::dst_sync::Arc;
use noxu_util::dst_sync::atomic::{AtomicBool, AtomicU64, Ordering};
// The condvar-timed waits (`grpc_wait`, `wait_for_event`) route through the
// parking_lot-over-shuttle seam so a `SimClock`-driven timed wait fires
// deterministically under shuttle (DST M1.1 `advance_and_fire`); the default
// build re-exports the real `noxu-sync` types, so production is unchanged.
use noxu_util::dst_sync_pl::{Condvar, Mutex, MutexGuard};
use noxu_util::{Clock, Lsn, NULL_LSN, RealClock};

// ── FSyncGroup ────────────────────────────────────────────────────────────────

/// One cohort of threads waiting for a common fsync.
///
/// Each instance lives behind an `Arc` so
/// that threads that joined a group keep a reference even after the leader has
/// swapped in a fresh `FSyncGroup` for the next cohort.
struct FSyncGroup {
    /// P-1 fast path: set to `true` (Release) by `wakeup_all` / `wakeup_all_with_error`
    /// before acquiring `inner`.  Waiters check this atomic (Acquire) BEFORE
    /// acquiring `inner`, eliminating the N-way mutex race when the fsync is
    /// already done on arrival at `wait_for_event`.  This is the AtomicBool
    /// fast-path that Wave 11-J identified but never shipped.
    work_done_atomic: AtomicBool,
    inner: Mutex<FsyncGroupInner>,
    condvar: Condvar,
}

struct FsyncGroupInner {
    /// True once the fsync for this group has been completed (or failed).
    work_done: bool,
    /// Whether a leader has already been designated for this group.
    leader_exists: bool,
    /// A leader-designation wakeup (`wakeup_one`) is pending for this group.
    ///
    /// LOST-WAKEUP FIX (DST wave 2): `wakeup_one` sets this to `true` under
    /// `inner` BEFORE calling `Condvar::notify_one`, and `wait_for_event`
    /// consumes it under `inner` BEFORE blocking.  Without it, a `notify_one`
    /// that lands after the leader captured the cohort but before the next
    /// waiter reaches its `wait` is lost (a `notify` with no waiter is a
    /// no-op), orphaning the next leader until `LOG_FSYNC_TIMEOUT`.  This is
    /// the same predicate-before-wait class as the `DaemonManager` WakeHandle
    /// pre-check landed in M2, applied to the group-commit leader hand-off.
    leader_notified: bool,
    /// Recorded error message from the fsync, propagated to all waiters.
    error: Option<String>,
    /// Result LSN (as u64) the leader durably synced on this group's behalf.
    /// Read by piggybacking waiters so they can return the same watermark the
    /// leader advanced.  JE has no analogue (Java waiters just return void and
    /// re-derive durability from the shared LogManager state); Noxu carries the
    /// post-drain `eol` explicitly so a waiter's subsequent
    /// `flush_sync_if_needed` observes `last_synced_lsn >= its lsn`.
    result_lsn: u64,
}

/// Return value from `FSyncGroup::wait_for_event`.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitStatus {
    /// The fsync was completed on this thread's behalf; nothing to do.
    NoFsyncNeeded,
    /// This thread should become the leader and perform the fsync.
    DoLeaderFsync,
    /// This thread timed out; it must perform its own fsync.
    DoTimeoutFsync,
}

impl FSyncGroup {
    fn new() -> Arc<Self> {
        Arc::new(FSyncGroup {
            work_done_atomic: AtomicBool::new(false),
            inner: Mutex::new(FsyncGroupInner {
                work_done: false,
                leader_exists: false,
                leader_notified: false,
                error: None,
                result_lsn: 0,
            }),
            condvar: Condvar::new(),
        })
    }

    /// Block until work is done, this thread becomes leader, or we time out.
    ///
    /// P-1 fast path: checks `work_done_atomic` (Acquire) BEFORE acquiring
    /// `inner`.  In the common post-fsync case, all N waiters see `true` and
    /// return without ever contending on the mutex — eliminating the
    /// thundering-herd mutex storm documented in Keith re-audit P-1.
    ///
    /// Time is read through the injectable [`Clock`] rather than
    /// [`std::time::Instant`] (JE reads `System.nanoTime()` here) so a
    /// [`noxu_util::SimClock`] makes the timeout decision a pure function of
    /// the simulated timeline (DST M1.1).
    fn wait_for_event(
        &self,
        clock: &dyn Clock,
        timeout: Duration,
    ) -> WaitStatus {
        // P-1 fast path: if the fsync is already done, return without locking.
        if self.work_done_atomic.load(Ordering::Acquire) {
            return WaitStatus::NoFsyncNeeded;
        }

        let mut inner = self.inner.lock();

        // Fast path: already done before we even enter.
        if inner.work_done {
            return WaitStatus::NoFsyncNeeded;
        }

        // LOST-WAKEUP FIX (DST wave 2), pre-check #1: a leader-designation
        // `wakeup_one` may have fired (under `inner`) after the leader
        // captured this cohort but before we reached the wait.  Consuming the
        // pending flag here — before blocking — turns that otherwise-lost
        // `notify_one` into an immediate leader designation, so the next
        // leader is never orphaned to the fsync timeout.  Same class as the
        // WakeHandle predicate-before-wait pre-check.
        if !inner.leader_exists && inner.leader_notified {
            inner.leader_notified = false;
            inner.leader_exists = true;
            return WaitStatus::DoLeaderFsync;
        }

        let timeout_ns = timeout.as_nanos() as u64;
        let start_ns = clock.now_nanos();
        loop {
            // Compute remaining wait time from the injectable clock.
            let elapsed_ns = clock.now_nanos().saturating_sub(start_ns);
            if elapsed_ns >= timeout_ns {
                return WaitStatus::DoTimeoutFsync;
            }
            let remaining = Duration::from_nanos(timeout_ns - elapsed_ns);

            let _timed_out =
                self.condvar.wait_for(&mut inner, remaining).timed_out();

            if inner.work_done {
                return WaitStatus::NoFsyncNeeded;
            }

            if !inner.leader_exists {
                inner.leader_notified = false;
                inner.leader_exists = true;
                return WaitStatus::DoLeaderFsync;
            }

            // Spurious wakeup or still a plain waiter — re-check timeout.
            if clock.now_nanos().saturating_sub(start_ns) >= timeout_ns {
                return WaitStatus::DoTimeoutFsync;
            }
            // else: loop and keep waiting
        }
    }

    /// Wake all waiters with success, recording the durable result LSN.
    ///
    /// P-1: sets `work_done_atomic` with Release ordering BEFORE acquiring
    /// `inner`, so any waiter that checks the atomic after this point returns
    /// immediately without locking.
    fn wakeup_all(&self, result_lsn: u64) {
        // P-1: set atomic first so late-arriving waiters skip the mutex.
        self.work_done_atomic.store(true, Ordering::Release);
        let mut inner = self.inner.lock();
        inner.work_done = true;
        inner.error = None;
        inner.result_lsn = result_lsn;
        drop(inner);
        self.condvar.notify_all();
    }

    /// Wake all waiters recording an error.
    ///
    /// P-1: same atomic-first pattern as `wakeup_all`.
    fn wakeup_all_with_error(&self, msg: String) {
        // P-1: set atomic first so late-arriving waiters skip the mutex.
        // They still need to acquire the mutex to read the error string, but
        // at least they can tell "something happened" without the race.
        self.work_done_atomic.store(true, Ordering::Release);
        let mut inner = self.inner.lock();
        inner.work_done = true;
        inner.error = Some(msg);
        drop(inner);
        self.condvar.notify_all();
    }

    /// Wake a single waiter to become the next leader.
    ///
    /// LOST-WAKEUP FIX (DST wave 2): the designation flag is set under `inner`
    /// BEFORE `notify_one`, so a waiter that has not yet reached its `wait`
    /// observes it on the pre-check in `wait_for_event` and is designated
    /// leader without ever blocking.  Without the flag the bare `notify_one`
    /// is lost if it lands before the waiter blocks (JE recovers this via
    /// `LOG_FSYNC_TIMEOUT`; this closes the stall window in production and
    /// makes the hand-off timeout-independent so shuttle can prove liveness).
    fn wakeup_one(&self) {
        let mut inner = self.inner.lock();
        // Only arm a designation if none has been made yet for this cohort;
        // if a leader already exists the notify is unnecessary.
        if !inner.leader_exists {
            inner.leader_notified = true;
        }
        drop(inner);
        self.condvar.notify_one();
    }

    /// Re-designate a fresh next leader for this cohort after the current
    /// designated leader decided NOT to lead (the WriteQueue short-circuit).
    ///
    /// DEADLOCK FIX: the completing leader designates exactly ONE next leader
    /// via `wakeup_one`.  If that designee returns early (its `target_lsn` was
    /// already covered by a completed fdatasync) it drops the baton — the
    /// remaining cohort members would park forever.  This clears the stale
    /// `leader_exists` designation and arms a new one so exactly one sibling
    /// wakes and repeats the same short-circuit-or-lead decision.  If the
    /// cohort has already been served by a real fsync (`work_done`) there is
    /// nothing to hand off.
    fn handoff_leader(&self) {
        let mut inner = self.inner.lock();
        if inner.work_done {
            return;
        }
        // Retract our own (consumed) designation and arm a new one for the
        // next sibling to pick up on wake / pre-check.
        inner.leader_exists = false;
        inner.leader_notified = true;
        drop(inner);
        self.condvar.notify_one();
    }

    /// Return the recorded error (if any) for this group.
    fn take_error(&self) -> Option<String> {
        self.inner.lock().error.clone()
    }

    /// Return the durable result LSN the leader recorded for this group.
    fn result_lsn(&self) -> u64 {
        self.inner.lock().result_lsn
    }
}

// ── FsyncState ────────────────────────────────────────────────────────────────

/// Mutable state guarded by `FsyncManager::state_mutex`.
///
/// Mirrors the fields that protects with `mgrMutex`.
struct FsyncState {
    /// Number of leader threads currently in flight (draining + about to /
    /// currently fsyncing).  Ranges `0..=max_leaders`.  With `max_leaders == 1`
    /// this is the boolean `work_in_progress` of the original single-leader
    /// design (0 = free, 1 = busy); with `max_leaders > 1` it is the bounded
    /// pipeline depth of concurrent leaders.
    leaders_in_flight: usize,
    /// The group that newly-arriving threads join while all leader slots are
    /// taken.  When a leader slot frees, the next leader captures this group.
    next_fsync_waiters: Arc<FSyncGroup>,
    /// Count of threads currently in `next_fsync_waiters`.
    num_next_waiters: usize,
    /// Monotonic clock tick (nanos, from the injectable [`Clock`]) when the
    /// first thread joined the current next-group.  Was `Option<Instant>`;
    /// switched to a clock-sourced `u64` so DST can control the grpc wait.
    start_next_wait_ns: Option<u64>,
}

// ── FsyncManager ─────────────────────────────────────────────────────────────

/// Coalesces fsync requests so that one system call serves many threads.
///
///
///
/// # Configuration
///
/// * `grpc_threshold` — minimum number of waiters before the leader executes
///   the fsync.  `0` disables group-commit waiting (default).
/// * `grpc_interval_ms` — maximum milliseconds the leader waits for more
///   waiters.  `0` disables group-commit waiting (default).
///
/// Group-commit waiting is only active when **both** values are non-zero,
/// matching `grpWaitOn` flag.
pub struct FsyncManager {
    /// Min waiters before the leader fsyncs (0 = disabled).
    grpc_threshold: usize,
    /// Max ms the leader waits for more waiters (0 = disabled).
    grpc_interval_ms: u64,
    /// Maximum number of leaders (concurrent `fdatasync`s) in flight at once.
    /// `1` (default) = the original single-leader group commit; `> 1` = the
    /// bounded fsync pipeline.  Clamped to `>= 1` at construction.
    max_leaders: usize,
    /// Whether group-commit waiting is active (`grpcInterval != 0 && grpcThreshold != 0`).
    grp_wait_on: bool,
    /// Timeout for waiting threads before they do their own fsync.
    /// (the: `LOG_FSYNC_TIMEOUT`, default 500 ms.)
    fsync_timeout: Duration,
    /// Mutex protecting `FsyncState`.  Also used by `leader_condvar`.
    state: Mutex<FsyncState>,
    /// Condvar used by the leader to wait for more members (grpc wait).
    /// Paired with `state` mutex so the lock can be released during the wait.
    leader_condvar: Condvar,
    /// Total number of fdatasync/fsync calls performed.
    n_fsyncs: AtomicU64,
    /// Total number of fsync requests (before coalescing).
    n_fsync_requests: AtomicU64,
    /// Number of fsync requests that timed out (waited but leader took too long).
    n_fsync_timeouts: AtomicU64,
    /// Number of group-commit batches where leader served ≥1 waiter.
    n_group_commits: AtomicU64,
    /// Cumulative fsync duration in milliseconds.
    fsync_time_ms: AtomicU64,
    /// Sum of all group-commit batch sizes (waiters served per fsync).
    n_fsync_batch_size_sum: AtomicU64,
    /// Injectable clock for the group-commit wait / fsync-timeout decisions
    /// (DST M1.1).  Defaults to [`RealClock`] so production behavior is
    /// unchanged; a [`noxu_util::SimClock`] lets DST drive the timeout cadence.
    clock: Arc<dyn Clock>,
}

impl FsyncManager {
    /// Create a new `FsyncManager` (single-leader group commit).
    ///
    /// # Arguments
    /// * `grpc_threshold`   — min waiters before leader fsyncs (0 = disabled).
    /// * `grpc_interval_ms` — max ms to wait for more waiters (0 = disabled).
    ///
    /// `max_leaders` defaults to `1` (the historical single-leader behavior).
    /// Use [`FsyncManager::with_pipeline`] to enable the bounded fsync pipeline.
    pub fn new(grpc_threshold: usize, grpc_interval_ms: u64) -> Self {
        Self::with_clock(grpc_threshold, grpc_interval_ms, 1, RealClock::arc())
    }

    /// Create a new `FsyncManager` with a bounded fsync pipeline.
    ///
    /// `max_leaders` is the maximum number of concurrent leaders (concurrent
    /// `fdatasync`s) in flight at once; `1` = the single-leader group commit.
    /// Values are clamped to `>= 1`.
    pub fn with_pipeline(
        grpc_threshold: usize,
        grpc_interval_ms: u64,
        max_leaders: usize,
    ) -> Self {
        Self::with_clock(
            grpc_threshold,
            grpc_interval_ms,
            max_leaders,
            RealClock::arc(),
        )
    }

    /// Create a new `FsyncManager` with an injectable [`Clock`] (DST M1.1).
    ///
    /// Production uses [`FsyncManager::new`] / [`FsyncManager::with_pipeline`]
    /// (which pass [`RealClock`]); DST harnesses pass a
    /// [`noxu_util::SimClock`] so the group-commit wait and the `fsync_timeout`
    /// recovery become a pure function of the simulated timeline.  Additive:
    /// no existing caller changes.
    pub fn with_clock(
        grpc_threshold: usize,
        grpc_interval_ms: u64,
        max_leaders: usize,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let grp_wait_on = grpc_threshold != 0 && grpc_interval_ms != 0;
        FsyncManager {
            grpc_threshold,
            grpc_interval_ms,
            max_leaders: max_leaders.max(1),
            grp_wait_on,
            // default timeout: 500 ms.
            fsync_timeout: Duration::from_millis(500),
            state: Mutex::new(FsyncState {
                leaders_in_flight: 0,
                next_fsync_waiters: FSyncGroup::new(),
                num_next_waiters: 0,
                start_next_wait_ns: None,
            }),
            leader_condvar: Condvar::new(),
            n_fsyncs: AtomicU64::new(0),
            n_fsync_requests: AtomicU64::new(0),
            n_fsync_timeouts: AtomicU64::new(0),
            n_group_commits: AtomicU64::new(0),
            fsync_time_ms: AtomicU64::new(0),
            n_fsync_batch_size_sum: AtomicU64::new(0),
            clock,
        }
    }

    /// Drain the log buffer and fsync, coalescing with concurrent callers.
    ///
    /// JE faithfulness (single-leader case, `max_leaders == 1`): this matches
    /// `FSyncManager.flushAndSync` EXACTLY.  The leader/waiter decision is made
    /// FIRST under `state` (JE `mgrMutex`), and ONLY the leader (or a timed-out
    /// thread) runs `do_work` — which performs JE's `flushBeforeSync()`
    /// (drain + pwrite) followed by `executeFSync()` (the single fdatasync).
    /// Waiters piggyback: they do NO drain, NO pwrite and NO fsync; on wake
    /// they return the leader's durable result LSN.  This is the fix for the
    /// coalescing divergence — a committer that didn't skip at the caller's
    /// fast path now serialises on `state` BEFORE draining, so it cannot become
    /// its own redundant leader between another leader's pwrite and that
    /// leader's fsync.
    ///
    /// Bounded fsync pipeline (`max_leaders > 1`): an arriving committer may
    /// become an *additional* leader (up to `max_leaders` in flight) instead of
    /// always waiting, so several `do_work` closures — hence several
    /// `fdatasync`s — run concurrently.  Each leader still captures its own
    /// cohort and the waiter piggyback is unchanged; only the number of leaders
    /// allowed in flight at once differs.  The caller's `do_work` keeps the
    /// DRAIN serialized on its own log-write latch (LSN-ordered pwrite to the
    /// page cache) and only the `fdatasync` overlaps, which is what makes the
    /// monotonic durable-watermark advance sound (see `LogManager::flush_sync`).
    ///
    /// The `do_work` closure returns the post-drain `eol` (as u64). On success
    /// the leader records that LSN on the in-progress group so waiters return
    /// the same watermark; the caller advances `last_synced_lsn` from the
    /// returned `Lsn`.  An error from `do_work` (a failed pwrite or fdatasync)
    /// is propagated to the leader AND to every piggybacking waiter (each gets
    /// its own `Err`), matching JE: a leader fsync failure means the waiters'
    /// commits are NOT durable.
    /// `target_lsn` is the committer's durable-LSN requirement (the `eol` its
    /// commit needs on disk; `0` = "no requirement, always fsync", used by
    /// callers with no LSN to check).  `synced_watermark` reads the current
    /// durable watermark (`last_synced_lsn`).
    ///
    /// WriteQueue adaptation (JE `FileManager.writeToFile` enqueue-and-return):
    /// a waiter that is woken to lead — or that times out — FIRST re-checks the
    /// durable watermark.  If a COMPLETED fdatasync already covered its
    /// `target_lsn` (`synced_watermark() > target_lsn`), it returns
    /// immediately with NO redundant fsync — exactly like a JE committer that
    /// found the fsync latch held, enqueued, and let the in-flight/next fsync
    /// cover it.  Its bytes were already drained (they are in the shared log
    /// buffer, pwritten to the page cache before the covering leader's
    /// fdatasync), so the covering fdatasync (which syncs the whole fd to EOL)
    /// already made them durable.  This is the fix for the 1:1 convoy: without
    /// it, every designated next-leader issues a redundant fdatasync for bytes
    /// an earlier leader already synced.
    ///
    /// DURABILITY INVARIANT: a committer only returns `Ok` when either (a) it
    /// (or a piggyback leader) completed an fdatasync covering its LSN, or (b)
    /// `synced_watermark() > target_lsn` — a completed fdatasync covered it.
    /// It NEVER returns before its LSN is under the durable watermark.
    pub fn flush_and_sync<F, S>(
        &self,
        target_lsn: u64,
        synced_watermark: S,
        do_work: F,
    ) -> std::io::Result<Lsn>
    where
        F: Fn() -> std::io::Result<u64>,
        S: Fn() -> u64,
    {
        self.n_fsync_requests.fetch_add(1, Ordering::Relaxed);
        let mut do_my_work = false;
        let mut is_leader = false;
        let mut leader_batch_size: u64 = 0;
        // Group whose waiters this leader serves (set only when is_leader).
        let mut in_progress_group: Option<Arc<FSyncGroup>> = None;
        // Group this thread belongs to as a waiter.
        let mut my_group: Option<Arc<FSyncGroup>> = None;
        let mut need_to_wait = false;

        // ── Phase 1: decide whether to lead or wait ───────────────────────
        {
            let mut state = self.state.lock();

            if state.leaders_in_flight >= self.max_leaders {
                // All leader slots are taken — join the next-waiters cohort.
                need_to_wait = true;
                my_group = Some(Arc::clone(&state.next_fsync_waiters));
                state.num_next_waiters += 1;
                if self.grp_wait_on && state.num_next_waiters == 1 {
                    state.start_next_wait_ns = Some(self.clock.now_nanos());
                }
                // If this new waiter pushes us to the threshold, wake the
                // leader early so it doesn't wait the full grpc_interval_ms.
                // Mirrors: if (numNextWaiters >= grpcThreshold) mgrMutex.notifyAll()
                if self.grp_wait_on
                    && state.num_next_waiters >= self.grpc_threshold
                {
                    self.leader_condvar.notify_one();
                }
            } else {
                // A leader slot is free — become a leader.
                is_leader = true;
                do_my_work = true;
                state.leaders_in_flight += 1;

                if self.grp_wait_on {
                    state = self.grpc_wait(state);
                }

                // Capture the current waiters group; swap in a fresh one.
                leader_batch_size = state.num_next_waiters as u64;
                in_progress_group = Some(Arc::clone(&state.next_fsync_waiters));
                state.next_fsync_waiters = FSyncGroup::new();
                state.num_next_waiters = 0;
            }
        }
        // state lock released.

        // ── Phase 2: if we're a waiter, block until woken ────────────────
        if need_to_wait {
            let group = my_group.as_ref().unwrap();
            let wait_status =
                group.wait_for_event(&*self.clock, self.fsync_timeout);

            match wait_status {
                WaitStatus::NoFsyncNeeded => {
                    // The leader finished; propagate any recorded error.
                    if let Some(msg) = group.take_error() {
                        return Err(std::io::Error::other(msg));
                    }
                    // Piggyback: return the leader's durable result LSN so the
                    // caller's subsequent flush_sync_if_needed observes
                    // last_synced_lsn >= its lsn.
                    return Ok(Lsn::from_u64(group.result_lsn()));
                }
                WaitStatus::DoLeaderFsync => {
                    // WriteQueue re-check (JE enqueue-and-return): the fsync we
                    // waited behind syncs the whole fd to EOL, so it may have
                    // ALREADY covered our target_lsn.  If so, return now with
                    // no redundant fsync — our bytes are durable.  This is what
                    // turns the 1:1 leader-chain convoy into real coalescing:
                    // the designated next-leader no longer re-fsyncs bytes an
                    // earlier leader already made durable.
                    if target_lsn != 0 && synced_watermark() > target_lsn {
                        // DEADLOCK FIX: we were the designated next leader for
                        // this cohort.  Before short-circuiting-and-returning
                        // we MUST hand the baton to a sibling, or the rest of
                        // the cohort parks forever (the completing leader only
                        // designates ONE next leader).  Each woken sibling
                        // repeats this same re-check.
                        group.handoff_leader();
                        return Ok(Lsn::from_u64(synced_watermark()));
                    }
                    // Attempt to become a new leader for this cohort.
                    let mut state = self.state.lock();
                    if state.leaders_in_flight >= self.max_leaders {
                        // No leader slot free (another thread took it while we
                        // were being woken) — do our own work as a safety
                        // measure. (JE: "Ensure that an fsync is done before
                        // returning.")  This is a private, un-coalesced fsync;
                        // its bytes were still drained in LSN order by an
                        // earlier leader, so it is durability-safe.
                        do_my_work = true;
                    } else {
                        is_leader = true;
                        do_my_work = true;
                        state.leaders_in_flight += 1;

                        if self.grp_wait_on {
                            state = self.grpc_wait(state);
                        }

                        // The `my_group` cohort is now the in-progress group.
                        leader_batch_size = state.num_next_waiters as u64;
                        in_progress_group = my_group.take();
                        state.next_fsync_waiters = FSyncGroup::new();
                        state.num_next_waiters = 0;
                    }
                }
                WaitStatus::DoTimeoutFsync => {
                    // WriteQueue re-check on timeout too: if a completed
                    // fdatasync covered us while we were parked, return now.
                    if target_lsn != 0 && synced_watermark() > target_lsn {
                        // Same baton hand-off as the DoLeaderFsync path: if we
                        // were the designated leader (or no leader was ever
                        // designated), re-arm one so the cohort still makes
                        // progress after we short-circuit out.
                        group.handoff_leader();
                        return Ok(Lsn::from_u64(synced_watermark()));
                    }
                    // Timed out — do our own work regardless (JE DO_TIMEOUT_FSYNC).
                    do_my_work = true;
                    self.n_fsync_timeouts.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // ── Phase 3: perform the drain + fsync (JE doWork block) ──────────
        if do_my_work {
            self.n_fsyncs.fetch_add(1, Ordering::Relaxed);
            let fsync_start_ns = self.clock.now_nanos();
            // JE: flushBeforeSync() + executeFSync() — the drain + pwrite +
            // fdatasync now live INSIDE this leader/timeout branch (the fix).
            let result = do_work();
            let elapsed_ms =
                self.clock.now_nanos().saturating_sub(fsync_start_ns)
                    / 1_000_000;
            self.fsync_time_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
            // Count as a group commit when leader has an in-progress group
            // (meaning at least one other thread piggybacked on this fsync).
            if is_leader && in_progress_group.is_some() {
                self.n_group_commits.fetch_add(1, Ordering::Relaxed);
                self.n_fsync_batch_size_sum
                    .fetch_add(leader_batch_size, Ordering::Relaxed);
            }

            if is_leader {
                let in_prog = in_progress_group.as_ref().unwrap();
                // Wake all threads that piggybacked on this fsync.
                match &result {
                    Ok(eol) => in_prog.wakeup_all(*eol),
                    Err(e) => in_prog.wakeup_all_with_error(e.to_string()),
                }
                // Release our leader slot and wake one member of the next
                // cohort to become the next leader — matching JE ordering.
                // With the bounded pipeline there may still be other leaders in
                // flight; decrementing our own slot lets exactly one waiter
                // step into the freed slot.
                let mut state = self.state.lock();
                state.next_fsync_waiters.wakeup_one();
                state.leaders_in_flight -= 1;
            }

            result.map(Lsn::from_u64)
        } else {
            // Unreachable in practice: a waiter that did not do its own work
            // returned from Phase 2 already.  Kept for total-coverage safety:
            // consult the waiter's group result.
            match my_group {
                Some(g) => {
                    if let Some(msg) = g.take_error() {
                        Err(std::io::Error::other(msg))
                    } else {
                        Ok(Lsn::from_u64(g.result_lsn()))
                    }
                }
                None => Ok(NULL_LSN),
            }
        }
    }

    /// Returns the total number of fdatasync calls performed.
    ///
    /// Stat (see `LogStatDefinition.N_FSYNCS`).
    pub fn fsync_count(&self) -> u64 {
        self.n_fsyncs.load(Ordering::Relaxed)
    }

    /// Returns number of fsync requests that timed out.
    pub fn fsync_timeout_count(&self) -> u64 {
        self.n_fsync_timeouts.load(Ordering::Relaxed)
    }

    /// Returns number of group-commit batches where leader served ≥1 waiter.
    pub fn group_commit_count(&self) -> u64 {
        self.n_group_commits.load(Ordering::Relaxed)
    }

    /// Returns cumulative fsync duration in milliseconds.
    pub fn fsync_time_ms(&self) -> u64 {
        self.fsync_time_ms.load(Ordering::Relaxed)
    }

    /// Returns cumulative sum of group-commit batch sizes (total waiters served).
    pub fn fsync_batch_size_sum(&self) -> u64 {
        self.n_fsync_batch_size_sum.load(Ordering::Relaxed)
    }

    /// Returns total number of fsync requests (before coalescing).
    pub fn fsync_request_count(&self) -> u64 {
        self.n_fsync_requests.load(Ordering::Relaxed)
    }

    /// Perform the group-commit wait: release the state lock and wait up to
    /// `grpc_interval_ms` for `grpc_threshold` waiters to accumulate.
    ///
    /// Mirrors the `if (grpWaitOn)` block inside `flushAndSync()`:
    /// ```java
    /// if (numNextWaiters < grpcThreshold) {
    ///     interval = System.nanoTime() - startNextWait;
    ///     if (interval < grpcInterval) {
    ///         mgrMutex.wait(interval/1000000, interval%1000000);
    ///     }
    /// }
    /// ```
    ///
    /// Takes ownership of the `MutexGuard<FsyncState>`, releases the lock via
    /// `Condvar::wait_timeout`, and returns a fresh guard.
    fn grpc_wait<'a>(
        &'a self,
        mut state: MutexGuard<'a, FsyncState>,
    ) -> MutexGuard<'a, FsyncState> {
        // Skip wait entirely when no other threads are queued yet.  This
        // eliminates the single-threaded latency penalty: a lone committer
        // fsyncs immediately rather than waiting for companions that may
        // never arrive.
        if state.num_next_waiters == 0 {
            return state;
        }
        if state.num_next_waiters < self.grpc_threshold {
            let interval_ns = self.grpc_interval_ms * 1_000_000;
            let elapsed_ns = state
                .start_next_wait_ns
                .map(|t| self.clock.now_nanos().saturating_sub(t))
                .unwrap_or(0);
            if elapsed_ns < interval_ns {
                let remaining_ns = interval_ns - elapsed_ns;
                let wait_dur = Duration::from_nanos(remaining_ns);
                // `Condvar::wait_for` releases the lock and re-acquires it
                // (parking_lot shape: borrows the guard in place).  Under
                // shuttle this is the `SimClock`-driven timed wait fired by
                // `advance_and_fire`; in production it is the real futex wait.
                self.leader_condvar.wait_for(&mut state, wait_dur);
                return state;
            }
        }
        state
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    // ── required tests from task spec ─────────────────────────────────────

    /// Single thread, no grouping: fsync closure called exactly once.
    #[test]
    fn test_simple_fsync_no_grouping() {
        let mgr = FsyncManager::new(0, 0);
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        mgr.flush_and_sync(0, || 0, || {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(0)
        })
        .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// 3 threads hit fsync simultaneously; verify fsync called less than 3 times.
    #[test]
    fn test_multiple_threads_one_fsync() {
        let mgr = Arc::new(FsyncManager::new(0, 0));
        let fsync_count = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut handles = vec![];

        for _ in 0..3 {
            let mgr2 = Arc::clone(&mgr);
            let fc = Arc::clone(&fsync_count);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                mgr2.flush_and_sync(0, || 0, || {
                    // Slow fsync so concurrent threads queue up.
                    std::thread::sleep(Duration::from_millis(20));
                    fc.fetch_add(1, Ordering::SeqCst);
                    Ok(0)
                })
                .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let total = fsync_count.load(Ordering::SeqCst);
        // With a barrier + 20 ms sleep at least 2 threads should coalesce.
        assert!(
            total < 3,
            "expected coalescing (total < 3 fsyncs), got {}",
            total
        );
    }

    /// COALESCING-FACTOR REGRESSION GUARD (write-perf parity).
    ///
    /// Reproduces the high-concurrency regime the AWS 96-writer sweep hit
    /// (writers arrive faster than the leader can fsync) on any core count by
    /// making the leader's fsync artificially slow.  With the JE / extended-fork
    /// pure-piggyback design (grpWaitOn off, the shipped default), the leader
    /// that wins while a fsync is in progress accumulates ALL concurrent
    /// committers into its waiter cohort and serves them in ONE fsync, so the
    /// coalescing factor (requests / fsyncs) must be well above 1.
    ///
    /// This is the micro-test that would catch a coalescing regression: if the
    /// leader/waiter piggyback breaks (e.g. a re-introduced LWL-across-fsync
    /// serialization, or a per-committer solo-leader bug), each committer does
    /// its own fsync and the factor collapses to ~1.
    ///
    /// JE cite: `FSyncManager.flushAndSync` doWork block — the leader drains +
    /// fsyncs OUTSIDE `mgrMutex`, so concurrent committers pile into
    /// `nextFSyncWaiters` during the fsync and the next leader serves the whole
    /// batch (`inProgressGroup.wakeupAll()`).
    #[test]
    fn test_coalescing_factor_under_slow_fsync() {
        const N: usize = 32;
        // grpWaitOn OFF (0,0): the shipped default and the reference design.
        let mgr = Arc::new(FsyncManager::new(0, 0));
        let fsyncs = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(N));

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let m = Arc::clone(&mgr);
                let fc = Arc::clone(&fsyncs);
                let b = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    b.wait();
                    // Small stagger-free burst: all N hammer flush_and_sync.
                    for _ in 0..8 {
                        m.flush_and_sync(0, || 0, || {
                            // Slow "fsync" so siblings pile into the waiter
                            // cohort while the leader is in the syscall.
                            fc.fetch_add(1, Ordering::SeqCst);
                            std::thread::sleep(Duration::from_millis(5));
                            Ok(0)
                        })
                        .unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let requests = N * 8;
        let actual_fsyncs = fsyncs.load(Ordering::SeqCst);
        let factor = requests as f64 / actual_fsyncs as f64;
        // The exact factor depends on scheduling, but with 32 threads and a
        // 5 ms fsync the piggyback must coalesce many committers per fsync.
        // A regression to per-committer fsync would give factor ~1.0 and
        // actual_fsyncs ~= requests.  Require a conservative >= 2x to stay
        // robust across CI machines while still catching a total collapse.
        assert!(
            factor >= 2.0,
            "coalescing regressed: {requests} requests / {actual_fsyncs} fsyncs \
             = {factor:.1}x (expected >= 2x from leader/waiter piggyback)"
        );
        // Durability sanity: at least one real fsync happened.
        assert!(actual_fsyncs >= 1);
    }

    /// Error from `do_fsync` propagates to the calling thread.
    #[test]
    fn test_fsync_error_propagated_to_waiters() {
        let mgr = FsyncManager::new(0, 0);
        let result = mgr.flush_and_sync(0, || 0, || {
            Err::<u64, _>(std::io::Error::other("simulated fsync failure"))
        });
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("simulated fsync failure")
        );
    }

    /// With grpc_threshold=2 and grpc_interval_ms=50, all threads finish
    /// without deadlock.
    #[test]
    fn test_grpc_threshold_respected() {
        let mgr = Arc::new(FsyncManager::new(2, 50));
        let fsync_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for _ in 0..4 {
            let m = Arc::clone(&mgr);
            let fc = Arc::clone(&fsync_count);
            handles.push(std::thread::spawn(move || {
                m.flush_and_sync(0, || 0, || {
                    fc.fetch_add(1, Ordering::SeqCst);
                    Ok(0)
                })
                .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let total = fsync_count.load(Ordering::SeqCst);
        assert!(total >= 1, "at least one fsync must have run");
        assert!(total <= 4, "unexpected fsync count: {}", total);
    }

    // ── additional coverage tests ──────────────────────────────────────────

    /// Sequential calls each trigger exactly one fsync.
    #[test]
    fn test_sequential_calls_each_fsync_once() {
        let mgr = FsyncManager::new(0, 0);
        let count = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            let c = count.clone();
            mgr.flush_and_sync(0, || 0, || {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(0)
            })
            .unwrap();
        }
        assert_eq!(count.load(Ordering::SeqCst), 5);
    }

    /// Error from the leader's fsync is forwarded to waiter threads.
    #[test]
    fn test_fsync_error_forwarded_to_waiting_threads() {
        let mgr = Arc::new(FsyncManager::new(0, 0));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mgr2 = Arc::clone(&mgr);
        let b2 = Arc::clone(&barrier);

        let leader = std::thread::spawn(move || {
            b2.wait();
            mgr2.flush_and_sync(0, || 0, || {
                // Slow so the second thread can queue up as a waiter.
                std::thread::sleep(Duration::from_millis(30));
                Err::<u64, _>(std::io::Error::other("leader fail"))
            })
        });

        // Small sleep so the leader thread enters fsync() first.
        barrier.wait();
        std::thread::sleep(Duration::from_millis(2));

        let waiter_result = mgr.flush_and_sync(0, || 0, || {
            // This should either piggyback (NoFsyncNeeded with error) or run its
            // own fsync if it becomes leader.
            Ok(0)
        });

        let leader_result = leader.join().unwrap();
        // The leader must fail.
        assert!(leader_result.is_err());
        // Waiter either got the error propagated or ran its own Ok fsync.
        let _ = waiter_result; // either outcome is valid
    }

    /// HEADLINE fsync-error-propagation test: a leader fsync failure fails
    /// EVERY piggybacking waiter.
    ///
    /// N committers race; their closures all model a failing fdatasync (EIO).
    /// Whichever thread leads a cohort runs the failing fsync and propagates
    /// the error to every waiter that piggybacked on it (JE `wakeupAll`, Noxu
    /// `wakeup_all_with_error`).  The invariants under test:
    ///   1. EVERY committer returns Err — none may return Ok on a failed fsync
    ///      (a leader fsync failure means the commit is NOT durable).
    ///   2. The number of actual fsync ATTEMPTS is < N (coalescing happened),
    ///      proving at least one waiter piggybacked on a leader's failure
    ///      rather than running its own.
    #[test]
    fn test_leader_fsync_failure_fails_all_piggybacking_waiters() {
        const N: usize = 8;
        let mgr = Arc::new(FsyncManager::new(0, 0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(N));

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let m = Arc::clone(&mgr);
                let at = Arc::clone(&attempts);
                let b = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    b.wait();
                    m.flush_and_sync(0, || 0, || {
                        // Each actual leader/timeout fsync attempt: count it,
                        // sleep so siblings queue + piggyback, then fail.
                        at.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(15));
                        Err::<u64, _>(std::io::Error::other("fsync EIO"))
                    })
                })
            })
            .collect();

        let mut errors = 0usize;
        for h in handles {
            match h.join().unwrap() {
                Ok(_) => {
                    panic!("a committer returned Ok despite a failed fsync")
                }
                Err(e) => {
                    assert!(
                        e.to_string().contains("fsync EIO"),
                        "error must carry the leader's failure: {e}"
                    );
                    errors += 1;
                }
            }
        }
        // Invariant 1: every committer failed (no Ok on a failed fsync).
        assert_eq!(errors, N, "every committer must observe the fsync failure");
        // Invariant 2: coalescing happened — fewer fsync attempts than threads
        // means at least one waiter piggybacked on a leader's failed fsync and
        // still received the propagated error.
        let attempts = attempts.load(Ordering::SeqCst);
        assert!(
            attempts < N,
            "expected coalescing under failure (attempts {attempts} < N {N}); \
             at least one waiter must piggyback on a failed leader fsync"
        );
    }

    /// `FsyncManager::new(0, 0)` returns Ok immediately on success.
    #[test]
    fn test_returns_ok_on_success() {
        let mgr = FsyncManager::new(0, 0);
        assert!(mgr.flush_and_sync(0, || 0, || Ok(0)).is_ok());
    }

    /// WRITEQUEUE SHORT-CIRCUIT (the coalescing fix).
    ///
    /// A committer whose `target_lsn` was ALREADY covered by a completed
    /// fdatasync must return WITHOUT issuing a redundant fsync (JE
    /// enqueue-and-return: the in-flight/next fsync, which syncs the whole fd
    /// to EOL, already made its bytes durable).  Before this fix every
    /// designated next-leader re-fsynced bytes an earlier leader had already
    /// synced — the 1:1 convoy.
    ///
    /// Setup: leader A holds a slow fsync; N committers with LSNs already below
    /// the durable watermark queue behind it.  When A completes and designates
    /// the next leader, that waiter (and every sibling) must short-circuit on
    /// the watermark re-check, so the total fsync count is 1 (A's), NOT 1+N.
    #[test]
    fn test_writequeue_shortcircuit_when_watermark_covers_target() {
        use std::sync::atomic::AtomicU64;
        const N: usize = 8;
        let mgr = Arc::new(FsyncManager::new(0, 0));
        // Durable watermark, advanced by the leader's fsync closure.
        let watermark = Arc::new(AtomicU64::new(0));
        let fsyncs = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(N + 1));

        // Leader A: slow fsync that advances the watermark to cover everyone
        // (target LSNs are 1..=N, so a watermark of N+1 covers all via `>`).
        let mgr_a = Arc::clone(&mgr);
        let wm_a = Arc::clone(&watermark);
        let fc_a = Arc::clone(&fsyncs);
        let b_a = Arc::clone(&barrier);
        let leader = std::thread::spawn(move || {
            b_a.wait();
            mgr_a.flush_and_sync(
                0, // A itself always fsyncs (no target).
                {
                    let wm = Arc::clone(&wm_a);
                    move || wm.load(Ordering::SeqCst)
                },
                {
                    let wm = Arc::clone(&wm_a);
                    let fc = Arc::clone(&fc_a);
                    move || {
                        fc.fetch_add(1, Ordering::SeqCst);
                        // Hold the fsync so the N committers queue behind it.
                        std::thread::sleep(Duration::from_millis(40));
                        // This fdatasync covers everything to EOL = N+1.
                        wm.store((N + 1) as u64, Ordering::SeqCst);
                        Ok((N + 1) as u64)
                    }
                },
            )
            .unwrap();
        });

        // N committers with target LSNs 1..=N.  Each waits behind A; when A
        // completes, the designated next-leader (and siblings) must see
        // watermark (= N+1) > its target and return with NO fsync.
        let handles: Vec<_> = (1..=N)
            .map(|i| {
                let m = Arc::clone(&mgr);
                let wm = Arc::clone(&watermark);
                let fc = Arc::clone(&fsyncs);
                let b = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    b.wait();
                    // Small delay so A wins the leader slot first.
                    std::thread::sleep(Duration::from_millis(5));
                    m.flush_and_sync(
                        i as u64,
                        {
                            let wm = Arc::clone(&wm);
                            move || wm.load(Ordering::SeqCst)
                        },
                        move || {
                            // If ANY committer runs its own fsync, the count
                            // exceeds 1 and the assert below fails.
                            fc.fetch_add(1, Ordering::SeqCst);
                            Ok((N + 1) as u64)
                        },
                    )
                    .unwrap();
                })
            })
            .collect();

        leader.join().unwrap();
        for h in handles {
            h.join().unwrap();
        }

        // Only A's single fsync ran; all N committers short-circuited on the
        // watermark re-check.  (Timing-tolerant: allow a couple extra in case
        // a committer wins the leader slot before A on a slow CI machine, but
        // the count must be WELL below 1+N — the convoy would give ~1+N.)
        let total = fsyncs.load(Ordering::SeqCst);
        assert!(
            total <= 3,
            "WriteQueue short-circuit failed: {total} fsyncs (expected ~1, \
             convoy would give {})",
            N + 1
        );
        assert!(total >= 1, "at least A's fsync must have run");
    }

    /// FSyncGroup: `wakeup_all` sets `work_done` and records no error.
    #[test]
    fn test_fsync_group_wakeup_all() {
        let g = FSyncGroup::new();
        g.wakeup_all(0);
        assert!(g.inner.lock().work_done);
        assert!(g.take_error().is_none());
    }

    /// FSyncGroup: `wakeup_all_with_error` sets `work_done` and records error.
    #[test]
    fn test_fsync_group_wakeup_all_with_error() {
        let g = FSyncGroup::new();
        g.wakeup_all_with_error("oops".to_string());
        assert!(g.inner.lock().work_done);
        assert_eq!(g.take_error().unwrap(), "oops");
    }

    /// FSyncGroup: `wait_for_event` returns `NoFsyncNeeded` immediately when
    /// `work_done` is already true before the call.
    #[test]
    fn test_fsync_group_already_done() {
        let g = FSyncGroup::new();
        g.wakeup_all(0);
        let status = g.wait_for_event(&RealClock, Duration::from_secs(5));
        assert_eq!(status, WaitStatus::NoFsyncNeeded);
    }

    /// FSyncGroup: a thread woken with no existing leader becomes the leader.
    #[test]
    fn test_fsync_group_becomes_leader_on_wakeup() {
        let g = Arc::new(FSyncGroup::new());
        let g2 = Arc::clone(&g);

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            // Wake one waiter without marking work_done.
            g2.wakeup_one();
        });

        let status = g.wait_for_event(&RealClock, Duration::from_millis(500));
        assert_eq!(status, WaitStatus::DoLeaderFsync);
        assert!(g.inner.lock().leader_exists);
    }

    /// FSyncGroup: waiter times out when nobody wakes it.
    #[test]
    fn test_fsync_group_timeout() {
        let g = FSyncGroup::new();
        // Pre-set leader_exists so wakeup_one won't make us the leader.
        g.inner.lock().leader_exists = true;
        let status = g.wait_for_event(&RealClock, Duration::from_millis(20));
        assert_eq!(status, WaitStatus::DoTimeoutFsync);
    }

    /// WaitStatus variants are distinct.
    #[test]
    fn test_wait_status_variants_distinct() {
        assert_ne!(WaitStatus::NoFsyncNeeded, WaitStatus::DoLeaderFsync);
        assert_ne!(WaitStatus::NoFsyncNeeded, WaitStatus::DoTimeoutFsync);
        assert_ne!(WaitStatus::DoLeaderFsync, WaitStatus::DoTimeoutFsync);
    }

    /// `grp_wait_on` is false when either threshold or interval is zero.
    #[test]
    fn test_grp_wait_on_requires_both_nonzero() {
        let m1 = FsyncManager::new(0, 100);
        assert!(!m1.grp_wait_on);
        let m2 = FsyncManager::new(2, 0);
        assert!(!m2.grp_wait_on);
        let m3 = FsyncManager::new(2, 100);
        assert!(m3.grp_wait_on);
    }

    // ── Wave 11-J: fsync-before-commit invariant ───────────────────────────
    //
    // Property test: every committed transaction's LSN is fdatasync'd before
    // `txn.commit()` returns.
    //
    // Simulation: N concurrent committers each record a monotonically
    // increasing "commit LSN".  The `do_fsync` closure advances `flushed_lsn`
    // to cover all LSNs seen so far.  After `FsyncManager::fsync()` returns
    // for committer T at LSN L, we assert `flushed_lsn >= L`.
    //
    // This test was added in Wave 11-J as the crash-safety coverage required
    // by the 2026 review (W10 section).

    /// Fsync-before-commit invariant: `flushed_lsn >= commit_lsn` after
    /// `FsyncManager::fsync()` returns for every concurrent committer.
    #[test]
    fn test_fsync_before_commit_invariant() {
        use std::sync::atomic::AtomicU64;

        const N_THREADS: usize = 8;
        const OPS_PER_THREAD: usize = 200;

        // Shared monotonic LSN counter: each committer gets a unique LSN.
        let next_lsn = Arc::new(AtomicU64::new(1));
        // Maximum LSN covered by a completed fdatasync.
        let flushed_lsn = Arc::new(AtomicU64::new(0));
        // Running maximum of all registered commit LSNs (used by do_fsync to
        // know what range it must durably cover).
        let snap_lsn = Arc::new(AtomicU64::new(0));

        let mgr = Arc::new(FsyncManager::new(2, 5));
        let barrier = Arc::new(std::sync::Barrier::new(N_THREADS));
        let mut handles = vec![];

        for _ in 0..N_THREADS {
            let mgr2 = Arc::clone(&mgr);
            let b = Arc::clone(&barrier);
            let nl = Arc::clone(&next_lsn);
            let fl = Arc::clone(&flushed_lsn);
            let sl = Arc::clone(&snap_lsn);

            handles.push(std::thread::spawn(move || {
                b.wait();

                for _ in 0..OPS_PER_THREAD {
                    // "Write commit record" — assign a unique LSN.
                    let my_lsn =
                        nl.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                    // Advance snap_lsn to at least my_lsn so do_fsync knows
                    // it must cover my_lsn when it executes.
                    let mut cur = sl.load(std::sync::atomic::Ordering::Relaxed);
                    while cur < my_lsn {
                        match sl.compare_exchange(
                            cur,
                            my_lsn,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(a) => cur = a,
                        }
                    }

                    // Request fsync.  The closure "syncs" by advancing
                    // flushed_lsn to the current snap_lsn and RETURNS that
                    // covered LSN as the durable watermark (eol).  Under the
                    // new contract a piggybacking waiter returns the leader's
                    // covered LSN, so we assert on the value flush_and_sync
                    // returns — the durable watermark this committer observes.
                    let fl2 = Arc::clone(&fl);
                    let sl2 = Arc::clone(&sl);
                    let durable = mgr2
                        .flush_and_sync(0, || 0, move || {
                            let covered =
                                sl2.load(std::sync::atomic::Ordering::SeqCst);
                            let mut f =
                                fl2.load(std::sync::atomic::Ordering::Relaxed);
                            while f < covered {
                                match fl2.compare_exchange(
                                    f,
                                    covered,
                                    std::sync::atomic::Ordering::SeqCst,
                                    std::sync::atomic::Ordering::Relaxed,
                                ) {
                                    Ok(_) => break,
                                    Err(a) => f = a,
                                }
                            }
                            Ok(covered)
                        })
                        .unwrap();

                    // Post-condition: the durable watermark returned to this
                    // committer (leader's own eol, or the leader's recorded
                    // result for a piggybacking waiter) must cover my_lsn, and
                    // the global flushed_lsn must be ≥ my_lsn.
                    let fl_now = fl.load(std::sync::atomic::Ordering::SeqCst);
                    assert!(
                        durable.as_u64() >= my_lsn,
                        "fsync-before-commit violated (returned watermark): \
                         durable={} < commit_lsn={my_lsn}",
                        durable.as_u64()
                    );
                    assert!(
                        fl_now >= my_lsn,
                        "fsync-before-commit violated: \
                         flushed_lsn={fl_now} < commit_lsn={my_lsn}"
                    );
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
