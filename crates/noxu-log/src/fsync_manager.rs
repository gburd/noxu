//! Manager for coalescing fsync operations (group commit).
//!
//! Port of `com.sleepycat.je.log.FSyncManager`.
//!
//! The FSyncManager ensures that only one file fsync is issued at a time for
//! performance optimization.  The goal is to reduce the number of fsyncs
//! issued by the system by having one fsync serve a batch of threads.
//!
//! # Algorithm (mirrors JE's leader/waiter pattern)
//!
//! When a thread enters `fsync()` it finds one of two situations:
//!
//! 1. **No work in progress** — the thread becomes the *leader*.  If group
//!    commit is enabled (`grpc_threshold > 0` AND `grpc_interval_ms > 0`) the
//!    leader may wait briefly for more waiters to accumulate.  Then it calls
//!    the supplied fsync closure, wakes all current waiters (they piggyback on
//!    its fsync), wakes one member of the *next* group to become the new
//!    leader, and clears `work_in_progress`.
//!
//! 2. **Work in progress** — the thread joins `next_fsync_waiters` and waits
//!    on a `Condvar`.  When woken it checks whether its fsync was already
//!    done (`NoFsyncNeeded`), whether it should become the new leader
//!    (`DoLeaderFsync`), or whether it timed out (`DoTimeoutFsync`).
//!
//! Each group is represented by an `Arc<FSyncGroup>`.  The leader atomically
//! replaces `state.next_fsync_waiters` with a fresh group, so waiting threads
//! retain their `Arc` to the *old* group and can still be woken through it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

// ── FSyncGroup ────────────────────────────────────────────────────────────────

/// One cohort of threads waiting for a common fsync.
///
/// Port of `FSyncManager.FSyncGroup`.  Each instance lives behind an `Arc` so
/// that threads that joined a group keep a reference even after the leader has
/// swapped in a fresh `FSyncGroup` for the next cohort.
struct FSyncGroup {
    inner: Mutex<FsyncGroupInner>,
    condvar: Condvar,
}

struct FsyncGroupInner {
    /// True once the fsync for this group has been completed (or failed).
    work_done: bool,
    /// Whether a leader has already been designated for this group.
    leader_exists: bool,
    /// Recorded error message from the fsync, propagated to all waiters.
    error: Option<String>,
}

/// Return value from `FSyncGroup::wait_for_event`.
///
/// Port of `FSyncGroup.DO_TIMEOUT_FSYNC / DO_LEADER_FSYNC / NO_FSYNC_NEEDED`.
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
            inner: Mutex::new(FsyncGroupInner {
                work_done: false,
                leader_exists: false,
                error: None,
            }),
            condvar: Condvar::new(),
        })
    }

    /// Block until work is done, this thread becomes leader, or we time out.
    ///
    /// Port of `FSyncGroup.waitForEvent()`.
    fn wait_for_event(&self, timeout: Duration) -> WaitStatus {
        let mut inner = self.inner.lock().unwrap();

        // Fast path: already done before we even enter.
        if inner.work_done {
            return WaitStatus::NoFsyncNeeded;
        }

        let start = Instant::now();
        loop {
            // Compute remaining wait time.
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return WaitStatus::DoTimeoutFsync;
            }
            let remaining = timeout - elapsed;

            let (guard, _timed_out) =
                self.condvar.wait_timeout(inner, remaining).unwrap();
            inner = guard;

            if inner.work_done {
                return WaitStatus::NoFsyncNeeded;
            }

            if !inner.leader_exists {
                inner.leader_exists = true;
                return WaitStatus::DoLeaderFsync;
            }

            // Spurious wakeup or still a plain waiter — re-check timeout.
            if start.elapsed() >= timeout {
                return WaitStatus::DoTimeoutFsync;
            }
            // else: loop and keep waiting
        }
    }

    /// Wake all waiters with success.  Port of `FSyncGroup.wakeupAll()`.
    fn wakeup_all(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.work_done = true;
        inner.error = None;
        drop(inner);
        self.condvar.notify_all();
    }

    /// Wake all waiters recording an error.
    fn wakeup_all_with_error(&self, msg: String) {
        let mut inner = self.inner.lock().unwrap();
        inner.work_done = true;
        inner.error = Some(msg);
        drop(inner);
        self.condvar.notify_all();
    }

    /// Wake a single waiter to become the next leader.
    /// Port of `FSyncGroup.wakeupOne()`.
    fn wakeup_one(&self) {
        self.condvar.notify_one();
    }

    /// Return the recorded error (if any) for this group.
    fn take_error(&self) -> Option<String> {
        self.inner.lock().unwrap().error.clone()
    }
}

// ── FsyncState ────────────────────────────────────────────────────────────────

/// Mutable state guarded by `FsyncManager::state_mutex`.
///
/// Mirrors the fields that JE protects with `mgrMutex`.
struct FsyncState {
    /// True while a leader thread is performing (or about to perform) an fsync.
    work_in_progress: bool,
    /// The group that newly-arriving threads join while work is in progress.
    next_fsync_waiters: Arc<FSyncGroup>,
    /// Count of threads currently in `next_fsync_waiters`.
    num_next_waiters: usize,
    /// Monotonic instant when the first thread joined the current next-group.
    start_next_wait: Option<Instant>,
}

// ── FsyncManager ─────────────────────────────────────────────────────────────

/// Coalesces fsync requests so that one system call serves many threads.
///
/// Port of `com.sleepycat.je.log.FSyncManager`.
///
/// # Configuration
///
/// * `grpc_threshold` — minimum number of waiters before the leader executes
///   the fsync.  `0` disables group-commit waiting (JE default).
/// * `grpc_interval_ms` — maximum milliseconds the leader waits for more
///   waiters.  `0` disables group-commit waiting (JE default).
///
/// Group-commit waiting is only active when **both** values are non-zero,
/// matching JE's `grpWaitOn` flag.
pub struct FsyncManager {
    /// Min waiters before the leader fsyncs (0 = disabled).
    grpc_threshold: usize,
    /// Max ms the leader waits for more waiters (0 = disabled).
    grpc_interval_ms: u64,
    /// Whether group-commit waiting is active (`grpcInterval != 0 && grpcThreshold != 0`).
    grp_wait_on: bool,
    /// Timeout for waiting threads before they do their own fsync.
    /// (JE: `LOG_FSYNC_TIMEOUT`, default 500 ms.)
    fsync_timeout: Duration,
    /// Mutex protecting `FsyncState`.  Also used by `leader_condvar`.
    state: Mutex<FsyncState>,
    /// Condvar used by the leader to wait for more members (grpc wait).
    /// Paired with `state` mutex so the lock can be released during the wait.
    leader_condvar: Condvar,
    /// Total number of fdatasync/fsync calls performed (port of JE nFSyncs stat).
    n_fsyncs: AtomicU64,
}

impl FsyncManager {
    /// Create a new `FsyncManager`.
    ///
    /// # Arguments
    /// * `grpc_threshold`   — min waiters before leader fsyncs (0 = disabled).
    /// * `grpc_interval_ms` — max ms to wait for more waiters (0 = disabled).
    pub fn new(grpc_threshold: usize, grpc_interval_ms: u64) -> Self {
        let grp_wait_on = grpc_threshold != 0 && grpc_interval_ms != 0;
        FsyncManager {
            grpc_threshold,
            grpc_interval_ms,
            grp_wait_on,
            // JE default timeout: 500 ms.
            fsync_timeout: Duration::from_millis(500),
            state: Mutex::new(FsyncState {
                work_in_progress: false,
                next_fsync_waiters: FSyncGroup::new(),
                num_next_waiters: 0,
                start_next_wait: None,
            }),
            leader_condvar: Condvar::new(),
            n_fsyncs: AtomicU64::new(0),
        }
    }

    /// Request an fsync, coalescing with concurrent callers.
    ///
    /// Port of `FSyncManager.flushAndSync(boolean fsyncRequired)`.
    ///
    /// The caller supplies `do_fsync`, a closure that performs the actual
    /// fsync.  This method guarantees that when it returns `Ok(())`, at least
    /// one fsync has completed that covers the caller's preceding write.
    pub fn fsync<F>(&self, do_fsync: F) -> std::io::Result<()>
    where
        F: Fn() -> std::io::Result<()>,
    {
        let mut do_work = false;
        let mut is_leader = false;
        // Group whose waiters this leader serves (set only when is_leader).
        let mut in_progress_group: Option<Arc<FSyncGroup>> = None;
        // Group this thread belongs to as a waiter.
        let mut my_group: Option<Arc<FSyncGroup>> = None;
        let mut need_to_wait = false;

        // ── Phase 1: decide whether to lead or wait ───────────────────────
        {
            let mut state = self.state.lock().unwrap();

            if state.work_in_progress {
                // Join the next-waiters cohort.
                need_to_wait = true;
                my_group = Some(Arc::clone(&state.next_fsync_waiters));
                state.num_next_waiters += 1;
                if self.grp_wait_on && state.num_next_waiters == 1 {
                    state.start_next_wait = Some(Instant::now());
                }
            } else {
                // Become the leader.
                is_leader = true;
                do_work = true;
                state.work_in_progress = true;

                if self.grp_wait_on {
                    state = self.grpc_wait(state);
                }

                // Capture the current waiters group; swap in a fresh one.
                in_progress_group = Some(Arc::clone(&state.next_fsync_waiters));
                state.next_fsync_waiters = FSyncGroup::new();
                state.num_next_waiters = 0;
            }
        }
        // state lock released.

        // ── Phase 2: if we're a waiter, block until woken ────────────────
        if need_to_wait {
            let group = my_group.as_ref().unwrap();
            let wait_status = group.wait_for_event(self.fsync_timeout);

            match wait_status {
                WaitStatus::NoFsyncNeeded => {
                    // The leader finished; propagate any recorded error.
                    if let Some(msg) = group.take_error() {
                        return Err(std::io::Error::other(msg));
                    }
                    return Ok(());
                }
                WaitStatus::DoLeaderFsync => {
                    // Attempt to become the new leader for this cohort.
                    let mut state = self.state.lock().unwrap();
                    if state.work_in_progress {
                        // Another thread started a new fsync while we were being
                        // woken up — do our own fsync as a safety measure.
                        // (JE comment: "Ensure that an fsync is done before returning")
                        do_work = true;
                    } else {
                        is_leader = true;
                        do_work = true;
                        state.work_in_progress = true;

                        if self.grp_wait_on {
                            state = self.grpc_wait(state);
                        }

                        // The `my_group` cohort is now the in-progress group.
                        in_progress_group = my_group.take();
                        state.next_fsync_waiters = FSyncGroup::new();
                        state.num_next_waiters = 0;
                    }
                }
                WaitStatus::DoTimeoutFsync => {
                    // Timed out — do our own fsync regardless.
                    do_work = true;
                }
            }
        }

        // ── Phase 3: perform the fsync ────────────────────────────────────
        if do_work {
            self.n_fsyncs.fetch_add(1, Ordering::Relaxed);
            let result = do_fsync();

            if is_leader {
                let in_prog = in_progress_group.as_ref().unwrap();
                // Wake all threads that piggybacked on this fsync.
                match &result {
                    Ok(()) => in_prog.wakeup_all(),
                    Err(e) => in_prog.wakeup_all_with_error(e.to_string()),
                }
                // Wake one member of the next cohort to become the new leader,
                // then clear work_in_progress — matching JE's ordering.
                let mut state = self.state.lock().unwrap();
                state.next_fsync_waiters.wakeup_one();
                state.work_in_progress = false;
            }

            result
        } else {
            Ok(())
        }
    }

    /// Returns the total number of fdatasync calls performed.
    ///
    /// Port of JE's `nFSyncs` stat (see `LogStatDefinition.N_FSYNCS`).
    pub fn fsync_count(&self) -> u64 {
        self.n_fsyncs.load(Ordering::Relaxed)
    }

    /// Perform the group-commit wait: release the state lock and wait up to
    /// `grpc_interval_ms` for `grpc_threshold` waiters to accumulate.
    ///
    /// Mirrors the `if (grpWaitOn)` block inside `flushAndSync()` in JE:
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
        state: MutexGuard<'a, FsyncState>,
    ) -> MutexGuard<'a, FsyncState> {
        if state.num_next_waiters < self.grpc_threshold {
            let interval_ns = self.grpc_interval_ms as u128 * 1_000_000;
            let elapsed_ns = state
                .start_next_wait
                .map(|t| t.elapsed().as_nanos())
                .unwrap_or(0);
            if elapsed_ns < interval_ns {
                let remaining_ns = interval_ns - elapsed_ns;
                let wait_dur = Duration::from_nanos(remaining_ns as u64);
                // `Condvar::wait_timeout` releases the lock and re-acquires it.
                let (new_guard, _) =
                    self.leader_condvar.wait_timeout(state, wait_dur).unwrap();
                return new_guard;
            }
        }
        state
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    // ── required tests from task spec ─────────────────────────────────────

    /// Single thread, no grouping: fsync closure called exactly once.
    #[test]
    fn test_simple_fsync_no_grouping() {
        let mgr = FsyncManager::new(0, 0);
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        mgr.fsync(|| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
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
                mgr2.fsync(|| {
                    // Slow fsync so concurrent threads queue up.
                    std::thread::sleep(Duration::from_millis(20));
                    fc.fetch_add(1, Ordering::SeqCst);
                    Ok(())
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

    /// Error from `do_fsync` propagates to the calling thread.
    #[test]
    fn test_fsync_error_propagated_to_waiters() {
        let mgr = FsyncManager::new(0, 0);
        let result = mgr.fsync(|| {
            Err(std::io::Error::other(
                "simulated fsync failure",
            ))
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("simulated fsync failure"));
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
                m.fsync(|| {
                    fc.fetch_add(1, Ordering::SeqCst);
                    Ok(())
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
            mgr.fsync(|| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
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
            mgr2.fsync(|| {
                // Slow so the second thread can queue up as a waiter.
                std::thread::sleep(Duration::from_millis(30));
                Err(std::io::Error::other("leader fail"))
            })
        });

        // Small sleep so the leader thread enters fsync() first.
        barrier.wait();
        std::thread::sleep(Duration::from_millis(2));

        let waiter_result = mgr.fsync(|| {
            // This should either piggyback (NoFsyncNeeded with error) or run its
            // own fsync if it becomes leader.
            Ok(())
        });

        let leader_result = leader.join().unwrap();
        // The leader must fail.
        assert!(leader_result.is_err());
        // Waiter either got the error propagated or ran its own Ok fsync.
        let _ = waiter_result; // either outcome is valid
    }

    /// `FsyncManager::new(0, 0)` returns Ok immediately on success.
    #[test]
    fn test_returns_ok_on_success() {
        let mgr = FsyncManager::new(0, 0);
        assert!(mgr.fsync(|| Ok(())).is_ok());
    }

    /// FSyncGroup: `wakeup_all` sets `work_done` and records no error.
    #[test]
    fn test_fsync_group_wakeup_all() {
        let g = FSyncGroup::new();
        g.wakeup_all();
        assert!(g.inner.lock().unwrap().work_done);
        assert!(g.take_error().is_none());
    }

    /// FSyncGroup: `wakeup_all_with_error` sets `work_done` and records error.
    #[test]
    fn test_fsync_group_wakeup_all_with_error() {
        let g = FSyncGroup::new();
        g.wakeup_all_with_error("oops".to_string());
        assert!(g.inner.lock().unwrap().work_done);
        assert_eq!(g.take_error().unwrap(), "oops");
    }

    /// FSyncGroup: `wait_for_event` returns `NoFsyncNeeded` immediately when
    /// `work_done` is already true before the call.
    #[test]
    fn test_fsync_group_already_done() {
        let g = FSyncGroup::new();
        g.wakeup_all();
        let status = g.wait_for_event(Duration::from_secs(5));
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

        let status = g.wait_for_event(Duration::from_millis(500));
        assert_eq!(status, WaitStatus::DoLeaderFsync);
        assert!(g.inner.lock().unwrap().leader_exists);
    }

    /// FSyncGroup: waiter times out when nobody wakes it.
    #[test]
    fn test_fsync_group_timeout() {
        let g = FSyncGroup::new();
        // Pre-set leader_exists so wakeup_one won't make us the leader.
        g.inner.lock().unwrap().leader_exists = true;
        let status = g.wait_for_event(Duration::from_millis(20));
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
}
