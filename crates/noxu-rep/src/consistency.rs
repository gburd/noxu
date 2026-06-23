//! Consistency policies for replica reads.
//!
//! Port of the JE `ReplicaConsistencyPolicy` hierarchy:
//! `NoConsistencyRequiredPolicy`, `TimeConsistencyPolicy`, and
//! `CommitPointConsistencyPolicy`
//! (`com.sleepycat.je.rep.{NoConsistencyRequiredPolicy,TimeConsistencyPolicy,
//! CommitPointConsistencyPolicy}`).
//!
//! ## What this does (REP-10)
//!
//! A read transaction that begins on a *replica* must not proceed until the
//! replica's applied state satisfies the configured policy.  JE implements
//! this in `ReplicaConsistencyPolicy.ensureConsistency` →
//! `Replica.ConsistencyTracker.awaitVLSN` / `lagAwait`, which BLOCKS the
//! `beginTransaction` call until the replica has replayed far enough, or the
//! policy timeout expires (→ `ReplicaConsistencyException`).
//!
//! [`ConsistencyTracker`] is the Rust equivalent: it reuses the REP-7
//! `last_applied_vlsn` handle (`ReplicaReplay::last_applied_vlsn_handle`) as
//! the wait predicate — NOT a parallel tracker — and blocks the caller until
//! the predicate holds or the timeout elapses (a clean [`RepError`], never a
//! hang).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use noxu_sync::{Condvar, Mutex};

use crate::error::{RepError, Result};

/// A consistency policy that determines what state a replica must be in
/// before a read operation can proceed.
///
/// Consistency policy hierarchy for replication.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ConsistencyPolicy {
    /// No consistency requirement -- read from any state.
    ///
    ///
    #[default]
    NoConsistency,

    /// Time-based consistency: the replica must be within `max_lag` of
    /// the master's commit point.
    ///
    ///
    TimeConsistency {
        /// Maximum permissible lag behind the master.
        max_lag: Duration,
        /// How long to wait for the replica to catch up.
        timeout: Duration,
    },

    /// Commit-point consistency: the replica must have applied up to
    /// a specific VLSN before the read can proceed.
    ///
    ///
    CommitPointConsistency {
        /// The VLSN sequence that must be applied on the replica.
        vlsn: i64,
        /// How long to wait for the replica to reach the VLSN.
        timeout: Duration,
    },
}

impl ConsistencyPolicy {
    /// Build a [`ConsistencyPolicy::CommitPointConsistency`] from a
    /// [`CommitToken`] minted by the master.
    ///
    /// Port of `new CommitPointConsistencyPolicy(commitToken, timeout, unit)`:
    /// a client that did a write on the master passes the returned token to a
    /// replica read so the read waits until the replica has replayed past it.
    pub fn commit_point(token: &crate::CommitToken, timeout: Duration) -> Self {
        ConsistencyPolicy::CommitPointConsistency {
            vlsn: token.vlsn() as i64,
            timeout,
        }
    }

    /// Checks whether the given replica state satisfies this consistency
    /// policy.
    ///
    /// - `current_vlsn`: The replica's current VLSN sequence.
    /// - `master_vlsn`: The master's current VLSN sequence.
    ///
    /// Returns `Ok(true)` if the consistency requirement is met, or an
    /// error describing why it is not.
    pub fn check_consistency(
        &self,
        current_vlsn: i64,
        master_vlsn: i64,
    ) -> Result<bool> {
        match self {
            ConsistencyPolicy::NoConsistency => Ok(true),

            ConsistencyPolicy::TimeConsistency { max_lag, .. } => {
                // Approximate: each VLSN is roughly 1ms of lag.
                // In a real implementation this would use timestamps from
                // heartbeat messages. Here we use VLSN difference as a proxy.
                let lag_vlsns = master_vlsn.saturating_sub(current_vlsn);
                if lag_vlsns < 0 {
                    // Replica is ahead -- shouldn't happen, but treat as ok.
                    return Ok(true);
                }
                let lag_ms = lag_vlsns as u64;
                let limit_ms = max_lag.as_millis() as u64;
                if lag_ms <= limit_ms {
                    Ok(true)
                } else {
                    Err(RepError::ReplicaLagExceeded { lag_ms, limit_ms })
                }
            }

            ConsistencyPolicy::CommitPointConsistency { vlsn, .. } => {
                if current_vlsn >= *vlsn {
                    Ok(true)
                } else {
                    Err(RepError::ConsistencyTimeout(
                        // Report the timeout configured for this policy.
                        self.timeout().unwrap_or(Duration::ZERO),
                    ))
                }
            }
        }
    }

    /// Returns the timeout associated with this policy, if any.
    pub fn timeout(&self) -> Option<Duration> {
        match self {
            ConsistencyPolicy::NoConsistency => None,
            ConsistencyPolicy::TimeConsistency { timeout, .. } => {
                Some(*timeout)
            }
            ConsistencyPolicy::CommitPointConsistency { timeout, .. } => {
                Some(*timeout)
            }
        }
    }
}

impl std::fmt::Display for ConsistencyPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsistencyPolicy::NoConsistency => write!(f, "NoConsistency"),
            ConsistencyPolicy::TimeConsistency { max_lag, timeout } => {
                write!(
                    f,
                    "TimeConsistency(max_lag={:?}, timeout={:?})",
                    max_lag, timeout
                )
            }
            ConsistencyPolicy::CommitPointConsistency { vlsn, timeout } => {
                write!(
                    f,
                    "CommitPointConsistency(vlsn={}, timeout={:?})",
                    vlsn, timeout
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ConsistencyTracker (REP-10 piece A): the blocking consistency-wait.
// ---------------------------------------------------------------------------

/// Tracks the replica's applied state and blocks a read until the configured
/// [`ConsistencyPolicy`] is satisfied.
///
/// Port of `com.sleepycat.je.rep.impl.node.Replica.ConsistencyTracker`
/// (`awaitVLSN` / `lagAwait` / `await`).  In JE the tracker holds ordered
/// `CountDownLatch`es that the replay thread *trips* as VLSNs are applied; a
/// reader parks on the latch with the policy timeout and gets a
/// `ReplicaConsistencyException` if it expires.
///
/// Here the predicate is the REP-7 `last_applied_vlsn` handle
/// (`ReplicaReplay::last_applied_vlsn_handle`) — the SAME `Arc<AtomicU64>` the
/// replay driver advances after each committed apply.  We do NOT add a
/// parallel tracker; we read the existing hook.  `master_vlsn` is the
/// master's latest known commit VLSN (the feeder stream / heartbeat
/// high-water), used by the time policy — JE's `masterTxnEndVLSN`.
#[derive(Clone)]
pub struct ConsistencyTracker {
    /// REP-7 hook: highest VLSN whose effects are visible in the replica's
    /// live tree.  Advanced by `ReplicaReplay`; read here as the predicate.
    last_applied_vlsn: Arc<AtomicU64>,

    /// Master's latest known commit VLSN (feeder stream / heartbeat
    /// high-water).  Port of `ConsistencyTracker.masterTxnEndVLSN`; used by
    /// [`ConsistencyPolicy::TimeConsistency`] to estimate the lag.
    master_vlsn: Arc<AtomicU64>,

    /// Parking lot signalled when `last_applied_vlsn` advances, so a waiting
    /// reader wakes promptly.  Port of the latch trip in
    /// `ConsistencyTracker.trackVLSN`.
    signal: Arc<(Mutex<()>, Condvar)>,
}

impl ConsistencyTracker {
    /// How often a waiter re-checks the predicate even without an explicit
    /// wake.  The replay thread advances `last_applied_vlsn` via a plain
    /// atomic store; this tick bounds the wakeup latency if a `notify`
    /// is ever missed, so the wait can never hang past the policy timeout.
    //
    // ponytail: 5ms re-check tick instead of wiring the latch-trip callback
    // into the replay thread (JE trips the latch from `trackVLSN`). The tick
    // bounds wakeup latency and guarantees no hang; wire an explicit notify
    // into `ReplicaReplay::advance_vlsn` if sub-ms read latency ever matters.
    const RECHECK_TICK: Duration = Duration::from_millis(5);

    /// Build a tracker over the REP-7 `last_applied_vlsn` handle.
    pub fn new(last_applied_vlsn: Arc<AtomicU64>) -> Self {
        Self {
            last_applied_vlsn,
            master_vlsn: Arc::new(AtomicU64::new(0)),
            signal: Arc::new((Mutex::new(()), Condvar::new())),
        }
    }

    /// The replica's last-applied VLSN (the wait predicate).
    pub fn last_applied_vlsn(&self) -> u64 {
        self.last_applied_vlsn.load(Ordering::Acquire)
    }

    /// Record the master's latest known commit VLSN (feeder / heartbeat).
    ///
    /// Port of `ConsistencyTracker.trackHeartbeat` updating `masterTxnEndVLSN`.
    /// Monotone.
    pub fn set_master_vlsn(&self, vlsn: u64) {
        self.master_vlsn.fetch_max(vlsn, Ordering::AcqRel);
    }

    /// The master's latest known commit VLSN.
    pub fn master_vlsn(&self) -> u64 {
        self.master_vlsn.load(Ordering::Acquire)
    }

    /// Wake any reader parked in [`Self::await_consistency`].
    ///
    /// Called when the replica applies a new entry (the replay thread can
    /// invoke this after advancing `last_applied_vlsn`).  Equivalent to the
    /// latch trip in `ConsistencyTracker.trackVLSN`.  Optional: a waiter also
    /// re-checks every [`Self::RECHECK_TICK`], so a missed notify only delays
    /// (never hangs) the read.
    pub fn notify_applied(&self) {
        let (_lock, cv) = &*self.signal;
        cv.notify_all();
    }

    /// Block until the replica's applied state satisfies `policy`, or the
    /// policy timeout expires.
    ///
    /// Port of `ReplicaConsistencyPolicy.ensureConsistency` →
    /// `ConsistencyTracker.awaitVLSN` / `lagAwait`:
    ///
    /// - [`ConsistencyPolicy::NoConsistency`]: returns immediately (JE
    ///   `NoConsistencyRequiredPolicy.ensureConsistency` is a no-op).
    /// - [`ConsistencyPolicy::CommitPointConsistency`]: waits until
    ///   `last_applied_vlsn >= token.vlsn` (JE `awaitVLSN` comparing against
    ///   `lastReplayedTxnVLSN`).
    /// - [`ConsistencyPolicy::TimeConsistency`]: waits until the estimated
    ///   lag behind the master is within `max_lag` (JE `lagAwait`).
    ///
    /// On timeout returns a clean [`RepError`] —
    /// [`RepError::ConsistencyTimeout`] for the commit-point policy and
    /// [`RepError::ReplicaLagExceeded`] for the time policy — the equivalent
    /// of JE's `ReplicaConsistencyException`.  NEVER hangs.
    pub fn await_consistency(&self, policy: &ConsistencyPolicy) -> Result<()> {
        let target_vlsn = match policy {
            // NoConsistencyRequiredPolicy.ensureConsistency: no-op.
            ConsistencyPolicy::NoConsistency => return Ok(()),

            // awaitVLSN(commitToken.getVLSN()).
            ConsistencyPolicy::CommitPointConsistency { vlsn, .. } => {
                *vlsn as u64
            }

            // lagAwait: convert the permissible lag into the VLSN the replica
            // must reach — master_vlsn back off by `max_lag` (1 VLSN ≈ 1ms, the
            // same proxy `check_consistency` uses; a real impl would use the
            // vlsn→time map).
            ConsistencyPolicy::TimeConsistency { max_lag, .. } => {
                let master = self.master_vlsn();
                let slack = max_lag.as_millis() as u64;
                master.saturating_sub(slack)
            }
        };

        // Fast path: already satisfied (JE awaitVLSN returns before parking
        // when `vlsn <= compareVLSN`).
        if self.last_applied_vlsn() >= target_vlsn {
            return Ok(());
        }

        let timeout = policy.timeout().unwrap_or(Duration::ZERO);
        let deadline = Instant::now() + timeout;
        let (lock, cv) = &*self.signal;
        let mut guard = lock.lock();
        loop {
            if self.last_applied_vlsn() >= target_vlsn {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                // Timed out — clean error, never a hang. JE throws
                // ReplicaConsistencyException here.
                return Err(self.timeout_error(policy, target_vlsn));
            }
            // Park until the next notify or the recheck tick, whichever is
            // sooner; bounded by the deadline so the timeout is honoured.
            let remaining = deadline - now;
            let wait = remaining.min(Self::RECHECK_TICK);
            let _ = cv.wait_for(&mut guard, wait);
        }
    }

    /// Build the timeout error for `policy`, matching the variant the
    /// non-blocking [`ConsistencyPolicy::check_consistency`] reports.
    fn timeout_error(
        &self,
        policy: &ConsistencyPolicy,
        target_vlsn: u64,
    ) -> RepError {
        match policy {
            ConsistencyPolicy::TimeConsistency { max_lag, .. } => {
                let lag_ms =
                    self.master_vlsn().saturating_sub(self.last_applied_vlsn());
                RepError::ReplicaLagExceeded {
                    lag_ms,
                    limit_ms: max_lag.as_millis() as u64,
                }
            }
            _ => {
                let _ = target_vlsn;
                RepError::ConsistencyTimeout(
                    policy.timeout().unwrap_or(Duration::ZERO),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_consistency_always_passes() {
        let policy = ConsistencyPolicy::NoConsistency;
        assert!(policy.check_consistency(0, 1000).unwrap());
        assert!(policy.check_consistency(1000, 1000).unwrap());
        assert!(policy.check_consistency(1000, 0).unwrap());
    }

    #[test]
    fn test_no_consistency_timeout_is_none() {
        let policy = ConsistencyPolicy::NoConsistency;
        assert!(policy.timeout().is_none());
    }

    #[test]
    fn test_time_consistency_within_lag() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        // Replica is 50 VLSNs behind, limit is 100ms.
        assert!(policy.check_consistency(950, 1000).unwrap());
    }

    #[test]
    fn test_time_consistency_at_limit() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        // Exactly at limit.
        assert!(policy.check_consistency(900, 1000).unwrap());
    }

    #[test]
    fn test_time_consistency_exceeds_lag() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        let result = policy.check_consistency(800, 1000);
        assert!(result.is_err());
        match result.unwrap_err() {
            RepError::ReplicaLagExceeded { lag_ms, limit_ms } => {
                assert_eq!(lag_ms, 200);
                assert_eq!(limit_ms, 100);
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_time_consistency_replica_ahead() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        // Replica ahead of master -- should pass.
        assert!(policy.check_consistency(1000, 500).unwrap());
    }

    #[test]
    fn test_time_consistency_timeout() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        assert_eq!(policy.timeout(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_commit_point_satisfied() {
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 500,
            timeout: Duration::from_secs(10),
        };
        assert!(policy.check_consistency(500, 1000).unwrap());
        assert!(policy.check_consistency(600, 1000).unwrap());
    }

    #[test]
    fn test_commit_point_not_satisfied() {
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 500,
            timeout: Duration::from_secs(10),
        };
        let result = policy.check_consistency(400, 1000);
        assert!(result.is_err());
        match result.unwrap_err() {
            RepError::ConsistencyTimeout(d) => {
                assert_eq!(d, Duration::from_secs(10));
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_commit_point_timeout() {
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 100,
            timeout: Duration::from_secs(10),
        };
        assert_eq!(policy.timeout(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_default_is_no_consistency() {
        assert_eq!(
            ConsistencyPolicy::default(),
            ConsistencyPolicy::NoConsistency
        );
    }

    #[test]
    fn test_display_no_consistency() {
        assert_eq!(
            ConsistencyPolicy::NoConsistency.to_string(),
            "NoConsistency"
        );
    }

    #[test]
    fn test_display_time_consistency() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(500),
            timeout: Duration::from_secs(10),
        };
        let s = policy.to_string();
        assert!(s.contains("TimeConsistency"));
        assert!(s.contains("500ms"));
    }

    #[test]
    fn test_display_commit_point() {
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 42,
            timeout: Duration::from_secs(5),
        };
        let s = policy.to_string();
        assert!(s.contains("CommitPointConsistency"));
        assert!(s.contains("42"));
    }

    #[test]
    fn test_clone_and_eq() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };
        let cloned = policy.clone();
        assert_eq!(policy, cloned);
    }

    // -- ConsistencyTracker (blocking wait) ------------------------------

    #[test]
    fn test_tracker_no_consistency_never_blocks() {
        let applied = Arc::new(AtomicU64::new(0));
        let tracker = ConsistencyTracker::new(applied);
        // master far ahead; NoConsistency returns immediately.
        tracker.set_master_vlsn(10_000);
        tracker.await_consistency(&ConsistencyPolicy::NoConsistency).unwrap();
    }

    #[test]
    fn test_tracker_commit_point_already_satisfied() {
        let applied = Arc::new(AtomicU64::new(500));
        let tracker = ConsistencyTracker::new(applied);
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 500,
            timeout: Duration::from_secs(5),
        };
        // Fast path: no wait.
        tracker.await_consistency(&policy).unwrap();
    }

    /// Headline behaviour: a commit-point read BLOCKS until the replica
    /// applies the target VLSN, then returns Ok (blocks-then-sees-it).
    #[test]
    fn test_tracker_commit_point_blocks_then_satisfied() {
        let applied = Arc::new(AtomicU64::new(0));
        let tracker = ConsistencyTracker::new(Arc::clone(&applied));
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 7,
            timeout: Duration::from_secs(5),
        };

        // Advance the replica from another thread after a short delay.
        let tracker_bg = tracker.clone();
        let applied_bg = Arc::clone(&applied);
        let bg = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            applied_bg.store(7, Ordering::Release);
            tracker_bg.notify_applied();
        });

        let start = Instant::now();
        tracker.await_consistency(&policy).unwrap();
        // It actually blocked (did not return on the fast path).
        assert!(start.elapsed() >= Duration::from_millis(40));
        assert!(applied.load(Ordering::Acquire) >= 7);
        bg.join().unwrap();
    }

    /// Headline behaviour: a commit-point read that never catches up returns
    /// a clean ConsistencyTimeout — NOT a hang.
    #[test]
    fn test_tracker_commit_point_times_out() {
        let applied = Arc::new(AtomicU64::new(0));
        let tracker = ConsistencyTracker::new(applied);
        let policy = ConsistencyPolicy::CommitPointConsistency {
            vlsn: 100,
            timeout: Duration::from_millis(80),
        };
        let start = Instant::now();
        let err = tracker.await_consistency(&policy).unwrap_err();
        // Returned (no hang) and within a sane bound of the timeout.
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(matches!(err, RepError::ConsistencyTimeout(_)));
    }

    /// Headline behaviour: a lagging replica blocks a time-consistency read
    /// until it catches up within the lag.
    #[test]
    fn test_tracker_time_blocks_then_catches_up() {
        let applied = Arc::new(AtomicU64::new(0));
        let tracker = ConsistencyTracker::new(Arc::clone(&applied));
        // master at 1000, permissible lag 100ms (=100 VLSN proxy) -> must
        // reach >= 900.
        tracker.set_master_vlsn(1000);
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
        };

        let tracker_bg = tracker.clone();
        let applied_bg = Arc::clone(&applied);
        let bg = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            applied_bg.store(920, Ordering::Release);
            tracker_bg.notify_applied();
        });

        let start = Instant::now();
        tracker.await_consistency(&policy).unwrap();
        assert!(start.elapsed() >= Duration::from_millis(30));
        bg.join().unwrap();
    }

    /// A time-consistency read that never catches up returns
    /// ReplicaLagExceeded — not a hang.
    #[test]
    fn test_tracker_time_times_out() {
        let applied = Arc::new(AtomicU64::new(0));
        let tracker = ConsistencyTracker::new(applied);
        tracker.set_master_vlsn(1000);
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(10),
            timeout: Duration::from_millis(80),
        };
        let start = Instant::now();
        let err = tracker.await_consistency(&policy).unwrap_err();
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(matches!(err, RepError::ReplicaLagExceeded { .. }));
    }
}
