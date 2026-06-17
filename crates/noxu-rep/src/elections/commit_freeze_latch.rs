//! Commit freeze latch (D3, JE `CommitFreezeLatch`).
//!
//! Freezes VLSN advancement on a node for the duration of an election round so
//! that the VLSN / DTVLSN the node reports in its Paxos `Promise` does not
//! advance mid-election. This makes the proposer's value selection see a stable
//! snapshot of each acceptor's progress.
//!
//! As JE notes, this is a "good faith effort" to freeze the VLSN, not a hard
//! guarantee — but it is a required protocol component that closes a class of
//! races where an acceptor reports VLSN=N in Phase 1 and then advances to
//! VLSN>N before Phase 2, so it accepts a value chosen against a stale VLSN.
//!
//! The class coordinates three roles (JE):
//!   - `freeze(proposal)` — invoked by the Acceptor in response to a Promise.
//!   - `vlsn_event(proposal)` — invoked by the Learner when an election result
//!     arrives; lifts the freeze if the result is for a newer-or-equal round.
//!   - `await_thaw()` — invoked by the Replay thread before advancing the VLSN;
//!     blocks until the freeze lifts or its timeout elapses.
//!
//! Both `vlsn_event` and `await_thaw` are no-ops in the absence of a freeze.

use crate::elections::proposal::Proposal;
use noxu_sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Default freeze timeout (JE `DEFAULT_LATCH_TIMEOUT = 5000ms`).
const DEFAULT_LATCH_TIMEOUT: Duration = Duration::from_millis(5000);

/// Internal mutable state behind the latch's mutex.
struct FreezeState {
    /// The current frozen proposal, if any (JE `proposal`).
    proposal: Option<Proposal>,
    /// The instant the freeze expires (JE `freezeEnd`).
    freeze_end: Option<Instant>,
    /// Generation counter — bumped on every `freeze`/thaw so `await_thaw`
    /// waiters can detect that the latch they waited on was superseded
    /// (equivalent to JE swapping in a fresh `CountDownLatch`).
    generation: u64,
    /// True while a freeze is in effect (the latch is "closed").
    frozen: bool,
}

/// Diagnostic counters (JE `freezeCount` / `awaitTimeoutCount` /
/// `awaitElectionCount`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FreezeStats {
    pub freeze_count: u64,
    pub await_timeout_count: u64,
    pub await_election_count: u64,
}

/// See module docs.
pub struct CommitFreezeLatch {
    state: Mutex<FreezeState>,
    /// Signalled when the freeze is lifted (thaw) or superseded.
    thaw_signal: Condvar,
    timeout: Duration,
    stats: Mutex<FreezeStats>,
}

impl Default for CommitFreezeLatch {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitFreezeLatch {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(FreezeState {
                proposal: None,
                freeze_end: None,
                generation: 0,
                frozen: false,
            }),
            thaw_signal: Condvar::new(),
            timeout: DEFAULT_LATCH_TIMEOUT,
            stats: Mutex::new(FreezeStats::default()),
        }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        let mut l = Self::new();
        l.timeout = timeout;
        l
    }

    pub fn stats(&self) -> FreezeStats {
        *self.stats.lock()
    }

    /// Initiate or extend a freeze in response to a new election proposal
    /// (JE `freeze`). A proposal that is not newer than the current frozen
    /// one is ignored. A newer proposal supersedes the existing freeze
    /// (waking any current waiters, who will re-check and re-freeze).
    pub fn freeze(&self, freeze_proposal: Proposal) {
        let mut st = self.state.lock();
        if let Some(ref cur) = st.proposal {
            // Older or equal proposal — ignore (JE `compareTo(proposal) <= 0`).
            if !freeze_proposal.is_better_than(cur) {
                return;
            }
            // A newer proposal supersedes: wake current waiters.
            st.generation = st.generation.wrapping_add(1);
            self.thaw_signal.notify_all();
        }
        st.proposal = Some(freeze_proposal);
        st.freeze_end = Some(Instant::now() + self.timeout);
        st.frozen = true;
        self.stats.lock().freeze_count += 1;
    }

    /// Invoked by the Learner when an election result arrives. Lifts the
    /// freeze only if the result's proposal is newer-or-equal to the one that
    /// established the freeze (JE `vlsnEvent`: `listenerProposal.compareTo(
    /// proposal) >= 0`). No-op if no freeze is in effect.
    pub fn vlsn_event(&self, listener_proposal: &Proposal) {
        let mut st = self.state.lock();
        let lift = match st.proposal {
            None => return, // nothing frozen
            // Lift when the result is newer-or-equal (not strictly older).
            Some(ref cur) => !cur.is_better_than(listener_proposal),
        };
        if lift {
            st.frozen = false;
            st.generation = st.generation.wrapping_add(1);
            self.thaw_signal.notify_all();
        }
    }

    /// Clears the latch, freeing any waiters (JE `clearLatch`).
    pub fn clear_latch(&self) {
        let mut st = self.state.lock();
        st.frozen = false;
        st.proposal = None;
        st.freeze_end = None;
        st.generation = st.generation.wrapping_add(1);
        self.thaw_signal.notify_all();
    }

    /// Wait for an event that unfreezes the VLSN (JE `awaitThaw`). Invoked by
    /// the Replay thread before advancing the VLSN. Completion always results
    /// in the freeze being lifted.
    ///
    /// Returns `true` if the await was satisfied by an election completing,
    /// `false` if no freeze was in effect or the freeze timed out. The latch
    /// must be re-initialized (via `freeze`) for a subsequent round.
    pub fn await_thaw(&self) -> bool {
        let mut st = self.state.lock();
        if !st.frozen {
            return false; // no freeze in effect
        }
        let my_generation = st.generation;
        loop {
            // Thawed (or superseded) by a notify that bumped the generation.
            if !st.frozen || st.generation != my_generation {
                // If frozen==false this was a genuine thaw (election event).
                if !st.frozen {
                    self.stats.lock().await_election_count += 1;
                    st.proposal = None;
                    st.freeze_end = None;
                    return true;
                }
                // generation changed but still frozen -> superseded by a newer
                // freeze; treat as a fresh wait on the new generation.
                return false;
            }
            let now = Instant::now();
            let end = st.freeze_end.unwrap_or(now);
            if now >= end {
                // Freeze timed out without an election event.
                self.stats.lock().await_timeout_count += 1;
                st.frozen = false;
                st.proposal = None;
                st.freeze_end = None;
                return false;
            }
            let remaining = end - now;
            let _ = self.thaw_signal.wait_for(&mut st, remaining);
        }
    }

    /// True if a freeze is currently in effect.
    pub fn is_frozen(&self) -> bool {
        self.state.lock().frozen
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn prop(name: &str, vlsn: u64, term: u64) -> Proposal {
        Proposal::with_timestamp(name.to_string(), vlsn, 1, term, 0)
    }

    #[test]
    fn await_thaw_no_freeze_returns_false() {
        let latch = CommitFreezeLatch::new();
        assert!(!latch.await_thaw(), "no freeze in effect");
    }

    #[test]
    fn freeze_then_election_event_thaws() {
        let latch =
            Arc::new(CommitFreezeLatch::with_timeout(Duration::from_secs(5)));
        latch.freeze(prop("n1", 100, 5));
        assert!(latch.is_frozen());
        let l2 = Arc::clone(&latch);
        let waiter = thread::spawn(move || l2.await_thaw());
        // Deliver the election result for the same round -> thaw.
        thread::sleep(Duration::from_millis(20));
        latch.vlsn_event(&prop("n1", 100, 5));
        let started = Instant::now();
        let thawed = waiter.join().unwrap();
        assert!(thawed, "election event must thaw the freeze");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "must wake on the event, not spin to timeout"
        );
        assert_eq!(latch.stats().await_election_count, 1);
    }

    #[test]
    fn freeze_times_out_without_event() {
        let latch = CommitFreezeLatch::with_timeout(Duration::from_millis(40));
        latch.freeze(prop("n1", 100, 5));
        let thawed = latch.await_thaw();
        assert!(!thawed, "timeout returns false");
        assert_eq!(latch.stats().await_timeout_count, 1);
        assert!(!latch.is_frozen());
    }

    #[test]
    fn older_proposal_does_not_extend_freeze() {
        let latch = CommitFreezeLatch::with_timeout(Duration::from_secs(5));
        latch.freeze(prop("n1", 200, 5));
        let before = latch.stats().freeze_count;
        // An older (lower vlsn/term) proposal must be ignored.
        latch.freeze(prop("n1", 100, 3));
        assert_eq!(
            latch.stats().freeze_count,
            before,
            "older proposal must not (re)freeze"
        );
    }

    #[test]
    fn older_election_event_does_not_thaw() {
        let latch = CommitFreezeLatch::with_timeout(Duration::from_millis(80));
        latch.freeze(prop("n1", 200, 5));
        // An election result for an OLDER round must not lift the freeze.
        latch.vlsn_event(&prop("n1", 100, 3));
        assert!(latch.is_frozen(), "older event must not thaw");
        // It then times out (no current event arrives).
        assert!(!latch.await_thaw());
    }
}
