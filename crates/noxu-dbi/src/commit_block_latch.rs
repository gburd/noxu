//! Transfer-scoped commit block latch (JE `MasterTransfer` phase 2).
//!
//! During a master transfer, JE `MasterTransfer` runs in two phases
//! (`MasterTransfer.java`, phase-1/phase-2 comments ~220-226, and the
//! `VLSNProgress` / `readyReplicas` completion logic ~254-283):
//!
//!   * **Phase 1** -- the candidate replica catches up to the master's
//!     *current* end-of-log; the master keeps accepting writes.
//!   * **Phase 2** -- the master **blocks new commit/abort operations** so the
//!     final VLSN gap cannot re-open while the winner is announced, then hands
//!     off. Only commits that have already acquired a VLSN can complete.
//!
//! Noxu's `transfer_master` originally implemented phase 1 only (against a
//! one-time VLSN snapshot). This latch is the missing phase 2: while it is
//! *blocked*, any thread that is about to assign a **new** commit VLSN
//! (`EnvironmentImpl::log_txn_commit`, and the replication layer's
//! `replicate_entry`) parks here first, so the master's observable tail (the
//! highest assigned VLSN) is frozen for the final catch-up re-confirm +
//! hand-off window. Commits that already hold a VLSN are unaffected.
//!
//! Like JE's `CommitFreezeLatch`, the block is a bounded "good faith" hold,
//! not an unbounded stall: `await_thaw` waits at most `timeout` and then
//! proceeds regardless, so a `transfer_master` that dies mid-window (or never
//! unblocks) degrades to the pre-block behaviour instead of wedging the commit
//! path forever. The transfer holds the block for only a tiny re-confirm +
//! signal window, so under normal operation the timeout is never reached.

use noxu_sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Default maximum time a committing thread will park on a blocked latch
/// before proceeding anyway (mirrors JE `CommitFreezeLatch`'s bounded hold).
const DEFAULT_BLOCK_TIMEOUT: Duration = Duration::from_millis(5000);

struct BlockState {
    /// True while new-commit VLSN assignment is blocked.
    blocked: bool,
    /// When the current block expires (bounded liveness backstop).
    block_end: Option<Instant>,
}

/// A transfer-scoped commit block. Shared (`Arc`) between the replication
/// layer (which engages / lifts it around a master transfer's final hand-off
/// window) and the commit path (which parks on it before assigning a new
/// commit VLSN).
pub struct CommitBlockLatch {
    state: Mutex<BlockState>,
    /// Signalled when the block is lifted.
    thaw: Condvar,
    timeout: Duration,
}

impl Default for CommitBlockLatch {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitBlockLatch {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(BlockState { blocked: false, block_end: None }),
            thaw: Condvar::new(),
            timeout: DEFAULT_BLOCK_TIMEOUT,
        }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        let mut l = Self::new();
        l.timeout = timeout;
        l
    }

    /// Engage the commit block (JE `MasterTransfer` phase-2 entry): new commit
    /// VLSN assignments will park in [`Self::await_thaw`] until [`Self::thaw`]
    /// or the bounded timeout. Idempotent -- re-blocking refreshes the
    /// deadline.
    pub fn block(&self) {
        let mut st = self.state.lock();
        st.blocked = true;
        st.block_end = Some(Instant::now() + self.timeout);
    }

    /// Lift the commit block (successful hand-off or abort / timeout of the
    /// transfer): wake every parked committer so they proceed.
    pub fn thaw(&self) {
        let mut st = self.state.lock();
        st.blocked = false;
        st.block_end = None;
        self.thaw.notify_all();
    }

    /// True while the block is engaged.
    pub fn is_blocked(&self) -> bool {
        self.state.lock().blocked
    }

    /// Called on the commit path *before* assigning a new commit VLSN. Parks
    /// while the block is engaged, returning when it is lifted or the bounded
    /// timeout elapses. A no-op (returns immediately) when not blocked.
    pub fn await_thaw(&self) {
        let mut st = self.state.lock();
        while st.blocked {
            let now = Instant::now();
            let end = st.block_end.unwrap_or(now);
            if now >= end {
                // Bounded backstop: proceed even though the block was never
                // explicitly lifted (transfer died mid-window). Degrades to
                // pre-block behaviour rather than wedging the commit path.
                st.blocked = false;
                st.block_end = None;
                return;
            }
            let _ = self.thaw.wait_for(&mut st, end - now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn await_thaw_no_block_returns_immediately() {
        let latch = CommitBlockLatch::new();
        let start = Instant::now();
        latch.await_thaw();
        assert!(start.elapsed() < Duration::from_millis(50));
        assert!(!latch.is_blocked());
    }

    #[test]
    fn block_parks_committer_until_thaw() {
        let latch =
            Arc::new(CommitBlockLatch::with_timeout(Duration::from_secs(5)));
        latch.block();
        assert!(latch.is_blocked());
        let l2 = Arc::clone(&latch);
        let committed_at = Arc::new(Mutex::new(None::<Instant>));
        let ca = Arc::clone(&committed_at);
        let waiter = thread::spawn(move || {
            l2.await_thaw();
            *ca.lock() = Some(Instant::now());
        });
        thread::sleep(Duration::from_millis(50));
        assert!(
            committed_at.lock().is_none(),
            "committer must park while blocked"
        );
        let thawed_at = Instant::now();
        latch.thaw();
        waiter.join().unwrap();
        let unblocked = committed_at.lock().unwrap();
        assert!(
            unblocked >= thawed_at,
            "committer must proceed only after thaw"
        );
        assert!(!latch.is_blocked());
    }

    #[test]
    fn block_times_out_without_thaw() {
        let latch = CommitBlockLatch::with_timeout(Duration::from_millis(40));
        latch.block();
        let start = Instant::now();
        latch.await_thaw(); // must proceed on the bounded backstop
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(30),
            "must wait ~timeout, waited {elapsed:?}"
        );
        assert!(elapsed < Duration::from_secs(2), "must not park forever");
        assert!(!latch.is_blocked());
    }
}
