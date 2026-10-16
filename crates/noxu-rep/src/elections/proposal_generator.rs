//! Time-based election proposal-number generator.
//!
//! Faithful port of JE
//! `com.sleepycat.je.rep.elections.TimebasedProposalGenerator`.
//!
//! # Why a time-based number (FID-ELECT-1)
//!
//! JE's Paxos acceptor is safe because every proposal carries a **globally
//! unique, ascending** proposal number.  JE forms it as
//!
//! ```text
//!   ms_time (8 bytes) | machineId (16 bytes) | locallyUniqueId (4 bytes)
//! ```
//!
//! The `ms_time` supplies the *increasing* component; `machineId` (the host
//! IP XORed with a `SecureRandom`) makes it unique across machines; the
//! `locallyUniqueId` disambiguates multiple generators in one process.  The
//! acceptor compares the whole number
//! (`StringProposal.compareTo`), so a later proposal from any proposer is
//! strictly greater and an acceptor that promised one proposal rejects every
//! lower-numbered one.
//!
//! Noxu's prior election "proposal number" was a per-node-local `term: u64`
//! counter that every node's driver started at `1`.  That is **not** globally
//! unique or ascending across nodes: every node reused the small integers, so
//! two proposers (or one proposer across rounds) could collide at the same
//! `term` and both be accepted — split-brain (FID-ELECT-1).
//!
//! # Noxu's faithful translation
//!
//! This generator supplies the **ascending, globally-ordered** component of
//! Noxu's proposal number — the analogue of JE's `ms_time` — as a `u64`
//! carried in the existing `term` wire field.  The **uniqueness**
//! (disambiguation of two proposers that land in the same millisecond) is
//! provided by the proposer's `node_name`, which is already carried in every
//! `ElectionProposal` and is unique per replication group — exactly the role
//! JE's `machineId` plays.  The acceptor keys its promise/accept on the whole
//! `(term, node_name)` pair (see `acceptor_state::ProposalNumber`), so the
//! Noxu proposal number is `(ms_time, node_name)`: globally unique and
//! ascending, with no new wire field or persisted-record layout change.
//!
//! The JE `machineId` (random per-JVM) and `locallyUniqueId` (per-generator)
//! segments collapse into `node_name` because Noxu already transports the
//! proposer identity in the proposal; a separate random machine id would add
//! nothing (names are already unique) and would require a breaking wire
//! change, which a patch release must not make.
//!
//! # Clock behaviour (JE `nextProposal`)
//!
//! JE guards against a stalled or **backwards** wall clock and against two
//! proposals in the same millisecond with a local monotonic counter:
//!
//! ```java
//! long proposalTime = System.currentTimeMillis();
//! synchronized (this) {
//!     if (proposalTime <= prevProposalTime) {
//!         proposalTime = ++prevProposalTime;   // never regress THIS node
//!     }
//!     prevProposalTime = proposalTime;
//! }
//! ```
//!
//! We port this exactly: `next()` returns `max(now_ms, prev + 1)` and records
//! it.  So this node's own proposal numbers never regress even if the wall
//! clock steps backward (an NTP correction) or two rounds fall in the same
//! millisecond.  `prev` is in-memory (reset on process restart), matching JE's
//! instance-field `prevProposalTime` — this is the documented residual JE
//! behaviour: a node whose wall clock is far **behind** the group cannot win
//! an election until its own `prev` (seeded from wall-clock on construction)
//! catches up to the terms it sees from other nodes.  That is safe (it only
//! delays that node from winning, never admits two masters) and is identical
//! to JE.

use noxu_util::dst_sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
        as u64
}

/// Generates an ascending sequence of election proposal numbers (the `term`
/// component), monotone even across a backwards clock step.
///
/// JE `TimebasedProposalGenerator`.
pub(crate) struct TimebasedProposalGenerator {
    /// The time (ms) used for the previous proposal, preventing duplicate or
    /// regressing proposals.  JE `prevProposalTime` — an in-memory instance
    /// field seeded from the wall clock at construction.
    prev: Mutex<u64>,
}

impl TimebasedProposalGenerator {
    /// Create a generator seeded from the current wall clock (JE seeds
    /// `prevProposalTime = System.currentTimeMillis()` at construction).
    pub(crate) fn new() -> Self {
        Self { prev: Mutex::new(now_ms()) }
    }

    /// Create a generator seeded from an explicit value (tests only).
    #[cfg(test)]
    pub(crate) fn with_prev(prev: u64) -> Self {
        Self { prev: Mutex::new(prev) }
    }

    /// Returns the next proposal-number component, strictly greater than every
    /// previous one returned by this generator.  JE `nextProposal`.
    ///
    /// `max(now_ms, prev + 1)`: if the wall clock has not advanced past the
    /// previous proposal (a stalled or backwards clock, or two calls in one
    /// millisecond), fall back to `prev + 1` so this node's proposals never
    /// regress.
    pub(crate) fn next(&self) -> u64 {
        let mut prev = self.prev.lock().unwrap();
        let now = now_ms();
        let t = if now <= *prev { prev.saturating_add(1) } else { now };
        *prev = t;
        t
    }

    /// Advance the generator so the next proposal is strictly greater than
    /// `observed` — used when this node sees a higher term from a peer, so its
    /// own next round out-numbers what it has already seen (keeps the group's
    /// proposal numbers globally ascending without a shared clock).  JE gets
    /// this for free from wall-clock time; Noxu nudges `prev` because the
    /// `term` component is the only ordering key on the wire.
    pub(crate) fn observe(&self, observed: u64) {
        let mut prev = self.prev.lock().unwrap();
        if observed > *prev {
            *prev = observed;
        }
    }
}

impl Default for TimebasedProposalGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_is_strictly_increasing() {
        let g = TimebasedProposalGenerator::new();
        let a = g.next();
        let b = g.next();
        let c = g.next();
        assert!(a < b, "{a} < {b}");
        assert!(b < c, "{b} < {c}");
    }

    #[test]
    fn next_never_regresses_on_backwards_clock() {
        // Seed `prev` far in the FUTURE: the real wall clock is "behind" it,
        // so every `next()` must fall back to prev+1 and keep ascending
        // (JE: a node whose clock is behind still produces ascending numbers,
        // it just can't out-number the group until it catches up).
        let future = now_ms() + 1_000_000;
        let g = TimebasedProposalGenerator::with_prev(future);
        let a = g.next();
        let b = g.next();
        assert_eq!(a, future + 1);
        assert_eq!(b, future + 2);
        assert!(a < b);
    }

    #[test]
    fn same_ms_calls_still_ascend() {
        // Two calls that land in the same millisecond: the monotone guard
        // bumps by 1 so they never collide.
        let g = TimebasedProposalGenerator::with_prev(now_ms());
        let mut last = 0;
        for _ in 0..1000 {
            let t = g.next();
            assert!(
                t > last,
                "proposal numbers must strictly ascend: {t} > {last}"
            );
            last = t;
        }
    }

    #[test]
    fn observe_advances_prev() {
        let g = TimebasedProposalGenerator::with_prev(100);
        g.observe(500);
        let t = g.next();
        assert!(t > 500, "after observing 500, next must exceed it: {t}");
        // Observing a lower value is a no-op.
        g.observe(10);
        let t2 = g.next();
        assert!(t2 > t);
    }
}
