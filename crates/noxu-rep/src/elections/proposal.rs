//! Election proposal.
//!
//! Port of `com.sleepycat.je.rep.elections.Proposer.Proposal` and
//! `com.sleepycat.je.rep.elections.TimebasedProposalGenerator`  -  represents a
//! candidate's bid to become master and defines the total ordering used to
//! decide which candidate wins.
//!
//! ## Ordering rules
//!
//! Proposals are compared in the following order (each tiebreaker is consulted
//! only when the previous field is equal):
//!
//! 1. **Higher VLSN wins**  -  the node with the most up-to-date data is
//!    preferred so that no committed transactions are lost.
//! 2. **Higher priority wins**  -  allows operators to steer mastership towards
//!    specific nodes (e.g. nodes with faster storage).
//! 3. **Higher term wins**  -  more recent elections take precedence.
//! 4. **Lexicographic node name**  -  deterministic tiebreaker when all else is
//!    equal.

use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

/// Represents a candidate's election proposal.
///
/// A proposal captures the state of the candidate at the moment it decides to
/// run for master. The [`Ord`] implementation encodes the election's preference
/// rules so that `max(proposals)` yields the winning candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    /// Name of the candidate node.
    pub node_name: String,
    /// Highest VLSN this node has acknowledged.
    pub vlsn: u64,
    /// Election priority assigned to this node (higher = preferred).
    pub priority: u32,
    /// Election term number.
    pub term: u64,
    /// Millisecond timestamp of when the proposal was created.
    pub timestamp_ms: u64,
}

impl Proposal {
    /// Create a new proposal, automatically timestamped to "now".
    pub fn new(node_name: String, vlsn: u64, priority: u32, term: u64) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self { node_name, vlsn, priority, term, timestamp_ms }
    }

    /// Create a proposal with an explicit timestamp (useful for tests and
    /// deserialization).
    pub fn with_timestamp(
        node_name: String,
        vlsn: u64,
        priority: u32,
        term: u64,
        timestamp_ms: u64,
    ) -> Self {
        Self { node_name, vlsn, priority, term, timestamp_ms }
    }

    /// Returns `true` if this proposal is strictly better than `other`
    /// according to the election ordering rules.
    pub fn is_better_than(&self, other: &Proposal) -> bool {
        self.cmp(other) == Ordering::Greater
    }
}

impl Ord for Proposal {
    fn cmp(&self, other: &Self) -> Ordering {
        // 1. Higher VLSN wins.
        self.vlsn
            .cmp(&other.vlsn)
            // 2. Higher priority wins.
            .then_with(|| self.priority.cmp(&other.priority))
            // 3. Higher term wins.
            .then_with(|| self.term.cmp(&other.term))
            // 4. Lexicographic node name tiebreaker.
            .then_with(|| self.node_name.cmp(&other.node_name))
    }
}

impl PartialOrd for Proposal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for Proposal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Proposal(node={}, vlsn={}, priority={}, term={}, ts={})",
            self.node_name,
            self.vlsn,
            self.priority,
            self.term,
            self.timestamp_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_sets_timestamp() {
        let p = Proposal::new("node1".into(), 100, 1, 1);
        assert!(p.timestamp_ms > 0);
    }

    #[test]
    fn test_with_timestamp() {
        let p = Proposal::with_timestamp("n".into(), 1, 1, 1, 42);
        assert_eq!(p.timestamp_ms, 42);
    }

    // --- VLSN ordering ---

    #[test]
    fn test_higher_vlsn_wins() {
        let a = Proposal::with_timestamp("node1".into(), 200, 1, 1, 0);
        let b = Proposal::with_timestamp("node2".into(), 100, 1, 1, 0);
        assert!(a.is_better_than(&b));
        assert!(!b.is_better_than(&a));
    }

    #[test]
    fn test_higher_vlsn_wins_regardless_of_priority() {
        let a = Proposal::with_timestamp("node1".into(), 200, 1, 1, 0);
        let b = Proposal::with_timestamp("node2".into(), 100, 999, 1, 0);
        assert!(a.is_better_than(&b));
    }

    // --- Priority ordering (same VLSN) ---

    #[test]
    fn test_higher_priority_wins_same_vlsn() {
        let a = Proposal::with_timestamp("node1".into(), 100, 10, 1, 0);
        let b = Proposal::with_timestamp("node2".into(), 100, 5, 1, 0);
        assert!(a.is_better_than(&b));
        assert!(!b.is_better_than(&a));
    }

    // --- Term ordering (same VLSN, same priority) ---

    #[test]
    fn test_higher_term_wins_same_vlsn_priority() {
        let a = Proposal::with_timestamp("node1".into(), 100, 5, 3, 0);
        let b = Proposal::with_timestamp("node2".into(), 100, 5, 1, 0);
        assert!(a.is_better_than(&b));
        assert!(!b.is_better_than(&a));
    }

    // --- Name tiebreaker ---

    #[test]
    fn test_name_tiebreaker() {
        let a = Proposal::with_timestamp("node_b".into(), 100, 5, 1, 0);
        let b = Proposal::with_timestamp("node_a".into(), 100, 5, 1, 0);
        // "node_b" > "node_a" lexicographically.
        assert!(a.is_better_than(&b));
        assert!(!b.is_better_than(&a));
    }

    #[test]
    fn test_equal_proposals() {
        let a = Proposal::with_timestamp("node1".into(), 100, 5, 1, 0);
        let b = Proposal::with_timestamp("node1".into(), 100, 5, 1, 0);
        assert!(!a.is_better_than(&b));
        assert!(!b.is_better_than(&a));
        assert_eq!(a, b);
    }

    // --- Ord / sorting ---

    #[test]
    fn test_sort_picks_best_proposal() {
        let proposals = [Proposal::with_timestamp("low".into(), 50, 1, 1, 0),
            Proposal::with_timestamp("high_vlsn".into(), 200, 1, 1, 0),
            Proposal::with_timestamp("high_prio".into(), 100, 99, 1, 0)];
        let best = proposals.iter().max().unwrap();
        assert_eq!(best.node_name, "high_vlsn");
    }

    #[test]
    fn test_sort_tiebreaker_chain() {
        let mut proposals = [Proposal::with_timestamp("c".into(), 100, 5, 1, 0),
            Proposal::with_timestamp("a".into(), 100, 5, 1, 0),
            Proposal::with_timestamp("b".into(), 100, 5, 1, 0)];
        proposals.sort();
        // Ascending: a < b < c
        assert_eq!(proposals[0].node_name, "a");
        assert_eq!(proposals[1].node_name, "b");
        assert_eq!(proposals[2].node_name, "c");
    }

    #[test]
    fn test_display() {
        let p = Proposal::with_timestamp("n1".into(), 42, 3, 7, 1000);
        let s = format!("{}", p);
        assert!(s.contains("n1"));
        assert!(s.contains("42"));
        assert!(s.contains("term=7"));
    }

    #[test]
    fn test_is_better_than_symmetry() {
        let a = Proposal::with_timestamp("x".into(), 10, 1, 1, 0);
        let b = Proposal::with_timestamp("y".into(), 20, 1, 1, 0);
        // Exactly one of the two is "better".
        assert!(b.is_better_than(&a));
        assert!(!a.is_better_than(&b));
    }

    #[test]
    fn test_zero_priority_loses() {
        let zero = Proposal::with_timestamp("node1".into(), 100, 0, 1, 0);
        let one = Proposal::with_timestamp("node2".into(), 100, 1, 1, 0);
        assert!(one.is_better_than(&zero));
    }
}
