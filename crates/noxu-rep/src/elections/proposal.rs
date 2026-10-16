//! Election proposal.
//!
//! Rep.elections.TimebasedProposalGenerator`  -  represents a
//! candidate's bid to become master and defines the total ordering used to
//! decide which candidate wins.
//!
//! ## Ordering rules (JE Ranking, major=DTVLSN, minor=VLSN)
//!
//! 0. **Higher DTVLSN wins** - the node with the most *durable* transactions
//!    (replicated to a majority) is preferred over one with a higher raw VLSN
//!    but uncommitted tail. DTVLSN 0 = UNINITIALIZED -> falls back to VLSN.
//!
//! Proposals are compared in the following order (each tiebreaker is consulted
//! only when the previous field is equal):
//!
//! 1. **Higher VLSN wins**  -  the node with the most up-to-date data is
//!    preferred so that no committed transactions are lost.
//! 2. **Higher priority wins**  -  allows operators to steer mastership towards
//!    specific nodes (e.g. nodes with faster storage).
//! 3. **Lexicographic node name**  -  deterministic tiebreaker when all else is
//!    equal (JE's socket-address tiebreak role).
//!
//! There is deliberately **no term / proposal-number component** in this
//! VALUE ranking: JE's `Ranking.compareTo` is only `(dtvlsn, vlsn)` and the
//! Paxos round identity is a separate concern (the `(term, proposer)` promise
//! key handled by the acceptor).  The round a candidate runs in must never
//! change which node is the best master.

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
    /// Durable Transaction VLSN (the MAJOR ranking key, JE Ranking.major).
    /// The highest VLSN known durable on a majority of nodes. 0 means
    /// UNINITIALIZED (pre-DTVLSN), in which case ranking falls back to `vlsn`
    /// (JE MasterSuggestionGenerator.getRanking: `if dtvlsn == UNINITIALIZED
    /// return Ranking(vlsn, 0)`). Defaults to 0 for back-compat constructors.
    pub dtvlsn: u64,
    /// Highest VLSN this node has acknowledged (the MINOR ranking key).
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

        Self { node_name, dtvlsn: 0, vlsn, priority, term, timestamp_ms }
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
        Self { node_name, dtvlsn: 0, vlsn, priority, term, timestamp_ms }
    }

    /// Returns `true` if this proposal is strictly better than `other`
    /// according to the election ordering rules.
    pub fn is_better_than(&self, other: &Proposal) -> bool {
        self.cmp(other) == Ordering::Greater
    }

    /// Builder: set the DTVLSN (the major ranking key). The election driver
    /// populates this from `ReplicatedEnvironment::get_dtvlsn()` so the most
    /// durable node, not merely the highest-raw-VLSN node, wins (D2).
    pub fn with_dtvlsn(mut self, dtvlsn: u64) -> Self {
        self.dtvlsn = dtvlsn;
        self
    }
}

impl Ord for Proposal {
    fn cmp(&self, other: &Self) -> Ordering {
        // JE election VALUE ranking (`Acceptor.SuggestionGenerator.Ranking`):
        // major = dtvlsn, minor = vlsn (`Ranking.compareTo`, Acceptor.java:
        // 305-311; value built by MasterSuggestionGenerator.getRanking =
        // `new Ranking(dtvlsn, vlsn)`, MasterSuggestionGenerator.java:81),
        // then the `RankingProposer.choosePhase2Value` priority tiebreak and
        // finally the socket-address tiebreak (RankingProposer.java:116-123,
        // "Use socket address to choose in case of a tie, so we always have a
        // consistent ordering").  Noxu uses the node name — a stable
        // per-group-unique identity — in the socket-address role.
        //
        // JE's ranking has NO term component: the election ROUND identity is
        // the separate Paxos `(term, proposer)` promise key, which must never
        // bias WHICH node is the best master.  (FID-ELECT-1: this ordering
        // previously carried a `term` tiebreak, a Noxu invention that let a
        // proposer's own live round term out-rank a peer's equally-good
        // suggestion carrying a stale acceptor term — so two tied candidates
        // each picked THEMSELVES, a value-selection duel; removed to match JE.)
        //
        // 1. Higher DTVLSN wins (the most-durable node).  When both DTVLSNs
        //    are 0 (UNINITIALIZED / pre-DTVLSN), this is a tie and the
        //    comparison falls through to VLSN — exactly JE getRanking's
        //    pre-DTVLSN fallback `Ranking(vlsn, 0)`.
        self.dtvlsn
            .cmp(&other.dtvlsn)
            // 2. Higher VLSN wins.
            .then_with(|| self.vlsn.cmp(&other.vlsn))
            // 3. Higher priority wins.
            .then_with(|| self.priority.cmp(&other.priority))
            // 4. Lexicographic node name tiebreaker (JE socket-address role).
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
    fn test_higher_dtvlsn_wins_over_higher_vlsn() {
        // D2: a node with a higher DTVLSN (more durable txns) beats a node
        // with a higher raw VLSN but uncommitted tail. JE Ranking(major=dtvlsn).
        let durable = Proposal::with_timestamp("durable".into(), 100, 1, 1, 0)
            .with_dtvlsn(90);
        let laggard_tail =
            Proposal::with_timestamp("laggard".into(), 200, 1, 1, 0)
                .with_dtvlsn(50);
        assert!(
            durable.is_better_than(&laggard_tail),
            "higher DTVLSN must win over higher raw VLSN"
        );
        assert!(!laggard_tail.is_better_than(&durable));
    }

    #[test]
    fn test_dtvlsn_tie_falls_back_to_vlsn() {
        // Equal (or both-zero/UNINITIALIZED) DTVLSN -> compare by VLSN, the
        // JE pre-DTVLSN fallback Ranking(vlsn, 0).
        let a =
            Proposal::with_timestamp("a".into(), 200, 1, 1, 0).with_dtvlsn(0);
        let b =
            Proposal::with_timestamp("b".into(), 100, 1, 1, 0).with_dtvlsn(0);
        assert!(a.is_better_than(&b), "dtvlsn tie -> higher vlsn wins");
        // Same with equal non-zero dtvlsn.
        let c =
            Proposal::with_timestamp("c".into(), 200, 1, 1, 0).with_dtvlsn(50);
        let d =
            Proposal::with_timestamp("d".into(), 100, 1, 1, 0).with_dtvlsn(50);
        assert!(c.is_better_than(&d));
    }

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

    // --- Term is NOT a ranking key (JE Ranking has no term) ---

    #[test]
    fn test_term_is_not_a_ranking_key() {
        // JE's election VALUE ranking is (dtvlsn, vlsn, priority, name) — the
        // Paxos round term/number must NOT bias which node is the best master.
        // Same vlsn+priority but different term: the term is ignored and the
        // node NAME breaks the tie (node2 > node1), regardless of term.  Here
        // node1 carries the HIGHER term (3) yet still loses to node2 (term 1).
        let a = Proposal::with_timestamp("node1".into(), 100, 5, 3, 0);
        let b = Proposal::with_timestamp("node2".into(), 100, 5, 1, 0);
        assert!(
            b.is_better_than(&a),
            "term must be ignored; name decides (node2 > node1)"
        );
        assert!(!a.is_better_than(&b));
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
        let proposals = [
            Proposal::with_timestamp("low".into(), 50, 1, 1, 0),
            Proposal::with_timestamp("high_vlsn".into(), 200, 1, 1, 0),
            Proposal::with_timestamp("high_prio".into(), 100, 99, 1, 0),
        ];
        let best = proposals.iter().max().unwrap();
        assert_eq!(best.node_name, "high_vlsn");
    }

    #[test]
    fn test_sort_tiebreaker_chain() {
        let mut proposals = [
            Proposal::with_timestamp("c".into(), 100, 5, 1, 0),
            Proposal::with_timestamp("a".into(), 100, 5, 1, 0),
            Proposal::with_timestamp("b".into(), 100, 5, 1, 0),
        ];
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
