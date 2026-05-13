//! Quorum policy for Flexible Paxos (Howard 2019).
//!
//! Classic Paxos uses the same simple-majority quorum for both Phase 1
//! (Prepare / Promise) and Phase 2 (Accept / Commit).  Flexible Paxos
//! (FPaxos) relaxes this: the two phases may use *different* quorum systems
//! as long as **every Phase 1 quorum intersects every Phase 2 quorum**
//! (Howard, UCAM-CL-TR-935, Theorem 1).
//!
//! This module provides [`QuorumPolicy`], which wraps three strategies:
//!
//! - [`QuorumPolicy::SimpleMajority`] — classic `(n/2)+1`, matches JE and
//!   the previous hard-coded behaviour.
//! - [`QuorumPolicy::Flexible`] — operator-chosen `phase1` / `phase2` sizes
//!   with a built-in safety check (`phase1 + phase2 > n`).
//! - [`QuorumPolicy::Expression`] — a full [`quoracle::QuorumSystem`] built
//!   from AND / OR / Choose expressions; the intersection property is
//!   validated by the quoracle library at construction time.

use hashbrown::HashSet;

use quoracle::{Expr, Node, QuorumSystem, choose, majority};

// ---------------------------------------------------------------------------
// QuorumPolicy
// ---------------------------------------------------------------------------

/// Controls how Phase 1 and Phase 2 election quorums are selected.
#[derive(Debug, Clone, Default)]
pub enum QuorumPolicy {
    /// Classic simple majority — `(n/2)+1` for both phases.  Default; matches
    /// JE's `RepGroup.quorumSize()`.
    #[default]
    SimpleMajority,

    /// Flexible Paxos (Howard 2019): distinct sizes for the two phases.
    ///
    /// Safety invariant (enforced by [`QuorumPolicy::validate`]):
    /// `phase1 + phase2 > n` (total electable nodes), which guarantees
    /// every Phase 1 quorum intersects every Phase 2 quorum.
    ///
    /// Example for a 5-node cluster:
    /// - `phase1 = 4, phase2 = 2` (4+2=6 > 5) → fast commits (2 ACKs),
    ///   safe elections (4/5 must agree).
    /// - `phase1 = 3, phase2 = 3` → classic majority.
    Flexible { phase1: usize, phase2: usize },

    /// Custom quoracle expression.
    ///
    /// `reads` is the Phase 1 quorum expression; `writes` is the Phase 2
    /// quorum expression.  [`QuorumSystem::new`] validates the intersection
    /// property at construction time and returns an error for invalid systems.
    ///
    /// Use [`QuorumPolicy::build_expression`] as a convenience constructor.
    Expression(QuorumSystem<String>),
}

impl QuorumPolicy {
    // -----------------------------------------------------------------------
    // Quorum size queries
    // -----------------------------------------------------------------------

    /// Returns the minimum Phase 1 (Prepare/Promise) quorum size.
    ///
    /// For `Expression` policies this is the size of the *smallest* read
    /// quorum in the system (worst case: any read quorum is acceptable).
    pub fn phase1_quorum(&self, electable: usize) -> usize {
        match self {
            Self::SimpleMajority => majority_size(electable),
            Self::Flexible { phase1, .. } => *phase1,
            Self::Expression(qs) => {
                qs.read_quorums()
                    .map(|q| q.len())
                    .min()
                    .unwrap_or(electable)
            }
        }
    }

    /// Returns the minimum Phase 2 (Accept/Commit) quorum size.
    pub fn phase2_quorum(&self, electable: usize) -> usize {
        match self {
            Self::SimpleMajority => majority_size(electable),
            Self::Flexible { phase2, .. } => *phase2,
            Self::Expression(qs) => {
                qs.write_quorums()
                    .map(|q| q.len())
                    .min()
                    .unwrap_or(electable)
            }
        }
    }

    /// Returns `true` if `voters` (a set of node names) satisfies the Phase 2
    /// quorum requirement given `electable` total electable nodes.
    pub fn is_valid_phase2_quorum(
        &self,
        voters: &HashSet<&str>,
        electable: usize,
    ) -> bool {
        voters.len() >= self.phase2_quorum(electable)
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    /// Validate safety: every Phase 1 quorum must intersect every Phase 2
    /// quorum.  For `SimpleMajority` and `Expression` this is always true by
    /// construction; for `Flexible` we check `phase1 + phase2 > n`.
    pub fn validate(&self, n: usize) -> Result<(), String> {
        match self {
            Self::SimpleMajority => Ok(()),
            Self::Flexible { phase1, phase2 } => {
                if phase1 + phase2 > n {
                    Ok(())
                } else {
                    Err(format!(
                        "Flexible quorum unsafe: phase1({phase1}) + phase2({phase2}) \
                         = {} which is NOT > n={n}. \
                         Safety requires phase1 + phase2 > n.",
                        phase1 + phase2
                    ))
                }
            }
            // QuorumSystem::new() already validated intersection.
            Self::Expression(_) => Ok(()),
        }
    }

    // -----------------------------------------------------------------------
    // Construction helpers
    // -----------------------------------------------------------------------

    /// Build a `QuorumPolicy::Expression` from a list of node names and
    /// desired phase sizes using quoracle's `choose(k, nodes)` combinator.
    ///
    /// Phase 1 expression = `choose(phase1_k, node_names)` (read quorum).
    /// Phase 2 expression = `choose(phase2_k, node_names)` (write quorum).
    ///
    /// Returns `Err` if the intersection property is violated or if the
    /// sizes are out of range.
    pub fn build_expression(
        node_names: &[String],
        phase1_k: usize,
        phase2_k: usize,
    ) -> Result<Self, quoracle::Error> {
        let nodes: Vec<Expr<String>> = node_names
            .iter()
            .map(|n| Expr::Node(Node::new(n.clone())))
            .collect();

        let reads = choose(phase1_k, nodes.clone())?;
        let writes = choose(phase2_k, nodes)?;
        let qs = QuorumSystem::new(reads, writes)?;
        Ok(Self::Expression(qs))
    }

    /// Build a majority-quorum `QuorumPolicy::Expression` (both phases use
    /// `majority(node_names)`).  Useful for testing quoracle integration
    /// without changing election behaviour.
    pub fn build_majority_expression(
        node_names: &[String],
    ) -> Result<Self, quoracle::Error> {
        let nodes: Vec<Expr<String>> = node_names
            .iter()
            .map(|n| Expr::Node(Node::new(n.clone())))
            .collect();
        let reads = majority(nodes)?;
        let qs = QuorumSystem::from_reads(reads);
        Ok(Self::Expression(qs))
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// `(n / 2) + 1` — classic simple majority.
pub(crate) fn majority_size(n: usize) -> usize {
    if n == 0 { 0 } else { (n / 2) + 1 }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Simple majority ---

    #[test]
    fn test_simple_majority_sizes() {
        let p = QuorumPolicy::SimpleMajority;
        assert_eq!(p.phase1_quorum(3), 2);
        assert_eq!(p.phase2_quorum(3), 2);
        assert_eq!(p.phase1_quorum(5), 3);
        assert_eq!(p.phase2_quorum(5), 3);
        assert_eq!(p.phase1_quorum(7), 4);
        assert_eq!(p.phase2_quorum(7), 4);
    }

    #[test]
    fn test_simple_majority_zero_nodes() {
        let p = QuorumPolicy::SimpleMajority;
        assert_eq!(p.phase1_quorum(0), 0);
        assert_eq!(p.phase2_quorum(0), 0);
    }

    #[test]
    fn test_simple_majority_validates() {
        assert!(QuorumPolicy::SimpleMajority.validate(5).is_ok());
    }

    // --- Flexible ---

    #[test]
    fn test_flexible_5node_phase1_4_phase2_2() {
        let p = QuorumPolicy::Flexible { phase1: 4, phase2: 2 };
        assert_eq!(p.phase1_quorum(5), 4);
        assert_eq!(p.phase2_quorum(5), 2);
        assert!(p.validate(5).is_ok(), "4+2=6 > 5 should be safe");
    }

    #[test]
    fn test_flexible_invalid_rejected() {
        // phase1=1, phase2=1, n=3 → 1+1=2, NOT > 3
        let p = QuorumPolicy::Flexible { phase1: 1, phase2: 1 };
        assert!(p.validate(3).is_err());
    }

    #[test]
    fn test_flexible_boundary_equal_rejected() {
        // phase1+phase2 == n is NOT safe (need strictly greater)
        let p = QuorumPolicy::Flexible { phase1: 2, phase2: 3 };
        assert!(p.validate(5).is_err(), "2+3=5 == 5, not strictly greater");
    }

    #[test]
    fn test_flexible_classic_majority_is_valid() {
        let p = QuorumPolicy::Flexible { phase1: 3, phase2: 3 };
        assert!(p.validate(5).is_ok(), "3+3=6 > 5");
    }

    // --- Expression via quoracle ---

    #[test]
    fn test_build_expression_choose_quorum() {
        let names: Vec<String> = (0..5).map(|i| format!("node{i}")).collect();
        // phase1=4, phase2=2 → 4+2=6 > 5, intersection property satisfied
        let policy = QuorumPolicy::build_expression(&names, 4, 2)
            .expect("4-of-5 reads and 2-of-5 writes must intersect");
        assert_eq!(policy.phase1_quorum(5), 4);
        assert_eq!(policy.phase2_quorum(5), 2);
        assert!(policy.validate(5).is_ok());
    }

    #[test]
    fn test_build_expression_non_intersecting_rejected() {
        let names: Vec<String> = (0..3).map(|i| format!("node{i}")).collect();
        // choose(1, 3 nodes) reads, choose(1, 3 nodes) writes — 1+1=2 ≤ 3
        // quoracle must reject this as the intersection property fails
        let result = QuorumPolicy::build_expression(&names, 1, 1);
        assert!(
            result.is_err(),
            "choose(1) reads and choose(1) writes over 3 nodes may not intersect"
        );
    }

    #[test]
    fn test_build_majority_expression() {
        let names: Vec<String> = (0..5).map(|i| format!("node{i}")).collect();
        let policy = QuorumPolicy::build_majority_expression(&names)
            .expect("majority expression should always be valid");
        // Majority of 5 = 3
        assert_eq!(policy.phase1_quorum(5), 3);
        assert_eq!(policy.phase2_quorum(5), 3);
    }

    // --- is_valid_phase2_quorum ---

    #[test]
    fn test_is_valid_phase2_quorum_simple() {
        let p = QuorumPolicy::SimpleMajority;
        let three: HashSet<&str> = ["a", "b", "c"].iter().copied().collect();
        let two: HashSet<&str> = ["a", "b"].iter().copied().collect();
        let one: HashSet<&str> = ["a"].iter().copied().collect();
        assert!(p.is_valid_phase2_quorum(&three, 5));
        assert!(p.is_valid_phase2_quorum(&two, 3)); // 2 >= majority(3)=2
        assert!(!p.is_valid_phase2_quorum(&one, 3));
    }
}
