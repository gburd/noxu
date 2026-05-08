//! Paxos-based master election protocol.
//!
//! Phase 1 + 2 — and
//! Rep.elections.Acceptor`.
//!
//! ## Protocol overview
//!
//! The election uses two-phase Paxos over the [`Channel`] abstraction:
//!
//! **Phase 1 (Prepare / Promise)**
//! The proposer broadcasts an `ElectionProposal` message to all peer channels.
//! Each acceptor responds with an `ElectionVote { granted: true }` (Promise)
//! if it has not already promised a higher-termed proposal, or
//! `ElectionVote { granted: false }` (Reject) otherwise.
//!
//! **Phase 2 (Accept / Accepted)**
//! If a majority promises, the proposer broadcasts an `ElectionResult`
//! announcing the winner. Each peer acknowledges with an `ElectionVote`.
//! If a majority accepts, the function returns `Some(winner_node_id)`.
//!
//! The winner is determined by [`Proposal`] ordering (highest VLSN, then
//! priority, then term, then node name). The proposer collects the best
//! proposal seen in Phase 1 promises and proposes that value in Phase 2,
//! matching approach.
//!
//! ## Acceptor
//!
//! [`run_acceptor`] runs the acceptor side of the protocol. It listens on a
//! channel for `ElectionProposal` / `ElectionResult` messages and responds
//! with votes.

use std::sync::Arc;
use std::time::Duration;

use crate::elections::proposal::Proposal;
use crate::error::{RepError, Result};
use crate::net::channel::Channel;
use crate::protocol::ProtocolMessage;
use crate::rep_group::RepGroup;

// ---------------------------------------------------------------------------
// NodeId type alias (u32 matches RepNode::node_id)
// ---------------------------------------------------------------------------

/// A numeric node identifier as stored in `RepNode::node_id`.
pub type NodeId = u32;

// ---------------------------------------------------------------------------
// run_election
// ---------------------------------------------------------------------------

/// Run a two-phase Paxos election.
///
/// # Arguments
/// * `node_id`       - This node's numeric ID.
/// * `node_name`     - This node's name (used in proposals).
/// * `group`         - The replication group (used to compute quorum).
/// * `channels`      - Open channels to all *peer* nodes (not self).
/// * `proposed_vlsn` - The VLSN this node would bring as master.
/// * `priority`      - This node's election priority.
/// * `term`          - The current election term number.
///
/// # Returns
/// `Some(node_id)` of the elected master (may be a different node if a better
/// candidate was discovered in Phase 1), or `None` if quorum was not reached.
///
/// 
pub fn run_election(
    node_id: NodeId,
    node_name: &str,
    group: &RepGroup,
    channels: &[Arc<dyn Channel>],
    proposed_vlsn: u64,
    priority: u32,
    term: u64,
) -> Option<NodeId> {
    let quorum = group.quorum_size() as usize;
    if quorum == 0 {
        return None;
    }

    // We count ourselves as one vote in both phases.
    let self_needed = if quorum > 0 { 1 } else { 0 };

    // -------------------------------------------------------------------------
    // Phase 1: Prepare / Promise
    // -------------------------------------------------------------------------
    // Build our proposal.
    let our_proposal =
        Proposal::new(node_name.to_string(), proposed_vlsn, priority, term);

    let phase1_msg = ProtocolMessage::ElectionProposal {
        node_name: node_name.to_string(),
        vlsn: proposed_vlsn,
        priority,
        term,
    };

    // Broadcast to all peers.
    let mut promises: Vec<Arc<dyn Channel>> = Vec::new();
    // Track the best proposal seen in promises (for phase 2 value selection).
    let mut best_proposal = our_proposal;

    let phase1_timeout = Duration::from_millis(500);

    for ch in channels {
        if let Ok(()) = send_message(ch.as_ref(), &phase1_msg) {
            match receive_message(ch.as_ref(), phase1_timeout) {
                Ok(Some(ProtocolMessage::ElectionVote {
                    granted: true,
                    ..
                })) => {
                    promises.push(Arc::clone(ch));
                }
                Ok(Some(ProtocolMessage::ElectionProposal {
                    node_name: peer_name,
                    vlsn: peer_vlsn,
                    priority: peer_priority,
                    term: peer_term,
                })) => {
                    // Acceptor returned a counter-proposal (its own state).
                    let peer_p = Proposal::new(
                        peer_name,
                        peer_vlsn,
                        peer_priority,
                        peer_term,
                    );
                    if peer_p.is_better_than(&best_proposal) {
                        best_proposal = peer_p;
                    }
                    // Still counts as a promise.
                    promises.push(Arc::clone(ch));
                }
                _ => {
                    // Rejected or timeout — skip.
                }
            }
        }
    }

    // Count self-vote: we always vote for our own proposal in phase 1.
    let total_promises = promises.len() + self_needed;
    if total_promises < quorum {
        return None;
    }

    // -------------------------------------------------------------------------
    // Phase 2: Accept
    // -------------------------------------------------------------------------
    // We propose the best value seen ("Value" mechanism).
    let winner_name = best_proposal.node_name;
    let accept_msg = ProtocolMessage::ElectionResult {
        master: winner_name.clone(),
        term,
    };

    let mut accepts = 0usize;
    let phase2_timeout = Duration::from_millis(500);

    for ch in &promises {
        if send_message(ch.as_ref(), &accept_msg).is_ok()
            && let Ok(Some(ProtocolMessage::ElectionVote {
                granted: true,
                ..
            })) = receive_message(ch.as_ref(), phase2_timeout)
        {
            accepts += 1;
        }
    }

    // Count self-accept.
    accepts += self_needed;

    if accepts >= quorum {
        // Resolve winner name to node_id by looking up in the group.
        let winner_id = if winner_name == node_name {
            node_id
        } else {
            group
                .get_node(&winner_name)
                .map(|n| n.node_id())
                .unwrap_or(node_id)
        };
        Some(winner_id)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// run_acceptor
// ---------------------------------------------------------------------------

/// Run the acceptor side of the Paxos protocol on the given channel.
///
/// Handles one complete election exchange (Phase 1 + Phase 2) for a single
/// proposer connection, following `Acceptor` class logic:
///
/// **Phase 1**: Receive `ElectionProposal` from proposer.
///   - If the incoming proposal number >= any previously promised proposal:
///     promise by returning own `ElectionProposal` (the suggestion value
///     containing this node's VLSN). This allows the proposer to learn
///     about better candidates.
///   - Otherwise: reject with `ElectionVote { granted: false }`.
///
/// **Phase 2**: Receive `ElectionResult` (the accept request).
///   - If `ElectionResult.term >= promised_term`: grant with
///     `ElectionVote { granted: true }`.
///   - Otherwise: reject.
///
/// Returns `Ok(Some(master_name))` when the acceptor grants phase 2,
/// `Ok(None)` on rejection or timeout, `Err` on protocol errors.
///
/// / `Acceptor::process(Propose)`.
pub fn run_acceptor(
    channel: &dyn Channel,
    node_name: &str,
    own_vlsn: u64,
    own_priority: u32,
    own_term: u64,
) -> Result<Option<String>> {
    let timeout = Duration::from_millis(500);

    let mut promised_term: Option<u64> = None;

    // -------------------------------------------------------------------------
    // Phase 1: receive Propose, send Promise (own suggestion) or Reject.
    // -------------------------------------------------------------------------
    let phase1 = match receive_message(channel, timeout)? {
        Some(m) => m,
        None => return Ok(None),
    };

    match phase1 {
        ProtocolMessage::ElectionProposal {
            node_name: _proposer,
            vlsn: _vlsn,
            priority: _priority,
            term,
        } => {
            // acceptor: reject only if a higher-numbered proposal was
            // already promised. Accept/promise the first proposal regardless
            // of the proposer's VLSN — the VLSN comparison happens at the
            // proposer level when it collects suggestions.
            let should_promise = promised_term
                .is_none_or(|promised| term >= promised);

            if should_promise {
                promised_term = Some(term);
                // Send Promise: return our own proposal as the suggestion
                // value so the proposer can pick the best candidate.
                // This is equivalent to SuggestionGenerator returning
                // the local node's VLSN in the Promise response.
                send_message(
                    channel,
                    &ProtocolMessage::ElectionProposal {
                        node_name: node_name.to_string(),
                        vlsn: own_vlsn,
                        priority: own_priority,
                        term: own_term,
                    },
                )?;
            } else {
                // Reject: a higher proposal was already promised.
                send_message(
                    channel,
                    &ProtocolMessage::ElectionVote {
                        voter: node_name.to_string(),
                        granted: false,
                        term: promised_term.unwrap_or(own_term),
                    },
                )?;
                return Ok(None);
            }
        }
        _ => {
            return Err(RepError::ProtocolError(
                "acceptor: expected ElectionProposal in phase 1".into(),
            ))
        }
    }

    // -------------------------------------------------------------------------
    // Phase 2: receive ElectionResult (Accept), send accept vote.
    // -------------------------------------------------------------------------
    let phase2 = match receive_message(channel, timeout)? {
        Some(m) => m,
        None => return Ok(None),
    };

    match phase2 {
        ProtocolMessage::ElectionResult { master, term } => {
            // Accept if the result term >= what we promised.
            if promised_term.is_some_and(|p| term >= p) {
                send_message(
                    channel,
                    &ProtocolMessage::ElectionVote {
                        voter: node_name.to_string(),
                        granted: true,
                        term,
                    },
                )?;
                Ok(Some(master))
            } else {
                send_message(
                    channel,
                    &ProtocolMessage::ElectionVote {
                        voter: node_name.to_string(),
                        granted: false,
                        term,
                    },
                )?;
                Ok(None)
            }
        }
        _ => Err(RepError::ProtocolError(
            "acceptor: expected ElectionResult in phase 2".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn send_message(ch: &dyn Channel, msg: &ProtocolMessage) -> Result<()> {
    ch.send(&msg.encode())
}

fn receive_message(
    ch: &dyn Channel,
    timeout: Duration,
) -> Result<Option<ProtocolMessage>> {
    match ch.receive(timeout)? {
        Some(bytes) => Ok(Some(ProtocolMessage::decode(&bytes)?)),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::channel::LocalChannelPair;
    use crate::node_type::NodeType;
    use crate::rep_group::RepGroup;
    use crate::rep_node::RepNode;

    fn make_group_3() -> RepGroup {
        let mut g = RepGroup::new("testgroup".into(), 1);
        for i in 1u32..=3 {
            g.add_node(RepNode::new(
                format!("node{}", i),
                NodeType::Electable,
                "127.0.0.1".into(),
                5000 + i as u16,
                i,
            ));
        }
        g
    }

    fn make_group_5() -> RepGroup {
        let mut g = RepGroup::new("testgroup".into(), 1);
        for i in 1u32..=5 {
            g.add_node(RepNode::new(
                format!("node{}", i),
                NodeType::Electable,
                "127.0.0.1".into(),
                5000 + i as u16,
                i,
            ));
        }
        g
    }

    // -----------------------------------------------------------------------
    // Helper: spin up acceptor threads on the B side of LocalChannelPairs.
    // -----------------------------------------------------------------------

    fn spawn_acceptors(
        pairs: Vec<LocalChannelPair>,
        acceptor_name: &str,
        own_vlsn: u64,
        own_priority: u32,
        own_term: u64,
    ) -> (Vec<Arc<dyn Channel>>, Vec<std::thread::JoinHandle<Option<String>>>)
    {
        let mut proposer_channels: Vec<Arc<dyn Channel>> = Vec::new();
        let mut handles = Vec::new();

        for pair in pairs {
            let ch_a: Arc<dyn Channel> = Arc::new(pair.channel_a);
            let ch_b: Arc<dyn Channel> = Arc::new(pair.channel_b);
            proposer_channels.push(ch_a);

            let name = acceptor_name.to_string();
            handles.push(std::thread::spawn(move || {
                run_acceptor(&*ch_b, &name, own_vlsn, own_priority, own_term)
                    .unwrap_or(None)
            }));
        }

        (proposer_channels, handles)
    }

    // -----------------------------------------------------------------------
    // Election majority tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_election_majority_3_node_cluster() {
        // 3-node group: quorum = 2. Proposer counts self as 1, needs 1 more.
        let group = make_group_3();

        // 2 peers.
        let pairs: Vec<LocalChannelPair> =
            (0..2).map(|_| LocalChannelPair::new()).collect();

        let (channels, acceptor_handles) =
            spawn_acceptors(pairs, "node2", 50, 1, 1);

        let winner = run_election(
            1,       // node_id
            "node1", // node_name
            &group,
            &channels,
            100, // vlsn (higher than peers)
            1,   // priority
            1,   // term
        );

        for h in acceptor_handles {
            h.join().unwrap();
        }

        assert!(winner.is_some(), "expected election to succeed");
        assert_eq!(winner.unwrap(), 1, "node1 should win (higher VLSN)");
    }

    #[test]
    fn test_election_majority_5_node_cluster() {
        // 5-node group: quorum = 3. Proposer counts self as 1, needs 2 more.
        let group = make_group_5();

        // 4 peers.
        let pairs: Vec<LocalChannelPair> =
            (0..4).map(|_| LocalChannelPair::new()).collect();

        let (channels, acceptor_handles) =
            spawn_acceptors(pairs, "peerN", 50, 1, 1);

        let winner = run_election(
            1,       // node_id
            "node1", // node_name
            &group,
            &channels,
            200, // vlsn (highest)
            1,   // priority
            1,   // term
        );

        for h in acceptor_handles {
            h.join().unwrap();
        }

        assert!(winner.is_some());
        assert_eq!(winner.unwrap(), 1);
    }

    #[test]
    fn test_election_no_quorum_no_peers() {
        // 3-node group: quorum = 2. No peer channels → self vote = 1 < 2.
        let group = make_group_3();
        let winner = run_election(1, "node1", &group, &[], 100, 1, 1);
        assert!(
            winner.is_none(),
            "should fail: self alone does not reach quorum of 2"
        );
    }

    #[test]
    fn test_election_single_node_group() {
        // 1-node group: quorum = 1. Self vote suffices.
        let mut group = RepGroup::new("g".into(), 1);
        group.add_node(RepNode::new(
            "node1".into(),
            NodeType::Electable,
            "127.0.0.1".into(),
            5001,
            1,
        ));

        let winner =
            run_election(1, "node1", &group, &[], /* no peers */ 100, 1, 1);
        assert_eq!(winner, Some(1));
    }

    #[test]
    fn test_acceptor_returns_own_suggestion_for_better_candidate() {
        // acceptors always promise the first proposal, but return their
        // own VLSN as a suggestion. When the acceptor has a higher VLSN, the
        // proposer should pick the acceptor as the better candidate.
        let pair = LocalChannelPair::new();
        let proposer_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let acceptor_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Acceptor has vlsn=999, proposer has vlsn=10.
        let handle = std::thread::spawn(move || {
            run_acceptor(&*acceptor_ch, "node2", 999, 1, 1).unwrap()
        });

        let mut group = RepGroup::new("g".into(), 1);
        group.add_node(RepNode::new(
            "node1".into(), NodeType::Electable, "127.0.0.1".into(), 5001, 1,
        ));
        group.add_node(RepNode::new(
            "node2".into(), NodeType::Electable, "127.0.0.1".into(), 5002, 2,
        ));

        // Proposer (node1, vlsn=10) sends to one peer.
        // Acceptor promises with suggestion (node2, vlsn=999).
        // Proposer picks node2 as winner. Phase 2 announces node2.
        // Quorum=2: self-vote(1) + peer-accept(1) = 2 >= 2.
        let winner = run_election(1, "node1", &group, &[proposer_ch], 10, 1, 1);

        let accepted = handle.join().unwrap();
        // Acceptor accepted the phase 2 result (node2 announced as master).
        assert!(accepted.is_some());
        assert_eq!(accepted.unwrap(), "node2");
        // Winner should be node2 (higher VLSN candidate).
        assert_eq!(winner, Some(2));
    }

    #[test]
    fn test_election_best_candidate_wins() {
        // node2 has higher VLSN. Both nodes run.  node1 proposes, node2
        // acceptor returns its better proposal. run_election should elect
        // node2 (best_proposal tracking in phase 1).
        let group = {
            let mut g = RepGroup::new("g".into(), 1);
            g.add_node(RepNode::new(
                "node1".into(), NodeType::Electable, "127.0.0.1".into(), 5001, 1,
            ));
            g.add_node(RepNode::new(
                "node2".into(), NodeType::Electable, "127.0.0.1".into(), 5002, 2,
            ));
            g
        };

        let pair = LocalChannelPair::new();
        let proposer_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let acceptor_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // node2 acceptor: own_vlsn=999 (better than node1's 10).
        let handle = std::thread::spawn(move || {
            run_acceptor(&*acceptor_ch, "node2", 999, 1, 1).unwrap()
        });

        // node1 proposes with vlsn=10 — lower than node2.
        // With quorum=2 and self-vote counted, node1 reaches quorum with
        // node2's promise. The elected master is the *best* proposal seen
        // in phase 1 (node2, vlsn=999).
        let winner =
            run_election(1, "node1", &group, &[proposer_ch], 10, 1, 1);

        let accepted = handle.join().unwrap();
        // node2 accepted the phase2 result.
        assert!(accepted.is_some());
        // Winner should be node2 (id=2) since it has higher VLSN.
        assert_eq!(winner, Some(2), "node2 should win with higher VLSN");
    }
}
