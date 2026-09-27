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

use crate::elections::phi_detector::PhiAccrualDetector;
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
    run_election_with_phi(
        node_id,
        node_name,
        group,
        channels,
        proposed_vlsn,
        priority,
        term,
        None,
        Duration::from_millis(500),
    )
}

/// Run a two-phase Paxos election with an optional phi accrual detector for
/// adaptive phase timeouts.
///
/// When a `phi_detector` is provided, the phase timeout is computed as
/// mean + 3*stddev of observed heartbeat inter-arrival times, clamped to
/// [50ms, 5s]. Otherwise, `fallback_timeout` is used.
pub fn run_election_with_phi(
    node_id: NodeId,
    node_name: &str,
    group: &RepGroup,
    channels: &[Arc<dyn Channel>],
    proposed_vlsn: u64,
    priority: u32,
    term: u64,
    phi_detector: Option<&PhiAccrualDetector>,
    fallback_timeout: Duration,
) -> Option<NodeId> {
    run_election_with_phi_dtvlsn(
        node_id,
        node_name,
        group,
        channels,
        proposed_vlsn,
        priority,
        term,
        0,
        phi_detector,
        fallback_timeout,
    )
}

/// A Phase-1 Promise as seen by the proposer's value selection, faithful to
/// JE `RankingProposer.choosePhase2Value`'s inputs.
///
/// JE distinguishes three buckets of promise: zero-priority (never a
/// candidate, counted only for diagnostics), arbiter (ranking remembered but
/// never a candidate value), and ordinary non-arbiter (a ranked candidate).
#[derive(Debug, Clone)]
pub struct PromiseRecord {
    /// The suggested candidate this promise advertises.
    pub proposal: Proposal,
    /// True if the suggesting node cannot be master (Arbiter / Monitor /
    /// Secondary) — JE `isArb` (suggestion NameIdPair == NULL). Such a
    /// promise is never chosen as the value, but its ranking is remembered
    /// for the RF=2 veto below.
    pub is_arbiter: bool,
}

/// Outcome of choosing the Phase-2 value, mirroring JE's tri-state:
/// a value, or `None` for the two distinct JE null cases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase2Value {
    /// A candidate was chosen (JE returns the `MasterValue`).
    Value(Proposal),
    /// No non-zero-priority node responded — JE increments
    /// `phase1NoNonZeroPrio` and returns null.
    NoNonZeroPriority,
    /// The only non-arbiter candidate lags an arbiter's remembered ranking
    /// (RF=2 `[#25311]`) — JE increments `phase1Arbiter` and returns null.
    /// Electing the lagging node would lose durable data the arbiter
    /// witnessed.
    ArbiterVeto,
}

/// Choose the Phase-2 value from a set of Phase-1 Promises, faithful to JE
/// `RankingProposer.choosePhase2Value` (elections/RankingProposer.java:60).
///
/// Rules (JE):
///  * Zero-priority promises are never chosen (they count only toward the
///    "no non-zero-priority node responded" diagnostic).
///  * Arbiter promises are never chosen as the value, but their ranking is
///    remembered for the RF=2 veto.
///  * Among non-arbiter, non-zero-priority promises the highest ranking wins
///    (Proposal `Ord`: dtvlsn, then vlsn, then priority, then term, name — the
///    same total order JE's `Ranking.compareTo` + priority + socket tie-break
///    imposes).
///  * RF=2 veto `[#25311]`: if there is at most one non-arbiter candidate and
///    an arbiter promised with a *strictly higher* ranking than the chosen
///    value, the election yields no value — the sole surviving node has lost
///    durable data the arbiter remembers, so electing it is unsafe. With two
///    or more non-arbiters the arbiter is ignored outright.
///
/// The caller supplies only the promises it accepted; the proposer's own
/// candidacy is passed as one of them.
pub fn choose_phase2_value(promises: &[PromiseRecord]) -> Phase2Value {
    let mut chosen: Option<Proposal> = None;
    let mut zero_prio_nodes = 0usize;
    let mut arb_best: Option<Proposal> = None;
    let mut non_arb_count = 0usize;

    for p in promises {
        // JE: zero-priority promises are skipped entirely.
        if p.proposal.priority == 0 {
            zero_prio_nodes += 1;
            continue;
        }
        if p.is_arbiter {
            // Remember the best arbiter ranking for the RF=2 veto; never a
            // candidate value.
            match arb_best {
                Some(ref cur) if !p.proposal.is_better_than(cur) => {}
                _ => arb_best = Some(p.proposal.clone()),
            }
            continue;
        }
        non_arb_count += 1;
        // Highest ranking wins (Proposal Ord). On an exact tie, keep the first
        // seen — deterministic, matching JE's "consistent ordering" intent.
        match chosen {
            Some(ref cur) if !p.proposal.is_better_than(cur) => {}
            _ => chosen = Some(p.proposal.clone()),
        }
    }

    match chosen {
        None => {
            if zero_prio_nodes > 0 {
                Phase2Value::NoNonZeroPriority
            } else {
                // No candidate at all (e.g. only arbiters). JE returns null;
                // treat as no-non-zero-priority for the caller (no master).
                Phase2Value::NoNonZeroPriority
            }
        }
        Some(value) => {
            // RF=2 arbiter veto [#25311].
            if non_arb_count <= 1
                && let Some(ref arb) = arb_best
                && arb.is_better_than(&value)
            {
                return Phase2Value::ArbiterVeto;
            }
            Phase2Value::Value(value)
        }
    }
}

/// As `run_election_with_phi`, but with the node's own DTVLSN as the major
/// election-ranking key (D2, JE Ranking(major=dtvlsn, minor=vlsn)). Production
/// passes `ReplicatedEnvironment::get_dtvlsn()`; the legacy entry points pass
/// 0 (UNINITIALIZED -> falls back to VLSN ordering, JE pre-DTVLSN behavior).
#[allow(clippy::too_many_arguments)]
pub fn run_election_with_phi_dtvlsn(
    node_id: NodeId,
    node_name: &str,
    group: &RepGroup,
    channels: &[Arc<dyn Channel>],
    proposed_vlsn: u64,
    priority: u32,
    term: u64,
    own_dtvlsn: u64,
    phi_detector: Option<&PhiAccrualDetector>,
    fallback_timeout: Duration,
) -> Option<NodeId> {
    // ---------------------------------------------------------------
    // F22 guard: a node that cannot be master (Arbiter, Monitor,
    // Secondary) must NOT propose itself as master, nor count a
    // counter-proposal from such a node as a candidate value in
    // Phase 2. Otherwise an Arbiter at the highest VLSN can win the
    // election and wedge the cluster, since it cannot serve reads or
    // generate VLSNs (`is_data_node() == false`).
    //
    // See the 2026 review finding F22.
    // ---------------------------------------------------------------
    let our_node_can_be_master = group
        .get_node(node_name)
        .map(|n| n.can_be_master())
        // If the proposer is not yet a known group member, fall back
        // to refusing election — only Electable members start rounds.
        .unwrap_or(false);
    if !our_node_can_be_master {
        log::warn!(
            "election: node {} (non-electable-as-master) refusing to \
             propose; arbiter / monitor / secondary cannot be master",
            node_name
        );
        return None;
    }

    // C5/V14 guard: NODE_PRIORITY == 0 means "electable but never chosen as
    // master" (JE RepParams.NODE_PRIORITY). Such a node still participates in
    // elections as an acceptor (it responds to peers via `run_acceptor` and
    // counts toward a peer's Phase 1 quorum), but it must NOT propose itself
    // as master — even at the highest VLSN. This mirrors the F22 arbiter
    // guard above: not-master-eligible nodes short-circuit before advertising
    // themselves as a candidate value. (A priority-0 peer's counter-proposal
    // is likewise never selected as the Phase 2 value — see the peer guard
    // below, which already filters `priority == 0` the same way it filters
    // arbiter promises.)
    if priority == 0 {
        log::warn!(
            "election: node {} has NODE_PRIORITY 0 (electable but not \
             master-eligible) refusing to propose itself as master",
            node_name
        );
        return None;
    }

    // Flexible Paxos: Phase 1 and Phase 2 may use different quorum sizes.
    // For SimpleMajority both equal (n/2)+1; for Flexible they differ.
    let phase1_quorum = group.phase1_quorum();
    let phase2_quorum = group.phase2_quorum();
    if phase1_quorum == 0 || phase2_quorum == 0 {
        return None;
    }

    // Adaptive phase timeout from phi accrual statistics.
    let phase_timeout = phi_detector
        .map(|p| p.suggested_phase_timeout(3.0, fallback_timeout))
        .unwrap_or(fallback_timeout);

    // We always count ourselves as one vote in both phases.
    let self_needed = 1usize;

    // -------------------------------------------------------------------------
    // Phase 1: Prepare / Promise
    // -------------------------------------------------------------------------
    // Build our proposal.
    let our_proposal =
        Proposal::new(node_name.to_string(), proposed_vlsn, priority, term)
            .with_dtvlsn(own_dtvlsn);

    let phase1_msg = ProtocolMessage::ElectionProposal {
        node_name: node_name.to_string(),
        vlsn: proposed_vlsn,
        priority,
        term,
        dtvlsn: own_dtvlsn,
    };

    // Broadcast to all peers.
    let mut promises: Vec<Arc<dyn Channel>> = Vec::new();
    // Collect the Promise suggestions for JE-faithful Phase-2 value selection
    // (choose_phase2_value). Our own candidacy is always the first record.
    let mut promise_records: Vec<PromiseRecord> =
        vec![PromiseRecord { proposal: our_proposal, is_arbiter: false }];

    let phase1_timeout = phase_timeout;

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
                    dtvlsn: peer_dtvlsn,
                })) => {
                    // F22 / C5 / V14: a peer that cannot be master (Arbiter /
                    // Monitor / Secondary) or advertises NODE_PRIORITY 0 is a
                    // Promise but never an ordinary candidate value. We record
                    // arbiter suggestions with `is_arbiter = true` so
                    // `choose_phase2_value` can apply the RF=2 [#25311] veto
                    // (an arbiter that remembers more durable data than a lone
                    // surviving node blocks the election). A priority-0 peer's
                    // suggestion is dropped by the zero-priority rule inside
                    // the chooser.
                    let peer_node = group.get_node(&peer_name);
                    let peer_can_be_master = peer_node
                        .as_ref()
                        .map(|n| n.can_be_master())
                        // Unknown peer name — be conservative, do NOT promote.
                        .unwrap_or(false);
                    // An arbiter (electable, not master-eligible by TYPE)
                    // is the RF=2 tie-breaker whose ranking must be seen.
                    let peer_is_arbiter = peer_node
                        .as_ref()
                        .map(|n| {
                            n.node_type().is_electable() && !n.can_be_master()
                        })
                        .unwrap_or(false);
                    let peer_p = Proposal::new(
                        peer_name,
                        peer_vlsn,
                        peer_priority,
                        peer_term,
                    )
                    .with_dtvlsn(peer_dtvlsn);
                    if peer_can_be_master && peer_priority != 0 {
                        promise_records.push(PromiseRecord {
                            proposal: peer_p,
                            is_arbiter: false,
                        });
                    } else if peer_is_arbiter {
                        promise_records.push(PromiseRecord {
                            proposal: peer_p,
                            is_arbiter: true,
                        });
                    }
                    // (Priority-0 non-arbiter peers contribute a Promise for
                    // quorum but no record — the chooser's zero-priority rule
                    // would drop them anyway; we simply do not record them.)
                    // Counts as a Promise either way (Arbiters DO participate
                    // in elections — they just cannot win).
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
    if total_promises < phase1_quorum {
        return None;
    }

    // -------------------------------------------------------------------------
    // Phase 2: Accept
    // -------------------------------------------------------------------------
    // JE-faithful value selection (RankingProposer.choosePhase2Value). The
    // RF=2 arbiter veto and the no-non-zero-priority case both yield no value,
    // which aborts this round (no master elected).
    let winner_name = match choose_phase2_value(&promise_records) {
        Phase2Value::Value(p) => p.node_name,
        Phase2Value::ArbiterVeto => {
            log::warn!(
                "election: arbiter remembers more durable data than the sole                  candidate ([#25311]); no master safely electable this round"
            );
            return None;
        }
        Phase2Value::NoNonZeroPriority => {
            log::info!(
                "election: no non-zero-priority master candidate responded"
            );
            return None;
        }
    };
    let accept_msg =
        ProtocolMessage::ElectionResult { master: winner_name.clone(), term };

    let mut accepts = 0usize;
    let phase2_timeout = phase_timeout;

    for ch in &promises {
        if send_message(ch.as_ref(), &accept_msg).is_ok()
            && let Ok(Some(ProtocolMessage::ElectionVote {
                granted: true, ..
            })) = receive_message(ch.as_ref(), phase2_timeout)
        {
            accepts += 1;
        }
    }

    // Count self-accept.
    accepts += self_needed;

    if accepts >= phase2_quorum {
        // Resolve winner name to node_id by looking up in the group.
        let winner_id = if winner_name == node_name {
            node_id
        } else {
            group.get_node(&winner_name).map(|n| n.node_id()).unwrap_or(node_id)
        };
        Some(winner_id)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Election identity binding (F3b / S1)
// ---------------------------------------------------------------------------

/// Bind a wire-reported proposer `node_name` to the channel's TLS-verified
/// peer identity.
///
/// F3b / S1: the acceptor reads the proposer's `node_name` straight off the
/// wire (`ElectionProposal { node_name, .. }`) and, historically, believed
/// it.  An allowlisted-but-compromised peer (cert CN `node-a`) could send
/// `ElectionProposal { node_name: "node-c", vlsn: <inflated>, .. }` and be
/// believed, impersonating another member.
///
/// When the channel is mutually authenticated (`peer_identity()` is `Some`),
/// the self-reported `node_name` MUST match one of the verified subject
/// names (case-insensitive) or the proposal is rejected with a
/// `ProtocolError`.  When it is `None` (plain TCP / `insecure_no_auth`), the
/// operator has explicitly opted out of peer authentication, so behaviour is
/// unchanged (documented opt-out) -- there is no verified identity to bind
/// against.
fn check_election_identity(
    channel: &dyn Channel,
    proposer: &str,
) -> Result<()> {
    if let Some(id) = channel.peer_identity()
        && !id.subject_names.iter().any(|n| n.eq_ignore_ascii_case(proposer))
    {
        log::warn!(
            "election: rejecting proposer '{}' -- name not backed by verified peer identity {:?}",
            proposer,
            id.subject_names
        );
        return Err(RepError::ProtocolError(format!(
            "election node_name '{proposer}' does not match verified peer identity {:?}",
            id.subject_names
        )));
    }
    Ok(())
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
    // Legacy entry point: no DTVLSN (pre-DTVLSN -> ranking falls back to VLSN).
    let own_dtvlsn: u64 = 0;
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
            node_name: proposer,
            vlsn: _vlsn,
            priority: _priority,
            term,
            dtvlsn: _dtvlsn,
        } => {
            // F3b/S1: bind the self-reported proposer name to the verified
            // TLS peer identity before trusting it.
            check_election_identity(channel, &proposer)?;
            // acceptor: reject only if a higher-numbered proposal was
            // already promised. Accept/promise the first proposal regardless
            // of the proposer's VLSN — the VLSN comparison happens at the
            // proposer level when it collects suggestions.
            let should_promise =
                promised_term.is_none_or(|promised| term >= promised);

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
                        dtvlsn: own_dtvlsn,
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
            ));
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
// run_acceptor_with_state
// ---------------------------------------------------------------------------

/// Like `run_acceptor`, but routes the promise/accept decisions through a
/// crash-durable `PersistentAcceptorState`.
///
/// Closes findings F5 and F31 of the 2026 review.
///
/// The Paxos invariant is that an acceptor never accepts a proposal at a
/// term lower than its highest promise.  The legacy `run_acceptor` keeps
/// the promise in a local stack variable, which is lost across process
/// restarts.  This variant calls `state.try_promise(t)` and
/// `state.try_accept(t, master)` so every state change is fsynced before
/// the response goes back to the proposer.
#[allow(clippy::too_many_arguments)]
pub fn run_acceptor_with_state(
    channel: &dyn Channel,
    node_name: &str,
    own_vlsn: u64,
    own_priority: u32,
    own_term: u64,
    own_dtvlsn: u64,
    state: &crate::elections::acceptor_state::PersistentAcceptorState,
    freeze_latch: Option<
        &crate::elections::commit_freeze_latch::CommitFreezeLatch,
    >,
) -> Result<Option<String>> {
    use crate::elections::commit_freeze_latch::round_proposal;
    let timeout = Duration::from_millis(500);

    // Lift the freeze this acceptor session established, if it is still the
    // current one.  `vlsn_event` only thaws when the arriving round is
    // newer-or-equal, so a freeze installed by a LATER round (a concurrent
    // session, or the local election driver) survives.
    let thaw = |term: u64| {
        if let Some(latch) = freeze_latch {
            latch.vlsn_event(&round_proposal(term));
        }
    };

    // Phase 1: receive Propose.
    let phase1 = match receive_message(channel, timeout)? {
        Some(m) => m,
        None => return Ok(None),
    };

    let phase1_term = match phase1 {
        ProtocolMessage::ElectionProposal {
            node_name: proposer,
            vlsn: _vlsn,
            priority: _priority,
            term,
            dtvlsn: _dtvlsn,
        } => {
            // F3b/S1: bind the self-reported proposer name to the verified
            // TLS peer identity before trusting it.  A rejection here returns
            // before any freeze is installed, so no thaw is needed.
            check_election_identity(channel, &proposer)?;
            if state.try_promise(term) {
                // Freeze commit-VLSN advancement for the duration of this
                // round: the VLSN/DTVLSN we are about to advertise in the
                // Promise must still describe this node when the proposer
                // picks a value in phase 2.  JE freezes at exactly this point
                // — `MasterSuggestionGenerator.getRanking` calls
                // `repNode.getVLSNFreezeLatch().freeze(proposal)` right before
                // computing the (dtvlsn, vlsn) ranking it returns in the
                // Promise (`MasterSuggestionGenerator.java:63`).
                if let Some(latch) = freeze_latch {
                    latch.freeze(round_proposal(term));
                }
                send_message(
                    channel,
                    &ProtocolMessage::ElectionProposal {
                        node_name: node_name.to_string(),
                        vlsn: own_vlsn,
                        priority: own_priority,
                        term: own_term,
                        dtvlsn: own_dtvlsn,
                    },
                )?;
                term
            } else {
                send_message(
                    channel,
                    &ProtocolMessage::ElectionVote {
                        voter: node_name.to_string(),
                        granted: false,
                        term: state.promised_term(),
                    },
                )?;
                return Ok(None);
            }
        }
        _ => {
            return Err(RepError::ProtocolError(
                "acceptor: expected ElectionProposal in phase 1".into(),
            ));
        }
    };

    // Phase 2: receive ElectionResult.  Every exit path from here on thaws the
    // freeze this session installed — an unresolved round must not leave the
    // replay path blocked for the whole latch timeout, and a latch that never
    // thaws would be worse than not freezing at all.
    let phase2 = match receive_message(channel, timeout) {
        Ok(Some(m)) => m,
        Ok(None) => {
            thaw(phase1_term);
            return Ok(None);
        }
        Err(e) => {
            thaw(phase1_term);
            return Err(e);
        }
    };

    match phase2 {
        ProtocolMessage::ElectionResult { master, term } => {
            // The round resolved (for good or ill): lift the freeze.  JE does
            // this from the Learner side — `MasterChangeListener.notify` calls
            // `repNode.getVLSNFreezeLatch().vlsnEvent(proposal)` as the first
            // thing it does on an election result
            // (`MasterChangeListener.java:49`).
            thaw(term.max(phase1_term));
            // Accept iff the result term EXACTLY equals the term we promised
            // in phase 1 (JE Acceptor.process(Accept): reject unless
            // promisedProposal.compareTo(accept.getProposal()) == 0). A
            // proposer that switched to a higher term mid-round (got a
            // phase-1 promise at T1, then sent a phase-2 Accept at T2 > T1
            // without a fresh phase 1) MUST be rejected — accepting it admits
            // two proposers reaching phase-2 quorum at different terms, the
            // classic split-brain failure mode.
            if term == phase1_term && state.try_accept(term, &master) {
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
        _ => {
            thaw(phase1_term);
            Err(RepError::ProtocolError(
                "acceptor: expected ElectionResult in phase 2".into(),
            ))
        }
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
            &group, &channels, 100, // vlsn (higher than peers)
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
            &group, &channels, 200, // vlsn (highest)
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

        let winner = run_election(
            1,
            "node1",
            &group,
            &[],
            /* no peers */ 100,
            1,
            1,
        );
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
            "node1".into(),
            NodeType::Electable,
            "127.0.0.1".into(),
            5001,
            1,
        ));
        group.add_node(RepNode::new(
            "node2".into(),
            NodeType::Electable,
            "127.0.0.1".into(),
            5002,
            2,
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
                "node1".into(),
                NodeType::Electable,
                "127.0.0.1".into(),
                5001,
                1,
            ));
            g.add_node(RepNode::new(
                "node2".into(),
                NodeType::Electable,
                "127.0.0.1".into(),
                5002,
                2,
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
        let winner = run_election(1, "node1", &group, &[proposer_ch], 10, 1, 1);

        let accepted = handle.join().unwrap();
        // node2 accepted the phase2 result.
        assert!(accepted.is_some());
        // Winner should be node2 (id=2) since it has higher VLSN.
        assert_eq!(winner, Some(2), "node2 should win with higher VLSN");
    }
}
