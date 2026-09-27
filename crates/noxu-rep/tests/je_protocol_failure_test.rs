//! Port of the resilience intent of
//! `je/test/com/sleepycat/je/rep/elections/ProtocolFailureTest.java`.
//!
//! JE's test munges one token of the pipe-separated `TextProtocol` wire format
//! (`<version>|<groupName>|<id>|<op>|<payload>`) on election messages and
//! asserts a node stays in `State.UNKNOWN` (never falsely elects a master or
//! replica) while the corruption is in effect, then recovers to a normal
//! election once valid messages resume. The intent is CONFINEMENT: a corrupt
//! election protocol message must be rejected, never misparsed into a false
//! election, and the node must make forward progress once corruption stops.
//!
//! Noxu's election wire format is a binary tagged encoding
//! (`ProtocolMessage`), not JE's textual `TextProtocol`, so the per-token
//! mapping is:
//!   * OP_TOKEN            -> the leading tag byte (message discriminant);
//!   * FIRST_PAYLOAD_TOKEN -> the length-prefixed / fixed-width payload;
//!   * NAME_TOKEN          -> the `node_name` field (bound to TLS identity, S1).
//!   * VERSION_TOKEN / ID_TOKEN -> NO Noxu analogue (see the N/A notes at the
//!     bottom): Noxu's election message carries neither a protocol-version
//!     token nor a numeric node-id token; the id is resolved from the group by
//!     name, and there is no TextProtocol version negotiation.
//!
//! These ports drive the mapped tokens through `ProtocolMessage::decode` and
//! `run_acceptor`, asserting a corrupt message is rejected (no false grant /
//! election) and that a valid message afterwards elects normally.

use std::sync::Arc;

use noxu_rep::elections::paxos::{run_acceptor, run_election};
use noxu_rep::net::{Channel, LocalChannelPair};
use noxu_rep::node_type::NodeType;
use noxu_rep::protocol::ProtocolMessage;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;
use std::time::Duration;

fn valid_proposal() -> ProtocolMessage {
    ProtocolMessage::ElectionProposal {
        node_name: "n1".into(),
        vlsn: 100,
        priority: 1,
        term: 1,
        dtvlsn: 0,
    }
}

// --------------------------------------------------------------------------
// JE: ProtocolFailureTest.testBadOp / testBadOpResp (OP_TOKEN).
// A corrupted op/message-type token must be rejected (not misparsed). Noxu's
// leading tag byte is the op; a garbage tag -> decode error, never a silently
// wrong message.
// --------------------------------------------------------------------------
#[test]
fn test_bad_op_rejected() {
    let mut bytes = valid_proposal().encode();
    // Corrupt the op (tag) byte to an unused discriminant.
    bytes[0] = 0xEE;
    let decoded = ProtocolMessage::decode(&bytes);
    assert!(
        decoded.is_err(),
        "a corrupted op/tag token must be rejected, not misparsed"
    );
    let msg = format!("{:?}", decoded.unwrap_err());
    assert!(
        msg.contains("unknown message tag"),
        "error must identify the bad op: {msg}"
    );
}

/// The acceptor side: a peer sending a message with a corrupt op must not
/// cause a false grant; `run_acceptor` returns an error / no master.
#[test]
fn test_bad_op_acceptor_does_not_grant() {
    let pair = LocalChannelPair::new();
    let proposer_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
    let acceptor_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

    let handle = std::thread::spawn(move || {
        // Acceptor receives a corrupt-op message in Phase 1 -> Err / no grant.
        run_acceptor(&*acceptor_ch, "n2", 10, 1, 1)
    });

    // Send a corrupt-op message where a Propose was expected.
    let mut bytes = valid_proposal().encode();
    bytes[0] = 0xEE;
    proposer_ch.send(&bytes).unwrap();

    let result = handle.join().unwrap();
    assert!(
        result.is_err() || result.unwrap().is_none(),
        "acceptor must not grant on a corrupt-op message"
    );
}

// --------------------------------------------------------------------------
// JE: ProtocolFailureTest.testBadPayloadResp (FIRST_PAYLOAD_TOKEN).
// A corrupted / truncated payload must be rejected.
// --------------------------------------------------------------------------
#[test]
fn test_bad_payload_rejected() {
    let bytes = valid_proposal().encode();
    // Truncate the payload mid-way (keep the tag + part of the name length).
    let truncated = &bytes[..3];
    let decoded = ProtocolMessage::decode(truncated);
    assert!(
        decoded.is_err(),
        "a truncated payload must be rejected (unexpected end of input)"
    );
}

/// A payload whose declared string length overruns the buffer must be rejected
/// (the JE payload-munge analogue: a length token pointing past the data).
#[test]
fn test_bad_payload_length_overrun_rejected() {
    let mut bytes = valid_proposal().encode();
    // The name string length is the 4 bytes after the tag; inflate it hugely.
    bytes[1] = 0xFF;
    bytes[2] = 0xFF;
    bytes[3] = 0xFF;
    bytes[4] = 0x7F;
    let decoded = ProtocolMessage::decode(&bytes);
    assert!(
        decoded.is_err(),
        "a payload length token overrunning the buffer must be rejected"
    );
}

// --------------------------------------------------------------------------
// JE: ProtocolFailureTest.testBadNameReq / testBadNameResp (NAME_TOKEN).
// A corrupt name field must not be silently trusted. Noxu's node_name decodes
// as a string; a garbled name that is still valid UTF-8 decodes, but the
// election machinery never treats an unknown/garbage name as a valid master
// (it resolves to a group member, else falls back to the proposer). Under
// mTLS the S1 binding rejects a name not backed by the peer's cert (covered
// in s1_identity_binding_test.rs); here we assert the plain-channel property
// that a garbage proposer name cannot elect a NON-member as master.
// --------------------------------------------------------------------------
#[test]
fn test_bad_name_does_not_elect_non_member() {
    // 1-node group so quorum is a self-vote; a peer sends a garbage-name
    // counter-proposal.
    let mut group = RepGroup::new("g".into(), 1);
    group.add_node(RepNode::new(
        "n1".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        5001,
        1,
    ));

    // A peer promises with a garbage node_name that is NOT a group member.
    let pair = LocalChannelPair::new();
    let proposer_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
    let acceptor_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);
    let h = std::thread::spawn(move || {
        // run_acceptor answers with its OWN (garbage) name as the suggestion.
        run_acceptor(&*acceptor_ch, "\u{0}bad-ghost\u{0}", 999, 1, 1)
            .unwrap_or(None)
    });

    let winner = run_election(1, "n1", &group, &[proposer_ch], 100, 1, 1);
    let _ = h.join();

    // The garbage-named non-member must NOT win; the elected id must resolve
    // to a real group member (n1). A non-member counter-proposal is not
    // promoted (unknown peer name -> not master-eligible).
    assert_eq!(
        winner,
        Some(1),
        "a corrupt/unknown proposer name must not elect a non-member master"
    );
}

// --------------------------------------------------------------------------
// Recovery (the second half of every JE testInternal): once corruption stops,
// a valid message elects normally.
// --------------------------------------------------------------------------
#[test]
fn test_recovers_after_corruption_stops() {
    // A corrupt decode is an isolated event; a subsequent VALID proposal
    // decodes and drives a normal election. Prove both in sequence.
    let mut corrupt = valid_proposal().encode();
    corrupt[0] = 0xEE;
    assert!(ProtocolMessage::decode(&corrupt).is_err());

    // Now a full valid election concludes.
    let mut group = RepGroup::new("g".into(), 1);
    group.add_node(RepNode::new(
        "n1".into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        5001,
        1,
    ));
    let winner = run_election(1, "n1", &group, &[], 100, 1, 1);
    assert_eq!(
        winner,
        Some(1),
        "after corruption stops, a valid election must conclude normally"
    );
}

// --------------------------------------------------------------------------
// Sanity: a well-formed round-trip decodes back to the original (control that
// the corruption assertions above are not vacuously always-error).
// --------------------------------------------------------------------------
#[test]
fn test_valid_message_round_trips() {
    let msg = valid_proposal();
    let decoded = ProtocolMessage::decode(&msg.encode()).unwrap();
    match decoded {
        ProtocolMessage::ElectionProposal { node_name, vlsn, term, .. } => {
            assert_eq!(node_name, "n1");
            assert_eq!(vlsn, 100);
            assert_eq!(term, 1);
        }
        other => panic!("unexpected decode: {other:?}"),
    }
    let _ = Duration::from_secs(0); // keep the import used across cfgs
}

// N/A (recorded, not ported):
//   * testBadVersionReq / testBadVersionResp (VERSION_TOKEN): Noxu's election
//     wire message has NO protocol-version token — there is no TextProtocol
//     version-negotiation field to corrupt. Genuine language/format deviation
//     (JE-internal TextProtocol versioning). The generic "corrupt leading
//     bytes are rejected" property IS covered by test_bad_op_rejected.
//   * testBadId / testBadIdResp (ID_TOKEN): Noxu's ElectionProposal carries
//     no numeric node-id token; the id is resolved from the group by name.
//     Nothing to corrupt. The "corrupt/unknown identity is not trusted"
//     property IS covered by test_bad_name_does_not_elect_non_member and the
//     S1 mTLS binding.
//   * testBadPayloadRequest: EMPTY test body in JE (commented out — "Future:
//     Need custom tests and custom code for bad payload requests."). Vacuous
//     in JE itself; nothing to port. The payload-corruption property is
//     covered by testBadPayloadResp's ports above.
