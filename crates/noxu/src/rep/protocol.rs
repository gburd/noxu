//! Replication protocol messages.
//!
//! Replication protocol message types.
//! and related classes. Uses a simple tag+length+value binary encoding.

use crate::rep::error::{RepError, Result};
use crate::rep::node_type::NodeType;
use crate::rep::rep_node::RepNode;

/// Type of change to the replication group membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupChangeType {
    /// A node is being added to the group.
    Add,
    /// A node is being removed from the group.
    Remove,
    /// A node's information is being updated.
    Update,
}

/// A message in the replication protocol.
///
/// These messages are exchanged between master and replica nodes for
/// handshaking, heartbeats, log replication, elections, and group
/// management.
#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolMessage {
    // --- Handshake ---
    /// Initial handshake from a node joining the replication stream.
    Handshake { node_name: String, group_name: String, node_type: NodeType },
    /// Response to a handshake.
    HandshakeResponse { accepted: bool, reason: Option<String> },

    // --- Heartbeat ---
    /// Heartbeat from master to replica.
    Heartbeat { master_vlsn: u64, timestamp_ms: u64 },
    /// Heartbeat response from replica to master.
    HeartbeatResponse { replica_vlsn: u64, timestamp_ms: u64 },

    // --- Replication stream ---
    /// A log entry being replicated from master to replica.
    LogEntry { vlsn: u64, entry_type: u8, data: Vec<u8> },
    /// Acknowledgment of a replicated log entry.
    Ack { vlsn: u64 },

    // --- Group management ---
    /// A group membership change request.
    GroupChange { change_type: GroupChangeType, node: RepNode },
    /// Response to a group change request.
    GroupChangeResponse { accepted: bool },

    // --- Election ---
    /// An election proposal from a candidate.
    ElectionProposal { node_name: String, vlsn: u64, priority: u32, term: u64 },
    /// A vote in response to an election proposal.
    ElectionVote { voter: String, granted: bool, term: u64 },
    /// The result of an election.
    ElectionResult { master: String, term: u64 },

    // --- Shutdown ---
    /// Graceful shutdown notification.
    Shutdown { reason: String },
}

// --- Wire format tags ---
const TAG_HANDSHAKE: u8 = 1;
const TAG_HANDSHAKE_RESPONSE: u8 = 2;
const TAG_HEARTBEAT: u8 = 3;
const TAG_HEARTBEAT_RESPONSE: u8 = 4;
const TAG_LOG_ENTRY: u8 = 5;
const TAG_ACK: u8 = 6;
const TAG_GROUP_CHANGE: u8 = 7;
const TAG_GROUP_CHANGE_RESPONSE: u8 = 8;
const TAG_ELECTION_PROPOSAL: u8 = 9;
const TAG_ELECTION_VOTE: u8 = 10;
const TAG_ELECTION_RESULT: u8 = 11;
const TAG_SHUTDOWN: u8 = 12;

impl ProtocolMessage {
    /// Encodes this message into a byte vector.
    ///
    /// Format: `[tag: u8][payload...]`
    /// Strings are encoded as `[len: u32 LE][utf8 bytes]`.
    /// Booleans as a single byte (0 or 1).
    /// Integers as little-endian fixed-width.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            ProtocolMessage::Handshake { node_name, group_name, node_type } => {
                buf.push(TAG_HANDSHAKE);
                encode_string(&mut buf, node_name);
                encode_string(&mut buf, group_name);
                buf.push(encode_node_type(node_type));
            }
            ProtocolMessage::HandshakeResponse { accepted, reason } => {
                buf.push(TAG_HANDSHAKE_RESPONSE);
                buf.push(if *accepted { 1 } else { 0 });
                match reason {
                    Some(r) => {
                        buf.push(1); // has reason
                        encode_string(&mut buf, r);
                    }
                    None => {
                        buf.push(0); // no reason
                    }
                }
            }
            ProtocolMessage::Heartbeat { master_vlsn, timestamp_ms } => {
                buf.push(TAG_HEARTBEAT);
                buf.extend_from_slice(&master_vlsn.to_le_bytes());
                buf.extend_from_slice(&timestamp_ms.to_le_bytes());
            }
            ProtocolMessage::HeartbeatResponse {
                replica_vlsn,
                timestamp_ms,
            } => {
                buf.push(TAG_HEARTBEAT_RESPONSE);
                buf.extend_from_slice(&replica_vlsn.to_le_bytes());
                buf.extend_from_slice(&timestamp_ms.to_le_bytes());
            }
            ProtocolMessage::LogEntry { vlsn, entry_type, data } => {
                buf.push(TAG_LOG_ENTRY);
                buf.extend_from_slice(&vlsn.to_le_bytes());
                buf.push(*entry_type);
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
            ProtocolMessage::Ack { vlsn } => {
                buf.push(TAG_ACK);
                buf.extend_from_slice(&vlsn.to_le_bytes());
            }
            ProtocolMessage::GroupChange { change_type, node } => {
                buf.push(TAG_GROUP_CHANGE);
                buf.push(encode_change_type(change_type));
                encode_rep_node(&mut buf, node);
            }
            ProtocolMessage::GroupChangeResponse { accepted } => {
                buf.push(TAG_GROUP_CHANGE_RESPONSE);
                buf.push(if *accepted { 1 } else { 0 });
            }
            ProtocolMessage::ElectionProposal {
                node_name,
                vlsn,
                priority,
                term,
            } => {
                buf.push(TAG_ELECTION_PROPOSAL);
                encode_string(&mut buf, node_name);
                buf.extend_from_slice(&vlsn.to_le_bytes());
                buf.extend_from_slice(&priority.to_le_bytes());
                buf.extend_from_slice(&term.to_le_bytes());
            }
            ProtocolMessage::ElectionVote { voter, granted, term } => {
                buf.push(TAG_ELECTION_VOTE);
                encode_string(&mut buf, voter);
                buf.push(if *granted { 1 } else { 0 });
                buf.extend_from_slice(&term.to_le_bytes());
            }
            ProtocolMessage::ElectionResult { master, term } => {
                buf.push(TAG_ELECTION_RESULT);
                encode_string(&mut buf, master);
                buf.extend_from_slice(&term.to_le_bytes());
            }
            ProtocolMessage::Shutdown { reason } => {
                buf.push(TAG_SHUTDOWN);
                encode_string(&mut buf, reason);
            }
        }
        buf
    }

    /// Decodes a message from a byte slice.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(RepError::ProtocolError("empty message".to_string()));
        }
        let tag = data[0];
        let mut pos = 1;

        match tag {
            TAG_HANDSHAKE => {
                let node_name = decode_string(data, &mut pos)?;
                let group_name = decode_string(data, &mut pos)?;
                let node_type = decode_node_type(data, &mut pos)?;
                Ok(ProtocolMessage::Handshake {
                    node_name,
                    group_name,
                    node_type,
                })
            }
            TAG_HANDSHAKE_RESPONSE => {
                let accepted = decode_bool(data, &mut pos)?;
                let has_reason = decode_bool(data, &mut pos)?;
                let reason = if has_reason {
                    Some(decode_string(data, &mut pos)?)
                } else {
                    None
                };
                Ok(ProtocolMessage::HandshakeResponse { accepted, reason })
            }
            TAG_HEARTBEAT => {
                let master_vlsn = decode_u64(data, &mut pos)?;
                let timestamp_ms = decode_u64(data, &mut pos)?;
                Ok(ProtocolMessage::Heartbeat { master_vlsn, timestamp_ms })
            }
            TAG_HEARTBEAT_RESPONSE => {
                let replica_vlsn = decode_u64(data, &mut pos)?;
                let timestamp_ms = decode_u64(data, &mut pos)?;
                Ok(ProtocolMessage::HeartbeatResponse {
                    replica_vlsn,
                    timestamp_ms,
                })
            }
            TAG_LOG_ENTRY => {
                let vlsn = decode_u64(data, &mut pos)?;
                let entry_type = decode_u8(data, &mut pos)?;
                let data_len = decode_u32(data, &mut pos)? as usize;
                let payload = decode_bytes(data, &mut pos, data_len)?;
                Ok(ProtocolMessage::LogEntry {
                    vlsn,
                    entry_type,
                    data: payload,
                })
            }
            TAG_ACK => {
                let vlsn = decode_u64(data, &mut pos)?;
                Ok(ProtocolMessage::Ack { vlsn })
            }
            TAG_GROUP_CHANGE => {
                let change_type = decode_change_type(data, &mut pos)?;
                let node = decode_rep_node(data, &mut pos)?;
                Ok(ProtocolMessage::GroupChange { change_type, node })
            }
            TAG_GROUP_CHANGE_RESPONSE => {
                let accepted = decode_bool(data, &mut pos)?;
                Ok(ProtocolMessage::GroupChangeResponse { accepted })
            }
            TAG_ELECTION_PROPOSAL => {
                let node_name = decode_string(data, &mut pos)?;
                let vlsn = decode_u64(data, &mut pos)?;
                let priority = decode_u32(data, &mut pos)?;
                let term = decode_u64(data, &mut pos)?;
                Ok(ProtocolMessage::ElectionProposal {
                    node_name,
                    vlsn,
                    priority,
                    term,
                })
            }
            TAG_ELECTION_VOTE => {
                let voter = decode_string(data, &mut pos)?;
                let granted = decode_bool(data, &mut pos)?;
                let term = decode_u64(data, &mut pos)?;
                Ok(ProtocolMessage::ElectionVote { voter, granted, term })
            }
            TAG_ELECTION_RESULT => {
                let master = decode_string(data, &mut pos)?;
                let term = decode_u64(data, &mut pos)?;
                Ok(ProtocolMessage::ElectionResult { master, term })
            }
            TAG_SHUTDOWN => {
                let reason = decode_string(data, &mut pos)?;
                Ok(ProtocolMessage::Shutdown { reason })
            }
            _ => Err(RepError::ProtocolError(format!(
                "unknown message tag: {}",
                tag
            ))),
        }
    }
}

// --- Encoding helpers ---

fn encode_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn encode_node_type(nt: &NodeType) -> u8 {
    match nt {
        NodeType::Electable => 0,
        NodeType::Monitor => 1,
        NodeType::Secondary => 2,
        NodeType::Arbiter => 3,
    }
}

fn encode_change_type(ct: &GroupChangeType) -> u8 {
    match ct {
        GroupChangeType::Add => 0,
        GroupChangeType::Remove => 1,
        GroupChangeType::Update => 2,
    }
}

fn encode_rep_node(buf: &mut Vec<u8>, node: &RepNode) {
    encode_string(buf, &node.name);
    buf.push(encode_node_type(&node.node_type));
    encode_string(buf, &node.host);
    buf.extend_from_slice(&node.port.to_le_bytes());
    buf.extend_from_slice(&node.node_id.to_le_bytes());
}

// --- Decoding helpers ---

fn ensure_remaining(data: &[u8], pos: usize, needed: usize) -> Result<()> {
    if pos + needed > data.len() {
        Err(RepError::ProtocolError(format!(
            "unexpected end of message at offset {}, need {} more bytes",
            pos, needed
        )))
    } else {
        Ok(())
    }
}

fn decode_u8(data: &[u8], pos: &mut usize) -> Result<u8> {
    ensure_remaining(data, *pos, 1)?;
    let val = data[*pos];
    *pos += 1;
    Ok(val)
}

fn decode_bool(data: &[u8], pos: &mut usize) -> Result<bool> {
    let val = decode_u8(data, pos)?;
    Ok(val != 0)
}

fn decode_u16(data: &[u8], pos: &mut usize) -> Result<u16> {
    ensure_remaining(data, *pos, 2)?;
    let val = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;
    Ok(val)
}

fn decode_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    ensure_remaining(data, *pos, 4)?;
    let val = u32::from_le_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
    ]);
    *pos += 4;
    Ok(val)
}

fn decode_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
    ensure_remaining(data, *pos, 8)?;
    let val = u64::from_le_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
        data[*pos + 5],
        data[*pos + 6],
        data[*pos + 7],
    ]);
    *pos += 8;
    Ok(val)
}

fn decode_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let len = decode_u32(data, pos)? as usize;
    let bytes = decode_bytes(data, pos, len)?;
    String::from_utf8(bytes).map_err(|e| {
        RepError::ProtocolError(format!("invalid UTF-8 in string: {}", e))
    })
}

fn decode_bytes(data: &[u8], pos: &mut usize, len: usize) -> Result<Vec<u8>> {
    ensure_remaining(data, *pos, len)?;
    let bytes = data[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(bytes)
}

fn decode_node_type(data: &[u8], pos: &mut usize) -> Result<NodeType> {
    let val = decode_u8(data, pos)?;
    match val {
        0 => Ok(NodeType::Electable),
        1 => Ok(NodeType::Monitor),
        2 => Ok(NodeType::Secondary),
        3 => Ok(NodeType::Arbiter),
        _ => {
            Err(RepError::ProtocolError(format!("unknown node type: {}", val)))
        }
    }
}

fn decode_change_type(data: &[u8], pos: &mut usize) -> Result<GroupChangeType> {
    let val = decode_u8(data, pos)?;
    match val {
        0 => Ok(GroupChangeType::Add),
        1 => Ok(GroupChangeType::Remove),
        2 => Ok(GroupChangeType::Update),
        _ => Err(RepError::ProtocolError(format!(
            "unknown change type: {}",
            val
        ))),
    }
}

fn decode_rep_node(data: &[u8], pos: &mut usize) -> Result<RepNode> {
    let name = decode_string(data, pos)?;
    let node_type = decode_node_type(data, pos)?;
    let host = decode_string(data, pos)?;
    let port = decode_u16(data, pos)?;
    let node_id = decode_u32(data, pos)?;
    Ok(RepNode::new(name, node_type, host, port, node_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: encode then decode, assert round-trip equality.
    fn round_trip(msg: &ProtocolMessage) {
        let encoded = msg.encode();
        let decoded = ProtocolMessage::decode(&encoded).unwrap();
        assert_eq!(*msg, decoded);
    }

    #[test]
    fn test_handshake_round_trip() {
        round_trip(&ProtocolMessage::Handshake {
            node_name: "node1".to_string(),
            group_name: "group1".to_string(),
            node_type: NodeType::Electable,
        });
    }

    #[test]
    fn test_handshake_all_node_types() {
        for nt in &[
            NodeType::Electable,
            NodeType::Monitor,
            NodeType::Secondary,
            NodeType::Arbiter,
        ] {
            round_trip(&ProtocolMessage::Handshake {
                node_name: "n".to_string(),
                group_name: "g".to_string(),
                node_type: *nt,
            });
        }
    }

    #[test]
    fn test_handshake_response_accepted() {
        round_trip(&ProtocolMessage::HandshakeResponse {
            accepted: true,
            reason: None,
        });
    }

    #[test]
    fn test_handshake_response_rejected() {
        round_trip(&ProtocolMessage::HandshakeResponse {
            accepted: false,
            reason: Some("group mismatch".to_string()),
        });
    }

    #[test]
    fn test_heartbeat_round_trip() {
        round_trip(&ProtocolMessage::Heartbeat {
            master_vlsn: 12345,
            timestamp_ms: 1700000000000,
        });
    }

    #[test]
    fn test_heartbeat_response_round_trip() {
        round_trip(&ProtocolMessage::HeartbeatResponse {
            replica_vlsn: 12340,
            timestamp_ms: 1700000000001,
        });
    }

    #[test]
    fn test_log_entry_round_trip() {
        round_trip(&ProtocolMessage::LogEntry {
            vlsn: 100,
            entry_type: 42,
            data: vec![1, 2, 3, 4, 5],
        });
    }

    #[test]
    fn test_log_entry_empty_data() {
        round_trip(&ProtocolMessage::LogEntry {
            vlsn: 1,
            entry_type: 0,
            data: vec![],
        });
    }

    #[test]
    fn test_log_entry_large_data() {
        let data = vec![0xAB; 10000];
        round_trip(&ProtocolMessage::LogEntry {
            vlsn: u64::MAX,
            entry_type: 255,
            data,
        });
    }

    #[test]
    fn test_ack_round_trip() {
        round_trip(&ProtocolMessage::Ack { vlsn: 999 });
    }

    #[test]
    fn test_group_change_add() {
        round_trip(&ProtocolMessage::GroupChange {
            change_type: GroupChangeType::Add,
            node: RepNode::new(
                "new_node".to_string(),
                NodeType::Electable,
                "10.0.0.5".to_string(),
                5001,
                7,
            ),
        });
    }

    #[test]
    fn test_group_change_remove() {
        round_trip(&ProtocolMessage::GroupChange {
            change_type: GroupChangeType::Remove,
            node: RepNode::new(
                "old_node".to_string(),
                NodeType::Monitor,
                "localhost".to_string(),
                6000,
                3,
            ),
        });
    }

    #[test]
    fn test_group_change_update() {
        round_trip(&ProtocolMessage::GroupChange {
            change_type: GroupChangeType::Update,
            node: RepNode::new(
                "node1".to_string(),
                NodeType::Secondary,
                "192.168.1.1".to_string(),
                7000,
                1,
            ),
        });
    }

    #[test]
    fn test_group_change_response_accepted() {
        round_trip(&ProtocolMessage::GroupChangeResponse { accepted: true });
    }

    #[test]
    fn test_group_change_response_rejected() {
        round_trip(&ProtocolMessage::GroupChangeResponse { accepted: false });
    }

    #[test]
    fn test_election_proposal_round_trip() {
        round_trip(&ProtocolMessage::ElectionProposal {
            node_name: "candidate1".to_string(),
            vlsn: 5000,
            priority: 10,
            term: 3,
        });
    }

    #[test]
    fn test_election_vote_granted() {
        round_trip(&ProtocolMessage::ElectionVote {
            voter: "voter1".to_string(),
            granted: true,
            term: 3,
        });
    }

    #[test]
    fn test_election_vote_denied() {
        round_trip(&ProtocolMessage::ElectionVote {
            voter: "voter2".to_string(),
            granted: false,
            term: 2,
        });
    }

    #[test]
    fn test_election_result_round_trip() {
        round_trip(&ProtocolMessage::ElectionResult {
            master: "new_master".to_string(),
            term: 4,
        });
    }

    #[test]
    fn test_shutdown_round_trip() {
        round_trip(&ProtocolMessage::Shutdown {
            reason: "maintenance window".to_string(),
        });
    }

    #[test]
    fn test_decode_empty_data() {
        let result = ProtocolMessage::decode(&[]);
        assert!(result.is_err());
        match result.unwrap_err() {
            RepError::ProtocolError(msg) => assert!(msg.contains("empty")),
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_decode_unknown_tag() {
        let result = ProtocolMessage::decode(&[255]);
        assert!(result.is_err());
        match result.unwrap_err() {
            RepError::ProtocolError(msg) => {
                assert!(msg.contains("unknown message tag"))
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_decode_truncated_heartbeat() {
        // Tag for heartbeat, but missing payload.
        let result = ProtocolMessage::decode(&[TAG_HEARTBEAT, 0, 0]);
        assert!(result.is_err());
        match result.unwrap_err() {
            RepError::ProtocolError(msg) => {
                assert!(msg.contains("unexpected end"))
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_decode_truncated_string() {
        // Handshake tag + string length says 100 bytes but only 2 provided.
        let mut data = vec![TAG_HANDSHAKE];
        data.extend_from_slice(&100u32.to_le_bytes());
        data.extend_from_slice(b"ab");
        let result = ProtocolMessage::decode(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_produces_non_empty() {
        let msgs = vec![
            ProtocolMessage::Handshake {
                node_name: "n".to_string(),
                group_name: "g".to_string(),
                node_type: NodeType::Electable,
            },
            ProtocolMessage::Ack { vlsn: 0 },
            ProtocolMessage::Shutdown { reason: "done".to_string() },
        ];
        for msg in &msgs {
            assert!(!msg.encode().is_empty());
        }
    }

    #[test]
    fn test_group_change_type_debug() {
        assert_eq!(format!("{:?}", GroupChangeType::Add), "Add");
        assert_eq!(format!("{:?}", GroupChangeType::Remove), "Remove");
        assert_eq!(format!("{:?}", GroupChangeType::Update), "Update");
    }

    #[test]
    fn test_unicode_string_round_trip() {
        round_trip(&ProtocolMessage::Shutdown {
            reason: "arret planifie".to_string(),
        });
    }

    #[test]
    fn test_max_values_round_trip() {
        round_trip(&ProtocolMessage::Heartbeat {
            master_vlsn: u64::MAX,
            timestamp_ms: u64::MAX,
        });
        round_trip(&ProtocolMessage::ElectionProposal {
            node_name: "x".to_string(),
            vlsn: u64::MAX,
            priority: u32::MAX,
            term: u64::MAX,
        });
    }

    #[test]
    fn test_zero_values_round_trip() {
        round_trip(&ProtocolMessage::Heartbeat {
            master_vlsn: 0,
            timestamp_ms: 0,
        });
        round_trip(&ProtocolMessage::Ack { vlsn: 0 });
    }
}
