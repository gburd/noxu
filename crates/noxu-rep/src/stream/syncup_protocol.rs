//! REP-1 STEP 5 (B): the syncup wire protocol (feeder<->replica matchpoint
//! negotiation).
//!
//! Port of the syncup message exchange in `FeederReplicaSyncup.java` /
//! `ReplicaFeederSyncup.java`, using the message set from
//! `BaseProtocol.{EntryRequest, Entry, EntryNotFound, AlternateMatchpoint,
//! StartStream, RestoreRequest, RestoreResponse}`.
//!
//! ## Handshake
//!
//! The replica drives a backward search over its own log (the
//! [`crate::stream::syncup_reader::SyncupLogView`]) and asks the feeder, for
//! each candidate matchpoint VLSN, "do you hold the same record at this VLSN?"
//! (`EntryRequest`). The feeder answers with:
//!   - [`SyncupMsg::Entry`]  — "yes, here is my record at that VLSN"
//!     (JE `Entry`);
//!   - [`SyncupMsg::EntryNotFound`] — "I do not hold that VLSN" (below my
//!     range) (JE `EntryNotFound`); or
//!   - [`SyncupMsg::AlternateMatchpoint`] — "that VLSN is above my range; here
//!     is my highest sync point as a counter-offer" (JE `AlternateMatchpoint`,
//!     only on the first exchange).
//!
//! When the records match (same LSN + fingerprint), the replica sends
//! [`SyncupMsg::StartStream`] with `matchpoint+1` and the two converge.
//! Otherwise the replica scans to its previous sync point and repeats. If the
//! search walks past the replica's contiguous range, the replica sends
//! [`SyncupMsg::RestoreRequest`] and falls back to network restore
//! (`ReplicaFeederSyncup.setupLogRefresh`).
//!
//! This module is the *transport* of that negotiation. The matchpoint
//! *decision* is [`crate::stream::syncup::find_matchpoint`] /
//! [`crate::stream::syncup::verify_rollback`]; the rollback *execution* is
//! REP-1 STEP 5 (C). The driver in [`crate::stream::syncup_reader`] and the
//! wiring in `replicated_environment` glue the three together.

use std::time::Duration;

use noxu_util::{NULL_VLSN, Vlsn};

use crate::error::{RepError, Result};
use crate::net::channel::Channel;
use crate::stream::syncup::{
    Matchpoint, SyncupView, VlsnEntry, find_matchpoint,
};

/// Service name registered with the dispatcher for the syncup handshake.
pub const SYNCUP_SERVICE_NAME: &str = "REP_SYNCUP";

/// Timeout for a single syncup message round-trip.
const SYNCUP_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

/// One syncup-protocol message. Wire form: a 1-byte opcode followed by a
/// fixed body (little-endian). Mirrors the `BaseProtocol` message classes used
/// during syncup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncupMsg {
    /// Replica → feeder: "do you have VLSN X (at LSN Y on my side)?"
    /// (JE `BaseProtocol.EntryRequest`). The LSN/fingerprint are the replica's
    /// own values so the feeder need not be asked to compute equality — the
    /// replica compares the returned [`SyncupMsg::Entry`].
    EntryRequest { vlsn: Vlsn },
    /// Feeder → replica: "here is my record at that VLSN" (JE `Entry`).
    Entry { vlsn: Vlsn, lsn: u64, fingerprint: u64, is_sync: bool },
    /// Feeder → replica: "I do not hold that VLSN" (below my range)
    /// (JE `EntryNotFound`).
    EntryNotFound,
    /// Feeder → replica: "that VLSN is above my range; here is my highest sync
    /// point as a counter-offer" (JE `AlternateMatchpoint`).
    AlternateMatchpoint {
        vlsn: Vlsn,
        lsn: u64,
        fingerprint: u64,
        is_sync: bool,
    },
    /// Replica → feeder: "matchpoint agreed; start streaming from this VLSN"
    /// (JE `StartStream`).
    StartStream { start_vlsn: Vlsn },
    /// Replica → feeder: "no matchpoint; I need a network restore"
    /// (JE `RestoreRequest`).
    RestoreRequest { failed_vlsn: Vlsn },
    /// Feeder → replica: acknowledgement of a restore request
    /// (JE `RestoreResponse`).
    RestoreResponse,
}

// Opcodes.
const OP_ENTRY_REQUEST: u8 = 1;
const OP_ENTRY: u8 = 2;
const OP_ENTRY_NOT_FOUND: u8 = 3;
const OP_ALT_MATCHPOINT: u8 = 4;
const OP_START_STREAM: u8 = 5;
const OP_RESTORE_REQUEST: u8 = 6;
const OP_RESTORE_RESPONSE: u8 = 7;

impl SyncupMsg {
    /// Serialize to wire bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(26);
        match self {
            SyncupMsg::EntryRequest { vlsn } => {
                b.push(OP_ENTRY_REQUEST);
                b.extend_from_slice(&vlsn.sequence().to_le_bytes());
            }
            SyncupMsg::Entry { vlsn, lsn, fingerprint, is_sync } => {
                b.push(OP_ENTRY);
                encode_record(&mut b, *vlsn, *lsn, *fingerprint, *is_sync);
            }
            SyncupMsg::EntryNotFound => b.push(OP_ENTRY_NOT_FOUND),
            SyncupMsg::AlternateMatchpoint {
                vlsn,
                lsn,
                fingerprint,
                is_sync,
            } => {
                b.push(OP_ALT_MATCHPOINT);
                encode_record(&mut b, *vlsn, *lsn, *fingerprint, *is_sync);
            }
            SyncupMsg::StartStream { start_vlsn } => {
                b.push(OP_START_STREAM);
                b.extend_from_slice(&start_vlsn.sequence().to_le_bytes());
            }
            SyncupMsg::RestoreRequest { failed_vlsn } => {
                b.push(OP_RESTORE_REQUEST);
                b.extend_from_slice(&failed_vlsn.sequence().to_le_bytes());
            }
            SyncupMsg::RestoreResponse => b.push(OP_RESTORE_RESPONSE),
        }
        b
    }

    /// Deserialize from wire bytes.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.is_empty() {
            return Err(RepError::ProtocolError(
                "syncup: empty message".into(),
            ));
        }
        let op = buf[0];
        let body = &buf[1..];
        match op {
            OP_ENTRY_REQUEST => {
                Ok(SyncupMsg::EntryRequest { vlsn: read_vlsn(body)? })
            }
            OP_ENTRY => {
                let (vlsn, lsn, fingerprint, is_sync) = decode_record(body)?;
                Ok(SyncupMsg::Entry { vlsn, lsn, fingerprint, is_sync })
            }
            OP_ENTRY_NOT_FOUND => Ok(SyncupMsg::EntryNotFound),
            OP_ALT_MATCHPOINT => {
                let (vlsn, lsn, fingerprint, is_sync) = decode_record(body)?;
                Ok(SyncupMsg::AlternateMatchpoint {
                    vlsn,
                    lsn,
                    fingerprint,
                    is_sync,
                })
            }
            OP_START_STREAM => {
                Ok(SyncupMsg::StartStream { start_vlsn: read_vlsn(body)? })
            }
            OP_RESTORE_REQUEST => {
                Ok(SyncupMsg::RestoreRequest { failed_vlsn: read_vlsn(body)? })
            }
            OP_RESTORE_RESPONSE => Ok(SyncupMsg::RestoreResponse),
            other => Err(RepError::ProtocolError(format!(
                "syncup: unknown opcode {other}"
            ))),
        }
    }
}

fn encode_record(
    b: &mut Vec<u8>,
    vlsn: Vlsn,
    lsn: u64,
    fingerprint: u64,
    is_sync: bool,
) {
    b.extend_from_slice(&vlsn.sequence().to_le_bytes());
    b.extend_from_slice(&lsn.to_le_bytes());
    b.extend_from_slice(&fingerprint.to_le_bytes());
    b.push(is_sync as u8);
}

fn decode_record(body: &[u8]) -> Result<(Vlsn, u64, u64, bool)> {
    if body.len() < 8 + 8 + 8 + 1 {
        return Err(RepError::ProtocolError(format!(
            "syncup: short record body ({} bytes)",
            body.len()
        )));
    }
    let seq = i64::from_le_bytes(body[0..8].try_into().unwrap());
    let lsn = u64::from_le_bytes(body[8..16].try_into().unwrap());
    let fingerprint = u64::from_le_bytes(body[16..24].try_into().unwrap());
    let is_sync = body[24] != 0;
    Ok((Vlsn::new(seq), lsn, fingerprint, is_sync))
}

fn read_vlsn(body: &[u8]) -> Result<Vlsn> {
    if body.len() < 8 {
        return Err(RepError::ProtocolError(format!(
            "syncup: short vlsn body ({} bytes)",
            body.len()
        )));
    }
    Ok(Vlsn::new(i64::from_le_bytes(body[0..8].try_into().unwrap())))
}

// ---------------------------------------------------------------------------
// Outcome of the handshake
// ---------------------------------------------------------------------------

/// Result of the replica's side of the syncup handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncupOutcome {
    /// A matchpoint was agreed at `matchpoint_vlsn` (LSN `matchpoint_lsn`);
    /// the replica must roll back its tail to it (if any) and resume
    /// streaming from `start_vlsn`.
    Matchpoint { matchpoint_vlsn: Vlsn, matchpoint_lsn: u64, start_vlsn: Vlsn },
    /// No common matchpoint; the replica must do a network restore.
    NeedsRestore { failed_vlsn: Vlsn },
}

// ---------------------------------------------------------------------------
// Replica side of the handshake
// ---------------------------------------------------------------------------

/// Run the replica's side of the syncup handshake over `channel`, driven by
/// the replica's local log view.
///
/// Port of `ReplicaFeederSyncup.findMatchpoint`: propose `lastSync`, walk back
/// over the replica's sync points on mismatch, and converge on the highest
/// common matchpoint. The feeder may counter with an `AlternateMatchpoint`
/// (when the replica's first proposal is above the feeder's range), which is
/// honoured if it falls inside the replica's contiguous range.
///
/// Returns [`SyncupOutcome::Matchpoint`] with `start_vlsn = matchpoint+1` on
/// success, or [`SyncupOutcome::NeedsRestore`] if no matchpoint exists.
pub fn replica_syncup_handshake(
    channel: &dyn Channel,
    replica: &dyn SyncupView,
) -> Result<SyncupOutcome> {
    // First candidate is the replica's lastSync (JE range.getLastSync).
    let mut candidate = replica.last_sync();
    let first = replica.first_vlsn();

    if candidate.is_null() {
        // No sync-able entries: ask the feeder for VLSN 1 (JE FIRST_VLSN).
        // If the feeder lacks it, network restore.
        send(channel, &SyncupMsg::EntryRequest { vlsn: Vlsn::new(1) })?;
        return match recv(channel)? {
            SyncupMsg::Entry { vlsn, .. } => Ok(SyncupOutcome::Matchpoint {
                matchpoint_vlsn: NULL_VLSN,
                matchpoint_lsn: 0,
                start_vlsn: vlsn, // start at VLSN 1
            }),
            _ => fall_back_to_restore(channel, Vlsn::new(1)),
        };
    }

    // First exchange accepts an AlternateMatchpoint counter-offer.
    let mut first_exchange = true;

    loop {
        // Look at our own record at the candidate.
        let replica_entry = match replica.entry(candidate) {
            Some(e) => e,
            None => return fall_back_to_restore(channel, candidate),
        };

        send(channel, &SyncupMsg::EntryRequest { vlsn: candidate })?;
        match recv(channel)? {
            SyncupMsg::Entry { vlsn, lsn, fingerprint, .. } => {
                if vlsn == candidate
                    && lsn == replica_entry.lsn
                    && fingerprint == replica_entry.fingerprint
                {
                    // Matchpoint found.
                    return converge(channel, candidate, replica_entry.lsn);
                }
                // Feeder holds this VLSN but the record differs: scan back.
            }
            SyncupMsg::AlternateMatchpoint { vlsn, .. } if first_exchange => {
                // Feeder counter-offers a lower matchpoint. Honour it only if
                // it is inside our contiguous range (JE getFeederRecord).
                if vlsn < first {
                    return fall_back_to_restore(channel, vlsn);
                }
                candidate = vlsn;
                first_exchange = false;
                continue; // re-request at the counter-offer
            }
            SyncupMsg::EntryNotFound => {
                return fall_back_to_restore(channel, candidate);
            }
            other => {
                return Err(RepError::ProtocolError(format!(
                    "syncup replica: unexpected response {other:?}"
                )));
            }
        }
        first_exchange = false;

        // No match at this candidate: scan to our previous sync point.
        match prev_sync(replica, candidate, first) {
            Some(prev) => candidate = prev,
            None => return fall_back_to_restore(channel, candidate),
        }
    }
}

/// Send StartStream and return a Matchpoint outcome.
fn converge(
    channel: &dyn Channel,
    matchpoint_vlsn: Vlsn,
    matchpoint_lsn: u64,
) -> Result<SyncupOutcome> {
    let start_vlsn = matchpoint_vlsn.next();
    send(channel, &SyncupMsg::StartStream { start_vlsn })?;
    Ok(SyncupOutcome::Matchpoint {
        matchpoint_vlsn,
        matchpoint_lsn,
        start_vlsn,
    })
}

/// Send RestoreRequest and return a NeedsRestore outcome (consuming the
/// feeder's RestoreResponse if it sends one).
fn fall_back_to_restore(
    channel: &dyn Channel,
    failed_vlsn: Vlsn,
) -> Result<SyncupOutcome> {
    send(channel, &SyncupMsg::RestoreRequest { failed_vlsn })?;
    // Best-effort: the feeder replies RestoreResponse; ignore receive errors
    // (the channel may already be closing).
    let _ = recv(channel);
    Ok(SyncupOutcome::NeedsRestore { failed_vlsn })
}

fn prev_sync(
    replica: &dyn SyncupView,
    from: Vlsn,
    first: Vlsn,
) -> Option<Vlsn> {
    let mut v = from.prev();
    while !v.is_null() && v >= first {
        if let Some(e) = replica.entry(v)
            && e.is_sync
        {
            return Some(v);
        }
        v = v.prev();
    }
    None
}

// ---------------------------------------------------------------------------
// Feeder side of the handshake
// ---------------------------------------------------------------------------

/// Run the feeder's side of the syncup handshake over `channel`, answering the
/// replica's `EntryRequest`s from the feeder's local log view.
///
/// Port of `FeederReplicaSyncup.execute` /`makeResponseToEntryRequest`:
///   - VLSN in range → [`SyncupMsg::Entry`];
///   - VLSN below range → [`SyncupMsg::EntryNotFound`];
///   - VLSN above range (first request only) → [`SyncupMsg::AlternateMatchpoint`]
///     with the feeder's `lastSync`.
///
/// Loops until the replica sends `StartStream` (returns the agreed start VLSN)
/// or `RestoreRequest` (returns `None`, network restore).
pub fn feeder_syncup_handshake(
    channel: &dyn Channel,
    feeder: &dyn SyncupView,
) -> Result<Option<Vlsn>> {
    let mut first_response = true;
    loop {
        let msg = recv(channel)?;
        match msg {
            SyncupMsg::EntryRequest { vlsn } => {
                let response =
                    make_entry_response(feeder, vlsn, first_response);
                first_response = false;
                send(channel, &response)?;
            }
            SyncupMsg::StartStream { start_vlsn } => {
                return Ok(Some(start_vlsn));
            }
            SyncupMsg::RestoreRequest { .. } => {
                send(channel, &SyncupMsg::RestoreResponse)?;
                return Ok(None);
            }
            other => {
                return Err(RepError::ProtocolError(format!(
                    "syncup feeder: unexpected request {other:?}"
                )));
            }
        }
    }
}

/// Build the feeder's response to an `EntryRequest`. JE
/// `FeederReplicaSyncup.makeResponseToEntryRequest` (DEFAULT mode).
fn make_entry_response(
    feeder: &dyn SyncupView,
    request_vlsn: Vlsn,
    is_first_response: bool,
) -> SyncupMsg {
    let first = feeder.first_vlsn();
    let last_sync = feeder.last_sync();

    // Below the feeder's range → EntryNotFound.
    if !first.is_null() && request_vlsn < first {
        return SyncupMsg::EntryNotFound;
    }

    // In range and held → Entry.
    if let Some(e) = feeder.entry(request_vlsn) {
        return SyncupMsg::Entry {
            vlsn: request_vlsn,
            lsn: e.lsn,
            fingerprint: e.fingerprint,
            is_sync: e.is_sync,
        };
    }

    // Above the feeder's range (not held, not below first): on the first
    // response, counter-offer the feeder's lastSync (JE AlternateMatchpoint).
    if is_first_response
        && !last_sync.is_null()
        && let Some(e) = feeder.entry(last_sync)
    {
        return SyncupMsg::AlternateMatchpoint {
            vlsn: last_sync,
            lsn: e.lsn,
            fingerprint: e.fingerprint,
            is_sync: e.is_sync,
        };
    }

    SyncupMsg::EntryNotFound
}

// ---------------------------------------------------------------------------
// Convenience: build a feeder view that answers from find_matchpoint logic
// ---------------------------------------------------------------------------

/// Compute the agreed matchpoint by running the replica handshake against a
/// LOCAL feeder view (no channel) — the in-process fast path used when both
/// nodes share an address space (the test harness). Equivalent in result to
/// running [`replica_syncup_handshake`] + [`feeder_syncup_handshake`] over a
/// real channel.
pub fn local_matchpoint(
    replica: &dyn SyncupView,
    feeder: &dyn SyncupView,
) -> Matchpoint {
    find_matchpoint(replica, feeder)
}

// ---------------------------------------------------------------------------
// Framed send/recv over the rep Channel
// ---------------------------------------------------------------------------

fn send(channel: &dyn Channel, msg: &SyncupMsg) -> Result<()> {
    channel.send(&msg.encode())
}

fn recv(channel: &dyn Channel) -> Result<SyncupMsg> {
    let frame = channel.receive(SYNCUP_TIMEOUT)?.ok_or_else(|| {
        RepError::NetworkError("syncup: no message received".into())
    })?;
    SyncupMsg::decode(&frame)
}

/// Build a [`VlsnEntry`] (helper for callers constructing feeder views).
pub fn vlsn_entry(lsn: u64, fingerprint: u64, is_sync: bool) -> VlsnEntry {
    VlsnEntry { lsn, fingerprint, is_sync }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::channel::LocalChannelPair;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct MapView {
        last_sync: Vlsn,
        last_txn_end: Vlsn,
        first: Vlsn,
        entries: HashMap<i64, VlsnEntry>,
    }
    impl MapView {
        fn new(first: i64, last_sync: i64, last_txn_end: i64) -> Self {
            Self {
                last_sync: Vlsn::new(last_sync),
                last_txn_end: Vlsn::new(last_txn_end),
                first: Vlsn::new(first),
                entries: HashMap::new(),
            }
        }
        fn put(mut self, v: i64, lsn: u64, fp: u64, sync: bool) -> Self {
            self.entries
                .insert(v, VlsnEntry { lsn, fingerprint: fp, is_sync: sync });
            self
        }
    }
    impl SyncupView for MapView {
        fn last_sync(&self) -> Vlsn {
            self.last_sync
        }
        fn last_txn_end(&self) -> Vlsn {
            self.last_txn_end
        }
        fn first_vlsn(&self) -> Vlsn {
            self.first
        }
        fn entry(&self, vlsn: Vlsn) -> Option<VlsnEntry> {
            self.entries.get(&vlsn.sequence()).copied()
        }
    }

    #[test]
    fn test_msg_roundtrip() {
        let msgs = vec![
            SyncupMsg::EntryRequest { vlsn: Vlsn::new(7) },
            SyncupMsg::Entry {
                vlsn: Vlsn::new(7),
                lsn: 0x1234,
                fingerprint: 0xABCD,
                is_sync: true,
            },
            SyncupMsg::EntryNotFound,
            SyncupMsg::AlternateMatchpoint {
                vlsn: Vlsn::new(5),
                lsn: 0x500,
                fingerprint: 0x55,
                is_sync: false,
            },
            SyncupMsg::StartStream { start_vlsn: Vlsn::new(8) },
            SyncupMsg::RestoreRequest { failed_vlsn: Vlsn::new(3) },
            SyncupMsg::RestoreResponse,
        ];
        for m in msgs {
            assert_eq!(SyncupMsg::decode(&m.encode()).unwrap(), m);
        }
    }

    /// Full handshake over a LocalChannel: a diverged replica (VLSN 6/7 differ
    /// from the feeder) converges on the highest common matchpoint (VLSN 4).
    #[test]
    fn test_handshake_diverged_converges() {
        let pair = LocalChannelPair::new();
        let replica_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let feeder_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Replica: sync points at 6 (divergent) and 4 (common); 5 non-sync.
        let replica = MapView::new(1, 6, 6)
            .put(6, 0x600, 0xDEAD, true)
            .put(5, 0x500, 0x55, false)
            .put(4, 0x400, 0x44, true);
        // Feeder: holds 8,6(diff),4(same).
        let feeder = MapView::new(1, 8, 8)
            .put(8, 0x800, 0x88, true)
            .put(6, 0x600, 0xBEEF, true)
            .put(4, 0x400, 0x44, true);

        let feeder_handle = std::thread::spawn(move || {
            feeder_syncup_handshake(feeder_ch.as_ref(), &feeder)
        });

        let outcome =
            replica_syncup_handshake(replica_ch.as_ref(), &replica).unwrap();
        assert_eq!(
            outcome,
            SyncupOutcome::Matchpoint {
                matchpoint_vlsn: Vlsn::new(4),
                matchpoint_lsn: 0x400,
                start_vlsn: Vlsn::new(5),
            }
        );
        let feeder_start = feeder_handle.join().unwrap().unwrap();
        assert_eq!(feeder_start, Some(Vlsn::new(5)));
    }

    /// Replica's first proposal is above the feeder's range; feeder counters
    /// with an AlternateMatchpoint (its lastSync=8) which the replica adopts.
    #[test]
    fn test_handshake_alternate_matchpoint() {
        let pair = LocalChannelPair::new();
        let replica_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let feeder_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Replica has 1..=10, lastSync=10. Feeder only has 1..=8, lastSync=8.
        let mut replica = MapView::new(1, 10, 10);
        for v in 1..=10 {
            replica = replica.put(v, (v as u64) * 0x100, v as u64, true);
        }
        let mut feeder = MapView::new(1, 8, 8);
        for v in 1..=8 {
            feeder = feeder.put(v, (v as u64) * 0x100, v as u64, true);
        }

        let feeder_handle = std::thread::spawn(move || {
            feeder_syncup_handshake(feeder_ch.as_ref(), &feeder)
        });
        let outcome =
            replica_syncup_handshake(replica_ch.as_ref(), &replica).unwrap();
        // VLSN 8 record matches on both sides → matchpoint 8, start 9.
        assert_eq!(
            outcome,
            SyncupOutcome::Matchpoint {
                matchpoint_vlsn: Vlsn::new(8),
                matchpoint_lsn: 0x800,
                start_vlsn: Vlsn::new(9),
            }
        );
        assert_eq!(feeder_handle.join().unwrap().unwrap(), Some(Vlsn::new(9)));
    }

    /// No common matchpoint → replica requests network restore.
    #[test]
    fn test_handshake_no_matchpoint_restore() {
        let pair = LocalChannelPair::new();
        let replica_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let feeder_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Replica's records all differ from the feeder's.
        let replica = MapView::new(4, 6, 6)
            .put(6, 0x600, 0x11, true)
            .put(5, 0x500, 0x22, true)
            .put(4, 0x400, 0x33, true);
        let feeder = MapView::new(1, 8, 8)
            .put(8, 0x800, 0x88, true)
            .put(6, 0x600, 0x99, true)
            .put(5, 0x500, 0x88, true)
            .put(4, 0x400, 0x77, true);

        let feeder_handle = std::thread::spawn(move || {
            feeder_syncup_handshake(feeder_ch.as_ref(), &feeder)
        });
        let outcome =
            replica_syncup_handshake(replica_ch.as_ref(), &replica).unwrap();
        assert!(matches!(outcome, SyncupOutcome::NeedsRestore { .. }));
        assert_eq!(feeder_handle.join().unwrap().unwrap(), None);
    }
}
