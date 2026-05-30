//! Replica stream  -  replica-side replication receiver.
//!
//! Tracks the state of
//! receiving replication data from the master, including pending entries,
//! applied/received VLSNs, and the master's latest known VLSN.
//!
//! The [`ReplicaReceiver`] provides the active I/O loop that reads framed
//! entries from the feeder channel, passes them to a [`LogWriter`], and sends
//! acks back.
//!
//! [`EnvironmentLogWriter`] is the live implementation of [`LogWriter`] that
//! writes replicated entries into the local `LogManager` and updates the
//! VLSN index.

use crate::log::{LogEntryType, Provisional};
use crate::sync::Mutex;
use std::sync::Arc;
use std::time::Duration;

use crate::rep::error::{RepError, Result};
use crate::rep::net::channel::Channel;

// CRC32 (Ethernet/zlib polynomial) for per-frame integrity verification.
// Same polynomial used in feeder.rs; see docs/checksum-selection.md.
use crc32fast::hash as crc32_hash;

// ---------------------------------------------------------------------------
// LogWriter trait
// ---------------------------------------------------------------------------

/// Sink for replicated log entries.
///
/// Corresponds to replay thread accepting log records and writing them
/// to the local environment. The replica calls `write_entry` for every entry
/// received from the master.
pub trait LogWriter: Send {
    /// Write a replicated log entry.
    ///
    /// # Arguments
    /// * `vlsn` - The VLSN of this entry.
    /// * `entry_type` - The log entry type byte.
    /// * `payload` - The raw entry payload.
    fn write_entry(
        &mut self,
        vlsn: u64,
        entry_type: u8,
        payload: &[u8],
    ) -> Result<()>;
}

// ---------------------------------------------------------------------------
// EnvironmentLogWriter
// ---------------------------------------------------------------------------

/// `LogWriter` implementation backed by the live `LogManager`.
///
/// Each `write_entry` call:
///   1. Resolves the `entry_type` byte to a `LogEntryType`.
///   2. Writes the payload to the local log via `LogManager::log()`.
///   3. Registers the returned LSN in the provided `vlsn_index` so that
///      the VLSN→LSN mapping is kept up-to-date on the replica.
///
///
pub struct EnvironmentLogWriter {
    /// Shared log manager for appending replicated entries.
    log_manager: Arc<crate::log::LogManager>,
    /// VLSN index: maps VLSN → (file_number, file_offset) on this replica.
    vlsn_index: Arc<crate::rep::vlsn::vlsn_index::VlsnIndex>,
}

impl EnvironmentLogWriter {
    /// Create a new `EnvironmentLogWriter`.
    ///
    /// # Arguments
    /// * `log_manager` — The live `LogManager` for this replica environment.
    /// * `vlsn_index`  — The VLSN index to update after each written entry.
    pub fn new(
        log_manager: Arc<crate::log::LogManager>,
        vlsn_index: Arc<crate::rep::vlsn::vlsn_index::VlsnIndex>,
    ) -> Self {
        Self { log_manager, vlsn_index }
    }
}

impl LogWriter for EnvironmentLogWriter {
    /// Write one replicated entry to the local log.
    ///
    /// Resolves `entry_type` → `LogEntryType`, appends the payload to the
    /// WAL, and records the assigned LSN in the VLSN index.  Returns an
    /// error if the entry type is unknown or the write fails.
    fn write_entry(
        &mut self,
        vlsn: u64,
        entry_type: u8,
        payload: &[u8],
    ) -> crate::rep::error::Result<()> {
        // Resolve the wire entry-type byte to the typed enum.
        let log_entry_type = LogEntryType::from_type_num(entry_type)
            .ok_or_else(|| {
                crate::rep::error::RepError::ProtocolError(format!(
                    "replica: unknown entry_type byte {}",
                    entry_type
                ))
            })?;

        // Write to the local WAL.  Replicated entries are non-provisional and
        // do not require an immediate fsync on every entry (the master already
        // fsynced before sending).
        let lsn = self
            .log_manager
            .log(log_entry_type, payload, Provisional::No, false, false)
            .map_err(|e| {
                crate::rep::error::RepError::DatabaseError(format!(
                    "replica log write failed: {}",
                    e
                ))
            })?;

        // Register VLSN → LSN in the replica's VLSN index so that
        // FeederRunner/ack tracking can correlate positions.
        // vlsn=0 is reserved as NULL_VLSN; skip it.
        if vlsn > 0 {
            self.vlsn_index.put(vlsn, lsn.file_number(), lsn.file_offset());
        }

        log::trace!(
            "replica: wrote entry vlsn={} type={} lsn=({},{})",
            vlsn,
            log_entry_type,
            lsn.file_number(),
            lsn.file_offset(),
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ReplicaReceiver
// ---------------------------------------------------------------------------

/// Wire frame header size (matches FeederRunner::FRAME_HEADER_LEN).
///
/// Format: `[vlsn:8][type:1][payload_len:4][crc32:4]` = 17 bytes.
const FRAME_HEADER_LEN: usize = 8 + 1 + 4 + 4; // vlsn + type + len + crc32

/// Active replica I/O loop.
///
/// `ReplicaReceiver` owns a channel to the master feeder. `run()` is a
/// blocking loop that:
///   1. Reads framed entries from the feeder.
///   2. Deserializes each entry: `[vlsn:8][type:1][len:4][crc32:4][payload]`.
///   3. Verifies `crc32fast::hash(payload) == crc32` — returns
///      [`RepError::FrameCorrupted`] on mismatch.
///   4. Passes the entry to `log_writer`.
///   5. Sends an 8-byte LE VLSN ack back to the master.
///   6. Returns when the channel is closed or an I/O error occurs.
///
/// Read thread and replay thread in the replica.
pub struct ReplicaReceiver {
    /// Channel to the master feeder.
    channel: Arc<dyn Channel>,
}

impl ReplicaReceiver {
    /// Create a new `ReplicaReceiver` on the given channel.
    pub fn new(channel: Arc<dyn Channel>) -> Self {
        Self { channel }
    }

    /// Run the replica receive loop.
    ///
    /// Blocks until the channel is closed or an unrecoverable error occurs.
    /// Each successfully received entry is passed to `log_writer`; then an
    /// ack `[vlsn: 8 bytes LE]` is sent back to the master.
    ///
    /// # Anti-replay / VLSN-ordering enforcement (LOG-7)
    ///
    /// The replica MUST observe strictly-increasing VLSNs across the
    /// connection.  Without this check the master could (accidentally
    /// or maliciously) replay an old frame, causing the replica to ack a
    /// VLSN it had already applied and silently overwrite a more-recent
    /// committed value.  We track a `received_vlsn` high-water mark and
    /// reject any incoming frame whose VLSN is `<= high-water` with a
    /// [`RepError::ProtocolError`].  Gaps above the high-water mark are
    /// allowed because the master is permitted to skip non-replicated
    /// entries.
    ///
    /// # Entry-type validation (LOG-10)
    ///
    /// The wire-format entry-type byte is also validated against the set
    /// of known [`LogEntryType`] variants before forwarding to
    /// `log_writer`.  An unknown byte indicates either a protocol-version
    /// skew or an attacker who flipped a header bit, and is logged at
    /// `error` level then skipped (the connection is kept open so the
    /// stream can recover from a single bad frame; a repeated stream of
    /// unknowns will surface via the operator alerting on the error log).
    pub fn run(&self, log_writer: &mut dyn LogWriter) -> Result<()> {
        let recv_timeout = Duration::from_secs(30);
        // LOG-7: strictly-increasing VLSN high-water mark.  0 == NULL_VLSN
        // (never assigned by the master), so "<= high-water" rejects 0
        // too once a real VLSN has arrived.
        let mut received_vlsn_high_water: u64 = 0;

        loop {
            // ----------------------------------------------------------------
            // Receive the next framed entry from the feeder.
            // ----------------------------------------------------------------
            let frame = match self.channel.receive(recv_timeout) {
                Ok(Some(f)) => f,
                Ok(None) => {
                    // Timeout with no data — keep waiting.
                    continue;
                }
                Err(RepError::ChannelClosed(_)) => {
                    // Master disconnected; clean shutdown.
                    return Ok(());
                }
                Err(e) => return Err(e),
            };

            // ----------------------------------------------------------------
            // Parse frame: [vlsn:8 LE][entry_type:1][payload_len:4 LE]
            //              [crc32:4 LE][payload]
            // ----------------------------------------------------------------
            if frame.len() < FRAME_HEADER_LEN {
                return Err(RepError::ProtocolError(format!(
                    "replica: short frame: {} bytes",
                    frame.len()
                )));
            }

            let vlsn = u64::from_le_bytes(frame[0..8].try_into().unwrap());
            let entry_type = frame[8];
            let payload_len =
                u32::from_le_bytes(frame[9..13].try_into().unwrap()) as usize;
            let expected_crc =
                u32::from_le_bytes(frame[13..17].try_into().unwrap());

            if frame.len() < FRAME_HEADER_LEN + payload_len {
                return Err(RepError::ProtocolError(format!(
                    "replica: frame payload truncated: expected {} bytes, got {}",
                    payload_len,
                    frame.len() - FRAME_HEADER_LEN,
                )));
            }

            let payload =
                &frame[FRAME_HEADER_LEN..FRAME_HEADER_LEN + payload_len];

            // ----------------------------------------------------------------
            // Verify CRC32 — reject corrupted frames before applying.
            // ----------------------------------------------------------------
            let actual_crc = crc32_hash(payload);
            if actual_crc != expected_crc {
                return Err(RepError::FrameCorrupted {
                    vlsn,
                    expected: expected_crc,
                    actual: actual_crc,
                });
            }

            // ----------------------------------------------------------------
            // LOG-7: enforce strictly-increasing VLSN order.
            //
            // VLSN 0 from the master is the NULL VLSN sentinel and is not
            // checked against the high-water mark (the feeder sends 0 for
            // entries that do not carry a VLSN).  All non-zero VLSNs MUST
            // strictly exceed the previous high-water; otherwise we have
            // either a replayed frame or a master that re-used a sequence
            // number — both are protocol-fatal.
            // ----------------------------------------------------------------
            if vlsn != 0 && vlsn <= received_vlsn_high_water {
                return Err(RepError::ProtocolError(format!(
                    "replica: VLSN ordering violation: incoming vlsn={vlsn} \
                     <= received high-water {received_vlsn_high_water}; \
                     possible replay attack or master clock-skew"
                )));
            }

            // ----------------------------------------------------------------
            // LOG-10: validate the entry-type byte against the catalog
            // before forwarding to the log writer.  An unknown type is
            // logged and the frame is skipped — but the connection stays
            // open so a single corrupt frame does not stall replication.
            // ----------------------------------------------------------------
            if LogEntryType::from_type_num(entry_type).is_none() {
                log::error!(
                    "replica: unknown entry_type byte {entry_type} on frame \
                     vlsn={vlsn}; skipping (LOG-10)"
                );
                // Skip this frame: do not advance high-water, do not ack.
                // The master will retransmit if the replica disconnects;
                // for now we just continue to the next frame.
                continue;
            }

            // ----------------------------------------------------------------
            // Apply the entry to the local log.
            // ----------------------------------------------------------------
            log_writer.write_entry(vlsn, entry_type, payload)?;

            // Advance the high-water mark only after a successful apply.
            if vlsn != 0 {
                received_vlsn_high_water = vlsn;
            }

            // ----------------------------------------------------------------
            // Send ack: [vlsn: 8 bytes LE]
            // ----------------------------------------------------------------
            let ack = vlsn.to_le_bytes();
            match self.channel.send(&ack) {
                Ok(()) => {}
                Err(RepError::ChannelClosed(_)) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}

/// The state of the replica's replication stream.
///
/// Replica state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaStreamState {
    /// Not connected to any master.
    Idle,
    /// Establishing connection to the master.
    Connecting,
    /// Actively receiving replication data.
    Streaming,
    /// Replaying log entries to catch up with the master.
    CatchingUp,
    /// Shutting down.
    Shutdown,
}

/// Tracks the state of receiving replication data from the master.
///
/// The replica stream receives log entries from the master, buffers them
/// in a pending queue, and tracks which VLSNs have been received vs.
/// applied to the local database.
///
///
pub struct ReplicaStream {
    /// Name of the master we are connected to.
    master_name: Mutex<Option<String>>,
    /// Current connection state.
    state: Mutex<ReplicaStreamState>,
    /// Last VLSN applied to the local database.
    applied_vlsn: Mutex<u64>,
    /// Last VLSN received from the master.
    received_vlsn: Mutex<u64>,
    /// Master's latest known VLSN (from heartbeat messages).
    master_vlsn: Mutex<u64>,
    /// Pending entries waiting to be applied: (vlsn, entry_type, data).
    pending_entries: Mutex<Vec<(u64, u8, Vec<u8>)>>,
}

impl Default for ReplicaStream {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaStream {
    /// Create a new replica stream in the idle state.
    pub fn new() -> Self {
        ReplicaStream {
            master_name: Mutex::new(None),
            state: Mutex::new(ReplicaStreamState::Idle),
            applied_vlsn: Mutex::new(0),
            received_vlsn: Mutex::new(0),
            master_vlsn: Mutex::new(0),
            pending_entries: Mutex::new(Vec::new()),
        }
    }

    /// Return the current stream state.
    pub fn get_state(&self) -> ReplicaStreamState {
        *self.state.lock()
    }

    /// Set the stream state.
    pub fn set_state(&self, state: ReplicaStreamState) {
        *self.state.lock() = state;
    }

    /// Return the last VLSN that has been applied to the local database.
    pub fn get_applied_vlsn(&self) -> u64 {
        *self.applied_vlsn.lock()
    }

    /// Return the last VLSN received from the master (may be ahead of
    /// `applied_vlsn` if entries are buffered).
    pub fn get_received_vlsn(&self) -> u64 {
        *self.received_vlsn.lock()
    }

    /// Return the master's latest known VLSN (from heartbeat updates).
    pub fn get_master_vlsn(&self) -> u64 {
        *self.master_vlsn.lock()
    }

    /// Set the master node name.
    pub fn set_master(&self, name: &str) {
        *self.master_name.lock() = Some(name.to_string());
    }

    /// Return the master node name, if set.
    pub fn get_master(&self) -> Option<String> {
        self.master_name.lock().clone()
    }

    /// Receive a log entry from the master.
    ///
    /// The entry is added to the pending queue and `received_vlsn` is
    /// updated if the new VLSN is greater than the current value.
    pub fn receive_entry(&self, vlsn: u64, entry_type: u8, data: Vec<u8>) {
        self.pending_entries.lock().push((vlsn, entry_type, data));
        let mut received = self.received_vlsn.lock();
        if vlsn > *received {
            *received = vlsn;
        }
    }

    /// Mark a VLSN as applied to the local database.
    ///
    /// The `applied_vlsn` is updated if the new VLSN is greater than the
    /// current value (applied VLSNs should advance monotonically).
    pub fn mark_applied(&self, vlsn: u64) {
        let mut applied = self.applied_vlsn.lock();
        if vlsn > *applied {
            *applied = vlsn;
        }
    }

    /// Update the master's known latest VLSN (typically from a heartbeat
    /// message).
    pub fn update_master_vlsn(&self, vlsn: u64) {
        let mut master = self.master_vlsn.lock();
        if vlsn > *master {
            *master = vlsn;
        }
    }

    /// Return the replication lag: the difference between the master's
    /// latest VLSN and the last applied VLSN.
    ///
    /// A lag of 0 means the replica is fully caught up with the master.
    pub fn get_lag(&self) -> u64 {
        let master = *self.master_vlsn.lock();
        let applied = *self.applied_vlsn.lock();
        master.saturating_sub(applied)
    }

    /// Drain all pending entries for processing.
    ///
    /// Returns the entries in the order they were received and leaves
    /// the pending queue empty.
    pub fn drain_pending(&self) -> Vec<(u64, u8, Vec<u8>)> {
        let mut pending = self.pending_entries.lock();
        std::mem::take(&mut *pending)
    }

    /// Check if the replica is caught up with the master.
    ///
    /// Returns `true` if **all** of the following are true:
    /// - the master VLSN is non-zero (i.e. at least one master VLSN has
    ///   been observed; a stream that has never received a master VLSN
    ///   reports `false`),
    /// - the applied VLSN equals or exceeds the master's latest known
    ///   VLSN, and
    /// - there are no pending entries.
    pub fn is_caught_up(&self) -> bool {
        let applied = *self.applied_vlsn.lock();
        let master = *self.master_vlsn.lock();
        let pending_empty = self.pending_entries.lock().is_empty();
        applied >= master && master > 0 && pending_empty
    }
}

impl std::fmt::Debug for ReplicaStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaStream")
            .field("master", &self.get_master())
            .field("state", &self.get_state())
            .field("applied_vlsn", &self.get_applied_vlsn())
            .field("received_vlsn", &self.get_received_vlsn())
            .field("master_vlsn", &self.get_master_vlsn())
            .field("lag", &self.get_lag())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rep::net::channel::LocalChannelPair;

    // -----------------------------------------------------------------------
    // LogWriter helper
    // -----------------------------------------------------------------------

    struct RecordingWriter {
        entries: Vec<(u64, u8, Vec<u8>)>,
    }

    impl RecordingWriter {
        fn new() -> Self {
            Self { entries: Vec::new() }
        }
    }

    impl LogWriter for RecordingWriter {
        fn write_entry(
            &mut self,
            vlsn: u64,
            entry_type: u8,
            payload: &[u8],
        ) -> Result<()> {
            self.entries.push((vlsn, entry_type, payload.to_vec()));
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // ReplicaReceiver tests
    // -----------------------------------------------------------------------

    fn make_frame(vlsn: u64, entry_type: u8, payload: &[u8]) -> Vec<u8> {
        let crc = crc32_hash(payload);
        let mut f = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
        f.extend_from_slice(&vlsn.to_le_bytes());
        f.push(entry_type);
        f.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        f.extend_from_slice(&crc.to_le_bytes());
        f.extend_from_slice(payload);
        f
    }

    #[test]
    fn test_replica_receiver_receives_and_acks() {
        let pair = LocalChannelPair::new();
        let master_side: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let replica_side: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Send 3 frames from the "master" side.
        let frames = vec![
            make_frame(1, 10, &[0xAA]),
            make_frame(2, 20, &[0xBB, 0xCC]),
            make_frame(3, 30, &[]),
        ];

        let master_clone = Arc::clone(&master_side);
        let send_handle = std::thread::spawn(move || {
            for f in &frames {
                master_clone.send(f).unwrap();
            }
            // Collect 3 acks.
            let mut acked = Vec::new();
            let timeout = Duration::from_secs(5);
            for _ in 0..3 {
                let ack = master_clone.receive(timeout).unwrap().unwrap();
                let vlsn = u64::from_le_bytes(ack[..8].try_into().unwrap());
                acked.push(vlsn);
            }
            // Close the channel to terminate the receiver loop.
            master_clone.close().unwrap();
            acked
        });

        let receiver = ReplicaReceiver::new(Arc::clone(&replica_side));
        let mut writer = RecordingWriter::new();
        receiver.run(&mut writer).unwrap();

        let acked = send_handle.join().unwrap();
        assert_eq!(acked, vec![1, 2, 3]);

        assert_eq!(writer.entries.len(), 3);
        assert_eq!(writer.entries[0], (1, 10, vec![0xAA]));
        assert_eq!(writer.entries[1], (2, 20, vec![0xBB, 0xCC]));
        assert_eq!(writer.entries[2], (3, 30, vec![]));
    }

    #[test]
    fn test_replica_receiver_stops_on_channel_close() {
        let pair = LocalChannelPair::new();
        let master_side: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let replica_side: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Close the master side immediately; receiver should return Ok.
        master_side.close().unwrap();

        let receiver = ReplicaReceiver::new(replica_side);
        let mut writer = RecordingWriter::new();
        // The replica_side's receive will fail with ChannelClosed since
        // it is still "open" but the master closed its end. The LocalChannel
        // implementation returns None on timeout (closed endpoint) then the
        // RecvError causes ChannelClosed. Let's just verify it terminates.
        // We accept either Ok or ChannelClosed here.
        let res = receiver.run(&mut writer);
        assert!(res.is_ok() || matches!(res, Err(RepError::ChannelClosed(_))));
    }

    // -----------------------------------------------------------------------
    // Feeder → Replica round-trip test using LocalChannel
    // -----------------------------------------------------------------------

    #[test]
    fn test_feeder_to_replica_round_trip() {
        use crate::rep::stream::feeder::{FeederRunner, LogScanner};
        use std::collections::VecDeque;

        struct SimpleScanner {
            items: VecDeque<(u64, u8, Vec<u8>)>,
        }
        impl LogScanner for SimpleScanner {
            fn next_entry(
                &mut self,
                from_vlsn: u64,
            ) -> Option<(u64, u8, Vec<u8>)> {
                if let Some(&(v, _, _)) = self.items.front()
                    && v >= from_vlsn
                {
                    return self.items.pop_front();
                }
                None
            }
        }

        let pair = LocalChannelPair::new();
        let master_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let replica_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Entries to replicate.  Entry-type bytes must be valid
        // `LogEntryType` variants (validated by LOG-10 enforcement); pick
        // FileHeader/IN/BIN/BINDelta/InsertLN.
        let valid_types: [u8; 5] = [1, 2, 3, 4, 10];
        let entries: Vec<(u64, u8, Vec<u8>)> = (1..=5)
            .map(|i| {
                let etype = valid_types[(i - 1) as usize];
                (i, etype, vec![i as u8; i as usize])
            })
            .collect();

        // Replica thread.
        let replica_ch_clone = Arc::clone(&replica_ch);
        let replica_handle = std::thread::spawn(move || {
            let receiver = ReplicaReceiver::new(replica_ch_clone);
            let mut writer = RecordingWriter::new();
            receiver.run(&mut writer).unwrap();
            writer.entries
        });

        // Feeder thread.
        let master_ch_clone = Arc::clone(&master_ch);
        let feeder_handle = std::thread::spawn(move || {
            let runner = FeederRunner::new(Arc::clone(&master_ch_clone), 1);
            let mut scanner =
                SimpleScanner { items: entries.into_iter().collect() };
            runner.run(&mut scanner).unwrap();
            runner.known_replica_vlsn()
        });

        // Let them run briefly, then close the channel.
        std::thread::sleep(Duration::from_millis(200));
        master_ch.close().unwrap();
        replica_ch.close().unwrap();

        let last_acked = feeder_handle.join().unwrap();
        let written = replica_handle.join().unwrap();

        assert_eq!(written.len(), 5);
        for (i, (vlsn, etype, payload)) in written.iter().enumerate() {
            let expected_vlsn = (i + 1) as u64;
            assert_eq!(*vlsn, expected_vlsn);
            assert_eq!(*etype, valid_types[i]);
            assert_eq!(payload.len(), expected_vlsn as usize);
        }
        assert_eq!(last_acked, 5);
    }

    /// LOG-7: a replayed (or out-of-order) frame whose VLSN is `<=` the
    /// already-received high-water mark must be rejected with
    /// [`RepError::ProtocolError`].  Without this check the master could
    /// re-send a previously-acked frame and the replica would silently
    /// overwrite a more-recent committed value.
    #[test]
    fn test_replica_rejects_replayed_vlsn() {
        let pair = LocalChannelPair::new();
        let master_side: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let replica_side: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Send VLSN=5 then VLSN=3 (replay).  Entry-type 10 = InsertLN.
        let frames =
            vec![make_frame(5, 10, b"first"), make_frame(3, 10, b"replay")];

        let master_clone = Arc::clone(&master_side);
        let _send_handle = std::thread::spawn(move || {
            for f in &frames {
                let _ = master_clone.send(f);
            }
            // Drain the ack for the first frame so the receiver can advance.
            let _ = master_clone.receive(Duration::from_secs(2));
        });

        let receiver = ReplicaReceiver::new(replica_side);
        let mut writer = RecordingWriter::new();
        let res = receiver.run(&mut writer);

        match res {
            Err(RepError::ProtocolError(msg)) => {
                assert!(
                    msg.contains("VLSN ordering violation"),
                    "expected VLSN-ordering protocol error, got: {msg}"
                );
            }
            other => {
                panic!("expected ProtocolError on replay, got {other:?}")
            }
        }

        // The first (in-order) frame should have been applied; the
        // replayed one MUST NOT be.
        assert_eq!(writer.entries.len(), 1);
        assert_eq!(writer.entries[0].0, 5);
    }

    /// LOG-7: equal VLSNs are also rejected (the rule is *strictly*
    /// increasing).
    #[test]
    fn test_replica_rejects_duplicate_vlsn() {
        let pair = LocalChannelPair::new();
        let master_side: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let replica_side: Arc<dyn Channel> = Arc::new(pair.channel_b);

        let frames = vec![make_frame(7, 10, b"a"), make_frame(7, 10, b"b")];

        let master_clone = Arc::clone(&master_side);
        let _send_handle = std::thread::spawn(move || {
            for f in &frames {
                let _ = master_clone.send(f);
            }
            let _ = master_clone.receive(Duration::from_secs(2));
        });

        let receiver = ReplicaReceiver::new(replica_side);
        let mut writer = RecordingWriter::new();
        let res = receiver.run(&mut writer);
        assert!(
            matches!(res, Err(RepError::ProtocolError(_))),
            "expected ProtocolError on duplicate VLSN, got {res:?}"
        );
        assert_eq!(writer.entries.len(), 1);
    }

    /// LOG-7: a gap in the VLSN sequence (`vlsn > high-water + 1`) is
    /// allowed — the master may legitimately skip VLSNs (non-replicated
    /// entries reuse the same connection).
    #[test]
    fn test_replica_allows_vlsn_gap() {
        let pair = LocalChannelPair::new();
        let master_side: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let replica_side: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // VLSN gap: 1 → 5 → 100.
        let frames = vec![
            make_frame(1, 10, b"a"),
            make_frame(5, 10, b"b"),
            make_frame(100, 10, b"c"),
        ];

        let master_clone = Arc::clone(&master_side);
        let send_handle = std::thread::spawn(move || {
            for f in &frames {
                master_clone.send(f).unwrap();
            }
            for _ in 0..3 {
                let _ = master_clone.receive(Duration::from_secs(2));
            }
            master_clone.close().unwrap();
        });

        let receiver = ReplicaReceiver::new(replica_side);
        let mut writer = RecordingWriter::new();
        receiver.run(&mut writer).unwrap();
        send_handle.join().unwrap();

        assert_eq!(writer.entries.len(), 3);
        assert_eq!(writer.entries[0].0, 1);
        assert_eq!(writer.entries[1].0, 5);
        assert_eq!(writer.entries[2].0, 100);
    }

    /// LOG-10: an unknown entry-type byte is logged and the frame is
    /// skipped.  The connection stays open; subsequent valid frames are
    /// applied normally.
    #[test]
    fn test_replica_skips_unknown_entry_type() {
        let pair = LocalChannelPair::new();
        let master_side: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let replica_side: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // VLSN=1: bogus entry_type=200 (no LogEntryType::from_type_num).
        // VLSN=2: valid InsertLN (10).  After my high-water bookkeeping,
        // VLSN=2 must still be accepted because the bogus frame did NOT
        // advance the high-water.
        let frames =
            vec![make_frame(1, 200, b"bogus"), make_frame(2, 10, b"good")];

        let master_clone = Arc::clone(&master_side);
        let send_handle = std::thread::spawn(move || {
            for f in &frames {
                master_clone.send(f).unwrap();
            }
            // Only the second (valid) frame produces an ack.
            let ack = master_clone.receive(Duration::from_secs(2)).unwrap();
            master_clone.close().unwrap();
            ack
        });

        let receiver = ReplicaReceiver::new(replica_side);
        let mut writer = RecordingWriter::new();
        receiver.run(&mut writer).unwrap();

        let ack = send_handle.join().unwrap();
        let acked_vlsn =
            u64::from_le_bytes(ack.unwrap()[..8].try_into().unwrap());

        assert_eq!(writer.entries.len(), 1, "bogus frame must be skipped");
        assert_eq!(writer.entries[0].0, 2);
        assert_eq!(writer.entries[0].1, 10);
        assert_eq!(acked_vlsn, 2);
    }

    // -----------------------------------------------------------------------
    // Original ReplicaStream state struct tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_new_replica_stream() {
        let stream = ReplicaStream::new();
        assert_eq!(stream.get_state(), ReplicaStreamState::Idle);
        assert_eq!(stream.get_applied_vlsn(), 0);
        assert_eq!(stream.get_received_vlsn(), 0);
        assert_eq!(stream.get_master_vlsn(), 0);
        assert!(stream.get_master().is_none());
        assert_eq!(stream.get_lag(), 0);
    }

    #[test]
    fn test_default() {
        let stream = ReplicaStream::default();
        assert_eq!(stream.get_state(), ReplicaStreamState::Idle);
    }

    #[test]
    fn test_state_transitions() {
        let stream = ReplicaStream::new();
        assert_eq!(stream.get_state(), ReplicaStreamState::Idle);

        stream.set_state(ReplicaStreamState::Connecting);
        assert_eq!(stream.get_state(), ReplicaStreamState::Connecting);

        stream.set_state(ReplicaStreamState::Streaming);
        assert_eq!(stream.get_state(), ReplicaStreamState::Streaming);

        stream.set_state(ReplicaStreamState::CatchingUp);
        assert_eq!(stream.get_state(), ReplicaStreamState::CatchingUp);

        stream.set_state(ReplicaStreamState::Shutdown);
        assert_eq!(stream.get_state(), ReplicaStreamState::Shutdown);
    }

    #[test]
    fn test_master_name() {
        let stream = ReplicaStream::new();
        assert!(stream.get_master().is_none());

        stream.set_master("master-node-1");
        assert_eq!(stream.get_master(), Some("master-node-1".to_string()));

        stream.set_master("master-node-2");
        assert_eq!(stream.get_master(), Some("master-node-2".to_string()));
    }

    #[test]
    fn test_receive_and_drain() {
        let stream = ReplicaStream::new();
        stream.receive_entry(1, 10, vec![0xAA]);
        stream.receive_entry(2, 20, vec![0xBB, 0xCC]);
        stream.receive_entry(3, 30, vec![]);

        assert_eq!(stream.get_received_vlsn(), 3);

        let entries = stream.drain_pending();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (1, 10, vec![0xAA]));
        assert_eq!(entries[1], (2, 20, vec![0xBB, 0xCC]));
        assert_eq!(entries[2], (3, 30, vec![]));

        // Pending should be empty now.
        let entries2 = stream.drain_pending();
        assert!(entries2.is_empty());
    }

    #[test]
    fn test_received_vlsn_monotonic() {
        let stream = ReplicaStream::new();
        stream.receive_entry(5, 1, vec![]);
        assert_eq!(stream.get_received_vlsn(), 5);

        // Out-of-order receive should not decrease received_vlsn.
        stream.receive_entry(3, 1, vec![]);
        assert_eq!(stream.get_received_vlsn(), 5);

        stream.receive_entry(7, 1, vec![]);
        assert_eq!(stream.get_received_vlsn(), 7);
    }

    #[test]
    fn test_mark_applied() {
        let stream = ReplicaStream::new();
        stream.mark_applied(5);
        assert_eq!(stream.get_applied_vlsn(), 5);

        stream.mark_applied(10);
        assert_eq!(stream.get_applied_vlsn(), 10);

        // Applied VLSN should not go backwards.
        stream.mark_applied(7);
        assert_eq!(stream.get_applied_vlsn(), 10);
    }

    #[test]
    fn test_update_master_vlsn() {
        let stream = ReplicaStream::new();
        stream.update_master_vlsn(100);
        assert_eq!(stream.get_master_vlsn(), 100);

        stream.update_master_vlsn(150);
        assert_eq!(stream.get_master_vlsn(), 150);

        // Should not go backwards.
        stream.update_master_vlsn(120);
        assert_eq!(stream.get_master_vlsn(), 150);
    }

    #[test]
    fn test_lag_calculation() {
        let stream = ReplicaStream::new();
        stream.update_master_vlsn(100);
        assert_eq!(stream.get_lag(), 100);

        stream.mark_applied(50);
        assert_eq!(stream.get_lag(), 50);

        stream.mark_applied(100);
        assert_eq!(stream.get_lag(), 0);

        // Applied exceeds master (shouldn't normally happen, but be safe).
        stream.mark_applied(110);
        assert_eq!(stream.get_lag(), 0);
    }

    #[test]
    fn test_is_caught_up() {
        let stream = ReplicaStream::new();
        // Not caught up: master_vlsn is 0.
        assert!(!stream.is_caught_up());

        stream.update_master_vlsn(10);
        // Not caught up: applied_vlsn is 0.
        assert!(!stream.is_caught_up());

        stream.mark_applied(10);
        // Caught up: applied == master, no pending.
        assert!(stream.is_caught_up());

        // Add a pending entry -> not caught up.
        stream.receive_entry(11, 1, vec![]);
        stream.update_master_vlsn(11);
        assert!(!stream.is_caught_up());

        // Drain and apply.
        stream.drain_pending();
        stream.mark_applied(11);
        assert!(stream.is_caught_up());
    }

    #[test]
    fn test_caught_up_with_excess_applied() {
        let stream = ReplicaStream::new();
        stream.update_master_vlsn(5);
        stream.mark_applied(10);
        // applied > master, no pending -> caught up.
        assert!(stream.is_caught_up());
    }

    #[test]
    fn test_receive_apply_cycle() {
        let stream = ReplicaStream::new();
        stream.set_master("master1");
        stream.set_state(ReplicaStreamState::Streaming);
        stream.update_master_vlsn(5);

        // Simulate receiving entries 1-5.
        for i in 1..=5 {
            stream.receive_entry(i, 1, vec![i as u8]);
        }
        assert_eq!(stream.get_received_vlsn(), 5);
        assert_eq!(stream.get_lag(), 5);

        // Drain and apply.
        let entries = stream.drain_pending();
        assert_eq!(entries.len(), 5);
        for (vlsn, _, _) in &entries {
            stream.mark_applied(*vlsn);
        }

        assert_eq!(stream.get_applied_vlsn(), 5);
        assert_eq!(stream.get_lag(), 0);
        assert!(stream.is_caught_up());
    }

    #[test]
    fn test_debug_format() {
        let stream = ReplicaStream::new();
        stream.set_master("test-master");
        stream.set_state(ReplicaStreamState::Streaming);
        let debug = format!("{:?}", stream);
        assert!(debug.contains("test-master"));
        assert!(debug.contains("Streaming"));
    }
}
