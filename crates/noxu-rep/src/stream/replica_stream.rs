//! Replica stream  -  replica-side replication receiver.
//!
//! Port of `com.sleepycat.je.rep.impl.node.Replica`. Tracks the state of
//! receiving replication data from the master, including pending entries,
//! applied/received VLSNs, and the master's latest known VLSN.
//!
//! The [`ReplicaReceiver`] provides the active I/O loop that reads framed
//! entries from the feeder channel, passes them to a [`LogWriter`], and sends
//! acks back.
//!
//! [`EnvironmentLogWriter`] is the live implementation of [`LogWriter`] that
//! writes replicated entries into the local `LogManager` and updates the
//! VLSN index. Port of `com.sleepycat.je.rep.impl.node.Replica.ReplayThread`.

use noxu_log::{LogEntryType, Provisional};
use noxu_sync::Mutex;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{RepError, Result};
use crate::net::channel::Channel;

// ---------------------------------------------------------------------------
// LogWriter trait
// ---------------------------------------------------------------------------

/// Sink for replicated log entries.
///
/// Corresponds to JE's replay thread accepting log records and writing them
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
/// Port of `com.sleepycat.je.rep.impl.node.Replica.ReplayThread`.
pub struct EnvironmentLogWriter {
    /// Shared log manager for appending replicated entries.
    log_manager: Arc<noxu_log::LogManager>,
    /// VLSN index: maps VLSN → (file_number, file_offset) on this replica.
    vlsn_index: Arc<crate::vlsn::vlsn_index::VlsnIndex>,
}

impl EnvironmentLogWriter {
    /// Create a new `EnvironmentLogWriter`.
    ///
    /// # Arguments
    /// * `log_manager` — The live `LogManager` for this replica environment.
    /// * `vlsn_index`  — The VLSN index to update after each written entry.
    pub fn new(
        log_manager: Arc<noxu_log::LogManager>,
        vlsn_index: Arc<crate::vlsn::vlsn_index::VlsnIndex>,
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
    ) -> crate::error::Result<()> {
        // Resolve the wire entry-type byte to the typed enum.
        let log_entry_type =
            LogEntryType::from_type_num(entry_type).ok_or_else(|| {
                crate::error::RepError::ProtocolError(format!(
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
                crate::error::RepError::DatabaseError(format!(
                    "replica log write failed: {}",
                    e
                ))
            })?;

        // Register VLSN → LSN in the replica's VLSN index so that
        // FeederRunner/ack tracking can correlate positions.
        // vlsn=0 is reserved as NULL_VLSN; skip it.
        if vlsn > 0 {
            self.vlsn_index.put(
                vlsn,
                lsn.file_number(),
                lsn.file_offset(),
            );
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
const FRAME_HEADER_LEN: usize = 8 + 1 + 4;

/// Active replica I/O loop.
///
/// `ReplicaReceiver` owns a channel to the master feeder. `run()` is a
/// blocking loop that:
///   1. Reads framed entries from the feeder.
///   2. Deserializes each entry: `[vlsn:8][type:1][len:4][payload]`.
///   3. Passes the entry to `log_writer`.
///   4. Sends an 8-byte LE VLSN ack back to the master.
///   5. Returns when the channel is closed or an I/O error occurs.
///
/// Port of the read thread + `ReplayThread` in JE's `Replica`.
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
    pub fn run(&self, log_writer: &mut dyn LogWriter) -> Result<()> {
        let recv_timeout = Duration::from_secs(30);

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
            // Parse frame: [vlsn:8 LE][entry_type:1][payload_len:4 LE][payload]
            // ----------------------------------------------------------------
            if frame.len() < FRAME_HEADER_LEN {
                return Err(RepError::ProtocolError(format!(
                    "replica: short frame: {} bytes",
                    frame.len()
                )));
            }

            let vlsn =
                u64::from_le_bytes(frame[0..8].try_into().unwrap());
            let entry_type = frame[8];
            let payload_len = u32::from_le_bytes(
                frame[9..13].try_into().unwrap(),
            ) as usize;

            if frame.len() < FRAME_HEADER_LEN + payload_len {
                return Err(RepError::ProtocolError(format!(
                    "replica: frame payload truncated: expected {} bytes, got {}",
                    payload_len,
                    frame.len() - FRAME_HEADER_LEN,
                )));
            }

            let payload = &frame[FRAME_HEADER_LEN..FRAME_HEADER_LEN + payload_len];

            // ----------------------------------------------------------------
            // Apply the entry to the local log.
            // ----------------------------------------------------------------
            log_writer.write_entry(vlsn, entry_type, payload)?;

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
/// Port of the replica state machine from JE's `Replica` class.
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
/// Port of `com.sleepycat.je.rep.impl.node.Replica`.
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
    /// Returns `true` if the applied VLSN equals or exceeds the master's
    /// latest known VLSN and there are no pending entries.
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
    use crate::net::channel::LocalChannelPair;

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
        let mut f = Vec::with_capacity(13 + payload.len());
        f.extend_from_slice(&vlsn.to_le_bytes());
        f.push(entry_type);
        f.extend_from_slice(&(payload.len() as u32).to_le_bytes());
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
                let vlsn =
                    u64::from_le_bytes(ack[..8].try_into().unwrap());
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
        use crate::stream::feeder::{FeederRunner, LogScanner};
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
                    && v >= from_vlsn {
                        return self.items.pop_front();
                    }
                None
            }
        }

        let pair = LocalChannelPair::new();
        let master_ch: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let replica_ch: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Entries to replicate.
        let entries: Vec<(u64, u8, Vec<u8>)> = (1..=5)
            .map(|i| (i, i as u8, vec![i as u8; i as usize]))
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
            let mut scanner = SimpleScanner {
                items: entries.into_iter().collect(),
            };
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
            assert_eq!(*etype, expected_vlsn as u8);
            assert_eq!(payload.len(), expected_vlsn as usize);
        }
        assert_eq!(last_acked, 5);
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
