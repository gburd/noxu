//! Feeder  -  master-side replication sender.
//!
//! Port of `com.sleepycat.je.rep.impl.node.Feeder`. Tracks the state of
//! feeding replication data to a single replica, including the current
//! VLSN position, acknowledged VLSN, output queue, and heartbeat tracking.
//!
//! The [`FeederRunner`] provides the active I/O loop that scans the log
//! forward from a given VLSN, frames each entry, and sends it to the replica
//! via a [`Channel`]. Acks are received on the same channel.

use noxu_sync::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{RepError, Result};
use crate::net::channel::Channel;

// ---------------------------------------------------------------------------
// Log scanner trait
// ---------------------------------------------------------------------------

/// An iterator over log entries starting from a given VLSN.
///
/// Corresponds to JE's `FeederSource` / `MasterFeederSource`. The scanner
/// returns `(vlsn, entry_type, payload)` tuples in VLSN order. Returning
/// `None` signals that there are no more entries *yet*; the caller will call
/// `next_entry` again after a short wait.
pub trait LogScanner: Send {
    /// Return the next available entry with VLSN >= `from_vlsn`, or `None` if
    /// no new entry is available at this moment.
    fn next_entry(&mut self, from_vlsn: u64)
        -> Option<(u64, u8, Vec<u8>)>;
}

// ---------------------------------------------------------------------------
// FeederRunner
// ---------------------------------------------------------------------------

/// Wire frame sizes.
///
/// Each entry sent over the wire is:
/// `[vlsn: 8 bytes LE][entry_type: 1 byte][payload_len: 4 bytes LE][payload]`
const FRAME_HEADER_LEN: usize = 8 + 1 + 4;

/// Active feeder I/O loop.
///
/// `FeederRunner` owns a channel to a specific replica and a starting VLSN.
/// `run()` is a blocking loop that:
///   1. Scans the log for entries at `vlsn_start` and beyond.
///   2. Frames each entry and sends it to the replica.
///   3. Reads ack messages back from the replica and advances `acked_vlsn`.
///   4. Returns when the channel is closed or an I/O error occurs.
///
/// Port of the output + input thread pair inside JE's `Feeder`.
pub struct FeederRunner {
    /// Channel to the replica.
    channel: Arc<dyn Channel>,
    /// First VLSN to send.
    vlsn_start: u64,
    /// Most recent VLSN acknowledged by the replica (tracked externally via
    /// the owning [`Feeder`] state struct, but also tracked here for quick
    /// access).
    known_replica_vlsn: Mutex<u64>,
}

impl FeederRunner {
    /// Create a new `FeederRunner`.
    ///
    /// # Arguments
    /// * `channel` - The channel to the replica.
    /// * `vlsn_start` - The VLSN from which to begin streaming.
    pub fn new(channel: Arc<dyn Channel>, vlsn_start: u64) -> Self {
        Self {
            channel,
            vlsn_start,
            known_replica_vlsn: Mutex::new(0),
        }
    }

    /// Return the last VLSN acknowledged by the replica.
    pub fn known_replica_vlsn(&self) -> u64 {
        *self.known_replica_vlsn.lock()
    }

    /// Run the feeder loop.
    ///
    /// Streams all log entries from `vlsn_start` to the replica, then polls
    /// for new entries. Acks from the replica update `known_replica_vlsn`.
    ///
    /// Returns `Ok(())` when the channel is closed gracefully, or `Err` on
    /// an I/O error.
    pub fn run(&self, log_scanner: &mut dyn LogScanner) -> Result<()> {
        let mut next_vlsn = self.vlsn_start;
        let poll_interval = Duration::from_millis(5);
        let ack_timeout = Duration::from_millis(1);

        loop {
            // ----------------------------------------------------------------
            // 1. Send all available log entries.
            // ----------------------------------------------------------------
            while let Some((vlsn, entry_type, payload)) =
                log_scanner.next_entry(next_vlsn)
            {
                self.send_entry(vlsn, entry_type, &payload)?;
                next_vlsn = vlsn + 1;
            }

            // ----------------------------------------------------------------
            // 2. Poll for acks from the replica (non-blocking style).
            // ----------------------------------------------------------------
            match self.channel.receive(ack_timeout) {
                Ok(Some(ack_bytes)) => {
                    if ack_bytes.len() >= 8 {
                        let vlsn = u64::from_le_bytes(
                            ack_bytes[..8].try_into().unwrap(),
                        );
                        let mut guard = self.known_replica_vlsn.lock();
                        if vlsn > *guard {
                            *guard = vlsn;
                        }
                    }
                    // Continue without sleeping — more acks may be waiting.
                    continue;
                }
                Ok(None) => {
                    // Timeout: no ack yet, check for more log entries.
                    std::thread::sleep(poll_interval);
                    continue;
                }
                Err(RepError::ChannelClosed(_)) => {
                    // Replica disconnected; clean shutdown.
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Frame and send a single log entry.
    ///
    /// Frame format: `[vlsn: 8 LE][entry_type: 1][payload_len: 4 LE][payload]`
    fn send_entry(
        &self,
        vlsn: u64,
        entry_type: u8,
        payload: &[u8],
    ) -> Result<()> {
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
        frame.extend_from_slice(&vlsn.to_le_bytes());
        frame.push(entry_type);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        self.channel.send(&frame)
    }
}

/// The state of a feeder connection to a replica.
///
/// Port of the feeder state machine from JE's `Feeder` class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeederState {
    /// Not connected to any replica.
    Idle,
    /// Performing the initial handshake (protocol negotiation).
    Handshaking,
    /// Actively streaming replication data.
    Streaming,
    /// Shutting down.
    Shutdown,
}

/// Tracks the state of feeding replication data to a single replica.
///
/// Each `Feeder` instance corresponds to one replica connection. The feeder
/// maintains a queue of outbound messages and tracks which VLSNs have been
/// sent vs. acknowledged.
///
/// Port of `com.sleepycat.je.rep.impl.node.Feeder`.
pub struct Feeder {
    /// Name of the replica this feeder is serving.
    replica_name: String,
    /// Current connection state.
    state: Mutex<FeederState>,
    /// Next VLSN to send to the replica.
    current_vlsn: Mutex<u64>,
    /// Last VLSN acknowledged by the replica.
    acked_vlsn: Mutex<u64>,
    /// Timestamp of last activity (send or receive).
    last_activity: Mutex<Instant>,
    /// Pending messages queued for sending to the replica.
    /// Each message is a serialized byte vector.
    output_queue: Mutex<Vec<Vec<u8>>>,
}

impl Feeder {
    /// Create a new feeder for the named replica.
    pub fn new(replica_name: String) -> Self {
        Feeder {
            replica_name,
            state: Mutex::new(FeederState::Idle),
            current_vlsn: Mutex::new(0),
            acked_vlsn: Mutex::new(0),
            last_activity: Mutex::new(Instant::now()),
            output_queue: Mutex::new(Vec::new()),
        }
    }

    /// Return the name of the replica this feeder is serving.
    pub fn get_replica_name(&self) -> String {
        self.replica_name.clone()
    }

    /// Return the current feeder state.
    pub fn get_state(&self) -> FeederState {
        *self.state.lock()
    }

    /// Set the feeder state.
    pub fn set_state(&self, state: FeederState) {
        *self.state.lock() = state;
    }

    /// Return the next VLSN that will be sent.
    pub fn get_current_vlsn(&self) -> u64 {
        *self.current_vlsn.lock()
    }

    /// Return the last VLSN acknowledged by the replica.
    pub fn get_acked_vlsn(&self) -> u64 {
        *self.acked_vlsn.lock()
    }

    /// Queue a log entry for sending to the replica.
    ///
    /// The entry is serialized into a byte vector containing the VLSN,
    /// entry type, and raw data. The current VLSN is advanced to one
    /// past the queued VLSN.
    pub fn queue_entry(&self, vlsn: u64, entry_type: u8, data: Vec<u8>) {
        let mut msg = Vec::with_capacity(9 + data.len());
        msg.extend_from_slice(&vlsn.to_le_bytes());
        msg.push(entry_type);
        msg.extend_from_slice(&data);

        self.output_queue.lock().push(msg);

        let mut current = self.current_vlsn.lock();
        if vlsn >= *current {
            *current = vlsn + 1;
        }

        *self.last_activity.lock() = Instant::now();
    }

    /// Record an acknowledgement from the replica.
    ///
    /// The acked VLSN is updated if the new value is greater than the
    /// current acked VLSN (acks should arrive in order, but we are
    /// defensive).
    pub fn record_ack(&self, vlsn: u64) {
        let mut acked = self.acked_vlsn.lock();
        if vlsn > *acked {
            *acked = vlsn;
        }
        *self.last_activity.lock() = Instant::now();
    }

    /// Return the replication lag: the difference between the current
    /// VLSN (next to send) and the last acknowledged VLSN.
    ///
    /// A lag of 0 means the replica is fully caught up.
    pub fn get_lag(&self) -> u64 {
        let current = *self.current_vlsn.lock();
        let acked = *self.acked_vlsn.lock();
        current.saturating_sub(acked)
    }

    /// Take all queued messages (drain the output queue).
    ///
    /// Returns the messages in the order they were queued and leaves
    /// the queue empty.
    pub fn drain_queue(&self) -> Vec<Vec<u8>> {
        let mut queue = self.output_queue.lock();
        std::mem::take(&mut *queue)
    }

    /// Check if the feeder has timed out (no activity within the given
    /// duration).
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.last_activity.lock().elapsed() > timeout
    }

    /// Update the activity timestamp to the current time.
    pub fn touch(&self) {
        *self.last_activity.lock() = Instant::now();
    }
}

impl std::fmt::Debug for Feeder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Feeder")
            .field("replica_name", &self.replica_name)
            .field("state", &self.get_state())
            .field("current_vlsn", &self.get_current_vlsn())
            .field("acked_vlsn", &self.get_acked_vlsn())
            .field("lag", &self.get_lag())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::channel::LocalChannelPair;
    use std::collections::VecDeque;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// A simple in-memory log scanner backed by a queue.
    struct VecLogScanner {
        entries: VecDeque<(u64, u8, Vec<u8>)>,
    }

    impl VecLogScanner {
        fn new(entries: Vec<(u64, u8, Vec<u8>)>) -> Self {
            Self { entries: entries.into_iter().collect() }
        }
    }

    impl LogScanner for VecLogScanner {
        fn next_entry(
            &mut self,
            from_vlsn: u64,
        ) -> Option<(u64, u8, Vec<u8>)> {
            if let Some(&(vlsn, _, _)) = self.entries.front() {
                if vlsn >= from_vlsn {
                    return self.entries.pop_front();
                }
            }
            None
        }
    }

    // -----------------------------------------------------------------------
    // FeederRunner tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_feeder_runner_sends_entries_via_local_channel() {
        // Build a 3-entry log.
        let entries = vec![
            (1u64, 10u8, vec![0xAA]),
            (2u64, 20u8, vec![0xBB, 0xCC]),
            (3u64, 30u8, vec![]),
        ];
        let pair = LocalChannelPair::new();
        let sender: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Receiver side: collect frames and send back acks.
        let recv_handle = {
            let receiver = Arc::clone(&receiver);
            std::thread::spawn(move || {
                let mut received: Vec<(u64, u8, Vec<u8>)> = Vec::new();
                let timeout = Duration::from_secs(5);

                for _ in 0..3 {
                    let frame = receiver.receive(timeout).unwrap().unwrap();
                    // Parse frame: [vlsn:8][type:1][len:4][payload]
                    let vlsn =
                        u64::from_le_bytes(frame[0..8].try_into().unwrap());
                    let entry_type = frame[8];
                    let payload_len = u32::from_le_bytes(
                        frame[9..13].try_into().unwrap(),
                    ) as usize;
                    let payload = frame[13..13 + payload_len].to_vec();
                    received.push((vlsn, entry_type, payload));

                    // Send ack.
                    let mut ack = Vec::with_capacity(8);
                    ack.extend_from_slice(&vlsn.to_le_bytes());
                    receiver.send(&ack).unwrap();
                }

                received
            })
        };

        let mut scanner = VecLogScanner::new(entries.clone());
        // FeederRunner polls for acks until the channel is closed.
        // Close the sender side after the scanner drains so run() returns.
        let runner = FeederRunner::new(Arc::clone(&sender), 1);

        // Run in a separate thread so we can close the channel.
        let runner_arc = Arc::new(runner);
        let runner_ref = Arc::clone(&runner_arc);
        let sender_ref = Arc::clone(&sender);
        let run_handle = std::thread::spawn(move || {
            let res = runner_ref.run(&mut scanner);
            res
        });

        // Wait for receiver to collect all 3 entries.
        let received = recv_handle.join().unwrap();
        assert_eq!(received.len(), 3);
        assert_eq!(received[0], (1, 10, vec![0xAA]));
        assert_eq!(received[1], (2, 20, vec![0xBB, 0xCC]));
        assert_eq!(received[2], (3, 30, vec![]));

        // Verify ack was tracked.
        assert_eq!(runner_arc.known_replica_vlsn(), 3);

        // Close the channel to terminate the run loop.
        sender_ref.close().unwrap();
        run_handle.join().unwrap().unwrap();
    }

    #[test]
    fn test_feeder_runner_empty_scanner_returns_on_close() {
        let pair = LocalChannelPair::new();
        let sender: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);

        let runner = FeederRunner::new(Arc::clone(&sender), 1);
        let sender_clone = Arc::clone(&sender);

        // Close the channel almost immediately.
        let close_handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            receiver.close().unwrap();
            sender_clone.close().unwrap();
        });

        let mut scanner = VecLogScanner::new(vec![]);
        let result = runner.run(&mut scanner);
        assert!(result.is_ok(), "expected Ok on channel close, got {:?}", result);
        close_handle.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Original Feeder state struct tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_new_feeder() {
        let feeder = Feeder::new("replica1".to_string());
        assert_eq!(feeder.get_replica_name(), "replica1");
        assert_eq!(feeder.get_state(), FeederState::Idle);
        assert_eq!(feeder.get_current_vlsn(), 0);
        assert_eq!(feeder.get_acked_vlsn(), 0);
        assert_eq!(feeder.get_lag(), 0);
    }

    #[test]
    fn test_state_transitions() {
        let feeder = Feeder::new("r1".to_string());
        assert_eq!(feeder.get_state(), FeederState::Idle);

        feeder.set_state(FeederState::Handshaking);
        assert_eq!(feeder.get_state(), FeederState::Handshaking);

        feeder.set_state(FeederState::Streaming);
        assert_eq!(feeder.get_state(), FeederState::Streaming);

        feeder.set_state(FeederState::Shutdown);
        assert_eq!(feeder.get_state(), FeederState::Shutdown);
    }

    #[test]
    fn test_queue_and_drain() {
        let feeder = Feeder::new("r1".to_string());
        feeder.queue_entry(1, 10, vec![0xAA, 0xBB]);
        feeder.queue_entry(2, 20, vec![0xCC]);
        feeder.queue_entry(3, 30, vec![]);

        let messages = feeder.drain_queue();
        assert_eq!(messages.len(), 3);

        // Verify message format: 8 bytes VLSN + 1 byte type + data.
        assert_eq!(messages[0].len(), 8 + 1 + 2);
        assert_eq!(messages[1].len(), 8 + 1 + 1);
        assert_eq!(messages[2].len(), 8 + 1 + 0);

        // Verify VLSN encoding.
        let vlsn_bytes: [u8; 8] = messages[0][0..8].try_into().unwrap();
        assert_eq!(u64::from_le_bytes(vlsn_bytes), 1);
        assert_eq!(messages[0][8], 10); // entry_type

        // Queue should be empty now.
        let messages2 = feeder.drain_queue();
        assert!(messages2.is_empty());
    }

    #[test]
    fn test_current_vlsn_advances() {
        let feeder = Feeder::new("r1".to_string());
        feeder.queue_entry(5, 1, vec![]);
        assert_eq!(feeder.get_current_vlsn(), 6);

        feeder.queue_entry(10, 1, vec![]);
        assert_eq!(feeder.get_current_vlsn(), 11);

        // Queueing a lower VLSN should not decrease current_vlsn.
        feeder.queue_entry(3, 1, vec![]);
        assert_eq!(feeder.get_current_vlsn(), 11);
    }

    #[test]
    fn test_ack_recording() {
        let feeder = Feeder::new("r1".to_string());
        feeder.queue_entry(1, 1, vec![]);
        feeder.queue_entry(2, 1, vec![]);
        feeder.queue_entry(3, 1, vec![]);

        feeder.record_ack(1);
        assert_eq!(feeder.get_acked_vlsn(), 1);

        feeder.record_ack(3);
        assert_eq!(feeder.get_acked_vlsn(), 3);

        // Ack should not go backwards.
        feeder.record_ack(2);
        assert_eq!(feeder.get_acked_vlsn(), 3);
    }

    #[test]
    fn test_lag_calculation() {
        let feeder = Feeder::new("r1".to_string());
        assert_eq!(feeder.get_lag(), 0);

        feeder.queue_entry(1, 1, vec![]);
        feeder.queue_entry(2, 1, vec![]);
        feeder.queue_entry(3, 1, vec![]);
        // current_vlsn = 4, acked_vlsn = 0 -> lag = 4.
        assert_eq!(feeder.get_lag(), 4);

        feeder.record_ack(2);
        // current_vlsn = 4, acked_vlsn = 2 -> lag = 2.
        assert_eq!(feeder.get_lag(), 2);

        feeder.record_ack(4);
        // Acked caught up to current.
        assert_eq!(feeder.get_lag(), 0);
    }

    #[test]
    fn test_timeout() {
        let feeder = Feeder::new("r1".to_string());
        // Just created, should not be timed out with a reasonable timeout.
        assert!(!feeder.is_timed_out(Duration::from_secs(60)));

        // With a zero timeout, should be timed out immediately (or nearly).
        // We can't guarantee sub-nanosecond timing, but a zero-duration
        // timeout is a reasonable edge case.
        assert!(feeder.is_timed_out(Duration::from_nanos(0)));
    }

    #[test]
    fn test_touch_resets_activity() {
        let feeder = Feeder::new("r1".to_string());
        // Wait a tiny bit then touch.
        std::thread::sleep(Duration::from_millis(5));
        feeder.touch();
        // After touch, the feeder should not be timed out for a reasonable
        // duration.
        assert!(!feeder.is_timed_out(Duration::from_secs(1)));
    }

    #[test]
    fn test_debug_format() {
        let feeder = Feeder::new("replica_debug".to_string());
        feeder.set_state(FeederState::Streaming);
        let debug = format!("{:?}", feeder);
        assert!(debug.contains("replica_debug"));
        assert!(debug.contains("Streaming"));
    }
}
