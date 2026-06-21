//! Feeder  -  master-side replication sender.
//!
//! Tracks the state of
//! feeding replication data to a single replica, including the current
//! VLSN position, acknowledged VLSN, output queue, and heartbeat tracking.
//!
//! The [`FeederRunner`] provides the active I/O loop that scans the log
//! forward from a given VLSN, frames each entry, and sends it to the replica
//! via a [`Channel`]. Acks are received on the same channel.
//!
//! [`EnvironmentLogScanner`] is the live implementation of [`LogScanner`]
//! backed by the real `LogManager` + `FileManager`.
//! Rep.impl.node.Feeder.MasterFeederSource`.

use noxu_dbi::EnvironmentImpl;
use noxu_log::MAX_ITEM_SIZE;
use noxu_log::entry_header::{MAX_HEADER_SIZE, MIN_HEADER_SIZE};
use noxu_log::file_header::LOG_VERSION as LOG_FILE_VERSION;
use noxu_log::file_header::on_disk_size as file_header_on_disk_size;
use noxu_log::file_manager::FileManager;
use noxu_sync::Mutex;
use noxu_util::lsn::{Lsn, NULL_LSN};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{RepError, Result};
use crate::net::channel::Channel;

// CRC32 (Ethernet/zlib polynomial via PCLMULQDQ on x86-64 — ~18 GiB/s).
// See docs/checksum-selection.md for the full benchmark rationale.
use crc32fast;

// ---------------------------------------------------------------------------
// Log scanner trait
// ---------------------------------------------------------------------------

/// An iterator over log entries starting from a given VLSN.
///
/// Corresponds to `FeederSource` / `MasterFeederSource`. The scanner
/// returns `(vlsn, entry_type, payload)` tuples in VLSN order. Returning
/// `None` signals that there are no more entries *yet*; the caller will call
/// `next_entry` again after a short wait.
pub trait LogScanner: Send {
    /// Return the next available entry with VLSN >= `from_vlsn`, or `None` if
    /// no new entry is available at this moment.
    fn next_entry(&mut self, from_vlsn: u64) -> Option<(u64, u8, Vec<u8>)>;
}

// ---------------------------------------------------------------------------
// EnvironmentLogScanner
// ---------------------------------------------------------------------------

/// `LogScanner` implementation backed by the live `EnvironmentImpl`.
///
/// Scans the log forward from an LSN cursor, returning entries that carry a
/// VLSN >= `from_vlsn`. On each call to `next_entry` the scanner advances
/// its internal file/offset position one entry at a time; when it reaches
/// the current end of the log it returns `None` (the `FeederRunner` will
/// call again after a brief poll interval).
///
///
pub struct EnvironmentLogScanner {
    /// The log `FileManager` used for raw byte-level reads.
    file_manager: Arc<FileManager>,
    /// Current scan position: next file number and byte offset to read.
    cursor_file: u32,
    cursor_offset: u64,
    /// Highest VLSN returned so far (to avoid duplicates on re-entrant calls).
    last_returned_vlsn: u64,
}

impl EnvironmentLogScanner {
    /// Create a scanner that starts at `start_lsn`.
    ///
    /// If `start_lsn` is `NULL_LSN` the scanner begins at the very first log
    /// entry in file 0.  The correct first-entry offset is resolved from
    /// file 0's `log_version` (32 for v2, 36 for v3).  If file 0 does not
    /// exist yet (empty log) the current-version default (36) is used.
    ///
    /// Obtain the `FileManager` from `EnvironmentImpl::get_log_manager()` →
    /// `LogManager` is not directly accessible, but `EnvironmentImpl` exposes
    /// a `get_log_manager()` returning `Option<Arc<LogManager>>`.  For the
    /// scanner we need the `FileManager` underneath it; the simplest approach
    /// is to construct the scanner directly from a `FileManager` Arc.
    pub fn new(env: &EnvironmentImpl, start_lsn: Option<Lsn>) -> Option<Self> {
        // We need the FileManager to do raw byte reads.  It is not directly
        // exposed on EnvironmentImpl, so we access it via the LogManager.
        // LogManager::file_manager is private, so we carry it separately.
        // For now, use the env_home path to construct a read-only FileManager.
        //
        // The master feeder reads the log files
        // directly, starting at the replica's current VLSN position.
        let env_home = env.get_env_home().to_path_buf();
        let fm = Arc::new(
            FileManager::new(&env_home, true, 256 * 1024 * 1024, 32).ok()?,
        );

        let (cursor_file, cursor_offset) = match start_lsn {
            Some(lsn) if lsn != NULL_LSN => {
                (lsn.file_number(), lsn.file_offset() as u64)
            }
            _ => {
                // Start from file 0, first entry offset.  Use the actual
                // header size of file 0 if it exists (v2 → 32, v3 → 36);
                // fall back to current-version default (36) if it does not.
                let file0_offset =
                    fm.file_header_size_for(0).unwrap_or_else(|_| {
                        file_header_on_disk_size(LOG_FILE_VERSION)
                    }) as u64;
                (0, file0_offset)
            }
        };

        Some(Self {
            file_manager: fm,
            cursor_file,
            cursor_offset,
            last_returned_vlsn: 0,
        })
    }

    /// Read the raw header+payload at `(file_num, offset)`.
    ///
    /// Returns `(entry_size_bytes, vlsn_opt, entry_type_byte, payload)` or
    /// `None` if the bytes don't form a valid entry (zero fill, truncation).
    fn read_raw_entry(
        &self,
        file_num: u32,
        offset: u64,
    ) -> Option<(usize, Option<u64>, u8, Vec<u8>)> {
        let mut hdr = [0u8; MIN_HEADER_SIZE];
        let n = self
            .file_manager
            .read_from_file(file_num, offset, &mut hdr)
            .ok()?;
        if n < MIN_HEADER_SIZE {
            return None;
        }
        // Zero-fill region past last written entry.
        if hdr[4] == 0 {
            return None;
        }

        let entry_type_byte = hdr[4];
        let flags = hdr[5];
        let item_size =
            u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]]) as usize;

        let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
        let header_size =
            if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };

        // Sanity cap: same shared MAX_ITEM_SIZE used by every log reader
        // so that an attacker who flips item_size cannot cause a 100 MiB
        // allocation here while passing other readers' bounds.
        if item_size > MAX_ITEM_SIZE {
            return None;
        }

        let entry_size = header_size + item_size;
        let mut full = vec![0u8; entry_size];
        let n = self
            .file_manager
            .read_from_file(file_num, offset, &mut full)
            .ok()?;
        if n < entry_size {
            return None;
        }

        // Extract VLSN from the header extension if present (8-byte LE i64).
        let vlsn_opt = if vlsn_present && full.len() >= MAX_HEADER_SIZE {
            let raw = i64::from_le_bytes(
                full[MIN_HEADER_SIZE..MAX_HEADER_SIZE].try_into().ok()?,
            );
            if raw > 0 {
                Some(raw as u64)
            } else {
                // Negative or zero i64 with vlsn_present flag set is a
                // contradiction (NULL VLSN should not have the flag set).
                // Surface it but keep the legacy "treat as missing" behaviour
                // so a single corrupt entry does not stall the feeder.
                // LOG-9.
                log::warn!(
                    "EnvironmentLogScanner: implausible VLSN value {} at \
                     file {:08x} offset {:#x}; treating as no-VLSN",
                    raw,
                    file_num,
                    offset,
                );
                None
            }
        } else {
            None
        };

        let payload = full[header_size..].to_vec();
        Some((entry_size, vlsn_opt, entry_type_byte, payload))
    }
}

impl LogScanner for EnvironmentLogScanner {
    /// Return the next entry with VLSN >= `from_vlsn`, advancing the cursor.
    ///
    /// Scans forward one entry at a time.  Returns `None` when the cursor
    /// reaches the end of the currently-written log.  The feeder will sleep
    /// briefly and call again.
    fn next_entry(&mut self, from_vlsn: u64) -> Option<(u64, u8, Vec<u8>)> {
        // Collect file numbers once per call; cheap (directory listing).
        let file_nums = self.file_manager.list_file_numbers().ok()?;
        if file_nums.is_empty() {
            return None;
        }

        loop {
            // Skip files before the current cursor file.
            if !file_nums.contains(&self.cursor_file) {
                // Advance to the next known file.
                let next =
                    file_nums.iter().find(|&&n| n > self.cursor_file).copied();
                match next {
                    Some(n) => {
                        self.cursor_file = n;
                        self.cursor_offset =
                            self.file_manager
                                .file_header_size_for(n)
                                .unwrap_or_else(|_| {
                                    file_header_on_disk_size(LOG_FILE_VERSION)
                                }) as u64;
                    }
                    None => return None, // No more files.
                }
            }

            let file_len =
                self.file_manager.get_file_length(self.cursor_file).ok()?;

            if self.cursor_offset >= file_len {
                // End of current file: move to next file.
                let next =
                    file_nums.iter().find(|&&n| n > self.cursor_file).copied();
                match next {
                    Some(n) => {
                        self.cursor_file = n;
                        self.cursor_offset =
                            self.file_manager
                                .file_header_size_for(n)
                                .unwrap_or_else(|_| {
                                    file_header_on_disk_size(LOG_FILE_VERSION)
                                }) as u64;
                        continue;
                    }
                    None => return None, // End of log.
                }
            }

            match self.read_raw_entry(self.cursor_file, self.cursor_offset) {
                None => {
                    // End of written data in this file; move to next.
                    let next = file_nums
                        .iter()
                        .find(|&&n| n > self.cursor_file)
                        .copied();
                    match next {
                        Some(n) => {
                            self.cursor_file = n;
                            self.cursor_offset = self
                                .file_manager
                                .file_header_size_for(n)
                                .unwrap_or_else(|_| {
                                    file_header_on_disk_size(LOG_FILE_VERSION)
                                })
                                as u64;
                            continue;
                        }
                        None => return None,
                    }
                }
                Some((entry_size, vlsn_opt, entry_type_byte, payload)) => {
                    self.cursor_offset += entry_size as u64;

                    if let Some(vlsn) = vlsn_opt
                        && vlsn >= from_vlsn
                        && vlsn > self.last_returned_vlsn
                    {
                        self.last_returned_vlsn = vlsn;
                        return Some((vlsn, entry_type_byte, payload));
                    }
                    // Entry has no VLSN, vlsn < from_vlsn, or already
                    // returned: keep scanning.
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FeederRunner
// ---------------------------------------------------------------------------

/// Wire frame format (all integers little-endian):
///
/// ```text
/// ┌──────────────────────────────────────────────────────────┐
/// │  vlsn        : u64  (8 bytes)                           │
/// │  entry_type  : u8   (1 byte)                            │
/// │  payload_len : u32  (4 bytes)                           │
/// │  crc32       : u32  (4 bytes) — CRC32 of payload bytes  │
/// ├──────────────────────────────────────────────────────────┤
/// │  payload     : [u8; payload_len]                        │
/// └──────────────────────────────────────────────────────────┘
/// ```
///
/// The receiver verifies `crc32fast::hash(payload) == crc32` before
/// applying the entry.  A mismatch is returned as [`RepError::FrameCorrupted`].
const FRAME_HEADER_LEN: usize = 8 + 1 + 4 + 4; // vlsn + type + len + crc32

/// Active feeder I/O loop.
///
/// `FeederRunner` owns a channel to a specific replica and a starting VLSN.
/// `run()` is a blocking loop that:
///   1. Scans the log for entries at `vlsn_start` and beyond.
///   2. Frames each entry and sends it to the replica.
///   3. Reads ack messages back from the replica and advances `acked_vlsn`.
///   4. Returns when the channel is closed or an I/O error occurs.
///
/// The runner is single-threaded: log scanning, framing, sending, and ack
/// polling all interleave inside the one `run()` loop on the caller's
/// thread. There is no separate output/input thread pair; the same loop
/// handles both directions.
pub struct FeederRunner {
    /// Channel to the replica.
    channel: Arc<dyn Channel>,
    /// First VLSN to send.
    vlsn_start: u64,
    /// Most recent VLSN acknowledged by the replica (tracked externally via
    /// the owning [`Feeder`] state struct, but also tracked here for quick
    /// access).
    known_replica_vlsn: Mutex<u64>,
    /// REP-9: name of the replica this runner serves, and a sink that
    /// forwards each inbound ack `(replica_name, acked_vlsn)` to the owning
    /// environment's `record_ack`.  Without this, production acks reached
    /// only the private `known_replica_vlsn` and never the `AckTracker`
    /// (commit-blocking quorum) or `Feeder::acked_vlsn` (DTVLSN ranking).
    /// Mirrors JE `FeederTxns.noteReplicaAck` being driven from the feeder
    /// input loop.
    replica_name: String,
    ack_sink: Option<AckSink>,
}

/// REP-9: callback invoked by `FeederRunner::run` for each inbound ack,
/// forwarding `(replica_name, acked_vlsn)` to `env.record_ack`.
pub type AckSink = Arc<dyn Fn(&str, u64) + Send + Sync>;

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
            replica_name: String::new(),
            ack_sink: None,
        }
    }

    /// REP-9: like [`Self::new`] but wires an ack sink so each inbound ack is
    /// forwarded to the owning environment (`env.record_ack(vlsn, name)`),
    /// bridging the production feeder path to the `AckTracker` and the
    /// `Feeder::acked_vlsn` that the DTVLSN computation reads.
    pub fn new_with_ack_sink(
        channel: Arc<dyn Channel>,
        vlsn_start: u64,
        replica_name: String,
        ack_sink: AckSink,
    ) -> Self {
        Self {
            channel,
            vlsn_start,
            known_replica_vlsn: Mutex::new(0),
            replica_name,
            ack_sink: Some(ack_sink),
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
                        {
                            let mut guard = self.known_replica_vlsn.lock();
                            if vlsn > *guard {
                                *guard = vlsn;
                            }
                        }
                        // REP-9 Part 1: forward the ack to the owning env so
                        // it reaches the AckTracker (commit-blocking quorum)
                        // AND Feeder::acked_vlsn (DTVLSN ranking). JE drives
                        // FeederTxns.noteReplicaAck from this same loop.
                        if let Some(sink) = &self.ack_sink {
                            sink(&self.replica_name, vlsn);
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
    /// Frame format (all LE):
    ///   `[vlsn:8][type:1][payload_len:4][crc32:4][payload]`
    fn send_entry(
        &self,
        vlsn: u64,
        entry_type: u8,
        payload: &[u8],
    ) -> Result<()> {
        let crc = crc32fast::hash(payload);
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
        frame.extend_from_slice(&vlsn.to_le_bytes());
        frame.push(entry_type);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(payload);
        self.channel.send(&frame)
    }
}

/// The state of a feeder connection to a replica.
///
/// Feeder state machine.
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
///
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
    /// entry type, and raw data. The current VLSN is advanced to one past
    /// the queued VLSN **only when `vlsn >= current_vlsn`**; if `vlsn`
    /// is older than the current position the entry is still queued but
    /// `current_vlsn` is left unchanged.
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
        fn next_entry(&mut self, from_vlsn: u64) -> Option<(u64, u8, Vec<u8>)> {
            if let Some(&(vlsn, _, _)) = self.entries.front()
                && vlsn >= from_vlsn
            {
                return self.entries.pop_front();
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
                    // Parse frame: [vlsn:8][type:1][len:4][crc32:4][payload]
                    let vlsn =
                        u64::from_le_bytes(frame[0..8].try_into().unwrap());
                    let entry_type = frame[8];
                    let payload_len =
                        u32::from_le_bytes(frame[9..13].try_into().unwrap())
                            as usize;
                    let expected_crc =
                        u32::from_le_bytes(frame[13..17].try_into().unwrap());
                    let payload = frame[17..17 + payload_len].to_vec();
                    let actual_crc = crc32fast::hash(&payload);
                    assert_eq!(
                        actual_crc, expected_crc,
                        "CRC mismatch for vlsn={vlsn}"
                    );
                    received.push((vlsn, entry_type, payload));

                    // Send ack.
                    let mut ack = Vec::with_capacity(8);
                    ack.extend_from_slice(&vlsn.to_le_bytes());
                    receiver.send(&ack).unwrap();
                }

                received
            })
        };

        let mut scanner = VecLogScanner::new(entries);
        // FeederRunner polls for acks until the channel is closed.
        // Close the sender side after the scanner drains so run() returns.
        let runner = FeederRunner::new(Arc::clone(&sender), 1);

        // Run in a separate thread so we can close the channel.
        let runner_arc = Arc::new(runner);
        let runner_ref = Arc::clone(&runner_arc);
        let sender_ref = Arc::clone(&sender);
        let run_handle =
            std::thread::spawn(move || runner_ref.run(&mut scanner));

        // Wait for receiver to collect all 3 entries.
        let received = recv_handle.join().unwrap();
        assert_eq!(received.len(), 3);
        assert_eq!(received[0], (1, 10, vec![0xAA]));
        assert_eq!(received[1], (2, 20, vec![0xBB, 0xCC]));
        assert_eq!(received[2], (3, 30, vec![]));

        // Verify ack was tracked.
        //
        // The receiver thread sends one ack per frame and exits as soon as
        // the third ack is queued — but `FeederRunner::run()` is on a
        // separate thread that polls the channel with a 1 ms timeout and
        // then sleeps 5 ms between polls. By the time `recv_handle.join()`
        // returns, the runner may not yet have drained all three acks
        // from the queue. Poll briefly (≤ 100 ms) for the runner to catch
        // up. 100 ms is ~6× the worst-case three-ack drain time on a
        // healthy machine, so a real perf regression on the runner's
        // ack-handling path will still surface as a test failure.
        let deadline = Instant::now() + Duration::from_millis(100);
        while runner_arc.known_replica_vlsn() < 3 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(
            runner_arc.known_replica_vlsn() == 3,
            "FeederRunner did not drain all 3 acks within 100 ms; \
             known_replica_vlsn() == {}, expected 3 (the receiver thread \
             sent 3 acks before exiting; the runner reads them one at a \
             time with a 1 ms timeout + 5 ms sleep cycle)",
            runner_arc.known_replica_vlsn()
        );

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
        assert!(
            result.is_ok(),
            "expected Ok on channel close, got {:?}",
            result
        );
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
        assert_eq!(messages[2].len(), (8 + 1));

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

    // -----------------------------------------------------------------------
    // FeederRunner edge cases (acks, runner restart)
    // -----------------------------------------------------------------------

    /// Build a sender + receiver channel pair and a runner that sends
    /// no entries (used by ack-handling edge cases that don't care about
    /// the entry stream).
    fn make_runner_with_pair(
        vlsn_start: u64,
    ) -> (Arc<dyn Channel>, Arc<dyn Channel>, FeederRunner) {
        let pair = LocalChannelPair::new();
        let sender: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);
        let runner = FeederRunner::new(Arc::clone(&sender), vlsn_start);
        (sender, receiver, runner)
    }

    #[test]
    fn test_feeder_runner_short_ack_is_ignored_then_close() {
        // Send a 4-byte "ack" (less than 8 bytes) — the runner must
        // not crash, must not advance known_replica_vlsn, and must
        // continue until the channel closes.
        let (sender, receiver, runner) = make_runner_with_pair(1);
        let receiver_clone = Arc::clone(&receiver);

        let close_handle = std::thread::spawn(move || {
            // Send a malformed (too-short) ack first.
            std::thread::sleep(Duration::from_millis(20));
            receiver_clone.send(&[0xAA, 0xBB, 0xCC]).unwrap();
            // Then close.
            std::thread::sleep(Duration::from_millis(40));
            sender.close().unwrap();
            receiver_clone.close().unwrap();
        });

        let mut scanner = VecLogScanner::new(vec![]);
        let r = runner.run(&mut scanner);
        assert!(r.is_ok(), "short-ack path should not error: {:?}", r);
        assert_eq!(
            runner.known_replica_vlsn(),
            0,
            "short ack must NOT advance known_replica_vlsn"
        );
        close_handle.join().unwrap();
    }

    #[test]
    fn test_feeder_runner_ack_advances_known_replica_vlsn() {
        let (sender, receiver, runner) = make_runner_with_pair(1);
        let receiver_clone = Arc::clone(&receiver);

        let close_handle = std::thread::spawn(move || {
            // Send ack vlsn=42, then ack vlsn=10 (must NOT regress
            // the watermark), then close.
            std::thread::sleep(Duration::from_millis(20));
            receiver_clone.send(&42u64.to_le_bytes()).unwrap();
            std::thread::sleep(Duration::from_millis(20));
            receiver_clone.send(&10u64.to_le_bytes()).unwrap();
            std::thread::sleep(Duration::from_millis(40));
            sender.close().unwrap();
            receiver_clone.close().unwrap();
        });

        let mut scanner = VecLogScanner::new(vec![]);
        let r = runner.run(&mut scanner);
        assert!(r.is_ok(), "ack path should not error: {:?}", r);
        assert_eq!(
            runner.known_replica_vlsn(),
            42,
            "ack must advance to highest, never regress"
        );
        close_handle.join().unwrap();
    }

    #[test]
    fn test_feeder_runner_restart_resumes_from_provided_vlsn() {
        // First run: send entries 1..=3. Stop. New runner starts at
        // vlsn=4. Verify it sends 4..=5 and stops cleanly.
        let entries: [(u64, u8, Vec<u8>); 5] = [
            (1u64, 0u8, b"e1".to_vec()),
            (2, 0, b"e2".to_vec()),
            (3, 0, b"e3".to_vec()),
            (4, 0, b"e4".to_vec()),
            (5, 0, b"e5".to_vec()),
        ];

        // First runner: vlsn_start = 1
        let (sender_a, receiver_a, runner_a) = make_runner_with_pair(1);
        let close_a = {
            let s = Arc::clone(&sender_a);
            let r = Arc::clone(&receiver_a);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(60));
                s.close().unwrap();
                r.close().unwrap();
            })
        };
        let received_a = {
            let r = Arc::clone(&receiver_a);
            std::thread::spawn(move || {
                let mut got = Vec::new();
                while let Ok(Some(frame)) =
                    r.receive(Duration::from_millis(100))
                {
                    got.push(frame);
                }
                got
            })
        };
        let mut scanner_a = VecLogScanner::new(entries[0..3].to_vec());
        runner_a.run(&mut scanner_a).unwrap();
        close_a.join().unwrap();
        let frames_a = received_a.join().unwrap();
        assert_eq!(
            frames_a.len(),
            3,
            "first runner must send 3 entries, got {}",
            frames_a.len()
        );

        // Second runner: vlsn_start = 4 — resumes where the first
        // runner left off (with a fresh channel).
        let (sender_b, receiver_b, runner_b) = make_runner_with_pair(4);
        let close_b = {
            let s = Arc::clone(&sender_b);
            let r = Arc::clone(&receiver_b);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(60));
                s.close().unwrap();
                r.close().unwrap();
            })
        };
        let received_b = {
            let r = Arc::clone(&receiver_b);
            std::thread::spawn(move || {
                let mut got = Vec::new();
                while let Ok(Some(frame)) =
                    r.receive(Duration::from_millis(100))
                {
                    got.push(frame);
                }
                got
            })
        };
        let mut scanner_b = VecLogScanner::new(entries[3..].to_vec());
        runner_b.run(&mut scanner_b).unwrap();
        close_b.join().unwrap();
        let frames_b = received_b.join().unwrap();
        assert_eq!(
            frames_b.len(),
            2,
            "second runner must send 2 entries (4 and 5), got {}",
            frames_b.len()
        );
    }

    #[test]
    fn test_feeder_runner_known_replica_vlsn_initial_zero() {
        let (_sender, _receiver, runner) = make_runner_with_pair(1);
        assert_eq!(runner.known_replica_vlsn(), 0);
    }

    // -----------------------------------------------------------------------
    // EnvironmentLogScanner — exercised against a real EnvironmentImpl
    // -----------------------------------------------------------------------

    #[test]
    fn test_environment_log_scanner_new_with_empty_env() {
        // Open a fresh environment and construct a scanner. Because
        // no log entries have been written, next_entry should return
        // None on the first call.
        let dir = tempfile::tempdir().expect("tempdir");
        let env = EnvironmentImpl::new(dir.path(), false, true)
            .expect("EnvironmentImpl::new");

        let scanner = EnvironmentLogScanner::new(&env, None);
        assert!(scanner.is_some(), "scanner construction should succeed");
        let mut scanner = scanner.unwrap();

        // Empty log → no entries.
        let r = scanner.next_entry(0);
        assert!(
            r.is_none(),
            "next_entry on empty log must return None, got {:?}",
            r
        );
    }

    #[test]
    fn test_environment_log_scanner_with_explicit_null_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let env = EnvironmentImpl::new(dir.path(), false, true)
            .expect("EnvironmentImpl::new");

        // NULL_LSN should be treated the same as None (start from
        // file 0, FILE_HEADER_SIZE offset).
        let scanner = EnvironmentLogScanner::new(&env, Some(NULL_LSN));
        assert!(scanner.is_some());
    }

    #[test]
    fn test_environment_log_scanner_with_explicit_start_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let env = EnvironmentImpl::new(dir.path(), false, true)
            .expect("EnvironmentImpl::new");

        // Non-null start LSN — the scanner should record file=5,
        // offset=128 as its cursor (no actual file at that offset
        // exists, so subsequent reads return None — tests the
        // initialisation path).
        let lsn = Lsn::new(5, 128);
        let scanner = EnvironmentLogScanner::new(&env, Some(lsn));
        assert!(scanner.is_some());
        let mut scanner = scanner.unwrap();
        // No file exists at this LSN — next_entry returns None.
        assert!(scanner.next_entry(0).is_none());
    }
}
