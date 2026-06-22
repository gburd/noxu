//! Peer-to-peer log distribution — master-feeder pattern.
//!
//! Each replica maintains a log range `[first_vlsn,
//! last_vlsn]` and can act as a feeder for other replicas that need entries
//! in that range.  This avoids routing all log-shipping traffic through the
//! master, which would otherwise be the sole bottleneck.
//!
//! ## Architecture
//!
//! ```text
//! Master ──► FeederRunner (master feeder) ──► Replica A
//!                                        ──► Replica B ──► PeerFeederRunner ──► Replica C
//! ```
//!
//! `PeerLogScanner` is a `LogScanner` implementation backed by an
//! in-memory `VecDeque<(vlsn, entry_type, payload)>`.  It is populated by
//! the local `ReplicaReceiver` as entries arrive from the master.
//!
//! `PeerFeederRunner` wraps a `FeederRunner` and a `PeerLogScanner`.  The
//! `GroupService` is queried to find peers that hold the needed VLSN range,
//! and a `FeederRunner` is spawned to stream entries to the requesting node.
//!
//! ## CBVLSN
//!
//! The Cleaner Barrier VLSN is the global minimum `known_vlsn` across all
//! active electable replicas.  The log cleaner uses this to decide which
//! log files it is safe to reclaim.  It is maintained by `GroupService`
//! and updated on every heartbeat / ack.
//!
//! Corresponds to `FeederReplicaSyncup`, `LocalCBVLSNUpdater`, and
//! `RepGroupImpl.getCBVLSN()` in the implementation.

use std::collections::VecDeque;
use std::sync::Arc;

use noxu_sync::Mutex;

use crate::error::{RepError, Result};
use crate::net::channel::Channel;
use crate::net::service_dispatcher::ServiceHandler;
use crate::stream::feeder::{FeederRunner, LogScanner};

/// Service name registered with `TcpServiceDispatcher` for peer log feeds.
pub const PEER_FEEDER_SERVICE_NAME: &str = "PEER_FEEDER";

// ---------------------------------------------------------------------------
// PeerLogScanner
// ---------------------------------------------------------------------------

/// Default maximum number of entries retained in a `PeerLogScanner`
/// in-memory queue.
///
/// Without a bound, every replicated entry stays in RAM until it is
/// consumed by a downstream peer. A long-running replica with no
/// downstream consumers therefore accumulates one VecDeque entry per
/// replicated record forever (audit finding F10).
///
/// 16 384 entries is enough headroom for the slowest expected
/// downstream peer to drain while keeping resident memory bounded
/// (assuming sub-MiB entries, this caps the queue at ~16 GiB worst
/// case; the byte-size cap is the harder bound).
pub const DEFAULT_PEER_SCANNER_MAX_ENTRIES: usize = 16_384;

/// Default maximum total payload size, in bytes, retained in a
/// `PeerLogScanner` queue.  Once the cumulative payload bytes exceed
/// this threshold, the oldest entries are evicted on each `push`.
///
/// 64 MiB matches the channel's `MAX_FRAME_PAYLOAD` and is large
/// enough to absorb a large in-flight transaction without being
/// large enough to OOM a small replica box.
pub const DEFAULT_PEER_SCANNER_MAX_BYTES: usize = 64 * 1024 * 1024;

/// A [`LogScanner`] backed by an in-memory queue of `(vlsn, type, payload)`
/// entries.
///
/// Entries are pushed by the `ReplicaReceiver` as they arrive from the
/// master (or from another peer).  The `PeerFeederRunner` consumes entries
/// from this queue and streams them to a downstream replica.
///
/// ## Bounded memory (F10)
///
/// The queue has two configurable bounds:
///
/// - `max_entries`: maximum entry count (default
///   [`DEFAULT_PEER_SCANNER_MAX_ENTRIES`]).
/// - `max_bytes`: maximum cumulative payload size in bytes
///   (default [`DEFAULT_PEER_SCANNER_MAX_BYTES`]).
///
/// On `push`, if either bound is exceeded the oldest entries are
/// evicted from the front of the queue until both bounds are
/// satisfied.  The evicted entries are no longer available for peer
/// streaming through this scanner; downstream peers that fall behind
/// the eviction window must catch up via the on-disk
/// `EnvironmentLogScanner` or via network restore.  This matches
/// HA semantics where peer-to-peer log distribution is
/// best-effort and the on-disk log is the durable source.
///
/// Closes finding F10 of the 2026 review.
///
/// Thread safety: the queue is protected by a `Mutex` so that the receiver
/// thread (writer) and the feeder thread (reader) can operate concurrently.
pub struct PeerLogScanner {
    queue: Mutex<VecDeque<(u64, u8, Vec<u8>)>>,
    /// The VLSN range currently held in `queue`: `(first, last)`.
    /// Updated lazily on `push`; used by `GroupService` callers to determine
    /// whether this scanner can serve a given VLSN.
    first_vlsn: Mutex<u64>,
    last_vlsn: Mutex<u64>,
    /// Maximum entry count before oldest-evicting begins.
    max_entries: usize,
    /// Maximum cumulative payload bytes before oldest-evicting begins.
    max_bytes: usize,
    /// Current cumulative payload bytes (updated on every push/evict).
    current_bytes: Mutex<usize>,
    /// Cumulative count of entries evicted by the F10 bound (for
    /// observability and tests).
    evicted_count: std::sync::atomic::AtomicU64,
}

impl PeerLogScanner {
    /// Create an empty scanner with the default F10 bounds.
    pub fn new() -> Self {
        Self::with_capacity(
            DEFAULT_PEER_SCANNER_MAX_ENTRIES,
            DEFAULT_PEER_SCANNER_MAX_BYTES,
        )
    }

    /// Create an empty scanner with explicit bounds.
    ///
    /// `max_entries` and `max_bytes` are both honoured; whichever is
    /// breached first triggers oldest-evicting on subsequent `push`
    /// calls.  Passing `usize::MAX` disables the corresponding bound
    /// (not recommended in production).
    pub fn with_capacity(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            first_vlsn: Mutex::new(0),
            last_vlsn: Mutex::new(0),
            max_entries: max_entries.max(1),
            max_bytes: max_bytes.max(1),
            current_bytes: Mutex::new(0),
            evicted_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Push a log entry into the scanner's queue.
    ///
    /// Called by the `ReplicaReceiver` each time an entry is applied.
    /// Entries are expected to be pushed in VLSN order, but this method is
    /// not enforcing: every entry is appended to the queue unconditionally
    /// and the cached `(first_vlsn, last_vlsn)` range is widened to cover
    /// the new VLSN. Out-of-order or duplicate entries are filtered later
    /// by [`LogScanner::next_entry`](crate::stream::feeder::LogScanner),
    /// which skips entries with `vlsn < from_vlsn`.
    ///
    /// **F10 bound**: after the new entry is appended, if the queue
    /// exceeds either `max_entries` or `max_bytes`, the oldest entries
    /// are evicted from the front until both bounds are satisfied. The
    /// retained `first_vlsn` is updated to the new front-of-queue VLSN
    /// so downstream peers that ask for an evicted VLSN range observe
    /// `log_range().first > from_vlsn` and know they must catch up via
    /// the durable log.
    pub fn push(&self, vlsn: u64, entry_type: u8, payload: Vec<u8>) {
        let payload_len = payload.len();
        {
            let mut last = self.last_vlsn.lock();
            if vlsn > *last {
                *last = vlsn;
            }
        }
        let mut q = self.queue.lock();
        let mut current_bytes = self.current_bytes.lock();
        // Append unconditionally.
        q.push_back((vlsn, entry_type, payload));
        *current_bytes += payload_len;

        // F10 eviction: drop oldest until both bounds are honoured.
        let mut evicted = 0u64;
        while q.len() > self.max_entries || *current_bytes > self.max_bytes {
            if let Some((_evicted_vlsn, _ty, evicted_payload)) = q.pop_front() {
                *current_bytes =
                    current_bytes.saturating_sub(evicted_payload.len());
                evicted += 1;
            } else {
                break;
            }
        }
        if evicted > 0 {
            self.evicted_count
                .fetch_add(evicted, std::sync::atomic::Ordering::Relaxed);
        }
        // Refresh first_vlsn from the (possibly mutated) queue front.
        let new_first = q.front().map(|(v, _, _)| *v).unwrap_or(0);
        drop(current_bytes);
        drop(q);
        *self.first_vlsn.lock() = new_first;
    }

    /// Cumulative number of entries dropped by the F10 bound since
    /// scanner construction.  Useful for monitoring whether downstream
    /// peers are keeping up.
    pub fn evicted_count(&self) -> u64 {
        self.evicted_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Current cumulative payload size in bytes (live snapshot).
    pub fn current_bytes(&self) -> usize {
        *self.current_bytes.lock()
    }

    /// Return the VLSN range currently held in this scanner.
    ///
    /// Returns `None` if the scanner is empty (no entries pushed yet).
    pub fn log_range(&self) -> Option<(u64, u64)> {
        let first = *self.first_vlsn.lock();
        let last = *self.last_vlsn.lock();
        if first == 0 { None } else { Some((first, last)) }
    }

    /// Return the number of entries currently queued.
    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }

    /// Returns true if no entries are queued.
    pub fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }
}

impl Default for PeerLogScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl LogScanner for PeerLogScanner {
    fn next_entry(&mut self, from_vlsn: u64) -> Option<(u64, u8, Vec<u8>)> {
        let mut q = self.queue.lock();
        // Skip entries with VLSN < from_vlsn (they were already seen by the
        // downstream replica). Track byte-budget reduction so the F10
        // bound stays accurate.
        let mut current_bytes = self.current_bytes.lock();
        while let Some(&(vlsn, _, _)) = q.front() {
            if vlsn >= from_vlsn {
                let entry = q.pop_front();
                if let Some((_, _, ref payload)) = entry {
                    *current_bytes =
                        current_bytes.saturating_sub(payload.len());
                }
                let new_first = q.front().map(|(v, _, _)| *v).unwrap_or(0);
                drop(current_bytes);
                drop(q);
                *self.first_vlsn.lock() = new_first;
                return entry;
            }
            if let Some((_, _, evicted_payload)) = q.pop_front() {
                *current_bytes =
                    current_bytes.saturating_sub(evicted_payload.len());
            }
        }
        let new_first = q.front().map(|(v, _, _)| *v).unwrap_or(0);
        drop(current_bytes);
        drop(q);
        *self.first_vlsn.lock() = new_first;
        None
    }
}

// ---------------------------------------------------------------------------
// PeerFeederSource — Arc-wrapped PeerLogScanner that implements LogScanner
// ---------------------------------------------------------------------------

/// A shared, `Arc`-wrapped `PeerLogScanner` that can be passed between
/// threads.
///
/// The `ReplicaReceiver` holds an `Arc<PeerFeederSource>` and calls
/// `push()` as entries arrive. A `PeerFeederRunner` holds another clone
/// and calls `next_entry()` to stream those entries downstream.
pub struct PeerFeederSource(pub Arc<PeerLogScanner>);

impl PeerFeederSource {
    /// Create a new `PeerFeederSource` backed by a fresh `PeerLogScanner`.
    pub fn new() -> Self {
        Self(Arc::new(PeerLogScanner::new()))
    }

    /// Return a clone of the inner `Arc<PeerLogScanner>` for the receiver
    /// thread to use when pushing entries.
    pub fn clone_scanner(&self) -> Arc<PeerLogScanner> {
        Arc::clone(&self.0)
    }
}

impl Default for PeerFeederSource {
    fn default() -> Self {
        Self::new()
    }
}

/// Adapter so `PeerFeederSource` can be used directly as a `LogScanner`.
///
/// We implement `LogScanner` on the *mutable reference* side: since
/// `PeerFeederSource` is `Arc`-wrapped, we implement on a thin wrapper
/// struct that holds an `Arc` and a local cursor.
pub struct PeerScannerAdapter {
    source: Arc<PeerLogScanner>,
    cursor_vlsn: u64,
}

impl PeerScannerAdapter {
    /// Create a new adapter starting from `start_vlsn`.
    pub fn new(source: Arc<PeerLogScanner>, start_vlsn: u64) -> Self {
        Self { source, cursor_vlsn: start_vlsn }
    }
}

impl LogScanner for PeerScannerAdapter {
    fn next_entry(&mut self, from_vlsn: u64) -> Option<(u64, u8, Vec<u8>)> {
        let effective_from = self.cursor_vlsn.max(from_vlsn);
        let entry = {
            let mut q = self.source.queue.lock();
            let mut result = None;
            while let Some(&(vlsn, _, _)) = q.front() {
                if vlsn >= effective_from {
                    result = q.pop_front();
                    break;
                }
                q.pop_front(); // discard stale entries
            }
            result
        };
        if let Some((vlsn, _, _)) = &entry {
            self.cursor_vlsn = vlsn + 1;
        }
        entry
    }
}

// ---------------------------------------------------------------------------
// PeerFeederRunner
// ---------------------------------------------------------------------------

/// Streams log entries from a `PeerLogScanner` to a downstream replica.
///
/// This is a thin wrapper around `FeederRunner` that uses a
/// `PeerScannerAdapter` as the log source instead of reading from disk.
///
/// Corresponds to 's peer-to-peer feeder path in `FeederReplicaSyncup`.
pub struct PeerFeederRunner {
    inner: FeederRunner,
    source: Arc<PeerLogScanner>,
    start_vlsn: u64,
}

impl PeerFeederRunner {
    /// Create a new peer feeder that streams entries from `source` to
    /// `channel`, starting at `start_vlsn`.
    pub fn new(
        channel: Arc<dyn Channel>,
        source: Arc<PeerLogScanner>,
        start_vlsn: u64,
    ) -> Self {
        let inner = FeederRunner::new(channel, start_vlsn);
        Self { inner, source, start_vlsn }
    }

    /// Run the peer feeder loop.
    ///
    /// Streams entries from the `PeerLogScanner` to the downstream replica.
    /// Returns when the channel is closed or an I/O error occurs.
    pub fn run(&self) -> Result<()> {
        let mut adapter =
            PeerScannerAdapter::new(Arc::clone(&self.source), self.start_vlsn);
        self.inner.run(&mut adapter)
    }

    /// Return the last VLSN acknowledged by the downstream replica.
    pub fn known_replica_vlsn(&self) -> u64 {
        self.inner.known_replica_vlsn()
    }
}

// ---------------------------------------------------------------------------
// Syncup helpers
// ---------------------------------------------------------------------------

/// Result of a peer syncup negotiation.
///
///  HA, `FeederReplicaSyncup` finds the highest VLSN that is committed
/// on BOTH the feeder and the replica (the "matchpoint").  The feeder then
/// streams entries from matchpoint+1 onwards.
///
/// We model this as a simple VLSN range comparison: if the peer holds
/// `[peer_first, peer_last]` and the replica needs `replica_needs` onwards,
/// we can serve if `peer_first <= replica_needs <= peer_last`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncupResult {
    /// The peer holds the needed range; stream from `start_vlsn`.
    CanServe { start_vlsn: u64 },
    /// The peer does not hold the needed VLSN; fall back to master or
    /// network restore.
    NeedsRestore,
}

/// Determine whether `peer_range` can serve a replica that needs
/// log entries starting from `replica_needs`.
///
/// This is the range-availability check only (CanServe/NeedsRestore). The
/// diverged-tail matchpoint search + rollback decision (REP-1 STEP 5) lives in
/// [`crate::stream::syncup`] (`find_matchpoint` + `verify_rollback`, ported
/// from JE `ReplicaFeederSyncup`); the live driver replaces this range check
/// with that decision core once the syncup wire protocol + backward reader
/// land. See `docs/src/maintainer/design-decisions.md` (REP-1).
pub fn negotiate_syncup(
    peer_range: Option<(u64, u64)>,
    replica_needs: u64,
) -> SyncupResult {
    match peer_range {
        Some((first, last))
            if first <= replica_needs && replica_needs <= last =>
        {
            SyncupResult::CanServe { start_vlsn: replica_needs }
        }
        _ => SyncupResult::NeedsRestore,
    }
}

// ---------------------------------------------------------------------------
// PeerFeederService — TCP ServiceHandler backed by a PeerLogScanner
// ---------------------------------------------------------------------------

/// A [`ServiceHandler`] that streams peer-held log entries to a requesting
/// downstream replica over the `"PEER_FEEDER"` service.
///
/// The service is registered on the `TcpServiceDispatcher` at startup.
/// When a downstream replica connects, the protocol is:
///
/// 1. The downstream sends `[start_vlsn: u64 LE]` (8 bytes) — the first VLSN
///    it needs.
/// 2. The server negotiates via `negotiate_syncup()`.
/// 3. If the range is available, a `PeerFeederRunner` is spawned in a new
///    thread and streams entries until the channel closes.
/// 4. If the range is not available, the server responds with
///    `[NEEDS_RESTORE: u8 = 1]` and closes the connection.
///
/// The `PeerLogScanner` (`source`) is populated by the node's own
/// `ReplicaReceiver` as entries arrive from the master.
pub struct PeerFeederService {
    source: Arc<PeerLogScanner>,
}

impl PeerFeederService {
    /// Create a new service backed by `source`.
    pub fn new(source: Arc<PeerLogScanner>) -> Self {
        Self { source }
    }
}

/// Wire-level response codes sent by the server.
const PEER_FEEDER_CAN_SERVE: u8 = 0;
const PEER_FEEDER_NEEDS_RESTORE: u8 = 1;

impl ServiceHandler for PeerFeederService {
    fn service_name(&self) -> &str {
        PEER_FEEDER_SERVICE_NAME
    }

    fn handle(&self, channel: Box<dyn Channel>) -> Result<()> {
        use std::time::Duration;

        // 1. Read the 8-byte start_vlsn from the downstream replica.
        let msg =
            channel.receive(Duration::from_secs(30))?.ok_or_else(|| {
                RepError::NetworkError(
                    "PEER_FEEDER: no start_vlsn received".into(),
                )
            })?;

        if msg.len() < 8 {
            return Err(RepError::NetworkError(format!(
                "PEER_FEEDER: short handshake ({} bytes)",
                msg.len()
            )));
        }
        let start_vlsn =
            u64::from_le_bytes(msg[..8].try_into().expect("slice of 8 bytes"));

        // 2. Negotiate: do we hold the requested VLSN range?
        let range = self.source.log_range();
        match negotiate_syncup(range, start_vlsn) {
            SyncupResult::CanServe { start_vlsn: sv } => {
                // Tell the downstream it can proceed.
                channel.send(&[PEER_FEEDER_CAN_SERVE])?;

                // 3. Stream entries in this thread (the dispatcher already
                //    called us from a per-connection thread).
                let channel_arc: Arc<dyn Channel> = Arc::from(channel);
                let runner = PeerFeederRunner::new(
                    channel_arc,
                    Arc::clone(&self.source),
                    sv,
                );
                let _ = runner.run();
                Ok(())
            }
            SyncupResult::NeedsRestore => {
                channel.send(&[PEER_FEEDER_NEEDS_RESTORE])?;
                Err(RepError::NetworkError(format!(
                    "PEER_FEEDER: cannot serve vlsn={start_vlsn}, \
                     range={range:?}"
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Client-side peer catch-up
// ---------------------------------------------------------------------------

/// Connect to a peer node's `PEER_FEEDER` service and pull log entries
/// starting from `start_vlsn`.
///
/// This is the client counterpart to [`PeerFeederService`].  It is called
/// by a replica that is behind and wants to catch up from a peer that holds
/// the needed VLSN range (rather than routing all traffic through the master).
///
/// Protocol (matches `PeerFeederService::handle`):
///   1. Open a TCP connection and request the `"PEER_FEEDER"` service via
///      `service_dispatcher::connect_to_service()`.
///   2. Send `[start_vlsn: u64 LE]`.
///   3. Read the one-byte response:
///      - `0` (`PEER_FEEDER_CAN_SERVE`) — peer has the range; proceed.
///      - `1` (`PEER_FEEDER_NEEDS_RESTORE`) — peer cannot serve; return
///        `Ok(false)` so the caller can fall back to the master.
///   4. Start a `ReplicaReceiver` loop on the channel, passing each entry
///      to `log_writer`.  Returns `Ok(true)` when the peer closes the
///      channel (i.e. the catch-up is complete).
///
/// # Pipelining
///
/// `catch_up_from_peer` is intentionally non-async.  Call it from a
/// dedicated thread per peer.  To pipeline catch-up from multiple peers
/// simultaneously, spawn one thread per peer (e.g. from a `ThreadPool`).
/// The [`MultiPeerCatchUp`] helper below manages this.
pub fn catch_up_from_peer(
    peer_addr: std::net::SocketAddr,
    start_vlsn: u64,
    log_writer: &mut dyn crate::stream::replica_stream::LogWriter,
) -> Result<bool> {
    use crate::net::service_dispatcher::connect_to_service;
    use crate::stream::replica_stream::ReplicaReceiver;
    use std::sync::Arc;
    use std::time::Duration;

    // Connect and request the PEER_FEEDER service.
    let channel = connect_to_service(peer_addr, PEER_FEEDER_SERVICE_NAME)?;

    // Send start_vlsn.
    channel.send(&start_vlsn.to_le_bytes())?;

    // Read the one-byte response.
    let resp = channel.receive(Duration::from_secs(30))?.ok_or_else(|| {
        RepError::NetworkError("no response from peer feeder".into())
    })?;
    if resp.is_empty() {
        return Err(RepError::NetworkError(
            "empty response from peer feeder".into(),
        ));
    }
    match resp[0] {
        PEER_FEEDER_CAN_SERVE => {}
        PEER_FEEDER_NEEDS_RESTORE => return Ok(false),
        other => {
            return Err(RepError::ProtocolError(format!(
                "peer feeder unknown response byte: {other:#x}"
            )));
        }
    }

    // Run the replica receive loop.
    let channel_arc: Arc<dyn Channel> = Arc::from(channel);
    let receiver = ReplicaReceiver::new(channel_arc);
    receiver.run(log_writer)?;

    Ok(true)
}

/// Pipelined catch-up from multiple peer nodes simultaneously.
///
/// Spawns one thread per peer in `peers` and waits for all to finish (or
/// for the first to succeed).  Returns the name of the peer that supplied
/// the entries, or `None` if no peer could serve the range.
///
/// The `log_writer_factory` closure is called once per thread to produce a
/// per-thread `LogWriter`.  The factory must be `Send + Sync`.
pub struct MultiPeerCatchUp {
    peers: Vec<(String, std::net::SocketAddr)>,
    start_vlsn: u64,
}

impl MultiPeerCatchUp {
    /// Create a new multi-peer catch-up request.
    ///
    /// `peers` is a list of `(node_name, socket_addr)` pairs to try.
    pub fn new(
        peers: Vec<(String, std::net::SocketAddr)>,
        start_vlsn: u64,
    ) -> Self {
        Self { peers, start_vlsn }
    }

    /// Run pipelined catch-up.
    ///
    /// Spawns one thread per peer and waits for the first to succeed.
    /// Each thread calls `catch_up_from_peer`; the winning thread's entries
    /// are applied through `make_writer()`.  Other threads are joined once
    /// the first succeeds.
    ///
    /// Returns the name of the first peer that successfully served the range,
    /// or `None` if all peers declined.
    pub fn run<F, W>(self, make_writer: F) -> Option<String>
    where
        F: Fn() -> W + Send + Sync + 'static,
        W: crate::stream::replica_stream::LogWriter + Send + 'static,
    {
        use std::sync::atomic::{AtomicBool, Ordering};
        let make_writer = std::sync::Arc::new(make_writer);
        let done = std::sync::Arc::new(AtomicBool::new(false));
        let winner: std::sync::Arc<noxu_sync::Mutex<Option<String>>> =
            std::sync::Arc::new(noxu_sync::Mutex::new(None));

        let mut handles = Vec::new();

        for (name, addr) in self.peers {
            let make_writer = std::sync::Arc::clone(&make_writer);
            let done = std::sync::Arc::clone(&done);
            let winner = std::sync::Arc::clone(&winner);
            let start_vlsn = self.start_vlsn;
            let name_clone = name.clone();

            let handle = std::thread::Builder::new()
                .name(format!("noxu-peer-catchup-{}", name))
                .spawn(move || {
                    if done.load(Ordering::Acquire) {
                        return; // another peer already won
                    }
                    let mut writer = make_writer();
                    match catch_up_from_peer(addr, start_vlsn, &mut writer) {
                        Ok(true) => {
                            if !done.swap(true, Ordering::AcqRel) {
                                *winner.lock() = Some(name_clone);
                            }
                        }
                        Ok(false) => {
                            log::debug!(
                                "peer '{}' cannot serve vlsn={start_vlsn}",
                                name
                            );
                        }
                        Err(e) => {
                            log::warn!(
                                "catch-up from peer '{}' failed: {e}",
                                name
                            );
                        }
                    }
                })
                .expect("failed to spawn peer catch-up thread");

            handles.push(handle);
        }

        for h in handles {
            let _ = h.join();
        }

        winner.lock().clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::channel::LocalChannelPair;
    use std::time::Duration;

    // -----------------------------------------------------------------------
    // PeerLogScanner unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_peer_scanner_push_and_log_range() {
        let scanner = PeerLogScanner::new();
        assert!(scanner.is_empty());
        assert!(scanner.log_range().is_none());

        scanner.push(5, 1, b"entry5".to_vec());
        scanner.push(6, 1, b"entry6".to_vec());
        scanner.push(10, 1, b"entry10".to_vec());

        assert_eq!(scanner.len(), 3);
        assert_eq!(scanner.log_range(), Some((5, 10)));
    }

    #[test]
    fn test_peer_scanner_next_entry_in_order() {
        let mut scanner = PeerLogScanner::new();
        for vlsn in [3u64, 4, 5, 6, 7] {
            scanner.push(vlsn, 1, vlsn.to_le_bytes().to_vec());
        }

        // Ask from_vlsn=4 — should skip 3 and return 4, 5, 6, 7.
        let mut results = Vec::new();
        while let Some((vlsn, _, _)) = scanner.next_entry(4) {
            results.push(vlsn);
        }
        assert_eq!(results, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_peer_scanner_skips_stale_entries() {
        let mut scanner = PeerLogScanner::new();
        for v in [1u64, 2, 3, 10, 11] {
            scanner.push(v, 1, vec![v as u8]);
        }
        // Ask from 10 — entries 1, 2, 3 are stale.
        let entry = scanner.next_entry(10);
        assert_eq!(entry.map(|(v, _, _)| v), Some(10));
    }

    #[test]
    fn test_peer_scanner_empty_returns_none() {
        let mut scanner = PeerLogScanner::new();
        assert!(scanner.next_entry(1).is_none());
    }

    // -----------------------------------------------------------------------
    // PeerScannerAdapter unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_peer_scanner_adapter_cursor_advances() {
        let source = Arc::new(PeerLogScanner::new());
        for v in [1u64, 2, 3, 4, 5] {
            source.push(v, 1, vec![v as u8]);
        }

        let mut adapter = PeerScannerAdapter::new(Arc::clone(&source), 1);
        let mut seen = Vec::new();
        while let Some((v, _, _)) = adapter.next_entry(1) {
            seen.push(v);
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);
    }

    // -----------------------------------------------------------------------
    // PeerFeederRunner integration test
    // -----------------------------------------------------------------------

    #[test]
    fn test_peer_feeder_runner_streams_to_replica() {
        let source = Arc::new(PeerLogScanner::new());
        for v in [10u64, 11, 12, 13, 14] {
            source.push(v, 1, format!("payload-{v}").into_bytes());
        }

        let pair = LocalChannelPair::new();
        let sender: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Receiver: collect all frames.
        let recv_handle = {
            let receiver = Arc::clone(&receiver);
            std::thread::spawn(move || {
                let mut vlsns = Vec::new();
                // Expect 5 frames then a timeout.
                for _ in 0..5 {
                    let frame = receiver
                        .receive(Duration::from_secs(5))
                        .unwrap()
                        .unwrap();
                    let vlsn =
                        u64::from_le_bytes(frame[0..8].try_into().unwrap());
                    vlsns.push(vlsn);
                    // Send ack.
                    let _ = receiver.send(&vlsn.to_le_bytes());
                }
                vlsns
            })
        };

        let runner =
            PeerFeederRunner::new(Arc::clone(&sender), Arc::clone(&source), 10);
        let sender_clone = Arc::clone(&sender);
        let run_handle = std::thread::spawn(move || {
            let _ = runner.run();
        });

        // Wait for receiver to collect all 5 frames.
        let vlsns = recv_handle.join().unwrap();
        assert_eq!(vlsns, vec![10, 11, 12, 13, 14]);

        sender_clone.close().unwrap();
        let _ = run_handle.join();
    }

    // -----------------------------------------------------------------------
    // negotiate_syncup tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_negotiate_syncup_can_serve() {
        assert_eq!(
            negotiate_syncup(Some((5, 20)), 10),
            SyncupResult::CanServe { start_vlsn: 10 }
        );
        // Exact boundary.
        assert_eq!(
            negotiate_syncup(Some((10, 10)), 10),
            SyncupResult::CanServe { start_vlsn: 10 }
        );
    }

    #[test]
    fn test_negotiate_syncup_needs_restore_too_early() {
        // Replica needs VLSN 3 but peer only has [5, 20].
        assert_eq!(
            negotiate_syncup(Some((5, 20)), 3),
            SyncupResult::NeedsRestore
        );
    }

    #[test]
    fn test_negotiate_syncup_needs_restore_too_late() {
        // Replica needs VLSN 25 but peer only has [5, 20].
        assert_eq!(
            negotiate_syncup(Some((5, 20)), 25),
            SyncupResult::NeedsRestore
        );
    }

    #[test]
    fn test_negotiate_syncup_no_range() {
        // Peer has no log range (just joined or restoring).
        assert_eq!(negotiate_syncup(None, 10), SyncupResult::NeedsRestore);
    }

    // -----------------------------------------------------------------------
    // GroupService CBVLSN integration
    // -----------------------------------------------------------------------

    #[test]
    fn test_group_service_cbvlsn_tracks_minimum() {
        use crate::group_service::{GroupService, NodeInfo};
        use crate::node_type::NodeType;
        use std::time::Instant;

        let gs = GroupService::new("test_group".to_string());

        // Add 3 electable nodes.
        for (name, port) in [("n1", 5001u16), ("n2", 5002), ("n3", 5003)] {
            gs.add_node(NodeInfo {
                name: name.to_string(),
                node_type: NodeType::Electable,
                host: "localhost".to_string(),
                port,
                node_id: port as u32,
                joined_at: Instant::now(),
                last_seen: Instant::now(),
                is_active: true,
                known_vlsn: 0,
                log_range: None,
                read_capacity_pct: 100,
                write_capacity_pct: 100,
                latency_hint_ms: 1,
            })
            .unwrap();
        }

        // Initially all known_vlsn = 0 → CBVLSN = 0.
        assert_eq!(gs.get_cbvlsn(), 0);

        // Update n1 and n2 but not n3 → CBVLSN = min(50, 40, 0) = 0.
        gs.update_node_vlsn("n1", 50);
        gs.update_node_vlsn("n2", 40);
        assert_eq!(gs.get_cbvlsn(), 0, "n3 still at 0, CBVLSN must be 0");

        // Now n3 also updates → CBVLSN = min(50, 40, 30) = 30.
        gs.update_node_vlsn("n3", 30);
        assert_eq!(gs.get_cbvlsn(), 30);

        // n3 advances → min(50, 40, 45) = 40.
        gs.update_node_vlsn("n3", 45);
        assert_eq!(gs.get_cbvlsn(), 40);
    }

    #[test]
    fn test_group_service_cbvlsn_monotone_nondecreasing() {
        use crate::group_service::{GroupService, NodeInfo};
        use crate::node_type::NodeType;
        use std::time::Instant;

        let gs = GroupService::new("cbvlsn_monotone".to_string());

        for (name, port) in [("a", 5001u16), ("b", 5002)] {
            gs.add_node(NodeInfo {
                name: name.to_string(),
                node_type: NodeType::Electable,
                host: "localhost".to_string(),
                port,
                node_id: port as u32,
                joined_at: Instant::now(),
                last_seen: Instant::now(),
                is_active: true,
                known_vlsn: 0,
                log_range: None,
                read_capacity_pct: 100,
                write_capacity_pct: 100,
                latency_hint_ms: 1,
            })
            .unwrap();
        }

        // CBVLSN must never decrease.
        let mut prev = 0u64;
        for (na, va, nb, vb) in [
            ("a", 10u64, "b", 5u64),
            ("a", 20, "b", 15),
            ("a", 25, "b", 22),
            ("a", 30, "b", 28),
        ] {
            gs.update_node_vlsn(na, va);
            gs.update_node_vlsn(nb, vb);
            let cbvlsn = gs.get_cbvlsn();
            assert!(
                cbvlsn >= prev,
                "CBVLSN must not decrease: was {prev}, now {cbvlsn}"
            );
            prev = cbvlsn;
        }
    }

    #[test]
    fn test_group_service_find_peers_with_vlsn() {
        use crate::group_service::{GroupService, NodeInfo};
        use crate::node_type::NodeType;
        use std::time::Instant;

        let gs = GroupService::new("peer_select".to_string());

        // Node a: holds [1, 100]
        // Node b: holds [50, 200]
        // Node c: no range
        for (name, port, range) in [
            ("a", 5001u16, Some((1u64, 100u64))),
            ("b", 5002, Some((50, 200))),
            ("c", 5003, None),
        ] {
            let mut info = NodeInfo {
                name: name.to_string(),
                node_type: NodeType::Electable,
                host: "localhost".to_string(),
                port,
                node_id: port as u32,
                joined_at: Instant::now(),
                last_seen: Instant::now(),
                is_active: true,
                known_vlsn: 0,
                log_range: range,
                read_capacity_pct: 100,
                write_capacity_pct: 100,
                latency_hint_ms: 1,
            };
            // Set last_seen differently so we can check sort order.
            info.last_seen = Instant::now()
                - std::time::Duration::from_millis(port as u64 * 10);
            gs.add_node(info).unwrap();
        }

        // VLSN 75: only a and b hold it.
        let peers = gs.find_peers_with_vlsn(75);
        assert!(peers.contains(&"a".to_string()));
        assert!(peers.contains(&"b".to_string()));
        assert!(!peers.contains(&"c".to_string()));

        // VLSN 150: only b holds it.
        let peers = gs.find_peers_with_vlsn(150);
        assert_eq!(peers, vec!["b".to_string()]);

        // VLSN 201: nobody holds it.
        assert!(gs.find_peers_with_vlsn(201).is_empty());
    }

    #[test]
    fn test_group_service_update_log_range() {
        use crate::group_service::{GroupService, NodeInfo};
        use crate::node_type::NodeType;
        use std::time::Instant;

        let gs = GroupService::new("log_range_test".to_string());
        gs.add_node(NodeInfo {
            name: "r1".to_string(),
            node_type: NodeType::Electable,
            host: "localhost".to_string(),
            port: 5001,
            node_id: 1,
            joined_at: Instant::now(),
            last_seen: Instant::now(),
            is_active: true,
            known_vlsn: 0,
            log_range: None,
            read_capacity_pct: 100,
            write_capacity_pct: 100,
            latency_hint_ms: 1,
        })
        .unwrap();

        // Initially no range.
        assert!(gs.get_node("r1").unwrap().log_range.is_none());

        // Update range.
        gs.update_node_log_range("r1", 100, 500);
        assert_eq!(gs.get_node("r1").unwrap().log_range, Some((100, 500)));

        // Extend range.
        gs.update_node_log_range("r1", 100, 800);
        assert_eq!(gs.get_node("r1").unwrap().log_range, Some((100, 800)));
    }

    // -----------------------------------------------------------------------
    // PeerFeederService::handle paths
    // -----------------------------------------------------------------------

    #[test]
    fn test_peer_feeder_service_can_serve() {
        use crate::net::channel::LocalChannelPair;

        // Source has range [10, 20]. Client requests start_vlsn=15
        // (in range). Service should respond with CAN_SERVE then
        // stream entries until close.
        let source = Arc::new(PeerLogScanner::new());
        for v in 10u64..=20 {
            source.push(v, 0, format!("e{}", v).into_bytes());
        }
        let svc = PeerFeederService::new(Arc::clone(&source));

        let pair = LocalChannelPair::new();
        let server_ch: Box<dyn Channel> = Box::new(pair.channel_a);
        let client_ch = pair.channel_b;

        // Client sends start_vlsn=15.
        client_ch.send(&15u64.to_le_bytes()).unwrap();

        // Service runs in another thread so we can observe the
        // streaming side.
        let svc_handle = std::thread::spawn(move || svc.handle(server_ch));

        // Read the 1-byte response.
        let resp = client_ch.receive(Duration::from_secs(2)).unwrap().unwrap();
        assert_eq!(
            resp,
            vec![PEER_FEEDER_CAN_SERVE],
            "service should reply CAN_SERVE for in-range start_vlsn"
        );

        // Drain a few frames then close to terminate the runner.
        let mut frames = 0;
        while let Ok(Some(_)) = client_ch.receive(Duration::from_millis(80)) {
            frames += 1;
            if frames >= 3 {
                break;
            }
        }
        // Close client side so runner returns.
        client_ch.close().unwrap();
        let _ = svc_handle.join().unwrap();
        assert!(frames >= 1, "service must have streamed at least one frame");
    }

    #[test]
    fn test_peer_feeder_service_needs_restore() {
        use crate::net::channel::LocalChannelPair;

        // Source has range [10, 20]. Client requests start_vlsn=5
        // (too early). Service replies NEEDS_RESTORE and errors.
        let source = Arc::new(PeerLogScanner::new());
        for v in 10u64..=20 {
            source.push(v, 0, vec![]);
        }
        let svc = PeerFeederService::new(Arc::clone(&source));

        let pair = LocalChannelPair::new();
        let server_ch: Box<dyn Channel> = Box::new(pair.channel_a);
        let client_ch = pair.channel_b;

        client_ch.send(&5u64.to_le_bytes()).unwrap();

        let r = svc.handle(server_ch);
        assert!(r.is_err(), "service must return Err on NEEDS_RESTORE");
        let resp = client_ch.receive(Duration::from_secs(2)).unwrap().unwrap();
        assert_eq!(
            resp,
            vec![PEER_FEEDER_NEEDS_RESTORE],
            "service should reply NEEDS_RESTORE for out-of-range start_vlsn"
        );
    }

    #[test]
    fn test_peer_feeder_service_short_handshake_errors() {
        use crate::net::channel::LocalChannelPair;

        let source = Arc::new(PeerLogScanner::new());
        let svc = PeerFeederService::new(Arc::clone(&source));

        let pair = LocalChannelPair::new();
        let server_ch: Box<dyn Channel> = Box::new(pair.channel_a);
        let client_ch = pair.channel_b;

        // Send only 4 bytes — too short to be a valid u64
        // start_vlsn. Service must Err with "short handshake".
        client_ch.send(&[0u8; 4]).unwrap();

        let r = svc.handle(server_ch);
        assert!(r.is_err(), "short handshake must error");
        let msg = format!("{}", r.err().unwrap());
        assert!(
            msg.contains("short handshake"),
            "expected 'short handshake' in error, got: {msg}"
        );
    }

    #[test]
    fn test_peer_feeder_service_no_handshake_errors() {
        use crate::net::channel::LocalChannelPair;

        let source = Arc::new(PeerLogScanner::new());
        let svc = PeerFeederService::new(Arc::clone(&source));

        let pair = LocalChannelPair::new();
        let server_ch: Box<dyn Channel> = Box::new(pair.channel_a);
        // Drop client side so receive returns None (no message).
        drop(pair.channel_b);

        let r = svc.handle(server_ch);
        assert!(r.is_err(), "no-handshake must error");
    }

    #[test]
    fn test_peer_feeder_service_name() {
        let source = Arc::new(PeerLogScanner::new());
        let svc = PeerFeederService::new(source);
        assert_eq!(
            svc.service_name(),
            PEER_FEEDER_SERVICE_NAME,
            "service_name must match the protocol const"
        );
    }

    // -----------------------------------------------------------------------
    // PeerFeederSource and adapter constructors
    // -----------------------------------------------------------------------

    #[test]
    fn test_peer_feeder_source_default_and_clone_scanner() {
        let src1 = PeerFeederSource::new();
        // default() and new() produce the same shape.
        let src2 = PeerFeederSource::default();
        let s1 = src1.clone_scanner();
        let s2 = src2.clone_scanner();
        // Distinct underlying scanners (each PeerFeederSource owns
        // its own Arc).
        s1.push(1, 0, b"a".to_vec());
        assert_eq!(s1.len(), 1);
        assert_eq!(s2.len(), 0);
    }

    #[test]
    fn test_peer_log_scanner_default_is_empty() {
        let s = PeerLogScanner::default();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert!(s.log_range().is_none());
    }

    #[test]
    fn test_peer_feeder_runner_known_replica_vlsn_initial_zero() {
        use crate::net::channel::LocalChannelPair;

        let pair = LocalChannelPair::new();
        let channel: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let source = Arc::new(PeerLogScanner::new());
        let runner = PeerFeederRunner::new(channel, source, 1);
        assert_eq!(runner.known_replica_vlsn(), 0);
    }

    // -----------------------------------------------------------------------
    // PeerScannerAdapter: stale-entry skipping
    // -----------------------------------------------------------------------

    #[test]
    fn test_peer_scanner_adapter_skips_stale_via_pop_front() {
        // After a known_replica_vlsn advance, push some entries
        // that are below the new floor — the adapter should
        // discard them via pop_front().
        let source = Arc::new(PeerLogScanner::new());
        for v in 1u64..=5 {
            source.push(v, 0, vec![]);
        }
        let mut adapter = PeerScannerAdapter::new(Arc::clone(&source), 3);
        // First call returns vlsn=3, skipping 1 and 2.
        let r = adapter.next_entry(3);
        assert!(r.is_some());
        let (vlsn, _, _) = r.unwrap();
        assert_eq!(vlsn, 3);
        // 4 next.
        let (vlsn, _, _) = adapter.next_entry(4).unwrap();
        assert_eq!(vlsn, 4);
    }
}
