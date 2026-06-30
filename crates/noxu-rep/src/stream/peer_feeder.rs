//! Peer-to-peer log distribution — JE's single feeder mechanism.
//!
//! `PeerFeederService` is this node's service-side feeder accept loop —
//! Noxu's [`com.sleepycat.je.rep.impl.node.FeederManager`]: it is registered
//! once per node on the TCP dispatcher, and on every inbound connection it
//! runs ONE [`FeederRunner`] (JE [`com.sleepycat.je.rep.impl.node.Feeder`])
//! driving a single [`LogScanner`] (JE
//! [`com.sleepycat.je.rep.stream.FeederSource`]).
//!
//! ## One mechanism, master OR replica (JE fidelity)
//!
//! ```text
//!   master ──PEER_FEEDER──► R1 ──PEER_FEEDER──► R2
//!     │  FeederRunner+EnvironmentLogScanner   │  FeederRunner+EnvironmentLogScanner
//!     └── reads master's WAL ─────────────────┴── reads R1's OWN WAL
//! ```
//!
//! JE `FeederSource.java` documents the source as "a real Master OR a
//! Replica in a Replica chain that is replaying log records it received
//! from some other source".  JE `Feeder.initMasterFeederSource(startVLSN)`
//! builds `new MasterFeederSource(repImpl, repNode.getVLSNIndex(), …)`
//! regardless of node role, and its output loop pulls
//! `feederSource.getWireRecord(feederVLSN, heartbeatMs)`
//! (`Feeder.java:1282`).  Noxu mirrors this exactly:
//!
//! | JE                                   | Noxu                                  |
//! |--------------------------------------|---------------------------------------|
//! | `FeederManager` (per-node accept)    | [`PeerFeederService`] (per-node accept) |
//! | `Feeder` thread + output loop        | [`FeederRunner`] (`run`)              |
//! | `FeederSource` / `MasterFeederSource`| [`LogScanner`] / [`EnvironmentLogScanner`] |
//! | `FeederReader` (VLSNIndex+WAL cursor)| [`EnvironmentLogScanner`]             |
//! | `getWireRecord(vlsn, waitTime)`      | [`LogScanner::next_entry`]            |
//!
//! A replica cascading to a downstream replica is the IDENTICAL mechanism as
//! the master feeding a replica — it just reads from the replica's own WAL
//! (which carries the entries it received + persisted via
//! [`crate::stream::replica_stream::EnvironmentLogWriter::log_with_vlsn`]).
//! Both the master ([`ReplicatedEnvironment::become_master`]) and a
//! cascading replica ([`ReplicatedEnvironment::become_replica`] with
//! `cascade_feeding`) register [`PeerFeederService::with_wal_source`], so
//! both serve downstream via `FeederRunner + EnvironmentLogScanner`.
//!
//! [`ReplicatedEnvironment::become_master`]: crate::ReplicatedEnvironment::become_master
//! [`ReplicatedEnvironment::become_replica`]: crate::ReplicatedEnvironment::become_replica
//!
//! ## In-memory queue — non-JE convenience, NOT a production feed
//!
//! [`PeerLogScanner`] is a `LogScanner` backed by an in-memory
//! `VecDeque<(vlsn, entry_type, payload)>`.  It exists ONLY as a fallback
//! for nodes that have no live `EnvironmentImpl` wired (no WAL to scan) —
//! e.g. the `replicate_entry` test convenience.  It is NEVER on a
//! production durability path: every node opened through `with_environment`
//! registers a WAL source and serves from the WAL.  It still feeds through
//! the SAME [`FeederRunner`] loop ([`PeerScannerAdapter`] is just another
//! `LogScanner`), so there is no second feeder mechanism — only a second,
//! non-JE, non-durable *source* for the env-less case.
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
use crate::stream::feeder::{EnvironmentLogScanner, FeederRunner, LogScanner};

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
/// master (or from another peer).  A [`FeederRunner`] driving a
/// [`PeerScannerAdapter`] consumes entries from this queue and streams them
/// to a downstream replica (the in-memory, env-less convenience source).
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
/// `push()` as entries arrive. A [`FeederRunner`] driving a
/// [`PeerScannerAdapter`] holds another clone of the inner scanner to stream
/// the entries to the downstream.
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
/// 3. If the range is available, a [`FeederRunner`] (driving an
///    [`EnvironmentLogScanner`] for the WAL path or a [`PeerScannerAdapter`]
///    for the in-memory path) streams entries until the channel closes.
/// 4. If the range is not available, the server responds with
///    `[NEEDS_RESTORE: u8 = 1]` and closes the connection.
///
/// The `PeerLogScanner` (`source`) is populated by the node's own
/// `ReplicaReceiver` as entries arrive from the master.
pub struct PeerFeederService {
    source: Arc<PeerLogScanner>,
    /// Optional WAL-backed feeder source for chained replication.
    ///
    /// When `Some`, the service serves the requested VLSN range from this
    /// node's OWN WAL via an [`EnvironmentLogScanner`] driven by a
    /// [`FeederRunner`] — the *same* machinery the master uses
    /// ([`crate::stream::feeder`]).  This is JE's cascading-feeder model:
    /// `FeederSource` is "a real Master OR a Replica in a Replica chain that
    /// is replaying log records it received from some other source"
    /// (`FeederSource.java`); `MasterFeederSource` reads the VLSNIndex + log
    /// on whatever node hosts it.
    ///
    /// `None` (default) preserves the in-memory `PeerLogScanner` pull path.
    wal_source: Option<WalFeederSource>,
    /// Count of downstream connections served via the WAL `FeederRunner` +
    /// `EnvironmentLogScanner` path (the JE `Feeder` + `MasterFeederSource`
    /// mechanism).  Lets the owning [`crate::ReplicatedEnvironment`] — and
    /// tests — PROVE that a cascading replica feeds downstream via the same
    /// mechanism the master uses, not the in-memory pull fallback.
    wal_feeds_served: Arc<std::sync::atomic::AtomicU64>,
}

/// WAL-backed feeder source for a chained (replica-to-replica) feed.
///
/// Holds a live [`EnvironmentImpl`] (whose WAL carries VLSN-tagged 22-byte
/// headers written by [`crate::stream::replica_stream::EnvironmentLogWriter`])
/// and the shared [`VlsnIndex`] used to negotiate the available VLSN range.
///
/// Faithful to JE `MasterFeederSource(repImpl, vlsnIndex, startVLSN)` — the
/// feeder source is constructed from the environment + VLSN index regardless
/// of whether the node is master or replica.
#[derive(Clone)]
pub struct WalFeederSource {
    env: Arc<noxu_dbi::EnvironmentImpl>,
    vlsn_index: Arc<crate::vlsn::vlsn_index::VlsnIndex>,
}

impl WalFeederSource {
    /// Create a WAL-backed feeder source.
    pub fn new(
        env: Arc<noxu_dbi::EnvironmentImpl>,
        vlsn_index: Arc<crate::vlsn::vlsn_index::VlsnIndex>,
    ) -> Self {
        Self { env, vlsn_index }
    }
}

impl PeerFeederService {
    /// Create a new service backed by an in-memory `source` (pull path).
    pub fn new(source: Arc<PeerLogScanner>) -> Self {
        Self {
            source,
            wal_source: None,
            wal_feeds_served: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Create a service that ALSO serves the chained-replication WAL feed.
    ///
    /// When a downstream replica connects, the service prefers the WAL
    /// source: it negotiates the VLSN range from the [`VlsnIndex`] and, if
    /// it can serve, streams entries from this node's WAL via an
    /// [`EnvironmentLogScanner`] + [`FeederRunner`] (the same path the
    /// master uses).  If the WAL cannot serve the requested range it falls
    /// back to the in-memory `source`, then to `NEEDS_RESTORE`.
    ///
    /// Used by [`crate::ReplicatedEnvironment::become_replica`] when
    /// `cascade_feeding` is enabled and a live `EnvironmentImpl` is wired.
    pub fn with_wal_source(
        source: Arc<PeerLogScanner>,
        wal_source: WalFeederSource,
    ) -> Self {
        Self {
            source,
            wal_source: Some(wal_source),
            wal_feeds_served: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Like [`Self::with_wal_source`] but shares `wal_feeds_served` with the
    /// owning [`crate::ReplicatedEnvironment`] so it (and tests) can PROVE
    /// the node served a downstream via the JE `Feeder`/`MasterFeederSource`
    /// path (`FeederRunner + EnvironmentLogScanner`).
    pub fn with_wal_source_counted(
        source: Arc<PeerLogScanner>,
        wal_source: WalFeederSource,
        wal_feeds_served: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self { source, wal_source: Some(wal_source), wal_feeds_served }
    }

    /// Number of downstream connections this service has served via the JE
    /// `Feeder`/`MasterFeederSource` path (`FeederRunner +
    /// EnvironmentLogScanner`).  `0` means no cascade/WAL feed has run yet.
    pub fn wal_feeds_served(&self) -> u64 {
        self.wal_feeds_served.load(std::sync::atomic::Ordering::SeqCst)
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

        // 2a. Chained replication (cascade): if a WAL source is wired, prefer
        //     serving the downstream from THIS node's own WAL via the same
        //     EnvironmentLogScanner + FeederRunner the master uses.  Faithful
        //     to JE's cascading-feeder model (see `WalFeederSource`).
        //
        //     The downstream sends start_vlsn=0 to mean "from the beginning";
        //     we serve from the first VLSN our VLSNIndex holds.  We can serve
        //     iff the requested start falls within `[first, last]` (or the
        //     range is non-empty and start_vlsn==0).
        if let Some(wal) = &self.wal_source {
            let range = wal.vlsn_index.get_range();
            let (first, last) = (range.first(), range.last());
            let have_data = last > 0 && first > 0;
            let effective_start =
                if start_vlsn == 0 { first } else { start_vlsn };
            let can_serve = have_data
                && effective_start >= first
                && effective_start <= last;
            if can_serve {
                channel.send(&[PEER_FEEDER_CAN_SERVE])?;
                let channel_arc: Arc<dyn Channel> = Arc::from(channel);
                // EnvironmentLogScanner starts at the log beginning and skips
                // entries with vlsn < effective_start; the FeederRunner then
                // streams VLSN-ordered entries exactly as the master does.
                //
                // JE fidelity: this is JE `Feeder` running `MasterFeederSource`
                // — `Feeder.initMasterFeederSource(startVLSN)` builds
                // `new MasterFeederSource(repImpl, repNode.getVLSNIndex(), …)`
                // and the output loop pulls
                // `feederSource.getWireRecord(feederVLSN, heartbeatMs)`
                // (`Feeder.java:1282`).  Here `FeederRunner` IS that loop and
                // `EnvironmentLogScanner` IS that `MasterFeederSource`
                // (a `FeederReader` over the VLSNIndex+WAL).  A cascading
                // replica reaches this branch with `wal.env` = its OWN env, so
                // it feeds downstream by the IDENTICAL mechanism the master
                // uses — reading its own WAL.
                let mut scanner =
                    match EnvironmentLogScanner::new(&wal.env, None) {
                        Some(s) => s,
                        None => {
                            // WAL scanner unavailable (read-only env / no log):
                            // the CAN_SERVE byte was already sent, so close the
                            // channel; the downstream will retry / fall back.
                            return Err(RepError::NetworkError(
                                "PEER_FEEDER: WAL scanner unavailable".into(),
                            ));
                        }
                    };
                // Record (for the env + tests) that THIS connection was served
                // by the JE `Feeder`/`MasterFeederSource` path — the proof
                // that the cascade uses the same mechanism as the master.
                self.wal_feeds_served
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let runner = FeederRunner::new(channel_arc, effective_start);
                let _ = runner.run(&mut scanner);
                return Ok(());
            }
            // WAL cannot serve the requested range: fall through to the
            // in-memory pull path, then to NEEDS_RESTORE.  A downstream that
            // asks for an evicted/old range that the mid-tier no longer holds
            // must catch up via network restore or fall back to the master.
        }

        // 2. Negotiate: do we hold the requested VLSN range in memory?
        let range = self.source.log_range();
        match negotiate_syncup(range, start_vlsn) {
            SyncupResult::CanServe { start_vlsn: sv } => {
                // Tell the downstream it can proceed.
                channel.send(&[PEER_FEEDER_CAN_SERVE])?;

                // 3. Stream entries in this thread (the dispatcher already
                //    called us from a per-connection thread).
                //
                //    SAME feeder loop as the WAL/cascade path: ONE
                //    `FeederRunner` (JE `Feeder`) driving ONE `LogScanner`
                //    (JE `FeederSource`).  Here the source is the in-memory
                //    `PeerScannerAdapter` (non-JE convenience for env-less
                //    nodes), not the WAL `EnvironmentLogScanner`.  There is no
                //    second feeder mechanism — only a second source.
                let channel_arc: Arc<dyn Channel> = Arc::from(channel);
                let mut source =
                    PeerScannerAdapter::new(Arc::clone(&self.source), sv);
                let runner = FeederRunner::new(channel_arc, sv);
                let _ = runner.run(&mut source);
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

/// Like [`catch_up_from_peer`] but polls `shutdown` so the receive loop can
/// exit promptly when the environment is closing (used by
/// [`crate::ReplicatedEnvironment::become_replica`]'s I/O thread so
/// `close()` can join it even while the upstream feeder stays connected).
pub fn catch_up_from_peer_until(
    peer_addr: std::net::SocketAddr,
    start_vlsn: u64,
    log_writer: &mut dyn crate::stream::replica_stream::LogWriter,
    shutdown: &std::sync::atomic::AtomicBool,
) -> Result<bool> {
    use crate::net::service_dispatcher::connect_to_service;
    use crate::stream::replica_stream::ReplicaReceiver;
    use std::sync::Arc;
    use std::time::Duration;

    let channel = connect_to_service(peer_addr, PEER_FEEDER_SERVICE_NAME)?;
    channel.send(&start_vlsn.to_le_bytes())?;
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

    let channel_arc: Arc<dyn Channel> = Arc::from(channel);
    let receiver = ReplicaReceiver::new(channel_arc);
    receiver.run_until(log_writer, Some(shutdown))?;

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
    // In-memory source streamed via the shared FeederRunner loop
    // -----------------------------------------------------------------------

    #[test]
    fn test_in_memory_source_streams_to_replica_via_feeder_runner() {
        // The in-memory `PeerLogScanner` feeds through the SAME `FeederRunner`
        // loop the WAL path uses — only the `LogScanner` source differs.
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

        let sender_clone = Arc::clone(&sender);
        let run_handle = std::thread::spawn(move || {
            let mut adapter = PeerScannerAdapter::new(Arc::clone(&source), 10);
            let runner = FeederRunner::new(sender, 10);
            let _ = runner.run(&mut adapter);
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
    fn test_feeder_runner_known_replica_vlsn_initial_zero() {
        use crate::net::channel::LocalChannelPair;

        let pair = LocalChannelPair::new();
        let channel: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let runner = FeederRunner::new(channel, 1);
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
