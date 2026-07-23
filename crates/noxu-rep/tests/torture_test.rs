//! Real-network replication chaos / torture test.
//!
//! This test exercises Noxu replication against failure modes that the
//! in-process `chaos_test.rs` cannot reach:
//!
//! - Real TCP **and** QUIC (single-stream and multiplexed) sockets under
//!   `tc netem` kernel-level fault injection (packet loss, latency, reorder,
//!   duplication, corruption)
//! - TCP/QUIC connection teardown from node crash
//! - VLSN state persisted to disk; verified after node restart
//! - 5-10 minute long-running driver that cycles through:
//!   - normal operation
//!   - lossy / high-latency chaos phases
//!   - random node crashes with restart
//!   - minority-partition → majority-partition heal
//!
//! ## Transport selection
//!
//! Set the `TRANSPORT` environment variable before running:
//!
//! ```text
//! TRANSPORT=tcp         # all nodes use TcpChannel (default)
//! TRANSPORT=quic        # all nodes use QuicChannel (single-stream)
//! TRANSPORT=quic_mux    # all nodes use QuicMultiplexedChannel
//!                       #   heartbeat sub-channel → elections
//!                       #   log sub-channel       → VLSN streaming
//! TRANSPORT=mix         # nodes 1+2 use TCP, node 3 uses QUIC
//! ```
//!
//! ## Running
//!
//! ```text
//! # Software fault injection only (no CAP_NET_ADMIN needed):
//! TRANSPORT=tcp cargo test -p noxu-rep --test torture_test -- --ignored --nocapture
//! TRANSPORT=quic --features quic  cargo test ...
//! TRANSPORT=mix  --features quic  cargo test ...
//!
//! # With tc netem (kernel-level faults, requires CAP_NET_ADMIN):
//! sudo TRANSPORT=quic_mux TORTURE_SECS=600 \
//!   cargo test -p noxu-rep --features quic --test torture_test -- --ignored --nocapture
//! ```
//!
//! ## Invariants verified
//!
//! 1. **Safety**: at most one winner per election term.
//! 2. **VLSN monotonicity**: each node's VLSN never decreases.
//! 3. **Durability**: after crash+restart, a node's VLSN >= last persisted.
//! 4. **Liveness**: elections succeed within RETRY_BUDGET attempts.
//! 5. **No panic**: protocol returns `None`/`Err` under all injected faults.

#![allow(clippy::unwrap_used, clippy::expect_used, dead_code, unused_imports)]

use hashbrown::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use noxu_evictor::{EvictionAlgorithm, EvictionPolicy};
use noxu_rep::elections::{run_acceptor, run_election};
use noxu_rep::net::{Channel, TcpChannel, TcpChannelListener};
use noxu_rep::stream::{FeederRunner, LogScanner};
use noxu_rep::{NodeType, RepGroup, RepNode};

/// Maximum time to wait for any transport connect under kernel netem chaos.
/// TCP's OS default (~2 min) and QUIC's internal retry timeout (~30 s) both
/// make election retries unbearably slow under MinorityPartition (80% loss).
/// 2 s is sufficient for loopback.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

// QUIC types — compiled only when the `quic` feature is enabled.
#[cfg(feature = "quic")]
use noxu_rep::net::{
    QuicChannel, QuicChannelListener, QuicMultiplexedChannel,
    QuicMultiplexedChannelListener, ReplicationChannel,
};

// ============================================================================
// Test configuration
// ============================================================================

/// Total test duration. Override with `TORTURE_SECS` env var.
fn torture_duration() -> Duration {
    let secs: u64 = std::env::var("TORTURE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    Duration::from_secs(secs)
}

/// Initial cluster size. Override with `TORTURE_NODES` env var (clamped 3–20).
fn num_nodes() -> usize {
    std::env::var("TORTURE_NODES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(3)
        .clamp(3, 20)
}

/// Payload bytes per log entry.  Override with `TORTURE_PAYLOAD_BYTES` (default 256).
/// With 7 nodes, 200 VLSNs/round, 256 B/entry and ~10 rounds/sec:
///   7-1=6 streams × 200 × 256 B ≈ 307 KB/round × 36 000 rounds ≈ 10.8 GB network I/O.
fn entry_payload_bytes() -> usize {
    std::env::var("TORTURE_PAYLOAD_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256)
}

const VLSNS_PER_ROUND: u64 = 200;
const RETRY_BUDGET: u32 = 30;

// ============================================================================
// Transport selection
// ============================================================================

/// Which network transport to use for a given node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportKind {
    Tcp,
    /// Single-stream QUIC (`QuicChannel`).  Requires `--features quic`.
    Quic,
    /// Multiplexed QUIC: heartbeat stream for elections, log stream for VLSN.
    /// Requires `--features quic`.
    QuicMux,
}

impl TransportKind {
    fn name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Quic => "quic",
            Self::QuicMux => "quic_mux",
        }
    }
}

/// Read `TRANSPORT` env var and return per-node transport assignments.
fn node_transports() -> Vec<TransportKind> {
    let raw = std::env::var("TRANSPORT").unwrap_or_else(|_| "tcp".to_string());
    let n = num_nodes();
    let kind = match raw.to_lowercase().as_str() {
        "quic" => TransportKind::Quic,
        "quic_mux" => TransportKind::QuicMux,
        "mix" => {
            // TCP for all but the last node, which uses QUIC.
            let mut v = vec![TransportKind::Tcp; n];
            if n > 0 {
                v[n - 1] = TransportKind::Quic;
            }
            return v;
        }
        _ => TransportKind::Tcp,
    };
    vec![kind; n]
}

// ============================================================================
// Transport-agnostic listener
// ============================================================================

/// A bound listener that accepts one incoming channel connection.
///
/// For QuicMux a single `accept()` call opens the full mux channel; the
/// caller chooses whether it wants the heartbeat or log sub-channel.
enum AnyListener {
    Tcp(TcpChannelListener),
    #[cfg(feature = "quic")]
    Quic(QuicChannelListener),
    #[cfg(feature = "quic")]
    QuicMux(QuicMultiplexedChannelListener),
}

impl AnyListener {
    fn bind(kind: TransportKind, addr: SocketAddr) -> Option<Self> {
        match kind {
            TransportKind::Tcp => {
                TcpChannelListener::bind(addr).ok().map(Self::Tcp)
            }
            #[cfg(feature = "quic")]
            TransportKind::Quic => {
                QuicChannelListener::bind(addr).ok().map(Self::Quic)
            }
            #[cfg(feature = "quic")]
            TransportKind::QuicMux => {
                QuicMultiplexedChannelListener::bind(addr)
                    .ok()
                    .map(Self::QuicMux)
            }
            #[allow(unreachable_patterns)]
            _ => {
                // quic feature disabled — fall back to TCP
                TcpChannelListener::bind(addr).ok().map(Self::Tcp)
            }
        }
    }

    fn local_addr(&self) -> SocketAddr {
        match self {
            Self::Tcp(l) => l.local_addr().unwrap(),
            #[cfg(feature = "quic")]
            Self::Quic(l) => l.local_addr().unwrap(),
            #[cfg(feature = "quic")]
            Self::QuicMux(l) => l.local_addr().unwrap(),
        }
    }

    /// Accept and return a channel for **election** (Paxos) messages.
    ///
    /// For QuicMux this is the heartbeat sub-channel.
    fn accept_election(self) -> Option<Box<dyn Channel>> {
        match self {
            Self::Tcp(l) => {
                l.accept().ok().map(|c| Box::new(c) as Box<dyn Channel>)
            }
            #[cfg(feature = "quic")]
            Self::Quic(l) => {
                l.accept().ok().map(|c| Box::new(c) as Box<dyn Channel>)
            }
            #[cfg(feature = "quic")]
            Self::QuicMux(l) => l.accept().ok().map(|mux| {
                Box::new(MuxElectionChannel(Arc::new(mux))) as Box<dyn Channel>
            }),
        }
    }

    /// Accept and return a channel for **VLSN log streaming**.
    ///
    /// For QuicMux this is the log sub-channel (independent of elections).
    fn accept_log(self) -> Option<Box<dyn Channel>> {
        match self {
            Self::Tcp(l) => {
                l.accept().ok().map(|c| Box::new(c) as Box<dyn Channel>)
            }
            #[cfg(feature = "quic")]
            Self::Quic(l) => {
                l.accept().ok().map(|c| Box::new(c) as Box<dyn Channel>)
            }
            #[cfg(feature = "quic")]
            Self::QuicMux(l) => l.accept().ok().map(|mux| {
                Box::new(MuxLogChannel(Arc::new(mux))) as Box<dyn Channel>
            }),
        }
    }
}

// ============================================================================
// Transport-agnostic connect helpers
// ============================================================================

/// Open an **election** channel to `addr` using the given transport.
fn connect_election(
    kind: TransportKind,
    addr: SocketAddr,
) -> Option<Arc<dyn Channel>> {
    match kind {
        TransportKind::Tcp => {
            // Use a short timeout so election retries stay fast under kernel
            // netem packet loss (OS default TCP timeout is ~2 min).
            match std::net::TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
                Ok(s) => Some(Arc::new(TcpChannel::new(s)) as Arc<dyn Channel>),
                Err(e) => {
                    eprintln!("[torture] TCP connect to {addr} failed: {e}");
                    None
                }
            }
        }
        #[cfg(feature = "quic")]
        TransportKind::Quic => {
            quic_connect_timeout(addr, CONNECT_TIMEOUT, |a| {
                QuicChannel::connect(a, "localhost")
                    .ok()
                    .map(|c| Arc::new(c) as Arc<dyn Channel>)
            })
        }
        #[cfg(feature = "quic")]
        TransportKind::QuicMux => {
            quic_connect_timeout(addr, CONNECT_TIMEOUT, |a| {
                QuicMultiplexedChannel::connect(a, "localhost").ok().map(
                    |mux| {
                        Arc::new(MuxElectionChannel(Arc::new(mux)))
                            as Arc<dyn Channel>
                    },
                )
            })
        }
        #[allow(unreachable_patterns)]
        _ => TcpChannel::connect(addr)
            .ok()
            .map(|c| Arc::new(c) as Arc<dyn Channel>),
    }
}

/// Open a **VLSN streaming** channel to `addr` using the given transport.
fn connect_log(
    kind: TransportKind,
    addr: SocketAddr,
) -> Option<Arc<dyn Channel>> {
    match kind {
        TransportKind::Tcp => {
            std::net::TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)
                .ok()
                .map(|s| Arc::new(TcpChannel::new(s)) as Arc<dyn Channel>)
        }
        #[cfg(feature = "quic")]
        TransportKind::Quic => {
            quic_connect_timeout(addr, CONNECT_TIMEOUT, |a| {
                QuicChannel::connect(a, "localhost")
                    .ok()
                    .map(|c| Arc::new(c) as Arc<dyn Channel>)
            })
        }
        #[cfg(feature = "quic")]
        TransportKind::QuicMux => {
            quic_connect_timeout(addr, CONNECT_TIMEOUT, |a| {
                QuicMultiplexedChannel::connect(a, "localhost").ok().map(
                    |mux| {
                        Arc::new(MuxLogChannel(Arc::new(mux)))
                            as Arc<dyn Channel>
                    },
                )
            })
        }
        #[allow(unreachable_patterns)]
        _ => TcpChannel::connect(addr)
            .ok()
            .map(|c| Arc::new(c) as Arc<dyn Channel>),
    }
}

/// Run a QUIC connect on a background thread and return the result within
/// `timeout`. QUIC's internal retry can take ~30 s under high packet loss;
/// this wrapper caps it at `CONNECT_TIMEOUT` (2 s) for loopback testing.
#[cfg(feature = "quic")]
fn quic_connect_timeout<F>(
    addr: SocketAddr,
    timeout: Duration,
    f: F,
) -> Option<Arc<dyn Channel>>
where
    F: FnOnce(SocketAddr) -> Option<Arc<dyn Channel>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f(addr));
    });
    rx.recv_timeout(timeout).ok().flatten()
}

// ============================================================================
// QuicMux sub-channel wrappers
//
// `QuicMultiplexedChannel` exposes sub-channels as `&dyn Channel` borrows.
// These wrappers hold `Arc<QuicMultiplexedChannel>` and forward to the
// appropriate sub-channel on each call, avoiding lifetime issues.
// ============================================================================

#[cfg(feature = "quic")]
struct MuxElectionChannel(Arc<QuicMultiplexedChannel>);

#[cfg(feature = "quic")]
impl Channel for MuxElectionChannel {
    fn send(&self, data: &[u8]) -> noxu_rep::error::Result<()> {
        self.0.heartbeat_channel().send(data)
    }
    fn receive(
        &self,
        timeout: Duration,
    ) -> noxu_rep::error::Result<Option<Vec<u8>>> {
        self.0.heartbeat_channel().receive(timeout)
    }
    fn close(&self) -> noxu_rep::error::Result<()> {
        self.0.heartbeat_channel().close()
    }
    fn is_open(&self) -> bool {
        self.0.heartbeat_channel().is_open()
    }
}

#[cfg(feature = "quic")]
struct MuxLogChannel(Arc<QuicMultiplexedChannel>);

#[cfg(feature = "quic")]
impl Channel for MuxLogChannel {
    fn send(&self, data: &[u8]) -> noxu_rep::error::Result<()> {
        self.0.log_channel().send(data)
    }
    fn receive(
        &self,
        timeout: Duration,
    ) -> noxu_rep::error::Result<Option<Vec<u8>>> {
        self.0.log_channel().receive(timeout)
    }
    fn close(&self) -> noxu_rep::error::Result<()> {
        self.0.log_channel().close()
    }
    fn is_open(&self) -> bool {
        self.0.log_channel().is_open()
    }
}

// ============================================================================
// tc netem RAII guard
// ============================================================================

/// Always-on baseline network variability applied at setup and restored after
/// each chaos spike.  Unlike the old zero-calm design, the network never
/// goes perfectly clean — there is always background noise.
struct TcNetemBaseline {
    loss: f32, // percent
    delay_ms: u64,
    jitter_ms: u64,
    reorder: f32, // percent
}

impl Default for TcNetemBaseline {
    fn default() -> Self {
        // Light permanent noise: 1% loss, 3 ± 2 ms jitter, 2% reorder.
        Self { loss: 1.0, delay_ms: 3, jitter_ms: 2, reorder: 2.0 }
    }
}

struct TcNetemGuard {
    pub active: bool,
    baseline: TcNetemBaseline,
}

impl TcNetemGuard {
    fn setup() -> Self {
        let baseline = TcNetemBaseline::default();
        // Clean up any leftover qdisc from a previous killed run.
        Self::run_tc(&["qdisc", "del", "dev", "lo", "root"]);
        let ok = Self::run_tc(&[
            "qdisc",
            "add",
            "dev",
            "lo",
            "root",
            "netem",
            "loss",
            &format!("{}%", baseline.loss),
            "delay",
            &format!("{}ms", baseline.delay_ms),
            &format!("{}ms", baseline.jitter_ms),
            "reorder",
            &format!("{}%", baseline.reorder),
            "50%",
        ]);
        if ok {
            eprintln!(
                "[torture] tc netem active on lo (baseline: \
                       loss={}% delay={}ms±{}ms reorder={}%)",
                baseline.loss,
                baseline.delay_ms,
                baseline.jitter_ms,
                baseline.reorder
            );
            Self { active: true, baseline }
        } else {
            eprintln!(
                "[torture] WARNING: tc netem not available \
                       (CAP_NET_ADMIN?); software-only fault injection"
            );
            Self { active: false, baseline }
        }
    }

    /// Apply a chaos spike.  The effective parameters are the maximum of the
    /// spike and the baseline on each axis so the baseline is never undercut.
    fn overlay(
        &self,
        loss_pct: f32,
        delay_ms: u64,
        jitter_ms: u64,
        reorder_pct: f32,
        dup_pct: f32,
        corrupt_pct: f32,
    ) {
        if !self.active {
            return;
        }
        let l = loss_pct.max(self.baseline.loss);
        let d = delay_ms.max(self.baseline.delay_ms);
        let j = jitter_ms.max(self.baseline.jitter_ms);
        let ro = reorder_pct.max(self.baseline.reorder);
        Self::run_tc(&[
            "qdisc",
            "change",
            "dev",
            "lo",
            "root",
            "netem",
            "loss",
            &format!("{l}%"),
            "delay",
            &format!("{d}ms"),
            &format!("{j}ms"),
            "reorder",
            &format!("{ro}%"),
            "50%",
            "duplicate",
            &format!("{dup_pct}%"),
            "corrupt",
            &format!("{corrupt_pct}%"),
        ]);
    }

    /// Gilbert-Elliott correlated burst loss: p13 = prob entering bad state,
    /// p31 = prob leaving bad state per packet.  With p13=5% p31=90% the
    /// average burst length is ~11 packets — worst case for TCP.
    fn overlay_burst_loss(&self, p13: f32, p31: f32) {
        if !self.active {
            return;
        }
        Self::run_tc(&[
            "qdisc",
            "change",
            "dev",
            "lo",
            "root",
            "netem",
            "loss",
            "state",
            &format!("{p13}%"),
            &format!("{p31}%"),
            "delay",
            &format!("{}ms", self.baseline.delay_ms),
            &format!("{}ms", self.baseline.jitter_ms),
            "reorder",
            &format!("{}%", self.baseline.reorder),
            "50%",
        ]);
    }

    /// Bandwidth cap via tc rate — stresses QUIC flow-control and TCP
    /// send-buffer backpressure paths.
    fn overlay_bandwidth_cap(&self, rate_mbit: u32) {
        if !self.active {
            return;
        }
        Self::run_tc(&[
            "qdisc",
            "change",
            "dev",
            "lo",
            "root",
            "netem",
            "rate",
            &format!("{rate_mbit}mbit"),
            "delay",
            &format!("{}ms", self.baseline.delay_ms),
            &format!("{}ms", self.baseline.jitter_ms),
            "loss",
            &format!("{}%", self.baseline.loss),
        ]);
    }

    /// Slotted delivery — packets are held and released in time slots,
    /// emulating cellular or satellite batched delivery.
    fn overlay_slot(&self, slot_min_ms: u64, slot_max_ms: u64) {
        if !self.active {
            return;
        }
        Self::run_tc(&[
            "qdisc",
            "change",
            "dev",
            "lo",
            "root",
            "netem",
            "slot",
            &format!("{slot_min_ms}ms"),
            &format!("{slot_max_ms}ms"),
            "delay",
            &format!("{}ms", self.baseline.delay_ms),
            &format!("{}ms", self.baseline.jitter_ms),
            "loss",
            &format!("{}%", self.baseline.loss),
        ]);
    }

    /// Queue bloat: deterministic delay + shallow queue cap.  Stresses
    /// election-timeout math when the queue fills and drops begin.
    fn overlay_queue_bloat(&self, delay_ms: u64, queue_limit: u32) {
        if !self.active {
            return;
        }
        let d = delay_ms.max(self.baseline.delay_ms);
        Self::run_tc(&[
            "qdisc",
            "change",
            "dev",
            "lo",
            "root",
            "netem",
            "delay",
            &format!("{d}ms"),
            "limit",
            &queue_limit.to_string(),
            "loss",
            &format!("{}%", self.baseline.loss),
        ]);
    }

    /// Revert to the always-on baseline (NOT to zero).  The cluster is never
    /// in a perfectly clean network — baseline noise always applies.
    fn overlay_calm(&self) {
        if !self.active {
            return;
        }
        Self::run_tc(&[
            "qdisc",
            "change",
            "dev",
            "lo",
            "root",
            "netem",
            "loss",
            &format!("{}%", self.baseline.loss),
            "delay",
            &format!("{}ms", self.baseline.delay_ms),
            &format!("{}ms", self.baseline.jitter_ms),
            "reorder",
            &format!("{}%", self.baseline.reorder),
            "50%",
            "duplicate",
            "0%",
            "corrupt",
            "0%",
        ]);
    }

    fn run_tc(args: &[&str]) -> bool {
        // tc netem is Linux-only.  On other platforms chaos falls back to
        // software-only fault injection (TcNetemGuard::active == false).
        #[cfg(not(target_os = "linux"))]
        {
            let _ = args;
            false
        }

        #[cfg(target_os = "linux")]
        {
            // 1. Try setuid helper (user installs via `make tc-helper`).
            let helper = std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|d| d.to_path_buf()))
                .map(|d| d.join("../../../scripts/tc_netem_helper"))
                .unwrap_or_else(|| PathBuf::from("scripts/tc_netem_helper"));
            if helper.exists()
                && Command::new(&helper)
                    .args(args)
                    .status()
                    .map(|s| s.success())
                    .unwrap_or(false)
            {
                return true;
            }
            // 2. Try direct tc (works if process already has CAP_NET_ADMIN).
            if Command::new("tc")
                .args(args)
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
            {
                return true;
            }
            // 3. Try passwordless sudo tc.
            if Command::new("sudo")
                .arg("-n")
                .arg("tc")
                .args(args)
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
            {
                return true;
            }
            false
        }
    }
}

impl Drop for TcNetemGuard {
    fn drop(&mut self) {
        if self.active {
            self.overlay_calm();
            Self::run_tc(&["qdisc", "del", "dev", "lo", "root"]);
        }
    }
}

// ============================================================================
// Per-node disk persistence (crash recovery)
// ============================================================================

struct NodeDisk {
    path: PathBuf,
}

impl NodeDisk {
    fn new(dir: &Path, id: u32) -> Self {
        Self { path: dir.join(format!("node{id}.vlsn")) }
    }

    fn save(&self, vlsn: u64) {
        let _ = fs::write(&self.path, vlsn.to_le_bytes());
    }

    fn load(&self) -> u64 {
        fs::read(&self.path)
            .ok()
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0)
    }
}

// ============================================================================
// In-memory log scanner
// ============================================================================

struct MemLogScanner {
    next: u64,
    max: u64,
}

impl MemLogScanner {
    fn new(start: u64, count: u64) -> Self {
        Self { next: start, max: start + count - 1 }
    }
}

impl LogScanner for MemLogScanner {
    fn next_entry(&mut self, from: u64) -> Option<(u64, u8, Vec<u8>)> {
        let v = self.next.max(from);
        if v > self.max {
            return None;
        }
        self.next = v + 1;
        let prefix = format!("e{v}");
        let target = entry_payload_bytes();
        let mut payload = prefix.into_bytes();
        if payload.len() < target {
            payload.resize(target, 0u8);
        }
        Some((v, 1u8, payload))
    }
}

// ============================================================================
// Cluster node
// ============================================================================

struct ClusterNode {
    id: u32,
    name: String,
    transport: TransportKind,
    vlsn: Arc<AtomicU64>,
    disk: NodeDisk,
    alive: Arc<AtomicBool>,
    last_election_port: Arc<Mutex<u16>>,
    last_log_port: Arc<Mutex<u16>>,
}

impl ClusterNode {
    fn new(id: u32, transport: TransportKind, dir: &Path) -> Self {
        let disk = NodeDisk::new(dir, id);
        let saved = disk.load();
        Self {
            id,
            name: format!("tn{id}"),
            transport,
            vlsn: Arc::new(AtomicU64::new(saved)),
            disk,
            alive: Arc::new(AtomicBool::new(true)),
            last_election_port: Arc::new(Mutex::new(0)),
            last_log_port: Arc::new(Mutex::new(0)),
        }
    }

    fn vlsn(&self) -> u64 {
        self.vlsn.load(Ordering::SeqCst)
    }

    fn advance_vlsn(&self, to: u64) {
        let old = self.vlsn.fetch_max(to, Ordering::SeqCst);
        if to > old {
            self.disk.save(to);
        }
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }
}

// ============================================================================
// Invariant log
// ============================================================================

struct InvariantLog {
    term_winners: Mutex<HashMap<u64, u32>>,
    vlsn_history: Mutex<HashMap<u32, u64>>, // last seen per node
    violations: AtomicU64,
}

impl InvariantLog {
    fn new() -> Self {
        Self {
            term_winners: Mutex::new(HashMap::new()),
            vlsn_history: Mutex::new(HashMap::new()),
            violations: AtomicU64::new(0),
        }
    }

    fn record_winner(&self, term: u64, winner: u32) {
        let mut m = self.term_winners.lock().unwrap();
        if let Some(&prev) = m.get(&term) {
            if prev != winner {
                eprintln!(
                    "[VIOLATION] split-brain: term={term} prev={prev} new={winner}"
                );
                self.violations.fetch_add(1, Ordering::SeqCst);
            }
        } else {
            m.insert(term, winner);
        }
    }

    fn record_vlsn(&self, node_id: u32, vlsn: u64) {
        let mut h = self.vlsn_history.lock().unwrap();
        let prev = *h.get(&node_id).unwrap_or(&0);
        if vlsn < prev {
            eprintln!(
                "[VIOLATION] vlsn regression: node={node_id} {prev}→{vlsn}"
            );
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
        h.insert(node_id, vlsn.max(prev));
    }

    fn check_durability(&self, id: u32, disk: u64, restart: u64) {
        if restart < disk {
            eprintln!(
                "[VIOLATION] durability: node={id} disk={disk} restart={restart}"
            );
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn violations(&self) -> u64 {
        self.violations.load(Ordering::SeqCst)
    }
}

// ============================================================================
// Election helper — transport-agnostic Paxos over real sockets
// ============================================================================

fn run_election_round(
    proposer_idx: usize,
    nodes: &[ClusterNode],
    group: &RepGroup,
    term: u64,
    inv: &InvariantLog,
) -> Option<u32> {
    let alive: Vec<usize> =
        (0..nodes.len()).filter(|&i| nodes[i].is_alive()).collect();
    if alive.len() < 2 || !nodes[proposer_idx].is_alive() {
        return None;
    }

    // Bind a fresh ephemeral election listener on each alive non-proposer.
    let mut listeners: Vec<(usize, AnyListener)> = Vec::new();
    for &i in &alive {
        if i == proposer_idx {
            continue;
        }
        let kind = nodes[i].transport;
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        match AnyListener::bind(kind, addr) {
            Some(l) => {
                // Set SO_RCVTIMEO so the acceptor thread doesn't block forever
                // when the proposer fails to connect under heavy netem chaos.
                #[allow(irrefutable_let_patterns)]
                if let AnyListener::Tcp(ref tl) = l {
                    let _ = tl.set_accept_timeout(Some(Duration::from_secs(6)));
                }
                *nodes[i].last_election_port.lock().unwrap() =
                    l.local_addr().port();
                listeners.push((i, l));
            }
            None => {
                eprintln!(
                    "[torture] election bind failed for node {}",
                    nodes[i].name
                );
                return None;
            }
        }
    }

    // Spawn one acceptor thread per listener.
    // Use a channel so we can wait with a timeout — QUIC accept() blocks
    // indefinitely and h.join() would hang forever if the proposer never
    // connects (e.g. under 80% packet loss in MinorityPartition).
    let mut acceptor_rxs: Vec<(
        usize,
        mpsc::Receiver<noxu_rep::error::Result<Option<String>>>,
    )> = Vec::new();
    for (i, listener) in listeners {
        let name = nodes[i].name.clone();
        let vlsn = nodes[i].vlsn();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let r = match listener.accept_election() {
                Some(ch) => run_acceptor(ch.as_ref(), &name, vlsn, 1, term),
                None => Ok(None),
            };
            let _ = tx.send(r);
        });
        acceptor_rxs.push((i, rx));
    }

    // Proposer connects to each acceptor's election port.
    let proposer = &nodes[proposer_idx];
    let mut peer_channels: Vec<Arc<dyn Channel>> = Vec::new();
    let peer_node_ids: Vec<usize> =
        acceptor_rxs.iter().map(|(i, _)| *i).collect();

    for &ni in &peer_node_ids {
        let port = *nodes[ni].last_election_port.lock().unwrap();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        // Proposer uses the *acceptor's* transport kind when connecting.
        match connect_election(nodes[ni].transport, addr) {
            Some(ch) => peer_channels.push(ch),
            None => {
                eprintln!(
                    "[torture] election connect failed to node {}",
                    nodes[ni].name
                );
                // Do NOT join — acceptor threads may be blocked in QUIC accept().
                // They'll self-terminate when the listener drops or process exits.
                return None;
            }
        }
    }

    let result = run_election(
        proposer.id,
        &proposer.name,
        group,
        &peer_channels,
        proposer.vlsn(),
        1,
        term,
    );

    // Drain acceptor results with a per-thread timeout to avoid blocking forever.
    for (_, rx) in &acceptor_rxs {
        let _ = rx.recv_timeout(Duration::from_secs(5));
    }

    if let Some(winner_id) = result {
        inv.record_winner(term, winner_id);
    }
    result
}

// ============================================================================
// VLSN streaming helper — transport-agnostic FeederRunner
// ============================================================================

fn stream_vlsns(
    master: &ClusterNode,
    replica: &ClusterNode,
    start_vlsn: u64,
    count: u64,
    inv: &InvariantLog,
) -> u64 {
    // Replica binds a listener using its own transport kind.
    let listener = match AnyListener::bind(
        replica.transport,
        "127.0.0.1:0".parse().unwrap(),
    ) {
        Some(l) => l,
        None => return 0,
    };
    let addr = listener.local_addr();

    let replica_id = replica.id;
    let replica_vlsn = Arc::clone(&replica.vlsn);
    let disk_path = replica.disk.path.clone();

    // Connect the master first.  If the connect fails (e.g. under heavy
    // netem chaos) we return immediately without spawning any receive thread.
    // The kernel buffers the connection in the TCP backlog, so the receive
    // thread's accept() will succeed immediately when it runs.
    let master_ch: Arc<dyn Channel> = match connect_log(replica.transport, addr)
    {
        Some(c) => c,
        None => return 0,
    };

    // Replica receive thread — accept() will return immediately from the
    // backlog since the master already connected above.
    let recv_h = std::thread::spawn(move || {
        let ch = match listener.accept_log() {
            Some(c) => c,
            None => return 0u64,
        };
        let mut received = 0u64;
        let mut last_vlsn = 0u64;
        loop {
            match ch.receive(Duration::from_millis(500)) {
                Ok(Some(frame)) if frame.len() >= 8 => {
                    let v = u64::from_le_bytes(frame[0..8].try_into().unwrap());
                    if v >= last_vlsn {
                        last_vlsn = v;
                        received += 1;
                        let old = replica_vlsn.fetch_max(v, Ordering::SeqCst);
                        if v > old {
                            let _ = fs::write(&disk_path, v.to_le_bytes());
                        }
                    }
                    let _ = ch.send(&v.to_le_bytes()); // ack
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        received
    });

    // Run FeederRunner in a background thread with a hard timeout.
    // runner.run() only terminates on ChannelClosed.  Under heavy netem chaos
    // the replica's TCP FIN can be delayed by OS retransmit backoff (up to
    // minutes at 80 % loss), causing the main torture loop to hang forever.
    // If the feeder has not finished within STREAM_TIMEOUT we force-close the
    // channel, which immediately delivers ChannelClosed to runner.run().
    const STREAM_TIMEOUT: Duration = Duration::from_secs(10);
    let master_ch_feeder = Arc::clone(&master_ch);
    let (feeder_done_tx, feeder_done_rx) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        let runner = FeederRunner::new(master_ch_feeder, start_vlsn);
        let mut scanner = MemLogScanner::new(start_vlsn, count);
        let _ = runner.run(&mut scanner);
        let _ = feeder_done_tx.send(());
    });
    if feeder_done_rx.recv_timeout(STREAM_TIMEOUT).is_err() {
        eprintln!(
            "[torture] stream_vlsns: feeder stuck >{STREAM_TIMEOUT:?}, force-closing channel"
        );
        let _ = master_ch.close();
        // Give the feeder up to 1 s to detect the close and exit.
        let _ = feeder_done_rx.recv_timeout(Duration::from_secs(1));
    }
    let _ = master_ch.close();

    let received = recv_h.join().unwrap_or(0);

    master.advance_vlsn(start_vlsn + count - 1);
    inv.record_vlsn(master.id, master.vlsn());
    inv.record_vlsn(replica_id, replica.vlsn());

    received
}

// ============================================================================
// Chaos phases
// ============================================================================

#[derive(Debug, Clone)]
enum ChaosPhase {
    Calm,
    ModeratePacketLoss,
    HighLatency,
    ReorderHeavy,
    DuplicateAndCorrupt,
    NodeCrash(usize),
    MinorityPartition,
    /// Add a new electable node to the replication group mid-run.
    PeerJoin,
    /// Remove an existing non-master node from the group mid-run.
    PeerLeave,
    /// Update the read/write capacity hint for a random group member.
    CapacityChange,
    /// Grow the cluster by two sequential PeerJoins.
    ClusterGrow,
    /// Shrink the cluster by two sequential PeerLeaves.
    ClusterShrink,
    /// Gilbert-Elliott correlated burst loss (bad-state Markov chain).
    BurstLoss,
    /// Hard bandwidth cap — stresses QUIC flow control and TCP send buffers.
    BandwidthCap,
    /// Slotted packet delivery — emulates cellular/satellite batching.
    SlottedDelivery,
    /// Queue-bloat: high deterministic delay + shallow queue limit.
    QueueBloat,
    /// Swap the primary and/or scan eviction policy on a node mid-run.
    EvictionPolicyChange {
        node_idx: usize,
        primary: EvictionAlgorithm,
        scan: EvictionAlgorithm,
    },
    /// Force the current master to step down and elect a different node.
    /// Verifies: no VLSN regression, no split-brain, new master elected.
    MasterHandover,
}

/// All eviction algorithms available for random selection.  Only LRU is
/// available by default; the scan-resistant policies are added under the
/// `experimental-eviction-policies` feature.
const ALL_ALGOS: &[EvictionAlgorithm] = &[
    EvictionAlgorithm::Lru,
    #[cfg(feature = "experimental-eviction-policies")]
    EvictionAlgorithm::Clock,
    #[cfg(feature = "experimental-eviction-policies")]
    EvictionAlgorithm::Arc,
    #[cfg(feature = "experimental-eviction-policies")]
    EvictionAlgorithm::Car,
    #[cfg(feature = "experimental-eviction-policies")]
    EvictionAlgorithm::Lirs,
];

// ============================================================================
// Main torture loop
// ============================================================================

#[test]
#[ignore = "torture: multi-node replication election/failover loop, duration varies (60–600 s); run with --ignored"]
fn torture_replication() {
    let duration = torture_duration();
    let transports = node_transports();

    eprintln!(
        "[torture] starting duration={duration:?} transport=[{}]",
        transports.iter().map(|t| t.name()).collect::<Vec<_>>().join(",")
    );

    let _state_tmp: Option<TempDir>;
    let state_dir_path: PathBuf;
    if let Ok(dir) = std::env::var("TORTURE_DIR") {
        std::fs::create_dir_all(&dir).expect("create TORTURE_DIR");
        state_dir_path = PathBuf::from(dir);
        _state_tmp = None;
    } else {
        let td = TempDir::new().expect("TempDir");
        state_dir_path = td.path().to_path_buf();
        _state_tmp = Some(td);
    }
    let n = num_nodes();

    let mut nodes: Vec<ClusterNode> = (1..=n as u32)
        .map(|id| {
            ClusterNode::new(id, transports[(id - 1) as usize], &state_dir_path)
        })
        .collect();

    // `members` tracks which indices into `nodes` are currently in the group.
    // It starts as [0..n] and grows/shrinks with PeerJoin/PeerLeave chaos.
    let mut members: Vec<usize> = (0..n).collect();
    // Next node ID for dynamically added peers.
    let mut next_node_id: u32 = n as u32 + 1;

    let mut group = RepGroup::new("torture".to_string(), 99);
    for n in &nodes {
        group.add_node(RepNode::new(
            n.name.clone(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            6900 + n.id as u16,
            n.id,
        ));
    }

    let inv = Arc::new(InvariantLog::new());
    let netem = Arc::new(TcNetemGuard::setup());
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_CAFE);
    let mut term = 1u64;
    let mut vlsn_counter = 1u64;
    let mut round = 0u64;
    let mut current_master: Option<usize> = None;

    // Per-node eviction policy pairs (primary, scan) — exercised each round.
    // Initialised to LRU; swapped chaotically by EvictionPolicyChange phases.
    let mut node_policies: Vec<(
        Box<dyn EvictionPolicy>,
        Box<dyn EvictionPolicy>,
    )> = (0..nodes.len())
        .map(|_| {
            (
                EvictionAlgorithm::Lru.new_policy(),
                EvictionAlgorithm::Lru.new_policy(),
            )
        })
        .collect();
    let mut node_algos: Vec<(EvictionAlgorithm, EvictionAlgorithm)> =
        vec![(EvictionAlgorithm::Lru, EvictionAlgorithm::Lru); nodes.len()];

    // Background network chaos thread — runs independently of the main loop
    // so the network is never perfectly clean.  The thread continuously cycles
    // between light noise and hard chaos spikes, updating tc netem every 50-250 ms.
    let bg_netem = Arc::clone(&netem);
    let bg_done = Arc::new(AtomicBool::new(false));
    let bg_done2 = Arc::clone(&bg_done);
    let bg_seed = rng.gen_range(0u64..u64::MAX);
    let bg_handle = std::thread::spawn(move || {
        let mut bg_rng = StdRng::seed_from_u64(bg_seed);
        while !bg_done2.load(Ordering::Relaxed) {
            if bg_rng.gen_bool(0.15) {
                // Hard spike: severe faults for 100-400 ms.
                let loss = bg_rng.gen_range(35.0f32..80.0);
                let delay = bg_rng.gen_range(40u64..200);
                bg_netem.overlay(loss, delay, delay / 4 + 1, 5.0, 2.0, 0.5);
                let spike_ms = bg_rng.gen_range(100u64..400);
                let mut slept = 0u64;
                while slept < spike_ms && !bg_done2.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(50));
                    slept += 50;
                }
            } else {
                // Background noise: light but non-zero faults.
                let loss = bg_rng.gen_range(0.5f32..12.0);
                let delay = bg_rng.gen_range(2u64..25);
                let reorder = bg_rng.gen_range(0.0f32..6.0);
                bg_netem.overlay(
                    loss,
                    delay,
                    delay / 3 + 1,
                    reorder,
                    0.3,
                    0.05,
                );
            }
            // Sleep in small increments so we can notice `bg_done` quickly.
            let sleep_ms = bg_rng.gen_range(50u64..250);
            let mut slept = 0u64;
            while slept < sleep_ms && !bg_done2.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(20));
                slept += 20;
            }
        }
    });

    // Stats
    let (mut n_elections, mut n_won, mut n_streams, mut n_crashes, mut n_chaos) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let (
        mut n_joins,
        mut n_leaves,
        mut n_cap_changes,
        mut n_evict_changes,
        mut n_handovers,
    ) = (0u64, 0u64, 0u64, 0u64, 0u64);

    let start = Instant::now();
    let mut next_report = start + Duration::from_secs(30);

    while start.elapsed() < duration {
        round += 1;

        // ── Multi-dimensional chaos injection ─────────────────────────────
        // Network chaos is handled by the background thread (constant, variable
        // intensity).  Here we inject independent structural events per round.

        // (a) Node crash — 8% per round.
        if rng.gen_bool(0.08) {
            let alive_m: Vec<_> = members
                .iter()
                .copied()
                .filter(|&i| nodes[i].is_alive())
                .collect();
            if alive_m.len() > 1 {
                let idx = alive_m[rng.gen_range(0..alive_m.len())];
                let n = &nodes[idx];
                let disk_vlsn = n.vlsn();
                eprintln!(
                    "[torture] r={round} crash node {} ({}), vlsn={disk_vlsn}",
                    n.name,
                    n.transport.name()
                );
                n.alive.store(false, Ordering::SeqCst);
                n_crashes += 1;
                if current_master == Some(idx) {
                    current_master = None;
                }

                let alive_flag = Arc::clone(&n.alive);
                let vlsn_atom = Arc::clone(&n.vlsn);
                let disk_path = n.disk.path.clone();
                let node_id = n.id;
                let inv2 = Arc::clone(&inv);
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(
                        100 + (node_id as u64) * 50,
                    ));
                    let restart_vlsn = fs::read(&disk_path)
                        .ok()
                        .and_then(|b| b.try_into().ok())
                        .map(u64::from_le_bytes)
                        .unwrap_or(0);
                    inv2.check_durability(node_id, disk_vlsn, restart_vlsn);
                    vlsn_atom.fetch_max(restart_vlsn, Ordering::SeqCst);
                    alive_flag.store(true, Ordering::SeqCst);
                    eprintln!(
                        "[torture] restart node{node_id} vlsn={restart_vlsn}"
                    );
                });
                n_chaos += 1;
            }
        }

        // (b) Membership change — 5% per round.
        if rng.gen_bool(0.05) {
            let membership_phase: Option<ChaosPhase> =
                match rng.gen_range(0u32..5) {
                    0 if members.len() < 7 => Some(ChaosPhase::PeerJoin),
                    1 if members.len() >= 4 => Some(ChaosPhase::PeerLeave),
                    2 => Some(ChaosPhase::CapacityChange),
                    3 if members.len() < 6 => Some(ChaosPhase::ClusterGrow),
                    4 if members.len() >= 5 => Some(ChaosPhase::ClusterShrink),
                    _ => None,
                };
            match membership_phase {
                Some(ChaosPhase::PeerJoin) => {
                    let new_id = next_node_id;
                    next_node_id += 1;
                    let new_node = ClusterNode::new(
                        new_id,
                        TransportKind::Tcp,
                        &state_dir_path,
                    );
                    let new_idx = nodes.len();
                    let new_name = new_node.name.clone();
                    nodes.push(new_node);
                    members.push(new_idx);
                    // Extend policy vectors for the new node.
                    node_policies.push((
                        EvictionAlgorithm::Lru.new_policy(),
                        EvictionAlgorithm::Lru.new_policy(),
                    ));
                    node_algos
                        .push((EvictionAlgorithm::Lru, EvictionAlgorithm::Lru));
                    group.add_node(RepNode::new(
                        new_name.clone(),
                        NodeType::Electable,
                        "127.0.0.1".to_string(),
                        6900 + new_id as u16,
                        new_id,
                    ));
                    n_joins += 1;
                    n_chaos += 1;
                    eprintln!(
                        "[torture] r={round} PeerJoin: {new_name} members={}",
                        members.len()
                    );
                }
                Some(ChaosPhase::PeerLeave) => {
                    let removable: Vec<usize> = members
                        .iter()
                        .copied()
                        .filter(|&i| {
                            nodes[i].is_alive() && current_master != Some(i)
                        })
                        .collect();
                    if members.len() >= 4 && !removable.is_empty() {
                        let mi = removable[rng.gen_range(0..removable.len())];
                        let name = nodes[mi].name.clone();
                        group.remove_node(&name);
                        members.retain(|&i| i != mi);
                        nodes[mi].alive.store(false, Ordering::SeqCst);
                        n_leaves += 1;
                        n_chaos += 1;
                        eprintln!(
                            "[torture] r={round} PeerLeave: {name} members={}",
                            members.len()
                        );
                    }
                }
                Some(ChaosPhase::CapacityChange) => {
                    let mi = members[rng.gen_range(0..members.len())];
                    let name = nodes[mi].name.clone();
                    if let Some(mut n) = group.remove_node(&name) {
                        let new_cap =
                            [50u32, 75, 100, 125, 150][rng.gen_range(0..5)];
                        n.write_capacity_pct = new_cap;
                        n.read_capacity_pct = new_cap;
                        group.add_node(n);
                        n_cap_changes += 1;
                        n_chaos += 1;
                        eprintln!(
                            "[torture] r={round} CapacityChange: {name} cap={new_cap}%"
                        );
                    }
                }
                Some(ChaosPhase::ClusterGrow) => {
                    for _ in 0..2 {
                        if members.len() >= 7 {
                            break;
                        }
                        let new_id = next_node_id;
                        next_node_id += 1;
                        let new_node = ClusterNode::new(
                            new_id,
                            TransportKind::Tcp,
                            &state_dir_path,
                        );
                        let new_idx = nodes.len();
                        let new_name = new_node.name.clone();
                        nodes.push(new_node);
                        members.push(new_idx);
                        node_policies.push((
                            EvictionAlgorithm::Lru.new_policy(),
                            EvictionAlgorithm::Lru.new_policy(),
                        ));
                        node_algos.push((
                            EvictionAlgorithm::Lru,
                            EvictionAlgorithm::Lru,
                        ));
                        group.add_node(RepNode::new(
                            new_name,
                            NodeType::Electable,
                            "127.0.0.1".to_string(),
                            6900 + new_id as u16,
                            new_id,
                        ));
                        n_joins += 1;
                    }
                    n_chaos += 1;
                    eprintln!(
                        "[torture] r={round} ClusterGrow: members={}",
                        members.len()
                    );
                }
                Some(ChaosPhase::ClusterShrink) => {
                    for _ in 0..2 {
                        let removable: Vec<usize> = members
                            .iter()
                            .copied()
                            .filter(|&i| {
                                nodes[i].is_alive() && current_master != Some(i)
                            })
                            .collect();
                        if members.len() < 4 || removable.is_empty() {
                            break;
                        }
                        let mi = removable[rng.gen_range(0..removable.len())];
                        let name = nodes[mi].name.clone();
                        group.remove_node(&name);
                        members.retain(|&i| i != mi);
                        nodes[mi].alive.store(false, Ordering::SeqCst);
                        n_leaves += 1;
                    }
                    n_chaos += 1;
                    eprintln!(
                        "[torture] r={round} ClusterShrink: members={}",
                        members.len()
                    );
                }
                _ => {}
            }
        }

        // (c) Master handover — 5% per round.
        // Force the current master to step down and elect a different node.
        // Verifies no VLSN regression and no split-brain.
        if rng.gen_bool(0.05) && current_master.is_some() {
            let old_midx = current_master.unwrap();
            if nodes[old_midx].is_alive() {
                let old_vlsn = nodes[old_midx].vlsn();
                let old_name = nodes[old_midx].name.clone();
                eprintln!(
                    "[torture] r={round} MasterHandover: stepping down {} vlsn={old_vlsn}",
                    old_name
                );

                // Step down: clear current master (simulates become_replica on old master).
                current_master = None;

                // Pick a different alive node as the new proposer.
                let candidates: Vec<usize> = members
                    .iter()
                    .copied()
                    .filter(|&i| i != old_midx && nodes[i].is_alive())
                    .collect();
                if !candidates.is_empty() {
                    let new_proposer =
                        candidates[rng.gen_range(0..candidates.len())];
                    let mut handover_won = false;
                    for _attempt in 0..RETRY_BUDGET {
                        n_elections += 1;
                        if let Some(wid) = run_election_round(
                            new_proposer,
                            &nodes,
                            &group,
                            term,
                            &inv,
                        ) {
                            let widx = nodes
                                .iter()
                                .position(|n| n.id == wid)
                                .unwrap_or(new_proposer);
                            // Verify: no VLSN regression — new master's VLSN >= old master's.
                            let new_vlsn = nodes[widx].vlsn();
                            if new_vlsn < old_vlsn {
                                eprintln!(
                                    "[VIOLATION] MasterHandover vlsn regression: \
                                           old={old_name}@{old_vlsn} new={}@{new_vlsn}",
                                    nodes[widx].name
                                );
                                inv.violations.fetch_add(1, Ordering::SeqCst);
                            }
                            // Verify: no split-brain — only one master.
                            current_master = Some(widx);
                            n_won += 1;
                            handover_won = true;
                            eprintln!(
                                "[torture] r={round} MasterHandover complete: \
                                       new master={} vlsn={new_vlsn}",
                                nodes[widx].name
                            );
                            break;
                        }
                        term += 1;
                    }
                    term += 1;
                    if !handover_won {
                        eprintln!(
                            "[torture] r={round} MasterHandover: \
                                   re-election failed after {RETRY_BUDGET} attempts"
                        );
                    }
                }
                n_handovers += 1;
                n_chaos += 1;
            }
        }

        // (d) Eviction policy change — 20% per round.
        // Randomly select primary and (optionally distinct) scan algorithms
        // for a random alive node; exercises all five policy implementations.
        {
            let alive_now: Vec<usize> = members
                .iter()
                .copied()
                .filter(|&i| nodes[i].is_alive() && i < node_policies.len())
                .collect();
            if rng.gen_bool(0.20) && !alive_now.is_empty() {
                let target_ni = alive_now[rng.gen_range(0..alive_now.len())];
                let new_primary = ALL_ALGOS[rng.gen_range(0..ALL_ALGOS.len())];
                let new_scan = if rng.gen_bool(0.5) {
                    ALL_ALGOS[rng.gen_range(0..ALL_ALGOS.len())]
                } else {
                    new_primary
                };
                node_policies[target_ni] =
                    (new_primary.new_policy(), new_scan.new_policy());
                node_algos[target_ni] = (new_primary, new_scan);
                n_evict_changes += 1;
                n_chaos += 1;
                eprintln!(
                    "[torture] r={round} evict_policy: node={} primary={new_primary:?} scan={new_scan:?}",
                    nodes[target_ni].name
                );
            }
        }

        // ── Election ──────────────────────────────────────────────────────
        // Use `members` so dynamically added/removed nodes are included.
        let alive: Vec<usize> =
            members.iter().copied().filter(|&i| nodes[i].is_alive()).collect();
        if alive.len() < 2 {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }

        let proposer = *alive.iter().max_by_key(|&&i| nodes[i].vlsn()).unwrap();
        let mut won = false;

        for _attempt in 0..RETRY_BUDGET {
            n_elections += 1;
            if let Some(wid) =
                run_election_round(proposer, &nodes, &group, term, &inv)
            {
                let widx =
                    nodes.iter().position(|n| n.id == wid).unwrap_or(proposer);
                current_master = Some(widx);
                n_won += 1;
                won = true;
                eprintln!(
                    "[torture] r={round} t={term} master={} ({}) vlsn={}",
                    nodes[widx].name,
                    nodes[widx].transport.name(),
                    nodes[widx].vlsn()
                );
                break;
            }
            term += 1;
        }
        term += 1;

        if !won {
            eprintln!(
                "[torture] r={round} election failed after {RETRY_BUDGET} attempts"
            );
            continue;
        }

        // ── VLSN streaming ────────────────────────────────────────────────
        if let Some(midx) = current_master {
            let sv = vlsn_counter;
            vlsn_counter += VLSNS_PER_ROUND;

            for &i in &alive {
                if i == midx {
                    continue;
                }
                let got = stream_vlsns(
                    &nodes[midx],
                    &nodes[i],
                    sv,
                    VLSNS_PER_ROUND,
                    &inv,
                );
                n_streams += 1;
                eprintln!(
                    "[torture] r={round} streamed {got}/{VLSNS_PER_ROUND} \
                           master({})→replica({} {})",
                    nodes[midx].transport.name(),
                    nodes[i].name,
                    nodes[i].transport.name()
                );
            }
            nodes[midx].advance_vlsn(sv + VLSNS_PER_ROUND - 1);
            inv.record_vlsn(nodes[midx].id, nodes[midx].vlsn());
        }

        // ── Eviction policy simulation ─────────────────────────────────────
        // Exercise the active (primary, scan) policies for every alive node to
        // ensure that policy switches (EvictionPolicyChange chaos) and all five
        // algorithm implementations are exercised under concurrent replication.
        let page_base = vlsn_counter.wrapping_mul(37);
        for &ni in &alive {
            if ni >= node_policies.len() {
                continue;
            }
            let (ref pri, ref scan) = node_policies[ni];
            // Insert fake cache pages — spread by node index to avoid collisions.
            let base = page_base + ni as u64 * 1000;
            for k in 0u64..20 {
                pri.insert(base + k);
            }
            for k in 0u64..10 {
                scan.insert_cold(base + 100 + k);
            }
            // Touch half the primary pages (hot path).
            for k in (0u64..20).step_by(2) {
                let _ = pri.touch(base + k);
            }
            // Evict a handful — exercises LRU/Clock/ARC/CAR/LIRS candidate selection.
            for _ in 0..5 {
                let _ = pri.evict_candidate();
            }
            for _ in 0..3 {
                let _ = scan.evict_candidate();
            }
        }

        // The background netem thread handles network restoration; no overlay_calm() here.

        // ── Periodic stats ────────────────────────────────────────────────
        let now = Instant::now();
        if now >= next_report {
            next_report = now + Duration::from_secs(30);
            // Sample current policies for one node to show in the report.
            let evict_info = if !alive.is_empty() {
                let ni = alive[0];
                if ni < node_algos.len() {
                    let (p, s) = node_algos[ni];
                    format!("{p:?}/{s:?}")
                } else {
                    "?/?".to_string()
                }
            } else {
                "-".to_string()
            };
            eprintln!(
                "[torture] elapsed={:.0?} r={round} elect={n_elections} won={n_won} \
                 streams={n_streams} crashes={n_crashes} chaos={n_chaos} \
                 joins={n_joins} leaves={n_leaves} cap_changes={n_cap_changes} \
                 handovers={n_handovers} evict_changes={n_evict_changes} evict=[{evict_info}] \
                 members={} violations={}",
                start.elapsed(),
                members.len(),
                inv.violations()
            );
        }
    }

    // ── Tear-down background netem thread ──────────────────────────────────
    bg_done.store(true, Ordering::Relaxed);
    let _ = bg_handle.join();

    // ── Final report ──────────────────────────────────────────────────────
    let netem_active = netem.active;
    let violations = inv.violations();

    eprintln!("[torture] ═══════════════════════════════════════════════════");
    eprintln!("[torture] FINAL  elapsed={:.1?}", start.elapsed());
    eprintln!(
        "[torture]   transport          : [{}]",
        transports.iter().map(|t| t.name()).collect::<Vec<_>>().join(",")
    );
    eprintln!("[torture]   tc netem active    : {netem_active}");
    eprintln!("[torture]   rounds             : {round}");
    eprintln!(
        "[torture]   elections          : {n_elections} attempted / {n_won} succeeded"
    );
    eprintln!("[torture]   vlsn streams       : {n_streams}");
    eprintln!("[torture]   node crashes       : {n_crashes}");
    eprintln!("[torture]   chaos rounds       : {n_chaos}");
    eprintln!("[torture]   peer joins         : {n_joins}");
    eprintln!("[torture]   peer leaves        : {n_leaves}");
    eprintln!("[torture]   capacity changes   : {n_cap_changes}");
    eprintln!("[torture]   master handovers   : {n_handovers}");
    eprintln!("[torture]   evict changes      : {n_evict_changes}");
    eprintln!("[torture]   final group size   : {}", members.len());
    for &mi in &members {
        let n = &nodes[mi];
        let (pa, sa) = if mi < node_algos.len() {
            node_algos[mi]
        } else {
            (EvictionAlgorithm::Lru, EvictionAlgorithm::Lru)
        };
        eprintln!(
            "[torture]   {} ({})  vlsn={}  evict={pa:?}/{sa:?}",
            n.name,
            n.transport.name(),
            n.vlsn()
        );
    }
    // Also report any dynamically removed nodes for completeness.
    for (i, n) in nodes.iter().enumerate() {
        if !members.contains(&i) {
            eprintln!(
                "[torture]   {} ({})  vlsn={} [removed]",
                n.name,
                n.transport.name(),
                n.vlsn()
            );
        }
    }
    eprintln!("[torture]   violations         : {violations}");
    eprintln!("[torture] ═══════════════════════════════════════════════════");

    assert_eq!(violations, 0, "{violations} invariant violation(s)");
    assert!(n_won > 0, "no elections succeeded");
}
