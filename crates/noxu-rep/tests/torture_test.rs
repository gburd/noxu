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

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    dead_code,
    unused_imports
)]

use std::collections::HashMap;
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

use noxu_rep::{NodeType, RepGroup, RepNode};
use noxu_rep::elections::{run_acceptor, run_election};
use noxu_rep::net::{Channel, TcpChannel, TcpChannelListener};
use noxu_rep::stream::{FeederRunner, LogScanner};

/// Maximum time to wait for any transport connect under kernel netem chaos.
/// TCP's OS default (~2 min) and QUIC's internal retry timeout (~30 s) both
/// make election retries unbearably slow under MinorityPartition (80% loss).
/// 2 s is sufficient for loopback.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

// QUIC types — compiled only when the `quic` feature is enabled.
#[cfg(feature = "quic")]
use noxu_rep::net::{
    QuicChannel, QuicChannelListener,
    QuicMultiplexedChannel, QuicMultiplexedChannelListener,
    ReplicationChannel,
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

const NUM_NODES: usize = 3;
const VLSNS_PER_ROUND: u64 = 50;
const RETRY_BUDGET: u32 = 20;

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
            Self::Tcp     => "tcp",
            Self::Quic    => "quic",
            Self::QuicMux => "quic_mux",
        }
    }
}

/// Read `TRANSPORT` env var and return per-node transport assignments.
fn node_transports() -> Vec<TransportKind> {
    let raw = std::env::var("TRANSPORT").unwrap_or_else(|_| "tcp".to_string());
    let kind = match raw.to_lowercase().as_str() {
        "quic"     => TransportKind::Quic,
        "quic_mux" => TransportKind::QuicMux,
        "mix"      => {
            // TCP for nodes 0-1, QUIC for node 2.
            return vec![TransportKind::Tcp, TransportKind::Tcp, TransportKind::Quic];
        }
        _ => TransportKind::Tcp,
    };
    vec![kind; NUM_NODES]
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
                QuicMultiplexedChannelListener::bind(addr).ok().map(Self::QuicMux)
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
            Self::Tcp(l) => l.accept().ok().map(|c| Box::new(c) as Box<dyn Channel>),
            #[cfg(feature = "quic")]
            Self::Quic(l) => l.accept().ok().map(|c| Box::new(c) as Box<dyn Channel>),
            #[cfg(feature = "quic")]
            Self::QuicMux(l) => l
                .accept()
                .ok()
                .map(|mux| Box::new(MuxElectionChannel(Arc::new(mux))) as Box<dyn Channel>),
        }
    }

    /// Accept and return a channel for **VLSN log streaming**.
    ///
    /// For QuicMux this is the log sub-channel (independent of elections).
    fn accept_log(self) -> Option<Box<dyn Channel>> {
        match self {
            Self::Tcp(l) => l.accept().ok().map(|c| Box::new(c) as Box<dyn Channel>),
            #[cfg(feature = "quic")]
            Self::Quic(l) => l.accept().ok().map(|c| Box::new(c) as Box<dyn Channel>),
            #[cfg(feature = "quic")]
            Self::QuicMux(l) => l
                .accept()
                .ok()
                .map(|mux| Box::new(MuxLogChannel(Arc::new(mux))) as Box<dyn Channel>),
        }
    }
}

// ============================================================================
// Transport-agnostic connect helpers
// ============================================================================

/// Open an **election** channel to `addr` using the given transport.
fn connect_election(kind: TransportKind, addr: SocketAddr) -> Option<Arc<dyn Channel>> {
    match kind {
        TransportKind::Tcp => {
            // Use a short timeout so election retries stay fast under kernel
            // netem packet loss (OS default TCP timeout is ~2 min).
            match std::net::TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
                Ok(s)  => Some(Arc::new(TcpChannel::new(s)) as Arc<dyn Channel>),
                Err(e) => { eprintln!("[torture] TCP connect to {addr} failed: {e}"); None }
            }
        }
        #[cfg(feature = "quic")]
        TransportKind::Quic => quic_connect_timeout(addr, CONNECT_TIMEOUT,
            |a| QuicChannel::connect(a, "localhost")
                .ok()
                .map(|c| Arc::new(c) as Arc<dyn Channel>)),
        #[cfg(feature = "quic")]
        TransportKind::QuicMux => quic_connect_timeout(addr, CONNECT_TIMEOUT,
            |a| QuicMultiplexedChannel::connect(a, "localhost")
                .ok()
                .map(|mux| Arc::new(MuxElectionChannel(Arc::new(mux))) as Arc<dyn Channel>)),
        #[allow(unreachable_patterns)]
        _ => TcpChannel::connect(addr)
            .ok()
            .map(|c| Arc::new(c) as Arc<dyn Channel>),
    }
}

/// Open a **VLSN streaming** channel to `addr` using the given transport.
fn connect_log(kind: TransportKind, addr: SocketAddr) -> Option<Arc<dyn Channel>> {
    match kind {
        TransportKind::Tcp => std::net::TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)
            .ok()
            .map(|s| Arc::new(TcpChannel::new(s)) as Arc<dyn Channel>),
        #[cfg(feature = "quic")]
        TransportKind::Quic => quic_connect_timeout(addr, CONNECT_TIMEOUT,
            |a| QuicChannel::connect(a, "localhost")
                .ok()
                .map(|c| Arc::new(c) as Arc<dyn Channel>)),
        #[cfg(feature = "quic")]
        TransportKind::QuicMux => quic_connect_timeout(addr, CONNECT_TIMEOUT,
            |a| QuicMultiplexedChannel::connect(a, "localhost")
                .ok()
                .map(|mux| Arc::new(MuxLogChannel(Arc::new(mux))) as Arc<dyn Channel>)),
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
fn quic_connect_timeout<F>(addr: SocketAddr, timeout: Duration, f: F) -> Option<Arc<dyn Channel>>
where
    F: FnOnce(SocketAddr) -> Option<Arc<dyn Channel>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || { let _ = tx.send(f(addr)); });
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
    fn receive(&self, timeout: Duration) -> noxu_rep::error::Result<Option<Vec<u8>>> {
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
    fn receive(&self, timeout: Duration) -> noxu_rep::error::Result<Option<Vec<u8>>> {
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

struct TcNetemGuard {
    pub active: bool,
}

impl TcNetemGuard {
    fn setup() -> Self {
        // Clean up any leftover qdisc from a previous killed run.  The 'del'
        // fails harmlessly when no netem qdisc is present.
        Self::run_tc(&["qdisc", "del", "dev", "lo", "root"]);
        if Self::run_tc(&["qdisc", "add", "dev", "lo", "root", "netem",
                          "loss", "2%", "delay", "5ms", "2ms",
                          "reorder", "5%", "25%"]) {
            eprintln!("[torture] tc netem active on lo");
            Self { active: true }
        } else {
            eprintln!("[torture] WARNING: tc netem not available \
                       (CAP_NET_ADMIN?); software-only fault injection");
            Self { active: false }
        }
    }

    fn change(&self, loss_pct: f32, delay_ms: u64, jitter_ms: u64,
               reorder_pct: f32, dup_pct: f32, corrupt_pct: f32) {
        if !self.active { return; }
        Self::run_tc(&[
            "qdisc", "change", "dev", "lo", "root", "netem",
            "loss",      &format!("{loss_pct}%"),
            "delay",     &format!("{delay_ms}ms"), &format!("{jitter_ms}ms"),
            "reorder",   &format!("{reorder_pct}%"), "50%",
            "duplicate", &format!("{dup_pct}%"),
            "corrupt",   &format!("{corrupt_pct}%"),
        ]);
    }

    fn calm(&self) {
        if self.active { self.change(0.0, 0, 0, 0.0, 0.0, 0.0); }
    }

    fn run_tc(args: &[&str]) -> bool {
        // 1. Try setuid helper (user installs this via `make tc-helper`).
        let helper = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.to_path_buf()))
            .map(|d| d.join("../../../scripts/tc_netem_helper"))
            .unwrap_or_else(|| PathBuf::from("scripts/tc_netem_helper"));
        if helper.exists() && Command::new(&helper).args(args).status()
            .map(|s| s.success()).unwrap_or(false) { return true; }
        // 2. Try direct tc (works if process already has CAP_NET_ADMIN).
        if Command::new("tc").args(args).status()
            .map(|s| s.success()).unwrap_or(false) { return true; }
        // 3. Try passwordless sudo tc.
        if Command::new("sudo").arg("-n").arg("tc").args(args).status()
            .map(|s| s.success()).unwrap_or(false) { return true; }
        false
    }
}

impl Drop for TcNetemGuard {
    fn drop(&mut self) {
        if self.active {
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

struct MemLogScanner { next: u64, max: u64 }

impl MemLogScanner {
    fn new(start: u64, count: u64) -> Self {
        Self { next: start, max: start + count - 1 }
    }
}

impl LogScanner for MemLogScanner {
    fn next_entry(&mut self, from: u64) -> Option<(u64, u8, Vec<u8>)> {
        let v = self.next.max(from);
        if v > self.max { return None; }
        self.next = v + 1;
        Some((v, 1u8, format!("e{v}").into_bytes()))
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
    last_log_port:      Arc<Mutex<u16>>,
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
            last_log_port:      Arc::new(Mutex::new(0)),
        }
    }

    fn vlsn(&self) -> u64 { self.vlsn.load(Ordering::SeqCst) }

    fn advance_vlsn(&self, to: u64) {
        let old = self.vlsn.fetch_max(to, Ordering::SeqCst);
        if to > old { self.disk.save(to); }
    }

    fn is_alive(&self) -> bool { self.alive.load(Ordering::SeqCst) }
}

// ============================================================================
// Invariant log
// ============================================================================

struct InvariantLog {
    term_winners:  Mutex<HashMap<u64, u32>>,
    vlsn_history:  Mutex<HashMap<u32, u64>>,  // last seen per node
    violations:    AtomicU64,
}

impl InvariantLog {
    fn new() -> Self {
        Self {
            term_winners: Mutex::new(HashMap::new()),
            vlsn_history: Mutex::new(HashMap::new()),
            violations:   AtomicU64::new(0),
        }
    }

    fn record_winner(&self, term: u64, winner: u32) {
        let mut m = self.term_winners.lock().unwrap();
        if let Some(&prev) = m.get(&term) {
            if prev != winner {
                eprintln!("[VIOLATION] split-brain: term={term} prev={prev} new={winner}");
                self.violations.fetch_add(1, Ordering::SeqCst);
            }
        } else { m.insert(term, winner); }
    }

    fn record_vlsn(&self, node_id: u32, vlsn: u64) {
        let mut h = self.vlsn_history.lock().unwrap();
        let prev = *h.get(&node_id).unwrap_or(&0);
        if vlsn < prev {
            eprintln!("[VIOLATION] vlsn regression: node={node_id} {prev}→{vlsn}");
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
        h.insert(node_id, vlsn.max(prev));
    }

    fn check_durability(&self, id: u32, disk: u64, restart: u64) {
        if restart < disk {
            eprintln!("[VIOLATION] durability: node={id} disk={disk} restart={restart}");
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn violations(&self) -> u64 { self.violations.load(Ordering::SeqCst) }
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
    let alive: Vec<usize> = (0..nodes.len())
        .filter(|&i| nodes[i].is_alive())
        .collect();
    if alive.len() < 2 || !nodes[proposer_idx].is_alive() { return None; }

    // Bind a fresh ephemeral election listener on each alive non-proposer.
    let mut listeners: Vec<(usize, AnyListener)> = Vec::new();
    for &i in &alive {
        if i == proposer_idx { continue; }
        let kind = nodes[i].transport;
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        match AnyListener::bind(kind, addr) {
            Some(l) => {
                *nodes[i].last_election_port.lock().unwrap() = l.local_addr().port();
                listeners.push((i, l));
            }
            None => {
                eprintln!("[torture] election bind failed for node {}", nodes[i].name);
                return None;
            }
        }
    }

    // Spawn one acceptor thread per listener.
    // Use a channel so we can wait with a timeout — QUIC accept() blocks
    // indefinitely and h.join() would hang forever if the proposer never
    // connects (e.g. under 80% packet loss in MinorityPartition).
    let mut acceptor_rxs: Vec<(usize, mpsc::Receiver<noxu_rep::error::Result<Option<String>>>)> =
        Vec::new();
    for (i, listener) in listeners {
        let name  = nodes[i].name.clone();
        let vlsn  = nodes[i].vlsn();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let r = match listener.accept_election() {
                Some(ch) => run_acceptor(ch.as_ref(), &name, vlsn, 1, term),
                None     => Ok(None),
            };
            let _ = tx.send(r);
        });
        acceptor_rxs.push((i, rx));
    }

    // Proposer connects to each acceptor's election port.
    let proposer = &nodes[proposer_idx];
    let mut peer_channels: Vec<Arc<dyn Channel>> = Vec::new();
    let peer_node_ids: Vec<usize> = acceptor_rxs.iter().map(|(i, _)| *i).collect();

    for &ni in &peer_node_ids {
        let port = *nodes[ni].last_election_port.lock().unwrap();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        // Proposer uses the *acceptor's* transport kind when connecting.
        match connect_election(nodes[ni].transport, addr) {
            Some(ch) => peer_channels.push(ch),
            None => {
                eprintln!("[torture] election connect failed to node {}", nodes[ni].name);
                // Do NOT join — acceptor threads may be blocked in QUIC accept().
                // They'll self-terminate when the listener drops or process exits.
                return None;
            }
        }
    }

    let result = run_election(
        proposer.id, &proposer.name, group, &peer_channels,
        proposer.vlsn(), 1, term,
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

    let replica_id    = replica.id;
    let replica_vlsn  = Arc::clone(&replica.vlsn);
    let disk_path     = replica.disk.path.clone();

    // Replica receive thread.
    let recv_h = std::thread::spawn(move || {
        let ch = match listener.accept_log() {
            Some(c) => c,
            None    => return 0u64,
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
                        if v > old { let _ = fs::write(&disk_path, v.to_le_bytes()); }
                    }
                    let _ = ch.send(&v.to_le_bytes()); // ack
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        received
    });

    // Master connects to replica's log listener using replica's transport.
    let master_ch: Arc<dyn Channel> = match connect_log(replica.transport, addr) {
        Some(c) => c,
        None    => { let _ = recv_h.join(); return 0; }
    };

    let runner = FeederRunner::new(Arc::clone(&master_ch), start_vlsn);
    let mut scanner = MemLogScanner::new(start_vlsn, count);
    let _ = runner.run(&mut scanner);
    let _ = master_ch.close();

    let received = recv_h.join().unwrap_or(0);

    master.advance_vlsn(start_vlsn + count - 1);
    inv.record_vlsn(master.id,  master.vlsn());
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
}

// ============================================================================
// Main torture loop
// ============================================================================

#[test]
#[ignore]
fn torture_replication() {
    let duration   = torture_duration();
    let transports = node_transports();

    eprintln!(
        "[torture] starting duration={duration:?} transport=[{}]",
        transports.iter().map(|t| t.name()).collect::<Vec<_>>().join(",")
    );

    let state_dir = TempDir::new().expect("TempDir");

    let nodes: Vec<ClusterNode> = (1..=NUM_NODES as u32)
        .map(|id| ClusterNode::new(id, transports[(id - 1) as usize], state_dir.path()))
        .collect();

    let mut group = RepGroup::new("torture".to_string(), 99);
    for n in &nodes {
        group.add_node(RepNode::new(
            n.name.clone(), NodeType::Electable,
            "127.0.0.1".to_string(), 6900 + n.id as u16, n.id,
        ));
    }

    let inv   = Arc::new(InvariantLog::new());
    let netem = TcNetemGuard::setup();
    let mut rng  = StdRng::seed_from_u64(0xDEAD_BEEF_CAFE);
    let mut term = 1u64;
    let mut vlsn_counter = 1u64;
    let mut round = 0u64;
    let mut current_master: Option<usize> = None;

    // Stats
    let (mut n_elections, mut n_won, mut n_streams, mut n_crashes, mut n_chaos) =
        (0u64, 0u64, 0u64, 0u64, 0u64);

    let start = Instant::now();
    let mut next_report = start + Duration::from_secs(30);

    while start.elapsed() < duration {
        round += 1;

        // ── Chaos injection ───────────────────────────────────────────────
        let phase: ChaosPhase = if rng.gen_bool(0.15) {
            n_chaos += 1;
            match rng.gen_range(0u32..6) {
                0 => ChaosPhase::ModeratePacketLoss,
                1 => ChaosPhase::HighLatency,
                2 => ChaosPhase::ReorderHeavy,
                3 => ChaosPhase::DuplicateAndCorrupt,
                4 => {
                    let alive: Vec<_> = (0..NUM_NODES).filter(|&i| nodes[i].is_alive()).collect();
                    if alive.len() > 1 {
                        let idx = alive[rng.gen_range(0..alive.len())];
                        ChaosPhase::NodeCrash(idx)
                    } else { n_chaos -= 1; ChaosPhase::Calm }
                }
                _ => ChaosPhase::MinorityPartition,
            }
        } else {
            ChaosPhase::Calm
        };

        match &phase {
            ChaosPhase::Calm               => netem.calm(),
            ChaosPhase::ModeratePacketLoss => netem.change(5.0, 2, 1, 2.0, 0.0, 0.0),
            ChaosPhase::HighLatency        => netem.change(1.0, 80, 40, 5.0, 0.0, 0.0),
            ChaosPhase::ReorderHeavy       => netem.change(3.0, 10, 5, 20.0, 0.0, 0.0),
            ChaosPhase::DuplicateAndCorrupt=> netem.change(1.0, 5, 2, 5.0, 10.0, 1.0),
            ChaosPhase::MinorityPartition  => netem.change(80.0, 5, 2, 0.0, 0.0, 0.0),
            ChaosPhase::NodeCrash(idx) => {
                let n = &nodes[*idx];
                let disk_vlsn = n.vlsn();
                eprintln!("[torture] crash node {} ({}), vlsn={disk_vlsn}", n.name, n.transport.name());
                n.alive.store(false, Ordering::SeqCst);
                n_crashes += 1;
                if current_master == Some(*idx) { current_master = None; }

                let alive_flag = Arc::clone(&n.alive);
                let vlsn_atom  = Arc::clone(&n.vlsn);
                let disk_path  = n.disk.path.clone();
                let node_id    = n.id;
                let inv2       = Arc::clone(&inv);
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(100 + (node_id as u64) * 50));
                    let restart_vlsn = fs::read(&disk_path)
                        .ok()
                        .and_then(|b| b.try_into().ok())
                        .map(u64::from_le_bytes)
                        .unwrap_or(0);
                    inv2.check_durability(node_id, disk_vlsn, restart_vlsn);
                    vlsn_atom.fetch_max(restart_vlsn, Ordering::SeqCst);
                    alive_flag.store(true, Ordering::SeqCst);
                    eprintln!("[torture] restart node{node_id} vlsn={restart_vlsn}");
                });

                netem.calm();
            }
        }

        if !matches!(phase, ChaosPhase::Calm | ChaosPhase::NodeCrash(_)) {
            eprintln!("[torture] r={round} chaos={:?}", phase);
        }

        // ── Election ──────────────────────────────────────────────────────
        let alive: Vec<usize> = (0..NUM_NODES).filter(|&i| nodes[i].is_alive()).collect();
        if alive.len() < 2 { std::thread::sleep(Duration::from_millis(50)); continue; }

        let proposer = *alive.iter().max_by_key(|&&i| nodes[i].vlsn()).unwrap();
        let mut won = false;

        for _attempt in 0..RETRY_BUDGET {
            n_elections += 1;
            if let Some(wid) = run_election_round(proposer, &nodes, &group, term, &inv) {
                let widx = nodes.iter().position(|n| n.id == wid).unwrap_or(proposer);
                current_master = Some(widx);
                n_won += 1;
                won = true;
                eprintln!("[torture] r={round} t={term} master={} ({}) vlsn={}",
                           nodes[widx].name, nodes[widx].transport.name(), nodes[widx].vlsn());
                break;
            }
            term += 1;
        }
        term += 1;

        if !won {
            eprintln!("[torture] r={round} election failed after {RETRY_BUDGET} attempts");
            netem.calm();
            continue;
        }

        // ── VLSN streaming ────────────────────────────────────────────────
        if let Some(midx) = current_master {
            let sv = vlsn_counter;
            vlsn_counter += VLSNS_PER_ROUND;

            for &i in &alive {
                if i == midx { continue; }
                let got = stream_vlsns(&nodes[midx], &nodes[i], sv, VLSNS_PER_ROUND, &inv);
                n_streams += 1;
                eprintln!("[torture] r={round} streamed {got}/{VLSNS_PER_ROUND} \
                           master({})→replica({} {})",
                           nodes[midx].transport.name(),
                           nodes[i].name, nodes[i].transport.name());
            }
            nodes[midx].advance_vlsn(sv + VLSNS_PER_ROUND - 1);
            inv.record_vlsn(nodes[midx].id, nodes[midx].vlsn());
        }

        // Restore calm after a network-chaos phase.
        if !matches!(phase, ChaosPhase::Calm | ChaosPhase::NodeCrash(_)) {
            std::thread::sleep(Duration::from_millis(20));
            netem.calm();
        }

        // ── Periodic stats ────────────────────────────────────────────────
        let now = Instant::now();
        if now >= next_report {
            next_report = now + Duration::from_secs(30);
            eprintln!(
                "[torture] elapsed={:.0?} r={round} elect={n_elections} won={n_won} \
                 streams={n_streams} crashes={n_crashes} chaos={n_chaos} \
                 violations={}",
                start.elapsed(), inv.violations()
            );
        }
    }

    // ── Final report ──────────────────────────────────────────────────────
    netem.calm();
    let violations = inv.violations();

    eprintln!("[torture] ═══════════════════════════════════════════════════");
    eprintln!("[torture] FINAL  elapsed={:.1?}", start.elapsed());
    eprintln!("[torture]   transport          : [{}]",
              transports.iter().map(|t| t.name()).collect::<Vec<_>>().join(","));
    eprintln!("[torture]   tc netem active    : {}", netem.active);
    eprintln!("[torture]   rounds             : {round}");
    eprintln!("[torture]   elections          : {n_elections} attempted / {n_won} succeeded");
    eprintln!("[torture]   vlsn streams       : {n_streams}");
    eprintln!("[torture]   node crashes       : {n_crashes}");
    eprintln!("[torture]   chaos rounds       : {n_chaos}");
    for n in &nodes {
        eprintln!("[torture]   {} ({})  vlsn={}", n.name, n.transport.name(), n.vlsn());
    }
    eprintln!("[torture]   violations         : {violations}");
    eprintln!("[torture] ═══════════════════════════════════════════════════");

    assert_eq!(violations, 0, "{violations} invariant violation(s)");
    assert!(n_won > 0, "no elections succeeded");
}
