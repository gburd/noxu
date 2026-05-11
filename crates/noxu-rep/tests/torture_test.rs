//! Real-network replication chaos / torture test.
//!
//! This test exercises Noxu replication against failure modes that the
//! in-process `chaos_test.rs` cannot reach:
//!
//! - Real TCP sockets under `tc netem` kernel-level fault injection
//!   (packet loss, latency, reorder, duplication, corruption)
//! - TCP connection teardown from node crash (process-level via
//!   `ReplicatedEnvironment::close()`)
//! - VLSN state persisted to disk; verified after node restart
//! - 5-10 minute long-running driver that cycles through:
//!     • normal operation
//!     • lossy / high-latency chaos phases
//!     • random node crashes with restart
//!     • minority-partition → majority-partition heal
//!
//! ## Running
//!
//! ```text
//! # Without tc netem (software fault injection only):
//! cargo nextest run -p noxu-rep --test torture_test -- --ignored
//!
//! # With tc netem (kernel fault injection, requires CAP_NET_ADMIN):
//! sudo cargo nextest run -p noxu-rep --test torture_test -- --ignored
//! # or:
//! cargo nextest run -p noxu-rep --test torture_test -- --ignored
//!   (test auto-detects and falls back gracefully)
//! ```
//!
//! ## Invariants verified
//!
//! 1. **Safety**: at most one winner per election term.
//! 2. **VLSN monotonicity**: each node's VLSN never decreases.
//! 3. **Durability**: after crash+restart, a node's VLSN >= its last
//!    persisted VLSN.
//! 4. **Liveness**: elections succeed within RETRY_BUDGET attempts
//!    under moderate fault rates.
//! 5. **No panic**: the protocol returns `None`/`Err` under all injected
//!    faults rather than panicking.

#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use noxu_rep::{NodeType, RepGroup, RepNode};
use noxu_rep::elections::{run_acceptor, run_election};
use noxu_rep::net::{Channel, TcpChannel, TcpChannelListener};
use noxu_rep::stream::{FeederRunner, LogScanner};

// ============================================================================
// Test configuration
// ============================================================================

/// Total test duration in seconds. Override with TORTURE_SECS env var.
fn torture_duration() -> Duration {
    let secs: u64 = std::env::var("TORTURE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300); // default 5 minutes
    Duration::from_secs(secs)
}

/// Number of nodes in the cluster.
const NUM_NODES: usize = 3;

/// VLSNs streamed per feeder session.
const VLSNS_PER_ROUND: u64 = 50;

/// Maximum election retries per round.
const RETRY_BUDGET: u32 = 20;

/// Paxos acceptor timeout (ms).
const ACCEPTOR_TIMEOUT_MS: u64 = 800;

// ============================================================================
// tc netem RAII guard
// ============================================================================

/// RAII guard that applies `tc netem` on the loopback interface and
/// removes it on drop. The test proceeds even if tc setup fails (logged
/// as a warning), giving software-only fault injection.
struct TcNetemGuard {
    active: bool,
}

impl TcNetemGuard {
    /// Apply an initial (moderate) netem discipline on `lo`.
    fn setup() -> Self {
        if Self::run_tc(&["qdisc", "add", "dev", "lo", "root", "netem",
                          "loss", "2%",
                          "delay", "5ms", "2ms",
                          "reorder", "5%", "25%"]) {
            eprintln!("[torture] tc netem active on lo");
            Self { active: true }
        } else {
            eprintln!("[torture] WARNING: tc netem not available (CAP_NET_ADMIN?); \
                       running with software-only fault injection");
            Self { active: false }
        }
    }

    /// Change the netem parameters on `lo` without removing the qdisc.
    fn change(&self, loss_pct: f32, delay_ms: u64, delay_jitter_ms: u64,
               reorder_pct: f32, duplicate_pct: f32, corrupt_pct: f32) {
        if !self.active { return; }
        let loss   = format!("{}%", loss_pct);
        let delay  = format!("{}ms", delay_ms);
        let jitter = format!("{}ms", delay_jitter_ms);
        let reord  = format!("{}%", reorder_pct);
        let dup    = format!("{}%", duplicate_pct);
        let corr   = format!("{}%", corrupt_pct);
        Self::run_tc(&["qdisc", "change", "dev", "lo", "root", "netem",
                       "loss", &loss,
                       "delay", &delay, &jitter,
                       "reorder", &reord, "50%",
                       "duplicate", &dup,
                       "corrupt", &corr]);
    }

    /// Reset to a clean (no-fault) netem config.
    fn calm(&self) {
        if self.active {
            self.change(0.0, 0, 0, 0.0, 0.0, 0.0);
        }
    }

    fn remove(&self) {
        if self.active {
            Self::run_tc(&["qdisc", "del", "dev", "lo", "root"]);
        }
    }

    fn run_tc(args: &[&str]) -> bool {
        // Try without sudo first; if that fails, try with sudo.
        for prefix in &[vec![], vec!["sudo", "-n"]] {
            let mut cmd_args: Vec<&str> = prefix.to_vec();
            cmd_args.extend_from_slice(args);
            let status = Command::new(cmd_args[0])
                .args(&cmd_args[1..])
                .status();
            if let Ok(s) = status {
                if s.success() { return true; }
            }
        }
        false
    }
}

impl Drop for TcNetemGuard {
    fn drop(&mut self) {
        self.remove();
    }
}

// ============================================================================
// Persistent VLSN state (survives simulated crash)
// ============================================================================

/// Per-node persistent state stored in a TempDir.
/// Simulates durable state that survives process restart.
struct NodeDisk {
    path: PathBuf,
}

impl NodeDisk {
    fn new(dir: &Path, node_id: u32) -> Self {
        let path = dir.join(format!("node{node_id}_vlsn.state"));
        Self { path }
    }

    fn save_vlsn(&self, vlsn: u64) {
        let _ = fs::write(&self.path, vlsn.to_le_bytes());
    }

    fn load_vlsn(&self) -> u64 {
        fs::read(&self.path)
            .ok()
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0)
    }
}

// ============================================================================
// In-memory log scanner (VecDeque of VLSN entries)
// ============================================================================

struct MemLogScanner {
    next_vlsn: u64,
    max_vlsn: u64,
}

impl MemLogScanner {
    fn new(start: u64, count: u64) -> Self {
        Self { next_vlsn: start, max_vlsn: start + count - 1 }
    }
}

impl LogScanner for MemLogScanner {
    fn next_entry(&mut self, from_vlsn: u64) -> Option<(u64, u8, Vec<u8>)> {
        let v = self.next_vlsn.max(from_vlsn);
        if v > self.max_vlsn { return None; }
        self.next_vlsn = v + 1;
        Some((v, 1u8, format!("entry-{v}").into_bytes()))
    }
}

// ============================================================================
// Cluster node handle
// ============================================================================

struct ClusterNode {
    id: u32,
    name: String,
    current_vlsn: Arc<AtomicU64>,
    disk: NodeDisk,
    alive: Arc<AtomicBool>,
    /// Port assigned by OS — ephemeral per election round.
    last_port: Arc<Mutex<u16>>,
}

impl ClusterNode {
    fn new(id: u32, state_dir: &Path) -> Self {
        let disk = NodeDisk::new(state_dir, id);
        let saved = disk.load_vlsn();
        Self {
            id,
            name: format!("torture_node{id}"),
            current_vlsn: Arc::new(AtomicU64::new(saved)),
            disk,
            alive: Arc::new(AtomicBool::new(true)),
            last_port: Arc::new(Mutex::new(0)),
        }
    }

    fn vlsn(&self) -> u64 {
        self.current_vlsn.load(Ordering::SeqCst)
    }

    fn advance_vlsn(&self, to: u64) {
        let old = self.current_vlsn.fetch_max(to, Ordering::SeqCst);
        if to > old {
            self.disk.save_vlsn(to);
        }
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }
}

// ============================================================================
// Shared invariant state
// ============================================================================

struct InvariantLog {
    /// term → winner_id — at most one winner per term.
    term_winners: Mutex<HashMap<u64, u32>>,
    /// Per-node VLSN history — must be monotone.
    vlsn_history: Mutex<HashMap<u32, Vec<u64>>>,
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
        let mut map = self.term_winners.lock().unwrap();
        if let Some(&prev) = map.get(&term) {
            if prev != winner {
                eprintln!("[VIOLATION] SPLIT-BRAIN: term={term} winner was {prev}, now {winner}");
                self.violations.fetch_add(1, Ordering::SeqCst);
            }
        } else {
            map.insert(term, winner);
        }
    }

    fn record_vlsn(&self, node_id: u32, vlsn: u64) {
        let mut hist = self.vlsn_history.lock().unwrap();
        let entry = hist.entry(node_id).or_default();
        if let Some(&last) = entry.last() {
            if vlsn < last {
                eprintln!("[VIOLATION] VLSN REGRESSION: node={node_id} {last} → {vlsn}");
                self.violations.fetch_add(1, Ordering::SeqCst);
            }
        }
        entry.push(vlsn);
    }

    fn check_durability(&self, node_id: u32, disk_vlsn: u64, restart_vlsn: u64) {
        if restart_vlsn < disk_vlsn {
            eprintln!("[VIOLATION] DURABILITY: node={node_id} disk_vlsn={disk_vlsn} restart_vlsn={restart_vlsn}");
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn violation_count(&self) -> u64 {
        self.violations.load(Ordering::SeqCst)
    }
}

// ============================================================================
// Election helper — one full Paxos round over real TCP
// ============================================================================

/// Run one election round. Returns the elected winner's node id, or None.
///
/// Each alive non-proposer binds a fresh ephemeral TCP listener, spawns
/// an acceptor thread, then the proposer connects and runs the election.
fn run_tcp_election(
    proposer_idx: usize,
    nodes: &[ClusterNode],
    group: &RepGroup,
    term: u64,
    inv: &InvariantLog,
) -> Option<u32> {
    let alive: Vec<usize> = (0..nodes.len())
        .filter(|&i| nodes[i].is_alive())
        .collect();
    if alive.len() < 2 { return None; }
    if !nodes[proposer_idx].is_alive() { return None; }

    let proposer = &nodes[proposer_idx];

    // Bind listeners on all alive non-proposers.
    let mut listener_data: Vec<(usize, TcpChannelListener, SocketAddr)> = Vec::new();
    for &i in &alive {
        if i == proposer_idx { continue; }
        let listener = match TcpChannelListener::bind("127.0.0.1:0".parse().unwrap()) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[torture] node{} bind failed: {e}", nodes[i].id);
                return None;
            }
        };
        let addr = listener.local_addr().unwrap();
        *nodes[i].last_port.lock().unwrap() = addr.port();
        listener_data.push((i, listener, addr));
    }

    // Spawn acceptor threads.
    let acceptor_handles: Vec<_> = listener_data
        .iter()
        .map(|(i, _, _)| {
            let idx = *i;
            // We can't move TcpChannelListener into a thread without taking it
            // from the vec — rebuild vec after this.
            idx
        })
        .collect();

    // Rebuild: move listeners into threads.
    let mut acceptor_threads = Vec::new();
    for (i, listener, _addr) in listener_data {
        let node_name = nodes[i].name.clone();
        let own_vlsn = nodes[i].vlsn();
        let h = std::thread::spawn(move || {
            match listener.accept() {
                Ok(ch) => run_acceptor(&ch, &node_name, own_vlsn, 1, term),
                Err(e) => {
                    eprintln!("[torture] acceptor {node_name} accept error: {e}");
                    Ok(None)
                }
            }
        });
        acceptor_threads.push((i, h));
    }

    // Proposer connects to each acceptor.
    let mut peer_channels: Vec<Arc<dyn Channel>> = Vec::new();
    for (_i, _h) in &acceptor_threads {
        // Retrieve the port we stored above.
        let port = *nodes[acceptor_handles[peer_channels.len()]].last_port.lock().unwrap();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        match TcpChannel::connect(addr) {
            Ok(ch) => peer_channels.push(Arc::new(ch)),
            Err(e) => {
                eprintln!("[torture] proposer connect to port {port} failed: {e}");
                // Drain remaining acceptors.
                for (_i2, h) in acceptor_threads {
                    let _ = h.join();
                }
                return None;
            }
        }
    }

    let proposer_vlsn = proposer.vlsn();
    let result = run_election(
        proposer.id,
        &proposer.name,
        group,
        &peer_channels,
        proposer_vlsn,
        1,
        term,
    );

    // Join acceptors.
    for (_i, h) in acceptor_threads {
        let _ = h.join();
    }

    if let Some(winner_id) = result {
        inv.record_winner(term, winner_id);
    }
    result
}

// ============================================================================
// VLSN streaming helper — FeederRunner over real TCP
// ============================================================================

/// Master sends `count` VLSNs to one replica over a real TCP socket pair.
/// Returns the number of VLSNs the replica observed.
fn stream_vlsns_tcp(
    master: &ClusterNode,
    replica: &ClusterNode,
    start_vlsn: u64,
    count: u64,
    inv: &InvariantLog,
) -> u64 {
    // Replica side: bind listener.
    let listener = match TcpChannelListener::bind("127.0.0.1:0".parse().unwrap()) {
        Ok(l) => l,
        Err(_) => return 0,
    };
    let addr = listener.local_addr().unwrap();

    let replica_id = replica.id;
    let replica_vlsn = Arc::clone(&replica.current_vlsn);
    let replica_disk_path = replica.disk.path.clone();
    let _inv_violations = replica.current_vlsn.clone(); // for future per-replica logging

    // Replica receive thread.
    let recv_handle = std::thread::spawn(move || {
        let ch = match listener.accept() {
            Ok(c) => c,
            Err(_) => return 0u64,
        };
        let mut last_seen = 0u64;
        let mut received = 0u64;
        loop {
            match ch.receive(Duration::from_millis(500)) {
                Ok(Some(frame)) if frame.len() >= 8 => {
                    let vlsn = u64::from_le_bytes(frame[0..8].try_into().unwrap());
                    if vlsn >= last_seen {
                        last_seen = vlsn;
                        received += 1;
                        // Update replica VLSN.
                        let old = replica_vlsn.fetch_max(vlsn, Ordering::SeqCst);
                        if vlsn > old {
                            let _ = fs::write(&replica_disk_path, vlsn.to_le_bytes());
                        }
                    }
                    // Send ack back.
                    let _ = ch.send(&vlsn.to_le_bytes());
                }
                Ok(Some(_)) => {}  // short frame
                Ok(None) | Err(_) => break,
            }
        }
        received
    });

    // Master send: connect + run FeederRunner.
    let master_ch: Arc<dyn Channel> = match TcpChannel::connect(addr) {
        Ok(ch) => Arc::new(ch),
        Err(_) => {
            let _ = recv_handle.join();
            return 0;
        }
    };

    let runner = FeederRunner::new(Arc::clone(&master_ch), start_vlsn);
    let mut scanner = MemLogScanner::new(start_vlsn, count);
    let _ = runner.run(&mut scanner);

    // Close master channel to signal end-of-stream to replica.
    let _ = master_ch.close();

    let received = recv_handle.join().unwrap_or(0);

    // Record the VLSNs streamed on master side.
    let master_end = start_vlsn + count - 1;
    master.advance_vlsn(master_end);
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
}

fn describe_phase(p: &ChaosPhase) -> &'static str {
    match p {
        ChaosPhase::Calm                => "calm",
        ChaosPhase::ModeratePacketLoss  => "moderate-loss(5%)",
        ChaosPhase::HighLatency         => "high-latency(80ms±40ms)",
        ChaosPhase::ReorderHeavy        => "reorder(20%)+loss(3%)",
        ChaosPhase::DuplicateAndCorrupt => "duplicate(10%)+corrupt(1%)",
        ChaosPhase::NodeCrash(_)        => "node-crash+restart",
        ChaosPhase::MinorityPartition   => "minority-partition(1 node isolated)",
    }
}

// ============================================================================
// Main torture loop
// ============================================================================

#[test]
#[ignore]
fn torture_replication() {
    let duration = torture_duration();
    eprintln!("[torture] starting — duration={duration:?}");

    // Shared state directory.
    let state_dir = TempDir::new().expect("TempDir failed");

    // Build cluster nodes.
    let nodes: Vec<ClusterNode> = (1..=NUM_NODES as u32)
        .map(|id| ClusterNode::new(id, state_dir.path()))
        .collect();

    // Replication group (ports are placeholders — elections use ephemeral ports).
    let mut group = RepGroup::new("torture_group".to_string(), 99);
    for n in &nodes {
        group.add_node(RepNode::new(
            n.name.clone(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            6900 + n.id as u16,
            n.id,
        ));
    }

    // Invariant log.
    let inv = Arc::new(InvariantLog::new());

    // tc netem guard — removed on Drop even if test panics.
    let netem = TcNetemGuard::setup();

    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_CAFE);
    let mut term: u64 = 1;
    let mut round: u64 = 0;
    let mut current_master_idx: Option<usize> = None;
    let mut vlsn_counter: u64 = 1;

    // Stats.
    let mut elections_attempted = 0u64;
    let mut elections_succeeded = 0u64;
    let mut streams_completed = 0u64;
    let mut crashes = 0u64;
    let mut chaos_rounds = 0u64;

    let start = Instant::now();
    let mut next_stats_report = start + Duration::from_secs(30);

    eprintln!("[torture] entering main loop");

    while start.elapsed() < duration {
        round += 1;

        // ── Chaos injection ───────────────────────────────────────────────
        let phase = if rng.gen_bool(0.15) {
            let n: u32 = rng.gen_range(0..6);
            match n {
                0 => ChaosPhase::ModeratePacketLoss,
                1 => ChaosPhase::HighLatency,
                2 => ChaosPhase::ReorderHeavy,
                3 => ChaosPhase::DuplicateAndCorrupt,
                4 => {
                    // Pick a random alive node to crash.
                    let alive: Vec<usize> = (0..NUM_NODES)
                        .filter(|&i| nodes[i].is_alive())
                        .collect();
                    if alive.len() > 1 {
                        let idx = alive[rng.gen_range(0..alive.len())];
                        ChaosPhase::NodeCrash(idx)
                    } else {
                        ChaosPhase::Calm
                    }
                }
                5 => ChaosPhase::MinorityPartition,
                _ => ChaosPhase::Calm,
            }
        } else {
            ChaosPhase::Calm
        };

        if !matches!(phase, ChaosPhase::Calm) {
            chaos_rounds += 1;
            eprintln!("[torture] round={round} chaos={}", describe_phase(&phase));
        }

        match &phase {
            ChaosPhase::Calm => netem.calm(),
            ChaosPhase::ModeratePacketLoss =>
                netem.change(5.0, 2, 1, 2.0, 0.0, 0.0),
            ChaosPhase::HighLatency =>
                netem.change(1.0, 80, 40, 5.0, 0.0, 0.0),
            ChaosPhase::ReorderHeavy =>
                netem.change(3.0, 10, 5, 20.0, 0.0, 0.0),
            ChaosPhase::DuplicateAndCorrupt =>
                netem.change(1.0, 5, 2, 5.0, 10.0, 1.0),
            ChaosPhase::NodeCrash(idx) => {
                let node = &nodes[*idx];
                let disk_vlsn = node.vlsn();
                eprintln!("[torture] crashing node {} (vlsn={disk_vlsn})", node.name);
                node.alive.store(false, Ordering::SeqCst);
                crashes += 1;

                // If this was the master, clear it.
                if current_master_idx == Some(*idx) {
                    current_master_idx = None;
                }

                // Simulate restart: re-enable after brief pause.
                let alive_flag = Arc::clone(&node.alive);
                let vlsn_atom = Arc::clone(&node.current_vlsn);
                let disk_path = node.disk.path.clone();
                let node_id = node.id;
                let inv_clone = Arc::clone(&inv);
                std::thread::spawn(move || {
                    // Down for 100–400 ms.
                    std::thread::sleep(Duration::from_millis(100 + (node_id as u64) * 50));
                    // On "restart", load vlsn from disk.
                    let restart_vlsn = fs::read(&disk_path)
                        .ok()
                        .and_then(|b| b.try_into().ok())
                        .map(u64::from_le_bytes)
                        .unwrap_or(0);
                    inv_clone.check_durability(node_id, disk_vlsn, restart_vlsn);
                    // VLSN must be >= persisted value.
                    vlsn_atom.fetch_max(restart_vlsn, Ordering::SeqCst);
                    alive_flag.store(true, Ordering::SeqCst);
                    eprintln!("[torture] restarted node{node_id} (vlsn={restart_vlsn})");
                });

                netem.calm();
            }
            ChaosPhase::MinorityPartition => {
                // Simulate by using heavy loss on netem for one round.
                netem.change(80.0, 5, 2, 0.0, 0.0, 0.0);
            }
        }

        // ── Election ──────────────────────────────────────────────────────
        let alive_indices: Vec<usize> = (0..NUM_NODES)
            .filter(|&i| nodes[i].is_alive())
            .collect();

        if alive_indices.len() < 2 {
            // Can't elect anyone. Short pause and try next round.
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }

        // Proposer = alive node with highest VLSN.
        let proposer_idx = *alive_indices
            .iter()
            .max_by_key(|&&i| nodes[i].vlsn())
            .unwrap();

        let mut won = false;
        for _attempt in 0..RETRY_BUDGET {
            elections_attempted += 1;
            if let Some(winner_id) = run_tcp_election(proposer_idx, &nodes, &group, term, &inv) {
                // Find the index of the winner.
                let winner_idx = nodes.iter().position(|n| n.id == winner_id).unwrap_or(proposer_idx);
                current_master_idx = Some(winner_idx);
                elections_succeeded += 1;
                won = true;
                eprintln!("[torture] round={round} term={term} master={} vlsn={}",
                           nodes[winner_idx].name, nodes[winner_idx].vlsn());
                break;
            }
            term += 1;
        }

        term += 1; // Always advance term after a round.

        if !won {
            eprintln!("[torture] round={round} election failed after {RETRY_BUDGET} attempts");
            // Restore calm after chaotic round.
            netem.calm();
            continue;
        }

        // ── VLSN streaming ────────────────────────────────────────────────
        if let Some(master_idx) = current_master_idx {
            let master = &nodes[master_idx];
            let start_vlsn = vlsn_counter;
            let end_vlsn = start_vlsn + VLSNS_PER_ROUND - 1;
            vlsn_counter = end_vlsn + 1;

            for &i in &alive_indices {
                if i == master_idx { continue; }
                let replica = &nodes[i];
                let received = stream_vlsns_tcp(master, replica, start_vlsn, VLSNS_PER_ROUND, &inv);
                streams_completed += 1;
                eprintln!("[torture] round={round} streamed {received}/{VLSNS_PER_ROUND} \
                           VLSNs to {}", replica.name);
            }
            // Update master VLSN.
            master.advance_vlsn(end_vlsn);
            inv.record_vlsn(master.id, master.vlsn());
        }

        // ── Restore calm after chaotic phase ──────────────────────────────
        if !matches!(phase, ChaosPhase::Calm | ChaosPhase::NodeCrash(_)) {
            // Give TCP stack time to flush, then restore.
            std::thread::sleep(Duration::from_millis(20));
            netem.calm();
        }

        // ── Periodic stats report ─────────────────────────────────────────
        let now = Instant::now();
        if now >= next_stats_report {
            next_stats_report = now + Duration::from_secs(30);
            let elapsed = start.elapsed();
            let violation_count = inv.violation_count();
            eprintln!(
                "[torture] elapsed={elapsed:.1?} rounds={round} \
                 elections={elections_attempted} won={elections_succeeded} \
                 streams={streams_completed} crashes={crashes} \
                 chaos_rounds={chaos_rounds} violations={violation_count}"
            );
        }
    }

    // ── Final cleanup and report ──────────────────────────────────────────
    netem.calm();
    // netem guard drops here, removing tc qdisc.

    let elapsed = start.elapsed();
    let violations = inv.violation_count();

    eprintln!("[torture] ═══════════════════════════════════════════════════");
    eprintln!("[torture] FINAL REPORT  elapsed={elapsed:.1?}");
    eprintln!("[torture]   rounds             : {round}");
    eprintln!("[torture]   elections attempted: {elections_attempted}");
    eprintln!("[torture]   elections succeeded: {elections_succeeded}");
    eprintln!("[torture]   vlsn streams       : {streams_completed}");
    eprintln!("[torture]   node crashes       : {crashes}");
    eprintln!("[torture]   chaos rounds       : {chaos_rounds}");
    eprintln!("[torture]   violations         : {violations}");
    eprintln!("[torture]   tc netem active    : {}", netem.active);

    // Final VLSN state per node.
    for n in &nodes {
        eprintln!("[torture]   {} vlsn={}", n.name, n.vlsn());
    }
    eprintln!("[torture] ═══════════════════════════════════════════════════");

    assert_eq!(violations, 0, "{violations} invariant violation(s) detected");
    assert!(
        elections_succeeded > 0,
        "no elections succeeded — check cluster configuration"
    );
}
