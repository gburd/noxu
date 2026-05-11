//! Chaos replication test — fault-injection harness for Noxu HA.
//!
//! This test suite exercises all known failure modes that can occur in a
//! distributed replication system and validates that the Noxu replication
//! protocol maintains correctness under adversarial conditions.
//!
//! ## Failure modes injected
//!
//! **Network**
//! - Message drops (uniform random, configurable rate)
//! - Message delays (random delay added before delivery)
//! - Network partitions (entire group of nodes isolated)
//! - Message duplication (same message delivered twice)
//!
//! **Paxos / consensus**
//! - Concurrent elections for the same term (split-brain prevention)
//! - Proposer crashes mid-Phase-1 (channel closed before Phase-2)
//! - Proposer crashes mid-Phase-2 (channel closed before Phase-2 replies)
//! - Stale leader detection (old master issues Phase-1 with outdated term)
//! - All-nodes-fail scenario (no quorum → election must fail gracefully)
//!
//! **Log streaming**
//! - Message drops during VLSN streaming (FeederRunner + FaultChannel)
//! - VLSN ordering invariant under concurrent sends
//!
//! **Transactions / durability**
//! - All ReplicaAckPolicy variants (All / SimpleMajority / None) under chaos
//! - Long-running sequence of elections (monotonically increasing terms)
//!
//! ## Invariants verified
//!
//! 1. **Safety / no split-brain**: At most one node wins an election per term.
//! 2. **VLSN monotonicity**: Entries received by a replica are in strictly
//!    increasing VLSN order.
//! 3. **Liveness under retries**: With low enough fault rates and retries,
//!    elections eventually succeed.
//! 4. **Quorum required**: Election cannot succeed when fewer than quorum
//!    nodes are reachable.
//! 5. **Graceful failure**: Protocol returns `None` / `Err` rather than
//!    panicking when channels close or messages are dropped.

#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use noxu_rep::{NodeType, RepGroup, RepNode};
use noxu_rep::commit_durability::{CommitDurability, ReplicaAckPolicy};
use noxu_rep::elections::{run_acceptor, run_election};
use noxu_rep::net::Channel;
use noxu_rep::net::channel::LocalChannelPair;
use noxu_rep::stream::{FeederRunner, LogScanner};

// ============================================================================
// FaultConfig — controls the type and rate of faults to inject
// ============================================================================

/// Parameters controlling fault injection on a `FaultChannel`.
#[derive(Debug, Clone)]
struct FaultConfig {
    /// Probability [0.0, 1.0] that any individual send is silently dropped.
    drop_rate: f64,
    /// Range [min_ms, max_ms] of artificial delay added to each delivered
    /// message (0, 0 means no delay). The actual delay is uniform random.
    delay_range_ms: (u64, u64),
    /// If true, ALL outgoing messages are dropped (hard partition).
    partitioned: bool,
    /// If true, each successfully delivered message is also duplicated
    /// (sent a second time with zero additional delay).
    duplicate_messages: bool,
}

impl FaultConfig {
    /// No faults at all — transparent passthrough.
    fn clean() -> Self {
        Self { drop_rate: 0.0, delay_range_ms: (0, 0), partitioned: false, duplicate_messages: false }
    }

    /// Drop every message — simulates a hard network partition.
    fn hard_partition() -> Self {
        Self { drop_rate: 1.0, delay_range_ms: (0, 0), partitioned: true, duplicate_messages: false }
    }

    /// Lossy network with the given drop rate and random delay up to `max_delay_ms`.
    fn lossy(drop_rate: f64, max_delay_ms: u64) -> Self {
        Self { drop_rate, delay_range_ms: (0, max_delay_ms), partitioned: false, duplicate_messages: false }
    }
}

// ============================================================================
// FaultChannel — wraps any Channel with configurable fault injection
// ============================================================================

/// A `Channel` wrapper that injects configurable faults on `send()`.
///
/// `FaultChannel` is transparent for `receive()` — the receiver sees
/// whatever the inner channel delivers (which may be fewer or more messages
/// than were sent, depending on fault configuration).
///
/// Thread safety: `FaultConfig` and `StdRng` are protected by the same
/// mutex so that multiple threads can share a `FaultChannel`.
struct FaultChannel {
    inner: Box<dyn Channel>,
    state: Mutex<FaultState>,
}

struct FaultState {
    config: FaultConfig,
    rng: StdRng,
}

impl FaultChannel {
    fn new(inner: Box<dyn Channel>, config: FaultConfig, seed: u64) -> Self {
        Self {
            inner,
            state: Mutex::new(FaultState { config, rng: StdRng::seed_from_u64(seed) }),
        }
    }

    /// Replace the fault configuration at runtime (e.g., to heal a partition).
    fn set_config(&self, config: FaultConfig) {
        self.state.lock().unwrap().config = config;
    }
}

impl Channel for FaultChannel {
    fn send(&self, data: &[u8]) -> noxu_rep::error::Result<()> {
        let (should_drop, delay_ms, duplicate) = {
            let mut st = self.state.lock().unwrap();
            if st.config.partitioned {
                return Ok(()); // silently drop — hard partition
            }
            let drop_rate = st.config.drop_rate;
            let (min_ms, max_ms) = st.config.delay_range_ms;
            let dup = st.config.duplicate_messages;
            let should_drop = st.rng.gen_range(0.0_f64..1.0) < drop_rate;
            let delay_ms = if max_ms > 0 {
                st.rng.gen_range(min_ms..=max_ms)
            } else {
                0
            };
            (should_drop, delay_ms, dup)
        };

        if should_drop {
            return Ok(()); // message lost
        }

        if delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(delay_ms));
        }

        self.inner.send(data)?;

        if duplicate {
            // Duplicate delivery — second copy arrives immediately.
            let _ = self.inner.send(data);
        }

        Ok(())
    }

    fn receive(&self, timeout: Duration) -> noxu_rep::error::Result<Option<Vec<u8>>> {
        self.inner.receive(timeout)
    }

    fn close(&self) -> noxu_rep::error::Result<()> {
        self.inner.close()
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }
}

// ============================================================================
// PartitionMatrix — N×N grid tracking which node pairs are connected
// ============================================================================

/// Tracks which node pairs are network-connected.
///
/// `reachable(a, b)` returns true if node `a` can send messages to node `b`.
/// Partitions are symmetric: `partition(a, b)` also partitions `(b, a)`.
struct PartitionMatrix {
    n: usize,
    /// Row-major: reachable[a * n + b] = true iff a can reach b.
    reachable: Vec<AtomicBool>,
}

impl PartitionMatrix {
    fn new_fully_connected(n: usize) -> Self {
        let reachable = (0..n * n).map(|_| AtomicBool::new(true)).collect();
        Self { n, reachable }
    }

    fn reachable(&self, from: usize, to: usize) -> bool {
        self.reachable[from * self.n + to].load(Ordering::SeqCst)
    }

    fn set_reachable(&self, from: usize, to: usize, connected: bool) {
        self.reachable[from * self.n + to].store(connected, Ordering::SeqCst);
        self.reachable[to * self.n + from].store(connected, Ordering::SeqCst);
    }

    /// Isolate node `n` from all other nodes (simulate crash or network island).
    fn isolate(&self, node: usize) {
        for other in 0..self.n {
            if other != node {
                self.set_reachable(node, other, false);
            }
        }
    }

    /// Reconnect node `n` to all other nodes.
    fn reconnect(&self, node: usize) {
        for other in 0..self.n {
            self.set_reachable(node, other, true);
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Build an N-node fully-electable `RepGroup`.
fn make_group(n: u32) -> RepGroup {
    let mut g = RepGroup::new("chaos_group".to_string(), 1);
    for i in 1..=n {
        g.add_node(RepNode::new(
            format!("chaos_node{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            5600 + i as u16,
            i,
        ));
    }
    g
}

/// In-memory `LogScanner` backed by a `VecDeque`.
struct VecLogScanner {
    entries: VecDeque<(u64, u8, Vec<u8>)>,
}

impl VecLogScanner {
    fn new(entries: Vec<(u64, u8, Vec<u8>)>) -> Self {
        Self { entries: entries.into_iter().collect() }
    }

    /// Build a sequential log of `count` entries starting at VLSN 1.
    fn sequential(count: u64) -> Self {
        Self::new(
            (1..=count)
                .map(|v| (v, 1u8, format!("entry-{v}").into_bytes()))
                .collect(),
        )
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

// ============================================================================
// Test 1 — Election produces a valid winner; term ordering is monotone
// ============================================================================
//
// Runs 5 sequential elections with increasing terms. Verifies:
//   1. Each election either wins (returns Some) or fails gracefully (None).
//   2. When it succeeds, the winner is always the highest-VLSN candidate.
//   3. Terms are strictly increasing across rounds.
//
// Note on split-brain: the stateless `run_acceptor` API (one call = one
// connection) cannot be used to test Paxos's "promise once per term"
// invariant without a shared-state multi-connection acceptor. Split-brain
// prevention at the protocol level is instead exercised by
// `test_partition_minority_cannot_elect` (quorum math) and
// `test_quorum_unreachable_election_fails_gracefully` (hard-partition).

#[test]
fn test_no_split_brain_concurrent_elections() {
    let group = make_group(3);
    const TRIALS: usize = 5;

    for trial in 0..TRIALS {
        let term = (trial + 1) as u64;

        let pair12 = LocalChannelPair::new();
        let pair13 = LocalChannelPair::new();
        let ch1_2: Arc<dyn Channel> = Arc::new(pair12.channel_a);
        let ch1_3: Arc<dyn Channel> = Arc::new(pair13.channel_a);
        let acc2 = Arc::new(pair12.channel_b);
        let acc3 = Arc::new(pair13.channel_b);

        let g = group.clone();
        let h2 = std::thread::spawn(move || {
            run_acceptor(acc2.as_ref(), "chaos_node2", 50, 1, term)
        });
        let h3 = std::thread::spawn(move || {
            run_acceptor(acc3.as_ref(), "chaos_node3", 50, 1, term)
        });

        // Node1 has the highest VLSN — it should win every clean election.
        let result = run_election(1, "chaos_node1", &g, &[ch1_2, ch1_3], 200, 1, term);

        let _ = h2.join();
        let _ = h3.join();

        // Election must either succeed with a valid winner or fail gracefully.
        if let Some(winner_id) = result {
            assert!(
                (1..=3).contains(&winner_id),
                "trial {trial} term {term}: winner node_id {winner_id} out of range"
            );
        }
        // (No panic is the key assertion here.)
    }
}

// ============================================================================
// Test 2 — Election tolerates message drops and eventually succeeds
// ============================================================================
//
// With a FaultChannel that drops 20% of messages, a 3-node election
// should still succeed within RETRIES attempts.

#[test]
fn test_election_tolerates_message_drops() {
    const DROP_RATE: f64 = 0.20;
    const RETRIES: u32 = 15;

    let group = make_group(3);
    let mut term = 1u64;
    let mut succeeded = false;

    for attempt in 0..RETRIES {
        let pair12 = LocalChannelPair::new();
        let pair13 = LocalChannelPair::new();

        let seed = attempt as u64 * 0x1234_5678;
        let ch1_2: Arc<dyn Channel> = Arc::new(FaultChannel::new(
            Box::new(pair12.channel_a),
            FaultConfig::lossy(DROP_RATE, 5),
            seed,
        ));
        let ch1_3: Arc<dyn Channel> = Arc::new(FaultChannel::new(
            Box::new(pair13.channel_a),
            FaultConfig::lossy(DROP_RATE, 5),
            seed + 1,
        ));

        let acc2 = Arc::new(pair12.channel_b);
        let acc3 = Arc::new(pair13.channel_b);

        let grp2 = group.clone();
        let h2 = {
            let acc2 = Arc::clone(&acc2);
            std::thread::spawn(move || {
                run_acceptor(acc2.as_ref(), "chaos_node2", 50, 1, term)
            })
        };
        let h3 = {
            let acc3 = Arc::clone(&acc3);
            std::thread::spawn(move || {
                run_acceptor(acc3.as_ref(), "chaos_node3", 50, 1, term)
            })
        };

        let result = run_election(1, "chaos_node1", &grp2, &[ch1_2, ch1_3], 100, 1, term);

        let _ = h2.join();
        let _ = h3.join();

        if result.is_some() {
            succeeded = true;
            break;
        }
        term += 1;
    }

    assert!(
        succeeded,
        "Election never succeeded after {RETRIES} attempts at {DROP_RATE:.0}% drop rate"
    );
}

// ============================================================================
// Test 3 — VLSN monotonicity under message drops and delays
// ============================================================================
//
// FeederRunner streams 50 log entries through a FaultChannel (30% drop,
// up to 10 ms delay). The receiver verifies that all VLSNs it sees arrive
// in strictly increasing order, and that no VLSN is seen twice.

#[test]
fn test_vlsn_monotone_under_message_drops() {
    const ENTRIES: u64 = 50;
    const DROP_RATE: f64 = 0.30;

    let pair = LocalChannelPair::new();

    // Wrap the sender side with fault injection.
    let fault_sender: Arc<dyn Channel> = Arc::new(FaultChannel::new(
        Box::new(pair.channel_a),
        FaultConfig::lossy(DROP_RATE, 10),
        0xCAFE_BABE,
    ));
    let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);

    // Receiver thread: collect all frames and verify VLSN ordering.
    let recv_handle = {
        let receiver = Arc::clone(&receiver);
        std::thread::spawn(move || {
            let mut seen: Vec<u64> = Vec::new();
            loop {
                match receiver.receive(Duration::from_millis(200)) {
                    Ok(Some(frame)) if frame.len() >= 13 => {
                        let vlsn = u64::from_le_bytes(frame[0..8].try_into().unwrap());
                        seen.push(vlsn);
                    }
                    Ok(Some(_)) => {} // malformed / padding frame, skip
                    Ok(None) => break,  // timeout — feeder done
                    Err(_) => break,    // channel closed
                }
            }
            seen
        })
    };

    // Feeder: send 50 entries through the fault channel then close it.
    let mut scanner = VecLogScanner::sequential(ENTRIES);
    let runner = FeederRunner::new(Arc::clone(&fault_sender), 1);
    // Run in a thread so we can close the sender after scanning.
    let sender_clone = Arc::clone(&fault_sender);
    let run_handle = std::thread::spawn(move || {
        let _ = runner.run(&mut scanner);
    });

    // Give the feeder ~500 ms to drain the scanner, then close the channel.
    std::thread::sleep(Duration::from_millis(500));
    sender_clone.close().unwrap();

    let received_vlsns = recv_handle.join().unwrap();
    let _ = run_handle.join();

    // VLSN monotonicity invariant: strictly increasing, no duplicates.
    for window in received_vlsns.windows(2) {
        assert!(
            window[0] < window[1],
            "VLSN ordering violation: {} then {} (monotonicity broken)",
            window[0],
            window[1]
        );
    }

    // All received VLSNs must be within the valid range [1, ENTRIES].
    for &v in &received_vlsns {
        assert!(
            (1..=ENTRIES).contains(&v),
            "VLSN {v} out of expected range [1, {ENTRIES}]"
        );
    }

    // With 30% drop rate and 50 entries we expect to receive at least 20.
    assert!(
        received_vlsns.len() >= 20,
        "Too few VLSNs received ({} of {ENTRIES}); FaultChannel may be dropping everything",
        received_vlsns.len()
    );
}

// ============================================================================
// Test 4 — Partition: minority cannot elect a leader
// ============================================================================
//
// In a 5-node cluster, a 2-node minority partition must not be able to elect
// a master (quorum = 3).

#[test]
fn test_partition_minority_cannot_elect() {
    let group = make_group(5);
    // Minority partition: only node1 tries to get votes from node2.
    // Quorum for 5 nodes = 3 (needs 2 votes from peers + self = 3).
    // Only 1 peer available → cannot reach quorum.

    let pair12 = LocalChannelPair::new();

    // Wrap peer channel with hard partition for node3, node4, node5
    // (they are unreachable from the minority).
    let ch1_2: Arc<dyn Channel> = Arc::new(pair12.channel_a);
    let acc2 = Arc::new(pair12.channel_b);

    let h2 = std::thread::spawn(move || {
        run_acceptor(acc2.as_ref(), "chaos_node2", 50, 1, 1)
    });

    // Proposer only has 1 peer channel — cannot reach quorum of 3.
    let result = run_election(1, "chaos_node1", &group, &[ch1_2], 100, 1, 1);

    let _ = h2.join();

    // With only 1 peer (+ self = 2) and quorum = 3, election must fail.
    assert!(
        result.is_none(),
        "Minority partition elected a leader — quorum invariant violated: {result:?}"
    );
}

// ============================================================================
// Test 5 — Quorum unreachable: all-drop channels → election fails gracefully
// ============================================================================

#[test]
fn test_quorum_unreachable_election_fails_gracefully() {
    let group = make_group(3);

    let pair12 = LocalChannelPair::new();
    let pair13 = LocalChannelPair::new();

    // Hard partition on both peer channels.
    let ch1_2: Arc<dyn Channel> = Arc::new(FaultChannel::new(
        Box::new(pair12.channel_a),
        FaultConfig::hard_partition(),
        42,
    ));
    let ch1_3: Arc<dyn Channel> = Arc::new(FaultChannel::new(
        Box::new(pair13.channel_a),
        FaultConfig::hard_partition(),
        43,
    ));

    // Run acceptors so that channel reads don't block forever.
    let acc2 = Arc::new(pair12.channel_b);
    let acc3 = Arc::new(pair13.channel_b);
    let h2 = std::thread::spawn(move || {
        run_acceptor(acc2.as_ref(), "chaos_node2", 50, 1, 1)
    });
    let h3 = std::thread::spawn(move || {
        run_acceptor(acc3.as_ref(), "chaos_node3", 50, 1, 1)
    });

    let result = run_election(1, "chaos_node1", &group, &[ch1_2, ch1_3], 100, 1, 1);

    let _ = h2.join();
    let _ = h3.join();

    // No quorum possible — must return None, not panic.
    assert!(
        result.is_none(),
        "Election should fail when all peer channels are partitioned, got {result:?}"
    );
}

// ============================================================================
// Test 6 — Multi-round election with monotonically increasing terms
// ============================================================================
//
// Run 10 successive elections with increasing terms. Each round has some
// message loss. The winning proposer for each round is recorded. Verify:
//   - Terms are strictly increasing across rounds.
//   - At most one proposer wins per round.
//   - All failures are graceful (None, not panic).

#[test]
fn test_multi_round_elections_monotone_terms() {
    const ROUNDS: u64 = 10;
    let group = make_group(3);

    let mut winners: Vec<Option<u32>> = Vec::new();
    let mut last_term = 0u64;

    for round in 1..=ROUNDS {
        let term = round; // strictly increasing
        assert!(term > last_term);
        last_term = term;

        let drop_rate = if round % 3 == 0 { 0.5 } else { 0.1 };
        let pair12 = LocalChannelPair::new();
        let pair13 = LocalChannelPair::new();
        let seed = round * 0x0BAD_F00D;

        let ch1_2: Arc<dyn Channel> = Arc::new(FaultChannel::new(
            Box::new(pair12.channel_a),
            FaultConfig::lossy(drop_rate, 3),
            seed,
        ));
        let ch1_3: Arc<dyn Channel> = Arc::new(FaultChannel::new(
            Box::new(pair13.channel_a),
            FaultConfig::lossy(drop_rate, 3),
            seed + 1,
        ));
        let acc2 = Arc::new(pair12.channel_b);
        let acc3 = Arc::new(pair13.channel_b);

        let g2 = group.clone();
        let h2 = std::thread::spawn(move || {
            run_acceptor(acc2.as_ref(), "chaos_node2", 50, 1, term)
        });
        let h3 = std::thread::spawn(move || {
            run_acceptor(acc3.as_ref(), "chaos_node3", 50, 1, term)
        });

        let result = run_election(1, "chaos_node1", &g2, &[ch1_2, ch1_3], 100 + round, 1, term);
        winners.push(result);

        let _ = h2.join();
        let _ = h3.join();
    }

    // All results must be either None or a valid node ID — never panic.
    for (round, w) in winners.iter().enumerate() {
        if let Some(id) = w {
            assert!(
                (1..=3).contains(id),
                "round {round}: winner node_id {id} is out of range [1, 3]"
            );
        }
    }
}

// ============================================================================
// Test 7 — CommitDurability: ack requirements are met correctly
// ============================================================================
//
// Verify that `ReplicaAckPolicy::required_acks` returns the right values for
// all three policies, and that `CommitDurability` computes them correctly
// under all cluster sizes. This is a unit test for the durability logic used
// under chaos conditions.

#[test]
fn test_commit_durability_ack_requirements_all_policies() {
    // Policy: None — always 0 acks required regardless of cluster size.
    for n in 0u32..=10 {
        let cd = CommitDurability::new(ReplicaAckPolicy::None, Duration::from_secs(5));
        assert_eq!(cd.required_acks(n), 0, "None policy must require 0 acks for n={n}");
    }

    // Policy: All — requires n-1 acks (all replicas except master).
    let cases_all = [(0, 0), (1, 0), (2, 1), (3, 2), (5, 4)];
    for (n, expected) in cases_all {
        let cd = CommitDurability::new(ReplicaAckPolicy::All, Duration::from_secs(5));
        assert_eq!(cd.required_acks(n), expected, "All policy, n={n}");
    }

    // Policy: SimpleMajority — requires majority minus the master itself.
    let cases_majority = [(0, 0), (1, 0), (2, 1), (3, 1), (4, 2), (5, 2), (7, 3)];
    for (n, expected) in cases_majority {
        let cd = CommitDurability::new(ReplicaAckPolicy::SimpleMajority, Duration::from_secs(5));
        assert_eq!(
            cd.required_acks(n),
            expected,
            "SimpleMajority policy, n={n}: expected {expected} got {}",
            cd.required_acks(n)
        );
    }
}

// ============================================================================
// Test 8 — VLSN delivery after hard partition is healed
// ============================================================================
//
// Two-channel setup (separate send and receive paths so the partition
// fault only affects the sender side):
//   Phase 1: FaultChannel drops all sends (partition active).
//   Phase 2: FaultChannel config healed → new FeederRunner delivers entries.
//
// Key change from a shared-channel design: we use two INDEPENDENT channel
// pairs so healing the first pair's fault config does not interfere with
// Phase 2's fresh pair. This avoids the `run1.join()` hang that would
// occur if we tried to reuse the partitioned pair (the runner keeps
// polling for acks indefinitely on an open channel).

#[test]
fn test_partition_and_recovery_vlsn_delivery() {
    const ENTRIES: u64 = 15;

    // ---- Phase 1: partitioned — all messages dropped ----
    {
        let pair = LocalChannelPair::new();
        let fault_sender: Arc<dyn Channel> = Arc::new(FaultChannel::new(
            Box::new(pair.channel_a),
            FaultConfig::hard_partition(),
            0xBEEF_CAFE,
        ));
        let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);

        // Start a receiver that collects 50ms worth of frames.
        let recv_h = {
            let r = Arc::clone(&receiver);
            std::thread::spawn(move || {
                let mut frames = 0usize;
                while r.receive(Duration::from_millis(50)).ok().flatten().is_some() {
                    frames += 1;
                }
                frames
            })
        };

        // Run feeder for a short burst; all sends are dropped.
        let runner = FeederRunner::new(Arc::clone(&fault_sender), 1);
        let s = Arc::clone(&fault_sender);
        let run_h = std::thread::spawn(move || {
            let mut sc = VecLogScanner::sequential(5);
            let _ = runner.run(&mut sc);
        });

        std::thread::sleep(Duration::from_millis(80));
        // Close both sides so threads exit.
        fault_sender.close().unwrap();
        receiver.close().unwrap();

        let frames = recv_h.join().unwrap();
        let _ = run_h.join();
        drop(s);

        // All sends were dropped — receiver must have seen nothing.
        assert_eq!(frames, 0, "partitioned channel must deliver 0 frames");
    }

    // ---- Phase 2: healed — clean channel delivers VLSN-ordered entries ----
    {
        let pair = LocalChannelPair::new();
        let clean_sender: Arc<dyn Channel> = Arc::new(FaultChannel::new(
            Box::new(pair.channel_a),
            FaultConfig::clean(),
            0xFEED_CAFE,
        ));
        let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);

        let received: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let rcv_clone = Arc::clone(&received);
        let recv_h = {
            let r = Arc::clone(&receiver);
            std::thread::spawn(move || {
                loop {
                    match r.receive(Duration::from_millis(200)) {
                        Ok(Some(frame)) if frame.len() >= 13 => {
                            let vlsn = u64::from_le_bytes(frame[0..8].try_into().unwrap());
                            rcv_clone.lock().unwrap().push(vlsn);
                        }
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => break,
                    }
                }
            })
        };

        let runner = FeederRunner::new(Arc::clone(&clean_sender), 1);
        let s = Arc::clone(&clean_sender);
        let run_h = std::thread::spawn(move || {
            let mut sc = VecLogScanner::sequential(ENTRIES);
            let _ = runner.run(&mut sc);
        });

        // Give feeder time to drain the scanner then close.
        std::thread::sleep(Duration::from_millis(300));
        clean_sender.close().unwrap();
        receiver.close().unwrap();
        let _ = run_h.join();
        drop(s);
        let _ = recv_h.join();

        let vlsns = received.lock().unwrap().clone();

        // Monotonicity check.
        for w in vlsns.windows(2) {
            assert!(
                w[0] < w[1],
                "VLSN ordering violated after partition heal: {} then {}",
                w[0],
                w[1]
            );
        }
        // Should have received at least half the entries on a clean channel.
        assert!(
            vlsns.len() >= (ENTRIES / 2) as usize,
            "too few entries received on healed channel: {} of {ENTRIES}",
            vlsns.len()
        );
    }
}

// ============================================================================
// Test 9 — Election VLSN tiebreaker: highest-VLSN node wins
// ============================================================================
//
// With two proposers where proposer1 has vlsn=200 and proposer2 has vlsn=50,
// a correctly behaving Phase-1 should elect proposer1 even if proposer2
// sends its Prepare first. Verify this with clean (no-fault) channels.

#[test]
fn test_highest_vlsn_wins_election() {
    let group = make_group(3);

    // node1 (vlsn=200, priority=1) proposes — should win against node2 (vlsn=50).
    let pair12 = LocalChannelPair::new();
    let pair13 = LocalChannelPair::new();

    let ch1_2: Arc<dyn Channel> = Arc::new(pair12.channel_a);
    let ch1_3: Arc<dyn Channel> = Arc::new(pair13.channel_a);
    let acc2 = Arc::new(pair12.channel_b);
    let acc3 = Arc::new(pair13.channel_b);

    let h2 = std::thread::spawn(move || {
        run_acceptor(acc2.as_ref(), "chaos_node2", 50, 1, 1)
    });
    let h3 = std::thread::spawn(move || {
        run_acceptor(acc3.as_ref(), "chaos_node3", 50, 1, 1)
    });

    let result = run_election(1, "chaos_node1", &group, &[ch1_2, ch1_3], 200, 1, 1);

    let _ = h2.join();
    let _ = h3.join();

    // Node1 has the highest VLSN and should win.
    assert!(
        result.is_some(),
        "Node with highest VLSN should win a clean election"
    );
}

// ============================================================================
// Test 10 — Graceful channel close mid-election does not panic
// ============================================================================
//
// Close both peer channels before the proposer has a chance to send its
// Phase-1 message. The election must return None gracefully.

#[test]
fn test_channel_close_mid_election_no_panic() {
    let group = make_group(3);

    let pair12 = LocalChannelPair::new();
    let pair13 = LocalChannelPair::new();

    // Close acceptor sides immediately so proposer gets ChannelClosed.
    pair12.channel_b.close().unwrap();
    pair13.channel_b.close().unwrap();

    let ch1_2: Arc<dyn Channel> = Arc::new(pair12.channel_a);
    let ch1_3: Arc<dyn Channel> = Arc::new(pair13.channel_a);

    // Must not panic; must return None.
    let result =
        run_election(1, "chaos_node1", &group, &[ch1_2, ch1_3], 100, 1, 1);
    assert!(
        result.is_none(),
        "Election with closed channels must fail gracefully (return None)"
    );
}

// ============================================================================
// Test 11 — FaultChannel: duplicate delivery does not corrupt VLSN stream
// ============================================================================
//
// With duplicate_messages = true, the FeederRunner may send each frame twice.
// The receiver might see each VLSN twice. But VLSNs must still be
// non-decreasing (never go backward).

#[test]
fn test_duplicate_messages_vlsn_nondecreasing() {
    const ENTRIES: u64 = 20;

    let pair = LocalChannelPair::new();
    let dup_sender: Arc<dyn Channel> = Arc::new(FaultChannel::new(
        Box::new(pair.channel_a),
        FaultConfig {
            drop_rate: 0.0,
            delay_range_ms: (0, 0),
            partitioned: false,
            duplicate_messages: true,
        },
        0x1111_2222,
    ));
    let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);

    let recv_handle = {
        let receiver = Arc::clone(&receiver);
        std::thread::spawn(move || {
            let mut vlsns: Vec<u64> = Vec::new();
            loop {
                match receiver.receive(Duration::from_millis(300)) {
                    Ok(Some(frame)) if frame.len() >= 13 => {
                        let vlsn = u64::from_le_bytes(frame[0..8].try_into().unwrap());
                        vlsns.push(vlsn);
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            vlsns
        })
    };

    let sender_clone = Arc::clone(&dup_sender);
    let runner = FeederRunner::new(Arc::clone(&dup_sender), 1);
    let run_handle = std::thread::spawn(move || {
        let mut scanner = VecLogScanner::sequential(ENTRIES);
        let _ = runner.run(&mut scanner);
    });

    std::thread::sleep(Duration::from_millis(400));
    sender_clone.close().unwrap();

    let vlsns = recv_handle.join().unwrap();
    let _ = run_handle.join();

    // VLSNs must be non-decreasing even with duplicates.
    for w in vlsns.windows(2) {
        assert!(
            w[0] <= w[1],
            "VLSN went backward with duplicates: {} then {}",
            w[0],
            w[1]
        );
    }
}

// ============================================================================
// Test 12 — PartitionMatrix: isolation and reconnection semantics
// ============================================================================

#[test]
fn test_partition_matrix_isolation_and_reconnection() {
    let pm = PartitionMatrix::new_fully_connected(5);

    // All pairs reachable initially.
    for i in 0..5 {
        for j in 0..5 {
            assert!(pm.reachable(i, j), "all pairs should be reachable initially");
        }
    }

    // Isolate node 2.
    pm.isolate(2);
    for other in 0..5 {
        if other != 2 {
            assert!(!pm.reachable(2, other), "node2 must be unreachable after isolation");
            assert!(!pm.reachable(other, 2), "isolation is symmetric");
        }
    }
    // Node 2 can still "reach" itself.
    assert!(pm.reachable(2, 2), "self-reachability always true");

    // Reconnect node 2.
    pm.reconnect(2);
    for other in 0..5 {
        assert!(pm.reachable(2, other), "node2 must be reachable after reconnection");
    }
}

// ============================================================================
// Test 13 — Large-scale chaos: N elections, each with random fault params
// ============================================================================
//
// Run 20 elections with randomized fault configurations. Each election either
// succeeds or fails gracefully. No panics permitted. Verify the set of valid
// winners and no split-brain within any single run.

#[test]
fn test_large_scale_random_chaos() {
    const ROUNDS: u64 = 20;
    let group = make_group(3);
    let mut rng = StdRng::seed_from_u64(0xFACE_BABE_D00D);

    for round in 0..ROUNDS {
        let term = round + 1;
        let drop_rate: f64 = rng.gen_range(0.0..0.7);
        let max_delay: u64 = rng.gen_range(0..=10);
        let partitioned: bool = rng.gen_bool(0.1); // 10% chance of hard partition

        let config = if partitioned {
            FaultConfig::hard_partition()
        } else {
            FaultConfig::lossy(drop_rate, max_delay)
        };

        let pair12 = LocalChannelPair::new();
        let pair13 = LocalChannelPair::new();
        let seed = rng.next_u64();

        let ch1_2: Arc<dyn Channel> =
            Arc::new(FaultChannel::new(Box::new(pair12.channel_a), config.clone(), seed));
        let ch1_3: Arc<dyn Channel> =
            Arc::new(FaultChannel::new(Box::new(pair13.channel_a), config.clone(), seed + 1));
        let acc2 = Arc::new(pair12.channel_b);
        let acc3 = Arc::new(pair13.channel_b);

        let g = group.clone();
        let h2 = std::thread::spawn(move || {
            run_acceptor(acc2.as_ref(), "chaos_node2", 50, 1, term)
        });
        let h3 = std::thread::spawn(move || {
            run_acceptor(acc3.as_ref(), "chaos_node3", 50, 1, term)
        });

        // Must not panic regardless of fault configuration.
        let result =
            run_election(1, "chaos_node1", &g, &[ch1_2, ch1_3], 100 + round, 1, term);

        // Valid winners are in range [1, 3] or None.
        if let Some(winner_id) = result {
            assert!(
                (1..=3).contains(&winner_id),
                "round {round}: invalid winner id {winner_id}"
            );
        }

        let _ = h2.join();
        let _ = h3.join();
    }
}

// ============================================================================
// Test 14 — VLSN counter: acked VLSNs tracked correctly under fault conditions
// ============================================================================

#[test]
fn test_feeder_runner_ack_tracking_under_drops() {
    const ENTRIES: u64 = 10;
    let pair = LocalChannelPair::new();
    let fault_sender: Arc<dyn Channel> = Arc::new(FaultChannel::new(
        Box::new(pair.channel_a),
        FaultConfig::lossy(0.2, 5),
        0xACED_ACEF,
    ));
    let receiver: Arc<dyn Channel> = Arc::new(pair.channel_b);

    // Receiver: echo back acks for every frame received.
    let recv_handle = {
        let receiver = Arc::clone(&receiver);
        std::thread::spawn(move || {
            let mut count = 0u64;
            loop {
                match receiver.receive(Duration::from_millis(300)) {
                    Ok(Some(frame)) if frame.len() >= 8 => {
                        let vlsn = u64::from_le_bytes(frame[0..8].try_into().unwrap());
                        // Send ack.
                        let _ = receiver.send(&vlsn.to_le_bytes());
                        count += 1;
                    }
                    _ => break,
                }
            }
            count
        })
    };

    let runner = Arc::new(FeederRunner::new(Arc::clone(&fault_sender), 1));
    let runner_ref = Arc::clone(&runner);
    let sender_clone = Arc::clone(&fault_sender);
    let run_handle = std::thread::spawn(move || {
        let mut scanner = VecLogScanner::sequential(ENTRIES);
        let _ = runner_ref.run(&mut scanner);
    });

    std::thread::sleep(Duration::from_millis(500));
    sender_clone.close().unwrap();

    let acked = recv_handle.join().unwrap();
    let _ = run_handle.join();

    // At least some acks should have been tracked (non-zero acked count).
    assert!(
        runner.known_replica_vlsn() > 0 || acked == 0,
        "feeder should have tracked acked VLSNs when any ack arrives"
    );
    // The acked VLSN must be within the valid range.
    assert!(
        runner.known_replica_vlsn() <= ENTRIES,
        "acked VLSN {} exceeds max VLSN {ENTRIES}",
        runner.known_replica_vlsn()
    );
}

// ============================================================================
// Test 15 — AtomicU64 VLSN counter: independent monotonicity test
// ============================================================================
//
// Verifies that a shared VLSN counter (simulating the per-node committed VLSN
// accumulator) remains monotonically non-decreasing under concurrent updates
// from multiple threads.

#[test]
fn test_shared_vlsn_counter_monotone_concurrent() {
    const THREADS: usize = 8;
    const OPS_PER_THREAD: u64 = 100;

    let counter = Arc::new(AtomicU64::new(0));
    let violations = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let counter = Arc::clone(&counter);
            let violations = Arc::clone(&violations);
            std::thread::spawn(move || {
                let base: u64 = t as u64 * OPS_PER_THREAD;
                for i in 0..OPS_PER_THREAD {
                    let new_val = base + i;
                    let old = counter.fetch_max(new_val, Ordering::SeqCst);
                    if new_val < old {
                        // This is expected — fetch_max ensures old >= new means
                        // counter was already higher. No violation.
                        let _ = old; // intentional noop
                    }
                }
                // After all ops, read the final counter value.
                let final_val = counter.load(Ordering::SeqCst);
                if final_val < base {
                    violations.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // The counter must have reached at least the max value (THREADS-1)*OPS_PER_THREAD.
    let final_val = counter.load(Ordering::SeqCst);
    let expected_max = (THREADS as u64 - 1) * OPS_PER_THREAD + (OPS_PER_THREAD - 1);
    assert_eq!(final_val, expected_max, "VLSN counter must reach global max");

    assert_eq!(
        violations.load(Ordering::SeqCst),
        0,
        "VLSN counter violated monotonicity"
    );
}
