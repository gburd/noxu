//! Replica group scale stress test.
//!
//! Answers: "how many replicas can participate in a group before we hit
//! fundamental resource limits?"  Three measurement phases, all using
//! in-memory `LocalChannelPair` channels so TCP/socket limits and OS
//! networking overhead are not the bottleneck.
//!
//! ## Phases
//!
//! ### Phase A — RepNode / RepGroup memory overhead
//! Create N `RepNode` objects inside a `RepGroup`. Snapshot `/proc/self/status`
//! VmRSS at power-of-two milestones to compute per-node heap cost.
//!
//! ### Phase B — Election latency vs group size
//! Run one full Paxos round (`run_election`) against N–1 in-memory acceptor
//! threads. Measure wall-clock time per election.
//! Sizes: 3, 7, 15, 31, 63, 127, 255, 511, 1023.
//!
//! ### Phase C — Channel throughput vs replica count
//! One master thread sends 100 message payloads to each of N replicas
//! sequentially using `LocalChannel::send()`, then closes each channel.
//! Replica threads drain concurrently.  Measures total wall-clock time
//! from the first send until the last replica finishes.
//! Sizes: 1, 10, 100, 500, 1_000, 5_000, 10_000.
//!
//! ## Running
//!
//! ```bash
//! cargo test -p noxu-rep --test replica_scale_test -- --ignored --nocapture
//! ```
//!
//! ## Interpreting results
//!
//! * Phase A prints `per-node bytes` — multiply by your target N to estimate
//!   heap budget.
//! * Phase B prints `election ms` — grows O(N) because Paxos is sequential
//!   over N peers. A sub-linear blip means the quorum was satisfied early.
//! * Phase C prints `stream ms` — measures the wall-clock time for the master
//!   to push all entries to all N replicas in parallel.  Should stay nearly
//!   flat until memory pressure causes swap or allocation failures.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    dead_code,
    unused_imports
)]

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use noxu_rep::elections::{run_acceptor, run_election, NodeId};
use noxu_rep::net::{Channel, LocalChannelPair};
use noxu_rep::{NodeType, RepGroup, RepNode};

// ── helpers ───────────────────────────────────────────────────────────────────

/// Read VmRSS from `/proc/self/status` in kilobytes.
fn rss_kb() -> u64 {
    let text = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            return kb;
        }
    }
    0
}

/// Build a `RepGroup` containing `n` electable nodes.
fn make_group(n: usize) -> RepGroup {
    let mut g = RepGroup::new("scalegroup".into(), 1);
    for i in 1..=(n as u32) {
        g.add_node(RepNode::new(
            format!("node{i}"),
            NodeType::Electable,
            "127.0.0.1".into(),
            6000 + i as u16,
            i,
        ));
    }
    g
}

// ── Phase A ───────────────────────────────────────────────────────────────────

fn phase_a_memory() {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  Phase A — RepNode / RepGroup memory overhead           ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  {:>10}  {:>12}  {:>14}  {:>14}", "nodes", "rss_kb", "delta_kb", "bytes/node");

    let milestones: &[usize] = &[
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024,
        2_048, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072,
        262_144, 524_288, 1_048_576,
    ];

    let mut prev_rss = rss_kb();
    let mut prev_n = 0usize;

    // Keep the group alive so the allocator does not reclaim pages.
    let mut group = RepGroup::new("scalegroup".into(), 1);
    let mut reached_limit = false;

    for &n in milestones {
        // Add nodes incrementally from prev_n to n.
        for i in (prev_n as u32 + 1)..=(n as u32) {
            group.add_node(RepNode::new(
                format!("node{i}"),
                NodeType::Electable,
                "127.0.0.1".into(),
                6000 + (i % 60000) as u16,
                i,
            ));
        }

        let rss = rss_kb();
        let delta = rss.saturating_sub(prev_rss);
        let added = (n - prev_n) as u64;
        let bytes_per_node = (delta * 1024).checked_div(added).unwrap_or(0);

        println!("  {:>10}  {:>12}  {:>14}  {:>14}", n, rss, delta, bytes_per_node);

        // Stop if we are using more than 4 GB RSS.
        if rss > 4 * 1024 * 1024 {
            println!("  [phase-a] 4 GB RSS limit reached at n={n}, stopping.");
            reached_limit = true;
            break;
        }

        prev_rss = rss;
        prev_n = n;
    }

    if !reached_limit {
        println!("  [phase-a] Completed all milestones up to {} nodes.", milestones.last().unwrap());
    }

    println!("  [phase-a] Final group size: {}", group.node_count());
    drop(group);
}

// ── Phase B ───────────────────────────────────────────────────────────────────

fn phase_b_election(n_total: usize) -> Option<Duration> {
    let n_peers = n_total - 1;
    let group = make_group(n_total);

    let pairs: Vec<LocalChannelPair> = (0..n_peers).map(|_| LocalChannelPair::new()).collect();

    let mut proposer_channels: Vec<Arc<dyn Channel>> = Vec::with_capacity(n_peers);
    let mut acceptor_handles = Vec::with_capacity(n_peers);

    for pair in pairs {
        let ch_a: Arc<dyn Channel> = Arc::new(pair.channel_a);
        let ch_b: Arc<dyn Channel> = Arc::new(pair.channel_b);
        proposer_channels.push(ch_a);

        acceptor_handles.push(std::thread::spawn(move || {
            run_acceptor(&*ch_b, "peer", 50, 1, 1).unwrap_or(None)
        }));
    }

    let t0 = Instant::now();
    let winner = run_election(
        1,
        "node1",
        &group,
        &proposer_channels,
        100, // vlsn (higher than peers so node1 wins)
        1,   // priority
        1,   // term
    );
    let elapsed = t0.elapsed();

    for h in acceptor_handles {
        let _ = h.join();
    }

    if winner.is_some() { Some(elapsed) } else { None }
}

fn run_phase_b() {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  Phase B — Election latency vs group size               ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  {:>8}  {:>12}  {:>12}", "nodes", "elapsed_ms", "result");

    let sizes: &[usize] = &[3, 7, 15, 31, 63, 127, 255, 511, 1023];

    for &n in sizes {
        let result = phase_b_election(n);
        match result {
            Some(d) => println!("  {:>8}  {:>12.2}  {:>12}", n, d.as_secs_f64() * 1000.0, "WIN"),
            None    => println!("  {:>8}  {:>12}  {:>12}", n, "—", "FAIL"),
        }
    }
}

// ── Phase C ───────────────────────────────────────────────────────────────────

const PHASE_C_MSGS: u64 = 100;
// 8-byte payload per message — keeps total memory << 1 MB even at 10K replicas.
const PHASE_C_PAYLOAD: &[u8; 8] = b"noxuscal";

/// Push PHASE_C_MSGS messages to each of `n_replicas` via LocalChannel, then
/// close each send-side.  Replica threads drain concurrently.
///
/// Returns `(elapsed_ms, all_ok)`.
fn phase_c_stream(n_replicas: usize) -> (f64, bool) {
    let barrier = Arc::new(Barrier::new(n_replicas + 1));
    let mut replica_handles = Vec::with_capacity(n_replicas);

    // channel_a → master (sender); channel_b → replica thread (receiver).
    let mut master_channels: Vec<Arc<dyn Channel>> = Vec::with_capacity(n_replicas);

    for _ in 0..n_replicas {
        let pair = LocalChannelPair::new();
        master_channels.push(Arc::new(pair.channel_a));

        let ch_b = Arc::new(pair.channel_b) as Arc<dyn Channel>;
        let barrier_c = Arc::clone(&barrier);

        let handle = std::thread::spawn(move || {
            let mut received: u64 = 0;
            barrier_c.wait();

            loop {
                match ch_b.receive(Duration::from_millis(200)) {
                    Ok(Some(_)) => {
                        received += 1;
                        if received >= PHASE_C_MSGS {
                            break;
                        }
                    }
                    Ok(None) => continue,
                    Err(_)   => break, // channel closed by master
                }
            }
            received
        });

        replica_handles.push(handle);
    }

    // Release all replica threads, then push messages to each in turn.
    barrier.wait();
    let t0 = Instant::now();

    for ch in &master_channels {
        for _ in 0..PHASE_C_MSGS {
            let _ = ch.send(PHASE_C_PAYLOAD);
        }
        let _ = ch.close();
    }

    let mut all_ok = true;
    for h in replica_handles {
        let got = h.join().unwrap_or(0);
        if got < PHASE_C_MSGS {
            all_ok = false;
        }
    }

    (t0.elapsed().as_secs_f64() * 1000.0, all_ok)
}

fn run_phase_c() {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  Phase C — Channel throughput vs replica count          ║");
    println!("║  (sequential master push, parallel replica drain)       ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  {:>10}  {:>14}  {:>16}  {:>8}", "replicas", "elapsed_ms", "msgs_total", "ok");

    let sizes: &[usize] = &[1, 10, 100, 500, 1_000, 5_000, 10_000];

    for &n in sizes {
        let (ms, ok) = phase_c_stream(n);
        let total_msgs = n as u64 * PHASE_C_MSGS;
        println!(
            "  {:>10}  {:>14.2}  {:>16}  {:>8}",
            n, ms, total_msgs,
            if ok { "PASS" } else { "FAIL" },
        );

        if !ok {
            println!("  [phase-c] Stopping at n={n} — some replicas missed messages.");
            break;
        }
    }
}

// ── Top-level test ────────────────────────────────────────────────────────────

/// Replica group scale test: measure RepNode memory, election latency, and
/// channel throughput as the group size grows.
///
/// Run with:
/// ```bash
/// cargo test -p noxu-rep --test replica_scale_test -- --ignored --nocapture
/// ```
#[test]
#[ignore = "long-running replica scale test; run explicitly with --ignored"]
fn replica_group_scale() {
    println!("\n═══════════════════════════════════════════════════════════");
    println!("  Noxu DB — Replica Group Scale Test");
    println!("  Host: {}", hostname());
    println!("  RSS baseline: {} KB", rss_kb());
    println!("═══════════════════════════════════════════════════════════");

    phase_a_memory();
    run_phase_b();
    run_phase_c();

    println!("\n═══════════════════════════════════════════════════════════");
    println!("  Scale test complete.");
    println!("═══════════════════════════════════════════════════════════");
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .unwrap_or_else(|_| "unknown".into())
        .trim()
        .to_string()
}

// ── Smoke test (runs in normal CI without --ignored) ─────────────────────────

/// Verify memory, election, and channel throughput all work at small scale.
#[test]
fn replica_scale_smoke() {
    // Phase A: 64 nodes should be cheap.
    let g = make_group(64);
    assert_eq!(g.node_count(), 64);
    drop(g);

    // Phase B: 3-node election completes quickly.
    let elapsed = phase_b_election(3).expect("3-node election should succeed");
    assert!(elapsed < Duration::from_secs(5), "election took too long: {elapsed:?}");

    // Phase C: 10 replicas, PHASE_C_MSGS messages each.
    let (ms, ok) = phase_c_stream(10);
    assert!(ok, "channel push to 10 replicas should succeed");
    assert!(ms < 5_000.0, "streaming to 10 replicas took too long: {ms:.0}ms");
}
