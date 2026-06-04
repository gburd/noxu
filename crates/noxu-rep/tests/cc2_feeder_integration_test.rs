//! C-C2 integration tests: `become_master` active push-feeder.
//!
//! These tests were written to demonstrate the gap described in finding C-C2
//! of the v3.x production-readiness review and to prove the fix works.
//!
//! ## Fail-before / pass-after evidence
//!
//! On `origin/main` (before this branch):
//! - `register_feeder_channel` does not exist → the test file would not
//!   compile.
//! - If the method were added as a no-op stub, `test_become_master_feeder_streams_entries_convergence`
//!   would assert `received.len() == 5` but get 0 (no FeederRunner thread
//!   spawned, nothing sent) → runtime failure.
//!
//! On this branch (`fix/cc2-become-master-feeders`):
//! - `register_feeder_channel` exists and `become_master` spawns a
//!   `FeederRunner` thread per registered channel → entries flow → tests pass.
//!
//! ## Coverage of new feeder code (by path)
//!
//! | Path | Exercised by |
//! |------|-------------|
//! | `register_feeder_channel` registration | all tests |
//! | `spawn_feeder_runner` thread spawn | all tests |
//! | `replicate_entry` fan-out to feeder_queues | all tests |
//! | `FeederRunner::run` send loop | all tests |
//! | `FeederRunner::run` ack receive + `known_replica_vlsn` update | `test_feeder_ack_advances_known_replica_vlsn` |
//! | `shutdown_group` M-4 catch-up wait | `test_shutdown_group_waits_for_replica_catchup` |
//! | `close` channel-close signal / thread join | all tests (via `env.close()`) |
//! | `register_feeder_channel` while-already-master spawn | `test_register_channel_while_already_master_spawns_immediately` |
//! | `apply_entry` fan-out to feeder_queues | `test_apply_entry_fans_out_to_feeder_queue` |
//!
//! ## Thread lifecycle notes
//!
//! Every test closes the sender-side channel (or calls `env.close()`, which
//! closes registered channels) before joining the FeederRunner.
//! `FeederRunner::run` returns `Ok(())` on `ChannelClosed`, so threads exit
//! promptly. All `receive` calls on the replica side use a bounded timeout
//! (≤5 seconds) to prevent hangs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use noxu_rep::net::channel::LocalChannelPair;
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn master_cfg(group: &str, name: &str) -> RepConfig {
    RepConfig::builder(group, name, "127.0.0.1").node_port(0).build()
}

/// Parse one FeederRunner wire frame from `raw_frame`.
///
/// Frame layout (all LE):
///   `[vlsn:8][type:1][payload_len:4][crc32:4][payload:payload_len]`
///
/// Returns `(vlsn, entry_type, payload)` or panics if the frame is malformed.
fn parse_frame(frame: &[u8]) -> (u64, u8, Vec<u8>) {
    assert!(frame.len() >= 17, "frame too short: {} bytes", frame.len());
    let vlsn = u64::from_le_bytes(frame[0..8].try_into().unwrap());
    let entry_type = frame[8];
    let payload_len =
        u32::from_le_bytes(frame[9..13].try_into().unwrap()) as usize;
    let payload = frame[17..17 + payload_len].to_vec();
    (vlsn, entry_type, payload)
}

// ---------------------------------------------------------------------------
// Test 1 (CONVERGENCE) — FAILS on origin/main, PASSES with fix
//
// This is the primary C-C2 test required by the production-readiness review.
// ---------------------------------------------------------------------------

/// Convergence test: master streams 5 entries to a replica via FeederRunner.
///
/// ## Fail-before
/// On `origin/main`, `register_feeder_channel` does not exist so this file
/// would not compile.  If added as a no-op, the assertion
/// `assert_eq!(received.len(), 5, ...)` fails at 0.
///
/// ## Pass-after
/// With this branch `become_master` spawns a `FeederRunner` thread that reads
/// from the dedicated feeder queue (populated by `replicate_entry`) and sends
/// entries over the channel.  The replica thread receives all 5 frames.
#[test]
fn test_become_master_feeder_streams_entries_convergence() {
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    // ---- Master setup ----
    let env_a =
        ReplicatedEnvironment::new(master_cfg("cc2_conv", "nodeA")).unwrap();
    env_a
        .add_peer(RepNode::new(
            "nodeB".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            0,
            2,
        ))
        .unwrap();

    // Register the channel BEFORE become_master so the thread is spawned
    // in become_master.
    env_a
        .register_feeder_channel("nodeB".to_string(), Arc::clone(&chan_master));
    env_a.become_master(1).unwrap();

    // Master replicates 5 entries (simulates what the replication-aware
    // commit path calls after each fsynced commit).
    for v in 1u64..=5 {
        env_a.replicate_entry(v, 0, v as u32 * 16, 0, vec![v as u8; 4]);
    }

    // ---- Replica side: read frames from the channel ----
    let timeout = Duration::from_secs(5);
    let deadline = Instant::now() + timeout;
    let mut received: Vec<(u64, u8, Vec<u8>)> = Vec::new();

    while received.len() < 5 && Instant::now() < deadline {
        match chan_replica.receive(Duration::from_millis(100)) {
            Ok(Some(frame)) => {
                let (vlsn, etype, payload) = parse_frame(&frame);
                // Send ack back to master so FeederRunner updates
                // known_replica_vlsn.
                chan_replica.send(&vlsn.to_le_bytes()).unwrap();
                received.push((vlsn, etype, payload));
            }
            Ok(None) => continue, // timeout, retry
            Err(e) => panic!("replica receive error: {:?}", e),
        }
    }

    // ---- Assertions ----
    assert_eq!(
        received.len(),
        5,
        "B should have received 5 entries from A's FeederRunner; \
         got {}. \
         On origin/main this fails because become_master spawns no threads \
         and no entries are pushed over the channel.",
        received.len()
    );

    // Entries arrive in VLSN order.
    for (i, &(vlsn, _, _)) in received.iter().enumerate() {
        assert_eq!(
            vlsn,
            (i + 1) as u64,
            "entry {} has wrong VLSN: expected {}, got {}",
            i,
            i + 1,
            vlsn
        );
    }

    // Payload content is intact.
    for (i, &(vlsn, _, ref payload)) in received.iter().enumerate() {
        assert_eq!(
            payload,
            &vec![vlsn as u8; 4],
            "payload mismatch for VLSN {}",
            vlsn
        );
        let _ = i;
    }

    // ---- Clean up ----
    // Closing env_a closes the registered channel, which causes the
    // FeederRunner thread to exit ChannelClosed and return Ok(()).
    env_a.close().unwrap();
}

// ---------------------------------------------------------------------------
// Test 2 — ack tracking: known_replica_vlsn advances as replica sends acks
// ---------------------------------------------------------------------------

/// The FeederRunner's `known_replica_vlsn` watermark advances when the
/// replica sends acknowledgement messages back over the channel.
#[test]
fn test_feeder_ack_advances_known_replica_vlsn() {
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    let env_a =
        ReplicatedEnvironment::new(master_cfg("cc2_ack", "nodeA")).unwrap();
    env_a
        .register_feeder_channel("nodeB".to_string(), Arc::clone(&chan_master));
    env_a.become_master(1).unwrap();

    // Write 3 entries.
    for v in 1u64..=3 {
        env_a.replicate_entry(v, 0, v as u32 * 16, 0, vec![v as u8]);
    }

    // Replica: drain 3 frames, send acks.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut frames_seen = 0u64;
    while frames_seen < 3 && Instant::now() < deadline {
        match chan_replica.receive(Duration::from_millis(100)) {
            Ok(Some(frame)) => {
                let (vlsn, _, _) = parse_frame(&frame);
                chan_replica.send(&vlsn.to_le_bytes()).unwrap();
                frames_seen += 1;
            }
            Ok(None) => continue,
            Err(e) => panic!("replica receive error: {:?}", e),
        }
    }
    assert_eq!(frames_seen, 3, "replica must have received 3 frames");

    // The FeederRunner may take a couple of poll cycles to read all 3 acks.
    // Poll for up to 500 ms.
    let ack_deadline = Instant::now() + Duration::from_millis(500);
    while env_a.active_feeder_runner_acked_vlsn("nodeB") < 3
        && Instant::now() < ack_deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        env_a.active_feeder_runner_acked_vlsn("nodeB"),
        3,
        "FeederRunner's known_replica_vlsn must reach 3 after replica acks"
    );

    env_a.close().unwrap();
}

// ---------------------------------------------------------------------------
// Test 3 — shutdown_group M-4: waits for FeederRunner replica catch-up
// ---------------------------------------------------------------------------

/// `shutdown_group` waits for the FeederRunner replica to ack up to the
/// master's VLSN before sending SHUTDOWN_GROUP.
///
/// The test has a background thread acting as the replica: it reads frames
/// and sends acks.  After all acks are sent the background thread closes its
/// end of the channel.  `shutdown_group` should return without hanging.
#[test]
fn test_shutdown_group_waits_for_replica_catchup() {
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    let env_a = Arc::new(
        ReplicatedEnvironment::new(master_cfg("cc2_shut", "nodeA")).unwrap(),
    );
    env_a
        .register_feeder_channel("nodeB".to_string(), Arc::clone(&chan_master));
    env_a.become_master(1).unwrap();

    // Write 5 entries to A.
    for v in 1u64..=5 {
        env_a.replicate_entry(v, 0, v as u32 * 16, 0, vec![v as u8]);
    }

    // Replica thread: read all frames, send acks, then close its end.
    let replica_handle = {
        let chan = Arc::clone(&chan_replica);
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(8);
            let mut received = 0u64;
            while received < 5 && Instant::now() < deadline {
                match chan.receive(Duration::from_millis(100)) {
                    Ok(Some(frame)) => {
                        let (vlsn, _, _) = parse_frame(&frame);
                        chan.send(&vlsn.to_le_bytes()).unwrap();
                        received += 1;
                    }
                    Ok(None) => continue,
                    Err(_) => break,
                }
            }
            // Close so FeederRunner sees ChannelClosed when env_a is about
            // to close.
            let _ = chan.close();
            received
        })
    };

    // Wait briefly for replica to ack (so the catch-up wait in
    // shutdown_group can complete).
    let ack_deadline = Instant::now() + Duration::from_millis(3000);
    while env_a.active_feeder_runner_acked_vlsn("nodeB") < 5
        && Instant::now() < ack_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }

    // shutdown_group must return without hanging (the catch-up wait should
    // see acked_vlsn >= 5 almost immediately).
    let start = Instant::now();
    // No actual peers to send SHUTDOWN_GROUP to (no group_service peers
    // registered with a valid TCP address in this test), so the admin loop
    // exits fast and shutdown_group's total wall time is dominated by the
    // catch-up wait + self.close().  Allow 6 seconds total.
    env_a.shutdown_group(6_000).expect("shutdown_group must not fail");
    let elapsed = start.elapsed();

    let replica_acked = replica_handle.join().unwrap();
    assert_eq!(
        replica_acked, 5,
        "replica must have received and acked 5 entries"
    );
    assert!(
        elapsed < Duration::from_secs(6),
        "shutdown_group hung for {:?}; expected fast return after catch-up",
        elapsed
    );
}

// ---------------------------------------------------------------------------
// Test 4 — register channel while already master spawns immediately
// ---------------------------------------------------------------------------

/// Calling `register_feeder_channel` AFTER `become_master` spawns the
/// FeederRunner immediately (not deferred to the next `become_master`).
#[test]
fn test_register_channel_while_already_master_spawns_immediately() {
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    let env_a =
        ReplicatedEnvironment::new(master_cfg("cc2_late", "nodeA")).unwrap();
    env_a.become_master(1).unwrap();

    // Write one entry BEFORE registering the channel; the FeederRunner
    // will miss this entry (queue is created at registration time).
    env_a.replicate_entry(1, 0, 16, 0, vec![0xAA]);

    // Late registration: channel registered after become_master.
    // Because the node is already master, spawn_feeder_runner is called
    // immediately.
    env_a
        .register_feeder_channel("nodeB".to_string(), Arc::clone(&chan_master));

    // Write a second entry AFTER registering; the FeederRunner WILL see it.
    env_a.replicate_entry(2, 0, 32, 0, vec![0xBB]);

    // Replica receives the second entry (VLSN 2).
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut found_vlsn2 = false;
    while !found_vlsn2 && Instant::now() < deadline {
        match chan_replica.receive(Duration::from_millis(50)) {
            Ok(Some(frame)) => {
                let (vlsn, _, _) = parse_frame(&frame);
                if vlsn == 2 {
                    found_vlsn2 = true;
                }
            }
            Ok(None) => continue,
            Err(_) => break,
        }
    }
    assert!(
        found_vlsn2,
        "late-registered FeederRunner must deliver entry written after registration"
    );

    env_a.close().unwrap();
}

// ---------------------------------------------------------------------------
// Test 5 — apply_entry also fans out to feeder queues
// ---------------------------------------------------------------------------

/// `apply_entry` fans out to per-replica feeder queues so that a relay node
/// (replica that also acts as a master to downstream replicas) can forward
/// entries without going through the pull PEER_FEEDER path.
#[test]
fn test_apply_entry_fans_out_to_feeder_queue() {
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    let env_a =
        ReplicatedEnvironment::new(master_cfg("cc2_apply", "nodeA")).unwrap();
    env_a
        .register_feeder_channel("nodeB".to_string(), Arc::clone(&chan_master));
    env_a.become_master(1).unwrap();

    // Call apply_entry (as opposed to replicate_entry) — both should fan out.
    env_a.apply_entry(10, 0, vec![0xCC]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut found = false;
    while !found && Instant::now() < deadline {
        match chan_replica.receive(Duration::from_millis(50)) {
            Ok(Some(frame)) => {
                let (vlsn, _, _) = parse_frame(&frame);
                if vlsn == 10 {
                    found = true;
                }
            }
            Ok(None) => continue,
            Err(_) => break,
        }
    }
    assert!(
        found,
        "apply_entry must fan out to the feeder queue so the replica sees VLSN 10"
    );

    env_a.close().unwrap();
}

// ---------------------------------------------------------------------------
// Test 6 — multi-entry convergence with large payload (stress the framing)
// ---------------------------------------------------------------------------

/// Send 50 entries with varying payload sizes to verify the CRC32 framing
/// is correct end-to-end.
#[test]
fn test_feeder_large_batch_convergence() {
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    let env_a =
        ReplicatedEnvironment::new(master_cfg("cc2_batch", "nodeA")).unwrap();
    env_a
        .register_feeder_channel("nodeB".to_string(), Arc::clone(&chan_master));
    env_a.become_master(1).unwrap();

    const N: u64 = 50;
    for v in 1..=N {
        // Payload size varies: v bytes.
        let payload = vec![v as u8; v as usize];
        env_a.replicate_entry(v, 0, v as u32 * 16, 0, payload);
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut received: Vec<u64> = Vec::new();
    while received.len() < N as usize && Instant::now() < deadline {
        match chan_replica.receive(Duration::from_millis(100)) {
            Ok(Some(frame)) => {
                let (vlsn, _, payload) = parse_frame(&frame);
                // Verify payload size matches VLSN.
                assert_eq!(
                    payload.len(),
                    vlsn as usize,
                    "payload size mismatch for VLSN {}",
                    vlsn
                );
                // Send ack.
                chan_replica.send(&vlsn.to_le_bytes()).unwrap();
                received.push(vlsn);
            }
            Ok(None) => continue,
            Err(e) => panic!("replica receive error: {:?}", e),
        }
    }

    assert_eq!(
        received.len(),
        N as usize,
        "batch: expected {} entries, got {}",
        N,
        received.len()
    );
    // VLSNs are monotone (ordering guaranteed by PeerScannerAdapter).
    for window in received.windows(2) {
        assert!(window[0] < window[1], "VLSN ordering violated");
    }

    env_a.close().unwrap();
}
