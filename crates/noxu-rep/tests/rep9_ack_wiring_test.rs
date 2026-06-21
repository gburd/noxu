//! REP-9 (HIGH): ack-durability quorum wired end-to-end.
//!
//! The quorum MATH was correct (electable-only, D6) but the ack path had
//! TWO DISCONNECTED HALVES with MISMATCHED KEY SPACES:
//!
//!   1. `await_replica_acks` registered/waited on a synthetic `commit_seq`
//!      (1,2,3...) — a standalone AtomicU64.
//!   2. Inbound replica acks arrive keyed by VLSN. The production
//!      `FeederRunner::run` stored each ack in its OWN private
//!      `known_replica_vlsn` mutex and NEVER called `env.record_ack`, so the
//!      AckTracker — and `update_dtvlsn_from_feeders` (which reads
//!      `Feeder::acked_vlsn`) — never saw production acks.
//!
//! Net on `origin/main`: every SIMPLE_MAJORITY / ALL commit blocked the full
//! `ack_timeout` then returned `InsufficientReplicas{available:0}` even when
//! every replica had acked, AND the DTVLSN election-ranking degraded to
//! VLSN-only because it never advanced.
//!
//! ## Fail-before / pass-after
//!
//! On `origin/main`:
//! - `rep9_simple_majority_commit_succeeds_via_production_feeder` blocks the
//!   full timeout then the wait returns `Timeout` (the synthetic commit_seq
//!   key never matches the VLSN-keyed acks, and acks never reach the tracker).
//! - `rep9_dtvlsn_advances_after_production_acks` sees DTVLSN stuck at 0.
//!
//! On this branch (`fix/ws-h1-rep9-ack-wiring`):
//! - the commit wait returns `Ok` promptly once a majority of electable
//!   replicas ack via the production feeder path.
//! - DTVLSN advances to the acked VLSN.
//!
//! JE refs: `FeederTxns` (per-txn ack tracking, `setupForAcks`/
//! `noteReplicaAck`), `FeederManager.getNumCurrentAckFeeders(commitVLSN)`
//! (counts qualifying feeders whose `getReplicaTxnEndVLSN() >= commitVLSN`),
//! `Durability.ReplicaAckPolicy.{ALL,SIMPLE_MAJORITY,NONE}`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use noxu_dbi::{ReplicaAckCoordinator, ReplicaAckPolicyKind};
use noxu_rep::net::channel::LocalChannelPair;
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};

fn master_cfg(group: &str, name: &str) -> RepConfig {
    RepConfig::builder(group, name, "127.0.0.1")
        .node_port(0)
        .node_type(NodeType::Electable)
        .build()
}

/// Spawn a replica reader thread that drains feeder frames and acks each
/// frame's VLSN back over the channel — exactly what the real
/// `ReplicaStream` does (`vlsn.to_le_bytes()` per applied frame). Returns a
/// handle whose join value is the highest VLSN it acked.
fn spawn_replica_acker(
    chan: Arc<dyn noxu_rep::net::Channel>,
    up_to: u64,
    deadline: Instant,
) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || {
        let mut high = 0u64;
        while high < up_to && Instant::now() < deadline {
            match chan.receive(Duration::from_millis(50)) {
                Ok(Some(frame)) => {
                    if frame.len() >= 8 {
                        let vlsn =
                            u64::from_le_bytes(frame[0..8].try_into().unwrap());
                        chan.send(&vlsn.to_le_bytes()).unwrap();
                        if vlsn > high {
                            high = vlsn;
                        }
                    }
                }
                Ok(None) => continue,
                Err(_) => break,
            }
        }
        high
    })
}

/// PRIMARY REP-9 test: a master commit with SIMPLE_MAJORITY durability and a
/// majority of ELECTABLE replicas acking via the PRODUCTION feeder path
/// returns SUCCESS promptly.
///
/// 3 electable nodes (master + 2 peers) → SimpleMajority needs 1 peer ack.
/// One peer acks via the real FeederRunner loop.
///
/// FAILS on `origin/main`: the wait blocks the full timeout then returns
/// Timeout (synthetic commit_seq vs VLSN key mismatch + acks never reach the
/// tracker). PASSES here.
#[test]
fn rep9_simple_majority_commit_succeeds_via_production_feeder() {
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    let env = Arc::new(
        ReplicatedEnvironment::new(master_cfg("rep9_maj", "master")).unwrap(),
    );
    // 2 electable peers → electable_count = 3 → SimpleMajority needs 1 ack.
    for i in 1..=2 {
        env.add_peer(RepNode::new(
            format!("peer{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            6_000 + i as u16,
            10 + i,
        ))
        .unwrap();
    }
    // Wire peer1's feeder channel (production path).
    env.init_self_weak();
    env.register_feeder_channel("peer1".to_string(), Arc::clone(&chan_master));
    env.become_master(1).unwrap();

    // Master "commits" three replicated entries: VLSN 1,2,3 stream to peer1.
    for v in 1u64..=3 {
        env.replicate_entry(v, 0, v as u32 * 16, 0, vec![v as u8]);
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let replica = spawn_replica_acker(Arc::clone(&chan_replica), 3, deadline);

    // The commit-blocking gate. With the wiring fixed this returns Ok once
    // peer1's ack for the commit VLSN reaches the tracker via record_ack.
    let started = Instant::now();
    let res = env.await_replica_acks(
        ReplicaAckPolicyKind::SimpleMajority,
        deadline - Instant::now(),
    );
    let elapsed = started.elapsed();

    let acked_high = replica.join().unwrap();
    assert_eq!(acked_high, 3, "replica should have acked up to VLSN 3");

    assert!(
        res.is_ok(),
        "SIMPLE_MAJORITY commit must SUCCEED once a majority of electable \
         replicas ack via the production feeder; got {res:?} after {elapsed:?}",
    );
    assert!(
        elapsed < Duration::from_secs(4),
        "commit must return promptly after acks, not block the full timeout; \
         waited {elapsed:?}",
    );

    env.close().unwrap();
}

/// `ReplicaAckPolicy::None` still short-circuits to Ok immediately.
#[test]
fn rep9_none_policy_short_circuits() {
    let env = Arc::new(
        ReplicatedEnvironment::new(master_cfg("rep9_none", "master")).unwrap(),
    );
    env.become_master(1).unwrap();

    let started = Instant::now();
    let res = env.await_replica_acks(
        ReplicaAckPolicyKind::None,
        Duration::from_secs(60),
    );
    let elapsed = started.elapsed();

    assert!(res.is_ok(), "None policy must succeed");
    assert!(
        elapsed < Duration::from_millis(50),
        "None policy must not block; waited {elapsed:?}",
    );
    env.close().unwrap();
}

/// DTVLSN must advance after production acks (the second, election-ranking
/// half of REP-9). On `origin/main` the FeederRunner never updated
/// `Feeder::acked_vlsn`, so `update_dtvlsn_from_feeders` saw nothing and the
/// DTVLSN stayed at 0.
///
/// 3 electable nodes → SimpleMajority durable-ack-count = floor(3/2) = 1
/// qualifying peer holding the VLSN advances the DTVLSN.
#[test]
fn rep9_dtvlsn_advances_after_production_acks() {
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    let env = Arc::new(
        ReplicatedEnvironment::new(master_cfg("rep9_dtvlsn", "master"))
            .unwrap(),
    );
    for i in 1..=2 {
        env.add_peer(RepNode::new(
            format!("peer{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            6_100 + i as u16,
            20 + i,
        ))
        .unwrap();
    }
    env.init_self_weak();
    env.register_feeder_channel("peer1".to_string(), Arc::clone(&chan_master));
    env.become_master(1).unwrap();

    assert_eq!(env.get_dtvlsn(), 0, "DTVLSN starts at 0");

    for v in 1u64..=5 {
        env.replicate_entry(v, 0, v as u32 * 16, 0, vec![v as u8]);
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let replica = spawn_replica_acker(Arc::clone(&chan_replica), 5, deadline);
    let acked_high = replica.join().unwrap();
    assert_eq!(acked_high, 5, "replica should have acked up to VLSN 5");

    // Give the FeederRunner a few poll cycles to drain all acks and forward
    // them to record_ack → update_dtvlsn_from_feeders.
    let dt_deadline = Instant::now() + Duration::from_secs(2);
    while env.get_dtvlsn() < 5 && Instant::now() < dt_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        env.get_dtvlsn(),
        5,
        "DTVLSN must advance to 5 once a majority of electable replicas ack \
         via the production feeder path; stuck at {} means the FeederRunner \
         never forwarded acks to record_ack / the Feeder::acked_vlsn",
        env.get_dtvlsn(),
    );

    env.close().unwrap();
}
