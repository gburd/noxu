//! B5 / V16 / F1: `transfer_master` must NOT hand off mastership to a
//! target replica that has not caught up to the master's current VLSN.
//!
//! JE parity: `MasterTransfer` tracks each ready replica's VLSN progress
//! (`MasterTransfer.java:254-283`, `VLSNProgress`) and only completes the
//! transfer to a replica that has *caught up* to the master's commit VLSN.
//! Noxu's sibling `shutdown_group` already implements the same catch-up wait
//! (`replicated_environment.rs`, M-4).  This suite pins that `transfer_master`
//! reuses that mechanism.
//!
//! ## Fail-before / pass-after evidence
//!
//! On base `b4cd7f7d`, `transfer_master` is "best-effort": it locates the
//! target, bumps the term, sends `TRANSFER_MASTER`, tells peers, and demotes
//! itself — with NO wait for the target's VLSN to reach the master's.  So
//! `transfer_master_refuses_lagging_target` FAILS: the transfer hands off
//! immediately to a replica that has only acked VLSN 2 while the master is at
//! VLSN 5, demoting the old master and losing its 3 most-recent commits.
//!
//! With the fix, `transfer_master` waits (bounded by `config.timeout`) for the
//! target's acked VLSN to reach the master's; if it does not, the transfer is
//! REFUSED (returns `Err`) and the old master stays master.
//!
//! The harness is deterministic: the replica side controls exactly which
//! VLSNs it acks, so the target's observed catch-up VLSN never depends on
//! timing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use noxu_rep::net::channel::LocalChannelPair;
use noxu_rep::{
    NodeType, RepConfig, RepNode, ReplicatedEnvironment,
    master_transfer::MasterTransferConfig,
};
use tempfile::TempDir;

fn config(name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder("g1", name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home)
        .build()
}

/// Build a `ReplicatedEnvironment` with the ADMIN service registered but
/// WITHOUT the election driver (matches `group_admin_test.rs::admin_env`).
fn admin_env(
    name: &str,
    env_home: &std::path::Path,
) -> Arc<ReplicatedEnvironment> {
    let env =
        Arc::new(ReplicatedEnvironment::new(config(name, env_home)).unwrap());
    env.register_admin_service();
    env
}

/// Parse one FeederRunner wire frame; returns the VLSN.
///
/// Frame layout (all LE):
///   `[vlsn:8][type:1][payload_len:4][crc32:4][payload:payload_len]`
fn frame_vlsn(frame: &[u8]) -> u64 {
    assert!(frame.len() >= 17, "frame too short: {} bytes", frame.len());
    u64::from_le_bytes(frame[0..8].try_into().unwrap())
}

/// Drive the master to VLSN 5 with a live FeederRunner for `target`, and let
/// the replica side ack up to `ack_upto` (inclusive), then stop acking.
///
/// Returns the `(master_env, target_env, replica_channel, dirs)`. The replica
/// channel is kept open (a background thread drains frames and acks up to the
/// watermark) so the FeederRunner stays alive.
struct Harness {
    master_env: Arc<ReplicatedEnvironment>,
    target_env: Arc<ReplicatedEnvironment>,
    _dir_master: TempDir,
    _dir_target: TempDir,
    _drain: std::thread::JoinHandle<()>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

fn setup(ack_upto: u64) -> Harness {
    let dir_master = TempDir::new().unwrap();
    let dir_target = TempDir::new().unwrap();

    let master_env = admin_env("master", dir_master.path());
    let master_addr = master_env.bound_addr().expect("master must bind");

    let target_env = admin_env("target", dir_target.path());
    let target_addr = target_env.bound_addr().expect("target must bind");

    // Wire the peers both ways.
    master_env
        .add_peer(RepNode::new(
            "target".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            target_addr.port(),
            2,
        ))
        .unwrap();
    target_env
        .add_peer(RepNode::new(
            "master".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            master_addr.port(),
            1,
        ))
        .unwrap();

    // In-memory feeder channel so the target's acked VLSN is observable via
    // `active_feeder_runner_acked_vlsn("target")`.
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    master_env.register_feeder_channel(
        "target".to_string(),
        Arc::clone(&chan_master),
    );
    master_env.become_master(1).unwrap();

    // Master replicates 5 entries (advances master VLSN to 5 and streams
    // frames to the target's FeederRunner).
    for v in 1u64..=5 {
        master_env.replicate_entry(v, 0, v as u32 * 16, 0, vec![v as u8; 4]);
    }

    // Background replica drain: ack every frame with VLSN <= ack_upto, drop
    // the rest. Deterministically caps the target's observed catch-up VLSN at
    // `ack_upto`.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let drain = std::thread::spawn(move || {
        while !stop_c.load(std::sync::atomic::Ordering::Relaxed) {
            match chan_replica.receive(Duration::from_millis(50)) {
                Ok(Some(frame)) => {
                    let vlsn = frame_vlsn(&frame);
                    if vlsn <= ack_upto {
                        let _ = chan_replica.send(&vlsn.to_le_bytes());
                    }
                }
                Ok(None) => continue,
                Err(_) => break,
            }
        }
    });

    // Wait until the target's acked VLSN has settled at `ack_upto` (bounded).
    let deadline = Instant::now() + Duration::from_secs(5);
    while master_env.active_feeder_runner_acked_vlsn("target") < ack_upto
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        master_env.active_feeder_runner_acked_vlsn("target"),
        ack_upto,
        "target's observed acked VLSN must settle at {ack_upto}"
    );
    assert_eq!(master_env.get_current_vlsn(), 5, "master must be at VLSN 5");

    Harness {
        master_env,
        target_env,
        _dir_master: dir_master,
        _dir_target: dir_target,
        _drain: drain,
        stop,
    }
}

impl Harness {
    fn teardown(self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = Arc::clone(&self.master_env).close();
        let _ = Arc::clone(&self.target_env).close();
    }
}

/// LAGGING target: acked VLSN 2, master at VLSN 5. `transfer_master` with a
/// bounded timeout must REFUSE (Err) and the old master must stay master.
///
/// FAILS on base b4cd7f7d: the best-effort transfer hands off immediately,
/// demoting the old master and promoting a target missing commits 3,4,5.
///
/// JE: `MasterTransferTest.testLateJoiner` (transfer to a not-yet-caught-up /
/// down target fails) and `MasterTransferTest.testAnotherTryAfterFailure`
/// (the refused transfer leaves the old master master; a later attempt to a
/// caught-up target then succeeds). The refuse half is here; the succeed half
/// is `transfer_master_succeeds_when_target_caught_up` below.
#[test]
fn transfer_master_refuses_lagging_target() {
    let h = setup(2);

    // Short timeout: the target will never catch up (drain stops acking past
    // VLSN 2), so the transfer must abort.
    let cfg = MasterTransferConfig::new(
        "target".to_string(),
        Duration::from_millis(500),
    );
    let res = Arc::clone(&h.master_env).transfer_master(cfg);

    assert!(
        res.is_err(),
        "transfer_master to a LAGGING target (acked 2 < master 5) must be \
         REFUSED, not handed off — otherwise commits 3,4,5 are lost. \
         On base b4cd7f7d this returns Ok (best-effort hand-off)."
    );
    assert!(
        h.master_env.is_master(),
        "old master must REMAIN master after a refused transfer (got {:?})",
        h.master_env.get_state()
    );

    h.teardown();
}

/// CAUGHT-UP target: acked VLSN 5 == master VLSN 5. `transfer_master` must
/// still succeed promptly (no regression).
///
/// JE: `MasterTransferTest.testConcurrentTxns` / `testAnotherTryAfterFailure`
/// (success once the target has caught up): JE drives phase-1/phase-2
/// completion by acks from the replica reaching the master's VLSN; here the
/// target's acked VLSN deterministically equals the master's, so the transfer
/// completes.
#[test]
fn transfer_master_succeeds_when_target_caught_up() {
    let h = setup(5);

    let cfg =
        MasterTransferConfig::new("target".to_string(), Duration::from_secs(5));
    let start = Instant::now();
    let res = Arc::clone(&h.master_env).transfer_master(cfg);
    let elapsed = start.elapsed();

    assert!(
        res.is_ok(),
        "transfer_master to a CAUGHT-UP target (acked 5 == master 5) must \
         succeed: {res:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "caught-up transfer must complete promptly (took {elapsed:?})"
    );
    assert!(
        h.master_env.is_replica(),
        "old master must be a replica after a successful transfer (got {:?})",
        h.master_env.get_state()
    );

    h.teardown();
}

/// JE: `MasterTransferTest.testConcurrentTxns` / `MasterTransferTest.testPhase2Timeout`.
///
/// B5/V16/F1 phase-2 (JE `MasterTransfer` phase-2 commit block): a master that
/// is ACTIVELY COMMITTING during the transfer window must not lose an
/// acknowledged commit.
///
/// This pins the residual window the review found: the phase-1 catch-up wait
/// compares the target against a ONE-TIME snapshot of the master VLSN, and
/// (before the fix) nothing froze new commits between that check and the
/// hand-off. So a commit that lands after the check advances the master past
/// the VLSN the target was confirmed to cover, and the target is handed
/// mastership already behind again -- the same loss shape, narrower.
///
/// Scenario: target is caught up to VLSN 5 (== master), phase-1 passes. A
/// concurrent writer commits VLSN 6 during the transfer window. The target
/// never acks past 5 (the drain stops at 5).
///
/// * On base c4478496 (no commit freeze): `replicate_entry(6)` advances the
///   master to 6 immediately; the transfer -- having only checked the stale
///   snapshot 5 -- hands off anyway, demoting the old master while the target
///   became master having acked only 5. The master`s VLSN at hand-off was 6
///   with the target at 5: commit 6 is lost. This test FAILS on base.
/// * With the fix (JE phase-2 freeze): `replicate_entry(6)` PARKS on the
///   transfer-scoped commit block until the transfer resolves, so the master`s
///   observable tail stays 5 through the re-confirm and hand-off. The transfer
///   re-confirms the target covers the master`s final VLSN (5) and hands off
///   cleanly; commit 6 only advances the tail AFTER the node is already a
///   replica -- it was never handed off as the master`s acknowledged tail.
///
/// The invariant asserted (independent of interleaving): if the transfer
/// succeeded (old master demoted), then the master`s VLSN observed at the
/// instant the transfer returned must NOT exceed the VLSN the target was
/// confirmed to cover (5). On base the observed VLSN is 6 > 5 -> FAIL.
#[test]
fn transfer_master_freezes_commits_during_handoff_window() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let h = setup(5);

    // A concurrent writer that advances the master VLSN to 6 during the
    // transfer window. It is armed just before the transfer starts and fires
    // immediately, so it races into the check->hand-off window. On the fixed
    // build `replicate_entry` parks on the commit freeze; on base it advances
    // the master to 6 with nothing stopping it.
    let master = Arc::clone(&h.master_env);
    let start_writer = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_advanced_to = Arc::new(AtomicU64::new(0));
    let sw = Arc::clone(&start_writer);
    let wa = Arc::clone(&writer_advanced_to);
    let writer = std::thread::spawn(move || {
        while !sw.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        // Post-check commit: advances (or tries to advance) the master to 6.
        // On the fixed build this call blocks on the transfer commit freeze
        // until the transfer resolves.
        master.replicate_entry(6, 0, 6 * 16, 0, vec![6u8; 4]);
        wa.store(master.get_current_vlsn(), Ordering::Release);
    });

    // Fire the writer and immediately start the transfer so the commit races
    // into the hand-off window. A generous timeout: the caught-up target
    // (acked 5) passes phase-1 promptly.
    start_writer.store(true, Ordering::Release);
    let cfg =
        MasterTransferConfig::new("target".to_string(), Duration::from_secs(5));
    let res = Arc::clone(&h.master_env).transfer_master(cfg);

    // Snapshot the master VLSN the instant the transfer returned.
    let vlsn_at_return = h.master_env.get_current_vlsn();

    // If the transfer handed off (Ok + demoted), the target only ever acked 5,
    // so the master`s tail at hand-off must not have exceeded 5 -- otherwise a
    // committed entry (6) was handed off missing.
    if res.is_ok() {
        assert!(
            h.master_env.is_replica(),
            "Ok transfer must have demoted the old master (got {:?})",
            h.master_env.get_state()
        );
        assert!(
            vlsn_at_return <= 5,
            "DATA LOSS: transfer handed off with master VLSN {vlsn_at_return} \
             > target`s confirmed VLSN 5. A commit that landed after the \
             catch-up check was handed off missing (target became master \
             having acked only 5). The phase-2 commit freeze must prevent a \
             post-check commit from advancing the handed-off tail."
        );
    } else {
        // A refused transfer is also acceptable (old master keeps its tail and
        // stays master) -- no acknowledged data is lost either way.
        assert!(
            h.master_env.is_master(),
            "a refused transfer must leave the old master as master"
        );
    }

    // Let the (possibly parked) writer finish before teardown.
    let _ = writer.join();
    let _ = writer_advanced_to.load(Ordering::Acquire);

    h.teardown();
}
