//! `CommitFreezeLatch` wiring: VLSN must not advance mid-election.
//!
//! The latch itself is unit-tested in
//! `noxu_rep::elections::commit_freeze_latch`.  These tests cover the WIRING —
//! that the election path actually freezes and the replay path actually
//! observes the freeze — which is what was missing (the primitive existed but
//! was called from nowhere, so a node could keep advancing its commit VLSN
//! while an election it had promised in was still in flight).
//!
//! JE call sites mirrored:
//!   - freeze -> `MasterSuggestionGenerator.getRanking`
//!     (MasterSuggestionGenerator.java:63)
//!   - vlsn_event -> `MasterChangeListener.notify`
//!     (MasterChangeListener.java:49)
//!   - await_thaw   -> `Replay.replayEntry` (Replay.java:525)
//!   - clear_latch  -> `Replica.shutdown` (Replica.java:305)

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use noxu_dbi::EnvironmentImpl;
use noxu_rep::elections::PersistentAcceptorState;
use noxu_rep::elections::commit_freeze_latch::{
    CommitFreezeLatch, round_proposal,
};
use noxu_rep::elections::paxos::run_acceptor_with_state;
use noxu_rep::net::channel::{Channel, LocalChannelPair};
use noxu_rep::protocol::ProtocolMessage;
use noxu_rep::stream::{EnvironmentLogWriter, LogWriter};
use noxu_rep::vlsn::VlsnIndex;
use noxu_rep::{RepConfig, ReplicatedEnvironment};

/// `LogEntryType::TxnCommit` — the entry type whose replay advances the commit
/// VLSN and is therefore the one the freeze gates.
const TXN_COMMIT: u8 = 30;
/// `LogEntryType::InsertLN` — a non-commit entry; must NOT be gated.
const INSERT_LN: u8 = 10;

fn log_env() -> (tempfile::TempDir, Arc<noxu_log::LogManager>) {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env = Arc::new(
        EnvironmentImpl::new(tmp.path(), false, true).expect("EnvironmentImpl"),
    );
    let log_mgr = env.get_log_manager().expect("log_manager");
    (tmp, log_mgr)
}

fn send(ch: &dyn Channel, msg: &ProtocolMessage) {
    ch.send(&msg.encode()).expect("send");
}

fn recv(ch: &dyn Channel) -> ProtocolMessage {
    let bytes = ch
        .receive(Duration::from_secs(5))
        .expect("receive")
        .expect("peer closed unexpectedly");
    ProtocolMessage::decode(&bytes).expect("decode")
}

/// HEADLINE: a replayed commit does NOT advance the VLSN while an election
/// round this node promised in is unresolved, and DOES once it resolves.
///
/// Drives the real acceptor (`run_acceptor_with_state`) over an in-process
/// channel pair, so the freeze is installed by the production election path,
/// not by the test.
///
/// - FAILS before the wiring: the acceptor never froze and the writer never
///   consulted the latch, so the commit landed immediately (the assert that
///   the VLSN is still 0 mid-election fails).
/// - PASSES after: the commit is deferred until the ElectionResult arrives.
#[test]
fn test_replayed_commit_does_not_advance_vlsn_mid_election() {
    let (_tmp, log_mgr) = log_env();
    // Long timeout: this test must be decided by the election result, never by
    // the latch expiring.
    let latch =
        Arc::new(CommitFreezeLatch::with_timeout(Duration::from_secs(30)));
    let acc_state = Arc::new(PersistentAcceptorState::in_memory());
    let vlsn_index = Arc::new(VlsnIndex::new(10));

    let pair = LocalChannelPair::new();
    let proposer: Arc<dyn Channel> = Arc::new(pair.channel_a);
    let acceptor: Arc<dyn Channel> = Arc::new(pair.channel_b);

    // --- the acceptor side: freezes on promise, thaws on result ------------
    let acceptor_latch = Arc::clone(&latch);
    let acceptor_thread = std::thread::spawn(move || {
        run_acceptor_with_state(
            &*acceptor,
            "n2",
            /* own_vlsn */ 4,
            /* own_priority */ 1,
            /* own_term */ 7,
            /* own_dtvlsn */ 4,
            &acc_state,
            Some(&acceptor_latch),
        )
    });

    // --- phase 1: propose, collect the promise -----------------------------
    send(
        &*proposer,
        &ProtocolMessage::ElectionProposal {
            node_name: "n1".into(),
            vlsn: 4,
            priority: 1,
            term: 7,
            dtvlsn: 4,
        },
    );
    match recv(&*proposer) {
        ProtocolMessage::ElectionProposal { .. } => {}
        other => {
            panic!("expected a Promise (ElectionProposal), got {:?}", other)
        }
    }
    // The promise has been sent, so the acceptor has frozen its commit VLSN.
    assert!(
        latch.is_frozen(),
        "the acceptor must freeze when it promises (JE \
         MasterSuggestionGenerator.java:63)"
    );

    // --- the replay side: a commit arrives mid-election --------------------
    let commit_landed = Arc::new(AtomicBool::new(false));
    let replay_thread = {
        let vlsn_index = Arc::clone(&vlsn_index);
        let latch = Arc::clone(&latch);
        let commit_landed = Arc::clone(&commit_landed);
        std::thread::spawn(move || {
            let mut writer = EnvironmentLogWriter::new(log_mgr, vlsn_index)
                .with_freeze_latch(latch);
            // A non-commit entry is not gated: it streams through even while
            // frozen (JE gates only the commit in Replay.replayEntry).
            writer.write_entry(5, INSERT_LN, b"ln-5").expect("write ln");
            // The commit IS gated: this blocks until the freeze lifts.
            writer
                .write_entry(6, TXN_COMMIT, b"commit-6")
                .expect("write commit");
            commit_landed.store(true, Ordering::SeqCst);
        })
    };

    // Give the replay thread ample time to get blocked in await_thaw.
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !commit_landed.load(Ordering::SeqCst),
        "the replayed commit must NOT be applied while the election round is \
         unresolved (JE Replay.java:525 awaitThaw)"
    );
    assert_eq!(
        vlsn_index.get_latest_vlsn(),
        5,
        "only the ungated non-commit entry (vlsn 5) may advance the index \
         mid-election; the commit at vlsn 6 must be deferred"
    );

    // --- phase 2: the election resolves -> the freeze lifts ---------------
    send(
        &*proposer,
        &ProtocolMessage::ElectionResult { master: "n1".into(), term: 7 },
    );
    match recv(&*proposer) {
        ProtocolMessage::ElectionVote { granted, .. } => {
            assert!(
                granted,
                "acceptor must accept the result for its promised term"
            )
        }
        other => panic!("expected an ElectionVote, got {:?}", other),
    }
    assert_eq!(
        acceptor_thread.join().unwrap().unwrap(),
        Some("n1".to_string())
    );

    // The deferred commit now proceeds, promptly (woken by the event, not by
    // the 30s timeout).
    let waited = Instant::now();
    replay_thread.join().expect("replay thread");
    assert!(
        waited.elapsed() < Duration::from_secs(5),
        "the commit must be released by the election result, not by the latch \
         timeout"
    );
    assert!(commit_landed.load(Ordering::SeqCst));
    assert_eq!(
        vlsn_index.get_latest_vlsn(),
        6,
        "after the election resolves the deferred commit must advance the VLSN"
    );
    assert!(!latch.is_frozen(), "the round is over; the latch must be clear");
    assert_eq!(
        latch.stats().await_election_count,
        1,
        "the thaw must be attributed to the election event"
    );
}

/// The freeze is BOUNDED: an election that never resolves cannot pin the
/// replay path forever.  The commit is delayed by at most the latch timeout
/// and then proceeds, degrading to the pre-freeze behaviour rather than
/// wedging the node.
#[test]
fn test_unresolved_election_does_not_block_replay_forever() {
    let (_tmp, log_mgr) = log_env();
    let latch =
        Arc::new(CommitFreezeLatch::with_timeout(Duration::from_millis(150)));
    let vlsn_index = Arc::new(VlsnIndex::new(10));

    // Freeze as the election path would, then never deliver a result.
    latch.freeze(round_proposal(9));

    let mut writer =
        EnvironmentLogWriter::new(log_mgr, Arc::clone(&vlsn_index))
            .with_freeze_latch(Arc::clone(&latch));
    let started = Instant::now();
    writer.write_entry(1, TXN_COMMIT, b"commit-1").expect("write commit");
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(100),
        "the commit must actually have been delayed by the freeze, was {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "an unresolved election must not block replay indefinitely, was {:?}",
        elapsed
    );
    assert_eq!(
        vlsn_index.get_latest_vlsn(),
        1,
        "the commit must land after all"
    );
    assert_eq!(latch.stats().await_timeout_count, 1);
    assert!(!latch.is_frozen());
}

/// A newer round supersedes an older freeze, and an OLDER round's result does
/// not lift a newer freeze — the `round_proposal` ordering must be by election
/// round, not by Noxu's election-ranking `Proposal` order (a laggard node's
/// later round has a LOWER vlsn/dtvlsn and would otherwise compare as older).
#[test]
fn test_freeze_ordering_is_by_election_round() {
    let latch =
        Arc::new(CommitFreezeLatch::with_timeout(Duration::from_secs(30)));

    latch.freeze(round_proposal(4));
    // A result for an older round must not release a newer freeze.
    latch.vlsn_event(&round_proposal(3));
    assert!(latch.is_frozen(), "an older round's result must not thaw");

    // A newer round re-freezes.
    latch.freeze(round_proposal(9));
    assert!(latch.is_frozen());
    // The old round's result still must not thaw it.
    latch.vlsn_event(&round_proposal(4));
    assert!(
        latch.is_frozen(),
        "round 4's result must not thaw round 9's freeze"
    );
    // Its own round's result does.
    latch.vlsn_event(&round_proposal(9));
    assert!(!latch.is_frozen(), "the current round's result must thaw");
}

/// `close()` clears the latch so a replay thread blocked awaiting an election
/// outcome is released and can observe the shutdown, instead of sitting out
/// the whole latch timeout.  JE `Replica.shutdown`: "Clear the latch in case
/// the replica loop is waiting for the outcome of an election"
/// (Replica.java:305).
#[test]
fn test_close_clears_the_freeze_latch() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let rep_env = ReplicatedEnvironment::new(
        RepConfig::builder("freeze_group", "n1", "127.0.0.1")
            .node_port(0)
            .env_home(tmp.path().to_path_buf())
            .build(),
    )
    .expect("ReplicatedEnvironment::new");

    let latch = rep_env.freeze_latch();
    latch.freeze(round_proposal(3));
    assert!(latch.is_frozen());

    rep_env.close().expect("close");
    assert!(
        !latch.is_frozen(),
        "close() must clear the freeze latch so a blocked replay thread is \
         released (JE Replica.java:305)"
    );
}
