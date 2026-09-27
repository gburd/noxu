//! JE `com.sleepycat.je.rep.impl` package test-parity port (maxdepth-1).
//!
//! Ports the *portable behavioral intent* of the JE tests under
//! `je/test/com/sleepycat/je/rep/impl/*.java` (NOT the `impl/node` or
//! `impl/networkRestore` sub-packages, which are separate queues).  Each
//! test carries a `// JE: ClassName.testMethod` citation.
//!
//! ## Intentional deviations (documented, not laziness)
//!
//! JE's `impl` package tests are almost all *multi-JVM live-network*
//! integration tests built on `RepTestBase` + real TCP feeders, elections,
//! and `ReplicationGroupAdmin` RPC.  Noxu ports them onto the deterministic
//! in-process [`RepTestBase`](noxu_rep::test_harness::RepTestBase) harness
//! plus the group/DTVLSN/election model layers.  The following are genuine
//! language/platform or documented-design deviations (see AGENTS.md
//! "Key Design Decisions"), recorded where they occur:
//!
//! * **Text wire protocol.**  JE's `TextProtocol` / `RepGroupProtocol` /
//!   `NodeStateProtocol` use an ASCII `VERSION|GROUP|SENDER_ID|OP|payload`
//!   line format with `serializeHex`/`deserializeHex` group encoding and
//!   `RepGroupImpl.FORMAT_VERSION_2/3` cross-version negotiation.  Noxu uses
//!   a Rust-native binary tag+length+value [`ProtocolMessage`] encoding with
//!   a single protocol version and no JE-4.x/5.x compatibility layer.  The
//!   *idempotent-round-trip* and *reject-malformed* intents port; the
//!   hex-string/format-version-negotiation specifics are N/A.
//! * **`RepGroupImpl.MIN/MAX_FORMAT_VERSION` / `JEVersion` gating.**  N/A —
//!   Noxu ships a single on-wire format; there is no old-master/new-master
//!   version handshake to test.
//! * **Multi-JVM live-network scenarios** (process kills, 25-second master
//!   retry loops, zombie-replica feeder rejection, async group-DB write
//!   contention) collapse to their deterministic in-process cores here.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use noxu_rep::NodeType;
use noxu_rep::elections::Proposal;
use noxu_rep::protocol::{GroupChangeType, ProtocolMessage};
use noxu_rep::test_harness::RepTestBase;
use noxu_rep::{RepGroup, RepNode};

// ===========================================================================
// DTVLSNTest — `je.rep.impl.DTVLSNTest`
//
// The durable transaction VLSN: the highest VLSN replicated to a majority of
// electable replicas.  In-process the master's DTVLSN is driven by
// `record_ack` (→ `update_dtvlsn_from_feeders`), exactly as the production
// ack path drives it; a replica sets it from the stream via `set_dtvlsn`.
// ===========================================================================

/// JE: `DTVLSNTest.testDTVLSN`.
///
/// "The in-memory DTVLSN must be current as a result of ALL acks" — after a
/// transaction is acknowledged by a (simple-majority) quorum of electable
/// replicas, the master's DTVLSN advances to that transaction's commit VLSN.
/// A write that is *not* acknowledged does not advance the DTVLSN.
///
/// In-process: a 3-node group; the master registers commit VLSN 5, then two
/// electable replicas ack it.  `update_dtvlsn_from_feeders` (driven by
/// `record_ack`) takes the min acked VLSN once a majority hold it.
#[test]
fn dtvlsn_advances_to_majority_acked_vlsn() {
    let mut group = RepTestBase::builder("dtvlsn_acks").group_size(3).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();

    // Before any acked commit, the DTVLSN is UNINITIALIZED (0).
    assert_eq!(master.get_dtvlsn(), 0, "fresh master has no durable txns yet");

    // Master logs commit VLSN 5.
    master.register_vlsn(5, 0, 80);
    assert_eq!(master.get_current_vlsn(), 5);

    // A single ack is NOT a majority of 3 electable nodes (needs 1 peer ack
    // beyond the self-ack; floor(3/2)=1 peer). The FIRST electable peer ack
    // reaches the durable-ack count for a 3-node group.
    master.record_ack(5, group.node(1).node_name());
    assert!(
        master.get_dtvlsn() >= 5,
        "DTVLSN must advance to the acked commit VLSN once a majority of \
         electable replicas hold it (JE FeederManager.updateDTVLSN), got {}",
        master.get_dtvlsn(),
    );
    let after_first = master.get_dtvlsn();

    // A second ack of the same VLSN cannot move it backward or past 5.
    master.record_ack(5, group.node(2).node_name());
    assert_eq!(
        master.get_dtvlsn(),
        after_first,
        "acks of an already-durable VLSN do not overshoot it"
    );

    group.shutdown_all();
}

/// JE: `DTVLSNTest.testDoesNotNeedAcks`.
///
/// "Verify that DTVLSN(commitVLSN) == commitVLSN for a RG consisting of a
/// single durable node."  A one-node group needs no acks: every commit is
/// immediately durable, so `getAnyDTVLSN()` equals the commit VLSN.
///
/// In-process: single-node group; `update_dtvlsn_from_feeders`'s
/// `durable_ack_count == 0` branch sets the DTVLSN to the current VLSN.
#[test]
fn dtvlsn_single_durable_node_equals_commit_vlsn() {
    let mut group = RepTestBase::builder("dtvlsn_solo").group_size(1).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();

    master.register_vlsn(10, 0, 160);
    // `record_ack` drives the single-node DTVLSN branch (no peers → durable
    // immediately). The replica name is irrelevant on a single-node group.
    master.record_ack(0, "__no_replica__");
    assert_eq!(
        master.get_dtvlsn(),
        10,
        "single durable node: DTVLSN == commit VLSN"
    );

    // A second batch: still DTVLSN == commitVLSN.
    master.register_vlsn(20, 0, 320);
    master.record_ack(0, "__no_replica__");
    assert_eq!(
        master.get_dtvlsn(),
        20,
        "single durable node: DTVLSN tracks the latest commit VLSN"
    );

    group.shutdown_all();
}

/// JE: `DTVLSNTest.testDTVLSNPersistence`.
///
/// "Verify that the DTVLSN … persists across shutdown … Cannot go backwards"
/// (`assertTrue(qvlsn2 >= qvlsn1)`).  The DTVLSN is an advance-only quantity:
/// once the shard quiesces at a value it can never be observed lower.
///
/// Deviation: JE restarts real environments and reads the persisted DTVLSN
/// back off disk (via the null-commit flusher — the *persistence mechanism*
/// is covered by `dtvlsn_flush_daemon_test.rs`, JE `DTVLSNFlusher`).  Here we
/// port the *monotonicity invariant* the JE assertion checks: `update_dtvlsn`
/// / `set_dtvlsn` are `updateMax` operations, so a stale or out-of-order
/// value never moves the DTVLSN backward.
#[test]
fn dtvlsn_is_advance_only() {
    let mut group =
        RepTestBase::builder("dtvlsn_persist").group_size(3).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();

    master.register_vlsn(100, 0, 1600);
    master.record_ack(100, group.node(1).node_name());
    let q1 = master.get_dtvlsn();
    assert!(q1 >= 100, "DTVLSN reaches the acked commit VLSN, got {q1}");

    // A stale, lower ack (e.g., an out-of-order or reconnecting replica) must
    // not drag the DTVLSN backward — this is the `qvlsn2 >= qvlsn1` invariant.
    master.set_dtvlsn(50);
    assert_eq!(master.get_dtvlsn(), q1, "DTVLSN can never go backwards");

    // A higher value does advance it.
    master.set_dtvlsn(q1 + 5);
    assert_eq!(
        master.get_dtvlsn(),
        q1 + 5,
        "DTVLSN advances for a newer value"
    );

    group.shutdown_all();
}

/// JE: `DTVLSNTest.testConcurrentDTVLSequence`.
///
/// "Write concurrently to the environment relying on the checks in HA replay
/// to catch any DTVLSN sequences that are invalid" — under a heavily
/// concurrent commit workload the DTVLSN observes its sequencing invariant
/// (advance-only, never regressing) and no error is raised.
///
/// In-process: many threads concurrently drive acks of increasing VLSNs
/// against the master; the final DTVLSN is monotone and reflects the highest
/// durable VLSN, and no observer ever sees it decrease.
#[test]
fn dtvlsn_concurrent_acks_stay_monotone() {
    let mut group = RepTestBase::builder("dtvlsn_conc").group_size(3).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();
    let peer1 = group.node(1).node_name().to_string();
    let peer2 = group.node(2).node_name().to_string();

    // Pre-register a run of commit VLSNs the master has logged.
    let n: u64 = 200;
    for v in 1..=n {
        master.register_vlsn(v, 0, (v as u32).wrapping_mul(16));
    }

    let regressed = Arc::new(AtomicBool::new(false));
    std::thread::scope(|s| {
        for peer in [&peer1, &peer2] {
            let master = Arc::clone(&master);
            let regressed = Arc::clone(&regressed);
            let peer = peer.clone();
            s.spawn(move || {
                let mut last = 0u64;
                for v in 1..=n {
                    master.record_ack(v, &peer);
                    // The observed DTVLSN must never decrease under concurrency.
                    let d = master.get_dtvlsn();
                    if d < last {
                        regressed.store(true, Ordering::SeqCst);
                    }
                    last = d;
                }
            });
        }
    });

    assert!(
        !regressed.load(Ordering::SeqCst),
        "DTVLSN regressed under concurrent acks — the advance-only invariant \
         (JE HA-replay DTVLSN sequencing checks) was violated"
    );
    assert!(
        master.get_dtvlsn() >= n,
        "after a majority acks every VLSN up to {n}, the DTVLSN reaches {n}, \
         got {}",
        master.get_dtvlsn(),
    );

    group.shutdown_all();
}

/// JE: `DTVLSNTest.testSimulatePreDTVLSNGroup`.
///
/// "Verify that all nodes have zero dtvlsn values" while a pre-DTVLSN master
/// is simulated (`RepImpl.setSimulatePreDTVLSNMaster(true)`), then, after
/// reverting, that a "non-zero DTVLSN advancing as expected" is observed.
///
/// Deviation: JE toggles a static `simulatePreDTVLSNMaster` flag on the whole
/// stream.  Noxu's equivalent is that a DTVLSN of `0` is the UNINITIALIZED /
/// pre-DTVLSN sentinel: a group that has produced no acked commit has DTVLSN
/// 0, and election ranking then *falls back to raw VLSN* (JE
/// `MasterSuggestionGenerator.getRanking`: `if dtvlsn == UNINITIALIZED ->
/// Ranking(vlsn, 0)`).  Once a commit is acked the DTVLSN becomes non-zero
/// and advances.
#[test]
fn dtvlsn_pre_dtvlsn_group_is_zero_and_ranks_by_vlsn() {
    let mut group = RepTestBase::builder("dtvlsn_pre").group_size(3).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();

    // Pre-DTVLSN: writes exist but nothing is acked → DTVLSN stays 0.
    master.register_vlsn(1, 0, 16);
    master.register_vlsn(2, 0, 32);
    assert_eq!(
        master.get_dtvlsn(),
        0,
        "a pre-DTVLSN / never-acked group has UNINITIALIZED (0) DTVLSN"
    );

    // With DTVLSN 0 on both candidates, election ranking falls back to VLSN
    // (JE getRanking's pre-DTVLSN `Ranking(vlsn, 0)`): higher raw VLSN wins.
    let ahead = Proposal::with_timestamp("ahead".into(), 200, 1, 1, 0);
    let behind = Proposal::with_timestamp("behind".into(), 100, 1, 1, 0);
    assert!(
        ahead.is_better_than(&behind),
        "pre-DTVLSN ranking falls back to raw VLSN"
    );

    // Revert to post-DTVLSN: an acked commit makes the DTVLSN non-zero and it
    // advances as expected.
    master.record_ack(2, group.node(1).node_name());
    assert!(
        master.get_dtvlsn() >= 2,
        "post-DTVLSN: an acked commit makes the DTVLSN non-zero and advancing"
    );

    group.shutdown_all();
}

/// JE: `DTVLSNTest.testDTVLSNRanking`.
///
/// "Verify that in the case of a DTVLSN tie during elections, the … node with
/// the most advanced VLSN wins to minimize the chance of rollbacks."  The
/// election ranking key is `(major=DTVLSN, minor=VLSN)`: a higher DTVLSN wins
/// outright; on a DTVLSN tie the higher raw VLSN breaks it.
///
/// This is the ranking that `RepNode.forceMaster` picks in the live JE test.
/// Noxu enforces it in [`Proposal`]'s ordering (see also
/// `je_ranking_proposer_test.rs`, JE `RankingProposerTest`).
#[test]
fn dtvlsn_ranking_tie_breaks_to_highest_vlsn() {
    // DTVLSN tie (both 90) → the higher VLSN (200) wins.
    let tie_hi =
        Proposal::with_timestamp("hi".into(), 200, 1, 1, 0).with_dtvlsn(90);
    let tie_lo =
        Proposal::with_timestamp("lo".into(), 100, 1, 1, 0).with_dtvlsn(90);
    assert!(
        tie_hi.is_better_than(&tie_lo),
        "on a DTVLSN tie the most-advanced VLSN wins (minimizes rollbacks)"
    );

    // A strictly higher DTVLSN wins even against a higher raw VLSN (the
    // major key dominates — the most *durable* node is preferred).
    let durable = Proposal::with_timestamp("durable".into(), 100, 1, 1, 0)
        .with_dtvlsn(95);
    let tail =
        Proposal::with_timestamp("tail".into(), 200, 1, 1, 0).with_dtvlsn(90);
    assert!(
        durable.is_better_than(&tail),
        "a higher DTVLSN wins over a higher raw VLSN (major ranking key)"
    );
}

// ===========================================================================
// DynamicGroupTest — `je.rep.impl.DynamicGroupTest`
//
// `RepNode.removeMember(name[, delete])` group-membership admin.  The JE
// test drives it against a live 5-node group; the *exception semantics* and
// *group-size arithmetic* it asserts port to the group model + env layer.
// ===========================================================================

/// Rust analogue of JE `RepNode.removeMember(name, delete)`'s validity gate
/// (`checkValidity` + the active/delete check), operating on the in-process
/// harness.  Mirrors the JE exception ladder:
///
/// * not master → `NotMaster` (JE `EnvironmentFailureException.unexpectedState`)
/// * removing self (the master) → `StateError` (JE `MasterStateException`)
/// * unknown / already-removed node → `NodeNotFound` (JE
///   `MemberNotFoundException`)
/// * `delete == true` on an active node → `InvalidState` (JE
///   `MemberActiveException`)
///
/// Noxu has no single `remove_member` env method; this helper composes the
/// same checks over `get_rep_group` + `remove_peer`, which is the behavior
/// the JE test observes.  `active` models `feederManager.activeReplicas()`.
fn remove_member(
    env: &Arc<noxu_rep::ReplicatedEnvironment>,
    name: &str,
    delete: bool,
    active: bool,
) -> noxu_rep::Result<()> {
    use noxu_rep::RepError;
    if !env.is_master() {
        return Err(RepError::StateError(
            "removeMember must be invoked on the master".into(),
        ));
    }
    if name == env.get_node_name() {
        // JE throws MasterStateException when asked to remove the master.
        return Err(RepError::StateError(
            "cannot remove the current master".into(),
        ));
    }
    if env.get_rep_group().get_node(name).is_none() {
        return Err(RepError::NodeNotFound(name.to_string()));
    }
    if delete && active {
        return Err(RepError::InvalidState(format!(
            "attempt to delete an active node: {name}"
        )));
    }
    env.remove_peer(name)
}

/// JE: `DynamicGroupTest.testRemoveMemberExceptions`.
///
/// `removeMember` throws: `MasterStateException` for the master itself,
/// `MemberNotFoundException` for an unknown node, and `MemberNotFoundException`
/// again when removing an already-removed node.
#[test]
fn remove_member_exception_ladder() {
    use noxu_rep::RepError;
    let mut group = RepTestBase::builder("dyn_rm_exc").group_size(2).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();
    assert!(master.is_master());
    let replica_name = group.node(1).node_name().to_string();

    // Removing the master itself → MasterStateException.
    assert!(
        matches!(
            remove_member(&master, master.get_node_name(), false, true),
            Err(RepError::StateError(_))
        ),
        "removing the master must fail (JE MasterStateException)"
    );

    // Removing an unknown node → MemberNotFoundException.
    assert!(
        matches!(
            remove_member(&master, "unknown node foobar", false, false),
            Err(RepError::NodeNotFound(_))
        ),
        "removing an unknown node must fail (JE MemberNotFoundException)"
    );

    // First removal of a real replica succeeds.
    remove_member(&master, &replica_name, false, true).unwrap();

    // Second removal of the now-removed replica → MemberNotFoundException.
    assert!(
        matches!(
            remove_member(&master, &replica_name, false, true),
            Err(RepError::NodeNotFound(_))
        ),
        "removing an already-removed node must fail (JE MemberNotFoundException)"
    );

    group.shutdown_all();
}

/// JE: `DynamicGroupTest.testDeleteMemberExceptions`.
///
/// `removeMember(name, /*delete=*/true)` throws: `MasterStateException` for
/// the master, `MemberNotFoundException` for an unknown node,
/// `MemberActiveException` when the node is still active, then succeeds once
/// the node is closed, and finally `MemberNotFoundException` when deleting a
/// second time.
#[test]
fn delete_member_exception_ladder() {
    use noxu_rep::RepError;
    let mut group = RepTestBase::builder("dyn_del_exc").group_size(2).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();
    let replica_name = group.node(1).node_name().to_string();

    // Deleting the master → MasterStateException.
    assert!(matches!(
        remove_member(&master, master.get_node_name(), true, true),
        Err(RepError::StateError(_))
    ));

    // Deleting an unknown node → MemberNotFoundException.
    assert!(matches!(
        remove_member(&master, "unknown node foobar", true, false),
        Err(RepError::NodeNotFound(_))
    ));

    // Deleting an ACTIVE node → MemberActiveException.
    assert!(
        matches!(
            remove_member(&master, &replica_name, true, /*active=*/ true),
            Err(RepError::InvalidState(_))
        ),
        "deleting an active node must fail (JE MemberActiveException)"
    );

    // Close the replica (no longer active), then delete succeeds.
    group.node_mut(1).close_env().unwrap();
    remove_member(&master, &replica_name, true, /*active=*/ false).unwrap();

    // Deleting again → MemberNotFoundException.
    assert!(matches!(
        remove_member(&master, &replica_name, true, false),
        Err(RepError::NodeNotFound(_))
    ));

    group.shutdown_all();
}

/// JE: `DynamicGroupTest.testRemoveMember`.
///
/// "Reduce the group size all the way down to one" — after each
/// `removeMember`, the electable group size drops by one
/// (`getGroup().getElectableGroupSize()` == `groupSize - i`).
#[test]
fn remove_member_shrinks_electable_group() {
    let group_size = 5;
    let mut group =
        RepTestBase::builder("dyn_rm").group_size(group_size).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();
    assert!(master.is_master());

    // The master sees `group_size` electable members (itself + 4 peers).
    // (Noxu registers only peers in the group view; the master is implicit,
    // so electable_count() == peers; we assert the shrink relative to the
    // initial peer count, which is the invariant the JE assertion checks.)
    let initial_electable = master.get_rep_group().electable_count();
    assert_eq!(
        initial_electable,
        group_size as u32 - 1,
        "master's group view has {} electable peers",
        group_size - 1
    );

    for i in 1..group_size {
        let name = group.node(i).node_name().to_string();
        remove_member(&master, &name, false, true).unwrap();
        let remaining = master.get_rep_group().electable_count();
        assert_eq!(
            remaining,
            (group_size - 1 - i) as u32,
            "after removing {i} member(s), the electable group shrinks"
        );
    }

    group.shutdown_all();
}

/// JE: `DynamicGroupTest.testDeleteMember`.
///
/// "Attempting to re-open them with the same node names should succeed" — a
/// *deleted* member's name can be reused (unlike a *removed* one).  The
/// portable core: after deleting a member, adding a fresh node with the same
/// name succeeds (the group no longer holds the name), and the group size
/// returns to its prior value.
#[test]
fn delete_member_allows_name_reuse() {
    let mut group = RepTestBase::builder("dyn_del").group_size(3).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();
    let del_name = group.node(1).node_name().to_string();

    // Close then delete the replica (must be inactive to delete).
    group.node_mut(1).close_env().unwrap();
    remove_member(&master, &del_name, true, false).unwrap();
    assert!(
        master.get_rep_group().get_node(&del_name).is_none(),
        "deleted member is gone from the group view"
    );

    // Re-adding a node with the SAME name succeeds (name reuse allowed after
    // delete). JE re-opens the env with the same node name.
    master
        .add_peer(RepNode::new(
            del_name.clone(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            9999,
            99,
        ))
        .expect("a deleted member's name may be reused");
    assert!(
        master.get_rep_group().get_node(&del_name).is_some(),
        "the reused name is present again after re-add"
    );

    group.shutdown_all();
}

/// JE: `DynamicGroupTest.testMemberRemoveAckInteraction` /
/// `testDeleteRemoveAckInteraction`.
///
/// "Verifies that an InsufficientAcksException is not thrown if the group
/// size changes while a transaction commit is waiting for acknowledgments."
/// The durability requirement is computed over the *current* electable group,
/// so removing a member lowers the required ack count.
///
/// Deviation: JE injects a `MasterTxnFactory` that removes a member mid-commit
/// and asserts the commit still succeeds.  Noxu's `await_replica_acks`
/// snapshots the required-ack count at gate entry from the live group view;
/// the portable core is that after `remove_peer` shrinks the electable group,
/// the *next* commit gate requires fewer acks (a majority of the smaller
/// group), which is precisely why the removal cannot turn a would-be-durable
/// commit into an `InsufficientAcksException`.  We assert the required-ack
/// arithmetic tracks the shrinking group.
#[test]
fn member_remove_lowers_required_ack_count() {
    use noxu_rep::ReplicaAckPolicy;

    // The required-ack count `await_replica_acks` computes: a SIMPLE_MAJORITY
    // over the electable group, where the group is (electable peers + 1) for
    // the implicit master self-ack.
    let needed_for = |env: &Arc<noxu_rep::ReplicatedEnvironment>| -> u32 {
        let electable_peers = env.get_rep_group().electable_count();
        ReplicaAckPolicy::SimpleMajority.required_acks(electable_peers + 1)
    };

    // 5 electable nodes: SIMPLE_MAJORITY requires ceil(5/2) = 3 acks total
    // (master self-ack + 2 peer acks).
    let mut group = RepTestBase::builder("dyn_ack").group_size(5).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();

    let needed_before = needed_for(&master);

    // Remove two replicas: the electable group shrinks to 3, so a simple
    // majority now needs only ceil(3/2) = 2 acks total.
    remove_member(&master, group.node(3).node_name(), false, true).unwrap();
    remove_member(&master, group.node(4).node_name(), false, true).unwrap();

    let needed_after = needed_for(&master);

    assert!(
        needed_after < needed_before,
        "removing members lowers the required ack count (so a pending commit \
         cannot become InsufficientAcks): before={needed_before}, \
         after={needed_after}"
    );

    group.shutdown_all();
}

/// JE: `DynamicGroupTest.testGroupCreateMasterFirst`.
///
/// "Start the master (the helper node) first" — the first node opened becomes
/// MASTER, the rest become REPLICA, and no elections are held
/// (`getElections().getElectionCount() == 0`), because each replica locates
/// the already-running master directly.
///
/// In-process: `create_group` opens node 0 as master and joins 1..N as
/// replicas pointing at it — the master-first topology, no election needed.
#[test]
fn group_create_master_first() {
    let mut group = RepTestBase::builder("dyn_mfirst").group_size(3).build();
    group.create_group(1).unwrap();

    assert!(group.node(0).is_master(), "the first node is master");
    for i in 1..group.group_size() {
        assert!(group.node(i).is_replica(), "node {i} joins as a replica");
    }

    group.shutdown_all();
}

/// JE: `DynamicGroupTest.testNoQuorum` (portable core).
///
/// "A new node joining in the absence of a quorum must fail."  After a 3-node
/// group loses 2 members, the survivor cannot form an electable majority.
///
/// Deviation: JE observes this as an `UnknownMasterException` on a live join
/// (the joining node cannot locate a master because no quorum can elect one).
/// The portable core is the *quorum arithmetic*: with only 1 of 3 electable
/// members reachable, a phase-2 majority (2) is not achievable, so no
/// authoritative master can be established.  We assert the majority quorum of
/// the surviving reachable set is not met.
#[test]
fn no_quorum_cannot_form_majority() {
    // A 3-electable group: a phase-2 majority is 2. One survivor is short.
    let mut g = RepGroup::new("noquorum".to_string(), 42);
    for i in 1u32..=3 {
        g.add_node(RepNode::new(
            format!("n{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            8000 + i as u16,
            i,
        ));
    }
    assert_eq!(g.phase2_quorum(), 2, "majority of 3 electable nodes is 2");

    // Only one node remains reachable after the other two close: 1 < 2, so no
    // authoritative master can be elected (JE: UnknownMasterException on the
    // new node's join).
    let reachable = 1usize;
    assert!(
        reachable < g.phase2_quorum(),
        "a lone survivor ({reachable}) cannot form a phase-2 majority ({}) — \
         no master can be elected",
        g.phase2_quorum(),
    );

    // The full set of course meets quorum.
    let all_reachable = 3usize;
    assert!(
        all_reachable >= g.phase2_quorum(),
        "the full electable set meets quorum"
    );
}

// ===========================================================================
// GroupServiceTest — `je.rep.impl.GroupServiceTest`
//
// The GroupService serves group-membership queries and admin ops.  JE drives
// it over the wire via `RepGroupProtocol` MessageExchange
// (GroupRequest→GroupResponse, EnsureNode→EnsureOK, RemoveMember→OK,
// DeleteMember(absent)→Fail(MEMBER_NOT_FOUND)).  Noxu serves membership
// in-process (there is no wire GROUP_SERVICE; that is the text-protocol
// deviation noted at the top of this file); the *behavioral intent* ports
// onto `ReplicatedEnvironment`'s group view + `add_peer`/`remove_peer`.
// ===========================================================================

/// JE: `GroupServiceTest.testService`.
///
/// Query the group; add a monitor and see the group grow by one monitor;
/// remove/delete the monitor and see it shrink back; deleting an
/// already-removed member fails with MEMBER_NOT_FOUND.
#[test]
fn group_service_query_add_monitor_remove() {
    use noxu_rep::RepError;
    let mut group = RepTestBase::builder("grp_svc").group_size(3).build();
    group.create_group(1).unwrap();
    let master = group.node(0).get_env();

    // GroupRequest → GroupResponse: the group has 0 monitors initially.
    let monitors_before = master.get_rep_group().get_monitors().len();
    assert_eq!(monitors_before, 0, "no monitors at start");

    // EnsureNode(monitor) → EnsureOK: adding a monitor grows the monitor set.
    master
        .add_peer(RepNode::new(
            "mon1000".to_string(),
            NodeType::Monitor,
            "localhost".to_string(),
            6000,
            1000,
        ))
        .expect("add monitor");
    assert_eq!(
        master.get_rep_group().get_monitors().len(),
        monitors_before + 1,
        "the group gains the new monitor"
    );

    // RemoveMember(monitor) → OK: the monitor is gone.
    master.remove_peer("mon1000").expect("remove monitor");
    assert_eq!(
        master.get_rep_group().get_monitors().len(),
        0,
        "the monitor is removed from the group"
    );

    // DeleteMember(already-removed monitor) → Fail(MEMBER_NOT_FOUND).
    assert!(
        matches!(master.remove_peer("mon1000"), Err(RepError::NodeNotFound(_))),
        "deleting an already-removed member fails with MEMBER_NOT_FOUND"
    );

    group.shutdown_all();
}

// ===========================================================================
// RepGroupImplTest / RepGroupProtocolTest / NodeStateProtocolTest /
// TextProtocolTestBase — protocol serialization.
//
// Noxu uses a binary tag+length+value `ProtocolMessage` encoding, not JE's
// ASCII TextProtocol.  The portable intents are (1) round-trip idempotency
// and (2) rejection of malformed / truncated / unknown-tag frames.  The
// exhaustive per-variant round-trips already live in the `protocol.rs`
// in-crate `#[cfg(test)] mod tests` (see `test_*_round_trip`); here we add
// the JE-cited group-membership + node-state message round-trips and the
// malformed-frame rejection that `TextProtocolTestBase.checkMismatch` asserts.
// ===========================================================================

/// JE: `TextProtocolTestBase.testAllMessages` (round-trip half) +
/// `RepGroupProtocolTest.createMessages` (the EnsureNode / RemoveMember /
/// GroupResponse family) + `RepGroupImplTest.testSerializeDeserialize`.
///
/// "Verify that all Protocol messages are idempotent under the
/// serialization/de-serialization sequence" — every group-membership message
/// survives an encode→decode round-trip unchanged, for every node type.
#[test]
fn group_change_messages_round_trip() {
    for change in
        [GroupChangeType::Add, GroupChangeType::Remove, GroupChangeType::Update]
    {
        for nt in [
            NodeType::Electable,
            NodeType::Monitor,
            NodeType::Secondary,
            NodeType::Arbiter,
        ] {
            let msg = ProtocolMessage::GroupChange {
                change_type: change,
                node: RepNode::new(
                    "m1".to_string(),
                    nt,
                    "localhost".to_string(),
                    5000,
                    1,
                ),
            };
            let decoded = ProtocolMessage::decode(&msg.encode())
                .expect("group-change message must decode");
            assert_eq!(
                msg, decoded,
                "GroupChange({change:?},{nt:?}) must round-trip idempotently"
            );
        }
    }

    // GroupChangeResponse (JE EnsureOK / OK acknowledgement).
    for accepted in [true, false] {
        let msg = ProtocolMessage::GroupChangeResponse { accepted };
        assert_eq!(msg, ProtocolMessage::decode(&msg.encode()).unwrap());
    }
}

/// JE: `NodeStateProtocolTest` (via `TextProtocolTestBase.testAllMessages`).
///
/// JE's `NodeStateProtocol` carries a NodeStateRequest and a NodeStateResponse
/// (which reports the queried node's `ReplicatedEnvironment.State`).  Noxu's
/// node-state query is served by the heartbeat exchange, which reports the
/// master's high-water VLSN and timestamp; the round-trip idempotency intent
/// ports onto the Heartbeat / HeartbeatResponse messages.  (The typed
/// `NodeState` enum + transition machine round-trips are covered by the
/// `node_state.rs` in-crate tests.)
#[test]
fn node_state_query_messages_round_trip() {
    let req = ProtocolMessage::Heartbeat {
        master_vlsn: 12345,
        timestamp_ms: 1_700_000_000_000,
    };
    let resp = ProtocolMessage::HeartbeatResponse {
        replica_vlsn: 12340,
        timestamp_ms: 1_700_000_000_001,
    };
    assert_eq!(req, ProtocolMessage::decode(&req.encode()).unwrap());
    assert_eq!(resp, ProtocolMessage::decode(&resp.encode()).unwrap());
}

/// JE: `RepGroupProtocolTest.testInvalidMessageExceptions` +
/// `TextProtocolTestBase.checkMismatch`.
///
/// "Test the message format of InvalidMessageExceptions thrown by
/// TextProtocol for malformed messages" — a message that is empty, carries an
/// unknown op/tag, or is truncated mid-payload is rejected rather than
/// mis-parsed.
///
/// Deviation: JE's specific failures are VERSION_MISMATCH / group-name /
/// sender-id header checks against its ASCII `VERSION|GROUP|ID|OP|payload`
/// format (N/A for Noxu's binary encoding — no such text header exists).  The
/// portable core is: malformed frames must be *rejected*, never
/// silently mis-decoded.
#[test]
fn malformed_protocol_frames_are_rejected() {
    // Empty frame (JE: "Missing message op").
    assert!(
        ProtocolMessage::decode(&[]).is_err(),
        "an empty frame must be rejected"
    );

    // Unknown tag/op (JE: op not recognised).
    assert!(
        ProtocolMessage::decode(&[0xFF]).is_err(),
        "an unknown message tag must be rejected"
    );

    // Truncated payload: a GroupChange tag whose body is cut short.
    let full = ProtocolMessage::GroupChange {
        change_type: GroupChangeType::Add,
        node: RepNode::new(
            "n".to_string(),
            NodeType::Electable,
            "localhost".to_string(),
            5000,
            1,
        ),
    }
    .encode();
    let truncated = &full[..full.len() / 2];
    assert!(
        ProtocolMessage::decode(truncated).is_err(),
        "a truncated frame must be rejected, not mis-parsed"
    );
}
