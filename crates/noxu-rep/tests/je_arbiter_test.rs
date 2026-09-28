//! Test-parity port of JE `com.sleepycat.je.rep.arb.ArbiterTest`.
//!
//! JE's `ArbiterTest` is a **live multi-JVM integration** test: it boots two
//! to four `ReplicatedEnvironment`s plus a standalone `Arbiter` process
//! (`getReadyArbiter`), does DML, kills nodes, and asserts election / write
//! availability. Noxu models an arbiter as a *static quorum participant + an
//! election ranking + the RF=2 DTVLSN veto*, NOT JE's live standalone
//! `ArbiterImpl` process nor the `arbitration.Arbiter` active-primary state
//! machine (`activateArbitration` / `isActive` / DesignatedPrimary dynamic
//! toggling). See `docs/src/maintainer/design-decisions.md` and the
//! `elections/election.rs::test_designated_primary_self_election` comment
//! (the static designated-primary deviation).
//!
//! This file ports the **portable arbiter behaviors** ArbiterTest exercises
//! at Noxu's model level:
//!
//!   * an arbiter provides election quorum so a 2-node group can elect a
//!     master when one node is down (`testMasterDown`, `testFlipMaster`,
//!     `testQuadElection`);
//!   * an arbiter never wins the election itself, and a lagging sole node is
//!     vetoed (`testOneMaster`, `testQuad` prevention half) — covered by the
//!     existing `arbiter_election_test.rs` F22 / veto tests, cited there;
//!   * an arbiter's **ack must count toward the RF=2 write quorum** and an
//!     `ALL`-durability commit must FAIL when only the arbiter (not a data
//!     replica) is available (`testReplicaDown`, `testGroupAckAndReJoin`).
//!     This was BUG-ARB-01 (Noxu excluded arbiter acks from the RF=2 write
//!     quorum, so a replica-down commit required 0 witnesses). Fixed on
//!     `fix/bug-arb-01`; these ports are now un-ignored and passing. The
//!     runtime ack-satisfaction half is proven end-to-end in
//!     `replica_ack_policy_test.rs::bug_arb_01_*`.
//!
//! Non-portable ArbiterTest methods (live Arbiter process, Java platform,
//! active-primary state machine) are recorded as N/A in the package report
//! (`/tmp/audit/remediation/tp-je-rep-arb.md`), not here.

use noxu_rep::commit_durability::ReplicaAckPolicy;
use noxu_rep::node_type::NodeType;
use noxu_rep::rep_group::RepGroup;
use noxu_rep::rep_node::RepNode;

fn electable(name: &str, id: u32) -> RepNode {
    RepNode::new(
        name.into(),
        NodeType::Electable,
        "127.0.0.1".into(),
        5000 + id as u16,
        id,
    )
}

fn arbiter(name: &str, id: u32) -> RepNode {
    RepNode::new(
        name.into(),
        NodeType::Arbiter,
        "127.0.0.1".into(),
        5000 + id as u16,
        id,
    )
}

/// The engine's OWN durability ack-group-size rule (JE
/// `RepGroupImpl.getAckGroupSize` + `DurabilityQuorum` arbiter accounting),
/// exposed as `RepGroup::ack_group_size`. This is the exact accounting
/// `ReplicatedEnvironment::await_replica_acks` applies (the master derives the
/// ack group from this method, differing only by the implicit-master `+1`
/// that these explicit-master groups already include). Driving it makes the
/// port non-vacuous w.r.t. the arbiter-ack behavior: neuter the arbiter
/// accounting in `ack_group_size` and these tests fail again.
fn engine_ack_group_size(group: &RepGroup) -> u32 {
    group.ack_group_size()
}

// ---------------------------------------------------------------------------
// Node-type invariants the whole test rests on.
// ---------------------------------------------------------------------------

/// JE: `ArbiterTest` (implicit throughout) — the standalone Arbiter is an
/// ELECTABLE node that is NOT master-eligible, whose type string is
/// `"ARBITER"`. Every ArbiterTest scenario (it provides quorum, prevents a
/// lagging node winning, is listed by `getArbiterNodes`) rests on these three
/// facts. Pinned here so a regression that made the arbiter master-eligible
/// or dropped it from elections would fail this port, not just the
/// integration behavior above it. See also `node_type.rs` unit tests.
#[test]
fn arbiter_node_type_invariants() {
    let a = NodeType::Arbiter;
    // Electable (participates in elections) ...
    assert!(a.is_electable(), "arbiter must participate in elections");
    // ... but never master ...
    assert!(!a.can_be_master(), "arbiter must never be elected master");
    // ... and stores no data.
    assert!(!a.is_data_node(), "arbiter is not a data node");
    // The JE ReplicationNode type string.
    assert_eq!(a.to_string(), "ARBITER");
}

/// JE: `ArbiterTest.testReconfigureRG` / `ArbiterTest.testRepGroupAdmin`
/// (`rg.getArbiterNodes()` — "assertTrue(arbNodes.size() == 1)") — an arbiter
/// registered in the group is a member, counts in the electable set (so it
/// contributes to election quorum), but is distinguishable from data
/// replicas. Noxu has no dedicated `getArbiterNodes()` accessor; the group
/// tracks the arbiter as an electable-non-data member, which is the property
/// those two JE tests actually assert (an arbiter is a first-class group
/// member visible to group administration). The dynamic
/// add/remove-arbiter-via-DbGroupAdmin half is N/A (live group-admin RPC over
/// a running arbiter process).
#[test]
fn arbiter_is_registered_electable_group_member() {
    let mut g = RepGroup::new("g".into(), 1);
    g.add_node(electable("node1", 1));
    g.add_node(electable("node2", 2));
    g.add_node(arbiter("arb", 3));

    // The arbiter is a member of the group.
    assert!(g.contains_node("arb"));
    assert_eq!(g.get_node("arb").unwrap().node_type(), NodeType::Arbiter);

    // Exactly one arbiter present — the JE `getArbiterNodes().size() == 1`.
    let arb_members: Vec<_> = g
        .get_nodes()
        .into_iter()
        .filter(|n| n.node_type() == NodeType::Arbiter)
        .collect();
    assert_eq!(arb_members.len(), 1, "exactly one arbiter in the group");

    // 3 electable = 2 data nodes + 1 arbiter (arbiter counts as electable).
    assert_eq!(g.electable_count(), 3);
}

// ---------------------------------------------------------------------------
// Arbiter provides election quorum (testMasterDown / testFlipMaster /
// testQuadElection): a 2-electable group (data node + arbiter) reaches a
// simple-majority election quorum of 2, so a lone data node PLUS the arbiter
// can elect a master when the other data node is down.
// ---------------------------------------------------------------------------

/// JE: `ArbiterTest.testMasterDown` (quorum half) / `ArbiterTest.testFlipMaster`
/// (quorum half) — in a master + replica + arbiter group, when the master
/// dies the surviving replica plus the arbiter still form an election quorum,
/// so the replica can be elected master (write availability is preserved).
///
/// Noxu models this as arithmetic on the electable set: a group of
/// {master, replica, arbiter} has `electable_count == 3`, simple-majority
/// quorum == 2. After the master leaves, {replica, arbiter} == 2 electable
/// still meet the quorum of 2 (computed over the *surviving* electable set),
/// so an election can conclude. The end-to-end "arbiter's Phase-1 promise
/// lets a real node win" is proven by
/// `arbiter_election_test.rs::f22_arbiter_with_highest_vlsn_does_not_win`
/// (an electable node wins with the arbiter contributing quorum) — cited
/// there. Here we pin the quorum ARITHMETIC that makes it possible, and
/// guard it against a regression that stopped counting the arbiter.
#[test]
fn arbiter_provides_election_quorum_two_node_group() {
    // Full group: master + replica + arbiter.
    let mut full = RepGroup::new("g".into(), 1);
    full.add_node(electable("master", 1));
    full.add_node(electable("replica", 2));
    full.add_node(arbiter("arb", 3));
    assert_eq!(full.electable_count(), 3);
    // Simple majority of 3 electable = 2.
    assert_eq!(full.quorum_size(), 2);

    // Master down: the surviving electable set is {replica, arbiter}.
    let mut survivors = RepGroup::new("g".into(), 1);
    survivors.add_node(electable("replica", 2));
    survivors.add_node(arbiter("arb", 3));
    assert_eq!(
        survivors.electable_count(),
        2,
        "replica + arbiter are both electable"
    );
    // Quorum over the 2 survivors is 2: replica's self-vote + arbiter's
    // promise = 2 >= 2, so the election can conclude and the replica wins.
    assert_eq!(survivors.quorum_size(), 2);

    // Vacuity guard: WITHOUT the arbiter, a lone replica is 1 electable with
    // quorum 1 — it could self-elect, which is NOT the property under test.
    // The arbiter is what makes the 2-node group a *2*-electable quorum, so a
    // regression that dropped the arbiter from the electable set would change
    // this number (and silently permit a single lagging node to self-elect,
    // exactly what testOneMaster forbids). Assert the arbiter is load-bearing.
    let mut lone = RepGroup::new("g".into(), 1);
    lone.add_node(electable("replica", 2));
    assert_eq!(lone.electable_count(), 1);
    assert_ne!(
        lone.quorum_size(),
        survivors.quorum_size(),
        "arbiter must change the quorum arithmetic (it is load-bearing)"
    );
}

// ---------------------------------------------------------------------------
// Arbiter ack contributes to the RF=2 write quorum (testReplicaDown /
// testGroupAckAndReJoin).  Was BUG-ARB-01 — fixed on `fix/bug-arb-01`.
//
// These two ports drive the ENGINE'S OWN durability accounting via
// `engine_ack_group_size` (`RepGroup::ack_group_size`, the exact rule
// `ReplicatedEnvironment::await_replica_acks` applies) feeding the real
// `RepGroup::required_acks` / `ReplicaAckPolicy::required_acks`. They assert
// the JE-CORRECT contract and now PASS; neutering the arbiter accounting in
// `ack_group_size` makes them fail again (non-vacuity).
// ---------------------------------------------------------------------------

/// JE: `ArbiterTest.testReplicaDown` (SIMPLE_MAJORITY half) +
/// `ArbiterTest.testGroupAckAndReJoin` (`stats.getAcks()`).
///
/// In an RF=2 group (one data replica + arbiter, `getAckGroupSize() == 2`),
/// with SIMPLE_MAJORITY durability the master needs one ack
/// (`minAckNodes(2) - 1 == 1`). When the *data replica* is down, JE's arbiter
/// STILL acks the commit (`DurabilityQuorum.getCurrentRequiredAckCount`:
/// `includeArbiters = !ALL`; `FeederManager.activeAckReplicas(true)` includes
/// the active arbiter feeder), and `ArbiterAcker` *persistently tracks the
/// high VLSN it acknowledges* — so the commit is durable against one witness.
///
/// BUG-ARB-01 (now fixed): Noxu had excluded arbiter acks from the write
/// quorum — `await_replica_acks` derived the ack group from data-only
/// electable peers and `count_ack_feeders_ge` counted only data feeders, so a
/// replica-down RF=2 group collapsed to {master} and
/// `required_acks(SIMPLE_MAJORITY) == 0` (durable with NO witness). The fix
/// counts the arbiter as the RF=2 data slot for the ack-group SIZE (JE
/// `getAckGroupSize` keeps the down replica registered → 2) and counts the
/// arbiter feeder's ack as the SIMPLE_MAJORITY witness (JE `useArbiter`).
///
/// This test exercises the engine's OWN ack-group-size rule
/// (`RepGroup::ack_group_size`) against a {master, arbiter} group and asserts
/// the JE-correct required-ack count of 1.
#[test]
fn arbiter_ack_counts_toward_rf2_simple_majority_quorum() {
    // RF=2 group, data replica DOWN: only {master, arbiter} remain.
    let mut group = RepGroup::new("rf2".into(), 1);
    group.add_node(electable("master", 1));
    group.add_node(arbiter("arb", 2));

    // The engine's OWN durability accounting (JE getAckGroupSize + arbiter):
    // the arbiter stands in for the missing RF=2 data replica, so the ack
    // group size is 2.
    let engine_count = engine_ack_group_size(&group);
    assert_eq!(
        engine_count, 2,
        "RF=2 with the replica down: the arbiter fills the second ack-group \
         slot (JE keeps the down replica registered => getAckGroupSize == 2)"
    );
    let engine_needed =
        ReplicaAckPolicy::SimpleMajority.required_acks(engine_count);
    // And via the same rule the engine applies (RepGroup::required_acks):
    assert_eq!(
        group.required_acks(ReplicaAckPolicy::SimpleMajority),
        engine_needed
    );

    // JE-correct contract: the arbiter is a SIMPLE_MAJORITY acker, so the
    // effective RF=2 ack group is {master, arbiter} == 2 and the master needs
    // exactly ONE remote ack (the arbiter's) — minAckNodes(2) - 1 == 1.
    assert_eq!(
        engine_needed, 1,
        "RF=2 SIMPLE_MAJORITY with the replica down must still require the \
         arbiter's ack (JE minAckNodes(2)-1 == 1). Engine yielded {engine_needed}."
    );
}

/// JE: `ArbiterTest.testReplicaDown` (ALL half) — "Insertion with ACK
/// durability of ALL should have failed." In an RF=2 group with the data
/// replica down, an `ALL` commit must throw `InsufficientReplicasException`
/// because `ALL` requires every electable DATA replica and the arbiter does
/// NOT substitute (`includeArbiters = !ALL` — the arbiter is excluded from
/// the ALL ack set). So ALL needs the replica, which is down → fail.
///
/// This is the CONTROL for the SIMPLE_MAJORITY case above: it proves the JE
/// behavior is not "arbiter always satisfies durability" but "arbiter counts
/// for SIMPLE_MAJORITY, never for ALL". Before BUG-ARB-01 was fixed Noxu
/// dropped the arbiter from the ack group, collapsing the count to
/// {master} == 1 and `required_acks(All, 1) == 0`, so an ALL commit succeeded
/// with NO witness — contradicting the JE assertion. The required COUNT is
/// now 1 (ack-group-size 2), and the ALL guarantee is enforced on the
/// satisfaction side: NO arbiter ack qualifies for ALL, so with the data
/// replica down an ALL commit cannot be satisfied and fails (proven
/// end-to-end by `replica_ack_policy_test.rs::
/// bug_arb_01_all_durability_is_not_satisfied_by_arbiter`).
#[test]
fn arbiter_all_durability_requires_the_data_replica_rf2() {
    // The ALL ack group is the RF=2 electable data set: {master, replica} == 2,
    // so ALL needs 1 remote ack — the replica's. With the replica down that
    // ack is unavailable and the commit must fail. The arbiter cannot supply
    // it: the arbiter ack qualifies only under SIMPLE_MAJORITY (JE
    // `useArbiter`), never under ALL.
    let all_needed = ReplicaAckPolicy::All.required_acks(2);
    assert_eq!(
        all_needed, 1,
        "ALL over {{master, replica}} needs the replica's ack; arbiter cannot \
         substitute (JE useArbiter is SIMPLE_MAJORITY only)"
    );

    // The engine, given the replica-down {master, arbiter} group, must STILL
    // require one ack for ALL (ack-group-size 2 => minAckNodes(2) - 1 == 1).
    // The required COUNT is identical to SIMPLE_MAJORITY (JE
    // getCurrentRequiredAckCount is policy-independent); what differs is that
    // NO arbiter ack qualifies under ALL, so the down data replica's absent
    // ack cannot be substituted and the commit fails ("ALL should have
    // failed"). Before the fix Noxu collapsed the group to {master} == 1 and
    // yielded required_acks(All, 1) == 0, silently committing with no witness.
    let mut group = RepGroup::new("rf2".into(), 1);
    group.add_node(electable("master", 1));
    group.add_node(arbiter("arb", 2));
    let engine_count = engine_ack_group_size(&group);
    let engine_all_needed = ReplicaAckPolicy::All.required_acks(engine_count);
    assert_eq!(
        engine_all_needed, all_needed,
        "engine ALL required-acks with the replica down must still be {all_needed} \
         (a data-replica ack), but Noxu yielded {engine_all_needed}. The ALL \
         guarantee: the arbiter never satisfies ALL (JE useArbiter is \
         SIMPLE_MAJORITY only), so with the data replica down ALL fails."
    );
    // Same rule via the engine's RepGroup::required_acks accessor.
    assert_eq!(group.required_acks(ReplicaAckPolicy::All), all_needed);
}
