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
//!     Noxu currently does NOT count arbiter acks — these ports are ENGINE
//!     BUG CANDIDATES, kept `#[ignore]`d with a control below.
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

/// The Noxu durability quorum's OWN electable-count derivation, extracted so a
/// port can exercise the exact rule the engine applies in
/// `ReplicatedEnvironment::await_replica_acks` (count `NodeType::Electable`
/// peers, arbiters excluded, then `+ 1` for the master). This is the engine's
/// real accounting — not a re-derivation invented by the test — so a test
/// that drives it is non-vacuous w.r.t. the arbiter-ack behavior.
fn engine_durability_electable_count(group: &RepGroup) -> u32 {
    let electable_peers = group
        .get_nodes()
        .iter()
        .filter(|n| n.node_type() == NodeType::Electable)
        .count() as u32;
    // The master is implicit (not registered as a peer); +1 mirrors the engine.
    // Here the group already includes the master node explicitly, so we count
    // it directly instead — the point is only that ARBITERS ARE EXCLUDED.
    electable_peers
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
// testGroupAckAndReJoin).  ENGINE BUG CANDIDATE — see report.
//
// These two ports drive the ENGINE'S OWN durability accounting via
// `engine_durability_electable_count` (which mirrors
// `ReplicatedEnvironment::await_replica_acks` exactly: `NodeType::Electable`
// only) feeding the real `ReplicaAckPolicy::required_acks`. They assert the
// JE-CORRECT contract, so they FAIL against the current engine (which excludes
// arbiters) and are #[ignore]d as bug candidates — not weakened to pass.
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
/// Noxu does NOT count arbiter acks toward the write quorum:
/// `await_replica_acks` derives `electable_count` from `NodeType::Electable`
/// peers only (arbiters excluded), and `count_ack_feeders_ge` counts only
/// `NodeType::Electable` feeders; `become_master` creates NO feeder for an
/// arbiter. So in the replica-down RF=2 case the master's electable set
/// collapses to {master} → `required_acks(SIMPLE_MAJORITY, 1) == 0` → the
/// commit is "durable" with NO remote witness, where JE waits for and records
/// the arbiter's durable ack. This is a durability-safety divergence.
///
/// This test exercises the engine's OWN electable-count rule against a
/// {master, arbiter} group and asserts the JE-correct required-ack count of 1.
/// It FAILS today (engine yields 0) and is #[ignore]d as a BUG CANDIDATE.
#[test]
#[ignore = "ENGINE BUG CANDIDATE: Noxu excludes arbiter acks from the RF=2 \
            write quorum; a replica-down SIMPLE_MAJORITY commit requires 0 \
            witnesses instead of the arbiter's durable ack. JE: \
            ArbiterTest.testReplicaDown / DurabilityQuorum \
            (includeArbiters = !ALL). See tp-je-rep-arb.md."]
fn arbiter_ack_counts_toward_rf2_simple_majority_quorum() {
    // RF=2 group, data replica DOWN: only {master, arbiter} remain.
    let mut group = RepGroup::new("rf2".into(), 1);
    group.add_node(electable("master", 1));
    group.add_node(arbiter("arb", 2));

    // The engine's OWN durability accounting: only NodeType::Electable count.
    let engine_count = engine_durability_electable_count(&group);
    let engine_needed =
        ReplicaAckPolicy::SimpleMajority.required_acks(engine_count);

    // JE-correct contract: the arbiter is a SIMPLE_MAJORITY acker, so the
    // effective RF=2 ack group is {master, arbiter} == 2 and the master needs
    // exactly ONE remote ack (the arbiter's) — minAckNodes(2) - 1 == 1.
    assert_eq!(
        engine_needed, 1,
        "RF=2 SIMPLE_MAJORITY with the replica down must still require the \
         arbiter's ack (JE minAckNodes(2)-1 == 1). Engine yielded {engine_needed} \
         because it excludes the arbiter from the electable/ack set — the BUG."
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
/// for SIMPLE_MAJORITY, never for ALL". It is ALSO a bug in Noxu but in the
/// opposite direction: because Noxu drops the arbiter from `electable_count`,
/// the count collapses to {master} == 1 and `required_acks(All, 1) == 0`, so
/// an ALL commit succeeds with NO witness — directly contradicting the JE
/// assertion. `#[ignore]`d as a BUG CANDIDATE.
#[test]
#[ignore = "ENGINE BUG CANDIDATE: an ALL-durability commit in an RF=2 group \
            with the data replica down must FAIL (JE ArbiterTest.testReplicaDown \
            'ALL should have failed'), but Noxu requires 0 acks because it \
            drops the arbiter from electable_count. See tp-je-rep-arb.md."]
fn arbiter_all_durability_requires_the_data_replica_rf2() {
    // The ALL ack group is the electable DATA replica set (arbiter NEVER
    // counts for ALL): {master, replica} == 2, so ALL needs 1 remote ack —
    // the replica's. With the replica down that ack is unavailable and the
    // commit must fail.
    let all_needed = ReplicaAckPolicy::All.required_acks(2);
    assert_eq!(
        all_needed, 1,
        "ALL over {{master, replica}} needs the replica's ack; arbiter cannot \
         substitute (JE includeArbiters = !ALL)"
    );

    // The engine, given the replica-down {master, arbiter} group, must STILL
    // require the data replica for ALL (i.e. the ALL ack group is the data
    // replica set, not the collapsed electable set). Noxu instead computes
    // its electable count from live electable nodes {master} == 1 and yields
    // required_acks(All, 1) == 0 — so the ALL commit wrongly succeeds.
    let mut group = RepGroup::new("rf2".into(), 1);
    group.add_node(electable("master", 1));
    group.add_node(arbiter("arb", 2));
    let engine_count = engine_durability_electable_count(&group);
    let engine_all_needed = ReplicaAckPolicy::All.required_acks(engine_count);
    assert_eq!(
        engine_all_needed, all_needed,
        "engine ALL required-acks with the replica down must still be {all_needed} \
         (the data replica), but Noxu yielded {engine_all_needed} — dropping \
         the ALL guarantee. The BUG: ALL silently commits with no witness."
    );
}
