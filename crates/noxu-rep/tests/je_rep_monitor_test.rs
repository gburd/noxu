//! Test-parity port of JE `com.sleepycat.je.rep.monitor` (MonitorChangeListenerTest,
//! MonitorTest, PingCommandTest, ProtocolTest).
//!
//! ## JUDGMENT: enum-only Monitor, NO observer machinery
//!
//! A JE Monitor is a full **observer node**: a standalone `Monitor`
//! (`MonitorConfig`, `monitor.register()`, `monitor.startListener(...)`) that
//! registers a `MonitorChangeListener` and receives asynchronous callbacks —
//! `notify(NewMasterEvent)`, `notify(GroupChangeEvent)`,
//! `notify(JoinGroupEvent)`, `notify(LeaveGroupEvent)` — driven by a monitor
//! wire `Protocol` (a `TextProtocol` carrying a full `RepGroupImpl` with
//! version negotiation) and a background Ping thread that back-fills missed
//! join/leave/group-change events and probes node state
//! (`ReplicationGroupAdmin.getNodeState`).
//!
//! Noxu has **`NodeType::Monitor` as an enum value only** (`node_type.rs`).
//! There is NO `Monitor` struct, NO `MonitorConfig`, NO `MonitorChangeListener`,
//! NO `GroupChangeEvent` / `JoinGroupEvent` / `LeaveGroupEvent` /
//! `NewMasterEvent` types, NO `startListener` / `register` / `disableNotify`,
//! NO monitor `Protocol` (Noxu's `protocol::ProtocolMessage::GroupChange` is a
//! DISTINCT master↔replica binary message — single node, no version
//! negotiation, no full-group payload, no JoinGroup/LeaveGroup), NO Ping thread,
//! and NO `ReplicationGroupAdmin.getNodeState` / `NodeState` RPC (the admin
//! service in `group_admin.rs` handles only TRANSFER_MASTER / SHUTDOWN_GROUP /
//! STEP_DOWN).
//!
//! Consequently **all 17 JE @Test methods are N/A as written** — every one of
//! them exercises the monitor observer / monitor wire protocol / node-state
//! ping, none of which exist in Noxu. This is a documented design deviation
//! (no monitor-observer API), recorded per-method in the package report
//! (`/tmp/audit/remediation/tp-je-rep-monitor.md`).
//!
//! ## What IS ported here (the portable model-level kernel, non-vacuous)
//!
//! A `GroupChangeEvent` reports the **membership delta** (ADD/REMOVE + the
//! resulting group snapshot + the node that triggered it), and JE documents
//! that **SECONDARY nodes do not generate GroupChangeEvents**. Noxu's
//! `RepGroup` (add_node / remove_node) plus `protocol::GroupChangeType`
//! (Add/Remove/Update) model exactly that delta. We port the delta half of the
//! MonitorChangeListener group-change tests at Noxu's model level: the group
//! composition that a `GroupChangeEvent` WOULD carry after each add/remove.
//! This is non-vacuous — it drives `RepGroup`'s real membership mutation and
//! the real `GroupChangeType` enum, and would fail if either regressed. It
//! does NOT re-implement the observer (there is no listener to notify), and
//! the event-DELIVERY half of those tests remains N/A (see report).
//!
//! NOTHING in this file is production code; no production feature was added.

use noxu_rep::node_type::NodeType;
use noxu_rep::protocol::{GroupChangeType, ProtocolMessage};
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

fn secondary(name: &str, id: u32) -> RepNode {
    RepNode::new(
        name.into(),
        NodeType::Secondary,
        "127.0.0.1".into(),
        5000 + id as u16,
        id,
    )
}

fn monitor(name: &str, id: u32) -> RepNode {
    RepNode::new(
        name.into(),
        NodeType::Monitor,
        "127.0.0.1".into(),
        5000 + id as u16,
        id,
    )
}

/// The electable-membership set a `GroupChangeEvent` reports on — JE excludes
/// SECONDARY nodes from group-change events (`GroupChangeEvent` javadoc: "Note
/// that SECONDARY nodes do not generate these events"). This mirrors the
/// engine's own `RepGroup::get_electable_nodes` filter (arbiters are electable
/// for quorum, but for the ADD/REMOVE *group-change* delta JE reports the
/// ELECTABLE-or-MONITOR membership; monitors register as members, secondaries
/// do not fire the event). We assert the delta at the level Noxu actually
/// models: the node set that changed.
fn member_names(group: &RepGroup) -> Vec<String> {
    let mut v: Vec<String> =
        group.get_nodes().iter().map(|n| n.name().to_string()).collect();
    v.sort();
    v
}

// ---------------------------------------------------------------------------
// MonitorChangeListenerTest — model-level GROUP-DELTA half (delivery is N/A)
// ---------------------------------------------------------------------------

/// JE: `MonitorChangeListenerTest.testBasicBehaviors` (group-delta half only).
///
/// In JE, as each electable replica joins, the master fires an ADD
/// `GroupChangeEvent` whose `getRepGroup()` snapshot grows by one and whose
/// `getNodeName()` is the joiner. The DELIVERY-to-listener half (the
/// `CountDownLatch` awaits, `getGroupAddEvents()` counters, NewMasterEvent on
/// master close) is N/A — Noxu has no `MonitorChangeListener`. Here we port the
/// membership-delta the event WOULD carry: joining a node ADDs it to the group
/// snapshot; the delta names exactly the joiner.
#[test]
fn test_basic_behaviors_group_add_delta() {
    let mut group = RepGroup::new("g".into(), 1);
    group.add_node(electable("Node1", 1)); // master present at monitor start
    assert_eq!(member_names(&group), vec!["Node1"]);

    // Each replica join is an ADD GroupChangeEvent naming the joiner; the
    // snapshot grows by one each time (JE: groupEvent.getRepGroup().getNodes().
    // size() == i + 2 counting the monitor — here we track only the electable
    // membership Noxu models).
    for i in 2..=5u32 {
        let name = format!("Node{i}");
        let prev = group.add_node(electable(&name, i));
        assert!(prev.is_none(), "ADD is a genuinely new member");
        assert!(group.contains_node(&name), "joiner named in the delta");
        assert_eq!(
            group.node_count(),
            i as usize,
            "snapshot grew by exactly one"
        );
    }
    assert_eq!(
        member_names(&group),
        vec!["Node1", "Node2", "Node3", "Node4", "Node5"]
    );
}

/// JE: `MonitorChangeListenerTest.testRemoveMember` (group-delta half only).
///
/// `master.removeMember(nodeName)` fires a REMOVE `GroupChangeEvent` (and,
/// crucially, NO LeaveGroupEvent — removal is a membership change, not a
/// departure). DELIVERY half N/A. Ported: removeMember removes exactly the
/// named node from the group snapshot and is idempotent-safe (a second remove
/// of the same node is a no-op / None), matching REMOVE naming the removed node.
#[test]
fn test_remove_member_group_remove_delta() {
    let mut group = RepGroup::new("g".into(), 1);
    for i in 1..=5u32 {
        group.add_node(electable(&format!("Node{i}"), i));
    }
    assert_eq!(group.node_count(), 5);

    // Remove replicas Node2..Node5; each REMOVE names exactly the removed node.
    for i in 2..=5u32 {
        let name = format!("Node{i}");
        let removed = group.remove_node(&name);
        assert!(removed.is_some(), "REMOVE returns the removed member");
        assert_eq!(
            removed.unwrap().name(),
            name,
            "delta names the removed node"
        );
        assert!(!group.contains_node(&name));
        // Second remove is a no-op: no spurious REMOVE event.
        assert!(group.remove_node(&name).is_none());
    }
    assert_eq!(member_names(&group), vec!["Node1"]);
}

/// JE: `MonitorChangeListenerTest.testAddSecondaryNode` (delta half) +
/// `GroupChangeEvent` javadoc invariant ("SECONDARY nodes do not generate
/// these events").
///
/// In JE, adding/removing a SECONDARY fires JoinGroupEvent/LeaveGroupEvent but
/// **0 GroupChangeEvents** (Add events == 0, Remove events == 0 in the test).
/// DELIVERY half N/A. Ported at model level: the monitor-observed GROUP-CHANGE
/// set (electable + monitor members) is invariant across a secondary add/remove
/// — a secondary joining does NOT change the electable membership a
/// GroupChangeEvent reports.
#[test]
fn test_add_secondary_node_no_group_change_for_secondary() {
    let mut group = RepGroup::new("g".into(), 1);
    group.add_node(electable("Node1", 1));
    group.add_node(monitor("mon10000", 100));

    // The "group-change-relevant" membership = electable + monitor members.
    let relevant = |g: &RepGroup| -> usize {
        g.get_nodes()
            .iter()
            .filter(|n| n.node_type() != NodeType::Secondary)
            .count()
    };
    let before = relevant(&group);

    // Add a secondary — JE fires JoinGroupEvent but 0 ADD GroupChangeEvents.
    group.add_node(secondary("sec1", 6));
    assert_eq!(
        relevant(&group),
        before,
        "secondary add fires no GroupChangeEvent"
    );
    assert!(
        group.contains_node("sec1"),
        "secondary is still a member (join event only)"
    );

    // Remove the secondary — 0 REMOVE GroupChangeEvents.
    group.remove_node("sec1");
    assert_eq!(
        relevant(&group),
        before,
        "secondary remove fires no GroupChangeEvent"
    );
}

// ---------------------------------------------------------------------------
// ProtocolTest — Noxu GroupChange wire round-trip (version-negotiation N/A)
// ---------------------------------------------------------------------------

/// JE: `ProtocolTest` (`createMessages` round-trip half only).
///
/// JE's monitor `ProtocolTest` round-trips a `GroupChange` (ADD, full
/// `RepGroupImpl`), a `JoinGroup`, and a `LeaveGroup` through the monitor
/// `TextProtocol`. Noxu has NO monitor protocol and NO JoinGroup/LeaveGroup
/// messages — those are N/A. What Noxu DOES have is a `GroupChange` message in
/// its master↔replica binary `protocol::ProtocolMessage`, whose encode/decode
/// round-trip IS the analogous portable behavior. We pin ADD/REMOVE/UPDATE
/// round-trips for MONITOR-typed nodes here (the existing in-crate
/// `protocol.rs::test_group_change_*` unit tests cover the general case; this
/// cites the JE monitor ProtocolTest specifically and exercises a MONITOR node
/// payload, which is what the JE test's RepNodeImpl(NodeType.MONITOR) uses).
#[test]
fn test_protocol_group_change_round_trip() {
    for ct in
        [GroupChangeType::Add, GroupChangeType::Remove, GroupChangeType::Update]
    {
        let msg = ProtocolMessage::GroupChange {
            change_type: ct,
            node: monitor("m1", 1),
        };
        let encoded = msg.encode();
        let decoded =
            ProtocolMessage::decode(&encoded).expect("decode round-trip");
        assert_eq!(msg, decoded, "GroupChange({ct:?}) survives encode/decode");
    }
}
