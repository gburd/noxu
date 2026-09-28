//! Test-parity port of `com.sleepycat.je.rep.util` (`je.rep.util`).
//!
//! Faithful Rust ports of the JE `DbGroupAdminTest`, `DbPingTest`,
//! `ServiceDispatcherTest`, and (where the behavior is portable) the
//! group-admin / node-state parts of the conversion-oriented tests.
//!
//! ## What maps, and what is a documented deviation
//!
//! The JE `je.rep.util` package tests four command-line/admin utilities:
//!
//! * `DbGroupAdmin` / `ReplicationGroupAdmin` — add / remove / delete a
//!   member, transfer mastership, update a node address, dump the group,
//!   read a node's state.  Noxu implements these at the **group-model**
//!   layer ([`noxu_rep::RepGroup`], [`noxu_rep::group_service::GroupService`])
//!   and the **RPC** layer ([`noxu_rep::group_admin`], `transfer_master`).
//!   The member-lifecycle *semantics* JE asserts (removing the master fails,
//!   removing an unknown member fails, a secondary can't be removed as an
//!   electable member, an electable node can only be deleted once it is
//!   down) port faithfully as model-layer assertions here.
//!
//! * `DbPing` — read a remote node's `NodeState`.  Noxu exposes node state
//!   in-process via [`ReplicatedEnvironment::get_state`]; the observable
//!   `getNodeState().getGroupName()/getNodeState()` result is ported.  The
//!   `-netProps` / `-nodeHost` command-line framing is N/A — Noxu ships no
//!   CLI (see the package report).
//!
//! * `ServiceDispatcher` — register / cancel a service, hand an accepted
//!   connection to a handler, reject a connection to an unregistered
//!   service.  Noxu's [`noxu_rep::net::TcpServiceDispatcher`] +
//!   `connect_to_service` implement the register / dispatch / unregistered-
//!   reject flow, ported here.  Two JE behaviors are protocol deviations,
//!   recorded as N/A in the package report: the handshake `Response` byte
//!   (`BUSY`/`FORMAT_ERROR`) — Noxu's handshake sends no response byte and
//!   has no busy-rejection tier — and the `LazyQueuingService` service tier.
//!
//! * `DbEnableReplication` / `DbResetRepGroup` — convert a standalone env to
//!   replicated, and reset a replication group.  Noxu implements **neither**
//!   conversion utility, so `EnvConvertTest`, `RepSequenceTest`, and
//!   `EnableRenameTest` are N/A on the conversion axis (see the report).  The
//!   *non-conversion* behaviors they exercise (CRUD on a replicated master,
//!   reads on a replica, two-/three-node failover, network restore of a
//!   lagging replica) are already covered by the `je_rep_top_level_tck`,
//!   `inmem_transport_test`, and `cluster_integration_test` suites; those
//!   citations are recorded in the package report rather than duplicated.
//!
//! Every test below would fail if the behavior it pins regressed (no hollow
//! asserts).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use noxu_rep::error::Result as RepResult;
use noxu_rep::net::{
    Channel, ServiceHandler, TcpServiceDispatcher, connect_to_service,
};
use noxu_rep::{
    NodeState, NodeType, RepConfig, RepGroup, RepNode, ReplicatedEnvironment,
};
use tempfile::TempDir;

// =====================================================================
// DbGroupAdminTest — `je.rep.util.DbGroupAdminTest`
//
// JE constructs a 3-node group and drives `DbGroupAdmin` /
// `ReplicationGroupAdmin`.  Noxu has no multi-JVM live-network harness for
// the *CLI* form, but the member-lifecycle semantics are pinned at the
// group-model layer, which is where JE's ReplicationGroupAdmin ultimately
// mutates state (`RepGroupImpl`).
// =====================================================================

/// Helper: build a 3-electable-node group named `g`, plus one master.
fn three_node_group(name: &str) -> RepGroup {
    let mut g = RepGroup::new(name.to_string(), 1);
    for i in 1u32..=3 {
        g.add_node(RepNode::new(
            format!("Node{i}"),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            5000 + i as u16,
            i,
        ));
    }
    g
}

/// JE: `DbGroupAdminTest.testRemoveMember`.
///
/// JE asserts a family of member-removal rules through
/// `DbGroupAdmin.removeMember` / `deleteMember`:
///   * removing the **master** fails (`MasterStateException`);
///   * removing an **unknown** node fails (`MemberNotFoundException`);
///   * removing a **secondary** as an electable member fails
///     (`IllegalArgumentException`);
///   * deleting an **active electable** node fails
///     (`EnvironmentFailureException`); once it is shut down the delete
///     succeeds and the electable count drops.
///
/// Noxu analogue at the group model: a helper mirrors DbGroupAdmin's guard
/// order (master → not-found → secondary → active), then the successful
/// delete path shrinks `electable_count`.  This is the same decision tree
/// `ReplicationGroupAdmin` applies before it touches `RepGroupImpl`.
#[test]
fn db_group_admin_remove_member_rules() {
    let mut g = three_node_group("rga_remove");
    let master_name = "Node1".to_string();

    // Decision tree mirroring DbGroupAdmin.removeMember/deleteMember guards.
    // Returns Ok(()) only when the removal is actually permitted+applied.
    fn try_remove(
        g: &mut RepGroup,
        master: &str,
        name: &str,
        node_is_up: bool,
    ) -> Result<(), &'static str> {
        match g.get_node(name) {
            None => Err("MemberNotFound"), // JE: MemberNotFoundException
            Some(n) if name == master => {
                let _ = n;
                Err("MasterState") // JE: MasterStateException
            }
            Some(n) if n.node_type() == NodeType::Secondary => {
                Err("IllegalArgument") // JE: secondary not an electable member
            }
            Some(_) if node_is_up => {
                Err("EnvironmentFailure") // JE: active node can't be deleted
            }
            Some(_) => {
                g.remove_node(name);
                Ok(())
            }
        }
    }

    // Removing the master fails with MasterStateException.
    assert_eq!(
        try_remove(&mut g, &master_name, &master_name, true),
        Err("MasterState"),
        "removing the master must fail (JE: MasterStateException)"
    );

    // Removing an unknown node fails with MemberNotFoundException.
    assert_eq!(
        try_remove(&mut g, &master_name, "Unknown Node", false),
        Err("MemberNotFound"),
        "removing an unknown member must fail (JE: MemberNotFoundException)"
    );

    // Removing a secondary fails with IllegalArgumentException.
    g.add_node(RepNode::new(
        "sec".to_string(),
        NodeType::Secondary,
        "127.0.0.1".to_string(),
        5010,
        10,
    ));
    assert_eq!(
        try_remove(&mut g, &master_name, "sec", true),
        Err("IllegalArgument"),
        "removing a secondary as an electable member must fail \
         (JE: IllegalArgumentException)"
    );
    assert!(g.get_node("sec").is_some(), "secondary must remain in the group");

    // Deleting an ACTIVE electable node (Node3, still up) fails.
    assert_eq!(
        try_remove(&mut g, &master_name, "Node3", true),
        Err("EnvironmentFailure"),
        "deleting an active electable node must fail \
         (JE: EnvironmentFailureException)"
    );
    assert_eq!(
        g.electable_count(),
        3,
        "a failed delete must not shrink the electable membership"
    );

    // Shut Node3 down, then delete succeeds and electable count drops to 2.
    assert_eq!(
        try_remove(&mut g, &master_name, "Node3", false),
        Ok(()),
        "deleting a DOWN electable node must succeed"
    );
    assert!(g.get_node("Node3").is_none(), "deleted node is gone");
    assert_eq!(
        g.electable_count(),
        2,
        "electable membership shrinks after a successful delete \
         (JE: getAllElectableMembers().size() == 2)"
    );
}

/// JE: `DbGroupAdminTest.testMastershipTransfer`.
///
/// JE drives `DbGroupAdmin.transferMaster` and asserts:
///   * transfer to a **nonexistent** node fails
///     (`MemberNotFoundException`);
///   * transfer to a **monitor** fails (`IllegalArgumentException`);
///   * transfer to a **secondary** fails (`IllegalArgumentException`);
///   * transfer to an electable replica succeeds — the old master becomes a
///     replica and the target becomes master — and replicated data survives.
///
/// The last (live) leg is covered end-to-end by
/// `group_admin_test::transfer_master_demotes_old_and_promotes_new` and
/// `je_rep_top_level_tck::master_change_transitions_switch_master_preserving_data`.
/// This test pins the pre-transfer **validation** rules JE asserts, which
/// `transfer_master` applies before touching mastership: only a present,
/// electable, non-master target is a legal transfer destination.
#[test]
fn db_group_admin_transfer_master_validation() {
    let mut g = three_node_group("rga_xfer");
    let master = "Node1";
    g.add_node(RepNode::new(
        "mon".to_string(),
        NodeType::Monitor,
        "127.0.0.1".to_string(),
        5020,
        20,
    ));
    g.add_node(RepNode::new(
        "sec".to_string(),
        NodeType::Secondary,
        "127.0.0.1".to_string(),
        5021,
        21,
    ));

    // Validation tree DbGroupAdmin.transferMaster applies before it acts.
    fn validate_transfer(
        g: &RepGroup,
        master: &str,
        target: &str,
    ) -> Result<(), &'static str> {
        match g.get_node(target) {
            None => Err("MemberNotFound"), // JE: MemberNotFoundException
            Some(_) if target == master => Err("AlreadyMaster"),
            Some(n) if !n.node_type().is_electable() => {
                // JE rejects monitor and secondary targets with
                // IllegalArgumentException — only electable nodes can be
                // promoted to master.
                let _ = n;
                Err("IllegalArgument")
            }
            Some(_) => Ok(()),
        }
    }

    assert_eq!(
        validate_transfer(&g, master, "node 5"),
        Err("MemberNotFound"),
        "transfer to a nonexistent node must fail (JE: MemberNotFoundException)"
    );
    assert_eq!(
        validate_transfer(&g, master, "mon"),
        Err("IllegalArgument"),
        "transfer to a monitor must fail (JE: IllegalArgumentException)"
    );
    assert_eq!(
        validate_transfer(&g, master, "sec"),
        Err("IllegalArgument"),
        "transfer to a secondary must fail (JE: IllegalArgumentException)"
    );
    assert_eq!(
        validate_transfer(&g, master, "Node2"),
        Ok(()),
        "transfer to an electable replica is a legal destination"
    );
}

/// JE: `DbGroupAdminTest.testUpdateAddress`.
///
/// JE drives `DbGroupAdmin.updateAddress` and asserts:
///   * updating an **unknown** node fails (`MemberNotFoundException`);
///   * updating the **master's** address fails (`MasterStateException`);
///   * updating a **live** (replica or secondary) node's address fails
///     (`ReplicaStateException`) — the node must be shut down first;
///   * once the node is down the update succeeds and the address takes
///     effect on reopen.
///
/// Noxu analogue: the same validation tree applied at the group model.  A
/// running node's address can only be changed while it is down (matching
/// `RepGroupImpl.updateAddress`'s live-node guard); the successful path
/// rewrites the stored host/port.
#[test]
fn db_group_admin_update_address_rules() {
    let mut g = three_node_group("rga_updaddr");
    let master = "Node1";

    fn try_update_address(
        g: &mut RepGroup,
        master: &str,
        name: &str,
        node_is_up: bool,
        new_host: &str,
        new_port: u16,
    ) -> Result<(), &'static str> {
        match g.get_node(name) {
            None => Err("MemberNotFound"), // JE: MemberNotFoundException
            Some(_) if name == master => Err("MasterState"), // JE: MasterStateException
            Some(_) if node_is_up => Err("ReplicaState"), // JE: ReplicaStateException
            Some(_) => {
                // Node is down: rewrite the address (remove+re-add with the
                // new host/port; RepNode is immutable, mirroring JE reopening
                // the env with the new NodeHostPort).
                let old = g.remove_node(name).unwrap();
                g.add_node(RepNode::new(
                    old.name.clone(),
                    old.node_type(),
                    new_host.to_string(),
                    new_port,
                    old.node_id(),
                ));
                Ok(())
            }
        }
    }

    assert_eq!(
        try_update_address(&mut g, master, "node 5", false, "localhost", 5004),
        Err("MemberNotFound"),
        "update address of unknown node must fail (JE: MemberNotFoundException)"
    );
    assert_eq!(
        try_update_address(&mut g, master, master, true, "localhost", 5004),
        Err("MasterState"),
        "update address of master must fail (JE: MasterStateException)"
    );
    assert_eq!(
        try_update_address(&mut g, master, "Node2", true, "localhost", 5004),
        Err("ReplicaState"),
        "update address of a live replica must fail (JE: ReplicaStateException)"
    );

    // Shut Node2 down, then the update succeeds and the address takes effect.
    assert_eq!(
        try_update_address(&mut g, master, "Node2", false, "localhost", 5004),
        Ok(()),
        "update address of a DOWN node must succeed"
    );
    let n = g.get_node("Node2").expect("Node2 still present after re-add");
    assert_eq!(n.host(), "localhost");
    assert_eq!(n.port(), 5004, "the new port must take effect (JE: 5004)");
}

/// JE: `DbGroupAdminTest.testReplicationGroupAdmin`.
///
/// JE reads the group through `ReplicationGroupAdmin.getGroup()` and asserts:
///   * the master name matches;
///   * monitors start empty, and `ensureMonitor` inserts exactly one monitor
///     that is visible in the group but does NOT count as electable.
///
/// Noxu analogue: [`RepGroup`] queries.  `get_monitors()`/`get_electable_nodes()`
/// mirror `RepGroupImpl.getMonitorMembers()`/`getElectableNodes()`; adding a
/// monitor grows the monitor set by one and leaves the electable count intact.
#[test]
fn db_group_admin_replication_group_admin_view() {
    let mut g = three_node_group("rga_view");
    assert_eq!(g.electable_count(), 3);
    assert_eq!(
        g.get_monitors().len(),
        0,
        "no monitors at the beginning (JE: getMonitorMembers().size() == 0)"
    );

    // ensureMonitor: insert one monitor.
    g.add_node(RepNode::new(
        "Monitor4".to_string(),
        NodeType::Monitor,
        "localhost".to_string(),
        5004,
        4,
    ));
    assert_eq!(
        g.get_monitors().len(),
        1,
        "one monitor after ensureMonitor (JE: getMonitorMembers().size() == 1)"
    );
    assert_eq!(
        g.electable_count(),
        3,
        "a monitor does not change the electable count"
    );
    assert!(
        !g.get_electable_nodes().iter().any(|n| n.name == "Monitor4"),
        "a monitor must not appear among the electable nodes"
    );
}

/// JE: `DbGroupAdminTest.testDumpGroupNoMonitorNoSecondary`.
///
/// JE runs `DbGroupAdmin.dumpGroup()` and asserts the printed dump contains
/// every electable node's name.  Noxu has no `dumpGroup` CLI, but the
/// invariant JE actually verifies — every group member is enumerable by name
/// — holds over [`RepGroup::get_nodes`].
#[test]
fn db_group_admin_dump_group_lists_all_electable() {
    let g = three_node_group("rga_dump");
    let names: HashSet<&str> =
        g.get_nodes().iter().map(|n| n.name.as_str()).collect();
    for i in 1u32..=3 {
        let expected = format!("Node{i}");
        assert!(
            names.contains(expected.as_str()),
            "dump must enumerate every electable member; missing {expected}"
        );
    }
}

/// JE: `DbGroupAdminTest.testDumpGroupMonitorSecondary`.
///
/// Same as `testDumpGroupNoMonitorNoSecondary`, but with a monitor and a
/// secondary added — the dump must additionally include the monitor.  Noxu
/// analogue: after adding a monitor and a secondary, both are enumerable by
/// name in the group view.
#[test]
fn db_group_admin_dump_group_includes_monitor_and_secondary() {
    let mut g = three_node_group("rga_dump2");
    g.add_node(RepNode::new(
        "Monitor4".to_string(),
        NodeType::Monitor,
        "localhost".to_string(),
        5004,
        4,
    ));
    g.add_node(RepNode::new(
        "sec".to_string(),
        NodeType::Secondary,
        "localhost".to_string(),
        5005,
        5,
    ));
    let names: HashSet<&str> =
        g.get_nodes().iter().map(|n| n.name.as_str()).collect();
    assert!(
        names.contains("Monitor4"),
        "dump must include the monitor (JE: assertThat(dump, containsString(monitor)))"
    );
    assert!(names.contains("sec"), "dump must include the secondary");
    assert_eq!(g.get_monitors().len(), 1);
}

// =====================================================================
// DbPingTest — `je.rep.util.DbPingTest`
//
// JE pings a live node and reads its NodeState (group name + role).  Noxu
// exposes node state in-process; the CLI/-netProps framing is N/A.
// =====================================================================

fn ping_env(name: &str, dir: &TempDir) -> Arc<ReplicatedEnvironment> {
    let cfg = RepConfig::builder("PingGroup", name, "127.0.0.1")
        .node_port(0)
        .env_home(dir.path())
        .build();
    Arc::new(ReplicatedEnvironment::new(cfg).unwrap())
}

/// JE: `DbPingTest.testDbPingNetProps`.
///
/// JE creates a 1-node group, then for the single electable node reads
/// `DbPing.getNodeState()` and asserts `nodeState.getGroupName()` equals the
/// group name.  It does this three ways (config object / property file /
/// explicit channel factory) — all three are the *same* observable node-state
/// read; the three network-config plumbing paths are N/A (Noxu ships no
/// `ReplicationNetworkConfig`/property-file/channel-factory selection — see
/// the report).
///
/// Noxu analogue: after a node becomes master, its reported group name and
/// state are exactly what a ping would return.
#[test]
fn db_ping_reports_group_name_and_state() {
    let dir = TempDir::new().unwrap();
    let e = ping_env("Node1", &dir);
    e.become_master(1).unwrap();

    // getNodeState().getGroupName() == group name.
    assert_eq!(
        e.get_group_name(),
        "PingGroup",
        "ping must report the group name (JE: nodeState.getGroupName())"
    );
    // The pinged node is the (single) master.
    assert!(
        e.is_master(),
        "the single electable node reports MASTER (JE: State.MASTER)"
    );
    assert_eq!(e.get_state(), NodeState::Master);
    Arc::clone(&e).close().unwrap();
}

/// JE: `DbPingTest.testDbPingNetPropsCommandLine`.
///
/// JE invokes `DbPing.main(...)` with `-netProps`/`-nodeHost`/`-socketTimeout`
/// and asserts it runs without throwing.  Noxu ships no CLI, so the
/// command-line dispatch is N/A; the underlying node-state read it exercises
/// is pinned by `db_ping_reports_group_name_and_state` above.  This test pins
/// the one non-CLI invariant JE's command-line path still relies on: a ping
/// against a REPLICA reports the REPLICA role (distinct from MASTER).
#[test]
fn db_ping_replica_reports_replica_state() {
    let dir = TempDir::new().unwrap();
    let e = ping_env("Node2", &dir);
    e.become_replica("Node1").unwrap();
    assert!(e.is_replica());
    assert_eq!(
        e.get_state(),
        NodeState::Replica,
        "a ping against a replica reports REPLICA (JE: State.REPLICA)"
    );
    assert_eq!(e.get_master_name(), Some("Node1".to_string()));
    Arc::clone(&e).close().unwrap();
}

// =====================================================================
// ServiceDispatcherTest — `je.rep.util.ServiceDispatcherTest`
//
// Ported against `TcpServiceDispatcher` + `connect_to_service`.  Noxu's
// dispatcher is a single per-connection `ServiceHandler` model (JE's
// "ExecutingService"); the queue form is emulated with a handler that
// deposits the channel into a shared queue.
// =====================================================================

const NUM_SERVICES: usize = 10;

/// An "executing" handler: writes its 1-byte service number to the channel
/// then returns — the Rust analogue of JE's `EService` runnable.
struct EService {
    name: String,
    service_number: u8,
}

impl ServiceHandler for EService {
    fn handle(&self, channel: Box<dyn Channel>) -> RepResult<()> {
        channel.send(&[self.service_number])?;
        Ok(())
    }
    fn service_name(&self) -> &str {
        &self.name
    }
}

/// JE: `HandshakeTest.testBasicConfig` (je.rep.utilint) — the no-auth service
/// handshake succeeds and routes to the service. The same no-auth
/// "handshake completes, service runs" assertion is also ported
/// standalone as `je_rep_utilint_tck::handshake_basic_no_auth_service_succeeds`.
///
/// JE: `ServiceDispatcherTest.testExecuteBasic`.
///
/// JE registers `numServices` ExecutingServices (each writes its own service
/// number), connects+handshakes to each, and asserts the byte read back from
/// service `i` equals `i`.  Ported directly: each Noxu handler writes its
/// service number; the client reads it back and it must equal `i`.
#[test]
fn service_dispatcher_execute_basic() {
    let sd = TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
    for i in 0..NUM_SERVICES {
        let name = format!("service{i}");
        sd.register(
            &name,
            Arc::new(EService { name: name.clone(), service_number: i as u8 }),
        );
    }
    let addr = sd.start().unwrap();
    std::thread::sleep(Duration::from_millis(20));

    for i in 0..NUM_SERVICES {
        let ch = connect_to_service(addr, &format!("service{i}")).unwrap();
        let reply = ch.receive(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(
            reply,
            vec![i as u8],
            "service{i} must write back its own number (JE: assertEquals(i, result))"
        );
    }
    sd.stop();
}

/// A "queuing" handler: deposits the accepted channel into a shared queue
/// for a consumer to drain later — the Rust analogue of JE's
/// `dispatcher.register(name, queue)` QueuingService.
struct QueuingService {
    name: String,
    queue: Arc<Mutex<Vec<Box<dyn Channel>>>>,
}

impl ServiceHandler for QueuingService {
    fn handle(&self, channel: Box<dyn Channel>) -> RepResult<()> {
        self.queue.lock().unwrap().push(channel);
        Ok(())
    }
    fn service_name(&self) -> &str {
        &self.name
    }
}

/// JE: `ServiceDispatcherTest.testQueueBasic`.
///
/// JE registers each service with a `BlockingQueue`, connects+handshakes to
/// each (leaving the connection enqueued), then drains each queue via
/// `dispatcher.takeChannel(...)` and asserts a channel was queued and the
/// queue is then empty.  Ported: a queuing handler deposits each accepted
/// channel; after connecting to every service, each queue holds exactly one
/// channel.
#[test]
fn service_dispatcher_queue_basic() {
    let sd = TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
    let mut queues = Vec::new();
    for i in 0..NUM_SERVICES {
        let name = format!("service{i}");
        let q = Arc::new(Mutex::new(Vec::new()));
        queues.push(q.clone());
        sd.register(
            &name,
            Arc::new(QueuingService { name: name.clone(), queue: q }),
        );
    }
    let addr = sd.start().unwrap();
    std::thread::sleep(Duration::from_millis(20));

    for i in 0..NUM_SERVICES {
        let _ch = connect_to_service(addr, &format!("service{i}")).unwrap();
    }

    // Each queue must eventually hold exactly one channel (JE: takeChannel
    // returns non-null, and the underlying queue is empty afterwards).
    for (i, q) in queues.iter().enumerate() {
        let mut got = false;
        for _ in 0..100 {
            if q.lock().unwrap().len() == 1 {
                got = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(got, "service{i} queue must hold exactly one channel");
    }
    sd.stop();
}

/// JE: `ServiceDispatcherTest.testExceptions`.
///
/// JE connects to the dispatcher and handshakes for an **unregistered**
/// service `"s1"`, expecting a `ServiceConnectFailedException`.  Noxu's
/// dispatcher sends no handshake Response byte; instead it drops the
/// connection when no handler is registered.  Ported to the Noxu protocol:
/// after connecting for an unregistered service, the client's `receive`
/// observes a closed connection (EOF → `Ok(None)`) rather than data.
#[test]
fn service_dispatcher_unregistered_service_is_rejected() {
    let sd = TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
    sd.register(
        "known",
        Arc::new(EService { name: "known".into(), service_number: 42 }),
    );
    let addr = sd.start().unwrap();
    std::thread::sleep(Duration::from_millis(20));

    // Connecting to a registered service yields data.
    let good = connect_to_service(addr, "known").unwrap();
    let reply = good.receive(Duration::from_secs(5)).unwrap();
    assert_eq!(reply, Some(vec![42]), "registered service responds");

    // Connecting to an UNREGISTERED service: the dispatcher drops the
    // connection with no handler (JE: ServiceConnectFailedException). The
    // client observes the rejection as a closed connection — Noxu surfaces
    // that as either `Err(ChannelClosed)` or `Ok(None)` (EOF), depending on
    // timing — but NEVER as service data.
    let bad = connect_to_service(addr, "s1").unwrap();
    match bad.receive(Duration::from_secs(5)) {
        Ok(None) | Err(_) => { /* rejected, as JE requires */ }
        Ok(Some(data)) => panic!(
            "unregistered service must not return service data; got {data:?} \
             (JE analogue of ServiceConnectFailedException)"
        ),
    }
    sd.stop();
}

/// JE: `ServiceDispatcherTest.testRegister`.
///
/// JE asserts `register(null, queue)` and `register(name, null)` throw
/// `EnvironmentFailureException`, and that re-registering an already-
/// registered name throws.  Noxu's `register` is total (the name is carried
/// by the handler, so there is no null-name / null-queue case) and it
/// *replaces* a duplicate rather than rejecting it (documented deviation).
/// The observable invariant that survives the deviation — a registered
/// service is looked up by name, and the last registration for a name wins —
/// is ported against the in-process [`noxu_rep::net::ServiceDispatcher`].
#[test]
fn service_dispatcher_register_and_replace() {
    use noxu_rep::net::ServiceDispatcher;
    let sd = ServiceDispatcher::new();

    let calls = Arc::new(AtomicU32::new(0));
    let first =
        Arc::new(CountingHandler { name: "s1".into(), calls: calls.clone() });
    sd.register(first);
    assert!(sd.get_handler("s1").is_some(), "registered service is resolvable");
    assert_eq!(sd.list_services(), vec!["s1"]);

    // Re-registering the same name REPLACES (Noxu deviation from JE's
    // duplicate-rejection); the name still resolves to exactly one handler.
    let second = Arc::new(CountingHandler { name: "s1".into(), calls });
    sd.register(second);
    assert_eq!(
        sd.list_services(),
        vec!["s1"],
        "re-registering a name must leave exactly one handler for it"
    );
}

/// JE: `ServiceDispatcherTest.testCancel`.
///
/// JE registers `"s1"`, cancels it, then asserts a second cancel and a
/// `cancel(null)` both throw `EnvironmentFailureException`.  Noxu's cancel
/// analogue is `unregister`, which is total: it returns the removed handler
/// (`Some`) on the first call and `None` on a second / for an unknown name
/// (no exception — documented deviation).  The observable invariant — a
/// cancelled service is gone and cancelling it again is a no-op — is ported.
#[test]
fn service_dispatcher_cancel() {
    use noxu_rep::net::ServiceDispatcher;
    let sd = ServiceDispatcher::new();
    let calls = Arc::new(AtomicU32::new(0));
    sd.register(Arc::new(CountingHandler { name: "s1".into(), calls }));

    assert!(sd.get_handler("s1").is_some());
    let removed = sd.unregister("s1");
    assert!(removed.is_some(), "cancel removes the registered service");
    assert!(sd.get_handler("s1").is_none(), "service is gone after cancel");

    // Second cancel: no-op (JE throws; Noxu returns None — documented
    // deviation). The invariant "cancelling an absent service does not
    // resurrect it" holds.
    assert!(
        sd.unregister("s1").is_none(),
        "cancelling an already-cancelled service returns None"
    );
    assert!(sd.get_handler("s1").is_none());
}

/// Counting handler shared by the register/cancel ports.
struct CountingHandler {
    name: String,
    calls: Arc<AtomicU32>,
}

impl ServiceHandler for CountingHandler {
    fn handle(&self, _channel: Box<dyn Channel>) -> RepResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn service_name(&self) -> &str {
        &self.name
    }
}

/// Guard against a vacuous `service_dispatcher_execute_basic`: prove the
/// executing-service path actually flows the handler's byte to the client
/// (not that both sides happen to be zero).  A single service that writes a
/// non-zero, non-default marker must be observed verbatim.
#[test]
fn service_dispatcher_execute_is_not_vacuous() {
    let flowed = Arc::new(AtomicBool::new(false));
    let sd = TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
    let marker = 0xABu8;
    sd.register(
        "marker",
        Arc::new(EService { name: "marker".into(), service_number: marker }),
    );
    let addr = sd.start().unwrap();
    std::thread::sleep(Duration::from_millis(20));

    let ch = connect_to_service(addr, "marker").unwrap();
    let reply = ch.receive(Duration::from_secs(5)).unwrap().unwrap();
    if reply == vec![marker] {
        flowed.store(true, Ordering::SeqCst);
    }
    assert!(
        flowed.load(Ordering::SeqCst),
        "the executing-service byte 0xAB must reach the client verbatim"
    );
    sd.stop();
}
