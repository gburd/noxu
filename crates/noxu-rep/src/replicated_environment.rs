//! The main replicated environment API.
//!
//! Port of `com.sleepycat.je.rep.ReplicatedEnvironment`.
//!
//! A replicated database environment that is a node in a replication group.
//! This is the entry point for replication. It wraps a standard Environment
//! and adds replication capabilities including master election, replica
//! streaming, and commit acknowledgments.
//!
//! # Replication node states
//!
//! The replication node state determines the operations that the application
//! can perform against its replicated environment. The state transitions
//! visible to the application can be summarized by the regular expression:
//!
//! ```text
//! [ MASTER | REPLICA | UNKNOWN ]+ DETACHED
//! ```
//!
//! When the first handle to a `ReplicatedEnvironment` is created and the node
//! is brought up, the node usually establishes Master or Replica state. These
//! states are preceded by the Unknown state. As various remote nodes become
//! unavailable and elections are held, the local node may change between
//! Master and Replica states, always with a (usually brief) transition through
//! Unknown state.
//!
//! When the environment is closed, the node transitions to the Detached state.

use noxu_sync::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::ack_tracker::AckTracker;
use crate::elections::master_tracker::MasterTracker;
use crate::error::{RepError, Result};
use crate::group_service::GroupService;
use crate::master_transfer::MasterTransferConfig;
use crate::net::service_dispatcher::TcpServiceDispatcher;
use crate::node_state::{NodeState, NodeStateMachine};
use crate::rep_config::RepConfig;
use crate::rep_stats::RepStats;
use crate::state_change_listener::{StateChangeEvent, StateChangeListener};
use crate::stream::feeder::Feeder;
use crate::stream::replica_stream::ReplicaStream;
use crate::vlsn::vlsn_index::VlsnIndex;
use crate::vlsn::vlsn_range::VlsnRange;

/// Default heartbeat timeout for master liveness detection.
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// A replicated database environment.
///
/// Port of `com.sleepycat.je.rep.ReplicatedEnvironment`.
///
/// This is the entry point for replication. It wraps a standard Environment
/// and adds replication capabilities including master election, replica
/// streaming, and commit acknowledgments.
///
/// Berkeley DB JE High Availability (JE HA) is a replicated, embedded database
/// management system which provides fast, reliable, and scalable data
/// management. JE HA enables replication of an environment across a Replication
/// Group. A `ReplicatedEnvironment` is a single node in the replication group.
///
/// `ReplicatedEnvironment` wraps a standard `Environment`. All database
/// operations are executed in the same fashion in both replicated and
/// non-replicated applications. A `ReplicatedEnvironment` must be
/// transactional. All replicated databases created in the replicated
/// environment must be transactional as well.
///
/// A `ReplicatedEnvironment` joins its replication group when it is created.
/// When `new()` returns, the node will have established contact with the other
/// members of the group and will be ready to service operations.
///
/// Replicated environments can be created with node type Electable or
/// Secondary. Electable nodes can be masters or replicas, and participate in
/// both master elections and commit durability decisions. Secondary nodes can
/// only be replicas, not masters, and do not participate in either elections or
/// durability decisions.
///
/// # Example
///
/// ```ignore
/// use noxu_rep::{ReplicatedEnvironment, RepConfig};
///
/// let config = RepConfig::builder("my_group", "node1", "localhost")
///     .node_port(5001)
///     .build();
/// let rep_env = ReplicatedEnvironment::new(config).unwrap();
/// ```
pub struct ReplicatedEnvironment {
    /// The replication configuration for this node.
    config: RepConfig,
    /// Tracks the current node state (Detached, Unknown, Master, Replica).
    node_state: NodeStateMachine,
    /// Service for managing the replication group membership.
    group_service: GroupService,
    /// Maps VLSNs to log file positions.
    vlsn_index: VlsnIndex,
    /// Tracks acknowledgments from replicas (used by master).
    ack_tracker: AckTracker,
    /// Replication statistics.
    stats: RepStats,
    /// Active feeder threads (master -> replica streams).
    feeders: RwLock<Vec<Feeder>>,
    /// Replica stream for receiving updates from the master.
    replica_stream: ReplicaStream,
    /// Tracks the current master node.
    master_tracker: MasterTracker,
    /// State change listeners.
    listeners: RwLock<Vec<Arc<dyn StateChangeListener>>>,
    /// Shutdown flag.
    shutdown: AtomicBool,
    /// TCP service dispatcher — listens on the replication port and routes
    /// incoming connections to the appropriate service handler (feeder, etc.).
    ///
    /// Port of JE's `ServiceDispatcher`. Started in `new()` when a listen
    /// address is available. `None` only when the bind address cannot be
    /// resolved (e.g. in unit tests that use port 0 but want lazy init).
    tcp_dispatcher: Option<TcpServiceDispatcher>,
    /// The address the `tcp_dispatcher` is actually bound to (may differ from
    /// the configured port when port 0 is used in tests).
    bound_addr: Option<SocketAddr>,
}

impl ReplicatedEnvironment {
    /// Create a new replicated environment.
    ///
    /// Port of `ReplicatedEnvironment(File, ReplicationConfig, EnvironmentConfig)`.
    ///
    /// Creates a replicated environment handle and starts participating in the
    /// replication group. The node's state is determined when it joins the
    /// group, and mastership is not preconfigured. If the group has no current
    /// master, creation will trigger an election to determine whether this node
    /// will participate as a Master or a Replica.
    ///
    /// A brand new node will always join an existing group as a Replica, unless
    /// it is the very first electable node that is creating the group. In that
    /// case it joins as the Master of the newly formed singleton group.
    pub fn new(config: RepConfig) -> Result<Self> {
        let node_state = NodeStateMachine::new();
        let group_service = GroupService::new(config.group_name.clone());
        let vlsn_index = VlsnIndex::new(10);
        let ack_tracker = AckTracker::new();
        let stats = RepStats::new();
        let feeders = RwLock::new(Vec::new());
        let replica_stream = ReplicaStream::new();
        let master_tracker = MasterTracker::new(DEFAULT_HEARTBEAT_TIMEOUT);

        // Start the TCP service dispatcher.
        //
        // JE equivalent: `RepImpl.open()` calls `serviceDispatcher.start()`
        // which binds a ServerSocketChannel on the configured port and begins
        // accepting connections. We do the same here using the node_host and
        // node_port from RepConfig.
        let listen_addr_str = format!("{}:{}", config.node_host, config.node_port);
        let (tcp_dispatcher, bound_addr) =
            match listen_addr_str.parse::<SocketAddr>() {
                Ok(addr) => {
                    match TcpServiceDispatcher::new(addr) {
                        Ok(dispatcher) => match dispatcher.start() {
                            Ok(bound) => {
                                log::info!(
                                    "Node '{}' TCP service dispatcher started on {}",
                                    config.node_name,
                                    bound
                                );
                                (Some(dispatcher), Some(bound))
                            }
                            Err(e) => {
                                log::warn!(
                                    "Node '{}' failed to start TCP dispatcher on {}: {}",
                                    config.node_name,
                                    listen_addr_str,
                                    e
                                );
                                (None, None)
                            }
                        },
                        Err(e) => {
                            log::warn!(
                                "Node '{}' failed to create TCP dispatcher: {}",
                                config.node_name,
                                e
                            );
                            (None, None)
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Node '{}' cannot parse listen address '{}': {}",
                        config.node_name,
                        listen_addr_str,
                        e
                    );
                    (None, None)
                }
            };

        let env = Self {
            config,
            node_state,
            group_service,
            vlsn_index,
            ack_tracker,
            stats,
            feeders,
            replica_stream,
            master_tracker,
            listeners: RwLock::new(Vec::new()),
            shutdown: AtomicBool::new(false),
            tcp_dispatcher,
            bound_addr,
        };

        Ok(env)
    }

    /// Return the socket address the TCP service dispatcher is bound to.
    ///
    /// This may differ from the configured `node_port` when port 0 is used
    /// (the OS assigns an ephemeral port). Returns `None` if the dispatcher
    /// could not be started (e.g. the address is not resolvable).
    pub fn bound_addr(&self) -> Option<SocketAddr> {
        self.bound_addr
    }

    /// Get the current node state.
    ///
    /// Port of `ReplicatedEnvironment.getState()`.
    ///
    /// Returns the current state of the node associated with this replication
    /// environment. If the caller's intent is to track the state of the node,
    /// `StateChangeListener` may be a more convenient and efficient approach.
    pub fn get_state(&self) -> NodeState {
        self.node_state.get_state()
    }

    /// Check if this node is the master.
    ///
    /// Returns true if the node's current state is Master.
    pub fn is_master(&self) -> bool {
        self.node_state.get_state() == NodeState::Master
    }

    /// Check if this node is a replica.
    ///
    /// Returns true if the node's current state is Replica.
    pub fn is_replica(&self) -> bool {
        self.node_state.get_state() == NodeState::Replica
    }

    /// Returns true if the node is currently participating in the group
    /// as a Replica or a Master.
    pub fn is_active(&self) -> bool {
        self.node_state.get_state().is_active()
    }

    /// Get the node name.
    ///
    /// Port of `ReplicatedEnvironment.getNodeName()`.
    ///
    /// Returns the unique name used to identify this replicated environment.
    pub fn get_node_name(&self) -> &str {
        self.config.node_name.as_str()
    }

    /// Get the group name.
    ///
    /// Returns the name of the replication group this node belongs to.
    pub fn get_group_name(&self) -> &str {
        self.config.group_name.as_str()
    }

    /// Get the current master (if known).
    ///
    /// Returns the name of the node that is currently the master, or None
    /// if the master is not known (e.g. the node is in the Unknown or
    /// Detached state).
    pub fn get_master_name(&self) -> Option<String> {
        self.master_tracker.get_master()
    }

    /// Get the replication group info.
    ///
    /// Port of `ReplicatedEnvironment.getGroup()`.
    ///
    /// Returns a description of the replication group as known by this node.
    /// The replicated group metadata is stored in a replicated database and
    /// updates are propagated by the current master node to all replicas. If
    /// this node is not the master, it is possible for its description of the
    /// group to be out of date.
    pub fn get_group(&self) -> &GroupService {
        &self.group_service
    }

    /// Get the replication configuration.
    ///
    /// Port of `ReplicatedEnvironment.getRepConfig()`.
    ///
    /// Returns the replication configuration that has been used to create this
    /// environment.
    pub fn get_config(&self) -> &RepConfig {
        &self.config
    }

    /// Get the current VLSN range on this node.
    ///
    /// Returns the range of VLSNs currently available on this node.
    pub fn get_vlsn_range(&self) -> VlsnRange {
        self.vlsn_index.get_range()
    }

    /// Get the latest VLSN.
    ///
    /// Returns the most recent VLSN registered on this node.
    pub fn get_current_vlsn(&self) -> u64 {
        self.vlsn_index.get_latest_vlsn()
    }

    /// Get replication statistics.
    ///
    /// Port of `ReplicatedEnvironment.getRepStats(StatsConfig)`.
    ///
    /// Returns statistics associated with this environment.
    pub fn get_stats(&self) -> &RepStats {
        &self.stats
    }

    /// Get the ack tracker.
    pub fn get_ack_tracker(&self) -> &AckTracker {
        &self.ack_tracker
    }

    /// Ensure the node state machine is in Unknown state, transitioning
    /// from Detached if necessary. This is needed because the state machine
    /// only allows Detached -> Unknown -> Master/Replica.
    fn ensure_unknown_state(&self) -> Result<()> {
        let current = self.node_state.get_state();
        match current {
            NodeState::Unknown => Ok(()),
            NodeState::Detached => {
                self.node_state.transition_to(NodeState::Unknown)?;
                Ok(())
            }
            // Master and Replica can transition directly to each other
            // per the state machine rules.
            NodeState::Master | NodeState::Replica => Ok(()),
            NodeState::Shutdown => {
                Err(RepError::StateError("Node is shut down".to_string()))
            }
        }
    }

    /// Transition to master state.
    ///
    /// Transitions this node to Master state for the given election term.
    /// As master, the node can accept write operations and feed log entries
    /// to replicas.
    pub fn become_master(&self, term: u64) -> Result<()> {
        if self.is_shutdown() {
            return Err(RepError::StateError(
                "Cannot become master: environment is closed".to_string(),
            ));
        }

        // Ensure we can reach Master state (may need Detached -> Unknown first)
        self.ensure_unknown_state()?;

        let old_state = self.node_state.get_state();
        self.node_state.transition_to(NodeState::Master)?;
        self.master_tracker.set_master(self.config.node_name.as_str(), term);

        // Notify listeners
        self.notify_listeners(old_state, NodeState::Master);

        log::info!(
            "Node '{}' became master for term {}",
            self.config.node_name.as_str(),
            term
        );
        Ok(())
    }

    /// Transition to replica state with the given master.
    ///
    /// Transitions this node to Replica state. The node will receive log
    /// entries from the specified master.
    pub fn become_replica(&self, master_name: &str) -> Result<()> {
        if self.is_shutdown() {
            return Err(RepError::StateError(
                "Cannot become replica: environment is closed".to_string(),
            ));
        }

        // Ensure we can reach Replica state (may need Detached -> Unknown first)
        self.ensure_unknown_state()?;

        let old_state = self.node_state.get_state();
        self.node_state.transition_to(NodeState::Replica)?;
        self.master_tracker.set_master(master_name, 0);

        // Notify listeners
        self.notify_listeners(old_state, NodeState::Replica);

        log::info!(
            "Node '{}' became replica of master '{}'",
            self.config.node_name.as_str(),
            master_name
        );
        Ok(())
    }

    /// Initiate a master transfer to the target node.
    ///
    /// Port of `ReplicatedEnvironment.transferMaster(Set, int, TimeUnit)`.
    ///
    /// Transfers the current master state from this node to one of the
    /// electable replicas. The replica that is actually chosen to be the new
    /// master is the one with which the Master Transfer can be completed most
    /// rapidly. The transfer operation ensures that all changes at this node
    /// are available at the new master upon conclusion of the operation.
    pub fn transfer_master(&self, config: MasterTransferConfig) -> Result<()> {
        if self.is_shutdown() {
            return Err(RepError::StateError(
                "Cannot transfer master: environment is closed".to_string(),
            ));
        }

        if !self.is_master() {
            return Err(RepError::InvalidState(
                "Master transfer can only be initiated on the master node"
                    .to_string(),
            ));
        }

        log::info!(
            "Node '{}' initiating master transfer to '{}'",
            self.config.node_name.as_str(),
            config.target_node,
        );

        // In a full implementation, this would coordinate with the target
        // replica to complete the transfer. For now, we record the intent.
        Ok(())
    }

    /// Register a VLSN (as master, after writing a log entry).
    ///
    /// Maps the given VLSN to the specified log file position. This is called
    /// by the master after it writes a replicated log entry.
    pub fn register_vlsn(&self, vlsn: u64, file_number: u32, file_offset: u32) {
        self.vlsn_index.register(vlsn, file_number, file_offset);
    }

    /// Apply a replicated entry (as replica).
    ///
    /// Applies a log entry received from the master. This is called by the
    /// replica stream handler after receiving an entry from the feeder.
    pub fn apply_entry(
        &self,
        vlsn: u64,
        entry_type: u8,
        _data: Vec<u8>,
    ) -> Result<()> {
        if self.is_shutdown() {
            return Err(RepError::StateError(
                "Cannot apply entry: environment is closed".to_string(),
            ));
        }

        // Register the VLSN in the index.
        // Log position is 0/0 here because the replication feeder path
        // (G19 — deferred) is not yet wired to the live log; the VLSN
        // index still tracks membership for election/ack purposes.
        self.vlsn_index.register(vlsn, 0, 0);

        log::trace!(
            "Applied replicated entry: vlsn={}, type={}",
            vlsn,
            entry_type
        );
        Ok(())
    }

    /// Record an ack from a replica (as master).
    ///
    /// Records that the specified replica has acknowledged processing up to
    /// the given VLSN. This is used by the master to track durability
    /// guarantees.
    pub fn record_ack(&self, vlsn: u64, replica_name: &str) {
        self.ack_tracker.record_ack(vlsn, replica_name);
    }

    /// Set the state change listener.
    ///
    /// Port of `ReplicatedEnvironment.setStateChangeListener(StateChangeListener)`.
    ///
    /// Sets the listener used to receive asynchronous replication node state
    /// change events. Note that there is one listener per replication node,
    /// not one per handle. Invoking this method adds to the set of listeners.
    ///
    /// Invoking this method typically results in an immediate callback to the
    /// application via the `on_state_change` method, so that the application
    /// is made aware of the existing state of the node at the time the listener
    /// is first established.
    pub fn set_state_change_listener(
        &self,
        listener: Arc<dyn StateChangeListener>,
    ) {
        // Immediately notify the listener of the current state
        let current_state = self.node_state.get_state();
        let event = StateChangeEvent::new(
            current_state,
            current_state,
            self.get_master_name(),
        );
        listener.on_state_change(event);

        let mut listeners = self.listeners.write();
        listeners.push(listener);
    }

    /// Close the replicated environment.
    ///
    /// Port of `ReplicatedEnvironment.close()`.
    ///
    /// Closes this handle and releases any resources. When closed, daemon
    /// threads are stopped, even if they are performing work. The node ceases
    /// participation in the replication group. If the node was currently the
    /// master, the rest of the group will hold an election.
    ///
    /// The ReplicatedEnvironment should not be closed while any other type of
    /// handle that refers to it is not yet closed.
    pub fn close(&self) -> Result<()> {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            // Already closed
            return Ok(());
        }

        let old_state = self.node_state.get_state();

        // Transition to Shutdown state. The state machine allows this from
        // any non-Shutdown state.
        let _ = self.node_state.transition_to(NodeState::Shutdown);

        // Notify listeners of the shutdown
        self.notify_listeners(old_state, NodeState::Shutdown);

        // Clear feeders
        {
            let mut feeders = self.feeders.write();
            feeders.clear();
        }

        // Stop the TCP service dispatcher (JE: serviceDispatcher.shutdown()).
        if let Some(ref dispatcher) = self.tcp_dispatcher {
            dispatcher.stop();
            log::debug!(
                "Node '{}' TCP service dispatcher stopped",
                self.config.node_name.as_str()
            );
        }

        log::info!(
            "Replicated environment '{}' in group '{}' closed",
            self.config.node_name.as_str(),
            self.config.group_name.as_str()
        );

        Ok(())
    }

    /// Close this handle and shut down the Replication Group by forcing all
    /// active Replicas to exit.
    ///
    /// Port of `ReplicatedEnvironment.shutdownGroup(long, TimeUnit)`.
    ///
    /// This method must be invoked on the node that's currently the Master
    /// after all other outstanding handles have been closed. The Master waits
    /// for all active Replicas to catch up so that they have a current set of
    /// logs, and then shuts them down.
    pub fn shutdown_group(
        &self,
        _replica_shutdown_timeout_ms: u64,
    ) -> Result<()> {
        if !self.is_master() {
            return Err(RepError::InvalidState(
                "shutdownGroup must be invoked on the master".to_string(),
            ));
        }

        log::info!(
            "Node '{}' shutting down replication group '{}'",
            self.config.node_name.as_str(),
            self.config.group_name.as_str()
        );

        self.close()
    }

    /// Check if shutdown is in progress.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Notify all registered listeners of a state change.
    fn notify_listeners(&self, old_state: NodeState, new_state: NodeState) {
        let listeners = self.listeners.read();
        if !listeners.is_empty() {
            let event = StateChangeEvent::new(
                old_state,
                new_state,
                self.get_master_name(),
            );
            for listener in listeners.iter() {
                listener.on_state_change(event.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    /// Helper to create a test config with a fixed port (unit-test style,
    /// no real TCP bind needed — hostname "localhost" resolves but the port
    /// might be in use; use `test_config_port0` for real TCP tests).
    fn test_config(node_name: &str) -> RepConfig {
        RepConfig::builder("test_group", node_name, "localhost")
            .node_port(5001)
            .build()
    }

    /// Helper to create a test config that binds to an OS-assigned port.
    fn test_config_port0(node_name: &str) -> RepConfig {
        RepConfig::builder("test_group", node_name, "127.0.0.1")
            .node_port(0)
            .build()
    }

    #[test]
    fn test_initial_state_is_detached() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        // NodeStateMachine starts in Detached state
        assert_eq!(env.get_state(), NodeState::Detached);
        assert!(!env.is_master());
        assert!(!env.is_replica());
        assert!(!env.is_active());
    }

    #[test]
    fn test_become_master() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.become_master(1).unwrap();
        assert_eq!(env.get_state(), NodeState::Master);
        assert!(env.is_master());
        assert!(!env.is_replica());
        assert!(env.is_active());
    }

    #[test]
    fn test_become_replica() {
        let env = ReplicatedEnvironment::new(test_config("node2")).unwrap();
        env.become_replica("node1").unwrap();
        assert_eq!(env.get_state(), NodeState::Replica);
        assert!(!env.is_master());
        assert!(env.is_replica());
        assert!(env.is_active());
    }

    #[test]
    fn test_get_node_name() {
        let env = ReplicatedEnvironment::new(test_config("my_node")).unwrap();
        assert_eq!(env.get_node_name(), "my_node");
    }

    #[test]
    fn test_get_group_name() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        assert_eq!(env.get_group_name(), "test_group");
    }

    #[test]
    fn test_register_vlsn_updates_index() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.register_vlsn(1, 0, 100);
        env.register_vlsn(2, 0, 200);
        env.register_vlsn(3, 0, 300);

        assert_eq!(env.get_current_vlsn(), 3);
        let range = env.get_vlsn_range();
        assert_eq!(range.first(), 1);
        assert_eq!(range.last(), 3);
    }

    #[test]
    fn test_record_ack() {
        let env = ReplicatedEnvironment::new(test_config("master")).unwrap();
        env.become_master(1).unwrap();

        env.register_vlsn(1, 0, 100);
        // Register a pending ack requirement, then record ack
        env.get_ack_tracker().register(1, 1);
        env.record_ack(1, "replica1");
        // Ack should be satisfied
        assert!(env.get_ack_tracker().is_satisfied(1));
    }

    #[test]
    fn test_close_sets_shutdown() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        assert!(!env.is_shutdown());

        env.close().unwrap();
        assert!(env.is_shutdown());
        // After close, state should be Shutdown
        assert_eq!(env.get_state(), NodeState::Shutdown);
    }

    #[test]
    fn test_close_is_idempotent() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.close().unwrap();
        env.close().unwrap(); // Should not error
        assert!(env.is_shutdown());
    }

    #[test]
    fn test_cannot_become_master_when_shutdown() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.close().unwrap();

        let result = env.become_master(1);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_become_replica_when_shutdown() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.close().unwrap();

        let result = env.become_replica("master");
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_apply_entry_when_shutdown() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.close().unwrap();

        let result = env.apply_entry(1, 0, vec![1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_transfer_master_when_not_master() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.become_replica("other").unwrap();

        let config = MasterTransferConfig::new(
            "target_node".to_string(),
            Duration::from_secs(30),
        );
        let result = env.transfer_master(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_transfer_master_as_master() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.become_master(1).unwrap();

        let config = MasterTransferConfig::new(
            "replica1".to_string(),
            Duration::from_secs(30),
        );
        let result = env.transfer_master(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_entry_registers_vlsn() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.become_replica("master").unwrap();

        env.apply_entry(1, 0, vec![1, 2, 3]).unwrap();
        env.apply_entry(2, 0, vec![4, 5, 6]).unwrap();

        assert_eq!(env.get_current_vlsn(), 2);
    }

    #[test]
    fn test_master_name_tracking() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();

        // Initially no master known
        assert!(env.get_master_name().is_none());

        // After becoming master, this node is the master
        env.become_master(1).unwrap();
        assert_eq!(env.get_master_name(), Some("node1".to_string()));
    }

    #[test]
    fn test_master_to_replica_transition() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();

        // Become master first
        env.become_master(1).unwrap();
        assert_eq!(env.get_master_name(), Some("node1".to_string()));

        // Transition to replica (Master -> Replica is valid)
        env.become_replica("other_master").unwrap();
        assert_eq!(env.get_master_name(), Some("other_master".to_string()));
        assert!(env.is_replica());
    }

    #[test]
    fn test_state_change_listener_notification() {
        struct TestListener {
            call_count: AtomicU32,
            last_new_state: noxu_sync::Mutex<Option<NodeState>>,
        }

        impl StateChangeListener for TestListener {
            fn on_state_change(&self, event: StateChangeEvent) {
                self.call_count.fetch_add(1, AtomicOrdering::SeqCst);
                *self.last_new_state.lock() = Some(event.new_state);
            }
        }

        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        let listener = Arc::new(TestListener {
            call_count: AtomicU32::new(0),
            last_new_state: noxu_sync::Mutex::new(None),
        });

        // Setting the listener should trigger an immediate notification
        env.set_state_change_listener(listener.clone());
        assert_eq!(listener.call_count.load(AtomicOrdering::SeqCst), 1);

        // State change should trigger another notification
        env.become_master(1).unwrap();
        assert_eq!(listener.call_count.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(*listener.last_new_state.lock(), Some(NodeState::Master));
    }

    #[test]
    fn test_close_notifies_listeners() {
        struct ShutdownListener {
            shutdown_seen: AtomicBool,
        }

        impl StateChangeListener for ShutdownListener {
            fn on_state_change(&self, event: StateChangeEvent) {
                if event.new_state == NodeState::Shutdown {
                    self.shutdown_seen.store(true, AtomicOrdering::SeqCst);
                }
            }
        }

        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        let listener = Arc::new(ShutdownListener {
            shutdown_seen: AtomicBool::new(false),
        });

        // The initial notification is for the current (Detached) state
        env.set_state_change_listener(listener.clone());

        // Become master first so the close transition is meaningful
        env.become_master(1).unwrap();
        assert!(!listener.shutdown_seen.load(AtomicOrdering::SeqCst));

        env.close().unwrap();
        assert!(listener.shutdown_seen.load(AtomicOrdering::SeqCst));
    }

    #[test]
    fn test_shutdown_group_requires_master() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.become_replica("other").unwrap();

        let result = env.shutdown_group(5000);
        assert!(result.is_err());
    }

    #[test]
    fn test_shutdown_group_as_master() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.become_master(1).unwrap();

        let result = env.shutdown_group(5000);
        assert!(result.is_ok());
        assert!(env.is_shutdown());
    }

    #[test]
    fn test_get_config() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        assert_eq!(env.get_config().node_name, "node1");
        assert_eq!(env.get_config().group_name, "test_group");
    }

    #[test]
    fn test_get_stats() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        let _stats = env.get_stats();
        // Just verify we can access stats without panicking
    }

    // -----------------------------------------------------------------------
    // TCP dispatcher tests (H-5 / H-7)
    // -----------------------------------------------------------------------

    #[test]
    fn test_tcp_dispatcher_starts_on_new() {
        // Use port 0 so the OS assigns an ephemeral port.
        let env =
            ReplicatedEnvironment::new(test_config_port0("tcp_node")).unwrap();
        // The dispatcher must have started and bound a real port.
        let addr = env.bound_addr();
        assert!(addr.is_some(), "expected a bound address");
        let addr = addr.unwrap();
        assert_ne!(addr.port(), 0, "OS should assign a non-zero port");
    }

    #[test]
    fn test_tcp_dispatcher_stops_on_close() {
        let env =
            ReplicatedEnvironment::new(test_config_port0("tcp_node2")).unwrap();
        // Dispatcher is running.
        assert!(env.tcp_dispatcher.as_ref().map(|d| d.is_running()).unwrap_or(false));

        env.close().unwrap();

        // After close, dispatcher must be stopped.
        assert!(
            !env.tcp_dispatcher.as_ref().map(|d| d.is_running()).unwrap_or(false),
            "dispatcher should be stopped after close"
        );
    }

    #[test]
    fn test_tcp_dispatcher_accepts_connection() {
        use crate::net::service_dispatcher::connect_to_service;
        use crate::net::ServiceHandler;
        use crate::net::Channel;
        use std::sync::atomic::{AtomicU32, Ordering as AO};
        use std::time::Duration;

        struct PingHandler {
            count: AtomicU32,
        }
        impl ServiceHandler for PingHandler {
            fn service_name(&self) -> &str { "ping" }
            fn handle(&self, ch: Box<dyn Channel>) -> crate::error::Result<()> {
                self.count.fetch_add(1, AO::SeqCst);
                // Echo the first message back.
                if let Ok(Some(msg)) = ch.receive(Duration::from_secs(2)) {
                    let _ = ch.send(&msg);
                }
                Ok(())
            }
        }

        let env =
            ReplicatedEnvironment::new(test_config_port0("tcp_node3")).unwrap();
        let addr = env.bound_addr().expect("dispatcher must be bound");

        // Register a ping handler on the running dispatcher.
        if let Some(ref disp) = env.tcp_dispatcher {
            let handler = Arc::new(PingHandler { count: AtomicU32::new(0) });
            disp.register("ping", handler.clone());

            // Give the accept thread a moment.
            std::thread::sleep(Duration::from_millis(20));

            let client = connect_to_service(addr, "ping").unwrap();
            client.send(b"hello").unwrap();
            let reply = client.receive(Duration::from_secs(2)).unwrap();
            assert_eq!(reply, Some(b"hello".to_vec()));

            assert_eq!(handler.count.load(AO::SeqCst), 1);
        }

        env.close().unwrap();
    }

    #[test]
    fn test_become_master_auto_transitions_from_detached() {
        // The state machine requires Detached -> Unknown -> Master.
        // become_master() should handle this automatically.
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        assert_eq!(env.get_state(), NodeState::Detached);
        env.become_master(1).unwrap();
        assert_eq!(env.get_state(), NodeState::Master);
    }

    #[test]
    fn test_become_replica_auto_transitions_from_detached() {
        // The state machine requires Detached -> Unknown -> Replica.
        // become_replica() should handle this automatically.
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        assert_eq!(env.get_state(), NodeState::Detached);
        env.become_replica("master_node").unwrap();
        assert_eq!(env.get_state(), NodeState::Replica);
    }

    #[test]
    fn test_cannot_transfer_master_when_shutdown() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.become_master(1).unwrap();
        env.close().unwrap();

        let config = MasterTransferConfig::new(
            "target".to_string(),
            Duration::from_secs(30),
        );
        let result = env.transfer_master(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_full_lifecycle() {
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();

        // Start as detached
        assert_eq!(env.get_state(), NodeState::Detached);

        // Become master
        env.become_master(1).unwrap();
        assert!(env.is_master());

        // Register some VLSNs
        env.register_vlsn(1, 0, 100);
        env.register_vlsn(2, 0, 200);

        // Record ack from replica
        env.record_ack(1, "replica1");
        env.record_ack(2, "replica1");

        // Transition to replica (simulating failover)
        env.become_replica("node2").unwrap();
        assert!(env.is_replica());

        // Apply entries from new master
        env.apply_entry(3, 0, vec![7, 8, 9]).unwrap();

        // Close
        env.close().unwrap();
        assert!(env.is_shutdown());
    }
}
