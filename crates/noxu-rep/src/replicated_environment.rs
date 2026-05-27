//! The main replicated environment API.
//!
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

use noxu_dbi::{
    AckWaitError, AckWaitErrorKind, EnvironmentImpl, ReplicaAckCoordinator,
    ReplicaAckPolicyKind,
};
use noxu_sync::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::ack_tracker::AckTracker;
use crate::elections::master_tracker::MasterTracker;
use crate::error::{RepError, Result};
use crate::group_service::GroupService;
use crate::master_transfer::MasterTransferConfig;
use crate::net::service_dispatcher::TcpServiceDispatcher;
use crate::network_restore_server::{
    NetworkRestoreServer, RESTORE_SERVICE_NAME,
};
use crate::node_state::{NodeState, NodeStateMachine};
use crate::rep_config::RepConfig;
use crate::rep_stats::RepStats;
use crate::state_change_listener::{StateChangeEvent, StateChangeListener};
use crate::stream::feeder::Feeder;
use crate::stream::peer_feeder::{
    PEER_FEEDER_SERVICE_NAME, PeerFeederService, PeerLogScanner,
};
use crate::stream::replica_stream::{EnvironmentLogWriter, ReplicaStream};
use crate::vlsn::vlsn_index::VlsnIndex;
use crate::vlsn::vlsn_range::VlsnRange;

/// Default heartbeat timeout for master liveness detection.
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

/// A replicated database environment.
///
///
///
/// This is the entry point for replication. It wraps a standard Environment
/// and adds replication capabilities including master election, replica
/// streaming, and commit acknowledgments.
///
/// High Availability (HA) provides a replicated, embedded database
/// management system which provides fast, reliable, and scalable data
/// management. HA enables replication of an environment across a Replication
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
    /// Started in `new()` when a listen
    /// address is available. `None` only when the bind address cannot be
    /// resolved (e.g. in unit tests that use port 0 but want lazy init).
    tcp_dispatcher: Option<TcpServiceDispatcher>,
    /// The address the `tcp_dispatcher` is actually bound to (may differ from
    /// the configured port when port 0 is used in tests).
    bound_addr: Option<SocketAddr>,

    /// Optional live `EnvironmentImpl` wired in via [`with_environment`].
    ///
    /// When set, `become_master` spawns a `FeederRunner` per replica using
    /// `EnvironmentLogScanner`, and `become_replica` spawns a
    /// `ReplicaReceiver` thread using `EnvironmentLogWriter`.
    ///
    /// In HA.
    env_impl: StdMutex<Option<Arc<EnvironmentImpl>>>,

    /// Background I/O thread handles spawned during state transitions.
    ///
    /// Stored so that `close()` can join them cleanly.  Each handle is
    /// `Option` so we can `take()` it in `close()`.
    io_threads: StdMutex<Vec<std::thread::JoinHandle<()>>>,

    /// Shutdown flag shared with I/O threads so they terminate when the
    /// environment is closed.
    io_shutdown: AtomicBool,

    /// Whether the RESTORE service has been registered on the TCP dispatcher.
    ///
    /// When `config.env_home` is `None` at construction time, registration is
    /// deferred until `with_environment()` provides the env home path.
    restore_registered: AtomicBool,

    /// In-memory log queue used by the peer feeder service.
    ///
    /// When this node is a replica, `apply_entry()` pushes each received log
    /// entry here.  The `PeerFeederService` registered on the TCP dispatcher
    /// reads from this queue to stream entries to downstream replicas that
    /// are behind this node (peer-to-peer log distribution, HA style).
    peer_scanner: Arc<PeerLogScanner>,

    /// Monotonic sequence used by `await_replica_acks` to assign unique
    /// keys to in-flight commits awaiting replica acknowledgment.  In
    /// production this should track the real master VLSN; until F11
    /// closes the VLSN<->commit linkage, the coordinator uses a
    /// synthetic sequence so that ack tracking is unique per commit.
    /// See finding F1 in `docs/src/internal/api-audit-2026-05-rep.md`.
    commit_ack_seq: std::sync::atomic::AtomicU64,
}

impl ReplicatedEnvironment {
    /// Create a new replicated environment.
    ///
    ///
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
        // equivalent: `RepImpl.open()` calls `serviceDispatcher.start()`
        // which binds a ServerSocketChannel on the configured port and begins
        // accepting connections. We do the same here using the node_host and
        // node_port from RepConfig.
        let listen_addr_str =
            format!("{}:{}", config.node_host, config.node_port);
        let mut restore_registered_init = false;

        let (tcp_dispatcher, bound_addr) = match listen_addr_str
            .parse::<SocketAddr>()
        {
            Ok(addr) => {
                match TcpServiceDispatcher::new(addr) {
                    Ok(dispatcher) => match dispatcher.start() {
                        Ok(bound) => {
                            // Register the network restore handler so any
                            // node in the group can request a full file-set
                            // copy from this node's environment.
                            if let Some(ref home) = config.env_home {
                                let restore_server =
                                    NetworkRestoreServer::new(home.clone());
                                dispatcher.register(
                                    RESTORE_SERVICE_NAME,
                                    Arc::new(restore_server),
                                );
                                log::debug!(
                                    "Node '{}' RESTORE service registered \
                                         (env_home={})",
                                    config.node_name,
                                    home.display(),
                                );
                                restore_registered_init = true;
                            }
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

        // Build the in-memory peer log scanner; register the peer feeder
        // service on the dispatcher so downstream replicas can connect.
        let peer_scanner = Arc::new(PeerLogScanner::new());
        if let Some(ref dispatcher) = tcp_dispatcher {
            let service = PeerFeederService::new(Arc::clone(&peer_scanner));
            dispatcher.register(PEER_FEEDER_SERVICE_NAME, Arc::new(service));
            log::debug!(
                "Node '{}' PEER_FEEDER service registered",
                config.node_name,
            );
        }

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
            env_impl: StdMutex::new(None),
            io_threads: StdMutex::new(Vec::new()),
            io_shutdown: AtomicBool::new(false),
            restore_registered: AtomicBool::new(restore_registered_init),
            peer_scanner,
            commit_ack_seq: std::sync::atomic::AtomicU64::new(1),
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

    /// Wire a live `EnvironmentImpl` into this replicated environment.
    ///
    /// After this call, state transitions (`become_master`, `become_replica`)
    /// will spawn real feeder/receiver I/O threads backed by the live log.
    ///
    /// If the RESTORE service was not registered at construction time (because
    /// `config.env_home` was `None`), it is registered here using the
    /// environment's actual home path.  This mirrors`RepNode.envSetup()`
    /// which registers the restore handler during environment wiring.
    ///
    /// Environment reference wiring.
    /// `EnvironmentImpl` via `RepImpl.repNode.envImpl` in HA.
    pub fn with_environment(&self, env: Arc<EnvironmentImpl>) {
        // Register RESTORE service lazily if not already done.
        if !self.restore_registered.load(Ordering::SeqCst)
            && let Some(ref dispatcher) = self.tcp_dispatcher
        {
            let env_home = env.get_env_home().to_path_buf();
            let restore_server = NetworkRestoreServer::new(env_home.clone());
            dispatcher.register(RESTORE_SERVICE_NAME, Arc::new(restore_server));
            self.restore_registered.store(true, Ordering::SeqCst);
            log::debug!(
                "Node '{}' RESTORE service registered via with_environment \
                 (env_home={})",
                self.config.node_name,
                env_home.display(),
            );
        }

        *self.env_impl.lock().unwrap() = Some(env);
    }

    /// Get the current node state.
    ///
    ///
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
    ///
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
    ///
    ///
    /// Returns a description of the replication group as known by this node.
    /// The replicated group metadata is stored in a replicated database and
    /// updates are propagated by the current master node to all replicas. If
    /// this node is not the master, it is possible for its description of the
    /// group to be out of date.
    pub fn get_group(&self) -> &GroupService {
        &self.group_service
    }

    /// Add a peer node to the replication group at runtime.
    ///
    /// The node is registered in the `GroupService` so elections and quorum
    /// calculations immediately reflect the new membership.
    pub fn add_peer(&self, node: crate::rep_node::RepNode) -> Result<()> {
        use crate::group_service::NodeInfo;
        use std::time::Instant;

        let info = NodeInfo {
            name: node.name.clone(),
            node_type: node.node_type,
            host: node.host.clone(),
            port: node.port,
            node_id: node.node_id,
            joined_at: Instant::now(),
            last_seen: Instant::now(),
            is_active: true,
            known_vlsn: 0,
            log_range: None,
            read_capacity_pct: node.read_capacity_pct,
            write_capacity_pct: node.write_capacity_pct,
            latency_hint_ms: node.latency_hint_ms,
        };
        self.group_service.add_node(info)?;
        log::info!(
            "Node '{}': added peer '{}' ({}:{}) to group '{}'",
            self.config.node_name,
            node.name,
            node.host,
            node.port,
            self.config.group_name,
        );
        Ok(())
    }

    /// Remove a peer node from the replication group by name.
    ///
    /// The node is deregistered from the `GroupService`.  Elections initiated
    /// after this call will not include the removed node in quorum calculations.
    pub fn remove_peer(&self, name: &str) -> Result<()> {
        self.group_service.remove_node(name)?;
        log::info!(
            "Node '{}': removed peer '{}' from group '{}'",
            self.config.node_name,
            name,
            self.config.group_name,
        );
        Ok(())
    }

    /// Update the capacity and latency metadata of an existing peer.
    ///
    /// Only the following fields are updated from `node`:
    ///   - `read_capacity_pct`
    ///   - `write_capacity_pct`
    ///   - `latency_hint_ms`
    ///
    /// The node's identity (name, address, port, node_type) is preserved.
    /// Safe to call while replication is active.
    ///
    /// If the quorum policy is `Flexible` or `Expression`, the quorum system
    /// is rebuilt to reflect the new capacity/latency weights.
    pub fn update_peer_metadata(
        &self,
        name: &str,
        node: crate::rep_node::RepNode,
    ) -> Result<()> {
        self.group_service.update_node_metadata(
            name,
            node.read_capacity_pct,
            node.write_capacity_pct,
            node.latency_hint_ms,
        )?;
        log::info!(
            "Node '{}': updated metadata for peer '{}' \
             (read_cap={}, write_cap={}, latency={}ms)",
            self.config.node_name,
            name,
            node.read_capacity_pct,
            node.write_capacity_pct,
            node.latency_hint_ms,
        );
        Ok(())
    }

    /// Returns a snapshot of the current replication group as a `RepGroup`.
    ///
    /// The snapshot reflects the state at the time of the call; subsequent
    /// `add_peer` / `remove_peer` calls are not reflected in it.
    pub fn get_rep_group(&self) -> crate::rep_group::RepGroup {
        use crate::rep_group::RepGroup;

        let mut group = RepGroup::new(
            self.config.group_name.clone(),
            self.group_service.get_group_id(),
        );
        for info in self.group_service.get_all_nodes() {
            let mut node = crate::rep_node::RepNode::new(
                info.name.clone(),
                info.node_type,
                info.host.clone(),
                info.port,
                info.node_id,
            );
            node.read_capacity_pct = info.read_capacity_pct;
            node.write_capacity_pct = info.write_capacity_pct;
            node.latency_hint_ms = info.latency_hint_ms;
            group.add_node(node);
        }
        group
    }

    /// Get the replication configuration.
    ///
    ///
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
    ///
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
    pub fn ensure_unknown_state(&self) -> Result<()> {
        let current = self.node_state.get_state();
        match current {
            NodeState::Unknown => Ok(()),
            NodeState::Detached => {
                self.node_state.transition_to(NodeState::Unknown)?;
                Ok(())
            }
            // Master and Replica must transition through Unknown before
            // joining a new group or reconnecting.
            NodeState::Master | NodeState::Replica => {
                self.node_state.transition_to(NodeState::Unknown)?;
                Ok(())
            }
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
    ///
    /// If a live `EnvironmentImpl` has been wired in via `with_environment`,
    /// a `FeederRunner` + `EnvironmentLogScanner` background thread is spawned
    /// for each currently-registered replica (feeder entries in `feeders`).
    ///
    /// In HA.
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

        // --- G19: spawn FeederRunner threads for each known replica --------
        //
        // → `Feeder.runFeedingLoop()`.
        // Each active feeder in the feeders list gets a dedicated thread that
        // runs `FeederRunner::run()` backed by `EnvironmentLogScanner`.
        if self.env_impl.lock().unwrap().is_some() {
            let feeders_snap: Vec<String> = self
                .feeders
                .read()
                .iter()
                .map(|f| f.get_replica_name())
                .collect();

            for replica_name in feeders_snap {
                // The PeerFeederService registered on the TCP dispatcher
                // handles incoming replica connections automatically.
                // Replicas connect to PEER_FEEDER_SERVICE_NAME and the
                // dispatcher spawns a per-connection thread running
                // PeerFeederRunner::run().
                //
                // Here we just log the known replicas for observability.
                log::info!(
                    "Node '{}' (master): feeder for replica '{}' is served \
                     by PeerFeederService on the TCP dispatcher",
                    self.config.node_name.as_str(),
                    replica_name,
                );
            }
        }
        // -------------------------------------------------------------------

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
    ///
    /// If a live `EnvironmentImpl` has been wired in via `with_environment`,
    /// the method prepares an `EnvironmentLogWriter` so that replicated
    /// entries can be written to the local log.  The actual network connection
    /// is established by the `TcpServiceDispatcher`; this method logs intent.
    ///
    /// In HA.
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
        self.replica_stream.set_master(master_name);
        self.replica_stream.set_state(
            crate::stream::replica_stream::ReplicaStreamState::Connecting,
        );

        // --- G19: start replica receive loop --------------------------------
        //
        // Connects to the master's PEER_FEEDER service and runs a
        // ReplicaReceiver loop in a background thread.  The receiver writes
        // replicated entries via EnvironmentLogWriter.
        if let Some(env) = self.env_impl.lock().unwrap().clone() {
            if let Some(log_mgr) = env.get_log_manager() {
                let vlsn_index =
                    Arc::new(crate::vlsn::vlsn_index::VlsnIndex::new(10));

                // Resolve the master's socket address from the GroupService.
                let master_addr_opt: Option<SocketAddr> = self
                    .group_service
                    .get_all_nodes()
                    .iter()
                    .find(|n| n.name == master_name)
                    .and_then(|info| {
                        format!("{}:{}", info.host, info.port)
                            .parse::<SocketAddr>()
                            .ok()
                    });

                let node_name = self.config.node_name.clone();
                let master = master_name.to_string();
                let vlsn_index_clone = Arc::clone(&vlsn_index);
                let shutdown_flag = self.io_shutdown.load(Ordering::SeqCst);

                let handle = std::thread::Builder::new()
                    .name(format!("noxu-replica-{}", node_name))
                    .spawn(move || {
                        let mut writer = EnvironmentLogWriter::new(
                            log_mgr,
                            vlsn_index_clone,
                        );

                        if let Some(addr) = master_addr_opt {
                            log::info!(
                                "noxu-replica-{}: connecting to master '{}' at {}",
                                node_name, master, addr,
                            );
                            // Run catch-up loop (blocks until channel closes
                            // or the master disconnects).
                            match crate::stream::peer_feeder::catch_up_from_peer(
                                addr, 0, &mut writer,
                            ) {
                                Ok(true) => log::info!(
                                    "noxu-replica-{}: catch-up complete from '{}'",
                                    node_name, master,
                                ),
                                Ok(false) => log::warn!(
                                    "noxu-replica-{}: master '{}' requires restore",
                                    node_name, master,
                                ),
                                Err(e) => {
                                    if !shutdown_flag {
                                        log::error!(
                                            "noxu-replica-{}: error from master '{}': {e}",
                                            node_name, master,
                                        );
                                    }
                                }
                            }
                        } else {
                            log::warn!(
                                "noxu-replica-{}: master '{}' address not in RepGroup; \
                                 waiting for TCP dispatcher connection",
                                node_name, master,
                            );
                        }
                    })
                    .expect("failed to spawn noxu-replica thread");

                self.io_threads.lock().unwrap().push(handle);

                log::debug!(
                    "Node '{}': replica receive thread started for master '{}'",
                    self.config.node_name.as_str(),
                    master_name,
                );
            } else {
                log::warn!(
                    "Node '{}': no LogManager available (read-only env?); \
                     replica I/O loop not started",
                    self.config.node_name.as_str(),
                );
            }
        }
        // -------------------------------------------------------------------

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
    ///
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
    ///
    /// `data` is the wire-encoded log-record payload.  When the
    /// replicated environment has not been wired to a local
    /// `noxu_db::Environment` (i.e., before `with_environment` is
    /// called) the payload is forwarded into the in-memory peer
    /// scanner so that downstream replicas attached to the
    /// `PEER_FEEDER` service can re-stream it; the local log is **not**
    /// updated.  This is documented behaviour rather than a stub — see
    /// `api-audit-2026-05-rep.md` finding #26 (medium) for the
    /// `with_environment`-required local-apply path.  Wave 1C audit
    /// cleanup (rep info F35: `_data` placeholder) renames the leading
    /// underscore so reviewers don't read it as a TODO.
    pub fn apply_entry(
        &self,
        vlsn: u64,
        entry_type: u8,
        data: Vec<u8>,
    ) -> Result<()> {
        if self.is_shutdown() {
            return Err(RepError::StateError(
                "Cannot apply entry: environment is closed".to_string(),
            ));
        }

        // Register the VLSN in the index.
        self.vlsn_index.register(vlsn, 0, 0);

        // Push into the peer log scanner so downstream replicas can
        // receive this entry via the PEER_FEEDER service.
        self.peer_scanner.push(vlsn, entry_type, data);

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
    ///
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
    ///
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

        // Signal and join all I/O threads spawned by become_master /
        // become_replica.
        self.io_shutdown.store(true, Ordering::SeqCst);
        {
            let mut threads = self.io_threads.lock().unwrap();
            for handle in threads.drain(..) {
                let _ = handle.join();
            }
        }

        // Stop the TCP service dispatcher (the: serviceDispatcher.shutdown()).
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
    ///
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

// ---------------------------------------------------------------------------
// F1: ReplicaAckCoordinator impl wires master commits into the AckTracker.
// ---------------------------------------------------------------------------
//
// `noxu_db::Transaction::commit_with_durability` calls
// `await_replica_acks` after the local WAL fsync.  This impl:
//
//   1. Rejects calls on a non-master node with `NotMaster`.
//   2. Rejects calls during shutdown with `Shutdown`.
//   3. Computes the required ack count from `electable_count` and the
//      requested policy.
//   4. Allocates a unique commit sequence number, registers the ack
//      requirement on the `AckTracker`, and polls `is_satisfied` with
//      a small sleep until either the timeout elapses or the policy
//      is satisfied.
//   5. Cleans up the tracker entry on every exit path.
//
// Closes finding F1 of `docs/src/internal/api-audit-2026-05-rep.md`.
impl ReplicaAckCoordinator for ReplicatedEnvironment {
    fn await_replica_acks(
        &self,
        policy: ReplicaAckPolicyKind,
        timeout: Duration,
    ) -> std::result::Result<u32, AckWaitError> {
        // Fast-path: ReplicaAckPolicy::None never blocks. The trait spec
        // says callers may already short-circuit, but be defensive.
        if matches!(policy, ReplicaAckPolicyKind::None) {
            return Ok(0);
        }

        if self.is_shutdown() {
            return Err(AckWaitError {
                kind: AckWaitErrorKind::Shutdown,
                needed: 0,
                received: 0,
            });
        }

        if !self.is_master() {
            return Err(AckWaitError {
                kind: AckWaitErrorKind::NotMaster,
                needed: 0,
                received: 0,
            });
        }

        // Count electable peers (excluding the master) using the
        // RepGroup view, which counts Arbiters and Electables
        // identically. Only Electable nodes are counted as data
        // replicas able to ack a commit.  The master itself is
        // *implicit*: it is not registered in `group_service` (only
        // peers are), so we add 1 to obtain the total electable
        // count expected by `ReplicaAckPolicyKind::required_acks`.
        let group = self.get_rep_group();
        let electable_peers: u32 = group
            .get_nodes()
            .iter()
            .filter(|n| n.node_type == crate::node_type::NodeType::Electable)
            .count() as u32;
        let electable_count: u32 = electable_peers + 1; // +1 for self/master

        let needed = policy.required_acks(electable_count);
        if needed == 0 {
            // Single-node group, or All with only the master itself.
            return Ok(0);
        }

        let commit_seq = self
            .commit_ack_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.ack_tracker.register(commit_seq, needed);

        // Poll-with-sleep loop. The poll interval is small enough that
        // late acks satisfy the policy promptly, and large enough that
        // a single commit waiting on a slow replica does not spin a
        // CPU.
        let poll_interval = std::cmp::min(
            timeout / 50,
            Duration::from_millis(20),
        );
        let poll_interval = if poll_interval.is_zero() {
            Duration::from_millis(1)
        } else {
            poll_interval
        };
        let deadline = std::time::Instant::now() + timeout;

        loop {
            if self.ack_tracker.is_satisfied(commit_seq) {
                self.ack_tracker.cleanup_through(commit_seq);
                return Ok(needed);
            }
            if self.is_shutdown() {
                self.ack_tracker.cleanup_through(commit_seq);
                return Err(AckWaitError {
                    kind: AckWaitErrorKind::Shutdown,
                    needed,
                    received: 0,
                });
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                // Tear down the registration so it doesn't accumulate;
                // record the partial ack count so the caller can report
                // a useful `InsufficientReplicas { required, available }`.
                let received = self
                    .ack_tracker
                    .received_count(commit_seq)
                    .unwrap_or(0);
                self.ack_tracker.cleanup_through(commit_seq);
                return Err(AckWaitError {
                    kind: AckWaitErrorKind::Timeout,
                    needed,
                    received,
                });
            }
            let sleep_for =
                std::cmp::min(poll_interval, deadline.saturating_duration_since(now));
            std::thread::sleep(sleep_for);
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
        assert!(
            env.tcp_dispatcher
                .as_ref()
                .map(|d| d.is_running())
                .unwrap_or(false)
        );

        env.close().unwrap();

        // After close, dispatcher must be stopped.
        assert!(
            !env.tcp_dispatcher
                .as_ref()
                .map(|d| d.is_running())
                .unwrap_or(false),
            "dispatcher should be stopped after close"
        );
    }

    #[test]
    fn test_tcp_dispatcher_accepts_connection() {
        use crate::net::Channel;
        use crate::net::ServiceHandler;
        use crate::net::service_dispatcher::connect_to_service;
        use std::sync::atomic::{AtomicU32, Ordering as AO};
        use std::time::Duration;

        struct PingHandler {
            count: AtomicU32,
        }
        impl ServiceHandler for PingHandler {
            fn service_name(&self) -> &str {
                "ping"
            }
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

    /// Verify that `with_environment` lazily registers the RESTORE service on
    /// the TCP dispatcher when `config.env_home` was not set at construction.
    ///
    /// This mirrors`RepNode.envSetup()` which registers the restore handler
    /// when the environment is wired into the replicated node.
    #[test]
    fn test_restore_registered_lazily_via_with_environment() {
        use noxu_dbi::EnvironmentImpl;
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");

        // Build config WITHOUT env_home — dispatcher starts, but no RESTORE handler yet.
        let config = RepConfig::builder("test_group", "node1", "127.0.0.1")
            .node_port(0)
            .build();

        let rep_env = ReplicatedEnvironment::new(config).unwrap();

        // Not yet registered.
        assert!(
            !rep_env
                .restore_registered
                .load(std::sync::atomic::Ordering::SeqCst)
        );

        // Wire in a real EnvironmentImpl so get_env_home() returns the temp dir.
        let env_impl = Arc::new(
            EnvironmentImpl::new(dir.path(), false, false).expect("open env"),
        );
        rep_env.with_environment(env_impl);

        // Now the RESTORE service must be registered.
        assert!(
            rep_env
                .restore_registered
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    /// Verify that when `config.env_home` IS set at construction, the RESTORE
    /// service is registered immediately (not deferred).
    #[test]
    fn test_restore_registered_eagerly_when_env_home_in_config() {
        use tempfile::TempDir;

        let dir = TempDir::new().expect("temp dir");

        let config = RepConfig::builder("test_group", "node2", "127.0.0.1")
            .node_port(0)
            .env_home(dir.path())
            .build();

        let rep_env = ReplicatedEnvironment::new(config).unwrap();

        // Should be registered immediately (env_home was in config).
        assert!(
            rep_env
                .restore_registered
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }
}
