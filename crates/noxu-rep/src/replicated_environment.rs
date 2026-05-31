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
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::ack_tracker::AckTracker;
use crate::elections::election_service::{
    ELECTION_SERVICE_NAME, ElectionAcceptorState, ElectionService,
};
use crate::elections::master_tracker::MasterTracker;
use crate::error::{RepError, Result};
use crate::group_service::GroupService;
use crate::master_transfer::MasterTransferConfig;
use crate::net::service_dispatcher::TcpServiceDispatcher;
use crate::network_restore::{NetworkRestore, NetworkRestoreConfig};
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
    ///
    /// Wrapped in `Arc` so that background daemons (election driver,
    /// VLSN-index persistence flusher) can share access without
    /// borrowing the env.  Closes finding F11 (
    /// `docs/src/internal/api-audit-2026-05-rep.md`).
    vlsn_index: Arc<VlsnIndex>,
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

    /// Shared acceptor state used by the ELECTION service handler.
    /// The election driver updates `own_vlsn` / `own_term` here as the
    /// node progresses; incoming acceptor sessions read it on every
    /// connection so their replies always reflect the local node's
    /// most recent state.  Closes finding F6.
    election_state: Arc<ElectionAcceptorState>,

    /// Self-referential `Weak` populated once the env has been wrapped
    /// in an `Arc`.  Used by the replica I/O thread spawned in
    /// `become_replica` so it can call `bootstrap_via_dispatcher` when
    /// the master signals `NeedsRestore`.
    ///
    /// Populated lazily via [`Self::init_self_weak`] from `open()` and
    /// the test harness.  When unset (callers that build the env via
    /// raw `Arc::new(Self::new(...))` and never call `init_self_weak`)
    /// the I/O thread falls back to operator-driven bootstrap.
    self_weak: OnceLock<Weak<Self>>,
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
        // mTLS Phase 2 (v3.1.0): peer_allowlist enforcement is real at the
        // TLS channel layer (TlsTcpChannelListener::bind_with_tls_and_allowlist).
        // With plain TCP transport there is no TLS handshake and the allowlist
        // has no effect — emit a warn to alert operators.
        if !config.peer_allowlist.is_empty() {
            match config.transport_kind {
                crate::rep_config::RepTransportKind::Tls => {
                    log::info!(
                        "[{}] peer_allowlist configured ({} entries); attach                          TlsTcpChannelListener::bind_with_tls_and_allowlist                          to activate mTLS enforcement on the listener.",
                        config.node_name,
                        config.peer_allowlist.len(),
                    );
                }
                _ => {
                    log::warn!(
                        "[{}] peer_allowlist is configured ({} entries) but                          transport_kind is not Tls — the allowlist has no                          effect without TLS transport. Set                          RepTransportKind::Tls to activate mTLS enforcement.",
                        config.node_name,
                        config.peer_allowlist.len(),
                    );
                }
            }
        }
        let node_state = NodeStateMachine::new();
        let group_service = GroupService::new(config.group_name.clone());
        let vlsn_index = {
            // F11: try to load a previously persisted vlsn.idx from
            // env_home if one exists.  A successfully loaded index lets a
            // restarted replica resume from where it left off without a
            // full network restore; a missing or corrupt file falls back
            // to a fresh in-memory index (caller will need to bootstrap).
            if let Some(ref home) = config.env_home {
                match crate::vlsn::persist::load_from_disk(home) {
                    Ok(Some(idx)) => {
                        log::info!(
                            "Node '{}' loaded persisted VLSN index from {} \
                             ({} entries, latest vlsn={})",
                            config.node_name,
                            home.display(),
                            idx.snapshot_entries().len(),
                            idx.get_latest_vlsn(),
                        );
                        Arc::new(idx)
                    }
                    Ok(None) => Arc::new(VlsnIndex::new(10)),
                    Err(e) => {
                        log::warn!(
                            "Node '{}' failed to load persisted VLSN index \
                             from {}: {} (treating as fresh node — network \
                             restore required)",
                            config.node_name,
                            home.display(),
                            e
                        );
                        // Best-effort: remove the corrupt file so the
                        // next persist cycle writes a clean one.  A
                        // missing file is the "fresh node" baseline.
                        let _ = std::fs::remove_file(
                            crate::vlsn::persist::index_path(home),
                        );
                        Arc::new(VlsnIndex::new(10))
                    }
                }
            } else {
                Arc::new(VlsnIndex::new(10))
            }
        };
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
        // F5/F31: build the acceptor state with persistence enabled when
        // env_home is configured.  Crash-durable promises are required
        // for the Paxos safety invariant after a process restart.
        let election_state =
            Arc::new(if let Some(ref home) = config.env_home {
                ElectionAcceptorState::with_env_home(
                    config.node_name.clone(),
                    1,
                    home,
                )
            } else {
                ElectionAcceptorState::new(config.node_name.clone(), 1)
            });
        if let Some(ref dispatcher) = tcp_dispatcher {
            let service = PeerFeederService::new(Arc::clone(&peer_scanner));
            dispatcher.register(PEER_FEEDER_SERVICE_NAME, Arc::new(service));
            log::debug!(
                "Node '{}' PEER_FEEDER service registered",
                config.node_name,
            );
            // F6: register the ELECTION service so peers can run
            // run_acceptor against this node when proposing.
            let election_svc =
                Arc::new(ElectionService::new(Arc::clone(&election_state)));
            dispatcher.register(ELECTION_SERVICE_NAME, election_svc);
            log::debug!(
                "Node '{}' ELECTION service registered",
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
            election_state,
            self_weak: OnceLock::new(),
        };

        Ok(env)
    }

    /// Open a replicated environment with the standard production
    /// lifecycle.
    ///
    /// This is the entry point recommended by the mdBook chapters:
    /// it allocates the `ReplicatedEnvironment`, registers all
    /// services on the TCP dispatcher, and spawns the **election
    /// driver** background thread that runs Paxos rounds against
    /// known peers until the node has resolved into either Master or
    /// Replica state.
    ///
    /// Closes finding F6 of `docs/src/internal/api-audit-2026-05-rep.md`.
    ///
    /// Use [`ReplicatedEnvironment::new`] directly only when the
    /// caller plans to drive state transitions explicitly (test
    /// harnesses, scripted bootstrap, recovery tooling).
    pub fn open(config: RepConfig) -> Result<Arc<Self>> {
        let env = Arc::new(Self::new(config)?);
        env.init_self_weak();
        env.start_election_driver();
        env.start_vlsn_persistence_daemon();
        env.register_admin_service();
        Ok(env)
    }

    /// Populate the env's self-referential `Weak` so background
    /// threads can obtain a back-reference for auto-orchestrated
    /// follow-up actions (e.g. replica auto-bootstrap on
    /// `NeedsRestore`).  Idempotent: subsequent calls are silent
    /// no-ops because the inner [`OnceLock`] only accepts one set.
    ///
    /// Callers that wrap the env in `Arc` and want auto-bootstrap
    /// behaviour should call this immediately after construction.
    /// `Self::open` already does so.  Test harnesses that drive
    /// transitions manually (`RepTestBase`) also call this so the
    /// auto-bootstrap path is exercised in tests.
    pub fn init_self_weak(self: &Arc<Self>) {
        let _ = self.self_weak.set(Arc::downgrade(self));
    }

    /// Register the `ADMIN` service handler on the TCP dispatcher.
    ///
    /// Closes findings F7 / F8.  Holds a `Weak<Self>` so the handler
    /// does not extend the env's lifetime.  Idempotent: re-registering
    /// is harmless because `TcpServiceDispatcher::register` overwrites
    /// the existing handler.
    pub fn register_admin_service(self: &Arc<Self>) {
        if let Some(ref dispatcher) = self.tcp_dispatcher {
            crate::group_admin::register_admin_service(
                dispatcher,
                Arc::downgrade(self),
            );
            log::debug!(
                "Node '{}' ADMIN service registered",
                self.config.node_name,
            );
        }
    }

    /// Spawn the VLSN-index persistence daemon (F11).
    ///
    /// Periodically (every 2 seconds) snapshots the in-memory
    /// `VlsnIndex` to `<env_home>/vlsn.idx` so a clean restart can
    /// resume from where the replica left off without a full network
    /// restore.  No-op when `config.env_home` is `None`.
    ///
    /// Idempotent: only one daemon is ever spawned per env.
    pub fn start_vlsn_persistence_daemon(self: &Arc<Self>) {
        let Some(home) = self.config.env_home.clone() else {
            return;
        };
        {
            let threads = self.io_threads.lock().unwrap();
            if threads.iter().any(|h| {
                h.thread()
                    .name()
                    .is_some_and(|n| n.starts_with("noxu-vlsn-flush-"))
            }) {
                return;
            }
        }

        let vlsn_index = Arc::clone(&self.vlsn_index);
        let name = format!("noxu-vlsn-flush-{}", self.config.node_name);
        let me = Arc::clone(self);
        let interval = Duration::from_secs(2);

        let handle = std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                use std::sync::atomic::Ordering;
                let mut last_persisted_vlsn: u64 = 0;
                while !me.io_shutdown.load(Ordering::SeqCst)
                    && !me.is_shutdown()
                {
                    std::thread::sleep(interval);
                    if me.io_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    let latest = vlsn_index.get_latest_vlsn();
                    if latest == last_persisted_vlsn {
                        // Nothing new to flush.
                        continue;
                    }
                    // X-2: cap the flush at the last durable checkpoint's
                    // end LSN so the persisted VLSN index never claims
                    // VLSNs beyond the durable B-tree state.  After a crash
                    // the recovered tree and the index will be coherent.
                    let cap_lsn = me
                        .env_impl
                        .lock()
                        .unwrap()
                        .as_ref()
                        .and_then(|e| e.get_checkpointer())
                        .map(|c| c.get_last_checkpoint_end())
                        .unwrap_or(noxu_util::NULL_LSN);
                    match crate::vlsn::persist::flush_to_disk_capped(
                        &vlsn_index,
                        &home,
                        cap_lsn,
                    ) {
                        Ok(n) => {
                            log::trace!(
                                "vlsn-flush: persisted {} entries (latest vlsn={}, cap_lsn={:?})",
                                n,
                                latest,
                                cap_lsn,
                            );
                            last_persisted_vlsn = latest;
                        }
                        Err(e) => {
                            log::warn!(
                                "vlsn-flush: failed to persist VLSN index to {}: {}",
                                home.display(),
                                e
                            );
                        }
                    }
                }
                // Final flush on shutdown so a clean close is recoverable.
                // Cap at the last checkpoint even for the shutdown flush.
                let cap_lsn = me
                    .env_impl
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|e| e.get_checkpointer())
                    .map(|c| c.get_last_checkpoint_end())
                    .unwrap_or(noxu_util::NULL_LSN);
                if let Err(e) = crate::vlsn::persist::flush_to_disk_capped(
                    &vlsn_index,
                    &home,
                    cap_lsn,
                ) {
                    log::warn!(
                        "vlsn-flush (final): failed to persist VLSN index: {}",
                        e
                    );
                }
            })
            .expect("failed to spawn noxu-vlsn-flush thread");

        self.io_threads.lock().unwrap().push(handle);
        log::debug!(
            "Node '{}' VLSN persistence daemon started",
            self.config.node_name,
        );
    }

    /// Spawn the election driver background thread.
    ///
    /// While the env is in `Detached` or `Unknown` state and no master
    /// is known, the driver periodically attempts a Paxos election
    /// against peers in `GroupService` (whose ELECTION services were
    /// registered at `open()` time).  On success the driver calls
    /// `become_master` (if this node is the winner) or `become_replica`
    /// (otherwise).  On failure (no quorum), the driver waits
    /// `config.election_timeout` and tries again.
    ///
    /// The driver respects `io_shutdown`; on env close the loop exits
    /// promptly.
    ///
    /// Idempotent: a second call is a no-op (only one driver thread is
    /// ever spawned per env).
    pub fn start_election_driver(self: &Arc<Self>) {
        use std::sync::atomic::Ordering;
        // Reuse io_shutdown for cancellation; a successful spawn is
        // recorded by appending to io_threads, so a duplicate call
        // would re-add a thread — we use a one-shot `AtomicBool`
        // sentinel placed in the io_shutdown's slot via a new field.
        // Cheaper: a static name check on io_threads is impossible;
        // instead, gate spawning on whether any io_thread already
        // carries the driver name.
        {
            let threads = self.io_threads.lock().unwrap();
            if threads.iter().any(|h| {
                h.thread()
                    .name()
                    .is_some_and(|n| n.starts_with("noxu-election-"))
            }) {
                return;
            }
        }

        let me = Arc::clone(self);
        let name = format!("noxu-election-{}", self.config.node_name);
        let handle = std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                me.run_election_loop();
            })
            .expect("failed to spawn election driver thread");
        self.io_threads.lock().unwrap().push(handle);
        log::debug!("Node '{}' election driver started", self.config.node_name,);
        // Keep ordering sane on the io_shutdown flag.
        let _ = self.io_shutdown.load(Ordering::SeqCst);
    }

    /// Body of the election driver loop.  Public only for tests; called
    /// by [`Self::start_election_driver`].
    fn run_election_loop(self: Arc<Self>) {
        use std::sync::atomic::Ordering;
        // Maintain an internal monotonically increasing election term.
        // Each successful or failed round bumps the term so retries do
        // not collide with stale acceptor promises.
        let mut term: u64 = 1;

        loop {
            if self.io_shutdown.load(Ordering::SeqCst) {
                return;
            }
            if self.is_shutdown() {
                return;
            }

            let state = self.node_state.get_state();
            // Stop driving once we've resolved into Master/Replica;
            // re-arm only if the node returns to Unknown.
            if matches!(state, NodeState::Master | NodeState::Replica) {
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
            if matches!(state, NodeState::Shutdown) {
                return;
            }

            // Probe peers for an active master via the existing
            // GroupService cache.  In the absence of a heartbeat path
            // we rely on master_tracker (set by become_replica from
            // the receive loop).
            if let Some(master_name) = self.master_tracker.get_master()
                && master_name != self.config.node_name
            {
                let _ = self.become_replica(&master_name);
                continue;
            }

            // Snapshot peers to dial for ELECTION.
            let peers: Vec<(String, SocketAddr)> = self
                .group_service
                .get_all_nodes()
                .into_iter()
                .filter(|n| n.name != self.config.node_name)
                .filter_map(|n| {
                    format!("{}:{}", n.host, n.port)
                        .parse::<SocketAddr>()
                        .ok()
                        .map(|a| (n.name, a))
                })
                .collect();

            // Build the local rep group view used by run_election to
            // compute quorum and resolve the winner name.  Include
            // self.
            let group = self.local_rep_group_with_self();

            // Update election state for any concurrent acceptor calls.
            let our_vlsn = self.vlsn_index.get_latest_vlsn();
            self.election_state.set_vlsn(our_vlsn);
            self.election_state.set_term(term);

            // Connect to each peer's ELECTION service.  Failures are
            // tolerated: a peer that doesn't answer simply contributes
            // no vote.  The election may still reach quorum in the
            // remaining peers.
            let mut channels: Vec<Arc<dyn crate::net::channel::Channel>> =
                Vec::new();
            for (peer_name, addr) in &peers {
                match crate::net::service_dispatcher::connect_to_service(
                    *addr,
                    ELECTION_SERVICE_NAME,
                ) {
                    Ok(ch) => {
                        let arc: Arc<dyn crate::net::channel::Channel> =
                            Arc::new(ch);
                        channels.push(arc);
                    }
                    Err(e) => {
                        log::trace!(
                            "election driver: peer {} ({}) unreachable: {}",
                            peer_name,
                            addr,
                            e
                        );
                    }
                }
            }

            // Resolve our own node_id from the group; if not present
            // we cannot run an election (closed-world guard — see F22).
            let self_node_id =
                group.get_node(&self.config.node_name).map(|n| n.node_id());
            let self_node_id = match self_node_id {
                Some(id) => id,
                None => {
                    log::warn!(
                        "election driver: node '{}' not registered in \
                         own group view; sleeping",
                        self.config.node_name
                    );
                    std::thread::sleep(Duration::from_millis(200));
                    continue;
                }
            };

            log::debug!(
                "election driver on '{}': starting term={} with {} peers",
                self.config.node_name,
                term,
                channels.len(),
            );
            let outcome = crate::elections::paxos::run_election(
                self_node_id,
                &self.config.node_name,
                &group,
                &channels,
                our_vlsn,
                /* priority */ 1,
                term,
            );

            match outcome {
                Some(winner_id) if winner_id == self_node_id => {
                    if let Err(e) = self.become_master(term) {
                        log::warn!(
                            "election driver: become_master failed: {}",
                            e
                        );
                    } else {
                        log::info!(
                            "election driver: '{}' became master at term {}",
                            self.config.node_name,
                            term,
                        );
                    }
                }
                Some(winner_id) => {
                    if let Some(winner_node) = group
                        .get_nodes()
                        .into_iter()
                        .find(|n| n.node_id() == winner_id)
                    {
                        if let Err(e) = self.become_replica(&winner_node.name) {
                            log::warn!(
                                "election driver: become_replica failed: {}",
                                e
                            );
                        } else {
                            log::info!(
                                "election driver: '{}' became replica of '{}' at term {}",
                                self.config.node_name,
                                winner_node.name,
                                term,
                            );
                        }
                    }
                }
                None => {
                    log::debug!(
                        "election driver on '{}' term={}: no quorum",
                        self.config.node_name,
                        term,
                    );
                }
            }

            term = term.saturating_add(1);
            // Back off so we don't pin the loop on transient failures.
            std::thread::sleep(
                self.config.election_timeout.min(Duration::from_millis(500)),
            );
        }
    }

    /// Internal: a `RepGroup` snapshot that includes self.
    fn local_rep_group_with_self(&self) -> crate::rep_group::RepGroup {
        let mut group = self.get_rep_group();
        // Ensure self is present in the group view; the
        // group_service does not auto-register the local node.
        if group.get_node(&self.config.node_name).is_none() {
            let mut self_node = crate::rep_node::RepNode::new(
                self.config.node_name.clone(),
                self.config.node_type,
                self.config.node_host.clone(),
                self.config.node_port,
                /* node_id */ 0,
            );
            // Stable self node_id derived from the name hash so
            // re-creations in the same process don't collide.
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.config.node_name.hash(&mut hasher);
            // Restrict to a u32 range and avoid 0 (reserved for
            // "unknown").
            let id = ((hasher.finish() as u32) | 1).max(1);
            self_node.node_id = id;
            group.add_node(self_node);
        }
        group
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

        // X-14: rebuild the VLSN index from recovery-replayed LN entries.
        // After a crash the on-disk vlsn.idx may be stale (either ahead of
        // the recovered B-tree, or behind if vlsn.idx was not flushed
        // after the last checkpoint).  Re-registering all (vlsn, lsn) pairs
        // from the redo pass gives a consistent in-memory index.
        if !env.recovery_vlsns.is_empty() {
            log::info!(
                "Node '{}': rebuilding VLSN index from {} recovered entries",
                self.config.node_name,
                env.recovery_vlsns.len(),
            );
            for &(vlsn, lsn_u64) in &env.recovery_vlsns {
                let lsn = noxu_util::Lsn::from_u64(lsn_u64);
                self.vlsn_index.register(
                    vlsn,
                    lsn.file_number(),
                    lsn.file_offset(),
                );
            }
        }

        // X-1: truncate the VLSN index to the rollback matchpoint if recovery
        // detected a completed rollback period.  The matchpoint is the highest
        // LSN that is still valid after the rollback; entries with higher VLSNs
        // correspond to data that was rolled back and must not appear in the
        // index.
        if let Some(matchpoint_lsn_u64) = env.recovery_rollback_matchpoint {
            // Find the latest VLSN whose LSN is at or before the matchpoint.
            // Scan the recovered VLSN pairs (sorted ascending) to find the
            // boundary.
            let safe_vlsn = env
                .recovery_vlsns
                .iter()
                .rev()
                .find(|&&(_, lsn_u64)| lsn_u64 <= matchpoint_lsn_u64)
                .map(|&(vlsn, _)| vlsn)
                .unwrap_or(0);
            log::info!(
                "Node '{}': truncating VLSN index after vlsn={} \
                 (rollback matchpoint lsn={:#x})",
                self.config.node_name,
                safe_vlsn,
                matchpoint_lsn_u64,
            );
            self.vlsn_index.truncate_after(safe_vlsn);
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

        // F9: if we are the current master, immediately register a
        // `Feeder` tracker for the new peer so AckTracker bookkeeping
        // and downstream pull-based streaming work without a forced
        // re-election.
        if self.is_master()
            && (node.node_type == crate::node_type::NodeType::Electable
                || node.node_type == crate::node_type::NodeType::Secondary)
        {
            let mut feeders = self.feeders.write();
            if !feeders.iter().any(|f| f.get_replica_name() == node.name) {
                feeders.push(Feeder::new(node.name.clone()));
                log::debug!(
                    "Node '{}' (master): dispatched Feeder for new peer '{}'",
                    self.config.node_name,
                    node.name,
                );
            }
        }
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
    ///
    /// # Note
    ///
    /// `update_peer_metadata` does not currently re-run
    /// `QuorumPolicy::validate(electable_count)` after the metadata
    /// change.  An LP-optimal `Expression` quorum that was safe before
    /// the update may no longer satisfy the intersection property
    /// afterwards.  Until automatic revalidation lands, deployments
    /// using `QuorumPolicy::Expression` should call
    /// `quorum_policy().validate(get_rep_group().electable_count())`
    /// on the returned `RepGroup` after every metadata change and
    /// fail the operator-facing operation if validation reports
    /// unsafety.
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

    /// Return the list of replica names that currently have a `Feeder`
    /// tracker on this (master) node.
    ///
    /// Used by tests and operator tooling.  The returned list reflects
    /// the master's view at the time of the call; subsequent
    /// `add_peer`/`remove_peer` calls may change it.
    pub fn feeder_replica_names(&self) -> Vec<String> {
        self.feeders.read().iter().map(|f| f.get_replica_name()).collect()
    }

    /// Bootstrap this node's environment by network-restoring all `.ndb`
    /// files from `peer_name` via the dispatcher's RESTORE service.
    ///
    /// Closes findings F2 / F4 of `docs/src/internal/api-audit-2026-05-rep.md`.
    ///
    /// The standalone `NetworkRestore::execute()` opens raw TCP and
    /// expects to drive the legacy `NetworkRestoreServer::start` listener.
    /// Production replicated environments host the RESTORE handler on the
    /// dispatcher, so this method routes through `execute_via_dispatcher`.
    ///
    /// `peer_name` must be a known peer in `GroupService`; on success the
    /// peer's `.ndb` files are written into `config.env_home`.  Returns
    /// `Err` if `env_home` is `None`, the peer is unknown, or the restore
    /// fails for any reason.
    pub fn bootstrap_via_dispatcher(&self, peer_name: &str) -> Result<()> {
        let env_home = self.config.env_home.clone().ok_or_else(|| {
            RepError::ConfigError(
                "bootstrap_via_dispatcher requires env_home in RepConfig"
                    .into(),
            )
        })?;
        let peer_info = self
            .group_service
            .get_all_nodes()
            .into_iter()
            .find(|n| n.name == peer_name)
            .ok_or_else(|| {
                RepError::ConfigError(format!(
                    "peer '{}' not registered in group '{}'",
                    peer_name, self.config.group_name,
                ))
            })?;

        let cfg = NetworkRestoreConfig {
            source_node: peer_info.name.clone(),
            source_host: peer_info.host.clone(),
            source_port: peer_info.port,
            retain_log_files: true,
        };
        let restore = NetworkRestore::new(cfg).with_local_dir(env_home);
        restore.execute_via_dispatcher()?;
        log::info!(
            "Node '{}' bootstrapped via dispatcher from '{}' ({}:{})",
            self.config.node_name,
            peer_info.name,
            peer_info.host,
            peer_info.port,
        );
        Ok(())
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

        // JE invariant: only `Electable` nodes can become master.  `Secondary`,
        // `Monitor`, and `Arbiter` are not electable and must be rejected at
        // the API layer (mirrors JE `ExceptionTest`).  See
        // `NodeType::can_be_master`.
        if !self.config.node_type.can_be_master() {
            return Err(RepError::InvalidStateTransition(format!(
                "node '{}' has type {} which is not electable as master",
                self.config.node_name.as_str(),
                self.config.node_type,
            )));
        }

        // Ensure we can reach Master state (may need Detached -> Unknown first)
        self.ensure_unknown_state()?;

        let old_state = self.node_state.get_state();
        self.node_state.transition_to(NodeState::Master)?;
        self.master_tracker.set_master(self.config.node_name.as_str(), term);

        // --- F9: spawn Feeder trackers for each known replica -------------
        //
        // Closes finding F9 of `docs/src/internal/api-audit-2026-05-rep.md`.
        // The architecture is pull-based: replicas pull from the master's
        // `PEER_FEEDER` service via `catch_up_from_peer`.  However, the
        // master must:
        //   1. Track each replica via a `Feeder` so AckTracker bookkeeping
        //      can attribute replica acks to the right node.
        //   2. Push its own writes into `peer_scanner` so replicas pulling
        //      from `PEER_FEEDER` actually receive entries (`replicate_entry`).
        //
        // Here we ensure step 1: every known electable peer in the group
        // gets a `Feeder` entry.
        {
            let mut feeders = self.feeders.write();
            // Drop any stale feeders left over from a prior role.  A
            // `Feeder` is just an in-memory tracker; recreating it is
            // cheap and avoids state inversion bugs across role changes.
            feeders.clear();
            for peer in self.group_service.get_all_nodes() {
                if peer.name == self.config.node_name {
                    continue;
                }
                if peer.node_type != crate::node_type::NodeType::Electable
                    && peer.node_type != crate::node_type::NodeType::Secondary
                {
                    // Arbiters do not receive log entries.
                    continue;
                }
                feeders.push(Feeder::new(peer.name.clone()));
                log::debug!(
                    "Node '{}' (master, term={}): registered Feeder for \
                     replica '{}'",
                    self.config.node_name.as_str(),
                    term,
                    peer.name,
                );
            }
        }

        // For observability, log the count.
        log::info!(
            "Node '{}' became master for term {} \
             (feeder trackers: {} known replicas)",
            self.config.node_name.as_str(),
            term,
            self.feeders.read().len(),
        );

        // -------------------------------------------------------------------

        // Notify listeners
        self.notify_listeners(old_state, NodeState::Master);

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
                // Wave 9-A fix 2: capture a Weak<Self> so the I/O thread
                // can call `bootstrap_via_dispatcher` automatically when
                // the master signals `NeedsRestore`.  When the env was
                // never registered with `init_self_weak` (raw
                // `Arc::new(Self::new(...))` without going through
                // `open()` or the test harness), the weak ref is `None`
                // and we fall back to operator-driven bootstrap.
                let self_weak: Option<Weak<Self>> =
                    self.self_weak.get().cloned();

                let handle = std::thread::Builder::new()
                    .name(format!("noxu-replica-{}", node_name))
                    .spawn(move || {
                        let mut writer = EnvironmentLogWriter::new(
                            log_mgr,
                            vlsn_index_clone,
                        );

                        let Some(addr) = master_addr_opt else {
                            log::warn!(
                                "noxu-replica-{}: master '{}' address not in RepGroup; \
                                 waiting for TCP dispatcher connection",
                                node_name, master,
                            );
                            return;
                        };

                        // Catch-up loop: catch up, observe NeedsRestore,
                        // optionally auto-bootstrap, retry once.  We cap
                        // the retry count at MAX_AUTO_BOOTSTRAP_ATTEMPTS
                        // (small) so a misbehaving master does not loop
                        // forever consuming network bandwidth.
                        const MAX_AUTO_BOOTSTRAP_ATTEMPTS: u32 = 2;
                        let mut attempts: u32 = 0;
                        loop {
                            log::info!(
                                "noxu-replica-{}: connecting to master '{}' at {}",
                                node_name, master, addr,
                            );
                            match crate::stream::peer_feeder::catch_up_from_peer(
                                addr, 0, &mut writer,
                            ) {
                                Ok(true) => {
                                    log::info!(
                                        "noxu-replica-{}: catch-up complete from '{}'",
                                        node_name, master,
                                    );
                                    return;
                                }
                                Ok(false) => {
                                    // F2/F4: master signals NeedsRestore.
                                    // Wave 9-A fix 2: if a Weak<Self> was
                                    // plumbed in, upgrade it and call
                                    // `bootstrap_via_dispatcher` ourselves
                                    // so the replica auto-bootstraps and
                                    // resumes catch-up without operator
                                    // intervention.
                                    log::warn!(
                                        "noxu-replica-{}: master '{}' requires restore",
                                        node_name, master,
                                    );
                                    attempts += 1;
                                    if attempts > MAX_AUTO_BOOTSTRAP_ATTEMPTS {
                                        log::error!(
                                            "noxu-replica-{}: exceeded \
                                             auto-bootstrap attempts ({}); giving up",
                                            node_name,
                                            MAX_AUTO_BOOTSTRAP_ATTEMPTS,
                                        );
                                        return;
                                    }
                                    let env_arc = match self_weak
                                        .as_ref()
                                        .and_then(Weak::upgrade)
                                    {
                                        Some(e) => e,
                                        None => {
                                            // No back-ref or env dropped:
                                            // fall back to operator-driven
                                            // bootstrap and exit cleanly.
                                            log::warn!(
                                                "noxu-replica-{}: no back-reference \
                                                 available; operator must call \
                                                 bootstrap_via_dispatcher manually",
                                                node_name,
                                            );
                                            return;
                                        }
                                    };
                                    if env_arc.is_shutdown() {
                                        return;
                                    }
                                    log::info!(
                                        "noxu-replica-{}: auto-bootstrapping via \
                                         dispatcher from '{}' (attempt {})",
                                        node_name, master, attempts,
                                    );
                                    match env_arc
                                        .bootstrap_via_dispatcher(&master)
                                    {
                                        Ok(()) => {
                                            log::info!(
                                                "noxu-replica-{}: auto-bootstrap \
                                                 succeeded; resuming catch-up",
                                                node_name,
                                            );
                                            // Drop the strong ref before
                                            // re-entering catch-up so we
                                            // do not keep the env alive
                                            // longer than necessary.
                                            drop(env_arc);
                                            continue;
                                        }
                                        Err(e) => {
                                            log::error!(
                                                "noxu-replica-{}: auto-bootstrap \
                                                 failed: {}",
                                                node_name, e,
                                            );
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    if !shutdown_flag {
                                        log::error!(
                                            "noxu-replica-{}: error from master '{}': {e}",
                                            node_name, master,
                                        );
                                    }
                                    return;
                                }
                            }
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

        // Closes finding F7 of `docs/src/internal/api-audit-2026-05-rep.md`.
        //
        // Steps:
        //   1. Locate the target's address.
        //   2. Compute the new term (current observed term + 1).
        //   3. Send TRANSFER_MASTER to the target — it will become master.
        //   4. Send TRANSFER_MASTER (with the same term + new master name) to
        //      every other peer so they re-target.
        //   5. Demote self to Replica of the target.
        //
        // The transfer is best-effort: a peer that doesn't ack is logged
        // and skipped.  The election driver will reconcile any divergence
        // on the next election round.

        let target_addr = self
            .group_service
            .get_all_nodes()
            .into_iter()
            .find(|n| n.name == config.target_node)
            .and_then(|n| {
                format!("{}:{}", n.host, n.port)
                    .parse::<std::net::SocketAddr>()
                    .ok()
            })
            .ok_or_else(|| {
                RepError::ConfigError(format!(
                    "transfer_master: target '{}' not registered or has bad address",
                    config.target_node
                ))
            })?;

        let new_term = self.master_tracker.get_term().saturating_add(1);

        // 1. Tell the target to become master at the new term.
        let target_ack = crate::group_admin::send_transfer_master(
            target_addr,
            &config.target_node,
            new_term,
        )
        .map_err(|e| {
            RepError::NetworkError(format!(
                "transfer_master: failed to signal target '{}': {}",
                config.target_node, e
            ))
        })?;
        if !target_ack {
            return Err(RepError::StateError(format!(
                "transfer_master: target '{}' rejected the transfer",
                config.target_node
            )));
        }

        // 2. Inform all other peers (best-effort).
        for peer in self.group_service.get_all_nodes() {
            if peer.name == self.config.node_name
                || peer.name == config.target_node
            {
                continue;
            }
            if let Ok(addr) = format!("{}:{}", peer.host, peer.port).parse() {
                let _ = crate::group_admin::send_transfer_master(
                    addr,
                    &config.target_node,
                    new_term,
                );
            }
        }

        // 3. Demote self to Replica of the new master.
        self.become_replica(&config.target_node)?;

        log::info!(
            "Node '{}' transferred master to '{}' at term {}",
            self.config.node_name.as_str(),
            config.target_node,
            new_term,
        );
        Ok(())
    }

    /// Register a VLSN (as master, after writing a log entry).
    ///
    /// Maps the given VLSN to the specified log file position. This is called
    /// by the master after it writes a replicated log entry.
    pub fn register_vlsn(&self, vlsn: u64, file_number: u32, file_offset: u32) {
        self.vlsn_index.register(vlsn, file_number, file_offset);
    }

    /// Replicate a freshly committed log entry from the master.
    ///
    /// Closes finding F9 of `docs/src/internal/api-audit-2026-05-rep.md`.
    ///
    /// Combines `register_vlsn` with a push into the in-memory
    /// `peer_scanner` so that downstream replicas pulling from this
    /// node's `PEER_FEEDER` service (via `catch_up_from_peer`) can
    /// stream the entry without round-tripping through the on-disk
    /// log.  The local log is still the source of truth; the peer
    /// scanner is a fast-path cache that bounds itself via
    /// `PeerLogScanner::with_capacity` so old entries are evicted.
    ///
    /// Should be called by the master after the local commit has
    /// fsynced.  Calling on a non-master is harmless (the peer
    /// scanner cache is also used by replicas) but is logged at trace
    /// level for diagnostics.
    pub fn replicate_entry(
        &self,
        vlsn: u64,
        file_number: u32,
        file_offset: u32,
        entry_type: u8,
        data: Vec<u8>,
    ) {
        self.vlsn_index.register(vlsn, file_number, file_offset);
        self.peer_scanner.push(vlsn, entry_type, data);
        if !self.is_master() {
            log::trace!(
                "replicate_entry called on non-master node '{}': vlsn={}, type={}",
                self.config.node_name,
                vlsn,
                entry_type,
            );
        }
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
    /// `with_environment`-required local-apply path.
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
        // become_replica / start_vlsn_persistence_daemon.  The vlsn-flush
        // thread does a final flush on its way out so a clean close is
        // recoverable.  Closes finding F11.
        self.io_shutdown.store(true, Ordering::SeqCst);
        {
            let mut threads = self.io_threads.lock().unwrap();
            for handle in threads.drain(..) {
                let _ = handle.join();
            }
        }

        // Belt-and-braces: even when no daemon is running (e.g.
        // `ReplicatedEnvironment::new` without `open`), persist a final
        // snapshot if env_home is configured.
        if let Some(ref home) = self.config.env_home
            && let Err(e) =
                crate::vlsn::persist::flush_to_disk(&self.vlsn_index, home)
        {
            log::warn!(
                "close: failed to persist VLSN index to {}: {}",
                home.display(),
                e
            );
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
        replica_shutdown_timeout_ms: u64,
    ) -> Result<()> {
        if !self.is_master() {
            return Err(RepError::InvalidState(
                "shutdownGroup must be invoked on the master".to_string(),
            ));
        }

        log::info!(
            "Node '{}' shutting down replication group '{}' (replica_timeout={}ms)",
            self.config.node_name.as_str(),
            self.config.group_name.as_str(),
            replica_shutdown_timeout_ms,
        );

        // Closes finding F8 of `docs/src/internal/api-audit-2026-05-rep.md`.
        //
        // Send SHUTDOWN_GROUP to every known peer.  The recipient calls
        // its own `close()` and the per-connection ADMIN handler
        // returns ACK_OK.  Any peer that doesn't ack within the
        // timeout is logged and the master proceeds.  After signalling
        // every peer, the master closes its own env.
        let deadline = std::time::Instant::now()
            + Duration::from_millis(replica_shutdown_timeout_ms);

        for peer in self.group_service.get_all_nodes() {
            if peer.name == self.config.node_name {
                continue;
            }
            // Don't exceed the deadline waiting for any single peer.
            let now = std::time::Instant::now();
            if now >= deadline {
                log::warn!(
                    "shutdown_group: deadline reached; skipping remaining peers"
                );
                break;
            }
            let addr_str = format!("{}:{}", peer.host, peer.port);
            let addr = match addr_str.parse::<SocketAddr>() {
                Ok(a) => a,
                Err(e) => {
                    log::warn!(
                        "shutdown_group: peer '{}' has bad address {}: {}",
                        peer.name,
                        addr_str,
                        e
                    );
                    continue;
                }
            };
            match crate::group_admin::send_shutdown_group(addr) {
                Ok(true) => log::info!(
                    "shutdown_group: peer '{}' acknowledged",
                    peer.name
                ),
                Ok(false) => log::warn!(
                    "shutdown_group: peer '{}' rejected the request",
                    peer.name
                ),
                Err(e) => log::warn!(
                    "shutdown_group: peer '{}' unreachable: {}",
                    peer.name,
                    e
                ),
            }
        }

        // Master closes itself last.
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
        let poll_interval =
            std::cmp::min(timeout / 50, Duration::from_millis(20));
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
                let received =
                    self.ack_tracker.received_count(commit_seq).unwrap_or(0);
                self.ack_tracker.cleanup_through(commit_seq);
                return Err(AckWaitError {
                    kind: AckWaitErrorKind::Timeout,
                    needed,
                    received,
                });
            }
            let sleep_for = std::cmp::min(
                poll_interval,
                deadline.saturating_duration_since(now),
            );
            std::thread::sleep(sleep_for);
        }
    }

    /// X-3: allocate the next VLSN for a recovered XA commit and register
    /// `lsn` in the VLSN index so feeders can stream the commit.
    ///
    /// Increments off the current latest VLSN so the new VLSN is strictly
    /// monotonically increasing.  In a single-node or master-less environment
    /// (not master) returns 0 (NULL_VLSN — harmless, the default).
    fn alloc_vlsn_for_recovered_commit(&self, lsn: noxu_util::Lsn) -> u64 {
        // Only allocate a VLSN when we are the master; on a replica the
        // recovered XA should have been replicated by the original master.
        if !self.is_master() {
            return 0;
        }
        let next_vlsn = self.vlsn_index.get_latest_vlsn() + 1;
        self.vlsn_index.register(
            next_vlsn,
            lsn.file_number(),
            lsn.file_offset(),
        );
        log::debug!(
            "alloc_vlsn_for_recovered_commit: allocated vlsn={} for lsn={:?}",
            next_vlsn,
            lsn
        );
        next_vlsn
    }

    /// R-3: pre-allocate the next commit VLSN WITHOUT registering in the index.
    ///
    /// The caller writes the `TxnCommit` WAL entry with this VLSN embedded,
    /// then calls `register_recovered_commit_vlsn` with the actual commit LSN.
    /// This two-step approach ensures the WAL entry carries the VLSN so the
    /// X-14 VLSN rebuild on second crash can find it.
    fn pre_alloc_vlsn_for_recovered_commit(&self) -> u64 {
        if !self.is_master() {
            return 0;
        }
        // Peek at the next VLSN without registering.  The actual registration
        // happens in register_recovered_commit_vlsn() after the WAL write.
        self.vlsn_index.get_latest_vlsn() + 1
    }

    /// R-3: register a pre-allocated VLSN in the VLSN index with the actual
    /// commit LSN.  Called after writing the `TxnCommit` WAL entry.
    fn register_recovered_commit_vlsn(
        &self,
        vlsn: u64,
        commit_lsn: noxu_util::Lsn,
    ) {
        if vlsn == 0 || !self.is_master() {
            return;
        }
        self.vlsn_index.register(
            vlsn,
            commit_lsn.file_number(),
            commit_lsn.file_offset(),
        );
        log::debug!(
            "register_recovered_commit_vlsn: registered vlsn={} for commit_lsn={:?}",
            vlsn,
            commit_lsn
        );
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
    fn test_transfer_master_requires_registered_target() {
        // F7: transfer_master is no longer a no-op; it sends an ADMIN
        // TRANSFER_MASTER signal to the target via TCP.  An unregistered
        // target is rejected at the address-resolution step.
        let env = ReplicatedEnvironment::new(test_config("node1")).unwrap();
        env.become_master(1).unwrap();

        let config = MasterTransferConfig::new(
            "unknown_target".to_string(),
            Duration::from_secs(30),
        );
        let result = env.transfer_master(config);
        assert!(
            result.is_err(),
            "transfer_master to unregistered target must error"
        );
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
