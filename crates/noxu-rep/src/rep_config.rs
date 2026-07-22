//! Replication configuration.
//!

use std::path::PathBuf;
use std::time::Duration;

use crate::commit_durability::CommitDurability;
use crate::consistency::ConsistencyPolicy;
use crate::node_type::NodeType;
use crate::quorum_policy::QuorumPolicy;
use crate::rep_node::RepNode;
use crate::stream::reconnect::ReconnectConfig;

/// Default election timeout.
const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_secs(10);
/// Default heartbeat interval.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
/// Default replication port.
///
/// Default port:
/// changed from `5001` (which collides with PostgreSQL's REPMGR
/// default and various Cisco services) to `14_001`, an unprivileged
/// IANA-unassigned default.  Most production deployments override
/// this; the new default is just intended to fail closed during
/// development rather than silently bind on something else's port.
const DEFAULT_NODE_PORT: u16 = 14_001;
/// Default per-phase election message timeout.
const DEFAULT_ELECTION_PHASE_TIMEOUT: Duration = Duration::from_millis(500);
/// Default phi accrual sample window size.
const DEFAULT_PHI_WINDOW_SIZE: usize = 200;

/// Wire-level transport selected for replication traffic.
///
/// The in-memory transport is a first-class
/// production option alongside TCP / TLS / QUIC.  This enum lets a
/// caller declare the transport choice in [`RepConfig`] so higher-level
/// orchestration code (e.g., the test harness, the `RepTestBase`
/// integration tests, embedded deployments) can route channel
/// construction through the right factory.
///
/// Note: noxu-rep's [`crate::net`] channel types are constructed
/// directly by the user code that drives the cluster (a `TcpListener`
/// on a port, a [`crate::net::InMemoryTransport::new_group`] mesh,
/// etc.).  This field is therefore advisory — it documents intent and
/// lets observability / chaos / harness layers introspect the
/// transport without inspecting individual channel types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepTransportKind {
    /// Plaintext TCP via [`crate::net::TcpChannel`].
    Tcp,
    /// TLS-encrypted TCP via `crate::net::TlsTcpChannel`
    /// (requires `tls-rustls` or `tls-native`).
    Tls,
    /// QUIC over UDP via `crate::net::QuicChannel`
    /// (requires the `quic` feature).
    Quic,
    /// In-process [`crate::net::InMemoryTransport`].  Useful for
    /// embedded deployments and integration tests that want real
    /// `ReplicatedEnvironment` behaviour without opening sockets.
    InMemory,
}

impl Default for RepTransportKind {
    /// Defaults to [`RepTransportKind::Tcp`] to preserve
    /// backward compatibility.
    fn default() -> Self {
        Self::Tcp
    }
}

/// Configuration for a replication node.
///
/// Use the builder
/// pattern to construct.
#[derive(Debug, Clone)]
pub struct RepConfig {
    /// Name of the replication group.
    pub group_name: String,
    /// Name of this node within the group (must be unique).
    pub node_name: String,
    /// Hostname or IP address for this node.
    pub node_host: String,
    /// Port for replication communication.
    pub node_port: u16,
    /// Type of this node.
    pub node_type: NodeType,
    /// Timeout for elections.
    pub election_timeout: Duration,
    /// Interval between heartbeat messages.
    pub heartbeat_interval: Duration,
    /// Default consistency policy for read operations.
    pub consistency_policy: ConsistencyPolicy,
    /// Default commit durability for replicated transactions.
    ///
    /// The `ack_timeout` field on `commit_durability` governs the
    /// commit-side wait for replica acks; there is no separate
    /// per-RepConfig replica-ack timeout.
    pub commit_durability: CommitDurability,
    /// Path to the local environment home directory (`.ndb` files).
    ///
    /// When set, `ReplicatedEnvironment` registers a `NetworkRestoreServer`
    /// on the service dispatcher so that other nodes can restore from this
    /// node via the `"RESTORE"` service.
    pub env_home: Option<PathBuf>,
    /// Quorum policy for elections. Default: `SimpleMajority`.
    pub quorum_policy: QuorumPolicy,
    /// Phi accrual suspicion threshold.
    ///
    /// `None` (default) uses a binary heartbeat timeout.
    /// `Some(8.0)` enables phi accrual detection with the paper's recommended
    /// threshold (mistake rate ≈ 10⁻⁸).
    pub phi_threshold: Option<f64>,
    /// Sliding-window size for phi accrual inter-arrival samples.
    ///
    /// Default `200` is adequate for LAN; use `1000` for WAN.
    pub phi_window_size: usize,
    /// Fully-described peers added to the replication group at startup.
    ///
    /// Useful for pre-populating quoracle capacity/latency metadata.
    pub initial_peers: Vec<RepNode>,
    /// Timeout per peer message exchange during Phase 1 and Phase 2 of an
    /// election.  Default: 500 ms.
    pub election_phase_timeout: Duration,
    /// Reconnection backoff configuration for replica partition recovery.
    pub reconnect_config: ReconnectConfig,
    /// Wire-level transport this node will use.
    ///
    /// This field lets callers declare whether they
    /// intend to drive replication over TCP, TLS, QUIC, or the
    /// in-process [`crate::net::InMemoryTransport`].  See
    /// [`RepTransportKind`] for the variants.  Defaults to
    /// [`RepTransportKind::Tcp`] for backward compatibility.
    pub transport_kind: RepTransportKind,

    /// Allowlist of peer subject names for mTLS enforcement (Phase 2, v3.1.0).
    ///
    /// When non-empty and [`RepTransportKind::Tls`] is configured, the
    /// server will:
    ///
    /// 1. **Require a client certificate** on every incoming TLS connection.
    /// 2. **Validate the chain** against the CA roots in the `TlsConfig`.
    /// 3. **Check subject names** — the peer's Subject Common Name (CN) and
    ///    every DNS Subject Alternative Name (SAN) entry are compared
    ///    case-insensitively against this list.  If none match, the
    ///    handshake is aborted before any application data is exchanged.
    ///
    /// Matching is exact (no wildcards).  Names are compared
    /// case-insensitively.  Whitespace-only and empty entries are ignored.
    ///
    /// The client side automatically presents its own certificate when the
    /// `TlsConfig` identity is `PemFiles` or `PemBytes`.
    ///
    /// ## Empty list
    ///
    /// An empty list means no peers are admitted (`PeerAllowlistVerifier`
    /// returns an error at construction time, which surfaces as a
    /// `RepError::ConfigError` from `TlsConfig::to_rustls_server_config_with_allowlist`).
    /// This is intentional fail-closed behaviour: an empty allowlist is
    /// almost certainly a misconfiguration.
    ///
    /// ## Transport requirement
    ///
    /// Enforcement requires `transport_kind = RepTransportKind::Tls`.  With
    /// plain TCP there is no TLS handshake and therefore no cert to inspect.
    /// Setting this field with a non-TLS transport emits a `log::warn!`.
    pub peer_allowlist: Vec<String>,

    /// TLS configuration for the service dispatcher (Phase 3).
    ///
    /// When set and `transport_kind` is [`RepTransportKind::Tls`],
    /// [`crate::replicated_environment::ReplicatedEnvironment`] will
    /// start a `TlsTcpServiceDispatcher` (feature `tls-rustls`)
    /// instead of the plain-TCP dispatcher.  Combined with a non-empty
    /// `peer_allowlist`, this enforces mTLS on every incoming replication
    /// connection at the dispatcher level.
    ///
    /// `None` (the default) preserves the Phase-2 behaviour: the
    /// dispatcher uses plain TCP and the operator must wire
    /// `TlsTcpChannelListener::bind_with_tls_and_allowlist` separately.
    pub tls_config: Option<crate::tls::TlsConfig>,

    /// Enable chained / replica-to-replica log feeding (default `false`).
    ///
    /// When `true`, a node that becomes a **replica** ALSO runs a feeder
    /// source on its `PEER_FEEDER` service, serving the VLSN-tagged log
    /// stream from its OWN WAL to a downstream replica.  This lets a
    /// mid-tier replica relay the stream (master → R1 → R2) instead of every
    /// replica connecting directly to the master.
    ///
    /// Faithful to JE's cascading-feeder model: `FeederSource` is
    /// documented as "a real Master OR a Replica in a Replica chain that is
    /// replaying log records it received from some other source"
    /// (`FeederSource.java`).  The feeder source on a replica reads its
    /// VLSNIndex + log files exactly as `MasterFeederSource` does on the
    /// master, so the downstream's syncup (REP-1) and live-apply (REP-7)
    /// work unchanged against a replica-feeder source.
    ///
    /// **Default `false`** preserves master-direct behaviour: a replica
    /// does not feed downstream peers unless cascade is explicitly enabled.
    ///
    /// **Durability bound**: a mid-tier replica does NOT count its
    /// downstream's acks toward the master's commit-durability quorum.
    /// JE evaluates the durability quorum at the master
    /// (`FeederManager.getNumCurrentAckFeeders`); a chained replica only
    /// tracks the downstream's progress for its own VLSN/lag bookkeeping.
    /// A downstream replica is therefore never more durable than the
    /// entries its mid-tier has itself persisted.
    pub cascade_feeding: bool,

    /// Explicit opt-out of the enforced-authentication default.
    ///
    /// By default (`false`) Noxu **refuses to start a replication
    /// dispatcher on an unauthenticated wire transport**: a node
    /// configured with the default [`RepTransportKind::Tcp`] (or QUIC)
    /// and no mTLS material fails [`crate::ReplicatedEnvironment::new`]
    /// with a `ConfigError` directing the operator to configure
    /// `transport_kind = Tls` + a `tls_config` + a non-empty
    /// `peer_allowlist`.  This closes the external review's core finding
    /// (NA-1/NA-4/NA-8): you cannot *accidentally* run an unauthenticated
    /// replication cluster.
    ///
    /// This mirrors BDB-JE HA's fail-closed posture: JE authenticates the
    /// data channel via mutual TLS (`SSLAuthenticator.isTrusted(SSLSession)`
    /// / `SSLMirrorAuthenticator`, `com.sleepycat.je.rep.net`), so a peer's
    /// identity is *verified from its certificate*, never self-claimed on
    /// the wire.  Noxu's `PeerAllowlistVerifier` (see `crate::auth`) is the
    /// direct analogue of `SSLAuthenticator`.
    ///
    /// Set `true` to permit the plaintext / skip-verify path for a
    /// trusted-network deployment, local development, or CI.  Doing so
    /// emits a loud `log::warn!` at startup — the transport carries **no**
    /// peer authentication and any host that can reach the port can join
    /// the group and impersonate a peer.  Only enable this when the
    /// replication network is fully isolated (host firewall / VPC
    /// segmentation with statically-known peer IPs).
    ///
    /// Under `cfg(test)` / the `test-harness` feature this defaults to
    /// `true` so the in-process test harness and unit tests keep working
    /// on the plaintext / in-memory transports without per-test PKI.  In a
    /// production build it is always `false` unless the operator sets it.
    pub insecure_no_auth: bool,
}

/// The default value of [`RepConfig::insecure_no_auth`].
///
/// Fail-closed (`false`) in a production build; opt-out (`true`) only when
/// compiled for tests (`cfg(test)`) or with the `test-harness` feature, so
/// the existing test suite and in-process harness keep running on the
/// plaintext / in-memory transports without per-test certificate material.
pub(crate) const DEFAULT_INSECURE_NO_AUTH: bool =
    cfg!(any(test, feature = "test-harness"));

impl RepConfig {
    /// Creates a builder for `RepConfig`.
    pub fn builder(
        group_name: &str,
        node_name: &str,
        node_host: &str,
    ) -> RepConfigBuilder {
        RepConfigBuilder {
            group_name: group_name.to_string(),
            node_name: node_name.to_string(),
            node_host: node_host.to_string(),
            node_port: DEFAULT_NODE_PORT,
            node_type: NodeType::Electable,
            election_timeout: DEFAULT_ELECTION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            consistency_policy: ConsistencyPolicy::default(),
            commit_durability: CommitDurability::default(),
            env_home: None,
            quorum_policy: QuorumPolicy::SimpleMajority,
            phi_threshold: None,
            phi_window_size: DEFAULT_PHI_WINDOW_SIZE,
            initial_peers: Vec::new(),
            election_phase_timeout: DEFAULT_ELECTION_PHASE_TIMEOUT,
            reconnect_config: ReconnectConfig::default(),
            transport_kind: RepTransportKind::default(),
            peer_allowlist: Vec::new(),
            tls_config: None,
            cascade_feeding: false,
            insecure_no_auth: DEFAULT_INSECURE_NO_AUTH,
        }
    }

    /// Convenience constructor matching the original v1.4 shape.
    ///
    /// Equivalent to `builder(group, node, host).node_port(port).build()`.
    /// Provided so doc snippets and short tests don't need to write the
    /// full builder chain.
    /// "`RepConfig::new` example").
    pub fn new(
        group_name: impl Into<String>,
        node_name: impl Into<String>,
        node_host: impl Into<String>,
        node_port: u16,
    ) -> RepConfig {
        let g = group_name.into();
        let n = node_name.into();
        let h = node_host.into();
        RepConfig::builder(&g, &n, &h).node_port(node_port).build()
    }

    /// Returns the socket address string for this node.
    pub fn socket_address(&self) -> String {
        format!("{}:{}", self.node_host, self.node_port)
    }
}

/// Builder for [`RepConfig`].
#[derive(Debug, Clone)]
pub struct RepConfigBuilder {
    group_name: String,
    node_name: String,
    node_host: String,
    node_port: u16,
    node_type: NodeType,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    consistency_policy: ConsistencyPolicy,
    commit_durability: CommitDurability,
    env_home: Option<PathBuf>,
    quorum_policy: QuorumPolicy,
    phi_threshold: Option<f64>,
    phi_window_size: usize,
    initial_peers: Vec<RepNode>,
    election_phase_timeout: Duration,
    reconnect_config: ReconnectConfig,
    transport_kind: RepTransportKind,
    peer_allowlist: Vec<String>,
    tls_config: Option<crate::tls::TlsConfig>,
    cascade_feeding: bool,
    insecure_no_auth: bool,
}

impl RepConfigBuilder {
    /// Sets the replication port.
    pub fn node_port(mut self, port: u16) -> Self {
        self.node_port = port;
        self
    }

    /// Sets the node type.
    pub fn node_type(mut self, node_type: NodeType) -> Self {
        self.node_type = node_type;
        self
    }

    /// Sets the election timeout.
    pub fn election_timeout(mut self, timeout: Duration) -> Self {
        self.election_timeout = timeout;
        self
    }

    /// Sets the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Sets the consistency policy.
    pub fn consistency_policy(mut self, policy: ConsistencyPolicy) -> Self {
        self.consistency_policy = policy;
        self
    }

    /// Sets the commit durability.
    pub fn commit_durability(mut self, durability: CommitDurability) -> Self {
        self.commit_durability = durability;
        self
    }

    /// Sets the environment home directory (serves `.ndb` files for network restore).
    pub fn env_home(mut self, path: impl Into<PathBuf>) -> Self {
        self.env_home = Some(path.into());
        self
    }

    /// Enable chained / replica-to-replica log feeding (default `false`).
    ///
    /// When `true`, a node that becomes a replica also runs a feeder source
    /// on its `PEER_FEEDER` service, serving its OWN WAL to a downstream
    /// replica (master → R1 → R2).  See [`RepConfig::cascade_feeding`] for
    /// the JE `FeederSource` citation and the durability bound.
    pub fn cascade_feeding(mut self, enabled: bool) -> Self {
        self.cascade_feeding = enabled;
        self
    }

    /// Sets the quorum policy for elections (default: `SimpleMajority`).
    pub fn quorum_policy(mut self, policy: QuorumPolicy) -> Self {
        self.quorum_policy = policy;
        self
    }

    /// Enable phi accrual failure detection with the given suspicion threshold.
    ///
    /// `8.0` is the paper's recommended production value (mistake rate ≈ 10⁻⁸).
    /// Call with `None` to revert to binary heartbeat timeout detection.
    pub fn phi_threshold(mut self, threshold: Option<f64>) -> Self {
        self.phi_threshold = threshold;
        self
    }

    /// Sets the phi accrual inter-arrival sample window size (default 200).
    pub fn phi_window_size(mut self, size: usize) -> Self {
        self.phi_window_size = size;
        self
    }

    /// Add a fully-described initial peer to the group at startup.
    pub fn add_initial_peer(mut self, node: RepNode) -> Self {
        self.initial_peers.push(node);
        self
    }

    /// Set the per-peer message timeout for Phase 1 and Phase 2 election
    /// exchanges (default: 500 ms).
    pub fn election_phase_timeout(mut self, timeout: Duration) -> Self {
        self.election_phase_timeout = timeout;
        self
    }

    /// Sets the reconnection backoff configuration for replica partition recovery.
    pub fn reconnect_config(mut self, config: ReconnectConfig) -> Self {
        self.reconnect_config = config;
        self
    }

    /// Sets the wire-level transport this node will use.
    ///
    /// Defaults to [`RepTransportKind::Tcp`].
    /// [`RepTransportKind::InMemory`] for in-process clusters.
    pub fn transport_kind(mut self, kind: RepTransportKind) -> Self {
        self.transport_kind = kind;
        self
    }

    /// Set the mTLS peer allowlist (Phase 2, v3.1.0).
    ///
    /// When non-empty and `transport_kind` is [`RepTransportKind::Tls`],
    /// incoming TLS connections must present a certificate whose Subject CN
    /// or DNS SAN matches at least one entry here (case-insensitive, exact
    /// match).  Connections that fail the check are rejected at the TLS
    /// handshake layer.
    ///
    /// See [`RepConfig::peer_allowlist`] for full details.
    pub fn peer_allowlist(mut self, names: Vec<String>) -> Self {
        self.peer_allowlist = names;
        self
    }

    /// Set the TLS configuration for the service dispatcher (Phase 3).
    ///
    /// When set and `transport_kind` is [`RepTransportKind::Tls`],
    /// [`crate::replicated_environment::ReplicatedEnvironment`] will use a
    /// TLS-capable service dispatcher that enforces mTLS when
    /// `peer_allowlist` is also non-empty.
    ///
    /// See [`RepConfig::tls_config`] for full details.
    pub fn tls_config(mut self, tls: crate::tls::TlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    /// Explicitly opt OUT of the enforced-authentication default.
    ///
    /// By default a node configured with an unauthenticated transport
    /// (the default [`RepTransportKind::Tcp`], or QUIC) and no mTLS
    /// material refuses to start.  Pass `true` here to permit the
    /// plaintext / skip-verify path for a trusted-network deployment,
    /// local development, or CI.  A loud `log::warn!` is emitted at
    /// startup when it is enabled.
    ///
    /// See [`RepConfig::insecure_no_auth`] for the full rationale and the
    /// BDB-JE `SSLAuthenticator` citation.
    pub fn insecure_no_auth(mut self, insecure: bool) -> Self {
        self.insecure_no_auth = insecure;
        self
    }

    /// Builds the `RepConfig`.
    pub fn build(self) -> RepConfig {
        RepConfig {
            group_name: self.group_name,
            node_name: self.node_name,
            node_host: self.node_host,
            node_port: self.node_port,
            node_type: self.node_type,
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            consistency_policy: self.consistency_policy,
            commit_durability: self.commit_durability,
            env_home: self.env_home,
            quorum_policy: self.quorum_policy,
            phi_threshold: self.phi_threshold,
            phi_window_size: self.phi_window_size,
            initial_peers: self.initial_peers,
            election_phase_timeout: self.election_phase_timeout,
            reconnect_config: self.reconnect_config,
            transport_kind: self.transport_kind,
            peer_allowlist: self.peer_allowlist,
            tls_config: self.tls_config,
            cascade_feeding: self.cascade_feeding,
            insecure_no_auth: self.insecure_no_auth,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_durability::ReplicaAckPolicy;

    #[test]
    fn test_builder_defaults() {
        let config = RepConfig::builder("group1", "node1", "localhost").build();
        assert_eq!(config.group_name, "group1");
        assert_eq!(config.node_name, "node1");
        assert_eq!(config.node_host, "localhost");
        assert_eq!(config.node_port, DEFAULT_NODE_PORT);
        assert_eq!(config.node_type, NodeType::Electable);
        assert_eq!(config.election_timeout, DEFAULT_ELECTION_TIMEOUT);
        assert_eq!(config.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL);
        assert_eq!(config.consistency_policy, ConsistencyPolicy::NoConsistency);
    }

    #[test]
    fn test_default_port_is_unprivileged() {
        // Wave 1C audit cleanup (rep low "default port collision"): the
        // default port must be in the IANA unassigned range and is not
        // shared with another well-known service we might collide with
        // (5001 was the v1.5.0 default; it overlaps with REPMGR among
        // others).
        let config = RepConfig::builder("g", "n", "h").build();
        assert_eq!(config.node_port, 14_001);
    }

    #[test]
    fn test_new_constructor_matches_builder() {
        let a = RepConfig::new("g", "n", "h", 6000);
        let b = RepConfig::builder("g", "n", "h").node_port(6000).build();
        // The two paths must produce the same on-the-wire identity.
        assert_eq!(a.group_name, b.group_name);
        assert_eq!(a.node_name, b.node_name);
        assert_eq!(a.node_host, b.node_host);
        assert_eq!(a.node_port, b.node_port);
        assert_eq!(a.node_type, b.node_type);
    }

    #[test]
    fn test_builder_custom_port() {
        let config = RepConfig::builder("g", "n", "h").node_port(6000).build();
        assert_eq!(config.node_port, 6000);
    }

    #[test]
    fn test_builder_node_type() {
        let config = RepConfig::builder("g", "n", "h")
            .node_type(NodeType::Secondary)
            .build();
        assert_eq!(config.node_type, NodeType::Secondary);
    }

    #[test]
    fn test_builder_timeouts() {
        let config = RepConfig::builder("g", "n", "h")
            .election_timeout(Duration::from_secs(20))
            .heartbeat_interval(Duration::from_millis(500))
            .build();
        assert_eq!(config.election_timeout, Duration::from_secs(20));
        assert_eq!(config.heartbeat_interval, Duration::from_millis(500));
    }

    #[test]
    fn test_builder_consistency_policy() {
        let policy = ConsistencyPolicy::TimeConsistency {
            max_lag: Duration::from_millis(500),
            timeout: Duration::from_secs(10),
        };
        let config = RepConfig::builder("g", "n", "h")
            .consistency_policy(policy.clone())
            .build();
        assert_eq!(config.consistency_policy, policy);
    }

    #[test]
    fn test_builder_commit_durability() {
        let durability = CommitDurability::new(
            ReplicaAckPolicy::All,
            Duration::from_secs(15),
        );
        let config = RepConfig::builder("g", "n", "h")
            .commit_durability(durability)
            .build();
        assert_eq!(config.commit_durability.ack_policy, ReplicaAckPolicy::All);
        assert_eq!(
            config.commit_durability.ack_timeout,
            Duration::from_secs(15)
        );
    }

    #[test]
    fn test_insecure_no_auth_default_matches_build_profile() {
        // The default is opt-out (true) only when compiled for tests /
        // the test-harness feature, and fail-closed (false) in production.
        let config = RepConfig::builder("g", "n", "h").build();
        assert_eq!(config.insecure_no_auth, DEFAULT_INSECURE_NO_AUTH);
        // Under `cargo test` this module compiles with cfg(test) set, so the
        // default here is the opt-out value.
        assert!(
            config.insecure_no_auth,
            "under cfg(test) the default must be the insecure opt-out"
        );
    }

    #[test]
    fn test_insecure_no_auth_builder_flips_it() {
        let secure =
            RepConfig::builder("g", "n", "h").insecure_no_auth(false).build();
        assert!(!secure.insecure_no_auth);
        let insecure =
            RepConfig::builder("g", "n", "h").insecure_no_auth(true).build();
        assert!(insecure.insecure_no_auth);
    }

    #[test]
    fn test_socket_address() {
        let config =
            RepConfig::builder("g", "n", "192.168.1.1").node_port(7000).build();
        assert_eq!(config.socket_address(), "192.168.1.1:7000");
    }

    #[test]
    fn test_builder_chaining() {
        let config = RepConfig::builder("mygroup", "node1", "10.0.0.1")
            .node_port(5555)
            .node_type(NodeType::Arbiter)
            .election_timeout(Duration::from_secs(30))
            .build();
        assert_eq!(config.group_name, "mygroup");
        assert_eq!(config.node_name, "node1");
        assert_eq!(config.node_host, "10.0.0.1");
        assert_eq!(config.node_port, 5555);
        assert_eq!(config.node_type, NodeType::Arbiter);
        assert_eq!(config.election_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_config_clone() {
        let config = RepConfig::builder("g", "n", "h").build();
        let cloned = config.clone();
        assert_eq!(config.group_name, cloned.group_name);
        assert_eq!(config.node_name, cloned.node_name);
    }

    #[test]
    fn test_config_debug() {
        let config = RepConfig::builder("g", "n", "h").build();
        let s = format!("{:?}", config);
        assert!(s.contains("RepConfig"));
    }
}
