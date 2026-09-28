//! Replication subscription for receiving replicated entries from a feeder.
//!
//! The
//! Subscription connects to a feeder node and receives a stream of
//! replicated log entries starting from a given VLSN. This is used by
//! subscribers that want to consume the replication stream without being
//! full replica members of the group.

use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use noxu_sync::Mutex;

use crate::error::{RepError, Result};

use crate::node_type::NodeType;
use crate::vlsn::VlsnRange;

/// The stream mode requested when a subscriber starts consuming the
/// replication stream from a given VLSN.
///
/// Port of `BaseProtocol.EntryRequestType` (JE
/// `com.sleepycat.je.rep.stream.BaseProtocol.EntryRequestType`). It governs
/// how the feeder resolves a requested start VLSN (`RV`) against the VLSN
/// range `[LOW, HIGH]` it currently holds:
///
/// ```text
/// -------------------------------------------------------------------
///     MODE      | RV < LOW  |   RV in [LOW, HIGH] | RV > HIGH
/// -------------------------------------------------------------------
///  DEFAULT      | NOT_FOUND |   REQUESTED ENTRY   | ALT MATCH POINT (lastSync)
///  AVAILABLE    |   LOW     |   REQUESTED ENTRY   | HIGH
///  NOW          |   HIGH    |   HIGH              | HIGH
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryRequestType {
    /// Below-range requests fail (`EntryNotFound` / `InsufficientLog`);
    /// above-range requests get the feeder's `lastSync` as an alternate
    /// matchpoint. This is the mode ordinary replicas use.
    Default,
    /// Clamp to what the feeder holds: below-range → `LOW`, above-range →
    /// `HIGH`. Never fails as long as the range is non-empty.
    Available,
    /// Always start at `HIGH` (the current tail), regardless of the request.
    Now,
}

/// Outcome of resolving a requested start VLSN against a feeder's VLSN range
/// under a given [`EntryRequestType`].
///
/// Mirrors the messages `FeederReplicaSyncup.makeResponseToEntryRequest`
/// returns: an `Entry` at a resolved VLSN, an `AlternateMatchpoint` (the
/// feeder's `lastSync`), or `EntryNotFound`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartResolution {
    /// The stream will start at this VLSN (JE `Entry`).
    Start(u64),
    /// The requested VLSN is above the range; the feeder counters with its
    /// `lastSync` entry as an alternate matchpoint (JE `AlternateMatchpoint`).
    AlternateMatchpoint(u64),
    /// The requested VLSN is below the range and cannot be served
    /// (JE `EntryNotFound`, surfaced to the subscriber as
    /// `InsufficientLogException`).
    NotFound,
}

impl EntryRequestType {
    /// All three modes, in declaration order — the equivalent of JE's
    /// `EntryRequestType.values()`.
    pub const fn values() -> [EntryRequestType; 3] {
        [
            EntryRequestType::Default,
            EntryRequestType::Available,
            EntryRequestType::Now,
        ]
    }

    /// Resolve a requested start VLSN `req` against the feeder's `range`
    /// under this mode.
    ///
    /// Port of `FeederReplicaSyncup.makeResponseToEntryRequest` (the pure
    /// request-type arm of it). `range.get_first()`/`get_last()` are the
    /// inclusive `[LOW, HIGH]` bounds; `range.get_last_sync()` is the
    /// `lastSync` alternate. A `0` sync VLSN means "no syncable entry"
    /// (JE `VLSN.NULL_VLSN`), which yields `NotFound` in `Default` mode.
    pub fn resolve(self, req: u64, range: &VlsnRange) -> StartResolution {
        let low = range.get_first();
        let high = range.get_last();

        // NOW: always the high end regardless of the requested VLSN.
        if self == EntryRequestType::Now {
            return StartResolution::Start(high);
        }

        if req < low {
            // Below the range.
            return match self {
                EntryRequestType::Available => StartResolution::Start(low),
                // Default mode: EntryNotFound.
                _ => StartResolution::NotFound,
            };
        }

        if req > high {
            // Above the range.
            return match self {
                EntryRequestType::Available => StartResolution::Start(high),
                _ => {
                    // Default mode: counter with lastSync (alternate
                    // matchpoint). A 0 sync VLSN means no syncable entry ->
                    // NotFound (network restore in JE).
                    let last_sync = range.get_last_sync();
                    if last_sync == 0 {
                        StartResolution::NotFound
                    } else {
                        StartResolution::AlternateMatchpoint(last_sync)
                    }
                }
            };
        }

        // In range: serve the requested entry.
        StartResolution::Start(req)
    }
}

/// Configuration for a replication subscription.
///
/// Specifies the subscriber
/// identity, the replication group to subscribe to, the feeder to connect
/// to, and the starting VLSN.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// Name of the subscriber node.
    pub subscriber_name: String,
    /// Name of the replication group.
    pub group_name: String,
    /// Hostname of the feeder to connect to.
    pub feeder_host: String,
    /// Feeder to connect to.
    pub feeder_port: u16,
    /// VLSN to start streaming from.
    pub start_vlsn: u64,
    /// Subscriber home directory (JE `SubscriptionConfig.subHome`). A
    /// mandatory, non-empty parameter in JE's `verifyParameters`.
    pub subscriber_home: String,
    /// Node type this subscriber presents to the feeder. JE only permits
    /// `SECONDARY` or `EXTERNAL`; Noxu has no `EXTERNAL` variant, so a
    /// subscription node is `Secondary` (see [`SubscriptionConfig::new`]).
    pub node_type: NodeType,
    /// Optional replication-group UUID. When set, the feeder must belong to
    /// the same group or subscription is rejected (JE `groupUUID`).
    pub group_uuid: Option<String>,
    /// Stream mode governing start-VLSN resolution (JE `streamMode`,
    /// default `DEFAULT`).
    pub stream_mode: EntryRequestType,
    /// Channel (replica) timeout in milliseconds (JE `getChannelTimeout`).
    pub channel_timeout_ms: u64,
    /// Pre-heartbeat timeout in milliseconds (JE `getPreHeartbeatTimeout`).
    pub pre_heartbeat_timeout_ms: u64,
    /// Stream-open timeout in milliseconds (JE `getStreamOpenTimeout`).
    pub stream_open_timeout_ms: u64,
    /// Heartbeat interval in milliseconds (JE `getHeartbeatIntervalMs`).
    pub heartbeat_interval_ms: u32,
    /// Input message queue size (JE `getInputMessageQueueSize`).
    pub input_message_queue_size: u32,
    /// Output message queue size (JE `getOutputMessageQueueSize`).
    pub output_message_queue_size: u32,
    /// Socket receive buffer size (JE `getReceiveBufferSize`).
    pub receive_buffer_size: u32,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        SubscriptionConfig {
            subscriber_name: String::new(),
            group_name: String::new(),
            feeder_host: String::new(),
            feeder_port: 0,
            start_vlsn: 0,
            subscriber_home: ".".into(),
            node_type: NodeType::Secondary,
            group_uuid: None,
            stream_mode: EntryRequestType::Default,
            channel_timeout_ms: 0,
            pre_heartbeat_timeout_ms: 0,
            stream_open_timeout_ms: 0,
            heartbeat_interval_ms: 0,
            input_message_queue_size: 0,
            output_message_queue_size: 0,
            receive_buffer_size: 0,
        }
    }
}

impl SubscriptionConfig {
    /// Construct a validated subscription config, mirroring JE's
    /// `SubscriptionConfig` constructor + `verifyParameters`.
    ///
    /// JE rejects (with `IllegalArgumentException`) a missing subscriber
    /// name, home, subscriber host/port, feeder host/port, or group name, and
    /// a node type that is neither `SECONDARY` nor `EXTERNAL`. Noxu surfaces
    /// these as `RepError::ConfigError`. Noxu has no `EXTERNAL` node type, so
    /// only `Secondary` is accepted here (see AGENTS.md: node roles are a
    /// closed enum; `EXTERNAL` is a JE-only external-consumer role).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subscriber_name: &str,
        subscriber_home: &str,
        feeder_host: &str,
        feeder_port: u16,
        group_name: &str,
        group_uuid: Option<String>,
        node_type: NodeType,
    ) -> Result<Self> {
        if subscriber_name.is_empty() {
            return Err(RepError::ConfigError(
                "subscriber node name cannot be empty".into(),
            ));
        }
        if subscriber_home.is_empty() {
            return Err(RepError::ConfigError(
                "subscription home directory cannot be empty".into(),
            ));
        }
        if feeder_host.is_empty() {
            return Err(RepError::ConfigError(
                "feeder host name cannot be empty".into(),
            ));
        }
        if feeder_port == 0 {
            return Err(RepError::ConfigError(
                "feeder host port cannot be zero".into(),
            ));
        }
        if group_name.is_empty() {
            return Err(RepError::ConfigError(
                "replication group name cannot be empty".into(),
            ));
        }
        // JE: only SECONDARY or EXTERNAL are legal subscription node types.
        // Noxu has only SECONDARY of that pair.
        if !node_type.is_secondary() {
            return Err(RepError::ConfigError(format!(
                "subscription node type must be SECONDARY, found: {}",
                node_type
            )));
        }
        Ok(SubscriptionConfig {
            subscriber_name: subscriber_name.to_string(),
            group_name: group_name.to_string(),
            feeder_host: feeder_host.to_string(),
            feeder_port,
            subscriber_home: subscriber_home.to_string(),
            node_type,
            group_uuid,
            ..Default::default()
        })
    }

    /// Set the stream mode (JE `setStreamMode`).
    pub fn set_stream_mode(&mut self, mode: EntryRequestType) {
        self.stream_mode = mode;
    }

    /// Get the stream mode (JE `getStreamMode`).
    pub fn get_stream_mode(&self) -> EntryRequestType {
        self.stream_mode
    }
}

/// Callback for receiving replicated entries.
///
/// Implementations process
/// each replicated entry as it arrives, handle errors, and are notified
/// when the subscriber catches up to the master's current position.
pub trait SubscriptionCallback: Send + Sync {
    /// Called when a new replicated entry is received.
    ///
    /// # Arguments
    /// * `vlsn` - The VLSN of this entry.
    /// * `entry_type` - The log entry type identifier.
    /// * `data` - The raw entry payload.
    fn on_entry(&self, vlsn: u64, entry_type: u8, data: &[u8]);

    /// Called when an error occurs during subscription processing.
    fn on_error(&self, error: &RepError);

    /// Called when the subscription has caught up with the master.
    fn on_caught_up(&self, vlsn: u64);
}

/// The current state of a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    /// Initial state, not yet started.
    Idle,
    /// Connecting to the feeder.
    Connecting,
    /// Actively receiving entries.
    Active,
    /// Caught up with the master's current VLSN.
    CaughtUp,
    /// An error has occurred.
    Error,
    /// The subscription has been shut down.
    Shutdown,
}

/// A subscription to a replication stream.
///
/// Manages the lifecycle of subscribing to
/// a feeder's replication stream: connecting, receiving entries, tracking
/// progress, and shutting down.
pub struct Subscription {
    /// Configuration for this subscription.
    config: SubscriptionConfig,
    /// Current subscription state.
    state: Mutex<SubscriptionState>,
    /// The most recently processed VLSN.
    current_vlsn: Mutex<u64>,
    /// Total number of entries received.
    entries_received: AtomicU64,
    /// Whether shutdown has been requested.
    shutdown: AtomicBool,
    /// The live TCP connection to the feeder node.
    ///
    /// Which calls
    /// `RepUtils.openSocket(feederAddr)` to connect to the feeder. Set to
    /// `Some` after a successful `start()` call.
    connection: Mutex<Option<TcpStream>>,
}

impl Subscription {
    /// Create a new subscription with the given configuration.
    pub fn new(config: SubscriptionConfig) -> Self {
        let start_vlsn = config.start_vlsn;
        Self {
            config,
            state: Mutex::new(SubscriptionState::Idle),
            current_vlsn: Mutex::new(start_vlsn),
            entries_received: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            connection: Mutex::new(None),
        }
    }

    /// Get the current subscription state.
    pub fn get_state(&self) -> SubscriptionState {
        *self.state.lock()
    }

    /// Get the most recently processed VLSN.
    pub fn get_current_vlsn(&self) -> u64 {
        *self.current_vlsn.lock()
    }

    /// Get the total number of entries received.
    pub fn get_entries_received(&self) -> u64 {
        self.entries_received.load(Ordering::Relaxed)
    }

    /// Get the subscription configuration.
    pub fn get_config(&self) -> &SubscriptionConfig {
        &self.config
    }

    /// Start the subscription by connecting to the feeder.
    ///
    /// Which calls
    /// `SubscriptionThread.start()`, which in turn invokes
    /// `RepUtils.openSocket(feederAddr)` to establish a TCP connection to the
    /// feeder node.
    ///
    /// Transitions: `Idle` → `Connecting` → `Active` on success, or
    /// `Idle` → `Connecting` → `Error` if the connection attempt fails.
    pub fn start(&self) -> Result<()> {
        let mut state = self.state.lock();
        match *state {
            SubscriptionState::Idle => {
                *state = SubscriptionState::Connecting;

                // Resolve the feeder address and open a TCP connection.
                // equivalent: RepUtils.openSocket(InetSocketAddress(host, port))
                let addr_str = format!(
                    "{}:{}",
                    self.config.feeder_host, self.config.feeder_port
                );
                match TcpStream::connect(&addr_str) {
                    Ok(stream) => {
                        *self.connection.lock() = Some(stream);
                        *state = SubscriptionState::Active;
                        Ok(())
                    }
                    Err(e) => {
                        *state = SubscriptionState::Error;
                        Err(RepError::SubscriptionError(format!(
                            "failed to connect to feeder at {}: {}",
                            addr_str, e
                        )))
                    }
                }
            }
            SubscriptionState::Shutdown => Err(RepError::SubscriptionError(
                "cannot start a shutdown subscription".into(),
            )),
            other => Err(RepError::SubscriptionError(format!(
                "cannot start from state {:?}",
                other
            ))),
        }
    }

    /// Start the subscription streaming from an explicit VLSN.
    ///
    /// Port of JE `Subscription.start(VLSN)`: a NULL start VLSN is rejected
    /// with `IllegalArgumentException`. In Noxu the VLSN is a `u64` where `0`
    /// denotes NULL_VLSN (VLSNs are 1-based; `FIRST_VLSN == 1`), so a `0`
    /// argument is rejected with `RepError::ConfigError`. On a valid VLSN the
    /// start VLSN is recorded and the connection proceeds exactly as
    /// [`Subscription::start`].
    pub fn start_from_vlsn(&self, vlsn: u64) -> Result<()> {
        if vlsn == 0 {
            return Err(RepError::ConfigError(
                "start VLSN cannot be null".into(),
            ));
        }
        *self.current_vlsn.lock() = vlsn;
        self.start()
    }

    /// Get the live TCP connection to the feeder, if connected.
    ///
    /// Returns a cloned handle to the underlying `TcpStream`. Callers use
    /// this to send/receive replication protocol messages.
    pub fn get_connection(&self) -> Option<TcpStream> {
        self.connection.lock().as_ref().and_then(|s| s.try_clone().ok())
    }

    /// Process an incoming replicated entry.
    ///
    /// Updates the current VLSN and entry count. In the full implementation,
    /// this would also invoke the subscription callback.
    pub fn process_entry(&self, vlsn: u64, _entry_type: u8, _data: Vec<u8>) {
        if self.shutdown.load(Ordering::SeqCst) {
            return;
        }
        *self.current_vlsn.lock() = vlsn;
        self.entries_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark the subscription as caught up with the master.
    pub fn mark_caught_up(&self) {
        let mut state = self.state.lock();
        if *state == SubscriptionState::Active {
            *state = SubscriptionState::CaughtUp;
        }
    }

    /// Transition the subscription to the error state.
    pub fn mark_error(&self) {
        let mut state = self.state.lock();
        if *state != SubscriptionState::Shutdown {
            *state = SubscriptionState::Error;
        }
    }

    /// Shutdown the subscription.
    ///
    /// Closes the TCP connection to the feeder (if open) and marks the
    /// subscription as shut down.
    /// which stops the `SubscriptionThread` and closes the feeder socket.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        *self.state.lock() = SubscriptionState::Shutdown;
        // Close the TCP connection if one was established.
        if let Some(stream) = self.connection.lock().take() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }

    /// Whether shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    /// Create a config that points at a non-listening address (port 1).
    /// Use only for tests that do NOT call `start()`.
    fn test_config_no_connect() -> SubscriptionConfig {
        SubscriptionConfig {
            subscriber_name: "sub1".into(),
            group_name: "group1".into(),
            feeder_host: "127.0.0.1".into(),
            feeder_port: 1, // nothing listening here
            start_vlsn: 0,
            ..Default::default()
        }
    }

    /// Bind a listener on an ephemeral port and return a config + the listener.
    /// Tests that call `start()` must use this so the TCP connect succeeds.
    fn test_config_with_listener() -> (SubscriptionConfig, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let config = SubscriptionConfig {
            subscriber_name: "sub1".into(),
            group_name: "group1".into(),
            feeder_host: "127.0.0.1".into(),
            feeder_port: port,
            start_vlsn: 0,
            ..Default::default()
        };
        (config, listener)
    }

    #[test]
    fn test_initial_state() {
        let sub = Subscription::new(test_config_no_connect());
        assert_eq!(sub.get_state(), SubscriptionState::Idle);
        assert_eq!(sub.get_current_vlsn(), 0);
        assert_eq!(sub.get_entries_received(), 0);
        assert!(!sub.is_shutdown());
    }

    #[test]
    fn test_start() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);
        sub.start().unwrap();
        assert_eq!(sub.get_state(), SubscriptionState::Active);
        // A connection must have been established.
        assert!(sub.get_connection().is_some());
    }

    #[test]
    fn test_start_fails_when_no_listener() {
        // Port 1 is not listening — start() must transition to Error and
        // return Err.
        let sub = Subscription::new(test_config_no_connect());
        let result = sub.start();
        assert!(result.is_err());
        assert_eq!(sub.get_state(), SubscriptionState::Error);
    }

    #[test]
    fn test_start_from_active_fails() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);
        sub.start().unwrap();
        let result = sub.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_start_after_shutdown_fails() {
        let sub = Subscription::new(test_config_no_connect());
        sub.shutdown();
        let result = sub.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_process_entries() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);
        sub.start().unwrap();

        sub.process_entry(1, 1, vec![0x01]);
        sub.process_entry(2, 1, vec![0x02]);
        sub.process_entry(3, 2, vec![0x03]);

        assert_eq!(sub.get_current_vlsn(), 3);
        assert_eq!(sub.get_entries_received(), 3);
    }

    #[test]
    fn test_process_entry_after_shutdown_ignored() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);
        sub.start().unwrap();
        sub.process_entry(1, 1, vec![0x01]);

        sub.shutdown();
        sub.process_entry(2, 1, vec![0x02]);

        // VLSN should not advance after shutdown.
        assert_eq!(sub.get_current_vlsn(), 1);
        // But the atomic counter was already incremented for entry 1.
        assert_eq!(sub.get_entries_received(), 1);
    }

    #[test]
    fn test_mark_caught_up() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);
        sub.start().unwrap();
        assert_eq!(sub.get_state(), SubscriptionState::Active);

        sub.mark_caught_up();
        assert_eq!(sub.get_state(), SubscriptionState::CaughtUp);
    }

    #[test]
    fn test_mark_caught_up_from_idle_no_change() {
        let sub = Subscription::new(test_config_no_connect());
        sub.mark_caught_up();
        // Should still be Idle since mark_caught_up only works from Active.
        assert_eq!(sub.get_state(), SubscriptionState::Idle);
    }

    #[test]
    fn test_mark_error() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);
        sub.start().unwrap();
        sub.mark_error();
        assert_eq!(sub.get_state(), SubscriptionState::Error);
    }

    #[test]
    fn test_mark_error_after_shutdown_no_change() {
        let sub = Subscription::new(test_config_no_connect());
        sub.shutdown();
        sub.mark_error();
        // Shutdown is terminal, should not change to Error.
        assert_eq!(sub.get_state(), SubscriptionState::Shutdown);
    }

    #[test]
    fn test_shutdown() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);
        sub.start().unwrap();
        assert!(!sub.is_shutdown());

        sub.shutdown();
        assert!(sub.is_shutdown());
        assert_eq!(sub.get_state(), SubscriptionState::Shutdown);
        // Connection must have been closed.
        assert!(sub.get_connection().is_none());
    }

    #[test]
    fn test_config_accessor() {
        let config = test_config_no_connect();
        let sub = Subscription::new(config);
        assert_eq!(sub.get_config().subscriber_name, "sub1");
        assert_eq!(sub.get_config().group_name, "group1");
        assert_eq!(sub.get_config().feeder_host, "127.0.0.1");
        assert_eq!(sub.get_config().feeder_port, 1);
    }

    #[test]
    fn test_start_vlsn_nonzero() {
        let mut config = test_config_no_connect();
        config.start_vlsn = 42;
        let sub = Subscription::new(config);
        assert_eq!(sub.get_current_vlsn(), 42);
    }

    #[test]
    fn test_full_lifecycle() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);

        // Idle -> Active (via real TCP connect)
        assert_eq!(sub.get_state(), SubscriptionState::Idle);
        sub.start().unwrap();
        assert_eq!(sub.get_state(), SubscriptionState::Active);
        assert!(sub.get_connection().is_some());

        // Process entries
        for i in 1..=10 {
            sub.process_entry(i, 1, vec![i as u8]);
        }
        assert_eq!(sub.get_current_vlsn(), 10);
        assert_eq!(sub.get_entries_received(), 10);

        // Caught up
        sub.mark_caught_up();
        assert_eq!(sub.get_state(), SubscriptionState::CaughtUp);

        // Shutdown — also closes the TCP connection
        sub.shutdown();
        assert_eq!(sub.get_state(), SubscriptionState::Shutdown);
        assert!(sub.is_shutdown());
        assert!(sub.get_connection().is_none());
    }

    // ------------------------------------------------------------------
    // Ported from je.rep.subscription.EntryRequestTypeTest
    //
    // JE's EntryRequestTypeTest spins up a live 1-node replication group,
    // populates data, optionally forces log cleaning to advance the VLSN
    // range past 1, then starts a real subscription in each stream mode and
    // asserts the FIRST VLSN the callback receives equals the mode's expected
    // resolution of the requested start VLSN against the feeder's VLSN range.
    // The live multi-thread feeder stream + cleaner are N/A here (Noxu has no
    // in-process feeder delivery); the PORTABLE, correctness-critical core is
    // the resolution table itself
    // (BaseProtocol.EntryRequestType x FeederReplicaSyncup.makeResponseTo
    // EntryRequest), ported as EntryRequestType::resolve and exercised here
    // exactly as the JE test's testLow/testInRange/testHigh expectations.
    // ------------------------------------------------------------------

    use crate::vlsn::VlsnRange;

    /// Build a range with an explicit lastSync (the DEFAULT-above-range
    /// alternate matchpoint). commit/first/last per JE VLSNRange.
    fn range(first: u64, last: u64, last_sync: u64) -> VlsnRange {
        let mut r = VlsnRange::with_range(first, last);
        // VlsnRange::extend advances last & sync; set sync directly via the
        // sync setter if present, else rebuild through record_sync.
        r.update_sync(last_sync);
        r
    }

    /// JE: EntryRequestTypeTest.testNoCleaning
    ///
    /// Without any cleaning the range starts at FIRST_VLSN (1). For every
    /// mode, a low (=1), in-range (=mid) and high (=MAX) request must resolve
    /// per the DEFAULT/AVAILABLE/NOW table, and none of them fails.
    #[test]
    fn test_entry_request_no_cleaning() {
        // Range [1, 100], lastSync = 90 (a sync point below the tail).
        let r = range(1, 100, 90);
        let low = 1u64; // FIRST_VLSN
        let mid = 50u64; // (1 + 100) / 2, in-range
        let high = u64::MAX; // above range

        for mode in EntryRequestType::values() {
            // testLow
            let exp_low = match mode {
                EntryRequestType::Default => StartResolution::Start(low),
                EntryRequestType::Available => {
                    StartResolution::Start(r.get_first())
                }
                EntryRequestType::Now => StartResolution::Start(r.get_last()),
            };
            assert_eq!(mode.resolve(low, &r), exp_low, "low, mode {:?}", mode);

            // testInRange
            let exp_mid = match mode {
                EntryRequestType::Default => StartResolution::Start(mid),
                EntryRequestType::Available => StartResolution::Start(mid),
                EntryRequestType::Now => StartResolution::Start(r.get_last()),
            };
            assert_eq!(mode.resolve(mid, &r), exp_mid, "mid, mode {:?}", mode);

            // testHigh
            let exp_high = match mode {
                // DEFAULT above-range counters with lastSync (alt matchpoint).
                EntryRequestType::Default => {
                    StartResolution::AlternateMatchpoint(r.get_last_sync())
                }
                EntryRequestType::Available => {
                    StartResolution::Start(r.get_last())
                }
                EntryRequestType::Now => StartResolution::Start(r.get_last()),
            };
            assert_eq!(
                mode.resolve(high, &r),
                exp_high,
                "high, mode {:?}",
                mode
            );
        }
    }

    /// JE: EntryRequestTypeTest.testCleaning
    ///
    /// After cleaning, the range's first VLSN has advanced past 1 (here the
    /// range is [10, 100]). NOW and AVAILABLE succeed for low/in/high. DEFAULT
    /// succeeds in-range and high (via alt matchpoint) but a below-range (low)
    /// request FAILS — JE raises InsufficientLogException; Noxu's resolve
    /// returns NotFound (the EntryNotFound the feeder would send, which the
    /// subscriber surfaces as insufficient-log).
    #[test]
    fn test_entry_request_cleaning() {
        // Cleaned range: first advanced to 10.
        let r = range(10, 100, 90);
        assert!(r.get_first() > 1, "expect log cleaned (first > FIRST_VLSN)");

        let low = 1u64; // below the cleaned range
        let mid = 55u64; // (10 + 100) / 2
        let high = u64::MAX;

        // NOW and AVAILABLE always succeed.
        for mode in [EntryRequestType::Now, EntryRequestType::Available] {
            let exp_low = match mode {
                EntryRequestType::Available => {
                    StartResolution::Start(r.get_first())
                }
                _ => StartResolution::Start(r.get_last()), // NOW
            };
            assert_eq!(mode.resolve(low, &r), exp_low, "clean low {:?}", mode);

            let exp_mid = match mode {
                EntryRequestType::Available => StartResolution::Start(mid),
                _ => StartResolution::Start(r.get_last()),
            };
            assert_eq!(mode.resolve(mid, &r), exp_mid, "clean mid {:?}", mode);

            // both clamp above-range to last
            assert_eq!(
                mode.resolve(high, &r),
                StartResolution::Start(r.get_last()),
                "clean high {:?}",
                mode
            );
        }

        // DEFAULT: in-range and high are good.
        assert_eq!(
            EntryRequestType::Default.resolve(mid, &r),
            StartResolution::Start(mid)
        );
        assert_eq!(
            EntryRequestType::Default.resolve(high, &r),
            StartResolution::AlternateMatchpoint(r.get_last_sync())
        );
        // DEFAULT low: must fail (JE InsufficientLogException == NotFound).
        assert_eq!(
            EntryRequestType::Default.resolve(low, &r),
            StartResolution::NotFound,
            "DEFAULT below-range must be NotFound (ILE)"
        );
    }

    // ------------------------------------------------------------------
    // Ported from je.rep.subscription.SubscriptionConfigTest
    // ------------------------------------------------------------------

    /// JE: SubscriptionConfigTest.testInitialziedParameters
    ///
    /// A freshly-built config exposes the subscriber name/home/host/port,
    /// feeder host/port, group name/UUID, a SECONDARY node type, and a
    /// DEFAULT stream mode.
    #[test]
    fn test_config_initialized_parameters() {
        let uuid = "cb675927-433a-4ed6-8382-0403e9861619".to_string();
        let config = SubscriptionConfig::new(
            "test-subscriber",
            "./test/subscription/",
            "localhost",
            6000,
            "rg1",
            Some(uuid.clone()),
            NodeType::Secondary,
        )
        .unwrap();

        assert_eq!(config.subscriber_home, "./test/subscription/");
        assert_eq!(config.subscriber_name, "test-subscriber");
        assert_eq!(config.feeder_host, "localhost");
        assert_eq!(config.feeder_port, 6000);
        assert_eq!(config.group_name, "rg1");
        assert_eq!(config.group_uuid.as_deref(), Some(uuid.as_str()));
        assert_eq!(config.node_type, NodeType::Secondary);
        assert_eq!(config.get_stream_mode(), EntryRequestType::Default);
    }

    /// JE: SubscriptionConfigTest.testNodeType
    ///
    /// JE allows SECONDARY and EXTERNAL and rejects ARBITER/ELECTABLE/MONITOR
    /// with IllegalArgumentException. Noxu has no EXTERNAL variant, so only
    /// SECONDARY is accepted; ELECTABLE/MONITOR/ARBITER are rejected with
    /// RepError::ConfigError.
    #[test]
    fn test_config_node_type() {
        // SECONDARY is accepted.
        let ok = SubscriptionConfig::new(
            "s",
            "./",
            "localhost",
            6000,
            "rg1",
            None,
            NodeType::Secondary,
        );
        assert!(ok.is_ok());

        // The non-supported node types are rejected.
        for bad in [NodeType::Arbiter, NodeType::Electable, NodeType::Monitor] {
            let r = SubscriptionConfig::new(
                "s",
                "./",
                "localhost",
                6000,
                "rg1",
                None,
                bad,
            );
            assert!(r.is_err(), "node type {:?} must be rejected", bad);
        }
    }

    /// JE: SubscriptionConfigTest.testSetParameters
    ///
    /// The timeout/interval/queue-size/buffer-size knobs round-trip through
    /// their setters/getters. Noxu stores them as plain config fields.
    #[test]
    fn test_config_set_parameters() {
        let mut config = SubscriptionConfig::new(
            "s",
            "./",
            "localhost",
            6000,
            "rg1",
            None,
            NodeType::Secondary,
        )
        .unwrap();

        let timeout = 10_000u64;
        config.channel_timeout_ms = timeout;
        assert_eq!(config.channel_timeout_ms, timeout);
        config.pre_heartbeat_timeout_ms = 2 * timeout;
        assert_eq!(config.pre_heartbeat_timeout_ms, 2 * timeout);
        config.stream_open_timeout_ms = 3 * timeout;
        assert_eq!(config.stream_open_timeout_ms, 3 * timeout);

        let interval = 2000u32;
        config.heartbeat_interval_ms = interval;
        assert_eq!(config.heartbeat_interval_ms, interval);

        let sz = 10_240u32;
        config.input_message_queue_size = sz;
        assert_eq!(config.input_message_queue_size, sz);
        config.output_message_queue_size = 2 * sz;
        assert_eq!(config.output_message_queue_size, 2 * sz);
        config.receive_buffer_size = 3 * sz;
        assert_eq!(config.receive_buffer_size, 3 * sz);
    }

    /// JE: SubscriptionConfigTest.testMissingParameters
    ///
    /// A config with any missing mandatory parameter (subscriber name, home,
    /// subscriber host/port, feeder host/port, or group name) is rejected. JE
    /// throws IllegalArgumentException; Noxu returns RepError::ConfigError.
    /// (JE's subscriber-host-port and feeder-host-port are one string in Noxu
    /// modeled as host + numeric port; a missing host or a zero port stand in
    /// for the null host/port pair.)
    #[test]
    fn test_config_missing_parameters() {
        // Missing subscriber node name.
        assert!(
            SubscriptionConfig::new(
                "",
                "./",
                "localhost",
                6000,
                "rg1",
                None,
                NodeType::Secondary
            )
            .is_err()
        );
        // Missing home.
        assert!(
            SubscriptionConfig::new(
                "s",
                "",
                "localhost",
                6000,
                "rg1",
                None,
                NodeType::Secondary
            )
            .is_err()
        );
        // Missing feeder host.
        assert!(
            SubscriptionConfig::new(
                "s",
                "./",
                "",
                6000,
                "rg1",
                None,
                NodeType::Secondary
            )
            .is_err()
        );
        // Missing feeder port (0 == null port pair).
        assert!(
            SubscriptionConfig::new(
                "s",
                "./",
                "localhost",
                0,
                "rg1",
                None,
                NodeType::Secondary
            )
            .is_err()
        );
        // Missing group name.
        assert!(
            SubscriptionConfig::new(
                "s",
                "./",
                "localhost",
                6000,
                "",
                None,
                NodeType::Secondary
            )
            .is_err()
        );
        // The all-present control must succeed (guards vacuity).
        assert!(
            SubscriptionConfig::new(
                "s",
                "./",
                "localhost",
                6000,
                "rg1",
                None,
                NodeType::Secondary
            )
            .is_ok()
        );
    }

    /// JE: SubscriptionConfigTest.testStreamMode
    ///
    /// Every EntryRequestType round-trips through set/get on the config.
    #[test]
    fn test_config_stream_mode() {
        let mut config = SubscriptionConfig::new(
            "s",
            "./",
            "localhost",
            6000,
            "rg1",
            None,
            NodeType::Secondary,
        )
        .unwrap();
        for mode in EntryRequestType::values() {
            config.set_stream_mode(mode);
            assert_eq!(config.get_stream_mode(), mode);
        }
    }

    // ------------------------------------------------------------------
    // Ported from je.rep.subscription.SubscriptionTest
    // ------------------------------------------------------------------

    /// JE: SubscriptionTest.testInvalidStartVLSN
    ///
    /// Starting a subscription from a NULL start VLSN is rejected. JE throws
    /// IllegalArgumentException from Subscription.start(VLSN); Noxu's
    /// start_from_vlsn(0) (0 == NULL_VLSN in the u64 encoding) returns
    /// RepError::ConfigError and does NOT connect.
    #[test]
    fn test_subscription_invalid_start_vlsn() {
        let sub = Subscription::new(test_config_no_connect());
        let r = sub.start_from_vlsn(0);
        assert!(r.is_err(), "null start VLSN must be rejected");
        // Must not have transitioned out of Idle (no connect attempted).
        assert_eq!(sub.get_state(), SubscriptionState::Idle);
    }

    /// JE: SubscriptionTest.testSubscriptionFromVLSN (VLSN-positioning half)
    ///
    /// JE subscribes from an explicit start VLSN and asserts the first VLSN
    /// the callback receives, and the subscription statistics' startVLSN, both
    /// equal the requested VLSN. Noxu has no live feeder delivery, so the
    /// portable half is that start_from_vlsn(v) records v as the current
    /// (start) VLSN before/through connection. The live-stream first-VLSN
    /// assertion is N/A (multi-JVM feeder).
    #[test]
    fn test_subscription_from_vlsn_positioning() {
        let (config, _listener) = test_config_with_listener();
        let sub = Subscription::new(config);
        let start = 100u64;
        sub.start_from_vlsn(start).unwrap();
        // The recorded start position is the requested VLSN.
        assert_eq!(sub.get_current_vlsn(), start);
        assert_eq!(sub.get_state(), SubscriptionState::Active);
    }

    /// JE: SubscriptionTest.testSubscriptionUnavailableVLSN
    ///
    /// If the requested start VLSN has been cleaned and is below the feeder's
    /// range, subscription fails (JE InsufficientLogException in sync-up).
    /// Noxu models the feeder-side decision as EntryRequestType::Default
    /// resolving a below-range VLSN to NotFound (the EntryNotFound the feeder
    /// returns, surfaced as insufficient-log). The live sync-up is N/A; the
    /// resolution that drives the failure is the portable core.
    #[test]
    fn test_subscription_unavailable_vlsn() {
        // Range advanced past zero (cleaned): [50, 200].
        let r = range(50, 200, 180);
        // A start at VLSN 1 (below range) in DEFAULT mode is not serviceable.
        assert_eq!(
            EntryRequestType::Default.resolve(1, &r),
            StartResolution::NotFound
        );
    }
}
