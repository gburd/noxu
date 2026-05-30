//! Replication subscription for receiving replicated entries from a feeder.
//!
//! The
//! Subscription connects to a feeder node and receives a stream of
//! replicated log entries starting from a given VLSN. This is used by
//! subscribers that want to consume the replication stream without being
//! full replica members of the group.

use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::sync::Mutex;

use crate::rep::error::{RepError, Result};

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
}
