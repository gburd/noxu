//! Replication subscription for receiving replicated entries from a feeder.
//!
//! Port of `com.sleepycat.je.rep.subscription.Subscription`. JE's
//! Subscription connects to a feeder node and receives a stream of
//! replicated log entries starting from a given VLSN. This is used by
//! subscribers that want to consume the replication stream without being
//! full replica members of the group.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::error::{RepError, Result};

/// Configuration for a replication subscription.
///
/// Corresponds to JE's `SubscriptionConfig`. Specifies the subscriber
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
    /// Port of the feeder to connect to.
    pub feeder_port: u16,
    /// VLSN to start streaming from.
    pub start_vlsn: u64,
}

/// Callback for receiving replicated entries.
///
/// Corresponds to JE's `SubscriptionCallback`. Implementations process
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
/// Port of JE's `Subscription`. Manages the lifecycle of subscribing to
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

    /// Start the subscription.
    ///
    /// Transitions from `Idle` to `Connecting`, then to `Active`.
    /// In the full implementation, this would establish a connection
    /// to the feeder and begin receiving entries. Currently simulates
    /// the state transition.
    pub fn start(&self) -> Result<()> {
        let mut state = self.state.lock();
        match *state {
            SubscriptionState::Idle => {
                *state = SubscriptionState::Connecting;
                // In the full implementation, we would establish the
                // connection here. For now, transition directly to Active.
                *state = SubscriptionState::Active;
                Ok(())
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
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        *self.state.lock() = SubscriptionState::Shutdown;
    }

    /// Whether shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SubscriptionConfig {
        SubscriptionConfig {
            subscriber_name: "sub1".into(),
            group_name: "group1".into(),
            feeder_host: "localhost".into(),
            feeder_port: 5001,
            start_vlsn: 0,
        }
    }

    #[test]
    fn test_initial_state() {
        let sub = Subscription::new(test_config());
        assert_eq!(sub.get_state(), SubscriptionState::Idle);
        assert_eq!(sub.get_current_vlsn(), 0);
        assert_eq!(sub.get_entries_received(), 0);
        assert!(!sub.is_shutdown());
    }

    #[test]
    fn test_start() {
        let sub = Subscription::new(test_config());
        sub.start().unwrap();
        assert_eq!(sub.get_state(), SubscriptionState::Active);
    }

    #[test]
    fn test_start_from_active_fails() {
        let sub = Subscription::new(test_config());
        sub.start().unwrap();
        let result = sub.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_start_after_shutdown_fails() {
        let sub = Subscription::new(test_config());
        sub.shutdown();
        let result = sub.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_process_entries() {
        let sub = Subscription::new(test_config());
        sub.start().unwrap();

        sub.process_entry(1, 1, vec![0x01]);
        sub.process_entry(2, 1, vec![0x02]);
        sub.process_entry(3, 2, vec![0x03]);

        assert_eq!(sub.get_current_vlsn(), 3);
        assert_eq!(sub.get_entries_received(), 3);
    }

    #[test]
    fn test_process_entry_after_shutdown_ignored() {
        let sub = Subscription::new(test_config());
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
        let sub = Subscription::new(test_config());
        sub.start().unwrap();
        assert_eq!(sub.get_state(), SubscriptionState::Active);

        sub.mark_caught_up();
        assert_eq!(sub.get_state(), SubscriptionState::CaughtUp);
    }

    #[test]
    fn test_mark_caught_up_from_idle_no_change() {
        let sub = Subscription::new(test_config());
        sub.mark_caught_up();
        // Should still be Idle since mark_caught_up only works from Active.
        assert_eq!(sub.get_state(), SubscriptionState::Idle);
    }

    #[test]
    fn test_mark_error() {
        let sub = Subscription::new(test_config());
        sub.start().unwrap();
        sub.mark_error();
        assert_eq!(sub.get_state(), SubscriptionState::Error);
    }

    #[test]
    fn test_mark_error_after_shutdown_no_change() {
        let sub = Subscription::new(test_config());
        sub.shutdown();
        sub.mark_error();
        // Shutdown is terminal, should not change to Error.
        assert_eq!(sub.get_state(), SubscriptionState::Shutdown);
    }

    #[test]
    fn test_shutdown() {
        let sub = Subscription::new(test_config());
        sub.start().unwrap();
        assert!(!sub.is_shutdown());

        sub.shutdown();
        assert!(sub.is_shutdown());
        assert_eq!(sub.get_state(), SubscriptionState::Shutdown);
    }

    #[test]
    fn test_config_accessor() {
        let config = test_config();
        let sub = Subscription::new(config.clone());
        assert_eq!(sub.get_config().subscriber_name, "sub1");
        assert_eq!(sub.get_config().group_name, "group1");
        assert_eq!(sub.get_config().feeder_host, "localhost");
        assert_eq!(sub.get_config().feeder_port, 5001);
    }

    #[test]
    fn test_start_vlsn_nonzero() {
        let mut config = test_config();
        config.start_vlsn = 42;
        let sub = Subscription::new(config);
        assert_eq!(sub.get_current_vlsn(), 42);
    }

    #[test]
    fn test_full_lifecycle() {
        let sub = Subscription::new(test_config());

        // Idle -> Active
        assert_eq!(sub.get_state(), SubscriptionState::Idle);
        sub.start().unwrap();
        assert_eq!(sub.get_state(), SubscriptionState::Active);

        // Process entries
        for i in 1..=10 {
            sub.process_entry(i, 1, vec![i as u8]);
        }
        assert_eq!(sub.get_current_vlsn(), 10);
        assert_eq!(sub.get_entries_received(), 10);

        // Caught up
        sub.mark_caught_up();
        assert_eq!(sub.get_state(), SubscriptionState::CaughtUp);

        // Shutdown
        sub.shutdown();
        assert_eq!(sub.get_state(), SubscriptionState::Shutdown);
        assert!(sub.is_shutdown());
    }
}
