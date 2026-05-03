//! Acknowledgment tracking for replication commits.
//!
//! Port of commit ack tracking from `com.sleepycat.je.rep.impl.node.RepNode`.
//! Tracks transaction commit acknowledgments from replicas to determine when
//! a transaction's durability requirements have been satisfied.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Tracks transaction commit acknowledgments from replicas.
///
/// When the master commits a transaction, it may need to wait for one or more
/// replicas to acknowledge receipt before considering the transaction durable.
/// The `AckTracker` manages pending acknowledgments, recording which replicas
/// have acked which VLSNs, and detecting when sufficient acks have been
/// received or when ack timeouts have occurred.
pub struct AckTracker {
    /// Maps VLSN to pending ack info.
    pending_acks: Mutex<HashMap<u64, PendingAck>>,
    /// Total acks received across all VLSNs.
    total_acks: Mutex<u64>,
    /// Total ack timeouts.
    total_timeouts: Mutex<u64>,
}

/// Internal state for a VLSN awaiting acknowledgments.
#[derive(Debug)]
struct PendingAck {
    /// The VLSN being tracked.
    vlsn: u64,
    /// Number of acks needed to satisfy durability.
    needed: u32,
    /// Map of replica name to the time the ack was received.
    received: HashMap<String, Instant>,
    /// When this pending ack was created.
    created: Instant,
}

impl PendingAck {
    fn new(vlsn: u64, needed: u32) -> Self {
        Self { vlsn, needed, received: HashMap::new(), created: Instant::now() }
    }

    fn is_satisfied(&self) -> bool {
        self.received.len() as u32 >= self.needed
    }
}

/// Result of recording an acknowledgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckResult {
    /// Ack received, but not yet sufficient to satisfy durability.
    Pending,
    /// This ack satisfied the durability requirement.
    Satisfied,
    /// VLSN not being tracked (already cleaned up or never registered).
    Unknown,
    /// Duplicate ack from this replica for this VLSN.
    Duplicate,
}

impl AckTracker {
    /// Create a new ack tracker.
    pub fn new() -> Self {
        Self {
            pending_acks: Mutex::new(HashMap::new()),
            total_acks: Mutex::new(0),
            total_timeouts: Mutex::new(0),
        }
    }

    /// Register a new VLSN that needs acknowledgments.
    ///
    /// If the VLSN is already registered, this is a no-op (the existing
    /// registration is preserved).
    pub fn register(&self, vlsn: u64, needed_acks: u32) {
        let mut pending = self.pending_acks.lock();
        pending
            .entry(vlsn)
            .or_insert_with(|| PendingAck::new(vlsn, needed_acks));
    }

    /// Record an acknowledgment from a replica for a VLSN.
    ///
    /// Returns the result indicating whether the ack was accepted and whether
    /// it satisfied the durability requirement.
    pub fn record_ack(&self, vlsn: u64, replica_name: &str) -> AckResult {
        let mut pending = self.pending_acks.lock();
        let ack = match pending.get_mut(&vlsn) {
            Some(a) => a,
            None => return AckResult::Unknown,
        };

        // Check for duplicate
        if ack.received.contains_key(replica_name) {
            return AckResult::Duplicate;
        }

        ack.received.insert(replica_name.to_string(), Instant::now());
        *self.total_acks.lock() += 1;

        if ack.is_satisfied() {
            AckResult::Satisfied
        } else {
            AckResult::Pending
        }
    }

    /// Check if a VLSN has sufficient acks.
    pub fn is_satisfied(&self, vlsn: u64) -> bool {
        let pending = self.pending_acks.lock();
        match pending.get(&vlsn) {
            Some(ack) => ack.is_satisfied(),
            None => false,
        }
    }

    /// Remove all pending acks for VLSNs <= the given value.
    ///
    /// This is used to clean up acks for transactions that have been
    /// durably committed and no longer need tracking.
    pub fn cleanup_through(&self, vlsn: u64) {
        let mut pending = self.pending_acks.lock();
        pending.retain(|&v, _| v > vlsn);
    }

    /// Get the number of pending (unsatisfied) acks.
    pub fn pending_count(&self) -> usize {
        self.pending_acks.lock().len()
    }

    /// Check for timed-out acks and return their VLSNs.
    ///
    /// An ack is considered timed out if it was registered more than
    /// `timeout` ago and has not yet been satisfied.
    pub fn check_timeouts(&self, timeout: Duration) -> Vec<u64> {
        let pending = self.pending_acks.lock();
        let now = Instant::now();
        let mut timed_out = Vec::new();
        for ack in pending.values() {
            if !ack.is_satisfied()
                && let Some(elapsed) = now.checked_duration_since(ack.created)
                && elapsed > timeout
            {
                timed_out.push(ack.vlsn);
                *self.total_timeouts.lock() += 1;
            }
        }
        timed_out
    }

    /// Get total number of acks received across all VLSNs.
    pub fn get_total_acks(&self) -> u64 {
        *self.total_acks.lock()
    }

    /// Get total number of ack timeouts.
    pub fn get_total_timeouts(&self) -> u64 {
        *self.total_timeouts.lock()
    }
}

impl Default for AckTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Basic register/ack flow ---

    #[test]
    fn test_new_tracker() {
        let tracker = AckTracker::new();
        assert_eq!(tracker.pending_count(), 0);
        assert_eq!(tracker.get_total_acks(), 0);
        assert_eq!(tracker.get_total_timeouts(), 0);
    }

    #[test]
    fn test_default_impl() {
        let tracker = AckTracker::default();
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn test_register() {
        let tracker = AckTracker::new();
        tracker.register(100, 2);
        assert_eq!(tracker.pending_count(), 1);
        assert!(!tracker.is_satisfied(100));
    }

    #[test]
    fn test_register_idempotent() {
        let tracker = AckTracker::new();
        tracker.register(100, 2);
        tracker.register(100, 5); // Should not overwrite
        assert_eq!(tracker.pending_count(), 1);
        // Record one ack  -  if needed was overwritten to 5 this wouldn't satisfy with 2
        tracker.record_ack(100, "replica1");
        tracker.record_ack(100, "replica2");
        assert!(tracker.is_satisfied(100));
    }

    #[test]
    fn test_record_ack_pending() {
        let tracker = AckTracker::new();
        tracker.register(100, 2);
        let result = tracker.record_ack(100, "replica1");
        assert_eq!(result, AckResult::Pending);
        assert!(!tracker.is_satisfied(100));
        assert_eq!(tracker.get_total_acks(), 1);
    }

    #[test]
    fn test_record_ack_satisfied() {
        let tracker = AckTracker::new();
        tracker.register(100, 2);
        tracker.record_ack(100, "replica1");
        let result = tracker.record_ack(100, "replica2");
        assert_eq!(result, AckResult::Satisfied);
        assert!(tracker.is_satisfied(100));
        assert_eq!(tracker.get_total_acks(), 2);
    }

    #[test]
    fn test_single_ack_needed() {
        let tracker = AckTracker::new();
        tracker.register(100, 1);
        let result = tracker.record_ack(100, "replica1");
        assert_eq!(result, AckResult::Satisfied);
        assert!(tracker.is_satisfied(100));
    }

    #[test]
    fn test_record_ack_unknown_vlsn() {
        let tracker = AckTracker::new();
        let result = tracker.record_ack(999, "replica1");
        assert_eq!(result, AckResult::Unknown);
        assert_eq!(tracker.get_total_acks(), 0);
    }

    #[test]
    fn test_record_ack_duplicate() {
        let tracker = AckTracker::new();
        tracker.register(100, 2);
        tracker.record_ack(100, "replica1");
        let result = tracker.record_ack(100, "replica1");
        assert_eq!(result, AckResult::Duplicate);
        assert!(!tracker.is_satisfied(100));
        // Duplicate should not increment total
        assert_eq!(tracker.get_total_acks(), 1);
    }

    #[test]
    fn test_is_satisfied_unknown_vlsn() {
        let tracker = AckTracker::new();
        assert!(!tracker.is_satisfied(999));
    }

    // --- Multiple VLSNs ---

    #[test]
    fn test_multiple_vlsns() {
        let tracker = AckTracker::new();
        tracker.register(100, 1);
        tracker.register(101, 2);
        tracker.register(102, 1);
        assert_eq!(tracker.pending_count(), 3);

        tracker.record_ack(100, "r1");
        assert!(tracker.is_satisfied(100));
        assert!(!tracker.is_satisfied(101));

        tracker.record_ack(101, "r1");
        assert!(!tracker.is_satisfied(101));
        tracker.record_ack(101, "r2");
        assert!(tracker.is_satisfied(101));
    }

    // --- Cleanup ---

    #[test]
    fn test_cleanup_through() {
        let tracker = AckTracker::new();
        tracker.register(100, 1);
        tracker.register(101, 1);
        tracker.register(102, 1);
        tracker.register(200, 1);
        assert_eq!(tracker.pending_count(), 4);

        tracker.cleanup_through(102);
        assert_eq!(tracker.pending_count(), 1);
        // Only VLSN 200 should remain
        assert_eq!(tracker.record_ack(100, "r1"), AckResult::Unknown);
        assert_eq!(tracker.record_ack(200, "r1"), AckResult::Satisfied);
    }

    #[test]
    fn test_cleanup_through_zero() {
        let tracker = AckTracker::new();
        tracker.register(100, 1);
        tracker.cleanup_through(0);
        assert_eq!(tracker.pending_count(), 1);
    }

    #[test]
    fn test_cleanup_through_all() {
        let tracker = AckTracker::new();
        tracker.register(1, 1);
        tracker.register(2, 1);
        tracker.cleanup_through(100);
        assert_eq!(tracker.pending_count(), 0);
    }

    // --- Timeout detection ---

    #[test]
    fn test_check_timeouts_none() {
        let tracker = AckTracker::new();
        tracker.register(100, 1);
        // Just registered, shouldn't be timed out with generous timeout
        let timed_out = tracker.check_timeouts(Duration::from_secs(60));
        assert!(timed_out.is_empty());
        assert_eq!(tracker.get_total_timeouts(), 0);
    }

    #[test]
    fn test_check_timeouts_with_expired() {
        let tracker = AckTracker::new();

        // Manually insert an old pending ack
        {
            let mut pending = tracker.pending_acks.lock();
            let mut ack = PendingAck::new(50, 1);
            ack.created = Instant::now() - Duration::from_secs(120);
            pending.insert(50, ack);
        }

        let timed_out = tracker.check_timeouts(Duration::from_secs(60));
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], 50);
        assert_eq!(tracker.get_total_timeouts(), 1);
    }

    #[test]
    fn test_check_timeouts_skips_satisfied() {
        let tracker = AckTracker::new();

        // Insert an old but satisfied pending ack
        {
            let mut pending = tracker.pending_acks.lock();
            let mut ack = PendingAck::new(50, 1);
            ack.created = Instant::now() - Duration::from_secs(120);
            ack.received.insert("r1".to_string(), Instant::now());
            pending.insert(50, ack);
        }

        let timed_out = tracker.check_timeouts(Duration::from_secs(60));
        assert!(timed_out.is_empty());
    }

    // --- Extra acks beyond needed ---

    #[test]
    fn test_extra_acks_beyond_needed() {
        let tracker = AckTracker::new();
        tracker.register(100, 1);
        assert_eq!(tracker.record_ack(100, "r1"), AckResult::Satisfied);
        // Additional ack from different replica
        assert_eq!(tracker.record_ack(100, "r2"), AckResult::Satisfied);
        assert_eq!(tracker.get_total_acks(), 2);
    }

    // --- Zero acks needed ---

    #[test]
    fn test_zero_acks_needed() {
        let tracker = AckTracker::new();
        tracker.register(100, 0);
        // Should be immediately satisfied (0 needed, 0 received)
        assert!(tracker.is_satisfied(100));
    }

    // --- Send + Sync ---

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AckTracker>();
    }
}
