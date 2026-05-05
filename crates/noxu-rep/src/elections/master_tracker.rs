//! Master tracking.
//!
//! Port of `com.sleepycat.je.rep.elections.MasterTracker`  -  maintains
//! knowledge of the current master and its liveness based on heartbeats.
//!
//! The tracker is consulted by replicas to determine:
//! - Who the current master is.
//! - Whether the master is still alive (heartbeat within timeout).
//! - Whether a new election result (with a higher term) should supersede the
//!   current master.

use std::time::{Duration, Instant};

use noxu_sync::RwLock;

/// Tracks the current known master of the replication group.
///
/// All methods are safe to call concurrently from multiple threads. Reads use
/// a shared lock; writes use an exclusive lock.
pub struct MasterTracker {
    /// Name of the current master, if known.
    current_master: RwLock<Option<String>>,
    /// Term in which the current master was elected.
    master_term: RwLock<u64>,
    /// Time of the last heartbeat from the master.
    last_heartbeat: RwLock<Option<Instant>>,
    /// Maximum time between heartbeats before the master is considered dead.
    heartbeat_timeout: Duration,
}

impl MasterTracker {
    /// Create a new tracker with the given heartbeat timeout.
    ///
    /// The tracker starts with no known master.
    pub fn new(heartbeat_timeout: Duration) -> Self {
        Self {
            current_master: RwLock::new(None),
            master_term: RwLock::new(0),
            last_heartbeat: RwLock::new(None),
            heartbeat_timeout,
        }
    }

    /// Set the current master and its term unconditionally.
    ///
    /// Also records a heartbeat at the current time.
    pub fn set_master(&self, name: &str, term: u64) {
        *self.current_master.write() = Some(name.to_string());
        *self.master_term.write() = term;
        *self.last_heartbeat.write() = Some(Instant::now());
    }

    /// Clear the current master.
    ///
    /// After this call, [`get_master`](Self::get_master) returns `None`.
    pub fn clear_master(&self) {
        *self.current_master.write() = None;
        *self.last_heartbeat.write() = None;
    }

    /// Returns the name of the current master, if known.
    pub fn get_master(&self) -> Option<String> {
        self.current_master.read().clone()
    }

    /// Returns the term of the current master.
    pub fn get_term(&self) -> u64 {
        *self.master_term.read()
    }

    /// Record a heartbeat from the master at the current time.
    pub fn record_heartbeat(&self) {
        *self.last_heartbeat.write() = Some(Instant::now());
    }

    /// Returns `true` if a master is set and its last heartbeat was within
    /// the configured timeout.
    pub fn is_master_alive(&self) -> bool {
        let master = self.current_master.read();
        if master.is_none() {
            return false;
        }
        drop(master);

        let hb = self.last_heartbeat.read();
        match *hb {
            Some(t) => t.elapsed() < self.heartbeat_timeout,
            None => false,
        }
    }

    /// Returns the duration since the last heartbeat, or `None` if no
    /// heartbeat has been recorded.
    pub fn time_since_heartbeat(&self) -> Option<Duration> {
        self.last_heartbeat.read().map(|t| t.elapsed())
    }

    /// Update the master only if `term` is greater than or equal to the
    /// current term.
    ///
    /// This ensures that stale election results (from older terms) cannot
    /// overwrite a more recent master.
    ///
    /// Returns `true` if the master was updated, `false` if the update was
    /// rejected due to a stale term.
    pub fn update_master(&self, name: &str, term: u64) -> bool {
        // Take write locks to perform the check-and-set atomically.
        let mut current_term = self.master_term.write();
        if term < *current_term {
            return false;
        }

        *current_term = term;
        *self.current_master.write() = Some(name.to_string());
        *self.last_heartbeat.write() = Some(Instant::now());

        true
    }

    /// Returns the configured heartbeat timeout.
    pub fn heartbeat_timeout(&self) -> Duration {
        self.heartbeat_timeout
    }
}

// Safety: all interior mutability is behind noxu_sync RwLocks.
unsafe impl Send for MasterTracker {}
unsafe impl Sync for MasterTracker {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    // --- Basic set / get / clear ---

    #[test]
    fn test_initial_state() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        assert!(tracker.get_master().is_none());
        assert_eq!(tracker.get_term(), 0);
        assert!(!tracker.is_master_alive());
        assert!(tracker.time_since_heartbeat().is_none());
    }

    #[test]
    fn test_set_master() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        tracker.set_master("node1", 1);

        assert_eq!(tracker.get_master(), Some("node1".to_string()));
        assert_eq!(tracker.get_term(), 1);
        assert!(tracker.is_master_alive());
    }

    #[test]
    fn test_clear_master() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        tracker.set_master("node1", 1);
        tracker.clear_master();

        assert!(tracker.get_master().is_none());
        assert!(!tracker.is_master_alive());
    }

    #[test]
    fn test_set_master_replaces_previous() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        tracker.set_master("node1", 1);
        tracker.set_master("node2", 2);

        assert_eq!(tracker.get_master(), Some("node2".to_string()));
        assert_eq!(tracker.get_term(), 2);
    }

    // --- Heartbeat ---

    #[test]
    fn test_record_heartbeat() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        tracker.set_master("node1", 1);

        // Heartbeat was set by set_master.
        let since = tracker.time_since_heartbeat().unwrap();
        assert!(since < Duration::from_millis(100));

        // Record another heartbeat.
        tracker.record_heartbeat();
        let since2 = tracker.time_since_heartbeat().unwrap();
        assert!(since2 < Duration::from_millis(100));
    }

    #[test]
    fn test_stale_master_detection() {
        // Use a very short timeout so the master becomes stale quickly.
        let tracker = MasterTracker::new(Duration::from_millis(10));
        tracker.set_master("node1", 1);

        assert!(tracker.is_master_alive());

        // Wait for the heartbeat to expire.
        thread::sleep(Duration::from_millis(20));
        assert!(!tracker.is_master_alive());
    }

    #[test]
    fn test_heartbeat_refresh_keeps_alive() {
        let tracker = MasterTracker::new(Duration::from_millis(50));
        tracker.set_master("node1", 1);

        thread::sleep(Duration::from_millis(30));
        tracker.record_heartbeat();
        assert!(tracker.is_master_alive());

        thread::sleep(Duration::from_millis(30));
        tracker.record_heartbeat();
        assert!(tracker.is_master_alive());
    }

    #[test]
    fn test_time_since_heartbeat_increases() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        tracker.set_master("node1", 1);

        let t1 = tracker.time_since_heartbeat().unwrap();
        thread::sleep(Duration::from_millis(10));
        let t2 = tracker.time_since_heartbeat().unwrap();

        assert!(t2 > t1);
    }

    // --- Term ordering ---

    #[test]
    fn test_update_master_higher_term_accepted() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        tracker.set_master("node1", 1);

        assert!(tracker.update_master("node2", 2));
        assert_eq!(tracker.get_master(), Some("node2".to_string()));
        assert_eq!(tracker.get_term(), 2);
    }

    #[test]
    fn test_update_master_same_term_accepted() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        tracker.set_master("node1", 5);

        // Same term  -  update is accepted (could be a re-election in the same
        // term or a late notification).
        assert!(tracker.update_master("node2", 5));
        assert_eq!(tracker.get_master(), Some("node2".to_string()));
    }

    #[test]
    fn test_update_master_lower_term_rejected() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        tracker.set_master("node1", 5);

        assert!(!tracker.update_master("node2", 3));
        // Master unchanged.
        assert_eq!(tracker.get_master(), Some("node1".to_string()));
        assert_eq!(tracker.get_term(), 5);
    }

    #[test]
    fn test_update_master_from_no_master() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        assert!(tracker.update_master("node1", 1));
        assert_eq!(tracker.get_master(), Some("node1".to_string()));
    }

    // --- Misc ---

    #[test]
    fn test_heartbeat_timeout_accessor() {
        let tracker = MasterTracker::new(Duration::from_secs(42));
        assert_eq!(tracker.heartbeat_timeout(), Duration::from_secs(42));
    }

    #[test]
    fn test_no_heartbeat_means_not_alive() {
        let tracker = MasterTracker::new(Duration::from_secs(5));
        // Master is cleared (no heartbeat).
        tracker.set_master("node1", 1);
        tracker.clear_master();
        assert!(!tracker.is_master_alive());
    }

    // --- Send + Sync ---

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MasterTracker>();
    }

    // --- Concurrent access ---

    #[test]
    fn test_concurrent_updates() {
        use std::sync::Arc;

        let tracker = Arc::new(MasterTracker::new(Duration::from_secs(5)));
        let mut handles = vec![];

        for i in 0..10 {
            let t = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                let name = format!("node{}", i);
                t.update_master(&name, i as u64);
                t.record_heartbeat();
                t.get_master();
                t.is_master_alive();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // After all threads finish, the master should be the one with the
        // highest term that was accepted.
        assert!(tracker.get_master().is_some());
        assert!(tracker.get_term() >= 1);
    }
}
