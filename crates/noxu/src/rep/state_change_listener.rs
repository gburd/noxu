//! State change notification for replication nodes.
//!
//! Rep.StateChangeEvent`.

use crate::rep::node_state::NodeState;

/// Callback for replication state change events.
///
///
///
/// An asynchronous mechanism for tracking the state of the replicated
/// environment and choosing how to route database operations. State determines
/// which operations are currently permitted on the node. For example, only the
/// Master node can execute write operations.
///
/// The listener is registered with the replicated environment using
/// `ReplicatedEnvironment::set_state_change_listener`. There is at most one
/// listener associated with the actual environment at any given instance in
/// time.
///
/// Implementations should do the minimal amount of work, queuing any resource
/// intensive operations for processing by another thread before returning to
/// the caller, so that it does not unduly delay the other housekeeping
/// operations performed by the internal thread which invokes this method.
pub trait StateChangeListener: Send + Sync {
    /// Notification of a state change.
    ///
    /// Initially invoked when the listener is first associated with the
    /// replicated environment, and subsequently each time there is a state
    /// change.
    fn on_state_change(&self, event: StateChangeEvent);
}

/// Describes a state change event.
///
///
///
/// Communicates the state change at a node to the StateChangeListener.
/// There is a distinct instance of this event representing each state
/// change at a node.
#[derive(Debug, Clone)]
pub struct StateChangeEvent {
    /// The previous state before the transition.
    pub old_state: NodeState,
    /// The new state after the transition.
    pub new_state: NodeState,
    /// The name of the current master, if known. Only set when the new state
    /// is Master or Replica.
    pub master_name: Option<String>,
    /// The time at which the event occurred.
    pub timestamp: std::time::Instant,
}

impl StateChangeEvent {
    /// Create a new state change event.
    pub fn new(
        old_state: NodeState,
        new_state: NodeState,
        master_name: Option<String>,
    ) -> Self {
        Self {
            old_state,
            new_state,
            master_name,
            timestamp: std::time::Instant::now(),
        }
    }

    /// Returns the state that the node has transitioned to.
    pub fn get_state(&self) -> NodeState {
        self.new_state
    }

    /// Returns the time the event occurred.
    pub fn get_event_time(&self) -> std::time::Instant {
        self.timestamp
    }

    /// Returns the node name identifying the master at the time of the event.
    ///
    /// Returns `None` if the node is in the Detached or Unknown state.
    pub fn get_master_node_name(&self) -> Option<&str> {
        self.master_name.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct TestListener {
        call_count: AtomicU32,
    }

    impl StateChangeListener for TestListener {
        fn on_state_change(&self, _event: StateChangeEvent) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_state_change_event_creation() {
        let event = StateChangeEvent::new(
            NodeState::Unknown,
            NodeState::Master,
            Some("node1".to_string()),
        );
        assert_eq!(event.old_state, NodeState::Unknown);
        assert_eq!(event.new_state, NodeState::Master);
        assert_eq!(event.get_master_node_name(), Some("node1"));
    }

    #[test]
    fn test_state_change_event_no_master() {
        let event =
            StateChangeEvent::new(NodeState::Master, NodeState::Unknown, None);
        assert_eq!(event.get_state(), NodeState::Unknown);
        assert_eq!(event.get_master_node_name(), None);
    }

    #[test]
    fn test_listener_trait() {
        let listener = Arc::new(TestListener { call_count: AtomicU32::new(0) });

        let event = StateChangeEvent::new(
            NodeState::Unknown,
            NodeState::Master,
            Some("node1".to_string()),
        );
        listener.on_state_change(event);
        assert_eq!(listener.call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_event_time_advances() {
        let e1 =
            StateChangeEvent::new(NodeState::Unknown, NodeState::Master, None);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let e2 = StateChangeEvent::new(
            NodeState::Master,
            NodeState::Replica,
            Some("m".into()),
        );
        assert!(
            e2.get_event_time() > e1.get_event_time(),
            "later event must have a later timestamp"
        );
    }

    #[test]
    fn test_event_clone_and_debug() {
        let e = StateChangeEvent::new(
            NodeState::Replica,
            NodeState::Master,
            Some("self".into()),
        );
        let c = e.clone();
        assert_eq!(c.old_state, e.old_state);
        assert_eq!(c.new_state, e.new_state);
        assert_eq!(c.master_name, e.master_name);
        // Debug should print something non-empty.
        let dbg = format!("{e:?}");
        assert!(!dbg.is_empty());
        assert!(dbg.contains("StateChangeEvent"));
    }

    #[test]
    fn test_listener_never_called_until_event() {
        let listener = Arc::new(TestListener { call_count: AtomicU32::new(0) });
        assert_eq!(listener.call_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_listener_multiple_events() {
        let listener = Arc::new(TestListener { call_count: AtomicU32::new(0) });
        for ns in [NodeState::Unknown, NodeState::Master, NodeState::Replica] {
            let e = StateChangeEvent::new(NodeState::Unknown, ns, None);
            listener.on_state_change(e);
        }
        assert_eq!(listener.call_count.load(Ordering::SeqCst), 3);
    }
}
