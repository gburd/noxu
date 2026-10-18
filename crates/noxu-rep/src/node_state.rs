//! Node state machine for replication nodes.
//!
//! encapsulates the
//! current replicator state and the ability to validate state transitions.

use noxu_sync::RwLock;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The possible states of a replication node.
///
///
///
/// These states determine which operations are permitted on the node. For
/// example, only the Master node can execute write operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeState {
    /// Node is not associated with the group. Its handle has been closed.
    /// No operations can be performed on the environment when it is in this
    /// state.
    Detached,

    /// Node is not currently in contact with the master, but is actively
    /// trying to establish contact with, or decide upon, a master. While in
    /// this state the node is restricted to performing just read operations
    /// on its environment. In a functioning group, this state is transitory.
    Unknown,

    /// Node is the unique master of the group and can both read and write
    /// to its environment. When the node transitions to this state, the
    /// application running on the node must make provisions to start
    /// processing application level write requests in addition to read
    /// requests.
    Master,

    /// Node is a replica that is being updated by the master. It is
    /// restricted to reading its environment. When the node transitions to
    /// this state, the application running on the node must arrange for all
    /// write requests to be routed to the master.
    Replica,

    /// Node is shutting down. No operations can be performed.
    Shutdown,
}

impl NodeState {
    /// Whether this state accepts write operations.
    pub fn is_writable(&self) -> bool {
        matches!(self, NodeState::Master)
    }

    /// Whether this state accepts read operations.
    pub fn is_readable(&self) -> bool {
        matches!(self, NodeState::Master | NodeState::Replica)
    }

    /// Whether this state is active (participating in group).
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            NodeState::Master | NodeState::Replica | NodeState::Unknown
        )
    }

    /// Whether this is the Master state.
    pub fn is_master(&self) -> bool {
        matches!(self, NodeState::Master)
    }

    /// Whether this is the Replica state.
    pub fn is_replica(&self) -> bool {
        matches!(self, NodeState::Replica)
    }

    /// Whether this is the Detached state.
    pub fn is_detached(&self) -> bool {
        matches!(self, NodeState::Detached)
    }

    /// Whether this is the Unknown state.
    pub fn is_unknown(&self) -> bool {
        matches!(self, NodeState::Unknown)
    }

    /// Returns the set of states that this state can transition to.
    fn valid_transitions(&self) -> &'static [NodeState] {
        match self {
            NodeState::Detached => &[NodeState::Unknown, NodeState::Shutdown],
            NodeState::Unknown => {
                &[NodeState::Master, NodeState::Replica, NodeState::Shutdown]
            }
            NodeState::Master => {
                &[NodeState::Unknown, NodeState::Replica, NodeState::Shutdown]
            }
            NodeState::Replica => {
                &[NodeState::Unknown, NodeState::Master, NodeState::Shutdown]
            }
            NodeState::Shutdown => &[],
        }
    }

    /// Check if a transition to the given state is valid from this state.
    pub fn can_transition_to(&self, new_state: NodeState) -> bool {
        self.valid_transitions().contains(&new_state)
    }
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Detached => write!(f, "DETACHED"),
            NodeState::Unknown => write!(f, "UNKNOWN"),
            NodeState::Master => write!(f, "MASTER"),
            NodeState::Replica => write!(f, "REPLICA"),
            NodeState::Shutdown => write!(f, "SHUTDOWN"),
        }
    }
}

/// Manages state transitions for a replication node.
///
/// encapsulates the
/// current replicator state, validates transitions, and tracks timing.
///
/// All methods are thread-safe. The state machine enforces that only valid
/// transitions are permitted according to the replication protocol.
pub struct NodeStateMachine {
    /// Current state of the node.
    state: RwLock<NodeState>,
    /// Time at which the last state change occurred.
    state_change_time: RwLock<Instant>,
    /// Total number of state transitions that have occurred.
    transition_count: RwLock<u64>,
    /// GAP-REPLAY-3 / BUG-REP-SCREEN-RACE: the shared replica-write screen,
    /// wired once the live `EnvironmentImpl` is attached
    /// ([`ReplicatedEnvironment::with_environment`]).
    ///
    /// The screen is a pure FUNCTION of the role: armed iff the node is **not**
    /// the `Master` (the dual of JE `RepImpl.checkIfMaster`, which admits only
    /// `MASTER` and throws for `REPLICA` / `UNKNOWN` / `DETACHED`, and of
    /// `createRepUserTxn`: `isMaster() ? MasterTxn : ReadonlyTxn`), rather than
    /// carried in a separately-mutable flag.  We honour that invariant by
    /// flipping the screen INSIDE the single state write lock that performs the
    /// role transition (see [`Self::apply_transition_locked`]), so the (role,
    /// screen) pair is updated atomically.  Before this field existed,
    /// `become_master` / `become_replica` stored the screen OUTSIDE the state
    /// lock in opposite orders, so a concurrent transition (election-driver
    /// daemon + a master-transfer handler + an application thread are all
    /// real callers) could leave a non-Master node with the screen CLEARED
    /// -- the S2 silent-divergence hole (a non-master accepting a local user
    /// write) and the `gap_replay_3_write_screen_test` flake.
    ///
    /// `None` until an env is wired (standalone / pre-wiring); the write path
    /// gates the screen on `is_replicated()` anyway, so a `None` here never
    /// weakens a standalone env.
    write_screen: RwLock<Option<Arc<AtomicBool>>>,
}

/// Test-only deterministic witness hook (BUG-REP-STATE-TOCTOU).
///
/// When set, [`NodeStateMachine::transition_via_unknown`] invokes this
/// callback *while holding the state write lock*, at the exact mid-point
/// between the intermediate `Unknown` hop and the final target hop — i.e. the
/// window a second actor would have interleaved into under the former
/// two-lock-acquisition `ensure_unknown_state(); transition_to(target)` code.
/// A witness installs a hook that unblocks and briefly waits on a second
/// thread running `become_master`; because the fix holds the lock across the
/// whole compound transition, that second thread must block, proving no
/// interleave is possible.
///
/// Stored in a module-level static (NOT a field on `NodeStateMachine`) so the
/// struct's layout — and therefore its auto-trait impls (`UnwindSafe` etc.)
/// and its entire public API surface — are byte-identical in every build
/// configuration, including when the `test-harness` feature is unified in by
/// a workspace dev-dependency. Compiled only for tests / the `test-harness`
/// feature; absent from any released build.
#[cfg(any(test, feature = "test-harness"))]
#[allow(clippy::type_complexity)]
static MID_TRANSITION_HOOK: std::sync::RwLock<
    Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
> = std::sync::RwLock::new(None);

impl NodeStateMachine {
    /// Create a new state machine starting in the Detached state.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(NodeState::Detached),
            state_change_time: RwLock::new(Instant::now()),
            transition_count: RwLock::new(0),
            write_screen: RwLock::new(None),
        }
    }

    /// The replica-write screen value implied by `state`.
    ///
    /// Armed (`true`) iff the node is **not** the `Master` -- i.e. a `Replica`,
    /// `Unknown`, `Detached`, or `Shutdown`.  This is the dual of JE
    /// `RepImpl.checkIfMaster` (RepImpl.java:1076-1098), whose role switch
    /// admits only `MASTER`:
    ///
    /// ```text
    ///   case MASTER:   break;                                  // write allowed
    ///   case REPLICA:  throw new ReplicaWriteException(..);     // screened
    ///   case UNKNOWN:  throw new UnknownMasterException(..);    // screened
    ///   case DETACHED: throw new UnknownMasterException(..);    // screened
    /// ```
    ///
    /// and of `RepImpl.createRepUserTxn` (RepImpl.java:1138-1145), which hands
    /// out a writable `MasterTxn` only when `isMaster()` and a read-only
    /// `ReadonlyTxn` otherwise.  Arming for every non-Master role closes the
    /// S2 hole that `matches!(state, Replica)` left open while a node passes
    /// through `Unknown` during `transition_via_unknown` (Master -> Unknown ->
    /// Replica), or sits in `Unknown` after losing its master -- in both of
    /// which it would otherwise ACCEPT a local user write.
    ///
    /// Only matters for a replicated env: the screen handle is wired exclusively
    /// by `ReplicatedEnvironment::with_environment`, and the write path gates
    /// the screen on `is_replicated()`, so a standalone env (whose state
    /// machine is never wired to a screen, and which is never `is_replicated()`)
    /// is unaffected.  The value is derived from the role and stored INSIDE the
    /// single state write lock, so the (role, screen) pair is always consistent
    /// to any observer and a concurrent transition can never leave a non-Master
    /// node unscreened.
    #[inline]
    fn screen_for(state: NodeState) -> bool {
        !matches!(state, NodeState::Master)
    }

    /// Wire the shared replica-write screen handle (GAP-REPLAY-3 /
    /// BUG-REP-SCREEN-RACE).
    ///
    /// Called from [`ReplicatedEnvironment::with_environment`] once the live
    /// `EnvironmentImpl` exists.  Takes the state read lock so the screen is
    /// seeded to the value implied by the CURRENT role atomically with any
    /// concurrent transition: if the node is already a Replica when the env is
    /// wired, the screen comes up armed; if already Master, cleared.
    pub fn set_write_screen(&self, handle: Arc<AtomicBool>) {
        // Order: take the state lock first, then publish the handle, so a
        // transition that races `set_write_screen` either (a) runs before us
        // and leaves `state` final -- we then seed the correct value -- or
        // (b) runs after us and sees the published handle and flips it under
        // its own state-lock critical section.  Either way the pair ends
        // consistent.
        let state = self.state.read();
        handle.store(Self::screen_for(*state), Ordering::SeqCst);
        *self.write_screen.write() = Some(handle);
    }

    /// Read the `(role, screen)` pair ATOMICALLY, under the state read lock.
    ///
    /// Returns the current [`NodeState`] and the screen value (`None` if no
    /// screen handle has been wired).  Because the screen is now flipped only
    /// inside the state write lock ([`Self::apply_transition_locked`]),
    /// sampling both under the state read lock yields a consistent pair: no
    /// transition can be mid-flight.  This is the sound observation point for
    /// the `gap_replay_3` composite-invariant witness (role == Replica =>
    /// screen armed; role == Master => screen cleared).
    pub fn role_and_screen(&self) -> (NodeState, Option<bool>) {
        let state = self.state.read();
        let screen = self
            .write_screen
            .read()
            .as_ref()
            .map(|h| h.load(Ordering::Relaxed));
        (*state, screen)
    }

    /// Install the test-only mid-transition hook (BUG-REP-STATE-TOCTOU
    /// witness). Process-global; call with `None` to clear. See
    /// [`MID_TRANSITION_HOOK`].
    #[cfg(any(test, feature = "test-harness"))]
    pub fn set_mid_transition_hook(
        &self,
        hook: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    ) {
        *MID_TRANSITION_HOOK.write().expect("hook lock") = hook;
    }

    /// Get the current state.
    pub fn get_state(&self) -> NodeState {
        *self.state.read()
    }

    /// Get the time at which the last state change occurred.
    pub fn get_state_change_time(&self) -> Instant {
        *self.state_change_time.read()
    }

    /// Get the total number of state transitions.
    pub fn get_transition_count(&self) -> u64 {
        *self.transition_count.read()
    }

    /// Transition to a new state, validating the transition is legal.
    ///
    /// Returns the previous state on success.
    ///
    /// # Errors
    ///
    /// Returns `RepError::InvalidStateTransition` if the transition from the
    /// current state to the new state is not permitted.
    pub fn transition_to(
        &self,
        new_state: NodeState,
    ) -> crate::error::Result<NodeState> {
        let mut state = self.state.write();
        self.apply_transition_locked(&mut state, new_state)
    }

    /// Apply a single validated transition while the state write lock is
    /// already held, returning the previous state on success.
    ///
    /// This is the shared body of [`Self::transition_to`] and the atomic
    /// compound transitions ([`Self::transition_via_unknown`],
    /// [`Self::ensure_unknown`], [`Self::transition_to_shutdown`]). Factoring
    /// it out lets a compound transition perform *all* of its intermediate and
    /// final steps under ONE write-lock acquisition, closing the
    /// check-then-act window (BUG-REP-STATE-TOCTOU / R-REPSTATE) that existed
    /// when a compound change released the lock between a read and the
    /// subsequent `transition_to`.
    ///
    /// The state lock must remain a leaf: no listener firing, master-tracker
    /// call, or network I/O may occur under it. This method only mutates the
    /// three state fields and logs, so it keeps that invariant.
    fn apply_transition_locked(
        &self,
        state: &mut NodeState,
        new_state: NodeState,
    ) -> crate::error::Result<NodeState> {
        let old_state = *state;

        if !old_state.can_transition_to(new_state) {
            return Err(crate::error::RepError::InvalidStateTransition(
                format!("{} -> {}", old_state, new_state),
            ));
        }

        *state = new_state;
        *self.state_change_time.write() = Instant::now();
        *self.transition_count.write() += 1;

        // GAP-REPLAY-3 / BUG-REP-SCREEN-RACE: flip the replica-write screen to
        // match the new role, INSIDE this one state write lock, so the (role,
        // screen) pair is always consistent to any observer.  Armed iff the
        // node is not writable (not Master) -- JE derives the same decision
        // from `isMaster()` at locker-creation time (`createRepUserTxn`).
        // This replaces the former out-of-lock stores in `become_master` /
        // `become_replica`, which could be overwritten by a concurrent
        // transition and leave a Replica unscreened (S2).  No listener firing,
        // network I/O, or env call happens here -- a `Relaxed` atomic store
        // through the already-held `Arc` keeps the state lock a leaf.
        //
        // Seeding invariant (maintenance note, review-screen-race-verdict #1):
        // `set_write_screen` is race-free when seeding the handle under only the
        // state READ lock *because* this read of `write_screen` happens under the
        // state WRITE lock. A future refactor that moves this read out of the
        // write lock must re-take the write lock in `set_write_screen`.
        if let Some(handle) = self.write_screen.read().as_ref() {
            handle.store(Self::screen_for(new_state), Ordering::Relaxed);
        }

        log::info!("Node state change from {} to {}", old_state, new_state);

        Ok(old_state)
    }

    /// Atomically route the node to `target`, passing through `Unknown` first
    /// when the current state requires it, under a SINGLE state write lock.
    ///
    /// This is the atomic replacement for the former
    /// `ensure_unknown_state(); transition_to(target)` two-step, whose
    /// separate lock acquisitions let a second actor (the election-driver
    /// daemon, which also drives `become_master` / `become_replica`)
    /// interleave and provoke a spurious `UNKNOWN -> UNKNOWN` or
    /// `MASTER -> MASTER` `InvalidStateTransition` (BUG-REP-STATE-TOCTOU).
    ///
    /// `target` must be `Master` or `Replica` — the two active roles reached
    /// via `Unknown`.
    ///
    /// Returns the state observed on entry (the "old state" for a listener
    /// event the caller fires *after* releasing the lock).
    ///
    /// Idempotence (modelled on JE: `RepNode.run()` guards its re-entry to
    /// `UNKNOWN` with `if (nodeState.getRepEnvState() != UNKNOWN)` —
    /// "Avoid unnecessary state changes" — and `NodeState.changeAndNotify`
    /// never rejects a transition): if the node is **already** in `target`,
    /// this is a no-op success returning `target`, NOT an
    /// `InvalidStateTransition`. Re-asserting the role the node already holds
    /// (e.g. the app calls `become_master` for the term it already won from
    /// the election driver) must not fail.
    ///
    /// # Errors
    ///
    /// Returns `RepError::StateError` if the node is `Shutdown`, and
    /// `RepError::InvalidStateTransition` if `target` is not an active role
    /// or the (post-Unknown) step is otherwise illegal.
    pub fn transition_via_unknown(
        &self,
        target: NodeState,
    ) -> crate::error::Result<NodeState> {
        debug_assert!(
            matches!(target, NodeState::Master | NodeState::Replica),
            "transition_via_unknown target must be an active role"
        );

        let mut state = self.state.write();
        let old_state = *state;

        match old_state {
            // Already in the requested role: idempotent no-op success. JE
            // treats re-asserting the current role as benign; the former
            // code's self-transition rejection is exactly the
            // `MASTER -> MASTER` flake.
            s if s == target => Ok(old_state),

            NodeState::Shutdown => Err(crate::error::RepError::StateError(
                "Node is shut down".to_string(),
            )),

            // From Detached/Master/Replica we must pass through Unknown
            // first (Detached -> Unknown, or role -> Unknown), then reach
            // the target. All under this one write lock.
            NodeState::Detached | NodeState::Master | NodeState::Replica => {
                self.apply_transition_locked(&mut state, NodeState::Unknown)?;
                self.fire_mid_transition_hook();
                self.apply_transition_locked(&mut state, target)?;
                Ok(old_state)
            }

            // Already Unknown: single hop to the target.
            NodeState::Unknown => {
                self.fire_mid_transition_hook();
                self.apply_transition_locked(&mut state, target)?;
                Ok(old_state)
            }
        }
    }

    /// Invoke the test-only mid-transition witness hook, if installed, while
    /// the state write lock is held. No-op (and zero-cost) in release builds.
    #[cfg(any(test, feature = "test-harness"))]
    fn fire_mid_transition_hook(&self) {
        let hook = MID_TRANSITION_HOOK.read().expect("hook lock").clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// No-op in builds without the test harness.
    #[cfg(not(any(test, feature = "test-harness")))]
    #[inline(always)]
    fn fire_mid_transition_hook(&self) {}

    /// Atomically ensure the node is in `Unknown`, transitioning from
    /// `Detached` / `Master` / `Replica` if necessary, under a SINGLE state
    /// write lock.
    ///
    /// Idempotent: if the node is already `Unknown` this is a no-op success
    /// (JE `RepNode.run()`: "Avoid unnecessary state changes"). `Shutdown`
    /// is an error.
    ///
    /// Returns the state observed on entry.
    ///
    /// # Errors
    ///
    /// Returns `RepError::StateError` if the node is `Shutdown`.
    pub fn ensure_unknown(&self) -> crate::error::Result<NodeState> {
        let mut state = self.state.write();
        let old_state = *state;
        match old_state {
            NodeState::Unknown => Ok(old_state),
            NodeState::Shutdown => Err(crate::error::RepError::StateError(
                "Node is shut down".to_string(),
            )),
            NodeState::Detached | NodeState::Master | NodeState::Replica => {
                self.apply_transition_locked(&mut state, NodeState::Unknown)?;
                Ok(old_state)
            }
        }
    }

    /// Atomically transition to `Shutdown` from any non-`Shutdown` state,
    /// returning the state observed on entry.
    ///
    /// `Shutdown` is reachable from every live state, so this never fails on
    /// a first call; a second call (already `Shutdown`) is a no-op returning
    /// `Shutdown`. Reads the old state and transitions under ONE write lock so
    /// the `old_state` reported to listeners cannot be stale relative to the
    /// transition that produced it.
    pub fn transition_to_shutdown(&self) -> NodeState {
        let mut state = self.state.write();
        let old_state = *state;
        if old_state == NodeState::Shutdown {
            return old_state;
        }
        // Shutdown is valid from every non-Shutdown state; the result is
        // therefore always Ok here.
        let _ = self.apply_transition_locked(&mut state, NodeState::Shutdown);
        old_state
    }

    /// Check if a transition to the given state is valid from the current state.
    pub fn can_transition_to(&self, new_state: NodeState) -> bool {
        self.state.read().can_transition_to(new_state)
    }

    /// Get the time spent in the current state.
    pub fn time_in_state(&self) -> Duration {
        self.state_change_time.read().elapsed()
    }
}

impl Default for NodeStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- NodeState enum tests ---

    #[test]
    fn test_node_state_is_writable() {
        assert!(NodeState::Master.is_writable());
        assert!(!NodeState::Replica.is_writable());
        assert!(!NodeState::Unknown.is_writable());
        assert!(!NodeState::Detached.is_writable());
        assert!(!NodeState::Shutdown.is_writable());
    }

    #[test]
    fn test_node_state_is_readable() {
        assert!(NodeState::Master.is_readable());
        assert!(NodeState::Replica.is_readable());
        assert!(!NodeState::Unknown.is_readable());
        assert!(!NodeState::Detached.is_readable());
        assert!(!NodeState::Shutdown.is_readable());
    }

    #[test]
    fn test_node_state_is_active() {
        assert!(NodeState::Master.is_active());
        assert!(NodeState::Replica.is_active());
        assert!(NodeState::Unknown.is_active());
        assert!(!NodeState::Detached.is_active());
        assert!(!NodeState::Shutdown.is_active());
    }

    #[test]
    fn test_node_state_convenience_methods() {
        assert!(NodeState::Master.is_master());
        assert!(!NodeState::Replica.is_master());
        assert!(NodeState::Replica.is_replica());
        assert!(!NodeState::Master.is_replica());
        assert!(NodeState::Detached.is_detached());
        assert!(NodeState::Unknown.is_unknown());
    }

    #[test]
    fn test_node_state_display() {
        assert_eq!(format!("{}", NodeState::Detached), "DETACHED");
        assert_eq!(format!("{}", NodeState::Unknown), "UNKNOWN");
        assert_eq!(format!("{}", NodeState::Master), "MASTER");
        assert_eq!(format!("{}", NodeState::Replica), "REPLICA");
        assert_eq!(format!("{}", NodeState::Shutdown), "SHUTDOWN");
    }

    // --- Valid transition tests ---

    #[test]
    fn test_valid_transitions_from_detached() {
        assert!(NodeState::Detached.can_transition_to(NodeState::Unknown));
        assert!(NodeState::Detached.can_transition_to(NodeState::Shutdown));
        assert!(!NodeState::Detached.can_transition_to(NodeState::Master));
        assert!(!NodeState::Detached.can_transition_to(NodeState::Replica));
        assert!(!NodeState::Detached.can_transition_to(NodeState::Detached));
    }

    #[test]
    fn test_valid_transitions_from_unknown() {
        assert!(NodeState::Unknown.can_transition_to(NodeState::Master));
        assert!(NodeState::Unknown.can_transition_to(NodeState::Replica));
        assert!(NodeState::Unknown.can_transition_to(NodeState::Shutdown));
        assert!(!NodeState::Unknown.can_transition_to(NodeState::Detached));
        assert!(!NodeState::Unknown.can_transition_to(NodeState::Unknown));
    }

    #[test]
    fn test_valid_transitions_from_master() {
        assert!(NodeState::Master.can_transition_to(NodeState::Unknown));
        assert!(NodeState::Master.can_transition_to(NodeState::Replica));
        assert!(NodeState::Master.can_transition_to(NodeState::Shutdown));
        assert!(!NodeState::Master.can_transition_to(NodeState::Detached));
        assert!(!NodeState::Master.can_transition_to(NodeState::Master));
    }

    #[test]
    fn test_valid_transitions_from_replica() {
        assert!(NodeState::Replica.can_transition_to(NodeState::Unknown));
        assert!(NodeState::Replica.can_transition_to(NodeState::Master));
        assert!(NodeState::Replica.can_transition_to(NodeState::Shutdown));
        assert!(!NodeState::Replica.can_transition_to(NodeState::Detached));
        assert!(!NodeState::Replica.can_transition_to(NodeState::Replica));
    }

    #[test]
    fn test_valid_transitions_from_shutdown() {
        assert!(!NodeState::Shutdown.can_transition_to(NodeState::Detached));
        assert!(!NodeState::Shutdown.can_transition_to(NodeState::Unknown));
        assert!(!NodeState::Shutdown.can_transition_to(NodeState::Master));
        assert!(!NodeState::Shutdown.can_transition_to(NodeState::Replica));
        assert!(!NodeState::Shutdown.can_transition_to(NodeState::Shutdown));
    }

    // --- NodeStateMachine tests ---

    #[test]
    fn test_initial_state() {
        let sm = NodeStateMachine::new();
        assert_eq!(sm.get_state(), NodeState::Detached);
        assert_eq!(sm.get_transition_count(), 0);
    }

    #[test]
    fn test_default_impl() {
        let sm = NodeStateMachine::default();
        assert_eq!(sm.get_state(), NodeState::Detached);
    }

    #[test]
    fn test_valid_transition_detached_to_unknown() {
        let sm = NodeStateMachine::new();
        let old = sm.transition_to(NodeState::Unknown).unwrap();
        assert_eq!(old, NodeState::Detached);
        assert_eq!(sm.get_state(), NodeState::Unknown);
        assert_eq!(sm.get_transition_count(), 1);
    }

    #[test]
    fn test_valid_transition_unknown_to_master() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        let old = sm.transition_to(NodeState::Master).unwrap();
        assert_eq!(old, NodeState::Unknown);
        assert_eq!(sm.get_state(), NodeState::Master);
        assert_eq!(sm.get_transition_count(), 2);
    }

    #[test]
    fn test_valid_transition_unknown_to_replica() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        let old = sm.transition_to(NodeState::Replica).unwrap();
        assert_eq!(old, NodeState::Unknown);
        assert_eq!(sm.get_state(), NodeState::Replica);
    }

    #[test]
    fn test_valid_transition_master_to_unknown() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        sm.transition_to(NodeState::Master).unwrap();
        let old = sm.transition_to(NodeState::Unknown).unwrap();
        assert_eq!(old, NodeState::Master);
        assert_eq!(sm.get_state(), NodeState::Unknown);
    }

    #[test]
    fn test_valid_transition_master_to_replica() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        sm.transition_to(NodeState::Master).unwrap();
        let old = sm.transition_to(NodeState::Replica).unwrap();
        assert_eq!(old, NodeState::Master);
        assert_eq!(sm.get_state(), NodeState::Replica);
    }

    #[test]
    fn test_valid_transition_replica_to_unknown() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        sm.transition_to(NodeState::Replica).unwrap();
        let old = sm.transition_to(NodeState::Unknown).unwrap();
        assert_eq!(old, NodeState::Replica);
    }

    #[test]
    fn test_valid_transition_replica_to_master() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        sm.transition_to(NodeState::Replica).unwrap();
        let old = sm.transition_to(NodeState::Master).unwrap();
        assert_eq!(old, NodeState::Replica);
        assert_eq!(sm.get_state(), NodeState::Master);
    }

    #[test]
    fn test_valid_transition_to_shutdown_from_all() {
        // From Detached
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Shutdown).unwrap();
        assert_eq!(sm.get_state(), NodeState::Shutdown);

        // From Unknown
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        sm.transition_to(NodeState::Shutdown).unwrap();
        assert_eq!(sm.get_state(), NodeState::Shutdown);

        // From Master
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        sm.transition_to(NodeState::Master).unwrap();
        sm.transition_to(NodeState::Shutdown).unwrap();
        assert_eq!(sm.get_state(), NodeState::Shutdown);

        // From Replica
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        sm.transition_to(NodeState::Replica).unwrap();
        sm.transition_to(NodeState::Shutdown).unwrap();
        assert_eq!(sm.get_state(), NodeState::Shutdown);
    }

    #[test]
    fn test_invalid_transition_detached_to_master() {
        let sm = NodeStateMachine::new();
        let result = sm.transition_to(NodeState::Master);
        assert!(result.is_err());
        assert_eq!(sm.get_state(), NodeState::Detached);
        assert_eq!(sm.get_transition_count(), 0);
    }

    #[test]
    fn test_invalid_transition_detached_to_replica() {
        let sm = NodeStateMachine::new();
        let result = sm.transition_to(NodeState::Replica);
        assert!(result.is_err());
        assert_eq!(sm.get_state(), NodeState::Detached);
    }

    #[test]
    fn test_invalid_transition_from_shutdown() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Shutdown).unwrap();

        assert!(sm.transition_to(NodeState::Detached).is_err());
        assert!(sm.transition_to(NodeState::Unknown).is_err());
        assert!(sm.transition_to(NodeState::Master).is_err());
        assert!(sm.transition_to(NodeState::Replica).is_err());
        assert!(sm.transition_to(NodeState::Shutdown).is_err());
        assert_eq!(sm.get_state(), NodeState::Shutdown);
        // Only one successful transition (Detached -> Shutdown)
        assert_eq!(sm.get_transition_count(), 1);
    }

    #[test]
    fn test_invalid_self_transition() {
        let sm = NodeStateMachine::new();
        // Detached -> Detached should fail
        assert!(sm.transition_to(NodeState::Detached).is_err());

        sm.transition_to(NodeState::Unknown).unwrap();
        // Unknown -> Unknown should fail
        assert!(sm.transition_to(NodeState::Unknown).is_err());
    }

    #[test]
    fn test_transition_counting() {
        let sm = NodeStateMachine::new();
        assert_eq!(sm.get_transition_count(), 0);

        sm.transition_to(NodeState::Unknown).unwrap();
        assert_eq!(sm.get_transition_count(), 1);

        sm.transition_to(NodeState::Master).unwrap();
        assert_eq!(sm.get_transition_count(), 2);

        sm.transition_to(NodeState::Unknown).unwrap();
        assert_eq!(sm.get_transition_count(), 3);

        sm.transition_to(NodeState::Replica).unwrap();
        assert_eq!(sm.get_transition_count(), 4);

        // Failed transition should not increment
        let _ = sm.transition_to(NodeState::Detached);
        assert_eq!(sm.get_transition_count(), 4);
    }

    #[test]
    fn test_time_in_state() {
        let sm = NodeStateMachine::new();
        // Should be very short since we just created it
        let d = sm.time_in_state();
        assert!(d < Duration::from_secs(1));
    }

    #[test]
    fn test_state_change_time_updates() {
        let sm = NodeStateMachine::new();
        let t1 = sm.get_state_change_time();
        sm.transition_to(NodeState::Unknown).unwrap();
        let t2 = sm.get_state_change_time();
        assert!(t2 >= t1);
    }

    #[test]
    fn test_can_transition_to_on_machine() {
        let sm = NodeStateMachine::new();
        assert!(sm.can_transition_to(NodeState::Unknown));
        assert!(sm.can_transition_to(NodeState::Shutdown));
        assert!(!sm.can_transition_to(NodeState::Master));

        sm.transition_to(NodeState::Unknown).unwrap();
        assert!(sm.can_transition_to(NodeState::Master));
        assert!(sm.can_transition_to(NodeState::Replica));
        assert!(!sm.can_transition_to(NodeState::Detached));
    }

    #[test]
    fn test_full_lifecycle() {
        let sm = NodeStateMachine::new();
        assert_eq!(sm.get_state(), NodeState::Detached);

        // Start up: enter election
        sm.transition_to(NodeState::Unknown).unwrap();
        assert_eq!(sm.get_state(), NodeState::Unknown);

        // Win election: become master
        sm.transition_to(NodeState::Master).unwrap();
        assert_eq!(sm.get_state(), NodeState::Master);
        assert!(sm.get_state().is_writable());
        assert!(sm.get_state().is_readable());

        // Master transfer: become replica
        sm.transition_to(NodeState::Replica).unwrap();
        assert_eq!(sm.get_state(), NodeState::Replica);
        assert!(!sm.get_state().is_writable());
        assert!(sm.get_state().is_readable());

        // Lose contact with master
        sm.transition_to(NodeState::Unknown).unwrap();
        assert_eq!(sm.get_state(), NodeState::Unknown);

        // Re-election: become master again
        sm.transition_to(NodeState::Master).unwrap();
        assert_eq!(sm.get_state(), NodeState::Master);

        // Shutdown
        sm.transition_to(NodeState::Shutdown).unwrap();
        assert_eq!(sm.get_state(), NodeState::Shutdown);
        assert!(!sm.get_state().is_writable());
        assert!(!sm.get_state().is_readable());
        assert!(!sm.get_state().is_active());

        assert_eq!(sm.get_transition_count(), 6);
    }

    // --- Atomic compound transition tests (BUG-REP-STATE-TOCTOU) ---

    #[test]
    fn test_transition_via_unknown_from_detached() {
        let sm = NodeStateMachine::new();
        // Detached -> (Unknown) -> Master, atomic.
        let old = sm.transition_via_unknown(NodeState::Master).unwrap();
        assert_eq!(old, NodeState::Detached);
        assert_eq!(sm.get_state(), NodeState::Master);
        // Two real hops happened (Detached->Unknown, Unknown->Master).
        assert_eq!(sm.get_transition_count(), 2);
    }

    #[test]
    fn test_transition_via_unknown_from_unknown() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Unknown).unwrap();
        let old = sm.transition_via_unknown(NodeState::Replica).unwrap();
        assert_eq!(old, NodeState::Unknown);
        assert_eq!(sm.get_state(), NodeState::Replica);
        // One hop: Unknown->Replica (plus the setup Detached->Unknown).
        assert_eq!(sm.get_transition_count(), 2);
    }

    #[test]
    fn test_transition_via_unknown_role_change() {
        let sm = NodeStateMachine::new();
        sm.transition_via_unknown(NodeState::Master).unwrap();
        // Master -> (Unknown) -> Replica.
        let old = sm.transition_via_unknown(NodeState::Replica).unwrap();
        assert_eq!(old, NodeState::Master);
        assert_eq!(sm.get_state(), NodeState::Replica);
    }

    #[test]
    fn test_transition_via_unknown_idempotent_master() {
        // The exact BUG-REP-STATE-TOCTOU shape: re-asserting Master while
        // already Master must be a no-op SUCCESS, never MASTER -> MASTER.
        let sm = NodeStateMachine::new();
        sm.transition_via_unknown(NodeState::Master).unwrap();
        let count_before = sm.get_transition_count();
        let old = sm.transition_via_unknown(NodeState::Master).unwrap();
        assert_eq!(old, NodeState::Master);
        assert_eq!(sm.get_state(), NodeState::Master);
        // No-op: no extra transition recorded.
        assert_eq!(sm.get_transition_count(), count_before);
    }

    #[test]
    fn test_transition_via_unknown_idempotent_replica() {
        let sm = NodeStateMachine::new();
        sm.transition_via_unknown(NodeState::Replica).unwrap();
        let count_before = sm.get_transition_count();
        let old = sm.transition_via_unknown(NodeState::Replica).unwrap();
        assert_eq!(old, NodeState::Replica);
        assert_eq!(sm.get_state(), NodeState::Replica);
        assert_eq!(sm.get_transition_count(), count_before);
    }

    #[test]
    fn test_transition_via_unknown_rejected_from_shutdown() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Shutdown).unwrap();
        assert!(sm.transition_via_unknown(NodeState::Master).is_err());
        assert_eq!(sm.get_state(), NodeState::Shutdown);
    }

    #[test]
    fn test_ensure_unknown_from_each_state() {
        // From Detached.
        let sm = NodeStateMachine::new();
        let old = sm.ensure_unknown().unwrap();
        assert_eq!(old, NodeState::Detached);
        assert_eq!(sm.get_state(), NodeState::Unknown);

        // Already Unknown: idempotent no-op.
        let count_before = sm.get_transition_count();
        let old = sm.ensure_unknown().unwrap();
        assert_eq!(old, NodeState::Unknown);
        assert_eq!(sm.get_state(), NodeState::Unknown);
        assert_eq!(sm.get_transition_count(), count_before);

        // From Master.
        let sm = NodeStateMachine::new();
        sm.transition_via_unknown(NodeState::Master).unwrap();
        let old = sm.ensure_unknown().unwrap();
        assert_eq!(old, NodeState::Master);
        assert_eq!(sm.get_state(), NodeState::Unknown);

        // From Replica.
        let sm = NodeStateMachine::new();
        sm.transition_via_unknown(NodeState::Replica).unwrap();
        let old = sm.ensure_unknown().unwrap();
        assert_eq!(old, NodeState::Replica);
        assert_eq!(sm.get_state(), NodeState::Unknown);
    }

    #[test]
    fn test_ensure_unknown_rejected_from_shutdown() {
        let sm = NodeStateMachine::new();
        sm.transition_to(NodeState::Shutdown).unwrap();
        assert!(sm.ensure_unknown().is_err());
        assert_eq!(sm.get_state(), NodeState::Shutdown);
    }

    #[test]
    fn test_transition_to_shutdown_from_each_state() {
        let sm = NodeStateMachine::new();
        assert_eq!(sm.transition_to_shutdown(), NodeState::Detached);
        assert_eq!(sm.get_state(), NodeState::Shutdown);
        // Second call is an idempotent no-op returning Shutdown.
        assert_eq!(sm.transition_to_shutdown(), NodeState::Shutdown);

        let sm = NodeStateMachine::new();
        sm.transition_via_unknown(NodeState::Master).unwrap();
        assert_eq!(sm.transition_to_shutdown(), NodeState::Master);
        assert_eq!(sm.get_state(), NodeState::Shutdown);
    }

    #[test]
    fn test_compound_transition_contested_single_lock() {
        // Two threads race the atomic compound transition on the same state
        // machine, exactly as the election driver races an application
        // become_master. With the atomic method, every call must either win
        // (observe a non-target old state and move the machine) or observe
        // the target already set and no-op; none may error.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        for _ in 0..200 {
            let sm = Arc::new(NodeStateMachine::new());
            let errors = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(std::sync::Barrier::new(2));

            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let sm = Arc::clone(&sm);
                    let errors = Arc::clone(&errors);
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        barrier.wait();
                        if sm.transition_via_unknown(NodeState::Master).is_err()
                        {
                            errors.fetch_add(1, Ordering::SeqCst);
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(
                errors.load(Ordering::SeqCst),
                0,
                "atomic compound transition must never error under contention"
            );
            assert_eq!(sm.get_state(), NodeState::Master);
        }
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NodeStateMachine>();
        assert_send_sync::<NodeState>();
    }
}
