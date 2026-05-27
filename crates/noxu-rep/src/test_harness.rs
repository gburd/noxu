//! In-memory test harness for replication group testing.
//!
//! This module provides Rust analogs of the JE `RepTestBase` /
//! `RepEnvInfo` (`com.sleepycat.je.rep.impl.RepTestBase` and
//! `com.sleepycat.je.rep.utilint.RepTestUtils`) classes, used by
//! the JE replication TCK to bring up multi-node groups in-process,
//! drive lifecycle transitions, replicate VLSN entries, and assert
//! invariants without depending on a real network.
//!
//! # Design philosophy
//!
//! noxu-rep's [`ReplicatedEnvironment`] is already drivable purely
//! in-process: `become_master`, `become_replica`, `register_vlsn`,
//! and `apply_entry` operate on the local node's state machine
//! without requiring any TCP wiring (the TCP receive loop in
//! `become_replica` is only spawned when an [`EnvironmentImpl`] has
//! been attached via `with_environment`).  This harness builds on
//! that property to provide a JE-style group abstraction that:
//!
//! * **Never opens TCP sockets.**  All "replication" between nodes
//!   is driven by the harness calling the appropriate method on each
//!   node directly.  This is the moral equivalent of running the
//!   group with an in-memory [`crate::net::LocalChannel`] transport,
//!   but without the protocol overhead — perfect for testing
//!   higher-level invariants (commit ordering, failover, group
//!   membership).
//! * **Avoids hangs.**  Tests that use this harness cannot hang on
//!   real network coordination because there is no real network.
//!   Every operation is bounded.
//! * **Stays close to JE TCK shape.**  Method names mirror JE's
//!   `RepEnvInfo` / `RepTestBase` so port translations are
//!   mechanical: `openEnv` → [`RepEnvInfo::open_env`], `closeEnv`
//!   → [`RepEnvInfo::close_env`], `createGroup` →
//!   [`RepTestBase::create_group`], `findMaster` →
//!   [`RepTestBase::find_master`], `populateDB` →
//!   [`RepTestBase::populate_db`], etc.
//!
//! Tests that exercise the real network protocol layer should
//! continue to use `cluster_integration_test.rs`-style
//! [`crate::net::TcpChannel`] / [`crate::net::TcpChannelListener`]
//! setups.  This harness is for the layer above.
//!
//! # Quick start
//!
//! ```no_run
//! # #[cfg(any(test, feature = "test-harness"))]
//! # fn demo() {
//! use noxu_rep::test_harness::RepTestBase;
//!
//! // Spin up a 3-node group, elect node 0 as master, replicate
//! // 100 entries, and assert all replicas applied them.
//! let mut group = RepTestBase::builder("demo_group").group_size(3).build();
//! group.create_group(/* master_term */ 1).unwrap();
//! group.populate_db(0, 100).unwrap();
//! group.assert_all_at_vlsn(100);
//! group.shutdown_all();
//! # }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use crate::error::{RepError, Result};
use crate::node_state::NodeState;
use crate::node_type::NodeType;
use crate::quorum_policy::QuorumPolicy;
use crate::rep_config::RepConfig;
use crate::replicated_environment::ReplicatedEnvironment;
use crate::state_change_listener::{StateChangeEvent, StateChangeListener};

// ---------------------------------------------------------------------------
// Port allocation
// ---------------------------------------------------------------------------

/// Process-wide monotonic port counter used to give each harness group a
/// disjoint port range.  noxu-rep's in-process state-machine harness does
/// not actually open these ports, but `RepConfig` requires a port to be
/// set, and giving each test its own range keeps any future TCP-using
/// harness extension forward-compatible.
static NEXT_BASE_PORT: AtomicU16 = AtomicU16::new(40_000);

fn alloc_base_port(group_size: usize) -> u16 {
    // Reserve `group_size + 16` ports per group to leave headroom for
    // mid-test add_peer expansions, monitors, etc.
    let span = (group_size as u16).saturating_add(16);
    let mut current = NEXT_BASE_PORT.load(Ordering::SeqCst);
    loop {
        let next = current.saturating_add(span);
        // Wrap around at 60_000 to stay clear of ephemeral port range.
        let next = if next >= 60_000 { 40_000 + span } else { next };
        match NEXT_BASE_PORT.compare_exchange(
            current,
            next,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return current,
            Err(actual) => current = actual,
        }
    }
}

// ---------------------------------------------------------------------------
// RepEnvInfo
// ---------------------------------------------------------------------------

/// Per-node information held by [`RepTestBase`].
///
/// Mirrors JE's `RepTestUtils.RepEnvInfo` — owns one node's
/// configuration and (optionally) its [`ReplicatedEnvironment`]
/// once `open_env` has been called.
///
/// Cloning a `RepEnvInfo` shares the underlying `Arc<ReplicatedEnvironment>`
/// so the harness can hand out cheap references without giving up ownership.
pub struct RepEnvInfo {
    config: RepConfig,
    /// Node id (1-based, matching JE convention).
    node_id: u32,
    /// `None` until `open_env` is called.
    env: Option<Arc<ReplicatedEnvironment>>,
}

impl RepEnvInfo {
    /// Construct a `RepEnvInfo` with a configuration but no open environment.
    /// Mirrors `new RepEnvInfo(envHome, repConfig, envConfig)` in JE.
    pub fn new(config: RepConfig, node_id: u32) -> Self {
        Self { config, node_id, env: None }
    }

    /// Open the [`ReplicatedEnvironment`] for this node.  After `open_env`
    /// the node is in [`NodeState::Detached`] (as just-opened) until a
    /// `become_master` / `become_replica` call drives a transition.
    ///
    /// Mirrors JE's `RepEnvInfo.openEnv`.
    pub fn open_env(&mut self) -> Result<Arc<ReplicatedEnvironment>> {
        if self.env.is_some() {
            return Err(RepError::StateError(
                "rep env already exists".to_string(),
            ));
        }
        let env = Arc::new(ReplicatedEnvironment::new(self.config.clone())?);
        env.init_self_weak();
        self.env = Some(Arc::clone(&env));
        Ok(env)
    }

    /// Close the environment and drop our handle.  After `close_env`,
    /// `open_env` may be called again to simulate a node restart.
    ///
    /// Mirrors JE's `RepEnvInfo.closeEnv`.
    pub fn close_env(&mut self) -> Result<()> {
        if let Some(env) = self.env.take() {
            env.close()?;
        }
        Ok(())
    }

    /// Drop the env handle without calling `close()` — simulates a crash.
    /// Subsequent `open_env` will create a fresh node.
    ///
    /// Mirrors JE's `RepEnvInfo.abnormalCloseEnv`.
    pub fn abnormal_close_env(&mut self) {
        let _ = self.env.take();
    }

    /// Returns the open env handle, panicking if `open_env` has not been
    /// called.  Use [`RepEnvInfo::env`] for a fallible accessor.
    pub fn get_env(&self) -> Arc<ReplicatedEnvironment> {
        self.env.as_ref().expect("open_env not called yet").clone()
    }

    /// Returns the open env handle, or `None` if not yet opened.
    pub fn env(&self) -> Option<&Arc<ReplicatedEnvironment>> {
        self.env.as_ref()
    }

    /// Returns the [`RepConfig`] for this node.
    pub fn rep_config(&self) -> &RepConfig {
        &self.config
    }

    /// Returns the node name (`config.node_name`).
    pub fn node_name(&self) -> &str {
        &self.config.node_name
    }

    /// Returns the 1-based node id.
    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    // ---- State accessors (JE: isMaster / isReplica / isUnknown) ----

    /// Returns `true` iff the node is currently in [`NodeState::Master`].
    pub fn is_master(&self) -> bool {
        self.env.as_ref().is_some_and(|e| e.get_state() == NodeState::Master)
    }

    /// Returns `true` iff the node is currently in [`NodeState::Replica`].
    pub fn is_replica(&self) -> bool {
        self.env.as_ref().is_some_and(|e| e.get_state() == NodeState::Replica)
    }

    /// Returns `true` iff the node is currently in [`NodeState::Unknown`].
    pub fn is_unknown(&self) -> bool {
        self.env.as_ref().is_some_and(|e| e.get_state() == NodeState::Unknown)
    }

    /// Returns the current node state, or `None` if the env is not open.
    pub fn state(&self) -> Option<NodeState> {
        self.env.as_ref().map(|e| e.get_state())
    }

    /// Returns the current VLSN, or `0` if the env is not open.
    pub fn current_vlsn(&self) -> u64 {
        self.env.as_ref().map(|e| e.get_current_vlsn()).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// RepTestBase + builder
// ---------------------------------------------------------------------------

/// JE-style replication group test fixture.
///
/// Mirrors JE's `RepTestBase` (`com.sleepycat.je.rep.impl.RepTestBase`).
/// Encapsulates a group of `N` nodes that share a group name, port range,
/// and election policy, and provides the lifecycle / replication / assertion
/// helpers that the JE rep TCK uses.
///
/// Use [`RepTestBase::builder`] to construct one; call
/// [`RepTestBase::create_group`] to bring up all nodes; call
/// [`RepTestBase::shutdown_all`] (or rely on `Drop`) to tear them down.
pub struct RepTestBase {
    group_name: String,
    nodes: Vec<RepEnvInfo>,
    /// Cached election term used by [`RepTestBase::create_group`] and
    /// [`RepTestBase::failover_to`].  Each successful failover increments
    /// this so that subsequent `become_master` calls observe a strictly
    /// increasing term.
    next_term: std::cell::Cell<u64>,
}

impl RepTestBase {
    /// Start building a new group with the given group name.
    pub fn builder(group_name: impl Into<String>) -> RepTestBaseBuilder {
        RepTestBaseBuilder::new(group_name)
    }

    /// Number of nodes in the group.
    pub fn group_size(&self) -> usize {
        self.nodes.len()
    }

    /// Borrow node at index `idx` (0-based — JE's `repEnvInfo[i]`).
    pub fn node(&self, idx: usize) -> &RepEnvInfo {
        &self.nodes[idx]
    }

    /// Borrow node at index `idx` mutably.
    pub fn node_mut(&mut self, idx: usize) -> &mut RepEnvInfo {
        &mut self.nodes[idx]
    }

    /// Borrow all nodes.
    pub fn nodes(&self) -> &[RepEnvInfo] {
        &self.nodes
    }

    /// Borrow all nodes mutably.
    pub fn nodes_mut(&mut self) -> &mut [RepEnvInfo] {
        &mut self.nodes
    }

    /// Returns the group name.
    pub fn group_name(&self) -> &str {
        &self.group_name
    }

    // ---- Lifecycle ----

    /// Open every node's env, elect node 0 as master with `term`, and join
    /// nodes 1..N as replicas pointing at node 0.
    ///
    /// Mirrors JE's `RepTestBase.createGroup` (which opens N nodes and
    /// expects the first to become master, the rest replicas).
    pub fn create_group(&mut self, term: u64) -> Result<()> {
        self.create_group_of_size(self.nodes.len(), term)
    }

    /// Same as [`Self::create_group`] but only brings up the first
    /// `first_n` nodes — JE's `createGroup(int firstn)` overload.
    pub fn create_group_of_size(
        &mut self,
        first_n: usize,
        term: u64,
    ) -> Result<()> {
        if first_n == 0 || first_n > self.nodes.len() {
            return Err(RepError::ConfigError(format!(
                "first_n ({first_n}) must be in 1..={}",
                self.nodes.len()
            )));
        }

        // Open all envs first so each node knows about its peers via the
        // GroupService / RepGroup state.
        for node in &mut self.nodes[..first_n] {
            if node.env.is_none() {
                node.open_env()?;
            }
        }

        // Add every other node as a peer of every node so that
        // `get_rep_group()` reflects the topology.  This mirrors JE's
        // helper-host handshake without needing TCP.
        let peer_specs: Vec<crate::rep_node::RepNode> = self.nodes[..first_n]
            .iter()
            .map(|n| {
                crate::rep_node::RepNode::new(
                    n.config.node_name.clone(),
                    n.config.node_type,
                    n.config.node_host.clone(),
                    n.config.node_port,
                    n.node_id,
                )
            })
            .collect();

        for node in &self.nodes[..first_n] {
            let env = node.get_env();
            for peer in &peer_specs {
                if peer.name == node.config.node_name {
                    continue;
                }
                // Best-effort: ignore "already exists" errors.
                let _ = env.add_peer(peer.clone());
            }
        }

        // Elect node 0 as master.
        self.nodes[0].get_env().become_master(term)?;
        let master_name = self.nodes[0].config.node_name.clone();

        // Other nodes become replicas pointing at node 0.
        for node in &self.nodes[1..first_n] {
            node.get_env().become_replica(&master_name)?;
        }

        self.next_term.set(term + 1);
        Ok(())
    }

    /// Close every node's env (master last, to avoid spurious elections —
    /// matches JE's `closeNodes`).
    pub fn shutdown_all(&mut self) {
        let mut master_idx: Option<usize> = None;
        for (idx, node) in self.nodes.iter_mut().enumerate() {
            if node.is_master() {
                master_idx = Some(idx);
                continue;
            }
            let _ = node.close_env();
        }
        if let Some(idx) = master_idx {
            let _ = self.nodes[idx].close_env();
        }
    }

    // ---- Master / replica accessors ----

    /// Find the unique master, or `None` if no node is currently master.
    /// Mirrors JE's `RepTestBase.findMaster`.
    pub fn find_master(&self) -> Option<&RepEnvInfo> {
        self.nodes.iter().find(|n| n.is_master())
    }

    /// Find the master, or `None` — mutable variant.
    pub fn find_master_mut(&mut self) -> Option<&mut RepEnvInfo> {
        self.nodes.iter_mut().find(|n| n.is_master())
    }

    /// Index of the unique master, or `None`.
    pub fn find_master_idx(&self) -> Option<usize> {
        self.nodes.iter().position(|n| n.is_master())
    }

    /// All replica nodes.
    pub fn replicas(&self) -> Vec<&RepEnvInfo> {
        self.nodes.iter().filter(|n| n.is_replica()).collect()
    }

    /// Wait up to `timeout` for some node to be master, polling at
    /// `Duration::from_millis(20)` intervals.  Returns the master's
    /// index on success.  Mirrors JE's `findMasterWait`.
    pub fn await_master(&self, timeout: Duration) -> Result<usize> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(idx) = self.find_master_idx() {
                return Ok(idx);
            }
            if Instant::now() >= deadline {
                return Err(RepError::StateError(format!(
                    "timeout: no master after {:?}",
                    timeout
                )));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Wait up to `timeout` for node `idx` to enter `target` state.
    pub fn await_state(
        &self,
        idx: usize,
        target: NodeState,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.nodes[idx].state() == Some(target) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RepError::StateError(format!(
                    "timeout: node {} did not reach {:?} after {:?} (current: {:?})",
                    idx,
                    target,
                    timeout,
                    self.nodes[idx].state(),
                )));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Wait up to `timeout` for node `idx`'s VLSN to reach at least `vlsn`.
    pub fn await_vlsn_at_least(
        &self,
        idx: usize,
        vlsn: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.nodes[idx].current_vlsn() >= vlsn {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RepError::StateError(format!(
                    "timeout: node {} did not reach VLSN {} after {:?} (current: {})",
                    idx,
                    vlsn,
                    timeout,
                    self.nodes[idx].current_vlsn(),
                )));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    // ---- Replication operations ----

    /// Register a single VLSN on the master and apply it to every other
    /// node (acting as replicas).  This is the in-process moral equivalent
    /// of "the master commits the txn, and the feeder streams it".
    ///
    /// `entry_type` is the replica-side `apply_entry` discriminator (a `u8`
    /// that on the JE side selects between LN / commit / abort entries).
    pub fn replicate_one(
        &self,
        vlsn: u64,
        file: u32,
        offset: u32,
        entry_type: u8,
    ) -> Result<()> {
        let master_idx = self.find_master_idx().ok_or_else(|| {
            RepError::StateError("no master to replicate from".to_string())
        })?;
        let master = self.nodes[master_idx].get_env();
        master.register_vlsn(vlsn, file, offset);

        for (i, node) in self.nodes.iter().enumerate() {
            if i == master_idx || !node.is_replica() {
                continue;
            }
            node.get_env().apply_entry(vlsn, entry_type, vec![0u8; 8])?;
        }
        Ok(())
    }

    /// Replicate `count` VLSN entries starting at `start_vlsn`.  Mirrors
    /// JE's `populateDB(rep, dbName, start, n)` for the harness layer:
    /// the master records each VLSN and replicas apply it in order.
    pub fn populate_db(&self, start_vlsn: u64, count: u64) -> Result<()> {
        for offset in 0..count {
            let vlsn = start_vlsn + offset;
            // entry_type=0 ⇒ generic LN_TRANSACTIONAL marker on the apply
            // side; the harness does not exercise type-specific logic.
            self.replicate_one(vlsn, 0, (vlsn as u32).wrapping_mul(16), 0)?;
        }
        Ok(())
    }

    /// Same as [`Self::populate_db`] but only writes to the master and
    /// leaves replicas in the dust — useful for partition / catch-up tests.
    pub fn populate_master_only(
        &self,
        start_vlsn: u64,
        count: u64,
    ) -> Result<()> {
        let master = self.find_master().ok_or_else(|| {
            RepError::StateError("no master to populate".to_string())
        })?;
        for offset in 0..count {
            let vlsn = start_vlsn + offset;
            master.get_env().register_vlsn(
                vlsn,
                0,
                (vlsn as u32).wrapping_mul(16),
            );
        }
        Ok(())
    }

    /// Replay `start_vlsn..start_vlsn+count` on a single replica — used to
    /// simulate a replica catching up after a partition.
    pub fn catch_up_replica(
        &self,
        replica_idx: usize,
        start_vlsn: u64,
        count: u64,
    ) -> Result<()> {
        let env = self.nodes[replica_idx].get_env();
        for offset in 0..count {
            let vlsn = start_vlsn + offset;
            env.apply_entry(vlsn, 0, vec![0u8; 8])?;
        }
        Ok(())
    }

    // ---- Failover ----

    /// Close the current master; mirrors JE's `leaveGroupAllButMaster`'s
    /// inverse — kill the master, leaving replicas in [`NodeState::Replica`]
    /// until a [`Self::failover_to`] call drives a new election.
    ///
    /// Returns the index of the closed master.
    pub fn close_master(&mut self) -> Result<usize> {
        let idx = self.find_master_idx().ok_or_else(|| {
            RepError::StateError("no master to close".to_string())
        })?;
        self.nodes[idx].close_env()?;
        Ok(idx)
    }

    /// Drive replica `replica_idx` through `Replica → Unknown → Master`
    /// using a fresh term, then point all other live replicas at the new
    /// master.  Mirrors JE's `transferMaster` for the in-process harness.
    pub fn failover_to(&mut self, replica_idx: usize) -> Result<()> {
        let term = self.next_term.get();
        self.next_term.set(term + 1);

        let target_env = self.nodes[replica_idx].get_env();
        target_env.ensure_unknown_state()?;
        target_env.become_master(term)?;

        let new_master_name = self.nodes[replica_idx].config.node_name.clone();
        for (i, node) in self.nodes.iter().enumerate() {
            if i == replica_idx {
                continue;
            }
            if node.env.is_none() {
                continue;
            }
            // Skip nodes that are already master (shouldn't happen) or
            // detached / shutdown.
            let env = node.get_env();
            let s = env.get_state();
            if matches!(s, NodeState::Detached | NodeState::Shutdown) {
                continue;
            }
            env.ensure_unknown_state()?;
            env.become_replica(&new_master_name)?;
        }
        Ok(())
    }

    // ---- Assertions ----

    /// Assert every node currently in [`NodeState::Master`] or
    /// [`NodeState::Replica`] reports `vlsn` as its `current_vlsn`.
    /// Panics on mismatch.
    pub fn assert_all_at_vlsn(&self, vlsn: u64) {
        for node in &self.nodes {
            if !(node.is_master() || node.is_replica()) {
                continue;
            }
            assert_eq!(
                node.current_vlsn(),
                vlsn,
                "node {} ({:?}) at unexpected VLSN",
                node.node_name(),
                node.state(),
            );
        }
    }

    /// Assert node `idx` is in `state`.
    pub fn assert_state(&self, idx: usize, state: NodeState) {
        assert_eq!(
            self.nodes[idx].state(),
            Some(state),
            "node {} ({}) wrong state",
            idx,
            self.nodes[idx].node_name(),
        );
    }
}

impl Drop for RepTestBase {
    fn drop(&mut self) {
        // Best-effort cleanup if the test forgot to call shutdown_all.
        self.shutdown_all();
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for [`RepTestBase`].  Use [`RepTestBase::builder`] to construct.
pub struct RepTestBaseBuilder {
    group_name: String,
    group_size: usize,
    base_port: Option<u16>,
    node_type: NodeType,
    election_timeout: Option<Duration>,
    quorum_policy: Option<QuorumPolicy>,
    name_prefix: Option<String>,
    /// Override the node type for specific indices (e.g. mark node 4 as
    /// Secondary in an otherwise-Electable group).
    node_type_overrides: Vec<(usize, NodeType)>,
}

impl RepTestBaseBuilder {
    fn new(group_name: impl Into<String>) -> Self {
        Self {
            group_name: group_name.into(),
            group_size: 3,
            base_port: None,
            node_type: NodeType::Electable,
            election_timeout: None,
            quorum_policy: None,
            name_prefix: None,
            node_type_overrides: Vec::new(),
        }
    }

    /// Number of nodes in the group (default: 3).
    pub fn group_size(mut self, n: usize) -> Self {
        self.group_size = n;
        self
    }

    /// Base port; node `i` will use `base_port + i`.  Default: a process-wide
    /// monotonically allocated port range that does not overlap other
    /// concurrently-running harness groups.
    pub fn base_port(mut self, p: u16) -> Self {
        self.base_port = Some(p);
        self
    }

    /// Default node type for every node (default: [`NodeType::Electable`]).
    pub fn node_type(mut self, t: NodeType) -> Self {
        self.node_type = t;
        self
    }

    /// Override the node type for a specific index.  May be called multiple
    /// times; later calls override earlier ones for the same index.
    pub fn override_node_type(mut self, idx: usize, t: NodeType) -> Self {
        self.node_type_overrides.push((idx, t));
        self
    }

    /// Election timeout passed to [`RepConfig`].
    pub fn election_timeout(mut self, t: Duration) -> Self {
        self.election_timeout = Some(t);
        self
    }

    /// Quorum policy passed to [`RepConfig`].
    pub fn quorum_policy(mut self, q: QuorumPolicy) -> Self {
        self.quorum_policy = Some(q);
        self
    }

    /// Per-node name prefix; the i-th node will be named
    /// `"{prefix}{i+1}"`.  Default: derived from the group name.
    pub fn name_prefix(mut self, p: impl Into<String>) -> Self {
        self.name_prefix = Some(p.into());
        self
    }

    /// Construct the [`RepTestBase`].  Does NOT open any envs — call
    /// [`RepTestBase::create_group`] to drive the lifecycle.
    pub fn build(self) -> RepTestBase {
        let base_port =
            self.base_port.unwrap_or_else(|| alloc_base_port(self.group_size));
        let prefix = self
            .name_prefix
            .unwrap_or_else(|| format!("{}_n", self.group_name));

        let mut overrides = std::collections::HashMap::new();
        for (idx, t) in self.node_type_overrides {
            overrides.insert(idx, t);
        }

        let mut nodes = Vec::with_capacity(self.group_size);
        for i in 0..self.group_size {
            let node_name = format!("{}{}", prefix, i + 1);
            let node_type = *overrides.get(&i).unwrap_or(&self.node_type);
            let port = base_port + i as u16;

            let mut b =
                RepConfig::builder(&self.group_name, &node_name, "127.0.0.1")
                    .node_port(port)
                    .node_type(node_type);
            if let Some(t) = self.election_timeout {
                b = b.election_timeout(t);
            }
            if let Some(q) = self.quorum_policy.clone() {
                b = b.quorum_policy(q);
            }
            let config = b.build();
            nodes.push(RepEnvInfo::new(config, (i + 1) as u32));
        }

        RepTestBase {
            group_name: self.group_name,
            nodes,
            next_term: std::cell::Cell::new(1),
        }
    }
}

// ---------------------------------------------------------------------------
// Listener helpers
// ---------------------------------------------------------------------------

/// `StateChangeListener` that counts master / replica / unknown / detached
/// / shutdown transitions.  Mirrors JE's `MasterListener` and friends but
/// generalized — every test that wanted a "wait for master became X"
/// listener can read the relevant counter.
#[derive(Default)]
pub struct CountingListener {
    pub master: std::sync::atomic::AtomicUsize,
    pub replica: std::sync::atomic::AtomicUsize,
    pub unknown: std::sync::atomic::AtomicUsize,
    pub detached: std::sync::atomic::AtomicUsize,
    pub shutdown: std::sync::atomic::AtomicUsize,
}

impl CountingListener {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn master_count(&self) -> usize {
        self.master.load(Ordering::SeqCst)
    }
    pub fn replica_count(&self) -> usize {
        self.replica.load(Ordering::SeqCst)
    }
    pub fn unknown_count(&self) -> usize {
        self.unknown.load(Ordering::SeqCst)
    }
    pub fn detached_count(&self) -> usize {
        self.detached.load(Ordering::SeqCst)
    }
    pub fn shutdown_count(&self) -> usize {
        self.shutdown.load(Ordering::SeqCst)
    }
}

impl StateChangeListener for CountingListener {
    fn on_state_change(&self, ev: StateChangeEvent) {
        let counter = match ev.new_state {
            NodeState::Master => &self.master,
            NodeState::Replica => &self.replica,
            NodeState::Unknown => &self.unknown,
            NodeState::Detached => &self.detached,
            NodeState::Shutdown => &self.shutdown,
        };
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Tests for the harness itself
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_produces_n_nodes_with_disjoint_names() {
        let group = RepTestBase::builder("hs1").group_size(4).build();
        assert_eq!(group.group_size(), 4);
        let names: Vec<&str> =
            group.nodes().iter().map(|n| n.node_name()).collect();
        assert_eq!(names, vec!["hs1_n1", "hs1_n2", "hs1_n3", "hs1_n4"]);
        // Ports are monotonically increasing.
        let ports: Vec<u16> =
            group.nodes().iter().map(|n| n.rep_config().node_port).collect();
        for w in ports.windows(2) {
            assert!(w[1] == w[0] + 1, "ports must be consecutive: {:?}", ports);
        }
    }

    #[test]
    fn create_group_elects_master_and_replicas() {
        let mut group = RepTestBase::builder("hs2").group_size(3).build();
        group.create_group(1).unwrap();

        assert!(group.nodes()[0].is_master(), "node 0 must be master");
        assert!(group.nodes()[1].is_replica(), "node 1 must be replica");
        assert!(group.nodes()[2].is_replica(), "node 2 must be replica");

        let m = group.find_master().unwrap();
        assert_eq!(m.node_name(), "hs2_n1");
    }

    #[test]
    fn populate_db_advances_all_replicas() {
        let mut group = RepTestBase::builder("hs3").group_size(3).build();
        group.create_group(1).unwrap();

        group.populate_db(1, 50).unwrap();
        group.assert_all_at_vlsn(50);
    }

    #[test]
    fn failover_drives_replica_to_master() {
        let mut group = RepTestBase::builder("hs4").group_size(3).build();
        group.create_group(1).unwrap();

        // Master writes 10 entries.
        group.populate_db(1, 10).unwrap();
        group.assert_all_at_vlsn(10);

        // Master crashes.
        let old_master = group.close_master().unwrap();
        assert_eq!(old_master, 0);

        // Failover to node 1 (a former replica).
        group.failover_to(1).unwrap();

        // Node 1 must be master, node 2 must be its replica.
        assert!(group.nodes()[1].is_master());
        assert!(group.nodes()[2].is_replica());

        // VLSN must not regress.
        assert!(group.nodes()[1].current_vlsn() >= 10);
    }

    #[test]
    fn await_master_finds_already_elected_master() {
        let mut group = RepTestBase::builder("hs5").group_size(3).build();
        group.create_group(1).unwrap();
        let idx = group.await_master(Duration::from_millis(200)).unwrap();
        assert_eq!(idx, 0);
    }

    #[test]
    fn await_master_times_out_when_no_master() {
        let group = RepTestBase::builder("hs6").group_size(3).build();
        let r = group.await_master(Duration::from_millis(50));
        assert!(r.is_err(), "must time out");
    }

    #[test]
    fn counting_listener_counts_transitions() {
        let mut group = RepTestBase::builder("hs7").group_size(2).build();
        group.create_group(1).unwrap();

        let listener = CountingListener::new();
        group.nodes()[0]
            .get_env()
            .set_state_change_listener(
                Arc::clone(&listener) as Arc<dyn StateChangeListener>
            );
        // Setting a listener fires once with the current state (Master).
        assert_eq!(listener.master_count(), 1);
    }

    #[test]
    fn catch_up_replica_after_partition() {
        let mut group = RepTestBase::builder("hs8").group_size(2).build();
        group.create_group(1).unwrap();

        // Phase 1: both in sync at VLSN 5.
        group.populate_db(1, 5).unwrap();
        group.assert_all_at_vlsn(5);

        // Phase 2: partition — master writes alone.
        group.populate_master_only(6, 10).unwrap();
        assert_eq!(group.nodes()[0].current_vlsn(), 15);
        assert_eq!(group.nodes()[1].current_vlsn(), 5);

        // Phase 3: replica catches up.
        group.catch_up_replica(1, 6, 10).unwrap();
        group.assert_all_at_vlsn(15);
    }
}
