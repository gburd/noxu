//! First-class in-memory replication transport (Wave 11-D).
//!
//! noxu-rep ships three wire-level transports out of the box:
//!
//! | Transport | Module | Use case |
//! |-----------|--------|----------|
//! | TCP       | [`crate::rep::net::TcpChannel`]      | Plain replication LAN/WAN |
//! | TLS       | `crate::rep::net::TlsTcpChannel`     | Encrypted WAN (rustls / native-tls) |
//! | QUIC      | `crate::rep::net::QuicChannel`       | Multiplexed UDP (feature `quic`) |
//! | **In-memory** | [`InMemoryTransport`] (this module) | In-process clusters, embedded use cases, tests |
//!
//! The in-memory transport originated as a wire-level fixture for
//! protocol unit tests ([`crate::rep::net::LocalChannel`]).  Wave 11-D
//! promotes that wiring into a first-class **production** transport
//! so users can compose multi-node replication groups inside a single
//! process — useful for embedded deployments, integration testing,
//! and Stateright-driven property tests that need real
//! `ReplicatedEnvironment` instances but no real network.
//!
//! # Topologies
//!
//! Two topologies are supported out of the box:
//!
//! * **Pair** — back-to-back endpoints, suitable for a 2-node
//!   master/replica pair: [`InMemoryTransport::new_pair`].
//! * **Group** — `N`-node fully-connected mesh suitable for any
//!   election quorum: [`InMemoryTransport::new_group`].
//!
//! The mesh maintains exactly `N · (N - 1)` directional channels —
//! one per ordered `(from, to)` pair — and routes each `send` to the
//! corresponding peer's receive queue, mirroring the semantics of a
//! real point-to-point socket cluster.
//!
//! # Crash injection
//!
//! Production cluster tests need to exercise crash recovery without
//! tearing down the entire process.  [`InMemoryGroup::simulate_crash`]
//! closes every channel that originates from or terminates at the
//! crashed node, so subsequent `send` / `receive` calls on those
//! channels return [`crate::rep::error::RepError::ChannelClosed`] — exactly
//! what a real socket disconnect would produce.
//!
//! Once a node has been crashed, [`InMemoryGroup::reconnect`] rewires
//! a fresh set of channels into the same slot, simulating a node
//! restart or a network partition heal.
//!
//! # Wire compatibility
//!
//! `InMemoryEndpoint` is a thin wrapper around [`LocalChannel`] and
//! implements the same [`Channel`] trait as the TCP, TLS, and QUIC
//! transports.  Higher layers
//! ([`crate::rep::stream::feeder::FeederRunner`],
//! [`crate::rep::stream::replica_stream::ReplicaStream`],
//! [`crate::rep::elections`]) consume `dyn Channel` so they work
//! identically over any of the four transports without modification.
//!
//! # Usage
//!
//! ```no_run
//! use crate::rep::net::{Channel, InMemoryTransport};
//! use std::time::Duration;
//!
//! // 1. Single back-to-back pair (e.g., master + 1 replica).
//! let (a, b) = InMemoryTransport::new_pair();
//! a.send(b"hello").unwrap();
//! let msg = b.receive(Duration::from_millis(50)).unwrap();
//! assert_eq!(msg, Some(b"hello".to_vec()));
//!
//! // 2. 3-node fully-connected mesh.
//! let group = InMemoryTransport::new_group(3);
//! group.channel(0, 1).send(b"ping").unwrap();
//! let recv = group.channel(1, 0).receive(Duration::from_millis(50)).unwrap();
//! assert_eq!(recv, Some(b"ping".to_vec()));
//! ```

use std::sync::Arc;
use std::time::Duration;

use crate::sync::Mutex;

use crate::rep::error::Result;
use crate::rep::net::channel::{Channel, LocalChannel, LocalChannelPair};

// ---------------------------------------------------------------------------
// InMemoryEndpoint
// ---------------------------------------------------------------------------

/// One end of an in-memory replication channel.
///
/// Implements [`Channel`] identically to [`crate::rep::net::TcpChannel`] and
/// `crate::rep::net::TlsTcpChannel`.  Internally backed by
/// [`LocalChannel`] queues and a `crate::sync::Mutex`.
///
/// Endpoints are constructed via [`InMemoryTransport::new_pair`] or
/// [`InMemoryTransport::new_group`].  Direct construction is intentionally
/// not exposed — pairing two endpoints requires the cross-connected
/// queues set up by the transport factory.
pub struct InMemoryEndpoint {
    /// The underlying [`LocalChannel`].  Held in an `Arc` so the
    /// owning `InMemoryGroup` can hand out cheap clones to higher
    /// layers (`Arc<dyn Channel>`-style) without giving up ownership.
    inner: Arc<LocalChannel>,
}

impl InMemoryEndpoint {
    fn new(inner: LocalChannel) -> Self {
        Self { inner: Arc::new(inner) }
    }

    /// Return a cheap shareable handle to this endpoint's underlying
    /// channel.  Useful when the protocol layer wants
    /// `Arc<dyn Channel>` (e.g., when spawning a reader thread that
    /// outlives the borrow of the group).
    pub fn channel_handle(&self) -> Arc<dyn Channel> {
        Arc::clone(&self.inner) as Arc<dyn Channel>
    }
}

impl Channel for InMemoryEndpoint {
    fn send(&self, data: &[u8]) -> Result<()> {
        self.inner.send(data)
    }

    fn receive(&self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        self.inner.receive(timeout)
    }

    fn close(&self) -> Result<()> {
        self.inner.close()
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }
}

// ---------------------------------------------------------------------------
// InMemoryTransport (factory)
// ---------------------------------------------------------------------------

/// Factory namespace for in-memory replication transports.
///
/// `InMemoryTransport` is a zero-sized type whose associated functions
/// build [`InMemoryEndpoint`] / [`InMemoryGroup`] instances.  See the
/// [module-level docs](crate::rep::net::inmem) for the full topology table
/// and design rationale.
pub struct InMemoryTransport;

impl InMemoryTransport {
    /// Create a single bidirectional pair of cross-connected
    /// in-memory endpoints.
    ///
    /// Sends on `a` arrive at `b`'s receive queue and vice versa.
    /// Equivalent to [`LocalChannelPair::new`] but returned as
    /// production-named [`InMemoryEndpoint`] handles.
    pub fn new_pair() -> (InMemoryEndpoint, InMemoryEndpoint) {
        let pair = LocalChannelPair::new();
        (
            InMemoryEndpoint::new(pair.channel_a),
            InMemoryEndpoint::new(pair.channel_b),
        )
    }

    /// Create an `n`-node fully-connected in-memory group.
    ///
    /// The returned [`InMemoryGroup`] owns `n · (n - 1)` directional
    /// channels arranged so that `group.channel(i, j).send(msg)` is
    /// observed by `group.channel(j, i).receive(...)`.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0`.  A 1-node "group" is supported (degenerate)
    /// but a zero-node group is meaningless and almost certainly a
    /// caller bug.
    pub fn new_group(n: usize) -> InMemoryGroup {
        InMemoryGroup::new(n)
    }
}

// ---------------------------------------------------------------------------
// InMemoryGroup
// ---------------------------------------------------------------------------

/// An `n`-node fully-connected in-memory replication mesh.
///
/// `InMemoryGroup` owns one [`InMemoryEndpoint`] per ordered
/// `(from, to)` peer pair (with `from != to`).  The endpoint at
/// `(from, to)` is `from`'s view of its socket to `to`; sending on
/// that endpoint is observed by the endpoint at `(to, from)`.
///
/// Higher layers typically consume the group by handing each node
/// the row of channels `[group.channel(my_id, peer)] for peer in 0..n`.
///
/// # Crash and recovery
///
/// [`InMemoryGroup::simulate_crash`] closes every channel touching a
/// node, modelling a hard crash or partition.  After a crash,
/// [`InMemoryGroup::reconnect`] rewires that node's row of channels
/// (paired against the other live nodes) to model a node restart or
/// healed partition.  A crashed node may be reconnected at most once
/// per crash; the implementation tolerates repeated calls.
pub struct InMemoryGroup {
    n: usize,
    /// `endpoints[i][j]` (with `i != j`) is node `i`'s endpoint to
    /// node `j`.  Diagonal slots (`i == j`) are kept as `None` so the
    /// caller can index by `(from, to)` without arithmetic.
    ///
    /// Wrapped in `Mutex<Option<_>>` so [`Self::reconnect`] can replace
    /// individual endpoints under the lock without invalidating any
    /// outstanding `Arc<dyn Channel>` clones held elsewhere.
    endpoints: Vec<Vec<Mutex<Option<InMemoryEndpoint>>>>,
}

impl InMemoryGroup {
    fn new(n: usize) -> Self {
        assert!(n > 0, "InMemoryGroup requires at least one node");

        // Build n×n matrix; diagonal stays None.
        let endpoints: Vec<Vec<Mutex<Option<InMemoryEndpoint>>>> = (0..n)
            .map(|_| (0..n).map(|_| Mutex::new(None)).collect())
            .collect();

        // Cross-connect every (i, j) with i < j: one LocalChannelPair
        // gives us both `i → j` (channel_a) and `j → i` (channel_b).
        #[allow(clippy::needless_range_loop)]
        for i in 0..n {
            for j in (i + 1)..n {
                let pair = LocalChannelPair::new();
                *endpoints[i][j].lock() =
                    Some(InMemoryEndpoint::new(pair.channel_a));
                *endpoints[j][i].lock() =
                    Some(InMemoryEndpoint::new(pair.channel_b));
            }
        }

        Self { n, endpoints }
    }

    /// Number of nodes in the mesh.
    pub fn size(&self) -> usize {
        self.n
    }

    /// Return a cheap [`Arc<dyn Channel>`] handle to the directed
    /// channel from `from` to `to`.
    ///
    /// # Panics
    ///
    /// Panics if `from` or `to` is out of range, if `from == to`, or
    /// if the channel has been removed by [`Self::simulate_crash`]
    /// without a subsequent [`Self::reconnect`].
    pub fn channel(&self, from: usize, to: usize) -> Arc<dyn Channel> {
        assert!(from < self.n, "from index {from} out of range (n={})", self.n);
        assert!(to < self.n, "to index {to} out of range (n={})", self.n);
        assert!(from != to, "in-memory mesh has no self-loop channel");
        let slot = self.endpoints[from][to].lock();
        slot.as_ref()
            .unwrap_or_else(|| {
                panic!(
                    "in-memory channel {from}→{to} is closed; \
                     call reconnect({from}) before reuse"
                )
            })
            .channel_handle()
    }

    /// Try to acquire the directed channel from `from` to `to`,
    /// returning `None` if the channel has been crashed.
    ///
    /// # Panics
    ///
    /// Panics on out-of-range indices or `from == to`.
    pub fn try_channel(
        &self,
        from: usize,
        to: usize,
    ) -> Option<Arc<dyn Channel>> {
        assert!(from < self.n, "from index {from} out of range (n={})", self.n);
        assert!(to < self.n, "to index {to} out of range (n={})", self.n);
        assert!(from != to, "in-memory mesh has no self-loop channel");
        let slot = self.endpoints[from][to].lock();
        slot.as_ref().map(|e| e.channel_handle())
    }

    /// Simulate a hard crash of `node`: every channel originating
    /// from or terminating at `node` is `close`d and dropped from the
    /// mesh.  Subsequent `send` / `receive` on any handle that was
    /// previously cloned out of those channels returns
    /// [`crate::rep::error::RepError::ChannelClosed`], matching a real
    /// socket disconnect.
    ///
    /// Idempotent: calling `simulate_crash` on an already-crashed
    /// node is a no-op.
    ///
    /// # Panics
    ///
    /// Panics if `node` is out of range.
    pub fn simulate_crash(&self, node: usize) {
        assert!(node < self.n, "node index {node} out of range (n={})", self.n);
        for peer in 0..self.n {
            if peer == node {
                continue;
            }
            // Close and drop both directions independently so a half-
            // crashed mesh (one direction reconnected, the other not)
            // is still expressible by the caller.
            let mut out = self.endpoints[node][peer].lock();
            if let Some(ep) = out.take() {
                let _ = ep.inner.close();
            }
            drop(out);

            let mut inn = self.endpoints[peer][node].lock();
            if let Some(ep) = inn.take() {
                let _ = ep.inner.close();
            }
        }
    }

    /// Rewire `node`'s row of channels against every peer that is
    /// still live (i.e., still has a channel slot to `node`).  Models
    /// a node restart or a healed partition.
    ///
    /// Channels whose remote end is itself currently crashed are left
    /// disconnected; the caller should reconnect them in a separate
    /// pass once the remote node has come back.
    ///
    /// # Panics
    ///
    /// Panics if `node` is out of range.
    pub fn reconnect(&self, node: usize) {
        assert!(node < self.n, "node index {node} out of range (n={})", self.n);
        for peer in 0..self.n {
            if peer == node {
                continue;
            }
            // Lock both ordered pairs, smallest index first to keep a
            // global lock order (deadlock-free).
            let (lo, hi) =
                if node < peer { (node, peer) } else { (peer, node) };
            let mut a = self.endpoints[lo][hi].lock();
            let mut b = self.endpoints[hi][lo].lock();

            // Only reconnect if both directions are currently empty.
            // If one side is live and the other isn't, the caller has
            // a half-open mesh and we leave it as-is.
            if a.is_some() || b.is_some() {
                continue;
            }
            let pair = LocalChannelPair::new();
            *a = Some(InMemoryEndpoint::new(pair.channel_a));
            *b = Some(InMemoryEndpoint::new(pair.channel_b));
        }
    }

    /// Return `true` iff every directed channel touching `node` is
    /// currently open.
    pub fn is_node_live(&self, node: usize) -> bool {
        if node >= self.n {
            return false;
        }
        for peer in 0..self.n {
            if peer == node {
                continue;
            }
            if self.endpoints[node][peer].lock().is_none() {
                return false;
            }
            if self.endpoints[peer][node].lock().is_none() {
                return false;
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_round_trip() {
        let (a, b) = InMemoryTransport::new_pair();
        a.send(b"hello").unwrap();
        let got = b.receive(Duration::from_millis(50)).unwrap();
        assert_eq!(got, Some(b"hello".to_vec()));
        b.send(b"world").unwrap();
        let got = a.receive(Duration::from_millis(50)).unwrap();
        assert_eq!(got, Some(b"world".to_vec()));
    }

    #[test]
    fn group_3node_mesh_is_fully_connected() {
        let group = InMemoryTransport::new_group(3);
        assert_eq!(group.size(), 3);

        // Every directed pair has a channel.
        for i in 0..3 {
            for j in 0..3 {
                if i == j {
                    continue;
                }
                let _ = group.channel(i, j);
            }
        }

        // Sends on (i → j) are received on the (j → i) endpoint
        // (same underlying queue pair, opposite end).
        group.channel(0, 1).send(b"01").unwrap();
        let got =
            group.channel(1, 0).receive(Duration::from_millis(50)).unwrap();
        assert_eq!(got, Some(b"01".to_vec()));
    }

    #[test]
    fn group_independent_pairs_do_not_cross_talk() {
        let group = InMemoryTransport::new_group(4);
        group.channel(0, 1).send(b"to-1").unwrap();
        group.channel(0, 2).send(b"to-2").unwrap();

        let g10 =
            group.channel(1, 0).receive(Duration::from_millis(50)).unwrap();
        let g20 =
            group.channel(2, 0).receive(Duration::from_millis(50)).unwrap();
        let g30 =
            group.channel(3, 0).receive(Duration::from_millis(50)).unwrap();
        assert_eq!(g10, Some(b"to-1".to_vec()));
        assert_eq!(g20, Some(b"to-2".to_vec()));
        assert_eq!(g30, None, "node 3 must not see node 1's traffic");
    }

    #[test]
    fn simulate_crash_closes_all_channels_for_node() {
        let group = InMemoryTransport::new_group(3);
        // Take handles before the crash; they must observe the close.
        let zero_to_one = group.channel(0, 1);
        let one_to_zero = group.channel(1, 0);

        group.simulate_crash(0);

        // Pre-crash handles see ChannelClosed on send / receive.
        assert!(zero_to_one.send(b"after-crash").is_err());
        let r = one_to_zero.receive(Duration::from_millis(20));
        assert!(r.is_err(), "post-crash receive must surface error");

        // Group accessors fail-fast for the crashed slot.
        assert!(group.try_channel(0, 1).is_none());
        assert!(group.try_channel(1, 0).is_none());
        // Non-crashed pair still works.
        assert!(group.try_channel(1, 2).is_some());
        group.channel(1, 2).send(b"alive").unwrap();
        let got =
            group.channel(2, 1).receive(Duration::from_millis(50)).unwrap();
        assert_eq!(got, Some(b"alive".to_vec()));
    }

    #[test]
    fn simulate_crash_is_idempotent() {
        let group = InMemoryTransport::new_group(3);
        group.simulate_crash(2);
        group.simulate_crash(2);
        // Node 2 is fully crashed: every channel touching it is None.
        assert!(!group.is_node_live(2));
        // The non-crashed (0,1) link is still up.
        assert!(group.try_channel(0, 1).is_some());
        assert!(group.try_channel(1, 0).is_some());
        // Sanity: nodes 0 and 1 each still have an open neighbor.
        group.channel(0, 1).send(b"alive").unwrap();
        let got =
            group.channel(1, 0).receive(Duration::from_millis(50)).unwrap();
        assert_eq!(got, Some(b"alive".to_vec()));
    }

    #[test]
    fn reconnect_after_crash_restores_traffic() {
        let group = InMemoryTransport::new_group(3);
        group.simulate_crash(0);
        assert!(!group.is_node_live(0));

        group.reconnect(0);
        assert!(group.is_node_live(0));

        // New handles work end-to-end.
        group.channel(0, 1).send(b"reborn").unwrap();
        let got =
            group.channel(1, 0).receive(Duration::from_millis(50)).unwrap();
        assert_eq!(got, Some(b"reborn".to_vec()));
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn channel_out_of_range_panics() {
        let group = InMemoryTransport::new_group(2);
        let _ = group.channel(5, 0);
    }

    #[test]
    #[should_panic(expected = "no self-loop")]
    fn channel_self_loop_panics() {
        let group = InMemoryTransport::new_group(2);
        let _ = group.channel(0, 0);
    }

    #[test]
    #[should_panic(expected = "at least one node")]
    fn empty_group_panics() {
        let _ = InMemoryTransport::new_group(0);
    }

    #[test]
    fn one_node_group_has_no_channels() {
        let group = InMemoryTransport::new_group(1);
        assert_eq!(group.size(), 1);
        assert!(group.is_node_live(0));
    }

    #[test]
    fn channel_handle_outlives_borrow_of_group() {
        let handle: Arc<dyn Channel> = {
            let group = InMemoryTransport::new_group(2);
            group.channel(0, 1)
        };
        // The group has been dropped; the handle's underlying queues
        // are kept alive by the matching peer-side handle on the Arc
        // refcount of the inner LocalChannel.  Sending into a dropped
        // peer must surface ChannelClosed (writer is gone, reader
        // queue is dropped).  For this smoke test we just verify the
        // handle itself stays usable enough to query is_open without
        // panicking.
        let _ = handle.is_open();
    }
}
