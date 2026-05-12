# Noxu Replication — Design and Implementation

## Overview

Noxu replication provides multi-node active/passive Paxos-based replication across
TCP and QUIC transports with Flexible Paxos quorum optimization, phi accrual failure
detection, and dynamic membership. The implementation lives in `crates/noxu-rep/` and
ports the BDB JE `com.sleepycat.je.rep` package with several significant enhancements:
FPaxos quorum decoupling, adaptive failure detection, QUIC multiplexed streams, and
LP-optimal quorum selection via the quoracle library.

The system is single-master, multiple-replica. Write transactions are accepted only at
the master and shipped to replicas via a VLSN-indexed log stream. Master election uses
two-phase Paxos; liveness is monitored by either a binary heartbeat timeout or a
continuous phi accrual failure detector.

---

## 1. Leader Election — Flexible Paxos (FPaxos)

### What it does

The election protocol (`crates/noxu-rep/src/elections/paxos.rs`) implements classic
two-phase Paxos with value selection:

**Phase 1 (Prepare / Promise):** The proposer broadcasts an `ElectionProposal`
containing its node name, VLSN, priority, and term to all peer channels. Each acceptor
that has not already promised a higher-termed proposal responds with its own
`ElectionProposal` (carrying its VLSN as the "suggestion value"). If it has already
promised a higher term, it rejects with `ElectionVote { granted: false }`.

**Phase 2 (Accept / Commit):** If a Phase 1 quorum of promises is collected, the
proposer selects the *best* candidate seen across all Phase 1 suggestions (highest
VLSN, then priority, then term, then node name) and broadcasts an `ElectionResult`
announcing that winner. Each peer that promised grants acceptance if the result term
meets or exceeds its promised term. If a Phase 2 quorum of accepts is reached, the
election succeeds and the winner becomes master.

The proposer always counts itself as one vote in both phases. A single-node cluster
therefore self-elects immediately with no network I/O.

**FPaxos optimization:** `run_election` uses separate `phase1_quorum` and
`phase2_quorum` values obtained from `RepGroup`, which delegates to the configured
`QuorumPolicy`. For `SimpleMajority` both equal `(n/2)+1`; for `Flexible` they may
differ as long as the intersection property holds.

### Why FPaxos

FPaxos Theorem 1 states: for safety, it suffices that every Phase 1 quorum Q1
intersects every Phase 2 quorum Q2 — i.e., for all Q1 in QS1, Q2 in QS2:
Q1 intersection Q2 is non-empty. The classic constraint (both quorums = majority) is a
special case.

This decoupling is valuable in practice:

- **5-node cluster, phase1=4, phase2=2:** Elections require 4/5 agreement (rare event,
  can tolerate slowness) but commits need only 2 ACKs (fast steady-state). This cuts
  commit latency nearly in half versus majority-3 at the cost of slower elections.
- **Write-heavy workloads:** Reducing phase2 from 3 to 2 eliminates waiting for the
  slowest replica in steady state.

### Alternatives considered

- **Raft** (Ongaro & Ousterhout 2014): Simpler leader election via randomized timeout,
  but fundamentally couples log replication quorum with election quorum. Cannot support
  FPaxos-style decoupling without breaking the Raft invariants. Rejected for this
  reason.
- **Multi-Paxos / distinguished-proposer optimization:** Would amortize Phase 1 across
  multiple values. Deferred — out of scope for current cluster sizes (3-9 nodes) where
  elections are infrequent.
- **ZAB (Zookeeper Atomic Broadcast):** Production-proven total-order broadcast but
  tightly coupled to the ZooKeeper JVM ecosystem. Rejected due to JVM dependency and
  less flexible quorum model.

### References

- Lamport, L. (1998). Paxos Made Simple. *ACM SIGACT News*, 32(4), 18-25.
- Howard, H., Schwarzkopf, M., Madhavapeddy, A., & Crowcroft, J. (2016). Flexible Paxos: Quorum Intersection Revisited. arXiv:1608.06696.
- Howard, H. (2019). Distributed Consensus Revised. UCAM-CL-TR-935. University of Cambridge.
- Ongaro, D., & Ousterhout, J. (2014). In Search of an Understandable Consensus Algorithm. *USENIX ATC 2014*.

---

## 2. Failure Detection — Phi Accrual Detector

### What it does

The phi accrual failure detector (`crates/noxu-rep/src/elections/phi_detector.rs`)
outputs a continuous suspicion level rather than a binary alive/dead decision:

```
phi(t) = -log10(P_later(t - T_last))
```

where `P_later(u)` is the probability that the next inter-arrival gap will be at least
`u`, estimated from the last `window_size` inter-arrival samples modeled as a Normal
distribution. The process is **suspected** when `phi >= threshold`.

Implementation details:

- **Normal CDF:** Computed via the Abramowitz & Stegun 26.2.17 rational approximation
  (max error < 1.5e-7). No external math library dependency.
- **Survival function:** `P_later(u) = 1 - Phi((u - mu) / sigma)` where mu and sigma
  are the sample mean and standard deviation of the sliding window.
- **Warm-up:** Returns phi=0 (always available) until at least 2 inter-arrival samples
  exist. This prevents false suspicion during startup.
- **Thread safety:** All state protected by `noxu_sync::RwLock`.

**Production defaults** (from the paper):
- `threshold = 8.0` — mistake rate approximately 10^-8 per heartbeat interval
- `window_size = 200` for LAN; `1000` for WAN

### Integration with MasterTracker

`MasterTracker` (`crates/noxu-rep/src/elections/master_tracker.rs`) wraps the phi
detector. When configured via `with_phi(detector)`, `is_master_alive()` returns
`phi_detector.is_available()` (phi < threshold) instead of the binary check
`elapsed < heartbeat_timeout`. The `record_heartbeat()` method feeds samples to both
the timestamp tracker and the phi detector.

The binary heartbeat timeout is retained as a fallback for deployments that do not
configure phi detection, matching BDB JE's original behavior.

### Adaptive election timeout (derived from phi statistics)

Election phase timeouts are currently fixed at 500ms (matching BDB JE defaults). The
planned enhancement derives adaptive timeouts from the phi detector's statistics:
`phase_timeout = mu + k*sigma` where k=3.0 (3-sigma gives 99.7% coverage), with a
floor of 50ms and ceiling of 5s.

Why not a fixed 500ms: BDB JE's default assumes LAN. This is wrong for WAN links,
loaded systems, or ultra-fast NVMe/RDMA networks where the optimal timeout differs by
orders of magnitude.

### Alternatives considered

- **Binary heartbeat timeout** (BDB JE style): Non-adaptive; must be manually tuned
  per deployment. Kept as the default fallback when no phi detector is configured.
- **SWIM gossip-based failure detection** (Das et al. 2002): Better suited for large
  clusters (>20 nodes) where O(n) heartbeats become expensive. Overkill for 3-9 node
  replication groups.
- **Adaptive Accrual Failure Detector** (Satzger et al. 2007): Extends phi with
  non-Gaussian distributions (e.g., exponential, burst-aware). Deferred as the Normal
  model is adequate for typical replication heartbeat patterns.

### References

- Hayashibara, N., Defago, X., Yared, R., & Katayama, T. (2004). The Phi Accrual Failure Detector. *SRDS 2004*, 66-78.
- Abramowitz, M., & Stegun, I. A. (1964). *Handbook of Mathematical Functions*. Formula 26.2.17.
- Das, A., Gupta, I., & Motivala, A. (2002). SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol. *DSN 2002*.
- Satzger, B., Pietzowski, A., Trumler, W., & Ungerer, T. (2007). A New Adaptive Accrual Failure Detector for Dependable Distributed Systems. *SAC 2007*.

---

## 3. Quorum System — quoracle

### What it does

The `QuorumPolicy` enum (`crates/noxu-rep/src/quorum_policy.rs`) provides three
strategies for determining election quorums:

1. **`SimpleMajority`** — Classic `(n/2)+1` for both phases. Default; matches BDB JE's
   `RepGroup.quorumSize()`.

2. **`Flexible { phase1, phase2 }`** — Operator-chosen sizes with a built-in safety
   check: `validate()` enforces `phase1 + phase2 > n` (the FPaxos intersection
   invariant). Example: 5 nodes, phase1=4, phase2=2 gives fast commits with safe
   elections.

3. **`Expression(QuorumSystem<String>)`** — A full quoracle `QuorumSystem` built from
   AND/OR/Choose expressions. The intersection property is validated by quoracle at
   construction time. Supports arbitrary quorum structures including grid quorums,
   weighted voting, and hierarchical schemes.

**Construction helpers:**

- `build_expression(node_names, phase1_k, phase2_k)` — builds Phase 1 as
  `choose(phase1_k, nodes)` and Phase 2 as `choose(phase2_k, nodes)`, validated by
  `QuorumSystem::new()`.
- `build_majority_expression(node_names)` — builds both phases as `majority(nodes)`;
  useful for testing quoracle integration without changing election behavior.

**quoracle library** (`_/rs-quoracle`): A Rust port providing `QuorumSystem::new(reads,
writes)` which validates the intersection property at construction. LP-optimal quorum
selection uses `microlp` (pure Rust, no C dependencies). `RepNode::to_quoracle_node()`
converts node capacity and latency metadata into quoracle `Node<String>` values for
load-optimal selection.

### Alternatives considered

- **Hardcoded simple majority:** Simple but cannot exploit FPaxos decoupling and offers
  no load-balancing. Kept as the `SimpleMajority` default for backward compatibility.
- **Grid quorums** (Naor & Wool 1998): Optimal load but require fixed grid topology.
  Available via the `Expression` variant for deployments that want it.
- **Weighted voting** (Thomas 1979): Assigns votes proportional to node capacity.
  Subsumed by the quoracle `Expression` variant which can express any weighted scheme.

### References

- Naor, M., & Wool, A. (1998). The Load, Capacity, and Availability of Quorum Systems. *SIAM J. Comput.*, 27(2), 423-447.
- Thomas, R. H. (1979). A Majority Consensus Approach to Concurrency Control. *ACM TODS*, 4(2), 180-209.
- Burd, G. rs-quoracle. https://codeberg.org/gregburd/rs-quoracle

---

## 4. Dynamic Membership

### What it does

`ReplicatedEnvironment` (`crates/noxu-rep/src/replicated_environment.rs`) exposes
runtime membership management:

- **`add_peer(node: RepNode)`** — Registers the node in the `GroupService` with its
  name, type, host, port, node_id, and initial metadata (joined_at, last_seen,
  is_active=true, known_vlsn=0). Elections and quorum calculations immediately reflect
  the new membership.

- **`remove_peer(name: &str)`** — Deregisters the node from the `GroupService`.
  Elections initiated after this call will not include the removed node in quorum
  calculations.

- **`get_rep_group()`** — Returns a snapshot `RepGroup` reflecting current membership
  at the time of the call.

### Constraints

- **Quorum preservation:** The cluster must maintain quorum throughout any membership
  change. Removing too many nodes can make the cluster unavailable. For `Flexible`
  policies, `phase1 + phase2 > n` must hold after every change;
  `RepGroup::rebuild_quorum_system()` validates this.

- **FPaxos safety:** After removing a node, if the new `n` violates
  `phase1 + phase2 > n`, the quorum policy must be downgraded (e.g., to
  SimpleMajority) or the phase sizes adjusted before the next election.

- **Live operation:** Membership changes are safe to perform while replication is
  active. The GroupService uses internal locking; only a brief group lock is held
  during the registration/deregistration, not during any network I/O.

---

## 5. Network Transport

### TCP transport (TcpChannel)

`TcpChannel` (`crates/noxu-rep/src/net/channel.rs:206`) provides a synchronous
`Channel` implementation over `std::net::TcpStream`.

**Wire framing:** `[payload_len: u32 LE][payload bytes]`. Every message is a
length-prefixed byte vector. The receiver reads exactly 4 bytes for the length, then
reads exactly that many payload bytes.

**Timeouts:**
- `TcpStream::connect_timeout(30s)` — prevents indefinite blocking from OS SYN backoff
  (Linux default can reach ~127s under packet loss).
- `set_write_timeout(Some(30s))` — caps send stalls under congestion.
- `set_read_timeout(caller_timeout)` — applied before reading the length prefix;
  WouldBlock/TimedOut returns `Ok(None)`.

**Bugs found and fixed in 6-hour soak testing:**
- Bug 1: Setting `set_read_timeout(None)` before payload read caused hangs under packet
  loss. Fixed: read timeout is always set.
- Bug 3: `TcpStream::connect()` (no timeout) blocked indefinitely under SYN loss with
  OS exponential backoff up to 127s. Fixed: switched to `connect_timeout(30s)`.

### QUIC transport (QuicChannel)

`QuicChannel` (`crates/noxu-rep/src/net/quic_channel.rs`) provides the same `Channel`
trait over a single QUIC bidirectional stream using Quinn 0.11.

**TLS:** QUIC mandates TLS 1.3. For intra-cluster replication, a self-signed
certificate is generated at runtime via `rcgen`. The client uses a custom
`SkipCertVerification` verifier — appropriate because replication peers are
authenticated at the Paxos layer and operate on a private network.

**Wire framing:** Identical to TCP: `[u32 LE length][payload]`.

**Synchronous bridge:** Quinn is async (Tokio). The synchronous `Channel` trait is
bridged via `Arc<Runtime>` + `runtime.block_on()` for each send/receive. Stream guards
use `tokio::sync::Mutex` to be held across await points.

**Bug 2:** Quinn-proto's MTUD (MTU discovery) assertion panics under netem
duplicate/corrupt injection. Fixed: `transport.mtu_discovery_config(None)` disables
PMTUD for loopback and test environments.

### QUIC multiplexed streams (QuicMultiplexedChannel)

`QuicMultiplexedChannel` (`crates/noxu-rep/src/net/quic_mux.rs`) extends the
single-stream QUIC transport with true stream multiplexing: one QUIC connection carries
four independent bidirectional streams:

| Stream | ID | Purpose |
|--------|-----|---------|
| Heartbeat | 0 | Elections and heartbeats (priority traffic) |
| Log | 1 | Log-entry shipping (master to replica) |
| Ack | 2 | Commit acknowledgements (replica to master) |
| Restore | 3 | Network restore file transfer |

**Why separate streams matter:** QUIC enforces per-stream flow control. On TCP, a large
log-shipping burst fills the socket send buffer and delays the next heartbeat — the
classic head-of-line (HOL) blocking problem. With separate QUIC streams, log
back-pressure on stream 1 has no effect on stream 0 heartbeats, so the
`PhiAccrualDetector` sees a tighter inter-arrival distribution and is less prone to
false elections.

**CBVLSN datagrams:** CBVLSN (Cluster-Based VLSN) heartbeat values are broadcast as
8-byte unreliable QUIC datagrams (RFC 9221). A lost datagram is immediately superseded
by the next broadcast (~10ms later), so reliability is unnecessary and retransmission
overhead is avoided. Datagram receive buffer: 64 KiB.

**Wire handshake:** On connection, the client opens four bidirectional QUIC streams (in
order 0-3) and writes a 5-byte handshake on each: `[NXMX: 4 bytes][stream_type: 1
byte]`. The server accepts four streams and validates the magic bytes and stream type.
The `NXMX` magic distinguishes multiplexed connections from single-stream `QuicChannel`
connections (which use `NXUR`).

**0-RTT reconnect:** Because `connect` stores the underlying `Endpoint`, TLS session
tickets from the initial connection are cached. On master failover, the endpoint can be
extracted via `take_endpoint()` and reused with `connect_with_endpoint()`: Quinn uses
the cached session ticket for 0-RTT early-data reconnect, cutting latency from ~3 RTT
(TCP+TLS) to ~1 RTT.

**Transport config:** Both `mux_server_config()` and `mux_insecure_client_config()`
disable PMTUD (same quinn-proto assertion fix as single-stream) and enable 64 KiB
datagram receive buffers.

### References

- Iyengar, J. & Thomson, M. (2021). QUIC: A UDP-Based Multiplexed and Secure Transport. RFC 9000, IETF.
- Pauly, T., Kinnear, E., & Wood, C. (2022). An Unreliable Datagram Extension to QUIC. RFC 9221, IETF.

---

## 6. VLSN and Log Replication

### What it does

VLSN (Virtual Log Sequence Number) provides a monotonically increasing, cluster-wide
logical clock for replicated log entries. Each committed write transaction on the master
is assigned the next VLSN, and this assignment is replicated to all nodes.

**VlsnIndex:** Maps VLSN values to log file positions (LSNs) for efficient random
access during replica catch-up and network restore.

**CBVLSN (Cluster-Based Barrier VLSN):** The minimum VLSN across all active replicas.
Log entries below CBVLSN are safe to reclaim by the log cleaner. Broadcast via
unreliable QUIC datagrams or piggybacked on TCP heartbeats.

**Log shipping architecture:**
- **EnvironmentLogScanner** (master side): Implements the `LogScanner` trait. Reads
  committed log entries from the master's log files starting at a given VLSN and feeds
  them to `Feeder` threads — one per connected replica.
- **EnvironmentLogWriter** (replica side): Receives log entries from the
  `ReplicaStream` and writes them into the replica's local log, advancing the local
  VLSN as entries are durably written.
- **NetworkRestore:** Full file-set transfer for new replicas or replicas that have
  fallen too far behind CBVLSN. Uses a dedicated TCP service
  (`NetworkRestoreServer`) registered on the `TcpServiceDispatcher`, or stream 3 on
  multiplexed QUIC connections.

### References

- Lamport, L. (1978). Time, Clocks, and the Ordering of Events in a Distributed System. *CACM*, 21(7), 558-565.
- Oracle. Berkeley DB Java Edition High Availability Guide. https://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/rep/package-summary.html
