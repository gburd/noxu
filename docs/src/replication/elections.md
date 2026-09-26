# Leader Elections

> **v2.0 status — GA.** The election driver is started by
> `ReplicatedEnvironment::open` and the acceptor's promise state is
> persisted to `<env_home>/acceptor.state` across restarts.

Noxu DB uses **Flexible Paxos (FPaxos)** for leader election, augmented by
the **phi accrual failure detector** for adaptive master failure detection.

## Flexible Paxos (FPaxos)

Classic Paxos requires both phases to use majority quorums, which is overly
conservative. FPaxos (Howard 2019) relaxes this:

**Theorem 1 (FPaxos Safety)**: Safety is preserved as long as every Phase 1
quorum intersects every Phase 2 quorum.

For a 5-node group, `phase1=4, phase2=2` satisfies `4+2>5`. This allows
elections to require 4 votes (high confidence) while commits need only 2 acks
(fast writes).

### Phase 1 — Prepare/Promise

The proposer broadcasts a `Prepare(term)` message to all nodes. Each node
that has not promised a higher term responds with `Promise(term, last_voted)`.
The proposer collects promises until `total_promises >= phase1_quorum`.

### Phase 2 — Accept/Accepted

The proposer selects the best candidate from the promises using the JE
`Ranking` order: higher **DTVLSN** (durable transaction VLSN) first, then
higher **VLSN**, then higher **node priority** (see below), then term, then
node name. It broadcasts `Accept(term, candidate)` to Phase 1 promisers. The
proposer collects accepts until `accepts >= phase2_quorum`.

### Election Term

The `term` is a monotonically increasing `u64`. A node always votes for the
candidate it most recently received a valid `Accept` for. A node rejects any
`Prepare` with a `term` lower than the highest it has seen.

## Phi Accrual Failure Detector

Heartbeats flow between nodes continuously. The phi accrual failure detector
(Hayashibara et al., SRDS 2004) computes a suspicion value φ from the
inter-arrival distribution of heartbeats:

```text
φ(now) = -log10(P(inter_arrival > (now - last_heartbeat)))
```

where the CDF is approximated using the Normal distribution
(Abramowitz & Stegun 26.2.17) fitted to the observed mean (μ) and stddev (σ).

**Threshold**: when `φ >= phi_threshold` (default 8.0), the master is
considered failed and an election begins.

### Adaptive Phase Timeout

Rather than using a fixed 500ms timeout, the election phase timeout is derived
from the phi detector's statistics:

```text
phase_timeout = max(μ + k·σ, 50ms)   where k=3.0 (99.7% of heartbeats)
capped at 5s to prevent long outages on degraded networks
```

Falls back to `election_phase_timeout` from `RepConfig` when fewer than 2
heartbeat samples are available.

## Node Priority (steering mastership)

Each node carries an election **priority** (JE `NODE_PRIORITY`,
`RepParams.NODE_PRIORITY`, exposed as a *mutable* rep param via
`ReplicationMutableConfig.java:165`). It is the ranking tiebreaker consulted
after DTVLSN and VLSN, and it gives operators a lever to steer mastership:

```rust
use noxu_rep::RepConfig;

// Prefer this node as master when election progress is otherwise equal
// (e.g. it sits on faster storage or in the primary datacenter).
let config = RepConfig::builder("grp", "node-a", "10.0.0.1")
    .node_priority(10)   // higher = preferred; default is 1
    .build();
```

Semantics, matching JE:

- **Higher priority is preferred** as master when DTVLSN / VLSN / term are
  equal. Priority never overrides progress — a more up-to-date node always
  wins first (no committed data is lost to satisfy a priority preference).
- **Priority `0` means "electable but never chosen as master".** A priority-0
  node still participates in elections as an acceptor and counts toward
  quorum, but it refuses to propose itself as master and its counter-proposal
  is never selected as the Phase 2 value — even if it holds the highest VLSN.
  This is the same not-master-eligible treatment applied to arbiters. Use it
  for a node that should help form a quorum (e.g. a witness / tie-breaker in a
  remote site) without ever taking over writes.
- **Default `1`** — an ordinary electable node.

### Changing priority at runtime

`NODE_PRIORITY` is mutable in JE, so Noxu exposes a setter on the running node.
The change takes effect on the **next election round** the driver starts; it
does **not** demote a node that is currently master (like JE, it only affects
future elections):

```rust
# use std::sync::Arc;
# use noxu_rep::ReplicatedEnvironment;
# fn demo(rep_env: &Arc<ReplicatedEnvironment>) {
// Take this node out of the running for future mastership without
// removing it from the group.
rep_env.set_node_priority(0);

// Read the current live priority.
let _p = rep_env.node_priority();
# }
```

## Quorum Policy

`QuorumPolicy` configures the quorum strategy:

```rust
// Classic: both phases use (n/2)+1
QuorumPolicy::SimpleMajority

// FPaxos: different phase1 and phase2 sizes
QuorumPolicy::Flexible { phase1: 4, phase2: 2 }  // for n=5

// LP-optimal: quoracle builds a quorum system from node capacity/latency
QuorumPolicy::Expression(quorum_system)
```

`validate()` enforces `phase1 + phase2 > n` at construction time, preventing
unsafe configurations from being created.

## Academic References

- Howard, H. (2019). *Distributed Consensus Revised*. UCAM-CL-TR-935.
- Hayashibara, N. et al. (2004). *The φ Accrual Failure Detector*. SRDS 2004.
- Lamport, L. (1998). *Paxos Made Simple*. ACM SIGACT News.
- Howard, H. et al. (2016). *Flexible Paxos*. PaPoC 2016.
