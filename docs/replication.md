# Noxu DB Replication Guide

This guide covers replication in Noxu DB, a Rust port of Berkeley DB Java Edition
(BDB JE) 7.5.11. Noxu DB's replication implementation is in the `noxu-rep` crate
(`crates/noxu-rep/`), which ports `com.sleepycat.je.rep`.

Noxu DB replication is a **single-master, multiple-replica** strategy. Write
transactions are accepted only at the master node and streamed to replicas using a
logical replication stream identified by Virtual Log Sequence Numbers (VLSNs).

> **Implementation status:** The `noxu-rep` crate has complete data structures,
> state machines, election logic, VLSN tracking, feeder/replica framing, and
> consistency policy enforcement. Network connections (TCP feeder/replica runner
> threads) exist structurally but are not yet wired to a running network loop.
> See the "Current Gaps" note at the end of each relevant section.

---

## Table of Contents

1. [Introduction to Replication](#1-introduction-to-replication)
2. [Architecture](#2-architecture)
3. [Replication Group Configuration](#3-replication-group-configuration)
4. [Starting a Replicated Environment](#4-starting-a-replicated-environment)
5. [State Changes](#5-state-changes)
6. [Elections](#6-elections)
7. [Consistency Policies](#7-consistency-policies)
8. [Commit Durability](#8-commit-durability)
9. [Secondary (Read-Only) Nodes](#9-secondary-read-only-nodes)
10. [Monitor Nodes](#10-monitor-nodes)
11. [Network Partitions and Split-Brain](#11-network-partitions-and-split-brain)
12. [Two-Node Groups](#12-two-node-groups)
13. [Adding and Removing Nodes](#13-adding-and-removing-nodes)
14. [Backups in a Replicated Environment](#14-backups-in-a-replicated-environment)
15. [Performance Considerations](#15-performance-considerations)

---

## 1. Introduction to Replication

Noxu DB High Availability (HA) is a replicated, single-master, embedded database
engine. It extends the data guarantees of a transactional system to processes
running on multiple physical hosts.

### Why Use Replication?

**Improved application availability.** By spreading data across multiple machines,
the application's data remains available even if hardware on any single machine
fails.

**Improved read performance.** Read operations can be distributed across many
replica nodes. This is especially valuable for read-heavy workloads and for
applications where readers are located far from the master — replicas at the network
edge reduce read latency.

**Improved transactional commit performance.** Durability typically requires a
synchronous disk write. Replication allows you to commit to the network (i.e., to
one or more replicas) instead of — or in addition to — committing to local disk.
This batches I/O more efficiently while maintaining a durability guarantee.

**Improved data durability.** With replication, data modifications reach multiple
disks, disk controllers, and power supplies. This eliminates the single point of
failure present in a standalone database.

### Fundamental Constraint

Every replicated application must be transactional. All databases created in a
replicated environment must also be transactional.

---

## 2. Architecture

### Replication Group

The set of all nodes participating in replication is called the **replication
group**. The group is identified by a unique name. It persists even when no nodes
are currently running.

Node types:

| Type | Elections | Stores data | Can be master |
|------|-----------|-------------|---------------|
| `Electable` | Yes | Yes | Yes |
| `Secondary` | No | Yes | No |
| `Monitor` | No | No | No |
| `Arbiter` | Yes (tie-breaking) | No | No |

In Rust:

```rust
use noxu_rep::NodeType;

// Electable: can become master or replica, participates in elections
NodeType::Electable

// Secondary: read-only replica, no elections, no quorum contribution
NodeType::Secondary

// Monitor: observes group state, routes requests; no data, no elections
NodeType::Monitor

// Arbiter: participates in elections for tie-breaking, stores no data
NodeType::Arbiter
```

Each node has a **unique name** within the group. Node names must not be reused
after a node is permanently removed.

### Master and Replica Roles

At any instant there is exactly one **Master** node. The master accepts all write
transactions. All other data nodes serve as **Replicas** and are read-only. If the
master becomes unavailable, the remaining electable nodes elect a new master.

### Virtual Log Sequence Numbers (VLSNs)

Every replicated log entry is assigned a group-wide monotonically increasing
**VLSN** (Virtual Log Sequence Number). VLSNs are used to:

- Identify a node's position in the replication stream.
- Allow replicas to resume streaming after a disconnect without re-transmitting
  entries the replica already has.
- Express consistency requirements ("this read must see at least VLSN N").

VLSNs are independent of the physical log file structure. Two nodes may have the
same VLSN mapped to different file offsets, but will have identical data contents.

```rust
// Registering a VLSN on the master after writing a log entry:
rep_env.register_vlsn(vlsn, file_number, file_offset);

// Applying a replicated entry on the replica:
rep_env.apply_entry(vlsn, entry_type, data)?;

// Querying the current VLSN and range:
let current = rep_env.get_current_vlsn();
let range   = rep_env.get_vlsn_range(); // VlsnRange { first, last }
```

### Feeder / Replica Stream Architecture

The master maintains one **Feeder** per connected replica. Each feeder:

1. Scans the log forward from the replica's known VLSN.
2. Frames each entry as `[vlsn: 8 LE][entry_type: 1][payload_len: 4 LE][payload]`.
3. Sends the frame to the replica over a `Channel`.
4. Receives ack messages from the replica (8-byte LE VLSN) and records them.

The ack stream enables the master to determine when enough replicas have applied an
entry to satisfy the configured durability policy.

Each replica runs a **ReplicaStream** that receives frames from the master's feeder,
applies entries to the local environment, and sends acks back.

**Current gap:** The `FeederRunner::run()` loop and `ReplicaStream` are implemented
and unit-tested against in-memory channels, but are not yet connected to live TCP
sockets in the startup path.

---

## 3. Replication Group Configuration

All replication configuration is expressed through `RepConfig`, built with its
builder API.

### Minimum Required Configuration

```rust
use noxu_rep::RepConfig;

let config = RepConfig::builder(
    "my_group",    // replication group name (unique per logical group)
    "node1",       // this node's name (unique within the group)
    "10.0.0.1",    // this node's hostname or IP
)
.node_port(5001)
.build();
```

The group name must be identical across all nodes that belong to the same
replication group. It is how Noxu DB detects misconfigured nodes.

### Helper Nodes

A **helper node** is an existing active member of the group that a new or restarting
node contacts to locate the current master. At least one helper host must be
provided when joining an existing group.

```rust
let config = RepConfig::builder("my_group", "node3", "10.0.0.3")
    .node_port(5001)
    .add_helper_host("10.0.0.1:5001".to_string())  // existing node
    .add_helper_host("10.0.0.2:5001".to_string())  // fallback
    .build();
```

The helper node does not need to be the master; any active member can redirect the
new node to the current master.

### Node Type

```rust
use noxu_rep::{RepConfig, NodeType};

// Secondary node (read-only, no elections)
let config = RepConfig::builder("my_group", "secondary1", "10.0.0.10")
    .node_port(5001)
    .node_type(NodeType::Secondary)
    .add_helper_host("10.0.0.1:5001".to_string())
    .build();
```

### Timeouts

| Field | Default | Purpose |
|-------|---------|---------|
| `election_timeout` | 10 s | Maximum time to wait for an election to complete |
| `heartbeat_interval` | 1 s | Interval between master heartbeat messages |
| `replica_ack_timeout` | 5 s | How long the master waits for a replica ack |
| `feeder_timeout` | 30 s | How long the master waits for a feeder response |

```rust
use std::time::Duration;

let config = RepConfig::builder("my_group", "node1", "10.0.0.1")
    .node_port(5001)
    .election_timeout(Duration::from_secs(15))
    .heartbeat_interval(Duration::from_millis(500))
    .replica_ack_timeout(Duration::from_secs(10))
    .feeder_timeout(Duration::from_secs(60))
    .build();
```

### Consistency and Durability Defaults

Default consistency and durability policies can be set on the config and will apply
to all operations that do not specify their own policy:

```rust
use noxu_rep::{RepConfig, ConsistencyPolicy, CommitDurability, ReplicaAckPolicy};
use std::time::Duration;

let config = RepConfig::builder("my_group", "node1", "10.0.0.1")
    .node_port(5001)
    .consistency_policy(ConsistencyPolicy::TimeConsistency {
        max_lag: Duration::from_millis(500),
        timeout: Duration::from_secs(5),
    })
    .commit_durability(CommitDurability::new(
        ReplicaAckPolicy::SimpleMajority,
        Duration::from_secs(5),
    ))
    .build();
```

---

## 4. Starting a Replicated Environment

### Creating the Environment

```rust
use noxu_rep::{ReplicatedEnvironment, RepConfig};

let config = RepConfig::builder("my_group", "node1", "10.0.0.1")
    .node_port(5001)
    .build();

let rep_env = ReplicatedEnvironment::new(config)?;
```

When `new()` returns, the node has joined the replication group. Its resulting state
depends on the group's current state:

- **First electable node in the group:** The node becomes the master of a
  singleton group.
- **Subsequent node, master is available:** The node joins as a replica.
- **No master is available:** The node initiates an election. If a simple majority
  of electable nodes is available, a master is elected.

### Determining the Current Role

```rust
match rep_env.get_state() {
    NodeState::Master  => { /* accept reads and writes */ }
    NodeState::Replica => { /* accept reads only, redirect writes */ }
    NodeState::Unknown => { /* election in progress; reads may be available */ }
    NodeState::Detached | NodeState::Shutdown => { /* environment is closed */ }
}

// Convenience helpers:
if rep_env.is_master()  { /* ... */ }
if rep_env.is_replica() { /* ... */ }
if rep_env.is_active()  { /* Master, Replica, or Unknown */ }
```

### Group Startup Sequence (New Group)

```
Node 1 starts  ->  No master found  ->  Singleton group  ->  MASTER
Node 2 starts  ->  Contacts node1 (helper)  ->  Joins as REPLICA
Node 3 starts  ->  Contacts node1 or node2  ->  Joins as REPLICA
```

### Subsequent Restarts

When a known group member restarts, it queries its known peers to find the current
master. If a master is found, it joins as a replica. If no master is found, it
participates in a new election.

### Closing the Environment

```rust
// Close this node only:
rep_env.close()?;

// Close this node and signal all replicas to shut down (master only):
rep_env.shutdown_group(5000)?;  // 5000ms timeout for replicas to catch up
```

When the master closes, the remaining electable replicas hold a new election.

---

## 5. State Changes

### Node State Machine

A node's state follows this transition graph:

```
Detached -> Unknown -> Master  \
                    -> Replica  +-> Unknown -> ... -> Shutdown
                                |
              Master  -> Replica (direct transition allowed)
              Replica -> Master  (direct transition allowed)
```

The complete set of valid transitions:

| From | To |
|------|----|
| Detached | Unknown, Shutdown |
| Unknown | Master, Replica, Shutdown |
| Master | Unknown, Replica, Shutdown |
| Replica | Unknown, Master, Shutdown |
| Shutdown | (none) |

In a well-functioning group, the Unknown state is transitory: the node quickly
resolves to Master or Replica. A node in Unknown state can still serve read
operations if it can satisfy its consistency requirements.

### StateChangeListener

Register a `StateChangeListener` to be notified asynchronously of state changes.
This is the recommended way to manage write routing in your application.

```rust
use std::sync::Arc;
use noxu_rep::{StateChangeListener, StateChangeEvent, NodeState};

struct MyRouter {
    // e.g. an AtomicBool or channel sender
}

impl StateChangeListener for MyRouter {
    fn on_state_change(&self, event: StateChangeEvent) {
        // Keep implementations minimal — queue heavy work elsewhere.
        match event.new_state {
            NodeState::Master => {
                // This node can now accept writes.
                // event.master_name == Some(this_node_name)
            }
            NodeState::Replica => {
                // Route writes to the master.
                // event.master_name identifies the current master.
                if let Some(master) = event.get_master_node_name() {
                    // update routing table to forward writes to `master`
                }
            }
            NodeState::Unknown => {
                // No master known; queue or reject writes.
            }
            NodeState::Shutdown => {
                // Environment is closing.
            }
            _ => {}
        }
    }
}

let router = Arc::new(MyRouter { /* ... */ });

// Registering triggers an immediate callback with the current state,
// so the listener is always initialized correctly.
rep_env.set_state_change_listener(router);
```

`StateChangeEvent` fields:

- `old_state`: the state before the transition
- `new_state`: the state after the transition
- `master_name`: `Some(name)` when in Master or Replica state; `None` otherwise
- `timestamp`: when the event occurred

### Handling Writes at a Replica

Replicas must not accept write transactions. When your application receives a write
request while in Replica state, it should redirect the request to the master node.

If a write is attempted on a replica, the operation will fail. The `StateChangeEvent`
includes the master's node name so your routing layer can construct the correct
connection target.

### Manual State Transitions (Internal)

These are primarily used by the election and replication subsystems, but are
available for testing and custom orchestration:

```rust
// Transition to master (after winning an election):
rep_env.become_master(term)?;

// Transition to replica (after losing election or on startup):
rep_env.become_replica("node1")?;
```

---

## 6. Elections

### Overview

Elections are held when:

- No master is known and a quorum of electable nodes is available.
- The current master becomes unreachable by a majority of electable nodes.

A node wins an election by receiving a simple majority of votes from electable
nodes. The node with the most up-to-date log (highest VLSN) wins. In a tie, the
election system makes a consistent, deterministic choice.

Once elected, a master retains its role until it becomes unavailable to the group.
There are no periodic re-elections.

### Quorum Requirement

Elections require a **simple majority** of the current electable group size:

| Electable nodes | Required for quorum |
|-----------------|---------------------|
| 1 | 1 |
| 2 | 2 |
| 3 | 2 |
| 4 | 3 |
| 5 | 3 |

If fewer than a majority of electable nodes are reachable, no election can be
held and the group becomes unavailable for writes.

**Important:** An electable node that has joined the group remains in the group
even when it is shut down. A long-term shutdown of electable nodes without removing
them from the group will reduce the effective number of nodes available for quorum.

### Election Configuration

```rust
use noxu_rep::elections::ElectionConfig;
use std::time::Duration;

let election_cfg = ElectionConfig::builder()
    .election_timeout(Duration::from_secs(15))  // wait before giving up
    .max_retries(3)                              // retry limit before Unknown
    .priority(10)                               // higher = preferred as master
    .build();
```

**Priority:** A node with a higher priority is preferred as master when VLSN counts
are equal. Setting priority to `0` means the node will never volunteer as master
(but can still vote).

### Election State Machine

Each election round progresses through:

```
Idle -> Proposing -> Voting -> Complete (Won or Lost)
                            -> Failed (NoQuorum or Timeout)
```

After a `Failed` outcome the election is reset and retried (up to `max_retries`
times). After `max_retries` failures the node transitions to Unknown state and
continues attempting to locate a master.

### Election Outcomes

```rust
use noxu_rep::elections::{ElectionOutcome};

match election.get_outcome() {
    Some(ElectionOutcome::Won  { master, term }) => { /* this node is master */ }
    Some(ElectionOutcome::Lost { master, term }) => { /* master is `master` */ }
    Some(ElectionOutcome::NoQuorum { votes_received, votes_needed }) => { /* retry */ }
    Some(ElectionOutcome::Timeout)               => { /* retry */ }
    None => { /* election still in progress */ }
}
```

### Proposal Evaluation

When a node receives a proposal from another candidate, it votes yes if and only if
the candidate's VLSN is strictly higher than the voter's own candidate VLSN. Ties
go to the existing proposal (no vote).

### Master Transfer

The master can hand off its role to a specific replica without holding a full
election:

```rust
use noxu_rep::MasterTransferConfig;
use std::time::Duration;

let transfer = MasterTransferConfig::new(
    "replica2".to_string(),
    Duration::from_secs(30),
);
rep_env.transfer_master(transfer)?;
```

This ensures the target replica has caught up before the transfer completes, so no
data is lost.

**Current gap:** `transfer_master` records the intent but does not yet coordinate
with the replica over the network.

---

## 7. Consistency Policies

A consistency policy determines how current a replica must be before it can serve a
read operation. Policies are expressed as a `ConsistencyPolicy` enum variant.

### NoConsistency (default)

The replica may be arbitrarily far behind the master. Reads always succeed
immediately. This is the highest-availability, lowest-consistency option.

```rust
use noxu_rep::ConsistencyPolicy;

let policy = ConsistencyPolicy::NoConsistency;
```

Use this when:
- Stale reads are acceptable (e.g. analytics, caching).
- You need the replica to be always available regardless of replication lag.

### TimeConsistency

The replica must be within `max_lag` of the master's commit point (measured by
VLSN delta as a proxy for time). If the replica is lagging, it will wait up to
`timeout` to catch up.

```rust
use noxu_rep::ConsistencyPolicy;
use std::time::Duration;

let policy = ConsistencyPolicy::TimeConsistency {
    max_lag: Duration::from_millis(500),  // acceptable lag
    timeout: Duration::from_secs(5),     // wait at most 5s to catch up
};
```

Use this when:
- Read operations need to see data that is at most N milliseconds old.
- Some delay is acceptable but unbounded staleness is not.

If the replica cannot meet the policy within `timeout`, `RepError::ReplicaLagExceeded`
is returned.

### CommitPointConsistency

The replica must have applied up to a specific VLSN before the read is allowed.

```rust
use noxu_rep::ConsistencyPolicy;
use std::time::Duration;

// After a write that returned VLSN 12345, ensure reads see that write:
let policy = ConsistencyPolicy::CommitPointConsistency {
    vlsn: 12345,
    timeout: Duration::from_secs(10),
};
```

Use this when:
- A client that performed a write must immediately read its own write from any node.
- A specific transaction's results must be visible before proceeding.

If the replica cannot reach the target VLSN within `timeout`,
`RepError::ConsistencyTimeout` is returned.

### Checking Consistency Programmatically

```rust
let current_vlsn: i64 = rep_env.get_current_vlsn() as i64;
let master_vlsn:  i64 = /* obtained from master heartbeat */ 0;

match policy.check_consistency(current_vlsn, master_vlsn) {
    Ok(true)  => { /* proceed with read */ }
    Ok(false) => unreachable!(),
    Err(e)    => { /* handle RepError::ReplicaLagExceeded or ConsistencyTimeout */ }
}
```

### Consistency vs. Read Availability Trade-off

| Policy | Read availability | Staleness |
|--------|-------------------|-----------|
| `NoConsistency` | Always available | Unbounded |
| `TimeConsistency` | Available unless lagging | Bounded by `max_lag` |
| `CommitPointConsistency` | May block until VLSN reached | Zero for that point |

A replica that is catching up after downtime, or one under heavy read load, may
fail to meet tighter consistency requirements. Design your application to fall back
gracefully (e.g., route to the master for that request) when consistency cannot be
met.

---

## 8. Commit Durability

### Overview

Durability in a replicated environment has two axes:

1. **Local durability** — whether the commit was flushed to the master's disk.
2. **Replica durability** — how many replicas acknowledged the commit before the
   master returned to the application.

The `CommitDurability` type controls replica acknowledgment.

### ReplicaAckPolicy

```rust
use noxu_rep::ReplicaAckPolicy;

// All electable replicas must acknowledge before commit returns.
ReplicaAckPolicy::All

// A simple majority of electable nodes (including master) must acknowledge.
// This is the default.
ReplicaAckPolicy::SimpleMajority

// No replica acknowledgment required; commit returns as soon as master writes.
ReplicaAckPolicy::None
```

### CommitDurability Configuration

```rust
use noxu_rep::{CommitDurability, ReplicaAckPolicy};
use std::time::Duration;

let durability = CommitDurability::new(
    ReplicaAckPolicy::SimpleMajority,
    Duration::from_secs(5),  // wait at most 5s for acks
);
```

### Required Acknowledgments by Policy

For a group with N electable nodes:

| Policy | Acks required (replicas only, not counting master) |
|--------|----------------------------------------------------|
| `All` | N - 1 |
| `SimpleMajority` (N=3) | 1 |
| `SimpleMajority` (N=5) | 2 |
| `None` | 0 |

### Durability vs. Write Availability Trade-off

A stricter ack policy improves durability guarantees but reduces write availability:

- `ReplicaAckPolicy::None`: writes always succeed at the master regardless of
  replica state.
- `ReplicaAckPolicy::SimpleMajority`: writes fail if fewer than a majority of
  replicas are reachable or too far behind.
- `ReplicaAckPolicy::All`: writes fail if any electable replica is unavailable.

Choose the policy that matches your recovery-point objective (RPO) and
write-availability requirements.

### Ack Tracking

The master tracks acknowledgments per VLSN per replica:

```rust
// Record an ack from a replica (called by the feeder on receiving an ack message):
rep_env.record_ack(vlsn, "replica2");

// Check if ack requirements are satisfied for a given VLSN:
let satisfied = rep_env.get_ack_tracker().is_satisfied(vlsn);
```

---

## 9. Secondary (Read-Only) Nodes

Secondary nodes provide additional read capacity without affecting elections or
quorum calculations. They are useful for:

- **Geographic read scaling:** Place secondaries near readers on high-latency links.
  They do not slow down elections because they are not counted in quorum.
- **Analytics workloads:** Offload expensive read queries to dedicated nodes without
  impacting master or electable replica performance.
- **Reporting replicas:** Keep an isolated, queryable copy of the data.

### Characteristics

- Stores a full copy of the replicated data.
- Receives the replication stream from the master (same as electable replicas).
- Cannot become master.
- Does not participate in elections.
- Does not contribute to transaction commit acknowledgments.
- Is **not** a persistent member of the group: when a secondary disconnects from
  the master it is no longer considered a group member.
- Secondary nodes do not affect the quorum size for elections.

### Configuration

```rust
use noxu_rep::{RepConfig, NodeType};

let config = RepConfig::builder("my_group", "secondary-us-west", "10.2.0.1")
    .node_port(5001)
    .node_type(NodeType::Secondary)
    .add_helper_host("10.0.0.1:5001".to_string())
    .build();

let rep_env = ReplicatedEnvironment::new(config)?;
```

### Operational Notes

- A secondary can use any consistency policy for its reads.
- If the secondary falls too far behind, it may need to perform a network restore
  (full copy from another node) before it can resume streaming.
- Because secondaries are not persistent members, you can add or remove them freely
  without affecting the quorum size for the remaining electable nodes.

---

## 10. Monitor Nodes

Monitor nodes observe the replication group without storing data or participating in
elections. They are intended for external services that need to route database
requests to the appropriate group member (e.g., a load balancer or proxy layer).

### Characteristics

- Does not store a database environment.
- Does not participate in elections.
- Does not contribute to quorum.
- Receives group membership change notifications (new master elected, nodes
  joining/leaving).
- Is a **persistent** member of the replication group.

### Use Cases

- A proxy that routes read requests to the nearest up-to-date replica.
- A monitoring daemon that alerts on master failover.
- An application-layer connection pool manager.

### Configuration

```rust
use noxu_rep::{RepConfig, NodeType};

let config = RepConfig::builder("my_group", "monitor1", "10.9.0.1")
    .node_port(5001)
    .node_type(NodeType::Monitor)
    .add_helper_host("10.0.0.1:5001".to_string())
    .build();
```

A monitor node registers a `StateChangeListener` to receive notifications:

```rust
use noxu_rep::{StateChangeListener, StateChangeEvent, NodeState};
use std::sync::Arc;

struct ProxyRouter { /* connection pool, etc. */ }

impl StateChangeListener for ProxyRouter {
    fn on_state_change(&self, event: StateChangeEvent) {
        if let Some(master) = event.get_master_node_name() {
            // Update routing table: all writes go to `master`
        }
    }
}

rep_env.set_state_change_listener(Arc::new(ProxyRouter { /* ... */ }));
```

### Notes

Because monitor nodes are persistent members, they must be explicitly removed from
the group when decommissioned (same procedure as electable nodes). Failing to remove
a defunct monitor node does not affect quorum (monitors are not counted), but it
does leave stale membership records.

---

## 11. Network Partitions and Split-Brain

### Split-Brain Risk

The most dangerous failure mode in a replicated system is **split-brain**: a network
partition causes two subsets of the group to each believe they are the sole active
partition, both electing a master. If both masters accept writes, the data diverges
and at least one side must discard its writes when the partition heals.

Noxu DB prevents split-brain through the quorum requirement: **an election can only
succeed if a simple majority of electable nodes participates**. This ensures at most
one partition can elect a master, because only one partition can have a majority.

### Behavior During a Partition

If a partition isolates the master from a majority of electable nodes:

1. The isolated master detects that replicas have become unreachable.
2. If using `ReplicaAckPolicy::SimpleMajority` or `All`, write transactions begin
   to fail (insufficient acks).
3. The majority partition holds an election and elects a new master.
4. The old master, unable to receive acks, should eventually stop accepting writes.

The minority partition (including the old master) becomes read-only for its
remaining nodes.

### Partition Healing

When the network partition heals:

1. Nodes on the minority side rejoin the group and discover the new master.
2. They transition to Replica state and receive the replication stream from the
   new master to catch up.
3. Any writes that were accepted only by the old master (if it somehow continued
   accepting writes) are lost.

### Availability vs. Consistency

The quorum requirement is a deliberate trade-off: it sacrifices write availability
(writes fail when a majority is unavailable) in exchange for consistency (no
split-brain). You can relax this trade-off only in the two-node case via the
designated-primary mechanism (see section 12).

---

## 12. Two-Node Groups

### The Two-Node Problem

A two-node group is especially vulnerable: the loss of either node means the
remaining node has only 1 of 2 votes — not a majority. The group cannot elect a
master and becomes unavailable for writes.

### Designated Primary

For two-node groups where continued availability is more important than strict
durability, you can designate one node as the **primary**. The primary can
self-elect as master with only its own vote when the non-primary is unavailable.

This is configured via `ElectionConfig::designated_primary`:

```rust
use noxu_rep::elections::ElectionConfig;
use std::time::Duration;

// On the designated primary node:
let election_cfg = ElectionConfig::builder()
    .designated_primary(true)
    .election_timeout(Duration::from_secs(10))
    .max_retries(3)
    .build();
```

The primary is said to be **active** when it is operating as master without the
non-primary. At that point:

- The effective quorum drops from 2 to 1.
- Durability policies that require replica acks (e.g. `SimpleMajority`) behave as
  if the non-primary is not present.
- The application takes on the risk that if both nodes diverge, at least one must
  discard writes.

### Critical Warning: Only One Primary

**Never designate both nodes as primary simultaneously.** If both nodes are
designated primary and they cannot communicate (e.g. during a network partition),
both will self-elect as master and accept writes independently, creating a
split-brain condition. Reconciling such a split requires data loss on at least one
node.

If the primary node fails, you can safely swap the primary designation to the
surviving node — but only after confirming the failed node is genuinely offline.

### When to Use Two-Node Groups

Two-node groups are appropriate when:
- You have only two machines but need continued availability.
- You accept the risk of reduced durability when the non-primary is absent.
- Writes during non-primary unavailability are idempotent or easily reconciled.

For stronger guarantees, prefer a three-node group. The third node adds a tiebreaker
and allows the group to tolerate one failure without any special configuration.

---

## 13. Adding and Removing Nodes

### Adding a Node

Adding a new electable node is done simply by starting it with the group name and a
helper host that points to an active group member:

```rust
let config = RepConfig::builder("my_group", "node4", "10.0.0.4")
    .node_port(5001)
    .add_helper_host("10.0.0.1:5001".to_string())
    .build();

let rep_env = ReplicatedEnvironment::new(config)?;
```

Preconditions:
- A simple majority of electable nodes must be active at the time the new node
  joins. The master must be reachable to register the new member persistently.
- Once registered, the new node is a permanent member of the group and is counted
  in future quorum calculations.

The master then streams the replication log to the new node to bring it up to date.
This is the **initial network restore** phase; it may take time if the master has a
large amount of data.

### Adding Secondary or Monitor Nodes

Secondary and monitor nodes can be added at any time without affecting quorum.
They use the same startup process but with the appropriate `NodeType`.

### Removing a Node

Removing an electable node requires careful coordination because the group size
(and therefore quorum requirements) decreases:

1. **Shut down the node first** (call `rep_env.close()`).
2. **Initiate removal** from a node that has contact with a majority.
3. A majority of active electable nodes must acknowledge the removal.

After removal, the node's name must not be reused for an electable node. If the
node is later restarted and needs to rejoin the group, it must be given a new unique
name.

**Why this matters:** An electable node that has been added to the group but then
shut down for a long time continues to count toward the quorum requirement. If
enough such "ghost" nodes accumulate, the group may be unable to elect a master
even if all currently-running nodes are healthy. Remove nodes you intend to keep
offline for extended periods.

### Long-term Offline Nodes

If you cannot remove an offline node (e.g., because you also lack a majority to
perform the removal), you have a majority-failure scenario. See the `groupreset`
utility and the managing-majority-failure appendix of the original JE guide for
recovery procedures.

---

## 14. Backups in a Replicated Environment

### Replication as Continuous Backup

Replication naturally provides real-time incremental backup: every committed write
on the master is replicated to N-1 other data nodes. For each write operation you
get an immediate copy on every active replica.

This significantly reduces the need for traditional backup operations. However, it
does not replace offline backups, because:

- Replication propagates logical changes, including accidental deletions.
- Node failures that corrupt the database (e.g., disk errors) may propagate to
  replicas before detection.
- An offline backup provides a recoverable snapshot for disaster recovery.

### Full Backups

For a full backup of a replicated environment:

1. Choose a node that is **current** (not lagging):
   - The master is always current.
   - An electable replica that must acknowledge commits before they return is
     always current.
   - A secondary node may lag; verify its VLSN matches the master before using it
     for backup.

2. Use `NetworkRestore` or the `DbBackup` equivalent to copy the environment
   directory.

3. If using a replica for backup, ensure it is not lagging:

```rust
// Check how far behind this replica is:
let our_vlsn    = rep_env.get_current_vlsn();
let master_vlsn = /* obtained via group metadata or heartbeat */;
let lag = master_vlsn.saturating_sub(our_vlsn);
if lag == 0 {
    // Safe to backup from this node
}
```

### Network Restore

If a node's environment is too far behind or corrupt, it can restore its environment
from another node:

```rust
use noxu_rep::{NetworkRestore, NetworkRestoreConfig};

let restore_config = NetworkRestoreConfig::new(
    "10.0.0.1:5001".to_string(), // source node
);
let restore = NetworkRestore::new(restore_config);
restore.restore()?;
```

After a network restore, the node rejoins the group and catches up via normal
replication streaming.

**Current gap:** `NetworkRestore` has the data structures and state machine but
does not yet perform a live file transfer.

### Backup Recommendations

- Take full offline backups from the master or a fully-caught-up replica.
- Schedule backups to avoid peak write periods to minimize the window of log files
  that must remain on disk during backup.
- Test restore procedures regularly — a backup you have never restored is
  unverified.

---

## 15. Performance Considerations

### Election Tuning

Election timeouts directly affect failover time:

- **Shorter `election_timeout`**: faster failover, but transient network hiccups
  may trigger unnecessary elections.
- **Shorter `heartbeat_interval`**: faster detection of master loss, but more
  network traffic.

For a typical LAN deployment, an `election_timeout` of 5–10 seconds and a
`heartbeat_interval` of 500ms–1s is a reasonable starting point.

### Durability vs. Throughput

`ReplicaAckPolicy::None` gives the highest write throughput because commits do not
wait for network round-trips. The trade-off is that up to one round-trip's worth of
committed transactions may be lost if the master fails before replicas receive them.

`ReplicaAckPolicy::SimpleMajority` is the best balance for most workloads: it
survives the loss of a minority of nodes without data loss and adds only one
network round-trip to the commit path.

`ReplicaAckPolicy::All` is the most durable option but will fail writes whenever
any electable replica is unavailable.

### Replica Lag

Replicas can lag when:

- The master is under a very high write load.
- The replica is under heavy read load (CPU and I/O contention).
- Network bandwidth between master and replica is saturated.
- A replica has been offline and is catching up.

Monitor lag via `Feeder::get_lag()` (from the master's perspective) or by comparing
`get_current_vlsn()` on the replica to the master's VLSN.

A lagging replica:
- Cannot meet strict consistency policies, causing read operations to block or fail.
- May delay commit acknowledgments, reducing write throughput if your ack policy
  requires it.

### Secondary Nodes and Latency

Secondary nodes are an effective way to serve reads from geographically distributed
locations without adding latency to elections. Because secondaries are not counted
in quorum, placing them on high-latency links does not slow down master failover.

### Read Scaling

Distribute read traffic across replicas according to their consistency policy
compliance. For workloads that tolerate some staleness, round-robin across all
replicas including secondaries. For strong-consistency reads, send to the master
or to a replica that has acknowledged recent commits.

### Subscription API

For external consumers that need a filtered or transformed view of the replication
stream (e.g., CDC pipelines, search index updates), use the subscription API
instead of connecting as a full replica:

```rust
use noxu_rep::{Subscription, SubscriptionConfig, SubscriptionCallback};

struct MyConsumer;

impl SubscriptionCallback for MyConsumer {
    fn on_entry(&self, vlsn: u64, entry_type: u8, data: &[u8]) {
        // process the replicated log entry
    }
}

let sub_config = SubscriptionConfig::default();
let _sub = Subscription::new(sub_config, Box::new(MyConsumer));
```

**Current gap:** `Subscription` has the interface and state machine, but the live
stream connection to the master's feeder is not yet wired.

---

## Current Implementation Gaps Summary

The following items are structurally present in `noxu-rep` but not yet fully wired
to live network I/O:

| Feature | Status |
|---------|--------|
| `FeederRunner::run()` | Implemented and unit-tested; not connected to TCP |
| `ReplicaStream` receive loop | Implemented and unit-tested; not connected to TCP |
| `transfer_master` network coordination | Intent recorded; network round-trip not implemented |
| `NetworkRestore` file transfer | State machine complete; file copy not implemented |
| `Subscription` stream connection | Interface complete; not connected to master feeder |
| TCP channel bind/accept | `ServiceDispatcher` and `DataChannel` exist; not started at environment open |

These gaps do not affect the correctness of the data structures, state machines,
election logic, VLSN tracking, consistency policy checking, or commit durability
bookkeeping — all of which are fully implemented and covered by tests.

---

## See Also

- `crates/noxu-rep/src/replicated_environment.rs` — `ReplicatedEnvironment` API
- `crates/noxu-rep/src/rep_config.rs` — `RepConfig` and `RepConfigBuilder`
- `crates/noxu-rep/src/elections/` — election state machine, config, Paxos
- `crates/noxu-rep/src/vlsn/` — VLSN index, range, and bucket
- `crates/noxu-rep/src/stream/` — feeder and replica stream
- `crates/noxu-rep/src/consistency.rs` — `ConsistencyPolicy`
- `crates/noxu-rep/src/commit_durability.rs` — `CommitDurability`, `ReplicaAckPolicy`
- `crates/noxu-rep/src/node_type.rs` — `NodeType`
- `crates/noxu-rep/src/node_state.rs` — `NodeState`, `NodeStateMachine`
- `crates/noxu-rep/src/state_change_listener.rs` — `StateChangeListener`, `StateChangeEvent`
- `_/je/docs/ReplicationGuide/` — original BDB JE Replication Guide HTML source
