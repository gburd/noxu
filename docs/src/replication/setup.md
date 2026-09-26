# Setup and Configuration

> **v2.0 status — GA.** All ten noxu-rep GA blockers identified in the
> May 2026 audit are closed in v2.0 (Waves 3-3 and 4-A). See
> Wave 4-A report for
> per-finding resolution notes.

This page covers how to configure and start a Noxu DB replicated environment.

## Dependencies

Enable the `replication` feature in your `Cargo.toml`:

```toml
[dependencies]
noxu = { version = "7", features = ["replication"] }
# For QUIC transport:
# noxu = { version = "7", features = ["replication"] }  # QUIC is bundled with replication
```

## Group Topology

A replication group consists of:

- **One master** — accepts all writes, feeds log to replicas
- **Zero or more replicas** — receive the log stream, serve reads

The minimum group size for fault tolerance is **3 nodes** (tolerates 1 failure).
A 5-node group tolerates 2 failures.

## RepConfig

Configure the replicated environment via `RepConfig::builder`. The builder
takes the **group name, node name, and node host** as three positional
arguments; everything else is a chained setter:

```rust
use noxu::replication::{
    CommitDurability, NodeType, QuorumPolicy, RepConfig, RepNode,
    ReplicaAckPolicy,
};
use std::time::Duration;

let rep_config = RepConfig::builder("prod-cluster", "node-1", "192.168.1.10")
    .node_port(14_001)
    // Election priority (JE NODE_PRIORITY). Higher = preferred as master on
    // equal progress; 0 = electable but never chosen. Default is 1.
    .node_priority(1)
    .election_phase_timeout(Duration::from_millis(500))
    // `phi_threshold` is `Option<f64>`: `Some(8.0)` turns ON phi-accrual
    // failure detection (the paper's recommended value); the default is
    // `None`, i.e. a binary heartbeat timeout.
    .phi_threshold(Some(8.0))
    .phi_window_size(200)
    .quorum_policy(QuorumPolicy::SimpleMajority)
    // Commit-side durability is a `CommitDurability` (ack policy + ack
    // timeout), not top-level RepConfig fields.
    .commit_durability(CommitDurability::new(
        ReplicaAckPolicy::SimpleMajority,
        Duration::from_secs(5),
    ))
    // `env_home` is where this node's `.ndb` files live; set it so the node
    // can serve a network restore to peers.
    .env_home("./data")
    // Peers are added one at a time. `RepNode::new` takes
    // (name, node_type, host, port, node_id).
    .add_initial_peer(RepNode::new(
        "node-2".to_string(),
        NodeType::Electable,
        "192.168.1.11".to_string(),
        14_001,
        2,
    ))
    .add_initial_peer(RepNode::new(
        "node-3".to_string(),
        NodeType::Electable,
        "192.168.1.12".to_string(),
        14_001,
        3,
    ))
    // Trusted-network / dev / CI opt-out of the enforced-mTLS default. Omit
    // this in production and configure `transport_kind(RepTransportKind::Tls)`
    // + `tls_config(..)` + `peer_allowlist(..)` instead.
    .insecure_no_auth(true)
    .build();
```

## ReplicatedEnvironment

`ReplicatedEnvironment::new` takes the `RepConfig` alone and constructs the
underlying `Environment` internally from `env_home` — you do **not** open an
`Environment` first and wrap it:

```rust
use noxu::replication::ReplicatedEnvironment;

let rep_env = ReplicatedEnvironment::new(rep_config)?;

// After construction, the node participates in elections.
// Check whether this node won master:
if rep_env.is_master() {
    println!("This node is master");
} else {
    println!("This node is replica");
}
```

## Key RepConfig Parameters

| Parameter | Default | Description |
|---|---|---|
| `group_name` (builder arg) | required | Replication group identifier |
| `node_name` (builder arg) | required | Unique name within the group |
| `node_host` (builder arg) | required | Hostname / IP for this node |
| `node_port` | `14001` | Replication port (override in production) |
| `node_type` | `Electable` | Node role |
| `node_priority` | `1` | Election priority (JE `NODE_PRIORITY`). Higher is preferred as master on equal progress; `0` = electable but never chosen. Runtime-mutable via `ReplicatedEnvironment::set_node_priority`. See [Leader Elections](./elections.md#node-priority-steering-mastership). |
| `election_phase_timeout` | 500 ms | FPaxos per-phase message timeout |
| `phi_threshold` | `None` (binary heartbeat) | `Some(8.0)` enables phi-accrual detection (Hayashibara 2004) |
| `phi_window_size` | `200` | Phi-accrual inter-arrival samples (use `1000` for WAN) |
| `quorum_policy` | `SimpleMajority` | Quorum strategy |
| `commit_durability` | `CommitDurability::default()` (`ack_timeout` 5 s) | Replica-ack policy + timeout for replicated commits |

## Dynamic Peer Management

Add or remove nodes at runtime without restarting:

```rust
use noxu::replication::{NodeType, RepNode};
use std::time::Duration;

// Add a new node to the group.
rep_env.add_peer(RepNode::new(
    "node-4".to_string(),
    NodeType::Electable,
    "192.168.1.13".to_string(),
    14_001,
    4,
))?;

// Remove a node from the group.
rep_env.remove_peer("node-4")?;

// Update capacity or latency hints for quorum optimization. Capacity is a
// fraction (1.0 = baseline); latency is a `Duration`.
rep_env.update_peer_metadata(
    "node-2",
    RepNode::new(
        "node-2".to_string(),
        NodeType::Electable,
        "192.168.1.11".to_string(),
        14_001,
        2,
    )
    .with_read_capacity(0.8)
    .with_write_capacity(0.6)
    .with_latency_hint(Duration::from_millis(5)),
)?;
```

See [Dynamic Membership](dynamic-membership.md) for details.
