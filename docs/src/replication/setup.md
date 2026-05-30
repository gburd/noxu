# Setup and Configuration

> **v2.0 status — GA.** All ten noxu-rep GA blockers identified in the
> May 2026 audit are closed in v2.0 (Waves 3-3 and 4-A). See
> [Wave 4-A report](../internal/wave-4-a-rep-ga-finish.md) for
> per-finding resolution notes.

This page covers how to configure and start a Noxu DB replicated environment.

## Dependencies

Enable the `replication` feature in your `Cargo.toml`:

```toml
[dependencies]
noxu = { version = "3", features = ["replication"] }
# For QUIC transport:
# noxu = { version = "3", features = ["replication"] }  # QUIC is bundled with replication
```

## Group Topology

A replication group consists of:

- **One master** — accepts all writes, feeds log to replicas
- **Zero or more replicas** — receive the log stream, serve reads

The minimum group size for fault tolerance is **3 nodes** (tolerates 1 failure).
A 5-node group tolerates 2 failures.

## RepConfig

Configure the replicated environment via `RepConfigBuilder`:

```rust
use noxu::replication::{RepConfig, RepNode, QuorumPolicy};

let rep_config = RepConfig::builder()
    .node_name("node-1")
    .node_address("192.168.1.10:5001")
    .group_name("prod-cluster")
    .election_phase_timeout(Duration::from_millis(500))
    .phi_threshold(8.0)
    .phi_window_size(1000)
    .quorum_policy(QuorumPolicy::SimpleMajority)
    .initial_peers(vec![
        RepNode::new("node-2", "192.168.1.11:5001"),
        RepNode::new("node-3", "192.168.1.12:5001"),
    ])
    .build()?;
```

## ReplicatedEnvironment

Open a replicated environment by wrapping a normal `Environment`:

```rust
use noxu::replication::ReplicatedEnvironment;

let env = Environment::open(Path::new("./data"), EnvironmentConfig::default())?;
let rep_env = ReplicatedEnvironment::new(env, rep_config)?;

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
| `node_name` | required | Unique name within the group |
| `node_address` | required | `host:port` for this node |
| `group_name` | required | Replication group identifier |
| `election_phase_timeout` | 500ms | FPaxos phase timeout (adaptive if phi detector has data) |
| `phi_threshold` | 8.0 | Phi suspicion threshold (Hayashibara 2004 recommends 8.0) |
| `phi_window_size` | 1000 | Heartbeat samples in sliding window |
| `quorum_policy` | `SimpleMajority` | Quorum strategy |
| `durability_sync_write` | `WriteNoSync` | Log sync policy |
| `replica_ack_timeout_ms` | 5000 | Timeout waiting for replica acks |

## Dynamic Peer Management

Add or remove nodes at runtime without restarting:

```rust
// Add a new node to the group
rep_env.add_peer(RepNode::new("node-4", "192.168.1.13:5001"))?;

// Remove a node from the group
rep_env.remove_peer("node-4")?;

// Update capacity or latency hints for quorum optimization
rep_env.update_peer_metadata("node-2", RepNode::new("node-2", "192.168.1.11:5001")
    .with_read_capacity_pct(80)
    .with_write_capacity_pct(60)
    .with_latency_hint_ms(5)
)?;
```

See [Dynamic Membership](dynamic-membership.md) for details.
