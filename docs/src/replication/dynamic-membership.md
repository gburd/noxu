# Dynamic Membership

> **v2.0 status — GA.** Adding/removing peers via `add_peer` /
> `remove_peer` is fully supported.  When feeder channels are registered via
> `register_feeder_channel`, master promotions automatically spawn a
> `FeederRunner` thread per replica (push path, v3.2.0).  Without registered
> channels, the pull path (`PeerFeederService`) remains the default.

Noxu DB supports adding and removing nodes from the replication group while
the group is actively serving traffic.

## Adding a Node

```rust
use noxu::replication::{NodeType, RepNode};
use std::time::Duration;

// `RepNode::new` takes (name, node_type, host, port, node_id). Capacity
// hints are fractions of the baseline (0.7 = 70%); the latency hint is a
// `Duration`.
let new_node = RepNode::new(
    "node-4".to_string(),
    NodeType::Electable,
    "192.168.1.14".to_string(),
    14_001,
    4,
)
.with_read_capacity(0.7)
.with_write_capacity(0.5)
.with_latency_hint(Duration::from_millis(3));

rep_env.add_peer(new_node)?;
```

`add_peer` registers the node in the group and begins streaming log entries
to it. The new node performs catch-up automatically via the `PeerFeederService`.

## Removing a Node

```rust
rep_env.remove_peer("node-4")?;
```

`remove_peer` removes the node from the group and stops its feeder thread.
Any pending acks from that node are discarded. If removing the node would
leave the group below a fault-tolerant size, a warning is logged.

## Updating Node Metadata

Node capacity and latency hints are used by `QuorumPolicy::Expression` for
LP-optimal quorum selection. Update them at runtime:

```rust
use noxu::replication::{NodeType, RepNode};
use std::time::Duration;

rep_env.update_peer_metadata(
    "node-2",
    RepNode::new(
        "node-2".to_string(),
        NodeType::Electable,
        "192.168.1.11".to_string(),
        14_001,
        2,
    )
    .with_read_capacity(0.9)
    .with_write_capacity(0.8)
    .with_latency_hint(Duration::from_millis(2)),
)?;
```

This briefly write-locks the quorum system for rebuild. It is safe to call
while replication streams are active.

## RepNode Fields

| Field | Type | Description |
|---|---|---|
| `name` | `String` | Unique node name |
| `node_type` | `NodeType` | Role (`Electable` / `Monitor` / `Secondary` / `Arbiter`) |
| `host` | `String` | Hostname or IP address |
| `port` | `u16` | Replication port |
| `node_id` | `u32` | Numeric node identifier |
| `read_capacity_pct` | `u32` | Relative read capacity × 100 (100 = baseline; may exceed 100) |
| `write_capacity_pct` | `u32` | Relative write capacity × 100 (100 = baseline; may exceed 100) |
| `latency_hint_ms` | `u64` | Estimated one-way latency in ms |

The `with_read_capacity(f64)` / `with_write_capacity(f64)` builders take a
**fraction** (e.g. `0.5` for a half-speed node) and store it as
`(cap * 100).round()`; `with_latency_hint(Duration)` takes a `Duration`.

## Quorum Rebuild

When a membership change occurs, `RepGroup::set_quorum_policy()` rebuilds
the `QuorumSystem` from the updated node list. The intersection property
(`phase1_quorum + phase2_quorum > n`) is re-validated after every change.

## Chaos Testing

The `PeerJoin`, `PeerLeave`, `CapacityChange`, `ClusterGrow`, and
`ClusterShrink` chaos phases in `torture_test.rs` exercise dynamic membership
under load. See [Chaos and Soak Testing](../maintainer/chaos-soak-testing.md).
