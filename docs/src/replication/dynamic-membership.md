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
let new_node = RepNode::new("node-4", "192.168.1.14:5001")
    .with_read_capacity_pct(70)
    .with_write_capacity_pct(50)
    .with_latency_hint_ms(3);

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
rep_env.update_peer_metadata("node-2",
    RepNode::new("node-2", "192.168.1.11:5001")
        .with_read_capacity_pct(90)
        .with_write_capacity_pct(80)
        .with_latency_hint_ms(2)
)?;
```

This briefly write-locks the quorum system for rebuild. It is safe to call
while replication streams are active.

## RepNode Fields

| Field | Type | Description |
|---|---|---|
| `name` | `String` | Unique node name |
| `address` | `String` | `host:port` |
| `read_capacity_pct` | `u8` | Relative read capacity 0–100 |
| `write_capacity_pct` | `u8` | Relative write capacity 0–100 |
| `latency_hint_ms` | `u32` | Estimated round-trip latency in ms |

## Quorum Rebuild

When a membership change occurs, `RepGroup::set_quorum_policy()` rebuilds
the `QuorumSystem` from the updated node list. The intersection property
(`phase1_quorum + phase2_quorum > n`) is re-validated after every change.

## Chaos Testing

The `PeerJoin`, `PeerLeave`, `CapacityChange`, `ClusterGrow`, and
`ClusterShrink` chaos phases in `torture_test.rs` exercise dynamic membership
under load. See [Chaos and Soak Testing](../maintainer/chaos-soak-testing.md).
