# Master Transfer

Master transfer moves the master role to a designated replica in a controlled,
non-disruptive way. No committed data is lost: the transfer only completes to a
target that has **caught up** to the current master's VLSN.

## When to Use Master Transfer

- Planned maintenance on the master node
- Rebalancing workloads (move master to a node with lower latency)
- Rolling upgrades (step through each node as master)

## Transfer Process

1. **Verify state**: The call must run on the current master.
2. **Wait for catch-up**: The master waits — bounded by the configured
   `timeout` — for the target replica's replicated VLSN to reach the master's
   current VLSN. If the target does not catch up within the timeout, the
   transfer is **refused** (returns an error) and the current master stays
   master. Handing off to a lagging replica would make it master while missing
   the old master's most recent commits, which would then be rolled back when
   the old master rejoins as a replica — loss of committed, acknowledged data.
   This mirrors JE `MasterTransfer`'s `VLSNProgress` / `readyReplicas`
   accounting.
3. **Hand off**: Once the target has caught up, the master signals the target
   (which becomes master at the next term) and notifies the other peers so they
   re-target.
4. **Reconnect**: The former master reconnects as a replica of the new master.

```rust
use noxu_rep::master_transfer::MasterTransferConfig;
use std::time::Duration;

let cfg = MasterTransferConfig::new("node-2".to_string(), Duration::from_secs(30));
rep_env.transfer_master(cfg)?;
```

If the target does not catch up within the timeout, `transfer_master` returns
an error and this node remains master. Set `MasterTransferConfig::with_force`
to skip the catch-up wait and hand off unconditionally — only when the operator
explicitly accepts the risk of losing the master's most recent commits.

> **Observability note.** The catch-up wait tracks the target's progress via
> its active feeder (the same VLSN-progress source `shutdown_group` uses). If
> the target is not being fed over a registered feeder channel, its progress
> cannot be observed and a non-forced transfer is refused rather than handed
> off blind.

## Rolling Restart

To perform a rolling restart of the cluster:

1. Transfer master to node-2, wait for it to catch up and take over.
2. Restart node-1 (former master).
3. Transfer master back to node-1 (optional).
4. Restart node-2, node-3 in sequence.

Each restart involves at most one master handoff and a brief write pause.
