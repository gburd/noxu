# Master Transfer

Master transfer moves the master role to a designated replica in a controlled,
non-disruptive way. No data is lost and write downtime is minimised.

## When to Use Master Transfer

- Planned maintenance on the master node
- Rebalancing workloads (move master to a node with lower latency)
- Rolling upgrades (step through each node as master)

## Transfer Process

1. **Drain**: The current master stops accepting new writes and waits for all
   pending transactions to commit.
2. **Sync**: Wait until the designated replica's VLSN equals the master's.
3. **Abdicate**: The master sends an `ABDICATE` message, which triggers an
   election. The designated replica wins (it has the highest VLSN).
4. **Reconnect**: Former master reconnects as a replica.

```rust
rep_env.initiate_master_transfer("node-2", Duration::from_secs(30))?;
```

Returns `RepError::ElectionFailed` if the designated node does not win the
election within the timeout.

## Rolling Restart

To perform a rolling restart of the cluster:

1. Transfer master to node-2: `transfer("node-2", ...)`
2. Restart node-1 (former master)
3. Transfer master back to node-1 (optional): `transfer("node-1", ...)`
4. Restart node-2, node-3 in sequence

Each restart involves at most one election and a brief write pause.
