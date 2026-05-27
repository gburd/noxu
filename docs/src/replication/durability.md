# Replication Durability Policies

> **v2.0 status — GA.** `ReplicaAckPolicy` is honoured on commit
> (Wave 3-3, F1).  `Transaction::commit_with_durability` blocks
> until the configured number of replicas have acknowledged or the
> commit timeout elapses.

In a replicated environment, durability involves both local disk persistence
and replica acknowledgments.

## ReplicaAckPolicy

Controls how many replicas must acknowledge a commit before the master
considers it durable:

| Policy | Acks required | Description |
|---|---|---|
| `None` | 0 | Master does not wait for replicas |
| `SimpleMajority` | (n_replicas/2)+1 | Wait for majority of replicas |
| `All` | n_replicas | Wait for all replicas |

Configure on `RepConfig`:

```rust
RepConfig::builder()
    .replica_ack_policy(ReplicaAckPolicy::SimpleMajority)
    .replica_ack_timeout_ms(5_000)
    // ...
```

Returns `RepError::InsufficientAcks { needed, received }` if the timeout
expires before enough acks arrive.

## Local SyncPolicy

Each node's local log sync policy is configured independently on
`EnvironmentConfig::durability_sync_commit`. For maximum durability in a
replicated environment, combine `SyncPolicy::Sync` on the master with
`ReplicaAckPolicy::SimpleMajority`.

## Group Commit

Group commit (`GroupCommit` in `noxu-txn`) batches concurrent commits into
a single `fsync` call. This is particularly effective under replication where
write fanout causes multiple concurrent transactions to wait for acks at the
same time.

On the master, `GroupCommitMaster` buffers commits until either:

- A batch size threshold is reached, or
- A time deadline expires (default: 1ms)

Then all buffered commits are flushed together in one `fsync`.

## Durability vs. Availability Trade-off

| Setting | Durability | Write latency | Availability |
|---|---|---|---|
| `Sync` + `All` | Maximum | Highest | Lowest (any replica failure blocks writes) |
| `Sync` + `SimpleMajority` | High | Medium | Good |
| `WriteNoSync` + `None` | Moderate | Lowest | Highest |
