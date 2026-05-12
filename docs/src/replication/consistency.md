# Consistency Policies

Replica reads can be stale if the replica has not yet applied the latest
entries from the master. Consistency policies let applications trade read
freshness for latency.

## NoConsistencyRequiredPolicy

Default for replica reads. The replica serves the read from its local state
regardless of how far behind it is.

```rust
let policy = ConsistencyPolicy::NoConsistencyRequired;
db.get_with_consistency(txn, key, policy)?;
```

Use when stale reads are acceptable (e.g., analytics, search indexes).

## TimeConsistencyPolicy

The replica waits until its VLSN is within a specified lag of the master.

```rust
let policy = ConsistencyPolicy::Time {
    permissible_lag: Duration::from_secs(5),
    timeout: Duration::from_secs(10),
};
```

Returns `RepError::ConsistencyTimeout` if the replica does not catch up
within `timeout`.

## CommitPointConsistencyPolicy

The replica waits until it has applied a specific VLSN.

```rust
let vlsn = master_env.get_current_vlsn()?;
let policy = ConsistencyPolicy::CommitPoint {
    vlsn,
    timeout: Duration::from_secs(10),
};
```

Use for read-your-writes: after a write on the master, read on a replica with
the commit VLSN to guarantee the write is visible.

## Replica Lag Monitoring

```rust
let stats = rep_env.get_rep_stats()?;
stats.replica_lag_ms()         // current lag in milliseconds
stats.known_master_vlsn()      // last VLSN seen from master
stats.local_vlsn()             // this node's applied VLSN
```
