# Consistency Policies

Replica reads can be stale if the replica has not yet applied the latest
entries from the master. A `ConsistencyPolicy` lets an application trade read
freshness for latency by *gating a read* until the replica is fresh enough.

The gate is a `ReplicatedEnvironment`-level call —
`begin_read_consistency(policy_override)` — that blocks until the replica
satisfies the policy (or times out), and is invoked *before* the read. It is
not a per-`Database`, per-key argument. `NoConsistency` never blocks; on the
master (which is by definition current) every policy returns immediately.

The three policy variants are:

```rust
use noxu::replication::ConsistencyPolicy;
use std::time::Duration;

// 1. No consistency requirement — read from whatever state the replica has.
let _no = ConsistencyPolicy::NoConsistency;

// 2. Time consistency — wait until the replica is within `max_lag` of master.
let _time = ConsistencyPolicy::TimeConsistency {
    max_lag: Duration::from_secs(5),
    timeout: Duration::from_secs(10),
};

// 3. Commit-point consistency — wait until a specific VLSN has been applied.
let _cp = ConsistencyPolicy::CommitPointConsistency {
    vlsn: 42,
    timeout: Duration::from_secs(10),
};
```

## No consistency (default)

The replica serves the read from its local state regardless of how far behind
it is. Use when stale reads are acceptable (analytics, search indexes).

```rust
use noxu::replication::ConsistencyPolicy;

let policy = ConsistencyPolicy::NoConsistency;
rep_env.begin_read_consistency(Some(&policy))?;
// ... now perform the read on the underlying database ...
```

## Time consistency

The replica waits until its VLSN is within `max_lag` of the master before the
read proceeds. `begin_read_consistency` returns
`RepError::ReplicaLagExceeded` (or `RepError::ConsistencyTimeout`) if the
replica does not catch up within `timeout`.

```rust
use noxu::replication::ConsistencyPolicy;
use std::time::Duration;

let policy = ConsistencyPolicy::TimeConsistency {
    max_lag: Duration::from_secs(5),
    timeout: Duration::from_secs(10),
};
rep_env.begin_read_consistency(Some(&policy))?;
```

## Commit-point consistency (read-your-writes)

For read-your-writes, mint a `CommitToken` from the master after a write and
pass it to the replica read: `ConsistencyPolicy::commit_point(&token, timeout)`
builds a `CommitPointConsistency` policy that waits until the replica has
replayed past that commit's VLSN.

```rust
use noxu::replication::ConsistencyPolicy;
use std::time::Duration;

// On the master, after committing the write, capture the commit token.
if let Some(token) = master_env.commit_token() {
    // On the replica read path, gate the read on that token.
    let policy = ConsistencyPolicy::commit_point(&token, Duration::from_secs(10));
    rep_env.begin_read_consistency(Some(&policy))?;
    // ... the write is now guaranteed visible on this replica ...
}
```

If you already have a raw VLSN, construct the variant directly:

```rust
use noxu::replication::ConsistencyPolicy;
use std::time::Duration;

let vlsn: i64 = master_env.get_current_vlsn() as i64;
let policy = ConsistencyPolicy::CommitPointConsistency {
    vlsn,
    timeout: Duration::from_secs(10),
};
rep_env.begin_read_consistency(Some(&policy))?;
```

## Replica lag monitoring

Replication statistics are exposed through `ReplicatedEnvironment::get_stats()`,
which returns a `&RepStats`. Its fields are atomics read directly:

```rust
use std::sync::atomic::Ordering;

let stats = rep_env.get_stats();
let max_lag_ms = stats.max_replica_lag_ms.load(Ordering::Relaxed);
let acks = stats.acks_received.load(Ordering::Relaxed);
let ack_timeouts = stats.ack_timeouts.load(Ordering::Relaxed);
let entries_applied = stats.entries_applied.load(Ordering::Relaxed);
```

> **Note:** not every `RepStats` field is populated by the current
> implementation — several counters (including the lag gauge) are not yet
> wired into production paths. Treat `RepStats` as the API surface, and see
> [Known Limitations](../operations/known-limitations.md) for the current
> state of replication observability.
