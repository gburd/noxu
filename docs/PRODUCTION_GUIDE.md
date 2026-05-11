# Noxu DB — Production Operations Guide

This guide covers sizing, monitoring, tuning, replication, recovery, and known
limitations for Noxu DB deployments.

---

## 1. Sizing

### Cache

The in-memory B-tree node cache is the primary performance lever.

| Workload | Recommended `cache_size` |
|----------|--------------------------|
| Hot dataset fits in RAM | 60–80% of available RAM |
| Mixed hot/cold | 30–40% of available RAM |
| Constrained environments | ≥ 2× average working-set size |

Configure at environment open time:

```rust
EnvironmentConfig::new(path)
    .with_cache_size(4 * 1024 * 1024 * 1024)  // 4 GiB
    .with_transactional(true)
```

Use `env.get_stats()?.cache_utilization_percent()` to monitor usage at runtime.
If this value is consistently above 90%, increase the cache or reduce the
working set.

Off-heap memory (for large environments) can be enabled separately:
`set_max_off_heap_memory(bytes)`.

### Log file size

Each log file (`.ndb`) is rolled when it exceeds `log_file_max_bytes`
(default 10 MiB).  Smaller files let the cleaner reclaim space more
aggressively but create more file-handle churn.

```rust
// 64 MiB log files — better for bulk-write workloads
.with_log_file_max_bytes(64 * 1024 * 1024)
```

### Thread pool sizing

Noxu DB uses several background daemon threads.  They run at normal priority
and do not need manual binding.

| Daemon | Count | Notes |
|--------|-------|-------|
| Checkpointer | 1 | wakes on bytes-written or time interval |
| Cleaner | 1 | I/O-bound; consider pinning on NVMe systems |
| Evictor | 1 | CPU-bound when cache is under pressure |
| INCompressor | 1 | low-CPU background task |
| FsyncManager | 1 | coalesces group commits; do not disable |

Application writer threads: recommend keeping ≤ 2× physical CPU cores to avoid
lock-manager contention.  For read-heavy workloads, `TransactionConfig::read_committed()`
readers scale linearly and are not bounded by the above guideline.

---

## 2. Monitoring

Call `env.get_stats()?` periodically (e.g., every 10 s) and export the fields
that matter.  All counters are cumulative since the environment was opened.

### Key fields and alert thresholds

```rust
let s = env.get_stats()?;
```

| Field | Alert condition | Action |
|-------|----------------|--------|
| `s.cache_utilization_percent()` | > 90% | Increase `cache_size` or reduce working set |
| `s.lock.n_waits / s.lock.n_requests` | > 5% | High lock contention; check key distribution or transaction sizes |
| `s.cleaner.runs == 0` after writes | No cleaner activity | Verify cleaner is not disabled; check utilization threshold |
| `s.cleaner.deletions` plateauing | Cleaner not keeping up | Reduce writer rate or lower `cleaner_min_utilization` |
| `s.checkpoint.checkpoints` | Not incrementing | Checkpointer stalled; check disk space and `NoxuError` |
| `s.evictor.nodes_evicted` rate | Consistently high | Cache too small for working set |
| `s.log.n_log_fsyncs` | Very high relative to commits | Group commit not effective; check `with_log_group_commit` config |
| `s.log.n_fsync_batch_size_sum / s.log.n_group_commits` | < 2 | Group commit not batching; verify concurrent writer count |
| `s.txn.n_aborts / s.txn.n_commits` | > 10% | High conflict rate; consider reducing transaction scope |

### Deriving group commit effectiveness

```rust
let avg_batch = if s.log.n_group_commits > 0 {
    s.log.n_fsync_batch_size_sum / s.log.n_group_commits
} else {
    0
};
// avg_batch > 4 means group commit is effectively coalescing I/O
```

### ExceptionListener for async error reporting

Register an `ExceptionListener` to receive callbacks on environment
invalidation events (log corruption, disk full, latch timeout):

```rust
use noxu_db::{ExceptionListener, ExceptionEvent};

struct MyListener;
impl ExceptionListener for MyListener {
    fn exception_thrown(&self, event: &ExceptionEvent) {
        eprintln!("Noxu exception: {:?} — {}", event.source, event.message);
        // alert, log to monitoring system, etc.
    }
}

let config = EnvironmentConfig::new(path)
    .set_exception_listener(Arc::new(MyListener));
```

After an invalidating exception the environment becomes unusable.
`env.is_valid()` returns `false`; all subsequent operations return
`NoxuError::EnvironmentFailure`.  The correct recovery path is to
close the environment and reopen (which triggers WAL replay).

---

## 3. Checkpoint Tuning

The checkpointer writes dirty B-tree nodes to log files and records a
stable recovery point.  More frequent checkpoints reduce recovery time
after a crash at the cost of additional I/O.

### Configuration knobs (via `EnvironmentConfig`)

```rust
// Checkpoint after every 32 MiB written (default: 20 MiB)
.with_checkpointer_bytes_interval(32 * 1024 * 1024)
```

```rust
// Manual checkpoint with force flag (bypasses interval check)
env.checkpoint(Some(CheckpointConfig::new().with_force(true)))?;
```

| `CheckpointConfig` method | Effect |
|--------------------------|--------|
| `.with_force(true)` | Run immediately regardless of bytes/time thresholds |
| `.with_k_bytes(n)` | Only checkpoint if ≥ n KiB have been written since last checkpoint |
| `.with_minutes(n)` | Only checkpoint if ≥ n minutes have elapsed |
| `.with_minimize_recovery_time(true)` | Flush all dirty nodes (expensive; use before planned shutdown) |

### Recommended production settings

- **OLTP workloads**: `checkpointer_bytes_interval = 64 MiB` (default is fine; tighten to 16 MiB if crash recovery must be < 5 s).
- **Bulk load**: disable automatic checkpointing (`set_run_checkpointer(false)`), call `env.checkpoint(...)` manually between batches, re-enable afterwards.
- **Before shutdown**: always call `env.checkpoint(Some(CheckpointConfig::new().with_minimize_recovery_time(true)))` to avoid a full recovery on next open.

---

## 4. Cleaner Tuning

The log cleaner reclaims disk space by copying live records out of
under-utilized log files and then deleting those files.

### Key parameters

```rust
// Only clean files that are < 75% live (default: 50%)
// Lower = less I/O but more disk usage
.with_cleaner_min_utilization(75)
```

```rust
// Disable writer throttling (not recommended for production)
// env config does NOT expose this directly; throttling is automatic
// based on how far behind the cleaner is
```

### Interpreting cleaner stats

```rust
let s = env.get_stats()?;
println!("cleaner runs={}, deletions={}", s.cleaner.runs, s.cleaner.deletions);
println!("reserved_log={} B", s.cleaner.reserved_log_size);
println!("available_log={} B", s.cleaner.available_log_size);
```

If `reserved_log_size` grows without `deletions` increasing, the cleaner is
reading files but cannot delete them (active cursors or open transactions are
pinning log files).  Keep transactions short-lived to unpin files promptly.

### Write throttling

When the cleaner falls behind, it signals writer threads to pause briefly
via `CleanerThrottle`.  This is automatic and transparent.  If you observe
sustained write latency spikes:
1. Check `s.cleaner.runs` — if it is not climbing, the cleaner may be disabled.
2. Lower `cleaner_min_utilization` (e.g., 60) to trigger cleaning sooner.
3. Increase log file size so each file takes longer to fill, giving the cleaner more time.

---

## 5. Replication Setup

Noxu DB uses a Paxos-based replication protocol over TCP.  A replication
group consists of 2N+1 electable nodes (minimum 3 for fault tolerance).

### 3-node topology

```
  ┌─────────────┐     port 5001     ┌─────────────┐
  │  node1      │◄─────────────────►│  node2      │
  │  (master*)  │                   │  (replica)  │
  └──────┬──────┘                   └──────┬──────┘
         │                                 │
         │           port 5001             │
         └──────────────┬──────────────────┘
                        │
                 ┌──────┴──────┐
                 │  node3      │
                 │  (replica)  │
                 └─────────────┘
  * master elected by Paxos; any node may become master after failover
```

### Node configuration

```rust
use noxu_rep::{RepConfig, NodeType};

// Node 1 (bootstrapping member)
let rep_cfg = RepConfig::builder("my-group", "node1", "10.0.0.1")
    .node_port(5001)
    .node_type(NodeType::Electable)
    .add_helper_host("10.0.0.2:5001".to_string())
    .add_helper_host("10.0.0.3:5001".to_string())
    .election_timeout(Duration::from_secs(10))   // default
    .heartbeat_interval(Duration::from_secs(1))  // default
    .replica_ack_timeout(Duration::from_secs(5)) // default
    .feeder_timeout(Duration::from_secs(30))     // default
    .env_home(env_path.clone())                  // enables network restore
    .build();
```

`helper_hosts` is the list of *existing* members the new node contacts to
join the group.  For the very first node, it can be empty or point to
itself.

### Node types

| `NodeType` | Description |
|-----------|-------------|
| `Electable` | Participates in elections and replication; can become master |
| `Secondary` | Receives replication stream but does not vote in elections |
| `Arbiter` | Votes in elections but holds no data; low disk footprint |

### Durability policy

```rust
use noxu_rep::CommitDurability;
use noxu_rep::commit_durability::ReplicaAckPolicy;

// Require all replicas to ack before commit returns (strongest)
RepConfig::builder(...)
    .commit_durability(CommitDurability::new(
        ReplicaAckPolicy::All,
        Duration::from_secs(5),
    ))
    .build()
```

| `ReplicaAckPolicy` | Meaning |
|--------------------|---------|
| `None` | Master does not wait for replicas (fastest, weakest durability) |
| `Simple` (default) | Wait for simple majority |
| `All` | Wait for all electable replicas (slowest, strongest durability) |

### State-change listener

```rust
use noxu_rep::StateChangeListener;

struct MyListener;
impl StateChangeListener for MyListener {
    fn state_change(&self, new_state: ReplicatedEnvironmentState, master: Option<&str>) {
        println!("node transitioned to {:?}, master={:?}", new_state, master);
    }
}
```

Register via `ReplicatedEnvironment::set_state_change_listener(Arc::new(MyListener))`.

---

## 6. Recovery Procedure

### Automatic recovery (normal path)

WAL recovery runs automatically on `Environment::open()`.  No manual steps
are required after a clean or unclean shutdown.  Recovery time is proportional
to the amount of data written since the last checkpoint.

### Manual recovery steps (corrupted environment)

1. **Identify corruption scope** — check logs for `NoxuError::EnvironmentFailure`
   with `EnvironmentFailureReason::LogChecksum` or `BtreeCorruption`.

2. **Stop all writers immediately** — do not attempt further writes once
   corruption is detected; the environment is invalidated and all operations
   return errors.

3. **Copy environment directory** — back up the entire `.ndb` directory before
   attempting any repair.

4. **Attempt normal reopen**:
   ```rust
   let env = Environment::open(
       EnvironmentConfig::new(path)
           .with_allow_create(false)
           .with_transactional(true),
   )?;
   ```
   If this succeeds, recovery is complete.

5. **If reopen fails — restore from replica** (replication environments only):
   Use the network restore protocol to sync from a healthy replica.
   The `env_home` field on `RepConfig` must be set on the source node.

6. **Last resort — restore from backup** using `BackupManager`-copied files.
   Replace the corrupted environment directory with the backup and reopen.

### Disk-full recovery

If `NoxuError::EnvironmentFailure { reason: DiskLimitExceeded, .. }` is
returned:

1. Free disk space (remove old log files outside the environment directory,
   expand the volume, etc.).
2. Close and reopen the environment.  The cleaner will resume and reclaim
   additional space automatically.

---

## 7. Known Limitations

| Limitation | Status | Workaround |
|-----------|--------|------------|
| **Concurrent write throughput gap vs JE** | Known — Noxu LockManager uses 64 shards; JE uses per-slot lock design that scales better at 16+ concurrent writers | Keep writer concurrency ≤ 8 threads per environment for optimal throughput; use disjoint key ranges when possible |
| **TiB-scale validation not automated** | `examples/scale_validation.rs` is a manual pre-production check; not run in CI | Run manually: `cargo run --example scale_validation -- --records 10000000 --threads 8` |
| **Sustained slow-test suite not in default CI** | P4/P5 tests marked `#[ignore]` to avoid CI timeouts | Run explicitly: `cargo nextest run -p noxu-db --profile slow --run-ignored all` |
| **`TupleSerdeBinding` sort order** | Uses `serde` binary encoding, not sort-preserving tuple encoding | Use raw `DatabaseEntry` with manually constructed sort-preserving keys for range scans on tuples |
| **Property-based tests timeout in fast nextest runs** | `noxu-db::prop_tests` and `noxu-collections::prop_tests` may timeout under default 60 s limit | Run with `--profile slow` or increase timeout in `.config/nextest.toml` |
| **Replication: server-side network restore** | TCP file transfer implemented; client-side `NetworkRestore::execute()` complete | Full production hardening of restore protocol is recommended before use in HA deployments |
| **No built-in metrics export** | `env.get_stats()` returns a snapshot; there is no Prometheus/OpenTelemetry integration | Wrap `get_stats()` in your own scraper thread |

---

## Quick-reference: `EnvironmentConfig` production defaults

```rust
EnvironmentConfig::new(path)
    .with_allow_create(true)
    .with_transactional(true)
    // Cache: 30% of available RAM, e.g. 8 GiB on a 32 GiB machine
    .with_cache_size(8 * 1024 * 1024 * 1024)
    // Log files: 64 MiB each (larger = less cleaner overhead)
    .with_log_file_max_bytes(64 * 1024 * 1024)
    // Checkpoint every 128 MiB written
    .with_checkpointer_bytes_interval(128 * 1024 * 1024)
    // Start cleaning files that are < 60% live (default 50%)
    .with_cleaner_min_utilization(60)
    // Group commit: batch up to 32 writers, flush every 2 ms
    .with_log_group_commit(32, 2)
    // Lock / txn timeouts to detect deadlocks quickly
    // (set via EnvironmentMutableConfig after open)
```
