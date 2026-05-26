# Checkpoint Tuning

The checkpointer writes dirty B-tree nodes to log files and records a
stable recovery point.  More frequent checkpoints reduce recovery time
after a crash at the cost of additional I/O.

## Configuration knobs (via `EnvironmentConfig`)

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

## Recommended production settings

- **OLTP workloads**: `checkpointer_bytes_interval = 64 MiB` (default is fine; tighten to 16 MiB
  if crash recovery must be < 5 s).
- **Bulk load**: disable automatic checkpointing (`set_run_checkpointer(false)`), call
  `env.checkpoint(...)` manually between batches, re-enable afterwards.
- **Before shutdown**: always call
  `env.checkpoint(Some(CheckpointConfig::new().with_minimize_recovery_time(true)))` to avoid a
  full recovery on next open.

---

## 4. Cleaner Tuning

The log cleaner reclaims disk space by copying live records out of
under-utilized log files and then deleting those files.

## Key parameters

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

## Interpreting cleaner stats

```rust
let s = env.get_stats()?;
println!("cleaner runs={}, deletions={}", s.cleaner.runs, s.cleaner.deletions);
println!("reserved_log={} B", s.cleaner.reserved_log_size);
println!("available_log={} B", s.cleaner.available_log_size);
```

If `reserved_log_size` grows without `deletions` increasing, the cleaner is
reading files but cannot delete them (active cursors or open transactions are
pinning log files).  Keep transactions short-lived to unpin files promptly.

## Write throttling

When the cleaner falls behind, it signals writer threads to pause briefly
via `CleanerThrottle`.  This is automatic and transparent.  If you observe
sustained write latency spikes:

1. Check `s.cleaner.runs` — if it is not climbing, the cleaner may be disabled.
2. Lower `cleaner_min_utilization` (e.g., 60) to trigger cleaning sooner.
3. Increase log file size so each file takes longer to fill, giving the cleaner more time.

---
