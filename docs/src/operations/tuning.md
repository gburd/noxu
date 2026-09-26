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

## Checkpoint images and eviction

A parent slot may reference a BIN delta rather than its older full image.
Eviction preserves that newer reference so a cache miss reads the checkpointed
values, not the delta's stale base. This fix requires no log-format migration.
It prevents future stale-image refaults; it does not restore values already
lost by an affected build. Validate suspect data against application records
or a known-good backup before resuming writes.

## Dirty BIN logging failures

A dirty BIN now stays resident when eviction cannot log its current image,
including when no logger is available. A previously logged image is not a
substitute for unlogged updates. Refusal leaves the candidate eligible for
retry and gives no node or byte eviction credit; cache pressure can therefore
remain high while logging is unavailable. Clean BINs with a valid image can
still be evicted without a logger.

Investigate storage errors rather than treating low eviction throughput as a
cache-sizing issue. Retry can proceed when logging succeeds again, but an
environment permanently invalidated by a WAL I/O failure must still follow
the normal failure/recovery procedure; this change does not clear invalidation.
No format migration is required, and already-lost updates are not restored.

This fix covers unsuccessful dirty-BIN logging only. WAL obsolete-image
accounting after failed writes remains an open blocker: retaining the dirty
node alone is not sufficient to make repeated logging attempts safe.
Dirty-generation/pin races between logging and detach, and dirty upper-IN
handling, also remain separate open safety issues. This is not a general
eviction data-loss-safety guarantee.

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

In a replicated group, the master additionally pins every log file at or after
the file containing the group's CBVLSN (the minimum VLSN acknowledged by any
active electable replica) so the cleaner never deletes a file a lagging replica
still needs. A replica that falls far behind (or is briefly disconnected) will
therefore hold the master's log files that cover its gap — so `deletions` can
stall until the slow replica catches up. Watch replica lag; the protection is
released automatically as the CBVLSN advances. See
[Replication concepts → CBVLSN](../replication/concepts.md).

## Write throttling

When the cleaner falls behind, it signals writer threads to pause briefly
via `CleanerThrottle`.  This is automatic and transparent.  If you observe
sustained write latency spikes:

1. Check `s.cleaner.runs` — if it is not climbing, the cleaner may be disabled.
2. Lower `cleaner_min_utilization` (e.g., 60) to trigger cleaning sooner.
3. Increase log file size so each file takes longer to fill, giving the cleaner more time.

---
