# Sizing

## Cache

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

## Log file size

Each log file (`.ndb`) is rolled when it exceeds `log_file_max_bytes`
(default 10 MiB).  Smaller files let the cleaner reclaim space more
aggressively but create more file-handle churn.

```rust
// 64 MiB log files — better for bulk-write workloads
.with_log_file_max_bytes(64 * 1024 * 1024)
```

## Thread pool sizing

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

