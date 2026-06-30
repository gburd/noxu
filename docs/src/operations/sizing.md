# Sizing

## Cache and Total Memory Budget (v3.0.0)

> **v3.0.0 change**: `cache_size` is now the **total** memory ceiling.
> The BIN tree Arbiter, log write buffers, and off-heap cache all count
> against it.  See [configuration.md](../reference/configuration.md) for
> the budget model and migration guidance.

The in-memory B-tree node cache is the primary performance lever.

| Workload | Recommended `cache_size` |
|----------|---------------------------|
| Hot dataset fits in RAM | 60–80% of available RAM |
| Mixed hot/cold | 30–40% of available RAM |
| Constrained environments | ≥ 2× average working-set size |

**Budget accounting** (v3.0.0):

```text
total_memory     = cache_size
log_buffers      = log_num_buffers x log_buffer_size  # default 3 x 1 MiB = 3 MiB
off_heap         = max_off_heap_memory                # default 0
bin_tree_budget  = cache_size - log_buffers - off_heap
```

Configure at environment open time:

```rust
EnvironmentConfig::new(path)
    .with_cache_size(4 * 1024 * 1024 * 1024)  // 4 GiB total
    .with_transactional(true)
```

Use `env.get_stats()?.cache_utilization_percent()` to monitor usage at runtime.
If this value is consistently above 90%, increase the cache or reduce the
working set.

Off-heap memory (for large environments) can be enabled separately:
`set_max_off_heap_memory(bytes)`.  This amount is **subtracted** from the
Arbiter (BIN tree) budget, so increase `cache_size` by the same amount to
maintain the BIN tree headroom.

## Log file size

Each log file (`.ndb`) is rolled when it exceeds `log_file_max_bytes`
(default 10 MiB).  Smaller files let the cleaner reclaim space more
aggressively but create more file-handle churn.

```rust
// 64 MiB log files — better for bulk-write workloads
.with_log_file_max_bytes(64 * 1024 * 1024)
```

## Disk-space limits (`MAX_DISK` / `FREE_DISK`)

Noxu DB can refuse new user writes before the disk fills, so the engine never
runs out of space mid-write and recovery stays possible. Two independent limits
(both faithful to BDB-JE) gate the user-write path:

| Limit | Builder | Meaning | Default |
|-------|---------|---------|---------|
| `MAX_DISK` | `with_max_disk(bytes)` | Absolute cap on total log size (sum of all `.ndb` files). | `0` = disabled |
| `FREE_DISK` | `with_free_disk(bytes)` | Keep at least this many bytes free on the filesystem. | `5 GiB` |

```rust
let cfg = EnvironmentConfig::new(path)
    .with_allow_create(true)
    .with_max_disk(50 * 1024 * 1024 * 1024)  // cap the log at 50 GiB
    .with_free_disk(2 * 1024 * 1024 * 1024); // and keep 2 GiB free
```

A write is prohibited when **either** limit is violated:
`availableLogSize = (maxDisk > 0) ? min(diskFree - freeDisk, maxDisk - totalLog)
: diskFree - freeDisk`; the write is refused (with `NoxuError::DiskLimitExceeded`)
when `availableLogSize <= 0`.

Behaviour while over the limit:

- **User `put`/`delete` are refused** with `DiskLimitExceeded` before anything
  is logged — no partial write, no corruption.
- **Reads, transaction aborts, and the cleaner/checkpointer's internal writes
  keep working** — the cleaner must write to free space, so internal writes are
  exempt (otherwise the environment would deadlock at the limit).
- The limit is checked with a single cached atomic load on the write path
  (no per-write `statvfs`); it is refreshed periodically by the checkpointer
  daemon and after every cleaner pass.
- Once the cleaner reclaims space, the next refresh clears the violation and
  writes resume automatically (no reopen). Call
  `Environment::refresh_disk_limit()` to recompute immediately.

When both limits are `0` the check is inert and write throughput is unaffected.
Leaving `FREE_DISK` at its 5 GiB default is recommended for production: it
reserves headroom so the cleaner and recovery always have room to run. See
[Recovery operations → Disk-full recovery](recovery-ops.md#disk-full-recovery).

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
