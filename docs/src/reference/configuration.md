# Configuration Reference

Noxu DB has 400+ configuration parameters organized by subsystem, all set on
`EnvironmentConfig` before opening the environment.

## Quick Reference: Most Important Parameters

| Parameter | Default | Description |
|---|---|---|
| `cache_size` | 60% RAM | **Total** memory budget (BIN tree + log buffers + off-heap) |
| `shared_cache` | `false` | Join the process-global shared cache (one budget across all sharing envs) |
| `max_off_heap_memory` | 0 | Off-heap evicted-BIN storage (subtracted from `cache_size`) |
| `log_file_max_bytes` | 10 MiB | Trigger log file rotation |
| `lock_timeout_ms` | 500 | Per-lock acquisition timeout |
| `txn_timeout_ms` | 0 (off) | Transaction age limit |
| `checkpoint_bytes` | 20 MiB | Log bytes between checkpoints |
| `checkpoint_interval_ms` | 20 000 | Time between checkpoints |
| `cleaner_min_utilization` | 50 | % below which files are cleaned |
| `cleaner_threads` | 1 | Concurrent cleaner threads |
| `evictor_threads` | 1 | Background evictor threads |

## Memory Budget Model (v3.0.0)

As of v3.0.0, `cache_size` is the **total** memory ceiling, matching JE
semantics.  The three internal memory pools are:

| Pool | Size |
|---|---|
| BIN tree (Arbiter) | `cache_size − log_buffers − off_heap_reserved` |
| Log write buffers | `log_num_buffers × log_buffer_size` (default 3 MiB) |
| Off-heap BIN store | `max_off_heap_memory` (default 0) |

**Example**: `cache_size = 256 MiB`, `log_buffer_size = 1 MiB`,
`log_num_buffers = 3`, `max_off_heap_memory = 0` →
Arbiter budget = 256 − 3 = 253 MiB.

> **v3.0.0 migration**: If you were relying on the pre-v3.0.0 behaviour
> where `cache_size` bounded only the BIN tree pool, increase `cache_size`
> by `log_num_buffers × log_buffer_size + max_off_heap_memory` to maintain
> the same BIN tree allocation.  See
> [`migrating.md`](../getting-started/migrating.md).

## Shared cache across environments (`shared_cache`)

Set `EnvironmentConfig::with_shared_cache(true)` to make an `Environment`
join a **process-global shared cache** instead of getting a private one.
All environments in the process that set `shared_cache = true` share:

- **one memory budget** — sized from the **first** joining env's
  `cache_size` (later joiners' `cache_size` is ignored for the shared
  budget, matching JE);
- **one evictor** with **one global LRU** spanning every sharing env's
  B-trees; eviction picks victims across **all** sharing envs, so total
  resident memory stays bounded by the ONE shared budget rather than the
  sum of the per-env budgets.

On `Environment::close` the env removes its trees from the shared LRU
before they drop (no dangling trees), and the shared evictor tears down
when the last member closes.  `shared_cache = false` (the default) gives
each env an independent private cache + budget + evictor — unchanged
behaviour.  Faithful to JE `EnvironmentConfig.setSharedCache` +
`SharedEvictor` + the shared `MemoryBudget`.

## `CommitNoSync` background flush

When using `Durability::CommitNoSync` (no flush on commit), the
`log_flush_no_sync_interval_ms` parameter controls how often the
background `LogFlushTask` daemon drains write buffers to the OS page
cache.  Default: 0 (disabled — data stays in write buffers until the
next synchronous commit or explicit `env.sync()`).  Setting a non-zero
value (e.g. 100 ms) ensures `CommitNoSync` data is visible to other
processes within the interval.

```rust
EnvironmentConfig::new(path)
    .with_log_flush_no_sync_interval_ms(100) // flush to OS every 100 ms
```

| `log_file_max_bytes` | 10 MiB | Trigger log file rotation |
| `lock_timeout_ms` | 500 | Per-lock acquisition timeout |
| `txn_timeout_ms` | 0 (off) | Transaction age limit |
| `checkpoint_bytes` | 20 MiB | Log bytes between checkpoints |
| `checkpoint_interval_ms` | 20 000 | Time between checkpoints |
| `cleaner_min_utilization` | 50 | % below which files are cleaned |
| `cleaner_threads` | 1 | Concurrent cleaner threads |
| `evictor_threads` | 1 | Background evictor threads |

## SyncPolicy Values

| Value | Behaviour |
|---|---|
| `Sync` | `fdatasync` after every commit — maximum durability |
| `WriteNoSync` | Write to OS page cache, no `fsync` |
| `NoSync` | No write or sync — maximum throughput, no durability guarantee |

## EnvironmentFailureReason

When the environment is invalidated by an internal error, the reason is:

| Variant | `invalidates_environment()` | `is_corrupted()` |
|---|---|---|
| `InsufficientDisk` | true | false |
| `LogWriteFailed` | true | false |
| `LogChecksumMismatch` | true | true |
| `TreeNodeCorrupt` | true | true |
| `ForcedShutdown` | true | false |
| *(15 more variants)* | varies | varies |

After an environment failure, `env.is_valid()` returns `false` and all
subsequent operations return `NoxuError::EnvironmentFailure { reason, msg }`.

## TransactionConfig

Per-transaction overrides, set before `begin_transaction`:

| Field | Type | Default | Description |
|---|---|---|---|
| `no_wait` | `bool` | `false` | Fail immediately on lock conflict instead of blocking |
| `read_committed` | `bool` | `false` | Release read locks after each operation |
| `read_uncommitted` | `bool` | `false` | Allow dirty reads |
| `serializable_isolation` | `bool` | `false` | Full serializable (phantom protection) |
| `importunate` | `bool` | `false` | Steal locks from waiters (priority txn) |
| `lock_timeout_ms` | `u64` | `0` (env default) | Per-txn lock acquisition timeout |
| `txn_timeout_ms` | `u64` | `0` (no limit) | Transaction total duration limit |
| `local_write` | `bool` | `false` | Writes stay local (replica read-only mode) |
| `durability` | `Option<Durability>` | `None` (env default) | Override commit sync policy |

## DatabaseConfig

Per-database options set when opening a database:

| Field | Type | Default | Description |
|---|---|---|---|
| `allow_create` | `bool` | `false` | Create database if it doesn't exist |
| `transactional` | `bool` | `false` | Participate in transactions |
| `sorted_duplicates` | `bool` | `false` | Allow duplicate keys (sorted by data) |
| `replicated` | `bool` | `false` | Participate in replication log |
| `key_prefixing` | `bool` | `false` | Enable key prefix compression in BINs |
| `cache_mode` | `CacheMode` | `Default` | Per-database eviction hint (**advisory** — accepted but not yet honored; see note below) |
| `bin_delta` | `bool` | `true` | Write BIN-deltas instead of full BINs |
| `use_existing_config` | `bool` | `false` | Open existing DB without reconfiguring |

> **Advisory cache hints.** `DatabaseConfig::cache_mode`,
> `ReadOptions::cache_mode`, and `WriteOptions::cache_mode` (all typed
> `CacheMode`) are accepted for API forward-compatibility but are **not yet
> honored** by the evictor: the per-database and per-operation hints do not
> reach the cache today, so setting them has no effect. The corresponding
> setters are `#[deprecated]` to make the no-op explicit. Honoring per-op /
> per-DB cache modes is tracked for a future release. The environment-level
> cache policy is honored.

## CursorConfig

Per-cursor options:

| Field | Type | Default | Description |
|---|---|---|---|
| `read_committed` | `bool` | `false` | Read-committed isolation for this cursor |
| `read_uncommitted` | `bool` | `false` | Dirty reads for this cursor |
| `evict_ln` | `bool` | `false` | Evict leaf nodes after reading (cache bypass) |
| `prefix_constraint` | `Option<Vec<u8>>` | `None` | Stop scan at prefix boundary |

## Diagnostics & Observability (`EnvironmentConfig`)

These `EnvironmentConfig` knobs give operators real signal about the engine.
Logging itself is **not** configured here — Noxu routes all diagnostics through
the Rust [`log`](https://docs.rs/log) facade / `noxu-observe` / `RUST_LOG` (the
JE-style `logging_level` / `trace_*` knobs are deprecated no-ops).

| Field | Type | Default | Description |
|---|---|---|---|
| `startup_dump_threshold_ms` | `u64` | `0` (off) | If `Environment::open` takes at least this many ms (startup is dominated by crash recovery), log a `warn!` startup summary with the elapsed time and a stats snapshot. JE: `STARTUP_DUMP_THRESHOLD`. |
| `stats_collect` | `bool` | `false` | Enable the background stats-file dumper (JE `StatCapture`). |
| `stats_collect_interval_secs` | `u64` | `300` | Sampling interval for the stats-file dumper. |
| `stats_file_directory` | `Option<PathBuf>` | env home | Output directory for rotating stats CSV files (`noxu.stat.<N>.csv`). JE: `STATS_FILE_DIRECTORY`. |
| `stats_file_row_count` | `u32` | `1000` | CSV data rows per stats file before rotation. JE: `STATS_FILE_ROW_COUNT`. |
| `stats_max_files` | `u32` | `100` | Max rotated stats files retained (oldest pruned). JE: `STATS_MAX_FILES`. |
| `env_check_leaks` | `bool` | `true` | At `Environment::close`, warn on any lock still held with an owner (leaked transaction/cursor). Diagnostic only. JE: `EnvironmentImpl` leak checking. |

When `stats_collect` is enabled, a `noxu-stats-file` daemon samples the same
snapshot `Environment::stats()` returns and appends a CSV row every
`stats_collect_interval_secs`; after `stats_file_row_count` rows it rotates to
a new `noxu.stat.<N>.csv`, retaining at most `stats_max_files`.  The CSV is
self-contained (no external recorder needed) and is aimed at simple
ops/monitoring; for a live metrics pipeline use `metrics_export` (the
`observability` feature) instead.

## Replication Parameters

Replication is configured on `RepConfig` / `RepConfigBuilder`, not
`EnvironmentConfig`. See [Setup and Configuration](../replication/setup.md).
