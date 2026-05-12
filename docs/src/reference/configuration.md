# Configuration Reference

Noxu DB has 400+ configuration parameters organized by subsystem, all set on
`EnvironmentConfig` before opening the environment.

## Quick Reference: Most Important Parameters

| Parameter | Default | Description |
|---|---|---|
| `cache_size` | 60% RAM | On-heap B+tree node cache |
| `max_off_heap_memory` | 0 | Off-heap evicted-BIN storage |
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

## Replication Parameters

Replication is configured on `RepConfig` / `RepConfigBuilder`, not
`EnvironmentConfig`. See [Setup and Configuration](../replication/setup.md).
