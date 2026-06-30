# Monitoring

## Built-in metrics export (the `observability` feature)

Instead of writing your own scraper loop, enable the `observability` feature on
`noxu-db` and let the engine publish its statistics continuously to the
[`metrics`](https://docs.rs/metrics) facade. This is the Rust-ecosystem
analogue of BDB-JE's read-only JMX MBean export: a background daemon samples the
same `get_stats()` snapshot on an interval and emits every field as a gauge or
counter. Whichever recorder you install (Prometheus, StatsD, OpenTelemetry, a
test recorder, …) then collects them. With the feature disabled the engine pulls
no metrics crates and the instrumentation compiles to nothing.

```toml
# Cargo.toml
noxu-db = { version = "6", features = ["observability"] }
# Optional built-in Prometheus exposition:
noxu-observe = { version = "6", features = ["prometheus"] }
```

```rust
use std::sync::Arc;
use std::time::Duration;
use noxu_db::metrics_export::MetricsExporter;

// 1. Install any `metrics` recorder. The built-in Prometheus convenience:
let handle = noxu_db::observe_crate::prometheus::install()?;

// 2. Start the periodic exporter (samples get_stats() every 10s).
let env = Arc::new(env);
let exporter = MetricsExporter::start(env.clone(), Duration::from_secs(10));

// 3. Serve `handle.render()` from your /metrics HTTP endpoint.
//    ... on shutdown:
exporter.stop();
```

### Exported metrics and their JE stat groups

Every metric maps 1:1 onto a field of `EnvironmentStats`, which mirrors a JE
`StatGroup`. Cumulative quantities are counters (`*_total`); instantaneous
quantities are gauges.

| Metric | Kind | JE stat group / name |
|--------|------|----------------------|
| `noxu_cache_size_bytes`, `noxu_cache_usage_bytes`, `noxu_cache_utilization_ratio` | gauge | `EnvironmentStats` cache budget / usage |
| `noxu_evictor_runs_total`, `noxu_evictor_nodes_evicted_total`, `noxu_evictor_bytes_evicted_total` | counter | `EvictorStatDefinition` (`EVICTOR_EVICTION_RUNS`, `EVICTOR_NODES_EVICTED`, `EVICTOR_*_BYTES`) |
| `noxu_evictor_bin_fetch_total`, `noxu_evictor_bin_fetch_miss_total`, `noxu_evictor_cache_hit_ratio`, `noxu_evictor_lru_size` | counter/gauge | `EVICTOR_BIN_FETCH`, `EVICTOR_BIN_FETCH_MISS`, derived hit-rate, `EVICTOR_PRI*_LRU_SIZE` |
| `noxu_log_fsyncs_total`, `noxu_log_fsync_requests_total`, `noxu_log_group_commits_total`, `noxu_log_fsync_batch_size_sum` | counter | `FSYNCMGR_FSYNCS`, `FSYNCMGR_FSYNC_REQUESTS`, `FSYNCMGR_N_GROUP_COMMIT_REQUESTS`, batch-size numerator |
| `noxu_log_bytes_written_total`, `noxu_log_bytes_read_total` | counter | `FILEMGR_SEQUENTIAL_WRITE_BYTES`, `FILEMGR_*_READ_BYTES` |
| `noxu_log_end_of_log_lsn`, `noxu_log_last_flush_lsn` | gauge | `LOGMGR` end-of-log / last-flush LSN |
| `noxu_lock_requests_total`, `noxu_lock_waits_total`, `noxu_lock_timeouts_total`, `noxu_lock_total_locks`, `noxu_lock_waiters` | counter/gauge | `LockStatDefinition` (`LOCK_REQUESTS`, `LOCK_WAITS`, `LOCK_*_TIMEOUTS`, `LOCK_TOTAL`, `LOCK_WAITERS`) |
| `noxu_txn_begins_total`, `noxu_txn_commits_total`, `noxu_txn_aborts_total`, `noxu_txn_active` | counter/gauge | `Txn` group (`nBegins`, `nCommits`, `nAborts`, `nActive`) |
| `noxu_cleaner_runs_total`, `noxu_cleaner_files_deleted_total`, `noxu_cleaner_min_utilization`, `noxu_cleaner_backlog`, `noxu_cleaner_total_log_size_bytes`, `noxu_cleaner_active_log_size_bytes` | counter/gauge | `CleanerStatDefinition` (`CLEANER_RUNS`, `CLEANER_DELETIONS`, `CLEANER_MIN_UTILIZATION`, `CLEANER_PENDING_LN_QUEUE_SIZE`, `CLEANER_*_LOG_SIZE`) |
| `noxu_checkpoint_count_total`, `noxu_checkpoint_last_interval_ms`, `noxu_checkpoint_last_id` | counter/gauge | `CheckpointStatDefinition` (`CKPT_CHECKPOINTS`, `CKPT_LAST_CKPT_INTERVAL`, `CKPT_LAST_CKPTID`) |
| `noxu_db_pri_inserts_total`, `noxu_db_pri_updates_total`, `noxu_db_pri_deletes_total`, `noxu_db_pri_searches_total` | counter | `THROUGHPUT_PRI_*` (see caveat below) |
| `noxu_databases_open` | gauge | open-database count |

> **Caveat — throughput counters.** The `noxu_db_pri_*` metrics are exported but
> currently read 0: the engine's per-database `ThroughputStats` counters are
> defined but not yet incremented on the write path. They are surfaced for
> forward compatibility; rely on `noxu_txn_*` and `noxu_db_operations_total`
> (emitted by the hot-path `observe_*` macros) for operation volume.

No recorder installed? The facade calls are cheap no-ops, so leaving the
exporter running has negligible overhead even when nothing is collecting.

## Manual scraping

If you prefer not to enable the feature, call `env.get_stats()?` periodically
(e.g., every 10 s) and export the fields that matter.  All counters are
cumulative since the environment was opened.

## Key fields and alert thresholds

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

## Deriving group commit effectiveness

```rust
let avg_batch = if s.log.n_group_commits > 0 {
    s.log.n_fsync_batch_size_sum / s.log.n_group_commits
} else {
    0
};
// avg_batch > 4 means group commit is effectively coalescing I/O
```

## ExceptionListener for async error reporting

Register an `ExceptionListener` to receive callbacks on environment
invalidation events (log corruption, disk full, latch timeout):

```rust
use noxu::{ExceptionListener, ExceptionEvent};

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
