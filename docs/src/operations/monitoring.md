# Monitoring

Call `env.get_stats()?` periodically (e.g., every 10 s) and export the fields
that matter.  All counters are cumulative since the environment was opened.

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

