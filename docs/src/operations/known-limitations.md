# 7. Known Limitations

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

# Quick-reference: `EnvironmentConfig` production defaults

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
