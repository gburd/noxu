# Benchmarking

Noxu DB performance is measured against Noxu DB 7.5.11 using a shared
benchmark harness in `benches/`.

## Benchmark Suites

Located in `benches/` at the workspace root:

| File | Description |
|---|---|
| `write_bench.rs` | Sequential writes, commit throughput |
| `read_bench.rs` | Sequential reads, point lookups |
| `txn_bench.rs` | Multi-operation transactions |
| `concurrent_bench.rs` | Mixed read/write under concurrency |
| `recovery_bench.rs` | Environment open (recovery) time |
| `replication_bench.rs` | Replication throughput and latency |
| `util_bench.rs` | Low-level utilities (CRC32, checksums) |

Noxu benchmarks are in `benches/JeBenchmark.java`.

## Running Benchmarks

```bash
# Rust benchmarks (criterion)
cargo bench --bench write_bench -- --bench

# Noxu benchmarks (requires JDK)
javac benches/JeBenchmark.java
java -cp benches:path/to/je-7.5.11.jar JeBenchmark

# Compare at all scales
SCALES="1K 10K 100K" scripts/run_benchmarks.sh
```

## Benchmark Workloads

| ID | Name | Description |
|---|---|---|
| w01 | `seq_write_1t` | Sequential writes, 1 thread |
| w03 | `seq_read_1t` | Sequential reads, 1 thread |
| w09 | `txn_multi_1t` | Multi-record transactions, 1 thread |
| w10_conc | `conc_8r8w_16t` | 8 readers + 8 writers, 16 threads |
| w10_gc | `txn_group_commit` | Group commit vs no group commit |
| w11 | `recovery` | Environment open time |

## Canonical Results (Session 31, NVMe /scratch)

| Workload | Scale | Noxu | Noxu | Winner |
|---|---|---|---|---|
| seq write/1t | 1K | 1651 ops/s | 1003 ops/s | Noxu +65% |
| seq write/1t | 10K | 1552 ops/s | 1374 ops/s | Noxu +13% |
| seq write/1t | 100K | 1468 ops/s | 1194 ops/s | Noxu +23% |
| seq read/1t | 1K | 493,542 ops/s | 42,358 ops/s | Noxu 12x |
| seq read/1t | 100K | 453,962 ops/s | 404,520 ops/s | Noxu +12% |
| txn_multi/1t | 100K | 7082 ops/s | 6980 ops/s | Equal |
| conc 8r8w/16t | 100K | 2823 ops/s | 10,426 ops/s | Noxu 3.7x |
| group commit/8t | 100K | 2010 ops/s | 1437 ops/s (no GC) | Noxu +40% |

**Key observations**:

- Read throughput advantage: no JVM warmup (12x at 1K, 12% at 100K)
- Write throughput advantage: fsync coalescing on NVMe
- Concurrency gap (w10_conc): Noxu's LM is 3.7x faster at 16 threads — known gap
- Group commit: 40% improvement confirmed
- Storage efficiency: 107 B/op Noxu vs 154 B/op Noxu (30% more compact)

## Interpreting Criterion Output

```text
write_bench/seq_write/1K
    time:   [632.45 µs 634.12 µs 636.23 µs]
    thrpt:  [1572.8 /s 1576.7 /s 1581.8 /s]
```

- `time` — per-operation latency (median, 95th percentile)
- `thrpt` — operations per second
- Confidence interval (1572.8–1581.8) — smaller is better

Criterion uses 100 samples for stable estimates. Warm up period is 3 seconds.

## Benchmark Environment Notes

- Run on NVMe storage (`/scratch/`) for canonical results; tmpfs gives higher
  numbers but is not representative of production
- Close all other applications during benchmarking
- Noxu requires JVM warmup: `JeBenchmark` runs a warmup pass before measuring
- Rust benchmarks: `codegen-units=1` in `Cargo.toml` profile for consistent compilation
