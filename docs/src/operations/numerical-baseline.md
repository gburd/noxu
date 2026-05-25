# Numerical Baseline

A documented procedure for collecting Noxu DB throughput and
latency numbers under a 24-hour sustained workload. Without
these, statements like "Noxu handles 10K writes/sec" or "p99
latency is 5ms" are claims rather than evidence.

## What this measures

For a 24-hour run on a representative machine, you get:

- **Throughput**: read and write operations per second, per
  60-second window. 1440 windows over a day shows whether
  throughput is steady, declining (cleaner falling behind,
  fragmentation, memory creep), or transiently bursty.
- **Latency**: p50 and p99 read and write latency per window,
  in nanoseconds.
- **Resource shape**: process RSS in KB and disk-bytes-written
  per window.
- **Error rate**: count of `Result::Err` per window.

The output is a CSV file suitable for ingestion by any plotting
tool (gnuplot, pandas, datasette, …).

## How to run

```sh
# Build the baseline binary.
cargo build --bin noxu-sustained-baseline --release \
    -p noxu-workload-bench

# Run for 24h with a 60s window, 8 readers + 8 writers, 256-byte
# values. Output goes to baseline.csv.
target/release/noxu-sustained-baseline \
    --dir /var/lib/noxu-baseline \
    --duration-secs 86400 \
    --window-secs 60 \
    --readers 8 \
    --writers 8 \
    --value-size 256 \
    --output baseline.csv
```

A short smoke run for verifying the harness works on a new
machine:

```sh
target/release/noxu-sustained-baseline \
    --dir /tmp/noxu-baseline-smoke \
    --duration-secs 30 \
    --window-secs 5 \
    --readers 4 \
    --writers 4 \
    --value-size 256
```

## Recommended hardware (for publishable numbers)

The hardware shapes the numbers. Publishable baselines should
specify:

- **Instance type / hardware**: e.g. `c7gn.4xlarge` (Graviton 3,
  16 vCPU, 32 GiB RAM, 25 Gbps network)
- **Disk**: e.g. `io2 Block Express, 16,000 IOPS provisioned, 1
  TiB`
- **Filesystem**: e.g. `xfs default mount options on a dedicated
  block device, no encryption layer`
- **Operating system**: e.g. `Amazon Linux 2023, kernel
  6.1.x-amzn`
- **CPU pinning / NUMA**: e.g. `not pinned; default Linux
  scheduler`

A different combination produces different numbers; that's not
a regression, it's a different test. The baseline exists to be
**reproducible**, not to compete for the highest absolute
number.

## Recommended workload variants

Run multiple 24h runs to capture different shapes:

| Variant | `--readers` | `--writers` | `--value-size` | What it shows |
|---|---|---|---|---|
| read-heavy | 8 | 1 | 256 | Cache-resident read throughput; latency floor |
| write-heavy | 1 | 8 | 256 | Cleaner / WAL throughput; commit-fsync stack |
| balanced | 8 | 8 | 256 | Mixed contention; lock manager pressure |
| large-value | 4 | 4 | 4096 | I/O-bound; cache-eviction pressure |
| many-readers | 32 | 4 | 256 | Read scalability; reader-vs-writer contention |

## Interpreting the CSV

A healthy 24h run looks like:

- `reads` and `writes` per window: stable from window 1 to 1440
  (variance < 10% within the same hour, < 30% across the whole
  day)
- `read_ns_p50`: stable, no drift
- `read_ns_p99`: stable; bursts < 10× p50
- `write_ns_p99`: stable; bursts only at checkpoint boundaries
  (correlated with `disk_bytes_written` spikes)
- `rss_kb`: stable, no monotonic growth
- `disk_bytes_written` per window: bounded (the cleaner is
  reclaiming as fast as the writers are filling)
- `err_count`: 0

Any monotonic increase or steady drift indicates a real issue
(cleaner backlog, memory leak, etc.) that should be triaged
through `docs/src/operations/runbooks.md`.

## Why we don't run this in CI

24 hours of CPU + several GiB of disk per run is too expensive
for per-PR. The baseline should be re-run on the release
branch whenever:

- the cleaner / log-cleaning code path changes
- the WAL / commit-fsync code path changes
- any allocator change (memory budget, off-heap)
- a major rust-toolchain upgrade
- the target hardware family changes

The output of each run should be archived alongside its run
metadata (hardware, kernel, filesystem, git commit) in a
versioned location (e.g. `docs/src/internal/baseline/<date>/`)
so claims like "v1.3.0 sustains 10K writes/sec on
c7gn.4xlarge" can be backed by a CSV.
