# "Where Noxu Leads" Benchmarks

Most cross-engine benchmarks measure raw throughput, where MVCC/LSM engines
have historically had an edge. This note documents three dimensions where
Noxu's design is *structurally* superior and can be measured to **beat**
WiredTiger (WT), Berkeley DB JE, and RocksDB/TidesDB — not merely narrow a gap.

The thesis: Noxu pays **none** of the three tail/memory sinks its competitors
do — no MVCC snapshot rwlock (WT pays a measured 38% per read), no version-chain
garbage collection, no LSM compaction. So on the dimensions those costs
dominate, Noxu should lead:

| Dimension | Metric | Noxu advantage |
|---|---|---|
| **L1** tail-latency stability under contention | `p999`, `p9999`, `max`, `TAIL` series | no GC/compaction jitter source → flattest tail |
| **L2** memory efficiency | `cache_hit_rate`, `ops_per_gb` | one version per record → more distinct records resident per GB |
| **L3** write amplification | `write_amp` | each LN written once (no LSM compaction re-writes) |

All three are emitted by the shared cross-engine driver
`benches/noxu-bench/src/bin/xbench.rs` (binary `noxu-xbench`). The workload
generator (keys, values, distributions, op mixes, RNG seed) is byte-identical
to the WiredTiger and TidesDB C drivers, so cross-engine comparisons stay fair;
these three metrics are the Noxu-side instrumentation added on top.

## Running

```bash
cargo build --release -p noxu-workload-bench --bin noxu-xbench

# L1 tail-stability: long sustained mixed load with a per-second TAIL series.
BENCH_DIR=/data/noxu BENCH_WORKLOAD=ycsb_a BENCH_RECORDS=10000000 \
BENCH_CACHE=$((2*1024*1024*1024)) BENCH_THREADS=64 BENCH_SECONDS=300 \
BENCH_DURABILITY=SYNC BENCH_TAIL_INTERVAL=1 \
  ./target/release/noxu-xbench

# L2 memory efficiency: fix the workload, sweep BENCH_CACHE, read cache_hit_rate.
for gb in 1 2 4 8 16; do
  BENCH_DIR=/data/noxu BENCH_WORKLOAD=ycsb_c BENCH_RECORDS=10000000 \
  BENCH_CACHE=$((gb*1024*1024*1024)) BENCH_THREADS=64 BENCH_SECONDS=120 \
  BENCH_SKIP_LOAD=$([ $gb -eq 1 ] && echo 0 || echo 1) \
    ./target/release/noxu-xbench
done

# L3 write amplification: any write-heavy workload; read write_amp.
BENCH_DIR=/data/noxu BENCH_WORKLOAD=tdb_write BENCH_RECORDS=10000000 \
BENCH_THREADS=64 BENCH_SECONDS=300 BENCH_DURABILITY=SYNC \
  ./target/release/noxu-xbench
```

> **Use real NVMe, not tmpfs.** The driver aborts on a tmpfs `BENCH_DIR` — write
> amplification and fsync behaviour are meaningless on a RAM filesystem.

## The metrics

### L1 — Tail-latency stability (`p999` / `p9999` / `max` + `TAIL` series)

The `RESULT` line now reports `p999` and `p9999` (in addition to the existing
`p50`/`p90`/`p99`) plus `max`. Set `BENCH_TAIL_INTERVAL=N` to also emit a `TAIL`
line every `N` seconds:

```text
TAIL t=1 ops_s=103581 p50=4 p99=15 p999=8460 p9999=31818 max_us_bucket=47840
TAIL t=2 ops_s=102558 p50=4 p99=15 p999=9035 p9999=31400 max_us_bucket=44727
```

Each `TAIL` line reports the percentiles of the operations that completed **in
that interval** (a snapshot-diff of the 65 536-bucket, 1 µs-granularity
histogram), so tail *flatness over time* is visible — not just a single
whole-run p99. This is the direct signal for L1: a GC- or compaction-driven
engine spikes p999/p9999 periodically; Noxu, having no such background source,
should stay flat. (The remeasure baseline already shows Noxu's flat `max ≈100 ms`
on `tdb_write` vs JE's `1024 ms`.)

Latencies are bucketed at 1 µs up to 65 535 µs; anything above lands in the top
bucket but the true `max` is tracked separately. `max_us_bucket` in the `TAIL`
series is the interval max clamped to the histogram range.

### L2 — Memory efficiency (`cache_hit_rate`, `ops_per_gb`)

* `cache_hit_rate` = `1 − (ln_faults / committed_reads)`, where `ln_faults` is
  the delta in the log's **random-read** counter over the measured phase (a
  random read == an LN faulted from the log because it was not cache-resident).
  A read that hits the cache does no random read.
* `ops_per_gb` = throughput ÷ cache size in GiB.
* `committed_reads`, `ln_faults` are emitted raw for transparency.

Fix the workload and sweep `BENCH_CACHE`: Noxu should reach a given
`cache_hit_rate` at a **smaller** cache than an MVCC engine, because it spends
zero cache on old row versions (exactly one version per record resident).
Publish `cache_hit_rate` (and `ops_per_gb`) as a function of cache size — the
"hit-rate-per-GB" curve is the L2 lead.

Validation sanity checks (from smoke runs, 50 k × 1 KiB records, Zipfian):
a 16 MiB cache on the ~50 MiB dataset gives `cache_hit_rate ≈ 0.82`; a 256 MiB
cache (whole dataset resident) gives `cache_hit_rate = 1.0000` with
`ln_faults = 0`.

### L3 — Write amplification (`write_amp`)

`write_amp` = physical bytes written ÷ committed user bytes.

* **Numerator** (physical): the log's sequential-write-bytes delta over the
  measured phase (`log_write_bytes`). This is what Noxu actually wrote to
  `/data`. If the log counter is unavailable it falls back to the
  `/proc/self/io` `write_bytes` delta (`proc_write_bytes`, emitted as a
  cross-check — the two agree within a couple of percent).
* **Denominator** (user): `committed_writes × BENCH_VALUE` — the number of
  successfully committed record writes times the value size.

A log-structured B+tree writes each LN **once**; the cleaner reclaims obsolete
space but does not re-sort/re-merge the dataset the way an LSM does. Expected
`write_amp` for pure inserts is ≈1.1 (the ~10% overhead is LN headers,
checksums, BIN/IN updates, and log-file framing) — confirmed by smoke runs
(`write_amp = 1.10` on `tdb_write`). An LSM (RocksDB/TidesDB) re-writes data
O(levels) times via compaction; write amplification of 10–30× is typical. On a
device where write endurance (flash wear) or per-IOP cost (cloud) matters, this
is a decisive Noxu lead. Run the same workload against RocksDB/TidesDB and
divide their bytes-written by the same user-byte denominator to compare.

## RESULT line fields (added)

```text
RESULT engine=noxu workload=... ... \
  p50=.. p90=.. p99=.. p999=.. p9999=.. max=.. \
  cache_hit_rate=.. committed_reads=.. ln_faults=.. cached_bins=.. lru_size=.. ops_per_gb=.. \
  committed_writes=.. user_bytes=.. log_write_bytes=.. proc_write_bytes=.. write_amp=..
```

`cache_hit_rate = -1.0000` means the workload did no reads (e.g. `tdb_write`) —
the metric is n/a, not a false 1.0. `write_amp = 0.000` means no committed
writes (e.g. `ycsb_c`).

## Known stats gaps (follow-ups, not blockers)

The metrics above are derived from stats the engine **does** maintain (log
sequential-write bytes, log random reads). The audit turned up counters that are
declared in `EvictorStats` but never incremented anywhere in the engine, so they
cannot yet back a metric:

* `evictor.ln_fetch` / `ln_fetch_miss` / `bin_fetch` / `bin_fetch_miss` /
  `upper_in_fetch` (+ misses) — all read 0. The LN-cache hit-rate is therefore
  derived from the **log random-read** counter instead, which is a correct proxy
  (an LN cache miss faults a random read from the log).
* `evictor.cached_bins` / `lru_size` — refreshed only by
  `Evictor::update_lru_stats()`, which the stats-snapshot path does not call, so
  they usually read 0. Emitted for transparency; a true **resident-records**
  count (distinct LNs held in cache) would be the ideal L2 numerator and is a
  worthwhile follow-up in core.

Wiring those counters (or adding a resident-records gauge) would let L2 report a
direct records-per-GB figure rather than the random-read-derived hit-rate; until
then the hit-rate proxy is accurate and sufficient for cross-engine comparison.
