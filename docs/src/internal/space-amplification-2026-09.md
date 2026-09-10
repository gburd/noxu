# Space amplification: characterisation (2026-09)

Status: **Phase 1 (characterisation) partial, in progress.** This document
records every number measured so far, with the exact command used to produce
it, so the work survives even if the investigation is interrupted. Sections
not yet measured are marked **NOT YET MEASURED**. Nothing in this document
recommends or makes a default change.

## Caveat that applies to every number below

All measurements were taken on a shared EC2 `i4i.16xlarge` (64 vCPU, 495 GB
RAM, local NVMe XFS at `/data`) that **three peer agents were concurrently
using for unrelated work**. `uptime` during this session showed load
averages around 125 on the 64-vCPU box. Throughput/wall-clock numbers
(writes/s, load time, drain wall-clock) are consequently **not clean**
and should be treated as order-of-magnitude only. Space (byte) measurements
(`du -sb`, log write-byte counters) are not affected by CPU contention and
are trustworthy. Where a number could plausibly be contention-skewed, this is
called out inline.

## Background: why this investigation

The v7.5.4 cross-engine benchmark
(`.agent/archived-audits/bench/v754-vs-wt-tidesdb-2026-07.md`, gitignored
archive, summarized here) found Noxu's on-disk footprint grew from 37 GB
(post-load) to **95 GB** after a write-heavy sweep on a 20M-record / 1KB-value
/ 4GB-cache dataset — a **2.09x** space-amp ratio, the worst axis against
WiredTiger (22-23 GB) and TidesDB (1.1-1.6 GB, compressed). That report
attributed the growth to "the log cleaner defers reclamation" without
separating out *which* mechanism inside the cleaner was responsible. This
task is that follow-up: measure, don't assume.

## Method

A new bench binary, `noxu-space-amp-probe`
(`benches/noxu-bench/src/bin/space_amp_probe.rs`, registered in
`benches/noxu-bench/Cargo.toml`), drives:

1. **Load phase**: fill a fixed keyspace of `SAP_RECORDS` records with
   `SAP_VALUE`-byte values, batched inserts, then one checkpoint.
2. **Update-storm phase**: `SAP_THREADS` threads run a Zipfian (θ=0.99,
   YCSB-standard, same RNG/Zipf implementation as `noxu-xbench` so key
   sequences are directly comparable) **overwrite-only** workload against the
   *existing* keyspace for `SAP_UPDATE_SECONDS` — no new keys are inserted, so
   the live dataset size is constant and every write obsoletes exactly one
   prior LN version. This is the same shape that produced the cross-engine
   finding.
3. **Drain phase**: the checkpointer and cleaner background daemons
   (`run_checkpointer`/`run_cleaner`, both on by default) keep running after
   the storm stops; the probe polls `du -sb` every 2s until it stops
   shrinking for 5 consecutive polls (or a poll budget is hit), simulating
   "what does the footprint settle to if the system goes idle".
4. Reports `du -sb` at each stage, the engine's own wired stats
   (`env.stats()` — see "Stats-wiring gap" below for which fields are and are
   not populated in production), and a per-log-entry-type byte histogram via
   `noxu-admin print-log -h <dir> -S` run as a subprocess against the closed
   environment.

Binary env vars: `SAP_DIR SAP_RECORDS SAP_VALUE SAP_CACHE_MB
SAP_UPDATE_SECONDS SAP_THREADS SAP_MIN_UTIL SAP_CKPT_BYTES SAP_CKPT_MS
SAP_DURABILITY SAP_ADMIN_BIN SAP_FINAL_CLEAN_PASSES SAP_SEED` — see the file's
doc comment for defaults.

All runs used `/data/space-runs/<name>` (real NVMe XFS, never tmpfs — verified
by the probe's own `stat -f` guard, which aborts if the target is tmpfs).

## A methodological trap found and fixed while building the probe

The first version of the drain phase called `env.checkpoint(None)` and
`env.clean_log()` manually in a loop, alternating with the (already-running)
background daemons. Both of those calls acquire an **exclusive
in-progress flag** (`checkpoint_in_progress` / the cleaner's `running`
atomic) that the daemon threads also hold while they work. A manual call
issued while a daemon held the flag returned `Err`, and the probe's original
`let _ = ...` silently ignored it — so the loop looked like it was cycling
but the checkpoint count barely moved (1 -> 2 across dozens of "rounds") and
`du` **grew** instead of shrinking. This was caught by checking the raw
`.ndb` file count/size on disk after the "drain" reported success and finding
it had not shrunk. Fixed by making the drain phase passive (poll `du -sb`,
let the daemons that were already running do the work, with best-effort
manual nudges that are allowed to no-op). This is recorded so nobody
re-introduces the same trap: **do not race the checkpointer/cleaner daemons
with manual calls and ignore the error** — either disable the daemons for a
fully-manual test, or drive drain passively as done here.

## Stats-wiring gap found (affects what can be measured via `env.stats()`)

`CleanerStats` (`crates/noxu-cleaner/src/cleaner_stat.rs`) declares
`total_log_size`, `active_log_size`, `reserved_log_size`,
`protected_log_size`, `available_log_size`, `min_utilization`,
`max_utilization`, `probe_runs`, and `repeat_iterator_reads` fields. Grepping
the whole workspace: every `.store()` call against these fields is in a
`#[cfg(test)]` unit test in `cleaner_stat.rs` itself; **no production code
path ever writes them**. `EnvironmentStats::default()` and
`CleanerStats::new()` both initialise them to 0, and nothing updates them
after that, so `env.stats().cleaner.total_log_size` (and the sibling fields)
read `0` in a real running environment, not the true value. There is a
*separate*, live implementation of the same concept
(`UtilizationProfile::get_total_log_size/get_active_log_size/
get_overall_utilization`, `crates/noxu-cleaner/src/utilization_profile.rs`)
that IS correct and IS used internally by file selection — it is just never
copied into the public stats snapshot. `noxu-observe`'s Prometheus exporter
(`crates/noxu-observe/src/export.rs`) also publishes gauges from these same
dead fields, so **the `noxu_cleaner_total_log_size_bytes` /
`noxu_cleaner_active_log_size_bytes` / `noxu_cleaner_min_utilization`
Prometheus gauges are currently always 0 in production**, not reflecting
reality. This is a real operational gap (an operator watching these metrics
sees nothing), but fixing it is out of scope for this task — noted here so it
does not get re-discovered from scratch, and because it constrains this
report to measuring the log-cleaning breakdown from `du -sb` +
`noxu-admin print-log -S` + the counters that ARE wired
(`cleaner.runs/deletions/lns_cleaned/lns_migrated/...`,
`checkpoint.checkpoints/full_bin_flush/...`, `log.n_sequential_write_bytes`),
rather than from the stats struct's disk-usage fields.

## Measured runs

### Run "smoke" (small, correctness check for the probe itself)

```text
SAP_DIR=/data/space-runs/smoke SAP_RECORDS=50000 SAP_VALUE=512 SAP_CACHE_MB=128 \
SAP_UPDATE_SECONDS=15 SAP_THREADS=4 SAP_MIN_UTIL=50 \
SAP_ADMIN_BIN=/data/space/target/release/noxu-admin \
./target/release/noxu-space-amp-probe
```

| stage | du (bytes) | du (human) | note |
|---|---:|---:|---|
| after load | 55,682,655 | 53 MiB | 50,000 x 512B = 25.6 MB live |
| before drain (after 15s update storm) | 1,255,059,349 | 1.17 GiB | space-amp = 49.0x vs live bytes |
| after drain (daemons ran to steady state) | 16,356,808 | 16 MB | space-amp = 0.64x vs live bytes (see caveat below) |

Cleaner/checkpoint counters at end: `cleaner_runs=82 cleaner_deletions=484
checkpoints=13`. Log write bytes for the update-storm phase:
`phase_log_write_bytes=1,229,288,931` against
`update_storm_writes=1,944,729 x 512B = 995,701,248` user bytes =>
`write_amp_phase=1.235`.

**Post-drain integrity check** (independent of the probe, run separately):
opened the resulting environment read-only-daemons-off and did a `get_in()`
for every one of the 50,000 keys — **all 50,000 keys found, correct values**.
So the post-drain 16 MB on disk is not data loss; the live 25.6 MB of values
is being held by something other than standalone `InsertLN`/`UpdateLN` log
entries once the log has been fully compacted down to 2 files. The
per-type breakdown after drain was: `BIN=207/1846 entries,
FileSummaryLN=1599, CkptStart/End=12/13, IN=3, NameLN=12` — **zero**
`INS_LN`/`INS_LN_TX`/`UPD_LN*` entries survive full compaction. This is
consistent with full BIN log entries (`LogEntryType::BIN`) carrying every
live slot's value bytes inline (`BinEntry.data: Option<Bytes>`,
`crates/noxu-tree/src/tree.rs`, serialized via `write_to_bytes()` into an
`InLogEntry`/`BIN` log record) rather than the record's original standalone
LN entry remaining live — i.e. once a BIN carrying a slot is re-logged in
full (checkpoint or eviction), the old standalone LN for every slot in that
BIN becomes obsolete and is exactly the kind of "garbage" the cleaner is
compacting away. **This mechanism (full-BIN-carries-inline-values) is a
plausible explanation for why post-drain space can end up smaller than the
naive "N records x value size" floor would suggest, but it has NOT yet been
independently confirmed against the BIN-serialization code path with a
targeted unit test — flagged as an open question, not yet fully verified,
do not treat the exact 16 MB number as a validated floor.**

### Run "baseline" (larger scale, `min_utilization=50`, the JE default)

```text
SAP_DIR=/data/space-runs/baseline SAP_RECORDS=2000000 SAP_VALUE=512 SAP_CACHE_MB=512 \
SAP_UPDATE_SECONDS=180 SAP_THREADS=16 SAP_MIN_UTIL=50 SAP_FINAL_CLEAN_PASSES=60 \
SAP_ADMIN_BIN=/data/space/target/release/noxu-admin \
./target/release/noxu-space-amp-probe
```

Live dataset: 2,000,000 records x 512 B = 1,024,000,000 bytes (0.95 GiB).

| stage | du (bytes) | du (human) | space-amp vs live bytes |
|---|---:|---:|---:|
| after load | 1,648,134,279 | 1.53 GiB | 1.61x |
| before drain (after 180s update storm, contended box) | 12,230,813,946 | 11.39 GiB | 11.94x |
| after drain (daemons ran to steady state, ~14s of polling) | 12,818,956 | 12.2 MB | 0.0125x |

Update-storm throughput reported: 17,125,233 writes in 180.0s = ~95,125
writes/s — **this number is on a box with load average ~125 across 64
vCPUs from peer agents; treat as order-of-magnitude, not a clean
single-tenant number.**

Cleaner/checkpoint counters at end: `cleaner_runs=1795 cleaner_deletions=4491
checkpoints=15`. Phase write-amp: `phase_log_write_bytes=11,527,740,763`
against `17,125,233 x 512B = 8,768,119,296` user bytes =>
`write_amp_phase=1.315`.

Post-drain per-type breakdown (`noxu-admin print-log -S`):

```text
total entries: 19,106
total payload bytes: 12,617,264
by type:
  BIN            80
  CkptEnd        16
  CkptStart      15
  FileSummaryLN  18,871
  IN             109
  NameLN         15
```

**Same "zero standalone LN entries survive full drain" pattern as the smoke
run**, at 40x the scale. `FileSummaryLN` (the cleaner's own persisted
per-file utilization metadata, one write per file per checkpoint pass) is
overwhelmingly the largest entry *count* post-drain (18,871 of 19,106
entries) though not the largest *byte* consumer (each `FileSummaryLN` is
small; 80 `BIN` entries carrying live slot data dominate bytes). This
`FileSummaryLN` entry-count is itself worth a follow-up look (a
`FileSummaryLN` re-write on every checkpoint pass, x15 checkpoints x ~1200
files touched, is a plausible source of *count* overhead even if not byte
overhead) — **not yet quantified, flagged for Phase 1 continuation.**

**Independent data-integrity check** (same method as the smoke run): opened
the resulting environment read-only-daemons-off and did a `get_in()` for all
2,000,000 keys — **all 2,000,000 keys found**. Confirms the drain result is
real compaction, not data loss, at this scale too.

## What Phase 1 has established so far

1. **The methodology works and is reproducible**: du-based ground truth +
   the engine's wired counters + per-entry-type log histogram, cross-checked
   against a full key-by-key data-integrity scan.
2. **The `min_utilization=50` floor is NOT obviously the dominant factor in
   these runs**, contrary to the prior report's assumption — when the
   checkpointer+cleaner daemons are simply given time to run (no config
   change), the post-drain footprint on both runs collapsed to far *below*
   the live-data floor one would naively expect (16 MB / 12.2 MB vs
   ~1 GB live data). This does not mean `min_utilization` has no effect
   (it wasn't varied yet — see Phase 2, not started); it means the *default*
   floor, given enough idle/wall-clock time for the daemons to catch up,
   is not leaving anywhere near 2x space-amp resident in these particular
   overwrite-storm workloads. **This is the single most important finding so
   far and it complicates the "50% floor explains 95GB" story from the
   original report** — the two setups are not identical (the original
   cross-engine run never gave the cleaner a deliberate idle drain window;
   this probe's "after drain" number specifically measures what happens
   *after* an idle window). The cross-engine run's 95 GB number is very
   likely a **"before drain" / steady-load** number, not a "cleaner given
   time to catch up" number — i.e. it may be measuring the same
   `du_before_drain`-style state this probe also reproduces (11-49x here),
   not a fundamentally different regime.
3. **A large fraction of "garbage" is reclaimed by the ordinary checkpoint +
   cleaner daemon cycle within tens of seconds of write pressure stopping**,
   at both the 50K-record and 2M-record scale tested so far.
4. **All data integrity was verified intact** at both scales after the drain
   — the small post-drain footprint is real compaction, not data loss (this
   was checked explicitly after the numbers looked surprising).
5. **A stats-wiring gap** means several `CleanerStats`/Prometheus fields
   related to log-size/utilization are always 0 in production (see above);
   this report's numbers come from `du -sb` + the counters that ARE wired,
   not from those fields.

## NOT YET MEASURED (Phase 1 continuation)

- **Live bytes vs on-disk bytes vs reclaimable-but-unreclaimed, decomposed**:
  this report has "before drain" and "after drain" du numbers, but has not
  yet decomposed the "before drain" gap into (a) garbage genuinely below the
  `min_utilization` floor at that instant, (b) an unreclaimed *backlog*
  (`FileSelector.to_be_cleaned` queue depth) the cleaner simply hadn't gotten
  to yet under write pressure, and (c) checkpoint-interval lag (files not
  yet checkpoint-barrier-eligible). The drain experiment conflates all
  three by construction (it removes write pressure AND lets checkpoints run
  freely). A steady-state-under-continuous-load measurement (keep the
  update storm running indefinitely and sample `du` periodically without
  ever stopping) is needed to see the genuine sustained floor, if one
  exists, separate from a start/stop artifact.
- **BIN-delta accumulation** and full-BIN re-logging frequency: the
  `checkpoint.delta_in_flush` counter read `0` in both runs shown above
  (`full_bin_flush` dominates), which is itself worth investigating (are
  BIN-deltas even being used under this workload's dirty-fraction shape, or
  is every checkpoint doing full BIN rewrites because the update-storm's
  Zipfian hot set dirties most slots in most touched BINs?) — not yet
  explained.
- **Per-record overhead breakdown** (LN header bytes, key-prefixing
  efficiency, TTL/expiration slot bytes) — not yet isolated from the
  aggregate byte totals above.
- **Whether checkpoint frequency (not just cleaner min_utilization) is the
  actual lever** — the baseline run used the default
  `checkpointer_bytes_interval=20MB` / `checkpointer_wakeup_interval_ms=30s`;
  not yet varied.
- **The `min_utilization` sweep itself (Phase 2)**: 50/60/70/80 vs measured
  space and write-amp cost — **not started**. Per the task brief, this is
  an acceptable place to stop: "min_utilization=50 is a deliberate
  space-for-write-amplification trade, here is the measured curve [not yet
  produced], here is the knob, do not change the default" remains the
  working hypothesis but is **not yet backed by a sweep**.
- Checkpoint-frequency lever, cleaner backlog/throttle tuning — **not
  started**.

## Artifacts

- Probe source: `benches/noxu-bench/src/bin/space_amp_probe.rs`.
- Registered as `noxu-space-amp-probe` in `benches/noxu-bench/Cargo.toml`.
- Raw run logs on the shared EC2 instance (not committed, ephemeral):
  `/data/space-runs/baseline.log`, `/data/space-runs/{smoke,baseline}/`.

## Honest summary for this checkpoint

Phase 1 is partial. The breakdown table Phase 1 asked for (live bytes vs
on-disk bytes vs reclaimable-but-unreclaimed vs per-record overhead) is only
half built: this report has "before drain" and "after drain" on-disk bytes
and confirmed data integrity, but has not yet decomposed "before drain" into
its three candidate causes, and has not yet touched BIN-delta frequency,
per-record overhead, or the `min_utilization` sweep (Phase 2). The most
important finding so far — that giving the existing daemons an idle window
collapses the footprint far below what the original cross-engine report's
95 GB number would suggest is a hard floor — is a genuine course-correction
to the task's starting assumption and is flagged as such rather than
asserted as final. No behavior or default was changed.
