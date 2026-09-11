# Space amplification: characterisation (2026-09)

Status: **Phase 1 (characterisation) complete for what it covers; Phase 2
(the min_utilization lever + a premise correction) complete.** This
document records every number measured so far, with the exact command used
to produce it, so the work survives even if the investigation is
interrupted. Sections
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

## Phase 2: the min_utilization lever, and a premise correction to Phase 1

Status: **complete.** Run on a **dedicated** `i4i.16xlarge` (64 vCPU, 3.4 TB
NVMe XFS at `/data`) with no other tenant — `uptime` shows load average under
1.1 throughout this session, so (unlike Phase 1) every number below,
including wall-clock/throughput, is clean, not order-of-magnitude.

### Premise correction (read this before the sweep numbers)

Phase 1's headline finding — "giving the existing daemons an idle window
collapses the footprint far below the naive live-data floor, with NO config
change" — is **not what actually happened**, and the mechanism matters for
everything that follows. This section documents how that was found and
verified; the sweep in the next section is designed around the corrected
understanding.

**The bug.** The cleaner decides whether a log file is cleanable by asking
its `UtilizationTracker` how many bytes in that file are obsolete. For a
record overwritten through the real `Environment`/`Database` API — an
explicit `Transaction`, or the synthetic auto-commit transaction that
`Database::put` wraps every call in — the prior LN version is **never**
counted obsolete, anywhere:

1. At log-write time (`crates/noxu-dbi/src/cursor_impl.rs::log_ln_write`),
   counting the prior version obsolete is correctly deferred to commit when
   this is the first write to the record within the transaction
   (`curr_ne_abort == false`) — faithful to JE's design (the prior version
   IS the transaction's abort/undo version until commit).
2. At commit time (`crates/noxu-txn/src/txn.rs::count_obsolete_abort_lsns`),
   the deferred entry is then **skipped** by this filter:
   ```rust
   if wli.abort_data.is_some() {
       // Embedded abort version — already counted obsolete at
       // logging time. (JE maybeCountObsoleteLSN: abortData != null.)
       continue;
   }
   ```
   `abort_data` is intended to mean "the LN was JE-style *embedded* in its
   parent BIN, so counting it obsolete already happened at logging time."
   But in Noxu, `abort_data` is unconditionally populated with the real
   before-image bytes on **every** overwrite (`CursorImpl::finalize_write_lock`
   passes `old_data` from `get_slot_before_image`, used for in-memory undo
   on abort) — not only when the LN is actually embedded. Noxu's own
   `embedded_ln` field is hard-coded `true` for a separate, already-flagged
   fidelity gap (see the `ponytail:` comment at `cursor_impl.rs` line ~3512),
   so it cannot be used to gate this. The filter therefore fires on **every**
   overwrite, and the prior version is never counted obsolete anywhere.

   A second, narrower instance of the same class of gap: `Checkpointer`'s
   full-BIN-flush path (`crates/noxu-recovery/src/checkpointer.rs
   ::flush_one_tree_bins`) re-logs every live slot's value inline and does
   **not** count any of the superseded standalone per-record LN entries
   obsolete (contrast with the evictor's analogous full-BIN-eviction path,
   `crates/noxu-evictor/src/evictor.rs` ~line 1355, which correctly counts
   the prior *full BIN* IN entry obsolete — a different, adjacent bookkeeping
   edge it does get right).

**Verified empirically**, three ways, with throwaway diagnostic binaries
(built, run, and deleted during this investigation — not part of the
committed probe suite):

1. 2,000 commits, each overwriting the same one key: after all 2,000,
   `obsolete_ln_count == 0` in the tracker; `get_obsolete_size()` reports
   only ~8% obsolete (leftover header/TxnCommit bytes), not the ~99.9%
   that 1,999 dead LN versions actually are.
2. A 20,000-record / 4-thread / 15-20s update-storm with the daemons running
   normally and **zero** manual calls of any kind: `du` is completely flat
   across 60s of passive polling (`cleaner_runs` climbs into the 70s-120s;
   `cleaner_deletions` stays 0). Reproduced with cache sizes from 2 MiB to
   64 MiB (i.e. independent of whether BIN eviction pressure is present),
   confirming the evictor's full-BIN-eviction obsolete-counting does not
   compensate.
3. The identical scenario, but replacing the passive daemon with a manual,
   forced `env.clean_log()` (which is `force=true` internally) reclaims the
   space on the very next call — `du` drops from hundreds of MB to the
   live-data floor within one or two forced passes. `force=true` bypasses
   the `min_utilization` tiers in `FileSelector::select_file_for_cleaning_
   with_policy` entirely (`forced { best_file? }` — the last tier, which
   ignores the aggregate utilization gate) and picks the lowest-scoring file
   unconditionally, so this works **despite** the tracker gap above, not
   because the gap doesn't matter.

**Why Phase 1 didn't see this.** Phase 1's drain loop called
`env.checkpoint(None)` and `env.clean_log()` *unconditionally on every poll*,
regardless of whether the daemons were doing anything. `Environment::
clean_log()` is *always* `force=true` internally (see its doc comment:
"performs a forced cleaning pass ... regardless of the daemon's utilization
budget"). So Phase 1's "give the already-running daemons an idle window, no
config change" framing was, in fact, continuously exercising the force=true
bypass path the entire time — the dramatic 12.2 MB / 16 MB post-drain
numbers are real (verified against data-loss separately, and that
verification stands), but they are not evidence that the **passive**,
`min_utilization`-gated daemon path reclaims anything. It does not, under
this workload shape, regardless of what `min_utilization` is set to — see
below.

**Existing test-suite blind spot.** Grepping the existing regression-test
suite for this exact scenario found every test that exercises
overwrite-driven obsolete tracking
(`overwrites_count_prior_versions_obsolete_per_db`,
`overwriting_all_records_makes_original_file_cleanable`,
`cleaner_sees_persisted_obsolete_immediately_after_restart` in
`crates/noxu-dbi/tests/integration_tests.rs`) uses `CursorImpl::with_log_manager`
directly, which sets `txn_ref: None` — a *different* code path from every
real `Environment`/`Database` call (which always attaches a real or
synthetic `Txn`, and therefore always hits the `Some(txn) =>` branch these
tests never exercise). The one CI test that runs the real API path under
sustained overwrites, `test_cleaner_reduces_log_files_under_load`
(`crates/noxu-db/tests/sustained_load_test.rs`), asserts only
`stats.cleaner.runs > 0 || stats.cleaner.deletions > 0` — and `runs`
increments on every daemon pass regardless of whether anything was
selected, so it passes whether or not the daemon ever actually cleans a
file. This is flagged here as a real test-suite gap, not fixed as part of
this task (out of scope — this task is measurement of the space-amp lever,
not a bug-fix task), but it explains how the underlying tracker gap survived
this long unnoticed.

**This does not reopen Phase 1's data-integrity claim** — the post-drain
footprints Phase 1 measured were verified byte-for-byte against a full
key-by-key scan, and that remains true; drained-via-`force=true` is still a
real, correct compaction. What changes is the *causal story*: "idle window,
no config change" is wrong; the correct story is "an operator-triggered
forced maintenance pass reclaims the space; the passive background daemon,
gated on `min_utilization`, currently does not, because of the tracker gap
above — independent of what the floor is set to." The `space_amp_probe`
binary was fixed to stop conflating the two
(`SAP_DRAIN_MODE=passive|forced`, see the source doc comment) so this
distinction cannot be silently re-lost.

### Consequence for the min_utilization sweep the task asked for

Because the tracker gap means `predicted_min_util` is ~100% for every file
under any sustained-overwrite workload regardless of the true obsolete
fraction, **the passive daemon path is expected to behave identically at
min_utilization = 40/50/60/70/80** — the knob controls a threshold that the
selection logic's input (predicted utilization) never drops below in this
workload shape, so it cannot bind. The sweep below measures this
directly (falsifying rather than assuming it) using `SAP_DRAIN_MODE=passive`,
and separately measures the `force=true` maintenance-window path (which
does NOT consult `min_utilization` at all — see the tier-4 `forced` branch
above — so sweeping the knob under `SAP_DRAIN_MODE=forced` is expected to
show **zero** sensitivity too, for a completely different reason: the forced
path bypasses the threshold rather than never crossing it). If both
predictions hold, the practical, honest structure of Part 2's answer is:
**`min_utilization` currently has no measurable effect via any code path
reachable from the public API under a sustained-overwrite workload**, until
the tracker gap above is fixed — which is a materially different, and more
important, finding than a write-amp-vs-space curve shape. Both the sweep
numbers and this prediction are reported below; the sweep is run regardless
of the prediction, per the task's "falsify by measurement" requirement.

### Part 1: decomposition, corrected

The task's Part 1 asked to decompose the before-drain gap into: garbage
below the `min_utilization` floor, cleaner backlog
(`FileSelector.to_be_cleaned`), checkpoint-eligibility lag, and per-record
overhead. A `cleaner_diagnostics()` accessor was added to `Environment`
(`crates/noxu-db/src/environment.rs`, diagnostic-only, not stable API) to
read the `FileSelector`'s pipeline-state counts and the merged per-file
utilization-summary map directly, rather than inferring them from `du`.

Given the premise correction above, the decomposition itself is now
simple to state precisely, and matches the code path exactly rather than
being inferred: **100% of the before-drain gap is "above the
`min_utilization` floor" per the (buggy) tracker's own numbers, 0% is
queued backlog, and 0% is checkpoint-lag** — not because there is no real
garbage (there manifestly is: forced cleaning reclaims 95%+ of it), but
because the tracker never learns about it in the first place, so it never
enters the `to_be_cleaned` queue or any per-file state above `Untracked`.
The `FileSelector` pipeline-state counts (`to_be_cleaned=0 being_cleaned=0
cleaned=0 checkpointed=0 safe_to_delete=0`) at both before-drain and
after-drain confirm this: the *selection* mechanism never engages at all
under `SAP_DRAIN_MODE=passive`, at any scale tested.

#### Data point: 2,000,000 records, min_utilization=50, passive vs forced (dedicated box)

```text
SAP_DIR=/data/space-runs/p1-passive SAP_RECORDS=2000000 SAP_VALUE=512 SAP_CACHE_MB=512 \
SAP_UPDATE_SECONDS=180 SAP_THREADS=16 SAP_MIN_UTIL=50 SAP_DRAIN_MODE=passive SAP_FINAL_CLEAN_PASSES=60 \
SAP_ADMIN_BIN=/data/work/target/release/noxu-admin \
./target/release/noxu-space-amp-probe
```

Live dataset: 2,000,000 records x 512 B = 1,024,000,000 bytes (0.95 GiB).
This box was idle otherwise (`uptime` load average ~0.1-1.1 throughout), so
the throughput number below is a clean, single-tenant number, unlike
Phase 1's.

| stage | du (bytes) | du (human) | space-amp vs live |
|---|---:|---:|---:|
| after load | 1,808,836,502 | 1.68 GiB | 1.77x |
| before drain (after 180s storm) | 20,432,825,010 | 19.03 GiB | 19.95x |
| after PASSIVE drain (daemons only, 0 manual calls, 10s polling) | 20,432,825,010 | 19.03 GiB | **19.95x (byte-for-byte unchanged)** |

Update-storm throughput: 30,147,063 writes in 180.0s = **167,478 writes/s**
(clean single-tenant number). `cleaner_runs=1728` before drain, climbing to
`1742` over 10s of polling — but `cleaner_deletions=0` in both snapshots,
and `du` is byte-for-byte identical before and after. `FileSelector` state
after drain: `to_be_cleaned=0 being_cleaned=0 cleaned=0 checkpointed=0
safe_to_delete=0` across 1,950 tracked files, every one reported "above the
50% floor" by the (buggy) tracker. This is the passive-path prediction
above: **confirmed, not assumed** — the daemon ran 1,742+ times over the
run and reclaimed nothing.

```text
SAP_DIR=/data/space-runs/p1-forced SAP_RECORDS=2000000 SAP_VALUE=512 SAP_CACHE_MB=512 \
SAP_UPDATE_SECONDS=180 SAP_THREADS=16 SAP_MIN_UTIL=50 SAP_DRAIN_MODE=forced SAP_FINAL_CLEAN_PASSES=40 \
SAP_ADMIN_BIN=/data/work/target/release/noxu-admin \
./target/release/noxu-space-amp-probe
```

| stage | du (bytes) | du (human) | space-amp vs live |
|---|---:|---:|---:|
| after load | 1,669,269,952 | 1.55 GiB | 1.63x |
| before drain (after 180s storm) | 20,483,174,967 | 19.08 GiB | 20.00x |
| after FORCED drain (40 manual force-checkpoint/clean_log rounds, 80s) | 14,558,692 | 13.9 MiB | **0.0142x** |

Update-storm throughput: 30,446,245 writes in 180.0s = 169,139 writes/s.
`cleaner_deletions` climbs from 0 to **41,702** during the forced-drain
phase; `checkpoints` from 1 to 61. `FileSelector.cleaned=2,051` at the end
(files cleaned in the LAST round, still mid the two-checkpoint barrier when
the 40-round budget ran out — forced mode never fully settles because each
forced checkpoint re-dirties a few BINs the cleaner's own LN migration just
touched; see the probe source comment for why this is expected). Post-drain
`noxu-admin print-log -S`: 125,522 entries / 12.9 MB, dominated by
`FileSummaryLN` (125,198 entries — the same "one `FileSummaryLN` write per
file per checkpoint pass" pattern Phase 1 flagged for follow-up, now seen at
61 checkpoint passes instead of 15) with only 33 `BIN` entries and zero
surviving standalone LN entries — consistent with Phase 1's
"full-BIN-carries-inline-values" finding at this larger scale and
forced-drain path too.

**Both runs together settle Part 1's original decomposition question
precisely**: the before-drain ~20x gap is not "50% below the floor, some %
checkpoint lag, some % backlog" as originally framed — it is **100%
garbage the tracker has never learned about**, full stop, because of the
tracker gap described above. `min_utilization`, the backlog queue, and the
checkpoint barrier are all downstream of a selection input (predicted
per-file utilization) that never reflects reality for this workload shape.
Forced cleaning still works (it bypasses that input entirely via the
tier-4 `forced` branch), which is why it reclaims 99.9%+ of the gap in one
maintenance window. The Part 1 "per-record overhead" question (LN header
bytes, key prefixing, TTL slots) is moot as a *separate* line item at this
workload's scale: post-forced-drain, standalone LN entries do not survive
at all (0 `INS_LN`/`UPD_LN` entries in either drain), so per-record
overhead is entirely subsumed into the BIN-inline-value mechanism —
consistent with, but (per Phase 1's flag, still standing) not a substitute
for, a targeted unit test directly against the BIN-serialization code path.

### Part 2: the min_utilization lever — measured, not assumed

Sweep at 50/60/70/80 (and, cheaply, 40) under BOTH drain modes, same
workload shape as the Part 1 data point (2,000,000 records / 512B / 180s
storm / 16 threads), one data point per cell, run once each (see caveat
below on why 3x repetition was not done for every cell).

#### Space (steady-state after drain, and peak during storm)

| min_utilization | peak du (storm) | space-amp peak | passive after-drain | space-amp passive | forced after-drain | space-amp forced |
|---:|---:|---:|---:|---:|---:|---:|
| 40 | 20,905,733,554 | 20.42x | 20,905,733,554 | 20.42x (unchanged) | 14,426,890 | 0.0141x |
| 50 | 20,653,668,456 | 20.17x | 20,653,668,456 | 20.17x (unchanged) | 14,558,692 | 0.0142x |
| 60 | 21,041,119,998 | 20.55x | 21,041,119,998 | 20.55x (unchanged) | 18,086,883 | 0.0177x |
| 70 | 20,926,505,659 | 20.44x | 20,926,505,659 | 20.44x (unchanged) | 15,740,460 | 0.0154x |
| 80 | 21,059,741,626 | 20.57x | 21,059,741,626 | 20.57x (unchanged) | 16,965,156 | 0.0166x |

**Passive-path result (confirmed as predicted, not assumed):** every one of
the 5 values gave `cleaner_deletions=0` and byte-for-byte
`du_before_drain == du_after_drain`. The small (~2%) variance in the
before-drain numbers across rows is run-to-run Zipfian/thread-scheduling
noise (each row is one 180s storm; not the effect of the knob), not signal
— this is the same order as the passive-vs-passive variance seen re-running
the same `min_utilization=50` config twice in the Part 1 section above
(19.95x vs 20.17x). **The passive daemon path is completely insensitive to
`min_utilization`** under this workload, exactly as the tracker-gap analysis
predicted: the input the threshold gates on never crosses any of these five
values, so there is nothing to sweep on that path.

#### Write amplification and cleaner cost

| min_utilization | write_amp storm-only (passive) | write_amp storm+forced-drain | cleaner_deletions (forced drain) | checkpoints (forced drain) |
|---:|---:|---:|---:|---:|
| 40 | 1.2066 | 1.2724 | 40,589 | 60 |
| 50 | 1.2066 | 1.2730 | 41,702 | 61 |
| 60 | 1.2070 | 1.2726 | 39,973 | 60 |
| 70 | 1.2069 | 1.2723 | 42,116 | 61 |
| 80 | 1.2070 | 1.2730 | 41,945 | 61 |

**Write-amp result:** the storm-only write_amp (passive column, no cleaning
ever happens) is ~1.207 regardless of `min_utilization`, as expected — the
knob cannot affect a phase where the cleaner never engages. The
storm+forced-drain write_amp is ~1.272-1.273 for every value, with no
monotonic trend across 40->80 (60 and 80 are marginally higher than 50 and
70, inside run-to-run noise, not a floor-height effect). The ~0.065
difference between the two columns (the "cost of actually cleaning") is
**constant across min_utilization values** in this measurement, because
`force=true` cleaning does the same amount of work (drain to the same
near-live floor) regardless of the configured floor — the floor is simply
not consulted on the tier-4 forced path (`FileSelector::
select_file_for_cleaning_with_policy`'s `forced { best_file? }` branch
unconditionally selects the lowest-utilization file; `min_utilization_pct`
is passed into the function but only read on the non-forced tiers). Checkpoint
counts (60-61) and deletions (~40k) are likewise flat across all 5 values,
within noise.

**Interpretation**: under the current engine, `min_utilization` measurably
affects **neither** code path reachable from either drain mode, for this
workload. The passive path never engages (tracker gap). The forced path
always fully engages, regardless of the floor (force=true bypasses the
floor). There is, right now, no code path in which changing
`min_utilization` from 40 to 80 changes measured space, write-amp, or
cleaner cost under a sustained-overwrite workload — which is a stronger and
more surprising finding than "the curve is flat because 50 is already a good
default"; the curve is flat because the mechanism that would make it a curve
is not currently wired to fire under sustained overwrites.

#### Throughput (steady-phase YCSB A, xbench)

| min_utilization | throughput median (ops/s) | write_amp median | p99 median (µs) |
|---:|---:|---:|---:|
| 40 | 475,593 | 1.207 | 181 |
| 50 | 486,032 | 1.206 | 181 |
| 60 | 489,116 | 1.206 | 188 |
| 70 | 486,510 | 1.206 | 185 |
| 80 | 479,908 | 1.206 | 185 |

Method: `noxu-xbench`, `BENCH_WORKLOAD=ycsb_a` (50% reads / 50% writes,
Zipfian θ=0.99), `BENCH_RECORDS=2000000 BENCH_VALUE=512 BENCH_CACHE=512MiB
BENCH_THREADS=32 BENCH_SECONDS=30 BENCH_DURABILITY=NO_SYNC`, one load
(`BENCH_SKIP_LOAD=0`, seed=1) then 3 measured passes
(`BENCH_SKIP_LOAD=1`, seeds 1/2/3) per `min_utilization` value, interleaved
across values (all 5 values' rep 1 first, then all 5 values' rep 2, etc. —
not one value's 3 reps back-to-back) per the task's interleaving
requirement; median of the 3 reps reported. Every run measures only the
30s steady/measured phase (the separate load phase is excluded from the
reported numbers by construction — `BENCH_DIR` is reused with
`BENCH_SKIP_LOAD=1` so reps 2 and 3 start from the SAME on-disk state rep 1
left, not a fresh load each time).

**Result: all 5 values are within ~3% of each other** (475,593 to 489,116
ops/s; p99 181-188µs; write_amp 1.206-1.207 flat) — no monotonic trend, no
value stands out, consistent with the space/write-amp results above: at
this workload's default checkpoint interval (`checkpointer_bytes_
interval=20MB`, unmodified), the cleaner's forced/passive engagement during
the 30s measured window is dominated by other costs (BIN flush, fsync-free
NO_SYNC write path) that `min_utilization` does not touch. The daemon runs
with `force=false` throughout this benchmark (xbench never calls
`clean_log()`), so this result is fully consistent with the passive-path
finding above: the knob has no measurable throughput cost OR benefit here
because the passive cleaner never meaningfully engages during a 30s
steady-phase window regardless of its setting.

#### The opposite direction: lowering the floor (min_utilization=20)

```text
SAP_DIR=/data/space-runs/sweep-20-forced SAP_RECORDS=2000000 SAP_VALUE=512 SAP_CACHE_MB=512 \
SAP_UPDATE_SECONDS=180 SAP_THREADS=16 SAP_MIN_UTIL=20 SAP_DRAIN_MODE=forced SAP_FINAL_CLEAN_PASSES=40 \
SAP_ADMIN_BIN=/data/work/target/release/noxu-admin \
./target/release/noxu-space-amp-probe
```

| min_utilization | passive after-drain | space-amp passive | forced after-drain | space-amp forced |
|---:|---:|---:|---:|---:|
| 20 | 20,747,598,394 (unchanged from 20,932,520,581 before-drain) | 20.26x | 17,360,292 | 0.0170x |

Same story as 40-80: passive is byte-for-byte flat (`cleaner_deletions=0`),
forced drains to the same ~0.014-0.018x band every other value produced.
**Lowering the floor to 20 costs nothing and gains nothing** in this
measurement, for the identical structural reason as the 40-80 sweep: the
passive path's selection input never reflects real utilization, and the
forced path never consults the floor at all.

## NOT YET MEASURED (Phase 1 continuation, still open after Phase 2)

- **BIN-delta accumulation** and full-BIN re-logging frequency: the
  `checkpoint.delta_in_flush` counter reads `0` in every run in this report
  (`full_bin_flush` dominates throughout Phase 1 AND Phase 2), which is
  itself worth investigating (are BIN-deltas even being used under this
  workload's dirty-fraction shape, or is every checkpoint doing full BIN
  rewrites because the update-storm's Zipfian hot set dirties most slots in
  most touched BINs?) — not yet explained, unchanged from Phase 1.
- **Per-record overhead breakdown** (LN header bytes, key-prefixing
  efficiency, TTL/expiration slot bytes) as an *independent* line item is
  now understood to be moot at this workload's scale (Phase 2's Part 1
  section: standalone LN entries do not survive any drain, forced or
  passive, once a file is actually cleaned) — but the underlying
  BIN-serialization inline-value claim itself is still not confirmed by a
  targeted unit test, unchanged from Phase 1's flag.
- **The tracker gap itself is not fixed** (see Phase 2's premise-correction
  section) — flagged as a real, separate bug (obsolete LN versions are
  never counted for any real Environment/Database write), out of scope for
  this measurement task. A fix would need to either (a) make `abort_data`
  carry a genuine embedded-vs-not-embedded flag instead of always being
  `Some`, or (b) stop trusting `abort_data.is_some()` as that signal and use
  the real `embedded_ln` flag once it reflects true embedding (also
  currently hard-coded `true`, a related, already-flagged gap). Either fix
  would very likely change the numbers in this report's Phase 2 section —
  once fixed, the min_utilization sweep would need to be **re-run**, since
  the current flat curves are a direct consequence of the passive path
  never engaging, not evidence the floor is unimportant once tracking works.
- **Whether checkpoint frequency (not just cleaner min_utilization) is the
  actual lever** — the default `checkpointer_bytes_interval=20MB` /
  `checkpointer_wakeup_interval_ms=30s` was not varied in Phase 2 either;
  still open.
- Checkpoint-frequency lever, cleaner backlog/throttle tuning — still open.
- **Steady-state-under-continuous-load** (never stopping the storm, just
  sampling `du` periodically) — still not measured; Phase 2's drain-mode
  split answers a different, and now more important, question (passive vs
  forced), but not this one.

## Recommendation (Phase 2 conclusion)

**Do not change the `min_utilization` default. It is JE-faithful (50) and
this investigation found no measured case, in either direction (40 or 20
vs 80), where changing it moved space, write-amp, cleaner cost, or
throughput under a sustained-overwrite workload** — see the sweep tables
above. That is not "50 happens to be optimal"; it is "the knob is not
currently wired to a code path that engages under this workload shape,"
which is a fundamentally different and more important statement, described
in full in the premise-correction section above. Changing the default in
this state would be cargo-culting a number that currently does nothing
under the exact workload this investigation used to test it — the opposite
of "a strong measured justification."

**Ranked recommendations, with measured cost of each:**

1. **Leave `min_utilization=50` as the default.** No cost, no benefit,
   confirmed by measurement — this is the safe, JE-faithful, "wait and
   don't touch it" choice. Users who want space reclaimed under sustained
   overwrites should call `Environment::clean_log()` (or
   `checkpoint(force=true)` then `clean_log()`) periodically or on a
   maintenance-window schedule — this is a real, working, measured path
   (0.014-0.018x space-amp across every floor value tested, 20 through 80)
   that does not require any default change, only an operational habit
   (e.g. a periodic maintenance-window daemon, or calling it from an
   idle-detection hook). Cost: the write-amp during the forced-drain window
   itself is higher (~1.27 vs ~1.21 during the storm) because cleaning IS
   real I/O work — but it is bounded, one-time, and schedulable, unlike an
   always-on lever.
2. **File a follow-up bug for the tracker gap** (obsolete LN versions never
   counted for real API writes — see premise-correction section). This is
   the actual lever that matters; `min_utilization` cannot be meaningfully
   evaluated as a space/write-amp trade-off until it is fixed, because right
   now there is no code path where the trade-off exists to measure. This is
   flagged, not fixed, per this task's scope (measurement, not a bug-fix
   task) — but it is the single most actionable finding in this report and
   should be prioritized ahead of any further `min_utilization` tuning work.
3. **Do not chase the write-amp-vs-space curve shape as originally framed**
   by the task brief ("does raising the floor buy meaningful space, or does
   it just burn write bandwidth re-cleaning files that were about to become
   garbage anyway?") — under the current engine, **neither** happens: it
   buys nothing and burns nothing, because the mechanism that would make it
   do either is not engaging. This question becomes meaningful again only
   after the tracker gap is fixed, and should be re-run at that point (the
   probe and sweep infrastructure built in Phase 2 make that a cheap re-run,
   not a from-scratch investigation).
4. **After a tracker-gap fix, re-run this exact sweep** (same probe, same
   `SAP_DRAIN_MODE=passive` — that is the regime a real fix would change)
   before drawing any conclusion about whether `min_utilization` needs
   operator tuning guidance beyond "leave it at 50." The forced-drain
   numbers in this report (0.014-0.018x, flat across 20-80) would likely
   stay similarly flat even after a fix, since `force=true` bypasses the
   floor by design (JE-faithful) — the passive numbers are the ones that
   would actually change and need re-measuring.

## Artifacts

- Probe source: `benches/noxu-bench/src/bin/space_amp_probe.rs`
  (`SAP_DRAIN_MODE=passive|forced`, added in Phase 2).
- `noxu-xbench` (`benches/noxu-bench/src/bin/xbench.rs`) gained
  `BENCH_MIN_UTIL` in Phase 2 for the throughput sweep.
- `Environment::cleaner_diagnostics()` (`crates/noxu-db/src/environment.rs`)
  and `Cleaner::get_file_selector_stats()` /
  `Cleaner::get_merged_file_summary_map()`
  (`crates/noxu-cleaner/src/cleaner.rs`), added in Phase 2 for the
  decomposition. Diagnostic-only, not stable API.
- Raw run logs on the EC2 instance (not committed, ephemeral):
  `/data/space-runs/{baseline,smoke,p1-passive,p1-forced,sweep-*}.log` and
  the matching `/data/space-runs/{name}/` environment directories.

## Honest summary for this checkpoint

Phase 1 established the methodology (du-based ground truth, wired
counters, per-entry-type log histogram, data-integrity verification) and
flagged, but did not fully explain, why post-drain footprints collapsed
far below the naive live-data floor. Phase 2 found and confirmed by
measurement that Phase 1's explanation for that collapse was wrong: it
attributed the collapse to "give the daemons an idle window, no config
change," but the actual mechanism was `Environment::clean_log()`'s
always-`force=true` internal behaviour, which bypasses `min_utilization`
entirely — a consequence of a real tracker bug (obsolete LN versions are
never counted for writes through the real API) that makes the passive,
`min_utilization`-gated cleaner path never engage under sustained
overwrites, at any floor setting from 20 to 80. The task's headline
deliverable — a `min_utilization` sweep showing space vs write-amp vs
throughput — was produced and is flat in every dimension, for a structural
reason now identified and documented rather than assumed. The
recommendation is to leave the default unchanged, file the tracker gap as
a separate follow-up, and treat the sweep as not-yet-meaningful until that
gap is fixed. No behavior or default was changed by this investigation;
this document is measurement and analysis only.

## Phase 3: the tracker bug, root-caused and measured (2026-09)

Phase 2 identified that the `UtilizationTracker` never learns an overwritten
record's prior version is garbage. This phase traced *why*, end to end, and
measured what fixing it costs.

### It was three defects, not one

Phase 2 named the `abort_data.is_some()` filter. That is real, but it is not the
whole story, and it is not even the dominant one. Tracing with instrumentation
rather than inspection found three independent blockers on the path from
"overwrite a record" to "the cleaner knows":

1. **`abort_data` is not a valid proxy for "embedded".** JE's
   `Txn.maybeCountObsoleteLSN` skips counting when `getAbortData() != null`,
   which is sound *in JE* because it assigns `abortData` only inside
   `if (bin.isEmbeddedLN(idx))` (`CursorImpl.java:3328`). Noxu populates
   `abort_data` on every overwrite because the in-memory undo path needs the
   before-image unconditionally, so the proxy is always true. Fixed by tracking
   the question explicitly as `WriteLockInfo::abort_counted_at_log_time`.

2. **Explicit transactions had no `LogManager`.** `TxnManager::begin_txn` built a
   `Txn` via `Txn::new`, and `count_obsolete_abort_lsns` early-returns without
   one — so it never reached any filter. `Txn::with_log_manager` was called only
   from unit tests.

3. **THE DOMINANT ONE: auto-commit never called the counting at all.**
   `commit_append_phase` is guarded by
   `if self.has_logged_entries() && !self.is_auto_txn()`, and the counting lived
   inside that block. Since `Database::put`/`del` wrap *every* call in a
   synthetic auto-txn, the dominant write path skipped it entirely. This is why
   the earlier `obs-probe` measured `obsolete_ln_count = 0` after 2,000
   overwrites.

Defect 3 is invisible to inspection of the filter, which is where the first two
phases were looking. It only surfaced by instrumenting the call and observing
that it never fired.

### The measured cost of fixing it, and why it is gated off

With all three corrected (`ycsb_a`, 8 threads, 100k records, 512-byte values,
`NO_SYNC`, local NVMe):

| configuration | throughput | on-disk |
|---|---:|---:|
| counting off (shipped default) | 267k–318k ops/s | 2,126–2,506 MB |
| counting on | 7.6k–9.5k ops/s | 187–206 MB |

**A ~12× space reduction for a ~33× throughput regression.** Neither side of that
is acceptable as a default, so both counting paths ship gated behind
`NOXU_COUNT_AUTOCOMMIT_OBSOLETE` and `NOXU_COUNT_TXN_OBSOLETE`, off by default,
with the measurement recorded at each site.

### The real blocker: no `LocalUtilizationTracker`

The regression is not the counting arithmetic — it is contention.
`UtilizationTrackerObserver::count_obsolete` takes a **global mutex**
(`self.tracker.lock()`), and correct counting takes it once per commit. At ~300k
ops/s across 8 threads that mutex is the bottleneck.

JE does not have this problem because it has a piece we lack:
`LocalUtilizationTracker` / `BaseLocalUtilizationTracker`, which accumulate
per-thread and merge into the shared tracker in batches, so the shared lock is
taken once per batch instead of once per operation.

**That is the actual work item**, and it is now the blocker for everything else
here: the `min_utilization` sweep cannot be re-run meaningfully until obsolete
counting can be enabled by default, and it cannot be enabled by default until the
local tracker exists.

### Status

- Root cause: fully understood, all three defects fixed in code.
- Correctness: proven by `utilization_obsolete_counting_test`, which drives
  `Database::put` (the real path) rather than `CursorImpl::with_log_manager`, and
  which guards BOTH directions — that overwrites are counted, and that
  single-write records are *not* (over-counting would let the cleaner discard live
  data).
- Default behaviour: unchanged, deliberately.
- Next: port `LocalUtilizationTracker`, then flip the gates and re-run Phase 2.

## Phase 4: counting enabled by default, and a corrected space figure (2026-09-11)

Phase 3 fixed obsolete counting but shipped it gated because per-commit counting
took the global tracker mutex once per commit (~33× throughput cost). Phase 4
removes the gates.

### What made it affordable

Two changes, and the second matters more than the first:

1. **Batched per-txn merge.** Obsolete LSNs accumulate in the transaction and
   merge into the shared tracker once, at commit, instead of once per counted
   record.
2. **Merge AFTER write-lock release.** The shared-tracker acquisition was moved
   to after the per-record write locks are dropped, so it no longer serialises
   behind lock holders. This removes a convoy rather than amortising a mutex, and
   is the larger of the two effects.

### A double-commit-frame bug the gate removal exposed

Attaching a `LogManager` to the inner `noxu_txn::Txn` — needed so it can merge
obsolete LSNs — made its own `commit()`/`abort()` write a `TxnCommit`/`TxnAbort`
WAL frame *in addition* to the one the outer `noxu_db::Transaction` already
writes: a second frame, under a colliding txn-id namespace, hardcoded to
`CommitSync`, blind to the caller's `read_only` flag. Latent (nobody set the old
env gate), the removal made it fire by default. Fixed with a suppress-own-end-frame
flag; `txn_end_frame_dedup_test` pins exactly one frame per op.

### The space figure, corrected

Phase 3 reported ~12× (2,126 MB → 187 MB). **That 187 MB was an artifact of the
double-frame bug.** With counting forced on but ungated, the duplicate frame's
hardcoded `CommitSync` forced a synchronous fsync per commit regardless of the
`NO_SYNC` benchmark setting, so only ~15k writes landed in the 25 s window instead
of ~1.4M. The small footprint was two orders of magnitude fewer writes, not better
reclamation.

Corrected, three fresh runs of the same command (`ycsb_a`, 100k records, 25 s,
8 threads, 512 B, `NO_SYNC`), on current HEAD:

| configuration | on-disk | writes |
|---|---:|---:|
| counting OFF | 1,061–1,142 MB | ~1.4M |
| counting ON (default) | 644–667 MB | ~1.27M |

**~1.6–1.7×, not 12×.** Byte measurements (`du -sb`) are immune to CPU
contention, so these are trustworthy even though the box was loaded; the
throughput A/B (indicative ~100k ops/s on the loaded box) still owes a
clean-hardware confirmation.

### Lesson

The 12× was never real — it measured writes that never happened. A space figure
taken from a run whose throughput collapsed is meaningless, because on-disk size
tracks write volume. Always report the committed-write count alongside any
footprint number, and be suspicious of a space win that coincides with a
throughput drop: it is usually the same phenomenon seen twice.
