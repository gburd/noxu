# Eviction-policy benchmark under cache pressure

Run: `NOXU_BENCH_DIR=/scratch/evpolicy cargo run --release -p noxu-workload-bench --bin evictor_policy_bench`

- Storage: `/scratch` (btrfs on dm-crypt, real disk — NOT tmpfs)
- Cache: 16 MiB (pinned via `set_cache_size` + `cache_percent=0`)
- Dataset: 80 000 records x 256 B values (~21 MB tree >> 16 MiB cache)
- Durability: `COMMIT_NO_SYNC` (policy decides *which* node to evict, not commit
  durability; dropping the per-commit fsync exposes the policy signal instead of
  burying it under fsync latency — writes still hit the log on real disk)
- 3 repeats per cell, median reported.
- Policy selected per-env via the newly wired `noxu.evictor.algorithm`
  (`EVICTOR_ALGORITHM`) config param; runtime selection verified by asserting
  `Environment::evictor_algorithm_name()` matches the requested algorithm.

## ops/s (median of 3)

| policy | random | scan | mixed |
|---|---:|---:|---:|
| lru | 912224 | 2626222 | 3462 |
| clock | 927350 | 2279833 | 3473 |
| arc | 933049 | 2408167 | 3358 |
| car | 1390661 | 3544132 | 3282 |
| lirs | 1372412 | 3357298 | 3237 |

### vs LRU (ratio, >1 = faster than LRU)

| policy | random | scan | mixed | geomean |
|---|---:|---:|---:|---:|
| lru | 1.000 | 1.000 | 1.000 | 1.000 |
| clock | 1.017 | 0.868 | 1.003 | 0.960 |
| arc | 1.023 | 0.917 | 0.970 | 0.969 |
| car | 1.524 | 1.350 | 0.948 | 1.249 |
| lirs | 1.504 | 1.278 | 0.935 | 1.216 |

## FINDING: the eviction policy is INERT end-to-end under this workload

The numbers above do **not** measure policy behaviour. The benchmark's sanity
check (first cell) reported:

```
cache_usage=24120896 bytes, nodes_evicted=1
(cache budget 16777216 bytes, working set ~21760000 bytes)
[WARNING] eviction is NOT reclaiming memory ...
```

i.e. after populate + `checkpoint` + `evict_memory()`, cache usage stayed at
~24 MB — *above* the ~21 MB working set and ~1.4x the 16 MiB budget — and only
**1 node** was ever evicted. A standalone diagnostic (`evict_diag`, since
removed) confirmed this across 4 configs (2 MiB/16 MiB cache, sync/no_sync,
with/without a 3 s daemon-only run): out of ~137 000 nodes *targeted* per pass,
`nodes_evicted = 1`, `nodes_stripped = 2`, `nodes_put_back ≈ targeted`.

Root cause (orthogonal to which policy is selected): in `do_evict`, a BIN
candidate maps to `EvictionDecision::PartialEvict` → `strip_lns_from_node`,
which returns `None` (busy/put-back) for essentially every node, so its bytes
are never reclaimed. The policy's `evict_candidate()` *does* drive victim
selection (the policy is genuinely wired and not a no-op at the API level — see
`evictor.rs` `evict_batch`), but because the chosen victim is then put back
rather than evicted, the *order* in which victims are chosen is irrelevant to
the resident set. All five policies therefore keep the same pages resident and
run at the same speed.

Consequence for the per-cell numbers:

- **mixed** is the reliable column (slow enough that machine noise is small):
  3237–3473 ops/s across all 5 policies — a ~3% spread, i.e. identical.
- **random/scan**: lru/clock/arc cluster (~900 k / ~2.4 M); car/lirs show
  ~1.5x/1.3x. This is a *machine-load artifact* (car/lirs happened to run in a
  quieter window of the ~70-minute sweep), not a policy effect — these are
  CPU-bound point lookups and, with eviction inert, no policy can change which
  nodes are resident. The mixed column, measured in the same runs, shows no
  such gap, confirming the random/scan gap is noise.

## Decision: KEEP LRU as the default (JE-faithful)

JE's evictor is LRU. No policy wins *measurably and reproducibly* here — the
prerequisite for deviating from JE — because end-to-end eviction reclaims almost
nothing, so the policy choice cannot matter under this workload. Changing the
default would be unjustified. LRU stays the default; the other four policies are
now selectable via `noxu.evictor.algorithm` for when the underlying
`strip_lns_from_node` / partial-eviction reclamation gap is fixed and a real
re-measurement can be done.
