# Cache eviction policies

Noxu DB evicts in-memory tree nodes (INs, BINs, and their embedded LN data)
when the cache exceeds its configured memory budget, mirroring BDB JE's
`Evictor`. The eviction *policy* decides which resident node leaves first.

## Default: LRU (the only tier-one policy)

The default and only production-supported policy is **LRU** — least-recently-
used with the tree-level priorities JE's `LRUEvictor` uses (internal nodes are
retained over leaf nodes so the upper tree stays hot). It is selected by
`EVICTOR_ALGORITHM=lru` (the default) and needs no feature flag.

LRU is the JE-faithful choice: JE's cache is an LRU-with-priorities well matched
to B-tree working sets, and it is the policy Noxu's eviction↔cleaner↔checkpoint
interaction tests validate.

## Experimental policies (feature-gated, off by default)

CLOCK, LIRS, ARC, CAR, and CoolHot (a COOL/HOT 2-bit cooling clock) are
preserved in the tree but compile only under the non-default cargo feature
`experimental-eviction-policies` (exposed as `noxu/experimental-eviction-
policies`). With the feature off they are absent from the build and the default
test matrix; `EvictionAlgorithm::from_name` falls back to LRU (with a warning)
for one of their names.

They are gated rather than removed because:

- No published benchmark yet demonstrates a workload where they beat LRU on
  Noxu's B-tree cache. Noxu's own benchmarking found CoolHot did not move
  throughput on the measured read/mixed workloads.
- Each additional policy multiplies the eviction↔cleaner↔checkpoint interaction
  surface — historically the most defect-prone area of this class of engine —
  so keeping them out of the default surface reduces the tier-one risk while
  preserving the code for future evidence-gathering.

To use one, build with the feature and set the algorithm:

```toml
[dependencies]
noxu = { version = "7", features = ["experimental-eviction-policies"] }
```

```rust
# use noxu::EnvironmentConfig;
# let mut config = EnvironmentConfig::default();
config.set_evictor_algorithm("coolhot"); // or clock | lirs | arc | car
```

## Intellectual-property note (ARC / CAR)

The **ARC** (Adaptive Replacement Cache, Megiddo & Modha, 2003) and **CAR**
(Clock with Adaptive Replacement, Bansal & Modha, 2004) algorithms were the
subject of IBM patents. The earliest of those patents have expired or are near
expiry, but deployments should confirm the current IP status for their
jurisdiction before enabling `arc` or `car` in a product. This note is provided
for prudence and is not legal advice. LRU, CLOCK, LIRS, and CoolHot are not
encumbered by these patents.
