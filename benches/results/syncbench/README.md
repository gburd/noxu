# `noxu-sync` vs `parking_lot` raw measurement output

Measurement box: AWS `i4i.16xlarge`, 64 vCPU (Intel Xeon Platinum 8375C
@ 2.90 GHz, 32 physical cores x 2 SMT, single NUMA node), 495 GiB RAM,
kernel 6.1.182 (AL2023), rustc 1.95.0, `--release`. Box otherwise idle
(load average before each run recorded in `log.txt`).

Verdict and analysis: `docs/src/internal/noxu-sync-vs-parking-lot-2026-09.md`.

| File | Contents |
|---|---|
| `bench-v3run{1,2,3}.txt` | criterion A/B, 3 independent runs (the published numbers) |
| `fairness-v3run{1,2,3}.txt` | throughput + fairness + starvation probe, 3 runs |
| `writer-starvation.txt` | dedicated rwlock writer-starvation probe (the decisive result) |
| `log.txt` | per-run host load before/after, build output |
| `bench-BIASED-run1-methodology-error.txt` | **DO NOT CITE** — kept deliberately as a documented methodology error; see the "Two harness bugs" section of the verdict doc |

## Why the biased run is retained

Its threaded numbers came from a driver that let each worker exit after a
fixed op quota, so a barging lock could shed contention early and inflate its
own throughput. The tell is visible in the file itself: `noxu-sync`'s
contended mutex appears to get *faster* from 16 to 64 threads (194 -> 108 ->
99 ns/op). Locks do not speed up under more contention. It is kept so the next
person to benchmark locks here recognises the signature rather than
rediscovering it.
