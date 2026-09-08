# `noxu-sync` vs `parking_lot`: publish the numbers or retire it (2026-09)

**Status:** measurement complete. **Recommendation: RETIRE** the custom
`RwLock`; retire the `Mutex` on a slower schedule or keep it behind a
documented, narrow justification. See [Recommendation](#recommendation).

## The question

An external engineering review flagged `noxu-sync` as one of two divergences
"hardest to justify on engineering-value grounds":

> Custom futex-based sync layer (`noxu-sync`) replacing `parking_lot` on hot
> paths... Plausible as a performance play, but it concentrates unsafe FFI in
> the most concurrency-critical layer, it is implicated in the TSAN
> suppressions, and it is the stated reason fair latches (a JE feature) cannot
> be implemented. Unless benchmarks show a decisive win over `parking_lot` on
> realistic workloads, this is complexity purchased at the cost of a JE feature
> and of auditability. **Publish the numbers or retire it.**

This document is those numbers. The headline result is not the throughput
comparison the review anticipated: it is that `noxu-sync`'s `RwLock` admits
**unbounded writer starvation**, which is a correctness-grade defect
independent of performance.

## Method

Measurement box: AWS `i4i.16xlarge`, 64 vCPU (Intel Xeon Platinum 8375C
@ 2.90 GHz, 32 physical cores x 2 SMT, single NUMA node), 495 GiB RAM,
XFS on NVMe at `/data` (verified `xfs`, not tmpfs), kernel 6.1.182 (AL2023),
rustc 1.95.0, `--release` (`lto = "thin"`, `codegen-units = 1`). The box was
otherwise idle; load average before each run is recorded in
`benches/results/syncbench/log.txt`.

Both implementations expose `lock_api::RawMutex` / `lock_api::RawRwLock`, so
every regime is written **once**, generic over the raw primitive, and
monomorphised per side inside the *same* `lock_api::Mutex` / `RwLock` wrapper.
No wrapper overhead is attributed to either side, and the measured code path
is the same shape for both.

Three harnesses, all in `crates/noxu-sync/benches/`:

| Harness | What it answers |
|---|---|
| `sync_vs_parking_lot.rs` (criterion) | ns/op per regime: uncontended, contended mutex (short/long critical section), rwlock read-only and read-heavy, at 2/4/8/16/32/64 threads |
| `sync_fairness.rs` | Throughput **and** fairness in one run: per-thread acquisition distribution (max/min, CoV, starvation count) plus a single-victim tail-latency probe |
| writer-starvation probe (`/tmp`, output archived) | One writer vs N continuous readers — isolates the defect found below |

Every number below is the **median of 3 independent runs**, with the
run-to-run spread quoted. Raw output: `benches/results/syncbench/`.

### Two harness bugs found and fixed (kept on the record)

Both bugs favoured `noxu-sync`. Finding them is why the published numbers
should be trusted more than a single clean-looking run.

**Bug 1 — per-thread quotas rewarded barging.** The first driver gave each
worker a fixed op quota and let it exit when finished. A barging (non-fair)
lock lets one thread race ahead, complete, and leave, which *decays the
offered concurrency* for the remaining threads and inflates measured
throughput. Since non-fairness is precisely the property under review, the
harness was paying the measured thing a bonus.

The artifact is visible in the retained output
(`bench-BIASED-run1-methodology-error.txt`): `noxu-sync`'s contended mutex
appears to get **faster** as threads increase — 194 -> 108 -> 99 ns/op at
16/32/64 threads — while `parking_lot` degrades monotonically (656 -> 829 ->
870). A lock does not speed up under more contention. That non-monotonicity is
the signature; an "8x win" read off it would have been an artifact. The file is
kept deliberately rather than deleted.

**Bug 2 — makespan semantics deadlocked.** The fix for bug 1 was to have
finished workers keep issuing uncounted ops until the slowest worker completed
its quota. That holds concurrency at N, but requires *every individual* thread
to finish. Under a lock that starves one participant, the harness hangs: the
criterion bench deadlocked in its own self-check for 15+ minutes, 7 threads
spinning and 1 parked in `futex_wait_queue`. **That hang was the first
symptom of the real defect**, initially misdiagnosed as mutex barging (the
starvation probe later refuted that) and correctly traced to rwlock writer
starvation.

Final driver: workers claim 256-op chunks from **one shared budget**.
Concurrency stays at N while work remains, but no individual thread must make
progress, so unfairness is *measured* rather than rewarded or fatal. The
self-check now runs under a 60 s watchdog that reports a livelock instead of
blocking.

## Result 1 — uncontended: a wash (the case that matters most)

The warm read path is overwhelmingly uncontended, so this is where a custom
lock would have to earn its keep. Filler work inside the critical section is
swept (0/8/32 iterations) and a `no_lock` arm (identical body over a `Cell`)
controls for harness overhead.

Median ns/op, 3 runs, spread <= 0.1 % on every lock arm:

| Regime | work | `noxu_sync` | `parking_lot` | `no_lock` | noxu cost | pl cost |
|---|---:|---:|---:|---:|---:|---:|
| mutex | 0 | 15.37 | **14.08** | 2.12 | 13.25 | 11.96 |
| mutex | 8 | **14.05** | 14.09 | 3.17 | 10.88 | 10.92 |
| mutex | 32 | **14.04** | 14.49 | 6.58 | 7.46 | 7.91 |
| rwlock read | 0 | **16.05** | 17.10 | 2.12 | 13.93 | 14.98 |
| rwlock read | 8 | **16.16** | 17.07 | 3.17 | 12.99 | 13.90 |
| rwlock read | 32 | **18.13** | 19.02 | 6.58 | 11.55 | 12.44 |
| rwlock write | 0 | **8.30** | 14.09 | 2.13 | 6.17 | 11.96 |
| rwlock write | 8 | **8.59** | 14.21 | 3.17 | 5.42 | 11.04 |
| rwlock write | 32 | **8.61** | 14.45 | 6.58 | 2.03 | 7.87 |

The work sweep moves the `no_lock` arm by the expected amount (2.12 -> 3.17 ->
6.58 ns) and moves both lock arms consistently, so the measurement is tracking
real work rather than criterion loop noise — the absolute numbers are sound.

Reading the lock cost (`impl - no_lock`):

- **Mutex: a wash.** 13.25 vs 11.96 ns uncontended (`parking_lot` ~1.3 ns
  *faster*); at 8 and 32 work units they are within 0.5 ns, i.e. within
  measurement resolution. `noxu-sync`'s mutex buys **nothing** on the
  uncontended path. Its `lock()` does an extra `owner.store(thread_id())` on
  every acquire, which is the ~1.3 ns.
- **RwLock read: a wash, marginally favouring `noxu-sync`** (~1.0 ns, ~6 %).
  Real but not decisive.
- **RwLock write: `noxu-sync` genuinely ~2x cheaper** (6.17 vs 11.96 ns). This
  is the one clean uncontended win — and see Result 3 for what it costs.

**Finding:** on the path where the engine spends most of its time, the custom
layer is a wash. That alone removes the primary performance justification.

## Result 2 — contended mutex: `noxu-sync` wins big above 16 threads

Aggregate ns/op (reciprocal of system-wide throughput), median of 3:

| threads | `noxu_sync` | spread | `parking_lot` | spread | `parking_lot` / `noxu_sync` |
|---:|---:|---:|---:|---:|---:|
| 2 | 57.0 | 8.7 % | **36.4** | 2.8 % | 0.64x |
| 4 | 106.3 | 2.6 % | **63.8** | 9.7 % | 0.60x |
| 8 | 155.2 | 19.9 % | **136.2** | 30.7 % | 0.88x |
| 16 | **197.2** | 0.9 % | 708.3 | 2.3 % | **3.59x** |
| 32 | **105.9** | 5.5 % | 886.0 | 10.5 % | **8.37x** |
| 64 | **101.5** | 6.3 % | 993.7 | 15.6 % | **9.79x** |

Long critical section (~100 ns) is the same shape: 0.46x at 2 threads, 3.44x
at 16, **6.42x** at 64.

This is a large, reproducible win — and it is real, not the bug-1 artifact
(the shared-budget driver holds concurrency at N; `noxu-sync` is now
non-monotonic only in the mild sense that 32/64 threads beat 16, discussed
below). At low thread counts (2–8) `parking_lot` is up to 1.6x **faster**.

The crossover is explained by mechanism, and it is the same mechanism in both
directions: `noxu-sync` spins 40 iterations then parks on a futex, and its
`unlock` is an unconditional `swap` + `futex_wake(1)`. At high thread counts
that converges on a *small effective working set* — a handful of threads
ping-pong the lock on-CPU while the rest sleep in the kernel, which is
excellent for aggregate throughput. `parking_lot`'s eventual-fairness handoff
(~every 0.5 ms it passes ownership directly to the queue head instead of
releasing for a race) deliberately spends throughput to bound starvation, and
that cost grows with the waiter count.

So the mutex win at high contention is **the fairness mechanism's absence,
measured as throughput**. That is a real engineering trade, not a free lunch.

### Where a contended mutex win would actually matter

`noxu_sync::Mutex` guards the **Log Write Latch** (`noxu-log`'s
`log_manager.rs:161`), the engine's single global write-path serialisation
point, plus evictor/cleaner state and `noxu-rep` (18 `Mutex` + 7 `RwLock`).
The LWL is held for LSN assignment and an in-memory memcpy and released before
`pwrite64`, so it is a short critical section under potentially high writer
concurrency — genuinely the shape where the 16+ thread win applies. This is
the strongest argument for keeping the custom mutex, and it is why the
recommendation below treats the mutex and the rwlock differently.

## Result 3 — the decisive finding: unbounded writer starvation

`noxu-sync`'s `RwLock` is documented non-fair *by design*
(`raw_rwlock.rs:20`: "Non-fair design: new readers are not blocked by pending
writers"), with bit 31 `WRITE_WAITING` marked "reserved, not currently used".
`lock_exclusive_slow` can only CAS when `state == 0`, and nothing ever blocks
incoming readers — so a continuous reader stream keeps the reader count above
zero and the writer waits forever.

Dedicated probe: one writer, N continuous readers, 3 s window.

| readers | impl | writer acquisitions | max writer wait |
|---:|---|---:|---:|
| 3 | `noxu_sync` | 699,462 | 0.04 ms |
| 3 | `parking_lot` | 8,026,945 | 0.02 ms |
| 7 | `noxu_sync` | **385** | **3000.20 ms** |
| 7 | `parking_lot` | 2,741,663 | 0.21 ms |
| 15 | `noxu_sync` | **1** | **3000.24 ms** |
| 15 | `parking_lot` | 1,037,412 | 0.38 ms |
| 31 | `noxu_sync` | **5** | **3000.22 ms** |
| 31 | `parking_lot` | 876,974 | 0.64 ms |
| 63 | `noxu_sync` | **1** | **3001.95 ms** |
| 63 | `parking_lot` | 330,314 | 0.55 ms |

At >= 7 concurrent readers the writer receives **essentially zero service for
the entire window** — a max wait equal to the full window means it never
acquired after its first attempt. `parking_lot`, in identical conditions,
serves ~1 million writes with a sub-millisecond worst case. The gap is ~6
orders of magnitude, and it is not a tuning difference: one lock bounds writer
wait, the other does not bound it at all.

This is also the true cause of harness bug 2's deadlock.

### Production exposure

This is not a synthetic-only concern. `noxu_sync::RwLock` instances with a
read-mostly-plus-writers shape, reached by bare `.write()` calls with **no
timeout**:

| Site | Shape | Consequence of starvation |
|---|---|---|
| `noxu-dbi/src/db_tree.rs:19,21` (`name_to_id`, `id_to_db`) | 7 read sites, 5 write sites | `create_database` / `remove_database` / `rename` can stall indefinitely while concurrent cursors read |
| `noxu-txn/src/txn_manager.rs:45` (`all_txns`) | read-heavy, 5 write sites | `register_txn` / `unregister_txn` stall under a read-heavy txn census |

`noxu-latch`'s `SharedLatch` is *less* exposed: it acquires via
`try_read_for(timeout)` / `try_write_for(timeout)`, so starvation there
surfaces as a diagnosable `LatchTimeout` rather than an indefinite hang. The
bare-`.write()` sites above have no such backstop.

Note the tree's node latches are **not** affected — see the scope correction
below.

### The mutex, by contrast, is fine on fairness

The single-victim tail-latency probe (1 light victim that yields, vs N-1
hammers) shows `noxu-sync`'s **mutex** is not the problem — it is *better*
than `parking_lot` here:

| threads | impl | victim acqs | median wait | p99 wait | max wait |
|---:|---|---:|---:|---:|---:|
| 8 | `noxu_sync` | 771,530 | 1.2 us | 11.3 us | **35.0 us** |
| 8 | `parking_lot` | 367,817 | 1.2 us | 31.3 us | 113.2 us |
| 32 | `noxu_sync` | 182,785 | 0.66 us | 40.3 us | **140.1 us** |
| 32 | `parking_lot` | 66,974 | 0.47 us | 200.5 us | 591.8 us |
| 64 | `noxu_sync` | 84,065 | 0.36 us | 98.7 us | **377.3 us** |
| 64 | `parking_lot` | 30,598 | 0.54 us | 499.2 us | **7.53 ms** |

No thread was starved (0 starved threads, max/min <= 2.3, CoV <= 0.26) for
either mutex at any thread count. `parking_lot`'s worse tail is the cost of
its fair-handoff mechanism (a handoff parks the current holder's successor
deterministically, adding latency to *some* acquisitions).

This is the honest, and slightly counter-intuitive, summary: **the custom
mutex is fair enough and fast under contention; the custom rwlock is the
liability.** A blanket "noxu-sync is unfair" claim would be wrong.

## Result 4 — contended rwlock: no throughput case

| threads | read-only `noxu` | read-only `pl` | read-heavy `noxu` | read-heavy `pl` |
|---:|---:|---:|---:|---:|
| 2 | 57.1 | 56.7 | 60.9 | **53.8** |
| 8 | 53.6 | 59.6 | **54.5** | 60.4 |
| 16 | 53.2 | 59.3 | 56.5 | 57.7 |
| 32 | 55.0 | 55.5 | **58.6** | 97.3 |
| 64 | 74.6 | 74.4 | **75.9** | 145.1 |

100 % readers: **indistinguishable** (0.99x–1.11x, inside the 15–28 %
run-to-run spread on the `parking_lot` side). Read-heavy: `noxu-sync` leads
1.66x at 32 and 1.91x at 64 threads — but that advantage *is* the writer
starvation. `noxu-sync` posts better aggregate numbers because its writers
are not being served; the 1-in-1024 writer ops that `parking_lot` completes
are precisely what costs it throughput. Quoting 1.91x as a win would be
quoting the defect as a feature.

**Finding:** the rwlock has no defensible throughput case. Read-only is a
wash; the read-heavy "win" is starvation measured as throughput.

## Scope correction: the review's premise is partly inaccurate

The review says the custom layer replaced `parking_lot` "on hot paths". For
the hottest path, the opposite is true — worth recording because it changes
the cost/benefit:

- **B-tree node latches are already `parking_lot::RwLock`.**
  `noxu-tree/src/tree.rs:44` is a literal `use parking_lot::RwLock`, and
  `noxu-tree/src/lib.rs:116` re-exports `parking_lot::RwLock as NodeRwLock`
  for downstream crates. `grep -c noxu_sync crates/noxu-tree/src/tree.rs` is
  **0**. The hand-over-hand read descent needs
  `read_arc()` / `ArcRwLockReadGuard` (owned guards), which `parking_lot`
  provides and `noxu-sync` does not implement. The engine has, in effect,
  already run this A/B for its hottest lock and chose `parking_lot`.
- **`noxu-latch` has only five instantiation sites engine-wide**: the tree's
  `root_latch` (constructed, but no acquire site found on any path),
  `file_manager`'s latch, per-`FileHandle` latches, and the `LogBufferPool`
  latch. "10 crates depend on `noxu-latch`" overstates hot-path exposure.
- Where `noxu-sync` genuinely is load-bearing: `noxu-log` (LWL, buffer pool,
  and direct `futex_wait`/`futex_wake` on the pin-count word), `noxu-txn`
  (`all_txns`), `noxu-dbi` (`db_tree`, cursor/`DatabaseImpl` via
  `dst_sync_pl`), evictor/cleaner mutexes, and `noxu-rep`.

## What the custom layer costs

- **Production `unsafe` surface in `noxu-sync`:** 10 `unsafe` blocks +
  4 `unsafe impl` + 3 `unsafe fn` = **17 items**, of which only **3** carry a
  `SAFETY:` comment. In particular `futex.rs`'s two raw `libc::syscall` FFI
  blocks — the most safety-critical code in the crate — have **no `SAFETY:`
  comment at all**. This contradicts `AGENTS.md`'s claim that "every
  production `unsafe` block has a `// SAFETY: …` comment" and is an
  auditability finding in its own right, independent of the retire decision.
- **TSAN suppressions:** the `race:std::thread::local` suppression covers,
  among others, `raw_mutex.rs`'s `thread_id()` thread-local — machinery that
  exists only to support `get_owner()`/`get_n_waiters()`.
- **`env_fair_latches` is blocked** (documented in
  `docs/src/operations/known-limitations.md`). The measurements show the
  situation is worse than "fair latches unavailable": the rwlock lacks even
  *writer preference*, which is strictly weaker than fairness and is what
  prevents the starvation above.
- **Diagnostics actually used:** `get_n_waiters()`, `get_owner()`,
  `reader_count()`, `is_locked_exclusive()` have **zero callers outside
  `noxu-sync`'s own tests**. The only genuine external consumer of the crate's
  extra API is `noxu-log/src/log_buffer.rs`, which imports
  `futex::{futex_wait, futex_wake}` directly.

## Recommendation

**RETIRE the custom `RwLock`. Treat the `Mutex` separately.**

The rwlock decision is not a performance judgement and should not wait on
one: it admits unbounded writer starvation, it has reachable production sites
with no timeout backstop, and its only throughput advantage is that defect
being measured as throughput. `parking_lot::RwLock` is a drop-in
`lock_api::RawRwLock` replacement, is ~1 ns slower on uncontended reads
(6 %, in exchange for bounded writer wait), and is already the engine's choice
for its hottest latch.

The mutex is a genuine trade and can be decided on engineering grounds:

| | Keep custom mutex | Retire to `parking_lot` |
|---|---|---|
| Uncontended | wash (~1.3 ns slower) | wash |
| Contended <= 8 threads | up to 1.6x slower | faster |
| Contended >= 16 threads | 3.6x–9.8x faster | slower |
| Tail latency | better (377 us vs 7.53 ms @ 64T) | worse |
| `unsafe` surface | retained | deleted |
| Fairness | no mechanism, but no starvation observed | eventual fairness |

Keeping it is defensible **if and only if** the LWL genuinely sees >= 16-way
writer concurrency in production; that is an engine-level question this
primitive benchmark cannot answer (see the gap below). If it does not, the
mutex is also a wash and should follow the rwlock out.

### Retiring: what it involves

Smaller than the review assumes, because the blast radius is narrower:

1. **RwLock (do this first, independent of the mutex).** Swap
   `noxu_sync::RwLock` -> `parking_lot::RwLock` in `noxu-util`'s
   `dst_sync_pl` re-export (`crates/noxu-util/src/dst_sync_pl.rs`, the
   `#[cfg(not(noxu_shuttle))]` arm) plus the direct importers
   (`noxu-dbi/db_tree.rs`, `noxu-txn/txn_manager.rs`, `noxu-engine`,
   `noxu-db`, `noxu-rep`). Both are `parking_lot`-shaped already
   (`.read()`/`.write()` return the guard), so call sites do not change.
   Verify no caller uses `reader_count()` / `is_locked_exclusive()` /
   `get_n_waiters()` — currently none do.
2. **Mutex (if retiring).** Same swap for `noxu_sync::Mutex`. `Condvar` goes
   with it; `parking_lot::Condvar` has the same `wait_for(&mut guard, dur)`
   shape that `dst_sync_pl` was built around.
3. **Carve-out: keep `futex.rs`.** `noxu-log/src/log_buffer.rs` waits on the
   *pin-count word itself*, not on a lock — `futex_wait(&write_pin_count,
   pins, ...)`. `parking_lot` cannot express "block on this arbitrary atomic
   until it changes". Either keep a minimal `noxu-sync` reduced to `futex.rs`
   (2 `unsafe` FFI blocks, which should finally get `SAFETY:` comments), or
   move those two functions into `noxu-log`. Deleting them is not an option
   without redesigning the buffer-pin protocol.
4. **Payoff:** ~15 of 17 `unsafe` items deleted; the
   `race:std::thread::local` TSAN suppression's `noxu-sync` justification
   disappears; `env_fair_latches` becomes implementable (`parking_lot`
   provides `unlock_fair` / fair handoff); and the writer-starvation exposure
   is closed.

Do **not** retire in the same change as this measurement — that was out of
scope here by instruction. This document is the input to that decision.

## What was NOT measured

- **No engine-level A/B.** There is no switch to swap `noxu-latch`'s inner
  primitive, and building one is a 10-crate change — explicitly out of scope
  as too risky for a benchmark. So the *end-to-end* effect on
  `ycsb_c` / `ycsb_a` / `tdb_write` throughput is **unknown**. This matters
  most for the mutex recommendation: whether the LWL's 16+ thread win shows up
  as engine throughput is unanswered. The clean way to get it is to add a
  swap seam behind `noxu-latch` + `dst_sync_pl` as the first step of the
  retirement, then A/B with the seam.
- **No claim about which primitive dominates a real workload profile.** No
  `perf` profile of a running engine was taken; the "where it matters"
  mapping above is from source inspection, not sampled cost.
- **Single host, single architecture.** 64-vCPU Intel Ice Lake, one NUMA node.
  `futex_wait`/`futex_wake` behaviour is Linux-specific, and `noxu-sync`'s
  non-Linux fallback (a `park_timeout` spin) was not benchmarked at all — it
  is plausibly much worse than `parking_lot` on macOS/Windows, which is a
  further argument for retirement if those platforms matter.
- **Starvation probe is a lower bound.** The 3 s window shows the writer
  starved for the *whole window*; the true unbounded-ness follows from the
  algorithm (no reader-blocking mechanism), not from a longer measurement.
- **`Condvar` was not benchmarked.** It is used by `noxu-txn`'s lock manager
  and `noxu-rep`; it would ride along with a mutex retirement.
