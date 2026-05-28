# Wave 11-H — Performance Investigation on JE-Wins Workloads

**Status.** Complete (this wave produces a report, not optimizations).
**Branch.** `fix/wave11-h-redo-perf-investigation` off `sprint/v2.3.1-base`.
**Inputs.** v2.3.0 benchmark report (`docs/src/operations/benchmarks.md`),
`docs/src/internal/post-v2.3.0-roadmap.md`.
**Outputs.** Per-workload root cause + concrete optimization
hypothesis + ROI plan for waves 11-I (cursor/BIN), 11-J (fsync), and
11-K (log scanner).

## Methodology

For each of the four JE-wins workloads (W03 sequential read, W04
random read, W10 4r4w/8r8w concurrent, W11 recovery) we built a
single-shot profiler binary (`benches/profiles/`,
`noxu-perf-profiler`) under the `bench-profile` Cargo profile
(release codegen + `debug = true`), recorded a CPU profile with
`perf record --call-graph dwarf -F 999`, and inspected the top-
self-time frames and their caller chains.  All captures live in
`benches/profiles/captures/<workload>/{top_self.txt,calltree.txt}`
together with a README describing how to regenerate them.

JE comparison points were read from `_/je/src/com/sleepycat/je/`
(read-only; archive not committed — `_/` is gitignored).  The two
JE classes most relevant to this investigation are
`com.sleepycat.je.tree.IN` (`findEntry`) and
`com.sleepycat.je.recovery.RecoveryManager`.

The hardware/OS the captures were taken on is the same machine that
produced the v2.3.0 benchmark numbers (Intel Core Ultra 7 258V, 8
physical cores, NixOS 25.11, `tmpfs` for the database directory).
On `tmpfs` `fdatasync` is effectively free, so W10's gap on this
substrate is not about fsync — see the W10 section.

> **Reminder.** Wave 11-H does not change production code.  All
> findings below are turned into hypotheses for waves 11-I/J/K.

---

## W03 / W04 — Single-threaded reads (Noxu ~1.9× slower)

### Where the time is going

`perf report --no-children --percent-limit 0.5` (W03, 4 k samples,
2 M reads in 2.4 s):

| % self | Frame |
|---:|---|
| 15.09% | `__memcmp_avx2_movbe` *(libc)* |
| 5.18%  | `noxu_dbi::cursor_impl::CursorImpl::find_bin_for_key` |
| 3.21%  | `noxu_tree::tree::Tree::search` |
| 3.19%  | `_int_malloc`, 2.55% `malloc` *(libc)* |
| 2.97%  | `noxu_txn::lock_manager::LockManager::lock_with_timeout` |
| 2.60%  | `crc32fast::baseline::update_fast_16` |
| 2.43%  | `noxu_dbi::cursor_impl::CursorImpl::get_data_from_tree` |
| 2.43%  | `noxu_txn::lock_manager::LockManager::release` |

The W04 (random-read) profile is structurally identical: 13.65 %
`__memcmp_avx2_movbe`, 5.40 % `find_bin_for_key`, 2.70 %
`get_data_from_tree`, 1.69 % `Tree::search`.

### Root cause #1 — duplicate descent on every read

`Database::get` (in `noxu-db`) calls `Cursor::search(SearchMode::Set)`,
which is implemented in `crates/noxu-dbi/src/cursor_impl.rs:693`.
Stripped to its essentials:

```rust
let found = {
    let db = self.db_impl.read();
    if let Some(tree) = db.get_real_tree() {
        tree.search(key)                              // descent #1
            .map(|sr| sr.exact_parent_found)
            .unwrap_or(false)
    } else { false }
};
match search_mode {
    SearchMode::Set | SearchMode::Both => {
        if found {
            let result: Option<(Vec<u8>, u64)> = {
                let db = self.db_impl.read();
                if let Some(tree) = db.get_real_tree() {
                    Self::get_data_from_tree(tree, key)   // descent #2
                } else { None }
            };
            …
```

The first descent (`Tree::search`) only learns whether the key
exists.  The second descent (`get_data_from_tree`) re-runs the
whole root→IN→BIN walk to grab the slot's data and LSN for locking.
Two complete tree descents per `get()`.

### Root cause #2 — linear scan in `get_data_from_tree` and `find_bin_for_key`

`get_data_from_tree` (cursor_impl.rs:1023) finds the entry inside
the BIN with `iter().find(...)`:

```rust
bin.entries
    .iter()
    .find(|e| e.key.as_slice() == suffix.as_slice())
```

This is the call site that the call graph attaches to the 15.09 %
`__memcmp_avx2_movbe` self-time:

```text
  --0.56%-- equal_same_length<u8,u8>  (inlined)
            …
            find<…cursor_impl::{impl#0}::get_data_from_tree…>
            get_data_from_tree (inlined)
```

A BIN holds up to ~128 sorted entries; an `iter().find()` over them
is `O(n)` average ~64 comparisons.  Worse, `find_bin_for_key`
(cursor_impl.rs:1882) descends through internal nodes with another
linear scan:

```rust
let mut idx = 0usize;
for (i, entry) in n.entries.iter().enumerate() {
    if i == 0 { idx = 0; }
    else if entry.key.as_slice() <= key { idx = i; }
    else { break; }
}
```

For a 100 K-record B+tree the typical fanout is ~128, descent
depth ~3 — every read does ~3 × 64 + 64 ≈ 256 comparisons just to
locate the slot.

The infrastructure for a binary search already exists:
`BinStub::find_entry_compressed` (`noxu-tree/src/tree.rs:414`)
binary-searches by `entries.binary_search_by(|e| e.key.as_slice().cmp(suffix))`.
`Tree::search` itself uses it.  But the cursor's data-fetch path
re-implements its own descent and skips it.

### What JE does

`com.sleepycat.je.tree.IN.findEntry(byte[] key, …)` is a single
binary search used for *every* node type, including the BIN:

```java
int high = nEntries - 1;
int low  = 0;
int middle = 0;
while (low <= high) {
    middle = (high + low) / 2;
    int s = entryKeys.compareKeys(key, middle, …);
    …
}
```

The JE cursor descends once via `Tree.search`, returns a
`(BIN, slotIndex)` pair, and reads the slot's LSN/data in place.
There is no second descent and no linear BIN scan.

### Optimization hypothesis (→ wave 11-I)

1. **Fold the two descents into one.**  Make `Tree::search` return
   a `BIN` reference (or a `BinSearchResult` carrying the slot's LSN
   and an `Option<&[u8]>` for embedded data) and let
   `Cursor::search` use it directly instead of re-descending via
   `get_data_from_tree`.  Where the cursor needs to drop the read
   guard before locking, capture `(slot_lsn, slot_data_clone)`
   under the guard then release.
2. **Replace the linear scans in `find_bin_for_key` and
   `get_data_from_tree`.**  For internal-node descent, use
   `entries.binary_search_by(|e| e.key.as_slice().cmp(key))` with
   the same "slot 0 is virtual −∞" treatment that
   `IN.findEntry` already encodes.  For BIN slot lookup, call the
   existing `BinStub::find_entry_compressed`.
3. **Move the format-`{:010}`-key allocation out of the timed
   loop.**  W03/W04 also spend ~3 % in `pad_integral` /
   `Display::fmt::<usize>` on the bench side; this is a benchmark
   artifact, not a Noxu issue, and JE has the same per-iteration
   `String.format` cost — flagged here only so the 11-I numbers do
   not credit Noxu twice.

### Estimated LOC range

50–150 LOC, almost entirely inside
`crates/noxu-dbi/src/cursor_impl.rs` and `crates/noxu-tree/src/tree.rs`
(public surface: a new `Tree::search_with_data` returning the slot
LSN + data bytes; existing `Tree::search` retained for
`key_exists_in_view`).  No on-disk format change, no new API
surface in `noxu-db`.

### Confidence

**High.**  The linear-scan and double-descent are both visible in
the profile *and* exist in the source today.  Removing one descent
roughly halves the read path's tree-walk cost; replacing the BIN
scan with binary search drops the BIN-level comparisons from ~64 to
~7 on a 128-entry BIN.  Combined we expect the W03 gap (1.92×) to
close to ≤ 1.20× — exactly the 11-I acceptance gate.

### Correctness risk

**Low / pure refactor.**  The new `Tree::search_with_data` call
must release the BIN read guard before the cursor calls `lock_ln`,
matching today's "release-then-lock" sequence in
`get_data_from_tree`.  Existing tests in
`crates/noxu-dbi/tests/`, `crates/noxu-db/tests/`, and
`crates/noxu-tree/src/tree.rs` (search-by-key) cover the merged
path.  Add one new property test: for a populated tree, the slot
LSN + data returned by the new combined call must equal those
obtained by separate `Tree::search` + `get_data_from_tree` calls.

---

## W10 — Concurrent 4r/4w and 8r/8w (Noxu 1.5–2.4× slower on tmpfs)

### Where the time is going

`perf report` (W10 8r8w, 16 k samples, 200 K ops in 35 s):

| % self | Frame |
|---:|---|
| 15.45% | `__memcmp_avx2_movbe` *(libc)* |
| 7.90%  | `noxu_sync::raw_mutex::NoxuRawMutex::lock_slow` |
| 3.12%  | `noxu_txn::lock_manager::LockManager::lock_with_timeout` |
| 2.76%  | `__memmove_avx_unaligned_erms` *(libc)* |
| 2.60%  | `crc32fast::baseline::update_fast_16` |
| 2.39%  | `malloc` *(libc)* |
| 2.35%  | `noxu_tree::tree::BinStub::find_entry_compressed` |
| 2.10%  | `noxu_dbi::cursor_impl::CursorImpl::find_bin_for_key` |
| 1.86%  | `noxu_db::database::Database::put` |
| 1.41%  | `std::thread::current::current` |
| 1.36%  | `noxu_log::log_manager::LogManager::log_internal` |
| **1.10%** | `noxu_log::fsync_manager::FsyncManager::fsync` |
| **1.01%** | `syscall` *(libc)* |

### Root cause: contention is on Noxu's mutexes, not on fsync

The roadmap entry framed W10 as an fsync-coalescing problem.  The
profile says **otherwise on tmpfs**.  `FsyncManager::fsync` (1.10 %)
plus the underlying `syscall` (1.01 %) account for ~2 % of CPU
time; the dominant costs are:

* **`NoxuRawMutex::lock_slow` (7.90 %).**  This is the futex slow
  path of `noxu-sync`'s raw mutex.  Inspecting `perf script` shows
  the contended mutexes include the `Mutex<FsyncState>` inside
  `FsyncManager`, the `Mutex<…HashMap…>` in `EnvironmentImpl`'s
  database registry (`reserve_rehash_inner` is visible in the
  call traces), and the mutex protecting the LogManager's pending
  buffers.
* **Tree-side memcmp (15.45 %).**  This is the same linear-BIN-
  scan / linear-IN-descent cost that dominates W03/W04 — and on
  W10 it's *amplified* because every reader and writer thread is
  hammering the same hot BINs.  Halving the per-op CPU cost for
  the read/write path also halves the effective lock hold time.
* **Allocation churn (~5 % across `malloc` + `cfree` + memmove).**
  `Database::put` and the cursor allocate new `Vec<u8>` for keys
  and values on every operation; `LogManager::log_internal`
  allocates per-record buffers.  Under contention these
  allocations all hit the same heap arenas.
* **`std::thread::current::current` (1.41 %) and
  `noxu_sync::raw_mutex::thread_id` (0.80 %).**  Every locker label
  is keyed by the thread ID, and the current implementation hashes
  via `thread::current()` which clones an `Arc<Thread>`.  Under
  heavy concurrency this is a measurable overhead.

### What JE does

JE's `LockManager` keeps a thread-local `LockerImpl` and avoids the
hashmap lookup on every lock.  JE's `LogBufferPool` flushes via a
single `LogFlusher` thread that pulls pending commits off a
lock-free queue, so writer threads do not contend on a global mutex
while waiting for fsync.  And JE's `IN.findEntry` is binary, not
linear, which keeps each individual op's BIN time short.

### Optimization hypothesis (→ wave 11-J, plus a J-prerequisite from 11-I)

1. **W10 will benefit substantially from 11-I alone.**  The 15.45 %
   memcmp + 2.10 % `find_bin_for_key` + 2.35 %
   `find_entry_compressed` all shrink with the cursor/BIN
   optimization above, and that win is independent of fsync.
   Estimate: 11-I alone closes ~30–40 % of the W10 gap on tmpfs.
2. **For the NVMe story (11-C output) the real fsync coalescing
   work belongs in 11-J.**  The hot mutex inside `FsyncManager`
   (`Mutex<FsyncState>` plus its leader-elect condvar) serializes
   leaders; on real storage where each fdatasync blocks for tens
   of microseconds this serialization is the binding constraint.
   The fix is to:
   * Replace the leader-election condvar with a single-writer
     lock-free queue of `FsyncWaiter`s (see JE's `LogFlusher`),
   * Cap the leader's wait window at one fdatasync round-trip so
     that more committers accumulate per syscall without inflating
     latency.
3. **Reduce allocator pressure on the put path.**  `Database::put`
   currently allocates two `Vec<u8>` for `(key, data)` per call.
   A pooled per-thread `Vec<u8>` (or accepting `&[u8]` end-to-end)
   eliminates ~3 % of the W10 CPU.  Keep this as a 11-J stretch
   item; it's likely small but pervasive.

### Estimated LOC range

* Cursor/BIN portion (covered by 11-I): see W03/W04 above.
* Fsync rework (11-J proper): 200–400 LOC inside
  `crates/noxu-log/src/fsync_manager.rs` and one or two callers in
  `noxu-log::log_manager` and `noxu-txn::txn`.  No public API
  change; the change is to the internal coalescing primitive.
* Allocator-pressure stretch: 100–200 LOC across `noxu-db` and
  `noxu-dbi`.

### Confidence

* W10 closes by ~30–40 % from 11-I alone: **high.**
* W10 closes to within 1.3× of JE on real NVMe after 11-J: **medium.**
  The acceptance gate (per the roadmap) is conditional on the 11-C
  NVMe re-run numbers, not the tmpfs numbers we have here.  If the
  11-C numbers show Noxu *already* closer on NVMe (likely, given
  Noxu 's group-commit window is non-zero there), the 11-J scope
  may shrink.

### Correctness risk

**Medium — needs new tests.**  Replacing the FsyncManager's
condvar-based leader election with a lock-free queue is a
concurrency change that needs a stateright spec or a new property
test ("every committed transaction's LSN is fsync'd before
`txn.commit()` returns").  Cursor/BIN refactor risk is the same as
the W03/W04 entry above (low, pure refactor).

---

## W11 — Recovery / re-open after clean close (Noxu 2.9× slower)

### Where the time is going

`perf report` (W11, 2 k samples, 30 re-opens in 6.4 s — 213 ms
each):

| % self | Frame |
|---:|---|
| 11.85% | `malloc` *(libc)* |
| 8.94%  | `__memcmp_avx2_movbe` *(libc)* |
| 7.36%  | `_int_free` |
| 6.54%  | `noxu_tree::tree::Tree::insert_recursive` |
| 5.12%  | `_int_malloc` |
| 4.27%  | `malloc_consolidate` |
| 3.80%  | `noxu_tree::tree::BinStub::insert_with_prefix` |
| 3.61%  | `__memmove_avx_unaligned_erms` |
| 3.64%  | `bytes::bytes::owned_drop` |
| 3.21%  | `unlink_chunk.isra.0` *(libc allocator)* |
| 3.12%  | `noxu_tree::tree::Tree::insert` |
| 3.02%  | `bytes::bytes::owned_clone` |
| 2.84%  | `noxu_dbi::file_manager_scanner::FileManagerLogScanner::parse_entry_from_bytes` |
| 1.32%  | `noxu_dbi::file_manager_scanner::FileManagerLogScanner::scan_files_forward` |

### Root cause: per-record allocation in redo

Recovery's redo path is `noxu_recovery::recovery_manager::redo_ln`
(crates/noxu-recovery/src/recovery_manager.rs:1120).  For every
LN log record the implementation does:

```rust
let data = rec.data.as_deref().map(<[u8]>::to_vec).unwrap_or_default();
…
tree.insert(rec.key.to_vec(), data, lsn);
```

Two allocations (`rec.key.to_vec()` and `rec.data.…to_vec()`) per
LN record, plus a third inside `Tree::insert` →
`BinStub::insert_with_prefix` when the slot needs a new
`BinEntry { key: Vec<u8>, data: Option<Vec<u8>>, … }`.  At 100 K
records that's ≥ 300 K small allocations during recovery.

The allocator profile (~28 % combined: 11.85 % `malloc`, 7.36 %
`_int_free`, 5.12 % `_int_malloc`, 4.27 % `malloc_consolidate`)
plus `bytes::owned_drop`/`owned_clone` (~6.6 %) is consistent with
that allocation-per-record pattern; the actual logical work
(`Tree::insert_recursive` 6.54 %, `BinStub::insert_with_prefix`
3.80 %, parse 2.84 %) is small in comparison.

The log scanner itself (`FileManagerLogScanner::scan_files_forward`
1.32 %, `parse_entry_from_bytes` 2.84 %) is **not** the bottleneck
— it already mmaps (via `Bytes::slice`) and avoids per-entry
syscalls.  The cost is paying *after* the bytes have been parsed,
when redo materializes them into the in-memory tree.

### What JE does

`com.sleepycat.je.recovery.RecoveryManager.redo` does not allocate
per-LN.  It reuses a `LogEntry` and `DatabaseEntry` instance across
all redo iterations, and the in-memory BIN slot stores
`(byte[] key, long lsn, byte[] data)` where `data` is a reference
into the log's read-only byte buffer — there is no per-slot copy
on redo.  JE's BIN backing storage is one large `byte[]` per BIN
with packed offsets; insertion writes into pre-allocated slot
arrays.

### Optimization hypothesis (→ wave 11-K)

1. **Avoid per-record `Vec<u8>` allocation in `redo_ln`.**  Either:
   * Hold the parsed `LnRecord`'s `Bytes` and pass `&[u8]`
     references straight into a new `Tree::redo_insert(&[u8] key,
     &[u8] data, Lsn)` that copies into the BIN slot once
     (eliminates two of the three allocations), **or**
   * Add a fast-path bulk-redo API on `Tree` that takes an
     iterator of `(Bytes key, Bytes data, Lsn)` and reserves BIN
     capacity per BIN before inserting (eliminates the geometric
     `Vec::resize` cost inside `BinStub::insert_with_prefix`).
2. **Reuse a scratch `Vec<u8>` across LN parses.**
   `parse_payload` and the wrappers around it currently allocate a
   fresh `Vec<u8>` for the deserialized key on each entry; the
   `bytes::owned_clone`/`owned_drop` 6.6 % shows up here.  A
   `RecoveryScratch { key_buf: Vec<u8>, data_buf: Vec<u8> }` reused
   across the redo loop will close most of that.
3. **Pre-warm BIN allocations.**  Before redo, walk the analysis
   pass's per-BIN dirty-slot map and call `entries.reserve(n)`
   once.  This skips `_int_malloc`/`malloc_consolidate` for the
   inserts that follow.

### Estimated LOC range

200–400 LOC across `crates/noxu-recovery/src/recovery_manager.rs`
(redo loop), `crates/noxu-tree/src/tree.rs` (new
`Tree::redo_insert` or `Tree::redo_apply_batch`), and
`crates/noxu-dbi/src/file_manager_scanner.rs` (scratch buffer for
`parse_entry_from_bytes`).  No on-disk format change; the public
API surface in `noxu-db` is unaffected (recovery is internal).

### Confidence

**Medium.**  The allocator dominates the profile, and removing
per-record allocations in redo is mechanically straightforward,
but JE's lead is partially structural (its BIN backing store is
fundamentally cheaper than `Vec<BinEntry>`).  We expect to close
the gap to ~1.5× (the roadmap's 11-K acceptance bar) without
restructuring the BIN backing store, but going below 1.3× likely
requires that deeper change.

### Correctness risk

**Medium — recovery touches durability invariants.**  The redo
path's contract ("the slot's LSN equals the log record's LSN after
redo") must be preserved exactly.  Existing recovery tests in
`crates/noxu-recovery/src/recovery_manager.rs::tests` and
`crates/noxu-engine/tests/` plus the
`noxu_spec::recovery` stateright spec cover the invariant.  Add
two new property tests:

* For a randomly populated, cleanly closed env, the post-recovery
  tree returned by the new redo path is byte-equal to the
  pre-close tree.
* For an env with an injected mid-batch crash, the
  `redo_apply_batch` path produces the same final state as the
  per-record path.

---

## Cross-workload findings

Two patterns appear in *every* profile and suggest cross-cutting
follow-ups outside the 11-I/J/K plan:

* **Per-operation `Vec<u8>` allocation for keys.**
  `Database::put`/`get` allocate a fresh `Vec<u8>` from the
  caller's `DatabaseEntry`, and `Bytes::owned_clone`/`owned_drop`
  show up in three of four profiles.  A pooled scratch buffer (or
  an end-to-end `&[u8]` API) is a 1–3 % win on every workload.
* **`crc32fast::baseline::update_fast_16` (2.6–2.8 %).**  CRC is
  computed on every log entry on the write path *and* every read
  during recovery scan.  Hardware CLMUL is in use already (15.8
  GiB/s at 1 KiB).  Not a 11-I/J/K target but worth noting for
  later.

---

## Recommended sprint structure

After ranking by **expected gap-closing per LOC**:

| Order | Wave | Workload(s) | Estimated LOC | Expected gap close | Confidence |
|---|---|---|---:|---|---|
| 1st | **11-I** | W03 + W04 (and ~30–40 % of W10) | 50–150 | W03/W04 → ≤ 1.2× JE; W10 → ≤ 1.7× JE on tmpfs | **high** |
| 2nd | **11-K** | W11 | 200–400 | W11 → ≤ 1.5× JE | medium |
| 3rd | **11-J** | W10 (NVMe story) | 200–400 + ~150 stretch | W10 → ≤ 1.3× JE on NVMe | medium (gated on 11-C) |

### Why this order

1. **11-I first.**  Highest leverage per LOC.  The cursor double-
   descent + linear-scan removal is a small, mechanical, well-
   tested change that materially improves *three* of the four
   workloads (W03, W04, and ~30–40 % of the W10 gap because every
   reader/writer thread reuses the same code path).  No format
   change, no concurrency rework, low correctness risk.

2. **11-K second.**  W11 is a one-time-per-startup cost; it's
   user-visible only at the recovery latency level.  But it's
   bounded scope (the redo loop), the optimization is mechanical
   (eliminate per-record allocations, pre-reserve BIN capacity),
   and once 11-I is in 11-K is the largest remaining single-
   workload gap.

3. **11-J last.**  Two reasons.  First, the W10 acceptance gate is
   defined against the 11-C *NVMe* re-run, and we don't have those
   numbers yet; on tmpfs the gap is dominated by tree-walk cost,
   not fsync.  Second, the FsyncManager rework is the highest-risk
   change of the three (touches concurrency primitives, needs
   stateright coverage).  Postponing it gives 11-I + 11-K time to
   land first and lets 11-C numbers refine the scope.

### Acceptance metrics for the next three waves

* 11-I succeeds when:
  * `make benchmarks` shows W03/W04 100 K within 1.2× of JE on
    the same hardware as v2.3.0,
  * No regression on W01/W05/W06/W09 (the Noxu-wins workloads),
  * All existing `cargo nextest run --workspace` tests pass plus
    the one new property test described above.

* 11-K succeeds when:
  * W11 100 K closes to within 1.5× of JE,
  * `noxu-recovery`'s existing tests + the two new property tests
    described above pass,
  * `noxu_spec::recovery` stateright run is still green.

* 11-J succeeds when:
  * On the 11-C NVMe re-run, W10 4r4w + 8r8w close to within 1.3×
    of JE,
  * No regression on W02/W07/W08 (the parity workloads),
  * Stateright spec for fsync ordering passes (new spec, ≤ 100
    LOC, captures "every committed txn's LSN ≤ flushed LSN").

---

## Reproducing this report

```bash
cd benches/profiles
cargo build --profile bench-profile -p noxu-perf-profiler

# Each workload, with the exact arguments used for the captures in
# benches/profiles/captures/.
perf record --call-graph dwarf -F 999 \
    -o captures/w03/w03.perf.data \
    -- ../../target/bench-profile/noxu-perf-profiler \
       --workload w03 --scale 10000 --repeats 200

# (likewise w04, w10, w11 — see captures/README.md)

perf report -i captures/w03/w03.perf.data --no-children \
    --stdio --percent-limit 0.5
perf report -i captures/w03/w03.perf.data \
    --stdio --percent-limit 0.5 -g graph,0.5,callee
```

The committed `top_self.txt` and `calltree.txt` files are the
output of those two `perf report` invocations.
